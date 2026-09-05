# The 2026-09-05 bucket-CORS surface (spec design
# docs/superpowers/specs/2026-09-05-s3-cors-design.md, close of the gap
# analysis Tier A#2): the GetBucketCors/PutBucketCors/DeleteBucketCors
# trio, the anonymous OPTIONS preflight answered from the stored config,
# and the Access-Control-* decoration of actual responses. Scenario tags
# carry @FR-033 (contracts/s3-surface.md §Bucket CORS); the @cors-off
# scenario rides @FR-021 (the capability toggles) with the toggle itself
# pinned by FR-033.
#
# Pinning notes (mirror the server unit suite; the wire view can differ
# where s3s's XML shape gate fires first):
# - put requires Content-MD5 — AWS three-state: missing → 400 InvalidRequest
#   with the verbatim AWS message; malformed (not 16-byte base64) → 400
#   InvalidDigest. Digest equality against the body is NOT verified (s3s
#   consumes the body first — recorded deviation), so a wrong-but-well-
#   formed value passes.
# - an empty rule set is ALSO a 400, but at the wire s3s's XML
#   deserializer answers MalformedXML before the handler's ≥1-rule
#   InvalidRequest (unit-pinned) can fire — the feature pins the wire
#   answer (AWS answers MalformedXML here too).
# - preflight and decoration both use AWS first-match semantics: the FIRST
#   rule whose origin matches wins, method/headers validated WITHIN that
#   rule, no fall-through — the no-decoration leg of the decoration
#   scenario pins exactly this.
# - bare-`*` origin rules answer ACAO "*" and OMIT Allow-Credentials; a
#   concrete rule echoes the origin with Allow-Credentials: true (Q11).
# - preflight denies answer 403 AccessDenied (s3s has no AccessForbidden —
#   recorded deviation; the two AWS messages verbatim, incl. the "evalution"
#   typo) and carry no Access-Control-* headers; a well-formed missing
#   bucket / no-config bucket answer the SAME message (existence oracle
#   closed). Bare OPTIONS (no Origin + Access-Control-Request-Method) is
#   not a preflight and falls through to s3s's 501 "Unknown operation".
# - capability off (feature on): the trio answers 501 "{name} is disabled",
#   and the preflight route is not registered, so a browser OPTIONS falls
#   through to the s3s 501 too.

@cors @FR-033
Feature: Bucket CORS configuration and OPTIONS preflight

  @cors @FR-033
  Scenario: Bucket CORS round trip, replace-all, and delete
    Given I create bucket "data"
    # Put TWO rules with a valid Content-MD5 (base64 of 16 bytes; the
    # ops never verify the digest against the body — recorded deviation).
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><ID>allow-example</ID><AllowedOrigin>https://example.com</AllowedOrigin><AllowedOrigin>https://*.example.net</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedHeader>x-amz-*</AllowedHeader><AllowedHeader>content-type</AllowedHeader><ExposeHeader>ETag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>DELETE</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    # …the GET echoes both rules in the stored order…
    When I send a "GET" request to "/data?cors"
    Then the response status is 200
    And the response body contains "<CORSRule>"
    And the response body contains "<ID>allow-example</ID>"
    And the response body contains "<AllowedOrigin>https://example.com</AllowedOrigin>"
    And the response body contains "<AllowedOrigin>https://*.example.net</AllowedOrigin>"
    And the response body contains "<AllowedMethod>GET</AllowedMethod>"
    And the response body contains "<AllowedMethod>PUT</AllowedMethod>"
    And the response body contains "<AllowedHeader>x-amz-*</AllowedHeader>"
    And the response body contains "<ExposeHeader>ETag</ExposeHeader>"
    And the response body contains "<MaxAgeSeconds>300</MaxAgeSeconds>"
    And the response body contains "<AllowedOrigin>*</AllowedOrigin>"
    And the response body contains "<AllowedMethod>DELETE</AllowedMethod>"
    # …Put replaces the whole configuration — never a merge…
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><ID>replace-all</ID><AllowedOrigin>https://other.example.com</AllowedOrigin><AllowedMethod>HEAD</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data?cors"
    Then the response body contains "<ID>replace-all</ID>"
    And the response body contains "<AllowedOrigin>https://other.example.com</AllowedOrigin>"
    And the response body contains "<AllowedMethod>HEAD</AllowedMethod>"
    And the response body does not contain "allow-example"
    And the response body does not contain "<MaxAgeSeconds>"
    # …and Delete clears the config back to no-config…
    When I send a "DELETE" request to "/data?cors"
    Then the response status is 204
    When I send a "GET" request to "/data?cors"
    Then the response status is 404
    And the error code is "NoSuchCORSConfiguration"
    # …while the delete stays idempotent.
    When I send a "DELETE" request to "/data?cors"
    Then the response status is 204

  @cors @FR-033
  Scenario: PutBucketCors validates the configuration and requires Content-MD5
    Given I create bucket "data"
    # Missing Content-MD5 → 400 InvalidRequest, the AWS message verbatim.
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
    Then the response status is 400
    And the error code is "InvalidRequest"
    And the response body contains "Missing required header for this request: Content-MD5"
    # Malformed Content-MD5 (not 16-byte base64) → 400 InvalidDigest.
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | not-base64! |
    Then the response status is 400
    And the error code is "InvalidDigest"
    # An EMPTY rule set is a 400 — but at the wire it is s3s's XML shape
    # gate (MalformedXML: the config must carry at least one CORSRule)
    # that fires before the handler's ≥1-rule InvalidRequest (unit-pinned)
    # — the wire answer is what the feature pins. AWS answers MalformedXML
    # here too.
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "MalformedXML"
    # More than 100 rules → 400 InvalidRequest.
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    And the response body contains "at most 100 rules"
    # A method outside the five AWS values → 400 InvalidRequest.
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>PATCH</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    And the response body contains "Invalid AllowedMethod: PATCH"

  @cors @FR-033
  Scenario: OPTIONS preflight answers allowed origins and methods
    Given I create bucket "data"
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><ID>concrete</ID><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>https://*.example.net</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    # The concrete rule: 200 with the echoed origin, the RULE's method
    # list (grilling Q9 — not the requested method), and credentials on.
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://example.com |
      | Access-Control-Request-Method | PUT |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "https://example.com"
    And the response header "access-control-allow-methods" is "GET, PUT"
    And the response header "access-control-allow-credentials" is "true"
    # The wildcard rule matches a subdomain and answers its origin.
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://wild.example.net |
      | Access-Control-Request-Method | GET |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "https://wild.example.net"
    # The apex does NOT match the single-`*` wildcard (pinned tinio choice)
    # — the request stays a 403 denial, not a 200.
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://example.net |
      | Access-Control-Request-Method | GET |
    Then the response status is 403
    And the error code is "AccessDenied"

  @cors @FR-033
  Scenario: OPTIONS preflight denies disallowed and unconfigured buckets
    Given I create bucket "data"
    # No CORS config → 403 AccessDenied; the answer never reveals
    # whether the bucket exists ("CORS is not enabled for this bucket.").
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://example.com |
      | Access-Control-Request-Method | GET |
    Then the response status is 403
    And the error code is "AccessDenied"
    And the response body contains "CORS is not enabled for this bucket."
    And the response header "access-control-allow-origin" is absent
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    # A disallowed origin → 403 AccessDenied, the AWS mismatch message
    # verbatim (the "evalution" typo is AWS's own — kept).
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://foreign.com |
      | Access-Control-Request-Method | GET |
    Then the response status is 403
    And the error code is "AccessDenied"
    And the response body contains "CORSResponse: This CORS request is not allowed. This is usually because the evalution of Origin, request method / Access-Control-Request-Method or Access-Control-Request-Headers are not whitelisted by the resource&apos;s CORS spec."
    And the response header "access-control-allow-origin" is absent
    # Origin matches, method does not → denied. First-match semantics: the
    # method is validated WITHIN the origin-matching rule, no fall-through.
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://example.com |
      | Access-Control-Request-Method | DELETE |
    Then the response status is 403
    And the error code is "AccessDenied"
    And the response body contains "is not allowed. This is usually because the evalution of Origin"
    # A bare OPTIONS (non-browser probe) is not a preflight: s3s answers
    # its own 501 "Unknown operation".
    When I send a "OPTIONS" request to "/data/a.txt"
    Then the response status is 501
    And the error code is "NotImplemented"
    And the response body contains "Unknown operation"

  @cors @FR-033
  Scenario: Actual GET responses carry Access-Control-Allow-Origin
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><ID>concrete</ID><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    # A matching concrete origin decorates the actual response.
    When I send a "GET" request to "/data/a.txt" with headers
      | Origin | https://example.com |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "https://example.com"
    And the response header "access-control-allow-methods" is "GET, HEAD"
    And the response header "access-control-allow-credentials" is "true"
    # HEAD rides the same decoration (the rule allows it).
    When I send a "HEAD" request to "/data/a.txt" with headers
      | Origin | https://example.com |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "https://example.com"
    # A bare-* rule answers the literal "*" and omits Allow-Credentials
    # (the two are incompatible — Q11) — the request skips the concrete
    # rule's origin and lands on the * rule.
    When I send a "GET" request to "/data/a.txt" with headers
      | Origin | https://other.example.net |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "*"
    And the response header "access-control-allow-credentials" is absent
    # 4xx answers are decorated too (s3s encodes op errors as Ok bodies —
    # matches AWS).
    When I send a "GET" request to "/data/missing.txt" with headers
      | Origin | https://example.com |
    Then the response status is 404
    And the error code is "NoSuchKey"
    And the response header "access-control-allow-origin" is "https://example.com"
    # Second PUT: the first origin-matching rule now allows PUT only — a
    # GET from its origin is NOT decorated (method validated within the
    # winning rule, no fall-through to the later * rule)…
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>PUT</AllowedMethod></CORSRule><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt" with headers
      | Origin | https://example.com |
    Then the response status is 200
    And the response header "access-control-allow-origin" is absent
    # …while an origin that skips rule 1 entirely reaches the * rule.
    When I send a "GET" request to "/data/a.txt" with headers
      | Origin | https://other.example.net |
    Then the response status is 200
    And the response header "access-control-allow-origin" is "*"

  @cors-off @FR-033
  Scenario: Disabled cors answers NotImplemented on the bucket trio and leaves OPTIONS to s3s
    Given I create bucket "data"
    When I send a "GET" request to "/data?cors"
    Then the response status is 501
    And the error code is "NotImplemented"
    And the response body contains "GetBucketCors is disabled"
    When I send a "PUT" request to "/data?cors" with headers and body "<CORSConfiguration><CORSRule><AllowedOrigin>https://example.com</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 501
    And the error code is "NotImplemented"
    And the response body contains "PutBucketCors is disabled"
    When I send a "DELETE" request to "/data?cors"
    Then the response status is 501
    And the error code is "NotImplemented"
    And the response body contains "DeleteBucketCors is disabled"
    # The preflight route is not registered with the capability off: a
    # browser OPTIONS falls through to s3s's unknown-op 501 (the legacy
    # behavior — never an Access-Control-* answer).
    When I send a "OPTIONS" request to "/data/a.txt" with headers
      | Origin | https://example.com |
      | Access-Control-Request-Method | GET |
    Then the response status is 501
    And the error code is "NotImplemented"
    And the response body contains "Unknown operation"
