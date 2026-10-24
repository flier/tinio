# S3 Object Lock (spec FR-043; design
# docs/superpowers/specs/2026-10-20-s3-object-lock-design.md OL1-OL31;
# protocol home contracts/s3-surface.md §Object Lock): the six operations, the
# CreateBucket header, the write headers and their checksum rule, the versioning
# states, the delete protection and the bulk per-key answer, the GET/HEAD echo
# (including the null version's identity wire), the marker mix, the multipart
# completion refusal, a bucket with no configuration (the negative-cache path),
# the capability off and read-only mode.
#
# The corpus is written against the contract, so its wire shapes are the ones
# s3s models: `?object-lock`, `?retention` and `?legal-hold`, with a
# `Content-MD5` on every XML write (the body is consumed before the handler, so
# the digest is presence-only — the same deviation the CORS, ACL and lifecycle
# surfaces record). The dates are fixed in the future rather than computed: a
# scenario asserts a refusal or an echo, never the clock.
@object-lock @FR-043
Feature: S3 Object Lock

  # The CreateBucket header is the one-step enablement (OL1, OL13): the bucket,
  # its `Enabled` versioning state and the configuration are written together;
  # `false` is an ordinary bucket; anything else is s3s's bool parser refusing
  # the request before the handler runs.
  Scenario: The CreateBucket header enables versioning and stores the configuration
    Given I create bucket "data" with the object lock header
    When I send a "GET" request to "/data?object-lock"
    Then the response status is 200
    And the response body contains "<ObjectLockEnabled>Enabled</ObjectLockEnabled>"
    And the response body does not contain "<Rule>"
    When I send a "GET" request to "/data?versioning"
    Then the response status is 200
    And the response body contains "<Status>Enabled</Status>"
    When I send a "PUT" request to "/plain" with headers
      | x-amz-bucket-object-lock-enabled | false |
    Then the response status is 200
    When I send a "GET" request to "/plain?object-lock"
    Then the response status is 404
    And the error code is "ObjectLockConfigurationNotFoundError"
    When I send a "PUT" request to "/refused" with headers
      | x-amz-bucket-object-lock-enabled | yes |
    Then the response status is 400
    And the error code is "InvalidArgument"

  # The six operations round-trip: the configuration (with a default rule), the
  # per-version retention (the default first, then an explicit replacement) and
  # the legal hold (set, then cleared — the retention keeps the lock row).
  Scenario: The six object lock operations round-trip
    Given I create bucket "data" with the object lock header
    When I put this object lock configuration on bucket "data"
      """xml
      <ObjectLockConfiguration>
        <ObjectLockEnabled>Enabled</ObjectLockEnabled>
        <Rule>
          <DefaultRetention>
            <Mode>GOVERNANCE</Mode>
            <Days>1</Days>
          </DefaultRetention>
        </Rule>
      </ObjectLockConfiguration>
      """
    Then the response status is 200
    When I send a "GET" request to "/data?object-lock"
    Then the response status is 200
    And the response body contains "<ObjectLockEnabled>Enabled</ObjectLockEnabled>"
    And the response body contains "<Mode>GOVERNANCE</Mode>"
    And the response body contains "<Days>1</Days>"
    # An explicit retention and a legal hold on the write.
    When I send a "PUT" request to "/data/notes.txt" with headers and body "first"
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | x-amz-object-lock-legal-hold | ON |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "notes"
    When I send a "GET" request to "/data/notes.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>COMPLIANCE</Mode>"
    And the response body contains "<RetainUntilDate>"
    When I send a "GET" request to "/data/notes.txt?legal-hold"
    Then the response status is 200
    And the response body contains "<Status>ON</Status>"
    # Clearing the hold keeps the compliance retention.
    When I put this legal hold on object "data/notes.txt" at version "{notes}"
      """xml
      <LegalHold><Status>OFF</Status></LegalHold>
      """
    Then the response status is 200
    When I send a "GET" request to "/data/notes.txt?legal-hold&versionId={notes}"
    Then the response status is 200
    And the response body contains "<Status>OFF</Status>"
    # The compliance retention the cleared hold never touched is still there.
    When I send a "GET" request to "/data/notes.txt?retention&versionId={notes}"
    Then the response status is 200
    And the response body contains "<Mode>COMPLIANCE</Mode>"
    # The default rule applies to a write that carries no lock header (and its
    # retention is what the checksum rule keys on — the digest is supplied).
    When I send a "PUT" request to "/data/later.txt" with headers and body "later"
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data/later.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>GOVERNANCE</Mode>"
    # A governance retention may be replaced by a stricter compliance one.
    When I put this retention on object "data/later.txt"
      """xml
      <Retention>
        <Mode>COMPLIANCE</Mode>
        <RetainUntilDate>2035-01-01T00:00:00Z</RetainUntilDate>
      </Retention>
      """
    Then the response status is 200
    When I send a "GET" request to "/data/later.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>COMPLIANCE</Mode>"

  # CopyObject does not inherit the source's lock (OL12): the destination's
  # headers or the bucket default apply, and a destination with neither has no
  # per-object lock.
  Scenario: A copy takes the destination's lock, not the source's
    Given I create bucket "data" with the object lock header
    And I send a "PUT" request to "/data/src.txt" with headers and body "source"
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I copy object "data/src.txt" to "data/dst.txt"
    Then the response status is 200
    When I send a "GET" request to "/data/dst.txt?retention"
    Then the response status is 404
    And the error code is "NoSuchObjectLockConfiguration"
    When I send a "PUT" request to "/data/dst2.txt" with headers
      | x-amz-copy-source | /data/src.txt |
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data/dst2.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>GOVERNANCE</Mode>"
    When I send a "GET" request to "/data/src.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>COMPLIANCE</Mode>"

  # OL13: a lock-enabled bucket can never return to unversioned or be
  # suspended, and the first configuration requires versioning `Enabled`.
  Scenario: A locked bucket cannot be suspended and a first configuration needs versioning
    Given I create bucket "data" with the object lock header
    When I send a "PUT" request to "/data?versioning" with headers and body "<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"
      | Content-Type | application/xml |
    Then the response status is 409
    And the error code is "InvalidBucketState"
    Given I create bucket "plain"
    When I put this object lock configuration on bucket "plain"
      """xml
      <ObjectLockConfiguration>
        <ObjectLockEnabled>Enabled</ObjectLockEnabled>
      </ObjectLockConfiguration>
      """
    Then the response status is 409
    And the error code is "InvalidBucketState"

  # OL17: a lock header is only meaningful on a bucket with a configuration;
  # the mode/date pair is all-or-nothing, and an unknown mode is the closed
  # enum's argument refusal.
  Scenario: The write headers are refused without a configuration or without their pair
    Given I create bucket "plain"
    And I enable versioning on "plain"
    When I send a "PUT" request to "/plain/a.txt" with headers and body "payload"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    Given I create bucket "data" with the object lock header
    When I send a "PUT" request to "/data/half.txt" with headers and body "payload"
      | x-amz-object-lock-mode | GOVERNANCE |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "PUT" request to "/data/date-only.txt" with headers and body "payload"
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "PUT" request to "/data/bad.txt" with headers and body "payload"
      | x-amz-object-lock-mode | SOMETIMES |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "InvalidArgument"

  # OL16: the checksum requirement belongs to the effective retention, never to
  # the legal hold alone.
  Scenario: A retention-bearing write requires a checksum
    Given I create bucket "data" with the object lock header
    When I send a "PUT" request to "/data/none.txt" with headers and body "payload"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "PUT" request to "/data/bad.txt" with headers and body "payload"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | not-a-digest |
    Then the response status is 400
    And the error code is "InvalidDigest"
    # A checksum header satisfies the requirement by presence; `[s3] checksum`
    # is off in this build, so the crc32 value is not verified — the leg pins
    # the accepted shape, not a validated digest.
    When I send a "PUT" request to "/data/crc.txt" with headers and body "payload"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | x-amz-checksum-crc32 | AAAAAA== |
    Then the response status is 200
    When I send a "PUT" request to "/data/hold.txt" with headers and body "payload"
      | x-amz-object-lock-legal-hold | ON |
    Then the response status is 200
    When I send a "GET" request to "/data/hold.txt?legal-hold"
    Then the response status is 200
    And the response body contains "<Status>ON</Status>"

  # OL8/OL11/OL15: a permanent delete of a protected version is 403; at the S3
  # tier there is no identity plane, so the bypass header has no authority and
  # a GOVERNANCE version refuses it too.
  Scenario: A protected permanent delete is refused
    Given I create bucket "data" with the object lock header
    And I send a "PUT" request to "/data/comp.txt" with headers and body "comp"
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "comp"
    And I send a "PUT" request to "/data/gov.txt" with headers and body "gov"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "gov"
    When I send a "DELETE" request to "/data/comp.txt?versionId={comp}"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/gov.txt?versionId={gov}" with headers
      | x-amz-bypass-governance-retention | true |
    Then the response status is 403
    And the error code is "AccessDenied"
    # Both versions survive the refused deletes.
    When I send a "GET" request to "/data/comp.txt?versionId={comp}"
    Then the response status is 200
    And the object body is "comp"
    When I send a "GET" request to "/data/gov.txt?versionId={gov}"
    Then the response status is 200
    And the object body is "gov"

  # OL17: a protected entry in DeleteObjects is a per-key AccessDenied and the
  # remaining entries proceed.
  Scenario: A bulk delete refuses only the protected entry
    Given I create bucket "data" with the object lock header
    And I send a "PUT" request to "/data/protected.txt" with headers and body "protected"
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "protected"
    And I upload "data/open.txt" with body "open"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "open"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>protected.txt</Key><VersionId>{protected}</VersionId></Object><Object><Key>open.txt</Key><VersionId>{open}</VersionId></Object></Delete>"
      | Content-Type | application/xml |
    Then the response status is 200
    And the response body contains "<Error>"
    And the response body contains "<Code>AccessDenied</Code>"
    And the response body contains "<Key>protected.txt</Key>"
    And the response body contains "<Deleted>"
    And the response body contains "<Key>open.txt</Key>"
    When I send a "GET" request to "/data/open.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"
    When I send a "GET" request to "/data/protected.txt?versionId={protected}"
    Then the response status is 200
    And the object body is "protected"

  # OL18: the lock headers ride GET and HEAD, for the current version and for
  # an explicitly addressed one.
  Scenario: GET and HEAD echo the lock headers
    Given I create bucket "data" with the object lock header
    And I send a "PUT" request to "/data/echo.txt" with headers and body "echo"
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | x-amz-object-lock-legal-hold | ON |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "echo"
    When I get object "data/echo.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is "GOVERNANCE"
    And the response header "x-amz-object-lock-legal-hold" is "ON"
    And the response has a "x-amz-object-lock-retain-until-date" header
    When I head object "data/echo.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is "GOVERNANCE"
    And the response header "x-amz-object-lock-legal-hold" is "ON"
    And the response has a "x-amz-object-lock-retain-until-date" header
    When I send a "GET" request to "/data/echo.txt?versionId={echo}"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is "GOVERNANCE"
    And the response header "x-amz-object-lock-legal-hold" is "ON"
    And the response has a "x-amz-object-lock-retain-until-date" header

  # OL18's identity-wire case: the null version has no id, so a lookup keyed by
  # `Option<Id>` would skip it. It becomes reachable through a suspended-bucket
  # null write followed by a versioning re-enable and a retention addressed as
  # `versionId=null`.
  Scenario: A locked null version echoes its lock headers
    Given I create bucket "data"
    And I enable versioning on "data"
    And I suspend versioning on "data"
    And I upload "data/null.txt" with body "null"
    Then the response status is 200
    When I enable versioning on "data"
    And I put this object lock configuration on bucket "data"
      """xml
      <ObjectLockConfiguration>
        <ObjectLockEnabled>Enabled</ObjectLockEnabled>
      </ObjectLockConfiguration>
      """
    Then the response status is 200
    When I put this retention on object "data/null.txt" at version "null"
      """xml
      <Retention>
        <Mode>GOVERNANCE</Mode>
        <RetainUntilDate>2035-01-01T00:00:00Z</RetainUntilDate>
      </Retention>
      """
    Then the response status is 200
    When I get object "data/null.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is "GOVERNANCE"
    When I send a "GET" request to "/data/null.txt?versionId=null"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is "GOVERNANCE"
    And the response header "x-amz-version-id" is "null"

  # OL9's three marker refusals: the explicitly addressed marker is 405, the
  # unaddressed request whose current version is a marker is the plain read's
  # 404 NoSuchKey, and a non-marker version without a lock is 404
  # NoSuchObjectLockConfiguration.
  Scenario: The marker mix answers 405, 404 NoSuchKey and 404 NoSuchObjectLockConfiguration
    Given I create bucket "data" with the object lock header
    And I upload "data/m.txt" with body "marked"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "opened"
    When I send a "GET" request to "/data/m.txt?retention"
    Then the response status is 404
    And the error code is "NoSuchObjectLockConfiguration"
    When I send a "DELETE" request to "/data/m.txt"
    Then the response status is 204
    And the response header "x-amz-delete-marker" is "true"
    And the response header "x-amz-version-id" is stored as "marker"
    When I send a "GET" request to "/data/m.txt?retention"
    Then the response status is 404
    And the error code is "NoSuchKey"
    When I send a "GET" request to "/data/m.txt?retention&versionId={marker}"
    Then the response status is 405
    And the error code is "MethodNotAllowed"
    When I send a "GET" request to "/data/m.txt?legal-hold&versionId={marker}"
    Then the response status is 405
    And the error code is "MethodNotAllowed"

  # OL6/OL17: the create-time intent is the only lock fact a completion may
  # act on, so a lock or bypass header on CompleteMultipartUpload is refused
  # before the parts are parsed — and the upload stays open.
  Scenario: CompleteMultipartUpload rejects lock headers
    Given I create bucket "data" with the object lock header
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "data"
    When I complete the multipart upload with headers
      | x-amz-object-lock-mode | GOVERNANCE |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I complete the multipart upload with headers
      | x-amz-bypass-governance-retention | true |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I complete the multipart upload
    Then the response status is 200

  # OL18's negative cache: a bucket with no configuration answers the config
  # read's own 404, skips the per-version lock read behind the cached `false`
  # (observable as the absent echo headers) and refuses the per-object reads
  # with the missing-configuration 400.
  Scenario: A bucket with no configuration serves no lock headers
    Given I create bucket "plain"
    And I upload "plain/a.txt" with body "hello"
    When I send a "GET" request to "/plain?object-lock"
    Then the response status is 404
    And the error code is "ObjectLockConfigurationNotFoundError"
    When I get object "plain/a.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is absent
    And the response header "x-amz-object-lock-legal-hold" is absent
    When I head object "plain/a.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is absent
    And the response header "x-amz-object-lock-retain-until-date" is absent
    When I send a "GET" request to "/plain/a.txt?retention"
    Then the response status is 400
    And the error code is "InvalidRequest"

  # OL20: the capability off refuses the six surfaces while enforcement keeps
  # running — the CreateBucket header is not gated, a protected delete still
  # 403s, and the GET/HEAD echo is skipped.
  @object-lock-off
  Scenario: The capability off answers NotImplemented and keeps enforcing
    Given I create bucket "data" with the object lock header
    When I send a "GET" request to "/data?object-lock"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I put this object lock configuration on bucket "data"
      """xml
      <ObjectLockConfiguration>
        <ObjectLockEnabled>Enabled</ObjectLockEnabled>
      </ObjectLockConfiguration>
      """
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "GET" request to "/data/a.txt?retention"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I put this retention on object "data/a.txt"
      """xml
      <Retention>
        <Mode>GOVERNANCE</Mode>
        <RetainUntilDate>2035-01-01T00:00:00Z</RetainUntilDate>
      </Retention>
      """
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "GET" request to "/data/a.txt?legal-hold"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I put this legal hold on object "data/a.txt"
      """xml
      <LegalHold><Status>ON</Status></LegalHold>
      """
    Then the response status is 501
    And the error code is "NotImplemented"
    # Enforcement is not a surface: an ungated write still receives the lock...
    When I send a "PUT" request to "/data/kept.txt" with headers and body "kept"
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "kept"
    # ... the protected delete still refuses ...
    When I send a "DELETE" request to "/data/kept.txt?versionId={kept}"
    Then the response status is 403
    And the error code is "AccessDenied"
    # ... and the echo is skipped with the capability off.
    When I get object "data/kept.txt"
    Then the response status is 200
    And the response header "x-amz-object-lock-mode" is absent

  # OL20: read-only serves the three reads and refuses the three writes. The
  # fixture is planted on disk because every mutating request — bucket create
  # included — answers AccessDenied here.
  @fs @read-only
  Scenario: A read-only run serves the three reads and refuses the three writes
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "GET" request to "/data?object-lock"
    Then the response status is 404
    And the error code is "ObjectLockConfigurationNotFoundError"
    When I send a "GET" request to "/data/a.txt?retention"
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "GET" request to "/data/a.txt?legal-hold"
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I put this object lock configuration on bucket "data"
      """xml
      <ObjectLockConfiguration>
        <ObjectLockEnabled>Enabled</ObjectLockEnabled>
      </ObjectLockConfiguration>
      """
    Then the response status is 403
    And the error code is "AccessDenied"
    When I put this retention on object "data/a.txt"
      """xml
      <Retention>
        <Mode>GOVERNANCE</Mode>
        <RetainUntilDate>2035-01-01T00:00:00Z</RetainUntilDate>
      </Retention>
      """
    Then the response status is 403
    And the error code is "AccessDenied"
    When I put this legal hold on object "data/a.txt"
      """xml
      <LegalHold><Status>ON</Status></LegalHold>
      """
    Then the response status is 403
    And the error code is "AccessDenied"

  # OL1/OL16: `PostObject` maps its form fields into `PutObjectInput`, so the
  # lock arrives as `x-amz-object-lock-*` form fields (and `content-md5`) rather
  # than headers. The form's default success status is s3s's 204.
  Scenario: A browser form POST carries the lock fields
    Given I create bucket "data" with the object lock header
    When I post an object form to bucket "data"
      | key | posted.txt |
      | x-amz-object-lock-mode | COMPLIANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
      | x-amz-object-lock-legal-hold | ON |
      | content-md5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 204
    When I send a "GET" request to "/data/posted.txt?retention"
    Then the response status is 200
    And the response body contains "<Mode>COMPLIANCE</Mode>"
    When I send a "GET" request to "/data/posted.txt?legal-hold"
    Then the response status is 200
    And the response body contains "<Status>ON</Status>"
    When I get object "data/posted.txt"
    Then the response status is 200
    And the object body is "post-body"
    And the response header "x-amz-object-lock-mode" is "COMPLIANCE"
    # The same form without the checksum is the retention write's 400: the
    # mapped form fields — not headers — drove the decision.
    When I post an object form to bucket "data"
      | key | refused.txt |
      | x-amz-object-lock-mode | GOVERNANCE |
      | x-amz-object-lock-retain-until-date | 2035-01-01T00:00:00Z |
    Then the response status is 400
    And the error code is "InvalidRequest"

  # The boto3 tier (FR-025): the same journey through the SDK, against the
  # spawned binary's identity-enforced plane — which is where the governance
  # bypass has an owner and can be honoured.
  @boto3
  Scenario: The object lock journey via the boto3 SDK
    When I run the boto3 script "boto3_object_lock.py"
    Then the external client output contains "BOTO3 OBJECT LOCK OK"
