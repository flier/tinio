# replaces the conditional-request and copy-range legs of
# tinio-server/tests/coverage_gaps.rs (RFC 7232 preconditions, the closed
# copy-source-range form)
@FR-003 @FR-014 @FR-027 @FR-028 @FR-029
Feature: Request conditions

  Scenario: Conditional PUT on a missing key fails the precondition
    Given I create bucket "data"
    When I send a "PUT" request to "/data/missing.txt" with headers and body "x"
      | If-Match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 412
    When I send a "GET" request to "/data/missing.txt"
    Then the response status is 404
    When I send a "PUT" request to "/data/missing.txt" with headers and body "x"
      | If-None-Match | * |
    Then the response status is 200

  # Task 8: the UploadPartCopy validation legs of the server suite
  # (upload_part_copy_range_and_conditionals): malformed ranges answer
  # InvalidArgument, a failing source conditional answers 412 (never 304
  # on the copy path), and out-of-range part numbers answer InvalidPart.
  # The closed-range copy and completion legs were already here.

  Scenario: UploadPartCopy rejects an open source range
    Given I create bucket "src"
    And I create bucket "dst"
    And I upload "src/key.bin" with body "0123456789"
    Then the response header "ETag" is stored
    When I send a "PUT" request to "/dst/parts.bin?partNumber=1&uploadId=none" with headers
      | x-amz-copy-source | /src/key.bin |
      | x-amz-copy-source-range | bytes=0- |
    Then the response status is 400
    And the error code is "InvalidArgument"
    Given I start a multipart upload for "dst/parts.bin"
    And I upload part copy 1 of "/src/key.bin" with range "bytes=2-5"
    Then the response status is 200
    And the response body contains "<CopyPartResult>"
    When I upload part copy 2 of "/src/key.bin" with range "junk"
    Then the response status is 400
    And the error code is "InvalidArgument"
    When I upload part copy 0 of "/src/key.bin" with range "bytes=0-1"
    Then the response status is 400
    And the error code is "InvalidPart"
    When I send a "PUT" request to "/dst/parts.bin?partNumber=2&uploadId={upload_id}" with headers
      | x-amz-copy-source               | /src/key.bin |
      | x-amz-copy-source-if-none-match | {etag} |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I complete the multipart upload
    Then the response status is 200
    When I get object "dst/parts.bin"
    Then the response status is 200
    And the object body is "2345"

  # Task 8: the date-based precondition semantics of the server's
  # conditions suite (if_modified_since_fails_when_not_modified_after,
  # if_unmodified_since_fails_when_modified_after,
  # precedence_failing_date_wins_over_matching_etag). The equal-boundary
  # legs are pinned by fixed dates: an object's mtime always lies between
  # the 1970 and 2038 instants (the server compares sub-second mtimes
  # against the second-truncated date, so a same-second round-trip of the
  # stored Last-Modified would never satisfy "not modified after").

  Scenario: Date-based conditions answer 304 and 412
    Given I create bucket "data"
    And I upload "data/cond.txt" with body "v1"
    Then the response header "ETag" is stored
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Modified-Since | Thu, 01 Jan 2038 00:00:00 GMT |
    Then the response status is 304
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Modified-Since | Thu, 01 Jan 1970 00:00:00 GMT |
    Then the response status is 200
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Unmodified-Since | Thu, 01 Jan 1970 00:00:00 GMT |
    Then the response status is 412
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Unmodified-Since | Thu, 01 Jan 2038 00:00:00 GMT |
    Then the response status is 200
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-None-Match       | {etag} |
      | If-Unmodified-Since | Thu, 01 Jan 1970 00:00:00 GMT |
    Then the response status is 412
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Match            | {etag} |
      | If-Unmodified-Since | Thu, 01 Jan 1970 00:00:00 GMT |
    Then the response status is 200

  # Task 8: the weak-tag and wildcard comparison legs of the conditions
  # suite (if_match_requires_exact_strong_match,
  # if_none_match_fails_on_match) — RFC 9110 §13.1.

  Scenario: Weak tags and wildcards follow RFC 9110 comparison
    Given I create bucket "data"
    And I upload "data/cond.txt" with body "v1"
    Then the response header "ETag" is stored
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-None-Match | W/{etag} |
    Then the response status is 304
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Match | W/{etag} |
    Then the response status is 412
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Match | * |
    Then the response status is 200
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-None-Match | * |
    Then the response status is 304

  # FR-015: CopyObject source conditionals (x-amz-copy-source-if-*) are
  # evaluated per S3 semantics — a failing source precondition answers
  # 412 (never 304 on the copy path), like the UploadPartCopy legs above.
  Scenario: CopyObject honors source and destination conditionals
    Given I create bucket "src"
    And I create bucket "dst"
    And I upload "src/key.bin" with body "payload"
    Then the response header "ETag" is stored
    When I send a "PUT" request to "/dst/copy.bin" with headers
      | x-amz-copy-source               | /src/key.bin |
      | x-amz-copy-source-if-none-match | {etag}       |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I send a "PUT" request to "/dst/copy.bin" with headers
      | x-amz-copy-source             | /src/key.bin |
      | x-amz-copy-source-if-match    | {etag}       |
      | x-amz-copy-source-if-none-match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 200
    When I send a "PUT" request to "/dst/copy2.bin" with headers
      | x-amz-copy-source | /src/key.bin |
      | x-amz-if-match    | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 412
    When I send a "PUT" request to "/dst/copy3.bin" with headers
      | x-amz-copy-source | /src/key.bin |
      | x-amz-if-none-match | * |
    Then the response status is 200
    # The destination both-present 400 is a request-shape error and
    # answers BEFORE the source is resolved: the same request against a
    # MISSING source answers 400 InvalidRequest, never NoSuchKey (the
    # deliberate shape-first ordering).
    When I send a "PUT" request to "/dst/copy4.bin" with headers
      | x-amz-copy-source   | /src/absent.bin |
      | x-amz-if-match      | *               |
      | x-amz-if-none-match | *               |
    Then the response status is 400
    And the error code is "InvalidRequest"

  # 2026-08-31 s3-conditionals design, FR-027 (AWS conditional writes):
  # If-Match + If-None-Match together on the destination protocol are a
  # request-shape error — 400 InvalidRequest, rejected before the body is
  # staged — and If-None-Match: * against an existing object answers 412.

  Scenario: Conditional PUT on an existing object enforces the both-header 400
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data/a.txt" with headers and body "world"
      | If-Match      | * |
      | If-None-Match | * |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "PUT" request to "/data/a.txt" with headers and body "world"
      | If-None-Match | * |
    Then the response status is 412
    And the error code is "PreconditionFailed"

  Scenario: Conditional delete enforces If-Match on an existing object
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    Then the response header "ETag" is stored
    When I send a "DELETE" request to "/data/a.txt" with headers
      | If-Match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I send a "DELETE" request to "/data/a.txt" with headers
      | If-Match | {etag} |
    Then the response status is 204

  Scenario: Conditional delete of a missing object stays idempotent
    # AWS model text: "if the ETag matches or if the object doesn't
    # exist, the operation will return a 204" — the conditions gate an
    # existing object only.
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    Then the response header "ETag" is stored
    When I send a "DELETE" request to "/data/a.txt" with headers
      | If-Match | {etag} |
    Then the response status is 204
    When I send a "DELETE" request to "/data/a.txt" with headers
      | If-Match | {etag} |
    Then the response status is 204
    When I send a "DELETE" request to "/data/a.txt" with headers
      | x-amz-if-match-size | 0 |
    Then the response status is 204
    # A malformed (negative) size is a request-shape error: 400
    # InvalidArgument even on a missing key — never a state-dependent
    # answer.
    When I send a "DELETE" request to "/data/a.txt" with headers
      | x-amz-if-match-size | -1 |
    Then the response status is 400
    And the error code is "InvalidArgument"

  Scenario: Conditional delete honors the date and size conditions
    Given I create bucket "data"
    And I upload "data/b.txt" with body "hello"
    When I send a "DELETE" request to "/data/b.txt" with headers
      | x-amz-if-match-last-modified-time | Wed, 21 Oct 2015 07:28:00 GMT |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I send a "DELETE" request to "/data/b.txt" with headers
      | x-amz-if-match-size | 999 |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I send a "DELETE" request to "/data/b.txt" with headers
      | x-amz-if-match-size | 5 |
    Then the response status is 204

  # FR-029 (RFC 9110 §13.1.5): If-Range gates the Range header only — a
  # strong-ETag match serves the Range, a stale value ignores it.

  Scenario: If-Range with a matching validator serves the Range
    Given I create bucket "data"
    And I upload "data/range.txt" with body "hello"
    Then the response header "ETag" is stored
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4 |
      | If-Range | {etag}    |
    Then the response status is 206
    And the response body is "llo"

  Scenario: If-Range stale, weak, wildcard, or garbage ignores the Range
    Given I create bucket "data"
    And I upload "data/range.txt" with body "hello"
    Then the response header "ETag" is stored
    # A stale validator drops the Range (the full 200) — also when the
    # Range would be unsatisfiable (416 requires the Range to apply)…
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4                          |
      | If-Range | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 200
    And the response body is "hello"
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=99-100                       |
      | If-Range | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 200
    And the response body is "hello"
    # …a weak tag never strong-matches (the Range is dropped, not
    # served — pins the strong-comparison branch of the parser)…
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4 |
      | If-Range | W/{etag}  |
    Then the response status is 200
    And the response body is "hello"
    # …and a wildcard or an unparseable value is not a valid If-Range:
    # the header is ignored, so the Range is served as if absent.
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4 |
      | If-Range | *         |
    Then the response status is 206
    And the response body is "llo"
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4 |
      | If-Range | garbage   |
    Then the response status is 206
    And the response body is "llo"

  Scenario: If-Range matching answers 416 on an unsatisfiable Range
    Given I create bucket "data"
    And I upload "data/range.txt" with body "hello"
    Then the response header "ETag" is stored
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=99-100 |
      | If-Range | {etag}       |
    Then the response status is 416
    And the error code is "InvalidRange"
    # The RFC 7232 conditions evaluate before If-Range and the Range:
    # a failing If-Match answers 412 even over a Range — and a matching
    # If-None-Match answers 304 over an UNSATISFIABLE Range (the old
    # fetch-first code answered 416 here; the precondition wins).
    When I send a "GET" request to "/data/range.txt" with headers
      | Range    | bytes=2-4                              |
      | If-Match | "deadbeefdeadbeefdeadbeefdeadbeef"     |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    When I send a "GET" request to "/data/range.txt" with headers
      | Range          | bytes=99-100 |
      | If-None-Match  | {etag}       |
    Then the response status is 304

  # FR-027 position lock: the copy-source family keeps the RFC 9110
  # §13.2.2 order (If-Match first, then If-None-Match) — both-present on
  # the SOURCE is not a 400: `*` + `*` against an existing source answers
  # 412 from the If-None-Match step.

  Scenario: CopyObject source both-present keeps the RFC order (no 400)
    Given I create bucket "data"
    And I upload "data/src.txt" with body "hello"
    When I send a "PUT" request to "/data/dst.txt" with headers
      | x-amz-copy-source               | /data/src.txt |
      | x-amz-copy-source-if-match      | *             |
      | x-amz-copy-source-if-none-match | *             |
    Then the response status is 412
    And the error code is "PreconditionFailed"

  # FR-028 (AWS conditional writes on the complete): the checks evaluate
  # against the object CURRENTLY at the key — the one being replaced.

  Scenario: CompleteMultipartUpload honors If-None-Match
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with headers
      | If-None-Match | * |
    Then the response status is 200
    # The object now exists: a second conditional complete answers 412.
    Given I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "world"
    When I complete the multipart upload with headers
      | If-None-Match | * |
    Then the response status is 412
    And the error code is "PreconditionFailed"

  Scenario: CompleteMultipartUpload honors If-Match
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload
    Then the response status is 200
    # s3s 0.15 emits no ETag header on the complete response — the
    # current object's validator comes from a GET instead.
    When I get object "data/big.bin"
    Then the response status is 200
    And the response header "ETag" is stored
    # A matching If-Match replaces the current object…
    Given I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "world"
    When I complete the multipart upload with headers
      | If-Match | {etag} |
    Then the response status is 200
    # …a mismatching one answers 412…
    Given I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "again"
    When I complete the multipart upload with headers
      | If-Match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    # …and a missing destination answers NoSuchKey, never 412.
    Given I start a multipart upload for "data/fresh.bin"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with headers
      | If-Match | * |
    Then the response status is 404
    And the error code is "NoSuchKey"

  Scenario: CompleteMultipartUpload rejects conditional bad shapes
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "hello"
    # Both headers in one request → 400 (the exact InvalidRequest code)…
    When I complete the multipart upload with headers
      | If-Match      | * |
      | If-None-Match | * |
    Then the response status is 400
    And the error code is "InvalidRequest"
    # …and If-None-Match accepts `*` only (AWS: "a header you provided
    # implies functionality that is not implemented").
    When I complete the multipart upload with headers
      | If-None-Match | "abc" |
    Then the response status is 501
    And the error code is "NotImplemented"

  Scenario: AbortMultipartUpload honors If-Match-Initiated-Time
    Given I create bucket "data"
    And I start a multipart upload for "data/abort.bin"
    And I upload part 1 with body "hello"
    When I send a "DELETE" request to "/data/abort.bin?uploadId={upload_id}" with headers
      | x-amz-if-match-initiated-time | Wed, 21 Oct 2015 07:28:00 GMT |
    Then the response status is 412
    And the error code is "PreconditionFailed"
    # The upload survived the failed condition (a plain abort succeeds)…
    When I abort the multipart upload
    Then the response status is 204
    # …and a missing upload answers NoSuchUpload regardless of the
    # condition.
    When I send a "DELETE" request to "/data/abort.bin?uploadId=missing" with headers
      | x-amz-if-match-initiated-time | Wed, 21 Oct 2015 07:28:00 GMT |
    Then the response status is 404
    And the error code is "NoSuchUpload"

  # FR-027 (2026-09-02 #2): the specific-If-None-Match 501 is the shared
  # destination write-shape gate — real AWS answers it on PutObject too
  # ("a header you provided implies functionality that is not
  # implemented"). The gate is request-shape: it fires before the body is
  # staged, so a fresh key answers 501 as well — a non-matching specific
  # value must never fall through to a live comparison or a silent
  # overwrite.

  Scenario: Conditional PUT rejects a specific If-None-Match value
    Given I create bucket "data"
    And I upload "data/spc.txt" with body "hello"
    When I send a "PUT" request to "/data/spc.txt" with headers and body "world"
      | If-None-Match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "PUT" request to "/data/fresh.txt" with headers and body "world"
      | If-None-Match | "abc" |
    Then the response status is 501
    And the error code is "NotImplemented"

  Scenario: CopyObject destination rejects a specific If-None-Match value
    Given I create bucket "data"
    And I upload "data/csrc.txt" with body "hello"
    And I upload "data/cdst.txt" with body "existing"
    When I send a "PUT" request to "/data/cdst.txt" with headers
      | x-amz-copy-source   | /data/csrc.txt |
      | x-amz-if-none-match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 501
    And the error code is "NotImplemented"
