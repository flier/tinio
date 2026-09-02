# replaces the conditional-request and copy-range legs of
# tinio-server/tests/coverage_gaps.rs (RFC 7232 preconditions, the closed
# copy-source-range form)
@FR-003 @FR-014
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
