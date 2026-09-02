# derived from specs/001-s3-local-server/contracts/s3-surface.md (objects,
# T025, SC-006); replaces tinio-server/tests/data_plane.rs
@T025 @FR-003 @FR-011 @FR-013 @FR-022 @SC-006
Feature: Object data plane

  Scenario: Full round trip with delete
    Given I create bucket "data"
    Given I upload "data/hello.txt" with body "hello world"
    Then the response status is 200
    Given I upload "data/empty" with 0 bytes
    And the response status is 200
    Given I upload "data/dir/sub/deep.txt" with 4 bytes
    When I get object "data/hello.txt"
    Then the response status is 200
    And the object body is "hello world"
    And the object ETag matches the MD5 of the uploaded bytes
    And the response header "Content-Type" is "text/plain"
    When I head object "data/empty"
    Then the response status is 200
    When I get object "data/empty"
    Then the object body length is 0
    And the object ETag matches the MD5 of the uploaded bytes
    And the response header "Content-Length" is "0"
    When I delete object "data/hello.txt"
    Then the response status is 204
    Then I send a "GET" request to "/data/hello.txt"
    And the response status is 404
    And the error code is "NoSuchKey"

  # Task 8: the range matrix is parameterized over representative windows
  # (the mem object suite's clamp/suffix semantics:
  # get_clamps_inclusive_range_to_object_size,
  # get_suffix_larger_than_object_returns_all,
  # unsatisfiable_ranges_are_invalid_range).

  Scenario Outline: Range requests answer 206 with the requested window
    Given I create bucket "data"
    And I upload "data/digits" with body "0123456789"
    When I send a "GET" request to "/data/digits" with headers
      | Range | <range> |
    Then the response status is <status>
    And the response header "Content-Range" is "<content-range>"
    And the object body is "<body>"

    Examples:
      | range      | status | content-range | body       |
      | bytes=2-5  | 206    | bytes 2-5/10  | 2345       |
      | bytes=-3   | 206    | bytes 7-9/10  | 789        |
      | bytes=8-99 | 206    | bytes 8-9/10  | 89         |
      | bytes=-100 | 206    | bytes 0-9/10  | 0123456789 |

  Scenario Outline: Unsatisfiable ranges answer 416
    Given I create bucket "data"
    And I upload "data/digits" with body "0123456789"
    When I send a "GET" request to "/data/digits" with headers
      | Range | <range> |
    Then the response status is 416
    And the error code is "InvalidRange"

    Examples:
      | range      |
      | bytes=99-  |
      | bytes=10-  |
      | bytes=10-20 |
      | bytes=-0   |

  # Task 8: PUT overwrite semantics (mem put_overwrites_existing_object)
  # and the legal-key matrix (core object valid_keys_accepted).

  Scenario: Overwrite replaces the object
    Given I create bucket "data"
    And I upload "data/overwrite.txt" with body "old"
    When I upload "data/overwrite.txt" with body "new-bytes"
    Then the response status is 200
    When I get object "data/overwrite.txt"
    Then the response status is 200
    And the object body is "new-bytes"
    And the object body length is 9
    And the object ETag matches the MD5 of the uploaded bytes

  Scenario Outline: Legal keys are accepted
    Given I create bucket "data"
    When I upload "data/<key>" with body ""
    Then the response status is 200

    Examples:
      | key              |
      | a                |
      | dir/file.txt     |
      | dir/sub/file.txt |
      | with%20space.txt |
      | %C3%BCmlaut.txt  |
      | dir/             |

  Scenario: Conditional requests answer 304 and 412
    Given I create bucket "data"
    And I upload "data/cond.txt" with body "v1"
    Then the response header "ETag" is stored
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-None-Match | {etag} |
    Then the response status is 304
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Match | {etag} |
    Then the response status is 200
    And the object body is "v1"
    When I send a "GET" request to "/data/cond.txt" with headers
      | If-Match | "deadbeefdeadbeefdeadbeefdeadbeef" |
    Then the response status is 412
    When I send a "PUT" request to "/data/cond.txt" with headers
      | If-None-Match | * |
    Then the response status is 412
    When I send a "PUT" request to "/data/fresh.txt" with headers
      | If-None-Match | * |
    Then the response status is 200

  Scenario: Folder markers are never objects
    Given I create bucket "data"
    And I upload "data/dir/" with body ""
    And the response status is 200
    When I list objects under "data/" with delimiter "/"
    Then the listing shows 0 keys
    When I send a "GET" request to "/data/dir/"
    Then the response status is 404
    And the error code is "NoSuchKey"
    When I send a "HEAD" request to "/data/dir/"
    Then the response status is 404
    Given I upload "data/dir/file.txt" with body "kept"
    When I delete object "data/dir/"
    Then the response status is 204
    When I get object "data/dir/file.txt"
    Then the response status is 200
    And the object body is "kept"
    When I delete object "data/dir/file.txt"
    When I delete object "data/dir/"
    Then the response status is 204

  Scenario: Concurrent writes never tear objects
    Given I create bucket "data"
    When I concurrently upload "data/shared.bin" and "data/shared.bin" with 4096 bytes each
    Then the object body length is 4096

  @fs
  Scenario: Interrupted upload leaves no partial object
    Given I create bucket "data"
    When I interrupt the upload of "data/aborted.bin" after 1024 of 1048576 bytes
    When I get object "data/aborted.bin"
    Then the response status is 404
    And the error code is "NoSuchKey"
    And no temp file remains under the state dir

  @fs
  Scenario: Out-of-band changes are served immediately
    Given I create bucket "data"
    And I write "out-of-band" to "data/dropped.txt" in the served root
    When I get object "data/dropped.txt"
    Then the response status is 200
    And the object body is "out-of-band"
    And the object ETag is the MD5 of "out-of-band"
    When I list objects under "data/"
    Then the listing shows 1 key

  # x-amz-meta-* user metadata is accepted on upload and dropped — never
  # stored, never echoed (contracts/s3-surface.md behavior notes).
  Scenario: User metadata is accepted and dropped
    Given I create bucket "data"
    When I send a "PUT" request to "/data/meta.txt" with headers and body "payload"
      | x-amz-meta-color | blue |
    Then the response status is 200
    When I send a "GET" request to "/data/meta.txt"
    Then the response status is 200
    And the response header "x-amz-meta-color" is absent
    When I send a "HEAD" request to "/data/meta.txt"
    Then the response status is 200
    And the response header "x-amz-meta-color" is absent

  # Content-Type is inferred from the key extension; an unknown extension
  # falls back to application/octet-stream (mime_guess).
  Scenario: Unknown extensions fall back to octet-stream
    Given I create bucket "data"
    And I upload "data/blob.zzz9" with body "x"
    When I send a "GET" request to "/data/blob.zzz9"
    Then the response status is 200
    And the response header "Content-Type" is "application/octet-stream"

  # Server-side copy (FR-015): the content never passes through the client.
  # Same-bucket, cross-bucket, overwrite, and the missing-source error.
  Scenario: Copy object within a bucket
    Given I create bucket "data"
    And I upload "data/src.txt" with body "copied content"
    When I copy object "data/src.txt" to "data/dst.txt"
    Then the response status is 200
    And the response body contains "<CopyObjectResult>"
    When I get object "data/dst.txt"
    Then the response status is 200
    And the object body is "copied content"
    And the object ETag matches the MD5 of the uploaded bytes

  Scenario: Copy object across buckets
    Given I create bucket "src"
    And I create bucket "dst"
    And I upload "src/file.bin" with body "cross-bucket"
    When I copy object "src/file.bin" to "dst/file.bin"
    Then the response status is 200
    When I get object "dst/file.bin"
    Then the response status is 200
    And the object body is "cross-bucket"

  Scenario: Copy overwrites an existing destination
    Given I create bucket "data"
    And I upload "data/src.txt" with body "new"
    And I upload "data/dst.txt" with body "old"
    When I copy object "data/src.txt" to "data/dst.txt"
    Then the response status is 200
    When I get object "data/dst.txt"
    Then the response status is 200
    And the object body is "new"
    And the object body length is 3

  Scenario: Copy of a missing source answers NoSuchKey
    Given I create bucket "data"
    When I copy object "data/missing.txt" to "data/dst.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"
