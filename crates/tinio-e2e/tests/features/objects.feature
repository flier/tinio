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

  # Task 11 (2026-08-31 s3-tagging-ops): GetObjectAttributes — the
  # requested subset of ETag / ObjectSize / StorageClass / Checksum /
  # ObjectParts of one object. Wire shape (s3s 0.15):
  # GET /{key}?attributes with the x-amz-object-attributes /
  # x-amz-max-parts / x-amz-part-number-marker headers; the response is
  # the GetObjectAttributes XML (the retained part list's total rides
  # the <PartsCount> member).
  # Task 12 (2026-09-03): the attributes scenarios below carry @FR-032
  # (contracts/s3-surface.md §GetObjectAttributes) — per-scenario tags,
  # because the feature-level tags above describe the pre-existing
  # scenarios.

  @FR-032
  Scenario: GetObjectAttributes answers the requested subset
    Given I create bucket "data"
    And I upload "data/plain.txt" with body "hello"
    # The header value may join the attributes with commas (the SDK wire
    # form) — each requested member is echoed…
    When I send a "GET" request to "/data/plain.txt?attributes" with headers
      | x-amz-object-attributes | ETag,ObjectSize |
    Then the response status is 200
    And the response body contains "<ETag>"
    And the response body contains "<ObjectSize>5</ObjectSize>"
    # …only the requested subset answers…
    When I send a "GET" request to "/data/plain.txt?attributes" with headers
      | x-amz-object-attributes | ObjectSize |
    Then the response status is 200
    And the response body contains "<ObjectSize>5</ObjectSize>"
    And the response body does not contain "<ETag>"
    # …a non-multipart object omits the ObjectParts container…
    When I send a "GET" request to "/data/plain.txt?attributes" with headers
      | x-amz-object-attributes | ETag,ObjectParts |
    Then the response status is 200
    And the response body contains "<ETag>"
    And the response body does not contain "<ObjectParts>"
    # …and a missing key answers NoSuchKey.
    When I send a "GET" request to "/data/missing.txt?attributes" with headers
      | x-amz-object-attributes | ETag |
    Then the response status is 404
    And the error code is "NoSuchKey"

  @FR-032
  Scenario: GetObjectAttributes paginates the retained part list
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with 5242881 bytes
    And I upload part 2 with 4096 bytes
    When I complete the multipart upload
    Then the response status is 200
    # The client-side max-parts cap truncates the page — the container
    # echoes the applied cap, the next marker, and the total count (a
    # completed object retains its assembly parts for this op).
    When I send a "GET" request to "/data/big.bin?attributes" with headers
      | x-amz-object-attributes | ObjectParts |
      | x-amz-max-parts         | 1           |
    Then the response status is 200
    And the response body contains "<PartNumber>1</PartNumber>"
    And the response body contains "<IsTruncated>true</IsTruncated>"
    And the response body contains "<NextPartNumberMarker>1</NextPartNumberMarker>"
    And the response body contains "<PartsCount>2</PartsCount>"
    # Resuming past the exclusive marker lists the rest, untruncated.
    When I send a "GET" request to "/data/big.bin?attributes" with headers
      | x-amz-object-attributes  | ObjectParts |
      | x-amz-part-number-marker | 1           |
    Then the response status is 200
    And the response body contains "<PartNumber>2</PartNumber>"
    And the response body contains "<IsTruncated>false</IsTruncated>"
    And the response body does not contain "<NextPartNumberMarker>"

  @checksum-on
  @FR-032
  Scenario: GetObjectAttributes echoes a recorded checksum
    Given I create bucket "data"
    # A checksummed plain put records the FULL_OBJECT kind (a multipart
    # completion records COMPOSITE — pinned by the server unit suite).
    When I send a "PUT" request to "/data/c.txt" with headers and body "hello"
      | x-amz-checksum-crc32 | NhCmhg== |
    Then the response status is 200
    And the response header "x-amz-checksum-crc32" is "NhCmhg=="
    When I send a "GET" request to "/data/c.txt"
    Then the response status is 200
    And the response header "x-amz-checksum-crc32" is "NhCmhg=="
    And the response header "x-amz-checksum-type" is "FULL_OBJECT"
    When I send a "GET" request to "/data/c.txt?attributes" with headers
      | x-amz-object-attributes | Checksum |
    Then the response status is 200
    And the response body contains "<ChecksumCRC32>NhCmhg==</ChecksumCRC32>"
    And the response body contains "<ChecksumType>FULL_OBJECT</ChecksumType>"

  # The request-checksum echo and the recorded echo (PUT/GET) are NOT
  # crc32-specific: every algorithm the API model carries must round-trip
  # through the same value-field plumbing. The digests are the standard
  # values of the body "hello" (pinned by the server unit suite's
  # known_vectors test — a hash-encoding change fails there first).
  @checksum-on
  @FR-032
  Scenario Outline: every checksum algorithm echoes on PUT and records on GET
    Given I create bucket "data"
    When I send a "PUT" request to "/data/<key>.txt" with headers and body "hello"
      | x-amz-checksum-<algo> | <digest> |
    Then the response status is 200
    And the response header "x-amz-checksum-<algo>" is "<digest>"
    When I send a "GET" request to "/data/<key>.txt"
    Then the response status is 200
    And the response header "x-amz-checksum-<algo>" is "<digest>"
    And the response header "x-amz-checksum-type" is "FULL_OBJECT"

    Examples:
      | key  | algo      | digest                                                             |
      | a    | crc32c    | mnG7TA==                                                          |
      | b    | crc64nvme | M3eFcAZSQlc=                                                       |
      | c    | sha1      | qvTGHdzF6KLavt4PO0gs2a6pQ00=                                      |
      | d    | sha512    | m3HSJL1i83hdltRq0+o9czGb+8KJDKra4t/3JRlnPKcjI8PZm6XBHXx6zG4UuMXaDEZjR1wuXDre9G9zvN7AQw== |
      | e    | xxhash64  | JseCfYifbaM=                                                       |
      | f    | xxhash3   | lVXoVVxi3P0=                                                        |
      | g    | xxhash128 | tenBrQcbPn/Hec+qXlI4GA==                                            |

  # A mismatched full-object checksum on a plain PUT is BadDigest and the
  # write is refused (the tee's mismatch surfaces through the commit).
  @checksum-on
  @FR-032
  Scenario: a wrong sha512 checksum on PUT is BadDigest
    Given I create bucket "data"
    When I send a "PUT" request to "/data/bad.txt" with headers and body "hello"
      | x-amz-checksum-sha512 | AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 400
    And the error code is "BadDigest"
    When I get object "data/bad.txt"
    Then the response status is 404
