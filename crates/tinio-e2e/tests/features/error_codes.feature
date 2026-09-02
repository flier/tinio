# derived from specs/001-s3-local-server/contracts/s3-surface.md (errors) and
# checklists/compatibility.md SC-004; replaces tinio-server/tests/error_codes.rs
@SC-004 @FR-005 @FR-006 @FR-012 @FR-021
Feature: S3 error codes over real HTTP

  Scenario: Missing bucket answers NoSuchBucket
    Given I send a "PUT" request to "/missing/a.txt" with body "x"
    Then the response status is 404
    And the error code is "NoSuchBucket"
    Given I send a "GET" request to "/missing"
    Then the response status is 404
    And the error code is "NoSuchBucket"

  Scenario: Missing object answers NoSuchKey
    Given I create bucket "data"
    And I send a "GET" request to "/data/missing.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"

  Scenario: HEAD on a missing object answers 404
    Given I create bucket "data"
    And I send a "HEAD" request to "/data/missing.txt"
    Then the response status is 404

  # Task 8: the naming-rule matrix parameterizes the single-name scenario
  # over the core bucket validation sets (valid_bucket_names_accepted,
  # invalid_bucket_names_rejected; the adjacent-dot rows carry
  # bucket_dot_segments_rejected's rule).

  Scenario Outline: Bucket names follow the S3 naming rules
    Given I send a "PUT" request to "/<name>"
    Then the response status is <status>
    And the error code is "<code>"

    Examples:
      | name    | status | code              |
      | Bad_Name | 400   | InvalidBucketName |
      | a       | 400    | InvalidBucketName |
      | ab      | 400    | InvalidBucketName |
      | BIG     | 400    | InvalidBucketName |
      | under_score | 400 | InvalidBucketName |
      | -lead   | 400    | InvalidBucketName |
      | trail-  | 400    | InvalidBucketName |
      | .lead   | 400    | InvalidBucketName |
      | trail.  | 400    | InvalidBucketName |
      | sp%20ace | 400   | InvalidBucketName |
      | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | 400 | InvalidBucketName |
      | my-bucket | 200  |                   |
      | my.bucket | 200  |                   |
      | aaa     | 200    |                   |
      | 123     | 200    |                   |
      | a.b-c.d | 200    |                   |

  Scenario: Bucket create/delete conflicts
    Given I create bucket "data"
    And I create bucket "data"
    Then the error code is "BucketAlreadyOwnedByYou"
    Given I upload "data/a.txt" with body "x"
    And I send a "DELETE" request to "/data"
    Then the error code is "BucketNotEmpty"
    Given I delete object "data/a.txt"
    And I send a "DELETE" request to "/data"
    Then the response status is 204

  @minimal-caps
  Scenario: Disabled capabilities answer NotImplemented
    Given I create bucket "data"
    And I send a "GET" request to "/data?list-type=2"
    Then the error code is "NotImplemented"
    Given I send a "GET" request to "/data"
    And I send a "POST" request to "/data/big.bin?uploads"
    Then the error code is "NotImplemented"

  Scenario: Operations outside the surface answer NotImplemented
    Given I create bucket "data"
    And I send a "GET" request to "/data?policy"
    Then the response status is 501
    And the error code is "NotImplemented"

  # Task 8: the traversal scenario parameterizes over the core key
  # validation sets (traversal_rejected, dot_segment_rejected,
  # empty_interior_segments_rejected, drive_letter_paths_rejected,
  # control_characters_rejected, the absolute-path legs) plus the
  # adjacent-dot bucket names (bucket_dot_segments_rejected) — every
  # row must be refused before any filesystem access.

  @fs @nested-root
  Scenario Outline: Traversal and invalid keys are rejected without fs access
    Given I create bucket "data"
    When I send a "PUT" request to "<path>" with body "x"
    Then the response status is 400
    And the error code is not empty
    And no file was written outside the served root
    # F09: the served root itself holds only the state dir and the
    # bucket — a rejected key must not stage inside the root either.
    And the served root contains only the state dir and the bucket

    Examples:
      | path                     |
      | /data/../evil.txt        |
      | /data/..%2Fevil2.txt     |
      | /data/a%2F..%2Fb         |
      | /data/a%2F..             |
      | /data/%2F                |
      | /data//abs.txt           |
      | /data/...                |
      | /data/..x                |
      | /data/x..                |
      | /data/a..b               |
      | /data/a/.../b            |
      | /data/a/./b              |
      | /data/a/.                |
      | /data/./x                |
      | /data/C:/foo             |
      | /data/C%3Afoo            |
      | /data/d:%5C.tinio%5Cstate |
      | /data/a%5C%5Cb           |
      | /data/a%5C%5C            |
      | /data/a%5C%2Fb           |
      | /data/a%2F%5Cb           |
      | /data/a%00b              |
      | /data/a%0Ab              |
      | /data/a%1Fb              |
      | /data/a%7Fb              |
      | /data/a%09b              |
      | /data/a//b               |
      | /data/a%2F%2F%2Fb        |
      | /data/a/b//c             |
      | /...                     |
      | /..a                     |
      | /a..                     |
      | /a..b                    |

  @checksum-on
  Scenario: UploadPart checksum mismatch is BadDigest and stores nothing
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    When I upload part 1 with body "hello world" and checksum-crc32 "y/Q5Jg=="
    Then the error code is "BadDigest"
    When I list the parts of the multipart upload
    Then the parts listing shows 0 parts
    When I upload part 1 with body "hello world" and checksum-crc32 "DUoRhQ=="
    Then the response status is 200
    And the response header "x-amz-checksum-crc32" is "DUoRhQ=="
    When I list the parts of the multipart upload
    Then the parts listing shows 1 part
    And the response body contains "<ChecksumCRC32>DUoRhQ==</ChecksumCRC32>"

  @checksum-on
  Scenario: UploadPart validates Content-MD5
    Given I create bucket "data"
    And I start a multipart upload for "data/md5.bin"
    When I upload part 1 with body "abc" and content-md5 "kAFQmDzST7DWlj99KOF/cg=="
    Then the response status is 200
    When I upload part 2 with body "def" and content-md5 "AAAAAAAAAAAAAAAAAAAAAA=="
    Then the response status is 400
    And the error code is "BadDigest"

  @checksum-on
  Scenario: UploadPart rejects conflicting checksum headers and bare algorithms
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    When I send a "PUT" request to "/data/big.bin?partNumber=1&uploadId={upload_id}" with headers and body "x"
      | x-amz-checksum-crc32  | y/Q5Jg== |
      | x-amz-checksum-sha256 | y/Q5Jg== |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "PUT" request to "/data/big.bin?partNumber=2&uploadId={upload_id}" with headers and body "x"
      | x-amz-checksum-algorithm | CRC32 |
    Then the response status is 400
    And the error code is "InvalidRequest"

  @checksum-on
  Scenario: UploadPart checksum algorithm must match the create algorithm
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin" with checksum-algorithm SHA256
    Then the response status is 200
    When I send a "PUT" request to "/data/big.bin?partNumber=1&uploadId={upload_id}" with headers and body "x"
      | x-amz-checksum-crc32 | y/Q5Jg== |
    Then the response status is 400
    And the error code is "InvalidRequest"

  @checksum-on
  Scenario Outline: CreateMultipartUpload rejects an invalid algorithm and type combination
    Given I create bucket "data"
    When I send a "POST" request to "/data/big.bin?uploads" with headers
      | x-amz-checksum-algorithm | <algo> |
      | x-amz-checksum-type      | <type> |
    Then the response status is 400
    And the error code is "InvalidRequest"

    Examples:
      | algo   | type        |
      | SHA256 | FULL_OBJECT |
      | SHA1   | FULL_OBJECT |

  @checksum-on
  Scenario Outline: CreateMultipartUpload accepts the valid algorithm and type combinations
    Given I create bucket "data"
    When I send a "POST" request to "/data/big.bin?uploads" with headers
      | x-amz-checksum-algorithm | <algo> |
      | x-amz-checksum-type      | <type> |
    Then the response status is 200

    Examples:
      | algo      | type        |
      | SHA256    | COMPOSITE   |
      | CRC32     | FULL_OBJECT |
      | CRC64NVME | FULL_OBJECT |
      | CRC32C    | COMPOSITE   |
