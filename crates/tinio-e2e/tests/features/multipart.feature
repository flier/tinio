# replaces the multipart legs of tinio-server/tests/coverage_gaps.rs (the
# non-final-part minimum and the part-number-marker validation); the full
# multipart suite grows this file in Task 8
@FR-014
Feature: Multipart

  Scenario: A non-final part smaller than the minimum answers EntityTooSmall
    Given I create bucket "data"
    Given I start a multipart upload for "data/small.bin"
    And I upload part 1 with 4 bytes
    Then the response status is 200
    When I complete the multipart upload
    Then the response status is 200
    When I get object "data/small.bin"
    Then the response status is 200
    And the object body length is 4
    Given I start a multipart upload for "data/big.bin"
    And I upload part 1 with 5 bytes
    And I upload part 2 with 4 bytes
    When I complete the multipart upload
    Then the response status is 400
    And the error code is "EntityTooSmall"

  Scenario: ListParts rejects a negative part-number-marker
    Given I create bucket "data"
    Given I start a multipart upload for "data/parts.bin"
    When I list the parts of the multipart upload with part-number-marker -1
    Then the response status is 400
    And the error code is "InvalidArgument"

  # Task 8: the spec-semantic multipart behaviors ported from the unit
  # suites — composed-ETag assembly (fs complete_assembles_byte_exact…,
  # server multipart_lifecycle), completion with a part subset (mem
  # complete_uses_only_listed_parts), post-completion identity (mem
  # complete_removes_upload_and_parts, fs
  # abort_after_complete_consume_is_no_such_upload), part re-upload (mem
  # overwrite_part_replaces_previous), NoSuchUpload identity checks,
  # completion part validation, ListParts pagination, and the checksum
  # completion path. Representative fixed values stand in for the unit
  # ranges: non-final parts at the 5 MiB minimum + 1 byte (the > 8 MiB
  # interop boundary stays in the interop features).

  Scenario Outline: Composed ETag assembly and post-completion identity
    Given I create bucket "data"
    And I start a multipart upload for "data/composed.bin"
    And I upload part 1 with 5242881 bytes
    And I upload part 2 with 5242881 bytes
    And I upload part 3 with 4096 bytes
    When I complete the multipart upload with the last <count> parts
    Then the response status is 200
    When I get object "data/composed.bin"
    Then the response status is 200
    And the object ETag matches the composed multipart form
    And the object body length is <length>
    When I complete the multipart upload
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I list the parts of the multipart upload
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I abort the multipart upload
    Then the response status is 404
    And the error code is "NoSuchUpload"

    Examples:
      | count | length   |
      | 3     | 10489858 |
      | 2     | 5246977  |

  Scenario: Re-uploading a part replaces the earlier content
    Given I create bucket "data"
    And I start a multipart upload for "data/reup.bin"
    And I upload part 1 with body "old"
    Then the response status is 200
    And the part ETag matches the MD5 of the uploaded body
    When I upload part 1 with body "newer"
    Then the response status is 200
    And the part ETag matches the MD5 of the uploaded body
    When I complete the multipart upload
    Then the response status is 200
    When I get object "data/reup.bin"
    Then the response status is 200
    And the object body is "newer"
    And the object body length is 5
    And the object ETag matches the composed multipart form

  Scenario: Abort removes the upload and its parts
    Given I create bucket "data"
    And I start a multipart upload for "data/abort.bin"
    And I upload part 1 with 4096 bytes
    When I abort the multipart upload
    Then the response status is 204
    When I list the parts of the multipart upload
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I abort the multipart upload
    Then the response status is 404
    And the error code is "NoSuchUpload"

  Scenario: Multipart operations on unknown uploads answer NoSuchUpload
    Given I create bucket "data"
    And I create bucket "other"
    And I start a multipart upload for "data/a.bin"
    And I upload part 1 with 4096 bytes
    When I send a "PUT" request to "/data/ghost.bin?partNumber=1&uploadId=ghost" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I send a "PUT" request to "/data/ghost.bin?partNumber=1&uploadId=..%2Fvictim%2Fabc" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    # F12: non-UUID upload id forms beyond the ghost above — the
    # Uuid::parse_str gate must answer NoSuchUpload, never resolve the
    # id into a path.
    When I send a "PUT" request to "/data/ghost.bin?partNumber=1&uploadId=a%2Fb" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I send a "PUT" request to "/data/ghost.bin?partNumber=1&uploadId=.." with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I send a "PUT" request to "/data/ghost.bin?partNumber=1&uploadId=" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I send a "PUT" request to "/data/b.bin?partNumber=1&uploadId={upload_id}" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I send a "PUT" request to "/other/a.bin?partNumber=1&uploadId={upload_id}" with body "x"
    Then the response status is 404
    And the error code is "NoSuchUpload"
    When I complete the multipart upload for "data/b.bin"
    Then the response status is 404
    And the error code is "NoSuchUpload"

  # F01: the checksum toggle is off in this scenario — CompletedPart
  # checksum entries are accepted and dropped (v1 pass-through); two
  # fields on one part must not answer InvalidRequest.
  Scenario: Completion accepts and drops part checksum entries with the toggle off
    Given I create bucket "data"
    And I start a multipart upload for "data/drop.bin"
    And I upload part 1 with 4096 bytes
    When I complete the multipart upload with two checksum fields on every part
    Then the response status is 200
    And the response header "x-amz-checksum-crc32" is absent

  # F02: ListMultipartUploads echoes no checksum spec while the toggle
  # is off (off = accept-and-drop, like ListParts).
  Scenario: ListMultipartUploads drops the checksum spec with the toggle off
    Given I create bucket "data"
    And I start a multipart upload for "data/spec.bin"
    When I send a "GET" request to "/data?uploads"
    Then the response status is 200
    And the uploads listing shows 1 upload
    And the response body does not contain "<ChecksumAlgorithm>"

  Scenario: Completion validates part numbers and etags
    Given I create bucket "data"
    And I start a multipart upload for "data/parts.bin"
    And I upload part 1 with 5242881 bytes
    When I complete the multipart upload with a mismatched etag for part 1
    Then the response status is 400
    And the error code is "InvalidPart"
    When I upload part 2 with 5242881 bytes
    And I complete the multipart upload with an extra part 7
    Then the response status is 400
    And the error code is "InvalidPart"
    When I send a "POST" request to "/data/parts.bin?uploadId={upload_id}" with body "<CompleteMultipartUpload></CompleteMultipartUpload>"
    Then the response status is 400
    And the error code is "InvalidRequest"

  Scenario: ListParts pages by part number
    Given I create bucket "data"
    And I start a multipart upload for "data/parts.bin"
    And I upload parts 1 through 24 with 1 bytes each
    When I list the parts of the multipart upload with max-parts 0
    Then the response status is 400
    And the error code is "InvalidArgument"
    When I list the parts of the multipart upload with max-parts -1
    Then the response status is 400
    And the error code is "InvalidArgument"
    When I list the parts of the multipart upload with max-parts 5
    Then the response status is 200
    And the parts listing shows 5 parts
    And the response body contains "<IsTruncated>true</IsTruncated>"
    When I list the parts of the multipart upload with part-number-marker 5 and max-parts 5
    Then the parts listing shows 5 parts
    And the response body contains "<PartNumber>6</PartNumber>"
    When I list the parts of the multipart upload with part-number-marker 10 and max-parts 5
    Then the parts listing shows 5 parts
    When I list the parts of the multipart upload with part-number-marker 15 and max-parts 5
    Then the parts listing shows 5 parts
    When I list the parts of the multipart upload with part-number-marker 20 and max-parts 5
    Then the parts listing shows 4 parts
    And the response body contains "<IsTruncated>false</IsTruncated>"
    And the response body contains "<PartNumber>24</PartNumber>"

  Scenario Outline: ListMultipartUploads rejects max-uploads below one
    Given I create bucket "data"
    When I send a "GET" request to "/data?uploads&max-uploads=<max>"
    Then the response status is 400
    And the error code is "InvalidArgument"

    Examples:
      | max |
      | 0   |
      | -1  |

  # Task 8: the ListMultipartUploads prefix filter and key-marker
  # pagination (mem list_uploads_filters_and_paginates,
  # bare_key_marker_skips_the_whole_key_group).

  Scenario: ListMultipartUploads filters and paginates by key marker
    Given I create bucket "data"
    And I start a multipart upload for "data/a.bin"
    And I start a multipart upload for "data/b.bin"
    And I start a multipart upload for "data/c.bin"
    When I send a "GET" request to "/data?uploads&prefix=b"
    Then the response status is 200
    And the uploads listing shows 1 upload
    And the response body contains "<Key>b.bin</Key>"
    When I send a "GET" request to "/data?uploads&max-uploads=1"
    Then the uploads listing shows 1 upload
    And the response body contains "<IsTruncated>true</IsTruncated>"
    When I send a "GET" request to "/data?uploads&key-marker=a.bin&max-uploads=10"
    Then the uploads listing shows 2 uploads
    And the response body contains "<Key>b.bin</Key>"
    And the response body contains "<Key>c.bin</Key>"

  Scenario: A bare key marker skips the whole same-key group
    Given I create bucket "data"
    And I start a multipart upload for "data/same.bin"
    And I start a multipart upload for "data/same.bin"
    When I send a "GET" request to "/data?uploads&key-marker=same.bin"
    Then the response status is 200
    And the uploads listing shows 0 uploads

  Scenario Outline: UploadPart validates the part number range
    Given I create bucket "data"
    And I start a multipart upload for "data/range.bin"
    When I upload part <part> with 0 bytes
    Then the response status is 400
    And the error code is "InvalidPart"

    Examples:
      | part  |
      | 0     |
      | 10001 |

  @checksum-on
  Scenario: CreateMultipartUpload echoes the checksum algorithm and type
    Given I create bucket "data"
    When I send a "POST" request to "/data/big.bin?uploads" with headers
      | x-amz-checksum-algorithm | CRC32 |
      | x-amz-checksum-type      | FULL_OBJECT |
    Then the response status is 200
    And the response header "x-amz-checksum-algorithm" is "CRC32"
    And the response header "x-amz-checksum-type" is "FULL_OBJECT"

  @checksum-on
  Scenario: Completion checksum mismatch is BadDigest and preserves the old object
    Given I create bucket "data"
    And I upload "data/big.bin" with body "precious"
    And I start a multipart upload for "data/big.bin" with checksum-algorithm CRC32
    And I upload part 1 with 5242881 bytes
    When I complete the multipart upload with checksum-crc32 "y/Q5Jg=="
    Then the response status is 400
    And the error code is "BadDigest"
    When I get object "data/big.bin"
    Then the response status is 200
    And the object body is "precious"
    When I complete the multipart upload with checksum-crc32 "nZC9/g=="
    Then the response status is 200
