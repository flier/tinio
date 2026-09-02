# derived from specs/001-s3-local-server/contracts/s3-surface.md (multipart,
# copy, cold listing, edge cases); replaces e2e/interop/advanced.sh +
# tinio-server/tests/advanced.rs and the edge legs of
# tinio-server/tests/edge.rs + mc.rs (T032, T033, T035)
@T033 @FR-015 @FR-024
Feature: Interop advanced scenarios (multipart, copy, cold listing, edges)

  @interop @aws
  Scenario: Multipart upload above 8 MiB answers a composed ETag
    When I run aws s3 mb s3://adv-bucket
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I run aws s3 cp "{work}/big.bin" s3://adv-bucket/big.bin
    And I run aws s3api head-object --bucket adv-bucket --key big.bin --query ETag --output text
    Then the external client output contains "-"
    When I run aws s3 cp s3://adv-bucket/big.bin "{work}/big-downloaded.bin"
    Then the scratch file "big-downloaded.bin" equals the scratch file "big.bin"

  @interop @aws
  Scenario: Server-side copy downloads byte-identical
    When I run aws s3 mb s3://adv-bucket
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I run aws s3 cp "{work}/big.bin" s3://adv-bucket/big.bin
    And I run aws s3 cp s3://adv-bucket/big.bin s3://adv-bucket/copy.bin
    And I run aws s3 cp s3://adv-bucket/copy.bin "{work}/copy-downloaded.bin"
    Then the scratch file "copy-downloaded.bin" equals the scratch file "big.bin"

  @interop @aws @rclone
  Scenario: rclone multipart copy and server-side copy round-trip
    When I run aws s3 mb s3://adv-bucket
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I configure the rclone remote
    And I run rclone copy "{work}/big.bin" tinio:adv-bucket/
    And I run rclone copy tinio:adv-bucket/big.bin "{work}/rclone-dl"
    Then the scratch file "rclone-dl/big.bin" equals the scratch file "big.bin"
    When I run rclone copy tinio:adv-bucket/big.bin tinio:adv-bucket/
    And I run rclone check tinio:adv-bucket "{work}/rclone-dl" --include "big.bin"

  @interop @aws @cold-listing
  Scenario: Cold listing serves hand-dropped files with the scanner on
    Given the served root contains a bucket "cold-bucket" with 50 files "file-"
    When I run aws s3 ls s3://cold-bucket/
    Then the external client output contains "file-50.txt"

  @interop @aws
  Scenario: Cold listing serves hand-dropped files without the scanner
    Given the served root contains a bucket "cold-bucket" with 50 files "file-"
    When I run aws s3 ls s3://cold-bucket/
    Then the external client output contains "file-50.txt"

  @interop @aws
  Scenario Outline: Special-character keys round-trip through real HTTP
    When I run aws s3 mb s3://edge-bucket
    Given I write 4096 deterministic bytes to the scratch file "data.bin"
    When I run aws s3 cp "{work}/data.bin" "<target>"
    And I run aws s3 cp "<target>" "{work}/dl.bin"
    Then the scratch file "dl.bin" equals the scratch file "data.bin"

    Examples:
      | target |
      | s3://edge-bucket/a b.txt |
      | s3://edge-bucket/中文.txt |
      | s3://edge-bucket/emoji-🎯.txt |
      | s3://edge-bucket/hash#pct%plus+at@.txt |
      | s3://edge-bucket/.hidden.txt |
      | s3://edge-bucket/a/b/c/d/e/f.txt |

  @interop @aws
  Scenario: Multipart size boundary: single PUT below, composed ETag above
    When I run aws s3 mb s3://edge-bucket
    Given I write 1048576 deterministic bytes to the scratch file "one.bin"
    When I run aws s3 cp "{work}/one.bin" s3://edge-bucket/one.bin
    And I run aws s3api head-object --bucket edge-bucket --key one.bin --query ETag --output text
    Then the external client output does not contain "-"
    Given I write 16777216 deterministic bytes to the scratch file "big.bin"
    When I run aws s3 cp "{work}/big.bin" s3://edge-bucket/big.bin
    And I run aws s3api head-object --bucket edge-bucket --key big.bin --query ETag --output text
    Then the external client output contains "-"
    When I run aws s3 cp s3://edge-bucket/big.bin "{work}/big-dl.bin"
    Then the scratch file "big-dl.bin" equals the scratch file "big.bin"

  @interop @aws
  Scenario: Range download answers exactly the requested window
    When I run aws s3 mb s3://edge-bucket
    Given I write 1048576 deterministic bytes to the scratch file "range.bin"
    When I run aws s3 cp "{work}/range.bin" s3://edge-bucket/range.bin
    And I run aws s3api get-object --bucket edge-bucket --key range.bin --range bytes=0-99 "{work}/part.bin"
    Then the scratch file "part.bin" is 100 bytes
    And the scratch file "part.bin" matches the prefix of the scratch file "range.bin"

  @interop @aws
  Scenario: Overwrite is last-write-wins
    When I run aws s3 mb s3://edge-bucket
    Given I write "first version" to the scratch file "v1.txt"
    And I write "second version - overwritten" to the scratch file "v2.txt"
    When I run aws s3 cp "{work}/v1.txt" s3://edge-bucket/overwrite.txt
    And I run aws s3 cp "{work}/v2.txt" s3://edge-bucket/overwrite.txt
    And I run aws s3 cp s3://edge-bucket/overwrite.txt "{work}/ov-dl.txt"
    Then the scratch file "ov-dl.txt" equals the scratch file "v2.txt"

  @interop @aws
  Scenario: Truncated listing pages resume via the continuation token
    Given the served root contains a bucket "paged-bucket" with 1100 files "obj-"
    When I run aws s3api list-objects-v2 --bucket paged-bucket --max-keys 100 --query KeyCount --output text
    Then the external client output equals "100"
    When I run aws s3api list-objects-v2 --bucket paged-bucket --max-keys 100 --query IsTruncated --output text
    Then the external client output contains "True"
    When I run aws s3api list-objects-v2 --bucket paged-bucket --max-keys 100 --query NextContinuationToken --output text
    Then the external client output is not empty
    And I capture the client output
    And I run aws s3api list-objects-v2 --bucket paged-bucket --max-keys 100 --continuation-token {captured} --query KeyCount --output text
    Then the external client output equals "100"

  @interop @aws
  Scenario: Missing objects and buckets answer the documented errors
    When I run aws s3 mb s3://edge-bucket
    Given I write "x" to the scratch file "x.txt"
    When I run aws s3 cp "{work}/x.txt" s3://edge-bucket/x.txt
    And I try aws s3api head-object --bucket edge-bucket --key missing.txt
    Then the external client error contains "404"
    When I try aws s3 ls s3://no-such-bucket/
    Then the external client error contains "NoSuchBucket"
    When I try aws s3 rb s3://edge-bucket
    Then the external client error contains "BucketNotEmpty"
    When I run aws s3 rm s3://edge-bucket/missing.txt
    And I try aws s3 rb s3://no-such-bucket
    Then the external client error contains "NoSuchBucket"

  @interop @aws
  Scenario: The shortest legal bucket name works
    When I run aws s3 mb s3://abc
    And I run aws s3 rb s3://abc

  @mc @T035 @SC-001
  Scenario: The mc basic journey (mb, cp, ls, rm, rb)
    When I configure the mc alias
    And I run mc mb tinio/mc-bucket
    Given I write "hello from mc" to the scratch file "hello.txt"
    When I run mc cp "{work}/hello.txt" tinio/mc-bucket/hello.txt
    And I run mc cp tinio/mc-bucket/hello.txt "{work}/downloaded.txt"
    Then the scratch file "downloaded.txt" equals the scratch file "hello.txt"
    Given I write "" to the scratch file "zero"
    When I run mc cp "{work}/zero" tinio/mc-bucket/zero
    And I run mc stat tinio/mc-bucket/zero
    Then the external client output contains "0 B"
    When I run mc stat tinio/mc-bucket/hello.txt
    Then the external client output contains "etag" ignoring case
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I run mc cp "{work}/big.bin" tinio/mc-bucket/big.bin
    And I run mc cp tinio/mc-bucket/big.bin "{work}/big-dl.bin"
    Then the scratch file "big-dl.bin" equals the scratch file "big.bin"
    When I run mc ls tinio/mc-bucket
    Then the external client output contains "hello.txt"
    When I run mc rm tinio/mc-bucket/hello.txt
    And I run mc rb tinio/mc-bucket --force

  @mc @T035
  Scenario Outline: mc special-character keys round-trip
    When I configure the mc alias
    And I run mc mb tinio/edge-bucket
    Given I write 4096 deterministic bytes to the scratch file "data.bin"
    When I run mc cp "{work}/data.bin" "<target>"
    And I run mc cp "<target>" "{work}/dl.bin"
    Then the scratch file "dl.bin" equals the scratch file "data.bin"

    Examples:
      | target |
      | tinio/edge-bucket/a b.txt |
      | tinio/edge-bucket/中文.txt |
      | tinio/edge-bucket/emoji-🎯.txt |

  @mc @T035
  Scenario: mc deep nesting and overwrite are last-write-wins
    When I configure the mc alias
    And I run mc mb tinio/edge-bucket
    Given I write 4096 deterministic bytes to the scratch file "data.bin"
    When I run mc cp "{work}/data.bin" tinio/edge-bucket/x/y/z.txt
    Given I write "mc version one" to the scratch file "v1.txt"
    And I write "mc version two - overwritten" to the scratch file "v2.txt"
    When I run mc cp "{work}/v1.txt" tinio/edge-bucket/ov.txt
    And I run mc cp "{work}/v2.txt" tinio/edge-bucket/ov.txt
    And I run mc cp tinio/edge-bucket/ov.txt "{work}/ov-dl.txt"
    Then the scratch file "ov-dl.txt" equals the scratch file "v2.txt"
