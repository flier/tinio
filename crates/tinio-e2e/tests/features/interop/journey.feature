# derived from specs/001-s3-local-server/contracts/s3-surface.md (the
# SC-001 basic scenario set, SC-002 no-client-overrides); replaces
# e2e/interop/journey.sh + tinio-server/tests/journey.rs and the boto3
# legs of tinio-server/tests/boto3.rs (T032, T034); also covers the
# multipart-checksum-validation journey (spec 2026-08-31, FR-026)
@SC-001 @T032 @FR-025
Feature: Interop journey (third-party S3 clients)

  @interop @aws @rclone @SC-002
  Scenario: The core journey with aws cli v2 and rclone
    When I run aws s3 mb s3://interop-bucket
    Given I write "hello from aws" to the scratch file "hello.txt"
    When I run aws s3 cp "{work}/hello.txt" s3://interop-bucket/hello.txt
    And I run aws s3 cp s3://interop-bucket/hello.txt "{work}/downloaded.txt"
    Then the scratch file "downloaded.txt" equals the scratch file "hello.txt"
    When I run aws s3 cp "{work}/hello.txt" s3://interop-bucket/dir/nested.txt
    And I run aws s3 ls s3://interop-bucket/
    Then the external client output contains "hello.txt"
    When I run aws s3 ls s3://interop-bucket/dir/
    Then the external client output contains "nested.txt"
    When I run aws s3 rm s3://interop-bucket/hello.txt
    And I run aws s3 rb s3://interop-bucket --force
    And I configure the rclone remote
    And I run rclone mkdir tinio:rclone-bucket
    Given I write "hello from rclone" to the scratch file "r.txt"
    When I run rclone copy "{work}/r.txt" tinio:rclone-bucket/
    And I run rclone copy tinio:rclone-bucket/r.txt "{work}/rclone-dl"
    Then the scratch file "rclone-dl/r.txt" equals the scratch file "r.txt"
    When I run rclone lsf tinio:rclone-bucket
    Then the external client output contains "r.txt"
    When I run rclone delete tinio:rclone-bucket/r.txt
    And I run rclone purge tinio:rclone-bucket

  @interop @aws @FR-008 @SC-002
  Scenario: An ephemeral --port 0 start serves alongside the first server
    When I start a second server
    And I run aws s3 mb s3://ephemeral-bucket
    And I run aws s3 rb s3://ephemeral-bucket

  @interop @aws @FR-008
  Scenario: Requests with invalid credentials are rejected
    When I try aws with wrong credentials s3 ls
    Then the external client error contains "InvalidAccessKeyId"
    When I try aws with wrong credentials s3 mb s3://never-created
    Then the external client error contains "InvalidAccessKeyId"
    And I run aws s3 mb s3://still-fine-bucket
    Then the external client output contains "still-fine-bucket"

  @interop @aws @checksum-on @FR-026
  Scenario: Multipart upload with aws cli CRC64NVME checksums is validated
    When I run aws s3 mb s3://checksum-bucket
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I run aws s3 cp "{work}/big.bin" s3://checksum-bucket/big.bin --checksum-algorithm CRC64NVME
    And I run aws s3 cp s3://checksum-bucket/big.bin "{work}/down.bin"
    Then the scratch file "down.bin" equals the scratch file "big.bin"
    When I run aws s3 rb s3://checksum-bucket --force

  @boto3 @T034 @SC-001
  Scenario: The SC-001 basic journey via the boto3 SDK
    When I run the boto3 script "boto3_journey.py"
    Then the external client output contains "BOTO3 JOURNEY OK"

  @boto3 @FR-021 @max-buckets-3
  Scenario: ListBuckets pagination respects the max_buckets cap
    When I run the boto3 script "boto3_buckets_pagination.py"
    Then the external client output contains "BUCKET PAGINATION OK"

  @boto3 @T034 @checksum-on
  Scenario: The boto3 journey with checksum validation enabled
    When I run the boto3 script "boto3_journey.py"
    Then the external client output contains "BOTO3 JOURNEY OK"
