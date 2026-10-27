# Tiered storage through the third-party clients (design docs/superpowers/specs/2026-10-24-tiered-storage-design.md, decisions T10, T11, T24 and T25): a spawned `tinio server` on the tiered backend — the scenario's root, `[storage] type = "tier"`, `[storage.pg]` and `[storage.tier]` — driven by aws cli v2, rclone, mc and the boto3 SDK.
# The suite is `@tier` (its own explicit `--tags` run, excluded from the default pass) and external (`TINIO_E2E_EXTERNAL=1`); every scenario truncates the shared database under the one pg permit and holds it while its server runs, because a writable tier server owns its database (one root per database, T18) — the spawned processes are serialized by the harness, not by `--concurrency`.
# The storage class is asserted only through `GetObjectAttributes`: HEAD and listing answers keep the S3 defaults, so a third-party tool reading them sees no tier (T24, T10).
# CI's tier-external job requires all four clients because it selects all four scenarios and its versions step fails closed; the hook checks presence per scenario.
@tier
Feature: Tiered storage through external S3 clients

  @interop @aws @FR-044 @FR-045
  Scenario: The aws cli v2 tier journey
    When I run aws s3api create-bucket --bucket tier-aws
    Given I write "cold payload from aws" to the scratch file "cold.txt"
    When I run aws s3api put-object --bucket tier-aws --key cold.txt --body "{work}/cold.txt" --storage-class STANDARD_IA
    And I run aws s3api get-object-attributes --bucket tier-aws --key cold.txt --object-attributes StorageClass --query StorageClass --output text
    Then the external client output equals "STANDARD_IA"
    When I run aws s3api get-object --bucket tier-aws --key cold.txt "{work}/cold-down.txt"
    Then the scratch file "cold-down.txt" equals the scratch file "cold.txt"
    # A second cold object carries the restart leg: the copy below promotes its cold source (the filesystem's copy opens the source file, T16), so the first object's class is asserted only before that.
    Given I write "kept payload from aws" to the scratch file "kept.txt"
    When I run aws s3api put-object --bucket tier-aws --key kept.txt --body "{work}/kept.txt" --storage-class STANDARD_IA
    And I run aws s3api get-object-attributes --bucket tier-aws --key kept.txt --object-attributes StorageClass --query StorageClass --output text
    Then the external client output equals "STANDARD_IA"
    # A copy of a cold source lands warm: CopyObject accepts the class header and ignores it (T10).
    When I run aws s3api copy-object --bucket tier-aws --key copy.txt --copy-source tier-aws/cold.txt
    And I run aws s3api get-object-attributes --bucket tier-aws --key copy.txt --object-attributes StorageClass --query StorageClass --output text
    Then the external client output equals "STANDARD"
    # UploadPartCopy assembles a multipart object from the cold source.
    When I run aws s3api create-multipart-upload --bucket tier-aws --key assembled.txt --query UploadId --output text
    Then the external client output is not empty
    And I capture the client output as the multipart upload id
    When I run aws s3api upload-part-copy --bucket tier-aws --key assembled.txt --part-number 1 --upload-id {upload-id} --copy-source tier-aws/cold.txt --query CopyPartResult.ETag --output text
    Then the external client output is not empty
    And I capture the client output
    When I complete the aws multipart upload "assembled.txt" in bucket "tier-aws" with the captured part
    Then the external client output contains "assembled.txt"
    When I run aws s3api get-object --bucket tier-aws --key assembled.txt "{work}/assembled.txt"
    Then the scratch file "assembled.txt" equals the scratch file "cold.txt"
    # The listing merges the warm and cold placements; the class is not asserted here.
    When I run aws s3 ls s3://tier-aws/
    Then the external client output contains "cold.txt"
    And the external client output contains "assembled.txt"
    # A restart reopens the same index and cold rows: the untouched cold object still serves, and its class is still the cold tier's.
    When the external tier server is restarted
    And I run aws s3api get-object --bucket tier-aws --key kept.txt "{work}/kept-after-restart.txt"
    Then the scratch file "kept-after-restart.txt" equals the scratch file "kept.txt"
    When I run aws s3api get-object-attributes --bucket tier-aws --key kept.txt --object-attributes StorageClass --query StorageClass --output text
    Then the external client output equals "STANDARD_IA"

  @interop @rclone @FR-044 @FR-045
  Scenario: The rclone tier journey
    When I configure the rclone remote
    And I run rclone mkdir tinio:tier-rclone
    Given I write "cold payload from rclone" to the scratch file "r.txt"
    When I run rclone copy --s3-storage-class STANDARD_IA "{work}/r.txt" tinio:tier-rclone/
    And I run rclone ls tinio:tier-rclone
    Then the external client output contains "r.txt"
    When I run rclone cat tinio:tier-rclone/r.txt
    Then the external client output contains "cold payload from rclone"

  @mc @FR-044 @FR-045 @FR-046
  Scenario: The mc tier journey and lifecycle round trip
    When I configure the mc alias
    And I run mc mb tinio/tier-mc
    Given I write "cold payload from mc" to the scratch file "m.txt"
    When I run mc cp --storage-class STANDARD_IA "{work}/m.txt" tinio/tier-mc/cold.txt
    And I run mc stat tinio/tier-mc/cold.txt
    Then the external client output contains "cold.txt"
    And the external client output contains "20 B"
    And the external client output contains "ETag" ignoring case
    When I run mc cp "{work}/m.txt" tinio/tier-mc/warm.txt
    And I run mc ls tinio/tier-mc
    Then the external client output contains "cold.txt"
    And the external client output contains "warm.txt"
    When I run mc cat tinio/tier-mc/cold.txt
    Then the external client output contains "cold payload from mc"
    # The lifecycle round trip: a transition-only rule to STANDARD_IA at the 30-day floor, listed and removed.
    # Listed with `--json`, which mc documents as its machine-readable form: the `ls` table is a layout the
    # client is free to stop printing (the current release renders none for this rule while still serving it),
    # and the rule's presence in the served configuration is what this scenario claims. The `ls` after the
    # removal stays table-mode on purpose: `--json` moves the error itself to stdout, and this leg asserts stderr.
    When I run mc ilm rule add --transition-days 30 --transition-tier STANDARD_IA tinio/tier-mc
    Then the external client output contains "added"
    When I run mc ilm rule ls --json tinio/tier-mc
    Then the external client output contains "StorageClass"
    And the external client output contains "STANDARD_IA"
    When I run mc ilm rule rm --all --force tinio/tier-mc
    And I try mc ilm rule ls tinio/tier-mc
    Then the external client error contains "lifecycle configuration does not exist"

  @boto3 @FR-044 @FR-045 @FR-046
  Scenario: The boto3 tier journey
    When I run the boto3 tier script
    Then the external client output contains "BOTO3 TIER OK"
