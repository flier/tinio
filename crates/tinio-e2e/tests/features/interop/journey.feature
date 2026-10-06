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
    And the client targets the second server
    And I run aws s3 mb s3://ephemeral-bucket
    Then the external client output contains "ephemeral-bucket"
    And I run aws s3 ls
    Then the external client output contains "ephemeral-bucket"
    When the client targets the first server
    And I run aws s3 ls
    Then the external client output does not contain "ephemeral-bucket"
    And I run aws s3 mb s3://first-bucket
    And I run aws s3 rb s3://first-bucket
    When the client targets the second server
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

  # The versioning journey (spec 2026-10-03-s3-versioning, FR-037/FR-038): the
  # same journey `versioning.feature` drives in process, replayed through aws
  # cli v2 — the client's own rendering of the version ids, the
  # bucket-addressed `?versions` listing, the marker's 404 and the two
  # version-addressed deletes. Added 2026-10-05 and run the same day against
  # aws cli v2 (`--tags '@interop and not @confinement'`, 22/22 scenarios).
  # The `--debug` leg is how the cli discloses a response
  # header: its normal error output carries only the code, and botocore's
  # debug dump is where `x-amz-delete-marker` becomes visible — printed as
  # `X-Amz-Delete-Marker`, which is why the assertion below is case-insensitive.
  @interop @aws @FR-037 @FR-038
  Scenario: The version history journey with aws cli v2
    When I run aws s3api create-bucket --bucket versioned-bucket
    And I run aws s3api put-bucket-versioning --bucket versioned-bucket --versioning-configuration Status=Enabled
    And I run aws s3api get-bucket-versioning --bucket versioned-bucket --query Status --output text
    Then the external client output contains "Enabled"
    Given I write "first" to the scratch file "first.txt"
    And I write "second" to the scratch file "second.txt"
    # Every version-creating write answers the identity it created; the id
    # captured here is the version this journey later purges by.
    When I run aws s3api put-object --bucket versioned-bucket --key notes.txt --body "{work}/first.txt" --query VersionId --output text
    Then the external client output is not empty
    And I capture the client output
    When I run aws s3api get-object --bucket versioned-bucket --key notes.txt --version-id {captured} "{work}/older.txt"
    Then the scratch file "older.txt" equals the scratch file "first.txt"
    # The overwrite supersedes the first version; the plain read serves the
    # current one.
    When I run aws s3api put-object --bucket versioned-bucket --key notes.txt --body "{work}/second.txt"
    And I run aws s3api get-object --bucket versioned-bucket --key notes.txt "{work}/current.txt"
    Then the scratch file "current.txt" equals the scratch file "second.txt"
    # The listing is bucket-addressed and scoped with prefix: both versions,
    # newest first.
    When I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query "length(Versions)" --output text
    Then the external client output equals "2"
    And I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query Versions[0].IsLatest --output text
    Then the external client output contains "True"
    # A plain delete inserts a marker: the delete answers the marker's own
    # identity, the key 404s with the marker disclosed, and the history keeps
    # both versions plus the marker.
    When I run aws s3api delete-object --bucket versioned-bucket --key notes.txt --query VersionId --output text
    Then the external client output is not empty
    And I capture the client output
    When I try aws s3api head-object --bucket versioned-bucket --key notes.txt --debug
    Then the external client error contains "404"
    And the external client error contains "x-amz-delete-marker" ignoring case
    When I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query "length(DeleteMarkers)" --output text
    Then the external client output equals "1"
    # Deleting the marker version restores the version below it.
    When I run aws s3api delete-object --bucket versioned-bucket --key notes.txt --version-id {captured}
    And I run aws s3api get-object --bucket versioned-bucket --key notes.txt "{work}/restored.txt"
    Then the scratch file "restored.txt" equals the scratch file "second.txt"
    # Purging the noncurrent version by id is permanent; the remaining
    # version still serves by its own id.
    When I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query Versions[1].VersionId --output text
    Then the external client output is not empty
    And I capture the client output
    When I run aws s3api delete-object --bucket versioned-bucket --key notes.txt --version-id {captured}
    And I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query "length(Versions)" --output text
    Then the external client output equals "1"
    And I run aws s3api list-object-versions --bucket versioned-bucket --prefix notes.txt --query Versions[0].VersionId --output text
    And I capture the client output
    And I run aws s3api get-object --bucket versioned-bucket --key notes.txt --version-id {captured} "{work}/newest.txt"
    Then the scratch file "newest.txt" equals the scratch file "second.txt"

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

  # The confined arm (plan 2026-09-29-sandbox-bdd-e2e §3d, G19/G20/G22): **two of this file's
  # journeys — the core one and the boto3 one — replayed** through a server spawned with
  # `--sandbox`, and, for the read-only legs below, `--read-only --sandbox`. A sample rather
  # than the whole file: the ephemeral-port, wrong-credential and checksum-multipart scenarios
  # keep their unconfined-only arms, so "the journey runs confined" is claimed for the journeys
  # replayed here and not for every scenario in this file.
  #
  # `@confinement` is the ask the `#[before]` hook reads (steps/clients.rs, `Confinement`). What
  # keeps these off a host that cannot confine is the **composed** default filter (off macOS
  # unless `TINIO_E2E_CONFINEMENT=1`) and CI's own `--tags` (`@interop and not @confinement`);
  # an explicit `--tags` replaces the composed filter, so a hand-written interop selection has
  # to carry `and not @confinement` itself — CI's interop job and `scripts/wsl-interop.sh` both
  # do, and the recipe below does too.
  #
  # The mutating operations a read-only refusal must be asserted against are
  # `contracts/s3-surface.md` §Read-only mode's list (the whole write group of the shared
  # operation table); each leg below pins its own client's rendering of a representative few,
  # and the server's own answer (status, code, message) is G9's fact in `confinement.feature` —
  # referred to here, not re-asserted.

  @interop @aws @rclone @confinement @FR-036
  Scenario: The core journey with aws cli v2 and rclone runs under confinement
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

  @boto3 @confinement @FR-036
  Scenario: The SC-001 basic journey via the boto3 SDK runs under confinement
    When I run the boto3 script "boto3_journey.py"
    Then the external client output contains "BOTO3 JOURNEY OK"

  @interop @aws @confinement @read-only @FR-023 @FR-036
  Scenario: The aws cli v2 read-only refusals are the client's own shape
    Given the served root contains a bucket "frozen" with 1 files "read-"
    And the served root contains a bucket "empty-bucket" with 0 files "none-"
    When I run aws s3 ls s3://frozen/
    Then the external client output contains "read-1.txt"
    Given I write "refused" to the scratch file "refused.txt"
    When I try aws s3 mb s3://refused-bucket
    Then the external client error contains "An error occurred (AccessDenied) when calling the CreateBucket operation: Access Denied"
    When I try aws s3 cp "{work}/refused.txt" s3://frozen/new.txt
    Then the external client error contains "An error occurred (AccessDenied) when calling the PutObject operation: Access Denied"
    When I try aws s3 rm s3://frozen/read-1.txt
    Then the external client error contains "An error occurred (AccessDenied) when calling the DeleteObject operation: Access Denied"
    When I try aws s3 rb s3://empty-bucket
    Then the external client error contains "An error occurred (AccessDenied) when calling the DeleteBucket operation: Access Denied"
    Given I write 10485760 deterministic bytes to the scratch file "big.bin"
    When I try aws s3 cp "{work}/big.bin" s3://frozen/big.bin
    Then the external client error contains "An error occurred (AccessDenied) when calling the CreateMultipartUpload operation: Access Denied"

  @interop @rclone @confinement @read-only @FR-023 @FR-036
  Scenario: The rclone read-only refusals are the client's own shape
    Given the served root contains a bucket "frozen" with 1 files "read-"
    And the served root contains a bucket "empty-bucket" with 0 files "none-"
    When I configure the rclone remote
    And I run rclone lsf tinio:frozen
    Then the external client output contains "read-1.txt"
    When I try rclone mkdir tinio:refused-bucket
    Then the external client error contains "operation error S3: CreateBucket, https response error StatusCode: 403"
    And the external client error contains "api error AccessDenied: Access Denied"
    Given I write "refused" to the scratch file "refused.txt"
    When I try rclone copy --s3-no-check-bucket "{work}/refused.txt" tinio:frozen/new.txt
    Then the external client error contains "operation error S3: PutObject, https response error StatusCode: 403"
    When I try rclone delete tinio:frozen/read-1.txt
    Then the external client error contains "operation error S3: DeleteObject, https response error StatusCode: 403"
    When I try rclone rmdir tinio:empty-bucket
    Then the external client error contains "operation error S3: DeleteBucket, https response error StatusCode: 403"

  @boto3 @confinement @read-only @FR-023 @FR-036
  Scenario: The boto3 SDK read-only refusals are the client's own shape
    Given the served root contains a bucket "frozen" with 1 files "read-"
    When I run the boto3 read-only script "boto3_journey.py"
    Then the external client output contains "BOTO3 READ-ONLY OK"
