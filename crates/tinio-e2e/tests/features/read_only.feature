# Read-only mode (FR-023): every mutating operation answers AccessDenied and
# reads are served unchanged.
#
# Fixtures are planted on disk: in this mode `Given I create bucket` and
# `Given I upload …` are themselves refused. Reads stop at what a planted
# root can express (buckets, objects); the mode's remaining halves are below,
# on the surface each needs: the three mode channels, FR-016's precedence over
# them, the relocated state dir and the in-root meta view are the `@cli`
# scenarios (the real binary, observed from outside), and a root that cannot
# be written at all is the `@unix` one.
@FR-023
Feature: Read-only mode

  @fs @read-only
  Scenario: Reads are served unchanged
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "HEAD" request to "/data"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the object body is "hello"
    When I send a "HEAD" request to "/data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/data?list-type=2"
    Then the response status is 200
    And the response body contains "<Key>a.txt</Key>"

  @fs @read-only
  Scenario: Bucket writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/other"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data"
    Then the response status is 403
    And the error code is "AccessDenied"

  # The two reads at the end are the refusals' other half: the mode's refusals are the
  # decorator's, not the filesystem's, so the deleted object is still there and the put one
  # never landed. Asserted here rather than beside each class, because one root carries them
  # all and a refusal that wrote would fail this pair wherever the write came from.
  @fs @read-only
  Scenario: Object writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data/b.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/a.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/b.txt" with headers
      | x-amz-copy-source | /data/a.txt |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the object body is "hello"
    When I send a "GET" request to "/data/b.txt"
    Then the response status is 404

  @fs @read-only
  Scenario: Batch delete is refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data?delete" with headers and body "<Delete><Object><Key>a.txt</Key></Object></Delete>"
      | Content-MD5 | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Multipart writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "POST" request to "/data/b.txt?uploads"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data/b.txt?uploadId=anything"
    Then the response status is 403
    And the error code is "AccessDenied"

  @fs @read-only
  Scenario: Tagging and ACL writes are refused
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/data?tagging" with headers and body "<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "PUT" request to "/data/a.txt?acl" with headers
      | x-amz-acl | private |
    Then the response status is 403
    And the error code is "AccessDenied"

  # The reference arm of the refusal below, and the reason it is a difference rather than a
  # request that never reaches the bucket check: the same request against the same fixture
  # answers NoSuchBucket on a read-write plane.
  @fs
  Scenario: A write to a missing bucket answers NoSuchBucket read-write
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/ghost/a.txt" with body "x"
    Then the response status is 404
    And the error code is "NoSuchBucket"

  @fs @read-only
  Scenario: A write to a missing bucket is refused too
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "PUT" request to "/ghost/a.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"

  # The root refuses new entries for the whole scenario, so a plane that wrote it would fail
  # here rather than pass; the restore guard is the step's (`the served root refuses new
  # entries`), because a 0500 tempdir cannot be removed. Absent on Windows, which has no mode
  # bits.
  @fs @read-only @unix
  Scenario: A genuinely read-only root still serves
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    And the served root refuses new entries
    When I send a "HEAD" request to "/data"
    Then the response status is 200
    When I send a "HEAD" request to "/data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt"
    Then the response status is 200
    And the object body is "hello"
    When I send a "GET" request to "/data?list-type=2"
    Then the response status is 200
    And the response body contains "<Key>a.txt</Key>"

  # The flag is the mode's first channel (decision 4), and the state dir it relocates is the
  # half no wire assertion can see: the run's own state lands in `TINIO_STATE_DIR` — the
  # harness hands every `@cli` child one — and the served root gains no `.tinio` of its own.
  # The discriminator is a signed write: read-write answers NoSuchBucket from the bucket
  # check, read-only refuses before it. The credentials are the harness's, pinned on the child
  # because a first start *generates* a random `[auth]` pair (`the child holds the harness's
  # credentials`).
  @cli @FR-023
  Scenario: The read-only flag relocates the state and refuses writes
    Given a storage root
    And the root holds bucket "data" with object "a.txt" and body "hello"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0 --read-only"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/ghost/a.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "GET" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the object body is "hello"
    And the run reports serving read-only on the endpoint it bound
    And the relocated state dir holds the state database
    And the root holds no state directory
    And its stdout is the readiness frame alone

  # The config file is the mode's third channel: it lives where discovery reads it for this
  # root (`<root>/.tinio/config.toml`), and no flag and no variable answer.
  @cli @FR-023
  Scenario: A config file turns read-only on
    Given a storage root
    And the root's state dir holds a "config.toml" file
      """
      version = 1

      [server]
      read_only = true
      """
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/ghost/a.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 403
    And the error code is "AccessDenied"

  @cli @FR-023
  Scenario: The environment turns read-only on
    Given a storage root
    And the child's environment holds "TINIO_READ_ONLY" = "1"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/ghost/a.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 403
    And the error code is "AccessDenied"

  # FR-016's chain over this mode's two lower channels: an explicit `TINIO_READ_ONLY=0` beats a
  # file saying `read_only = true`. The write really lands — the discriminator's 404 alone could
  # come from a plane that refuses writes some other way — and the state stays in the root's own
  # directory, which is what a read-write run resolves whatever `TINIO_STATE_DIR` says.
  @cli @FR-023 @FR-016
  Scenario: The environment beats the config file
    Given a storage root
    And the root's state dir holds a "config.toml" file
      """
      version = 1

      [server]
      read_only = true
      """
    And the child's environment holds "TINIO_READ_ONLY" = "0"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/ghost/a.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 404
    And the error code is "NoSuchBucket"
    When I send a "PUT" request to "/fresh" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/fresh/a.txt" signed as "root" with headers and body "body"
      | Content-Type | text/plain |
    Then the response status is 200
    And the file "fresh/a.txt" under the served root has 1 line containing "body"
    And the root still holds its state file

  @cli @FR-023 @FR-016
  Scenario: The flag beats the environment
    Given a storage root
    And the child's environment holds "TINIO_READ_ONLY" = "0"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0 --read-only"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/ghost/a.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 403
    And the error code is "AccessDenied"

  # A missing root is never created, and the two resolution routes — the derived
  # `<home>/.tinio/roots/<hash>/` layout and an explicit `TINIO_STATE_DIR` — fail the same way:
  # the root is canonicalized before the mode and before the state dir is resolved, so the run
  # ends during resolution whichever state dir it would have used. The explicit route runs
  # first because the derived one is the harness's `the child names no state directory` and
  # nothing gives the variable back.
  @cli @FR-023
  Scenario: A missing root is not created in read-only mode
    Given a storage root
    When I run "tinio server {missing} --read-only"
    Then the run fails with exit code 2
    And its stderr names the missing root
    And the missing root was not created
    Given the child names no state directory
    When I run "tinio server {missing} --read-only"
    Then the run fails with exit code 2
    And its stderr names the missing root
    And the missing root was not created

  # Decision 15: the in-root meta database is not read. The read-write run proves the tag exists
  # and is visible on the wire (`planted-key`), which is what makes the read-only run's empty
  # view a fact about the mode rather than about a fixture nothing wrote; the object the tag
  # belonged to still serves.
  @cli @FR-023
  Scenario: The in-root meta database is not read
    Given a storage root
    And the root holds bucket "data" with object "a.txt" and body "hello"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/data?tagging" signed as "root" with headers and body "<Tagging><TagSet><Tag><Key>planted-key</Key><Value>planted-value</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data?tagging" signed as "root"
    Then the response status is 200
    And the response body contains "planted-key"
    When I stop the server through its control channel
    When I run "tinio server {root} --port 0 --read-only"
    And the client is bound to the run's endpoint
    When I send a "GET" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the object body is "hello"
    When I send a "GET" request to "/data?tagging" signed as "root"
    Then the response status is 200
    And the response body does not contain "<Tag>"
