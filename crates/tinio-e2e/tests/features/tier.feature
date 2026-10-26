# Tiered storage (design docs/superpowers/specs/2026-10-24-tiered-storage-design.md,
# decisions T5, T10, T11, T21 and T25): the layer served in process over a
# temp root and the shared PostgreSQL database.
#
# `@tier` is a backend tag: the default run excludes it (steps/tag_filter.rs)
# and the tier pass is its own explicit `--tags` run with
# `TINIO_E2E_BACKEND=tier`. The harness constructs the layer through the
# crate's `testing` constructor (`from_parts`), which skips the pg ownership
# lock and the root-identity check a per-scenario harness cannot satisfy; the
# shared database is served under the same exclusive permit and truncated
# before each scenario, exactly like the `@pg` pass.
#
# The moves, the collector and the passes are driven by steps rather than
# waited on, so an assertion never races a background cadence. The scenarios
# are the product's own: the CRUD, range, tagging, ACL, listing, attributes
# and lifecycle steps run unchanged over the tier backend.
Feature: Tiered storage backend

  @tier
  Scenario: Objects round-trip with CRUD
    Given I create bucket "data"
    When I upload "data/hello.txt" with body "hello world"
    Then the response status is 200
    When I get object "data/hello.txt"
    Then the response status is 200
    And the object body is "hello world"
    And the object ETag matches the MD5 of the uploaded bytes
    When I head object "data/hello.txt"
    Then the response status is 200
    And the response header "Content-Length" is "11"
    When I upload "data/hello.txt" with body "replaced"
    Then the response status is 200
    When I get object "data/hello.txt"
    Then the object body is "replaced"
    When I delete object "data/hello.txt"
    Then the response status is 204
    When I get object "data/hello.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"

  @tier
  Scenario: A cold object is served from the cold plane
    Given I create bucket "data"
    And I upload "data/cold.txt" with body "cold bytes"
    When I demote object "data/cold.txt"
    Then the served root holds the file "data/cold.txt"
    When I send a "GET" request to "/data/cold.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD_IA</StorageClass>"
    When I get object "data/cold.txt"
    Then the response status is 200
    And the object body is "cold bytes"
    And the object ETag matches the MD5 of the uploaded bytes
    When I head object "data/cold.txt"
    Then the response status is 200
    And the response header "Content-Length" is "10"
    When the tier collector runs
    Then the served root does not hold the file "data/cold.txt"
    When I list objects under "data/"
    Then the response status is 200
    And the listing contains "cold.txt"

  @tier
  Scenario: A lifecycle transition demotes the object
    Given I create bucket "data"
    And I upload "data/archive/old.txt" with body "archive me"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>archive</ID>
          <Filter><Prefix>archive/</Prefix></Filter>
          <Status>Enabled</Status>
          <Transition>
            <Date>2000-01-01T00:00:00Z</Date>
            <StorageClass>STANDARD_IA</StorageClass>
          </Transition>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 200
    When the lifecycle pass runs
    When I send a "GET" request to "/data/archive/old.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD_IA</StorageClass>"
    When I get object "data/archive/old.txt"
    Then the response status is 200
    And the object body is "archive me"

  @tier
  Scenario: Heat promotes a cold object back to the warm plane
    Given I create bucket "data"
    And I upload "data/hot.txt" with body "heat me"
    When I demote object "data/hot.txt"
    And I get object "data/hot.txt"
    And I get object "data/hot.txt"
    When the tier mover runs a pass
    When I send a "GET" request to "/data/hot.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD</StorageClass>"
    When I get object "data/hot.txt"
    Then the response status is 200
    And the object body is "heat me"

  # The product's own cadence, not a driven pass (T13): the `@tier-idle`
  # configuration carries a three-second `demote_idle` and a 100 ms
  # `scan_interval`, the object is written and never read, and the step polls
  # the placement only until the background pass demotes it. The poll is
  # bounded (a 15 s budget, so a loaded host still cannot flake), so a mover
  # that never demotes fails instead of hanging, and the wire assertions that
  # follow are the `GetObjectAttributes` class and the still-readable bytes.
  @tier @tier-idle
  Scenario: An idle warm object is demoted by the background pass
    Given I create bucket "data"
    And I upload "data/idle.txt" with body "idle bytes"
    When I send a "GET" request to "/data/idle.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD</StorageClass>"
    When the tier idle pass demotes "data/idle.txt"
    And I send a "GET" request to "/data/idle.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD_IA</StorageClass>"
    When I get object "data/idle.txt"
    Then the response status is 200
    And the object body is "idle bytes"
    And the object ETag matches the MD5 of the uploaded bytes

  @tier @batch
  Scenario: A buffered write waits in the buffer and reaches the root on the flush
    Given I create bucket "data"
    When I upload "data/buffered.txt" with body "buffered"
    Then the response status is 200
    And the tier buffer holds "data/buffered.txt"
    And the served root does not hold the file "data/buffered.txt"
    When the tier buffer flushes
    Then the served root holds the file "data/buffered.txt"
    When I get object "data/buffered.txt"
    Then the response status is 200
    And the object body is "buffered"

  @tier
  Scenario: A tier root survives a restart
    Given I create bucket "data"
    And I upload "data/warm.txt" with body "warm across a restart"
    And I upload "data/cold.txt" with body "cold across a restart"
    And I demote object "data/cold.txt"
    When the tier server is restarted
    When I get object "data/warm.txt"
    Then the response status is 200
    And the object body is "warm across a restart"
    When I get object "data/cold.txt"
    Then the response status is 200
    And the object body is "cold across a restart"
    When I send a "GET" request to "/data/cold.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response body contains "<StorageClass>STANDARD_IA</StorageClass>"
    When I delete object "data/cold.txt"
    Then the response status is 204
    When I get object "data/cold.txt"
    Then the response status is 404
    And the error code is "NoSuchKey"

  @tier
  Scenario: Copy and rename reconcile a cold source
    Given I create bucket "data"
    And I upload "data/src.bin" with body "source bytes"
    And I demote object "data/src.bin"
    When I copy object "data/src.bin" to "data/copy.bin"
    Then the response status is 200
    When I get object "data/copy.bin"
    Then the response status is 200
    And the object body is "source bytes"
    When I send a "GET" request to "/data/copy.bin?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response body contains "<StorageClass>STANDARD</StorageClass>"
    When I send a "PUT" request to "/data/renamed.bin?renameObject" with headers
      | x-amz-rename-source | src.bin |
    Then the response status is 200
    When I get object "data/renamed.bin"
    Then the response status is 200
    And the object body is "source bytes"
    When I get object "data/src.bin"
    Then the response status is 404
    And the error code is "NoSuchKey"
    When I delete object "data/renamed.bin"
    Then the response status is 204
    When I get object "data/renamed.bin"
    Then the response status is 404

  @tier
  Scenario: A versioned bucket refuses a storage class
    Given I create bucket "data"
    And I enable versioning on "data"
    When I send a "PUT" request to "/data/ia.bin" with headers and body "body"
      | x-amz-storage-class | STANDARD_IA |
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I get object "data/ia.bin"
    Then the response status is 404
    When I upload "data/plain.bin" with body "plain"
    Then the response status is 200

  @tier @acl
  Scenario: Tags and an ACL round-trip on a cold object
    When I send a "PUT" request to "/data" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/data/tagged.txt" signed as "root" with headers and body "hello"
      | x-amz-acl | public-read |
    Then the response status is 200
    When I demote object "data/tagged.txt"
    When I send a "PUT" request to "/data/tagged.txt?tagging" signed as "root" with headers and body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
      | Content-Type | application/xml |
      | Content-MD5  | AAAAAAAAAAAAAAAAAAAAAA== |
    Then the response status is 200
    When I send a "GET" request to "/data/tagged.txt?tagging" signed as "root"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    And the response body contains "<Value>prod</Value>"
    When I get object "data/tagged.txt"
    Then the response status is 200
    And the object body is "hello"

  @tier
  Scenario Outline: Range requests read a cold object
    Given I create bucket "data"
    And I upload "data/digits" with body "0123456789"
    And I demote object "data/digits"
    When I send a "GET" request to "/data/digits" with headers
      | Range | <range> |
    Then the response status is <status>
    And the response header "Content-Range" is "<content-range>"
    And the object body is "<body>"

    Examples:
      | range      | status | content-range | body       |
      | bytes=2-5  | 206    | bytes 2-5/10  | 2345       |
      | bytes=-3   | 206    | bytes 7-9/10  | 789        |
      | bytes=-100 | 206    | bytes 0-9/10  | 0123456789 |

  @tier
  Scenario: GetObjectAttributes reports the class of each plane
    Given I create bucket "data"
    And I upload "data/class.txt" with body "hello"
    When I send a "GET" request to "/data/class.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD</StorageClass>"
    When I demote object "data/class.txt"
    When I send a "GET" request to "/data/class.txt?attributes" with headers
      | x-amz-object-attributes | StorageClass |
    Then the response status is 200
    And the response body contains "<StorageClass>STANDARD_IA</StorageClass>"

  # The process-global registry the management `/metrics` endpoint renders,
  # read in-process: the in-process plane serves no management route, so the
  # scrape goes through the same default-registry helper
  # (`tinio_util::testing::metrics::exposition`) rather than a wire endpoint.
  # Every expected movement is a before/after delta, never a global total:
  # the registry outlives a retry.
  @tier
  Scenario: Tier reads record hits, misses and served bytes
    Given I create bucket "data"
    And I upload "data/warm.txt" with body "warm object"
    And I upload "data/cold.txt" with body "cold"
    And I demote object "data/cold.txt"
    When I sample the tier metric counters
    And I get object "data/warm.txt"
    And I get object "data/cold.txt"
    When I scrape the tier metrics
    Then the tier metrics report the served planes
      | family                  | tier | delta |
      | tinio_tier_hits_total   | warm | 1     |
      | tinio_tier_misses_total | warm | 1     |
      | tinio_tier_hits_total   | cold | 1     |
      | tinio_tier_bytes_total  | warm | 11    |
      | tinio_tier_bytes_total  | cold | 4     |
    And the metrics exposition reports read-only tier 0

  @tier
  Scenario: Listing pages merge cold and warm keys
    Given I create bucket "data"
    And I upload "data/a.txt" with body "a"
    And I upload "data/b.txt" with body "b"
    And I upload "data/c.txt" with body "c"
    And I upload "data/d.txt" with body "d"
    And I demote object "data/b.txt"
    And I demote object "data/d.txt"
    When I list objects under "data/" with max-keys 2
    Then the response status is 200
    And a truncated listing resumes with the next page

  @tier
  Scenario: A large object streams to the cold plane and back
    Given I create bucket "data"
    And I upload "data/large.bin" with 2097152 bytes
    Then the response status is 200
    When I demote object "data/large.bin"
    When the tier collector runs
    Then the served root does not hold the file "data/large.bin"
    When I get object "data/large.bin"
    Then the response status is 200
    And the object body length is 2097152
    And the object ETag matches the MD5 of the uploaded bytes

  @tier
  Scenario: A read-only run serves reads and refuses writes
    Given I create bucket "data"
    And I upload "data/warm.txt" with body "warm"
    And I upload "data/cold.txt" with body "cold"
    And I demote object "data/cold.txt"
    When the tier server is restarted read-only
    When I get object "data/warm.txt"
    Then the response status is 200
    And the object body is "warm"
    When I get object "data/cold.txt"
    Then the response status is 200
    And the object body is "cold"
    When I head object "data/warm.txt"
    Then the response status is 200
    When I list objects under "data/"
    Then the response status is 200
    And the listing contains "warm.txt"
    And the listing contains "cold.txt"
    When I send a "PUT" request to "/data/new.txt" with body "x"
    Then the response status is 403
    And the error code is "AccessDenied"
    When I delete object "data/warm.txt"
    Then the response status is 403
    And the error code is "AccessDenied"
    And the served root does not hold the file "data/new.txt"
