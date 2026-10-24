# The Object Lock verbs' process surface (`contracts/cli.md` §`tinio object-lock`,
# §`tinio retention`, §`tinio legalhold`; FR-043's second CLI exception to
# FR-007): the seven commands over the management plane's two
# `/buckets/{bucket}/object-lock` routes, driven through the real binary.
#
# Every leg spawns a server on the scenario's root, plants the bucket, and —
# where a per-version verb is the subject — writes the object through the wire
# so the addressed version is a real version row. The verbs themselves open no
# store and name no S3 client (OL3): they resolve the root exactly as `status`
# and `stop` do and ask the running server.
#
# The groups exist only in a build carrying the `object-lock` cargo feature,
# and the plane's routes only beside the capability (OL20/OL23). The `@cli`
# tag on every scenario is the harness's own (cli.rs): a feature-level tag is
# not part of a scenario's tag set as far as the hooks read it.
@cli @object-lock @FR-043
Feature: CLI object lock verbs

  # OL24's auto-enable-versioning convenience: `object-lock enable` drives the
  # plane's enable path, which enables the bucket's versioning first. The
  # version-id header on the next write is the proof — a versioned write
  # carries an identity, an unversioned one does not.
  @cli @object-lock @FR-043
  Scenario: The enable verb enables versioning before it writes the configuration
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I run "tinio object-lock enable data {root}"
    Then the run succeeds
    And its stdout contains "object lock enabled for data"
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the response has a "x-amz-version-id" header
    When I run "tinio retention info data {root}"
    Then the run succeeds
    And its stdout contains "object lock is enabled for data: no default retention"

  # The per-version verbs: `set` writes the computed retain-until through the
  # object route, `info` reads it back, and `clear` carries the implicit
  # governance bypass — after which the version carries no retention at all.
  @cli @object-lock @FR-043
  Scenario: A version's retention is set, read and cleared
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I run "tinio object-lock enable data {root}"
    Then the run succeeds
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    When I run "tinio retention set data/a.txt governance 30d {root}"
    Then the run succeeds
    And its stdout contains "retention for data/a.txt: GOVERNANCE until"
    When I run "tinio retention info data/a.txt {root}"
    Then the run succeeds
    And its stdout contains "retention for data/a.txt: GOVERNANCE until"
    When I run "tinio retention clear data/a.txt {root}"
    Then the run succeeds
    And its stdout contains "retention cleared for data/a.txt"
    When I run "tinio retention info data/a.txt {root}"
    Then the run fails with exit code 1
    And its stderr contains "no retention"

  # The bucket-configuration scope: `set --default` enables versioning first
  # (OL24), so the per-version verb it is followed by works on the bucket it
  # enabled; `info <bucket>` without a key is the bucket-config fallback; and
  # `clear --default` clears only the rule — the enabled bit and every
  # per-version lock survive it.
  @cli @object-lock @FR-043
  Scenario: The bucket configuration is written, read and cleared through retention
    Given a storage root
    And the root holds bucket "photos" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I send a "PUT" request to "/photos/a.txt" signed as "root"
    Then the response status is 200
    When I run "tinio retention info photos {root}"
    Then the run succeeds
    And its stdout contains "object lock is not enabled for photos"
    When I run "tinio retention set photos --default governance 3y {root}"
    Then the run succeeds
    And its stdout contains "default retention for photos: GOVERNANCE 3YEARS"
    When I run "tinio retention info photos {root}"
    Then the run succeeds
    And its stdout contains "object lock is enabled for photos: default retention GOVERNANCE 3YEARS"
    When I run "tinio retention set photos/a.txt governance 5d {root}"
    Then the run succeeds
    And its stdout contains "retention for photos/a.txt: GOVERNANCE until"
    When I run "tinio retention clear photos --default {root}"
    Then the run succeeds
    And its stdout contains "default retention cleared for photos"
    When I run "tinio retention info photos {root}"
    Then the run succeeds
    And its stdout contains "object lock is enabled for photos: no default retention"
    When I run "tinio retention info photos/a.txt {root}"
    Then the run succeeds
    And its stdout contains "retention for photos/a.txt: GOVERNANCE until"

  # The three `mc` output shapes (OL30): retention's
  # `mode`/`until`/`urlpath`/`versionID`/`status`/`error`, the bucket
  # configuration's `op`/`enabled`/`mode`/`validity`, and the legal hold's
  # `legalhold`/`urlpath`/`key`/`versionID`. The field set is exact per
  # document (steps/cli_object_lock.rs). A failed keyed read still renders its
  # document and exits 1.
  @cli @object-lock @FR-043
  Scenario: The JSON documents mirror mc's field names
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I run "tinio object-lock enable data --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value  |
      | op       | enable |
      | enabled  | true   |
      | mode     |        |
      | validity |        |
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    When I run "tinio retention set data/a.txt governance 30d --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value      |
      | mode      | GOVERNANCE |
      | until     | <rfc3339>  |
      | urlpath   | data/a.txt |
      | versionID |            |
      | status    | success    |
      | error     |            |
    When I run "tinio retention info data/a.txt --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value      |
      | mode      | GOVERNANCE |
      | until     | <rfc3339>  |
      | urlpath   | data/a.txt |
      | versionID |            |
      | status    | success    |
      | error     |            |
    When I run "tinio legalhold set data/a.txt --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value       |
      | legalhold | ON          |
      | urlpath   | data/a.txt  |
      | key       | a.txt       |
      | versionID |             |
    # The read is the assertion that separates "stored" from "echoed": `set`'s
    # document is built from the request, while `info` is the plane's view.
    When I run "tinio legalhold info data/a.txt --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value      |
      | legalhold | ON         |
      | urlpath   | data/a.txt |
      | key       | a.txt      |
      | versionID |            |
    When I run "tinio legalhold info data/a.txt {root}"
    Then the run succeeds
    And its stdout contains "legal hold for data/a.txt: ON"
    When I run "tinio legalhold clear data/a.txt --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value      |
      | legalhold | OFF        |
      | urlpath   | data/a.txt |
      | key       | a.txt      |
      | versionID |            |
    When I run "tinio retention clear data/a.txt --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field     | value      |
      | mode      |            |
      | until     |            |
      | urlpath   | data/a.txt |
      | versionID |            |
      | status    | success    |
      | error     |            |
    When I run "tinio retention info data/a.txt --json {root}"
    Then the run fails with exit code 1
    And its stdout is the JSON document
      | field     | value      |
      | mode      |            |
      | until     |            |
      | urlpath   | data/a.txt |
      | versionID |            |
      | status    | error      |
      | error     | no retention |
    When I run "tinio retention set data --default compliance 30d --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value      |
      | op       | set        |
      | enabled  | true       |
      | mode     | COMPLIANCE |
      | validity | 30DAYS     |
    When I run "tinio retention info data --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value      |
      | op       | info       |
      | enabled  | true       |
      | mode     | COMPLIANCE |
      | validity | 30DAYS     |
    When I run "tinio retention clear data --default --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value |
      | op       | clear |
      | enabled  | true  |
      | mode     |       |
      | validity |       |

  # The wire's identity drives the per-version verbs, not only the current
  # address: the scenario captures the id the versioned write returned and
  # names it on set, info and clear. The governance shortening is the bypass
  # pair — refused without `--bypass` and applied with it (OL11/OL15).
  @cli @object-lock @FR-043
  Scenario: A version id addresses the retention verbs and the bypass
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I run "tinio object-lock enable data {root}"
    Then the run succeeds
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "version"
    # A second write makes the captured id noncurrent: an ignored `--version-id`
    # would address this newer version, so the checks below would see it instead.
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the response has a "x-amz-version-id" header
    When I run "tinio retention set data/a.txt governance 30d --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "retention for data/a.txt: GOVERNANCE until"
    When I run "tinio retention info data/a.txt {root}"
    Then the run fails with exit code 1
    And its stderr contains "no retention"
    When I run "tinio retention info data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "retention for data/a.txt: GOVERNANCE until"
    When I run "tinio retention set data/a.txt governance 1d --version-id {version} {root}"
    Then the run fails with exit code 1
    And its stderr contains "access denied"
    When I run "tinio retention set data/a.txt governance 1d --version-id {version} --bypass {root}"
    Then the run succeeds
    And its stdout contains "retention for data/a.txt: GOVERNANCE until"
    When I run "tinio retention clear data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "retention cleared for data/a.txt"
    When I run "tinio retention info data/a.txt --version-id {version} {root}"
    Then the run fails with exit code 1
    And its stderr contains "no retention"

  # The legal-hold verbs' version address: set, read and clear at the id the
  # write returned. The `info` between them is the plane's stored answer.
  @cli @object-lock @FR-043
  Scenario: A version id addresses the legal-hold verbs
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When the client is bound to the run's endpoint
    And I run "tinio object-lock enable data {root}"
    Then the run succeeds
    When I send a "PUT" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the response header "x-amz-version-id" is stored as "version"
    When I run "tinio legalhold set data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "legal hold for data/a.txt: ON"
    When I run "tinio legalhold info data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "legal hold for data/a.txt: ON"
    When I run "tinio legalhold clear data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "legal hold for data/a.txt: OFF"
    When I run "tinio legalhold info data/a.txt --version-id {version} {root}"
    Then the run succeeds
    And its stdout contains "legal hold for data/a.txt: OFF"

  # OL24's no-op: `clear --default` never enables. It reports the bucket as
  # not enabled and writes nothing — the bucket configuration is still absent
  # afterwards, which is what the final read proves.
  @cli @object-lock @FR-043
  Scenario: `retention clear --default` is a no-op on a bucket that is not enabled
    Given a storage root
    And the root holds bucket "cold" with object "seed.txt" and body "seed"
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When I run "tinio retention clear cold --default {root}"
    Then the run succeeds
    And its stdout contains "object lock is not enabled for cold"
    When I run "tinio retention clear cold --default --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value |
      | op       | clear |
      | enabled  | false |
      | mode     |       |
      | validity |       |
    When I run "tinio retention info cold {root}"
    Then the run succeeds
    And its stdout contains "object lock is not enabled for cold"
    When I run "tinio retention info cold --json {root}"
    Then the run succeeds
    And its stdout is the JSON document
      | field    | value |
      | op       | info  |
      | enabled  | false |
      | mode     |       |
      | validity |       |

  # FR-007's no-server refusal, the sentence `status` and `stop` answer: the
  # verbs never open a store, so no running server is exit 1 for every group.
  @cli @object-lock @FR-043
  Scenario: The verbs refuse a root with no server
    Given a storage root
    When I run "tinio object-lock enable data {root}"
    Then the run fails with exit code 1
    And its stderr contains "no running server"
    When I run "tinio retention info data {root}"
    Then the run fails with exit code 1
    And its stderr contains "no running server"
    When I run "tinio legalhold info data/a.txt {root}"
    Then the run fails with exit code 1
    And its stderr contains "no running server"

  # The contract's own refusals (exit 2), checked before the plane is asked:
  # a keyed verb without a key, a `--default` with an object-only flag, the
  # strict `Nd`/`Ny` spelling, and an `object-lock enable` aimed at an object.
  @cli @object-lock @FR-043
  Scenario: A target or flag the contract does not admit is a usage error
    Given a storage root
    And the root holds bucket "data" with object "seed.txt" and body "seed"
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When I run "tinio retention set data governance 30d {root}"
    Then the run fails with exit code 2
    And its stderr contains "needs a key"
    When I run "tinio retention set data/a.txt governance 30D {root}"
    Then the run fails with exit code 2
    And its stderr contains "the units are"
    When I run "tinio retention set data --default governance 30d --bypass {root}"
    Then the run fails with exit code 2
    And its stderr contains "takes no --bypass"
    When I run "tinio retention info data --version-id abc {root}"
    Then the run fails with exit code 2
    And its stderr contains "takes no --version-id"
    When I run "tinio legalhold set data {root}"
    Then the run fails with exit code 2
    And its stderr contains "needs a key"
    When I run "tinio object-lock enable data/a.txt {root}"
    Then the run fails with exit code 2
    And its stderr contains "takes a bucket, not an object"
