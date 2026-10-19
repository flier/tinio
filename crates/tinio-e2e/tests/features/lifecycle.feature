# The 2026-10-18 object lifecycle (spec FR-042; design
# docs/superpowers/specs/2026-10-18-s3-object-lifecycle-design.md §LC1-LC23;
# protocol home contracts/s3-surface.md §Lifecycle): the three `?lifecycle`
# operations on the bucket subresource, the two refusal classes, the read-only
# and capability-off answers, and one pass that really expires a key.
#
# The corpus is written against the contract, which is what let it exist before
# the implementation; the default run selects it like any other.
@lifecycle @FR-042
Feature: Bucket lifecycle configuration

  # The stored document, whole: the PUT replaces the bucket's configuration
  # and the GET serves it back — one rule with the Days form and a prefix
  # filter, out of the accepted element set.
  @lifecycle @FR-042
  Scenario: A configuration round-trips
    Given I create bucket "data"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>expire-logs</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 200
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 200
    And the response body contains "<ID>expire-logs</ID>"
    And the response body contains "<Prefix>logs/</Prefix>"
    And the response body contains "<Status>Enabled</Status>"
    And the response body contains "<Days>30</Days>"

  # An existing bucket that stores no configuration — the answer is the
  # resource's own 404, never an empty 200.
  @lifecycle @FR-042
  Scenario: A bucket with no configuration answers NoSuchLifecycleConfiguration
    Given I create bucket "data"
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 404
    And the error code is "NoSuchLifecycleConfiguration"

  # The delete is idempotent, as on the CORS surface: the second DELETE on the
  # same bucket, with nothing left to delete, still answers 204 — the storage's
  # bucket check is the only thing that can answer NoSuchBucket.
  @lifecycle @FR-042
  Scenario: Delete clears the configuration and stays idempotent
    Given I create bucket "data"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>expire-logs</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 200
    When I send a "DELETE" request to "/data?lifecycle"
    Then the response status is 204
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 404
    And the error code is "NoSuchLifecycleConfiguration"
    When I send a "DELETE" request to "/data?lifecycle"
    Then the response status is 204

  # A transition names a storage tier this server does not have: the semantic
  # class (`InvalidRequest`), never the schema one, and nothing is stored. The
  # rule is otherwise complete — a filter, one action — so `Transition` is its
  # only violation: a rule with neither a `Filter` nor a legacy `Prefix` is the
  # *schema* class (`MalformedXML`), and two violations in one document would
  # make the answer depend on which check runs first. The contract's "naming
  # that the server has no storage tiers" is the handler's sentence, not this
  # corpus's — the class is the behaviour pinned here.
  @lifecycle @FR-042
  Scenario: A transition is refused
    Given I create bucket "data"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>to-glacier</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
          <Transition>
            <Days>30</Days>
            <StorageClass>GLACIER</StorageClass>
          </Transition>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 400
    And the error code is "InvalidRequest"
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 404
    And the error code is "NoSuchLifecycleConfiguration"

  # Read-only mode (FR-023): the two writes are write rows of the operation
  # table, so the plane refuses them before the storage is reached, and the GET
  # is served — the subresource's own 404, not the mode's 403, is what shows the
  # read path ran. Fixtures are planted on disk for the same reason
  # `read_only.feature` plants them: `I create bucket` is refused here too.
  @fs @read-only @lifecycle @FR-042
  Scenario: A read-only run serves the GET and refuses the two writes
    Given the served root holds bucket "data" with object "a.txt" and body "hello"
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 404
    And the error code is "NoSuchLifecycleConfiguration"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>expire-logs</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "DELETE" request to "/data?lifecycle"
    Then the response status is 403
    And the error code is "AccessDenied"

  # The capability off (`[s3] lifecycle = false`, contracts/config.md): all
  # three answer the derived `NotImplemented` and no pass runs. The switch is
  # `@minimal-caps`'s arm in `steps/mod.rs`'s `config_from_tags`, which must
  # clear the lifecycle capability once the server carries one (Task 6) — the
  # same carry the ACL and CORS toggles took in their own tasks.
  @minimal-caps @lifecycle @FR-042
  Scenario: The capability off answers NotImplemented on all three
    Given I create bucket "data"
    When I send a "GET" request to "/data?lifecycle"
    Then the response status is 501
    And the error code is "NotImplemented"
    When I put this lifecycle configuration on bucket "data"
      """xml
      <LifecycleConfiguration>
        <Rule>
          <ID>expire-logs</ID>
          <Filter><Prefix>logs/</Prefix></Filter>
          <Status>Enabled</Status>
          <Expiration><Days>30</Days></Expiration>
        </Rule>
      </LifecycleConfiguration>
      """
    Then the response status is 501
    And the error code is "NotImplemented"
    When I send a "DELETE" request to "/data?lifecycle"
    Then the response status is 501
    And the error code is "NotImplemented"

  # The one real expiry: a `Date` in the past is accepted and takes effect at
  # the next pass, so the key goes. **A `@cli` scenario, not an in-process one**:
  # the pass is the serving path's (`ServingBackend::background`, Task 10), which
  # the harness's own plane does not run, so the smallest `[lifecycle]` cadence
  # the config allows is the only way to watch a pass inside a test's patience —
  # and the wait is bounded rather than a sleep. The rule goes in through the
  # plane's JSON (`tinio lifecycle put`), the S3 PUT's checksum requirement being
  # the wire's business and not this leg's.
  @cli @lifecycle @FR-042
  Scenario: A date in the past expires the key on the next pass
    Given a storage root
    And the root's state dir holds a "config.toml" file
      """
      version = 1

      [lifecycle]
      delay = "10ms"
      cycle = "1s"
      """
    And the root holds bucket "data" with object "a.txt" and body "hello"
    And the root's state dir holds a "lifecycle.json" file
      """
      {"rules":[{"id":"expire-now","status":"Enabled","expiration":{"date":1577836800}}]}
      """
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    And the client is bound to the run's endpoint
    When I run "tinio lifecycle put data {root}/.tinio/lifecycle.json {root}"
    Then the run succeeds
    And the key "/data/a.txt" is gone within 30000 milliseconds
