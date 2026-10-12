# The pg backend's process surface: the `@cli` scenarios a `[storage] type = "pg"`
# configuration drives through the real binary. Every other `@cli` scenario is
# filesystem-rooted, so the pre-runtime half a pg run alone reaches — the
# synthetic root `resolve::pg` derives from the dsn, the state directory, the
# access log, the sandbox plan and the `[storage] type` selector — had no
# automated evidence before this file; these scenarios are that evidence.
#
# **A scratch database per scenario, never the shared `tinio` one.** PG12's
# ownership lock is process-lifetime and the shared fixture belongs to every
# other leg of the pass, so `Given a scratch pg database and a pg configuration`
# creates a database of its own on `TINIO_PG_DSN`'s server and drops it when the
# scenario ends (on every path out, a failing assertion included).
#
# **`TINIO_STATE_DIR` names the served root.** A pg run serves no directory — the
# database is the store — so `resolve::pg::resolve_root`'s override arm takes it
# verbatim as the *synthetic root* (`an_explicit_state_dir_moves_the_synthetic_root`
# pins the rule), and its state directory is then `<root>/.tinio`: the ordinary
# machinery, observable with the same steps every other `@cli` scenario uses.
# Pointing the variable at the scenario's scratch root is what makes a pg run's
# `state`, socket and control channel land where this harness reads them.
#
# `@pg` keeps these out of the fs and mem legs (the default filter always
# excludes it, and the mem pass's list carries `not @pg`); the pg pass's own list
# carries no such exclusion, so they run there and nowhere else (PG17).
@T055 @FR-007
Feature: CLI over a PostgreSQL store

  Background:
    Given a storage root
    And a scratch pg database and a pg configuration
    And the child's state directory override is the served root

  @cli @pg
  Scenario: A pg configuration serves, answers a request, and stops cleanly
    When I run "tinio server --config {root}/pg.toml --port 0"
    Then the readiness line reports a bound port
    And the root publishes a state file only when the run is serving
    And the file "{root}/.tinio/access.log" exists
    When the client is bound to the run's endpoint
    And I send a "PUT" request to "/pgcli" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/pgcli/hello.txt" signed as "root" with headers and body "a request that round-trips the pg database"
      | Content-Type | text/plain |
    Then the response status is 200
    When I send a "GET" request to "/pgcli/hello.txt" signed as "root"
    Then the response status is 200
    And the object body is "a request that round-trips the pg database"
    When I stop the server through its control channel
    And the run ends
    Then the run succeeds
    And the root's state file is gone within 2000 milliseconds
    And the endpoint the run reported stops accepting

  @cli @pg
  Scenario: A second writable server on the same pg database is refused
    When I run "tinio server --config {root}/pg.toml --port 0"
    Then the readiness line reports a bound port
    When I run "tinio server --config {root}/pg.toml --port 0"
    Then the run fails with exit code 1
    And the run never reported readiness
    And its stdout contains "postgres advisory lock"
