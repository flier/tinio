# FR-016's env-file channel, end to end over a pg store: a run whose
# `TINIO_PG_DSN` comes from `<state dir>/.env` rather than from the
# configuration document or the process environment. The CLI lane's unit legs
# cover the resolution, but `dotenvy` writes the *process* environment, which a
# Rust test may not mutate (`docs/tests.md`), so the wiring had no automated
# process-surface leg; this file is that leg -- the real binary, spawned, with
# the dsn in the file alone and every `TINIO_*` scrubbed from its environment.
#
# **A scratch database per scenario, never the shared `tinio` one.** PG12's
# ownership lock is process-lifetime and the shared fixture belongs to every
# other leg of the pass, so `Given a scratch pg database and a pg configuration
# with no dsn` creates a database of its own on `TINIO_PG_DSN`'s server and
# drops it when the scenario ends (on every path out, a failing assertion
# included).
#
# **`TINIO_STATE_DIR` names the served root.** A pg run serves no directory, so
# the override arm takes it verbatim as the *synthetic root* and its state
# directory is then `<root>/.tinio` -- where the `.env` file the run loads is
# written, and where its `state` file and access log land.
#
# `@pg` keeps these out of the fs and mem legs (the default filter always
# excludes it); the pg pass's own list carries no such exclusion, so they run
# there and nowhere else (PG17), serialized because every scenario shares one
# database.
@T055 @FR-007 @FR-016
Feature: An env-only PostgreSQL dsn from the state directory's .env

  Background:
    Given a storage root
    And a scratch pg database and a pg configuration with no dsn
    And the child's state directory override is the served root
    And the pg dsn is written to the state directory's environment file

  @cli @pg
  Scenario: A pg run whose dsn is only in the state dir's .env starts and serves
    When I run "tinio server --config {root}/pg.toml --port 0"
    Then the readiness line reports a bound port
    And the file "{root}/.tinio/access.log" exists
    When the client is bound to the run's endpoint
    And I send a "PUT" request to "/pgenv" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/pgenv/hello.txt" signed as "root" with headers and body "an env-only dsn round-trips the pg database"
      | Content-Type | text/plain |
    Then the response status is 200
    When I send a "GET" request to "/pgenv/hello.txt" signed as "root"
    Then the response status is 200
    And the object body is "an env-only dsn round-trips the pg database"
    When I stop the server through its control channel
    And the run ends
    Then the run succeeds
