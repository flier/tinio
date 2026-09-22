# The CLI's process surface (T055): every scenario here spawns the real `tinio`
# binary. The readiness line, `--daemon`'s parent probe, the round trip, the
# state-dir and mode rules `status`/`stop` are held to (decision 19), the `.env`
# level of FR-016's chain, and the two sandbox refusals.
#
# These are the legs `crates/tinio/tests/lifecycle.rs` used to hold. That file
# keeps only the console-handler leg, which no scenario can express: it needs
# `GenerateConsoleCtrlEvent` under `#[cfg(windows)]` and one of the workspace's
# five enumerated `#[allow(unsafe_code)]` sites. Its module doc says so, and
# `docs/tests.md`'s "CLI (tinio)" section carries the leg-by-leg mapping.
#
# The harness (`tests/steps/cli.rs`) owns one scratch directory per scenario:
# the served root, the state dir every child is handed in `TINIO_STATE_DIR`, the
# systemd notify socket and each run's two stream files all live under it, so no
# scenario touches a real root or the operator's home. `{root}` is that scratch
# root and `{state}` the relocated state dir.
@T055 @FR-007
Feature: CLI lifecycle

  @cli
  Scenario: A bare invocation is a usage error
    Given a storage root
    When I run "tinio"
    Then the run fails with exit code 2
    And its stderr is not empty

  @cli @FR-016
  Scenario: The environment supplies a flag
    Given a storage root
    And the child's environment holds "TINIO_PORT" = "0"
    When I run "tinio server {root}"
    Then the readiness line reports a bound port

  @cli @FR-016
  Scenario: A `.env` file supplies a flag and the credential pair
    Given a storage root
    And the root's state dir holds a ".env" file
      """
      TINIO_PORT=0
      TINIO_ACCESS_KEY=from-env-file
      TINIO_SECRET_KEY=from-env-file-secret
      """
    When I run "tinio server {root}"
    Then the readiness line reports a bound port
    And its stderr contains "credentials from TINIO_ACCESS_KEY/TINIO_SECRET_KEY"

  @cli
  Scenario: A second server on the same root is refused
    Given a storage root
    And a server is serving the root
    When I run "tinio server {root} --port 0"
    Then the run fails with exit code 1
    And its stderr contains "already"

  @cli
  Scenario: `--daemon` returns only once the server is ready
    Given a storage root
    When I run "tinio server {root} --port 0 --daemon"
    Then the run succeeds
    And the readiness line reports a bound port
    And the run returned within 15000 milliseconds
    And the daemon published its state file
    When I run "tinio stop {root}"
    Then the run succeeds
    And the root's state file is gone within 2000 milliseconds

  @cli
  Scenario: A daemon whose startup fails exits one and says where to look
    Given a storage root
    And a server is serving the root
    When I run "tinio server {root} --port 0 --daemon"
    Then the run fails with exit code 1
    And its stderr contains "tinio status"

  @cli @unix
  Scenario: The systemd notifications follow the lifecycle
    Given a storage root
    And the systemd notify socket is bound
    And systemd's watchdog interval is 500000 microseconds
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    And systemd received "READY=1"
    And systemd received "WATCHDOG=1" within 500 milliseconds
    And systemd received a second "WATCHDOG=1" within 1000 milliseconds
    When I run "tinio stop {root}"
    Then the run succeeds
    And systemd received "STOPPING=1"

  @cli @SC-005 @SC-007
  Scenario: A server starts, reports and stops
    Given a storage root
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    And the server became ready within 1000 milliseconds
    When I run "tinio status {root}"
    Then the run succeeds
    And its stdout contains "running"
    And its stdout names the bound endpoint
    And its stdout names the root's state dir
    When I run "tinio stop {root}"
    Then the run succeeds
    And the run returned within 2000 milliseconds
    And the root's state file is gone within 2000 milliseconds

  @cli
  Scenario: Status on an uninitialised root is a reported state
    Given a storage root
    When I run "tinio status {root}"
    Then the run fails with exit code 1
    And its stdout contains "stopped"

  @cli
  Scenario: Stop with no server is an error
    Given a storage root
    When I run "tinio stop {root}"
    Then the run fails with exit code 1
    And its stderr contains "no running server"

  @cli
  Scenario: A state file nobody answers for is a stopped server
    Given a storage root
    And a server is serving the root
    When I kill the server
    Then the root still holds its state file
    When I run "tinio status {root}"
    Then the run fails with exit code 1
    And its stdout contains "stopped"

  @cli
  Scenario: A read-only client finds the server in the other candidate state dir
    Given a storage root
    And a server is serving the root
    When I run "tinio status {root} --read-only"
    Then the run succeeds
    And its stdout names the root's state dir
    And its stdout does not name the relocated state dir
    When I run "tinio stop {root} --read-only"
    Then the run succeeds

  @cli
  Scenario: A client with no mode flag takes the root's own state dir first
    Given a storage root
    And a server is serving the root
    When I run "tinio status {root}"
    Then the run succeeds
    And its stdout names the root's state dir
    When I run "tinio stop {root}"
    Then the run succeeds

  @cli
  Scenario: A client leaves a fresh root exactly as it found it
    Given a storage root
    When I run "tinio status {root}"
    Then the run fails with exit code 1
    And its stdout contains "stopped"
    And the root holds no state directory
    And the root holds exactly what it held before
    When I run "tinio stop {root}"
    Then the run fails with exit code 1
    And the root holds no state directory

  @cli @FR-036
  Scenario: A sandbox that cannot be honoured refuses and never serves
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the run fails
    And its stderr contains "sandbox"
    And the root publishes no state file

  @cli @FR-036
  Scenario: The sandbox variable is never silently ignored
    Given a storage root
    And the binary's "--sandbox" flag is probed
    And the child's environment holds "TINIO_SANDBOX" = "1"
    When I run "tinio server {root} --port 0"
    Then the run fails
    And its stderr contains "sandbox"
    And the refusal names the platform where the flag is built in, and the build where it is not
    And the root publishes no state file
