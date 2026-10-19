# The lifecycle verbs' process surface (`contracts/cli.md` §`tinio lifecycle`,
# FR-042): the four verbs over the management plane's
# `/buckets/{bucket}/lifecycle` routes, driven through the real binary — the
# `cli_lifecycle.feature` shape. The verbs configure a server-side policy and
# touch no bucket and no object (FR-007's one exception), so every leg needs a
# server on the scenario's root and a root that already has the bucket.
#
# The verbs exist only in a build carrying the `lifecycle` cargo feature, and
# the plane's routes only beside it.
@lifecycle @cli @FR-042
Feature: CLI lifecycle verbs

  # One bucket through all four verbs: `ls` on a bucket that stores nothing is
  # the plane's refusal, `add` upserts by `--id`, `rm` deletes the whole
  # configuration, and `put` replaces it from a file. The root is named
  # positionally on every verb, the way `status`/`stop` are, so discovery never
  # decides which root the verb asks about.
  @cli @lifecycle @FR-042
  Scenario: The four verbs configure one bucket
    Given a storage root
    And the root holds bucket "data" with object "a.txt" and body "hello"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    When I run "tinio lifecycle ls data {root}"
    Then the run fails with exit code 1
    And its stderr contains "no lifecycle configuration"
    When I run "tinio lifecycle add data {root} --id expire-logs --prefix logs/ --expire-days 30"
    Then the run succeeds
    When I run "tinio lifecycle ls data {root}"
    Then the run succeeds
    And its stdout contains "expire-logs"
    And its stdout contains "logs/"
    And its stdout contains "30"
    When I run "tinio lifecycle rm data {root}"
    Then the run succeeds
    When I run "tinio lifecycle ls data {root}"
    Then the run fails with exit code 1
    And its stderr contains "no lifecycle configuration"
    Given the root's state dir holds a "lifecycle.json" file
      """
      {"rules":[{"id":"from-file","status":"Enabled","expiration":{"days":7}}]}
      """
    When I run "tinio lifecycle put data {root}/.tinio/lifecycle.json {root}"
    Then the run succeeds
    When I run "tinio lifecycle ls data {root}"
    Then the run succeeds
    And its stdout contains "from-file"

  # FR-007's no-server refusal, the pair `status` and `stop` answer: the verbs
  # never open a backend store, so a root with nothing running is exit 1 with
  # the server-less sentence — never an empty configuration.
  @cli @lifecycle @FR-042
  Scenario: A verb with no server refuses
    Given a storage root
    When I run "tinio lifecycle ls data {root}"
    Then the run fails with exit code 1
    And its stderr contains "no running server"
