# Confinement (FR-036): the channels that request it — the `--sandbox` flag,
# `TINIO_SANDBOX`, `[server] sandbox` and `.env`'s spelling of the variable —
# and the precedence between them (FR-016): refused by name where nothing can
# honour a request, honoured where the target's backend is wired. Which of the
# two a scenario owes is dispatched, never accepted either way (`steps/cli.rs`'s
# `SandboxChannel` and the `the {string} request is answered, never ignored`
# step, over the probe `Cli::knows_flag` reads), and the confinement is witnessed
# both ways: a scenario that claims it *happened* asserts `the run was confined`,
# and one that claims it did not asserts `the run was not confined` — each the
# two-carrier pair, so a run that merely mentioned the channel cannot pass in
# either direction.
#
# `@confinement` is the host gate on this file's scenarios: they exercise a
# confinement, which no leg may assume — a Windows launcher pays an ACL grant
# worth tens of seconds of variance, so the runner runs them on macOS (where the
# launcher only binds and spawns) and takes them elsewhere only on
# `TINIO_E2E_CONFINEMENT=1` (`tests/cucumber.rs`).
#
# The scenario families below the pair above are the plan's §3a
# (`docs/superpowers/plans/2026-09-29-sandbox-bdd-e2e.md`): each channel alone,
# the precedence between them, `.env` as the environment channel one level lower
# and what it cannot reach, and the refusals a bad value earns — over FR-016's
# chain and FR-036's switch.
#
# The G8/G9 pair is the plan's §3b, and they are the coverage that plan exists
# for: the S3 journey a confined run actually serves — a bucket created, an
# object put, got back byte-identical and listed, by a signed client bound to
# the spawned binary's own endpoint — read-write (G8) and through the
# `--read-only --sandbox` pair (G9), where the write is refused and the reads
# still answer. §5 retires the Linux Rust leg G8 is strictly stronger than,
# which proved only that the listener answered a bare connect.
#
# The rest of §3b follows them: what a run *reports* about itself and what the
# operator sees when that reporting fails (G10's pid and G12's publish failure),
# and the two channel facts beside it (G13's HTTP control channel — a launcher
# fact, with the in-place arm's measured difference named where the scenario
# is — and G14's OTLP trade, FR-036's own ruling).
#
# §3b closes with the refusals an operator can walk into: the exposure set that
# cannot be resolved (G15 — the served path a file, a link in the chain a grant
# would write through, a DIR that is not there, and no DIR at all) and the state
# directory nested with the served root (G16), which is one rule read in both
# directions and, on macOS, two more arrivals. Each scenario's comment names the
# layer whose sentence lands, because for three of these rows that is *not* the
# layer that owns the rule — the configuration layer and the storage open answer
# first, and a scenario that asserted the owner's sentence would fail.
#
# Source: `specs/001-s3-local-server/contracts/cli.md` (the CLI surface prose)
# with FR-036 as the requirement; `docs/tests.md`'s "CLI (tinio)" section
# carries the coverage facts — a feature-off build refuses by build and never
# by platform, while a feature-on host must reach readiness.
Feature: Confinement

  # The two channels that cannot be shrunk to a probe — the flag a parser without the feature does
  # not know, and the variable that cannot be compiled away — and the pair is
  # `crates/tinio-server/tests/serve_sandbox_channels.rs`'s cases 7 and 8 until the placement
  # cleanup retired that file (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`).
  # Case 7's subject — a build whose parser does not know the flag — cannot live here (cucumber
  # drives one binary), so it stays Rust in `crates/tinio-server/tests/sandbox_feature_off.rs`.
  # **The refusal's own shape moved with the scenarios**, into the dispatch step both `Then`s use
  # (`steps/cli.rs`'s `refused_by_build`, over `refusal_frame_only`): a build's `--sandbox`
  # refusal, which clap answers before any command is dispatched, writes no frame at all and
  # creates nothing under the run's root; a channel the crate itself refuses is framed by the
  # serving arm's error path — exactly the encoder's three stdout lines — and lands before the
  # store, `<root>/.tinio/meta.redb`, the serving path's first write under the root. A build whose
  # parser knows the flag owes the honour arm instead, and the probe is what dispatches.
  @cli @confinement @FR-036
  Scenario: A `--sandbox` request is never silently ignored
    Given a storage root
    And the binary's "--sandbox" flag is probed
    When I run "tinio server {root} --port 0 --sandbox"
    Then the "--sandbox" request is answered, never ignored
    And the root publishes a state file only when the run is serving

  @cli @confinement @FR-036
  Scenario: The `TINIO_SANDBOX` variable is never silently ignored
    Given a storage root
    And the binary's "--sandbox" flag is probed
    And the child's environment holds "TINIO_SANDBOX" = "1"
    When I run "tinio server {root} --port 0"
    Then the "TINIO_SANDBOX" request is answered, never ignored
    And the root publishes a state file only when the run is serving

  # G1: each channel is enough on its own, and every row is held to the same
  # shape — the request answered *and* the confinement witnessed — so a run that
  # merely mentioned the channel cannot pass. The flag and variable rows restate
  # the pair above deliberately; the configuration key is the channel this file
  # did not have.
  @cli @confinement @FR-036
  Scenario Outline: Each channel is enough on its own
    Given a storage root
    And the binary's "--sandbox" flag is probed
    And the "<channel>" channel asks for the confinement
    When I run "<command>"
    Then the "<channel>" request is answered, never ignored
    And the run was confined

    Examples:
      | channel          | command                                |
      | --sandbox        | tinio server {root} --port 0 --sandbox |
      | TINIO_SANDBOX    | tinio server {root} --port 0           |
      | [server] sandbox | tinio server {root} --port 0           |

  # G2 (FR-016): the higher level answers even when it says `false`, so a
  # variable `0` over a configuration `true`, and an explicit `--sandbox=false`
  # over a variable `1`, both leave the run serving *unconfined*. **The two
  # assertions beside each other are the point**: the run serves, and it did not
  # confine — `the run was not confined` is `the run was confined`'s mirror
  # (`steps/cli.rs`), the platform's `step=` record absent *and* the platform's
  # own cross-check negative, so a row fails whether the precedence broke towards
  # a refusal or towards a confinement the higher `false` should have beaten.
  @cli @confinement @FR-036 @FR-016
  Scenario Outline: A higher explicit `false` beats a lower `true`
    Given a storage root
    And the "<lower>" channel asks for the confinement
    And the child's environment holds "TINIO_SANDBOX" = "<value>"
    When I run "<command>"
    Then the readiness line reports a bound port
    And the run was not confined

    Examples:
      | lower            | value | command                                      |
      | [server] sandbox | 0     | tinio server {root} --port 0                 |
      | TINIO_SANDBOX    | 1     | tinio server {root} --port 0 --sandbox=false |

  # The third of §3a's precedence pairs, and the mirror of the outline above:
  # the `--sandbox` flag is the highest level of FR-016's chain and answers
  # before `TINIO_SANDBOX`, so the variable's `0` below it does not switch the
  # confinement off — the run is confined, and the witness is what says so. (The
  # flag's presence is the higher level's answer: the bare flag is `true`, which
  # is the `default_missing_value` its declaration carries.)
  @cli @confinement @FR-036 @FR-016
  Scenario: The flag answers before the variable
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX" = "0"
    When I run "tinio server {root} --port 0 --sandbox"
    Then the run was confined

  # G2's third reader, and the spelling the outline above does not reach: **the
  # configuration file's own `false`**. `[server] sandbox = false` never reaches the
  # process as an environment variable, so it is read by the configuration probe
  # alone (`crates/tinio-cli/src/config/discover.rs`'s `load_or_create` over the file the
  # step plants) — and a run that refused it would be a bug of the same family as the
  # one the outline's own rows close. The pair asserted here is the
  # outline's own: the run serves, and it did not confine — the platform's `step=`
  # record absent *and* the platform's own cross-check negative, so the row fails
  # whether the key broke towards a refusal or towards a confinement.
  @cli @confinement @FR-036 @FR-016
  Scenario: A configuration key switched off is not a refusal
    Given a storage root
    And the root's state dir holds a ".tinio.toml" file
      """
      version = 1

      [server]
      sandbox = false
      """
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    And the run was not confined

  # G3: `<state dir>/.env`'s `TINIO_SANDBOX` is the environment channel one level
  # lower, because the file is loaded into the process environment between the
  # CLI's two parses (`crates/tinio-cli/src/resolve/locate.rs`'s
  # `apply_env_file`) — so what asks for the confinement is the variable, and a
  # refusal names `TINIO_SANDBOX` and never the file it was read from.
  @cli @confinement @FR-036
  Scenario: `.env` is the environment channel one level lower
    Given a storage root
    And the binary's "--sandbox" flag is probed
    And the ".env" channel asks for the confinement
    When I run "tinio server {root} --port 0"
    Then the ".env" request is answered, never ignored
    And the run was confined

  # G4: dotenvy never overrides a variable the process already has, which is what
  # puts the process environment above the file without a precedence rule of its
  # own (`apply_env_file` again) — so a `TINIO_SANDBOX=0` the operator exported
  # leaves the file's `1` unread and the run unconfined, asserted the two ways
  # G2's rows are: it serves, and it did not confine.
  @cli @confinement @FR-036 @FR-016
  Scenario: An exported variable beats `.env`
    Given a storage root
    And the ".env" channel asks for the confinement
    And the child's environment holds "TINIO_SANDBOX" = "0"
    When I run "tinio server {root} --port 0"
    Then the readiness line reports a bound port
    And the run was not confined

  # G5 (FR-016): the file reaches the confinement switch but not the answers that
  # place it. The mode and the state directory are resolved by the discovery
  # parse, *before* `<state dir>/.env` is loaded — the order
  # `crates/tinio-cli/src/resolve/mod.rs`'s module doc owns — so a
  # `TINIO_READ_ONLY=1` in the file does not make the run read-only. Where the
  # mode shows is where the run's `state` file lands: a read-only run resolves
  # the relocated state directory (FR-023) and publishes there, while this run's
  # file is the root's own.
  @cli @confinement @FR-036 @FR-016
  Scenario: `.env` cannot select the read-only mode
    Given a storage root
    And the root's state dir holds a ".env" file
      """
      TINIO_READ_ONLY=1
      """
    When I run "tinio server {root} --port 0"
    Then the root publishes a state file only when the run is serving

  # G6: each platform's own variable is read from the process environment
  # *after* `apply_env_file`, so a value the file supplies reaches the backend
  # exactly as an exported one does — and the run's own record is what says so:
  # the Linux in-place arm re-emits the model it resolved (a forced `userns`
  # either resolves to the user namespace or refuses the run), and the macOS
  # launcher logs the mechanism it resolved before anything acts on it.
  @cli @confinement @FR-036 @linux
  Scenario: `.env` reaches the model variable
    Given a storage root
    And the root's state dir holds a ".env" file
      """
      TINIO_SANDBOX=1
      TINIO_SANDBOX_MODEL=userns
      """
    When I run "tinio server {root} --port 0"
    Then the run was confined
    And its stderr carries a "model" step line naming
      """
      model=UserNamespace
      """

  @cli @confinement @FR-036 @macos
  Scenario: `.env` reaches the mechanism variable
    Given a storage root
    And the root's state dir holds a ".env" file
      """
      TINIO_SANDBOX=1
      TINIO_SANDBOX_MACOS_MECHANISM=sandbox-init
      """
    When I run "tinio server {root} --port 0"
    Then the run was confined
    And its stderr carries a "mechanism" step line naming
      """
      mechanism=sandbox-init
      """

  # G7: a value the switch cannot read is a usage error, never a run that
  # silently takes the default. The variable's spellings are the tolerant
  # parser's (`tinio-util`'s `parse_switch`), and `--sandbox 1` is refused
  # because the switch's value must be attached with `=` — `require_equals` is
  # what keeps the `1` from being read as the flag's value. Nothing served
  # either way, which is the half `the root publishes no state file` asserts.
  @cli @confinement @FR-036
  Scenario Outline: A bad value is refused, never defaulted
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX" = "<value>"
    When I run "<command>"
    Then the run fails with exit code 2
    And its stderr contains "<message>"
    And the root publishes no state file

    Examples:
      | value | command                                  | message                 |
      | yes   | tinio server {root} --port 0             | expected 0/1/true/false |
      | 1     | tinio server {root} --port 0 --sandbox 1 | unexpected argument '1' |

  # The platform variables are the backends' own, and a value its own backend
  # does not accept is refused rather than silently defaulted — the refusal
  # names the variable and both values, and nothing serves.
  #
  # **Every selector's typo is one usage class**, so both rows below assert the
  # same code — and so does the `@windows` "An unknown capability name fails the
  # run" row further down: the usage class is the `Error::Usage { detail }`
  # variant (`crates/tinio-sandbox-core/src/error.rs`) — the structural variant
  # that replaced the `is_usage` classifier on 2026-10-01, the operator-visible
  # sentence unchanged — and each platform builds the sentence its own `detail`
  # carries, one definition each rather than spelling it twice. One operator
  # mistake therefore answers one exit code whichever variable it was typed into.
  @cli @confinement @FR-036 @linux
  Scenario: A bad model is refused
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX" = "1"
    And the child's environment holds "TINIO_SANDBOX_MODEL" = "bogus"
    When I run "tinio server {root} --port 0"
    Then the run fails with exit code 2
    And its stderr contains "TINIO_SANDBOX_MODEL must be `userns` or `root`"
    And the root publishes no state file

  # The macOS mechanism's typo **is** the usage class (`Error::Usage { detail }`
  # carries that one sentence as its `detail`; the `is_usage` classifier is gone
  # since 2026-10-01, and `docs/sandbox.md` fixes the code): the same refusal
  # this variable earns everywhere, exit 2.
  @cli @confinement @FR-036 @macos
  Scenario: A bad mechanism is refused
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX" = "1"
    And the child's environment holds "TINIO_SANDBOX_MACOS_MECHANISM" = "bogus"
    When I run "tinio server {root} --port 0"
    Then the run fails with exit code 2
    And its stderr contains "TINIO_SANDBOX_MACOS_MECHANISM must be `sandbox-exec` or `sandbox-init`"
    And the root publishes no state file

  # G8 — **the coverage this plan exists for**: no e2e anywhere signed an S3
  # request under `--sandbox` before this scenario, and §5 retires the Linux
  # Rust leg it is strictly stronger than — `sandbox_e2e.rs`'s
  # `a_confined_server_serves`, whose probe was a bare connect. The requests
  # here are real SigV4 requests (the suite's own signer, `steps/acl.rs`'s
  # `signed as "root"`) against the endpoint the run itself reported — never a
  # raw connect, and never the in-process plane.
  #
  # The journey is the subject and `the run was confined` is what makes it a
  # claim about a *confined* run: a server that served every one of these
  # requests with no jail in place fails there, on the two-carrier witness,
  # rather than passing on a data-plane leg it happens to share with the
  # unconfined scenarios.
  #
  # **The credentials are the harness's, pinned on the child** (`the child holds
  # the harness's credentials`), because a first start *generates* a random
  # `[auth]` pair into the config it creates: the run serves the pair the
  # resolution produced, and the suite cannot know a generated one. The pinned
  # pair is the one `signed as "root"` signs with, which keeps this to one
  # signing path (`steps/cli.rs`'s `harness_credentials`).
  @cli @confinement @FR-036
  Scenario: A confined server completes a real S3 journey
    Given a storage root
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0 --sandbox"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/confined" signed as "root"
    Then the response status is 200
    When I send a "PUT" request to "/confined/a.txt" signed as "root" with headers and body "a confined payload"
      | Content-Type | text/plain |
    Then the response status is 200
    When I send a "GET" request to "/confined/a.txt" signed as "root"
    Then the response status is 200
    And the object body is "a confined payload"
    When I send a "GET" request to "/confined?list-type=2" signed as "root"
    Then the response status is 200
    And the response body contains "<Key>a.txt</Key>"
    And the run was confined

  # G9: the same journey through the `--read-only --sandbox` pair — reads served
  # from a root the mode never wrote, and a write refused.
  #
  # **The fixture is planted on disk before the run**, because the mode refuses
  # the very writes that would build it over the wire: that is the reason
  # `read_only.feature` plants its roots too, and why the step here is the `@cli`
  # harness's own (`the root holds bucket …`, `steps/cli.rs`).
  #
  # **The refusal asserted is one representative write's client-visible shape**
  # and not the refusal set: the status, the code and AWS's default message are
  # `contracts/s3-surface.md` §Read-only mode's (the whole `write` group of the
  # shared operation table answers `AccessDenied` with `Access Denied`), and that
  # section owns the list. What this scenario adds to the mechanism-level leg
  # (`crates/tinio-server/tests/sandbox_e2e.rs`, which asks the mounted root
  # through `/proc/<pid>/root` and stays) is the half only a client can give: a
  # signed reader is answered `200` with the bytes while the writer beside it is
  # refused, through a confinement that is asserted rather than assumed.
  #
  # **And the disk is read beside the client's status**: the refused write left
  # no `data/b.txt` under the served root — the assertion
  # `crates/tinio-sandbox-windows/tests/confine.rs`'s
  # `a_read_only_root_refuses_writes_and_still_reads` carried until this cleanup
  # moved the journey here, so a server that answered `AccessDenied` and wrote
  # anyway fails rather than passing on the client's view alone.
  @cli @confinement @FR-036
  Scenario: A confined read-only server refuses the write and still serves the read
    Given a storage root
    And the root holds bucket "data" with object "a.txt" and body "a confined payload"
    And the child holds the harness's credentials
    When I run "tinio server {root} --port 0 --sandbox --read-only"
    And the client is bound to the run's endpoint
    When I send a "PUT" request to "/data/b.txt" signed as "root" with headers and body "x"
      | Content-Type | text/plain |
    Then the response status is 403
    And the error code is "AccessDenied"
    And the response body contains "<Message>Access Denied</Message>"
    And the served root holds no file "data/b.txt"
    When I send a "GET" request to "/data/a.txt" signed as "root"
    Then the response status is 200
    And the object body is "a confined payload"
    When I send a "GET" request to "/data?list-type=2" signed as "root"
    Then the response status is 200
    And the response body contains "<Key>a.txt</Key>"
    And the run was confined

  # G10: the two artifacts a *serving* run owes agree — the readiness frame on stdout and the
  # `state` file every client reads — and the file names the process this scenario spawned.
  #
  # **Which process that is differs by arm and the assertion does not**: an in-place backend
  # confines the process this harness spawned, so `state` names the server itself; a launcher
  # publishes the file on the child's behalf, so `state` names the launcher — the same pid on
  # both, and `steps/cli.rs`'s `state_names_the_spawned_process` is where the difference and
  # Linux's extra half (a spawned process with no descendant) are spelled.
  #
  # **The directory the file lands in is the run's own, and it is deliberately not the
  # harness's `TINIO_STATE_DIR`.** FR-023 relocates the state directory for a **read-only**
  # run, so `<scratch>/state` — what every child here is handed — is that mode's first
  # candidate; this run is read-write, and `resolve::resolve_state_dir` answers `<root>/.tinio`
  # whatever the variable says. The pid step resolves the run's own directory
  # (`Cli::state_dir_of`) rather than assuming, which is the same rule `state_file` follows for
  # the read-write path; a read-only run's file is a different scenario's subject.
  @cli @confinement @FR-036
  Scenario: Readiness and `state` agree, and `state` names the spawned process
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And the root publishes a state file only when the run is serving
    And the state file names the spawned process

  # G12: the `state` publish is the one fallible step left on a launcher's launch path, and its
  # failure is owned rather than ignored — the launcher ends the child it just launched and exits
  # non-zero, instead of leaving a confined server serving with no file naming it (`Child::abort`,
  # `crates/tinio-cli/src/sandbox/confine.rs`).
  #
  # **The induction is a directory planted at `<state dir>/state.tmp`**, the path `State::write`
  # clears with a tolerant remove that tolerates `NotFound` alone: `remove_file` cannot take a
  # directory (linux `EISDIR`, Windows `ERROR_ACCESS_DENIED`), so the removal fails, the publish
  # fails with it, and the plant outlives the run — **sticky by construction**, which is why the
  # scenario's teardown is what removes it (`steps/cli.rs`'s `remove_sticky_state_tmp`, called on
  # every path out of a scenario). It is **probe-immune**: the pre-flight probe only ever creates
  # and removes its own `state-probe-<pid>-<nanos>` entry, so the refusal this induction earns is
  # the publish's and never the probe's.
  #
  # **The state dir is the read-write one** (`<root>/.tinio`), which is where a launcher publishes
  # and where that teardown helper reaches — and **the launcher platforms are the whole of it**:
  # Linux confines in place and publishes from the serving process itself
  # (`commands/start/serve.rs`'s `plane.publish`, reached when no launcher adopted the run), a
  # different call site with no child to end.
  #
  # **The run reports readiness before it fails** — the child is listening by the time the
  # launcher publishes — so the scenario waits for the ending rather than reading it off the
  # capture, and the evidence is the pair that makes this the *child* dying rather than the
  # launcher merely leaving: the run ends non-zero, and nothing accepts on the endpoint it
  # reported, which a live confined child would still hold (it owns the listener the launcher
  # handed it).
  @cli @confinement @FR-036 @macos @windows
  Scenario: A failed `state` publish ends the child
    Given a storage root
    And the root's state dir holds a "state.tmp" directory
    When I run "tinio server {root} --port 0 --sandbox"
    And the run ends
    Then the run fails
    And the root publishes no state file
    And the endpoint the run reported stops accepting

  # G13: an HTTP control channel is not the local one, so **a launcher hands nothing over for
  # it** — `attach_control` returns before the probe and before the bind when the selection's
  # local form is absent, and `publish_state` is reached exactly when a handoff exists. `state`
  # is the record of the channel a launcher bound; with none bound there is no record to write,
  # and nothing refuses the run either: the pre-flight `refuse_unwritable_state_dir` rides the
  # same handoff.
  #
  # **"Serves" is half of the assertion and it is not free**: the confined child binds that TCP
  # channel itself, inside the jail. Windows' container permits it — a loopback bind needs no
  # capability there, the measurement `crates/tinio-sandbox-windows/src/socket.rs` records while
  # initialising Winsock — while macOS's profile grants no network operation at all, so a
  # confined child there is refused every `bind()` (measured on macOS 15.8; the row is
  # `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5's, and
  # `crates/tinio-sandbox-macos/src/profile.rs` is where the absence is asserted). **Linux is the in-place
  # arm and inverts the file's half**: it serves, and the plane publishes its own `state` with
  # the TCP channel as `control_name` (measured in WSL2, 2026-09-30) — the launcher's absence of
  # a record is a launcher fact, which is why this scenario is Windows'.
  @cli @confinement @FR-036 @windows
  Scenario: A run with an HTTP control channel publishes no `state` and is not probed
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox --api http://127.0.0.1:0"
    Then the readiness line reports a bound port
    And the root publishes no state file

  # G14: FR-036's ruling on the one trade the confinement does not refuse — a configured
  # `[telemetry] otlp_endpoint` and a confinement cannot both be honoured, so the combination is
  # **warned about, naming both**, and the run proceeds (design decision 25, `contracts/cli.md`).
  # The confinement cuts the confined process's outbound path and the exporter is the only one
  # this product has, so what is lost is the export; losing an observer is not grounds for
  # refusing to serve.
  #
  # **The warning is the rule's one reader** (`sandbox::lost_export` answers the channel and the
  # endpoint, `commands/start/run.rs` emits it where the log is live), and the assertions are its
  # facts: the sentence that names the trade, the channel that asked for the confinement and the
  # endpoint the confinement costs. The endpoint asserted is the fixture's own spelling, and the
  # run's readiness beside it is the "still serves, nothing refused" half — a refused run would
  # have exited with a refusal sentence rather than serving.
  #
  # The configuration is planted as the document the run reads (`<state dir>/.tinio.toml`, the
  # same file the config channel's scenarios write), and the telemetry section parses in every
  # build: the `otel` cargo feature decides whether an exporter exists, not whether the key is
  # understood, which is what lets a shipped default build assert the warning.
  @cli @confinement @FR-036
  Scenario: OTLP plus a confinement warns and serves
    Given a storage root
    And the root's state dir holds a ".tinio.toml" file
      """
      version = 1
      [telemetry]
      otlp_endpoint = "http://127.0.0.1:4317"
      """
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And the root publishes a state file only when the run is serving
    And its stderr contains "the confinement cuts the outbound path only the OTLP exporter uses"
    And its stderr contains "channel=`--sandbox`"
    And its stderr contains "endpoint=http://127.0.0.1:4317"

  # G15: the exposure set is resolved and refused through the product — a path a run is handed
  # that cannot be served is refused, and the operator reads why instead of driving a server
  # that never came up. **Two of the four are `resolve_root`'s usage refusals**
  # (`crates/tinio-cli/src/resolve/locate.rs`), and they are the same answer whether or not a
  # confinement was asked for: a DIR that is not there, and no DIR at all — the last being the
  # exposure set the plan calls *empty* (`tinio server --sandbox` names no path at all), since
  # the walk-up from the child's own working directory finds no reserved `.tinio` above the
  # scratch tree and the resolution has nothing to hand the confinement.
  #
  # **The file row is a read-only run, and that is the mode that makes it reachable.** A
  # read-write run resolves `<file>/.tinio` before anything is exposed, so the configuration
  # layer's own error is what an operator sees there (measured on this host and in WSL2,
  # 2026-09-30); with the state directory elsewhere the run reaches the layer that would have to
  # *expose* the path, and that layer refuses it. **The refusal's words differ by platform** —
  # Windows' launcher refuses it in the whitelist
  # (`crates/tinio-sandbox-windows/src/backend/exposures/whitelist.rs`, `NOT_A_DIRECTORY`: "is
  # not a directory, and this launcher grants the caller's paths as directories …") while the
  # shared storage open answers "storage root is not a directory: …" — so the row pins the
  # fragment both sentences carry rather than either sentence whole.
  @cli @confinement @FR-036
  Scenario: A served path that is a file is refused
    Given a storage root
    And the root holds a file "a.txt"
    When I run "tinio server {root}/a.txt --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "is not a directory"
    And the run never reported readiness

  # The link row, and **the state directory is the channel it needs**: a junction above the
  # served root, or a junction the root itself is, is resolved by `resolve_root` before any arm
  # sees it (measured on this host 2026-09-30 — such a run serves), while `TINIO_STATE_DIR`
  # reaches the exposure set as the operator spelled it
  # (`crates/tinio-cli/src/sandbox/product.rs`'s `exposures`). The launcher then refuses to
  # grant through it (`crates/tinio-sandbox-windows/src/backend/exposures/reparse.rs`, whose
  # rule is `is a reparse point …`), which is the half a grant written on the link's *target*
  # cannot give back. Read-only for the reason the file row is: a read-write run resolves
  # `<root>/.tinio` whatever the variable says.
  @cli @confinement @FR-036 @windows
  Scenario: A junction in the exposure chain is refused
    Given a storage root
    And the child's state directory is a junction
    When I run "tinio server {root} --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "cannot be exposed: is a reparse point"
    And the run never reported readiness

  # The two refusals that need no fixture: the DIR the run names is not there, and the run names
  # none. Both are exit 2 — `resolve_root`'s own error class — and neither leaves a run behind,
  # which is what `the run never reported readiness` states beside the exit status.
  #
  # **The "never served" half is the readiness frame and not a `state` file here**, and for these
  # two rows that is the only form it can take: neither run gets far enough to *have* a state
  # directory (one names a path that does not exist, the other has no root at all), so a path
  # assertion would name a file no serving run would write either. The step's own doc carries the
  # rule; every refusal row in this block uses it for that reason.
  @cli @confinement @FR-036
  Scenario Outline: A run with no usable root is refused
    Given a storage root
    When I run "<command>"
    Then the run fails with exit code 2
    And its stderr contains "<message>"
    And the run never reported readiness

    Examples:
      | command                                     | message |
      | tinio server {root}/nope --port 0 --sandbox | is not usable |
      | tinio server --port 0 --sandbox             | no storage root: pass DIR |

  # G16: the state-dir containment refusals reach the operator. **Both directions of one rule**
  # (`crates/tinio-cli/src/sandbox/product.rs`'s `refuse_in_root_state_dir`), which is why they
  # are one outline: a state directory that *holds* the served root, whose read-write exposure
  # entry would cover the root's siblings too, and one that is *inside* it, which only a
  # read-only run refuses (a read-write run's state directory **is** `<root>/.tinio` and has to
  # be served). Read-only is the mode both rows run in, and it is also the only mode either
  # clause is reachable in: a read-write run resolves `<root>/.tinio` whatever `TINIO_STATE_DIR`
  # says (`crates/tinio-cli/src/resolve/locate.rs`'s `resolve_state_dir`), so the mode the first
  # sentence names can never be `read-write` through the product's surface — measured on this
  # host and in WSL2, 2026-09-30, where such a run serves.
  #
  # **The in-root row's sentence is the configuration layer's, and that is the arrival an
  # operator gets**: the run writes `<state dir>/.tinio.toml` before the confinement is asked
  # for, and on a root that does not hold the file yet, that write is refused first
  # (`crates/tinio-cli/src/config/discover.rs`'s `load_or_create`, over the same containment rule).
  # The sandbox's own clause is what lands on a root that already holds the file — a second
  # arrival, not this row.
  #
  # **And every row here pins stdout, not only stderr** (`crates/tinio-cli/src/run.rs`'s serving arm frames the `Err` that `start::run` returns — decision 19's "a startup failure is an event rather than an absence"), so a refusal is one frame carrying the sentence and three lines and nothing else: the assertion the retired `crates/tinio-server/tests/serve_sandbox_channels.rs`'s in-process helper kept and this layer's migration of case 11 dropped — `steps/cli.rs`'s `refusal_frame_only` is that shape's one home now, shared with the channel dispatch.
  @cli @confinement @FR-036
  Scenario Outline: A state directory nested with the served root is refused
    Given a storage root
    And the child's state directory <nesting>
    When I run "tinio server {root} --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "<message>"
    And its stdout is the refusal frame and nothing else, carrying "<message>"
    And the run never reported readiness

    Examples:
      | nesting                   | message |
      | holds the served root     | read-only mode cannot serve a root the state directory holds |
      | is inside the served root | read-only mode never writes under the served root |

  # The outline's **spelling variant**, and the one Windows alone has: one directory, two
  # spellings. On NTFS `<scratch>\root` and `<scratch>\ROOT` resolve to the same tree while a
  # literal component comparison calls them unrelated — the fold that once let a state directory
  # spelled in the other case past both guards and put exactly the read-write entry the row above
  # refuses inside a root the operator asked to expose read-only
  # (`tinio_util::path::contains_under_any_spelling` is the shared rule both layers now call).
  #
  # **The sentence is the outline's in-root one**, because the configuration layer answers first on
  # a root that holds no configuration file yet — the same layering the outline's comment states.
  # **`@windows` because the fold is**: on a case-sensitive volume the two spellings are two
  # directories, the state directory is *outside* the served root, and the run this scenario
  # expects to be refused would serve instead — a red leg for the filesystem's reason rather than
  # for the product's, which is why the row is not in the outline above (a tag binds a scenario).
  @cli @confinement @FR-036 @windows
  Scenario: A state directory spelled in the root's other case is refused
    Given a storage root
    And the child's state directory is inside the served root under its other case
    When I run "tinio server {root} --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "read-only mode never writes under the served root"
    And its stdout is the refusal frame and nothing else, carrying "read-only mode never writes under the served root"
    And the run never reported readiness

  # The two state-dir refusals only macOS reaches, written so the platform leg runs them rather
  # than a later reader inventing them. **Derived, not named**:
  # `crates/tinio-cli/src/sandbox/product.rs`'s `refuse_underived_state_dir` is reached when the
  # resolution had to derive `<home>/.tinio/roots/<hash>/` (`StateDirSource::Derived`), because
  # the confinement exposes the state directory itself and grants nothing for its parent. The
  # platform fact is `tinio_sandbox::platform_facts`'s at the call site, and the guard beside it
  # is `probe_state_dir_writable`'s — macOS's alone, both.
  @cli @confinement @FR-036 @macos
  Scenario: A read-only run that must derive its state directory is refused
    Given a storage root
    And the child names no state directory
    When I run "tinio server {root} --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "with the state directory derived from the home directory"
    And its stdout is the refusal frame and nothing else, carrying "with the state directory derived from the home directory"
    And the run never reported readiness

  # The write probe's own refusal (`refuse_unwritable_state_dir`), which stands in front of the
  # `state` publish rather than discovering the failure with a confined child already serving.
  # The fixture makes the directory unwritable **and** leaves the two files the launcher would
  # otherwise create in it — its configuration file and the access log the observer's sink opens
  # (`install_subscriber` runs before the probe, so an empty directory refuses the run at the log
  # sink; measured on macOS 15.8, 2026-09-30). What is left refusing is the probe's own
  # `create_new`, which is the sentence this row asserts; the fixture step's doc carries the
  # measurement.

  @cli @confinement @FR-036 @macos
  Scenario: A state directory that cannot take the write probe is refused
    Given a storage root
    And the child's state directory refuses new entries
    When I run "tinio server {root} --port 0 --read-only --sandbox"
    Then the run fails with exit code 1
    And its stderr contains "cannot take a new entry"
    And its stdout is the refusal frame and nothing else, carrying "cannot take a new entry"
    And the run never reported readiness

  # G17: **the Windows run's operator-visible contract**, as the plan's §3b row split into one
  # scenario per fact. The row bundles seven things an operator greps a launcher's log for, and
  # each is one behaviour; every line asserted here is the launcher's own `tracing` output on the
  # stderr the confined child inherits (`crates/tinio-cli/src/sandbox/confine.rs` installs the
  # subscriber before the launch, so the whole record is one stream), read through
  # `its stderr carries a "…" step line naming`, whose docstring is the selector — a bare field
  # name asks for the field, `field=value` pins it. Nothing matches a prefix of the line: the
  # timestamp, the level, the target and the field order belong to whichever subscriber the
  # process installed (`sandbox-shared.md` §7, and `steps/cli.rs`'s `step_line` doc).
  #
  # **Two of the seven need an ending, and that is what shapes their scenarios.** A serving run has
  # no `child exit` and no profile deletion yet, and the harness captures the *last* run's streams,
  # so a run stopped through `tinio stop` would have those lines land in the stopped run's capture,
  # which no step can read afterwards. Both therefore run a second confined server on a root an
  # earlier server already serves: the confined child refuses the storage the first run holds open
  # (measured on this host 2026-09-30 — `database error: Database already open. Cannot acquire
  # lock.`), so the run ends inside the capture with its whole record. The first server is
  # unconfined and takes `--api http://127.0.0.1:0` (G13's HTTP-only control channel) so the
  # confined run can still bind the root's local channel; for the profile fact it is a *confined*
  # first run, because a profile has to exist for the second one to take over.

  # **Fact 1 — the grant banner**: the line the launcher writes *before* it grants anything, naming
  # the profile whose SID it is about to grant to and the paths it is about to grant
  # (`crates/tinio-sandbox-windows/src/backend/mod.rs`'s `grant_whitelist`). The count is the
  # whitelist's own list, so it is the exposure set's size — a read-write run's is the served root
  # and the program directory, 2 on every host, and the harness's `TINIO_STATE_DIR` does not add to
  # it because a read-write run resolves its state directory in-root (measured 2026-09-30).
  @cli @confinement @FR-036 @windows
  Scenario: The launcher reports the ACL grant it is about to make
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And its stderr carries a "grant the ACLs" step line naming
      """
      sid
      exposures=2
      granting
      """

  # **Fact 2 — the stale-profile sweep's summary** (`crates/tinio-sandbox-windows/src/sweep.rs`):
  # whether the machine's residue was reaped, and, when it was not, why. **The shape is the skipped
  # one on purpose**, because a sweep that ran carries no `skipped=` field at all — the field is
  # `None` when there is nothing to report, and `None` records nothing (measured: `reaped=0
  # locked=0 failed=0 other_tinio_processes=0` and no fifth field, 2026-09-30). The sweep is gated
  # on the process table, so a run that starts while another `tinio` serves reaps nothing and says
  # so; the counters pinned to zero are that same fact (it did nothing), and
  # `other_tinio_processes` is presence-only because the count is the machine's rather than the
  # scenario's — the serving run's confined child counts too (measured: 1 with the first server
  # alone, 2 once its launcher has a confined child of its own). **The reason is
  # asserted whole beside the field**, because a quoted value cannot be a `field=value` requirement:
  # the step compares the unquoted value the log holds against the requirement's own text.
  @cli @confinement @FR-036 @windows
  Scenario: The sweep's summary reports what it reaped and why it did not
    Given a storage root
    When I run "tinio server {root} --port 0 --api http://127.0.0.1:0"
    Then the readiness line reports a bound port
    When I run "tinio server {root} --port 0 --sandbox"
    Then its stderr carries a "sweep the stale profiles" step line naming
      """
      reaped=0
      locked=0
      failed=0
      other_tinio_processes
      skipped
      """
    And its stderr contains "another tinio process is alive"

  # **Fact 3 — how the child ended** (`crates/tinio-sandbox-windows/src/process.rs`'s
  # `step="child exit"`): the pid the launcher spawned and the code it left. The ending is the
  # storage refusal the block above argues, and what the launcher reports is the *child's* code —
  # the status it forwards beside this line is P2's fact.
  @cli @confinement @FR-036 @windows
  Scenario: The launcher reports how its child ended
    Given a storage root
    When I run "tinio server {root} --port 0 --api http://127.0.0.1:0"
    Then the readiness line reports a bound port
    When I run "tinio server {root} --port 0 --sandbox"
    Then its stderr carries a "child exit" step line naming
      """
      pid
      code=1
      """

  # **Fact 4 — the profile's life as one run reports it**: `CreateAppContainerProfile`'s
  # `taken_over` and `DeleteAppContainerProfile`'s `absent`
  # (`crates/tinio-sandbox-windows/src/sid.rs`), which together answer "did this run find a profile
  # for its root, and did it leave one". **The induction is the residue rule read from the other
  # side**: the second run finds the first run's profile (one root derives one name) and takes it
  # over rather than creating one, and the sweep does not reap it first because the first run is
  # alive — the gate `other_tinio_processes` exists for. A *killed* first run would leave the same
  # profile behind and the next run would reap it before deriving its name, so `taken_over` would
  # read `false`; the fresh-root arm (nothing to take over) is
  # `crates/tinio-server/tests/sandbox_e2e_windows.rs`'s.
  @cli @confinement @FR-036 @windows
  Scenario: A run reports the profile it took over and the one it left absent
    Given a storage root
    When I run "tinio server {root} --port 0 --api http://127.0.0.1:0 --sandbox"
    Then the readiness line reports a bound port
    When I run "tinio server {root} --port 0 --sandbox"
    Then its stderr carries a "CreateAppContainerProfile" step line naming
      """
      name
      sid
      taken_over=true
      """
    And its stderr carries a "DeleteAppContainerProfile" step line naming
      """
      name
      sid
      absent=true
      """

  # **Fact 5 — the salt degrade, and the run continuing anyway**: the per-user salt could not be
  # used, so this run derives the *unsalted* package name for its root and says what that exposes
  # (`crates/tinio-sandbox-windows/src/sid.rs`'s `degrade`; design §6). **The induction is the state
  # directory's create**: `LOCALAPPDATA` is pointed at a file under the served root, so
  # `<LOCALAPPDATA>\tinio` cannot be created and the salt's load fails with the file system's own
  # error. The path is *relative*, which every child resolves against its working directory — the
  # scenario's scratch dir — so no scenario has to spell a temp path. The two fields are
  # presence-only and deliberately so: the `error=` text is **localized**, so the host renders the
  # sentence in its own locale and only the appended ` (os error 183)` — the code's own spelling,
  # carried by every locale — is stable; this host's create failed with `os error 183`. The second
  # half of the fact — that the run still serves — is the readiness line asserted beside it.
  @cli @confinement @FR-036 @windows
  Scenario: A run without a usable salt warns and still serves
    Given a storage root
    And the root holds a file "not-a-directory"
    And the child's environment holds "LOCALAPPDATA" = "root/not-a-directory"
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And its stderr carries a "load the ACL salt" step line naming
      """
      path
      error
      """

  # **Fact 6 — the grant phase reports what it cost** (`backend/grant.rs`'s `report_grant_phase`):
  # `elapsed_ms` and the exposure count — at `debug!` for every run, and at `warn!` with
  # `threshold_ms` beside them once the phase has spent 15 s of the 30 s readiness budget.
  # **The warning's own arm is not induced, and what is asserted is the half that is reachable**:
  # 15 s of granting needs a corpus of roughly 90,000 objects under an exposed directory (design
  # §6 measures 0.16 ms/object on NTFS), which no scenario can plant cheaply, so the line asserted
  # is the one the same function writes for every run — selected *by its fields*, which is what
  # makes the warning reachable at all if it ever fires (it carries both of these, plus
  # `threshold_ms`). `TINIO_LOG_LEVEL=debug` is the harness's channel for it.
  @cli @confinement @FR-036 @windows
  Scenario: The grant phase reports what it cost
    Given a storage root
    And the child's environment holds "TINIO_LOG_LEVEL" = "debug"
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And its stderr carries a "grant the ACLs" step line naming
      """
      elapsed_ms
      exposures=2
      """

  # G18: **the capability set is chosen through the environment** — `TINIO_SANDBOX_CAPABILITIES`,
  # the one security-relevant parser on this operator's surface
  # (`crates/tinio-sandbox/src/serve.rs`'s Windows arm, over `backend::Capability::parse_list`),
  # unasserted at every level before this. A value is a comma-separated list of the three names a
  # container can hold (`internetClient`, `internetClientServer`, `privateNetworkClientServer`);
  # anything else is an `unknown AppContainer capability` and ends the run. **The two cases are two
  # scenarios rather than one outline's rows, because they do not share a shape**: the valid one
  # serves and the unknown one is refused, and the file's other outlines' rows differ in
  # parameters alone (the same wall B4's rows hit).
  #
  # **What the honoured arm can and cannot show at this surface.** The capability being *honoured*
  # — an outbound connection it opens, or the absence of one — is the network leg's
  # (`crates/tinio-sandbox-windows/tests/network.rs`, where the same host reaches an address with
  # `internetClient` and not with an empty list). What a CLI run shows is that the environment's
  # value reaches the container's construction without a refusal, and that a real confinement
  # happened beside it — which is what the witness is asked for here.
  @cli @confinement @FR-036 @windows
  Scenario: A valid capability name is honoured
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX_CAPABILITIES" = "internetClient"
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And the run was confined

  # **The refused arm, and the value is a list with one good name in it on purpose**: the parse
  # refuses the whole value rather than silently narrowing the container's set to the entries it
  # recognised — which is the security fact this row exists for. The sentence names the three
  # accepted names and never the offending entry (the parser has no per-entry text), so the
  # assertion pins the parser's own words; **the exit code is the platform selectors' own usage
  # class** — a value outside the accepted set is one operator mistake wherever it is typed
  # (`Error::Usage { detail }`, `crates/tinio-sandbox-core/src/error.rs`) — and the absent `state`
  # file is the "refused, never served" half.
  @cli @confinement @FR-036 @windows
  Scenario: An unknown capability name fails the run
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX_CAPABILITIES" = "internetClient, bogus"
    When I run "tinio server {root} --port 0 --sandbox"
    Then the run fails with exit code 2
    And its stderr contains "unknown AppContainer capability (expected internetClient, internetClientServer or privateNetworkClientServer)"
    And the root publishes no state file

  # P2 (the plan's §3c): **the launcher answers with the status its child left, not with one of
  # its own** — on a clean exit the launcher's exit code *is* the child's. **The macOS arm of the
  # assertion is this scenario's alone now**: §5's migration (`plans/2026-09-29-sandbox-bdd-e2e.md`)
  # retired the `Some(0)` assertion it used to strengthen in
  # `crates/tinio-server/tests/sandbox_macos_e2e.rs`, whose leg now waits for the launcher to end
  # and asserts no status at all, while `sandbox_e2e_windows.rs`'s stop leg asserts only as "some
  # code exists"; the value a launcher
  # has *instead* is **1**, the fallback `crates/tinio-cli/src/commands/start/run.rs`'s
  # `launcher_exit` keeps "only where there is no code to forward at all — a child killed by a
  # signal". So a run read as a 0 is a status the launcher carried out of its wait; a launcher
  # that had none answers 1 and fails here.
  #
  # **The two launcher platforms, and no third host** (the plan's §3 platform rule: a scenario
  # with positive platform tags runs where one of them matches). Linux is not one of them —
  # decision 6 confines it **in place**, so there is no second process whose status could be
  # forwarded; that shape is the in-place arm's, and G10's pid step is where it is asserted
  # (`steps/cli.rs`'s `linux_has_no_descendants`, the plan's §3c P1 — collapsed into G10 rather
  # than repeated here).
  #
  # **The ending has to be a drain, and the stop is the one available.** A kill is the killer's
  # ending rather than the child's: on macOS it leaves the child no code at all — exactly the
  # input the fallback answers for — and on Windows it is whatever code the killer passed
  # (`kill_and_reap`'s `Child::kill`), so neither is a status the child chose. The run is ended
  # through the control channel a client reads `state` for: `tinio stop <root>`
  # (`contracts/cli.md`), the client the harness's own step runs and requires to succeed. The
  # step's doc carries why that client is spawned outside `I run`: a client captured as this
  # scenario's run would make the assertions after it read the *client's* own 0, which answers
  # whether or not the launcher forwarded anything.
  #
  # **The child's own code is asserted beside this by the launcher's log, on each platform
  # separately and never here**: the record of the ending is a `step=` line whose name and fields
  # differ (Windows' `child exit` with `code=`, macOS's `exit` with `status=`), so one shared
  # scenario cannot name both. G17's fact 3 owns the Windows record (`The launcher reports how
  # its child ended`) and `crates/tinio-server/tests/sandbox_macos_e2e.rs`'s own leg the macOS
  # one.
  @cli @confinement @FR-036 @macos @windows
  Scenario: The launcher forwards its child's status
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    When I stop the server through its control channel
    And the run ends
    Then the run succeeds

  # P4 (the plan's §3c): **the launcher's death takes the confined child.** The child is spawned
  # inside a job object the launcher holds with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` (design
  # decision 20, first half — `crates/tinio-sandbox-windows/src/job.rs`), the kernel closes that
  # handle however the launcher dies, and the last handle going away terminates everything still in
  # the job — so the launcher's death is the one thing that ends a confined server, and where an
  # operator sees it is the port the run reported.
  #
  # **This is the launcher-death half of the leg the plan split**
  # (`crates/tinio-server/tests/sandbox_e2e_windows.rs`'s
  # `a_confined_server_holds_one_ace_while_it_runs_and_after_the_launcher_is_killed`), and the
  # **ACE half stays in Rust**: §5's verdict is that a step layer has no package-SID oracle except
  # the log, so `ace_count(&icacls(&root), &sid) == 1` has no home here. What this scenario asserts
  # is the disappearance of the service, never the ACL.
  #
  # **`I kill the server` is the launcher's death, and on this arm that is not a figure of
  # speech**: the process this scenario spawns *is* the launcher, so the kill reaches it and the
  # child it holds goes with the job. The launcher's own exit is observed by that step's wait
  # (`kill_and_reap`'s `Child::kill` then `Child::wait`) and asserted nowhere, because its code is
  # whatever the killer passed and never a status the launcher chose — the distinction P2's
  # scenario is built on, and the reason this one reads the *child's* disappearance rather than the
  # parent's status.
  #
  # **The readiness line beside it is the positive control**: the endpoint was reported and served
  # on, so the refusal below is a listener that went away and not one that never existed. The probe
  # the last step runs accepts a refusal alone — a connect that failed any other way is retried
  # inside the window rather than read as gone (`steps/cli.rs`'s `endpoint_stops_accepting`, whose
  # doc is where that rule and the measurement behind its two clocks live).
  #
  # **Windows is the one host this holds on, and the other two are why.** macOS launches with no
  # job object — a killed launcher leaves its confined child serving, decision 20's accepted gap,
  # which is why this harness sweeps that grandchild by name there (`steps/common.rs`'s
  # `sweep_confined_grandchild`) — while Linux confines in place (decision 6), so the process a
  # kill ends *is* the server and there is no launcher whose death could take a child.
  @cli @confinement @FR-036 @windows
  Scenario: The launcher's death takes the confined child
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    When I kill the server
    Then the endpoint the run reported stops accepting

  # P5 (the plan's §3c): **a bucket inside the served root that is a junction out of it is
  # invisible to the container** — the S3 surface answers `NoSuchBucket` for it, and that answer is
  # what this scenario reads. It is the CLI-observable half of the leg the plan migrates and then
  # deletes (`crates/tinio-server/tests/sandbox_e2e_windows.rs`'s
  # `a_confined_server_refuses_an_escaping_bucket_junction_as_nonexistent`), and the suite's only
  # ask that the containment proof keep resolving reparse points *inside* the jail.
  #
  # **The code is the assertion and the status is its consequence.** `ensure_bucket` asks the
  # storage's `bucket_dir` — the containment proof — first, and what turns a refusal there into
  # `NoSuchBucket` is its own reparse fallback: the bucket path is read with `symlink_metadata`,
  # the reparse point is seen, and *any* `bucket_dir` error is rewritten while `follow_symlinks` is
  # off (`crates/tinio-fs/src/backend/objects.rs`). So the 404 is `s3s`'s mapping of that code, and
  # a bare status assertion would not discriminate: the world where the proof accepted the bucket
  # reaches the object-level lstat policy instead (`check_symlinks`, same file), which refuses the
  # link it walks through with `AccessDenied` — 403, not 404. Both worlds were measured on this
  # host by the leg being migrated; the listing of such a bucket is a third answer (`InternalError`
  # in both worlds) and is deliberately not a probe here.
  #
  # **The fixture's premise is the out-of-root grant, and it is why the SID comes from the launcher
  # and never from this side.** The resolution adopts a link's target only where the target or its
  # parent exists, and `exists()` swallows the refusal — so against a target the container cannot
  # reach, the proof answers lexically in either world and this scenario would assert nothing.
  # `And the launcher's package SID is granted to "outside"` is applied *between* the launch and
  # the requests, because the confined child resolves the path per request; the step is task A3's,
  # the mask is `acl::grant_spec`'s, and the SID is the launcher's own `grant the ACLs` event —
  # never re-derived, since a `PackageSid` built in the test process would create the very profile
  # the launcher is using (`crates/tinio-server/tests/support/windows_e2e_harness.rs` is where that
  # trap is recorded).
  #
  # **The control read is what keeps the refusal from being a server that failed to start**, and
  # its body is asserted beside its status so the bytes come from *this* tree — the migrated leg's
  # own argument, over its own `plain` bucket.
  #
  # **`And the run was confined` is what makes this a claim about a *confined* run.** The reparse
  # fallback is not the container's: the same tree served unconfined answers the same code, so
  # without the witness this scenario would pass on a server with no jail at all. The witness is
  # this platform's two-carrier pair (`steps/cli.rs`'s `windows_witness`): the launcher's
  # `grant the ACLs` record, and the package SID's ACE read back off the served root by `icacls`.
  @cli @confinement @FR-036 @windows
  Scenario: An escaping junction inside the root is refused as nonexistent
    Given a storage root
    And the child holds the harness's credentials
    And the root holds bucket "plain" with object "plain.txt" and body "plain"
    And the root holds an escaping junction "leak" to "outside"
    When I run "tinio server {root} --port 0 --sandbox"
    And the launcher's package SID is granted to "outside"
    And the client is bound to the run's endpoint
    When I send a "GET" request to "/plain/plain.txt" signed as "root"
    Then the response status is 200
    And the object body is "plain"
    When I send a "GET" request to "/leak/outside.txt" signed as "root"
    Then the response status is 404
    And the error code is "NoSuchBucket"
    And the run was confined

  # P3 (the plan's §3c): **a confined child that dies after the launcher spawned it and before it
  # reported readiness becomes the launcher's own `error` frame** — one frame, the parent's own
  # sentence, with the child's exit status in it — and the run exits with the child's code rather
  # than with a fallback of the launcher's. This is `design §12` bullet 4's leg, and the one the
  # plan records as having no leg on macOS: Windows has its own in Rust
  # (`crates/tinio-server/tests/sandbox_e2e_windows.rs`'s
  # `a_child_that_never_reports_is_the_parents_own_error_frame`, induced by a config file the
  # container cannot read — re-induced 2026-10-07, when the directory junction it used to plant
  # stopped refusing the child: the launcher resolves the root before it builds the exposure set,
  # so the junction's target is what is granted and the child resolves to a granted path and
  # serves), which §5 keeps where it is because that induction is Windows-only. What macOS's arm
  # asserts
  # is `crates/tinio-cli/src/commands/start/run.rs`'s `launcher_exit`: the one frame this process
  # emits on its own behalf, and the child status it forwards beside it.
  #
  # **The induction: `<root>/.tinio/.env` is a symlink to an existing, parseable dotenv file
  # outside the served root.** Two writes and no race. The state directory is lexically in-root
  # for a read-write run (`resolve::resolve_state_dir` returns `<root>/.tinio` whatever
  # `TINIO_STATE_DIR` says), so the rendered profile never names it — and the path the child
  # reaches *through* the link resolves to a file no rule names, because Seatbelt compares a
  # rule's spelling against the kernel-resolved path, so the exposure's `(allow file-read* …)`
  # does not cover it. Both processes load that file (`crates/tinio-cli/src/run.rs`'s
  # `command_line` is the launcher's path and the confined child's alike): the unconfined
  # launcher reads it happily a moment earlier, and the child cannot — so it dies **with no
  # frame at all**, which is exactly the shape this leg needs.
  #
  # **Two fixtures that do *not* work, so a future reader does not reach for them**: a
  # **symlinked root** (the launcher adds every exposure's canonical spelling to the profile, so
  # both spellings are granted) and a **read-only** run (the state directory relocates out of
  # the root and the premise changes).
  #
  # **The child's exit code alone is not evidence** — `1` is also the launcher's fallback for a
  # child killed by a signal (`launcher_exit`'s `status.code()` arm) — so the frame and its
  # sentence carry the claim, and **three witnesses say a child ran at all**: the launcher's
  # `step="spawn"` line with a pid, its `step="exit"` line, and the child's own
  # `tinio: configuration error:` line, which reaches the operator unmediated because decision 19
  # makes the child's stderr the log channel. **Not the errno**: the child's refusal is measured
  # as `Operation not permitted (os error 1)` on the guest (macOS 15.8, 2026-09-30) while the
  # plan's fact-hunt measured `PermissionDenied`/`NotFound` on other builds, so the sentence is
  # the assertion and the error kind is not.
  #
  # **Both mechanisms hold, and the leg asserts one frame under either — so the mechanism is the
  # scenario's own axis and not the environment's.** The two rows are the two values of
  # `TINIO_SANDBOX_MACOS_MECHANISM` (the default and the alternative, decision 6), set through
  # the harness's variable channel because `Cli::command` strips every `TINIO_*` the *runner*
  # exports: the leg is a claim about the launcher/child pair, and which of the two ways the pair
  # was confined must not change it. `sandbox-exec` refuses the read inside the helper's profile,
  # while `sandbox-init`'s child applies the text to itself before the read (`crate::run` calls
  # `sandbox::apply_self_confinement` before the parse, decision 26) — so the same read is
  # refused either way, the child is spawned first either way, and only the mechanism's own check
  # line differs (`step="preflight"` versus `step="sandbox-init validator"`, which belongs to the
  # mechanism-selection rows above and is deliberately not asserted here).
  @cli @confinement @FR-036 @macos
  Scenario Outline: A child that dies before readiness becomes the launcher's error frame
    Given a storage root
    And the child's environment holds "TINIO_SANDBOX_MACOS_MECHANISM" = "<mechanism>"
    And the root's state dir holds a ".env" link to a file outside the root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the run fails with exit code 1
    And its stdout is exactly one error frame saying "the confined child exited (exit status: 1) without reporting readiness"
    And its stderr carries a "spawn" step line naming
      """
      pid
      """
    And its stderr carries a "exit" step line naming
      """
      status
      """
    And its stderr contains "tinio: configuration error: failed to load environment file"

    Examples:
      | mechanism    |
      | sandbox-exec |
      | sandbox-init |

  # C6 (the plan's §4 A3, and the clause it left unmet): the metric-scrape pair — `I read the
  # metrics from the control channel` and `the server's metrics carry` — was registered with no
  # scenario that used it, so the families A5 declared had no leg. This is that leg, and it reads
  # the *product's own* exposition rather than any test-side copy of it.
  #
  # **The route is the control channel, never the S3 port**: `/metrics` is the management plane's
  # (`crates/tinio-api/src/router.rs`), and the endpoint is the `control_name` the run's own `state`
  # file records — resolved by the scrape step rather than re-derived, the rule `tinio status`
  # follows. The families are declared by the crate that observes the fact
  # (`crates/tinio-sandbox-linux/src/metrics.rs`, whose module doc argues the dependency direction
  # and the process-global registry), and on Linux the observing process **is** the serving one
  # (design decision 6) — which is the `@linux` gate: a launcher arm builds no runtime and has no
  # HTTP surface, so its families are unreachable by construction (the plan's §1). The names are
  # `specs/001-s3-local-server/data-model.md` §Metrics'.
  #
  # **The run is read-only because that is what gives this family a sample at all**: the arm is
  # recorded where a read-only exposure takes it (`enter.rs`'s `enforce_bind_read_only`), and
  # prometheus omits a family that holds none — a read-write run publishes no
  # `tinio_sandbox_read_only_arm` line, so only this mode can witness it.
  #
  # **The assertion is the family's name and its label, never the arm's value.** Which of the two
  # arms an exposure lands is the kernel's answer rather than the scenario's (`remount` on the
  # hosts measured so far, `overlay` where a locked per-mount flag forces the fallback), and the
  # plan's §1 leaves that value to the crate whose own leg induces it
  # (`crates/tinio-sandbox-linux`; the standalone `read-only-arm:` line the metric replaces). So
  # the docstring line stops at the label's own spelling: `tinio_sandbox_read_only_arm{arm=`
  # matches the sample line and nothing else in an exposition, since the `# HELP` and `# TYPE`
  # lines carry no label. The seccomp family rides this same scrape and is deliberately not this
  # scenario's subject — one behaviour per scenario, and what the two families' *values* are is
  # the crate's to assert.
  #
  # **The witness says the exposition came from a confined run**, the pair every other confined
  # scenario here uses (`steps/cli.rs`'s `linux_witness`): the `step="model"` re-emit plus the
  # `/proc/<pid>/root` probes. A server that answered the scrape with no jail in place fails
  # there rather than passing on a route the unconfined scenarios share.
  @cli @confinement @FR-036 @linux
  Scenario: A confined read-only run exposes the read-only arm family
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox --read-only"
    And I read the metrics from the control channel
    Then the server's metrics carry
      """
      tinio_sandbox_read_only_arm{arm=
      """
    And the run was confined

  # **The signal an operator's `kill` sends, which no scenario in this suite had ever sent**: every
  # ending here is a `SIGKILL` (`I kill the server`) or a control-channel drain (`I stop the server
  # through its control channel`), so a confined server that ignored `SIGTERM` could outlive a
  # polite stop with every row still green.
  #
  # **The defect this row exists for, in two sentences.** The confinement's seccomp filter denies
  # the whole `sendto` syscall number (`crates/tinio-sandbox-linux/src/seccomp.rs`'s eleven-entry
  # `DENIED`), and the signal machinery the CLI built woke its selector thread through
  # `signal-hook`'s own self-pipe — whose wake is a hardcoded `libc::send`, which on Linux **is**
  # `sendto`; the library drops that failed wake on purpose, so the byte never landed, the selector
  # thread stayed blocked in `recvfrom`, and `SIGTERM`, `SIGINT` and `SIGHUP` were all silently
  # dead inside a confined run (measured, WSL2 2026-09-30: `sendto(5, "X", 1, MSG_DONTWAIT, NULL,
  # 0) = -1 EPERM` confined against `= 1` unconfined, one syscall, same binary). A row that had
  # existed would have caught it the first time a confined server was asked to stop; what caught
  # it instead was the gate's `pgrep -x tinio` leak check (`docs/tests.md`'s WSL2 section, where
  # the finding was recorded, now closed by the CLI's own wake — an owned socketpair written with
  # `write(2)`, which the confinement permits where it refuses `sendto`).
  #
  # **Both carriers, and their order is the cross-check's**: `the run was confined` is asserted
  # *before* the signal, because the kernel half of that witness resolves `/proc/<pid>/root` and a
  # process that has already exited has none — and it is what keeps this row from passing on an
  # unconfined run, where the signal path was never broken. The ending beside it is the contract's:
  # the first termination signal drains and exits 0 (`contracts/cli.md`), and the harness's own
  # bound turns a server still serving into a failure with its two streams rather than a hang.
  #
  # **`@linux` because the in-place arm is the only one where the signal reaches the server**
  # (decision 6): on a launcher arm the process this harness holds is the launcher and the confined
  # child is a different process. The family's host gate is `@confinement` (opt-in off macOS), and
  # a host that cannot confine fails here on `the run was confined` rather than skipping —
  # `docs/tests.md`'s fail-not-skip rule; `TINIO_SANDBOX_ALLOW_SKIP` is the crate suites' opt-out
  # and has no step in this harness.
  @cli @confinement @FR-036 @linux
  Scenario: A confined server stops on SIGTERM
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox"
    Then the readiness line reports a bound port
    And the run was confined
    When I send the server SIGTERM
    Then the run stops on the signal

  # A5's secret-leak rule, asked of the streams a real run writes: the control token the run
  # publishes reaches neither the frame stream (stdout) nor the operational log (stderr) — the leg
  # `crates/tinio-server/tests/sandbox_e2e.rs` carried until the placement cleanup moved it here
  # (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`; that file's other leg stays,
  # and its module doc says why the `EROFS` probe has no step).
  #
  # **The needle is the run's own published `state`, never a string this suite invented**:
  # `neither stream carries the run's published control token` parses the file the run wrote
  # (`steps/cli.rs`'s `published_state`, which reads it through `State` rather than grepping it, so
  # the credential never reaches this log), and the value is the one `tinio-api`'s `generate_token`
  # minted inside the run and nowhere else — so what a failure reports is "the run printed its own
  # token" and not "the run printed something that happens to be this test's string".
  #
  # **The two steps before it are what keep the absence from being vacuous**: the readiness frame
  # is the run's own record of having served, and the in-place re-emit line is the log A5 is about
  # (`the run was confined` asserts it with its fields) — a stream that never carried them would
  # make "the token is in neither" a statement about an empty buffer. `NO_COLOR`, which makes that
  # line greppable, is the harness's own (`steps/cli.rs`'s `Cli::command`).
  #
  # **The caveat the move owes a reader**: on Linux the Rust leg ran in the default workspace leg
  # (`cargo nextest --workspace --exclude tinio-e2e`), while a `@confinement` scenario is
  # operator-opt-in off macOS — the coverage change `docs/tests.md`'s WSL2 section records.
  @cli @confinement @FR-036 @linux
  Scenario: A confined run writes its control token to neither stream
    Given a storage root
    When I run "tinio server {root} --port 0 --sandbox --read-only"
    Then the readiness line reports a bound port
    And the run was confined
    And neither stream carries the run's published control token
