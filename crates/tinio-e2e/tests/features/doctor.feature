# `tinio doctor` (T057): offline diagnostics of a directory, driven through the
# real binary. The report's two renderings and its exit codes, `--dry-run` and
# `--fix` with their exclusivity and their stopped-server rule, and every
# repairable defect the reclamation matrix names are the scenarios below;
# `contracts/cli.md`'s `tinio doctor` section is the prose they are derived
# from.
#
# **One behaviour per scenario, and the legs are journeys.** The fixture is
# built once and the same root is run plain, under `--dry-run` and under
# `--fix`, because that contrast *is* the contract: a cleanup proposal is
# information in a plain run and a finding under `--dry-run`, and a repair is a
# repair only against the report that proposed it. A scenario that asserted one
# mode alone could not see the difference the mode exists for.
#
# **The fixture is a root whose only defect is the one under test.** A valid
# `meta.redb` is not a file a scenario can write, so the store fixtures ask
# `tinio-fs` for one, and the `state` file is written through the plane's own
# `State` rather than as a hand-built document — a Windows root path in a string
# literal would need its backslashes escaped to be JSON at all. `the root holds
# the private state a crashed run leaves` is the composite the plain and `--fix`
# journeys share; the narrower fixtures beside it exist for the legs that need
# one piece without the others.
#
# **What stays in Rust**: `the_json_report_is_the_schema_shape`
# (`crates/tinio/tests/doctor.rs`) — the implementation half of
# `contracts/doctor-report.schema.json`, and what `docs/tests.md`'s T057 gate
# (`cargo test -p tinio --test doctor`) names. The step layer reads the
# document's own severity field (`steps/cli.rs`'s `report_severity`) and matches
# the findings' detail text, which the JSON encoding preserves verbatim, but the
# key-set comparison is the schema's leg and stays where the schema is read.
#
# The `@unix` scenarios are the socket half of the reclamation matrix: a
# Windows control channel is a named pipe, and a pipe cannot outlive its
# process, so there is never socket residue to probe or reclaim there. The
# `@windows` scenario is the one induction that needs a share mode: it holds the
# stale temp file open with no sharing, which is what makes a repair stage fail
# on a real filesystem rather than in a mock.
@T057 @FR-007
Feature: Doctor diagnostics

  @cli
  Scenario: A plain run diagnoses a crashed root and leaves it alone
    Given a storage root
    And the root holds the private state a crashed run leaves
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "private state"
    And its stdout contains "a crashed server left a stale"
    And its stdout does not contain "home root-state dirs"
    And the file "{root}/.tinio/tmp/leftover" holds "partial"
    And the file "{root}/.tinio/meta.redb" exists

  # The same fixture, one flag apart. The stale `state` file is a defect this
  # scenario is not about and the state/socket leg reports either way, so it is
  # removed between the two runs and what is left is a root whose only issue is
  # a cleanup proposal — the one pair `--dry-run` exists to separate.
  @cli
  Scenario: `--dry-run` promotes a proposal a plain run reports as information
    Given a storage root
    And the root holds the private state a crashed run leaves
    When I run "tinio doctor {root} --dry-run"
    Then the run fails with exit code 1
    And its stdout contains "would clear leftover temp file"
    And the file "{root}/.tinio/tmp/leftover" holds "partial"
    When the root's state file is removed
    And I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "would clear leftover temp file"
    And the file "{root}/.tinio/tmp/leftover" holds "partial"

  @cli
  Scenario: `--dry-run` and `--fix` together are a usage error
    Given a storage root
    And the root holds the private state a crashed run leaves
    When I run "tinio doctor {root} --dry-run --fix"
    Then the run fails with exit code 2
    And its stderr is not empty

  @cli
  Scenario: `--fix` repairs the private state and never the user data
    Given a storage root
    And the root holds the private state a crashed run leaves
    And the root holds bucket "bucket" with object "object.txt" and body "user data"
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/state" does not exist
    And its stdout contains "home root-state dirs"
    And its stdout contains "T144"
    And the file "{root}/bucket/object.txt" holds "user data"

  # The induction is a real one: the stale temp file is held open with **no
  # sharing at all**, and Windows refuses to unlink a file opened that way,
  # where a plain `File::open` shares delete and would let the stage succeed.
  # The file is asserted *present* and never read back: the handle that makes
  # the stage fail is this process's own, and no second open of it — not even
  # this process's — can succeed while it is held.
  @cli @windows
  Scenario: `--fix` reports a failed stage and still emits a JSON document
    Given a storage root
    And the root holds the private state a crashed run leaves
    And the leftover temp file refuses to be removed
    When I run "tinio doctor {root} --fix --json"
    Then the run fails with exit code 1
    And the JSON report's severity is error
    And its stdout contains "repairs applied"
    And the file "{root}/.tinio/state" does not exist
    And the file "{root}/.tinio/tmp/leftover" exists

  # A live control channel is a refusal a `--fix` run *reports*, never a bare
  # failure of the command: stdout carries it, the exit code is the report's,
  # and `--fix --json` on the same fixture is still a document. The plain run
  # beside it is the other half — a server that is running is a *diagnosis*, and
  # its row is `ok`. That run still exits 1, for a reason this fixture cannot
  # avoid and which the contract states: the server holds the store open, so the
  # meta legs answer the "not inspected" warn (`database::Error::is_already_open`)
  # rather than an error.
  @cli
  Scenario: `--fix` refuses while a server is running
    Given a storage root
    And a server is serving the root
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "state file: a server is running"
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And its stdout contains "control channel"
    And its stdout contains "running"
    And its stderr does not contain "control channel"
    When I run "tinio doctor {root} --fix --json"
    Then the JSON report's severity is error

  # A `state` file this build cannot read is not proof that nobody is behind it:
  # one of `Unreadable`'s causes is a version this build does not read, which is
  # exactly what a *newer* server writes. The dial is the recorded channel name,
  # so a Windows named pipe answers the same way a unix socket does — which is
  # why this scenario is not platform-gated.
  @cli
  Scenario: `--fix` leaves a foreign-version state file alone while its channel answers
    Given a storage root
    And a server is serving the root
    And the root's state file records a version this build does not read
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And its stdout contains "stop it before --fix"
    And its stdout does not contain "repairs applied"
    And the file "{root}/.tinio/state" exists

  @cli
  Scenario: A store-less root is clean and gains no store
    Given a configured storage root
    When I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "there is no store; nothing to clear"
    And its stdout contains "not present; nothing to check"
    And the file "{root}/.tinio/meta.redb" does not exist

  @cli
  Scenario: `--dry-run` on a store-less root gains no store
    Given a configured storage root
    When I run "tinio doctor {root} --dry-run"
    Then the report holds no problem other than the host's disk
    And its stdout contains "there is no store; nothing to clear"
    And the file "{root}/.tinio/meta.redb" does not exist

  # Neither rendering may quote the bytes it read, and the fixture's token is
  # what makes the assertion witnessable: `the root's state file carries the
  # fixture's token` fails here if a later edit stopped writing it, rather than
  # letting the two absences below pass over a root that never had a token.
  @cli
  Scenario: The state file's token never reaches a rendering
    Given a storage root
    And the root holds the private state a crashed run leaves
    Then the root's state file carries the fixture's token
    When I run "tinio doctor {root}"
    Then its stdout does not contain "doctor-must-never-print-this-token"
    And its stderr does not contain "doctor-must-never-print-this-token"
    When I run "tinio doctor {root} --json"
    Then its stdout does not contain "doctor-must-never-print-this-token"
    And its stderr does not contain "doctor-must-never-print-this-token"

  @cli
  Scenario: An unreadable state file is a warning and `--fix` clears it
    Given a configured storage root
    And the root holds a store
    And the root's state dir holds a "state" file
      """
      not a document
      """
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "state file: not inspected:"
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/state" does not exist

  # The socket row must speak from a **re-probe** after the repair. Before the
  # refresh it rendered the pre-repair snapshot's `Unreadable` verdict — a `warn`
  # "not inspected" for a `state` file the repair had already removed — which
  # made a repaired run exit 1 for a file that was gone.
  @cli @unix
  Scenario: A repaired state directory is re-probed for a stale socket
    Given a configured storage root
    And the root holds a store
    And the root's state dir holds a "state" file
      """
      not a document
      """
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And its stdout does not contain "stale control socket: not inspected"

  # V5/R28: a read-only client that only probes derives
  # `<home>/.tinio/roots/<hash>/` and must not create it — the creating form
  # minted it merely because the run asked where it would be. Unix-only: `dirs`
  # reads `HOME` there, while a Windows run would be sent at the developer's
  # real profile.
  @cli @unix
  Scenario: A read-only client creates no home state tree
    Given a configured storage root
    And the child names no state directory
    When I run "tinio doctor {root} --read-only"
    Then the report holds no problem other than the host's disk
    And the home state dir was not created

  # `NoStateFile` is an answer, not silence: a crashed instance can leave the
  # socket behind with no `state` file at all, so the derived name is probed and
  # the row names it.
  @cli @unix
  Scenario: No state file still probes the derived control socket
    Given a configured storage root
    And the root holds a store
    When I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "no `state` file in"
    And its stdout contains "no control socket at"
    And its stdout names the derived control socket path
    And the derived control socket does not exist

  # The socket a crashed unix run leaves: bound once so the file exists, then
  # closed so nothing answers it. The scenario never probes it itself — the
  # report's own `reclaim_stale(path, true)` is the probe under test, and the
  # "a stale control socket at <path>" row is its answer.
  @cli @unix
  Scenario: A stale control socket beside a stale state file is reclaimed by `--fix`
    Given a configured storage root
    And the root holds a store
    And the root's state dir holds a stale control socket
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "a stale control socket at"
    And its stdout names the derived control socket path
    And the derived control socket exists
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/state" does not exist
    And the derived control socket does not exist

  # The path rule, and the leg a `--fix` that re-derived would fail while passing
  # the one above: the `state` file's `control_name` is at a path that is *not*
  # the derived one — the shape `[api.unix] path` produces, the documented remedy
  # for a deep root — and `--fix` must remove *that* file. The derived path is
  # never created.
  @cli @unix
  Scenario: `--fix` reclaims the recorded socket path, not the derived one
    Given a configured storage root
    And the root holds a store
    And the root's state dir holds a stale control socket recorded at "recorded.sock"
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "a stale control socket at"
    And its stdout contains "recorded.sock"
    And its stdout does not name the derived control socket path
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/recorded.sock" does not exist
    And the derived control socket does not exist

  # The fragmentation leg: the marker forces the rule `needs_compact` owns (a
  # small temp-dir store never crosses its 64 MiB floor on the ratio alone), and
  # the row is a finding even in a plain run — the `Proposals` mapping is the
  # cleanup leg's, not this one's.
  @cli
  Scenario: A marked store is reported as fragmented
    Given a configured storage root
    And the root holds a store
    And the root's store needs compaction
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "meta.redb fragmentation"
    And its stdout contains "compaction would reclaim space"
    And its stdout contains "integrity check passed"

  @cli
  Scenario: `--fix` compacts a marked store
    Given a configured storage root
    And the root holds a store
    And the root's store needs compaction
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the root's store no longer needs compaction

  @cli
  Scenario: A corrupt meta store is an error and the report still renders
    Given a configured storage root
    And the root's state dir holds a "meta.redb" file
      """
      not a redb file
      """
    When I run "tinio doctor {root}"
    Then the run fails with exit code 1
    And its stdout contains "meta.redb"
    And its stdout contains "severity: error"
    And its stdout contains "storage root"

  @cli
  Scenario: `--fix` on a store it cannot open still reports
    Given a configured storage root
    And the root's state dir holds a "meta.redb" file
      """
      not a redb file
      """
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the report holds an error finding carrying "could not be opened"
    And the report's "repairs applied" row does not carry "could not be opened"

  # A row whose object file is gone: the `Full` repair's meta-orphan leg. The
  # integrity and fragmentation rows are asserted beside it, because they are
  # the same store's other two answers and a report missing one of them would
  # read as a clean store.
  @cli
  Scenario: Orphaned meta entries are reported and reclaimed
    Given a configured storage root
    And the root holds bucket "data" with object "object.txt" and body "user data"
    And the root's store holds an orphaned meta entry for "orphan.txt" in the bucket "data"
    When I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "orphaned meta entry"
    When I run "tinio doctor {root} --dry-run"
    Then the run fails with exit code 1
    And its stdout contains "would reclaim orphaned meta entry for orphan.txt in data"
    And its stdout contains "integrity check passed"
    And its stdout contains "meta.redb fragmentation"
    When I run "tinio doctor {root} --dry-run --json"
    Then the JSON report's severity is warn
    And its stdout contains "orphaned meta entry"
    And the file "{root}/data/orphan.txt" does not exist
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/data/object.txt" holds "user data"
    And the root's store holds no meta entry for "orphan.txt" in the bucket "data"

  # A multipart subtree whose bucket directory no longer exists
  # (failure-handling.md §3): the `Startup`/`Full` repair's bucket-orphaned
  # stage, which has no idle grace (an upload directory without a `UPLOADS`
  # record does, and a fresh fixture would survive it).
  @cli
  Scenario: Abandoned multipart uploads are reported and removed
    Given a configured storage root
    And the root holds a store
    And the root holds bucket "live" with object "object.txt" and body "user data"
    And the root holds an abandoned multipart subtree for the missing bucket "gone"
    When I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "multipart subtree of missing bucket gone"
    When I run "tinio doctor {root} --dry-run"
    Then the run fails with exit code 1
    And its stdout contains "would remove multipart subtree of missing bucket gone"
    And the file "{root}/.tinio/multipart/gone/u1/part-1" holds "x"
    When I run "tinio doctor {root} --dry-run --json"
    Then the JSON report's severity is warn
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/multipart/gone" does not exist
    And the file "{root}/live/object.txt" holds "user data"

  # A `BUCKETS` row whose bucket directory is gone: the stale-record stage. The
  # record is seeded through `tinio_fs::bucket::store` — a redb row is not a file
  # a scenario can write — and the read-back is the store's own, not a second
  # guess at the row.
  @cli
  Scenario: Orphaned bucket records are reported and pruned
    Given a configured storage root
    And the root holds bucket "live" with object "object.txt" and body "user data"
    And the root's store holds a stale bucket record for "gone"
    When I run "tinio doctor {root}"
    Then the report holds no problem other than the host's disk
    And its stdout contains "stale bucket record"
    When I run "tinio doctor {root} --dry-run"
    Then the run fails with exit code 1
    And its stdout contains "would prune stale bucket record for gone"
    When I run "tinio doctor {root} --dry-run --json"
    Then the JSON report's severity is warn
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/live/object.txt" holds "user data"
    And the root's store holds no bucket record for "gone"

  # B1: `--read-only` moves the state dir, and the run must diagnose **that**
  # directory and name it. `TINIO_STATE_DIR` stands in for the home-derived
  # relocation — it is the documented explicit form of it, and the only form a
  # Windows process can be pointed at (`dirs` resolves the home through the shell
  # API there, so `HOME`/`USERPROFILE` do not move it).
  @cli
  Scenario: A read-only run diagnoses the relocated state dir
    Given a configured storage root
    And the relocated state dir holds a stale state file
    When I run "tinio doctor {root} --read-only"
    Then the run fails with exit code 1
    And its stdout names the relocated state dir
    And its stdout contains "a crashed server left a stale"
    And the file "{root}/.tinio/state" does not exist

  # B13 (M-3): the one mode no other scenario drives — `--fix` **owns** the
  # store, so it creates the one a diagnostics pass deliberately leaves absent,
  # and still never touches user data.
  @cli
  Scenario: `--fix` on a store-less root creates the store and leaves the data
    Given a configured storage root
    And the root holds bucket "live" with object "object.txt" and body "user data"
    And the file "{root}/.tinio/meta.redb" does not exist
    When I run "tinio doctor {root} --fix"
    Then the run fails with exit code 1
    And the file "{root}/.tinio/meta.redb" exists
    And its stdout contains "meta.redb"
    And the file "{root}/live/object.txt" holds "user data"

  # B13 (M-4): `--fix --read-only` repairs in the **relocated** state dir and
  # writes nothing in the served root — not even the store the repair path opens.
  @cli
  Scenario: `--fix --read-only` repairs the relocated state dir and not the root
    Given a configured storage root
    And the relocated state dir holds a stale state file
    When I run "tinio doctor {root} --fix --read-only"
    Then the run fails with exit code 1
    And the file "{state}/state" does not exist
    And the file "{state}/meta.redb" exists
    And the file "{root}/.tinio/meta.redb" does not exist

  # **A host that cannot confine, which is the one host state a scenario has to create rather
  # than ask about.** The runner's own token is Medium, so a plain `I run` makes `doctor` see a
  # host that can, and the `warn` row this leg asserts on could never be witnessed here. The run
  # is spawned under a Low integrity token (`tinio_sandbox_windows::testing::low_integrity`),
  # which is the state a would-be confiner is refused in, so the capability probe reads the same
  # `E_ACCESSDENIED` a real confined host reports and `writability` fails for the same reason —
  # the second failure is why the exit code is 1 rather than only the row being `warn`. The leg
  # is the text rendering on purpose: `--json` writes one field per line, so a finding's
  # `severity` and `check` never share a line and `doctor_row` (`steps/doctor.rs`) could not read
  # the row at all.
  @cli @windows
  Scenario: doctor reports a host that cannot confine
    Given a storage root
    When I run "tinio doctor {root}" under a low-integrity token
    Then the run fails with exit code 1
    And the report holds a warn finding carrying "host confinement"
    And the report holds a warn finding carrying "hr=0x80070005"

  # **The same refusal against the second state the predicate has to tell apart.** An earlier
  # probe killed between its create and its delete leaves `tinio.sandbox.capability-probe`
  # behind; the scenario above runs with the name absent, this one with it present, and both owe
  # the same `When`/`Then`s. The residue is manufactured with `PackageSid::derive` +
  # `mem::forget` (the seeding step), which is exactly what a hard kill leaves.
  #
  # **This is a state fixture with a regression role, not a reproduction of an observed
  # defect.** On this host the residue cannot defeat the probe either way: measured by the
  # wave-E review (and by Task 4 fix 3's own probe), `CreateAppContainerProfile`'s permission
  # check precedes its existence check, so a Low token is refused `E_ACCESSDENIED`
  # (`0x80070005`) with the residue present and never gets `ALREADY_EXISTS` (`0x800700B7`) —
  # the takeover of a colliding name is never reached by a denied token here. It is kept
  # because the clear-before-create must not regress on a host whose check order differs, and
  # because the state is real on any host.
  #
  # **The HRESULT is deliberately not pinned here.** With residue present the refused call is
  # the clear of the existing name rather than the create, and its HRESULT need not be the
  # create's `0x80070005`; the code pin belongs to the scenario above, which has no residue.
  # What this one pins is the row itself: severity `warn`, check `host confinement`, and the
  # action the refusal tells the operator to take.
  @cli @windows
  Scenario: doctor reports a host that cannot confine with a leftover probe profile
    Given a storage root
    And an earlier probe left its profile behind
    When I run "tinio doctor {root}" under a low-integrity token
    Then the run fails with exit code 1
    And the report holds a warn finding carrying "host confinement"
    And the report holds a warn finding carrying "ordinary user session"
