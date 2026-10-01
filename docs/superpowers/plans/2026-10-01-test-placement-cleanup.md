# Test placement cleanup

Audit 2026-10-01 over every `crates/*/tests/*.rs`: unit tests moved beside the function they exercise, product-surface flows moved to cucumber, and the rest kept with a reason a third party can check. The resulting convention is now `docs/tests.md`'s §Placement.

## Operator rulings (2026-10-01)

- Every test previously marked "not moving" was re-examined; what could move, moved.
- Platform crates are in scope. macOS work is verified on the dockurr guest; Linux arms are cross-checked on WSL2 or in a `--target` build.
- Duplicates are deleted, with per-item accounting.
- The `@confinement` family migrates even though this drops those arms from the default CI legs. The coverage change is recorded in `docs/tests.md`, not absorbed.
- The `tinio-sandbox-windows` harness is promoted to lib code **with a process-wide serialization lock**, so `docs/tests.md`'s "never two child-spawning suites concurrent" holds under any runner.

## UNIT → src

| Source | Tests | Destination |
|---|---|---|
| `tinio-store/tests/rows.rs` | 23 | 12 per-table src files; 2 tests split in two, 3 deleted; file retired |
| `tinio-api/tests/management.rs` | 7 | `src/{transport,router,client,openapi}.rs`; 2 of the 7 deleted as duplicates with their unique assertion folded; file retired (`tests/` keeps `fixtures/`) |
| `tinio-cli/tests/service.rs` | 5 | `src/service/mod.rs` (3 added, 2 merged); file retired |
| `tinio-config/tests/load.rs` | 3 | `src/schema/config.rs` (all merged); file retired |
| `tinio-core/tests/validation.rs` | 3 plain | `src/object.rs` (merged; the 8 proptests stay) |
| `tinio-fs/tests/layout.rs` | 3 | `src/backend/tests.rs` (2), `src/database/open.rs` (1, module gate widened from `cfg(all(test, unix))`) |
| `tinio-fs/tests/proptest_{meta,multipart}.rs` | 7 | `src/meta.rs`, `src/multipart.rs`; seed files renamed to `src/*.proptest-regressions` and pinned with `FileFailurePersistence::WithSource` |
| `tinio-select/tests/parquet.rs` | 3 | `src/parquet.rs`; fixture stays, `include_bytes!` re-pointed; the dangling `[[test]] parquet` stanza removed |
| `tinio-select/tests/stream_large.rs` | 3 | `src/json/mod.rs`; the `#[ignore]` generator reaches the bench-shared support module by `#[path]` |
| `tinio-sandbox-windows/tests/{backend,handles,acl,job,sid,profile,signals}.rs` | 28 | `src/backend/tests.rs`, `src/acl.rs`, `src/sid.rs`, `src/job.rs`, `src/control.rs`, `src/launch.rs`, `src/socket.rs`, `src/process.rs`; 4 files retired |
| `tinio-sandbox-windows/tests/{launch,listener,network,payload,profile}.rs` | 13 | the same src files, after the harness promotion below; 5 files retired |
| `tinio-sandbox/tests/facade.rs` | 6 of 11 | `src/lib.rs` (2 delegations, 4 platform/delegation legs) |
| `tinio-sandbox-macos/tests/relay.rs` | 2 | `src/relay.rs`; file retired |
| `tinio-server/tests/read_only.rs` | 3 | `read_only.feature` (already carried) |
| `tinio-server/tests/serve_sandbox_channels.rs` | 3 | `confinement.feature` (already carried) for the two channel legs, `steps/cli.rs`'s dispatch for the refusal shape; **the third — the feature-off flag refusal — is the 2026-09-29 plan §5 `keep` row and stays Rust**, `tests/sandbox_feature_off.rs` (restored 2026-10-01); file retired |
| `tinio-server/tests/serve_read_only.rs` | 7 | `read_only.feature` / `cli_lifecycle.feature`; file retired |
| `tinio-server/tests/sandbox_e2e.rs` | 1 of 2 | `confinement.feature` (`@linux`) |
| `tinio/tests/doctor.rs` | 25 of 26 | new `doctor.feature` (26 scenarios); the file went 995 lines → 57 |
| `tinio-sandbox-windows/tests/confine.rs` | 7 of 11 | 5 carried by `confinement.feature`; 1 folded into `src/acl.rs`; 2 duplicates. One leg split: its flow half deleted, its out-of-root mechanism kept under a new name because no scenario can induce it |

The Windows harness: `tests/common/` → `src/testing.rs` (the `testing` feature), child/launcher roles become inline `#[test]` entry points named by full libtest path under `--exact`, `tempfile` moved from `[dev-dependencies]` to an optional dependency behind `testing`, and `static LAUNCH` serializes every confined launch. The lock's own test caught a real defect on first run — the acquire had been placed after the probe write.

## Deletions

| Test | Covered by |
|---|---|
| `tinio-store/tests/rows.rs::object_meta_put_round_trips_all_elements` | `src/contract.rs::meta_row_round_trips_the_eight_element_shape` — its 3 uncovered legs reinstated in `src/meta.rs` |
| `tinio-store/tests/rows.rs::object_meta_get_self_heals_garbage_tags_and_checksum` | `src/contract.rs::meta_validate_self_heals_every_element_independently` |
| `tinio-store/tests/rows.rs::state_version_and_compact_marker_round_trip` | `src/contract.rs::state_round_trips_version_and_compact_marker` |
| `tinio-api/tests/management.rs::metrics_without_a_gauge_source_is_404` | `src/router.rs::a_plane_without_a_gauge_source_has_no_metrics_endpoint` + folded error-body assert |
| `tinio-api/tests/management.rs::a_non_loopback_bind_guards_every_endpoint` | `src/transport.rs::a_non_loopback_tcp_plane_requires_the_token` + `src/router.rs::tcp_requires_the_token_on_every_registered_endpoint` |
| `tinio-api/tests/management.rs::the_state_file_and_the_socket_are_private` | `src/state.rs::the_file_is_private` + `src/bind.rs::the_socket_file_is_private` |
| `tinio-api/tests/management.rs::a_scrape_flood_does_not_run_the_storage_scan` | `src/router.rs` (burst folded in; `refreshes()==9`, `scans()==0`) |
| `tinio-api/tests/management.rs::the_pipe_serves_sequential_connections_and_answers_one_open_at_the_stop` | `src/transport.rs::the_pipe_pool_serves_back_to_back_dials_with_no_gap` (the `!serving.is_finished()` assert folded in) |
| `tinio-sandbox-macos/tests/network.rs::the_profile_grants_no_network_operation` | `src/profile.rs::the_profile_grants_no_network_operation_at_all` |
| `tinio-sandbox-windows/tests/acl.rs::a_grant_on_a_path_icacls_cannot_see_fails` | `src/acl.rs::the_grant_refuses_a_path_it_cannot_open` (a strict superset) |
| `tinio-sandbox-windows/tests/confine.rs::{a_second_grant_replaces_the_ace_instead_of_adding_one, a_wider_request_is_not_satisfied_by_a_narrower_ace}` | `src/acl.rs::a_satisfied_grant_is_not_written_twice` |

## Kept, with a checkable reason

| Tests | Reason |
|---|---|
| `tinio-util/tests/spawn_guard.rs` | its subject is the `crates/*/tests/**` tree; inlining puts the gate inside the region it declares out of scope |
| `tinio-e2e/tests/{cucumber.rs,traceability.rs}`, `steps/{sigv4,tag_filter}.rs` | the harness and the spec-ID gate themselves; `tinio-e2e` has no lib target by design |
| `tinio/tests/lifecycle.rs` | `CARGO_BIN_EXE_tinio` resolves only in the owning package's integration tests, and no step can raise a Windows console control event |
| `tinio-sandbox-linux/tests/jail.rs` | spawns `CARGO_BIN_EXE_sandbox-child` (integration-only), and the probe needs the host's real namespaces |
| `tinio-sandbox-macos/tests/{exposures,startup,network,injection,handover}.rs` + `harness/` | every leg re-execs the test binary through a tests-only harness and measures a confined child's kernel-level results |
| `tinio-server/tests/sandbox_e2e.rs` (EROFS leg) | the kernel's answer through `/proc/<confined-pid>/root` |
| `tinio-server/tests/sandbox_e2e_windows.rs` (3) | package-SID DACL oracle, Windows console event, junction/seal fixture |
| `tinio-server/tests/sandbox_macos_e2e.rs` (3) | grandchild process-table oracle; two legs whose subject is a helper defined in the file |
| `tinio-server/tests/read_only.rs::state_lands_under_the_injected_home` | no scenario reaches the *derived* state path; every `@cli` child is handed an explicit `TINIO_STATE_DIR` |
| `tinio-store/tests/lifecycle.rs` (1) | a cross-table flow — 9 tables, 4 phases, one read txn asserting all of them. It is neither a unit test (no single subject) nor cucumber-eligible (no product surface can see the row tables), so `tests/` is its home; `rows.rs`'s per-table siblings moved, this one did not. |
| `tinio-core/tests/validation.rs` (8 proptests) | a property suite over the **public API surface** (`object::key` + `bucket::name` constructors, `accepted_keys_never_escape_base`); it is a crate-boundary conformance layer, not one module's unit test. The `tinio-fs` proptests moved because each exercises one module's store; this one does not. |
| `tinio-sandbox/tests/facade.rs` (5 residue) | the subject IS the facade's published surface — `the_vocabulary_is_re_exported`, `the_launcher_surface_is_re_exported`, `the_launch_failure_surface_is_re_exported`, `the_control_handoff_carries_the_platforms_own_form`, `the_adopted_names_answer_one_error_type`. An in-crate `mod tests` resolves `crate::`/`super::` paths for `pub(crate)` items too, so it cannot fail on a name that stopped being *exported*; the `tests/` boundary is the assertion. |
| `tinio-api/tests/management.rs` residue | retired with the file — its legs moved; `tests/` keeps only `fixtures/` |
| `tinio-sandbox-windows/tests/{confine,events,signals}.rs` residue | the two console legs (CI drops `binary(signals)` from *run*), the foreign-stdout oracle, and two inductions the CLI cannot produce |
| `tinio-select/tests/{fixtures,support}/` | the committed fixture and the bench-shared support module are read by three layers |

## Incident (2026-10-01, after the gates below had passed)

At **13:26:08** every crate manifest and `Cargo.lock` was rewritten by something outside this workstream — the signature is an automated "unused dependency" pass: all `[dev-dependencies]` blocks stripped, real dependencies removed with them (`tinio-e2e` lost `tinio-server`/`tinio-fs`/`tinio-core`/`secrecy`/`hmac` and its `parquet = ["tinio-server/select-parquet"]` became `parquet = []`; `tinio-auth` lost `bytes`/`tinio-mem`; `tinio-sandbox-core` lost `tracing-subscriber`). The tree stopped compiling: 28 targets across 13 crates.

Repair (operator-ruled): 16 manifests and `Cargo.lock` restored from `HEAD` verbatim — the rewrite was pure deletion there; the two files carrying intended changes were merged instead of overwritten — `crates/tinio-sandbox-windows/Cargo.toml` kept the `tempfile` → `testing`-gated optional dependency, and `crates/tinio/Cargo.toml` kept the concurrent `full`-feature refactor (`default = ["full"]`) that the operator chose to retain while its stripped `[dev-dependencies]` came back. `crates/tinio-select/Cargo.toml`'s dangling `[[test]] parquet` stanza (restored with HEAD, but its file is retired by this plan) was removed again. Manifest diff against HEAD is now three files, +21/−32.

**Not restored**: `crates/tinio/Cargo.toml` lost a comment explaining why `tinio-cli` carries no feature pins (a `--no-default-features` build must not ship a confined binary). That comment went with the retained refactor; `docs/cargo.md` says rationale belongs in `docs/`, so it moved there (Features section) rather than back into the manifest.

**Re-verified after the repair**, and the signature is unchanged from the pre-incident run: `cargo check/clippy -D warnings/doc -D warnings --workspace` all Finished; cucumber 15 features / 279 scenarios (279 passed) / 2258 steps; traceability 14 passed; `cargo hack --each-feature` on `tinio` and `tinio-server` Finished; the workspace test leg reproduces exactly the same six failing targets with the same counts (306/6, 86/34, 1/3, 2/1, 3/1, 0/3) and the same environmental causes.

## Verification (2026-10-01, this host unless noted)

- **Workspace gates**: `cargo check --workspace --all-targets` → Finished; `cargo clippy --workspace --all-targets -- -D warnings` → Finished, zero warnings; `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` → Finished.
- **Workspace test leg** (`cargo test --workspace --exclude tinio-e2e --no-fail-fast`): exit 101, and every failure is the host precondition below — `tinio-sandbox-windows` lib 86/34, `tests/confine.rs` 1/3, `tests/events.rs` 2/1, `tests/signals.rs` 3/1, `sandbox_e2e_windows.rs` 0/3, `tinio-fs` lib 306/6. The 34 = the 11 container legs this cleanup moved + 23 that were failing before it (`acl` 12, `sweep` 6, `sid` 3, `backend` 2); every panic is `RegCreateKeyExW … AppContainer\Storage\…` or the harness's `host_cannot`.
- **Feature-gate drift**: `cargo hack check --package tinio --each-feature --no-dev-deps` and the same for `tinio-server` → both Finished, no warnings.
- **Cucumber**: `cargo build -p tinio` → Finished; `--test traceability` 14 passed; the default run → **15 features / 279 scenarios (279 passed) / 2258 steps (2258 passed)**.
- `cargo check -p tinio-sandbox-windows --all-targets` → Finished.
- `tinio-store` 39 tests, `tinio-cli` 102 + 69 doctests, `tinio-config` 101, `tinio-core` 129 + 8, `tinio-api` 48 + 72 doctests, `tinio-sandbox` 6 + 5, `tinio-select` 435 passed / 2 ignored — all green.
- `tinio-e2e` default run: **279 scenarios (279 passed), 2258 steps**; `--test traceability` 14 passed.
- WSL2 (`Ubuntu-26.04`): the `@unix` read-only scenario, the new `@linux` confinement token scenario (5 passed), and the `--target x86_64-unknown-linux-gnu` checks — the last caught a real compile error the Windows build cannot see.
- macOS guest (15.8.1, x86_64): the crate suite runs green with the relay legs inline (lib 49, exposures 3, handover 1, injection 2, network 2, startup 2, doctests 35); `tests/network.rs` is one test shorter, as the duplicate deletion requires.
- **Known-environmental, not regressions**: `CreateAppContainerProfile` is refused on this host (`hr=0x80070005`) under the tool sandbox, so `tinio-sandbox-windows`'s container legs (~34 in the lib, plus `--test confine`) and `tinio-server`'s `sandbox_e2e_windows` fail here — the never-moved baseline fails identically. `tinio-fs` has 6 pre-existing failures from a refused `mklink /J` (os error 1314).

## Open

- The `@confinement` coverage change is recorded in `docs/tests.md`; a CI leg for it was ruled out (a hosted runner owes no ACE budget).
- `docs/superpowers/plans/2026-09-18-sandbox-p2-windows.md` and other historical plans still name retired test targets; they are records and were left as written.
