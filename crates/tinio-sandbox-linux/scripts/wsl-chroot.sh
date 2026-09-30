#!/usr/bin/env bash
# Verify the Linux sandbox inside WSL2, under both confinement models.
# Usage: bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh [cargo test args...]
#
# WHAT IT NEEDS. A Linux host (this is the WSL2 leg of `docs/tests.md`'s
# platform-gated verification) with an unprivileged user namespace and
# passwordless `sudo`, plus everything the external e2e leg at the end shells
# out to: `aws` (cli v2), `rclone` and `mc` on `PATH`, and the boto3 venv the
# `@boto3` scenarios run inside (`TINIO_BOTO3_PYTHON` overrides its location).
# Anything missing fails that leg; it is never a skip, because a suite that
# certifies the harness must not report success for a client it never ran.
set -euo pipefail
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "wsl-chroot.sh needs Linux (run it inside WSL2)"; exit 1
fi
cd "$(dirname "$0")/../../.."   # the repository root, wherever this is invoked from
if [[ "$(pwd)" == /mnt/* ]]; then
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}"  # ext4 build artifacts
  echo "on /mnt — building to $CARGO_TARGET_DIR"
fi

# The confinement tests *skip* on a host that cannot confine, and a suite of
# skips exits 0 — so a run on the wrong kernel would report success for having
# tested nothing. Gate on the real precondition first, and fail loudly.
if ! unshare --user --map-root-user true 2>/dev/null; then
  echo "FATAL: this kernel refuses an unprivileged user namespace, so the" >&2
  echo "       user-namespace leg cannot run and every confinement test would" >&2
  echo "       only print 'skipping:'. Check /proc/sys/user/max_user_namespaces" >&2
  echo "       and any container policy, or run the root leg via sudo below." >&2
  exit 1
fi

# `RUST_TEST_NOCAPTURE=1` on both model legs, for one line: the jail test prints
# the read-only arm it read out of the child (`read-only-arm: remount` or
# `… overlay`), and the harness would otherwise swallow it along with the rest of
# a passing test's output. Which arm each model takes is what separates these two
# legs — a run that says `2 passed` twice cannot be told from one that exercised
# the same mechanism twice — so the run's own output has to carry it.
echo "== user-namespace model =="
TINIO_SANDBOX_MODEL=userns RUST_TEST_NOCAPTURE=1 \
  cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== root model =="
# A target directory of its own for the privileged leg. Cargo run under `sudo`
# leaves root-owned artifacts behind, and the user-owned legs that follow then
# fail to rebuild over them — so the two identities never share one.
#
# The environment is passed through `sudo env` rather than `sudo -E`: this
# host's sudoers says "preserving the environment is not supported, '-E' is
# ignored", so an `-E VAR=…` spelling runs the leg with the variable **unset**
# and the root model silently becomes the default one — which is exactly the
# two-leg reasoning this script exists for. The explicit PATH is also what
# finds cargo at all under `sudo`, whose `secure_path` rarely includes
# `~/.cargo/bin`.
SUDO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}-root"
sudo env "PATH=$PATH" "HOME=$HOME" "CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}" \
  "CARGO_TARGET_DIR=$SUDO_TARGET_DIR" TINIO_SANDBOX_MODEL=root RUST_TEST_NOCAPTURE=1 \
  cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== the confined server, end to end =="
# The `tinio` binary is built first, and separately: a filtered
# `cargo test --test sandbox_e2e` builds test targets only, and the test resolves
# the serving binary from its own profile directory. The next leg needs the same
# binary, built by the same profile, which is why this build sits in front of
# both.
cargo build -p tinio
cargo test -p tinio-server --test sandbox_e2e
echo
echo "== the external e2e suite =="
# **The external form, not the plain one.** `tests/cucumber.rs` filters the
# external tags out unless `TINIO_E2E_EXTERNAL` is set, and the migrated
# readiness reader (`wait_for_ready`, which decodes the `listening`/`error`
# event frame rather than scraping a printed line) is reached only through
# `External::start` — so `cargo test -p tinio-e2e --test cucumber` spawns no
# `serve` at all and cannot fail if that reader is wrong. CI proves the
# migration in its `interop` jobs; this is the leg that proves it here.
# Everything the external set reaches for is asked for **here**, not discovered
# two hundred scenarios in: the three client binaries its scenarios shell out to,
# and the venv the `@boto3` ones run inside (boto3 must never be the system
# python). The venv's conventional home is the target directory, which is what
# the harness's own `target_dir()` resolves, `TINIO_BOTO3_PYTHON` overriding it.
for tool in aws rclone mc; do
  command -v "$tool" >/dev/null || {
    echo "FATAL: '$tool' is not on PATH, so the external e2e suite cannot run." >&2
    echo "       Install it, or drop this leg deliberately — a skipped leg here" >&2
    echo "       would certify a harness that never ran." >&2
    exit 1
  }
done
BOTO3_VENV="${TINIO_BOTO3_PYTHON:-${CARGO_TARGET_DIR:-$PWD/target}/tinio-e2e-venv/bin/python3}"
if [[ ! -x "$BOTO3_VENV" ]]; then
  echo "FATAL: the boto3 venv python is not at $BOTO3_VENV, and the '@boto3'" >&2
  echo "       scenarios run inside it. Create the venv there, or point" >&2
  echo "       TINIO_BOTO3_PYTHON at your own:" >&2
  echo "       python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3" >&2
  exit 1
fi
TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e --test cucumber
echo
echo "== the confined scenarios, end to end =="
# **The real gate for `@confinement`, which CI never selects.** The cucumber
# harness excludes it by default (`crates/tinio-e2e/tests/cucumber.rs`), so the
# operator's opt-in plus this script are what run it.
#
# **An explicit `--tags` REPLACES the harness's default filter** rather than
# narrowing it — the default exclusions arrive through `CUCUMBER_FILTER_TAGS`,
# written only when no `--tags` was given — so the string below is the whole
# selection and nothing the default excluded stays excluded; that is why it is
# spelled out, and why the opt-in is the variable beside it rather than the tag.
#
# **The platform axis is re-stated in the runner's own spelling**: a scenario
# carrying platform tags runs where at least one of them matches, so this Linux
# host excludes the scenarios that carry platform tags and match neither
# `@linux` nor `@unix` — a `@macos`/`@windows` scenario runs on the launcher
# platforms, and its steps exist nowhere else, which `fail_on_skipped()` would
# turn into a red gate rather than a skip.
#
# **`--concurrency 1` is measured, not tidiness**: two launchers at once write
# DACLs on the same program directory and one blows the 30 s readiness bound,
# which reads as a random red (six identical runs on the phase's Windows host:
# 4.6 s, 8.7 s, 8.7 s, 18.1 s, 57.7 s, and one killed at >120 s).
TINIO_E2E_CONFINEMENT=1 cargo test -p tinio-e2e --test cucumber -- --tags '@cli and @FR-036 and not ((@linux or @macos or @windows or @unix) and not (@linux or @unix))' --concurrency 1

# **No server process outlives the legs, and the check is the gate rather than tidiness.** A
# finished run once left 19 `tinio server <root> --port 0` processes alive after their parent had
# died; they held the built binary open, the next build failed at the uplift step, and every later
# measurement ran against a stale binary until a hash comparison caught it. The check lives here
# because libtest has no run-level teardown to hang it on. **Run this script in a guest with no
# other `tinio` server running**: it asserts the whole process table, which is the plan's own shape
# (`plans/2026-09-25-sandbox-followups.md` §13).
if pgrep -x tinio > /dev/null 2>&1; then
  echo "a tinio server process outlived the legs:" >&2
  pgrep -ax tinio >&2 || true
  exit 1
fi
