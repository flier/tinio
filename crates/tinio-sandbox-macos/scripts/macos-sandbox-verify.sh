#!/usr/bin/env bash
# Verify the macOS sandbox on a real Mac: the crate's confinement tests, the
# confined server end to end, and the profile's necessity table — and fail if
# the server's own process was refused anything the profile was not expected to
# refuse.
#
# **This is the only place this crate's own tests run.** The crate takes
# `#![cfg(target_os = "macos")]` at its root, so on Linux and Windows it compiles
# to an empty library and its unit tests, integration tests and doctests do not
# exist there; a `cargo test -p tinio-sandbox-macos` on either host is a green run
# of nothing. Everything below needs this Mac.
#
# The check asserts exactly zero *unexpected* refusals, on purpose: a refusal
# outside the allow-list below is information about the profile rather than a
# reason to relax the check. Investigate the profile, never this script.
#
# **Every confined process carries a noise floor**, and `EXPECTED` below names it
# and explains each entry. None of it is something the server asked for, and none
# can be granted away without granting a great deal more than the profile should
# — the executable's own directory is deliberately not exposed (decision 22 is
# Windows's) and `/private/var/run/syslog` is a network path. They are therefore
# named as expected rather than granted, and anything else is the finding.
#
# **The sysctl floor is not in that list, and the 2026-09-26 re-audit is why.** The
# six sysctl names the loader's own startup probes are refused
# (`hw.ephemeral_storage`, `kern.bootargs`, `kern.osvariant_status`,
# `kern.secure_kernel`, `kern.version`, `security.mac.lockdown_mode_state`) are
# *filters* the profile's `sysctl` rule can name exactly — a name, unlike a path
# tree or a device node, can be granted with nothing beside it — so the rule grants
# them and a refusal of any of them below is a genuine finding. The rule's own
# `evidence` carries the measurement that bounded the set.
#
# **Every window has exactly one owner, and the window this check reads is the
# product path's.** The log is system-wide and asynchronous, so two things are
# load-bearing. Stream it into a file for the whole run and read the file only once
# the stream has stopped, or a record the run already produced can be missed. And
# **attribute by process name**: the record is what makes a denial ours — a run
# that looked like it had one refusal in fact had zero, because the line came
# from `mobileassetd`, a system daemon. The confined server is the `tinio`
# binary (`sandbox-exec` execs in place, so the process is named for the program
# it becomes, and the exec itself is the other name below); matching
# `Sandbox: tinio(` is what separates it from everything else on the machine.
#
# **The gate's window stops before the pruning experiment, which runs in a window
# of its own.** The experiment removes one rule at a time and re-runs the probe, so
# its refusals are its own expected result rather than a finding about the shipped
# profile: with `process-exec` removed, `sandbox-exec` is refused the exec of the
# probe *by construction*, and a single stream spanning both phases feeds that
# expected refusal to the assertion — a red no profile can fix. Each phase
# therefore starts and stops its own stream, and the experiment's records are
# reported rather than asserted on.
#
# **`log stream`, not `log show`.** On macOS 15.8 `log show` returns nothing for
# refusals even with `--info --debug` and an hour-wide window; only the live
# stream reports them.
#
# CI does not use this script and does not assert on refusals: there the log is
# shared with everything else on the runner and the leg runs concurrently with
# the rest of the matrix, so a refusal is evidence to read rather than a gate
# (design §12). The exact-zero assertion belongs here, on a quiet machine.
#
# Usage: bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "macos-sandbox-verify.sh needs macOS (Seatbelt is a macOS facility)"; exit 1
fi

if ! command -v sandbox-exec >/dev/null; then
  echo "sandbox-exec is missing. It is deprecated by Apple and still shipped;"
  echo "without it --sandbox has no mechanism on macOS and fails closed."
  exit 1
fi

# The gate's window, and the experiment's: two files, one stream each at a time.
LOG="$(mktemp -t tinio-sandbox-refusals)"
PRUNE_LOG="$(mktemp -t tinio-sandbox-prune)"
LOG_PID=""
PRUNE_PID=""
trap 'for pid in ${LOG_PID:-} ${PRUNE_PID:-}; do kill "$pid" 2>/dev/null || true; done; rm -f "$LOG" "$PRUNE_LOG"' EXIT
log stream --predicate 'sender == "Sandbox"' --style compact >"$LOG" 2>/dev/null &
LOG_PID=$!

echo "== the confinement tests (startup, exposures, network, relay, injection, handover) =="
cargo test -p tinio-sandbox-macos -- --nocapture

echo
echo "== the confined server, end to end =="
# The e2e spawns the `tinio` binary and resolves it from the test binary's own
# profile directory, so it has to exist first: a filtered `cargo test --test …`
# builds test targets only.
cargo build -p tinio
cargo test -p tinio-server --test sandbox_macos_e2e -- --nocapture

# **The gate's window closes here, and it closes with a `wait`.** Everything the
# check at the end reads was produced between the stream above and these two lines;
# stopping the stream is not enough on its own, because it is asynchronous and a
# record the run already produced can still be in flight when the signal lands.
kill "$LOG_PID" 2>/dev/null || true
wait "$LOG_PID" 2>/dev/null || true
LOG_PID=""

echo
echo "== the baseline's necessity table =="
# **The experiment runs in its own window, for the reason the header gives**: the
# refusals it provokes are the removed rule's expected result — `process-exec`'s is
# the probe's own exec being denied — and they must not reach the assertion below.
log stream --predicate 'sender == "Sandbox"' --style compact >"$PRUNE_LOG" 2>/dev/null &
PRUNE_PID=$!
bash crates/tinio-sandbox-macos/scripts/prune-profile.sh
kill "$PRUNE_PID" 2>/dev/null || true
wait "$PRUNE_PID" 2>/dev/null || true
PRUNE_PID=""
# Reported so that the experiment's own refusals are visible as the experiment's,
# and never asserted on: the check below reads "$LOG" alone.
echo "the experiment's window (its expected refusals, not this check's input) carried"
echo "$(grep -cE 'Sandbox: (tinio|sandbox-exec)\(' "$PRUNE_LOG" || true) refusal(s) attributed to tinio/sandbox-exec"

echo
echo "== the server's own refusals =="
# The noise floor, named so that the check asserts on the rest. Each entry is
# spelled as the log spells it, and the executable's is a suffix match because
# the path depends on the profile directory the run was built in.
#
# **The `dtracehelper` entry is spelled `file-read-data`, and that is a correction
# of the spelling this list shipped with rather than a widening of it.** The process
# opens the trace device read-write and the kernel logs the first access it refuses,
# so the two faces are one denial: with nothing granted the log says
# `file-read-data`, and the write face appears only once the read is granted.
# Measured on macOS 15.8, 2026-09-26, three confined `/bin/echo` runs per arm under
# a profile with no device rule: nothing granted → `3 × file-read-data`; `(allow
# file-read-data (literal "/dev/dtracehelper"))` → `3 × file-write-data`; `(allow
# file-write-data …)` → `3 × file-read-data`. The shipped entry named the write
# face, which the shipped profile cannot produce (the read is refused first), and
# the first gate run that got past the other two reds above was refused exactly
# `file-read-data /dev/dtracehelper`. Only that spelling is named below: naming the
# write face too would pre-authorise a record this profile cannot emit, and a write
# face appearing later means the profile has begun granting the read — a finding.
# The device itself is still not granted, for the reason the paragraph above gives:
# the five device literals the profile used to carry were deleted by measurement.
#
# **The Cryptexes entry arrived with the allowance audit's deletion of the
# `system` read grant** — `(allow file-read* (subpath "/System") (subpath "/usr")
# (subpath "/Library"))`, which both measurement paths called unnecessary. That
# cryptex path is inside the deleted rule's `(subpath "/System")`, so the deletion
# is what makes it deniable; dyld probes macOS 15's OS cryptex while it resolves
# the child's image, and the probe is denied on some runs and not others (measured
# 2026-09-25: denied for `tinio` on
# one run and absent on the next, with the same profile). On the run where it was
# denied the server still reached `event: listening`, served a request and exited
# 0 — which is what the audit's verdict rests on. Re-granting `/System` to silence
# a benign probe would restore exactly the grant the audit deleted, so the line is
# named here instead: that is this script's rule for a floor entry (investigate the
# profile; this profile is behaving as measured), not a relaxation of the check.
EXPECTED='file-read-data .*/(tinio|sandbox-exec)$|file-read-data /dev/dtracehelper|network-outbound /private/var/run/syslog|file-read-data /System/Volumes/Preboot/Cryptexes/OS'
refusals=$(grep -E 'Sandbox: (tinio|sandbox-exec)\(' "$LOG" | grep -Ev "$EXPECTED" || true)
if [[ -n "$refusals" ]]; then
  echo "the profile no longer covers the server:"
  echo "$refusals"
  echo "fix the profile, not this check"
  exit 1
fi
echo "no unexpected refusal for the server's own process"
