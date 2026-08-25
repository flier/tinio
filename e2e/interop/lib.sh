#!/usr/bin/env bash
# Shared interop harness (tasks T032–T035): option parsing, scratch root,
# server build/start, fail-accumulating run(). Sourced by the scenario
# scripts; see README.md for the coverage matrix.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"

SERVER_BIN="${SERVER_BIN:-}"
KEEP=0
SERVER_PID=""
fail=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --server-binary) SERVER_BIN="$2"; shift 2 ;;
        --keep) KEEP=1; shift ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

SCRATCH="$(mktemp -d)"
mkdir -p "$SCRATCH/root"
# Stop the running server (idempotent).
# On Git Bash/MSYS `$!` is not the MSYS pid of a native Windows process and
# `kill` is a no-op, so the server leaks (TROUBLESHOOTING §4). Resolve real
# pids by command line; on Linux `ps -W` does not exist and we fall back to
# the `$!` pid.
stop_server() {
    local pids
    pids="$(ps -W 2>/dev/null | awk '/debug[\\/]examples[\\/]serve/ {print $1}')" || true
    if [[ -n "$pids" ]]; then
        kill $pids 2>/dev/null || true
        # MSYS kill (TerminateProcess) is asynchronous: wait until the
        # processes are gone so the redb lock is released before a
        # sibling server starts on the same root (advanced.sh restarts
        # on one root; without the wait it fails DatabaseAlreadyOpen).
        for _ in $(seq 1 50); do
            if ! ps -W 2>/dev/null | grep -q '/debug/examples/serve'; then
                break
            fi
            sleep 0.1
        done
    else
        [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}
cleanup() {
    stop_server
    if [[ $KEEP -eq 0 ]]; then
        rm -rf "$SCRATCH"
    else
        echo "scratch root kept at $SCRATCH"
    fi
}
trap cleanup EXIT

# Build (once) the serve example binary when none was given.
ensure_server_binary() {
    if [[ -z "$SERVER_BIN" ]]; then
        SERVER_BIN="$REPO/target/debug/examples/serve"
        if [[ ! -x "$SERVER_BIN" && -x "${SERVER_BIN}.exe" ]]; then
            SERVER_BIN="${SERVER_BIN}.exe"
        fi
        if [[ ! -x "$SERVER_BIN" ]]; then
            echo "building serve example..." >&2
            (cd "$REPO" && cargo build -p tinio-server --example serve) >&2
            if [[ ! -x "$SERVER_BIN" && -x "${SERVER_BIN%.exe}.exe" ]]; then
                SERVER_BIN="${SERVER_BIN%.exe}.exe"
            elif [[ ! -x "$SERVER_BIN" && -x "${SERVER_BIN}.exe" ]]; then
                SERVER_BIN="${SERVER_BIN}.exe"
            fi
        fi
    fi
}

# Start the server on an ephemeral port and echo the endpoint (after
# polling the log for the readiness marker). The optional third argument
# sets TINIO_SCANNER; any value other than 0/1 falls back to the config
# gate, so passing "" is the unset behavior.
start_server() {
    local root="$1" log="$2" scanner="${3:-}"
    ensure_server_binary
    TINIO_SCANNER="$scanner" "$SERVER_BIN" "$root" --port 0 > "$log" 2>&1 &
    SERVER_PID=$!
    local endpoint=""
    for _ in $(seq 1 50); do
        if grep -q "listening on" "$log" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    # `grep -oE` + `cut` instead of `grep -oP` (BSD grep on macOS lacks -P).
    endpoint="$(grep -oE 'listening on [0-9.:]+' "$log" | head -1 | cut -d' ' -f3)"
    if [[ -z "$endpoint" ]]; then
        echo "server did not start:" >&2
        cat "$log" >&2
        # `return`, not `exit`: this function runs inside a command
        # substitution — an `exit` here would only exit the subshell (its
        # propagation to the parent depends on bash's set -e quirks). The
        # caller's `|| exit 1` after the substitution is the explicit,
        # version-independent failure path.
        return 1
    fi
    echo "$endpoint"
}

# Run a command, accumulating failures instead of aborting the scenario.
run() {
    echo ">> $*"
    if ! "$@" > "$SCRATCH/out.log" 2>&1; then
        echo "FAILED: $*" >&2
        cat "$SCRATCH/out.log" >&2
        fail=1
    fi
}

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

# Portable N-byte file (BSD `head` has no `-c`).
write_bytes() {
    local dest="$1" nbytes="$2"
    dd if=/dev/urandom of="$dest" bs="$nbytes" count=1 2>/dev/null
}
