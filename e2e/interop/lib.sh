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
    # The tracked PID always goes first (T03/F08): a pattern sweep must
    # never REPLACE it — with a custom --server-binary the pattern would
    # miss the script's own server, which would leak holding the redb
    # lock (a later restart on the same root fails DatabaseAlreadyOpen).
    # The pid/bin written by `start_server`'s subshell are the parent's
    # only view of the tracked server (command substitution loses the
    # variables themselves).
    if [[ -z "$SERVER_PID" && -f "$SCRATCH/server.pid" ]]; then
        SERVER_PID="$(cat "$SCRATCH/server.pid")"
    fi
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
    fi
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
        # POSIX fallback: `ps -W` does not exist; find serve processes
        # by command line (the Windows pattern-kill above already
        # handled Git Bash/MSYS). Anchored to OUR resolved binary path —
        # a bare pattern like 'debug/examples/serve' would kill every
        # matching process on the machine (other checkouts, a manual
        # test session).
        if [[ -z "$SERVER_BIN" && -f "$SCRATCH/server.bin" ]]; then
            SERVER_BIN="$(cat "$SCRATCH/server.bin")"
        fi
        if [[ -n "$SERVER_BIN" ]]; then
            local pgrep_pids
            pgrep_pids="$(pgrep -f "$SERVER_BIN" 2>/dev/null)" || true
            if [[ -n "$pgrep_pids" ]]; then
                kill $pgrep_pids 2>/dev/null || true
                for _ in $(seq 1 50); do
                    if ! pgrep -f "$SERVER_BIN" > /dev/null 2>&1; then
                        break
                    fi
                    sleep 0.1
                done
            fi
        fi
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
    # `start_server` runs inside a command substitution: its subshell
    # assignments are invisible to the parent, and `stop_server` needs
    # the binary path for the Linux pid sweep. Persist it to the scratch
    # root, the one path both sides share.
    echo "$SERVER_BIN" > "$SCRATCH/server.bin"
}

# Start the server on an ephemeral port and echo the endpoint (after
# polling the log for the readiness marker). The optional third argument
# sets TINIO_SCANNER; any value other than 0/1 falls back to the config
# gate, so passing "" is the unset behavior. The optional fourth argument
# is a config file passed through `--config` ("" = none).
start_server() {
    local root="$1" log="$2" scanner="${3:-}" config="${4:-}"
    ensure_server_binary
    local args=("$SERVER_BIN" "$root" --port 0)
    if [[ -n "$config" ]]; then
        args+=(--config "$config")
    fi
    TINIO_SCANNER="$scanner" "${args[@]}" > "$log" 2>&1 &
    SERVER_PID=$!
    echo "$SERVER_PID" > "$SCRATCH/server.pid"

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
