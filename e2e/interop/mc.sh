#!/usr/bin/env bash
# mc (MinIO Client) basic-journey scenario (task T035) — best-effort client
# per FR-025 (targeted/manual, NOT CI-gated).
#
# The SC-001 basic scenario set via `mc`: mb/cp/ls/rm/rb, large-file copy
# (multipart), `mc stat` ETag check, zero-byte object. Requires the mc
# binary for targeted runs.
#
# Usage: mc.sh [--server-binary PATH]

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ENDPOINT="$(start_server "$SCRATCH/root" "$SCRATCH/server.log")"

mc alias set tinio "http://$ENDPOINT" minioadmin minioadmin > /dev/null

run mc mb "tinio/mc-bucket"
echo "hello from mc" > "$SCRATCH/hello.txt"
run mc cp "$SCRATCH/hello.txt" "tinio/mc-bucket/hello.txt"
run mc cp "tinio/mc-bucket/hello.txt" "$SCRATCH/downloaded.txt"
cmp "$SCRATCH/hello.txt" "$SCRATCH/downloaded.txt" || { echo "download not byte-identical" >&2; fail=1; }

# Zero-byte object.
: > "$SCRATCH/zero"
run mc cp "$SCRATCH/zero" "tinio/mc-bucket/zero"
# `run` captures the command output in out.log (a caller-side redirect
# would only capture the echo) — grep there, before the next run.
run mc stat "tinio/mc-bucket/zero"
grep -q "0 B" "$SCRATCH/out.log" || { echo "zero-byte stat unexpected" >&2; cat "$SCRATCH/out.log" >&2; fail=1; }

# ETag check via mc stat (`ETag` in newer mc releases).
run mc stat "tinio/mc-bucket/hello.txt"
grep -qi "etag" "$SCRATCH/out.log" || { echo "stat missing etag" >&2; cat "$SCRATCH/out.log" >&2; fail=1; }

# Large file (multipart).
write_bytes "$SCRATCH/big.bin" 10485760
run mc cp "$SCRATCH/big.bin" "tinio/mc-bucket/big.bin"
run mc cp "tinio/mc-bucket/big.bin" "$SCRATCH/big-dl.bin"
cmp "$SCRATCH/big.bin" "$SCRATCH/big-dl.bin" || { echo "large copy not byte-identical" >&2; fail=1; }

# List + delete (`run` captures the command output in out.log — a
# caller-side redirect would only capture the echo).
run mc ls "tinio/mc-bucket"
grep -q "hello.txt" "$SCRATCH/out.log" || { echo "list missing hello.txt" >&2; fail=1; }
run mc rm "tinio/mc-bucket/hello.txt"
run mc rb "tinio/mc-bucket" --force

if [[ $fail -ne 0 ]]; then
    echo "MC JOURNEY FAILED" >&2
    exit 1
fi
echo "MC JOURNEY OK"
