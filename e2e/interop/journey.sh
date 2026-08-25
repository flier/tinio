#!/usr/bin/env bash
# Interop core-journey scenario (task T032).
#
# The SC-001 basic scenario set through aws cli v2 and rclone against
# 127.0.0.1 with no client-side addressing overrides (SC-002): create
# bucket, upload, download byte-identical, list with prefix/delimiter,
# delete object, delete bucket. CI-gated (FR-025).
#
# Usage: journey.sh [--server-binary PATH] [--keep]
#   --server-binary PATH  the serve example binary (default: built here)
#   --keep                keep the scratch root on failure

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# --- aws cli v2 journey ----------------------------------------------------
ENDPOINT="$(start_server "$SCRATCH/root" "$SCRATCH/server.log")" || exit 1
echo "server on $ENDPOINT (pid $SERVER_PID)"
AWS="aws --endpoint-url http://$ENDPOINT --region us-east-1"

run $AWS s3 mb "s3://interop-bucket"
echo "hello from aws" > "$SCRATCH/hello.txt"
run $AWS s3 cp "$SCRATCH/hello.txt" "s3://interop-bucket/hello.txt"
run $AWS s3 cp "s3://interop-bucket/hello.txt" "$SCRATCH/downloaded.txt"
cmp "$SCRATCH/hello.txt" "$SCRATCH/downloaded.txt" || { echo "download not byte-identical" >&2; fail=1; }

run $AWS s3 cp "$SCRATCH/hello.txt" "s3://interop-bucket/dir/nested.txt"
run $AWS s3 ls "s3://interop-bucket/"
grep -q "hello.txt" "$SCRATCH/out.log" || { echo "list missing hello.txt" >&2; fail=1; }
run $AWS s3 ls "s3://interop-bucket/dir/"
grep -q "nested.txt" "$SCRATCH/out.log" || { echo "prefix listing missing nested.txt" >&2; fail=1; }

run $AWS s3 rm "s3://interop-bucket/hello.txt"
run $AWS s3 rb "s3://interop-bucket" --force

# --- rclone journey --------------------------------------------------------
run rclone config create tinio s3 provider Minio access_key_id minioadmin secret_access_key minioadmin endpoint "http://$ENDPOINT"
run rclone mkdir "tinio:rclone-bucket"
echo "hello from rclone" > "$SCRATCH/r.txt"
run rclone copy "$SCRATCH/r.txt" "tinio:rclone-bucket/"
mkdir -p "$SCRATCH/rclone-dl"
run rclone copy "tinio:rclone-bucket/r.txt" "$SCRATCH/rclone-dl/"
cmp "$SCRATCH/r.txt" "$SCRATCH/rclone-dl/r.txt" || { echo "rclone download not byte-identical" >&2; fail=1; }
run rclone lsf "tinio:rclone-bucket"
grep -q "r.txt" "$SCRATCH/out.log" || { echo "rclone list missing r.txt" >&2; fail=1; }
run rclone delete "tinio:rclone-bucket/r.txt"
run rclone purge "tinio:rclone-bucket"

# --- ephemeral-port run ----------------------------------------------------
echo ">> ephemeral-port run"
FIRST_PID="$SERVER_PID"
EP2="$(start_server "$SCRATCH/root2" "$SCRATCH/server2.log")" || exit 1
# The bucket ops below keep using the first server's endpoint; the second
# run proves `--port 0` startup.
run $AWS s3 mb "s3://ephemeral-bucket"
run $AWS s3 rb "s3://ephemeral-bucket"
kill "$FIRST_PID" 2>/dev/null || true
stop_server

if [[ $fail -ne 0 ]]; then
    echo "INTEROP JOURNEY FAILED" >&2
    exit 1
fi
echo "INTEROP JOURNEY OK"
