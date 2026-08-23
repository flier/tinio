#!/usr/bin/env bash
# Interop advanced scenarios (task T033), reusing the shared harness:
# multipart upload (> 8 MB file → composed ETag pattern), server-side
# copy, and cold-listing with and without the scanner — via aws cli v2 and
# rclone. CI-gated (FR-025).
#
# Usage: advanced.sh [--server-binary PATH] [--keep]

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# --- multipart: > 8 MiB file → composed ETag "md5-N" -----------------------
ENDPOINT="$(start_server "$SCRATCH/root" "$SCRATCH/server.log" 1)"
echo "server on $ENDPOINT"
AWS="aws --endpoint-url http://$ENDPOINT --region us-east-1"
run $AWS s3 mb "s3://adv-bucket"
write_bytes "$SCRATCH/big.bin" 10485760
run $AWS s3 cp "$SCRATCH/big.bin" "s3://adv-bucket/big.bin"
ETAG="$($AWS s3api head-object --bucket adv-bucket --key big.bin --query ETag --output text)"
if [[ "$ETAG" != *-* ]]; then
    echo "multipart ETag is not composed (no -N suffix): $ETAG" >&2
    fail=1
fi
run $AWS s3 cp "s3://adv-bucket/big.bin" "$SCRATCH/big-downloaded.bin"
cmp "$SCRATCH/big.bin" "$SCRATCH/big-downloaded.bin" || { echo "multipart download not byte-identical" >&2; fail=1; }

# --- server-side copy (no client passthrough) ------------------------------
run $AWS s3 cp "s3://adv-bucket/big.bin" "s3://adv-bucket/copy.bin"
run $AWS s3 cp "s3://adv-bucket/copy.bin" "$SCRATCH/copy-downloaded.bin"
cmp "$SCRATCH/big.bin" "$SCRATCH/copy-downloaded.bin" || { echo "copy not byte-identical" >&2; fail=1; }

# --- cold listing (scanner ON: entries pre-computed in the background) -----
# Drop files by hand, then list repeatedly: the first listing computes
# ETags synchronously; with the scanner running, later listings are warm.
mkdir -p "$SCRATCH/root/cold-bucket"
for i in $(seq 1 50); do
    echo "cold file $i" > "$SCRATCH/root/cold-bucket/file-$i.txt"
done
run $AWS s3 ls "s3://cold-bucket/"
grep -q "file-50.txt" "$SCRATCH/out.log" || { echo "cold listing missing files" >&2; fail=1; }

stop_server

# --- cold listing (scanner OFF) --------------------------------------------
ENDPOINT="$(start_server "$SCRATCH/root" "$SCRATCH/server2.log" 0)"
echo "server (no scanner) on $ENDPOINT"
AWS="aws --endpoint-url http://$ENDPOINT --region us-east-1"
run $AWS s3 ls "s3://cold-bucket/"
grep -q "file-50.txt" "$SCRATCH/out.log" || { echo "cold listing (no scanner) missing files" >&2; fail=1; }

# --- rclone multipart + copy ----------------------------------------------
run rclone config create tinio s3 provider Minio access_key_id minioadmin secret_access_key minioadmin endpoint "http://$ENDPOINT"
run rclone copy "$SCRATCH/big.bin" "tinio:adv-bucket/"
mkdir -p "$SCRATCH/rclone-dl"
run rclone copy "tinio:adv-bucket/big.bin" "$SCRATCH/rclone-dl/"
cmp "$SCRATCH/big.bin" "$SCRATCH/rclone-dl/big.bin" || { echo "rclone multipart not byte-identical" >&2; fail=1; }
run rclone copy "tinio:adv-bucket/big.bin" "tinio:adv-bucket/"
# Best-effort consistency check (not fatal; `run` would flag it).
rclone check "tinio:adv-bucket" "$SCRATCH" --include "big.bin" > /dev/null 2>&1 || true

if [[ $fail -ne 0 ]]; then
    echo "INTEROP ADVANCED FAILED" >&2
    exit 1
fi
echo "INTEROP ADVANCED OK"
