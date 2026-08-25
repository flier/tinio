#!/usr/bin/env bash
# boto3 basic-journey scenario (task T034) — best-effort client per FR-025
# (targeted/manual, NOT CI-gated).
#
# The SC-001 basic scenario set via the boto3 SDK: create bucket, upload,
# download byte-identical, list with prefix/delimiter, delete object,
# zero-byte round-trip, multipart via `upload_file` (> 8 MB → composed
# ETag pattern). Runs inside an isolated venv (never the system python) —
# provision once: `python3 -m venv <target>/tinio-e2e-venv` + `pip install
# boto3` in it (TROUBLESHOOTING.md §5); `TINIO_BOTO3_PYTHON` overrides the
# venv path.
#
# Usage: boto3.sh [--server-binary PATH]

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# Resolve the venv python (same convention as the Rust port,
# crates/tinio-server/tests/boto3.rs).
BOTO3_PYTHON="${TINIO_BOTO3_PYTHON:-}"
if [[ -z "$BOTO3_PYTHON" ]]; then
    if [[ -x "$REPO/target/tinio-e2e-venv/Scripts/python.exe" ]]; then
        BOTO3_PYTHON="$REPO/target/tinio-e2e-venv/Scripts/python.exe"
    elif [[ -x "$REPO/target/tinio-e2e-venv/bin/python3" ]]; then
        BOTO3_PYTHON="$REPO/target/tinio-e2e-venv/bin/python3"
    fi
fi
if [[ -z "$BOTO3_PYTHON" || ! -x "$BOTO3_PYTHON" ]]; then
    echo "boto3 venv python not found — provision it once (see TROUBLESHOOTING.md §5):" >&2
    echo "  python3 -m venv $REPO/target/tinio-e2e-venv" >&2
    echo "  $REPO/target/tinio-e2e-venv/Scripts/pip install boto3" >&2
    echo "  (or point TINIO_BOTO3_PYTHON at your venv python)" >&2
    exit 1
fi

ENDPOINT="$(start_server "$SCRATCH/root" "$SCRATCH/server.log")" || exit 1

"$BOTO3_PYTHON" - "$ENDPOINT" <<'PY'
import sys

import boto3
from botocore.client import Config

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

# Basic journey.
s3.create_bucket(Bucket="boto3-bucket")
s3.put_object(Bucket="boto3-bucket", Key="hello.txt", Body=b"hello from boto3")
got = s3.get_object(Bucket="boto3-bucket", Key="hello.txt")["Body"].read()
assert got == b"hello from boto3", "download not byte-identical"

# Zero-byte round-trip.
s3.put_object(Bucket="boto3-bucket", Key="empty", Body=b"")
assert s3.get_object(Bucket="boto3-bucket", Key="empty")["Body"].read() == b""

# List with prefix/delimiter.
s3.put_object(Bucket="boto3-bucket", Key="dir/nested.txt", Body=b"nested")
page = s3.list_objects_v2(Bucket="boto3-bucket", Delimiter="/")
assert "dir/" in [p["Prefix"] for p in page.get("CommonPrefixes", [])]
page = s3.list_objects_v2(Bucket="boto3-bucket", Prefix="dir/")
assert page["KeyCount"] == 1

# Multipart via upload_file (> 8 MiB → composed ETag with -N suffix).
with open("/tmp/tinio-boto3-big.bin", "wb") as f:
    f.write(b"x" * (10 * 1024 * 1024))
s3.upload_file("/tmp/tinio-boto3-big.bin", "boto3-bucket", "big.bin")
head = s3.head_object(Bucket="boto3-bucket", Key="big.bin")
assert "-" in head["ETag"].strip('"'), f"composed ETag expected, got {head['ETag']}"
dl = s3.download_file("boto3-bucket", "big.bin", "/tmp/tinio-boto3-big-dl.bin")
with open("/tmp/tinio-boto3-big-dl.bin", "rb") as f:
    assert f.read() == b"x" * (10 * 1024 * 1024)

# Delete object.
s3.delete_object(Bucket="boto3-bucket", Key="hello.txt")
try:
    s3.head_object(Bucket="boto3-bucket", Key="hello.txt")
    raise AssertionError("object still exists after delete")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "404"

print("BOTO3 JOURNEY OK")
PY
