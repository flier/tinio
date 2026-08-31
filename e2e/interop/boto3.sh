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

# The journey client lives in the checked-in boto3_journey.py (driven
# by tests/boto3.rs too) — one copy, not a heredoc twin.
"$BOTO3_PYTHON" "$REPO/crates/tinio-server/tests/boto3_journey.py" "$ENDPOINT"

# Stop the journey server before starting the second one: on POSIX
# `stop_server`'s fallback kills only `$SERVER_PID` (which the second
# start would overwrite), so the journey server would leak. Safe here —
# the journey client has already exited.
stop_server

# ListBuckets pagination: the `[s3] max_buckets = 3` cap forces a small
# page size below the bucket count. Paginate with the boto3 list_buckets
# paginator; assert at least two pages occur — a dropped or ignored cap
# (default max_buckets = 10000) returns everything in one page and fails
# that assertion — and every bucket is seen exactly once.
cat > "$SCRATCH/paginate.toml" <<'CFG'
version = 1

[s3]
max_buckets = 3
CFG
PAGINATE_ENDPOINT="$(start_server "$SCRATCH/root-paginate" "$SCRATCH/paginate.log" "" "$SCRATCH/paginate.toml")" || exit 1

# The client lives in the checked-in boto3_buckets_pagination.py (driven
# by tests/boto3.rs too) — one copy, not a heredoc twin.
"$BOTO3_PYTHON" "$REPO/crates/tinio-server/tests/boto3_buckets_pagination.py" "$PAGINATE_ENDPOINT"

# Stop the pagination server explicitly, mirroring the journey server
# above (F08): relying on the EXIT trap's pattern kill alone would leak
# a server started from a custom --server-binary path (the pattern
# misses it, and only the LAST $SERVER_PID would be killed).
stop_server
