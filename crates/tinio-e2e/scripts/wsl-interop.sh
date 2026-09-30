#!/usr/bin/env bash
# Run the @interop suite inside WSL2 (Linux-side aws-cli/rclone).
# Usage: bash crates/tinio-e2e/scripts/wsl-interop.sh [cucumber args...]
set -euo pipefail
for c in aws rclone; do command -v "$c" >/dev/null || { echo "missing $c — sudo apt install awscli rclone"; exit 1; }; done
if [[ "$(pwd)" == /mnt/* ]]; then
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}"  # ext4 build artifacts
  echo "on /mnt — building to $CARGO_TARGET_DIR"
fi
# `and not @confinement` mirrors the CI interop job: an explicit --tags replaces the runner's
# composed filter, so bare @interop would also select the external clients' confined legs
# (`interop/*.feature`) — which is what CI excludes explicitly, and a host that cannot confine
# would fail them. A forwarded --tags replaces this one (last wins); to run the confined legs
# deliberately, name them: --tags '@confinement and (@interop or @boto3 or @mc)'.
cargo test -p tinio-e2e --test cucumber -- --tags '@interop and not @confinement' --retry 1 "$@"
