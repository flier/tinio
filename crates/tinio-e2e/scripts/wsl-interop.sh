#!/usr/bin/env bash
# Run the @interop suite inside WSL2 (Linux-side aws-cli/rclone).
# Usage: bash crates/tinio-e2e/scripts/wsl-interop.sh [cucumber args...]
set -euo pipefail
for c in aws rclone; do command -v "$c" >/dev/null || { echo "missing $c — sudo apt install awscli rclone"; exit 1; }; done
if [[ "$(pwd)" == /mnt/* ]]; then
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}"  # ext4 build artifacts
  echo "on /mnt — building to $CARGO_TARGET_DIR"
fi
cargo test -p tinio-e2e --test cucumber -- --tags @interop --retry 1 "$@"
