# Quickstart: Validation Guide

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

Runnable end-to-end validation scenarios proving the feature works per spec. Contracts: [contracts/cli.md](contracts/cli.md), [contracts/config.md](contracts/config.md), [contracts/management-api.md](contracts/management-api.md), [contracts/s3-surface.md](contracts/s3-surface.md), [contracts/minio-compat.md](contracts/minio-compat.md). Data model: [data-model.md](data-model.md).

## Prerequisites

- Latest stable Rust toolchain + `cargo`
- For interop scenarios: aws cli v2, rclone, `curl`, `jq` (installed in CI on all three OSes)
- A scratch storage root, e.g. `%TEMP%\tinio-demo` (Windows) or `/tmp/tinio-demo` (shown below; substitute on Windows)

## 1. Build and test gate (constitution quality gates)

```sh
cargo build --workspace
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
cargo doc --no-deps
```

Expected: all green. (CI also runs the stable toolchain matrix, semver-checks, audit.)

## 2. Full S3 journey with aws cli v2 (SC-001, SC-002)

```sh
# Start anonymous server on a scratch root (auto-creates .tinio/ + config)
# No --port → default 9000 (Minio-compatible); --port 0 selects an OS-assigned ephemeral port (tests).
tinio server /tmp/tinio-demo --anonymous &

export AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy
export AWS_ENDPOINT_URL=http://127.0.0.1:9000 AWS_REGION=us-east-1
# path-style addressing against an IP endpoint — no client-side overrides needed (SC-002)

aws s3 mb s3://photos
aws s3 cp ./pic.jpg s3://photos/album/pic.jpg
aws s3 ls s3://photos --recursive
aws s3 cp s3://photos/album/pic.jpg ./pic-back.jpg
aws s3 rm s3://photos/album/pic.jpg
aws s3 rb s3://photos
```

Expected (SC-006): `pic.jpg` physically appears at `/tmp/tinio-demo/photos/album/pic.jpg`; the round-trip download is byte-identical; dropping a file into the root by hand is immediately visible to `aws s3 ls`.

## 3. Multipart and copy (FR-014, FR-015)

```sh
# >8MB file → aws cli uses multipart automatically
dd if=/dev/urandom of=/tmp/big.bin bs=1M count=20
aws s3 mb s3://data
aws s3 cp /tmp/big.bin s3://data/big.bin --expected-size 20971520
aws s3api head-object --bucket data --key big.bin   # ETag matches "md5hex-N" pattern
aws s3 cp s3://data/big.bin s3://data/big-copy.bin  # server-side copy
tinio stop /tmp/tinio-demo                   # §4 restarts the server
```

Expected: multipart completes; parts directory under `.tinio/multipart/` is empty after completion; copy succeeds without client data passthrough.

## 4. Management plane (FR-018, SC-007)

```sh
tinio server /tmp/tinio-demo --anonymous &
tinio status /tmp/tinio-demo   # running, endpoint, root, PID — < 1 s
tinio stop   /tmp/tinio-demo   # graceful; process exits; no partial files
tinio status /tmp/tinio-demo   # stopped
tinio server /tmp/tinio-demo --anonymous &
tinio server /tmp/tinio-demo --anonymous   # second instance → single-instance error, exit 1
```

Metrics (Linux; on Windows the management channel is a named pipe — use `tinio status`-style client or the SDK):
```sh
curl --unix-socket /tmp/tinio-demo/.tinio/tinio.sock http://localhost/metrics | head
curl --unix-socket /tmp/tinio-demo/.tinio/tinio.sock http://localhost/openapi.json | head
```

TCP exposure requires the token on ALL endpoints (start with `--api http://127.0.0.1:9001` or add the `[api.http]` section to the config, default port 9001):
```sh
tinio stop /tmp/tinio-demo
tinio server /tmp/tinio-demo --anonymous --api http://127.0.0.1:9001 &
TOKEN=$(jq -r .token /tmp/tinio-demo/.tinio/state)
curl -H "X-Tinio-Token: $TOKEN" http://127.0.0.1:9001/metrics | head   # 200
curl http://127.0.0.1:9001/metrics                                      # 401 (token-less rejected)
```

Expected: `tinio_http_*`, `tinio_s3_*`, `tinio_storage_*` families present; storage gauges respect the 30 s TTL cache; token-less TCP requests to `/metrics` get 401.

## 5. Auth and error codes (FR-008/009, SC-004)

```sh
# wrong credentials → rejected
AWS_ACCESS_KEY_ID=wrong AWS_SECRET_ACCESS_KEY=wrong \
  aws s3 ls --endpoint-url http://127.0.0.1:9000
# → SignatureDoesNotMatch (exit non-zero)

# traversal attempt → rejected, no FS access
# NOTE: --path-as-is is required — curl otherwise normalizes `..` away before sending
curl --path-as-is -X PUT --data-binary x http://127.0.0.1:9000/photos/../../evil --aws-sigv4 ...
# → error response; /tmp/evil must NOT exist

# invalid bucket name → InvalidBucketName
aws s3 mb s3://.bad --endpoint-url http://127.0.0.1:9000   # → InvalidBucketName

# missing resources → NoSuchBucket / NoSuchKey
aws s3 ls s3://nope --endpoint-url http://127.0.0.1:9000
```

## 6. rclone interop (SC-002, independent implementation)

```sh
rclone config create tinio s3 provider=Other \
  endpoint=http://127.0.0.1:9000 access_key_id=dummy secret_access_key=dummy \
  env_auth=false --non-interactive
rclone copyto /tmp/big.bin tinio:data/rclone.bin
rclone check /tmp/big.bin tinio:data/rclone.bin
rclone ls tinio:data
rclone deletefile tinio:data/rclone.bin
```

Expected: all commands succeed; hash check passes (ETag-based).

## 7. Crash recovery and sweep (FR-011/014, design decision)

```sh
tinio stop /tmp/tinio-demo                 # stop the §4/§5/§6 server first
tinio server /tmp/tinio-demo --anonymous &
# start a multipart upload, then kill -9 the server mid-upload
# (e.g., upload a huge file and kill the process)
tinio server /tmp/tinio-demo --anonymous   # restart works; parts still complete-able
# leave upload idle; after 7 days (or configured shorter TTL) the sweep removes it
# temp files older than temp_ttl_hours are removed on the sweep tick
```

Expected: restart succeeds; no partial object ever appears in listings; `list_multipart_uploads` still shows the interrupted upload until it completes/aborts/expires.

## 8. Diagnostics (doctor)

```sh
tinio doctor /tmp/tinio-demo
tinio doctor /tmp/tinio-demo --json
tinio doctor /tmp/tinio-demo --dry-run   # list what a fix would change
tinio doctor /tmp/tinio-demo --fix       # apply cleanups (server must be stopped)
```

Expected after a normal session: clean report, exit 0. After `kill -9` (stale state/socket) or with orphaned metadata, doctor reports warnings/errors and exits 1; `--dry-run` lists the repairs without touching anything, `--fix` removes the stale state/socket, orphaned meta, abandoned multipart, stale temps, and stale `~/.tinio/roots/<hash>/` dirs whose root no longer exists. Symlink check: `ln -s /etc /tmp/tinio-demo/photos/etc-link` — served by default; with `--no-follow-symlinks` the GET is rejected and the entry disappears from listings. Nested-root check: a `.tinio/` directory at any depth is never served — `aws s3 cp` of a key containing a `.tinio` segment is rejected and listings skip it.

## 9. Zero-byte and large objects (SC-003, FR edge cases)

```sh
touch /tmp/empty && aws s3 cp /tmp/empty s3://data/empty && aws s3 cp s3://data/empty /tmp/empty-back
# → zero-byte round-trip; file exists with size 0

# 1 GB object with flat memory (watch RSS of the tinio process)
dd if=/dev/zero of=/tmp/gig.bin bs=1M count=1024
aws s3 cp /tmp/gig.bin s3://data/gig.bin
aws s3 cp s3://data/gig.bin /tmp/gig-back.bin
```

Expected: memory stays flat during both transfers (bounded buffers, no full-object buffering).

## 10. Read-only mode (FR-023)

```sh
mkdir -p /tmp/tinio-archive/photos && cp ./pic.jpg /tmp/tinio-archive/photos/
chmod -R a-w /tmp/tinio-archive   # genuinely read-only root (unix only — on Windows skip this; --read-only alone suffices, the FS need not be read-only)

tinio server /tmp/tinio-archive --read-only --anonymous --port 9002 &   # non-standard: 9000 is the demo server, 9001 is the API default
aws s3 ls s3://photos --endpoint-url http://127.0.0.1:9002                      # works
aws s3 cp s3://photos/pic.jpg ./pic-ro.jpg --endpoint-url http://127.0.0.1:9002 # byte-identical
aws s3 cp ./pic.jpg s3://photos/x.jpg --endpoint-url http://127.0.0.1:9002      # → AccessDenied
tinio stop /tmp/tinio-archive
```

Expected: reads behave identically to normal mode; every mutating call returns `AccessDenied`; `/tmp/tinio-archive` contains no `.tinio/` (state, config with generated credentials, logs, and the ETag meta store all live under `~/.tinio/roots/<hash>/`); `tinio status /tmp/tinio-archive` works via the home state dir.

## CI note

Scenarios 1–3, 6 run in CI on Windows/Linux/macOS; scenarios 5 (error codes), 8 (doctor incl. `--fix`/`--dry-run`), and 10 (read-only mode; the `chmod` step is unix-only) run everywhere; scenario 7 is manual/CI-long-TTL variant; the TTL-cache behavior is covered by unit tests rather than wall-clock waits. Scenario 9: the zero-byte round-trip runs in CI; the 1 GB transfer is a manual SC-003 check (a smaller streaming smoke test runs in CI via the criterion benches). Dedicated integration tests additionally cover: addressing style (aws cli v2 against `127.0.0.1` and `localhost`, no client overrides), cold listing with and without the scanner, any-depth `.tinio` hiding incl. nested roots, and stop-wait confirmation (see plan.md Testing).
