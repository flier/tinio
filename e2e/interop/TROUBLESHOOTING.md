# Interop Troubleshooting

Known failure signatures, root causes, and fixes encountered while running the `e2e/interop` scenarios (`journey.sh`, `advanced.sh`, `boto3.sh`, `mc.sh`) against the serve example. Windows-host notes included — the scenarios are CI-gated on Linux, so the platform quirks here are for targeted/manual runs on a Windows dev machine.

## 1. Server: empty `delimiter=` rolls every object into an empty prefix

**Symptom**: `mc rb --force` fails with `The bucket you tried to delete is not empty` — after `mc` printed `Removed <bucket>/ successfully` for its recursive delete. The access log shows `ListObjectsV2 → 200` while objects exist on disk.

**Root cause**: S3 clients send `delimiter=` (empty value) to mean "no delimiter". The s3s framework parses it as `Some("")`, and the listing engine then rolls **every** key up into the empty common prefix `""` — the page comes back with zero objects and one empty prefix. `mc`'s recursive delete (which lists before removing) therefore believes the bucket is empty, skips the object deletions, and the final `DeleteBucket` answers `409 BucketNotEmpty`. Affects ListObjects V1/V2 and ListMultipartUploads.

**Diagnosis**:

```bash
# What mc actually sends (requests only):
mc --debug rb tinio/mc-bucket --force 2>&1 | grep -E "GET /|DELETE /"
#   GET /bucket/?versions=                → 501 (expected; versioning not implemented)
#   GET /bucket/?delimiter=&...&list-type=2&prefix=   → 200, but body has 0 <Contents>

# Reproduce with any client: an explicit empty delimiter empties the page:
aws --endpoint-url http://$EP s3api list-objects-v2 --bucket b \
    --delimiter "" --query '{count:KeyCount, keys:Contents[].Key}' 
#   → count: null, keys: null (CommonPrefixes contains "")
```

**Fix** (server): normalize an empty delimiter to `None` at the S3 mapping layer — `crates/tinio-server/src/backend/listing.rs` (`list_page`) and `crates/tinio-server/src/backend/multipart.rs` (`op_list_multipart_uploads`): `delimiter.filter(|d| !d.is_empty())`. Regression test: `backend::listing::tests::empty_delimiter_means_no_delimiter`.

**Verification**: `mc rb tinio/bucket --force` succeeds.

## 2. Script: `run mc ls > file` captures only the echo

**Symptom**: `mc.sh` reports `list missing hello.txt` even though the object exists; the check greps `$SCRATCH/list.txt`.

**Root cause**: `run` (lib.sh) redirects the command's stdout into `$SCRATCH/out.log` internally. A caller-side redirect (`run mc ls ... > "$SCRATCH/list.txt"`) therefore only captures the `>> mc ls ...` echo line — the listing itself lands in `out.log`, so the grep of `list.txt` can never match.

**Fix**: grep `out.log` instead (the pattern the stat checks already use), or don't go through `run` for output you need to inspect.

**Watch out**: `out.log` is overwritten by the **next** `run` — do the grep immediately after the command, before any further `run` call.

## 3. Script: `grep "etag"` vs `ETag` in `mc stat`

**Symptom**: `mc.sh` reports `stat missing etag`; the check greps `out.log` for the lowercase `etag`.

**Root cause**: newer `mc` releases print the header as `ETag` (older ones printed `etag`). The case-sensitive grep misses it.

**Fix**: `grep -qi "etag"` (case-insensitive).

## 4. Windows: server processes survive `kill` and lock `serve.exe`

**Symptom**: a rebuild fails with `LINK : fatal error LNK1104: cannot open file "...\serve.exe"`, and `netstat`/Task Manager show many `serve.exe` processes left over from previous scenario runs.

**Root cause**: on Git Bash/MSYS, `kill $PID` (lib.sh's `stop_server`) does not terminate the Windows process — MSYS PIDs differ from Windows PIDs, and the Rust binary does not handle the delivered signal. Every aborted scenario run leaks a server process that keeps `target/debug/examples/serve.exe` mapped, so the linker cannot overwrite it.

**Check what is running** (identify your own by command line):

```powershell
Get-CimInstance Win32_Process -Filter "Name='serve.exe'" |
  Select-Object ProcessId, CommandLine | Format-List
```

**Cleanup**: terminate only the processes this session started (command line matches `serve.exe <tmp-root> --port 0`):

```powershell
Stop-Process -Id <pid> -Force     # or: taskkill /PID <pid> /F
```

**Workarounds while a build is blocked**:
- Build into a separate target dir: `cargo build -p tinio-server --example serve --target-dir /tmp/tinio-e2e-build`, then pass `--server-binary /tmp/tinio-e2e-build/debug/examples/serve.exe`.
- Run tests without building examples (the lingering exe only blocks example linking): `cargo test --workspace --all-features --lib --tests`.

## 5. Environment: missing clients, boto3 venv, `python3` shim

- aws cli is not on `PATH` by default on Windows — prepend `C:\Program Files\Amazon\AWSCLIV2` (and the `mc` WinGet links dir) to `PATH` before running the scripts.
- `boto3.sh` needs Python + boto3. Install into a throwaway venv (keep the system Python untouched):

  ```bash
  python -m venv /tmp/tinio-e2e-venv
  /tmp/tinio-e2e-venv/Scripts/pip install boto3
  ```

- The scripts invoke plain `python3`, but a Windows venv only ships `python.exe`. **Do not copy `python.exe` out of the venv** — it then fails with `failed to locate pyvenv.cfg`. Use a bash wrapper on `PATH`:

  ```bash
  printf '#!/usr/bin/env bash\nexec /tmp/tinio-e2e-venv/Scripts/python.exe "$@"\n' \
    > /tmp/e2e-bin/python3 && chmod +x /tmp/e2e-bin/python3
  PATH="/tmp/e2e-bin:$PATH" ./e2e/interop/boto3.sh ...
  ```

- Bucket names must be ≥ 3 characters (`s3api create-bucket --bucket t` answers `InvalidBucketName` — that is the client's own validation, not a server bug).

## 6. Diagnostics toolbox

| Need | Command |
|------|---------|
| Requests + statuses mc sends | `mc --debug <cmd> 2>&1 \| grep -E "GET /|PUT /|DELETE /\|HTTP/1.1"` (response bodies are not printed) |
| What the server answered | server log (`$SCRATCH/server.log`, or stderr when run manually); access-log `request=` line has the path but **no query string** |
| aws cli without config files | `export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_EC2_METADATA_DISABLED=true` |
| Compare one parameter in isolation | `aws s3api list-objects-v2 --bucket b --<param> --query '...' --output json` |

## Quick reference: symptom → cause → fix

| Symptom | Cause | Fix |
|---------|-------|-----|
| `mc rb --force` → "bucket is not empty" after "Removed … successfully" | empty `delimiter=` empties listings (server bug) | `delimiter.filter(\|d\| !d.is_empty())` in the mapping layer (see §1) |
| `list missing hello.txt` | `run … > file` captures only the echo | grep `out.log` right after the command (§2) |
| `stat missing etag` | `mc` prints `ETag`, script greps `etag` | `grep -qi` (§3) |
| LNK1104 cannot open serve.exe | leaked `serve.exe` processes hold the file | kill by PID (`Stop-Process`), or `--target-dir` build / `--lib --tests` (§4) |
| `failed to locate pyvenv.cfg` | `python.exe` copied out of the venv | bash wrapper shim instead (§5) |
| `ListObjectVersions is not implemented yet` (501) | versioning is outside the v1 surface — **expected** | clients degrade; nothing to fix |
| rclone: "Failed to read versioning status, assuming unversioned" | same as above — **expected** | nothing to fix |
