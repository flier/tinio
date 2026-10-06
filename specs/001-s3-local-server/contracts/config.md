# Contract: Configuration

**Branch**: `001-s3-local-server` | **Date**: 2026-08-21

**Implementation**: `crates/tinio-config/src/schema/mod.rs` (`Config::parse`, garde validation, `SmartDefault` defaults); `.env` loading in `sources.rs` (T018). Credential generation (`credentials.rs`) lands in US2/US3.

## Config file: `<root>/.tinio/.tinio.toml`

Auto-created with generated credentials on first `start` (in read-only mode: created under the home state dir instead — see below). `<state-dir>/config.toml` is accepted as an alias when `.tinio.toml` is absent (auto-create always writes `.tinio.toml`). Sections and keys are validated; **unknown keys are rejected at startup** (fail-fast). All paths relative to the state dir (`<root>/.tinio/`, or `~/.tinio/roots/<sha256(canonical root)16>/` in read-only mode — or the explicit `TINIO_STATE_DIR` when set, see below) unless absolute.

```toml
version = 1              # config format version (future migrations)

[server]
host = "127.0.0.1"       # default
port = 9000              # default (Minio-compatible); 0 = OS-assigned ephemeral port (tests; actual port in logs/state)
read_only = false        # true = read-only mode (FR-023): reject all S3 writes, read the root's own .tinio/meta.redb read-only, tool state under ~/.tinio/roots/<hash>/

[scanner]
# presence = background ETag scanner on (auto-created config includes this section; omit to disable; FR-024)
# Minio-aligned keys (mc admin config set myminio scanner delay=... max_wait=... cycle=...)
delay = 10.0             # seconds between scan iterations (pacing/throttle)
max_wait = "15s"         # max time to wait for a scan slot when throttled
cycle = "24h"            # full-tree scan cycle (re-scan for out-of-band changes)

[auth]
access_key = "..."       # generated on first start (≥ 16 bytes, CSPRNG random)
secret_key = "..."       # generated on first start (≥ 32 bytes, CSPRNG random)
# NOTE: no `anonymous` key — anonymous mode is flag/env only (deliberate); the key is rejected as unknown.

[owner]                  # the root user's canonical-account identity (FR-033; optional — absent = the defaults)
canonical_id = "..."     # the root account's 64-lowercase-hex canonical ID; default hex(SHA-256("tinio")) = d16b7e8c...
display_name = "tinio"   # the owner element's presentation name; default "tinio"
# local_uid = 1000       # unix-only: the root user's landed files chown to this uid; a Windows config with
                         # this key fails validation at parse; requires chown privilege (root/CAP_CHOWN) at startup

# [[users]]              # additional signable principals (FR-033; optional — absent = root only)
# access_key = "..."     # SigV4 access key (≥ 1 char)
# secret_key = "..."     # SigV4 secret key (non-empty — the [auth] rule body)
# canonical_id = "..."   # 64 lowercase hex, optional — default hex(SHA-256(access_key))
# display_name = "..."   # optional — default the access key
# local_uid = 1000       # unix-only, as above; unique access keys + unique canonical IDs
                         # (incl. colliding with the root [owner].canonical_id) are enforced

[log]
verbosity = "info"                # error | warn | info | debug
access_log = "access.log"         # access-log file name (relative to the state dir, or absolute)
access_log_format = "combined"    # combined | common | custom nginx-style string
server_log_format = "text"        # text | json  (json defaults file name server.json)
server_log_file = ""              # empty = stderr; daemon mode defaults to server.log / server.json

[s3]
multipart = true          # multipart ops + upload_part_copy
copy_object = true
list_objects_v1 = true
list_objects_v2 = true
delete_objects = true
cors = true               # bucket CORS ops (get/put/delete_bucket_cors) + OPTIONS preflight + response decoration; off ⇒ NotImplemented and the preflight route absent (FR-033; added 2026-09-05)
acl = true                 # owners & ACLs (FR-033): the four ACL ops + write-path headers + access-layer enforcement, default on; off = 501 + accept-and-drop (the no-identity legacy plane)
versioning = true          # object versioning: get/put_bucket_versioning + list_object_versions + versionId addressing, default on; off = 501 on the three ops and every version-addressed request refused, never answered from the current version
max_buckets = 10000   # ListBuckets page-size cap (0 = unlimited; larger max-buckets requests are clamped — the AWS documented ceiling; values above 10,000 are REJECTED at parse — the wire ceiling makes them dead config, F04)
max_keys = 0          # ListObjects page-size cap (0 = unlimited, the default — preserves current behavior)
allow_zero_page_size = false # escape hatch: true restores the legacy empty page for max-keys / max-parts / max-uploads = 0 (ListBuckets stays strict 1..=10,000)
checksum = false      # validate and echo x-amz-checksum-* on multipart uploads (default false = accepted and dropped; see the multipart checksum design 2026-08-31)
sig_v2 = false            # SigV2 off by default; DEPRECATED (weaker scheme; aws cli v2 / rclone never use it) — enabling prints a startup warning; slated for removal in v2
temp_ttl_hours = 24       # stale temp-write sweep timeout; default 24 (hours), minimum 1 (0 is refused at parse)
multipart_expire_days = 7 # abandoned-upload sweep timeout; default 7 (days), minimum 1 (0 is refused at parse)
noncurrent_keep = 0       # retain the newest N noncurrent versions per key; 0 = unlimited (keep every version)
noncurrent_expire_days = 0 # expire a noncurrent version D days after it stopped being current; 0 = never
block_grace_minutes = 10 # minimum age before an unreferenced block may be reclaimed; default 10, minimum 1 (0 is refused at parse)
block_bytes_warn = 0 # warn when the block stores hold more than this many bytes; 0 = never warn

[storage.fs]              # filesystem backend keys
follow_symlinks = false   # reject access through symlinks + exclude from listings (default; true = follow — a link inside a bucket can then escape the storage root)
compact_threshold_percent = 20  # state-database fragmentation % triggering compact at startup (5..=90)
meta_batch_size = 128     # meta entries per write-pipeline batch (1..=4096; the cold list/scanner flush threshold — default from the set_batch benchmark knee)
meta_batch_bytes = 262144 # estimated bytes per write-pipeline batch (1024..=16 MiB; ≈ 56 B + key length per entry — the second flush trigger)

# [pipeline.io]          # IO task pipeline (ETag computation: bounded file reads + hashing)
# workers = 2            # worker-thread count (1..=64; each worker runs one blocking task); replaces the old fixed 16-way cold-list hash concurrency (buffer_unordered) — benchmark-backed, pipeline-spec.md §3.3
# priority = "normal"    # normal | low | high (normal = OS default thread priority; low/high = lowest/highest legal)
# capacity = 1024        # bounded queue capacity (1..=65536; the backpressure bound)

# [pipeline.db]          # DB write pipeline (batched meta writes)
# workers = 1            # worker-thread count (1..=4; redb is single-writer, so 1 is the write-throughput optimum)
# priority = "normal"
# capacity = 1024

[api.unix]              # local channel, unix form — Linux/macOS (part of three-choose-one)
# presence = local channel on (the auto-created config includes this section; omit it to disable)
# The transports are mutually exclusive — exactly one of unix/http/https may be enabled
# (three-choose-one). On Windows use [api.pipe] instead of [api.unix]. More than one is a
# startup error (US2 orchestration).
path = ""                 # socket path (relative to .tinio/, or absolute); empty = the derived tinio-<sha1(canonical root)[..16]>.sock in the state dir

# [api.pipe]              # local channel, Windows form — use this section on Windows instead of [api.unix]
# path = ""              # named-pipe name (empty = derived tinio-<sha1>)

# [api.http]              # presence of this section enables TCP HTTP exposure (mutually exclusive with unix/pipe/https; port default 9001)
# host = "127.0.0.1"
# port = 9001

# [api.https]             # presence of this section (or `--api https://`) enables TCP HTTPS; requires cert + key (mutually exclusive with unix/pipe/http)
# host = "127.0.0.1"
# port = 9001
# cert = ""               # PEM certificate path
# key = ""                # PEM private key path

[telemetry]
otlp_endpoint = "http://127.0.0.1:4317"   # required when the section is present (empty → startup error); omit [telemetry] to disable; requires the `otel` cargo feature
```

## Environment variables

`TINIO_ACCESS_KEY`, `TINIO_SECRET_KEY`, `TINIO_HOST`, `TINIO_PORT`, `TINIO_ANONYMOUS`, `TINIO_READ_ONLY`, `TINIO_SANDBOX` (`0`/`1`; the `[server] sandbox` key's channel, same strictness as `TINIO_READ_ONLY`), `TINIO_STATE_DIR` (read-only mode's explicit state dir — see below), `TINIO_SCANNER` (`0`/`1`), `TINIO_LOG_LEVEL`, `TINIO_ACCESS_LOG` (access-log file), `TINIO_LOG_FORMAT` (server log format), `TINIO_ACCESS_LOG_FORMAT`, `TINIO_API_UNIX` (`0`/`1`), `TINIO_API_HTTP` (`host:port`), `TINIO_API_HTTPS` (`host:port`), `TINIO_API_HTTPS_CERT`, `TINIO_API_HTTPS_KEY`.

*(2026-09-22)* **Every one of these variables the CLI honours is read by the CLI's own parser** — a clap argument with an `env` attribute, declared beside the flag it backs (`crates/tinio-cli/src/cli.rs`), so a flag, its variable and its help text cannot drift apart and no module reads the environment beside the parser. Three consequences are worth stating because they are visible from outside:

- the **boolean switches** (`TINIO_READ_ONLY`, `TINIO_SANDBOX`) carry a custom value parser rather than clap's `SetTrue` action: `SetTrue` accepts the literals `true`/`false` only, so `TINIO_READ_ONLY=1` — the spelling this contract documents and the harnesses set — would die in the parser with "invalid value '1'". The switches take a value-optional form (`--read-only` bare means `true`), and the parser accepts `1`/`true`/`0`/`false`, trimming surrounding whitespace and refusing everything else (a typo must not silently switch a confinement off, nor serve a writable root);
- **`TINIO_STATE_DIR`, `TINIO_ACCESS_KEY` and `TINIO_SECRET_KEY` are environment-only arguments**: their flags (`--state-dir`, `--access-key`, `--secret-key`) are hidden, because the environment is the documented channel and a secret typed on a command line is visible in the process table. `service` carries the state-dir and mode channels as well, since its flag table has no `--read-only`;
- a value from **`.env`** arrives at the same parser as an ordinary environment answer: the file is loaded into the process environment between the CLI's two parses (see the precedence section), so it sits one level below the invoking environment without a precedence rule of its own.

`TINIO_*` takes precedence; when absent, the corresponding `MINIO_*` variable is accepted as a fallback for credentials: `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` (legacy names) and `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (modern names) map to the access/secret key pair.

*(2026-09-20 correction, plan-audit round)* **Eight** of the names above are **not honoured by the CLI feature** (`docs/superpowers/specs/2026-09-20-cli-integration-design.md`): the seven `TINIO_ACCESS_LOG`, `TINIO_ACCESS_LOG_FORMAT`, `TINIO_API_UNIX`, `TINIO_API_HTTP`, `TINIO_API_HTTPS`, `TINIO_API_HTTPS_CERT` and `TINIO_API_HTTPS_KEY` — the CLI's surface there is the `--api*` flags and the `[log]` keys — and `TINIO_ANONYMOUS`, whose `--anonymous` flag, wiring and anonymous-mode semantics are **US3's T087**, so no run in this phase reads the variable either. Setting one of these variables is silently ignored today — a fact this contract states rather than leaves to be discovered. The names stay listed because this contract describes the finished product; whoever wires one owns its tests — `tasks.md`'s T146 records the first seven's debt and T087 owns the eighth.

## Read-only mode (`[server] read_only = true` / `--read-only` / `TINIO_READ_ONLY=1`)

- All S3 mutating operations are rejected with `AccessDenied`; the storage root is never written (may be a genuinely read-only filesystem). The refusal set is *every* mutating operation — tagging, ACL and CORS writes included — and is listed in `contracts/s3-surface.md` (§Read-only mode), together with the mode's two recorded consequences: a write to a missing bucket answers `AccessDenied` rather than `NoSuchBucket`, and the root's own `.tinio/meta.redb` **is read, read-only** (amended 2026-10-03: the recorded tags, ACLs, CORS configuration and version history are served; an open that fails degrades the run to an empty metadata view).
- The **tool state** relocates from `<root>/.tinio/` to `~/.tinio/roots/<sha256(canonical root)16>/` (mode 0700; home resolved via the `dirs` crate) — the name is the first 16 hex characters of `sha2::Sha256` over the canonical root path. State, socket, logs and the auto-created config live there with the same layout; the **storage's** state dir does not move — it is the root's own `<root>/.tinio/`, opened read-only, so the metadata a read-write run wrote stays where it is.
- `TINIO_STATE_DIR` overrides the derived location: an explicit state dir, used **verbatim** (trimmed; an empty value is an error, never the current directory). It is deliberately **not** part of the precedence chain below — it selects the state dir, not a config value — and it is read **once**, before `<state dir>/.env` is loaded (the file lives in the directory this value selects, so a value the file itself supplied could not choose it). Its parents are **not** created mode 0700 and are **not** symlink-checked, because the caller chose the location; the backend still creates the leaf itself and sets it 0700 on unix (on Windows there are no mode bits — ACL-restricted to the current user). It exists for the read-only deployments with no writable home (service accounts, containers, immutable images), where `dirs::home_dir()` is `None` and the derivation would fail startup outright. **It also replaces the home-derived candidate a *client* probes**, in either mode: `status` and `stop` probe the resolved state dir first and then the other candidate — `<root>/.tinio` for a read-only run, the home-derived directory for a read-write one — and the variable answers in place of that second derivation, so a machine with no usable home needs no second variable to be reported on. It never replaces `<root>/.tinio`: a read-write run still serves `<root>/.tinio`, which is where FR-020's reserved directory and the `meta.redb` layout live.
- Hardening: a read-only storage refuses an explicit state dir **inside** the served root (a construction error, not a warning — it would silently write the tree the mode promises to leave alone, and a writable root would never complain; the comparison is made against both path shapes, and on Windows it is case-insensitive — per component, Rust's lowercasing as an approximation of NTFS's own equivalence), and the derived `<home>/.tinio` and `<home>/.tinio/roots` are refused if they are symlinks (normal mode's `prove_contained` already refuses a symlinked `<root>/.tinio`; `TINIO_STATE_DIR` is the escape hatch for a deliberate layout).
- Config read rule: a pre-existing `<root>/.tinio/.tinio.toml` or `<root>/.tinio/config.toml` is still read — never written (`.tinio.toml` wins when both exist). The root's `.tinio/` is otherwise read as the storage's state dir (its `meta.redb`, opened read-only); everything a run *writes* goes to the tool state dir. When the config file is absent, it is auto-created with generated credentials **in the home state dir** instead.
- `.env` is loaded only from the state dir in read-only mode.
- **Implemented so far** (FR-023 delivered through the entry point that exists — the `serve` example — ahead of the CLI): the resolver (`tinio_config::state`, `TINIO_STATE_DIR` included), the mode-0700 parents, the in-root refusal, the tool-state relocation (2026-10-03: the storage reads the root's own `meta.redb` read-only; the earlier fs-side relocation of `meta.redb`/multipart/tmp is what that amendment supersedes), the scanner/cleanup gating, the S3 write rejection and the `--read-only`/`TINIO_READ_ONLY` overlay. The root-side config read above, the `.env` load from the state dir and tinio-api's half of the relocation (state file, control socket, logs) land with the CLI — US2's `server`/`start` surface and the management plane — as the read-only follow-up tasks.md tracks.

## `.env` file: `<root>/.tinio/.env`

Loaded via `dotenvy` if present; standard dotenv syntax (KEY=VALUE lines, comments, quoting, CRLF tolerated). Contains the same `TINIO_*` names.

## Precedence (FR-016)

```
CLI flags > process environment > .env > config file
```

*(2026-09-22)* The chain keeps its four levels and is resolved by the CLI in one place; **the top three are the parser's**. `<state dir>/.env` is loaded into the process environment (dotenvy semantics: an already-set variable is never overridden), and because the CLI parses its command line both before and after that load — the first parse answers only `DIR`, `--config`, the mode and `TINIO_STATE_DIR`, which are what decide where the file is — a value the file supplied reaches the same parser as an exported one and is indistinguishable from it in the parser's provenance. The file is the third level by **when** it was read, not by what the parser says about it, and the config file is the fourth because it is the only level the parser has no answer for. `TINIO_READ_ONLY` is the exception the read-only section describes: the mode is resolved from the first parse alone, so a `TINIO_READ_ONLY` line inside `.env` never selects it.

`--anonymous` (flag/env) overrides configured credentials with a warning logged (FR-009).

## Validation rules

- Unknown config keys / sections → startup error.
- Unknown `access_log_format` variables → startup error (fixed variable set: `$remote_addr`, `$remote_user`, `$time_local`, `$request`, `$status`, `$body_bytes_sent`, `$http_referer`, `$http_user_agent`, `$request_time`). The set is closed by design — it cannot reference the Authorization header, query strings, or credentials, a security property that keeps secrets out of access logs (spec §FR-017); extending the set requires revisiting this guarantee.
- `[server] port`: default 9000 (Minio-compatible); `0` = OS-assigned ephemeral port (for tests; reported in logs/state); explicit 1–65535 = fixed port. The auto-created config on first start writes `port = 9000`. Host any; verbosity in the four levels; boolean-typed keys must be booleans (`[server]` read_only, `[s3]`/`[storage.fs]`/`[telemetry]` toggles); `[scanner]` and `[api.*]` transports are presence-gated (section present = on, absent = off).
- Credential presence rules: no creds + no anonymous → generated session creds (printed once); first start → config auto-created with persisted creds.
- Backend selection is deferred: v1 is filesystem-only (`tinio-fs`); the `[storage]` section holds backend behavior keys (nested per backend — `[storage.fs]` for the filesystem), and a `type` selection key will be added when a second backend (`tinio-s3`, `tinio-webdav`) lands.
- `[s3]` capability groups are also strippable at compile time via default-on cargo features (`multipart`, `copy`, `list-v1`, `list-v2`, `cors`, `acl`); when a group is not compiled, its keys here are schema-known and silently ignored. **A disabled capability answers `NotImplemented` under the operation's own derived wire name — `"<Operation> is disabled"` — uniformly for every gated operation**; the six multipart operations are included, their message having changed from the group-level `"multipart is disabled"` (2026-09-18 — a wire-visible normalization, recorded in `contracts/s3-surface.md`). Only the message is normalized: the error **code** (`NotImplemented`) is unchanged, and no test or client matched the old strings. `[s3] cors` = the bucket CORS operations, the OPTIONS preflight, and the `Access-Control-*` decoration of actual responses (FR-033); default **true**; `false` answers `NotImplemented "<Operation> is disabled"` on the trio and leaves the preflight route unregistered (a browser OPTIONS falls through to s3s's own 501). `[s3] acl` = owners & ACLs (FR-033): the four ACL ops + write-path headers + access-layer enforcement; default **true**; `false` answers `NotImplemented` on the four ops with the write headers accepted and dropped (the no-identity legacy plane).
- `[s3] max_buckets` / `max_keys`: the ListBuckets / ListObjects page-size caps, `u32`. `max_buckets` defaults to 10,000 (the AWS documented maximum); `max_keys` defaults to 0 (unlimited, preserving current behavior). Multipart listings have no caps (AWS documents none). The wire also rejects `max-buckets` above 10,000 (`InvalidArgument`, never a silent clamp). Effective range for `max_buckets` is **0..=10,000** (F04): a value above 10,000 is rejected at config parse — the wire ceiling makes it dead configuration (it could never clamp a request the wire lets through).
- `[s3] allow_zero_page_size`: boolean, default **false** (strict). **Breaking-change signal** (F06): since 2026-08, the pre-existing listing surfaces (`max-keys` V1/V2, `max-parts`, `max-uploads`) answer `InvalidArgument` for values below 1 — where the pre-2026-08 server answered an empty page (`.max(0)`). A client that has always sent `0` breaks with 400 after an upgrade; set `allow_zero_page_size = true` to restore the legacy empty page (0 — and negatives, clamped to 0 — accepted on those surfaces). ListBuckets keeps the AWS-documented 1..=10,000 validation regardless.
- `[s3] checksum`: boolean, default **false** — validate and echo `x-amz-checksum-*` on multipart uploads (spec 2026-08-31). Off (the default) = the v1 behavior: checksums accepted and dropped. On: per-part values (headers or aws-chunked trailers) are validated in the streaming pass (`BadDigest` on mismatch, the part is never stored), the create-time algorithm is persisted and enforced across parts, and Complete validates the full-object value pre-commit (COMPOSITE composition / FULL_OBJECT CRC linearization) — see `contracts/s3-surface.md` for the effective coverage and the documented deviations.
- `[s3] acl`: boolean, default **true** — the owners & ACLs subsystem (FR-033): the four ACL ops answer `NotImplemented` (501) when off, the write-path `x-amz-acl`/`x-amz-grant-*` headers and `x-amz-expected-bucket-owner` are accepted and dropped unvalidated, and no owner resolution / no-access enforcement happens (the pre-ACL legacy plane, root-only auth). Also strippable at compile time via the `acl` cargo feature (`[features]` default-on group; the key stays schema-known and ignored when the feature is off). Gated the same as `tagging` (the FR-021 double-gate pattern).
- `[s3] versioning`: boolean, default **true** — object versioning (FR-037–FR-041): `GetBucketVersioning` / `PutBucketVersioning` / `ListObjectVersions` plus the `versionId`-addressed object operations. Off answers `NotImplemented` on the three operations and refuses every version-addressed request through the fail-open guard (`501 NotImplemented "versioning is disabled"`) — never answered from the current version, which the guard exists to prevent. Also strippable at compile time via the default-on `versioning` cargo feature (a stripped build answers `"versioning is not compiled in"` for a version-addressed request; the key stays schema-known and ignored when the feature is off).
- `[s3] noncurrent_keep`: `u64`, default **0** — retain the newest N noncurrent versions per key and expire the rest; 0 = unlimited (keep every noncurrent version, the safe reading of an unset key).
- `[s3] noncurrent_expire_days`: `u64`, default **0** — expire a noncurrent version this many days after it stopped being current; 0 = never expire by age. With `noncurrent_keep` set too, whichever threshold is met first expires the version.
- `[s3] block_grace_minutes`: `u64`, default **10**, minimum **1** (garde `range(min = 1)`: 0 is refused at parse, never a silent disarming of the guard) — the minimum age before an unreferenced content block may be reclaimed, the grace that narrows the window in which a publish still in flight stores a block the live set does not yet know about.
- `[s3] block_bytes_warn`: `u64`, default **0** (= never warn) — warn when the versioned buckets' block stores hold more than this many bytes, compared against the sweep's own block-walk total (`tinio_storage_block_bytes`); the store has no hard cap, so this is the capacity-planning signal, not an enforcement.
- `[s3] temp_ttl_hours`: `u64`, default **24** (hours), minimum **1** (garde `range(min = 1)`: 0 is refused at parse) — the sweeper's stale temp-write timeout. A zero does not mean "unlimited" as the retention keys' 0 does: it arms the sweeper maximally, removing a staged write temp mid-PUT, so the floor is 1.
- `[s3] multipart_expire_days`: `u64`, default **7** (days), minimum **1** (garde `range(min = 1)`: 0 is refused at parse) — the sweeper's abandoned-upload timeout (idle = max(initiated_at, latest part mtime)), also the grace the startup repair judges an orphan upload by. A zero would abort every in-flight multipart upload on the first pass, so the floor is 1.
- `[owner]` (optional): the root user's canonical-account identity — `canonical_id` (64 lowercase hex, default `hex(SHA-256("tinio"))` — the core constant; the anonymous special canonical ID `65a011a29cdf8ec533ec3d1ccaae921c` is REJECTED at parse: an owner configured with it would BE the anonymous principal), `display_name` (default `"tinio"`), `local_uid` (optional, **unix-only**: the root user's landed files chown to this uid — Windows rejects the key at parse via the same garde rule). The `[auth]` pair + this element are the root user's identity in the ACL identity map (the minioadmin pair remains root in default configs).
- `[[users]]` (optional, repeatable): a signable principal — `access_key` (≥ 1 char), `secret_key` (non-empty — the `[auth]` rule body), `canonical_id` (64 lowercase hex, optional, default `hex(SHA-256(access_key))` — the `derive_canonical_id` core helper), `display_name` (optional, default the access key), `local_uid` (optional, unix-only, as above). Garde validations: unique access keys, unique canonical IDs **including a collision with the root `[owner].canonical_id`** (a user colliding with the root ID would become root — rejected with an `InvalidAccessKeyId`-style config error at startup); secret keys are cleartext TOML (the existing `[auth]` precedent — the config file (0600 on unix, ACL-restricted on Windows) must be protected by the operator, documented).
- **OS-mapping validation (FR-033, §5a)**: `local_uid` on Windows is rejected at config parse time (fail fast); when ANY `local_uid` is configured (in `[owner]` or `[[users]]`), startup verifies chown privilege (root euid or CAP_CHOWN — a real chown probe at startup, fail-closed: the probe's failure answers a config error unless granted; no silent degradation to server-user ownership) before the server binds.
- The `[api.https]` section (or `--api https://`) requires both `cert` and `key` (PEM paths); missing → startup error.
- `http` and `https` are mutually exclusive transports (three-choose-one with the local channel): at most one of `unix`/`pipe`/`http`/`https` may be enabled — the local channel is `unix` on Linux/macOS and `pipe` on Windows, so `unix` and `pipe` are mutually exclusive too; more than one is a startup error.
- The local channel has two platform forms: `[api.unix]` `path` (Linux/macOS — socket path, relative to `.tinio/` unless absolute; **empty = the derived `tinio-<sha1(canonicalize(root))[..16]>.sock` in the state dir**, not a fixed name) and `[api.pipe]` `path` (Windows — named-pipe name, empty = derived `tinio-<sha1(root)>`). Use the platform-appropriate section; both participate in the three-choose-one exclusivity. The unix default is **derived from the root** because two roots sharing one state dir — the deployment `TINIO_STATE_DIR` exists to allow (below) — must not collide on one socket path, which would fail the second server's single-instance bind while it served a *different* root. Whichever of the two wins, its resolved absolute path is what `state.control_name` records, and clients read that name rather than re-deriving it. There is no fallback for a path over the `sun_path` limit: the bind failure names the remedy (`[api.unix] path` may be absolute, so it can point at a short directory).
- `[storage.fs] follow_symlinks`: boolean, default **false** (secure default: access never resolves through a symlink and link entries are excluded from listings, so a link inside a bucket cannot escape the storage root); `true` = follow symlinks (opt-in).
- `[storage.fs] compact_threshold_percent`: 5–90, default 20; the state-database fragmentation percentage that triggers compaction at startup (offline, before the store handles are shared; `doctor --fix` triggers the same).
- `[storage.fs] meta_batch_size` / `meta_batch_bytes`: the streaming meta-batch flush thresholds of the cold list/scanner producers (pipeline-spec.md §3.2, Q5). Computed entries accumulate into one batch, which is flushed through the DB write pipeline (`MetaWriteBatchTask` — one batch = one write transaction) once the entry count reaches `meta_batch_size` (1–4096, default 128 — the task-2.5 `set_batch` benchmark knee, Q6) **or** the estimated bytes reach `meta_batch_bytes` (1024–16 MiB, default 262144 = 256 KiB; the per-entry estimate is ≈ 56 B + key byte length). The defaults live in the tinio-core storage module and are shared by the config schema and `FsOptions`, so the two cannot drift.
- `[pipeline.io]` / `[pipeline.db]`: the task pipelines (IO: ETag computation — CPU/IO-bound; DB: batched meta writes). The section is presence-gated (Q8): an absent `[pipeline]` section resolves to the defaults, and the auto-created config never emits it. Keys per section: `workers` (io: 1–64, default 2 — the task-5 full-pipeline benchmark knee: 1→2 ≈ +2×, 2→4 regresses on the short-key axis, 4→8 flat, basis in task-5-report.md; db: 1–4, default 1 — redb is a single-writer store, so more than one worker adds no write throughput, verified at +0.5%/+1.7% vs 2), `priority` (`normal`/`low`/`high`, default `normal`; `normal` = no thread priority is set — the OS default; `low`/`high` = the lowest/highest legal thread-priority values — Windows `THREAD_PRIORITY_IDLE` / `THREAD_PRIORITY_TIME_CRITICAL`), `capacity` (1–65536, default 1024; the bounded-queue capacity, i.e. the backpressure bound). Each pipeline runs on its own tokio runtime with `workers` worker threads named `tinio-pipeline-io` / `tinio-pipeline-db`, and the configured priority is applied to exactly those threads.
- `[server] read_only`: boolean, default false; see the read-only-mode section above. In read-only mode, `[s3]` write-related toggles are moot (writes are rejected regardless).
- `[server] sandbox`: boolean, default false (FR-036) — confine the server process. The three channels are `--sandbox`, `TINIO_SANDBOX` (`1`/`true`/`0`/`false`) and this key, following the precedence chain above; any one of them is enough, and an explicit `false` from a higher level wins over a lower level's `true`. A confinement request that this build or this platform cannot honour is a **refusal, never a silent no-op**: in a build without the `sandbox` cargo feature this key (or the variable) is a usage error naming the build, and where the feature is present but the platform's backend has not landed, a startup error naming the platform. Either way nothing is served. What the confinement grants and how it composes with read-only mode is in FR-036; on macOS and Windows the process becomes a launcher that supervises a confined child, so the readiness frame is the child's (relayed by the launcher) and the run's `state` is written by the launcher once the relay reports readiness rather than by the child (`contracts/cli.md`'s "on ready" bullet owns the pair).
- `TINIO_SANDBOX` is the variable's name (`1`/`true`/`0`/`false` only, the same strictness as `TINIO_READ_ONLY` — a typo must not silently switch a confinement off). It is the `--sandbox` flag's own environment channel, read by the same parser as `TINIO_READ_ONLY` through that flag's `env` attribute, and in a build without the `sandbox` cargo feature — where the flag is not in the surface at all — the variable keeps a hidden argument of its own, so the channel still exists and still refuses rather than being silently ignored. The `.env` file is the same channel one level lower: it is loaded into the process environment between the CLI's two parses, so what it supplies arrives as an ordinary environment answer.
- `.env` **never selects the mode**: `TINIO_READ_ONLY` is not an input while the mode is being resolved, because the `.env` file's own location is a function of the mode — `<root>/.tinio/.env` normally, the relocated state dir's `.env` in read-only mode — so letting the file decide the mode would close a cycle. A `TINIO_READ_ONLY` line inside `.env` is therefore **inert**: the mode is resolved before the file is read, and the file's contribution is to that process's environment and not to the next run's. The variable must be exported by the invoking environment (or selected by `--read-only` or the config key). Everything below the mode — `TINIO_SANDBOX` included — does read `.env`, because by then the state dir is known and the file has been loaded.
- `[scanner]` section: presence = background ETag scanner on (FR-024; the auto-created config includes it; omitted = off); keys are Minio-aligned (`mc admin config set myminio scanner ...`): `delay` float seconds ≥ 0 (default 10.0 — pacing between scan iterations), `max_wait` duration string (default `15s` — max wait for a scan slot when throttled), `cycle` duration string (default `24h` — full-tree re-scan cadence). **Disabled in read-only mode** (amended 2026-10-03): the scanner's derived-metadata writes have no writable home there — the storage's metadata source is the root's own `<root>/.tinio/meta.redb`, opened read-only, and the relocated home state dir holds the run's **tool state** only (socket, state file, logs). Env: `TINIO_SCANNER` (`0`/`1`).
- Any TCP exposure (http/https section present or `--api` flags) requires the token on ALL management endpoints (including `/metrics` and `/openapi.json`) — with the one bind-address exception `contracts/management-api.md` states: a **loopback** bind keeps `/metrics` alone open, because a Prometheus scrape cannot send a custom header. The `serve` example's interim unauthenticated `GET /metrics` on the data-plane listener (F10, pipeline-spec.md §4) is **gone** as of T075: the management plane's token-gated endpoint landed and the data-plane route was removed with it, so there is no unauthenticated metrics route on either plane and the rule above governs `/metrics` wherever it is served.
- The `[api]` section requires the `api` cargo feature (default on). In builds without it, the keys are schema-known and silently ignored, not rejected as unknown. With the `api` feature compiled in, exactly one management transport must be enabled after resolution (three-choose-one: `unix`/`http`/`https`) — `--no-api-unix` with no TCP transport configured is a startup error.
- Repeatable `--api <URL>` flags select the transport per scheme, and the **presence of any `--api <URL>` flag disables every transport the flags do not name**: with one or more such flags the flags are the whole selection, so `--api http://127.0.0.1:9001` on an auto-created root serves one transport and not two, and where a flag lands on a scheme the config file also names, the flag's value replaces that subsection's (a flag that does not carry the `https` pair still takes it from `[api.https]`). Without any `--api <URL>` flag the config file is the only thing selecting transports, and a config with **no `[api]` section at all** enables the derived local channel; `--no-api-unix` is the switch that turns that local channel off, and it is the startup error the bullet above describes when no other transport is left. The three-choose-one exclusivity applies after all flags and config are resolved.
