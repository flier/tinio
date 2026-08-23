# Interop Client Coverage Matrix (task T036)

Third-party S3 client coverage per FR-025. Every scenario below is run
against the serve example binary (`crates/tinio-server/examples/serve.rs`,
US1) and later the facade binary (US2), bound to `127.0.0.1` with **no
client-side addressing overrides** (SC-002).

## Tiers

| Tier | Clients | Gating |
|------|---------|--------|
| Mandated | aws cli v2, rclone | CI-gated (`.github/workflows/ci.yml` interop stage) |
| Best-effort | boto3, mc | Targeted/manual — NOT CI-gated (FR-025); promoting them into CI requires an FR-025 amendment |

## Scenarios → tools

| Scenario | aws cli v2 | rclone | boto3 | mc |
|----------|:----------:|:------:|:-----:|:--:|
| Create bucket | ✓ | ✓ (mkdir) | ✓ | ✓ (mb) |
| Delete bucket (empty) | ✓ (rb) | ✓ (purge) | — | ✓ (rb) |
| Upload object | ✓ (cp) | ✓ (copy) | ✓ | ✓ (cp) |
| Download (byte-identical) | ✓ | ✓ | ✓ | ✓ |
| List full / prefix / delimiter | ✓ | ✓ (lsf) | ✓ | ✓ (ls) |
| Zero-byte object | ✓ | ✓ | ✓ | ✓ |
| Multipart (> 8 MiB, composed ETag) | ✓ | ✓ | ✓ (upload_file) | ✓ |
| Server-side copy | ✓ | ✓ | — | — |
| Delete object (idempotent) | ✓ | ✓ (delete) | ✓ | ✓ (rm) |
| Bucket-not-empty on delete | ✓ | — | — | ✓ (rb --force) |
| Auth (SigV4) | US3 (auth.sh) | US3 | — | ✓ (alias) |
| Cold listing w/ and w/o scanner | ✓ (advanced.sh) | — | — | — |
| Ephemeral port (`--port 0`) | ✓ (journey.sh) | — | — | — |

## Known deviations (documented)

- `x-amz-checksum-*` headers are accepted and ignored (v1 has no checksum
  verification).
- `x-amz-meta-*` user metadata is accepted and dropped (not stored, not
  returned).
- Content-Type is inferred from the extension at serve time
  (`mime_guess`), not stored.
- ETags: single uploads `"<md5>"`, multipart `"<md5>-N"` (AWS composed
  form).
- SigV2 is disabled by default (aws cli v2 and rclone always use SigV4).

## Unsupported clients (v1, per FR-025)

Clients that require virtual-hosted-style addressing or features outside
the v1 surface (e.g. s3cmd in some configurations, WinSCP, CloudBerry) are
not supported — path-style addressing is the only mode.

## Run

```bash
# CI-gated (requires aws cli v2 + rclone on PATH)
./e2e/interop/journey.sh
./e2e/interop/advanced.sh

# Targeted/manual (requires the client)
./e2e/interop/boto3.sh
./e2e/interop/mc.sh
```

## Troubleshooting

Known failure signatures, root causes, and fixes (server bugs, script
pitfalls, Windows process/env quirks) are collected in
[TROUBLESHOOTING.md](TROUBLESHOOTING.md) — read it first when a scenario
fails.
