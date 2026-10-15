# `tinio-winsec` — the current-user-only Windows DACLs the config files and the management plane share

`tinio-winsec` is the one writer of the security descriptor that the config files (`tinio-config`) and the management plane's state file (`tinio-api`) publish, and the one read-back that verifies it. What it installs is a single access-allowed entry — the current user, `FILE_ALL_ACCESS`, no inheritance — on a descriptor marked DACL-protected, so the directory's own inheritable ACEs stay out of the object. It is Windows-only: off Windows the crate compiles to nothing. It does not own the sandbox launcher's exposure grants (`tinio-sandbox-windows`) or named-pipe creation (`tinio-api`).

## The guarantee

**A restricted file has never existed under another ACL.** The descriptor travels with the create call, so there is no window in which the object is governed by the directory's inherited grants, and a path that already exists is refused rather than truncated and reused. The path form re-asserts the same descriptor on a file that already exists; it is the second line and not the first, and it cannot stand in for the create-time form.

**The read-back is the claim's evidence.** `acl_is_current_user_only` answers `true` only when every ACE names the current user's SID with full control; an absent or empty DACL answers `false`, so "restricted" is never inferred from silence. A failed Win32 call is an `Err`, never an `Ok(false)`.

## Shared with the workspace

- The **one ACE walk** (`walk_dacl`, and the `Ace` each entry is handed over as): `tinio-sandbox-windows`'s ACL whitelist reads a descriptor through it — by name for its oracle, by handle for its skip — so the Win32 sequence has no second copy.
- The **one sentence a failed Win32 call is reported with** (`win32_error_message`): `tinio-api`'s pipe read-back wraps it in its own error, so the two crates' messages cannot drift apart. What stays with each caller is the attribution — which path the failure belongs to.

## Layout

| File | What it is |
|---|---|
| `src/dacl.rs` | the DACL writer, the read-back, the ACE walk and the shared failure wording — the crate's Win32 surface |
| `src/lib.rs` | the crate root: the `#[cfg(windows)]` gate and the re-export |

## Gates

```console
cargo test -p tinio-winsec
```

The tests install real DACLs — including entries this crate would never write, so the walk's classification is measured rather than assumed — on a scratch directory under the process's temp directory. Off Windows the crate is empty and the command has nothing to run.
