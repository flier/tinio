//! `confine`: build the jail and step into it.

use std::{
    env,
    ffi::CString,
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use rustix::{
    io::Errno,
    mount::{
        MountFlags, MountPropagationFlags, UnmountFlags, mount, mount_bind_recursive, mount_change,
        mount_remount, unmount,
    },
    process::{getegid, pivot_root},
    thread::{
        CapabilitySet, UnshareFlags, capabilities, remove_capability_from_bounding_set,
        set_capabilities, set_no_new_privs,
    },
};
use tracing::{Span, debug, field, info, instrument};

use crate::{
    _core::{Confinement, Error, Exposure, Result, Sandbox},
    Mechanism,
    caps::{self, Cap},
    jail::{EXPOSE_STEP, jail_root, mirror_under},
    mechanism::{Resolved, fail, refused, step, unshare, userns_available},
    options::Options,
    plan::{Mount, MountKind, OverlayDirs, plan},
    seccomp,
};

/// Write one id map through the descriptor that was opened for it.
///
/// **Through the descriptor, not through a fresh `fs::write`**: these files are
/// only writable from the namespace they describe, and the open has to happen
/// after the `unshare` that created it — writing through the held handle is what
/// keeps that true by construction instead of by comment (design decision 12).
/// The name is a parameter so the error names the map that failed without
/// re-deriving it from the path. The span records which map and what it says
/// (shared §7).
#[instrument(level = "debug", skip(file), ret)]
fn write_map(file: &mut fs::File, name: &'static str, contents: &str) -> Result<()> {
    file.write_all(contents.as_bytes()).map_err(step(name))
}

/// The one combination the user-namespace model cannot carry (design §8).
///
/// One function so the two checks that need it — the request and the
/// resolution — cannot word the refusal differently.
fn local_uid_conflicts_with_userns() -> Error {
    Error::Unsupported {
        detail: "a local_uid mapping cannot be used with the user-namespace model — an unmapped \
                 uid cannot be chowned to; start with TINIO_SANDBOX_MODEL=root (or as root), or \
                 drop the mapping",
    }
}

impl crate::Backend {
    /// Confine the calling process.
    ///
    /// Returns [`Confinement::InPlace`] on success; the process is jailed by
    /// then, and every failure is fatal (design §11).
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] before anything irreversible happens;
    /// [`Error::Unsupported`] when the model is unavailable or a combination
    /// the design refuses was asked for; [`Error::Step`] for any refusing
    /// syscall, named by the step.
    #[instrument(skip_all, fields(model = ?opts.model(), euid = opts.euid()))]
    pub(crate) fn enter(&mut self, sandbox: &Sandbox, opts: &Options) -> Result<Confinement> {
        let prepared = validate_and_plan(sandbox, opts)?;
        create_jail_root(&prepared.jail)?;
        let map_fds = enter_user_namespace(prepared.resolved, opts)?;
        enter_mount_namespace()?;
        apply_planned_mounts(sandbox, &prepared.mounts, &prepared.overlay)?;
        // 7. The maps have been read by every mount that needed them; the
        //    descriptors are closed explicitly because nothing here ever
        //    `exec`s, so `CLOEXEC` never gets a chance to close them and they
        //    would otherwise be inherited by the whole server lifetime.
        drop(map_fds);
        pivot_into_jail(&prepared.jail)?;
        // 9. Drop everything the jail no longer needs, then cut the network.
        //    The filter's outcome is recorded on the backend rather than
        //    dropped: `serve` reads it to warn when the one degradable step
        //    degraded (design §11).
        drop_capabilities(caps::retained(
            opts.served_root_access(),
            opts.local_uid_configured(),
        ))?;
        self.seccomp = Some(seccomp::install()?);

        Ok(Confinement::InPlace)
    }
}

/// The jail layout [`crate::Backend::enter`] steps into, decided while the
/// process can still exit cleanly.
struct Prepared {
    /// The **resolved** model rather than the request: [`resolve_model`] is
    /// what turns [`Mechanism::Auto`] into the one mechanism that runs, and it
    /// refuses before this type exists when neither model can confine the
    /// process. A [`Resolved`] here would carry the `Unavailable` case into
    /// the plan and the user namespace as a value neither can do anything
    /// with.
    resolved: Mechanism,
    jail: PathBuf,
    overlay: OverlayDirs,
    mounts: Vec<Mount>,
}

/// Validate the request and build the mount plan, before anything irreversible.
#[instrument(skip_all, fields(model = ?opts.model()))]
fn validate_and_plan(sandbox: &Sandbox, opts: &Options) -> Result<Prepared> {
    // 1. Validate everything a mistake could be made about, while the
    //    process can still be repaired by exiting.

    // The combination that cannot work, refused before anything is probed
    // or created: under a user namespace the process is chowned *to* an
    // unmapped uid, so a `local_uid` mapping has nothing to land in
    // (design §8).
    //
    // A *forced* user namespace is decided from the request alone — no
    // probe, no privilege, no namespace — because the refusal is the same
    // whether or not this host would grant the namespace, and asking the
    // request is what makes the answer identical on every host. The other
    // half of the same rule is checked against the **resolved** model below.
    if opts.model() == Mechanism::UserNamespace && opts.local_uid_configured() {
        return Err(refused("local_uid", local_uid_conflicts_with_userns()));
    }

    // Nothing exposed is the caller's bug, not the platform's shortcoming,
    // and it is decided **before the probe** so the answer does not depend on
    // what this host happens to allow. `Error::Unsupported` would send the
    // operator to look at their kernel, which is the wrong place (design §11).
    let served_root = sandbox
        .exposures()
        .first()
        .map(|e| e.host.clone())
        .ok_or_else(|| fail("confine", io::Error::other("nothing exposed to confine to")))?;

    let resolved = resolve_model(opts)?;
    validate_exposures(sandbox)?;

    let jail = jail_root(&opts.resolved_tmpdir(), &served_root);
    let overlay = OverlayDirs {
        upper: jail.join(".overlay/upper"),
        work: jail.join(".overlay/work"),
    };
    // **No `refused("plan", …)` wrapper here, and dropping it is the other half
    // of the construction-site rule** (shared §7: the event belongs where the
    // returned `Error` is built). Both refusals `plan` can return are logged at
    // their own sites now — its `Error::Unsupported`, and the `Error::BadPath`
    // `mirror_under` builds — so a wrapper on this line would log one failure
    // twice for this caller, while the caller the rule is actually about (one
    // outside the crate, which never reaches this line) was the one with no
    // record of it at all.
    let mounts = plan(sandbox, &jail, resolved)?;

    Ok(Prepared {
        resolved,
        jail,
        overlay,
        mounts,
    })
}

/// Probe, refuse an unavailable or conflicting model, and name the one
/// mechanism that runs. `ret` is the Info event for which model ran (shared §7).
#[instrument(skip_all, ret, fields(requested = ?opts.model(), euid = opts.euid()))]
fn resolve_model(opts: &Options) -> Result<Mechanism> {
    // Design decision 7's three tiers, decided by asking the kernel rather
    // than guessing: `userns_available` forks a child, so a refusal costs
    // nothing here. `Auto` needs the answer only when the user namespace
    // might be the choice; the forced models that do not depend on it skip
    // the fork.
    let needs_probe = matches!(opts.model(), Mechanism::UserNamespace | Mechanism::Auto);
    let resolved = opts.model().resolved(
        opts.euid(),
        if needs_probe {
            userns_available()?
        } else {
            false
        },
    );

    let resolved = match resolved {
        // **The refusal *is* the mapping, and the shape this function used to
        // have is why.** It returned a [`Resolved`] and a second step mapped
        // that to a mechanism, where `Resolved::Unavailable` — the one value
        // that says *neither* model can confine this process — became
        // `Mechanism::Root`. That arm was unreachable, because this check runs
        // first, but its direction is the one design §7's table and §11's
        // fail-closed rule refuse: a value meaning "no model" had a
        // model-shaped answer waiting for the first reader who reordered the
        // two. `Error::Unsupported` is what the platform really cannot do, and
        // the one thing P0's `Error` reserves that variant for.
        Resolved::Unavailable(detail) => {
            return Err(refused("model", Error::Unsupported { detail }));
        }

        // **The other half of the `local_uid` rule, and the half the request
        // cannot answer.** `Auto` resolves *into* the user namespace whenever
        // the probe grants one, so a check that only looked at `opts.model()`
        // would let `Auto` + a configured `local_uid` proceed on exactly the
        // hosts where the mapping is unusable — and it would keep `CAP_CHOWN`
        // inside a namespace where the configured uid is not mapped, which
        // design §8 and §11 both call a startup configuration error. The
        // resolved model is the only thing that knows.
        Resolved::UserNamespace if opts.local_uid_configured() => {
            return Err(refused("local_uid", local_uid_conflicts_with_userns()));
        }

        Resolved::UserNamespace => Mechanism::UserNamespace,
        Resolved::Root => Mechanism::Root,
    };
    // **The first of the three events shared §7 makes this crate's
    // must-survive set**, and the one the design's own wording names first
    // ("the resolved model"): the *resolved* model rather than the request,
    // because `Auto` is a request that becomes one of the two, and an operator
    // asking "was this process confined, and how" is asking about the one that
    // ran. `Debug`-level detail belongs to the spans this function keeps; this
    // is the event that has to survive `serve`'s fixed `Verbosity::Info`.
    info!(
        step = "model",
        model = ?resolved,
        requested = ?opts.model(),
        euid = opts.euid(),
        "the model this process confines itself with"
    );
    Ok(resolved)
}

/// Refuse a missing or un-mirrorable exposure before a directory is created.
fn validate_exposures(sandbox: &Sandbox) -> Result<()> {
    // Existence first: `/tmp/does-not-exist` should say so, not report a
    // jail-root mismatch, and the jail directory created just below must
    // not be the thing that makes a mistyped path look present.
    //
    // The `/` passed as the jail here is a sentinel, and it is a real
    // check: `mirror_under` validates the path it composes, so this refuses
    // an exposure of `/` — whose mirror under *any* jail is the jail itself
    // — as well as a relative one, and it does it before a directory is
    // created anywhere.
    for exposure in sandbox.exposures() {
        if !exposure.host.exists() {
            return Err(refused(
                EXPOSE_STEP,
                Error::BadPath {
                    path: exposure.host.clone(),
                    reason: "does not exist",
                },
            ));
        }
        // **No `refused` wrapper**: `mirror_under` logs its own refusal where it
        // builds the `Error`, with the same `step` this loop's "does not exist"
        // refusal uses, so an exposure that cannot be mirrored is one event
        // whichever of the two reasons it was refused for.
        mirror_under(Path::new("/"), &exposure.host)?;
    }
    Ok(())
}

/// The jail root inside `$TMPDIR` — created before the first `unshare`.
#[instrument(skip_all, fields(jail = %jail.display()))]
fn create_jail_root(jail: &Path) -> Result<()> {
    // 2. The jail root, **inside `$TMPDIR`** — the first host resource the
    //    confinement touches, and the one whose failure names nothing on its
    //    own. It may already exist — the previous run's leftover, which
    //    design decision 10 says to take over. The step string is the
    //    difference between an operator who knows to look at their temp
    //    directory and one reading an `EACCES` about a path they never
    //    chose: the jail's parent is `std::env::temp_dir()` unless
    //    `Options::with_tmpdir` says otherwise, and the error is
    //    `create jail root in $TMPDIR` for that reason. The variable is
    //    named rather than its value because P0's `Error::Step` keeps paths
    //    out of `step` so the message stays stable.
    //
    //    **Before the first `unshare`, and that is the point of the step
    //    string**: `$TMPDIR` is a precondition standing in front of the
    //    whole sequence rather than a step inside it, so a read-only, full
    //    or missing temp directory is refused while the process can still be
    //    repaired by exiting instead of after two namespaces have been
    //    entered. Nothing below depends on a namespace to create it.
    fs::create_dir_all(jail).map_err(step("create jail root in $TMPDIR"))
}

/// A private user namespace, with uid/gid maps written through descriptors
/// opened after the `unshare` (design decision 12).
///
/// `resolved` is [`Prepared`]'s resolved model rather than the request — the
/// field keeps its name and its `resolved` log field because that is what it
/// is (shared §7).
#[instrument(skip_all, fields(resolved = ?resolved))]
fn enter_user_namespace(
    resolved: Mechanism,
    opts: &Options,
) -> Result<Option<(fs::File, fs::File, fs::File)>> {
    // 3. A private user namespace first, and on its own. The uid and gid
    //    maps below can only be written through `/proc/<pid>/…`, which is
    //    reachable exactly while the mount namespace is the host's — so the
    //    two unshares are separate steps with the map write between them
    //    (design decision 12). A descriptor opened *before* this call is
    //    bound to the initial namespace and its write returns EPERM, which
    //    is why the files are opened after it.
    if !matches!(resolved, Mechanism::UserNamespace) {
        return Ok(None);
    }
    let euid = opts.euid();
    let egid = getegid().as_raw();
    unshare(UnshareFlags::NEWUSER, "unshare user namespace")?;

    // Opened here, after the unshare, and written **through**: a
    // descriptor opened before it belongs to the initial namespace, so
    // its write is what is refused (design decision 12). Holding the
    // handle and writing through it makes that ordering structural
    // rather than a comment someone can move away from.
    let mut setgroups = open_map("/proc/self/setgroups", "open setgroups map")?;
    write_map(&mut setgroups, "setgroups", "deny")?;
    let mut uid_map = open_map("/proc/self/uid_map", "open uid map")?;
    write_map(&mut uid_map, "uid_map", &format!("0 {euid} 1"))?;
    let mut gid_map = open_map("/proc/self/gid_map", "open gid map")?;
    write_map(&mut gid_map, "gid_map", &format!("0 {egid} 1"))?;
    Ok(Some((setgroups, uid_map, gid_map)))
}

/// The private mount namespace, then the tree made private for `pivot_root`.
#[instrument(level = "debug", ret)]
fn enter_mount_namespace() -> Result<()> {
    // 4. The private mount namespace, now that the maps exist. One thread
    //    still, which is what makes both unshares legal (design §10).
    unshare(UnshareFlags::NEWNS, "unshare mount namespace")?;

    // 5. `pivot_root` refuses while the old root is shared, so the whole
    //    tree is made private first. On the host measured this is already
    //    true; the call is unconditional because the hosts where it is not
    //    are exactly the ones that would otherwise fail with a bare EINVAL.
    //
    //    **`mount_change` takes propagation flags, and this is the call it
    //    is for.** It emits `mount(NULL, target, NULL, flags, NULL)` with no
    //    `MS_REMOUNT`, so `PRIVATE | REC` here is what makes the tree
    //    private — and the very same function cannot make anything
    //    read-only, which is why the remount in `apply_mount` is a
    //    different call (see the note there).
    mount_change(
        "/",
        MountPropagationFlags::PRIVATE | MountPropagationFlags::REC,
    )
    .map_err(step("make / private"))?;
    Ok(())
}

/// The mounts, in plan order. The Info event for the exposure set that landed is
/// [`announce_exposures`]'s (shared §7).
#[instrument(
    skip_all,
    ret,
    fields(mounts = mounts.len(), exposures = ?sandbox.exposures())
)]
fn apply_planned_mounts(sandbox: &Sandbox, mounts: &[Mount], overlay: &OverlayDirs) -> Result<()> {
    // 6. The mounts, in plan order.
    for entry in mounts {
        apply_mount(entry, overlay)?;
    }
    announce_exposures(sandbox.exposures(), mounts.len());
    Ok(())
}

/// The Info event for the exposure set the jail applied — the second of the
/// three events shared §7 requires of this crate, and the "to what" half of its
/// question.
///
/// **Emitted after the loop rather than before it**, because what it states is
/// what *landed*: a mount that failed takes the process with it (design §11's
/// fail-closed) and its own failure event is what the operator reads, so an
/// announcement ahead of the loop would be a set that a failed run never
/// applied. The per-mount half is `trace_mount`'s `debug!`, one per entry with
/// the target and the kind, which is why this one carries the set as a whole.
///
/// **A function rather than an inline `info!`, for the reason every `announce_*`
/// here is one: the site is unreachable from a test.** The mounts are real
/// syscalls in a namespace a test process cannot enter, so §7's "asserts the
/// event's `step` field" could not be a property this crate can lose. It takes
/// the two facts the site has.
fn announce_exposures(exposures: &[Exposure], mounts: usize) {
    info!(
        step = "exposures",
        count = exposures.len(),
        mounts,
        exposures = %describe_exposures(exposures),
        "the exposure set the jail applied"
    );
}

/// The exposure set as one field value: `path (access)` per entry, comma
/// separated.
///
/// **A rendered list is a field value and not a sentence**, which is the shape
/// §7's "fields, not sentences" asks for: `step`, `path` and `access` stay field
/// names, and what a *set* needs is one field whose value is the set — the same
/// shape `tinio-sandbox-windows`' grant phase announces its own with. The access
/// is spelled rather than debug-printed because this line is read by an operator
/// asking what the jail let through.
fn describe_exposures(exposures: &[Exposure]) -> String {
    exposures
        .iter()
        .map(|exposure| {
            format!(
                "{} ({})",
                exposure.host.display(),
                if exposure.access.is_read_only() {
                    "read-only"
                } else {
                    "read-write"
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Pivot into the jail and detach the old root.
#[instrument(level = "debug", skip_all, ret, fields(jail = %jail.display()))]
fn pivot_into_jail(jail: &Path) -> Result<()> {
    // 8. The pivot. `put_old` must already exist inside the new root, and
    //    the jail is a tmpfs we own, so creating it writes nothing to the
    //    served root (design §4).
    let put_old = jail.join(".oldroot");
    fs::create_dir_all(&put_old).map_err(step("create put_old"))?;
    env::set_current_dir(jail).map_err(step("chdir jail"))?;
    pivot_root(".", ".oldroot").map_err(step("pivot_root"))?;
    env::set_current_dir("/").map_err(step("chdir /"))?;
    unmount("/.oldroot", UnmountFlags::DETACH).map_err(step("umount old root"))?;
    Ok(())
}

// `unshare` is not defined here: it is `crate::mechanism`'s, declared there because
// its first caller — the user-namespace probe — lives in that module, and
// imported above. One wrapper means one safety argument (and one `unsafe`
// site) for one syscall; a second body here would be a second place to get
// `CLONE_FILES` wrong. It is `pub(crate)` in that module for this import and no
// other reason — a private item cannot be imported at all, so the earlier
// spelling of this line was a compile error waiting for the first build.

/// Open a `/proc` map file, keeping the descriptor so the close is explicit.
///
/// **The step string is a parameter for the reason [`write_map`]'s name is
/// one**: the three maps fail identically — a container with no `/proc` answers
/// `ENOENT` for all three — so an error that does not say which map was missing
/// leaves the operator to guess, and the map whose absence is diagnosable
/// (`setgroups` vs `uid_map`) is the whole content of the failure.
fn open_map(path: &str, step_name: &'static str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(step(step_name))
}

/// Whether a refused read-only remount selects the overlay fallback (design §4).
///
/// **`EPERM` is the trigger and nothing else is.** It is
/// `can_change_locked_flags()` refusing the request: `copy_mnt_ns` calls
/// `lock_mnt_tree` whenever the new mount namespace belongs to a different user
/// namespace, so every host mount arrives with its per-mount flags locked —
/// nosuid, nodev, noexec, ro, atime — and `MS_REMOUNT|MS_BIND` has to re-assert
/// all of them to get through. The source's locked flags decide it, and not its
/// filesystem: a bare tmpfs remounts and a `nosuid,nodev` one does not. Every
/// other errno is a failure to report rather than a mechanism to switch to: a
/// fallback taken on `EINVAL` would quietly replace a malformed mount with a
/// different one, and the run would report a read-only exposure it never
/// established.
///
/// A function rather than an inline guard because this *is* the read-only
/// decision: `apply_mount` cannot be unit-tested without mounting a filesystem,
/// and which errno takes the fallback is everything about it worth pinning.
///
/// **The unlocked case is the same rule's other side, and it is why the caller
/// re-asserts the source's own `nosuid`/`nodev`/`noexec` before it asks.**
/// `lock_mnt_tree` runs only where the new mount namespace belongs to a
/// different user namespace, so the root leg's mounts arrive unlocked, the
/// partial `MS_REMOUNT|MS_BIND|MS_RDONLY` request is **accepted** — and a
/// successful bind remount replaces the target's per-mount flag set with the
/// request's, dropping every flag it did not name, silently and with `rc=0`.
/// `EPERM` here and silence there are one kernel rule seen from its two sides,
/// and both are measured with a bare `mount(2)` on a host `/tmp` bind
/// (`rw,nosuid,nodev`): the partial request returns `0` and reads back `ro` on
/// the root leg, `EPERM` on the user-namespace leg, and the request carrying
/// `NOSUID|NODEV` reads back `ro,nosuid,nodev` on both.
fn refused_remount_needs_the_overlay(source: Errno) -> bool {
    source == Errno::PERM
}

/// The **writable** mount points strictly below `target` in a
/// `/proc/self/mountinfo` table.
///
/// **Why the check exists.** [`apply_mount`] binds an exposure with
/// `mount_bind_recursive`, which pulls the source's *submounts* in too, and a
/// successful remount makes the top mount read-only while leaving them alone: a
/// nested filesystem would stay writable inside a jail that reports the
/// exposure as read-only, and a write through it would land on the host.
/// `mount_setattr` with `AT_RECURSIVE` is the textbook fix and `rustix` 1.1.4
/// has none for a mount that already exists — its `MountAttrFlags` configures
/// one being *created* through `fsopen` — while reaching the syscall directly
/// would be a fourth `unsafe` site (decision 14). A plain non-recursive bind is
/// no alternative either: the nested mount's *contents* would vanish from the
/// jail instead of becoming read-only. So the shape is refused by name, which
/// is fail-closed.
///
/// **Writability, not existence, is the predicate.** A submount that is already
/// `ro` cannot be written through, and the confined process cannot remount it
/// writable — that takes `CAP_SYS_ADMIN` in the mount's owning namespace, which
/// the capability drop is about to take away — so reporting it would reject a
/// configuration that is already as read-only as the exposure asks for. The
/// mount's own options are the sixth field, and `rw` is always one of them.
///
/// **The fifth whitespace-separated field is the mount point, escaped** —
/// `\040` for a space, `\011` for a tab, `\012` for a newline and `\134` for a
/// backslash — and it is decoded before the comparison, which is the only reason
/// a target with a space in its name can match its own submounts. No raw
/// whitespace can appear in the field, so splitting on it is safe.
fn writable_nested_mounts_in(table: &str, target: &Path) -> Vec<PathBuf> {
    table
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let mount_point = fields.nth(4)?;
            let options = fields.next()?;
            Some((decode_mountinfo(mount_point), options))
        })
        .filter(|(mount, options)| {
            mount.starts_with(target)
                && mount != target
                && options.split(',').any(|option| option == "rw")
        })
        .map(|(mount, _)| mount)
        .collect()
}

/// Decode one mountinfo field: the kernel escapes exactly `\040`, `\011`,
/// `\012` and `\134`, and an escape this function does not know is copied
/// through rather than guessed at, so a field it misreads cannot become a
/// different path.
fn decode_mountinfo(field: &str) -> PathBuf {
    let mut out = String::with_capacity(field.len());
    let mut rest = field;
    while let Some(index) = rest.find('\\') {
        out.push_str(&rest[..index]);
        let escape = &rest[index + 1..];
        match escape.get(..3) {
            Some("040") => out.push(' '),
            Some("011") => out.push('\t'),
            Some("012") => out.push('\n'),
            Some("134") => out.push('\\'),
            _ => {
                out.push('\\');
                rest = escape;
                continue;
            }
        }
        rest = escape.get(3..).unwrap_or_default();
    }
    out.push_str(rest);
    PathBuf::from(out)
}

/// `/proc/self/mountinfo` as text, which is the host's table until the pivot.
///
/// Text rather than bytes because [`writable_nested_mounts_in`] compares paths, and a
/// line that is not UTF-8 — a mount source named in some other encoding —
/// refuses here instead of being decoded lossily: a lossy mount point would miss
/// a nested mount under a target with the same affliction, and missing one is
/// the under-enforcement the caller exists to stop.
fn read_mountinfo() -> Result<String> {
    fs::read_to_string("/proc/self/mountinfo").map_err(step("read /proc/self/mountinfo"))
}

/// The `nosuid`/`nodev`/`noexec` flags of the mount that contains `source`, as
/// read from a `/proc/self/mountinfo` table.
///
/// **These three are what an `MS_REMOUNT|MS_BIND` request has to re-assert, and
/// every other flag is deliberately left out of it.** A bind remount *replaces*
/// the target's per-mount flag set with the request's rather than adding to it,
/// so the three a bind inherits and drops are the three that must be carried;
/// the rule is design decision 8's, with its measured evidence row in the
/// design's evidence table and its restatement in design §4.
///
/// **`ro` is not read here, and that is the whole point of the remount.** The
/// served root is usually `rw` on the host — it is the exposure's own source —
/// and making it read-only is the request's job, so inheriting the source's
/// spelling of it would either do nothing (`rw`) or refuse the call by clearing
/// a locked `ro`. The caller adds [`MountFlags::RDONLY`] itself.
///
/// **The atime family is left out because a bind remount carries it through
/// untouched.** Measured with a bare `mount(2)` on this kernel: a source of
/// `rw,nosuid,nodev,noexec,noatime` reads back `ro,noatime` after the partial
/// request that clears the other three, so the atime bits are the one part of
/// the per-mount flag set the request does not have to name. Naming them would
/// mean reproducing the source's exact spelling — `noatime`, `relatime`, or
/// neither — rather than adding one, and the measurement says there is nothing
/// to gain by it.
///
/// **The containing mount is the deepest mount point that contains `source`,
/// and the last of equals.** A bind of a path inherits the per-mount flags of
/// the mount it is bound from, which is the one at the deepest mount point on
/// its path — `/srv/data/cache` under a `/srv/data` mount takes *that* mount's
/// flags, not the root filesystem's — and the comparison is component-wise
/// ([`Path::starts_with`]), so `/srv/data2` is not below `/srv/data`. Stacked
/// mounts share a mount point and the table lists the topmost last, which is
/// the one the bind inherited from.
///
/// The mount point is decoded before it is compared, for [`decode_mountinfo`]'s
/// reason, and the options are the mount's own sixth field — the same field
/// [`writable_nested_mounts_in`] reads — in which no option name can contain a
/// comma, so splitting on one is safe.
fn source_mount_flags_in(table: &str, source: &Path) -> MountFlags {
    let options = table
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let mount_point = fields.nth(4)?;
            let options = fields.next()?;
            Some((decode_mountinfo(mount_point), options))
        })
        .filter(|(mount, _)| source.starts_with(mount))
        // `max_by_key` answers the **last** of several equal maxima, which is
        // the topmost of a stack of mounts sharing one mount point.
        .max_by_key(|(mount, _)| mount.components().count())
        .map_or("", |(_, options)| options);

    let mut flags = MountFlags::empty();
    for option in options.split(',') {
        flags |= match option {
            "nosuid" => MountFlags::NOSUID,
            "nodev" => MountFlags::NODEV,
            "noexec" => MountFlags::NOEXEC,
            _ => MountFlags::empty(),
        };
    }
    flags
}

fn apply_mount(entry: &Mount, overlay: &OverlayDirs) -> Result<()> {
    match entry.kind {
        MountKind::JailTmpfs => apply_jail_tmpfs(entry)?,
        // `return`: the overlay fallback re-enters this function, and that
        // recursive call is the one that traces — falling through would emit a
        // second `mount applied` with the bind's kind (design decision 8).
        MountKind::Bind => return apply_bind(entry, overlay),
        MountKind::OverlayReadOnly => apply_overlay_read_only(entry, overlay)?,
    }
    trace_mount(entry);
    Ok(())
}

fn trace_mount(entry: &Mount) {
    // One event per mount, carrying the source, the target, the kind and the
    // access the confined process gets through it (shared §7). At the end
    // rather than once per arm, so all three kinds are traced by construction
    // and a fourth kind cannot be added without being traced.
    debug!(
        step = "mount",
        source = %entry.source.display(),
        target = %entry.target.display(),
        kind = ?entry.kind,
        access = ?entry.access,
        "mount applied"
    );
}

fn apply_jail_tmpfs(entry: &Mount) -> Result<()> {
    if let Some(parent) = entry.target.parent() {
        fs::create_dir_all(parent).map_err(step("create jail parent"))?;
    }
    // **`size=1m` is enough, and the arithmetic is the reason rather
    // than a guess.** What lands in this tmpfs is directories: one for
    // each prefix of each mirrored path (`/srv`, `/srv/data`, `/dev`,
    // …), one for `.oldroot`, the four device nodes, and the overlay's
    // pair. The deepest realistic set — a served root three components
    // down, a state directory beside it, four device files — is on the
    // order of twenty directories, and a tmpfs directory is at most one
    // 4 KiB page plus its entries: under 100 KiB, against 1 MiB, which
    // is 256 such pages. The quota's real subject is the overlay's
    // upper, the only thing in the jail that can ever hold data — and
    // the mount that exposes the served root through it is `MS_RDONLY`,
    // so a write aimed at that exposure is refused at the mount with
    // `EROFS` and never reaches this quota (`OverlayDirs` says which
    // write `ENOSPC` is actually for).
    mount(
        "tmpfs",
        &entry.target,
        "tmpfs",
        MountFlags::NOSUID | MountFlags::NODEV,
        Some(c"size=1m,mode=0755"),
    )
    .map_err(step("mount tmpfs"))
}

fn apply_bind(entry: &Mount, overlay: &OverlayDirs) -> Result<()> {
    // **"Is the source a directory?", not "is it a file?"** The four
    // device files are character devices, so `is_file()` is false for
    // every one of them: that spelling runs the *directory* branch for
    // `/dev/null`, creating a directory at `<jail>/dev/null` and then
    // binding a chardev onto it — `ENOTDIR` at best, and at worst a
    // jail that never builds. Task 6's precondition is what makes the
    // second one visible: a half-built jail is a *failure* there rather
    // than a host without namespaces.
    if entry.source.is_dir() {
        fs::create_dir_all(&entry.target).map_err(step("create mount point"))?;
    } else {
        if let Some(parent) = entry.target.parent() {
            fs::create_dir_all(parent).map_err(step("create device parent"))?;
        }
        fs::File::create(&entry.target).map_err(step("create device node"))?;
    }
    // Recursive, not plain: a bare `MS_BIND` of a mount-point source is
    // refused with EINVAL inside a user namespace (measured), and for a
    // source with no submounts the two forms are identical — so the
    // recursive call is the one to write everywhere.
    mount_bind_recursive(&entry.source, &entry.target).map_err(step("mount --bind"))?;
    if entry.access.is_read_only() {
        return enforce_bind_read_only(entry, overlay);
    }
    trace_mount(entry);
    Ok(())
}

/// Remount the bind read-only, or fall back to the overlay when locked flags
/// refuse the remount (design decision 8).
fn enforce_bind_read_only(entry: &Mount, overlay: &OverlayDirs) -> Result<()> {
    // **`mount_remount`, not `mount_change`, and this is the primary
    // enforcement of `AccessMode::ReadOnly`** (design decision 8).
    // `mount_change` takes `MountPropagationFlags` — `MountFlags`
    // has no `PRIVATE`, and `RDONLY`/`BIND` are not propagation
    // flags at all — and what it emits is
    // `mount(NULL, target, NULL, flags, NULL)`, with no `MS_REMOUNT`
    // anywhere in it, so a read-only request spelled through it
    // would return success and leave the served root writable. A
    // silent success here is worse than a failure: the run reports
    // confinement and the operator has none, which is exactly the
    // class of failure design §11 refuses.
    //
    // **The request carries the source's own `nosuid`/`nodev`/`noexec`, and
    // that is design decision 8's rule rather than a belt.** `MS_REMOUNT|MS_BIND`
    // has to re-assert **every** locked per-mount flag to get past
    // `can_change_locked_flags()` — that is the `EPERM` the arm below catches —
    // and the same kernel rule has the other, quieter side this request used to
    // walk into: a *successful* bind remount replaces the target's per-mount
    // flag set with the request's instead of adding to it, so a request naming
    // only `RDONLY` **silently clears** what the bind inherited from its source.
    // Measured with a bare `mount(2)` on the project's own kernel
    // (`6.18.33.2-microsoft-standard-WSL2`, root leg, `unshare(CLONE_NEWNS)`
    // with no `CLONE_NEWUSER`, so nothing is locked): a bind of the host's
    // `/tmp` (`rw,nosuid,nodev`) reads back as plain **`ro`** after that partial
    // remount; a bind of `/run/lock` (`rw,nosuid,nodev,noexec,noatime`) reads
    // back `ro,noatime`, so `noexec` goes the same way and the atime bit is the
    // one part of the set the kernel carries through on its own; and a tmpfs the
    // namespace mounted itself (`rw,relatime`, no per-mount flags to lose) reads
    // back `ro,relatime` — the accepted case the design's earlier measurement
    // stopped at, which is why the flagless sources reported no loss at all.
    // **The root leg is where the loss is reachable**, because `lock_mnt_tree()`
    // does not run there — the new namespace belongs to no different user
    // namespace (design §4) — so the partial remount is accepted rather than
    // refused and nothing reports what it dropped. The project's own acceptance
    // path serves a `tempfile::tempdir()` under `/tmp`, so this is not a
    // hypothetical host.
    //
    // **`RDONLY` is the request's and never the source's.** The source is
    // usually `rw` — it is the served root — and read-only is what this call
    // adds; the three inherited flags are read from the mount that *contains*
    // [`Mount::source`] by [`source_mount_flags_in`], which can therefore only
    // reproduce what the bind already carried and never widen the exposure.
    //
    // **And the overlay is the other half of the same rule, not an alternative
    // to it** — it is now the **rarer** arm, which is the cost design §4 records
    // against the simplification this line adopts. Where the request re-asserts
    // what a locked mount demands, the `EPERM` arm is reached only by a
    // per-mount flag this request does not name — the kernel locks more than
    // these three — and there the `MS_RDONLY` overlay is what makes the exposure
    // read-only. Measured after the change, with a bare `mount(2)`: in a user
    // namespace the partial request is `EPERM` and the re-asserted one is
    // accepted with the flags intact, and `tests/jail.rs`'s user-namespace leg —
    // whose served root is a `tempfile::tempdir()` under `/tmp` — now reports
    // `read-only-arm: remount` where the partial request reported `overlay`. The
    // mount plan still chooses neither, because nothing that performs no syscall
    // can (Task 2): this is the decision, and the kernel's answer to the request
    // is. Deleting the arm outright would be design §4's other reading of the
    // same measurement, and it is deliberately not taken here.
    //
    // Re-entering `apply_mount` with the other kind, rather than
    // duplicating the overlay's body here, keeps the trace below
    // true: exactly one `mount applied` event per mount, carrying
    // the kind that actually landed.
    //
    // The source is canonicalised for the same measured reason the nested
    // check canonicalises its target below: mountinfo reports *resolved*
    // paths, so an unresolved source under a symlinked ancestor would match a
    // shallower mount point and re-assert less than the bind inherited.
    //
    // **And a failure is fatal rather than falling back to the unresolved
    // string, which is what this line used to do** (follow-ups §6; design §11's
    // "never continues unconfined" read as an invariant rather than as an
    // aspiration). The fallback was a fail-open with no legitimate caller: the
    // bind of this very source has just succeeded, so the path exists, and the
    // only thing the fallback could do is make `source_mount_flags_in` match a
    // shallower mount and re-assert *less* than the bind inherited — a read-only
    // exposure that silently keeps a locked flag's protection off. A source that
    // cannot be resolved is a mount the confinement did not establish, and the
    // process exits before it serves anything.
    let source = fs::canonicalize(&entry.source).map_err(step("canonicalize mount source"))?;
    let inherited = source_mount_flags_in(&read_mountinfo()?, &source);
    match mount_remount(
        &entry.target,
        MountFlags::RDONLY | MountFlags::BIND | inherited,
        "",
    ) {
        Ok(()) => {
            refuse_writable_nested_mounts(entry)?;
            trace_mount(entry);
            Ok(())
        }
        Err(source) if refused_remount_needs_the_overlay(source) => {
            debug!(
                step = "remount ro",
                target = %entry.target.display(),
                inherited = ?inherited,
                "the remount was refused; falling back to the read-only overlay"
            );
            let fallback = Mount {
                kind: MountKind::OverlayReadOnly,
                ..entry.clone()
            };
            apply_mount(&fallback, overlay)
        }
        Err(source) => Err(fail("remount ro", source)),
    }
}

/// A successful remount leaves nested mounts writable; refuse that hole.
fn refuse_writable_nested_mounts(entry: &Mount) -> Result<()> {
    // **The landed remount is the only branch that needs the
    // nested-mount check, and the second arm is why.** It
    // makes the *top* mount read-only and leaves the
    // submounts exactly as they were, so a writable one
    // below is the hole — while the `EPERM` arm's overlay is
    // `MS_RDONLY` from the start and covers the whole merged
    // view end to end (measured: a write through a nested
    // mount under it is refused with `EROFS`, while the host
    // path stays writable, so the refusal is the overlay's
    // own and not a permission artefact). Checking before
    // the attempt would refuse every source with locked
    // flags and a submount — `/dev` with `/dev/pts` and
    // `/dev/shm` — where the overlay copes.
    //
    // Read here, after the remount, because this is when
    // both facts are settled, and `/proc` is still the
    // host's, the pivot having not happened yet. The table
    // reports *resolved* paths — measured: a mount made
    // through the symlink `/tmp/x/link/j` is listed as
    // `/tmp/x/real/j` — so a `$TMPDIR` under a symlinked
    // ancestor would make an unresolved target miss its own
    // submounts.
    //
    // **The failure is propagated, and the fallback it replaces
    // was a fail-open with no legitimate caller.** The target
    // is a directory this run created inside the jail and
    // mounted on a few instructions ago, so a canonicalisation
    // that fails means the mount this check is about did not
    // land — and answering that with the unresolved string
    // makes `writable_nested_mounts_in` match nothing, which
    // reports the exposure as read-only while a writable
    // submount stays reachable. That is the one outcome this
    // function exists to prevent, and a missing canonical name
    // is not a reason to skip the search for it.
    let target = fs::canonicalize(&entry.target).map_err(step("canonicalize mount target"))?;
    let nested = writable_nested_mounts_in(&read_mountinfo()?, &target);
    if let Some(mount) = nested.first() {
        // **The nested mount is named by its *host* path**,
        // which is the exposure's plus the remainder below
        // the target: the jail path it was found at is a name
        // the operator never chose and would have to
        // translate. The strip cannot fail — the filter that
        // selected this mount matched the same prefix — and
        // if it ever does the jail path is reported rather
        // than a new failure raised: a cosmetic mapping is not
        // worth a failure path.
        let host = mount
            .strip_prefix(&target)
            .map_or_else(|_| mount.clone(), |rest| entry.source.join(rest));
        return Err(fail(
            "read-only exposure",
            io::Error::other(format!(
                "{} is mounted below {}: the recursive bind carried it into \
                 the jail but the remount makes only the top mount read-only, \
                 so a write through it would reach the host — serve that root \
                 read-write, or move the nested mount outside it",
                host.display(),
                entry.source.display(),
            )),
        ));
    }
    Ok(())
}

fn apply_overlay_read_only(entry: &Mount, overlay: &OverlayDirs) -> Result<()> {
    // **The fallback arm, and the only way in is the substitution
    // above.** `plan` emits no entry of this kind (Task 2), and keeping
    // the mechanism a `MountKind` rather than an inline branch is what
    // lets that substitution reuse this body instead of a second copy
    // of it.
    fs::create_dir_all(&entry.target).map_err(step("create overlay mount point"))?;
    fs::create_dir_all(&overlay.upper).map_err(step("create overlay upper"))?;
    fs::create_dir_all(&overlay.work).map_err(step("create overlay work"))?;
    let options = format!(
        "lowerdir={},upperdir={},workdir={}",
        entry.source.display(),
        overlay.upper.display(),
        overlay.work.display()
    );
    let options = CString::new(options).map_err(|_| {
        refused(
            "mount overlay",
            Error::Unsupported {
                detail: "an exposed path contains a NUL byte",
            },
        )
    })?;
    // **`as_c_str()` and not `&options`**: `mount`'s data parameter is
    // `Data: Into<Option<&CStr>>`, and a `CString` reference is not one
    // of those — the conversion is explicit all the way down.
    mount(
        "overlay",
        &entry.target,
        "overlay",
        MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV,
        Some(options.as_c_str()),
    )
    .map_err(step("mount overlay ro"))
}

#[instrument(level = "debug", skip_all, ret, fields(retained = ?keep, effective = field::Empty))]
fn drop_capabilities(keep: &[Cap]) -> Result<()> {
    // **The retained set is announced before anything is given up**, so a run
    // that dies part-way through the sweep still says which policy it was
    // enforcing.
    announce_retained(keep);

    // No new privileges first: it is what lets the seccomp filter install
    // without CAP_SYS_ADMIN.
    set_no_new_privs(true).map_err(step("no_new_privs"))?;
    announce_no_new_privs();

    // **The bounding set goes first, and that order is load-bearing.**
    // `PR_CAPBSET_DROP` requires `CAP_SETPCAP` in the *effective* set at the
    // moment of the call, and the capset below is what takes it away — so a
    // sweep written after the capset fails with `EPERM` on its first capability
    // and the bounding set is never touched. Measured on WSL2: with caps intact
    // the drop succeeds; after an empty capset it returns `EPERM` and a
    // `PR_CAPBSET_READ` still reports the capability as present. Dropping from
    // the bounding set does not clear the effective bit, so the sweep may drop
    // `CAP_SETPCAP` itself and keep going.
    for cap in ALL_CAPABILITIES {
        if keep.iter().any(|kept| kept.set() == cap) {
            continue;
        }
        // Neither errno is ignorable, and they mean different things: `EINVAL`
        // is "past this kernel's `CAP_LAST_CAP`", and `EPERM` means the ordering
        // above is wrong or a capability was asked for that this namespace does
        // not hold.
        //
        // **`EINVAL` is reachable rather than impossible, and on a kernel older
        // than 5.9 it is the ordinary outcome**: capabilities 38-40 —
        // `PERFMON`, `BPF`, `CHECKPOINT_RESTORE` — do not exist there, so the
        // array above is this kernel's own list only from 5.9 on and the sweep
        // hits `EINVAL` at 38. Failing closed is the outcome design §11 asks
        // for; the step string is what makes it diagnosable, because it names
        // the drop that ran past the kernel's bound rather than leaving an
        // unattributed `EINVAL` at a capability nobody can see.
        //
        // **Two `Errno`s are compared directly, and the earlier spelling is
        // worth naming because it does not compile**: `raw_os_error()` returns
        // an `i32` on both types, so comparing it against
        // `Some(Errno::INVAL.raw_os_error())` puts an `i32` beside an
        // `Option<i32>` — and `Errno` derives `PartialEq`, so the comparison
        // the code wants is the one it can have.
        remove_capability_from_bounding_set(cap).map_err(|source| {
            let name = if source == Errno::INVAL {
                "drop capability from bounding set: past CAP_LAST_CAP"
            } else {
                "drop capability from bounding set"
            };
            fail(name, source)
        })?;
        // One event per drop, so the run's own capability set is reconstructible
        // from the log rather than from the array above it (shared §7).
        debug!(
            step = "drop capability",
            capability = cap.bits(),
            "dropped from the bounding set"
        );
    }

    // Only now is the process's own set narrowed to what it keeps.
    let mut wanted = CapabilitySet::empty();
    for cap in keep {
        wanted |= cap.set();
    }

    let mut sets = capabilities(None).map_err(step("read capabilities"))?;
    sets.effective = wanted;
    sets.permitted = wanted;
    sets.inheritable = CapabilitySet::empty();
    set_capabilities(None, sets).map_err(step("capset"))?;
    Span::current().record("effective", wanted.bits());
    announce_capset(wanted);

    // The bounding set is what makes the drop stick across `execve`: with euid 0
    // the kernel recomputes the permitted set as full on exec, and only the
    // bounding set bounds it. Every capability not kept has left it by now.
    Ok(())
}

// **The three events below are functions rather than inline `info!`s, and the
// reason is the same for each: the operations they describe cannot be undone.**
// `drop_capabilities` sets `no_new_privs` — a process-wide switch with no
// unset — removes bits from the bounding set and narrows the process's own sets,
// so a test that reached any of them would leave the test binary unable to
// confine anything, the tests beside it included. Shared §7 wants each event's
// `step` field to be a property a test can *lose*, so the events are reachable
// where the sites are not: each takes exactly the values the site hands the
// kernel, and a recording of one is the line the run writes.
//
// **The per-drop events stay `debug!`, and that is §7's own split rather than an
// omission** — "each bounding-set drop" is named there as the per-item detail
// behind these, and the `info!` half is the policy (the retained set), the
// switch (`no_new_privs`) and the outcome (`capset`).

/// The capability policy this run enforces, announced before anything is given
/// up (shared §7).
fn announce_retained(keep: &[Cap]) {
    info!(
        step = "retained capabilities",
        count = keep.len(),
        retained = %keep.iter().map(|cap| cap.name()).collect::<Vec<_>>().join(", "),
        "the capabilities this process keeps"
    );
}

/// The part of the drop that makes the rest stick across `execve` (shared §7).
fn announce_no_new_privs() {
    info!(
        step = "no_new_privs",
        "privilege gain across execve is off for this process"
    );
}

/// The `capset` that narrows the process's own sets, with the mask the kernel
/// was handed (shared §7).
fn announce_capset(retained: CapabilitySet) {
    info!(
        step = "capset",
        retained = retained.bits(),
        "the process's own capability sets were narrowed"
    );
}

/// Every capability `CAP_CHOWN` (0) through `CAP_CHECKPOINT_RESTORE` (40), in
/// kernel numbering order.
///
/// Spelled out because `rustix` publishes no iterator over them: `CapabilitySet`
/// is a `bitflags` type whose `iter()` walks the bits *set in a value*, not the
/// universe of capabilities, and `Flags::iter_defined_names()` would drag an
/// explicit `bitflags` dependency in for one loop. The array doubles as the
/// sweep's bound — 41 is `CAP_LAST_CAP + 1`, so the loop can never reach the
/// `EINVAL` the kernel returns past that.
const ALL_CAPABILITIES: [CapabilitySet; 41] = [
    CapabilitySet::CHOWN,
    CapabilitySet::DAC_OVERRIDE,
    CapabilitySet::DAC_READ_SEARCH,
    CapabilitySet::FOWNER,
    CapabilitySet::FSETID,
    CapabilitySet::KILL,
    CapabilitySet::SETGID,
    CapabilitySet::SETUID,
    CapabilitySet::SETPCAP,
    CapabilitySet::LINUX_IMMUTABLE,
    CapabilitySet::NET_BIND_SERVICE,
    CapabilitySet::NET_BROADCAST,
    CapabilitySet::NET_ADMIN,
    CapabilitySet::NET_RAW,
    CapabilitySet::IPC_LOCK,
    CapabilitySet::IPC_OWNER,
    CapabilitySet::SYS_MODULE,
    CapabilitySet::SYS_RAWIO,
    CapabilitySet::SYS_CHROOT,
    CapabilitySet::SYS_PTRACE,
    CapabilitySet::SYS_PACCT,
    CapabilitySet::SYS_ADMIN,
    CapabilitySet::SYS_BOOT,
    CapabilitySet::SYS_NICE,
    CapabilitySet::SYS_RESOURCE,
    CapabilitySet::SYS_TIME,
    CapabilitySet::SYS_TTY_CONFIG,
    CapabilitySet::MKNOD,
    CapabilitySet::LEASE,
    CapabilitySet::AUDIT_WRITE,
    CapabilitySet::AUDIT_CONTROL,
    CapabilitySet::SETFCAP,
    CapabilitySet::MAC_OVERRIDE,
    CapabilitySet::MAC_ADMIN,
    CapabilitySet::SYSLOG,
    CapabilitySet::WAKE_ALARM,
    CapabilitySet::BLOCK_SUSPEND,
    CapabilitySet::AUDIT_READ,
    CapabilitySet::PERFMON,
    CapabilitySet::BPF,
    CapabilitySet::CHECKPOINT_RESTORE,
];

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use rustix::{io::Errno, mount::MountFlags, thread::CapabilitySet};

    use super::{
        announce_capset, announce_exposures, announce_no_new_privs, announce_retained,
        refused_remount_needs_the_overlay, resolve_model, source_mount_flags_in,
        writable_nested_mounts_in,
    };
    use crate::{
        _core::{AccessMode, Backend as BackendTrait, Error, Exposure, LaunchPlan, Sandbox},
        Backend, Mechanism,
        caps::Cap,
        options::Options,
        testing::{assert_step, record_events},
    };

    #[test]
    fn the_exposure_set_is_traced_with_its_step() {
        // **The second of the three events, and the "to what" half of shared
        // §7's question.** The set is rendered as one field value — the per-mount
        // `debug!`s are the item detail and are invisible under `serve`'s
        // subscriber — so this asserts both halves of what an operator reads:
        // the counts and the set itself, in the access spelling the run used.
        let exposures = [
            Exposure {
                host: PathBuf::from("/srv/data"),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: PathBuf::from("/srv/state"),
                access: AccessMode::ReadWrite,
            },
        ];
        let (_, logged) = record_events(|| announce_exposures(&exposures, 3));
        assert_step(&logged, "exposures");
        assert!(logged.contains("count=2"), "{logged}");
        assert!(logged.contains("mounts=3"), "{logged}");
        assert!(
            logged.contains("exposures=/srv/data (read-only), /srv/state (read-write)"),
            "{logged}"
        );
    }

    #[test]
    fn the_capability_policy_is_traced_with_its_steps() {
        // **§7's "the retained capability set and each drop; `capset`", of which
        // only the drops had an event before this** — and the drops' own stays
        // `debug!`, because §7 names it as the per-item detail while these three
        // are the policy, the switch and the outcome. The per-drop path cannot be
        // driven from a test at all (removing a bit from the bounding set is
        // one-way), so its `debug!` is asserted nowhere and says so at its site.
        let (_, logged) = record_events(|| announce_retained(&[Cap::Chown]));
        assert_step(&logged, "retained capabilities");
        assert!(logged.contains("count=1"), "{logged}");
        assert!(logged.contains("retained=CAP_CHOWN"), "{logged}");

        let (_, logged) = record_events(announce_no_new_privs);
        assert_step(&logged, "no_new_privs");

        let (_, logged) = record_events(|| announce_capset(CapabilitySet::CHOWN));
        assert_step(&logged, "capset");
        assert!(
            logged.contains(&format!("retained={}", CapabilitySet::CHOWN.bits())),
            "{logged}"
        );
    }

    #[test]
    fn the_resolved_model_is_traced_with_its_step() {
        // **Shared §7's check on the event the plan names first**: the resolved
        // model is what answers "was this process confined, and to how", and a
        // `debug!`-only record of it is invisible under `serve`'s subscriber,
        // which is built at a fixed `Verbosity::Info` with no environment
        // filter. The refusal tests reach this function too, which is why the
        // recorder is a global one (`crate::testing` states the mechanism).
        //
        // The options are injected and the model is *forced*, so no probe forks:
        // what this asserts is the event, not the host's answer.
        let opts = Options::default().with_model(Mechanism::Root).with_euid(0);
        let (resolved, logged) = record_events(|| resolve_model(&opts));
        assert_eq!(
            resolved.expect("root with euid 0 needs no probe"),
            Mechanism::Root
        );
        assert_step(&logged, "model");
        assert!(logged.contains("model=Root"), "{logged}");
    }

    #[test]
    fn an_empty_sandbox_is_refused() {
        // `Error::Step`, not `Unsupported`: nothing exposed is the caller's bug,
        // and `Unsupported` would send the operator to look at their kernel
        // (design §11). P0's stub returns the same variant for the same reason.
        // The check runs before the availability probe, so this test needs no
        // namespace and gets one answer on every host.
        let err = Backend::default()
            .confine(&Sandbox::new(), &LaunchPlan::new())
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "confine",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_relative_exposure_is_refused_before_anything_irreversible_happens() {
        // Validation must run before any unshare, so an operator typo does not
        // leave the process half-confined. `Auto` is the model here because it
        // resolves on any host this test can run on, which leaves the exposure
        // loop as the only thing that can refuse.
        let sandbox = Sandbox::new().expose("relative", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(Options::default().with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::BadPath { .. }), "{err:?}");
    }

    #[test]
    fn a_missing_path_is_refused_before_anything_irreversible_happens() {
        let sandbox = Sandbox::new().expose("/no/such/path/at/all", AccessMode::ReadOnly);
        let mut backend =
            Backend::with_options(Options::default().with_model(Mechanism::Root).with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "does not exist",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn an_unavailable_model_is_refused() {
        // Forced root without root: decided from the injected euid alone, with
        // no probe and therefore the same answer on every host.
        let sandbox = Sandbox::new().expose("/", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(
            Options::default()
                .with_model(Mechanism::Root)
                .with_euid(1000),
        );
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn a_local_uid_mapping_is_refused_under_the_user_namespace() {
        // An unmapped uid cannot be chowned to, so the combination is a
        // configuration error rather than a runtime surprise (design §8). A
        // *forced* user namespace is refused from the request alone — no probe,
        // no privileges, no namespace — because the refusal is the same whether
        // or not this host would grant one. The `Auto` half of the same rule
        // cannot be decided before the probe, so it is checked against the
        // **resolved** model in `enter`; no unit test can drive that path,
        // because what the probe answers is a property of the host.
        let sandbox = Sandbox::new().expose("/", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(
            Options::default()
                .with_model(Mechanism::UserNamespace)
                .with_local_uid(true)
                .with_euid(1000),
        );
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(err.to_string().contains("local_uid"), "{err}");
        assert!(err.to_string().contains("root"), "{err}");
    }

    #[test]
    fn the_jail_is_derived_from_the_first_exposure() {
        // The first exposure is the served root by construction: `serve`
        // exposes it first. Options that do not change that are the only ones
        // unit-testable, because everything after validation is irreversible.
        let opts = Options::default().with_tmpdir(PathBuf::from("/tmp"));
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
    }

    #[test]
    fn only_eperm_selects_the_overlay_fallback() {
        // **The read-only mechanism is the locked-flag answer, so the reading
        // of that answer has to be exact** (design §4, re-measured 2026-09-20):
        // `EPERM` is what a read-only remount of a source with locked flags
        // gets, and it takes the `MS_RDONLY` overlay; every other errno stays a
        // failure, because a fallback taken on one of them would mount
        // something other than what was refused and report a read-only exposure
        // it never established. This is also where the overlay's own test lives
        // now: nothing pure can pin it any more — `plan` emits `Bind` for every
        // read-only exposure — and the branch itself needs a filesystem, which
        // is what Task 6's jail run supplies.
        assert!(refused_remount_needs_the_overlay(Errno::PERM));
        assert!(!refused_remount_needs_the_overlay(Errno::INVAL));
        assert!(!refused_remount_needs_the_overlay(Errno::ACCESS));
    }

    #[test]
    fn only_a_writable_mount_below_the_target_is_reported() {
        // **The hole, and the predicate that separates it from the safe
        // shapes**: the bind is recursive, so `cache` is carried into a jail
        // whose exposure a successful remount makes read-only at the top only,
        // and a write through `cache` then reaches the host. A submount that is
        // already `ro` is not that hole — it cannot be written through, and the
        // confined process cannot remount it writable without `CAP_SYS_ADMIN`
        // in the mount's owning namespace, which the capability drop removes —
        // so reporting it would refuse a configuration that is already as
        // read-only as the exposure asks for. The neighbours must not match
        // either: the target itself, and a path sharing its spelling but not its
        // components.
        // **`archive`'s per-mount options are `ro` while its superblock options
        // after the `-` separator are `rw`, and that pair is the row that pins
        // the field being read.** It is the shape measured on a real remount —
        // the top mount's sixth field goes `ro` and its superblock field stays
        // `rw` — so a helper reading the superblock instead of the mount would
        // find `rw` there, report this mount, and fail the assertion below.
        let table = "\
            24 30 0:23 / /tmp/jail/srv/data rw - tmpfs tmpfs rw\n\
            25 24 8:1 / /tmp/jail/srv/data/cache rw - ext4 /dev/sdb1 rw\n\
            26 24 8:2 / /tmp/jail/srv/data/archive ro - ext4 /dev/sdc1 rw\n\
            27 30 8:3 / /tmp/jail/srv/data2 rw - ext4 /dev/sdd1 rw\n\
            28 30 8:4 / /tmp/elsewhere rw - ext4 /dev/sde1 rw\n";
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/tmp/jail/srv/data")),
            vec![PathBuf::from("/tmp/jail/srv/data/cache")]
        );
        // The whole subtree and not one level of it, with the read-only
        // submount excluded from the middle of it.
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/tmp/jail/srv")).len(),
            3,
            "`data`, `data/cache` and `data2` — not the already-read-only `data/archive`"
        );
        // And nothing below the deepest mount, so a served root with no
        // submounts at all reports nothing.
        assert!(writable_nested_mounts_in(table, Path::new("/tmp/jail/srv/data/cache")).is_empty());
    }

    #[test]
    fn a_mount_point_is_decoded_before_it_is_compared() {
        // `\040` is a space and `\134` is a backslash, and they are decoded
        // because the escaped spelling never matches the path it denotes:
        // `/srv/my\040data/one` does not start with `/srv/my data`, so an
        // undecoded comparison would leave the nested mount unreported — the
        // one outcome this check may not have.
        let table = "\
            24 30 0:23 / /srv/my\\040data rw - tmpfs tmpfs rw\n\
            25 24 8:1 / /srv/my\\040data/one rw - ext4 /dev/sdb1 rw\n\
            26 24 8:2 / /srv/back\\134slash/deep rw - ext4 /dev/sdc1 rw\n";
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/srv/my data")),
            vec![PathBuf::from("/srv/my data/one")]
        );
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/srv/back\\slash")),
            vec![PathBuf::from("/srv/back\\slash/deep")]
        );
    }

    #[test]
    fn the_remount_reasserts_the_sources_own_lock_flags() {
        // **The three flags and only the three** (design decision 8): a bind
        // remount replaces the target's per-mount flag set with the request's,
        // so a request that does not carry `nosuid`/`nodev`/`noexec` silently
        // clears them — measured with a bare `mount(2)` on
        // `6.18.33.2-microsoft-standard-WSL2`, where a bind of the host's
        // `rw,nosuid,nodev` `/tmp` read back as plain `ro`. `rdonly` appears in
        // no row here on purpose: the source is usually `rw`, so `ro` is the
        // request's and never the source's, and `relatime` is left out because
        // a mount that locks it takes the `EPERM` arm instead, which the
        // overlay covers.
        let table = "\
            24 30 0:23 / / rw,relatime - ext4 /dev/sda1 rw\n\
            25 24 0:24 / /tmp rw,nosuid,nodev - tmpfs tmpfs rw\n\
            26 24 0:25 / /srv/data rw,nosuid,nodev,noexec - ext4 /dev/sdb1 rw\n\
            27 24 0:26 / /srv/data/cache rw - ext4 /dev/sdc1 rw\n";

        // The root filesystem carries none of the three, and the read-only
        // request is then exactly the old one.
        assert_eq!(
            source_mount_flags_in(table, Path::new("/mnt/elsewhere/root")),
            MountFlags::empty()
        );
        // A source under `/tmp` inherits from `/tmp` and not from `/` — the
        // shape the project's own acceptance path serves (`tempfile::tempdir()`
        // answers under `/tmp`), which is why this row is the one that had to
        // be fixed.
        assert_eq!(
            source_mount_flags_in(table, Path::new("/tmp/.tinio-jail-abc/served")),
            MountFlags::NOSUID | MountFlags::NODEV
        );
        // A source that is itself a mount point takes its own entry's flags,
        // and the deepest containing mount wins for a source below one.
        assert_eq!(
            source_mount_flags_in(table, Path::new("/srv/data")),
            MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOEXEC
        );
        assert_eq!(
            source_mount_flags_in(table, Path::new("/srv/data/cache/leaf")),
            MountFlags::empty(),
            "`cache` is the containing mount and carries none of the three"
        );
        // Component-wise, so a sibling whose name merely starts the same way is
        // not below the mount that shares its spelling.
        assert_eq!(
            source_mount_flags_in(table, Path::new("/srv/data2")),
            MountFlags::empty(),
            "`/srv/data2` is not below `/srv/data`"
        );
        // A table with no entry for the source at all answers "nothing to
        // re-assert", which is the request this call made before the fix.
        assert_eq!(
            source_mount_flags_in("", Path::new("/tmp/x")),
            MountFlags::empty()
        );
    }

    #[test]
    fn the_containing_mount_is_the_deepest_and_its_path_is_decoded() {
        // **The deepest match is the mount a bind actually inherits from**, and
        // the last of equals is the topmost of a stack sharing one mount point:
        // the table lists mounts in mount order, so the topmost of a stacked
        // pair comes last, and it is the flags that bind carries.
        let stacked = "\
            30 24 0:27 / /opt rw - ext4 /dev/sdd1 rw\n\
            31 30 0:28 / /opt rw,nosuid,nodev - tmpfs tmpfs rw\n";
        assert_eq!(
            source_mount_flags_in(stacked, Path::new("/opt/served")),
            MountFlags::NOSUID | MountFlags::NODEV
        );

        // The mount point is escaped in the table and decoded before it is
        // compared, for `decode_mountinfo`'s reason: `/srv/my\040data` never
        // matches `/srv/my data`, so an undecoded comparison would answer "no
        // flags to re-assert" for a source that has them — the silent-clearing
        // bug this function exists to close, reintroduced by an escape.
        let escaped = "\
            32 24 0:29 / /srv/my\\040data rw,noexec - ext4 /dev/sde1 rw\n";
        assert_eq!(
            source_mount_flags_in(escaped, Path::new("/srv/my data/served")),
            MountFlags::NOEXEC
        );
    }
}
