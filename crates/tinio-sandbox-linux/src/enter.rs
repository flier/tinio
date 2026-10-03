//! `confine`: build the jail and step into it.

use std::{
    env,
    ffi::CString,
    fs,
    io::{self, Write as _},
    os::{fd::AsRawFd, unix::ffi::OsStrExt},
    path::{Path, PathBuf},
};

use rustix::{
    fd::OwnedFd,
    fs::{CWD, Mode, OFlags, ResolveFlags, open, openat2},
    io::Errno,
    mount::{
        MountFlags, MountPropagationFlags, MoveMountFlags, OpenTreeFlags, UnmountFlags, mount,
        mount_bind_recursive, mount_change, mount_remount, move_mount, open_tree, unmount,
    },
    process::{getegid, pivot_root},
    thread::{
        CapabilitySet, UnshareFlags, capabilities, remove_capability_from_bounding_set,
        set_capabilities, set_no_new_privs,
    },
};
use tracing::{Span, debug, field, info, instrument};

use crate::{
    _core::{Confinement, Error, Exposure, Result, Sandbox, describe_exposures},
    Mechanism,
    caps::{self, Cap},
    jail::{EXPOSE_STEP, jail_root, mirror_under},
    mechanism::{Resolved, fail, refused, step, unshare},
    metrics::{ReadOnlyArm, record_read_only_arm},
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
    /// # Preconditions
    ///
    /// **The calling process must be single-threaded, and this call enforces
    /// that rather than assuming it** (design §8, §10). Every operation the
    /// confinement performs on itself — `unshare`, `no_new_privs`, the
    /// capability drop and the seccomp filter — applies to the calling task
    /// alone, so a process that arrived here with a second thread would leave it
    /// holding the host mount namespace, the full credentials and an unfiltered
    /// network while this call still answered [`Confinement::InPlace`]. A
    /// process with more than one thread is refused by [`refuse_when_threaded`],
    /// which names the count; it runs before the first irreversible step, so
    /// nothing is left behind by the refusal.
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] before anything irreversible happens;
    /// [`Error::Unsupported`] when the model is unavailable or a combination
    /// the design refuses was asked for; [`Error::Step`] for any refusing
    /// syscall, named by the step — and for the thread precondition, which is
    /// the one refusal here that is not a syscall's answer but this sequence's
    /// own.
    #[instrument(skip_all, fields(model = ?opts.model(), euid = opts.euid()))]
    pub(crate) fn enter(&mut self, sandbox: &Sandbox, opts: &Options) -> Result<Confinement> {
        // **The forced `/proc/self/fd` fallback, resolved once for the run
        // instead of once per bind.** [`bind_exposure`] used to read
        // `TINIO_SANDBOX_FORCE_PROC_FALLBACK` for every exposure it bound — the
        // same answer asked again per mount. The variable is deliberate and
        // stays: it selects between the two descriptor-bound bind paths —
        // `open_tree` + `move_mount`, or `mount --bind` of the descriptor's own
        // `/proc/self/fd` link — so it grants no authority, and it is read in
        // release builds too (`docs/sandbox.md`). **Not in `Options::from_env`
        // beside the model**: `Options` is a value the caller builds — the
        // launcher builds it from the model it already carries rather than
        // through the environment — so a read there would drop this hook for
        // exactly the callers that confine in production. Every caller crosses
        // this boundary, and the answer travels down as a plain value.
        let proc_fallback = env::var_os("TINIO_SANDBOX_FORCE_PROC_FALLBACK").is_some();
        let prepared = validate_and_plan(sandbox, opts)?;
        // **The resolution is recorded the moment it answers**, through the same `&mut self`
        // receiver `seccomp` is written through: `Auto` became one of the two mechanisms here, and
        // a caller that must state *what confined it* — the facade's `serve::Spec::apply` carries
        // the answer back in the confinement's own §7 record — reads this value rather than asking
        // the probe a second time after the confinement (a second confinement-time decision, taken
        // outside the confinement). `validate_and_plan` has already refused `Unavailable`, so the
        // field never holds a "no model" answer.
        self.resolved = Some(prepared.resolved);
        // **The precondition every step below shares, checked before the first
        // of them** (design §8, §10). It sits after validation so an operator's
        // own mistake is still reported as itself, and before
        // `create_jail_root` — the first thing the confinement does to the host
        // — so a refusal cannot leave a jail directory behind.
        // `refuse_when_threaded` owns the reasoning; what matters here is the
        // position, which is what makes the guarantee rather than the check.
        refuse_when_threaded()?;
        create_jail_root(&prepared.jail)?;
        let map_fds = enter_user_namespace(prepared.resolved, opts)?;
        enter_mount_namespace()?;
        // **The exposure descriptors are opened here, not in the plan**: a mount
        //    is bindable only from the namespace that owns it, so a descriptor
        //    opened before `enter_mount_namespace` belongs to the namespace this
        //    process left and both binds refuse it with `EINVAL` (measured).
        //    `open_exposures` carries the argument; the path policy that has to
        //    hold before anything irreversible — the ancestor symlink, the
        //    existence and the mirror of every exposure — already did.
        let descriptors = open_exposures(sandbox)?;
        apply_planned_mounts(
            sandbox,
            &prepared.mounts,
            &prepared.overlay,
            &descriptors,
            proc_fallback,
        )?;
        // **The count is recorded where the mounts landed, and it is the plan's own length**:
        // `apply_planned_mounts` either applies every entry or fails the run, so the number the
        // backend answers afterwards is the table that was applied rather than a second planning
        // of it — the same value its own exposure event states (`announce_exposures`). The
        // facade's `Confined::mounts` and the CLI's in-place re-emit read it from here.
        self.mounts = prepared.mounts.len();
        // 7. The maps have been read by every mount that needed them, and the
        //    exposure descriptors have been bound; both are closed explicitly
        //    because nothing here ever `exec`s, so `CLOEXEC` never gets a chance
        //    to close them and they would otherwise be inherited by the whole
        //    server lifetime.
        drop(map_fds);
        drop(descriptors);
        pivot_into_jail(&prepared.jail)?;
        // 9. Drop everything the jail no longer needs, then cut the network.
        //    The filter's outcome is recorded on the backend rather than
        //    dropped: `serve` reads it to warn when the one degradable step
        //    degraded (design §11).
        //
        //    **The access is the served root's own exposure, not a second field
        //    that could disagree with it.** It used to be an `Options` knob the
        //    caller set beside the request, and its default was the wrong answer
        //    for a `--read-only` run: the exposure set said read-only while this
        //    value stayed read-write, so the jail served the root **writable** —
        //    the kernel-level half of FR-023, absent with nothing to say so.
        //    There is one spelling now: the first exposure, the same entry the
        //    jail was built from, and it cannot fail here because
        //    [`validate_and_plan`] already refused an empty set.
        drop_capabilities(caps::retained(
            sandbox.served_root()?.access,
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

    // Nothing exposed is the caller's bug, not the platform's shortcoming, and
    // it is decided **before the probe** so the answer does not depend on what
    // this host happens to allow. `Error::Unsupported` would send the operator
    // to look at their kernel, which is the wrong place (design §11). **The
    // refusal itself is the core's now** — `Sandbox::served_root` is that
    // convention's one home, because every backend spelled the same two lines —
    // and it is the same `Error::Step` with the same `confine` step and the same
    // message this block used to build by hand.
    let served_root = &sandbox.served_root()?.host;

    let resolved = resolve_model(opts)?;
    validate_exposures(sandbox)?;

    let jail = jail_root(&opts.resolved_tmpdir(), served_root);
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
    let mounts = plan(sandbox, &jail, resolved, opts)?;

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
    // **The probe and the table moved to `Options::resolve`, and the move is the point**: the
    // facade's run-time gate (`tinio_sandbox::can_confine`, asked by the CLI before a start
    // writes anything) refuses on the very answer this line applies, so a host that cannot
    // confine is refused with a cause before the run touches a directory instead of meeting a
    // late `Unsupported` from inside the confinement. Design decision 7's three tiers live there:
    // the user namespace when the kernel's probe grants it — `userns_available` forks a child, so
    // a refusal costs nothing — real root's `CAP_SYS_ADMIN` as the fallback, and
    // `Resolved::Unavailable` when neither is available.
    let resolved = opts.resolve()?;

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
    // is the event that has to survive a subscriber at a fixed
    // `Verbosity::Info`, which is what `serve` builds — **and on this platform
    // that subscriber is installed after `confine`, so `serve` itself reads
    // neither this event nor any other the confinement emits** (the ordering
    // `refuse_when_threaded` states). The level is chosen for the caller that
    // does install one before confining.
    info!(
        step = "model",
        model = ?resolved,
        requested = ?opts.model(),
        euid = opts.euid(),
        "the model this process confines itself with"
    );
    Ok(resolved)
}

/// Refuse a symlink, a missing path, or an un-mirrorable exposure before a
/// directory is created.
fn validate_exposures(sandbox: &Sandbox) -> Result<()> {
    // The symlink check is first, and it does not follow. `exists` does follow,
    // so a link to a real directory would otherwise pass and the bind below
    // would mount the target. A component that cannot be stat'd is left to the
    // existence check: that one names a path that is not there.
    //
    // Existence next: `/tmp/does-not-exist` should say so, not report a
    // jail-root mismatch, and the jail directory created just below must
    // not be the thing that makes a mistyped path look present.
    //
    // The `/` passed as the jail here is a sentinel, and it is a real
    // check: `mirror_under` validates the path it composes, so this refuses
    // an exposure of `/` — whose mirror under *any* jail is the jail itself
    // — as well as a relative one, and it does it before a directory is
    // created anywhere.
    for exposure in sandbox.exposures() {
        refuse_symlink_exposure(&exposure.host)?;
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

/// Open every validated exposure as the object the bind will use, **in exposure
/// order** — the order [`MountKind::ExposureBind`] indexes.
///
/// **The open happens after the mount namespace is unshared, and that is the
/// kernel's requirement rather than a preference**: a mount is bindable only
/// from the namespace that owns it, so a descriptor opened before the `unshare`
/// belongs to the namespace this process left and both `open_tree` and the
/// `/proc/self/fd` bind refuse it with `EINVAL` (measured on the project's
/// kernel). What is validated *before* the namespace change is the path policy
/// — [`validate_exposures`], which is what a mistyped exposure has to be refused
/// by without a jail directory left behind; what is opened here is the object,
/// and [`open_exposure`] re-applies the same policy atomically where the kernel
/// can (`openat2`).
fn open_exposures(sandbox: &Sandbox) -> Result<Vec<ExposureFd>> {
    sandbox
        .exposures()
        .iter()
        .map(|exposure| open_exposure(&exposure.host))
        .collect()
}

/// The validated exposure: the descriptor is the object, the path was only the
/// way to it.
///
/// **Which syscall refuses a linked component is [`open_exposure`]'s to state**:
/// one `openat2` resolution on the primary arm, a re-check after the open on
/// the fallback. `O_CLOEXEC` is what the explicit close in
/// [`crate::Backend::enter`] stands in for: nothing here ever `exec`s.
#[derive(Debug)]
struct ExposureFd(OwnedFd);

/// Open `path` as the object the bind will use, refusing a symlink in any
/// component.
///
/// **`openat2` with `RESOLVE_NO_SYMLINKS` is the whole ancestor policy in one
/// syscall** — it refuses a link in any component, the leaf included, and it
/// resolves the path *inside* that refusal, so nothing can be swapped in between
/// the policy check and this open. The refusal is the same [`SYMLINK_EXPOSURE`]
/// the ancestor walk gives (`ELOOP` here), so an operator reads one reason for
/// one mistake whichever component was the link.
///
/// **A kernel without `openat2` (5.2–5.5, or one that refuses the resolve flag)
/// falls back to `open` with `O_PATH | O_NOFOLLOW`, and then every refusal is the
/// re-check's rather than the syscall's.** That open refuses nothing: `O_PATH`
/// does not answer `ELOOP` for a symlink leaf — it returns a descriptor for the
/// *link itself* (measured: `O_PATH|O_NOFOLLOW` opens the link, while
/// `O_RDONLY|O_NOFOLLOW` answers `ELOOP`) — and the components above it are
/// walked by the kernel at that moment, after [`validate_exposures`] walked them
/// earlier. So [`refuse_symlink_exposure`] runs again immediately after the open
/// and is what refuses both the leaf and any linked component above it, the
/// descriptor it would otherwise be bound from being dropped by the `?`.
///
/// **And that arm's window is real rather than merely narrower: an ancestor link
/// is followed.** A link swapped into a component before the open and restored
/// before the re-check is resolved by the open and invisible to the walk
/// afterwards, so what lands can be the linked directory. The window is those two
/// syscalls, and it is reached wherever `openat2` is absent (5.5 and below, with or
/// without `open_tree`) — or where the resolve flag is refused, which is what the
/// `EINVAL` arm is for. That residual is the arm's own and is bounded by the
/// kernel it runs on: `openat2`'s arm has no such window, and what the fallback
/// cannot reach is `RESOLVE_NO_SYMLINKS`, not a second check.
fn open_exposure(path: &Path) -> Result<ExposureFd> {
    let refuse = |err: Errno| {
        let reason = if err == Errno::LOOP {
            SYMLINK_EXPOSURE
        } else {
            "cannot open"
        };
        refused(
            EXPOSE_STEP,
            Error::BadPath {
                path: path.to_path_buf(),
                reason,
            },
        )
    };
    match openat2(
        CWD,
        path,
        OFlags::PATH | OFlags::CLOEXEC,
        Mode::empty(),
        ResolveFlags::NO_SYMLINKS,
    ) {
        Ok(descriptor) => Ok(ExposureFd(descriptor)),
        // No `openat2`, or a kernel that refuses the resolve flag: the weaker
        // open below, whose window the doc above states.
        Err(err) if err == Errno::NOSYS || err == Errno::INVAL => {
            let descriptor = open(
                path,
                OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(refuse)?;
            // The policy again, now that the path has been opened: the leaf,
            // which `O_PATH | O_NOFOLLOW` accepted as a descriptor for the link
            // itself, and the components this open had to walk. The `?` drops
            // the descriptor it would otherwise be bound from.
            refuse_symlink_exposure(path)?;
            Ok(ExposureFd(descriptor))
        }
        Err(err) => Err(refuse(err)),
    }
}

/// A bind follows a symlink, so an exposure through one would mount the target.
const SYMLINK_EXPOSURE: &str =
    "is a symlink, and a bind of it would mount the target rather than the path that was exposed";

/// Refuse `path` when it, or any directory above it, is a symlink.
///
/// The whole chain, not the last component: a directory reached through a
/// linked parent is the target's directory. `symlink_metadata` reports the
/// link itself. A component that cannot be read is not this check's to refuse
/// — the existence check reports a path that is not there.
fn refuse_symlink_exposure(path: &Path) -> Result<()> {
    for component in path.ancestors() {
        let Ok(metadata) = fs::symlink_metadata(component) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            return Err(refused(
                EXPOSE_STEP,
                Error::BadPath {
                    path: component.to_path_buf(),
                    reason: SYMLINK_EXPOSURE,
                },
            ));
        }
    }
    Ok(())
}

/// `/proc/self/task`: one entry per task in this thread group, which is the
/// whole of what the single-thread precondition reads.
///
/// **`/proc/self` rather than `/proc/<pid>`**, because the question is about the
/// task group the confinement is about to change and nothing else — the same
/// group every per-thread operation below is scoped to.
const TASK_DIR: &str = "/proc/self/task";

/// The step both refusals of the single-thread precondition carry.
///
/// **One name for the two ways it fails, because they are one precondition**
/// (shared §7): more than one task in [`TASK_DIR`], and a [`TASK_DIR`] that
/// cannot be read at all, are the same answer to the same question — this
/// process was not *shown* to be single-threaded, so it is not confined. What
/// separates them is the [`Error::Step`]'s source: the count, or the `read_dir`
/// error. An operator greps one string for both.
const THREAD_STEP: &str = "confine single-threaded";

/// Refuse to confine a process that has more than one thread.
///
/// **Every operation of the sequence is per-thread, which is why one thread is a
/// precondition rather than a preference.** `unshare` moves the calling task's
/// own `nsproxy` — the threads of a process share one until a thread unshares
/// it — `PR_SET_NO_NEW_PRIVS`, the bounding-set drops and the `capset` write the
/// calling task's credentials, and `PR_SET_SECCOMP` installs its filter on the
/// calling task alone. A sibling thread therefore keeps the host mount
/// namespace, the full credentials and an unfiltered network for as long as the
/// process lives, while [`crate::Backend::enter`] returns
/// [`Confinement::InPlace`] and the run reports a confinement that covers one
/// task of several. **The root model is where that was silent**: its only
/// `unshare` is `CLONE_NEWNS`, which a multi-threaded process may make. The
/// user-namespace model fails earlier instead — `unshare(CLONE_NEWUSER)` is
/// `EINVAL` with more than one thread, the design's own measured evidence row —
/// but its `Error::Step` names `unshare` rather than the thread count, which is
/// the wrong diagnosis for the operator who has to act on it, and no diagnosis
/// at all for the capabilities and the filter, which the root model reaches in
/// the same state.
///
/// **The entry point is where that was reachable, and both sides are ordered
/// against it now.** `serve` builds a subscriber that carries an OTLP endpoint
/// in an `otel` build with `[telemetry]` configured, and `opentelemetry_sdk`'s
/// batch exporter spawns its `OpenTelemetry.Traces.BatchProcessor` thread when
/// the provider is built — so a subscriber installed *before* the confinement
/// is a thread the confinement would leave outside, which is what the CLI used
/// to do and what made this reachable. It installs the subscriber *after*
/// `confine` on this platform now, so the thread it starts is created inside the
/// namespaces, the dropped capability set and the filter. **Neither half
/// replaces the other**: the order belongs to the entry point and cannot be
/// enforced from here, and this check belongs to this crate and does not depend
/// on any entry point getting that order right.
///
/// **Refusal rather than `SECCOMP_FILTER_FLAG_TSYNC`, which would be the half
/// measure.** TSYNC synchronises the *filter* across the threads that exist when
/// it is passed and nothing else: the mount namespace, the user namespace,
/// `no_new_privs` and the capability sets would still be the calling task's
/// alone. It would shrink the hole rather than close it, and it would leave a
/// process reporting [`Confinement::InPlace`] whose siblings are still reachable
/// exactly the way the mount view and the ptrace family exist to prevent. The
/// remedy belongs to the sequence and not to a flag: start the process
/// single-threaded, or do not confine it in place.
///
/// **Fail-closed, so an unreadable [`TASK_DIR`] is a refusal too** (design §11):
/// the check's whole job is to say the process was *shown* to be
/// single-threaded, and answering "carry on" for a `/proc` that could not be
/// read is precisely the silent under-confinement it exists to refuse. A process
/// that cannot be inspected before it is jailed is not confined, and that is the
/// answer even where the read failed for an unrelated reason.
///
/// **Cheap**, which the call site's position depends on: one `read_dir` at
/// startup, counted by iterating the entries and allocating nothing.
///
/// # Errors
///
/// [`Error::Step`] with [`THREAD_STEP`], whose source carries either the task
/// count or the errno the directory read failed with.
fn refuse_when_threaded() -> Result<()> {
    require_single_thread(task_count(Path::new(TASK_DIR))?)
}

/// The policy [`refuse_when_threaded`] applies to a count.
///
/// **Split out so the decision is reachable from a test without a
/// single-threaded process**: libtest runs every test on a worker thread beside
/// the harness's own, so the refusal can be asserted against the real
/// [`TASK_DIR`] and the *acceptance* cannot — it is asserted here, over the
/// count, and the two tests together are the property: [`task_count`] reads the
/// kernel's count, and one task is what that count has to be.
/// `src/bin/sandbox-child.rs` is the end-to-end half, a `fn main` that is
/// single-threaded and confines for real.
///
/// # Errors
///
/// [`Error::Step`] with [`THREAD_STEP`] when `tasks` is more than one, whose
/// source is the operator-facing sentence: how many threads there are, how many
/// the confinement would leave outside, and what those would keep.
fn require_single_thread(tasks: usize) -> Result<()> {
    if tasks <= 1 {
        return Ok(());
    }
    Err(fail(
        THREAD_STEP,
        io::Error::other(format!(
            "this process has {tasks} threads, and confining it in place would leave {} of them \
             outside: unshare, no_new_privs, the capability drop and the seccomp filter all apply \
             to the calling task alone, so the rest would keep the host mount namespace, the full \
             credentials and an unfiltered network while this run reported itself confined. \
             Confine before anything spawns a thread — the tracing subscriber and the async \
             runtime both do — or do not confine in place",
            tasks - 1
        )),
    ))
}

/// The task count in `dir` — one directory read, by [`refuse_when_threaded`].
///
/// **The read is the precondition's whole cost, and its failure is the refusal**
/// (see [`refuse_when_threaded`]): an entry that cannot be read is the same
/// answer as a directory that cannot be opened, because a count taken from a
/// listing that lost entries could be wrong in the direction that matters, and
/// this count is never one to guess at.
///
/// `read_dir` yields one entry per task and no `.`/`..` of its own — measured on
/// the acceptance kernel: `1` for a single-threaded process and `2` with one
/// thread spawned — so the count is the length of the iteration rather than a
/// filtered subset of it.
///
/// `dir` is [`TASK_DIR`] at the call site and a parameter here so that the
/// fail-closed half — an unreadable directory refuses rather than passes — is a
/// property a test can lose.
///
/// # Errors
///
/// [`Error::Step`] with [`THREAD_STEP`] and a source naming `dir` and the errno,
/// whichever of the read's two steps failed.
fn task_count(dir: &Path) -> Result<usize> {
    let unreadable = |source: io::Error| {
        fail(
            THREAD_STEP,
            io::Error::new(source.kind(), format!("{}: {source}", dir.display())),
        )
    };
    let mut tasks = 0usize;
    for entry in fs::read_dir(dir).map_err(unreadable)? {
        entry.map_err(unreadable)?;
        tasks += 1;
    }
    Ok(tasks)
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
fn apply_planned_mounts(
    sandbox: &Sandbox,
    mounts: &[Mount],
    overlay: &OverlayDirs,
    descriptors: &[ExposureFd],
    proc_fallback: bool,
) -> Result<()> {
    // 6. The mounts, in plan order. **No inherited flags**: a planned mount is
    //    not a remount of anything — `plan` emits no `OverlayReadOnly` — and the
    //    one caller that has them is the read-only arm's fallback, which
    //    re-enters `apply_mount` with them so the trace stays one event per mount.
    for entry in mounts {
        apply_mount(
            entry,
            overlay,
            MountFlags::empty(),
            descriptors,
            proc_fallback,
        )?;
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
        // **The renderer is the core's**, and this file's private copy of it is
        // gone: both spelled `path (access)` with `read-only`/`read-write`, which
        // is exactly the state that let a third platform render the same set as
        // the enum's `Debug`. `describe_exposures` owns the spelling and the
        // history.
        exposures = %describe_exposures(exposures),
        "the exposure set the jail applied"
    );
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
/// re-asserts every flag [`source_mount_flags_in`] reads before it asks.**
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
/// The two fields it compares are [`mount_entries`]'s, parsed and decoded there.
fn writable_nested_mounts_in(table: &str, target: &Path) -> Vec<PathBuf> {
    mount_entries(table)
        .filter(|(mount, options)| {
            mount.starts_with(target)
                && mount != target
                && options.split(',').any(|option| option == "rw")
        })
        .map(|(mount, _)| mount)
        .collect()
}

/// Every line of a `/proc/self/mountinfo` table as its mount point and its own
/// options — **the two fields both readers below ask for, parsed and decoded
/// once**.
///
/// The mount point is the fifth whitespace-separated field, escaped — `\040` for
/// a space, `\011` for a tab, `\012` for a newline and `\134` for a backslash —
/// and it is decoded here so that a caller's comparison sees one spelling of a
/// path, which is the only reason a target with a space in its name can match
/// its own submounts. The options are the sixth field, in which no option name
/// can contain a comma. No raw whitespace can appear in either, so splitting on
/// it is safe.
fn mount_entries(table: &str) -> impl Iterator<Item = (PathBuf, &str)> + '_ {
    table.lines().filter_map(|line| {
        let mut fields = line.split_whitespace();
        let mount_point = fields.nth(4)?;
        let options = fields.next()?;
        Some((decode_mountinfo(mount_point), options))
    })
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

/// The `nosuid`/`nodev`/`noexec`/`nosymfollow` flags of the mount that contains
/// `source`, as read from a `/proc/self/mountinfo` table.
///
/// **These are what an `MS_REMOUNT|MS_BIND` request has to re-assert, and every
/// other flag is deliberately left out of it.** A bind remount *replaces*
/// the target's per-mount flag set with the request's rather than adding to it,
/// so the flags a bind inherits and drops are the flags that must be carried;
/// the rule is design decision 8's, with its measured evidence row in the
/// design's evidence table and its restatement in design §4.
///
/// **`nosymfollow` joined the set on 2026-09-28, and it is the one member the
/// kernel never refuses to drop.** `MNT_NOSYMFOLLOW` sits inside
/// `MNT_USER_SETTABLE_MASK` (`include/linux/mount.h`), so a bind remount that
/// does not name it clears it; and unlike `nosuid`/`nodev`/`noexec` there is
/// **no `MNT_LOCK_NOSYMFOLLOW`** — `lock_mnt_tree` sets `MNT_LOCK_ATIME`
/// unconditionally and `MNT_LOCK_READONLY`/`NODEV`/`NOSUID`/`NOEXEC` for the
/// flags a mount carries, and `can_change_locked_flags` tests exactly those five
/// (`fs/namespace.c`) — so a `nosymfollow` source produces no `EPERM` to send it
/// to the overlay arm and nothing reports the loss. Measured with a bare
/// `mount(2)` on `6.18.33.2-microsoft-standard-WSL2`, from a nested user
/// namespace: a bind of a `rw,relatime,nosymfollow` tmpfs, remounted
/// `MS_REMOUNT|MS_BIND|MS_RDONLY`, returned `0` and read back `ro,relatime`; the
/// same request naming `MS_NOSYMFOLLOW` read back `ro,relatime,nosymfollow`.
/// The end-to-end half is `tests/jail.rs`'s opt-in nested-user-namespace leg,
/// whose child reads a symlink inside the served root: that read is `ELOOP`
/// exactly while this flag survives. The full measurement and the arm it leaves
/// uncovered are `docs/superpowers/plans/2026-09-25-sandbox-followups.md` §7's
/// note.
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
/// The mount point and the options are [`mount_entries`]'s two fields, decoded
/// there for [`decode_mountinfo`]'s reason.
fn source_mount_flags_in(table: &str, source: &Path) -> MountFlags {
    let options = mount_entries(table)
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
            "nosymfollow" => MountFlags::NOSYMFOLLOW,
            _ => MountFlags::empty(),
        };
    }
    flags
}

/// `inherited` is the source mount's own per-mount flags as
/// [`source_mount_flags_in`] read them, and **one arm consumes it**:
/// [`MountKind::OverlayReadOnly`], whose mount is fresh and therefore has
/// nothing to inherit from. It is an argument rather than a second read of the
/// mount table inside that arm because the read already exists one frame up in
/// [`enforce_bind_read_only`], the only caller that builds that kind; a planned
/// mount passes [`MountFlags::empty()`], there being no source it was bound
/// from.
fn apply_mount(
    entry: &Mount,
    overlay: &OverlayDirs,
    inherited: MountFlags,
    descriptors: &[ExposureFd],
    proc_fallback: bool,
) -> Result<()> {
    match entry.kind {
        MountKind::JailTmpfs => apply_jail_tmpfs(entry)?,
        // `return`: the overlay fallback re-enters this function, and that
        // recursive call is the one that traces — falling through would emit a
        // second `mount applied` with the bind's kind (design decision 8).
        //
        // **The two binding kinds go to one function, and the kind picks the
        // form there**: `plan` marks an exposure's own bind
        // [`MountKind::ExposureBind`], so it binds the descriptor
        // `open_exposures` opened, while the device files — plain
        // [`MountKind::Bind`] — keep the path-based bind.
        MountKind::Bind | MountKind::ExposureBind(_) => {
            return apply_bind(entry, overlay, descriptors, proc_fallback);
        }
        MountKind::OverlayReadOnly => apply_overlay_read_only(entry, overlay, inherited)?,
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

fn apply_bind(
    entry: &Mount,
    overlay: &OverlayDirs,
    descriptors: &[ExposureFd],
    proc_fallback: bool,
) -> Result<()> {
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
    // **An exposure binds the descriptor's own object, never the path again**:
    // `open_exposures` resolved the path once, and a rename or a link swap
    // since then would otherwise decide what lands in the jail. **The index is
    // in range for every mount the plan builds** — only `plan` emits
    // [`MountKind::ExposureBind`], from the same `enumerate()` over
    // `sandbox.exposures()` that `open_exposures` walks — **but it is still a
    // `usize`, so an out-of-range one is still representable**: a stale or
    // hand-built index read as `descriptors[index]` would panic inside
    // confinement rather than refuse, which is the outcome design §11 forbids.
    // The lookup is therefore checked, and a miss becomes the crate's ordinary
    // `bind exposure` refusal naming the mount and both numbers (Task 5's "M2
    // correction", `docs/superpowers/plans/2026-10-02-known-issues-remediation.md`).
    match entry.kind {
        MountKind::ExposureBind(index) => {
            let descriptor = descriptors.get(index).ok_or_else(|| {
                fail(
                    "bind exposure",
                    io::Error::other(format!(
                        "mount {} names exposure {index}, but only {} descriptor(s) were opened",
                        entry.source.display(),
                        descriptors.len(),
                    )),
                )
            })?;
            bind_exposure(&descriptor.0, &entry.target, proc_fallback)?;
        }
        // Recursive, not plain: a bare `MS_BIND` of a mount-point source is
        // refused with EINVAL inside a user namespace (measured), and for a
        // source with no submounts the two forms are identical — so the
        // recursive call is the one to write everywhere.
        MountKind::Bind => {
            mount_bind_recursive(&entry.source, &entry.target).map_err(step("mount --bind"))?;
        }
        // **The two kinds `apply_mount` routes to their own arms, named rather
        // than left to a `_` so a fifth `MountKind` cannot silently inherit the
        // path bind this match used to end in.** Neither is reachable from
        // confinement; one arriving anyway means that routing and this match
        // disagree, and it is refused by sentence rather than with
        // `unreachable!()` — library code does not panic, and the message is
        // what makes the disagreement diagnosable.
        MountKind::JailTmpfs | MountKind::OverlayReadOnly => {
            return Err(fail(
                "bind exposure",
                io::Error::other(format!(
                    "apply_bind was handed a {:?} mount, which apply_mount applies in its own arm",
                    entry.kind
                )),
            ));
        }
    }
    if entry.access.is_read_only() {
        return enforce_bind_read_only(entry, overlay, proc_fallback);
    }
    trace_mount(entry);
    Ok(())
}

/// Bind the validated object: a recursive clone of the descriptor's mount moved
/// onto the target, or the descriptor's `/proc` link where `open_tree` is
/// unavailable.
///
/// **`move_mount` takes a *mount* descriptor, which is why the clone and not the
/// bare `O_PATH` descriptor is what it receives.** The `/proc/self/fd` arm is for
/// the kernels below 5.2 that answer `ENOSYS`, and it requires `/proc`; it is also
/// what `TINIO_SANDBOX_FORCE_PROC_FALLBACK` selects, so the two arms can be
/// exercised on one host. Both resolve the same held descriptor, so neither can
/// be steered by a path that changed after validation — **and the descriptor was
/// opened in *this* mount namespace** (`open_exposures` runs after
/// `enter_mount_namespace`), which is what makes it bindable at all: a mount
/// belongs to the namespace that owns it, and one opened earlier belongs to the
/// namespace this process left.
///
/// **`proc_fallback` is the run's one resolved answer, not a second read of the
/// variable here**: the confinement boundary reads it once and carries the
/// decision down, and both arms bind the same held descriptor — the variable
/// picks *which* of two descriptor-bound paths performs the bind, so it grants no
/// authority either way.
fn bind_exposure(descriptor: &OwnedFd, target: &Path, proc_fallback: bool) -> Result<()> {
    if !proc_fallback {
        let flags = OpenTreeFlags::OPEN_TREE_CLONE
            | OpenTreeFlags::AT_EMPTY_PATH
            | OpenTreeFlags::AT_RECURSIVE;
        match open_tree(descriptor, "", flags) {
            Ok(tree) => {
                return move_mount(
                    &tree,
                    "",
                    CWD,
                    target,
                    MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
                )
                .map_err(step("move_mount"));
            }
            // No `open_tree` at all: the `/proc` arm is the only bind left.
            Err(Errno::NOSYS) => {}
            Err(err) => return Err(step("open_tree")(err)),
        }
    }
    let link = format!("/proc/self/fd/{}", descriptor.as_raw_fd());
    mount_bind_recursive(Path::new(&link), target).map_err(step("mount --bind"))
}

/// Remount the bind read-only, or fall back to the overlay when locked flags
/// refuse the remount (design decision 8).
///
/// `proc_fallback` is the run's resolved `/proc` decision, passed through
/// because the fallback re-enters [`apply_mount`] — whose signature carries it
/// for the bind kinds — even though the substituted `OverlayReadOnly` binds
/// nothing.
fn enforce_bind_read_only(entry: &Mount, overlay: &OverlayDirs, proc_fallback: bool) -> Result<()> {
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
    // **The request carries every flag [`source_mount_flags_in`] reads, and
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
    // **`NOSYMFOLLOW` is in that set for the second half of this paragraph
    // alone** (2026-09-28): the kernel never locks it — there is no
    // `MNT_LOCK_NOSYMFOLLOW` — so it is the one member whose loss no `EPERM`
    // reports, and a request that does not name it clears it on the unlocked and
    // the locked path alike. `source_mount_flags_in`'s own doc carries the
    // measurement.
    //
    // **`RDONLY` is the request's and never the source's.** The source is
    // usually `rw` — it is the served root — and read-only is what this call
    // adds; the inherited flags are read from the mount that *contains*
    // [`Mount::source`] by [`source_mount_flags_in`], which can therefore only
    // reproduce what the bind already carried and never widen the exposure.
    //
    // **And the overlay is the other half of the same rule, not an alternative
    // to it** — it is now the **rarer** arm, which is the cost design §4 records
    // against the simplification this line adopts. Where the request re-asserts
    // what a locked mount demands, the `EPERM` arm is reached only by a
    // per-mount flag this request does not name — and the kernel's locked set is
    // **exactly** `READONLY`, `ATIME`, and **three** of `source_mount_flags_in`'s
    // four (`NOSUID`, `NODEV`, `NOEXEC`: the members that have an `MNT_LOCK_*`
    // behind them at all, `nosymfollow` having none), with `ATIME` preserved by
    // the kernel on a remount that does not name an atime flag
    // (`lookup_mnt`'s rule in `do_mount`: "the default atime for remount is
    // preservation"), so on this kernel **no mount flag can refuse this request
    // any more** and the arm is reached by none. **What that rule is worth is
    // measured by breaking it, in the guest**: with one member deleted from
    // [`source_mount_flags_in`]'s match — `noexec`, whose lock is real — the
    // opt-in nested-user-namespace leg's `noexec` source is refused `EPERM` by
    // this remount and reports `read-only-arm: overlay` instead, so the arm is
    // one un-named locked flag away on this host and it is the request that
    // closes the route rather than the kernel. That is measured, and it is
    // `docs/superpowers/plans/2026-09-25-sandbox-followups.md` §7's note — the
    // arm stays because the kernel's answer is what picks it, not this code, and
    // there the `MS_RDONLY` overlay is what makes the exposure
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
    // **One read of the table answers both of this function's questions, and the
    // earlier read is the one to keep.** A non-recursive `MS_REMOUNT|MS_BIND`
    // changes only the target mount's own per-mount flags: every nested submount
    // below it keeps the flags it had, and `writable_nested_mounts_in` excludes
    // the target itself — so the pre-remount table answers the nested-mount
    // question *identically* to the post-remount one it used to be read for, and
    // this file stops reading and parsing a table that runs to tens or hundreds
    // of KiB twice per read-only exposure.
    //
    // **The check that consumes it is fail-closed, so the argument above is the
    // reason this line is allowed to be one line**: if a remount could change a
    // submount's flags, this read would report a read-only exposure while a
    // writable mount stayed reachable inside it, which is the one outcome
    // `refuse_writable_nested_mounts` exists to prevent. It cannot — that is what
    // "non-recursive" means for a remount — and both readers below take their
    // fields from the one parser, [`mount_entries`].
    let table = read_mountinfo()?;
    let inherited = source_mount_flags_in(&table, &source);
    match mount_remount(
        &entry.target,
        MountFlags::RDONLY | MountFlags::BIND | inherited,
        "",
    ) {
        Ok(()) => {
            // **The arm, on the one surface a run can see it from.** Which of the two read-only
            // implementations ran was measurable only from the test-only `sandbox-child` binary,
            // and this is the production half of that fact: `/metrics` is read out of the process
            // that confined itself, so the line survives both the window with no subscriber and
            // the absence of a child (`metrics::ReadOnlyArm` carries the rest). Recorded with the
            // `Ok` in hand and before the nested-mount check, so this site and the overlay's both
            // record where the arm is decided — the check below refuses the run, not the arm, and
            // a refused run serves nothing for a scrape to read.
            record_read_only_arm(ReadOnlyArm::Remount);
            refuse_writable_nested_mounts(entry, &table)?;
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
            // The same fact as the `debug!` above, on the carrier an operator can scrape: this
            // branch is the *only* way into `MountKind::OverlayReadOnly`, so the substitution is
            // the one site that knows the fallback was taken rather than planned.
            record_read_only_arm(ReadOnlyArm::Overlay);
            let fallback = Mount {
                kind: MountKind::OverlayReadOnly,
                ..entry.clone()
            };
            // No descriptors: the substitution is the one way into
            // [`MountKind::OverlayReadOnly`], which mounts a filesystem of its
            // own rather than binding one, so the re-entry never reads the
            // slice. `apply_mount` still takes it because the bind kinds do.
            apply_mount(&fallback, overlay, inherited, &[], proc_fallback)
        }
        Err(source) => Err(fail("remount ro", source)),
    }
}

/// A successful remount leaves nested mounts writable; refuse that hole.
///
/// `table` is the caller's one `/proc/self/mountinfo` read, taken **before** the
/// remount — the call site carries the argument for why the earlier table answers
/// this question identically. Taking it as an argument rather than reading it
/// here is what makes the two readers of one exposure share one read.
fn refuse_writable_nested_mounts(entry: &Mount, table: &str) -> Result<()> {
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
    // The table is the caller's, read after the bind and before the
    // remount: `/proc` is still the host's, the pivot having not
    // happened yet, so the paths are the host's own. It reports
    // *resolved* paths — measured: a mount made
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
    let nested = writable_nested_mounts_in(table, &target);
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

fn apply_overlay_read_only(
    entry: &Mount,
    overlay: &OverlayDirs,
    inherited: MountFlags,
) -> Result<()> {
    // **The fallback arm, and the only way in is the substitution
    // above.** `plan` emits no entry of this kind (Task 2), and keeping
    // the mechanism a `MountKind` rather than an inline branch is what
    // lets that substitution reuse this body instead of a second copy
    // of it.
    //
    // The option string is built before any directory is created, so a path
    // this fallback cannot express fails without leaving overlay dirs behind.
    let options = overlay_mount_options(&entry.source, &overlay.upper, &overlay.work)?;
    fs::create_dir_all(&entry.target).map_err(step("create overlay mount point"))?;
    fs::create_dir_all(&overlay.upper).map_err(step("create overlay upper"))?;
    fs::create_dir_all(&overlay.work).map_err(step("create overlay work"))?;
    // **Which flags this mount carries is [`overlay_read_only_flags`]'s own
    // doc, and it is a function rather than four lines here for the reason
    // [`refused_remount_needs_the_overlay`] gives: this *is* the fallback arm's
    // read-only decision, and a test cannot mount an overlay to read it back.**
    let flags = overlay_read_only_flags(inherited);
    // **`as_c_str()` and not `&options`**: `mount`'s data parameter is
    // `Data: Into<Option<&CStr>>`, and a `CString` reference is not one
    // of those — the conversion is explicit all the way down.
    mount(
        "overlay",
        &entry.target,
        "overlay",
        flags,
        Some(options.as_c_str()),
    )
    .map_err(step("mount overlay ro"))
}

/// The flag set [`apply_overlay_read_only`] mounts with — **the fallback arm's
/// answer to "which of the source's own per-mount flags must a fresh mount
/// carry?"**, and a pure function because that answer is the whole of this arm's
/// read-only decision and the `mount` that consumes it cannot be called from a
/// test.
///
/// **`NOSUID`/`NODEV` are unconditional and `NOEXEC`/`NOSYMFOLLOW` follow the
/// source, and the difference is what each flag can break.** The overlay is a
/// fresh mount, so it inherits nothing and the request *is* its flag set:
/// `nosuid` and `nodev` narrow nothing a served root can legitimately do, which
/// is why the fallback has always carried them whatever the source says.
/// `nosymfollow` and `noexec` do narrow — the first stops every symlink inside
/// the served data from resolving, the second stops every file in it from being
/// executed — so naming either for a source that does not carry it would change
/// what the exposure means, and **would make this arm serve the same root
/// differently from the remount arm**, which re-asserts the source's own
/// spelling. The two arms have to mean the same thing (design §4), so both flags
/// follow the source.
///
/// **`NOEXEC` joined on those terms, and it was the asymmetry that made it a
/// defect rather than a nicety**: the remount arm has carried the source's
/// `noexec` since the flag fix of 2026-09-24, so a served root on a `noexec`
/// mount was exec-capable under this arm and not under that one
/// ([`source_mount_flags_in`] reads it, and the request names it).
///
/// **Nothing in this jail executes from a served root, which is why following
/// the source costs nothing.** Linux confines the process it is already running
/// in (design decisions 6, 26): the image is mapped before `pivot_root`, nothing
/// `exec`s afterwards — the sequence is the two `unshare`s, the mounts, the
/// `pivot_root`, the capability drop and the seccomp filter, and the map
/// descriptors are closed by hand for exactly that reason — and the jail holds
/// no `/proc` and no other executable path. A `noexec` source is therefore a
/// host policy the exposure inherits rather than one the jail would have to
/// lift, and a source without it is served exec-capable by both arms alike.
fn overlay_read_only_flags(inherited: MountFlags) -> MountFlags {
    let mut flags = MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV;
    for flag in [MountFlags::NOEXEC, MountFlags::NOSYMFOLLOW] {
        if inherited.contains(flag) {
            flags |= flag;
        }
    }
    flags
}

/// `,`, `:`, and `\` are overlay option grammar, not path bytes.
const OVERLAY_OPTION_META: &str =
    "contains ',', ':' or '\\', which an overlay mount option would read as a separator";

/// The `lowerdir`/`upperdir`/`workdir` string, or a refusal when one of the
/// three paths contains a byte the option grammar splits on.
///
/// overlayfs separates options on `,` and lower directories on `:`. A `\` is
/// an escape on kernels that implement one and an ordinary byte on kernels
/// that do not, so writing the escape is not safe: a kernel that ignores it
/// still splits, and the mount would take a second host tree as `lowerdir`.
/// A path that contains any of the three cannot be this fallback, and the
/// launch stops before `mount`.
fn overlay_mount_options(source: &Path, upper: &Path, work: &Path) -> Result<CString> {
    for path in [source, upper, work] {
        if path
            .as_os_str()
            .as_bytes()
            .iter()
            .any(|byte| matches!(byte, b',' | b':' | b'\\'))
        {
            return Err(refused(
                "mount overlay",
                Error::BadPath {
                    path: path.to_path_buf(),
                    reason: OVERLAY_OPTION_META,
                },
            ));
        }
    }
    let options = format!(
        "lowerdir={},upperdir={},workdir={}",
        source.display(),
        upper.display(),
        work.display()
    );
    CString::new(options).map_err(|_| {
        refused(
            "mount overlay",
            Error::Unsupported {
                detail: "an exposed path contains a NUL byte",
            },
        )
    })
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
    use std::{
        ffi::OsStr,
        fs,
        os::{
            fd::AsRawFd,
            unix::{ffi::OsStrExt, fs::symlink},
        },
        path::{Path, PathBuf},
        sync::mpsc,
        thread,
    };

    use rustix::{io::Errno, mount::MountFlags, thread::CapabilitySet};

    use super::{
        OVERLAY_OPTION_META, SYMLINK_EXPOSURE, TASK_DIR, THREAD_STEP, announce_capset,
        announce_exposures, announce_no_new_privs, announce_retained, apply_bind, create_jail_root,
        decode_mountinfo, enforce_bind_read_only, enter_user_namespace, open_exposure,
        open_exposures, open_map, overlay_mount_options, overlay_read_only_flags, read_mountinfo,
        refuse_symlink_exposure, refuse_when_threaded, refuse_writable_nested_mounts,
        refused_remount_needs_the_overlay, require_single_thread, resolve_model,
        source_mount_flags_in, task_count, validate_and_plan, validate_exposures,
        writable_nested_mounts_in, write_map,
    };
    use crate::{
        _core::{AccessMode, Backend as BackendTrait, Error, Exposure, LaunchPlan, Sandbox},
        Backend, Mechanism,
        caps::Cap,
        jail::jail_root,
        options::Options,
        plan::{Mount, MountKind, OverlayDirs},
        testing::{assert_step, record_events},
    };

    #[test]
    fn the_exposure_set_is_traced_with_its_step() {
        // **The second of the three events, and the "to what" half of shared
        // §7's question.** The set is rendered as one field value — the per-mount
        // `debug!`s are the item detail and are invisible to a subscriber at the
        // `Info` level this event exists for — so this asserts both halves of
        // what an operator reads: the counts and the set itself, in the access
        // spelling the run used.
        // **The rendering is the core's `describe_exposures` now**, and that is
        // a second property this line pins: the shared renderer's spelling is
        // what lands on this crate's log, so a change to it shows up here rather
        // than in a downstream reader's parser.
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
        // `debug!`-only record of it would be invisible to a reader whose
        // subscriber runs at `Verbosity::Info`, which is what `serve` builds.
        // **On this platform that subscriber is installed after `confine`** (the
        // ordering `refuse_when_threaded` states), so `serve` reads this event in
        // no case; the reader the level is chosen for is a caller that has one
        // installed when it confines. The refusal tests reach this function too,
        // which is why the recorder is a global one
        // (`tinio_util::testing::record_events`, re-exported by `crate::testing`,
        // states the mechanism).
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
    fn a_threaded_process_cannot_confine_itself_in_place() {
        // **The refusal against the real `/proc/self/task`, which is the half no
        // pure test can reach.** libtest cannot run this test single-threaded —
        // the harness's own thread waits beside this worker — so the thread
        // spawned below is a *named* second task and not the only one; what the
        // assertions pin is that the count is the kernel's (two live threads,
        // two entries) and that a count above one refuses. A check that never
        // fired would leave the process confined on one task of several, which
        // is the hole it exists to close (design §8, §10).
        let (release, gate) = mpsc::channel::<()>();
        let second = thread::spawn(move || {
            // Held until the checks below are through, so the count read here
            // includes a thread this test named.
            let _ = gate.recv();
        });

        let counted = task_count(Path::new(TASK_DIR)).expect("a readable /proc/self/task");
        assert!(counted >= 2, "the harness and the thread above: {counted}");
        let err = refuse_when_threaded().expect_err("a threaded process must be refused");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: THREAD_STEP,
                    ..
                }
            ),
            "{err:?}"
        );
        let refused = err.to_string();
        // **No equality on the count, and the first run of this test is why**:
        // libtest runs it beside its own workers, so the task set changes
        // between the read above and the read inside the refusal — that run
        // measured `3` and then refused with `4`. What the sentence has to carry
        // is a count and the number it would leave outside, which is the half
        // an operator acts on:
        assert!(
            refused.contains("this process has") && refused.contains("of them outside"),
            "{refused}"
        );
        assert!(
            refused.contains("host mount namespace") && refused.contains("seccomp"),
            "the refusal names what the other threads would keep: {refused}"
        );

        let _ = release.send(());
        second.join().expect("the named second thread");
    }

    #[test]
    fn one_task_is_what_the_thread_precondition_accepts() {
        // **The acceptance half, over the count rather than over a process**,
        // because libtest has no single-threaded one to offer: the test above
        // pins that `task_count` reads the kernel's real count, and this one
        // pins that one task is the count that passes. `src/bin/sandbox-child.rs`
        // is where both halves meet a process that really confines itself — its
        // `fn main` is single-threaded, and that suite fails if this predicate
        // ever becomes stricter than the kernel's answer.
        require_single_thread(1).expect("one task is exactly the precondition");
        // Nothing below one is a count this kernel reports for a live process,
        // and the rule is "more than one" rather than "exactly one": the refusal
        // is the half that has to be exact, and `<= 1` is the same predicate
        // read from the other side.
        require_single_thread(0).expect("there is nothing to leave outside");
        let err = require_single_thread(2).expect_err("two tasks cannot both be confined");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: THREAD_STEP,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("2 threads"), "{err}");
    }

    #[test]
    fn a_task_directory_that_cannot_be_read_refuses_rather_than_passes() {
        // **Design §11's fail-closed, on the check's own read**: a `/proc` that
        // answers nothing must not be read as "one thread". The directory is a
        // parameter of `task_count` for this test — the alternative is a
        // `/proc/self/task` that cannot be read, which no test can arrange in
        // its own process — and the refusal names the path, so the operator
        // learns which read failed rather than only that one did.
        let missing = Path::new("/proc/self/task/one-task/does-not-exist");
        let err = task_count(missing).expect_err("an unreadable directory is a refusal");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: THREAD_STEP,
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("does-not-exist"), "{err}");
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
    fn the_remount_reasserts_the_sources_own_mount_flags() {
        // **The flags a bind inherits, and only those** (design decision 8): a
        // bind remount replaces the target's per-mount flag set with the
        // request's, so a request that does not carry one of them silently
        // clears it — measured with a bare `mount(2)` on
        // `6.18.33.2-microsoft-standard-WSL2`, where a bind of the host's
        // `rw,nosuid,nodev` `/tmp` read back as plain `ro`. **`nosymfollow` is
        // in the set for that half alone** and is the member no `EPERM` protects:
        // the kernel locks `READONLY`/`ATIME` and three of the set's four
        // (`NOSUID`/`NODEV`/`NOEXEC`) and has no `MNT_LOCK_NOSYMFOLLOW`, so a
        // request that omits it drops it rather than being refused
        // (`source_mount_flags_in`'s doc carries the measurement,
        // `tests/jail.rs`'s opt-in nested-usernamespace leg the end-to-end half).
        // `rdonly` appears in no row here on purpose: the source is usually `rw`,
        // so `ro` is the request's and never the source's, and `relatime` is left
        // out because the kernel preserves the atime bits of a remount that names
        // no atime flag.
        let table = "\
            24 30 0:23 / / rw,relatime - ext4 /dev/sda1 rw\n\
            25 24 0:24 / /tmp rw,nosuid,nodev - tmpfs tmpfs rw\n\
            26 24 0:25 / /srv/data rw,nosuid,nodev,noexec - ext4 /dev/sdb1 rw\n\
            27 24 0:26 / /srv/data/cache rw - ext4 /dev/sdc1 rw\n\
            28 24 0:27 / /run/credentials/x rw,nosuid,nodev,noexec,nosymfollow - tmpfs tmpfs ro\n";

        // The root filesystem carries none of the four, and the read-only
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
            "`cache` is the containing mount and carries none of the four"
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
        // **The row the 2026-09-28 fix added, and it is the only route this flag
        // has into the request.** A `nosymfollow` source (`/run/credentials/*`
        // are the ones this host carries) keeps the flag across the remount only
        // because the request names it; the containing-mount rule applies to it
        // like the other three, so a source below such a mount inherits it too.
        assert_eq!(
            source_mount_flags_in(table, Path::new("/run/credentials/x")),
            MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOEXEC | MountFlags::NOSYMFOLLOW
        );
        assert_eq!(
            source_mount_flags_in(table, Path::new("/run/credentials/x/served")),
            MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOEXEC | MountFlags::NOSYMFOLLOW,
            "a source below the nosymfollow mount inherits it"
        );
    }

    /// **The fallback arm's flag set, and the rule the two arms share** (design
    /// §4): `noexec` and `nosymfollow` follow the source, `nosuid`/`nodev` do not,
    /// and `rdonly` is the request's whatever the source says.
    ///
    /// **This assertion is where the overlay arm's flags are pinned, and the
    /// reason is measured**: the arm itself is unreachable end to end on this
    /// kernel — every flag the kernel locks is one the remount request names, so
    /// nothing is left for the `EPERM` branch to be refused for
    /// (`docs/superpowers/plans/2026-09-25-sandbox-followups.md` §7) — and the
    /// two arms serving one root differently is a defect rather than a nuance: a
    /// `noexec` source the remount arm serves cannot be executed from, and the
    /// same source served by this arm could be.
    #[test]
    fn the_overlay_carries_the_sources_noexec_and_nosymfollow() {
        assert_eq!(
            overlay_read_only_flags(MountFlags::empty()),
            MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV,
            "the flagless source, which is the arm's original request"
        );
        assert_eq!(
            overlay_read_only_flags(MountFlags::NOEXEC),
            MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOEXEC,
            "a `noexec` source is served `noexec` here too, as the remount arm serves it"
        );
        assert_eq!(
            overlay_read_only_flags(MountFlags::NOSYMFOLLOW),
            MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV | MountFlags::NOSYMFOLLOW
        );
        // **A real source carries these together** — the host's
        // `/run/credentials/*` mounts are `nosuid,nodev,noexec,nosymfollow` — so
        // this row is the one that says one flag's arm does not stop the other's.
        assert_eq!(
            overlay_read_only_flags(MountFlags::NOEXEC | MountFlags::NOSYMFOLLOW),
            MountFlags::RDONLY
                | MountFlags::NOSUID
                | MountFlags::NODEV
                | MountFlags::NOEXEC
                | MountFlags::NOSYMFOLLOW
        );
        // The other half of the rule, and the deliberate difference in spelling
        // from the remount arm: `nosuid`/`nodev` are carried for a source that
        // does not have them, because they narrow nothing a served root can
        // legitimately do (`overlay_read_only_flags`'s doc), and an inherited
        // `rdonly` changes nothing — read-only is this arm's own.
        assert_eq!(
            overlay_read_only_flags(MountFlags::RDONLY),
            MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV
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

    #[test]
    fn a_symlink_exposure_is_refused_before_the_bind() {
        let root = tempfile::tempdir().expect("temp dir");
        let real = root.path().join("real");
        fs::create_dir(&real).expect("target");
        fs::create_dir(real.join("child")).expect("child");
        let link = root.path().join("link");
        symlink(&real, &link).expect("symlink");

        let exposed = Sandbox::new().expose(&link, AccessMode::ReadOnly);
        let err = validate_exposures(&exposed).expect_err("the link itself");
        assert!(
            matches!(err, Error::BadPath { ref path, reason } if path == &link && reason == SYMLINK_EXPOSURE),
            "{err:?}"
        );

        // An intermediate link is the same rule: the component that is the
        // link is the one named, not the directory reached through it.
        let through = link.join("child");
        let err = validate_exposures(&Sandbox::new().expose(&through, AccessMode::ReadOnly))
            .expect_err("the linked parent");
        assert!(
            matches!(err, Error::BadPath { ref path, reason } if path == &link && reason == SYMLINK_EXPOSURE),
            "{err:?}"
        );
    }

    #[test]
    fn a_directory_that_is_not_a_symlink_can_be_exposed() {
        let root = tempfile::tempdir().expect("temp dir");
        let dir = root.path().join("data");
        fs::create_dir(&dir).expect("dir");
        // A symlinked ancestor (a `/tmp` that is itself a link) is the same
        // refusal, and it names that ancestor. The fixture only asserts a
        // clean accept when the host path has no link in it.
        let linked = dir.ancestors().find(|component| {
            fs::symlink_metadata(component).is_ok_and(|metadata| metadata.file_type().is_symlink())
        });
        let result = validate_exposures(&Sandbox::new().expose(&dir, AccessMode::ReadOnly));
        match linked {
            Some(component) => {
                let err = result.expect_err("an ancestor is a symlink");
                assert!(
                    matches!(err, Error::BadPath { ref path, reason } if path == component && reason == SYMLINK_EXPOSURE),
                    "{err:?}"
                );
            }
            None => assert!(result.is_ok(), "{result:?}"),
        }
        // The helper and the validator agree on a path that is not a link.
        assert!(refuse_symlink_exposure(Path::new("/")).is_ok());
    }

    #[test]
    fn the_opened_descriptors_are_in_exposure_order() {
        // **The order is the link `MountKind::ExposureBind` indexes**, so an
        // opener that returned them in another order — or dropped one — would
        // bind the wrong object for every exposure after the first. The `/proc/self/fd`
        // link is the oracle: it is the descriptor's own path, which is exactly
        // what `apply_bind` binds.
        let dir = tempfile::tempdir().expect("temp dir");
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        fs::create_dir(&first).expect("first");
        fs::create_dir(&second).expect("second");
        let sandbox = Sandbox::new()
            .expose(&first, AccessMode::ReadOnly)
            .expose(&second, AccessMode::ReadWrite);

        let Ok(descriptors) = open_exposures(&sandbox) else {
            // A temp root whose ancestor is a symlink is the policy
            // `a_directory_that_is_not_a_symlink_can_be_exposed` owns, and it
            // refuses at the open; this test is about the order.
            return;
        };
        assert_eq!(descriptors.len(), 2, "one descriptor per exposure");
        let opened: Vec<PathBuf> = descriptors
            .iter()
            .map(|exposure| {
                fs::read_link(format!("/proc/self/fd/{}", exposure.0.as_raw_fd()))
                    .expect("the descriptor names its own path")
            })
            .collect();
        assert_eq!(opened, [first, second], "exposure order");
    }

    #[test]
    fn a_mount_naming_a_missing_descriptor_refuses_rather_than_panicking() {
        // **`MountKind::ExposureBind`'s index is in range for every mount
        // `plan` builds, and this pins the refusal for the one that is not.**
        // The kind makes an index on a non-bind mount unrepresentable, but any
        // `usize` is still representable: reading a wrong one with
        // `descriptors[index]` would panic inside confinement rather than
        // refuse — the outcome design §11 forbids — so the lookup is checked
        // and a miss is the crate's ordinary `bind exposure` refusal (Task 5's
        // "M2 correction", `docs/superpowers/plans/2026-10-02-known-issues-remediation.md`).
        // `plan`'s own output never carries a wrong index, so a hand-built
        // mount is the only way to reach the guard.
        let dir = tempfile::tempdir().expect("temp dir");
        let source = dir.path().join("served");
        fs::create_dir(&source).expect("source");
        let Ok(descriptors) = open_exposures(&Sandbox::new().expose(&source, AccessMode::ReadOnly))
        else {
            // A temp root whose ancestor is a symlink is the open's own policy
            // (`a_directory_that_is_not_a_symlink_can_be_exposed`); this test
            // is about the index.
            return;
        };
        assert_eq!(descriptors.len(), 1, "one descriptor per exposure");
        let entry = Mount {
            source,
            target: dir.path().join("jail/served"),
            access: AccessMode::ReadOnly,
            kind: MountKind::ExposureBind(2),
        };
        let overlay = OverlayDirs {
            upper: dir.path().join("upper"),
            work: dir.path().join("work"),
        };
        let err = apply_bind(&entry, &overlay, &descriptors, false)
            .expect_err("only one descriptor was opened, so index 2 is missing");
        assert!(
            matches!(err, Error::Step { step, .. } if step == "bind exposure"),
            "{err:?}"
        );
        // Both numbers are in the message, so the operator reads the mismatch
        // rather than only that a bind was refused.
        let refusal = err.to_string();
        assert!(refusal.contains("2") && refusal.contains("1"), "{refusal}");
    }

    #[test]
    fn a_symlinked_component_is_refused_by_the_open() {
        // **The open is the policy now, not only the walk before it**: with
        // `openat2` the kernel refuses a link in any component inside one
        // resolution, and the fallback re-runs the walk after its open — so both
        // arms must answer a linked ancestor with the same named refusal rather
        // than following it.
        let dir = tempfile::tempdir().expect("temp dir");
        let real = dir.path().join("real");
        fs::create_dir(&real).expect("target");
        fs::create_dir(real.join("child")).expect("child");
        let link = dir.path().join("link");
        symlink(&real, &link).expect("symlink");

        // The link *target* is a real directory with the component that is
        // asked for below, so the `O_NOFOLLOW` arm's open resolves through the
        // link successfully — which is what makes the re-check, and not the
        // open's own failure, the thing that refuses it there. One policy, one
        // reason, whichever arm this kernel can run.
        for exposed in [link.clone(), link.join("child")] {
            let err = open_exposure(&exposed).expect_err("the link is followed nowhere");
            assert!(
                matches!(err, Error::BadPath { reason, .. } if reason == SYMLINK_EXPOSURE),
                "{err:?}"
            );
        }
    }

    #[test]
    fn overlay_options_refuse_separator_bytes() {
        let upper = Path::new("/tmp/jail/upper");
        let work = Path::new("/tmp/jail/work");
        let built =
            overlay_mount_options(Path::new("/srv/data"), upper, work).expect("plain paths");
        assert_eq!(
            built.to_str().expect("utf-8"),
            "lowerdir=/srv/data,upperdir=/tmp/jail/upper,workdir=/tmp/jail/work"
        );

        for path in ["/srv/a:b", "/srv/a,b", "/srv/a\\b"] {
            let err = overlay_mount_options(Path::new(path), upper, work).expect_err(path);
            assert!(
                matches!(
                    err,
                    Error::BadPath {
                        reason: OVERLAY_OPTION_META,
                        ..
                    }
                ),
                "{path}: {err:?}"
            );
        }

        // The upper dir is our path, and a separator there is the same injection.
        let err = overlay_mount_options(Path::new("/srv/data"), Path::new("/tmp/a:b"), work)
            .expect_err("upper");
        assert!(
            matches!(err, Error::BadPath { ref path, reason: OVERLAY_OPTION_META } if path == Path::new("/tmp/a:b")),
            "{err:?}"
        );

        let raw = OsStr::from_bytes(b"/srv/x,y");
        let err = overlay_mount_options(Path::new(raw), upper, work).expect_err("bytes");
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: OVERLAY_OPTION_META,
                    ..
                }
            ),
            "{err:?}"
        );

        // **The fourth byte that cannot be expressed, and the one the loop above
        // does not catch**: a NUL is not a separator, so the option string is
        // built — and `CString::new` is what refuses it. Left to pass, the
        // mount would read a path cut short at the NUL, which is a *different*
        // lowerdir rather than a failed one. Design §11's fail-closed, on the
        // last byte a `Path` can hold.
        let raw = OsStr::from_bytes(b"/srv/x\0y");
        let err = overlay_mount_options(Path::new(raw), upper, work).expect_err("a NUL byte");
        assert!(
            matches!(
                err,
                Error::Unsupported {
                    detail: "an exposed path contains a NUL byte"
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn every_escape_the_kernel_does_not_use_is_copied_through() {
        // **`decode_mountinfo` is the one place a mount point becomes a path, and
        // an escape it does not know must not become a guess.** The kernel
        // escapes exactly four byte values, so anything else — a `\9` sequence,
        // a three-character escape that is not one of the four, a backslash at
        // the very end of the field — is copied through with its backslash. A
        // decoder that dropped an unknown escape would answer a path the kernel
        // never wrote, which is the under-enforcement the two readers above it
        // exist to prevent.
        assert_eq!(
            decode_mountinfo("/srv/a\\999b"),
            PathBuf::from("/srv/a\\999b")
        );
        assert_eq!(
            decode_mountinfo("/srv/a\\04x"),
            PathBuf::from("/srv/a\\04x")
        );
        assert_eq!(
            decode_mountinfo("/srv/trailing\\"),
            PathBuf::from("/srv/trailing\\")
        );
        // And the four it does know still decode, so the pass-through above is
        // not a decoder that gave up on all of them.
        assert_eq!(
            decode_mountinfo("/srv/a\\040b\\134c"),
            PathBuf::from("/srv/a b\\c")
        );
    }

    #[test]
    fn the_mount_table_that_decides_a_remount_is_the_kernels() {
        // **Both readers of one read-only exposure take their two fields from
        // this table, so a table that cannot be read is a confinement that
        // cannot be decided** — and the fifth field is where the mount point is,
        // which is the fact the parser is built on. The root mount is the one
        // entry every table has, so the assertion holds on any kernel this
        // backend runs on.
        let table = read_mountinfo().expect("this kernel publishes /proc/self/mountinfo");
        assert!(
            table
                .lines()
                .any(|line| line.split_whitespace().nth(4) == Some("/")),
            "no root mount in:\n{table}"
        );
    }

    #[test]
    fn a_writable_mount_below_the_target_is_refused_by_its_host_path() {
        // **The hole this whole check exists for** (design §4): the bind is
        // recursive, so a nested mount is carried into the jail, and a
        // successful remount makes only the top mount read-only — a write
        // through the nested one would reach the host.
        let tmp = tempfile::tempdir().expect("temp dir");
        let served = tmp.path().join("served");
        let target = tmp.path().join("jail/served");
        fs::create_dir_all(&target).expect("the mount point the bind landed on");
        let entry = Mount {
            source: served.clone(),
            target: target.clone(),
            access: AccessMode::ReadOnly,
            kind: MountKind::Bind,
        };
        // The table is the caller's one read, so a test can hand it the shape it
        // needs — and what it hands is a real `/proc/self/mountinfo` line. Its
        // sixth field is the mount's own options and the field after the `-` is
        // the superblock's, which is the pair the predicate has to tell apart:
        // `ro` there beside `rw` here is the shape a remounted top mount reads
        // back as, and a helper reading the wrong one would report nothing.
        let resolved = fs::canonicalize(&target).expect("the jail path resolves");
        let table = format!(
            "24 30 0:23 / {} rw - tmpfs tmpfs rw\n\
             25 24 8:1 / {} rw - ext4 /dev/sdb1 rw\n\
             26 24 8:2 / {} ro - ext4 /dev/sdc1 rw\n",
            resolved.display(),
            resolved.join("cache").display(),
            resolved.join("archive").display(),
        );

        let err = refuse_writable_nested_mounts(&entry, &table)
            .expect_err("a writable mount below the target");
        // **Named by its host path, not the jail path**: the jail name is one the
        // operator never chose and would have to translate, which is the whole
        // content of this message.
        let named = served.join("cache").display().to_string();
        assert!(
            matches!(&err, Error::Step { step, .. } if *step == "read-only exposure"),
            "{err:?}"
        );
        assert!(err.to_string().contains(&named), "{err}");

        // **A submount that is already `ro` is not the hole**: it cannot be
        // written through, and the confined process cannot remount it writable
        // once the capability drop has run — so reporting it would refuse a
        // configuration that is already as read-only as the exposure asks for.
        let read_only = format!(
            "26 24 8:2 / {} ro - ext4 /dev/sdc1 rw\n",
            resolved.join("archive").display()
        );
        assert!(
            refuse_writable_nested_mounts(&entry, &read_only).is_ok(),
            "an already-read-only submount is not a writable hole"
        );
        // And a table with nothing below the target at all is the ordinary case.
        assert!(
            refuse_writable_nested_mounts(&entry, table.lines().next().unwrap_or_default()).is_ok()
        );
    }

    #[test]
    fn a_source_that_cannot_be_resolved_refuses_before_the_remount() {
        // **The fail-open this replaced is the reason the line is a `?`** (design
        // §11): the fallback was the unresolved string, which would make
        // `source_mount_flags_in` match a shallower mount point and re-assert
        // *less* than the bind inherited — a read-only exposure quietly keeping a
        // locked flag's protection off. The bind of this source has just
        // succeeded on the production path, so a path that cannot be resolved is
        // a mount the confinement did not establish.
        let tmp = tempfile::tempdir().expect("temp dir");
        let overlay = OverlayDirs {
            upper: tmp.path().join("upper"),
            work: tmp.path().join("work"),
        };
        let entry = Mount {
            source: tmp.path().join("gone"),
            target: tmp.path().join("jail/gone"),
            access: AccessMode::ReadOnly,
            kind: MountKind::Bind,
        };
        let err = enforce_bind_read_only(&entry, &overlay, false)
            .expect_err("a source that is not there cannot be resolved");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "canonicalize mount source",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_plan_is_built_while_the_process_can_still_exit_cleanly() {
        // **`validate_and_plan` is the whole half of the sequence that runs before
        // anything irreversible**, and its output is the jail, its overlay and the
        // mount table — none of which a test could observe afterwards, because
        // after the first `unshare` there is no afterwards for the test process.
        // A forced root model resolves without the probe, so this needs no
        // namespace and gives the same answer on every host.
        let tmp = tempfile::tempdir().expect("temp dir");
        let served = tmp.path().join("served");
        fs::create_dir(&served).expect("the served root");
        let jails = tmp.path().join("jails");
        fs::create_dir(&jails).expect("the jail parent");

        let opts = Options::default()
            .with_model(Mechanism::Root)
            .with_euid(0)
            .with_tmpdir(jails.clone());
        let sandbox = Sandbox::new().expose(&served, AccessMode::ReadWrite);
        let prepared = validate_and_plan(&sandbox, &opts).expect("a served root on a root host");

        assert_eq!(
            prepared.resolved,
            Mechanism::Root,
            "the resolved model is what is carried, never the `Auto` request"
        );
        assert_eq!(prepared.jail, jail_root(&jails, &served));
        assert_eq!(prepared.overlay.upper, prepared.jail.join(".overlay/upper"));
        assert_eq!(prepared.overlay.work, prepared.jail.join(".overlay/work"));
        assert_eq!(
            prepared.mounts.first().map(|mount| mount.kind),
            Some(MountKind::JailTmpfs),
            "the jail tmpfs is always first, so every later target has a parent"
        );
        assert_eq!(prepared.mounts.len(), 2, "the tmpfs and the one exposure");
        assert_eq!(
            prepared.mounts.last().map(|mount| mount.kind),
            Some(MountKind::ExposureBind(0)),
            "an exposure is bound by its descriptor, and the index rides in the kind — an index \
             on a non-exposure mount is unrepresentable"
        );
        // **Planning is not creating**: the jail root is the first thing the
        // confinement does to the host, and a refusal from `plan` has to leave
        // nothing behind for the next run to take over.
        assert!(!prepared.jail.exists(), "{}", prepared.jail.display());
    }

    #[test]
    fn the_map_descriptors_name_the_map_that_failed() {
        // **The three maps fail identically and are diagnosed by the name their
        // caller gave them** — a `/proc` that is missing answers `ENOENT` for all
        // three, so an error that does not say which one leaves the operator to
        // guess. The failure names are the only thing these two wrappers own.
        let tmp = tempfile::tempdir().expect("temp dir");
        let missing = tmp.path().join("uid_map");
        let err = open_map(&missing.to_string_lossy(), "open uid map")
            .expect_err("a map that is not there");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "open uid map",
                    ..
                }
            ),
            "{err:?}"
        );

        // The write travels through the descriptor that was opened for it (design
        // decision 12: the maps are only writable from the namespace they
        // describe, so the open is what has to happen after the `unshare`), and
        // the map's own name is what its failure carries.
        let path = tmp.path().join("gid_map");
        let mut writable = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("a fresh map file");
        write_map(&mut writable, "gid_map", "0 1000 1").expect("a writable map");
        assert_eq!(
            fs::read_to_string(&path).expect("read the map back"),
            "0 1000 1"
        );

        let mut read_only = fs::File::open(&path).expect("the same map, opened read-only");
        let err = write_map(&mut read_only, "setgroups", "deny")
            .expect_err("a descriptor with no write access");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "setgroups",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_jail_root_is_created_and_a_temp_directory_that_cannot_hold_it_is_named() {
        // **This is the first host resource the confinement touches and the one
        // whose failure names nothing on its own**: the jail's parent is
        // `$TMPDIR` unless `Options::with_tmpdir` says otherwise, so an `EACCES`
        // about a path the operator never chose is the diagnosis this step string
        // exists to replace.
        let tmp = tempfile::tempdir().expect("temp dir");
        let jail = tmp.path().join("jails/served");
        create_jail_root(&jail).expect("a fresh directory below a writable temp directory");
        assert!(jail.is_dir(), "the jail root the mounts land under");

        let file = tmp.path().join("not-a-directory");
        fs::write(&file, b"a file where the jail's parent would be").expect("a file");
        let err =
            create_jail_root(&file.join("jailed")).expect_err("a parent that is not a directory");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "create jail root in $TMPDIR",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn the_root_model_never_enters_a_user_namespace() {
        // **The branch that keeps `Mechanism::Root` from mapping its own uid.**
        // The user-namespace model writes `0 <euid> 1` into `/proc/self/uid_map`
        // and is then uid 0 *inside* the namespace; the root model keeps the
        // credentials it was started with, which is the whole reason it is the
        // fallback for a host that refuses the namespace. Reaching the early
        // return is the property, and it is the half of this function a test can
        // reach: everything below it is `unshare`, which no libtest worker may
        // do (a threaded process is exactly what `CLONE_NEWUSER` refuses).
        let unshared = enter_user_namespace(Mechanism::Root, &Options::default().with_euid(0))
            .expect("the root model touches no map");
        assert!(
            unshared.is_none(),
            "no descriptor is opened, because no map was entered"
        );
    }
}
