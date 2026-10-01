//! The facade's **published surface, and nothing else**: every name a consumer writes as
//! `tinio_sandbox::…` is bound here, because that is the one assertion an in-crate `#[cfg(test)]`
//! module cannot make — a re-export narrowed from `pub` to crate visibility still compiles inside
//! the crate, so only a consumer's side of the boundary sees the reduction.
//!
//! The facade's own *logic* — the backend this build selects, the platform selection resolving to
//! a real backend, the target backend's enforcement, the macOS mechanism's defaults — lives beside
//! the code it exercises, in `src/lib.rs` (`plans/2026-10-01-test-placement-cleanup.md`).

#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::os::fd::{BorrowedFd, OwnedFd};
#[cfg(unix)]
use std::time::Duration;
use std::{
    ffi::{OsStr, OsString},
    io,
    net::TcpListener,
    path::PathBuf,
    process::ExitStatus,
    thread::JoinHandle,
};

// The pid the fork-probe handshake takes, under the same `#[cfg]` the names that
// use it are: `tinio-sandbox` re-exports the probe because the equality with the
// core's surface is the rule, and a `#[cfg(unix)]` dev-dependency is what lets
// this file name the one foreign type in that surface.
#[cfg(unix)]
use rustix::process::Pid;
// The core crate, under the facade's own alias for it: this is the path the **type
// half** of every binding in `the_launcher_surface_is_re_exported` names, so a name the
// core stops publishing fails there rather than only on the facade's side.
use tinio_sandbox::_core as core;
use tinio_sandbox::{AccessMode, Backend, ControlSocket, Sandbox, serve};

/// Binding the host predicate here is what makes a facade-native name that stops resolving a
/// compile error rather than a discovery: `the_launcher_surface_is_re_exported` can see only the
/// names its re-export list carries, and this one is not among them. `backend_available` is the
/// neighbouring facade-native case, and it is bound nowhere: its only binding is the doctest in
/// `lib.rs` that asserts the answer this target gives, which pins no name against drift.
#[test]
fn the_host_predicate_is_published() {
    let _: fn() -> Result<(), tinio_sandbox::Error> = tinio_sandbox::can_confine;
}

#[test]
fn the_vocabulary_is_re_exported() {
    let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
    assert_eq!(sandbox.exposures().len(), 1);
    assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadOnly);
}

/// Every item the facade publishes, named through `tinio_sandbox::`.
///
/// **Every name this list puts in a type position is bound through both crates,
/// and that is what makes this list the core's list rather than a second
/// hand-copy of it.** The *type* half of a binding that names a core item is the
/// core's path (`tinio_sandbox::_core::…`, the facade's own alias for the core
/// crate) and its *value* half is the facade's, so one line fails for either of
/// the two ways the surface can drift: a name the core stops publishing (or
/// publishes with another shape, which the `fn`-pointer half catches) fails the
/// type, and a name the core publishes but the facade does not re-export fails
/// the value. An earlier draft enforced only the facade's side, which let a core
/// item — `SandboxSpec` was the live one — be omitted from the facade with this
/// test still green; an earlier draft before that was wrong in both directions at
/// once, naming `Error`, `Result`, `Exposure` and `Sandbox` that the core did not
/// export while `Event`, `adopt` and `Adopted` were missing from the facade.
///
/// **One direction still cannot be seen, and it is inherent rather than an
/// omission.** A core item added to `tinio_sandbox_core`'s root and never
/// re-exported leaves the facade compiling, and this test cannot name a name it
/// does not list — Rust has no way to enumerate another crate's surface at
/// compile time. So "the two lists are equal" remains the rule those additions
/// are made under, and the core's root list (`crates/tinio-sandbox-core/src/lib.rs`,
/// whose comment states that rule) is where the owner of the list lives. What this
/// test guarantees is the other half: **every name this list carries is bound in
/// both crates**, so no name *on* the list can disappear from either side
/// unnoticed, and the drift is a missing line in one file rather than a silently
/// narrower facade.
///
/// **"Every name it carries" is a property to re-check when a name is added, not
/// a claim to take on trust** — the failure it guards against is silent by
/// construction, and it has already happened once: `Exposure`, `Terminal`,
/// `LaunchPayload`, `Confinement` and `is_launcher_fd_arg` were all on the list
/// and none was bound through the facade, so five deletions would have left this
/// test green. Types that appear only in type positions need a **value** binding
/// of their own (a literal, or a variant); functions and constants are bound by a
/// path in the value position. The check is mechanical:
/// `rg -o 'tinio_sandbox::([A-Za-z_]+)' crates/tinio-sandbox/tests/facade.rs` minus
/// the names the file's own `use` line brings in must cover the list in
/// `crates/tinio-sandbox/src/lib.rs`.
///
/// The paths are not mirrored either: the facade publishes names flat, so
/// `tinio_sandbox::launcher::Observer` does not exist even though
/// `tinio_sandbox_core::launcher::Observer` does.
///
/// Written as a binding per item rather than a `use` list so the failure names
/// the missing symbol.
#[test]
fn the_launcher_surface_is_re_exported() {
    // The imports this test needs are at the file top, not here: `docs/style.md`
    // says "Top `use`; never inline".
    //
    // `control` is the one local helper, because the owned control handle's constructor takes a
    // real descriptor on the platform that duplicates one and a value on the platform that does
    // not — and this test's subject is the facade's list of names, not that difference.
    let control = || -> tinio_sandbox::ControlHandle {
        #[cfg(unix)]
        {
            let source = File::open("/dev/null").expect("a descriptor to duplicate");
            tinio_sandbox::ControlHandle::duplicate(&source).expect("duplicate a descriptor")
        }
        #[cfg(windows)]
        {
            tinio_sandbox::ControlHandle::inherited(1040)
        }
    };

    // serve::Spec is the facade's own confine entry (not a core re-export): bind the constructor
    // and apply so a rename or drop fails here rather than only at a CLI call site.
    let _: fn() -> serve::Spec = serve::Spec::new;
    let _: fn(serve::Spec) -> tinio_sandbox::Result<Option<tinio_sandbox::Child>> =
        serve::Spec::apply;

    // The vocabulary — Task 1's half of the surface.
    let _: fn() -> core::Sandbox = tinio_sandbox::Sandbox::new;
    let _: fn(&core::Sandbox) -> &[core::Exposure] = tinio_sandbox::Sandbox::exposures;
    let _: fn(&core::Sandbox) -> core::Result<&core::Exposure> =
        tinio_sandbox::Sandbox::served_root;
    let _: fn(Vec<core::Exposure>) -> core::Sandbox = tinio_sandbox::Sandbox::from_exposures;
    let _: fn(&[core::Exposure]) -> String = tinio_sandbox::describe_exposures;
    let _: fn(core::AccessMode) -> bool = tinio_sandbox::AccessMode::is_read_only;
    let _: fn(&core::Error) -> String = |err| err.to_string();
    let _: core::Result<()> = Ok(());
    // **The enforcement vocabulary — the strength a backend answers for, and the shapes a
    // command's end is classified by.** Each is bound through both crates for the reason the
    // three names below are, and each carries a value rather than a type alone: a variant in the
    // value position is what pins the declaration and the expression as one type, and the
    // `fn`-pointer is what pins `diagnostics` to the core's own answer type.
    let _: core::Diagnostics = tinio_sandbox::Diagnostics::NOT_REPORTED;
    let _: core::Enforcement = tinio_sandbox::Enforcement::Full;
    let _: core::Refused = tinio_sandbox::Refused::Path(PathBuf::from("/srv/data/.tinio"));
    let _: core::Attribution = tinio_sandbox::Attribution::Broken {
        detail: String::new(),
    };
    // The same enum's other arm, bound as a value for the same reason: it is what the launcher
    // answers for a child that reported and then ended, so a consumer matching on it reaches
    // this variant through the facade rather than only through the core.
    let _: core::Attribution = tinio_sandbox::Attribution::Ran;
    let _: fn() -> core::Diagnostics = tinio_sandbox::diagnostics;
    // **A type that is only ever a *type* needs a value binding to be bound at all**, and
    // these three are the names that slipped through for exactly that reason: each appeared
    // beneath the core's path and never the facade's, so deleting it from the facade left this
    // test green. A literal answers both halves at once — the declared type is the core's
    // spelling and the expression is the facade's, which pins them as one type and pins the
    // public shape a consumer writes.
    let _: core::Exposure = tinio_sandbox::Exposure {
        host: PathBuf::from("/srv/data"),
        access: AccessMode::ReadOnly,
    };
    let _: core::Terminal =
        tinio_sandbox::Terminal::Listening("127.0.0.1:9000".parse().expect("addr"));
    let _: core::Confinement = tinio_sandbox::Confinement::InPlace;
    // The four shapes a backend builds a failure through — `refused` is the
    // primitive, `fail` and `step` build `Error::Step`, `launch` builds
    // `Error::Launch`. The two `impl Into<…>` parameters are fixed by the
    // binding's own type, and `step` is called rather than bound: it returns an
    // opaque `impl Fn`, which no `fn`-pointer binding can name.
    let _: fn(&'static str, core::Error) -> core::Error = tinio_sandbox::refused;
    let _: fn(&'static str, io::Error) -> core::Error = tinio_sandbox::fail;
    let _: fn(&'static str, PathBuf, io::Error) -> core::Error = tinio_sandbox::launch;
    let _: core::Error = tinio_sandbox::step::<io::Error>("mount")(io::Error::other("boom"));

    // The launcher machinery — Task 2's.
    //
    // **`platform()`'s answer is one more `Backend`, and this is the only name for that
    // trait.** The arm used to republish the core's `Backend` as `Platform`, which is one
    // fact with two public spellings — the rule the module doc above states; the constructor
    // stays because which type answers the trait is the platform's own and a consumer cannot
    // name one type for three targets.
    let mut platform = tinio_sandbox::platform();
    let _: &mut dyn Backend = &mut platform;
    // The object lifetime is spelled rather than left to the default: `&mut dyn Backend` in
    // this position means `&'a mut (dyn Backend + 'a)`, which is a *more* general pointer
    // than the method's `for<'a> fn(&'a mut (dyn Backend + 'static), …)`.
    let _: fn(
        &mut (dyn Backend + 'static),
        &core::Sandbox,
        &core::LaunchPlan,
    ) -> core::Result<core::Confinement> = <dyn Backend as Backend>::confine;
    let _: fn(String) -> core::LaunchPlan = tinio_sandbox::LaunchPlan::for_launch;
    let _: fn(&core::LaunchPlan) -> &str = tinio_sandbox::LaunchPlan::config;
    // **The predicate beside the text it qualifies**, and the pair a launcher's precondition
    // reads: `config()` answers the text, `has_config()` answers whether there is one at all —
    // an empty text being a configuration the child parses into defaults rather than a plan
    // nothing was ever handed.
    let _: fn(&core::LaunchPlan) -> bool = tinio_sandbox::LaunchPlan::has_config;
    let _: fn(&str) -> Option<core::Event> = tinio_sandbox::Event::decode;
    let _: fn(&mut core::Observer<Vec<u8>>, &str, &str) -> Option<core::Event> =
        tinio_sandbox::Observer::record;
    let _: fn(&core::Observer<Vec<u8>>) -> Option<&core::Terminal> =
        tinio_sandbox::Observer::terminal;
    let _: fn(Vec<OsString>, io::Empty) -> core::Result<Option<core::Adopted>> =
        tinio_sandbox::adopt;
    // **`filter_argv` is the one binding here whose answer is a pair**, and the tuple nested in a
    // `Result` in a `fn` pointer is what took this line over clippy's `type_complexity`
    // threshold. The pair is named once, beside the binding that spells it, rather than at the
    // file top: no other name in the list carries the filtered argv with the adopted record.
    type Filtered = (Vec<OsString>, Option<core::Adopted>);
    let _: fn(Vec<OsString>, io::Empty) -> core::Result<Filtered> = tinio_sandbox::filter_argv;
    let _: fn(i64) -> OsString = tinio_sandbox::listener_fd_arg;
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_listener_fd_arg;
    let _: fn(&OsStr) -> Option<i64> = tinio_sandbox::listener_fd_from_arg;
    let _: &str = tinio_sandbox::LISTENER_FD_PREFIX;
    // The predicate over **both** elements, which had no binding at all: it answers the
    // question every argv filter asks, so a facade that stopped publishing it would take out
    // the one call that keeps a descriptor element out of clap's parser.
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_launcher_fd_arg;
    // The control channel's quartet, beside the listener's: the second inherited
    // descriptor a launcher hands over, spelled by the same four shapes — the Windows
    // launcher's pipe pool and the macOS launcher's single unix socket, both of which
    // are why the core grew a second prefix.
    let _: fn(i64) -> OsString = tinio_sandbox::control_fd_arg;
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_control_fd_arg;
    let _: fn(&OsStr) -> Option<i64> = tinio_sandbox::control_fd_from_arg;
    let _: &str = tinio_sandbox::CONTROL_FD_PREFIX;
    // **The confinement a launcher hands a child that confines itself**, and the reason
    // this guard had to be widened: it is the public field type of
    // `LaunchPayload::confinement` and the answer `Adopted::confinement` gives, so a
    // facade consumer naming either must be able to name this type — and the facade
    // omitted it while this test stayed green, because the list simply did not contain it.
    let _: core::SandboxSpec = tinio_sandbox::SandboxSpec {
        mechanism: String::new(),
        profile: String::new(),
        params: Vec::new(),
    };
    let _: fn(&core::Adopted) -> Option<&core::SandboxSpec> = tinio_sandbox::Adopted::confinement;
    let _: fn(&mut core::LaunchPayload) -> &mut Option<core::SandboxSpec> =
        |payload| &mut payload.confinement;
    // The owned handle a plan carries and the platform clears the flag on: the value the
    // child's element is spelled from, on the platform where the clear has to happen after
    // the profile pre-flight. `with_control` is a builder rather than a `fn`-pointer shape,
    // so it is called with the plan it consumes.
    let planned = tinio_sandbox::LaunchPlan::default().with_control(control());
    let _: i64 = planned.controls()[0].value();
    let _: core::Child = tinio_sandbox::Child::new(|| unreachable!("not waited"));
    let _: fn(core::Child, JoinHandle<Option<core::Terminal>>) -> core::Child =
        tinio_sandbox::Child::with_relay;
    let _: fn(&core::Child) -> Option<&core::Terminal> = tinio_sandbox::Child::terminal;
    // `&mut Child` rather than `Child`: the caller reads the terminal after this
    // returns, and a by-value signature would take the child away (decision 19).
    let _: fn(&mut core::Child, fn()) -> core::Result<ExitStatus> = tinio_sandbox::supervise;
    let _: fn(&mut core::LaunchPayload) -> &mut String = |payload| &mut payload.config;
    // **The payload itself, from both sides**, and not only through the two field projections
    // above: those named it beneath the core's path alone, so the facade's own spelling was
    // never exercised. The literal is the facade's name over the core's declared type, so the
    // two are pinned as one type and the three public fields are pinned with them.
    let _: core::LaunchPayload = tinio_sandbox::LaunchPayload {
        config: String::new(),
        token: None,
        confinement: None,
    };
    // **The fork probe's three names, bound where a probe exists** — the core publishes them under
    // the same `#[cfg]`, because a probe is a unix handshake (a fork, a pipe, a `waitpid`) and the
    // list above has to compile on every target. `await_verdict` is the `fn`-pointer binding and
    // the other two are values: the step names through the constructor, and the child's half
    // through the report it writes.
    #[cfg(unix)]
    {
        let _: fn(OwnedFd, Pid, core::ProbeSteps, Duration) -> core::Result<bool> =
            tinio_sandbox::await_verdict;
        let _: fn(OwnedFd, bool) -> io::Result<()> = tinio_sandbox::report_verdict;
        let _: core::ProbeSteps = tinio_sandbox::ProbeSteps::named("probe");
    }
    // **The macOS mechanism's re-export half**, and the reason it is bound here rather than in the
    // test that moved: `tinio_sandbox::macos::…` is the path a consumer writes, so the binding
    // belongs at this boundary, the one place a narrowed re-export is visible at all (file
    // header). The defaults themselves moved beside the selection they describe, in `src/lib.rs`.
    #[cfg(target_os = "macos")]
    {
        let _: tinio_sandbox::macos::Mechanism = tinio_sandbox::macos::Mechanism::SandboxExec;
        let _: tinio_sandbox::macos::Options = tinio_sandbox::macos::Options::default();
    }
}

/// The **launch-failure pair** — the killer a launcher may hand over, and the call that runs it —
/// resolves through the facade, and the closure handed to the builder is the one the action
/// reaches.
///
/// **Its own test rather than two bindings in the list above, because neither half is a `fn`
/// pointer.** `with_kill` is generic over the closure the caller writes and `kill` is an inherent
/// method rather than a free function, so what resolves is the name at the shape the launcher
/// writes (`crates/tinio-cli/src/sandbox/confine.rs`'s failed-publish path: `child.abort()`, which
/// runs the platform's killer and reaps). **The pair is reachable on every target and only *used* on the
/// launcher platforms**: `tinio-sandbox-core`'s `Child` carries both methods unconditionally, while
/// the `Option` is `None` for every child whose platform already owns the process's lifetime — see
/// `crates/tinio-sandbox-core/src/launcher/supervise.rs` for why a dropped child is not an ended
/// one where no job object exists.
#[test]
fn the_launch_failure_surface_is_re_exported() {
    let mut ended: tinio_sandbox::Child =
        tinio_sandbox::Child::new(|| unreachable!("not waited")).with_kill(|| Ok(()));
    let _: fn(&mut tinio_sandbox::Child) -> io::Result<()> = tinio_sandbox::Child::kill;
    ended
        .kill()
        .expect("the child reaches the killer `with_kill` installed");
}

/// **The control handoff carries the platform's own form, and this leg is where that is
/// pinned** — the one facade shape the 2026-09-27 change split in two.
///
/// A unix launcher hands **borrowed descriptors**, so the "this descriptor is open in this
/// process" fact is the type system's before any code runs, while Windows hands the
/// **handle values** its channel keeps owning — the form `CreateProcessW` marks inheritable.
/// A facade that went back to one `&[i64]` fails the unix binding here, and one that gave
/// Windows the unix shape fails the other arm: the equality this test keeps is with the
/// platform's handoff form, which is why the two arms are `#[cfg]`-selected rather than
/// bent into one signature.
#[test]
fn the_control_handoff_carries_the_platforms_own_form() {
    #[cfg(unix)]
    {
        let _: fn(&[BorrowedFd<'_>]) -> core::Result<Vec<core::ControlHandle>> =
            tinio_sandbox::control_handles;
    }
    #[cfg(not(unix))]
    {
        let _: fn(&[i64]) -> core::Result<Vec<core::ControlHandle>> =
            tinio_sandbox::control_handles;
    }
}

/// **The three adopted names answer one type on every platform that publishes
/// them** — the rule [`tinio_sandbox::adopted_listener`] states, pinned the way
/// `the_launcher_surface_is_re_exported` pins the core's surface: a `fn`-pointer
/// binding per name, at the shared [`tinio_sandbox::Result`].
///
/// **Why the arms repeat and why that is not a copy.** Each name is
/// `#[cfg]`-selected — the child path that calls them is gated by `feature = "sandbox"`
/// alone — so one binding cannot cover both platforms, and what the pair proves is
/// the fact no single-target leg can see: the platform crate's own error
/// (`std::io::Result` on macOS, the core's `Result` on Windows) does not reach a
/// caller of the facade. An arm that went back to it stops compiling here.
///
/// `on_stop` is bound by instantiating its `impl Fn()` parameter with `fn()`
/// (`fn()` is `Fn() + Send + 'static`), which is the only shape a `fn`-pointer
/// binding can name.
#[test]
fn the_adopted_names_answer_one_error_type() {
    let _: fn(&tinio_sandbox::Adopted) -> tinio_sandbox::Result<TcpListener> =
        tinio_sandbox::adopted_listener;
    let _: fn(fn()) -> tinio_sandbox::Result<()> = tinio_sandbox::on_stop;
    let _: fn(&tinio_sandbox::Adopted) -> tinio_sandbox::Result<ControlSocket> =
        tinio_sandbox::adopted_control;
    // The three typo sentences, bound the same way and for the same reason: every
    // selector's refusal is one class of operator mistake — the core's `Usage` variant —
    // and the construction below is the whole classification check, because the class is
    // the variant the caller builds rather than a sentence a caller matches. What the
    // assertion adds is the other half: the operator's sentence renders unchanged through
    // the facade.
    let _: &'static str = tinio_sandbox::MACOS_MECHANISM_USAGE;
    let mechanism = tinio_sandbox::Error::Usage {
        detail: tinio_sandbox::MACOS_MECHANISM_USAGE,
    };
    assert!(
        mechanism
            .to_string()
            .contains(tinio_sandbox::MACOS_MECHANISM_USAGE),
        "{mechanism}"
    );
    let _: &'static str = tinio_sandbox::LINUX_MODEL_USAGE;
    let model = tinio_sandbox::Error::Usage {
        detail: tinio_sandbox::LINUX_MODEL_USAGE,
    };
    assert!(
        model.to_string().contains(tinio_sandbox::LINUX_MODEL_USAGE),
        "{model}"
    );
    let _: &'static str = tinio_sandbox::WINDOWS_CAPABILITY_USAGE;
    let capability = tinio_sandbox::Error::Usage {
        detail: tinio_sandbox::WINDOWS_CAPABILITY_USAGE,
    };
    assert!(
        capability
            .to_string()
            .contains(tinio_sandbox::WINDOWS_CAPABILITY_USAGE),
        "{capability}"
    );
}
