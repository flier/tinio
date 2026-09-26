//! The facade is the only crate a consumer names, so it is the only place
//! that has to prove the platform selection actually resolves.

#[cfg(unix)]
use std::fs::File;
#[cfg(any(target_os = "macos", target_os = "windows"))]
use std::net::TcpListener;
#[cfg(target_os = "macos")]
use std::os::unix::net::UnixListener;
use std::{
    ffi::{OsStr, OsString},
    io,
    path::PathBuf,
    process::ExitStatus,
    thread::JoinHandle,
};

use tinio_sandbox::{AccessMode, Backend, LaunchPlan, Sandbox, platform};

#[test]
fn the_facade_resolves_exactly_one_backend() {
    let mut backend = platform();
    // Confining an empty sandbox is allowed to fail for any platform-specific
    // reason; what this asserts is that the call reaches a real backend rather
    // than failing to compile or panicking on a missing selection.
    let result = backend.confine(&Sandbox::new(), &LaunchPlan::new());
    match result {
        Ok(_) => {}
        Err(err) => assert!(!err.to_string().is_empty()),
    }
}

#[test]
fn the_vocabulary_is_re_exported() {
    let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
    assert_eq!(sandbox.exposures().len(), 1);
    assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadOnly);
}

/// Every item the facade publishes, named through `tinio_sandbox::`.
///
/// **This test enforces one direction, and the direction it cannot enforce is
/// worth naming rather than implying.** Every binding below names a
/// `tinio_sandbox::` item together with the shape the core gives it, so the
/// facade has to supply that name with that shape: a name the facade claims
/// while the core does not publish it — or publishes with a different
/// signature, which the `fn`-pointer bindings catch — fails to compile here,
/// and so does a facade that stops re-exporting one of the names listed.
/// An earlier draft was wrong in both of those ways at once: it named `Error`,
/// `Result`, `Exposure` and `Sandbox` that the core did not export, while
/// `Event`, `adopt` and `Adopted` were missing from it. **`reemit` is not one of
/// these either way**: it is `pub(crate)` and `#[cfg(test)]`, so it is a name
/// neither list has ever had to carry.
///
/// **What it cannot see is a name that is not in this list.** A core item added
/// later and never re-exported leaves the facade — and this test — compiling,
/// so "the two lists are equal" is the rule those additions are made under
/// rather than a property this test proves. The paths are not mirrored either:
/// the facade publishes names flat, so `tinio_sandbox::launcher::Observer` does
/// not exist even though `tinio_sandbox_core::launcher::Observer` does.
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

    // The vocabulary — Task 1's half of the surface.
    let _: fn() -> tinio_sandbox::Sandbox = tinio_sandbox::Sandbox::new;
    let _: fn(&tinio_sandbox::Sandbox) -> &[tinio_sandbox::Exposure] =
        tinio_sandbox::Sandbox::exposures;
    let _: fn(Vec<tinio_sandbox::Exposure>, bool) -> tinio_sandbox::Sandbox =
        tinio_sandbox::Sandbox::from_exposures;
    let _: fn(tinio_sandbox::AccessMode) -> bool = tinio_sandbox::AccessMode::is_read_only;
    let _: fn(&tinio_sandbox::Error) -> String = |err| err.to_string();
    let _: tinio_sandbox::Result<()> = Ok(());
    // The four shapes a backend builds a failure through — `refused` is the
    // primitive, `fail` and `step` build `Error::Step`, `launch` builds
    // `Error::Launch`. The two `impl Into<…>` parameters are fixed by the
    // binding's own type, and `step` is called rather than bound: it returns an
    // opaque `impl Fn`, which no `fn`-pointer binding can name.
    let _: fn(&'static str, tinio_sandbox::Error) -> tinio_sandbox::Error = tinio_sandbox::refused;
    let _: fn(&'static str, io::Error) -> tinio_sandbox::Error = tinio_sandbox::fail;
    let _: fn(&'static str, PathBuf, io::Error) -> tinio_sandbox::Error = tinio_sandbox::launch;
    let _: tinio_sandbox::Error =
        tinio_sandbox::step::<io::Error>("mount")(io::Error::other("boom"));

    // The launcher machinery — Task 2's.
    let _: fn(
        &mut tinio_sandbox::Platform,
        &tinio_sandbox::Sandbox,
        &tinio_sandbox::LaunchPlan,
    ) -> tinio_sandbox::Result<tinio_sandbox::Confinement> =
        <tinio_sandbox::Platform as tinio_sandbox::Backend>::confine;
    let _: fn(String) -> tinio_sandbox::LaunchPlan = tinio_sandbox::LaunchPlan::for_launch;
    let _: fn(&tinio_sandbox::LaunchPlan) -> &str = tinio_sandbox::LaunchPlan::config;
    let _: fn(&str) -> Option<tinio_sandbox::Event> = tinio_sandbox::Event::decode;
    let _: fn(&mut tinio_sandbox::Observer<Vec<u8>>, &str, &str) -> Option<tinio_sandbox::Event> =
        tinio_sandbox::Observer::record;
    let _: fn(&tinio_sandbox::Observer<Vec<u8>>) -> Option<&tinio_sandbox::Terminal> =
        tinio_sandbox::Observer::terminal;
    let _: fn(Vec<OsString>, io::Empty) -> tinio_sandbox::Result<Option<tinio_sandbox::Adopted>> =
        tinio_sandbox::adopt;
    let _: fn(i64) -> OsString = tinio_sandbox::listener_fd_arg;
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_listener_fd_arg;
    let _: fn(&OsStr) -> Option<i64> = tinio_sandbox::listener_fd_from_arg;
    let _: &str = tinio_sandbox::LISTENER_FD_PREFIX;
    // The control channel's quartet, beside the listener's: the second inherited
    // descriptor a launcher hands over, spelled by the same four shapes — the Windows
    // launcher's pipe pool and the macOS launcher's single unix socket, both of which
    // are why the core grew a second prefix.
    let _: fn(i64) -> OsString = tinio_sandbox::control_fd_arg;
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_control_fd_arg;
    let _: fn(&OsStr) -> Option<i64> = tinio_sandbox::control_fd_from_arg;
    let _: &str = tinio_sandbox::CONTROL_FD_PREFIX;
    // The owned handle a plan carries and the platform clears the flag on: the value the
    // child's element is spelled from, on the platform where the clear has to happen after
    // the profile pre-flight. `with_control` is a builder rather than a `fn`-pointer shape,
    // so it is called with the plan it consumes.
    let planned = tinio_sandbox::LaunchPlan::default().with_control(control());
    let _: i64 = planned.controls()[0].value();
    let _: tinio_sandbox::Child = tinio_sandbox::Child::new(|| unreachable!("not waited"));
    let _: fn(
        tinio_sandbox::Child,
        JoinHandle<Option<tinio_sandbox::Terminal>>,
    ) -> tinio_sandbox::Child = tinio_sandbox::Child::with_relay;
    let _: fn(&tinio_sandbox::Child) -> Option<&tinio_sandbox::Terminal> =
        tinio_sandbox::Child::terminal;
    // `&mut Child` rather than `Child`: the caller reads the terminal after this
    // returns, and a by-value signature would take the child away (decision 19).
    let _: fn(&mut tinio_sandbox::Child, fn()) -> tinio_sandbox::Result<ExitStatus> =
        tinio_sandbox::supervise;
    let _: fn(&mut tinio_sandbox::LaunchPayload) -> &mut String = |payload| &mut payload.config;
}

/// The **launch-failure pair** — the killer a launcher may hand over, and the call that runs it —
/// resolves through the facade, and the closure handed to the builder is the one the action
/// reaches.
///
/// **Its own test rather than two bindings in the list above, because neither half is a `fn`
/// pointer.** `with_kill` is generic over the closure the caller writes and `kill` is an inherent
/// method rather than a free function, so what resolves is the name at the shape the macOS launcher
/// writes (`crates/tinio-cli/src/sandbox/macos.rs`'s `retract`: install the platform's kill, then
/// call it on the failure path). **The pair is reachable on every target and only *used* on the
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
#[cfg(any(target_os = "macos", target_os = "windows"))]
#[test]
fn the_adopted_names_answer_one_error_type() {
    let _: fn(&tinio_sandbox::Adopted) -> tinio_sandbox::Result<TcpListener> =
        tinio_sandbox::adopted_listener;
    let _: fn(fn()) -> tinio_sandbox::Result<()> = tinio_sandbox::on_stop;
    #[cfg(target_os = "macos")]
    let _: fn(&tinio_sandbox::Adopted) -> tinio_sandbox::Result<UnixListener> =
        tinio_sandbox::adopted_control;
}

#[cfg(target_os = "linux")]
#[test]
fn linux_selects_the_in_process_backend() {
    // An empty sandbox has nothing to confine to, so Linux refuses — which is
    // itself the proof that the Linux backend is the one wired in.
    let mut backend = platform();
    let err = backend
        .confine(&Sandbox::new(), &LaunchPlan::new())
        .expect_err("an empty sandbox must be refused");
    assert!(err.to_string().contains("nothing exposed"), "{err}");
}

// **macOS alone, until P3's backend landed — and the assertion is now the opposite one.** The
// Windows composition landed in P2's Task 9 and this test left that platform the moment its
// backend stopped answering `Unsupported`; P3's macOS composition has now landed, so the same
// thing is true here. What is asserted is the *composition* rather than a launch: a backend that
// reaches `confine` at all has replaced the stub, and an exposure it cannot express is the one
// refusal this call can produce without spawning `sandbox-exec` (a tree with no `sandbox-exec`
// answers `Unsupported`, which would make the assertion below vacuous).
#[cfg(target_os = "macos")]
#[test]
fn macos_selects_a_launcher_backend_rather_than_the_stub() {
    let mut backend = platform();
    let err = backend
        .confine(
            &Sandbox::new().expose("relative/path", AccessMode::ReadOnly),
            &LaunchPlan::new(),
        )
        .expect_err("a relative exposure cannot be expressed in a profile");
    assert!(
        matches!(err, tinio_sandbox::Error::BadPath { .. }),
        "the macOS backend validates its exposures: {err}"
    );
    assert!(
        !err.to_string().contains("not implemented"),
        "the stub is gone: {err}"
    );
}

/// **The macOS mechanism is reachable through this facade, and the backend this
/// facade builds is the default one.**
///
/// The two halves are one fact stated from the two sides the CLI's arm needs: the
/// platform's vocabulary (`Options`, `Mechanism`) is at `tinio_sandbox::macos::…`,
/// so that arm writes one crate rather than a target-gated dependency of its own
/// (design decision 2), and the default mechanism is **`sandbox-exec`** — the one
/// every allowance was measured against and the one a run that sets no variable
/// gets. A facade that stopped re-exporting the module, or a `Options::default`
/// that answered the other mechanism, fails here rather than in a launch.
#[cfg(target_os = "macos")]
#[test]
fn macos_publishes_the_mechanism_and_defaults_to_the_helper() {
    use tinio_sandbox::macos::{Mechanism, Options};

    assert_eq!(Options::default().mechanism(), Mechanism::SandboxExec);
    assert_eq!(
        Mechanism::from_env(None).expect("absent means the default"),
        Mechanism::SandboxExec
    );
    assert_eq!(
        Mechanism::from_env(Some("sandbox-init")).expect("the alternative"),
        Mechanism::SandboxInit
    );
}
