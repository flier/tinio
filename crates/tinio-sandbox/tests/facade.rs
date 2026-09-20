//! The facade is the only crate a consumer names, so it is the only place
//! that has to prove the platform selection actually resolves.

use std::{
    ffi::{OsStr, OsString},
    io,
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

    // The vocabulary — Task 1's half of the surface.
    let _: fn() -> tinio_sandbox::Sandbox = tinio_sandbox::Sandbox::new;
    let _: fn(&tinio_sandbox::Sandbox) -> &[tinio_sandbox::Exposure] =
        tinio_sandbox::Sandbox::exposures;
    let _: fn(Vec<tinio_sandbox::Exposure>, bool) -> tinio_sandbox::Sandbox =
        tinio_sandbox::Sandbox::from_exposures;
    let _: fn(tinio_sandbox::AccessMode) -> bool = tinio_sandbox::AccessMode::is_read_only;
    let _: fn(&tinio_sandbox::Error) -> String = |err| err.to_string();
    let _: tinio_sandbox::Result<()> = Ok(());

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

#[cfg(any(target_os = "macos", target_os = "windows"))]
#[test]
fn the_launcher_platforms_answer_unimplemented_for_now() {
    let mut backend = platform();
    let err = backend
        .confine(&Sandbox::new(), &LaunchPlan::new())
        .expect_err("the stub backends are not implemented yet");
    assert!(err.to_string().contains("not implemented"), "{err}");
}
