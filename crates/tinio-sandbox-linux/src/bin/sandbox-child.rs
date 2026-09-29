//! The confined half of the jail test: build the jail, then report what is
//! reachable from inside it, one `key: value` line per check on stdout.
//!
//! A separate executable rather than a re-executed test binary, because
//! `unshare(CLONE_NEWUSER)` refuses in a multi-threaded process and libtest
//! runs every test on a worker thread. `fn main` is single-threaded, which is
//! the one property this file depends on.
//!
//! A `[[bin]]` target rather than an example, which is also a measured choice:
//! `CARGO_BIN_EXE_<name>` exists for binaries only (naming an example in
//! `env!(…)` fails to compile), and a filtered `cargo test --test jail` builds
//! binaries but not examples — so the profile-walking fallback an example would
//! need breaks on exactly the command this test is run with.
//!
//! stdout rather than a file: after `pivot_root` the only paths that exist are
//! the ones the jail exposes, so a report written to the parent's temp
//! directory would fail precisely when it is most needed — on the host that
//! cannot confine and must say so.
//!
//! **Every import is inside the gated `main`, and the gate is why.** A crate
//! root's `#![cfg]` does not cover `src/bin/`, so this target is built on hosts
//! where `tinio-sandbox-linux` is an empty library — and a file-scope
//! `use tinio_sandbox_linux::{…}` there is `E0432: unresolved import`, which
//! stops `cargo check --workspace --all-targets` on every non-Linux host. The
//! std imports sit beside them because the only thing that uses them is the
//! same gated body, and an ungated one would be an unused import there.

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        env, fs,
        net::{TcpListener, TcpStream},
        path::Path,
        process::Command,
    };

    use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, Sandbox};
    use tinio_sandbox_linux::{Backend, Options};

    // **The second mode of this probe, and it exists so a prober installs the
    // crate's own program rather than a copy of the policy.** `filter_program`
    // is pure and the filter is irreversible, so a test cannot install it and a
    // prober that spelled the instruction list a second time could drift from
    // the one the kernel actually gets. This mode prints the compiled program —
    // one `struct sock_filter` per line, `code jt jf k` — and returns before
    // anything is unshared, mapped or mounted; the WSL2 script's harness is what
    // reads it (`scripts/wsl-chroot.sh`'s seccomp leg).
    if env::var_os("TINIO_SANDBOX_TEST_DUMP_FILTER").is_some() {
        use tinio_sandbox_linux::seccomp::{Arch, filter_program};

        let arch = Arch::current().expect("this target has a syscall table");
        for instruction in filter_program(arch) {
            println!(
                "{:04x} {:02x} {:02x} {:08x}",
                instruction.code, instruction.jt, instruction.jf, instruction.k
            );
        }
        return;
    }

    let root = env::var("TINIO_SANDBOX_TEST_ROOT").expect("child needs a root");
    // **The address the parent is listening on, not one on the network.** It is
    // named by the parent because only the parent can hold the listener that
    // makes it reachable by construction, and a probe that is reachable by
    // construction is what stops the outbound assertion from being skipped for
    // an environmental reason.
    let probe = env::var("TINIO_SANDBOX_TEST_PROBE").expect("child needs a probe address");
    let access = match env::var("TINIO_SANDBOX_TEST_ACCESS").as_deref() {
        Ok("read-only") => AccessMode::ReadOnly,
        _ => AccessMode::ReadWrite,
    };
    let state = env::var("TINIO_SANDBOX_TEST_STATE").ok();

    let mut sandbox = Sandbox::new().expose(&root, access);
    if let Some(state) = &state {
        sandbox = sandbox.expose(state, AccessMode::ReadWrite);
    }

    // **The model comes from the environment, through its one reader.** The two
    // legs of the WSL2 run must not run the same code — and the code they
    // differ in is the read-only mechanism, which the *source mount's flags*
    // pick rather than the model (design §4): the served root here is a tempdir
    // under `/tmp`, whose tmpfs is `nosuid,nodev`, so its per-mount flags
    // arrive locked in a namespace owned by a different user namespace and the
    // read-only remount of it is refused with `EPERM`. So `=userns` reaches
    // that `EPERM` and takes the `MS_RDONLY` overlay fallback, while `=root`
    // under `sudo` gets the remount the design calls the primary mechanism;
    // those two are the implementations of `AccessMode::ReadOnly` this platform
    // has (design §9, §12). The model read from the environment is what decides
    // *which of the two a leg can reach at all*. A child that discovered its
    // own model — `Auto`, or a hand-rolled "root when euid is 0 and the probe
    // failed" — would choose the *same* one
    // on a userns-capable host under `sudo`, so the remount path would never run
    // while the suite reported success for having tested half of the matrix.
    // Unset is still `Auto`, which is the right answer for a hand run.
    //
    // No `with_euid`: this process *is* the one being confined, so
    // `Options::euid()` reads the kernel — the production path, and the one
    // worth exercising.
    //
    // **`with_device_files(true)` is what the production caller asks for too**:
    // `serve` exposes the four device files, and the jail is a tmpfs that
    // carries none of them unless they are bound in. **The served root's access
    // is deliberately not set here**: `enter` derives it from the exposure
    // above — the same `access` this process exposed the root with — and that
    // second spelling is what once handed a `--read-only` run a read-write
    // served root.
    let options = match Options::from_env() {
        Ok(options) => options.with_device_files(true),
        Err(err) => {
            // **Not `confine-failed:`.** A typo in the variable is a
            // configuration error the parent has to fail on, not a host that
            // cannot confine; the prefix is what tells the two apart.
            println!("bad-model: {err}");
            return;
        }
    };
    let mut backend = Backend::with_options(options);

    if let Err(err) = backend.confine(&sandbox, &Default::default()) {
        println!("confine-failed: {err}");
        return;
    }

    match fs::read_to_string(Path::new(&root).join("bucket/obj.txt")) {
        Ok(body) => println!("read-ok: {}", body.trim()),
        Err(err) => println!("read-failed: {err}"),
    }
    println!("host-etc-visible: {}", Path::new("/etc/passwd").exists());
    // **The host's own `/etc/passwd` *through* the old root, which is the
    // classic `pivot_root` escape.** `/.oldroot` itself survives the detach — it
    // is the mount point directory `enter` created — so a probe for the
    // directory answers `true` either way and proves nothing; a probe for a file
    // only the host's root has is `true` exactly while the old root is still
    // mounted there, which is what a dropped or broken
    // `unmount("/.oldroot", UnmountFlags::DETACH)` leaves behind.
    println!(
        "old-root-visible: {}",
        Path::new("/.oldroot/etc/passwd").exists()
    );
    // **`$HOME` itself, not `$HOME/.ssh`.** The narrower probe is `false` on a
    // host with no `~/.ssh` whether the process is confined or not, so it could
    // only ever agree with the run; the directory is `true` on the host and
    // `false` in the jail on every host, because the parent makes it and nothing
    // mirrors it in.
    println!(
        "host-home-visible: {}",
        env::var("HOME")
            .map(|home| Path::new(&home).is_dir())
            .unwrap_or(false)
    );
    match fs::write(Path::new(&root).join("bucket/should-not-exist"), b"nope") {
        Ok(()) => println!("served-root-write: ALLOWED"),
        Err(err) => println!("served-root-write: refused ({})", err.kind()),
    }
    // **A symlink in the served root, and whether this view follows it.** A
    // per-mount flag is invisible from in here unless something it governs is
    // reachable: `nosuid`/`nodev` want a setuid binary or a device node and
    // `noexec` an executable, while `nosymfollow` wants one symlink — one line of
    // the parent's fixture, and the only one of the four whose loss nothing else
    // reports (`source_mount_flags_in`'s doc says why). The opt-in
    // nested-user-namespace leg seeds `alias`, and beside it the executable the
    // `noexec` probe below runs; on the legs that seed neither, `NotFound` is
    // the honest reading of "this root has none".
    match fs::read(Path::new(&root).join("alias")) {
        Ok(_) => println!("served-root-symlink: followed"),
        Err(err) => println!("served-root-symlink: refused ({:?})", err.kind()),
    }
    // **Whether this view may execute a file in the served root, on the same
    // terms** — the wrapper of the opt-in nested-user-namespace leg seeds an
    // executable `exec-probe` inside its `noexec` tmpfs, and the legs that seed
    // nothing answer `NotFound`.
    //
    // **The kind is the assertion, never the failure**, and the reason is what
    // this probe is for: `EACCES` (`PermissionDenied`) is the *mount* refusing
    // the exec, while a run whose arm dropped the flag gets past that check and
    // dies later on the interpreter the jail does not carry (`NotFound`: the
    // seeded file's `#!/bin/sh` names a `/bin/sh` no jail exposes) — so "the
    // exec did not succeed" would hold in both states and pin neither. That is
    // also why the file is a script rather than this very binary: a copy of a
    // dynamic executable would fail the same way for the same missing reason.
    match Command::new(Path::new(&root).join("exec-probe")).output() {
        Ok(_) => println!("served-root-exec: allowed"),
        Err(err) => println!("served-root-exec: refused ({:?})", err.kind()),
    }
    // **Which of the two read-only implementations actually ran**, reported as
    // the one fact that separates them: the overlay arm creates `OverlayDirs`'
    // two directories under the jail root and the plain remount arm creates
    // neither, so `/.overlay/upper` inside the jail is a consequence of the
    // branch and not an inference about the host. `statfs`'s
    // `OVERLAYFS_SUPER_MAGIC` would be the inference: a served root that is
    // already an overlay — anything in a container — would read as `overlay` on
    // the remount arm. Nothing to report when nothing was exposed read-only,
    // which is why this line is absent rather than `none` on a read-write run.
    if access.is_read_only() {
        println!(
            "read-only-arm: {}",
            if Path::new("/.overlay/upper").exists() {
                "overlay"
            } else {
                "remount"
            }
        );
    }
    if let Some(state) = &state {
        match fs::write(Path::new(state).join("log.txt"), b"ok") {
            Ok(()) => println!("state-dir-write: ALLOWED"),
            Err(err) => println!("state-dir-write: refused ({})", err.kind()),
        }
    }
    // **The error's kind is reported beside the error, because those two are
    // the whole diagnosis.** A refusal is not enough on its own: `ECONNREFUSED`
    // from a probe nothing is listening on and `EPERM` from the filter are the
    // same word and opposite facts, and only the second is the network cut.
    println!(
        "outbound-connect: {}",
        match TcpStream::connect(probe.as_str()) {
            Ok(_) => "SUCCEEDED".to_string(),
            Err(err) => format!("refused ({:?}: {err})", err.kind()),
        }
    );
    println!(
        "loopback-listen: {}",
        TcpListener::bind("127.0.0.1:0").is_ok()
    );
}

/// The workspace builds on every host, so this target has to exist on the ones
/// that cannot confine: an empty `main` keeps `cargo check --workspace` and
/// `cargo clippy --all-targets` honest everywhere. Only the Linux-gated test
/// ever spawns this binary, so on any other target there is nothing to run.
#[cfg(not(target_os = "linux"))]
fn main() {}
