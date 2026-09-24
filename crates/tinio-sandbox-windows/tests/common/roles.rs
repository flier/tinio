//! Child and launcher roles a test binary re-enters.

use std::{
    env, fs, io,
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process,
    time::Duration,
};

use tinio_sandbox_core::{AccessMode, Confinement, supervise};
use tinio_sandbox_windows::{Options, launch};

use super::launch::launch_confined_at;

/// Append one line to the report the parent reads.
pub fn append_to_report(report: &Path, line: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(report)
        .expect("open the report");
    writeln!(file, "{line}").expect("write the report");
}

/// The confined child's role, shared by every test binary that launches one.
///
/// Inert when there is no payload: that is design decision 17's discriminator,
/// exercised by the tests exactly as production uses it, and it is why the
/// launcher-side test functions can share a binary with this one. The early
/// return is the discriminator rather than a skipped test — a process with no
/// payload is the launcher, and the role it plays there is no role at all.
pub fn child_role() {
    let Some(adopted) =
        launch::adopt(env::args_os(), io::stdin().lock()).expect("adopt the payload")
    else {
        return;
    };
    // Core's rule hands back a descriptor; rebuilding it is this crate's half.
    let listener = launch::listener(&adopted).expect("the harness bound a listener");
    child_role_body(adopted.config(), listener);
}

/// The report directory a probe file's `report=` entry names.
///
/// The probe carries the **report file**, because that is what the ACL probes
/// append to; the files the signal suite's roles agree on are its siblings, so
/// the directory is the part both ends need.
pub fn probe_dir(probe: &str) -> PathBuf {
    probe
        .lines()
        .find_map(|line| line.strip_prefix("report="))
        .map(PathBuf::from)
        .and_then(|report| report.parent().map(Path::to_path_buf))
        .expect("the probe carries a report path")
}

pub fn launcher_role(publish_address: bool) {
    if env::var_os("TINIO_SANDBOX_TEST_LAUNCHER").is_none() {
        return;
    }
    let tree = PathBuf::from(env::var("TINIO_SANDBOX_TEST_TREE").expect("the tree path"));
    let (confinement, address) =
        launch_confined_at(&tree, AccessMode::ReadWrite, Options::default(), None);
    if publish_address {
        fs::write(tree.join("address"), address.to_string()).expect("publish the address");
    }
    let Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    // The relay is armed inside `Backend::confine` by the time this returns, and
    // the child cannot have been reached before then: it reads the payload, which
    // is written after the arming (design §3 steps 7 and 9).
    let status = supervise(&mut child, || ()).expect("supervise the confined child");
    // The status is the child's, forwarded the way `serve` forwards it: this
    // process exists to be a launcher, and its exit code is what the test reads.
    process::exit(status.code().unwrap_or(1));
}

/// The role's body, so the variant with a stop hook can share it
/// ([`child_role_with_stop_hook`], which arms the hook and beats before calling
/// this).
///
/// The first parameter is the payload's configuration **text** — `Adopted::config`
/// is a `String` in P0's surface, and this harness is the one reader that treats
/// it as a path to the probe file it wrote.
pub fn child_role_body(config: &str, listener: TcpListener) {
    let probe_path = config.to_owned();
    let probe = fs::read_to_string(&probe_path).expect("read the probe file");
    let value = |key: &str| {
        probe
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")))
            .expect("a probe key")
            .to_string()
    };
    let root = PathBuf::from(value("root"));
    let denied = PathBuf::from(value("denied"));
    let report = PathBuf::from(value("report"));

    let mut log = String::new();
    match fs::read_to_string(root.join("granted.txt")) {
        Ok(_) => log.push_str("granted-read: OK\n"),
        Err(err) => log.push_str(&format!("granted-read: refused ({})\n", err.kind())),
    }
    match fs::read_to_string(denied.join("secret.txt")) {
        Ok(_) => log.push_str("denied-read: READ\n"),
        Err(err) => log.push_str(&format!("denied-read: refused ({})\n", err.kind())),
    }
    match fs::write(denied.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("denied-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("denied-write: refused ({})\n", err.kind())),
    }
    match fs::write(root.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("root-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("root-write: refused ({})\n", err.kind())),
    }
    append_to_report(&report, log.trim_end());

    // The outbound attempt, only when a test asked for one: a test that only
    // checks "connect failed" proves nothing, so the parent proves the target
    // reachable unconfined first (design §12).
    if let Some(target) = probe
        .lines()
        .find_map(|line| line.strip_prefix("target="))
        .and_then(|raw| raw.parse::<SocketAddr>().ok())
    {
        let outcome = match TcpStream::connect_timeout(&target, Duration::from_secs(5)) {
            Ok(_) => "outbound: SUCCEEDED".to_string(),
            Err(err) => format!("outbound: refused ({})", err.kind()),
        };
        append_to_report(&report, &outcome);
    }

    // The listener this process inherited: answer one connection, which is what
    // the parent is waiting for.
    let (mut stream, _) = listener.accept().expect("accept");
    stream.write_all(b"pong").expect("answer");
    stream.flush().expect("flush the answer");
}
