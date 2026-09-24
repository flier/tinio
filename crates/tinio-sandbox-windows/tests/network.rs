//! The network guarantees, measured rather than assumed.
//!
//! Five questions are asked here and none of them is answered by looking at the
//! code that implements it. Three are design §7's: that an AppContainer with no
//! network capability cannot initiate an outbound connection, that the *same*
//! container reaches one when `internetClient` is granted to it, and that a
//! capability other than `internetClient` does not open the internet. The fourth
//! is whether a granted capability is live in the container at all —
//! [`a_granted_capability_is_enabled_in_the_container`], which asks it through an
//! ACL rather than a socket and is described below. The fifth is the question
//! decision 16 leaves explicitly open — whether a listener the parent bound and
//! handed in reaches the host's LAN interface.
//!
//! # Why the positive half is not decoration
//!
//! **A test that only checks "connect failed" proves nothing.** A machine with no
//! route to the target fails it too, which is exactly the state the design's §7
//! gap was opened in: the authoring host refused `1.1.1.1:53` identically with
//! *and* without `internetClient` and could not say whether that was container
//! isolation or its own firewall. Two measurements close that gap and neither is
//! sufficient alone:
//!
//! 1. **The baseline**, taken outside the confinement: an ordinary process on
//!    this host opens a TCP connection to the target. This is a property of the
//!    *machine* — route, firewall, upstream policy — and it is decided before
//!    anything is spawned ([`host_ready`]).
//! 2. **The control**, taken inside it with `internetClient` granted: the same
//!    confined child reaches the same address.
//!
//! With both in hand, the refusal in
//! [`an_address_that_works_outside_is_refused_inside`] is the container's policy
//! rather than a missing route. **The control asserts rather than records**, and
//! it earned that the hard way: an earlier revision printed what the container
//! reported under `internetClient` and passed either way, and on 2026-09-22 that
//! shape would have hidden a real defect — the capability SIDs the crate handed
//! `CreateProcessW` carried no `SE_GROUP_ENABLED`, so a container granted
//! `internetClient` behaved exactly like one granted nothing. The failing
//! assertion is what found it (design §7's Windows row and the evidence table
//! carry the measurement, and `sid::CapabilitySids::resolve` carries the fix).
//!
//! # The one observable that does not need the network
//!
//! [`a_granted_capability_is_enabled_in_the_container`] reads the capability out
//! of the container through an ACL instead of a socket, and it exists for two
//! reasons. It is the measurement Task 9's report said could separate "the
//! container's list was read from `Options`" from "it was silently emptied" and
//! could not be made — and it is the only leg here that still means something on
//! a host whose own policy refuses the container every non-loopback destination,
//! where every socket-based reading is the same "refused" whatever the
//! capabilities are.
//!
//! # Nothing here skips in silence
//!
//! [`host_ready`] asks the one question of the machine before the first spawn,
//! and a host that answers no **fails with its reason named** unless the operator
//! has deliberately set
//! [`ALLOW_SKIP`](tinio_sandbox_windows::testing::ALLOW_SKIP). That is Task 11's
//! shape, and it is deliberate: the whole point of this suite is that a green run
//! means the measurement was taken.

#![cfg(windows)]

mod common;

use std::{
    env,
    io::Read as _,
    net::{IpAddr, SocketAddr, TcpStream, UdpSocket},
    path::Path,
    process::Command,
    time::Duration,
};

use tinio_sandbox_core::{AccessMode, Confinement, supervise};
use tinio_sandbox_windows::{Capability, Options, acl, testing};

/// The capability SID `Capability::InternetClient` resolves to.
///
/// Spelled here rather than read from the enum so the ACL this suite writes is
/// an independent statement of which SID it expects the container to carry: a
/// test that asked `Capability::sid()` for the value to put in the `icacls`
/// argument would agree with a wrong constant.
const INTERNET_CLIENT_SID: &str = "S-1-15-3-1";

/// The address the baseline and both confined attempts use.
///
/// `1.1.1.1:53` is the default, and it was chosen for what a refusal would mean:
/// it is a public resolver, so a successful TCP handshake is a real round trip
/// to the internet rather than an artefact of a closed port on a reachable host.
/// `TINIO_SANDBOX_TEST_TARGET` points the suite at whatever address a given host
/// actually reaches.
fn target() -> SocketAddr {
    env::var("TINIO_SANDBOX_TEST_TARGET")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| "1.1.1.1:53".parse().expect("a literal"))
}

/// Whether an **unconfined** process on this host reaches `address`.
///
/// The baseline, and the only thing this suite knows about the machine rather
/// than about the container. Five seconds is the confined child's own budget for
/// the same attempt (the harness's `child_role_body`), so the two halves are
/// comparable rather than one being more patient than the other.
fn baseline(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, Duration::from_secs(5)).is_ok()
}

/// **The host's precondition, probed before anything is spawned — and the only
/// place this suite's opt-out is decided.** Returns `false` when this machine
/// cannot do what the measurement needs *and* the operator has deliberately said
/// so; the test then returns without asserting anything about the product.
///
/// One capability of the **machine** is asked about: whether an ordinary process
/// here can reach [`target`] at all. A host that cannot reach it unconfined
/// cannot be told apart from a container that refused it, so every leg of this
/// suite would assert nothing — which is why this is a precondition and not a
/// printed note.
///
/// **The control is not reachable from here.** Whether `internetClient` makes
/// the container reach the target is not a property of this machine; it is the
/// finding, and it lives in the test below where it can fail.
///
/// **The assert-before-print order, the variable's name and the remedy in the
/// message are [`testing::host_cannot`]'s**, so this suite, `tinio-server`'s
/// Windows harness and P1's jail test cannot drift apart on any of the three.
fn host_ready(target: SocketAddr) -> bool {
    if baseline(target) {
        return true;
    }
    let reason = format!("an unconfined process on this host cannot reach {target}");
    testing::host_cannot(
        "the network suite",
        &reason,
        ", or set TINIO_SANDBOX_TEST_TARGET to an address this host reaches (design §7)",
    )
}

/// What the confined child reports about reaching out.
///
/// The target travels in the probe file the child already reads, so nothing about
/// this run goes through the environment — the rule every confined role in this
/// crate follows (design decision 17), and the reason `Tree::target` exists.
///
/// **A launch that fails panics inside the harness with the backend's own
/// error**, which is Task 9's rule: a test whose subject never started has
/// nothing to report, and a value it could return `None` on would make "the
/// backend refused to confine anything" look green.
fn confined_outbound(volume: &Path, options: Options, target: SocketAddr) -> String {
    let (_dir, mut tree) = common::tree(volume);
    tree.target = Some(target);
    // The harness's run, and it is the one whose body this function used to
    // spell: the child probes and reports before it accepts, so the harness's
    // `answer` both waits for it and proves inbound still works — with the
    // connect and the read bounded by `common::TIMEOUT`, which a block spelling
    // the same four lines without the timeout is the hazard Task 6 paid for. Its
    // `supervise` joins the relay and reaps the child, so a caller's assertion
    // that fails still leaves no process behind.
    common::run_confined_with(&tree, AccessMode::ReadWrite, options).report
}

/// The confined child's own role. The body lives in the harness because the
/// confinement suite launches the same role from a different test binary.
#[test]
fn child_role() {
    common::child_role();
}

/// The outbound guarantee: an address this host reaches is refused inside.
///
/// **The baseline passed or this test did not run**, so the refusal read out of
/// the child's own report is the container's policy rather than a missing route,
/// and the loopback round trip `common::answer` performed proves the confinement
/// left inbound alone — the other half of §7's rule, and the reason a refusal
/// cannot be explained by a child that never started.
///
/// **A failure here is a design finding, not a broken test.** An AppContainer
/// with an empty capability set that reaches an address this host reaches
/// unconfined falsifies §7's Windows row, and the row is what would change.
#[test]
fn an_address_that_works_outside_is_refused_inside() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = common::one_volume();
    let report = confined_outbound(&volume, Options::default(), target);
    assert!(
        report.contains("outbound: refused"),
        "the container reached {target} with no capability, which §7's Windows row says it \
         cannot: {report}"
    );
}

/// The positive control: the same address, with `internetClient` granted.
///
/// **This asserts, and it is the assertion that found the capability defect.** It
/// is the only measurement here that tells "the container's capability list was
/// read from `Options` and honoured" from "it was silently emptied" — the two are
/// indistinguishable from every other observable in the suite, because a
/// container with no capabilities at all behaves exactly like one whose
/// configured list was dropped. The pair of tests either side of it is what makes
/// each of them mean something: same host, same address, same child, one
/// capability between them.
///
/// **A failure here is a finding, never a flake.** It says the container did not
/// reach an address this host reaches unconfined with `internetClient` granted.
/// On 2026-09-22 it failed for exactly that reason and the cause was in this
/// crate rather than on the host: the capability SIDs were handed to
/// `CreateProcessW` with no attribute set, so the capability sat in the child's
/// token and was honoured by nothing. If it fails again, the two candidates are
/// that regression and a host whose own policy refuses the container's traffic —
/// and [`a_granted_capability_is_enabled_in_the_container`] is what tells the two
/// apart, because it does not touch the network.
#[test]
fn the_same_address_is_reached_with_internet_client() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = common::one_volume();
    let options = Options {
        capabilities: vec![Capability::InternetClient],
        ..Default::default()
    };
    let report = confined_outbound(&volume, options, target);
    eprintln!(
        "measured: with internetClient the confined child reported `{}`",
        report.lines().last().unwrap_or_default()
    );
    assert!(
        report.contains("outbound: SUCCEEDED"),
        "the container did not reach {target} with internetClient granted, while this host \
         reaches it unconfined: {report}"
    );
}

/// A capability that is not `internetClient` does not open the internet.
///
/// The third arm, and the one that makes the control's reading a *policy* rather
/// than a coincidence: `privateNetworkClientServer` is the capability §7 says a
/// non-loopback bind needs, so it is a non-empty list that must not reach a
/// public resolver. Together with the test above it separates three outcomes a
/// single capability would confound: a list that is read and honoured per
/// capability, a list that is emptied, and a container that opens the network
/// whenever any capability is present.
#[test]
fn a_capability_that_is_not_internet_does_not_open_the_network() {
    let target = target();
    if !host_ready(target) {
        return;
    }
    let volume = common::one_volume();
    let options = Options {
        capabilities: vec![Capability::PrivateNetworkClientServer],
        ..Default::default()
    };
    let report = confined_outbound(&volume, options, target);
    assert!(
        report.contains("outbound: refused"),
        "the container reached {target} with privateNetworkClientServer and no internetClient, \
         so a non-empty capability list is what granted it rather than the name of the \
         capability: {report}"
    );
}

/// The same question asked of an ACL instead of a socket.
///
/// **This is Task 9's carry, and the reason it is asked twice.** Its report
/// records that `a_capability_from_the_options_reaches_the_container` cannot
/// distinguish "the container's list was read from `Options`" from "it was
/// silently emptied", that no observable in its own suite could, and that the
/// network probe is the only measurement that can. That is true of the *network*
/// arm above — and a network arm is exactly what a locked-down host takes away,
/// since a host that refuses the container every destination reports the same
/// refusal whatever its capabilities are.
///
/// A capability SID is also a token SID an access check consults, so granting one
/// read on a directory and asking the child to read through it is the same
/// question with no network in the way: the child reads the file if and only if
/// the SID is in its token **and enabled**. Measured 2026-09-22, the second half
/// of that is not free — with `Attributes: 0` the ACE granted nothing, and this
/// leg is what makes a regression there fail on every host rather than only on
/// the ones whose outbound happens to work.
#[test]
fn a_granted_capability_is_enabled_in_the_container() {
    let volume = common::one_volume();
    let granted = capability_is_visible(&volume, vec![Capability::InternetClient]);
    let absent = capability_is_visible(&volume, Vec::new());
    eprintln!(
        "measured: the {INTERNET_CLIENT_SID} ACE was readable with internetClient={granted}, \
         with no capability={absent}"
    );
    assert!(
        granted,
        "a directory whose ACL grants {INTERNET_CLIENT_SID} read was not readable by a container \
         the operator granted internetClient, so either the list did not reach the container's \
         token or it reached it disabled (a capability whose entry is not SE_GROUP_ENABLED is \
         honoured by nothing)"
    );
    assert!(
        !absent,
        "a directory whose ACL grants {INTERNET_CLIENT_SID} read was readable by a container with \
         no capability at all, so this ACE says nothing about the list"
    );
}

/// Whether the confined child reads the `denied` file when `denied` carries an
/// ACE for the `internetClient` capability SID.
///
/// The harness's own tree is reused rather than a new one added: `denied` is the
/// directory the child already tries to read and reports on, it already carries
/// `Users` (so a pass cannot be an inherited ACE nobody meant), and the grant is
/// the suite's own `icacls` call — the launcher never writes a capability ACE,
/// which is the same rule that keeps the confined child out of `icacls` (design
/// decisions 15 and 17).
fn capability_is_visible(volume: &Path, capabilities: Vec<Capability>) -> bool {
    let (_dir, tree) = common::tree(volume);
    let granted = Command::new(acl::icacls_binary())
        .arg(&tree.denied)
        .args(["/grant", &format!("*{INTERNET_CLIENT_SID}:(OI)(CI)R"), "/q"])
        .output()
        .expect("grant the capability SID");
    assert!(
        granted.status.success(),
        "icacls could not grant {INTERNET_CLIENT_SID} on {}: {}",
        tree.denied.display(),
        String::from_utf8_lossy(&granted.stderr).trim()
    );
    common::run_confined_with(
        &tree,
        AccessMode::ReadWrite,
        Options {
            capabilities,
            ..Default::default()
        },
    )
    .report
    .contains("denied-read: READ")
}

/// The address this host would use to reach the network, when nothing says
/// otherwise.
///
/// **Discovered rather than configured, so the LAN leg below cannot pass by
/// doing nothing.** A `UdpSocket` connected to the suite's own target performs a
/// route lookup and nothing else — no packet is sent, no name is resolved — and
/// the address it reports is this host's own source address on the interface that
/// route selects. On a host with no route there is no answer, and the leg reports
/// that rather than a pass.
///
/// `TINIO_SANDBOX_TEST_LAN_ADDR` overrides the **interface only**; the port comes
/// from the listener the suite binds, so two runs cannot collide on a fixed one.
fn lan_ip() -> Option<IpAddr> {
    if let Ok(raw) = env::var("TINIO_SANDBOX_TEST_LAN_ADDR") {
        if let Ok(address) = raw.parse::<SocketAddr>() {
            return Some(address.ip());
        }
        if let Ok(ip) = raw.parse::<IpAddr>() {
            return Some(ip);
        }
    }
    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0))).ok()?;
    socket.connect(target()).ok()?;
    let local = socket.local_addr().ok()?;
    let ip = local.ip();
    (!ip.is_loopback() && !ip.is_unspecified()).then_some(ip)
}

/// The LAN question decision 16 leaves open, recorded and never asserted.
///
/// **What is measured is narrower than "the LAN", and saying so is the point.**
/// The connection below is made by this same host to this host's own interface
/// address; it travels the local stack rather than a cable. It establishes
/// whether the container's policy gates a connection addressed to a non-loopback
/// interface **at all** — and it established on 2026-09-22 that it does not: a
/// container with no capability at all answered on `192.168.1.10`. What it
/// cannot establish is whether a client on another machine gets through, because
/// that is a different packet arriving on the same interface from off the host,
/// and a second machine is what would settle it. Nothing in the design depends on
/// the answer (decision 16, §7's Windows note), which is why this leg records
/// rather than asserts.
///
/// **It is run for real rather than behind an unset variable.** A leg that
/// returned early when nobody configured it would report nothing at all, which is
/// the shape this suite exists to avoid; the interface is discovered when nothing
/// overrides it, and a host with no route fails with that reason named.
///
/// The two arms differ only in `privateNetworkClientServer`, which §7 says a
/// non-loopback bind needs: if the answer is ever "refused without it, answered
/// with it", that sentence is confirmed rather than asserted.
#[test]
fn the_lan_answer_is_recorded() {
    let Some(ip) = lan_ip() else {
        let reason = "this host has no non-loopback route to the suite's target, so there is no \
                      LAN interface to measure against";
        testing::host_cannot(
            "the LAN leg",
            reason,
            ", or set TINIO_SANDBOX_TEST_LAN_ADDR=<ip> to name the interface",
        );
        return;
    };
    let volume = common::one_volume();
    for (label, capabilities) in [
        ("no capability", Vec::new()),
        (
            "privateNetworkClientServer",
            vec![Capability::PrivateNetworkClientServer],
        ),
    ] {
        let (_dir, tree) = common::tree(&volume);
        // The port is the wildcard bind's, so the address that reaches it is this
        // host's own interface on the same port.
        let bind = SocketAddr::from(([0, 0, 0, 0], 0));
        let (confinement, bound) = common::launch_confined(
            &tree,
            AccessMode::ReadWrite,
            Options {
                capabilities,
                ..Default::default()
            },
            Some(bind),
        );
        let Confinement::Supervise(child) = confinement else {
            panic!("the Windows backend must launch a child");
        };
        let lan = SocketAddr::new(ip, bound.port());
        let reached = match TcpStream::connect_timeout(&lan, Duration::from_secs(5)) {
            Ok(mut stream) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("bound the read");
                let mut pong = [0u8; 4];
                Some(stream.read_exact(&mut pong).is_ok() && &pong == b"pong")
            }
            Err(_) => None,
        };
        eprintln!(
            "recorded: [{label}] the container bound {bound}; a same-host connection to {lan} {}",
            match reached {
                Some(true) => "was answered by the confined child",
                Some(false) => "connected but the confined child said something else",
                None => "was refused, so the container did not answer on that interface",
            }
        );
        // **Every spawn is waited for or killed on every path** (Task 11's rule).
        // A connection that was answered was also the child's `accept`, so it is
        // on its way out and can be waited for; one that was not leaves it blocked
        // in `accept` for a caller that is never coming, and dropping the
        // supervision is what ends it — the `Child`'s closure drops with it,
        // closing the job handle and taking the child with it (decision 20), and
        // the `Grants` guard rolls the ACEs back on the same path (Task 9's
        // `a_dropped_supervision_also_rolls_back`).
        if reached == Some(true) {
            let mut child = child;
            supervise(&mut child, || ()).expect("supervise the confined child");
        } else {
            drop(child);
        }
    }
}
