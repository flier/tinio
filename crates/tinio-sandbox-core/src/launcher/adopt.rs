//! The child's half of the handoff: `adopt`, `filter_argv` and what they recover.
//!
//! **A process decides it is a child from its argv, and recovers its launch from stdin.** A
//! launcher writes the descriptor elements (the two prefixes in [`payload`](super::payload)) and
//! the payload itself; this module drops the elements the argument parser must never see and
//! decodes the frame into [`Adopted`] — or answers `None`, which is both "this process is the
//! launcher" and "the frame did not decode".
//!
//! **The mount point of the fail-closed rules, all of them stated on the items below**: stdin is
//! read only when the listener element is present, and every path that cannot prove this process is
//! a launched child returns `None` rather than an error a caller could mistake for a verdict.

use std::{ffi::OsString, io::Read};

use tracing::debug;

use crate::{
    error::Result,
    launcher::payload::{
        LaunchPayload, SandboxSpec, control_fd_from_arg, is_launcher_fd_arg, listener_fd_from_arg,
    },
};

/// What a confined child recovers from its own launch (design decision 17).
///
/// **Construction goes through [`Adopted::new`] or [`adopt`], and reads go through the
/// accessors.** Every field is crate-private: a `pub` field beside an accessor that returns the
/// same value is one fact with two public spellings, and the pair drifts the moment either grows
/// a rule of its own. `listener_fd` used to be the exception — a comment here argued that the
/// Windows platform's `launch::listener` read the field directly — and it was not one: that
/// reader and macOS's both call [`Self::listener`] now, and the CLI always did, so no caller
/// needed a spelling the accessor could not serve.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Adopted;
///
/// let adopted = Adopted::new("port = 9000", Some(7), vec![9], None, None);
/// assert_eq!(adopted.listener(), Some(7));
/// assert_eq!(adopted.controls(), [9]);
/// assert_eq!(adopted.token(), None);
/// assert!(adopted.confinement().is_none());
/// ```
#[derive(Clone, PartialEq, Eq, derive_more::Debug)]
pub struct Adopted {
    /// The configuration text the parent forwarded, unparsed.
    ///
    /// `#[debug(skip)]` for the same reason [`LaunchPayload::config`] carries
    /// it: this text can hold credentials, and a `Debug` that prints them leaks
    /// them into the first log line, `dbg!` or `unwrap_err()` that happens to
    /// touch a value containing this one. A caller that needs to see the
    /// configuration prints it deliberately.
    #[debug(skip)]
    pub(crate) config: String,
    /// The inherited listener's descriptor, which only the platform can turn
    /// back into a socket. Read through [`Adopted::listener`] — the field is
    /// crate-private for the reason the struct's own doc gives.
    pub(crate) listener_fd: Option<i64>,
    /// The inherited control channel's handles, when the parent created the channel — a
    /// launcher platform, either because a container's process cannot create the pipes its
    /// current-user-only DACL needs (Windows) or because the confinement's profile grants no
    /// `bind()` for a socket (macOS).
    ///
    /// Spelled exactly like [`Adopted::listener_fd`], and for the same reason: only
    /// the platform can turn the values back into the objects they name, so the core
    /// carries numbers and the conversion stays in the platform crate.
    ///
    /// **One entry per instance the launcher bound**, in that order: the channel is a pool, and
    /// the child serves on every instance it was handed so that one of them serving a request does
    /// not leave the name deaf.
    pub(crate) control_fds: Vec<i64>,
    /// The management plane's token, when the launcher sent one: the value the
    /// child's plane verifies `/status` and `/stop` against, and the value the
    /// launcher already published in `state`.
    ///
    /// **The child adopts it rather than generating one**, which is the whole
    /// point of the field: the token a client reads out of `state` is the
    /// launcher's, so a plane that minted its own would refuse every client that
    /// found it — `/status` and `/stop` would answer `401` for a server that is
    /// running and discoverable. `None` is every launch that carried none: the
    /// unconfined runs, the in-place platforms, and the confined shapes where the
    /// launcher has no `state` file to own (it then generates its own token and
    /// publishes as it always did).
    ///
    /// `#[debug(skip)]`: a bearer credential earns the same treatment in `Debug`
    /// as the configuration text above.
    #[debug(skip)]
    pub(crate) token: Option<String>,
    /// What this process must do to confine **itself**, when its launcher's
    /// mechanism hands it that job rather than wrapping its `exec`.
    ///
    /// **The child never reads the mechanism's environment variable.** The
    /// launcher resolves the mechanism, renders the profile and states both here,
    /// so the confinement a child applies is the one its launcher chose even if
    /// the environment changed in between — and `None`, the ordinary case, is
    /// every launch whose confinement was already applied to it (`sandbox-exec`
    /// and Windows) or that was never confined at all.
    ///
    /// **Printed by `Debug`**, unlike the two fields above: the profile names no
    /// path and holds no secret — the same bytes `sandbox-exec` carries in its
    /// own process list — and a refusal here is diagnosed from exactly this text.
    pub(crate) confinement: Option<SandboxSpec>,
}

/// Split launcher descriptor elements out of `argv` and [`adopt`] the payload.
///
/// **The child-side half of the handoff, beside [`adopt`].** A launcher writes
/// `tinio-sandbox-fd` and `tinio-sandbox-control-fd` into the child's argv; the
/// parser that follows must never see them. [`is_launcher_fd_arg`] is the
/// predicate both prefixes share, so the split and the adoption cannot drift
/// apart: an element this function keeps is one [`adopt`] would have discarded,
/// and an element it takes is one [`adopt`] reads.
///
/// **`stdin` is read only when a listener element is present** — [`adopt`]'s own
/// rule, unchanged. A process nothing launched still never touches its stdin.
///
/// # Examples
///
/// ```
/// use std::{ffi::OsString, io};
///
/// use tinio_sandbox_core::{filter_argv, listener_fd_arg};
///
/// let argv = vec![OsString::from("server"), listener_fd_arg(7)];
/// let (argv, adopted) = filter_argv(argv, io::empty()).unwrap();
/// assert_eq!(argv, vec![OsString::from("server")]);
/// // No payload on the empty reader: the element was present, the frame did not decode.
/// assert!(adopted.is_none());
/// ```
pub fn filter_argv(
    argv: impl IntoIterator<Item = OsString>,
    stdin: impl Read,
) -> Result<(Vec<OsString>, Option<Adopted>)> {
    let (launcher_args, filtered): (Vec<_>, Vec<_>) =
        argv.into_iter().partition(|arg| is_launcher_fd_arg(arg));
    let adopted = adopt(launcher_args, stdin)?;
    Ok((filtered, adopted))
}

/// Recover a launch, or decide this process is the launcher.
///
/// **Reads `stdin` only when `args` carries the listener element.** A process
/// started from a shell owns its stdin — a terminal, a heredoc, another tool's
/// pipe — so a blind read would block a launcher that nothing launched, waiting
/// for input that is never coming, and present as a hang rather than a refusal.
///
/// Returns `Ok(None)` for "this is the launcher": no element, or an element
/// whose payload does not decode. Both are ordinary outcomes, and both fail
/// closed if the process turns out to be inside a confinement after all.
///
/// Arguments are taken rather than read from the environment so the whole rule
/// is unit-testable without a subprocess.
///
/// Which of those outcomes the call reached is traced (`step = "adopt"`): that
/// an element was present and whether a payload decoded, never the payload's
/// contents, which carry credentials.
///
/// # Examples
///
/// ```
/// use std::{ffi::OsString, io};
///
/// use tinio_sandbox_core::adopt;
///
/// // No element: the launcher, and stdin is never touched.
/// let none = adopt(Vec::<OsString>::new(), io::empty()).unwrap();
/// assert!(none.is_none());
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    mut stdin: impl Read,
) -> Result<Option<Adopted>> {
    // Collected once, because **two** elements are read out of the same list: the
    // listener's decides whether this process is a child at all, and the control
    // channel's is optional. A second pass over the iterator is not available, and a
    // second `adopt` call would read stdin twice.
    let args: Vec<OsString> = args.into_iter().collect();
    let Some(listener_fd) = args.iter().find_map(|arg| listener_fd_from_arg(arg)) else {
        debug!(step = "adopt", listener = false, payload = "absent");
        return Ok(None);
    };
    // Only read when a listener element is present, so a launcher nothing launched
    // still never touches its own stdin. A control element alone is not a launch:
    // the listener element is what a launcher always writes (design §10 step 6).
    // The control elements are a **set**: the launcher writes one per instance, so this collects
    // every one rather than the first. The listener element above is still the only thing that
    // decides this process is a child — a control element on its own is a forged argv, and the
    // fail-safe that rule protects is `a_control_element_alone_is_not_a_launch`.
    let control_fds: Vec<i64> = args
        .iter()
        .filter_map(|arg| control_fd_from_arg(arg))
        .collect();
    let mut text = String::new();
    // The parent writes the payload and closes; a read to EOF is the whole
    // protocol, and a failure to read is a payload failure rather than an IO
    // error the caller could act on differently.
    if stdin.read_to_string(&mut text).is_err() {
        debug!(step = "adopt", listener = true, payload = "unreadable");
        return Ok(None);
    }
    // `serde_json::from_str` already rejects trailing bytes after the object,
    // which is the property that matters: a child must be certain the whole
    // stream was meant for it.
    match serde_json::from_str::<LaunchPayload>(&text) {
        Ok(payload) => {
            debug!(step = "adopt", listener = true, payload = "decoded");
            Ok(Some(Adopted::new(
                payload.config,
                Some(listener_fd),
                control_fds,
                payload.token,
                payload.confinement,
            )))
        }
        Err(_) => {
            debug!(step = "adopt", listener = true, payload = "rejected");
            Ok(None)
        }
    }
}

impl Adopted {
    /// Build the value a child holds — the configuration text, the inherited descriptors and
    /// the launcher's token — each as [`adopt`] would have read it.
    ///
    /// **The one public constructor.** [`adopt`] is the other way to one of these — the one a real
    /// child uses, decoding its launch payload — and it builds through this function, so a caller
    /// assembling the shape by hand (a test driving a role) produces exactly what the wire
    /// produces rather than a second spelling of it. Reads go through [`Self::config`],
    /// [`Self::listener`], [`Self::controls`], [`Self::token`] and [`Self::confinement`]: every
    /// field is private on purpose (the struct's own doc says why), with no exception left to
    /// name.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new("port = 9000", Some(7), Vec::new(), None, None);
    /// assert_eq!(adopted.config(), "port = 9000");
    /// assert_eq!(adopted.listener(), Some(7));
    /// assert!(adopted.controls().is_empty());
    /// ```
    pub fn new(
        config: impl Into<String>,
        listener_fd: Option<i64>,
        control_fds: Vec<i64>,
        token: Option<String>,
        confinement: Option<SandboxSpec>,
    ) -> Self {
        Self {
            config: config.into(),
            listener_fd,
            control_fds,
            token,
            confinement,
        }
    }

    /// The configuration text the parent forwarded, unparsed (design decision
    /// 18).
    ///
    /// Empty when the parent sent none, which is the ordinary case for a launch
    /// that took its configuration from somewhere else.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new("port = 9000", None, Vec::new(), None, None);
    /// assert_eq!(adopted.config(), "port = 9000");
    /// ```
    pub fn config(&self) -> &str {
        &self.config
    }

    /// The inherited listener's descriptor, when the parent handed one over.
    ///
    /// The descriptor is still the platform's to turn into a socket: only
    /// `from_raw_socket` or an `OwnedFd` conversion knows what it names, which
    /// is why the conversion stays in the platform crate.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
    /// assert_eq!(adopted.listener(), Some(7));
    /// ```
    pub fn listener(&self) -> Option<i64> {
        self.listener_fd
    }

    /// The inherited control channel's handles, when the parent created the channel.
    ///
    /// **What the child does with them is the whole point**: a non-empty answer means the
    /// launcher already created the control channel — its pipes exist with a DACL only the
    /// unrestricted launcher could attach, or its socket is bound where only the unconfined
    /// process may bind one — so the child adopts those objects rather than creating a second
    /// one. Empty is every other shape of launch: an in-place platform, an `api`-less build, and
    /// a run whose selection carries no local channel at all.
    ///
    /// One entry per instance, and the child serves on all of them: a Windows channel needs more
    /// than one so that a client can reach a listening instance while another serves, while a
    /// macOS channel is one socket and the platform refuses any other count. The single-handle
    /// spelling this replaced answered `Option<i64>`, which is the shape a pool cannot use.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), vec![9, 13], None, None);
    /// assert_eq!(adopted.controls(), [9, 13]);
    /// ```
    pub fn controls(&self) -> &[i64] {
        &self.control_fds
    }

    /// The management plane's token, when the launcher sent one.
    ///
    /// **`Some` is the launcher's token and not the child's to replace.** The value a client
    /// reads out of `state` is this one, so the plane the child builds is the plane that token
    /// guards — the `token` field's own doc carries the refusal a child that minted its own
    /// would produce. `None` is every launch whose management plane publishes its own `state`
    /// and therefore generates its own token.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), vec![9], Some("00".repeat(32)), None);
    /// assert_eq!(adopted.token().map(str::len), Some(64));
    /// ```
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// What this process must do to confine **itself**, when its launcher's
    /// mechanism handed it that job.
    ///
    /// **`Some` is a command, not a hint**: the child is the only process that can
    /// apply a profile to itself, and the process that applied it is one-shot and
    /// irreversible, so a child that met one of these and served anyway would be
    /// serving unconfined under a launcher that reported a confinement. `None` is
    /// every launch whose confinement was already applied — `sandbox-exec`
    /// wrapping the `exec`, or Windows' container at process creation — and every
    /// unconfined run.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
    /// assert!(adopted.confinement().is_none());
    /// ```
    pub fn confinement(&self) -> Option<&SandboxSpec> {
        self.confinement.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, ffi::OsString, fmt::Debug, io, mem, sync::Once};

    use tracing::{
        Event, Subscriber,
        field::{Field, Visit},
        subscriber::set_global_default,
    };
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt as _},
        registry,
    };

    use super::*;
    use crate::{
        _util::testing::Untouchable,
        launcher::payload::{control_fd_arg, listener_fd_arg},
    };

    /// The fields of one recorded event that the assertion below names.
    ///
    /// Captured as a field rather than as rendered text, which is the point: a
    /// test that matched on a formatted message would pass for a log line that
    /// merely mentioned the words.
    #[derive(Debug, Default)]
    struct Recorded {
        step: Option<String>,
        payload: Option<String>,
    }

    impl Visit for Recorded {
        fn record_str(&mut self, field: &Field, value: &str) {
            match field.name() {
                "step" => self.step = Some(value.to_owned()),
                "payload" => self.payload = Some(value.to_owned()),
                _ => {}
            }
        }

        fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
    }

    thread_local! {
        /// What [`Recorder`] captured **on this thread**.
        ///
        /// Per-thread, while the recorder itself is process-wide, and the two
        /// halves answer two different problems: the subscriber must be global for
        /// the callsites' cached interest to be computed against it (see
        /// [`install_recorder`]), and the buffer must be per-thread for an
        /// exact-equality assertion to read the events of *its* test rather than
        /// every event the binary emitted through the same callsites.
        static EVENTS: RefCell<Vec<Recorded>> = const { RefCell::new(Vec::new()) };
    }

    /// A subscriber that records what the traced operations emitted, so "it
    /// logs" is a property a test can lose.
    #[derive(Debug)]
    struct Recorder;

    impl<S: Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut recorded = Recorded::default();
            event.record(&mut recorded);
            if recorded.step.is_some() {
                EVENTS.with_borrow_mut(|events| events.push(recorded));
            }
        }
    }

    /// Install [`Recorder`] as the **process-wide** subscriber, once.
    ///
    /// **Global rather than scoped, and that is the fix rather than a preference.**
    /// `tracing` caches every callsite's *interest* in a process-wide registry, and
    /// that interest is recomputed from the dispatchers installed when the cache is
    /// rebuilt: with no subscriber installed anywhere, a rebuild caches `never`
    /// for the callsites `adopt` emits through, and an event emitted in that window
    /// is dropped instead of recorded. `with_default` installs a *thread-local*
    /// dispatcher, and it does not take part in another thread's rebuild — so a
    /// concurrently running test was enough to lose one of the three events this
    /// module counts, measured at 3 runs in 40. A global default takes part in
    /// every rebuild, so those callsites stay `sometimes` from here on, and
    /// `set_global_default` rebuilds once itself, which clears whatever was cached
    /// before it.
    ///
    /// The error is ignored rather than unwrapped: another owner of the process
    /// default is not this test's failure, and no test in this crate sets one
    /// today.
    fn install_recorder() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = set_global_default(registry().with(Recorder));
        });
    }

    /// Take everything [`Recorder`] captured on this thread.
    fn recorded() -> Vec<Recorded> {
        EVENTS.with_borrow_mut(mem::take)
    }

    // **The reader is [`Untouchable`]'s** (`tinio_util::testing`), which owns the
    // argument for why an EOF-returning stand-in cannot express this rule: what
    // this module asserts is that `adopt` never reads stdin, and only a reader
    // that fails on being read at all can tell that from a reader that returns
    // nothing.

    #[test]
    fn adopt_does_not_touch_stdin_without_the_listener_element() {
        // The fail-safe against a launcher blocking forever on an operator's
        // terminal: a process nothing launched owns its stdin, and reading it
        // would hang rather than refuse.
        assert!(
            adopt(Vec::<OsString>::new(), Untouchable)
                .unwrap()
                .is_none()
        );
        assert!(
            adopt([OsString::from("--port")], Untouchable)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn adopt_returns_the_configuration_text() {
        // The payload travels through `adopt` here rather than only through the
        // doc example, which never reaches it.
        let adopted = adopt(
            [listener_fd_arg(7)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#.to_vec()),
        )
        .unwrap()
        .expect("a listener element and a decodable payload");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(
            adopted.controls(),
            [] as [i64; 0],
            "a launch that named no control channel hands none back"
        );
        assert_eq!(
            adopted.token(),
            None,
            "a frame with no token field carries no token"
        );
    }

    #[test]
    fn adopt_hands_back_the_token_the_frame_carried() {
        // The third thing a payload can carry, and the one the child may not invent: the plane
        // this process builds verifies against the launcher's token, so an `adopt` that dropped
        // it would leave every `/status` and `/stop` on a discoverable server answering 401.
        let token = "00".repeat(32);
        let frame = format!(r#"{{"config":"port = 9000","token":"{token}"}}"#);
        let adopted = adopt([listener_fd_arg(7)], io::Cursor::new(frame))
            .unwrap()
            .expect("an element and a decodable payload");
        assert_eq!(adopted.token(), Some(token.as_str()));
    }

    #[test]
    fn adopt_hands_back_the_confinement_the_frame_carried() {
        // The fourth thing a payload can carry, and the one a child acts on rather than reads:
        // a mechanism that confines the child in-process puts the render and its values here,
        // and an `adopt` that dropped them would leave that child serving **unconfined** under a
        // launcher that reported a confinement — the failure decision 5's fail-closed rule is
        // about. The absent direction is the other half: every launch whose confinement was
        // already applied carries none, and `None` is what keeps `sandbox-exec`'s child and
        // Windows' reading the frame they were written against.
        let frame = r#"{"config":"port = 9000","confinement":{"mechanism":"sandbox-init","profile":"(deny default)","params":[["E0","/srv/data"]]}}"#;
        let adopted = adopt([listener_fd_arg(7)], io::Cursor::new(frame))
            .unwrap()
            .expect("an element and a decodable payload");
        let confinement = adopted.confinement().expect("the frame carried one");
        assert_eq!(confinement.mechanism, "sandbox-init");
        assert_eq!(confinement.profile, "(deny default)");
        assert_eq!(confinement.param("E0"), Some("/srv/data"));

        assert!(
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new(br#"{"config":"port = 9000"}"#)
            )
            .unwrap()
            .expect("the same frame without the field")
            .confinement()
            .is_none()
        );
    }

    #[test]
    fn adopt_reads_both_inherited_descriptors_out_of_one_argv() {
        // The two elements are read in the same pass, and neither stands in for the
        // other: a child of the Windows launcher inherits a listener and a control
        // pipe, so an `adopt` that answered one of them would leave the other's
        // handoff silently unconsumed.
        let adopted = adopt(
            [listener_fd_arg(7), control_fd_arg(9)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#),
        )
        .unwrap()
        .expect("both elements and a decodable payload");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(adopted.controls(), [9]);
    }

    #[test]
    fn filter_argv_drops_launcher_elements_and_adopts_the_payload() {
        // The child-side half of the handoff: clap must never see a descriptor element, and the
        // payload on stdin is what `adopt` already decodes — one call, so a consumer does not
        // re-partition argv itself.
        let argv = vec![
            OsString::from("server"),
            OsString::from("/srv/data"),
            listener_fd_arg(7),
            control_fd_arg(9),
            control_fd_arg(13),
        ];
        let (filtered, adopted) = filter_argv(
            argv,
            io::Cursor::new(br#"{"config":"port = 9000"}"#.to_vec()),
        )
        .unwrap();
        assert_eq!(
            filtered,
            vec![OsString::from("server"), OsString::from("/srv/data")],
            "clap must never see either element"
        );
        let adopted = adopted.expect("the payload decoded");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(adopted.controls(), [9, 13]);
    }

    #[test]
    fn filter_argv_leaves_stdin_alone_when_no_launcher_element_is_present() {
        let (filtered, adopted) = filter_argv(vec![OsString::from("server")], Untouchable).unwrap();
        assert_eq!(filtered, vec![OsString::from("server")]);
        assert!(adopted.is_none());
    }

    #[test]
    fn adopt_reads_every_control_element_the_launcher_wrote() {
        // The channel is a pool, so the launcher writes one element per instance and `adopt` must
        // hand back *all* of them: a child given one instance answers a client and then stops
        // listening, which is the defect the pool exists to close.
        let adopted = adopt(
            [listener_fd_arg(7), control_fd_arg(9), control_fd_arg(13)],
            io::Cursor::new(br#"{"config":"port = 9000"}"#),
        )
        .unwrap()
        .expect("three elements and a decodable payload");
        assert_eq!(adopted.listener(), Some(7));
        assert_eq!(
            adopted.controls(),
            [9, 13],
            "every instance the launcher bound, in its order"
        );
    }

    #[test]
    fn a_control_element_alone_is_not_a_launch() {
        // The listener element is what a launcher always writes, and it is what decides
        // whether this process is a child at all. A control element on its own is a
        // forged argv, and reading stdin for it would hang an operator's terminal —
        // the fail-safe `adopt_does_not_touch_stdin_without_the_listener_element`
        // states, restated for the element beside it.
        assert!(
            adopt([control_fd_arg(9)], Untouchable).unwrap().is_none(),
            "a control element without a listener element is not a launch"
        );
    }

    #[test]
    fn adopt_logs_its_outcome_and_why() {
        // `adopt` is the operation of the traced four that needs no child
        // process: whether a listener element was present and whether a payload
        // decoded is the whole decision it makes.
        install_recorder();
        // Whatever this thread recorded before now is not this test's business:
        // the harness may run more than one test on one thread.
        let _ = recorded();
        let (launcher, adopted, rejected) = (
            adopt(Vec::<OsString>::new(), io::empty()),
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new(r#"{"config":"port = 9000"}"#),
            ),
            adopt(
                [listener_fd_arg(7)],
                io::Cursor::new("not a payload at all"),
            ),
        );

        // The outcomes the events describe, so the test is about the events and
        // not about the returns.
        assert!(
            launcher.unwrap().is_none(),
            "no element: this process is the launcher"
        );
        assert_eq!(
            adopted
                .unwrap()
                .expect("an element and a decodable payload")
                .listener(),
            Some(7)
        );
        assert!(
            rejected.unwrap().is_none(),
            "an element whose payload does not decode"
        );

        // One event per call, each naming its operation — and each saying why,
        // which is the half a diagnosis needs.
        let events = recorded();
        let steps: Vec<_> = events.iter().map(|event| event.step.as_deref()).collect();
        assert_eq!(
            steps,
            [Some("adopt"), Some("adopt"), Some("adopt")],
            "{events:?}"
        );
        let payloads: Vec<_> = events
            .iter()
            .map(|event| event.payload.as_deref())
            .collect();
        assert_eq!(
            payloads,
            [Some("absent"), Some("decoded"), Some("rejected")],
            "{events:?}"
        );
    }
}
