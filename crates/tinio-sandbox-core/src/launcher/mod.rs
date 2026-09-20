//! The launcher contract shared by the child-launching platforms.

pub mod payload;
pub mod relay;
pub mod supervise;

use std::{ffi::OsString, io::Read, net::TcpListener};

use tracing::debug;

// `Adopted`, `adopt` and `LaunchPlan` are declared below in this module rather
// than in a submodule, so `lib.rs` re-exporting them as `launcher::{Adopted,
// adopt, LaunchPlan}` resolves. A `pub use self::adopt::…` would name a module
// that does not exist.
pub use self::{
    payload::{
        LISTENER_FD_PREFIX, LaunchPayload, is_listener_fd_arg, listener_fd_arg,
        listener_fd_from_arg,
    },
    relay::{Observer, Terminal},
    supervise::{Child, supervise},
};
use crate::error::Result;

/// Everything a launcher needs that is not an exposed path.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::LaunchPlan;
///
/// let plan = LaunchPlan::default();
/// assert!(plan.payload().is_none());
/// assert!(plan.listener().is_none());
/// ```
#[derive(Debug, Default)]
pub struct LaunchPlan {
    payload: Option<LaunchPayload>,
    listener: Option<TcpListener>,
}

impl LaunchPlan {
    /// An empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carry a payload.
    pub fn with_payload(mut self, payload: LaunchPayload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// The payload, when one was set.
    pub fn payload(&self) -> Option<&LaunchPayload> {
        self.payload.as_ref()
    }

    /// **Own** the bound listener the child will serve on (design decision 16).
    ///
    /// Owned rather than named by descriptor: the parent must keep the socket
    /// alive for the child's whole lifetime, and a plan that carried a bare
    /// number while the socket lived somewhere else would let the two drift
    /// apart with nothing to notice it. The platform derives the descriptor
    /// when it builds the child's argv.
    ///
    /// The listener lives **here and not in the payload** (design §1: the plan
    /// holds what the launcher needs that is not a path) — carrying it in both
    /// would be two sources of truth for one fact.
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The owned listener, when the parent bound one.
    pub fn listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// The configuration text the launcher forwards **unparsed** (design
    /// decision 18). Empty when no payload was set.
    pub fn config(&self) -> &str {
        self.payload
            .as_ref()
            .map_or("", |payload| payload.config.as_str())
    }

    /// Build the plan a parent uses before launching a child.
    ///
    /// No listener parameter: [`Self::with_listener`] carries it, so a caller
    /// cannot set the same fact in two places.
    ///
    /// Infallible because the payload it builds is one string. The earlier shape
    /// had to generate a nonce here and was `Result` for it; with the nonce gone
    /// (design decision 17) there is nothing left to fail.
    pub fn for_launch(config: String) -> Self {
        Self::default().with_payload(LaunchPayload { config })
    }
}

/// What a confined child recovers from its own launch (design decision 17).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Adopted;
///
/// let adopted = Adopted {
///     config: "port = 9000".into(),
///     listener_fd: Some(7),
/// };
/// assert_eq!(adopted.listener_fd, Some(7));
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
    pub config: String,
    /// The inherited listener's descriptor, which only the platform can turn
    /// back into a socket.
    pub listener_fd: Option<i64>,
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
    let Some(listener_fd) = args.into_iter().find_map(|arg| listener_fd_from_arg(&arg)) else {
        debug!(step = "adopt", listener = false, payload = "absent");
        return Ok(None);
    };
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
            Ok(Some(Adopted {
                config: payload.config,
                listener_fd: Some(listener_fd),
            }))
        }
        Err(_) => {
            debug!(step = "adopt", listener = true, payload = "rejected");
            Ok(None)
        }
    }
}

impl Adopted {
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
    /// let adopted = Adopted {
    ///     config: "port = 9000".into(),
    ///     listener_fd: None,
    /// };
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
    /// let adopted = Adopted {
    ///     config: String::new(),
    ///     listener_fd: Some(7),
    /// };
    /// assert_eq!(adopted.listener(), Some(7));
    /// ```
    pub fn listener(&self) -> Option<i64> {
        self.listener_fd
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

    /// A reader that fails the test if anything reads from it.
    ///
    /// An EOF-returning reader cannot tell the two behaviours apart: `io::empty()`
    /// and a `Cursor` over an empty buffer behave identically whether or not they
    /// are read, so a blind read-to-end would pass a test built on one. This is
    /// hostile to being read at all, which is the property under test.
    struct Untouchable;

    impl io::Read for Untouchable {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("`adopt` read stdin without a listener element in argv");
        }
    }

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
