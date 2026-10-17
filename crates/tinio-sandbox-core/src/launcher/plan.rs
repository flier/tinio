//! The **launch plan**: the facts a launcher owns before it spawns a child.
//!
//! **One type, because every fact here belongs to the launcher and is read together.** The
//! configuration text, the bound listener (design decision 16), the inherited control handles and
//! the management plane's token (design decision 17) are the launcher's own half of a launch —
//! `Backend::confine` receives exactly this value — while what the *child* recovers from the
//! handoff is `Adopted`, in `adopt.rs`, and the frame that carries the text is `payload.rs`.
//!
//! **The plan deliberately holds the configuration's *text* and not a payload**, because the
//! frame's other two fields are the plan's token and the platform's confinement: a plan carrying a
//! whole payload could carry a token no frame would ever write. The field docs below are where
//! that argument lives.

// The descriptor fixture the Unix plan leg opens: `cfg(unix)` because only the Unix
// `ControlHandle` is built from a real descriptor — Windows' is copied from a value the launcher's
// channel owns, so that leg spells a number instead — and `cfg(test)` because nothing else here
// reads a file.
#[cfg(all(test, unix))]
use std::fs;
use std::net::TcpListener;

use crate::launcher::control::ControlHandle;

/// Everything a launcher needs that is not an exposed path.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::LaunchPlan;
///
/// let plan = LaunchPlan::default();
/// assert_eq!(plan.config(), "");
/// assert!(plan.listener().is_none());
/// assert!(plan.controls().is_empty());
/// assert!(plan.token().is_none());
/// ```
#[derive(Default, derive_more::Debug)]
pub struct LaunchPlan {
    /// The configuration text the launcher forwards unparsed, when it was given
    /// one — **the text rather than a payload**, because the text is the only
    /// thing a launcher ever reads back: the frame's other two fields are the
    /// plan's (`token`) or the platform's (`confinement`), and a plan that held a
    /// whole payload could carry a token no frame would ever put on the wire.
    ///
    /// `#[debug(skip)]` for the reason [`LaunchPayload::config`] carries it: the
    /// text is credential-bearing, and a plan is the kind of value that reaches a
    /// log line through a `?` on a failing launch.
    #[debug(skip)]
    config: Option<String>,
    listener: Option<TcpListener>,
    /// The inherited control-channel handles, in the order the launcher bound them. A `Vec`
    /// rather than one value because the channel is a **pool**: one instance answers a client and
    /// then stops listening, so the child needs every instance the launcher created (see
    /// [`Self::with_controls`]).
    controls: Vec<ControlHandle>,
    /// The management plane's token, when the launcher generated one (see
    /// [`Self::with_token`]).
    ///
    /// `#[debug(skip)]` because it is a bearer credential: a plan is the kind of value that
    /// reaches a log line through a `?` on a failing launch, and the token guards `/status` and
    /// `/stop`.
    #[debug(skip)]
    token: Option<String>,
}

impl LaunchPlan {
    /// An empty plan.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// // Every fact a plan can carry is absent, which is the shape of the in-place platforms'
    /// // call: they confine this process and hand no child anything.
    /// let plan = LaunchPlan::new();
    /// assert_eq!(plan.config(), "");
    /// assert!(plan.listener().is_none());
    /// assert!(plan.controls().is_empty());
    /// assert!(plan.token().is_none());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Carry the management plane's **token** — the value the child verifies `/status` and
    /// `/stop` against, and the value the launcher publishes in `state` (design decision 17's
    /// private channel, extended to the one secret the two processes share).
    ///
    /// **The token lives on the plan and not in the configuration text**, because the
    /// platform is what serializes the frame: `spawn_supervised_child` builds it from
    /// `config()` and this value at the moment it writes the child's stdin, so the plan is
    /// the one place the token is set, and the frame is a spelling of it on the wire rather
    /// than a second source. It is private launcher→child data for the reason the payload is
    /// stdio: an argv element or an environment variable is readable by other processes, and
    /// this is the only credential either process holds.
    ///
    /// **A plan with no token is the ordinary plan.** Nothing generates one here: the launcher
    /// that will publish `state` is the only caller that has a reason to, and a plan built for
    /// any other launch carries none.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// let token = "00".repeat(32);
    /// let plan = LaunchPlan::for_launch("port = 9000".into()).with_token(token.clone());
    /// assert_eq!(plan.token(), Some(token.as_str()));
    /// ```
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    /// The token, when the launcher set one.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// let token = "00".repeat(32);
    /// let plan = LaunchPlan::for_launch("port = 9000".into()).with_token(token.clone());
    /// // The value the child verifies `/status` and `/stop` against, and the one the launcher
    /// // publishes in `state`: one value, read off the plan rather than carried twice.
    /// assert_eq!(plan.token(), Some(token.as_str()));
    /// assert_eq!(LaunchPlan::new().token(), None);
    /// ```
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::net::TcpListener;
    ///
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// // The launcher binds before any runtime exists, and the child inherits the socket
    /// // rather than the right to bind one (design decision 16).
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let plan = LaunchPlan::new().with_listener(listener);
    /// assert!(plan.listener().is_some());
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The owned listener, when the parent bound one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::net::TcpListener;
    ///
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// let plan = LaunchPlan::new().with_listener(TcpListener::bind("127.0.0.1:0")?);
    /// // The parent keeps the socket alive for the child's whole lifetime, so the plan owns
    /// // it rather than naming the descriptor it derived.
    /// assert!(plan.listener().is_some());
    /// assert!(LaunchPlan::new().listener().is_none());
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// Carry the inherited **control channel's** handles (design decision 16, extended to
    /// the control plane).
    ///
    /// Each handle is a [`ControlHandle`] rather than a bare number, and that type's own doc
    /// carries why: the platform crate has to reach the descriptor **after** the profile
    /// pre-flight and clear its close-on-exec flag there, so that the throwaway probe inherits
    /// **neither** descriptor — the clear is what comes *after* the probe, and one performed
    /// before it would hand the probe the launcher's listening sockets. The listener takes the
    /// same treatment from the other side: it is owned by the plan as a socket, and its clear
    /// happens at the same moment.
    ///
    /// The handles travel as the raw values the platform turns back into the channel's objects,
    /// exactly as [`Adopted::controls`](crate::launcher::Adopted::controls) spells them, and for
    /// the listener's reason in reverse:
    /// the listener socket is *owned* here because the parent must keep it alive, while the
    /// control channel is owned by the parent only long enough to hand it over — the child serves
    /// on the objects it inherited.
    ///
    /// **A set, because one platform's channel is a pool and the other's is not.** On Windows the
    /// channel is a pool of pipe instances, and a client only ever reaches one that is
    /// *listening*: the launcher creates them all (only it can — the pipe carries Windows' own
    /// default named-pipe descriptor rather than one naming the package SID, and
    /// the child's identity is the package SID), and the child serves on all of them so that one
    /// instance serving a request does not leave the name deaf — handing over fewer than the
    /// launcher bound is a channel that answers once and then stops. On macOS the channel is a
    /// single unix socket, and the reason it travels at all is the profile rather than an
    /// identity: the confined child's `bind()` is a network operation that profile grants nothing
    /// for, so the launcher binds the socket outside the sandbox and the child adopts the
    /// descriptor. One entry there, and the platform refuses any other count.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// // Two bound sockets to hand over — the fixture, not the subject: what this leg asserts is
    /// // the plan's own ordering and count, and each value is whatever the platform's duplicate
    /// // answered.
    /// let left = ControlHandle::duplicate(UnixListener::bind("/run/tinio/left.sock")?)?;
    /// let right = ControlHandle::duplicate(UnixListener::bind("/run/tinio/right.sock")?)?;
    /// let plan = LaunchPlan::default().with_controls([left, right]);
    /// assert_eq!(plan.controls().len(), 2);
    ///
    /// // The single-handle spelling is the same fact: a plan that carries one instance.
    /// assert_eq!(
    ///     LaunchPlan::default()
    ///         .with_control(ControlHandle::duplicate(UnixListener::bind(
    ///             "/run/tinio/one.sock"
    ///         )?)?)
    ///         .controls()
    ///         .len(),
    ///     1
    /// );
    /// # Ok(())
    /// # }
    /// # #[cfg(not(unix))]
    /// # fn main() {}
    /// ```
    pub fn with_controls(mut self, handles: impl IntoIterator<Item = ControlHandle>) -> Self {
        self.controls = handles.into_iter().collect();
        self
    }

    /// Carry **one** inherited control-channel handle — [`Self::with_controls`] with a single
    /// element, for the callers whose channel is one instance: macOS's unix socket, and a
    /// one-instance pipe.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// let handle = ControlHandle::duplicate(UnixListener::bind("/run/tinio/control.sock")?)?;
    /// let plan = LaunchPlan::default().with_control(handle);
    /// assert_eq!(plan.controls().len(), 1);
    /// # Ok(())
    /// # }
    /// # #[cfg(not(unix))]
    /// # fn main() {}
    /// ```
    pub fn with_control(self, handle: ControlHandle) -> Self {
        self.with_controls([handle])
    }

    /// The inherited control channel's handles, in the order the launcher bound them.
    ///
    /// # Examples
    ///
    /// **Every arm is spelled out, because neither `ControlHandle` constructor exists on both
    /// platforms** — `duplicate` is `#[cfg(unix)]`, `inherited` is `#[cfg(windows)]`, and the
    /// type itself is neither on a third target. An example that named one of them here was a
    /// doctest this file could not compile on the other launcher platform, which is invisible to
    /// a `#[cfg]`-aware check but not to `cargo test --doc` on that target.
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # fn main() -> std::io::Result<()> {
    /// use std::os::unix::net::UnixListener;
    ///
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// let handle = ControlHandle::duplicate(UnixListener::bind("/run/tinio/control.sock")?)?;
    /// let plan = LaunchPlan::new().with_control(handle);
    /// // A set rather than one value, because the channel is a pool: the child serves on
    /// // every instance the launcher bound.
    /// assert_eq!(plan.controls().len(), 1);
    /// assert!(LaunchPlan::new().controls().is_empty());
    /// # Ok(())
    /// # }
    /// # #[cfg(windows)]
    /// # fn main() {
    /// use tinio_sandbox_core::{ControlHandle, LaunchPlan};
    ///
    /// // The Windows form copies the handle value the launcher's channel owns.
    /// let plan = LaunchPlan::new().with_control(ControlHandle::inherited(1040));
    /// assert_eq!(plan.controls().len(), 1);
    /// assert!(LaunchPlan::new().controls().is_empty());
    /// # }
    /// # #[cfg(not(any(unix, windows)))]
    /// # fn main() {}
    /// ```
    pub fn controls(&self) -> &[ControlHandle] {
        &self.controls
    }

    /// The configuration text the launcher forwards **unparsed** (design
    /// decision 18). Empty when the plan carries none.
    ///
    /// **The text and not a payload, because the text is all a plan can honestly
    /// hold.** The frame's other two fields are not the plan's to carry: the token
    /// is [`Self::with_token`]'s, and the confinement is the platform's, so a plan
    /// that stored a whole [`LaunchPayload`](crate::launcher::LaunchPayload) accepted a token no
    /// launcher would ever write — `for_launch` set the other two fields to `None` and nothing
    /// read them.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// let plan = LaunchPlan::for_launch("port = 9000".into());
    /// assert_eq!(plan.config(), "port = 9000");
    /// assert_eq!(
    ///     LaunchPlan::new().config(),
    ///     "",
    ///     "a plan that was given no text forwards no configuration"
    /// );
    /// ```
    pub fn config(&self) -> &str {
        self.config.as_deref().unwrap_or("")
    }

    /// Whether the plan carries a configuration text at all.
    ///
    /// **Presence is not `config().is_empty()`, and the difference is a launch that
    /// really happens.** The text is the operator's document, forwarded unparsed
    /// (design decision 18), so an **empty one is a legitimate configuration** —
    /// the child parses it into every default and serves — while a plan that was
    /// never given one has nothing to forward. A caller that conflated the two
    /// would refuse a launch on the strength of its configuration being empty,
    /// which is not a refusal this crate has any reason to make.
    ///
    /// **It is the precondition a launcher asks before it serializes a frame**:
    /// the payload's only source is [`Self::for_launch`], so a plan built any other
    /// way carries nothing, and a launcher that wrote the frame anyway would hand
    /// its child an empty configuration instead of refusing. `Some("")` is `true`,
    /// which is what makes the predicate the honest half of the pair and
    /// [`Self::config`] the spelling of the text itself.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// assert!(LaunchPlan::for_launch("port = 9000".into()).has_config());
    /// assert!(
    ///     LaunchPlan::for_launch(String::new()).has_config(),
    ///     "an empty text is a configuration: the child parses it into defaults"
    /// );
    /// assert!(!LaunchPlan::new().has_config());
    /// ```
    pub fn has_config(&self) -> bool {
        self.config.is_some()
    }

    /// Build the plan a parent uses before launching a child.
    ///
    /// No listener parameter: [`Self::with_listener`] carries it, so a caller
    /// cannot set the same fact in two places.
    ///
    /// Infallible because the one thing it carries is a string. The earlier shape
    /// had to generate a nonce here and was `Result` for it; with the nonce gone
    /// (design decision 17) there is nothing left to fail.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPlan;
    ///
    /// // The launcher's own constructor: the configuration's text, and nothing else yet.
    /// let plan = LaunchPlan::for_launch("port = 9000".into());
    /// assert_eq!(plan.config(), "port = 9000");
    /// assert_eq!(plan.token(), None);
    /// ```
    pub fn for_launch(config: String) -> Self {
        Self {
            config: Some(config),
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Handles for the plan legs below, in the shape each platform's `ControlHandle` takes.
    ///
    /// **A real descriptor on Unix and a bare value on Windows**, which is the same difference the
    /// type itself carries: Unix's constructor duplicates a descriptor (so the test opens one) and
    /// Windows' copies the handle the launcher's channel owns (so the test spells one). The
    /// ordering assertion below needs two distinct values, and each platform's constructor is what
    /// makes them distinct.
    fn control_handles() -> (ControlHandle, ControlHandle) {
        #[cfg(unix)]
        {
            let duplicate = || {
                let source = fs::File::open("/dev/null").expect("a descriptor to duplicate");
                ControlHandle::duplicate(&source).expect("duplicate a descriptor")
            };
            (duplicate(), duplicate())
        }
        #[cfg(windows)]
        {
            let copy = |value| ControlHandle::inherited(value);
            (copy(1040), copy(1044))
        }
    }

    #[test]
    fn the_plan_carries_the_inherited_control_handles() {
        // The accessor pair, and the `Default` both `bind`ers rely on: a plan that
        // never named a control handle answers empty, which is the shape of every
        // `api`-less launch and of a launcher run whose selection names no local
        // channel. It is not the *unix* shape: macOS is a unix launcher platform and
        // carries one when it has a local channel.
        let (first, second) = control_handles();
        assert_ne!(
            first.value(),
            second.value(),
            "each duplicate is its own descriptor, so the plan's ordering is readable"
        );
        assert!(LaunchPlan::default().controls().is_empty());
        assert_eq!(
            LaunchPlan::new().with_control(first).controls().len(),
            1,
            "the single-handle spelling is a one-instance channel"
        );
        let (third, fourth) = control_handles();
        let plan = LaunchPlan::new()
            .with_controls([third, fourth])
            .controls()
            .iter()
            .map(ControlHandle::value)
            .collect::<Vec<_>>();
        assert_eq!(plan.len(), 2, "a pool is every instance the launcher bound");
        assert_ne!(
            plan[0], plan[1],
            "the pool is a set of handles, in its order: {plan:?}"
        );
        // And it rides beside the other facts rather than replacing one: the listener, the
        // configuration text and the control handles are separate handoffs.
        let plan = LaunchPlan::for_launch("port = 9001".into()).with_controls([second]);
        assert_eq!(plan.controls().len(), 1);
        assert_eq!(plan.config(), "port = 9001");
    }

    #[test]
    fn the_plan_carries_the_token_beside_the_rest() {
        // The fourth fact a plan can carry, and the two halves that make it one: a plan that
        // never called `with_token` answers `None` — every unconfined and in-place launch — and
        // one that did answers the token the platform will put in the payload.
        assert_eq!(LaunchPlan::default().token(), None);
        let token = "ab".repeat(32);
        let plan = LaunchPlan::for_launch("port = 9000".into()).with_token(token.clone());
        assert_eq!(plan.token(), Some(token.as_str()));
        assert_eq!(
            plan.config(),
            "port = 9000",
            "the token rides beside the payload, replacing nothing"
        );
    }

    #[test]
    fn an_empty_configuration_is_still_a_configuration() {
        // **The predicate's whole reason: presence is not emptiness.** A plan built by
        // `for_launch` carries a text even when the text is empty — the child parses it into
        // defaults and serves — while a plan built any other way carries none, and a launcher's
        // precondition has to tell those two apart before it writes a frame.
        assert!(LaunchPlan::for_launch("port = 9000".into()).has_config());
        assert!(LaunchPlan::for_launch(String::new()).has_config());
        assert!(!LaunchPlan::new().has_config());
        assert!(!LaunchPlan::default().has_config());
        // And the accessor cannot see the difference the predicate exists for: both plans answer
        // the same empty text, which is why `config().is_empty()` is not the question to ask.
        assert_eq!(LaunchPlan::for_launch(String::new()).config(), "");
        assert_eq!(LaunchPlan::new().config(), "");
    }
}
