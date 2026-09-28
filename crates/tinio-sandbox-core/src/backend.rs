//! The trait every platform implements, and what it answers.

use crate::{
    error::Result,
    launcher::{Child, LaunchPlan},
    sandbox::Sandbox,
};

/// What the call site must do once [`Backend::confine`] returns.
///
/// This is what keeps `#[cfg]` out of `main()`: Linux confines the running
/// process, macOS and Windows start a child, and the caller reacts to the
/// answer instead of branching on the platform (design decision 2).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Confinement;
///
/// let answer = Confinement::InPlace;
/// assert!(matches!(answer, Confinement::InPlace));
/// ```
#[derive(Debug)]
pub enum Confinement {
    /// The calling process is confined; carry on. Linux always answers this.
    InPlace,
    /// A confined child is running. Wait for it, forward its exit status, and
    /// undo whatever the launch set up — [`crate::supervise()`] does all three.
    Supervise(Child),
}

/// A platform's confinement mechanism.
///
/// **This trait answers "confine this", and never "is there a mechanism here".** A `Backend`
/// value exists only where an implementation does, so a caller cannot ask a target that has
/// none whether it has one — and the answer is needed *before* an arm chooses a backend, to
/// refuse a request rather than reach an arm that cannot exist. The availability question is
/// therefore published beside the platform selection, as `tinio_sandbox::backend_available`,
/// and deliberately not as a method here: this trait stays the one call every mechanism
/// answers, and a target with no implementation has nothing to answer it on.
///
/// **`confine` takes `&mut self`**, and the receiver is the honest one in both
/// directions: on Linux the call changes the calling process irreversibly
/// (design decision 26), while a launcher backend uses the receiver to record
/// what it set up — the state it reports afterwards. A backend that had to reach
/// for interior mutability to do that was expressing the contract wrongly.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Backend, Confinement, LaunchPlan, Result, Sandbox};
///
/// struct Noop;
///
/// impl Backend for Noop {
///     fn confine(&mut self, _sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
///         Ok(Confinement::InPlace)
///     }
/// }
/// ```
pub trait Backend {
    /// Confine the process, or start a confined child for it.
    ///
    /// # Errors
    ///
    /// Any failure is fatal: the caller must exit rather than continue
    /// unconfined (design §11).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{Backend, Confinement, LaunchPlan, Result, Sandbox};
    ///
    /// // The trait's own example is the shape an implementation takes; this is the call site
    /// // that shape exists for, where the answer — and not a `#[cfg]` — decides what the
    /// // process does next.
    /// struct Noop;
    ///
    /// impl Backend for Noop {
    ///     fn confine(&mut self, _sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
    ///         Ok(Confinement::InPlace)
    ///     }
    /// }
    ///
    /// let mut backend = Noop;
    /// match backend.confine(&Sandbox::new(), &LaunchPlan::new())? {
    ///     Confinement::InPlace => {}
    ///     Confinement::Supervise(_child) => {}
    /// }
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement>;
}

/// What a target's confinement does, so a caller branches on the fact and not on the OS.
///
/// **Each backend publishes its own instance**, as the `FACTS` constant beside its [`Backend`]
/// implementation: every field here is a statement about that backend's mechanism, so the crate
/// that owns the mechanism is the crate that answers for it. A table of `cfg!(target_os = …)`
/// assembled in a crate that owns none of them is a second home for the same fact — and the one
/// that drifts when a mechanism changes.
///
/// **Ending the child of a launch that fails after it exists is not a field here.** Each launcher
/// platform installs its own killer on the child it starts ([`Child::with_kill`]), so
/// [`Child::abort`] ends the child on every launcher platform and the caller's call is
/// unconditional — a field here could only restate the answer the platform already gave when it
/// built the child.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::PlatformFacts;
///
/// // What a backend that confines in place answers: it launches nothing, so the three facts a
/// // launch has are not its to state.
/// let in_place = PlatformFacts {
///     launches_child: false,
///     refuses_derived_state_dir: false,
///     probe_state_dir_writable: false,
///     control_socket: false,
/// };
/// assert!(!in_place.launches_child);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlatformFacts {
    /// The backend launches a supervised child. An in-place backend confines this process.
    pub launches_child: bool,
    /// The mechanism grants the state directory and not its parent.
    pub refuses_derived_state_dir: bool,
    /// The launcher proves the state directory can take a file before it launches.
    pub probe_state_dir_writable: bool,
    /// The launcher rebuilds the inherited control channel as a unix socket, rather than reading
    /// handle values the child inherited.
    pub control_socket: bool,
}

impl PlatformFacts {
    /// No mechanism at all: what a target with no backend wired answers, and what an in-place
    /// backend answers for the three facts a launch would own.
    pub const NONE: Self = Self {
        launches_child: false,
        refuses_derived_state_dir: false,
        probe_state_dir_writable: false,
        control_socket: false,
    };
}
