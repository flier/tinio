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
