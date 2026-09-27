//! The confinement request: which host paths must stay reachable, and how.

use std::path::PathBuf;

use crate::_core::AccessMode;

/// One host path the confined process must be able to reach.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Exposure};
///
/// let exposure = Exposure {
///     host: Path::new("/srv/data").into(),
///     access: AccessMode::ReadWrite,
/// };
/// assert!(!exposure.access.is_read_only());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exposure {
    /// The path as the host sees it. The confined process sees the same
    /// string — that is what makes the confinement invisible to the rest of
    /// the program (design decision 9).
    pub host: PathBuf,
    /// Whether the confined process may write there.
    pub access: AccessMode,
}

/// The set of paths a confined process may reach, plus the fixed device files.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Sandbox};
///
/// let sandbox = Sandbox::new()
///     .expose("/srv/tinio", AccessMode::ReadWrite)
///     .device_files();
/// assert!(sandbox.device_files_enabled());
/// ```
#[derive(Debug, Clone, Default)]
pub struct Sandbox {
    exposures: Vec<Exposure>,
    device_files: bool,
}

impl Sandbox {
    /// An empty request: nothing exposed, no device files.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Sandbox;
    ///
    /// let sandbox = Sandbox::new();
    /// assert!(sandbox.exposures().is_empty());
    /// assert!(!sandbox.device_files_enabled());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Require `host` to remain reachable, with `access`.
    ///
    /// Exposing the same path twice keeps the **most permissive** access: a
    /// narrower entry can never silently downgrade a wider one.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let sandbox = Sandbox::new()
    ///     .expose("/srv/data", AccessMode::ReadWrite)
    ///     .expose("/srv/data", AccessMode::ReadOnly);
    /// assert_eq!(sandbox.exposures().len(), 1, "one entry per path");
    /// assert_eq!(
    ///     sandbox.exposures()[0].access,
    ///     AccessMode::ReadWrite,
    ///     "the narrower entry did not downgrade the wider one"
    /// );
    /// ```
    pub fn expose(mut self, host: impl Into<PathBuf>, access: AccessMode) -> Self {
        merge_exposure(&mut self.exposures, host.into(), access);
        self
    }

    /// Also bind the platform's device files, where the platform has the
    /// concept. Linux binds `/dev/{null,zero,random,urandom}`; they are
    /// defensive rather than load-bearing, because Rust's `getrandom` is a
    /// syscall and needs no `/dev` (design decision 12).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Sandbox;
    ///
    /// // Opt-in: a request that never asked for them carries no device files.
    /// assert!(!Sandbox::new().device_files_enabled());
    /// assert!(Sandbox::new().device_files().device_files_enabled());
    /// ```
    pub fn device_files(mut self) -> Self {
        self.device_files = true;
        self
    }

    /// The exposures, in the order they were added.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let sandbox = Sandbox::new()
    ///     .expose("/srv/a", AccessMode::ReadOnly)
    ///     .expose("/srv/b", AccessMode::ReadWrite);
    /// let hosts: Vec<&Path> = sandbox
    ///     .exposures()
    ///     .iter()
    ///     .map(|exposure| exposure.host.as_path())
    ///     .collect();
    /// assert_eq!(hosts, [Path::new("/srv/a"), Path::new("/srv/b")]);
    /// ```
    pub fn exposures(&self) -> &[Exposure] {
        &self.exposures
    }

    /// Whether [`Self::device_files`] was called.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Sandbox;
    ///
    /// // The flag is an argument `from_exposures` takes rather than one it remembers, and
    /// // this is the read a caller transforming an existing set passes back in.
    /// let taken = Sandbox::from_exposures(Vec::new(), true);
    /// assert!(taken.device_files_enabled());
    /// ```
    pub fn device_files_enabled(&self) -> bool {
        self.device_files
    }

    /// Take a set that was already built, with the device-file flag it was
    /// built with.
    ///
    /// **It applies [`Self::expose`]'s most-permissive rule to the vector it is
    /// given**, so a caller merging two sets — or rebuilding one it has just
    /// transformed — cannot leave a path carrying both a read-only entry and the
    /// read-write entry that was meant to widen it. Applying the rule to a set
    /// that already obeys it changes nothing: the first entry for a path stays
    /// where it was and a later, narrower duplicate merges into it.
    ///
    /// `expose` is the only way to *add*, so a caller that must transform the
    /// whole set — the launcher platforms canonicalise paths and add the
    /// executable's directory — would otherwise iterate [`Self::exposures`] and
    /// re-`expose` each one, rebuilding the set to change it. With a dozen
    /// exposures that is a dozen re-insertions, and a silent second application
    /// of a rule the caller has already applied.
    ///
    /// **The device-file flag is a parameter rather than something this
    /// remembers**, and that is the whole reason the signature has two
    /// arguments: a one-argument form drops the flag, so a caller rebuilding a
    /// set that had called [`Self::device_files`] would get a jail with no
    /// `/dev` and nothing to notice it. A caller transforming an existing set
    /// passes [`Self::device_files_enabled`]; a caller building one from
    /// scratch says `true` or `false` and means it.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let built = Sandbox::new()
    ///     .expose("/srv/data", AccessMode::ReadOnly)
    ///     .device_files();
    /// let taken = Sandbox::from_exposures(built.exposures().to_vec(), built.device_files_enabled());
    /// assert_eq!(taken.exposures(), built.exposures());
    /// assert!(
    ///     taken.device_files_enabled(),
    ///     "the flag travels with the set"
    /// );
    /// ```
    pub fn from_exposures(exposures: Vec<Exposure>, device_files: bool) -> Self {
        let mut merged = Vec::with_capacity(exposures.len());
        for exposure in exposures {
            merge_exposure(&mut merged, exposure.host, exposure.access);
        }
        Self {
            exposures: merged,
            device_files,
        }
    }
}

/// Insert `host` with `access`, or widen the entry already there for that path.
///
/// **The most-permissive rule lives here rather than on the `expose` path**, so
/// that a set rebuilt through [`Sandbox::from_exposures`] obeys it too: with the
/// rule inside `expose` alone, a vector carrying two entries for one path
/// arrived at the platform as it stood, and a path the caller meant to expose
/// read-only went in with a read-write entry beside it.
///
/// The incoming value rather than an `AccessMode::ReadWrite` literal: the two
/// agree while the enum has two variants, and a literal would silently coalesce
/// whatever variant it grows.
fn merge_exposure(exposures: &mut Vec<Exposure>, host: PathBuf, access: AccessMode) {
    match exposures.iter_mut().find(|e| e.host == host) {
        Some(existing) => {
            if !access.is_read_only() {
                existing.access = access;
            }
        }
        None => exposures.push(Exposure { host, access }),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::_core::AccessMode;

    #[test]
    fn expose_records_path_and_access() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        let exposures = sandbox.exposures();
        assert_eq!(exposures.len(), 1);
        assert_eq!(exposures[0].host, Path::new("/srv/data"));
        assert_eq!(exposures[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn duplicate_paths_keep_the_most_permissive_access() {
        // `serve` exposes the served root read-write and its state directory
        // read-write again; a later narrower entry must never downgrade it.
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadWrite)
            .expose("/srv/data", AccessMode::ReadOnly);
        assert_eq!(sandbox.exposures().len(), 1);
        assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn from_exposures_keeps_the_most_permissive_access_too() {
        // **The path a launcher platform takes, and the one the rule had to
        // move onto.** A called-out set is assembled from somewhere else — a
        // canonicalised rebuild of an existing set, a set merged with another,
        // the executable's directory added — so the same path can arrive twice
        // with different access, and a vector stored as given handed the
        // platform both entries and left the choice of which to apply to it.
        //
        // Both orders, because the merge has to reach the same answer either
        // way: `data` is widened by its second entry, `logs` arrives narrower
        // after a wider one and must not lose it.
        let called_out = vec![
            Exposure {
                host: "/srv/data".into(),
                access: AccessMode::ReadWrite,
            },
            Exposure {
                host: "/srv/data".into(),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: "/srv/logs".into(),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: "/srv/logs".into(),
                access: AccessMode::ReadWrite,
            },
        ];
        let sandbox = Sandbox::from_exposures(called_out, false);
        assert_eq!(sandbox.exposures().len(), 2, "one entry per path");
        assert_eq!(sandbox.exposures()[0].host, Path::new("/srv/data"));
        assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadWrite);
        assert_eq!(sandbox.exposures()[1].host, Path::new("/srv/logs"));
        assert_eq!(sandbox.exposures()[1].access, AccessMode::ReadWrite);
    }

    #[test]
    fn the_builder_does_not_touch_the_filesystem() {
        // Recorded verbatim, even though the path does not exist — validation
        // is the platform's job, so the builder stays testable everywhere.
        let sandbox = Sandbox::new().expose("/no/such/path/anywhere", AccessMode::ReadOnly);
        assert_eq!(
            sandbox.exposures()[0].host,
            Path::new("/no/such/path/anywhere")
        );
    }

    #[test]
    fn device_files_are_opt_in() {
        assert!(!Sandbox::new().device_files_enabled());
        assert!(Sandbox::new().device_files().device_files_enabled());
    }
}
