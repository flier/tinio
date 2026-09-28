//! The confinement request: which host paths must stay reachable, and how.

use std::{collections::HashMap, io, path::PathBuf};

use crate::{
    _core::AccessMode,
    error::{Result, fail},
};

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

/// One exposure set as one line: `host (access)` per entry, comma separated.
///
/// **The renderer every backend was writing for itself, and there was more than
/// one spelling of it.** Linux kept a private copy that said `read-only` and
/// `read-write`; Windows debug-printed the enum — `ReadOnly` — through its own
/// public `describe`, so one set read two ways in the two logs an operator reads
/// to find out what a jail let through. **This is the human spelling**, and the
/// choice is made here rather than by a `Display` impl on [`AccessMode`] because
/// that type is a two-variant policy rather than a user-facing string: this
/// function is the one place that has to render it for a reader. Windows' arm
/// therefore changes with this signature — its `describe` becomes a call to this
/// function and its `(ReadOnly)` spelling goes away.
///
/// **A rendered list is a field value and not a sentence** (shared §7 of the
/// `--sandbox` plans): the entries stay one field of the announcing event, and an
/// empty set renders as the empty string rather than as a word for "none".
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Exposure, describe_exposures};
///
/// let set = [
///     Exposure {
///         host: "/srv/data".into(),
///         access: AccessMode::ReadWrite,
///     },
///     Exposure {
///         host: "/srv/state".into(),
///         access: AccessMode::ReadOnly,
///     },
/// ];
/// assert_eq!(
///     describe_exposures(&set),
///     "/srv/data (read-write), /srv/state (read-only)"
/// );
/// assert_eq!(describe_exposures(&[]), "");
/// ```
pub fn describe_exposures(exposures: &[Exposure]) -> String {
    describe_exposures_iter(exposures)
}

/// The same rendering over **any borrowed sequence** of them — a caller that holds the entries in
/// two sets passes their chain rather than collecting a third set only to render it.
///
/// The slice form above stays the facade's pinned signature; this is the shape a caller with the
/// entries already split reaches for.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Exposure, describe_exposures_iter};
///
/// let read = [Exposure {
///     host: "/srv/data".into(),
///     access: AccessMode::ReadOnly,
/// }];
/// let write = [Exposure {
///     host: "/srv/state".into(),
///     access: AccessMode::ReadWrite,
/// }];
///
/// assert_eq!(
///     describe_exposures_iter(read.iter().chain(&write)),
///     "/srv/data (read-only), /srv/state (read-write)"
/// );
/// ```
pub fn describe_exposures_iter<'a>(exposures: impl IntoIterator<Item = &'a Exposure>) -> String {
    use std::fmt::Write as _;

    // One buffer rather than a `Vec<String>` joined afterwards: the separator is the only thing
    // that intermediate collection was carrying.
    let mut rendered = String::new();
    for exposure in exposures {
        if !rendered.is_empty() {
            rendered.push_str(", ");
        }
        let access = if exposure.access.is_read_only() {
            "read-only"
        } else {
            "read-write"
        };
        let _ = write!(rendered, "{} ({access})", exposure.host.display());
    }
    rendered
}

/// The set of paths a confined process may reach.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Sandbox};
///
/// let sandbox = Sandbox::new().expose("/srv/tinio", AccessMode::ReadWrite);
/// assert_eq!(sandbox.exposures().len(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Sandbox {
    exposures: Vec<Exposure>,
}

impl Sandbox {
    /// An empty request: nothing exposed.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Sandbox;
    ///
    /// let sandbox = Sandbox::new();
    /// assert!(sandbox.exposures().is_empty());
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

    /// The served root: the first exposure, or a refusal when nothing is exposed.
    ///
    /// **The convention every backend restates, in the one place that can own it.**
    /// A launcher has to name the root it serves — Linux derives the jail's whole
    /// layout from it, and the other two confine around it — and each of them built
    /// the same two lines to get it: the first of [`Self::exposures`] (the caller
    /// exposes the root before anything else, so the first entry is the root by
    /// construction), or a refusal when there is no first entry at all. The two
    /// copies that existed were already identical, `fail("confine", …)` with
    /// `nothing exposed to confine to`, so this is that refusal and not a new one.
    ///
    /// **The refusal is [`Error::Step`](crate::Error::Step) rather than
    /// [`Error::Unsupported`](crate::Error::Unsupported)** because an empty set is
    /// the caller's mistake rather than this platform's shortcoming (design §11): an
    /// `Unsupported` here would send an operator to look at their kernel for a
    /// request that never said what to confine to. It is built through [`fail`], so
    /// the event and the returned error carry one `step` string — the reason the two
    /// platform copies used that constructor too.
    ///
    /// # Errors
    ///
    /// [`Error::Step`](crate::Error::Step) with the step `confine` when the request
    /// exposes nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let sandbox = Sandbox::new()
    ///     .expose("/srv/tinio", AccessMode::ReadWrite)
    ///     .expose("/srv/state", AccessMode::ReadWrite);
    /// // The first exposure is the root, and it carries its own access with it.
    /// let root = sandbox.served_root()?;
    /// assert_eq!(root.host, Path::new("/srv/tinio"));
    /// assert_eq!(root.access, AccessMode::ReadWrite);
    /// assert!(Sandbox::new().served_root().is_err());
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub fn served_root(&self) -> Result<&Exposure> {
        self.exposures
            .first()
            .ok_or_else(|| fail("confine", io::Error::other("nothing exposed to confine to")))
    }

    /// Take a set that was already built.
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
    /// **One pass, keyed by host, rather than a scan of the output per input
    /// entry.** The sets this is handed are not builder-sized: the Windows launcher
    /// hands it the caller's exposures plus one entry per sibling DLL it has to
    /// keep reachable (~70 in this workspace's `target/debug/deps`), and calls it
    /// more than once per launch, so the scan `merge_exposure` does for the
    /// builder — right for a chain that names three paths — spent about n²/2 path
    /// comparisons here to answer what one hash lookup per entry answers. The map
    /// is `at`, a `HashMap<PathBuf, usize>` from a host to the position it took in
    /// `merged`; it is only ever looked up and inserted into, never iterated, so no
    /// hash seed can reorder anything and **the output is the input's
    /// first-occurrence order**: the first entry for a host keeps its position, and
    /// every later duplicate widens that entry where it stands instead of being
    /// appended beside it.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let built = Sandbox::new()
    ///     .expose("/srv/data", AccessMode::ReadOnly)
    ///     .expose("/srv/data", AccessMode::ReadWrite);
    /// let taken = Sandbox::from_exposures(built.exposures().to_vec());
    /// assert_eq!(taken.exposures(), built.exposures());
    /// ```
    pub fn from_exposures(exposures: Vec<Exposure>) -> Self {
        let mut merged = Vec::with_capacity(exposures.len());
        let mut at: HashMap<PathBuf, usize> = HashMap::with_capacity(exposures.len());
        for exposure in exposures {
            match at.get(exposure.host.as_path()).copied() {
                Some(landed) => widen(&mut merged[landed], exposure.access),
                None => {
                    at.insert(exposure.host.clone(), merged.len());
                    merged.push(exposure);
                }
            }
        }
        Self { exposures: merged }
    }
}

/// Widen `exposure` to `access` when `access` is the wider of the two.
///
/// **The most-permissive rule lives in one function rather than on the `expose`
/// path**, so that a set rebuilt through [`Sandbox::from_exposures`] obeys it
/// too: with the rule inside `expose` alone, a vector carrying two entries for one
/// path arrived at the platform as it stood, and a path the caller meant to expose
/// read-only went in with a read-write entry beside it. The two call sites *find*
/// the entry differently — [`merge_exposure`] scans the set it is appending to,
/// `from_exposures` remembers each host's position as it goes — but only this
/// function decides what finding one *means*, so the two cannot drift apart on
/// which access survives a collision.
///
/// The incoming value rather than an `AccessMode::ReadWrite` literal: the two
/// agree while the enum has two variants, and a literal would silently coalesce
/// whatever variant it grows.
fn widen(exposure: &mut Exposure, access: AccessMode) {
    if !access.is_read_only() {
        exposure.access = access;
    }
}

/// Insert `host` with `access`, or widen the entry already there for that path.
///
/// **The scan is the shape of the builder that uses it**, and this is now its only
/// caller: a chain names one served root and a directory or two beside it, so
/// looking for the host among the entries so far is free, and the map that would
/// replace it would have to be a field of [`Sandbox`] kept in sync by every
/// mutator. [`Sandbox::from_exposures`], which is handed whole sets at once, pays
/// for the map instead — see its docs for why that is the one that needed it.
fn merge_exposure(exposures: &mut Vec<Exposure>, host: PathBuf, access: AccessMode) {
    match exposures.iter_mut().find(|e| e.host == host) {
        Some(existing) => widen(existing, access),
        None => exposures.push(Exposure { host, access }),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::_core::AccessMode;
    // The refusal's own variant, which no code above names: the module imports the alias and the
    // builder it is thrown through, and only this module matches on the value.
    use crate::error::Error;

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
        let sandbox = Sandbox::from_exposures(called_out);
        assert_eq!(sandbox.exposures().len(), 2, "one entry per path");
        assert_eq!(sandbox.exposures()[0].host, Path::new("/srv/data"));
        assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadWrite);
        assert_eq!(sandbox.exposures()[1].host, Path::new("/srv/logs"));
        assert_eq!(sandbox.exposures()[1].access, AccessMode::ReadWrite);
    }

    #[test]
    fn a_duplicate_host_keeps_the_first_entrys_position() {
        // **The other half of the merge's contract, which the scan got right by
        // construction.** A host's position in the set is its *first* occurrence's,
        // and callers build on that — the served root is the first entry by
        // convention — so a merge that widened a duplicate by moving it to the end
        // would relocate the head of the set. The duplicate here sits in the middle
        // of the input, which is where an append-on-widen merge and a keep-in-place
        // one disagree about the order.
        let called_out = vec![
            Exposure {
                host: "/srv/first".into(),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: "/srv/repeated".into(),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: "/srv/last".into(),
                access: AccessMode::ReadWrite,
            },
            Exposure {
                host: "/srv/repeated".into(),
                access: AccessMode::ReadWrite,
            },
        ];
        let sandbox = Sandbox::from_exposures(called_out);
        let hosts: Vec<&Path> = sandbox
            .exposures()
            .iter()
            .map(|exposure| exposure.host.as_path())
            .collect();
        assert_eq!(
            hosts,
            [
                Path::new("/srv/first"),
                Path::new("/srv/repeated"),
                Path::new("/srv/last"),
            ],
            "the widened duplicate stayed where its first entry was"
        );
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
    fn the_served_root_is_the_first_exposure() {
        // The convention every backend derives from: the caller exposes the root first, so the
        // first entry is the root, and its access rides with it rather than being asked for
        // separately.
        let sandbox = Sandbox::new()
            .expose("/srv/tinio", AccessMode::ReadWrite)
            .expose("/srv/state", AccessMode::ReadOnly);
        let root = sandbox.served_root().expect("a first exposure");
        assert_eq!(root.host, Path::new("/srv/tinio"));
        assert_eq!(root.access, AccessMode::ReadWrite);
        // A *narrower* first entry is still the root: the order decides which path it is, and the
        // access is read off the entry rather than guessed from it.
        let read_only = Sandbox::new().expose("/srv/tinio", AccessMode::ReadOnly);
        assert!(
            read_only
                .served_root()
                .expect("a first exposure")
                .access
                .is_read_only()
        );
    }

    #[test]
    fn an_empty_request_has_no_served_root() {
        // **The refusal, and the variant that carries it.** An empty exposure set is the caller's
        // mistake rather than a platform's shortcoming, so it is `Error::Step` (with the step
        // `confine`) and not `Error::Unsupported` — the error a backend used to build for this at
        // its own call site, to the character.
        let err = Sandbox::new()
            .served_root()
            .expect_err("nothing is exposed, so there is no root");
        assert!(
            matches!(
                err,
                Error::Step {
                    step: "confine",
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(
            err.to_string().contains("nothing exposed to confine to"),
            "{err}"
        );
    }

    #[test]
    fn a_rendered_set_spells_access_for_a_reader() {
        // The one spelling of a set: `read-only`/`read-write` rather than the enum's `Debug`, in
        // the caller's order, and nothing at all for an empty set.
        let set = [
            Exposure {
                host: "/srv/data".into(),
                access: AccessMode::ReadWrite,
            },
            Exposure {
                host: "/srv/state".into(),
                access: AccessMode::ReadOnly,
            },
        ];
        assert_eq!(
            describe_exposures(&set),
            "/srv/data (read-write), /srv/state (read-only)"
        );
        assert_eq!(describe_exposures(&[]), "");
        assert_eq!(
            describe_exposures(&set[..1]),
            "/srv/data (read-write)",
            "one entry renders as itself, with no separator to trim"
        );
    }
}
