//! Sibling DLLs beside the program, and the one pattern the grant uses.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::_core::{AccessMode, Exposure};

/// Decision 22's entry for the program's own directory: the path the scan is of,
/// exposed read-only.
///
/// One spelling for the entry both assemblies of the rule append — the public set
/// and the launch's whitelist — so the two cannot come to different entries for
/// the same directory.
pub(super) fn program_directory(siblings: &SiblingScan) -> Exposure {
    Exposure {
        host: siblings.dir().to_path_buf(),
        access: AccessMode::ReadOnly,
    }
}

/// The `*.dll` files a [`SiblingScan`] found, as read-only exposure entries.
///
/// The mapping from the ACL layer's enumeration into exposures, and it is a
/// function of the scan rather than of a `&Path` so that there is no way to spell
/// this that reads the directory a second time: the set below and the per-file
/// grants are about one enumeration, which is what keeps them from disagreeing
/// about which files the program has beside it.
///
/// **The entries are for the banner and the count**, and each one is the host the
/// grant opens by handle ([`Grants::apply_siblings`]): a DLL's spelling reaches
/// the ACL layer, which is why the set is kept rather than only its length.
///
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
pub(super) fn sibling_exposures(scan: &SiblingScan) -> Vec<Exposure> {
    scan.files()
        .iter()
        .map(|host| Exposure {
            host: host.clone(),
            access: AccessMode::ReadOnly,
        })
        .collect()
}

/// One enumeration of a program directory's sibling DLLs, and the three answers
/// every caller derives from it.
///
/// **The type exists because three consumers must agree about one file set** —
/// the exposure entries the launcher's whitelist lists for the banner, the count
/// that banner carries, and the per-file grants — and each of them used to read
/// the directory for itself. The cost of that was measurable rather than
/// theoretical: the measured program directory in this workspace is
/// `target/debug/deps`, **5,129 entries**, and `confine` enumerated it twice on
/// every launch (once through `exposures`, once through the other reader's pattern)
/// *plus* a `symlink_metadata` on each row of that directory. Two enumerations are
/// also two chances to disagree, which is the failure the shared set exists to
/// prevent.
///
/// **One scan, and the grant enumerates nothing of its own**: [`Self::files`] is
/// what [`Grants::apply_siblings`] opens by handle, one object at a time, so the
/// set the banner lists and the set the ACEs land on are the same objects.
/// [`Self::pattern`] is the set's own spelling, which the grant no longer goes
/// through (a wildcard names objects no handle can be opened on) and which the
/// tests that pin the enumeration keep.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::backend::SiblingScan;
///
/// // Lexical, and one enumeration for every reader: the scan answers about the
/// // path it was given whether or not that directory is there, and a directory
/// // holding no DLL has nothing to grant.
/// let scan = SiblingScan::scan(Path::new(r"E:\build"));
/// assert_eq!(scan.dir(), Path::new(r"E:\build"));
/// assert_eq!(scan.pattern().is_none(), scan.is_empty());
/// ```
///
/// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
pub struct SiblingScan {
    dir: PathBuf,
    files: Vec<PathBuf>,
}

impl SiblingScan {
    /// Enumerate `dir` once.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::backend::SiblingScan;
    ///
    /// // Lexical: the scan answers about the path it was given whether or not
    /// // that directory is there, and one holding no DLL has nothing to grant.
    /// let scan = SiblingScan::scan(Path::new(r"E:\build"));
    /// assert_eq!(scan.dir(), Path::new(r"E:\build"));
    /// assert_eq!(scan.pattern().is_none(), scan.is_empty());
    /// ```
    pub fn scan(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
            files: sibling_dlls(dir),
        }
    }

    /// Whether the directory holds no sibling at all.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::backend::SiblingScan;
    ///
    /// let scan = SiblingScan::scan(Path::new(r"E:\build"));
    /// assert_eq!(scan.is_empty(), scan.files().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The set [`Self::files`] enumerates, spelled as **the tool's own pattern**:
    /// `dir` plus `*.dll`, or `None` when there is nothing to spell.
    ///
    /// **The pattern is what the enumeration is, not the mechanism a grant goes
    /// through any more.** It was the batch: `icacls` takes a single `name` pattern
    /// and no second path (measured 2026-09-21 on this host — a two-path invocation
    /// exits `87 Invalid parameter` naming the second path and writes nothing, a
    /// comma-joined pair exits `123`, and the flags-first order is refused
    /// outright), so one invocation covered every DLL directly inside the
    /// directory. The grant is per file now ([`Grants::apply_siblings`]): a wildcard
    /// is not an object, so nothing can be opened through it, and the batch is the
    /// one shape that could grant an object the enumeration never checked. What the
    /// pattern still answers is the question it always could — which files this
    /// scan is about, in the spelling the tool matches them with — and it is `None`
    /// for a directory with no sibling rather than an unmatched pattern, which the
    /// tool reports as an error (measured the same day: `icacls <dir>\*.dllx
    /// /grant:r …` exits `2` `The system cannot find the file specified.`).
    ///
    /// **The base is `self.dir` itself, and the parent is never an answer.** A
    /// pattern built one level up has the same `*.dll` leaf and would name the
    /// parent's DLLs, not the program's — the silent `0xC0000135` that rule exists
    /// to close. A path whose final component is a real name always has a parent,
    /// verbatim `\\?\` prefix or not, so there is nothing to fall back for.
    ///
    /// [`Grants::apply_siblings`]: crate::acl::Grants::apply_siblings
    pub fn pattern(&self) -> Option<PathBuf> {
        if self.is_empty() {
            return None;
        }
        Some(self.dir.join("*.dll"))
    }

    /// The files this scan found, in the order every caller sees them.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::backend::SiblingScan;
    ///
    /// let scan = SiblingScan::scan(Path::new(r"E:\build"));
    /// // Sorted, and the same set the `*.dll` pattern spells — which is what
    /// // makes one enumeration the answer for every reader.
    /// assert_eq!(scan.files().is_empty(), scan.pattern().is_none());
    /// ```
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// The directory the scan is of, for the callers that name it in a log line.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::backend::SiblingScan;
    ///
    /// let scan = SiblingScan::scan(Path::new(r"E:\build"));
    /// assert_eq!(scan.dir(), Path::new(r"E:\build"));
    /// ```
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// The `*.dll` entries directly inside `dir`, in a stable order — **by name, and
/// nothing is filtered on what the name happens to resolve to.**
///
/// **Directly inside, and no deeper**, because that is the boundary the decision
/// drew rather than the one a loader has: a DLL in a subdirectory of the program is
/// not a sibling and is not granted, and a binary that needs one is a case the
/// design records as a consequence of the rule.
///
/// The suffix test is on the file name and is case-insensitive, because Windows
/// does not distinguish `.dll` from `.DLL` and a `dll` the loader finds is a DLL
/// that has to be granted.
///
/// **`is_file` is gone, and its removal is the fix rather than a simplification.**
/// It follows a link, so what it answered about a reparse point was about the
/// link's **target**, and the enumeration's own answer was wrong in both
/// directions at once (both measured on this host 2026-09-27): a **junction**
/// named `x.dll` resolved to a directory, was dropped from the set — and was still
/// matched by the `*.dll` pattern the grant then ran, which wrote the ACE on the
/// link's own object; a **file symlink** named `x.dll` resolved to a file, stayed
/// in the set, and was granted through the pattern with the ACE landing on its
/// **target**, outside the program's directory. The enumeration is by name now, and
/// every match is examined on the object itself: `canonical_whitelist` refuses a
/// sibling that is a reparse point, and the grant refuses one a second time on the
/// handle it opens, so what the enumeration lists and what the grant acts on are
/// one set. A **directory** named `x.dll` is enumerated too and fails the launch at
/// the grant's kind check, which is the same fail-closed direction the operator
/// chose for DLLs.
///
/// A directory that `read_dir` cannot open holds no entries this function can name,
/// which is why the error is not propagated: the caller's next step is the
/// resolution that reports a path it cannot reach, and a second error here would
/// replace it. **A name that was listed is a different matter**: a stat that fails
/// for one of those is a refusal at the sibling check, not an absence here.
///
/// The order is sorted so that what the caller derives from it — the exposure
/// vector, the log line, the per-file grants — does not depend on the
/// filesystem's enumeration order.
///
/// **A caller that needs both a list and a pattern wants [`SiblingScan`]**, which
/// reads the directory once for both; this is the enumeration alone, and it is
/// public for the tests that assert the boundary rule against a real directory.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::backend::{SiblingScan, sibling_dlls};
///
/// // The same set [`SiblingScan`] reports, which is what makes the two one
/// // enumeration rather than two readings of a directory.
/// let scan = SiblingScan::scan(Path::new(r"E:\build"));
/// assert_eq!(
///     sibling_dlls(Path::new(r"E:\build")).len(),
///     scan.files().len()
/// );
/// ```
pub fn sibling_dlls(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<PathBuf> = entries
        .flatten()
        .filter(|entry| {
            Path::new(&entry.file_name())
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("dll"))
        })
        .map(|entry| entry.path())
        .collect();
    files.sort();
    files
}
