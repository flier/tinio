//! The audit walk: the on-disk names at **any** depth, and the link entries.
//!
//! Split out of `checks.rs` (2026-09-23, code-review round): the walk is a self-contained
//! `read_dir` traversal with rules of its own — every entry typed without following the link, the
//! root's `.tinio` skipped whole, no depth limit — and the validators it feeds. `checks.rs` is the
//! check list that calls it; the check list owns *which* checks run, this module owns how the two
//! name-shaped ones walk.

use std::{fs, path::Path};

use tokio::task::spawn_blocking;

use crate::{
    _core::{bucket, object},
    _fs::is_symlink_or_reparse,
    report::{Finding, Severity},
};

/// The audit walk: the on-disk names at **any** depth, and the link entries, run off the
/// runtime's workers.
///
/// The walk is this check's own because the crate's enumerators skip exactly what it must
/// report: `bucket_names` `continue`s past an invalid name, and the listing walk skips a
/// `.tinio` segment at any depth and skips link entries while `follow_symlinks` is disabled.
/// One `spawn_blocking` for the whole traversal — not one per entry — because it is a
/// synchronous `std::fs` loop inside an async command that also drives `tokio::fs` work on
/// the same pool.
pub(super) async fn walk_findings(root: &Path, follow_symlinks: bool) -> Vec<Finding> {
    let walked = root.to_path_buf();
    let named = walked.clone();
    match spawn_blocking(move || walk_audit(&walked)).await {
        Ok(walk) => {
            let mut findings = Vec::with_capacity(3);
            if let Some(finding) = inspection_finding(&walk, named.as_path()) {
                findings.push(finding);
            }
            findings.push(name_finding(walk.invalid_names, walk.invalid_count));
            findings.push(symlink_finding(
                walk.symlinks,
                walk.symlinks_count,
                follow_symlinks,
            ));
            findings
        }
        // A panicking walk is a failure of this process, not a fact about the root: the row
        // says the audit did not run rather than pretending it found nothing, and the symlink
        // leg says the same instead of disappearing with it.
        Err(err) => vec![
            Finding::check(
                "on-disk names",
                Severity::Error,
                format!("the audit walk did not run: {err}"),
            ),
            Finding::check(
                "symlinks",
                Severity::Warn,
                format!("not inspected: the audit walk did not run: {err}"),
            ),
        ],
    }
}

/// The count-and-sample detail the walk's two rows share: the total, the capped sample beside
/// it, and "and N more" for what the sample left out.
///
/// Keyed on the total, never on the sample: a capped collector can hold no entry while the
/// total is non-zero, and "0 …: " is the row that would render then. "and N more" only makes
/// sense beside a sample — with nothing shown, the count is the whole row.
fn sampled_detail(total: usize, samples: &[String], subject: &str) -> String {
    let mut detail = if samples.is_empty() {
        format!("{total} {subject}")
    } else {
        format!("{total} {subject}: {}", samples.join(", "))
    };
    if let Some(rest) = total
        .checked_sub(samples.len())
        .filter(|rest| *rest > 0 && !samples.is_empty())
    {
        detail.push_str(&format!(" and {rest} more"));
    }
    detail
}

/// Adds one sample, stopping at [`SAMPLES`]: the cap is the collector's rather than the
/// printer's, which is what bounds the memory the walk holds as well as the text it renders.
fn push_sample(samples: &mut Vec<String>, text: String) {
    if samples.len() < SAMPLES {
        samples.push(text);
    }
}

/// The name check's row: one `warn` however many names it finds, because the finding is what
/// is read and the severity is what the exit code follows. The collector caps the sample at
/// [`SAMPLES`] and counts the rest, which bounds the text *and* the memory.
pub(crate) fn name_finding(samples: Vec<String>, total: usize) -> Finding {
    if total == 0 {
        return Finding::check(
            "on-disk names",
            Severity::Ok,
            "every bucket name and object key is valid",
        );
    }
    Finding::check(
        "on-disk names",
        Severity::Warn,
        sampled_detail(total, &samples, "invalid name(s)"),
    )
}

/// The symlink row: the link entries present while following is disabled, which the storage
/// does not serve. `total` is the count of every reparse-point entry the walk saw — the
/// sample is capped at [`SAMPLES`], the magnitude is not.
pub(crate) fn symlink_finding(
    samples: Vec<String>,
    total: usize,
    follow_symlinks: bool,
) -> Finding {
    // The contract's check is "link entries present while `follow_symlinks` is disabled"
    // (the doctor section's symlink-check bullet): with following enabled the storage serves those links, so the
    // row is `ok` and says so rather than warning about what the server does on purpose.
    if follow_symlinks {
        return Finding::check(
            "symlinks",
            Severity::Ok,
            format!("following is enabled; {total} link entr(ies) are served"),
        );
    }
    if total == 0 {
        return Finding::check("symlinks", Severity::Ok, "no link entries present");
    }
    Finding::check(
        "symlinks",
        Severity::Warn,
        sampled_detail(
            total,
            &samples,
            "link entr(ies) present while `follow_symlinks` is disabled",
        ),
    )
}

/// The collected observations of one walk: the sample of offending names (capped), how many
/// there were in total, and the sample of link entries (capped).
#[derive(Debug, Default)]
pub(crate) struct Walk {
    /// The first [`SAMPLES`] offending names, in walk order.
    pub(crate) invalid_names: Vec<String>,
    /// How many offending names there were, sampled or not.
    pub(crate) invalid_count: usize,
    /// The first [`SAMPLES`] link entries, in walk order.
    pub(crate) symlinks: Vec<String>,
    /// How many link entries there were, sampled or not.
    pub(crate) symlinks_count: usize,
    /// Directories whose entries could not be read at all.
    pub(crate) unreadable_dirs: usize,
    /// Entries the walk could not place: a failed metadata probe, a directory-entry error
    /// mid-iteration, or a path that would not strip the root prefix.
    pub(crate) unreadable_entries: usize,
}

/// How many offenders the report samples: one cap bounds both the text and the memory the walk
/// holds, because the collector stops here rather than the printer.
pub(crate) const SAMPLES: usize = 20;

/// The text a name is judged on **on this platform**: Windows' `\\` separator becomes `/`
/// (the key spelling the validators and the wire use), while unix keeps the path verbatim — a
/// backslash is a legal filename byte there, and rewriting it would judge a different name
/// than the disk holds.
pub(crate) fn name_text(relative: &Path) -> String {
    let text = relative.to_string_lossy();
    #[cfg(windows)]
    {
        text.replace('\\', "/")
    }
    #[cfg(not(windows))]
    {
        text.into_owned()
    }
}

/// The never-silent row for a walk that could not read everything it enumerated: `None` when
/// it read every entry, a `warn` "not inspected" otherwise. A name check that skipped
/// directories must never read as a clean bill (the plan's "a leg that does not run is never
/// silent"); the row's name is the name check's, because that is the leg the skips belong to.
pub(crate) fn inspection_finding(walk: &Walk, root: &Path) -> Option<Finding> {
    let skipped = walk.unreadable_dirs + walk.unreadable_entries;
    (skipped > 0).then(|| {
        Finding::check(
            "on-disk names",
            Severity::Warn,
            format!(
                "not inspected: {} directories and {} entries under {} could not be read",
                walk.unreadable_dirs,
                walk.unreadable_entries,
                root.display()
            ),
        )
    })
}

/// The traversal itself: `read_dir` only, every entry typed **without following the link**,
/// the root's own `.tinio` skipped whole, and no depth limit — the any-depth rule is the
/// contract's.
///
/// A directory's name is not judged where it could be either a bucket or an intermediate key
/// segment: directories are descended into, and every **non-directory** entry's path words —
/// the bucket name at depth 1, the full object key below — go through `tinio_core`'s
/// validators, so "what a legal name is" keeps its one owner.
pub(crate) fn walk_audit(root: &Path) -> Walk {
    let mut walk = Walk::default();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            // A directory the process cannot open is a *skipped* leg, not a clean one: the
            // count is what `inspection_finding` turns into the "not inspected" row.
            Err(_) => {
                walk.unreadable_dirs += 1;
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    walk.unreadable_entries += 1;
                    continue;
                }
            };
            let path = entry.path();
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(_) => {
                    walk.unreadable_entries += 1;
                    continue;
                }
            };
            let relative = match path.strip_prefix(root) {
                Ok(relative) => relative.to_path_buf(),
                Err(_) => {
                    walk.unreadable_entries += 1;
                    continue;
                }
            };
            let text = name_text(&relative);
            if is_symlink_or_reparse(&metadata) {
                walk.symlinks_count += 1;
                push_sample(&mut walk.symlinks, text);
                continue;
            }
            if metadata.is_dir() {
                // The root's own reserved directory is ours and is never walked into; a
                // nested one is exactly the case the any-depth rule exists for, and its
                // entries are judged below like any other.
                if relative == Path::new(object::RESERVED_SEGMENT) {
                    continue;
                }
                stack.push(path);
            }
            if let Some(problem) = name_problem(&relative, &text) {
                walk.invalid_count += 1;
                push_sample(&mut walk.invalid_names, problem);
            }
        }
    }
    walk
}

/// Why `relative` is not a legal on-disk name, `None` when it is: the bucket rule at depth 1,
/// and the object-key rule — including any-depth `.tinio` segments — below it.
///
/// **The key rule is given the key, not the path.** A request's key is what follows the bucket —
/// the router splits the first segment off before the backend validates it
/// (`tinio-server`'s `key()` runs on `req.input.key`) — so handing the bucket-inclusive path to
/// `object::key` would judge a string no request ever carries, and the rules it would miss are
/// exactly the **start-anchored** ones: a leading bucket segment hides the key's first byte from
/// the drive-letter and absolute-path rules. `C:foo` is refused as a key; `my-bucket/C:foo` is
/// not refused as a path, which is how a root could hold a file the API can never address while
/// this leg reported it clean.
pub(crate) fn name_problem(relative: &Path, text: &str) -> Option<String> {
    if relative.components().count() == 1 {
        let name = text.to_string();
        if object::is_reserved_key(&name) {
            return Some(format!("`{name}` is the reserved segment"));
        }
        return bucket::name(name).err().map(|err| err.to_string());
    }
    if object::is_reserved_key(text) {
        return Some(format!("`{text}` holds a reserved segment"));
    }
    // `text` is `/`-separated on every platform (`name_text` normalises Windows' `\`), so the
    // first split is the bucket boundary. The reserved-segment check above stays on the whole
    // path, which is equivalent: it matches a segment at any depth.
    let key = text.split_once('/').map_or(text, |(_, key)| key);
    object::key(key.to_string())
        .err()
        .map(|err| err.to_string())
}
