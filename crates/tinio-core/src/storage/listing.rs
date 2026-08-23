//! Shared S3 listing pagination and delimiter grouping.

/// Group, filter, and paginate a key-sorted item stream — the shared S3
/// listing engine for both object and multipart-upload listings.
///
/// `items` must be ordered by key (backends sort their scans, or yield an
/// already-ordered cursor). Key-sorted input makes delimiter rollups
/// contiguous, so common prefixes deduplicate against the last one in O(1)
/// and a prefix is emitted at the first key that rolls up to it. Keys
/// strictly after `marker` (exclusive) are kept; S3 `MaxKeys` counts both
/// objects and common prefixes. The scan stops after one probe entry past
/// the page (to set the truncation flag) and does not consume the rest of
/// `items`. Returns the page, the rolled-up prefixes, the truncation flag,
/// and the resume marker (the last key of the page when truncated).
///
/// # Examples
///
/// ```rust
/// use tinio_core::storage::group_and_paginate;
///
/// let items = vec!["a.txt".to_string(), "dir/x".to_string()];
/// let (keys, prefixes, truncated, next) = group_and_paginate(
///     items,
///     "",
///     Some("/"),
///     None,
///     1000,
///     |k| k.as_str(),
/// );
/// assert_eq!(keys, ["a.txt"]);
/// assert_eq!(prefixes, ["dir/"]);
/// assert!(!truncated);
/// assert_eq!(next, None);
/// ```
pub fn group_and_paginate<T>(
    items: impl IntoIterator<Item = T>,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max: usize,
    key_of: impl Fn(&T) -> &str,
) -> (Vec<T>, Vec<String>, bool, Option<String>) {
    // Merge objects and common prefixes into one lexicographic stream.
    // S3 `MaxKeys` counts both; the continuation token is the last returned
    // entry (object key or prefix), exclusive for the next page.
    let mut keys = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut last_prefix: Option<String> = None;
    let mut count = 0usize;
    let mut last_emitted: Option<String> = None;

    for item in items {
        let key = key_of(&item);
        let entry = match delimiter.and_then(|delim| common_prefix(key, prefix, delim)) {
            Some(cp) => {
                if last_prefix.as_deref() == Some(cp) {
                    continue;
                }
                last_prefix = Some(cp.to_string());
                (cp.to_string(), true)
            }
            None => {
                last_prefix = None;
                (key.to_string(), false)
            }
        };
        if marker.is_some_and(|after| entry.0.as_str() <= after) {
            continue;
        }
        if count == max {
            let next = if max == 0 {
                Some(entry.0)
            } else {
                last_emitted
            };
            return (keys, common_prefixes, true, next);
        }
        last_emitted = Some(entry.0.clone());
        if entry.1 {
            common_prefixes.push(entry.0);
        } else {
            keys.push(item);
        }
        count += 1;
    }
    (keys, common_prefixes, false, None)
}

/// The rolled-up common prefix (`prefix + head + delim`) when `key` groups
/// under `delimiter`, `None` otherwise.
fn common_prefix<'a>(key: &'a str, prefix: &str, delimiter: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    let (head, _) = rest.split_once(delimiter)?;
    Some(&key[..prefix.len() + head.len() + delimiter.len()])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paginate(
        items: &[&str],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
        max: usize,
    ) -> (Vec<String>, Vec<String>, bool, Option<String>) {
        group_and_paginate(
            items.iter().map(|s| (*s).to_string()),
            prefix,
            delim,
            marker,
            max,
            String::as_str,
        )
    }

    fn pulled(
        items: &[&str],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
        max: usize,
    ) -> (Vec<String>, Vec<String>, bool, Option<String>, usize) {
        let mut n = 0usize;
        let iter = items.iter().map(|s| (*s).to_string()).inspect(|_| n += 1);
        let (keys, prefixes, truncated, next) =
            group_and_paginate(iter, prefix, delim, marker, max, String::as_str);
        (keys, prefixes, truncated, next, n)
    }

    #[test]
    fn empty_input_is_not_truncated() {
        let (keys, prefixes, truncated, next) = paginate(&[], "", None, None, 10);
        assert!(keys.is_empty());
        assert!(prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn exact_fill_is_not_truncated() {
        let (keys, prefixes, truncated, next) = paginate(&["a.txt", "b.txt"], "", None, None, 2);
        assert_eq!(keys, ["a.txt", "b.txt"]);
        assert!(prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn no_delimiter_marker_is_exclusive_and_paginates() {
        let items = ["a.txt", "b.txt", "c.txt", "d.txt"];
        let (keys, prefixes, truncated, next) = paginate(&items, "", None, Some("a.txt"), 2);
        assert_eq!(keys, ["b.txt", "c.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("c.txt"));

        let (keys, prefixes, truncated, next) = paginate(&items, "", None, Some("c.txt"), 2);
        assert_eq!(keys, ["d.txt"]);
        assert!(prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn marker_equal_to_last_key_yields_empty() {
        let (keys, prefixes, truncated, next) =
            paginate(&["a.txt", "b.txt"], "", None, Some("b.txt"), 1000);
        assert!(keys.is_empty());
        assert!(prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn max_zero_resume_is_the_first_grouped_entry() {
        let (keys, prefixes, truncated, next) = paginate(&["a.txt", "b.txt"], "", None, None, 0);
        assert!(keys.is_empty() && prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));

        let (keys, prefixes, truncated, next) =
            paginate(&["dir/a", "z.txt"], "", Some("/"), None, 0);
        assert!(keys.is_empty() && prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("dir/"));
    }

    #[test]
    fn max_zero_on_empty_is_not_truncated() {
        let (keys, prefixes, truncated, next) = paginate(&[], "", None, None, 0);
        assert!(keys.is_empty() && prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn streams_and_stops_after_one_probe_past_the_page() {
        let (keys, prefixes, truncated, next, n) =
            pulled(&["a.txt", "b.txt", "c.txt", "d.txt"], "", None, None, 2);
        assert_eq!(keys, ["a.txt", "b.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b.txt"));
        assert_eq!(n, 3, "one probe key past the page, nothing further");
    }

    #[test]
    fn marker_skips_do_not_count_toward_max() {
        let (keys, _, truncated, next, n) = pulled(
            &["a.txt", "b.txt", "c.txt", "d.txt"],
            "",
            None,
            Some("a.txt"),
            2,
        );
        assert_eq!(keys, ["b.txt", "c.txt"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("c.txt"));
        assert_eq!(n, 4, "skipped marker key plus page plus one probe");
    }

    #[test]
    fn delimiter_listing_counts_prefixes_toward_max_and_paginates() {
        let items = ["a.txt", "b.txt", "dir/c.txt"];
        let (keys, prefixes, truncated, next) = paginate(&items, "", Some("/"), None, 1);
        assert_eq!(keys, ["a.txt"]);
        assert!(
            prefixes.is_empty(),
            "common prefixes must not leak onto every page: {prefixes:?}"
        );
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));

        let (keys, prefixes, truncated, next) = paginate(&items, "", Some("/"), Some("a.txt"), 1);
        assert_eq!(keys, ["b.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b.txt"));

        let (keys, prefixes, truncated, next) = paginate(&items, "", Some("/"), Some("b.txt"), 1);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["dir/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn delimiter_keeps_scanning_a_rollup_until_the_next_entry() {
        let (keys, prefixes, truncated, next, n) = pulled(
            &["dir/a", "dir/b", "dir/c", "z.txt"],
            "",
            Some("/"),
            None,
            1,
        );
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["dir/"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("dir/"));
        assert_eq!(
            n, 4,
            "keys under the same prefix collapse to one entry; the next key is the truncation probe"
        );
    }

    #[test]
    fn start_after_a_common_prefix_does_not_reemit_it() {
        let (keys, prefixes, truncated, next) = paginate(
            &["dir/a", "dir/b", "z.txt"],
            "",
            Some("/"),
            Some("dir/"),
            1000,
        );
        assert_eq!(keys, ["z.txt"]);
        assert!(
            prefixes.is_empty(),
            "resuming after a rolled-up prefix must not emit it again: {prefixes:?}"
        );
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn object_marker_inside_a_rollup_skips_the_whole_prefix() {
        // "dir/" < "dir/c.txt", so the rolled-up listing entry is not > marker.
        let (keys, prefixes, truncated, next) = paginate(
            &["dir/a.txt", "dir/c.txt", "dir/e.txt", "z.txt"],
            "",
            Some("/"),
            Some("dir/c.txt"),
            1000,
        );
        assert_eq!(keys, ["z.txt"]);
        assert!(
            prefixes.is_empty(),
            "a raw-key take(max) after start_after would re-emit dir/: {prefixes:?}"
        );
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn two_common_prefixes_are_separate_entries() {
        let (keys, prefixes, truncated, next) =
            paginate(&["a/x", "b/y", "c.txt"], "", Some("/"), None, 2);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["a/", "b/"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b/"));

        let (keys, prefixes, truncated, next) =
            paginate(&["a/x", "b/y", "c.txt"], "", Some("/"), Some("b/"), 1000);
        assert_eq!(keys, ["c.txt"]);
        assert!(prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn object_between_prefixes_resets_rollup() {
        let (keys, prefixes, truncated, next) =
            paginate(&["a/x", "mid.txt", "z/y"], "", Some("/"), None, 1000);
        assert_eq!(keys, ["mid.txt"]);
        assert_eq!(prefixes, ["a/", "z/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn nested_delimiter_under_prefix() {
        let (keys, prefixes, truncated, next) = paginate(
            &["dir/a.txt", "dir/sub/b.txt", "dir/sub/c.txt"],
            "dir/",
            Some("/"),
            None,
            1000,
        );
        assert_eq!(keys, ["dir/a.txt"]);
        assert_eq!(prefixes, ["dir/sub/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn key_without_delimiter_after_prefix_stays_an_object() {
        let (keys, prefixes, truncated, next) =
            paginate(&["dir/a.txt", "dirfile"], "dir", Some("/"), None, 1000);
        assert_eq!(keys, ["dirfile"]);
        assert_eq!(prefixes, ["dir/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }
}
