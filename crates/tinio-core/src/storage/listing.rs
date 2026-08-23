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
    // Same loop as `group_and_paginate_ordered` with the order = key —
    // implemented separately because the borrowed key cannot be returned
    // through the generic `order_of: Fn(&T) -> O` (an owned `O` would
    // allocate per item). Here the only allocation is the emitted
    // marker's key copy (at most one page).
    let mut keys = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut last_prefix: Option<String> = None;
    let mut count = 0usize;
    let mut last_emitted: Option<String> = None;
    if max == 0 {
        return (keys, common_prefixes, false, None);
    }
    for item in items {
        let key = key_of(&item);
        // The key drives delimiter grouping only — a delimiter-less
        // caller never materializes it (objects compare by key).
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
                (String::new(), false)
            }
        };
        // The marker is exclusive-after: rolled-up entries compare with
        // their prefix string, objects with their key.
        let compare = if entry.1 {
            entry.0.as_str()
        } else {
            key
        };
        if marker.is_some_and(|after| compare <= after) {
            continue;
        }
        if count == max {
            return (keys, common_prefixes, true, last_emitted);
        }
        if entry.1 {
            last_emitted = Some(entry.0.clone());
            common_prefixes.push(entry.0);
        } else {
            last_emitted = Some(key.to_string());
            keys.push(item);
        }
        count += 1;
    }
    (keys, common_prefixes, false, None)
}

/// The composite `key\0upload_id` order of a multipart-upload listing.
///
/// The composite lets a page resume inside a same-key group (S3
/// `upload-id-marker`); it is well-defined because keys never contain
/// `\0` (control characters are rejected at construction).
pub fn uploads_order(key: &str, upload_id: &str) -> String {
    format!("{key}\0{upload_id}")
}

/// Split a composite uploads-order marker back into its key and
/// upload-id halves (`None` upload id for an object-only marker).
pub fn split_uploads_order(order: &str) -> (&str, Option<&str>) {
    match order.split_once('\0') {
        Some((key, upload_id)) => (key, Some(upload_id)),
        None => (order, None),
    }
}

/// Pure order pagination, without prefix/delimiter grouping: keep only
/// items whose order is strictly after `marker` (exclusive), stop one
/// probe item past the page (the truncation flag), and return the resume
/// marker (the order of the last item of the page when truncated).
/// `max = 0` requests nothing — and no marker either (an exclusive-after
/// marker would skip the first item of the next page forever). `items`
/// must be ordered by `order_of`. The shared ListParts pagination (part
/// numbers) of both backends.
pub fn paginate_ordered<T, O: Ord>(
    items: impl IntoIterator<Item = T>,
    marker: Option<&O>,
    max: usize,
    order_of: impl Fn(&T) -> O,
) -> (Vec<T>, bool, Option<O>) {
    let mut page = Vec::new();
    let mut last: Option<O> = None;
    if max == 0 {
        return (page, false, None);
    }
    for item in items {
        let order = order_of(&item);
        if marker.is_some_and(|after| order <= *after) {
            continue;
        }
        if page.len() == max {
            return (page, true, last);
        }
        last = Some(order);
        page.push(item);
    }
    (page, false, None)
}

/// Like [`group_and_paginate`], but the marker comparison and the returned
/// resume marker use `order_of` instead of the key — e.g. a composite
/// `key\0upload_id` for multipart-upload listings, where the key alone
/// cannot position a page inside a same-key group. Prefix/delimiter
/// grouping still uses the key. `items` must be ordered by `order_of`
/// (lexicographic).
///
/// A resume marker is only meaningful when it came from this engine's own
/// output — the last emitted object order or rolled-up prefix. A
/// client-supplied marker positioned *inside* a rollup (e.g.
/// `key-marker=dir/b.txt` on a `delimiter=/` listing) falls within an
/// already-emitted prefix: the entries still sorting after it are absorbed
/// by the rollup, so the page can legitimately come back empty and
/// untruncated.
pub fn group_and_paginate_ordered<T>(
    items: impl IntoIterator<Item = T>,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max: usize,
    key_of: impl Fn(&T) -> &str,
    order_of: impl Fn(&T) -> String,
) -> (Vec<T>, Vec<String>, bool, Option<String>) {
    // Merge objects and common prefixes into one lexicographic stream.
    // S3 `MaxKeys` counts both; the continuation token is the last
    // returned entry (object order or prefix), exclusive for the next
    // page.
    let mut keys = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut last_prefix: Option<String> = None;
    let mut count = 0usize;
    let mut last_emitted: Option<String> = None;

    if max == 0 {
        // Nothing was requested and nothing emitted: no resume marker
        // either — an exclusive-after marker would skip the first item
        // of the next page.
        return (keys, common_prefixes, false, None);
    }
    for item in items {
        // The key drives delimiter grouping only — a delimiter-less
        // caller never materializes it (objects compare by order).
        let entry = match delimiter.and_then(|delim| common_prefix(key_of(&item), prefix, delim)) {
            Some(cp) => {
                if last_prefix.as_deref() == Some(cp) {
                    continue;
                }
                last_prefix = Some(cp.to_string());
                (cp.to_string(), true)
            }
            None => {
                last_prefix = None;
                (String::new(), false)
            }
        };
        // The marker is exclusive-after: rolled-up entries compare with
        // their prefix string, objects with their order (the composite
        // `key\0upload_id` for uploads). The order is only meaningful
        // for objects — rolled-up prefixes never allocate it.
        let order = (!entry.1).then(|| order_of(&item));
        let compare = match &order {
            Some(order) => order.as_str(),
            None => entry.0.as_str(),
        };
        if marker.is_some_and(|after| compare <= after) {
            continue;
        }
        if count == max {
            return (keys, common_prefixes, true, last_emitted);
        }
        if entry.1 {
            last_emitted = Some(entry.0.clone());
            common_prefixes.push(entry.0);
        } else {
            last_emitted = order;
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
    fn paginate_ordered_by_numbers() {
        // The ListParts shape: u32 orders, exclusive-after marker,
        // one probe past the page, resume marker on truncation.
        let items = [3u32, 5, 7, 9];
        let (page, truncated, next) = paginate_ordered(items, None, 2, |n| *n);
        assert_eq!(page, [3, 5]);
        assert!(truncated);
        assert_eq!(next, Some(5));

        let (page, truncated, next) = paginate_ordered(items, next.as_ref(), 2, |n| *n);
        assert_eq!(page, [7, 9]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn paginate_ordered_marker_skips_do_not_count() {
        let items = [1u32, 2, 3, 4];
        let (page, truncated, next) = paginate_ordered(items, Some(&2), 2, |n| *n);
        assert_eq!(page, [3, 4]);
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn paginate_ordered_zero_max_is_empty_untruncated() {
        let items = [1u32, 2];
        let (page, truncated, next) = paginate_ordered(items, None, 0, |n| *n);
        assert!(page.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
    }

    #[test]
    fn ordered_pagination_resumes_inside_a_same_key_group() {
        // The composite `key\0upload_id` order positions a page precisely
        // between two uploads of the same key.
        let items = [("k", "u1"), ("k", "u2"), ("z", "u3")];
        let page1 = group_and_paginate_ordered(
            items,
            "",
            None,
            None,
            1,
            |(k, _)| *k,
            |(k, u)| format!("{k}\0{u}"),
        );
        assert_eq!(page1.0, [("k", "u1")]);
        assert!(page1.2);
        assert_eq!(page1.3.as_deref(), Some("k\0u1"));

        let page2 = group_and_paginate_ordered(
            items,
            "",
            None,
            page1.3.as_deref(),
            10,
            |(k, _)| *k,
            |(k, u)| format!("{k}\0{u}"),
        );
        assert_eq!(page2.0, [("k", "u2"), ("z", "u3")]);
        assert!(!page2.2);
        assert_eq!(page2.3, None);
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
    fn zero_max_returns_an_empty_untruncated_page() {
        // `max = 0` must not emit a resume marker — an exclusive-after
        // marker would skip the first item of the next page forever.
        let (keys, prefixes, truncated, next) = paginate(&["a.txt", "b.txt"], "", None, None, 0);
        assert!(keys.is_empty() && prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);

        let (keys, prefixes, truncated, next) =
            paginate(&["dir/a", "z.txt"], "", Some("/"), None, 0);
        assert!(keys.is_empty() && prefixes.is_empty());
        assert!(!truncated);
        assert_eq!(next, None);
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
