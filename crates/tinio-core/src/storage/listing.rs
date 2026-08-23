//! Shared S3 listing pagination and delimiter grouping.

/// Group, filter, and paginate an already key-sorted item list — the shared
/// S3 listing engine for both object and multipart-upload listings.
///
/// `items` must be ordered by key (backends sort their scans); key-sorted
/// input makes delimiter rollups contiguous, so common prefixes deduplicate
/// against the last one in O(1). Keys strictly after `marker` (exclusive)
/// are kept, then the page is truncated to `max`. Returns the page, the
/// rolled-up prefixes, the truncation flag, and the resume marker (the last
/// key of the page when truncated).
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
    items: Vec<T>,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max: usize,
    key_of: impl Fn(&T) -> &str,
) -> (Vec<T>, Vec<String>, bool, Option<String>) {
    // Merge objects and common prefixes into one lexicographic stream.
    // S3 `MaxKeys` counts both; the continuation token is the last returned
    // entry (object key or prefix), exclusive for the next page.
    let mut entries: Vec<(String, Option<T>)> = Vec::with_capacity(items.len());
    let mut prefixes: Vec<String> = Vec::new();
    for item in items {
        let key = key_of(&item);
        if let Some(cp) = delimiter.and_then(|delim| common_prefix(key, prefix, delim)) {
            if prefixes.last().map(String::as_str) != Some(cp) {
                prefixes.push(cp.to_string());
            }
            continue;
        }
        entries.push((key.to_string(), Some(item)));
    }
    if delimiter.is_some() {
        entries.extend(prefixes.into_iter().map(|cp| (cp, None)));
        entries.sort_by(|a, b| a.0.cmp(&b.0));
    }
    if let Some(after) = marker {
        entries.retain(|(key, _)| key.as_str() > after);
    }
    let truncated = entries.len() > max;
    let next = if truncated {
        entries.get(max.saturating_sub(1)).map(|(key, _)| key.clone())
    } else {
        None
    };
    entries.truncate(max);
    let mut keys = Vec::with_capacity(entries.len());
    let mut common_prefixes = Vec::with_capacity(entries.len());
    for (key, item) in entries {
        match item {
            Some(item) => keys.push(item),
            None => common_prefixes.push(key),
        }
    }
    (keys, common_prefixes, truncated, next)
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

    #[test]
    fn delimiter_listing_counts_prefixes_toward_max_and_paginates() {
        let items = ["a.txt", "b.txt", "dir/c.txt"].map(str::to_string).to_vec();
        let (keys, prefixes, truncated, next) =
            group_and_paginate(items.clone(), "", Some("/"), None, 1, String::as_str);
        assert_eq!(keys, ["a.txt"]);
        assert!(
            prefixes.is_empty(),
            "common prefixes must not leak onto every page: {prefixes:?}"
        );
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));

        let (keys, prefixes, truncated, next) = group_and_paginate(
            items.clone(),
            "",
            Some("/"),
            Some("a.txt"),
            1,
            String::as_str,
        );
        assert_eq!(keys, ["b.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("b.txt"));

        let (keys, prefixes, truncated, next) =
            group_and_paginate(items, "", Some("/"), Some("b.txt"), 1, String::as_str);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["dir/"]);
        assert!(!truncated);
        assert_eq!(next, None);
    }
}
