//! Shared S3 listing pagination and delimiter grouping.

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

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
/// let (keys, prefixes, truncated, next) =
///     group_and_paginate(items, "", Some("/"), None, 1000, |k| k.as_str());
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
    // allocate per item). Per emitted item the only allocation is the
    // rolled-up prefix string — one `String` per new group, shared by
    // the dedup marker and the emitted entry; the truncation marker is
    // recomputed from the page once, at the probe.
    let mut keys = Vec::with_capacity(max.min(1024));
    let mut common_prefixes = Vec::with_capacity(max.min(1024));
    let mut rollup = RollupMirror::new();
    let mut count = 0usize;
    let mut last_was_prefix = false;
    if max == 0 {
        return (keys, common_prefixes, false, None);
    }
    for item in items {
        let key = key_of(&item);
        // The key drives delimiter grouping only — a delimiter-less
        // caller never materializes it (objects compare by key).
        let entry = match delimiter.and_then(|delim| common_prefix(key, prefix, delim)) {
            Some(cp) => {
                if rollup.is_rolled(cp) {
                    continue;
                }
                let cp = cp.to_string();
                rollup.record_rollup(&cp);
                (cp, true)
            }
            None => {
                rollup.reset();
                (String::new(), false)
            }
        };
        // The marker is exclusive-after: rolled-up entries compare with
        // their prefix string, objects with their key.
        let compare = if entry.1 { entry.0.as_str() } else { key };
        if marker.is_some_and(|after| compare <= after) {
            continue;
        }
        if count == max {
            // The resume marker is the last emitted entry: the last
            // page key, or the last rolled-up prefix.
            let next = if last_was_prefix {
                common_prefixes.last().cloned()
            } else {
                keys.last().map(|item| key_of(item).to_string())
            };
            return (keys, common_prefixes, true, next);
        }
        if entry.1 {
            common_prefixes.push(entry.0);
        } else {
            keys.push(item);
        }
        last_was_prefix = entry.1;
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

/// The composite page-resume marker of an S3 ListMultipartUploads
/// listing from its wire halves (`key-marker` plus the optional
/// `upload-id-marker`). A bare key marker skips the whole key group
/// (S3: only keys strictly greater than `key-marker` are listed) — the
/// sentinel upload id sorts after every real one (upload ids never
/// contain `\0`, and `\u{10FFFF}` is beyond any legal upload-id
/// character). One home for the conversion, shared by both backends.
pub fn key_marker_order(
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
) -> Option<String> {
    match (key_marker, upload_id_marker) {
        (Some(key), Some(upload_id)) => Some(uploads_order(key, upload_id)),
        (Some(key), None) => Some(uploads_order(key, "\u{10FFFF}")),
        _ => None,
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
    // page. Per emitted item the only allocations are the order string —
    // required by the marker comparison for objects — and the rolled-up
    // prefix string, one per new group and shared by the dedup marker
    // and the emitted entry; the truncation marker is recomputed from
    // the page once, at the probe.
    let mut keys = Vec::with_capacity(max.min(1024));
    let mut common_prefixes = Vec::with_capacity(max.min(1024));
    let mut rollup = RollupMirror::new();
    let mut count = 0usize;
    let mut last_was_prefix = false;

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
                if rollup.is_rolled(cp) {
                    continue;
                }
                let cp = cp.to_string();
                rollup.record_rollup(&cp);
                (cp, true)
            }
            None => {
                rollup.reset();
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
            // The resume marker is the last emitted entry: the last
            // page order, or the last rolled-up prefix.
            let next = if last_was_prefix {
                common_prefixes.last().cloned()
            } else {
                keys.last().map(&order_of)
            };
            return (keys, common_prefixes, true, next);
        }
        if entry.1 {
            common_prefixes.push(entry.0);
        } else {
            keys.push(item);
        }
        last_was_prefix = entry.1;
        count += 1;
    }
    (keys, common_prefixes, false, None)
}

/// Like [`group_and_paginate_ordered`], but over an **unordered** item
/// stream (the fs backend's bounded-memory uploads page, item 7e): every
/// item is examined, but only the page is held in memory — a max-heap
/// keeps the `max + 1` smallest **distinct** entries after the marker
/// (entries = the rolled-up prefix string for delimiter groups —
/// deduplicated against the heap — or the composite order). The heap's
/// `max + 1`-th entry is the truncation probe; the resume marker is the
/// last entry of the page — page size, marker, and resume semantics
/// identical to the ordered engine over a key-sorted stream (pinned by
/// the equality matrix test).
///
/// The rollup dedup is against prefixes currently in the heap (an
/// evicted prefix is re-offered by a later row of the same group — one
/// entry either way). A marker positioned *inside* a rolled-up prefix
/// skips the whole group identically in both engines: a rollup row's
/// order IS the prefix string, so `"dir/" <= marker` — the page
/// legitimately comes back empty and untruncated (the ordered engine's
/// documented resume semantics).
///
/// This function form consumes the whole stream at once. The stateful
/// [`UnorderedPager`] is the incremental form for async producers: an
/// item stream fed in as it is produced (e.g. an fs dirent walk) is
/// offered item by item and the page is yielded at the end, without
/// materializing the scan.
pub fn group_and_paginate_unordered<T>(
    items: impl IntoIterator<Item = T>,
    prefix: &str,
    delimiter: Option<&str>,
    marker: Option<&str>,
    max: usize,
    key_of: impl Fn(&T) -> &str,
    order_of: impl Fn(&T) -> String,
) -> (Vec<T>, Vec<String>, bool, Option<String>) {
    // Thin wrapper over the stateful pager — the same loop, with the
    // stream offered item by item.
    let mut pager = UnorderedPager::new(prefix, delimiter, marker, max, key_of);
    for item in items {
        pager.offer(item, &order_of);
    }
    pager.finish()
}

/// The stateful, incremental form of [`group_and_paginate_unordered`]:
/// [`Self::offer`](UnorderedPager::offer) accepts one item of the stream at a
/// time, [`finish`](UnorderedPager::finish) yields the page — the fs
/// backend feeds its async dirent walks through it item by item instead
/// of materializing and sorting the whole set. Semantics are exactly the
/// function form's: `max = 0` short-circuits (`offer` is a no-op and
/// `finish` returns an empty, untruncated page with no marker), the
/// marker is exclusive-after, delimiter rollups deduplicate against the
/// heap, and the page is the `max` smallest distinct entries with the
/// `max + 1`-th as the truncation probe. The `prefix`/`delimiter`/
/// `marker` arguments are cloned into the pager in `new`, so it can
/// outlive its call frame (across awaits).
pub struct UnorderedPager<T, F> {
    prefix: String,
    delimiter: Option<String>,
    marker: Option<String>,
    max: usize,
    key_of: F,
    heap: BinaryHeap<HeapEntry<T>>,
    heap_prefixes: HashSet<String>,
}

impl<T, F> UnorderedPager<T, F>
where
    F: for<'a> Fn(&'a T) -> &'a str, // key_of — same bound as the function form
{
    /// Start a page with the same parameters as
    /// [`group_and_paginate_unordered`]. The composite order of a plain
    /// row is supplied per [`Self::offer`] call — the pager holds only the
    /// borrowed-key `key_of` (an `order_of` held in the constructor
    /// would be dead weight for every `offer_keyed`-only caller).
    pub fn new(
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max: usize,
        key_of: F,
    ) -> Self {
        Self {
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            marker: marker.map(str::to_string),
            max,
            key_of,
            heap: BinaryHeap::with_capacity(max.saturating_add(1).min(1024)),
            heap_prefixes: HashSet::new(),
        }
    }

    /// Offer one item of the stream. A no-op when `max = 0`. `order_of`
    /// builds the composite order of a plain row (a delimiter-less
    /// caller passes `|item| key_of(item).to_string()`); a rollup row
    /// orders by its prefix string, which stays borrowed through the
    /// rejection checks and is materialized only at admission.
    pub fn offer(&mut self, item: T, order_of: impl Fn(&T) -> String) {
        if self.max == 0 {
            return;
        }
        // The key drives delimiter grouping only — a delimiter-less
        // caller never materializes it (objects compare by order).
        let key = (self.key_of)(&item);
        let cp = self
            .delimiter
            .as_deref()
            .and_then(|delim| common_prefix(key, &self.prefix, delim));
        // The comparison view: the rolled-up prefix (borrowed), or the
        // composite order. A plain row's order is owned — only the
        // caller can build it, so it is materialized up front; a rollup
        // row's prefix stays borrowed through the checks.
        let composite = match cp {
            Some(_) => None,
            None => Some(order_of(&item)),
        };
        let view: &str = match composite {
            Some(ref order) => order,
            None => cp.expect("a rollup row borrows its prefix"),
        };
        // Exclusive-after marker — BEFORE the rollup dedup (see the
        // divergence note on the function: a marker-skipped prefix is
        // not in the heap, so a later row of the same group re-offers
        // it).
        if self.marker.as_deref().is_some_and(|after| view <= after) {
            return;
        }
        // The rollup is already counted while it is in the heap (an
        // evicted prefix is offered again — one entry either way).
        if cp.is_some() && self.heap_prefixes.contains(view) {
            return;
        }
        // The bounded-k admission: when the heap is full, the entry
        // displaces the current largest only if smaller. The `view`
        // borrow ends with the materialization below (the last use
        // before `item` moves).
        let cap = self.max.saturating_add(1);
        if heap_evict_larger(&mut self.heap, &mut self.heap_prefixes, view, cap) {
            return;
        }
        let order = match composite {
            Some(order) => order,
            None => cp.expect("a rollup row borrows its prefix").to_string(),
        };
        heap_push(
            &mut self.heap,
            &mut self.heap_prefixes,
            order,
            cp.is_none().then_some(item),
        );
    }

    /// Offer one item whose order IS its key — the keyed fast path of
    /// [`Self::offer`] for key-ordered listings (the fs object page): the
    /// marker skip, the dedup set, and the bounded-k admission compare
    /// the **borrowed** key/prefix, and the order String is materialized
    /// only for entries that actually enter the heap — a `max_keys`
    /// page of a huge bucket allocates O(page) order strings, not
    /// O(entries), and a rolled-up group's rows pay no allocation at
    /// all on the rejection paths (marker skip, already rolled up, heap
    /// full — E1). Semantics are identical to [`Self::offer`] with
    /// `order_of = |item| key_of(item).to_string()`.
    pub fn offer_keyed(&mut self, item: T) {
        if self.max == 0 {
            return;
        }
        // The key drives delimiter grouping only — a delimiter-less
        // caller never materializes it (objects compare by key).
        let key = (self.key_of)(&item);
        let cp = self
            .delimiter
            .as_deref()
            .and_then(|delim| common_prefix(key, &self.prefix, delim));
        // The comparison view: the rolled-up prefix, or the borrowed key.
        let view: &str = cp.unwrap_or(key);
        if self.marker.as_deref().is_some_and(|after| view <= after) {
            return;
        }
        if cp.is_some() && self.heap_prefixes.contains(view) {
            return;
        }
        // The bounded-k admission on the borrowed view — a rejected
        // entry (heap full and not smaller) allocates nothing. The
        // `key`/`cp` borrow ends with the materialization below (the
        // last use before `item` moves), so the admission call is the
        // only allocation this offer can pay.
        let cap = self.max.saturating_add(1);
        if heap_evict_larger(&mut self.heap, &mut self.heap_prefixes, view, cap) {
            return;
        }
        let order = match cp {
            Some(cp) => cp.to_string(),
            None => key.to_string(),
        };
        heap_push(
            &mut self.heap,
            &mut self.heap_prefixes,
            order,
            cp.is_none().then_some(item),
        );
    }

    /// Yield the page — `(keys, common_prefixes, truncated, next)` with
    /// the same semantics as [`group_and_paginate_unordered`]: the `max`
    /// smallest distinct entries after the marker, the truncation flag
    /// (a `max + 1`-th entry was offered), and the resume marker (the
    /// last entry of the page when truncated).
    pub fn finish(self) -> (Vec<T>, Vec<String>, bool, Option<String>) {
        let max = self.max;
        if max == 0 {
            // Nothing was requested and nothing emitted: no resume
            // marker either — an exclusive-after marker would skip the
            // first item of the next page.
            return (Vec::new(), Vec::new(), false, None);
        }
        // Split the heap: ascending page plus the probe entry.
        let mut sorted = self.heap.into_sorted_vec();
        let truncated = sorted.len() > max;
        if truncated {
            sorted.pop(); // the probe — beyond the page
        }
        let next = if truncated {
            sorted.last().map(|entry| entry.order.clone())
        } else {
            None
        };
        let mut keys = Vec::with_capacity(max.min(1024));
        let mut common_prefixes = Vec::with_capacity(max.min(1024));
        for entry in sorted {
            match entry.item {
                Some(item) => keys.push(item),
                None => common_prefixes.push(entry.order),
            }
        }
        (keys, common_prefixes, truncated, next)
    }
}

/// One entry of the unordered pagination's bounded max-heap: the entry's
/// order string — the composite `key\0upload_id` order for objects, the
/// rolled-up prefix for delimiter groups — plus the item when the entry
/// is an object (`None` = a common prefix). Ordered by `order`
/// (max-heap: the largest is displaced first, keeping the `max + 1`
/// smallest).
struct HeapEntry<T> {
    order: String,
    item: Option<T>,
}

impl<T> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.order == other.order
    }
}
impl<T> Eq for HeapEntry<T> {}
impl<T> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order.cmp(&other.order)
    }
}

/// The capped max-heap of the `cap` smallest entries (the standard
/// bounded-k selection — the final heap holds the `cap` smallest of
/// everything offered). `heap_prefixes` mirrors the prefixes currently
/// in the heap — the rollup dedup set: an evicted prefix is removed, so
/// a later row of the same group is offered again, which is correct (it
/// is ONE entry either way — the heap's `cap`-th slot is the truncation
/// probe, and a duplicate can only displace a distinct entry, so
/// `len == cap` still means "at least `cap` distinct entries").
/// Push an entry into the heap and mirror a rollup into the dedup set.
/// Shared by both offer paths.
fn heap_push<T>(
    heap: &mut BinaryHeap<HeapEntry<T>>,
    heap_prefixes: &mut HashSet<String>,
    order: String,
    item: Option<T>,
) {
    if item.is_none() {
        heap_prefixes.insert(order.clone());
    }
    heap.push(HeapEntry { order, item });
}

/// The bounded-k admission step shared by [`UnorderedPager::offer`] and
/// [`UnorderedPager::offer_keyed`]: when the heap is full, `view`
/// displaces the current largest only if it is smaller (the eviction
/// removes the displaced rollup from the dedup set, so a later row of
/// the same group re-offers it — one entry either way). Returns `true`
/// when the entry does NOT belong (the heap is full and `view` is not
/// smaller) — the caller then drops it without materializing its order
/// (E1). When it returns `false` there is room (or the largest was
/// evicted) and the caller pushes.
fn heap_evict_larger<T>(
    heap: &mut BinaryHeap<HeapEntry<T>>,
    heap_prefixes: &mut HashSet<String>,
    view: &str,
    cap: usize,
) -> bool {
    if heap.len() < cap {
        return false; // room — no eviction needed
    }
    if view >= heap.peek().expect("non-empty heap").order.as_str() {
        return true; // rejected — the caller drops the entry
    }
    let displaced = heap.pop().expect("non-empty heap");
    if displaced.item.is_none() {
        heap_prefixes.remove(&displaced.order);
    }
    false
}

/// The rolled-up common prefix (`prefix + head + delim`) when `key` groups
/// under `delimiter`, `None` otherwise. Public so backends implementing
/// their own in-scan pagination share the single home of the rollup rule
/// (the fs backend's bounded-memory uploads page, item 7e).
pub fn common_prefix<'a>(key: &'a str, prefix: &str, delimiter: &str) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    let (head, _) = rest.split_once(delimiter)?;
    Some(&key[..prefix.len() + head.len() + delimiter.len()])
}

/// The delimiter-group state of an ordered listing scan — one shared
/// mirror for the engines ([`group_and_paginate`],
/// [`group_and_paginate_ordered`]) and the mem `list_objects` pre-filter
/// (A4): `is_rolled` reports whether a key's rolled-up prefix was
/// already emitted, `record_rollup`/`reset` update the group. The
/// mem pre-filter's equivalence relies on the engine's ordering —
/// dedup check BEFORE the row's validation, group update AFTER — so
/// the two-phase shape (check, then record) is the contract, not a
/// convenience; a one-shot `on_rollup` would advance the group on a
/// row the caller later discards.
#[derive(Debug, Default)]
pub struct RollupMirror {
    last: Option<String>,
}

impl RollupMirror {
    /// Start with no group in progress.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `cp` is the group currently being emitted.
    pub fn is_rolled(&self, cp: &str) -> bool {
        self.last.as_deref() == Some(cp)
    }

    /// Record `cp` as the group in progress.
    pub fn record_rollup(&mut self, cp: &str) {
        self.last = Some(cp.to_string());
    }

    /// A plain key ends the group.
    pub fn reset(&mut self) {
        self.last = None;
    }
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
    fn rollup_mirror_checks_before_recording() {
        // A4: the mirror's two-phase shape is the contract — `is_rolled`
        // reports the CURRENT group without advancing it, so a caller
        // that discards a row between the check and the record (the mem
        // pre-filter's tampered-row skip) never loses the group.
        let mut mirror = RollupMirror::new();
        assert!(!mirror.is_rolled("a/"));
        assert!(!mirror.is_rolled("a/"), "a check alone must not advance");
        mirror.record_rollup("a/");
        assert!(mirror.is_rolled("a/"));
        mirror.reset();
        assert!(!mirror.is_rolled("a/"));
        // A new group replaces the old; a plain key between two groups
        // of the SAME prefix still resets (the engine emits `a/` twice).
        mirror.record_rollup("a/");
        mirror.record_rollup("b/");
        assert!(!mirror.is_rolled("a/"));
        assert!(mirror.is_rolled("b/"));
        mirror.reset();
        mirror.record_rollup("a/");
        assert!(mirror.is_rolled("a/"), "the group restarts after a reset");
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

    fn unordered(
        items: &[&str],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
        max: usize,
    ) -> (Vec<String>, Vec<String>, bool, Option<String>) {
        group_and_paginate_unordered(
            items.iter().map(|s| (*s).to_string()),
            prefix,
            delim,
            marker,
            max,
            String::as_str,
            |s| s.to_string(),
        )
    }

    fn unordered_incremental(
        items: &[&str],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
        max: usize,
    ) -> (Vec<String>, Vec<String>, bool, Option<String>) {
        // The stateful pager fed the same stream, one offer per item —
        // the matrix pins the incremental path too.
        let mut pager = UnorderedPager::new(prefix, delim, marker, max, String::as_str);
        for item in items {
            pager.offer((*item).to_string(), |s| s.to_string());
        }
        pager.finish()
    }

    fn unordered_keyed(
        items: &[&str],
        prefix: &str,
        delim: Option<&str>,
        marker: Option<&str>,
        max: usize,
    ) -> (Vec<String>, Vec<String>, bool, Option<String>) {
        // The keyed fast path (`offer_keyed` — the order IS the key):
        // must agree with the composite-order pager on every
        // combination (the matrix pins it).
        let mut pager = UnorderedPager::new(prefix, delim, marker, max, String::as_str);
        for item in items {
            pager.offer_keyed((*item).to_string());
        }
        pager.finish()
    }

    #[test]
    fn unordered_matches_ordered_on_every_combination() {
        // The unordered variant (the bounded-memory uploads page) agrees
        // with the ordered engine on every marker/rollup combination,
        // including a marker positioned INSIDE a rolled-up prefix: a
        // rollup row's order IS the prefix string, so `"dir/" <= marker`
        // skips the whole group identically in both engines (a marker
        // inside a rollup legitimately yields an empty, untruncated
        // page — the ordered engine's documented resume semantics). The
        // stateful pager (the incremental form) is pinned by the same
        // matrix.
        let items = ["a.txt", "dir/b.txt", "dir/c.txt", "dir/sub/d.txt", "z.txt"];
        let mut combos = 0usize;
        for (prefix, delim) in [
            ("", None),
            ("", Some("/")),
            ("dir/", Some("/")),
            ("z", None),
        ] {
            for marker in [
                None,
                Some("a.txt"),
                Some("dir/b.txt"),
                Some("dir/"),
                Some("z.txt"),
            ] {
                for max in [0usize, 1, 2, 3, 1000] {
                    combos += 1;
                    let ordered = paginate(&items, prefix, delim, marker, max);
                    let unordered = unordered(&items, prefix, delim, marker, max);
                    let incremental = unordered_incremental(&items, prefix, delim, marker, max);
                    let keyed = unordered_keyed(&items, prefix, delim, marker, max);
                    assert_eq!(
                        ordered, unordered,
                        "prefix={prefix:?} delim={delim:?} marker={marker:?} max={max}"
                    );
                    assert_eq!(
                        unordered, incremental,
                        "pager diverges: prefix={prefix:?} delim={delim:?} marker={marker:?} max={max}"
                    );
                    assert_eq!(
                        incremental, keyed,
                        "keyed pager diverges: prefix={prefix:?} delim={delim:?} marker={marker:?} max={max}"
                    );
                }
            }
        }
        assert_eq!(combos, 100, "the matrix ran");
    }

    #[test]
    fn unordered_matches_ordered_on_shuffled_input() {
        // T09: the equivalence matrix above feeds SORTED input only —
        // the eviction/re-offer path (a rolled-up prefix evicted from
        // the bounded heap, later re-offered by a later row of the same
        // group) needs shuffled-input coverage: the unordered engine's
        // page must be the same k smallest distinct offers regardless
        // of offer order. A deterministic xorshift PRNG (no RNG dep)
        // drives Fisher–Yates shuffles; every shuffle must agree with
        // the ordered engine over the same (sorted) multiset — marker
        // respected, no double-emitted prefixes.
        let base = [
            "a.txt",
            "b.txt",
            "dir/c.txt",
            "dir/d.txt",
            "dir/sub/e.txt",
            "z.txt",
        ];
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for round in 0..300 {
            let mut items = base.to_vec();
            for i in (1..items.len()).rev() {
                let j = (next() as usize) % (i + 1);
                items.swap(i, j);
            }
            // The ORDERED engine assumes sorted input — the shuffle is
            // compared through the sorted multiset: the unordered engine
            // over the shuffle must equal both the ordered engine and
            // itself over the sorted multiset.
            let mut sorted = items.clone();
            sorted.sort_unstable();
            for (prefix, delim) in [
                ("", None),
                ("", Some("/")),
                ("dir/", Some("/")),
                ("dir/s", Some("/")),
            ] {
                for marker in [
                    None,
                    Some("a.txt"),
                    Some("dir/c.txt"),
                    Some("dir/"),
                    Some("z.txt"),
                ] {
                    for max in [0usize, 1, 2, 3, 1000] {
                        let ordered = paginate(&sorted, prefix, delim, marker, max);
                        let unordered = unordered(&items, prefix, delim, marker, max);
                        assert_eq!(
                            ordered, unordered,
                            "round={round} shuffled input: prefix={prefix:?} delim={delim:?} \
                             marker={marker:?} max={max} items={items:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn unordered_rollup_collapses_to_one_entry_for_the_page() {
        // A rolled-up prefix is ONE entry for the page: with `max = 1`
        // the rollup fills the page and the next distinct entry is the
        // truncation probe — identical to the ordered engine.
        let items = ["a.txt", "dir/x", "dir/y", "z.txt"];
        let (keys, prefixes, truncated, next) = unordered(&items, "", Some("/"), None, 1);
        assert_eq!(keys, ["a.txt"]);
        assert_eq!(prefixes, Vec::<String>::new(), "{prefixes:?}");
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));

        // The ordered engine agrees (the rollup dedup keeps `dir/` one
        // entry; `z.txt` is the probe).
        let (keys, prefixes, truncated, next) = paginate(&items, "", Some("/"), None, 1);
        assert_eq!(keys, ["a.txt"]);
        assert!(prefixes.is_empty());
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a.txt"));
    }

    #[test]
    fn unordered_heap_displaces_a_prefix_marker() {
        // The bounded heap displaces rolled-up prefixes like objects: the
        // third distinct entry (cap = max + 1) evicts the largest — here
        // the `m/` marker — which must leave the dedup set so a later row
        // of the same group can re-offer it (one entry either way).
        let (keys, prefixes, truncated, next) =
            unordered(&["m/x", "z.txt", "a/y"], "", Some("/"), None, 1);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["a/"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a/"));

        // The evicted prefix is re-offered by a later row of its group.
        let (keys, prefixes, truncated, next) =
            unordered(&["m/x", "z.txt", "a/y", "m/late"], "", Some("/"), None, 1);
        assert!(keys.is_empty());
        assert_eq!(prefixes, ["a/"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("a/"));
    }

    #[test]
    fn unordered_pager_matches_batch_across_chunked_offers() {
        // The stateful pager accepts a stream in arbitrary chunks: split
        // offers equal the one-shot function form (the engine is
        // order-independent — every item is examined, only the page is
        // held).
        let items = ["z.txt", "a.txt", "dir/b.txt", "m/x", "dir/c.txt"];
        let batch = unordered(&items, "", Some("/"), None, 2);
        let mut pager = UnorderedPager::new("", Some("/"), None, 2, String::as_str);
        for chunk in items.chunks(2) {
            for item in chunk {
                pager.offer((*item).to_string(), |s| s.to_string());
            }
        }
        assert_eq!(pager.finish(), batch);
    }

    #[test]
    fn heap_entries_compare_by_order_alone() {
        // The heap's `PartialEq`/`Ord` ignore the payload: two entries
        // with the same order are equal regardless of their items.
        let a = HeapEntry {
            order: "k".into(),
            item: Some(1),
        };
        let b = HeapEntry {
            order: "k".into(),
            item: Some(2),
        };
        let c = HeapEntry {
            order: "z".into(),
            item: None::<i32>,
        };
        assert!(a == b);
        assert!(a != c);
        assert!(a < c);
    }
}
