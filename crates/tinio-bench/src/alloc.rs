//! The counting allocator a bench leg installs as its `#[global_allocator]`:
//! the channel of an A/B reading that the host's flush regime cannot move.
//!
//! A `GlobalAlloc` implementation is `unsafe`, and the crate whose benches need
//! the count first (`crates/tinio-fs`) sets `unsafe_code = "forbid"` in its
//! `[lints.rust]` — a `forbid` that reaches every target of the package from
//! the command line, and that an `#![allow]` inside the bench target cannot
//! lift (`E0453: allow(unsafe_code) incompatible with previous forbid`,
//! measured). So the two `unsafe` blocks live here, in the one crate whose
//! whole job is the harness, and the module's own `#![allow(unsafe_code)]` is
//! their enumeration.
//!
//! Installing it is safe code, and it is one line in a bench target:
//!
//! ```
//! use tinio_bench::CountingAlloc;
//!
//! #[global_allocator]
//! static ALLOCATOR: CountingAlloc = CountingAlloc::new();
//! ```
//!
//! Counted is every **allocation request**, not the live heap: a `Box` that is
//! created and dropped inside one operation request counts once, and the
//! default `GlobalAlloc::realloc` (allocate, copy, free) counts as one
//! allocation of the new size. `reset` starts a window; `usage` reads it.
//!
//! The counter adds two relaxed atomic increments to every allocation the
//! process makes, the harness's own included — the timed operation's *count* is
//! still exact, but a leg that allocates pays those increments inside its own
//! measurement, so an allocation-heavy leg reads a few nanoseconds slower than
//! the same code without the allocator installed.
#![allow(unsafe_code)]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

/// The allocator a bench leg installs: `System`'s behaviour, plus two counts.
pub struct CountingAlloc;

impl CountingAlloc {
    /// The one value a `#[global_allocator]` static needs.
    pub const fn new() -> Self {
        Self
    }
}

impl Default for CountingAlloc {
    fn default() -> Self {
        Self::new()
    }
}

/// Allocation requests served since the last [`reset`].
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

/// Bytes those requests asked for.
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: the same layout, handed to the allocator this wrapper
        // delegates to; the pointer it returns is what the caller receives.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout came from the `alloc` above, which
        // served them through `System`, and the contract of `dealloc` is the
        // caller's other half of `alloc`'s.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// What the process allocated since the last [`reset`], as two unsigned counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    /// Allocation requests served.
    pub allocations: u64,
    /// Bytes those requests asked for.
    pub bytes: u64,
}

/// The counts for the window opened by the last [`reset`].
pub fn usage() -> Usage {
    Usage {
        allocations: ALLOCATIONS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
    }
}

/// Open a fresh counting window. Call it after the fixture is built and the
/// loop's own storage is reserved, so neither is charged to the operation.
pub fn reset() {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
}

// **One test, one window, and lower bounds rather than equalities** — both for
// the same reason. The counters are process-global and `cargo test` runs
// sibling tests on parallel threads: another thread's allocation landing in
// the window only ever *adds* to it, but a second `reset` in a second test
// would zero it out from under the first, so there is exactly one resequencer
// here. Exactness is a property of a single-threaded window — the leg's own
// timed loop — and what pins the exact counts is the legs' readings, where
// `alloc_4` reports `allocs_per_op=4.000` and `receiver_now` reports `0.000`
// (`docs/benches.md`).
#[cfg(test)]
mod tests {
    use std::hint::black_box;

    use super::{reset, usage};

    #[test]
    fn a_window_counts_what_it_allocates() {
        reset();
        let one = Box::new([0u8; 64]);
        black_box(&one);
        let first = usage();
        drop(one);
        assert!(
            first.allocations >= 1,
            "the box's request must be counted: {first:?}"
        );
        assert!(first.bytes >= 64, "its 64 bytes must be counted: {first:?}");

        reset();
        let many: Vec<Box<[u8; 64]>> = (0..100).map(|_| Box::new([0u8; 64])).collect();
        black_box(&many);
        let second = usage();
        drop(many);
        assert!(
            second.allocations >= 100,
            "a hundred boxes are at least a hundred requests: {second:?}"
        );
        assert!(
            second.bytes >= 6_400,
            "and at least theirs in bytes: {second:?}"
        );
    }
}
