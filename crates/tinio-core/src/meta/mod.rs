//! The persistence contracts: decoded metadata rows and the async traits a
//! backend implements over its own transport.
//!
//! Four contracts live here. [`Metadata`] declares the row error once and
//! aggregates the three metadata categories ([`BucketMeta`], [`ObjectMeta`],
//! [`UploadMeta`]); [`VersionMeta`] is standalone (never a supertrait, D20)
//! and owns the version rows, the bucket's versioning state and the key's
//! current slot; [`ObjectContent`](crate::content::ObjectContent) owns object
//! and part bytes. The block plane is the existing
//! [`BlockStore`](crate::blob::BlockStore).
//!
//! Every row type here is **decoded** — a domain type or an owned string, never
//! a redb key or value tuple, never a table handle. The wire codecs the stored
//! rows share live once in [`bucket`] — the owner/ACL split
//! ([`bucket::decode_acl`], [`bucket::encode_owner`], [`bucket::encode_grants`]),
//! the strict grants decode and the CORS decode — and every row module calls
//! them: one home for the encode/decode rules, shared by every backend.
//!
//! The traits are used **generically** (`M: Metadata + VersionMeta`), never as
//! `dyn` (D6): each category method returns `<Self as Metadata>::Error` and
//! carries a `where Self: Metadata` clause, which makes [`Metadata`]
//! dyn-incompatible. Methods take slices and argument structs, never closures
//! or extra generic parameters, so the surface stays uniform for every
//! backend.
//!
//! Properties the contracts carry, each pinned by the conformance suite in
//! `tinio_util::testing::metadata`:
//!
//! 1. **Atomicity per method.** `Ok` means every row of the operation
//!    committed; `Err` means none did. `put_with_parts` writes the object row
//!    and its retained part list together; `move_` moves both together.
//! 2. **Ordering.** Ascending by the key's UTF-8 byte sequence; part numbers
//!    order numerically.
//! 3. **Prefix boundaries.** A per-bucket traversal never yields another
//!    bucket's row.
//! 4. **The self-heal asymmetry, both halves.** A point read treats a corrupt
//!    row as absent and heals it; [`ObjectMeta::walk`] fails on a corrupt row;
//!    [`ObjectMeta::walk_healing`] never fails and heals.
//! 5. **Idempotence.** Drains, clears, removals and `remove_bucket` are no-ops
//!    on absent state and report success.
//! 6. **The visibility point.** `ObjectContent::publish` returns only when the
//!    bytes are visible; the metadata row describing them is written after it.
//! 7. **Error projection.** Each contract's `Error` converts into
//!    [`storage::Error`]; the per-crate enums and their lift rules are
//!    unchanged.
//! 8. **Refusals are visible.** A `put_guarded` refusal projects to
//!    [`storage::Error::NoSuchBucket`] and `remove_if_empty`'s two outcomes to
//!    `NoSuchBucket` / [`storage::Error::NotEmpty`].
//! 9. **Buckets are content-side too.** `create_bucket`/`remove_bucket` are the
//!    content plane's bucket lifecycle, and `is_empty` sees everything the
//!    backend counts as bucket content.

use std::error::Error as StdError;

use crate::storage;

pub mod bucket;
pub mod object;
pub mod upload;
pub mod version;

pub use bucket::BucketMeta;
pub use object::ObjectMeta;
pub use upload::UploadMeta;
pub use version::VersionMeta;

/// The metadata contract: the row error, and the three category traits that
/// carry the operations.
///
/// A backend implements the three categories and declares its error type once,
/// here. The error must convert into [`storage::Error`] — the S3 mapping layer
/// and the conformance suite speak only the layering error.
///
/// # Examples
///
/// The three categories are only callable on a complete metadata store — the
/// methods are bound by `Self: Metadata`:
///
/// ```ignore
/// use tinio_core::meta::{Metadata, ObjectMeta};
///
/// fn needs_object_meta<M: ObjectMeta + Metadata>() {}
/// ```
pub trait Metadata: Send + Sync + 'static + BucketMeta + ObjectMeta + UploadMeta {
    /// This backend's row error; it must project into [`storage::Error`].
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content;

    /// The compile-time equality pin: the sole `Same` impl is the blanket
    /// `impl<T> Same<T> for T`, so a pair of distinct types never satisfies
    /// the bound — `assert_same` fails to resolve (E0277) unless both type
    /// arguments are the same type.
    trait Same<T> {}
    impl<T> Same<T> for T {}

    fn assert_same<A: Same<B>, B>() {}

    /// The shape probe (plan Task 1 Step 2, R1): the four contracts are
    /// **satisfiable together, used generically**. The probe is a generic
    /// function bounded on the contracts; the `const` below applies it to type
    /// variables carrying the whole bound set, so an unsatisfiable set fails
    /// the build. No concrete store is instantiated — that is deliberately
    /// deferred with `ops_probe` until the ops engine exists.
    ///
    /// The plan's original form named
    /// `Ops<Store, Store, Store, Store, Blocks, Hooks>`; that type does not
    /// exist yet — the ops engine is a later phase — so the probe's
    /// instantiation is deferred with it. There is deliberately **no** `dyn`
    /// probe: the category methods' `where Self: Metadata` clause makes
    /// `Metadata` dyn-incompatible (round 40), and a dyn form never resolves.
    #[allow(dead_code)]
    fn shape_probe<M, C>(store: &M, content: &C)
    where
        M: Metadata + version::VersionMeta,
        C: content::ObjectContent,
    {
        let _ = (store, content);
    }

    /// The deferred ops-engine instantiation: this is the call the plan's Step 2
    /// names, kept here so the future `Ops` probe has one obvious home.
    #[allow(dead_code)]
    fn ops_probe<M, C>(store: &M, content: &C)
    where
        M: Metadata + version::VersionMeta,
        C: content::ObjectContent,
    {
        shape_probe(store, content);
    }

    #[test]
    fn the_equality_pin_resolves() {
        assert_same::<(u32, u64), (u32, u64)>();
    }

    /// The bound set's well-formedness, and nothing stronger: the four
    /// contracts combine in one `where` clause and both probes apply to such
    /// type variables, so the item below compiles — no run and no store
    /// instance needed, which is why it is a `const` rather than a test body.
    ///
    /// It does **not** prove satisfiability: that needs a concrete witness
    /// type, and no in-repo type implements the contracts yet (P1 Tasks 5-8).
    /// The first implementor is the witness. `_pin` is never instantiated, so
    /// nothing here is applied to a real type.
    const _ALL_FOUR_CONTRACTS_COMBINE: () = {
        // A generic `fn` bounded by the four contracts that applies its own
        // probes to them. The definition proves they are nameable together and
        // nothing more: nothing instantiates `_pin`, and a `where` clause no
        // type can satisfy is not an error, so this compiles either way. The
        // witness is a concrete implementor. No `dyn` form is attempted
        // (round 40).
        fn same_shape<A: Same<B>, B>() {}
        fn _pin<M, C>()
        where
            M: Metadata + version::VersionMeta,
            C: content::ObjectContent,
        {
            same_shape::<M, M>();
            let _ = shape_probe::<M, C>;
            let _ = ops_probe::<M, C>;
        }
    };

    #[test]
    fn the_adversarial_key_ordering_is_the_utf8_byte_sequence() {
        // `-` (0x2D) sorts before `/` (0x2F), so a walk over these three keys
        // yields them in this order — the pair that separates a byte-ordered
        // walk from a locale-ordered one. COLLATE "C" is the PostgreSQL
        // encoding of the same rule (spec D17/D40).
        let ordered = ["data", "data-2", "data/"];
        for pair in ordered.windows(2) {
            let (left, right) = (pair[0], pair[1]);
            assert!(
                left.as_bytes() < right.as_bytes(),
                "`{left}` must sort before `{right}` by UTF-8 byte sequence"
            );
        }
        assert!(
            "data-2".as_bytes() < "data/".as_bytes(),
            "the pair is adversarial precisely because the two orders differ"
        );
    }
}
