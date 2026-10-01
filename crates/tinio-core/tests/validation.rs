//! Property tests for key and bucket-name validation (task T027).
//!
//! The checked constructors ([`tinio_core::object::key`] /
//! [`tinio_core::bucket::name`]) are the single gate for untrusted input:
//! these properties assert they never panic on arbitrary input, reject the
//! documented patterns (traversal, absolute paths, control characters,
//! `.tinio` segments at any depth, bucket-name rules), and that every
//! accepted key maps to a path that stays inside its base directory.

use std::{
    path::{Component, Path},
    str::from_utf8,
};

use prop::collection;
use proptest::prelude::*;
use tinio_core::{bucket, object};

// Any string (including non-UTF8 bytes) must never panic the constructors.
proptest! {
    #[test]
    fn constructors_never_panic_on_arbitrary_strings(s in "\\PC*") {
        let _ = object::key(s.clone());
        let _ = bucket::name(s);
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in collection::vec(any::<u8>(), 0..64)) {
        if let Ok(s) = from_utf8(&bytes) {
            let _ = object::key(s);
            let _ = bucket::name(s);
        }
    }
}

// Traversal sequences, absolute paths, dot segments, and control
// characters are rejected (FR-006).
proptest! {
    #[test]
    fn traversal_sequences_rejected(mid in "[a-z]{0,4}", tail in "[a-z]{0,4}") {
        for key in [
            format!("..{tail}"),
            format!("{mid}/..{tail}"),
            format!("a/../{tail}"),
            format!("{mid}/../{tail}"),
            "...".to_string(),
        ] {
            prop_assert!(object::key(&key).is_err(), "{key:?} must be rejected");
        }
    }

    #[test]
    fn absolute_paths_rejected(tail in "[a-z]{1,8}") {
        for key in [format!("/{tail}"), format!("\\\\{tail}")] {
            prop_assert!(object::key(&key).is_err(), "{key:?} must be rejected");
        }
    }

    #[test]
    fn control_characters_rejected(control in 0u8..32u8, s in "[a-z]{1,4}") {
        let key = format!("{s}\u{1}") + &(control as char).to_string();
        prop_assert!(object::key(&key).is_err(), "{key:?} must be rejected");
    }
}

// A `.tinio` segment at any depth is flagged reserved (FR-020).
proptest! {
    #[test]
    fn tinio_segment_reserved_at_any_depth(prefix in "[a-z]{1,4}", suffix in "[a-z]{1,4}") {
        for key in [
            ".tinio".to_string(),
            format!(".tinio/{suffix}"),
            format!("{prefix}/.tinio"),
            format!("{prefix}/.tinio/{suffix}"),
            format!("{prefix}/a/.tinio/b/{suffix}"),
        ] {
            let key = object::key(&key).unwrap();
            prop_assert!(key.is_reserved(), "{key:?} must be reserved");
        }
    }
}

// Bucket-name rules (FR-012): length, charset, leading/trailing dot or
// hyphen, adjacent dots.
proptest! {
    #[test]
    fn bucket_name_rules(seed in "[a-z0-9.-]{0,80}") {
        if let Ok(name) = bucket::name(&seed) {
            let name = name.as_ref();
            prop_assert!((3..=63).contains(&name.len()));
            prop_assert!(name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-'));
            prop_assert!(!name.starts_with('.') && !name.starts_with('-'));
            prop_assert!(!name.ends_with('.') && !name.ends_with('-'));
            prop_assert!(!name.contains(".."));
        }
    }
}

// Every accepted key joins into a path that stays inside its base
// directory (the escape-proof property backing the path mapping).
proptest! {
    #[test]
    fn accepted_keys_never_escape_base(
        segs in collection::vec("[a-zA-Z0-9._ -]{1,12}", 1..6),
        trailing_slash in any::<bool>(),
    ) {
        let mut key = segs.join("/");
        if trailing_slash {
            key.push('/');
        }
        let Ok(key) = object::key(&key) else {
            return Ok(()); // some combinations are invalid; skip
        };
        prop_assert!(!key.is_reserved());
        let base = Path::new("/srv/data/bucket");
        let path = base.join(&*key);
        let rel = path.strip_prefix(base).unwrap();
        prop_assert!(!rel.is_absolute());
        prop_assert!(rel.components().all(|c| matches!(c, Component::Normal(_))));
        prop_assert_eq!(rel.components().count(), key.split('/').filter(|s| !s.is_empty()).count());
    }
}

// The plain key-classification cases (`representative_valid_and_invalid_keys`,
// `platform_charset_left_to_the_backend`, `reserved_flag_vs_rejection`) moved
// inline to `object.rs`'s own tests, beside the constructor they exercise; the
// file keeps the proptest paradigm (`docs/tests.md`).
