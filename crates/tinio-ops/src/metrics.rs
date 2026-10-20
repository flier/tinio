//! The read-heal family, declared once.
//!
//! A heal is a write inside a read: a row the read path recomputes against
//! the content and persists, and today it leaves no aggregate signal. This
//! module owns family 10; the object group's `healed_row` records it, and the
//! label is the closed healed-row kind — `object_row` or `bucket_row`. No
//! path, key, bucket or identifier may become a label.
//!
//! This is a metrics vocabulary, not a layer: no `tracing`, no `tinio-core`
//! type, no `tokio`.
//!
//! The module is crate-private: its one observer is `object::healed_row`.
//! A name recorded here renders as
//! `tinio_storage_read_heals_total{kind="object_row"}`.

use std::sync::LazyLock;

use prometheus::{IntCounterVec, register_int_counter_vec};
use tinio_util::metrics::RegisteredExt;

/// The registered family.
static FAMILY: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_storage_read_heals_total",
        "Rows a read path healed against the content, by healed row kind",
        &["kind"]
    )
    .registered("tinio_storage_read_heals_total")
});

/// The closed kind of row a read path healed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealKind {
    /// An object metadata row.
    ObjectRow,
    /// A bucket metadata row.
    BucketRow,
}

impl HealKind {
    /// The label value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ObjectRow => "object_row",
            Self::BucketRow => "bucket_row",
        }
    }
}

/// Record one read-path heal.
pub(crate) fn record_read_heal(kind: HealKind) {
    FAMILY.with_label_values(&[kind.as_str()]).inc();
}

#[cfg(test)]
mod tests {
    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    #[test]
    fn the_family_renders_its_recorded_values() {
        record_read_heal(HealKind::ObjectRow);

        assert_registered(&["tinio_storage_read_heals_total"]);
        let text = exposition();
        assert!(
            text.contains(r#"tinio_storage_read_heals_total{kind="object_row"}"#),
            "the read-heals counter carries its kind: {text}"
        );
    }
}
