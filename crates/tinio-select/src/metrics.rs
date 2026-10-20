//! S3 Select stream families: bytes scanned and rows returned, by input framing.
//!
//! Both are costs the request span cannot see: one Select request streams an
//! object of unknown size, so the scan volume and the output row count are the
//! engine's own signal. The registry is process-global; the crate rendering
//! `/metrics` gathers these families without this crate knowing it.

#![expect(
    clippy::expect_used,
    reason = "argued: every error these registrations can answer is about each family's own name — prometheus 0.14's default registry refuses a name it already holds (`AlreadyReg`) or holds with a different label set or help (`Msg`). Each name has one registration site in the repo (its own `LazyLock` here, whose closure runs once per process) against the family list `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not persisted)`; a collision is a source-level mistake, not a runtime condition, and `LazyLock::new` has no `Result` to return it through. The module's test reads both names back out of the gathered exposition."
)]

use std::sync::LazyLock;

use parse_display::Display;
use prometheus::{IntCounterVec, register_int_counter_vec};

/// Input framing — the closed `format` label domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(style = "lowercase")]
pub enum Format {
    Csv,
    Json,
    Parquet,
}

impl Format {
    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Parquet => "parquet",
        }
    }
}

/// Uncompressed bytes the record readers consumed, cumulative.
static SCANNED_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_s3_select_scanned_bytes_total",
        "Uncompressed input bytes scanned by S3 Select, by input framing",
        &["format"]
    )
    .expect("register tinio_s3_select_scanned_bytes_total")
});

/// Output rows serialized for the client, cumulative.
static ROWS_RETURNED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_s3_select_rows_returned_total",
        "Rows returned by S3 Select, by input framing",
        &["format"]
    )
    .expect("register tinio_s3_select_rows_returned_total")
});

/// Record one stream's cost: the uncompressed bytes it scanned and the rows it
/// serialized for the client.
pub fn record_stream(format: Format, scanned: u64, rows: u64) {
    let format = format.as_str();
    SCANNED_BYTES.with_label_values(&[format]).inc_by(scanned);
    ROWS_RETURNED.with_label_values(&[format]).inc_by(rows);
}

#[cfg(test)]
mod tests {
    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    #[test]
    fn the_select_families_render_their_recorded_values() {
        for format in [Format::Csv, Format::Json, Format::Parquet] {
            record_stream(format, 1, 1);
        }

        assert_registered(&[
            "tinio_s3_select_scanned_bytes_total",
            "tinio_s3_select_rows_returned_total",
        ]);
        let text = exposition();
        for format in ["csv", "json", "parquet"] {
            for family in [
                "tinio_s3_select_scanned_bytes_total",
                "tinio_s3_select_rows_returned_total",
            ] {
                let series = format!(r#"{family}{{format="{format}"}}"#);
                assert!(text.contains(&series), "missing {series}: {text}");
            }
        }
    }
}
