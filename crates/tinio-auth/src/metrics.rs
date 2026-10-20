//! The auth decision families: credential lookups and authorization outcomes.
//!
//! Both are P0 security signals: an unknown-key spike is credential probing,
//! and the allow/deny aggregate is the decision itself. Labels stay a fixed
//! pair — a denial reason is a span field, never a label — so a client cannot
//! grow a series. The registry is process-global, so the management plane
//! renders these families without this crate knowing it.

#![expect(
    clippy::expect_used,
    reason = "argued: every registration in this module answers a duplicate or mismatched family at source level, not a runtime condition — one `LazyLock` site per family runs once per process, the registry refuses `AlreadyReg`/`Msg`, and `LazyLock::new` has no `Result` to return it through (design 2026-10-09 D2d); one exception for the module, not one per family"
)]

use std::sync::LazyLock;

use prometheus::{IntCounterVec, register_int_counter_vec};

/// Credential lookups by result: `found` for a configured access key,
/// `unknown` for one the identity map does not hold.
static CREDENTIAL_LOOKUPS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_auth_credential_lookups_total",
        "Credential lookups by result",
        &["result"]
    )
    .expect("register tinio_auth_credential_lookups_total")
});

/// Authorization decisions by outcome.
static AUTHORIZATIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "tinio_auth_authorizations_total",
        "Authorization decisions by outcome",
        &["decision"]
    )
    .expect("register tinio_auth_authorizations_total")
});

/// Record one credential lookup; `found` is whether the access key resolved.
///
/// # Examples
///
/// ```rust
/// tinio_auth::metrics::record_credential_lookup(false);
/// ```
pub fn record_credential_lookup(found: bool) {
    let result = if found { "found" } else { "unknown" };
    CREDENTIAL_LOOKUPS.with_label_values(&[result]).inc();
}

/// Record one authorization decision; `allowed` is whether the request passed.
///
/// # Examples
///
/// ```rust
/// tinio_auth::metrics::record_authorization(true);
/// ```
pub fn record_authorization(allowed: bool) {
    let decision = if allowed { "allowed" } else { "denied" };
    AUTHORIZATIONS.with_label_values(&[decision]).inc();
}

#[cfg(test)]
mod tests {

    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::*;

    #[test]
    fn the_auth_families_render_their_recorded_values() {
        record_credential_lookup(true);
        record_credential_lookup(false);
        record_authorization(true);
        record_authorization(false);

        assert_registered(&[
            "tinio_auth_credential_lookups_total",
            "tinio_auth_authorizations_total",
        ]);
        let text = exposition();
        for series in [
            r#"tinio_auth_credential_lookups_total{result="found"}"#,
            r#"tinio_auth_credential_lookups_total{result="unknown"}"#,
            r#"tinio_auth_authorizations_total{decision="allowed"}"#,
            r#"tinio_auth_authorizations_total{decision="denied"}"#,
        ] {
            assert!(text.contains(series), "missing {series}: {text}");
        }
    }
}
