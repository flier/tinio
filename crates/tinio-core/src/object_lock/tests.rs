use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use time::{Date, Month, PrimitiveDateTime, Time};

use super::*;

/// The fixture's time of day: every constructed instant carries it, so the
/// calendar-year tests can prove the time of day survives.
const HOUR: u8 = 12;
const MINUTE: u8 = 34;
const SECOND: u8 = 56;

fn at(year: i32, month: Month, day: u8) -> SystemTime {
    SystemTime::from(
        PrimitiveDateTime::new(
            Date::from_calendar_date(year, month, day).expect("a valid fixture date"),
            Time::from_hms(HOUR, MINUTE, SECOND).expect("a valid fixture time"),
        )
        .assume_utc(),
    )
}

fn days(count: u32) -> Period {
    Period::Days(count)
}

fn years(count: u32) -> Period {
    Period::Years(count)
}

fn rule(mode: Mode, period: Period) -> Rule {
    Rule {
        default_retention: DefaultRetention { mode, period },
    }
}

fn config(mode: Mode, period: Period) -> Config {
    Config {
        enabled: true,
        rule: Some(rule(mode, period)),
    }
}

fn retention(mode: Mode, retain_until: SystemTime) -> Retention {
    Retention { mode, retain_until }
}

fn lock(mode: Mode, retain_until: SystemTime, legal_hold: LegalHold) -> Lock {
    Lock {
        retention: Some(retention(mode, retain_until)),
        legal_hold,
    }
}

fn hold_only(legal_hold: LegalHold) -> Lock {
    Lock {
        retention: None,
        legal_hold,
    }
}

fn nanos(instant: SystemTime) -> i64 {
    i64::try_from(
        instant
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("the fixture is after the epoch")
            .as_nanos(),
    )
    .expect("the fixture's nanos fit an i64")
}

// The validation matrix.

#[test]
fn a_disabled_configuration_is_refused() {
    let invalid = Config {
        enabled: false,
        rule: None,
    }
    .validate()
    .expect_err("a configuration is only accepted enabled");
    assert_eq!(invalid.reason, ENABLED);
    assert_eq!(invalid.kind, InvalidKind::Semantic);
    assert_eq!(invalid.to_string(), ENABLED);
}

#[test]
fn enabling_without_a_default_rule_is_accepted() {
    assert_eq!(
        Config {
            enabled: true,
            rule: None
        }
        .validate(),
        Ok(())
    );
}

#[test]
fn days_are_bounded() {
    for (count, accepted) in [
        (0, false),
        (1, true),
        (MAX_DAYS, true),
        (MAX_DAYS + 1, false),
    ] {
        let result = config(Mode::Governance, days(count)).validate();
        assert_eq!(
            result.is_ok(),
            accepted,
            "DefaultRetention Days {count} must be {}",
            if accepted { "accepted" } else { "refused" }
        );
        if let Err(invalid) = result {
            assert_eq!(invalid.reason, DAYS_RANGE);
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
    }
}

#[test]
fn years_are_bounded() {
    for (count, accepted) in [
        (0, false),
        (1, true),
        (MAX_YEARS, true),
        (MAX_YEARS + 1, false),
    ] {
        let result = config(Mode::Compliance, years(count)).validate();
        assert_eq!(
            result.is_ok(),
            accepted,
            "DefaultRetention Years {count} must be {}",
            if accepted { "accepted" } else { "refused" }
        );
        if let Err(invalid) = result {
            assert_eq!(invalid.reason, YEARS_RANGE);
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
    }
}

#[test]
fn a_valid_rule_is_accepted() {
    assert_eq!(
        config(Mode::Governance, days(30)).validate(),
        Ok(()),
        "the ordinary governance/days rule"
    );
    assert_eq!(
        config(Mode::Compliance, years(7)).validate(),
        Ok(()),
        "the ordinary compliance/years rule"
    );
}

#[test]
fn a_rule_carries_exactly_one_period() {
    // The model's `Period` is an enum, so "both" and "neither" are
    // unrepresentable. The wire is where a malformed rule arrives, and the
    // decoder must refuse every spelling that is not one period.
    assert_eq!(
        config(Mode::Governance, days(1)).rule_wire(),
        "GOVERNANCE,DAYS,1"
    );
    for wire in [
        "GOVERNANCE",              // neither period
        "GOVERNANCE,DAYS",         // neither period
        "GOVERNANCE,DAYS,",        // neither period
        "GOVERNANCE,DAYS,YEARS,1", // both periods
        "GOVERNANCE,,1",           // neither period
    ] {
        let decoded = Config::from_rule_wire(true, wire);
        assert_eq!(decoded.rule, None, "{wire} is not one period");
        assert!(decoded.enabled, "{wire}: the enabled bit survives");
    }
}

#[test]
fn retention_dates_must_be_strictly_future() {
    let now = at(2024, Month::January, 1);
    let past = retention(Mode::Governance, now - Duration::from_secs(1));
    let equal = retention(Mode::Compliance, now);
    let future = retention(Mode::Governance, now + Duration::from_secs(1));

    let invalid = past.validate(now).expect_err("a past date is refused");
    assert_eq!(invalid.reason, RETAIN_FUTURE);
    assert_eq!(invalid.kind, InvalidKind::Semantic);
    assert_eq!(
        equal.validate(now).expect_err("a date of now is refused"),
        Invalid {
            reason: RETAIN_FUTURE,
            kind: InvalidKind::Semantic,
        }
    );
    assert_eq!(future.validate(now), Ok(()));
}

#[test]
fn legal_hold_status_is_on_or_off() {
    assert_eq!(LegalHold::parse("ON"), Ok(LegalHold::On));
    assert_eq!(LegalHold::parse("OFF"), Ok(LegalHold::Off));
    for refused in ["", "on", "On", "off", "YES", " ON", "OFF "] {
        let invalid = LegalHold::parse(refused)
            .expect_err("anything but the two canonical spellings is refused");
        assert_eq!(invalid.reason, LEGAL_HOLD_STATUS);
        assert_eq!(invalid.kind, InvalidKind::Shape);
    }
}

#[test]
fn mode_and_legal_hold_share_one_spelling() {
    assert_eq!(Mode::Governance.to_string(), "GOVERNANCE");
    assert_eq!(Mode::Compliance.to_string(), "COMPLIANCE");
    assert_eq!(Mode::from_str("GOVERNANCE"), Ok(Mode::Governance));
    assert_eq!(Mode::from_str("COMPLIANCE"), Ok(Mode::Compliance));
    assert!(Mode::from_str("governance").is_err());

    assert_eq!(LegalHold::On.to_string(), "ON");
    assert_eq!(LegalHold::Off.to_string(), "OFF");
    assert_eq!(LegalHold::from_str("ON"), Ok(LegalHold::On));
    assert_eq!(LegalHold::from_str("OFF"), Ok(LegalHold::Off));
    assert!(LegalHold::from_str("ON ").is_err());
}

#[test]
fn a_legal_hold_only_write_still_receives_the_default() {
    let now = at(2024, Month::March, 1);
    let rule = rule(Mode::Compliance, days(10));
    let written = write_lock(None, Some(&rule), now);

    assert_eq!(
        written.retention,
        Some(retention(
            Mode::Compliance,
            now + Duration::from_secs(10 * 86_400)
        )),
        "a write carrying only a legal-hold header still resolves the default retention"
    );
    assert_eq!(
        written.legal_hold,
        LegalHold::Off,
        "the legal hold is the request's, applied over this retention"
    );
}

#[test]
fn an_explicit_retention_overrides_the_default() {
    let now = at(2024, Month::March, 1);
    let rule = rule(Mode::Compliance, days(10));
    let explicit = retention(Mode::Governance, now + Duration::from_secs(3));

    let written = write_lock(Some(&explicit), Some(&rule), now);
    assert_eq!(written.retention, Some(explicit));
    assert_eq!(written.legal_hold, LegalHold::Off);

    let bare = write_lock(None, None, now);
    assert_eq!(bare.retention, None);
    assert_eq!(bare.legal_hold, LegalHold::Off);
}

#[test]
fn a_days_default_is_a_fixed_span() {
    let now = at(2024, Month::March, 1);
    assert_eq!(
        default_retention_at(&rule(Mode::Governance, days(3)), now),
        now + Duration::from_secs(3 * 86_400)
    );
}

#[test]
fn a_years_default_is_calendar_years() {
    let leap_day = at(2024, Month::February, 29);
    assert_eq!(
        default_retention_at(&rule(Mode::Governance, years(1)), leap_day),
        at(2025, Month::February, 28),
        "Feb 29 in a non-leap target year falls back to Feb 28, keeping the time of day"
    );
    assert_eq!(
        default_retention_at(&rule(Mode::Governance, years(4)), leap_day),
        at(2028, Month::February, 29),
        "a leap target year keeps the leap day"
    );
    assert_eq!(
        default_retention_at(
            &rule(Mode::Governance, years(1)),
            at(2023, Month::March, 31)
        ),
        at(2024, Month::March, 31),
        "an ordinary date advances by the same day and month"
    );
}

#[test]
fn an_unrepresentable_target_year_has_no_date() {
    let date = Date::from_calendar_date(2024, Month::January, 1).expect("a valid fixture date");
    assert_eq!(
        add_years(date, u32::MAX),
        None,
        "a year past the calendar's range is not a date"
    );
}

#[test]
fn the_decoder_reads_a_non_canonical_count_leniently() {
    let zero_padded = Config::from_rule_wire(true, "GOVERNANCE,DAYS,030");
    assert_eq!(
        zero_padded,
        config(Mode::Governance, days(30)),
        "leading zeros decode to the same rule"
    );
    let plus_signed = Config::from_rule_wire(true, "GOVERNANCE,DAYS,+1");
    assert_eq!(
        plus_signed,
        config(Mode::Governance, days(1)),
        "a leading plus sign decodes to the same rule"
    );
    assert_eq!(
        plus_signed.rule_wire(),
        "GOVERNANCE,DAYS,1",
        "re-encoding a lenient input is canonical"
    );
}

// The protection matrix.

#[test]
fn governance_extends_without_the_bypass() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(100),
        LegalHold::Off,
    );
    let requested = retention(Mode::Governance, now + Duration::from_secs(200));

    let applied = apply_retention(Some(&current), Some(&requested), Bypass::None, now)
        .expect("an extension never needs the governance bypass");
    assert_eq!(
        applied,
        Some(lock(
            Mode::Governance,
            now + Duration::from_secs(200),
            LegalHold::Off
        ))
    );
}

#[test]
fn governance_shortening_needs_the_bypass() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(200),
        LegalHold::Off,
    );
    let requested = retention(Mode::Governance, now + Duration::from_secs(100));

    let refusal = apply_retention(Some(&current), Some(&requested), Bypass::None, now)
        .expect_err("a shortening without the bypass is refused");
    assert_eq!(refusal, RefusalReason::Governance);

    let applied = apply_retention(Some(&current), Some(&requested), Bypass::Governance, now)
        .expect("the governance bypass permits a shortening");
    assert_eq!(
        applied,
        Some(lock(
            Mode::Governance,
            now + Duration::from_secs(100),
            LegalHold::Off
        ))
    );
}

#[test]
fn governance_removal_needs_the_bypass() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(200),
        LegalHold::Off,
    );

    let refusal = apply_retention(Some(&current), None, Bypass::None, now)
        .expect_err("removing a governance retention without the bypass is refused");
    assert_eq!(refusal, RefusalReason::Governance);

    assert_eq!(
        apply_retention(Some(&current), None, Bypass::Governance, now),
        Ok(None),
        "the bypass permits the removal, and no hold is left"
    );
}

#[test]
fn governance_can_strengthen_to_compliance() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(100),
        LegalHold::Off,
    );
    let requested = retention(Mode::Compliance, now + Duration::from_secs(200));

    let applied = apply_retention(Some(&current), Some(&requested), Bypass::None, now)
        .expect("a longer compliance retention strengthens the lock");
    assert_eq!(
        applied,
        Some(lock(
            Mode::Compliance,
            now + Duration::from_secs(200),
            LegalHold::Off
        ))
    );
}

#[test]
fn governance_cannot_shorten_by_changing_mode() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(200),
        LegalHold::Off,
    );
    let requested = retention(Mode::Compliance, now + Duration::from_secs(100));

    let refusal = apply_retention(Some(&current), Some(&requested), Bypass::None, now)
        .expect_err("a mode change that shortens the date is a shortening");
    assert_eq!(refusal, RefusalReason::Governance);
}

#[test]
fn compliance_extends_and_may_repeat_itself() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Compliance,
        now + Duration::from_secs(100),
        LegalHold::Off,
    );

    for requested in [
        retention(Mode::Compliance, now + Duration::from_secs(200)),
        retention(Mode::Compliance, now + Duration::from_secs(100)),
    ] {
        let applied = apply_retention(Some(&current), Some(&requested), Bypass::None, now)
            .expect("a compliance retention can be extended, never shortened");
        assert_eq!(
            applied,
            Some(lock(
                Mode::Compliance,
                requested.retain_until,
                LegalHold::Off
            ))
        );
    }
}

#[test]
fn compliance_refuses_shortening_removal_and_mode_change_even_with_the_bypass() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Compliance,
        now + Duration::from_secs(200),
        LegalHold::Off,
    );

    for requested in [
        Some(retention(Mode::Compliance, now + Duration::from_secs(100))),
        None,
        Some(retention(Mode::Governance, now + Duration::from_secs(400))),
    ] {
        let refusal = apply_retention(Some(&current), requested.as_ref(), Bypass::Governance, now)
            .expect_err("compliance ignores the governance bypass");
        assert_eq!(refusal, RefusalReason::Compliance);
    }
}

#[test]
fn an_unlocked_version_can_be_set_and_left_alone() {
    let now = at(2024, Month::January, 1);

    assert_eq!(
        apply_retention(None, None, Bypass::None, now),
        Ok(None),
        "leaving an unlocked version alone writes no lock"
    );

    let requested = retention(Mode::Compliance, now + Duration::from_secs(10));
    assert_eq!(
        apply_retention(None, Some(&requested), Bypass::None, now),
        Ok(Some(lock(
            Mode::Compliance,
            now + Duration::from_secs(10),
            LegalHold::Off
        )))
    );
}

#[test]
fn clearing_retention_keeps_the_legal_hold() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(10),
        LegalHold::On,
    );

    assert_eq!(
        apply_retention(Some(&current), None, Bypass::Governance, now),
        Ok(Some(hold_only(LegalHold::On))),
        "a clear removes the retention only; the hold survives"
    );
}

#[test]
fn clearing_an_expired_compliance_retention_is_allowed() {
    let now = at(2024, Month::January, 1);
    let current = lock(Mode::Compliance, now, LegalHold::Off);

    assert_eq!(
        apply_retention(Some(&current), None, Bypass::None, now),
        Ok(None),
        "an expired retention is inert and can be cleared"
    );

    let requested = retention(Mode::Governance, now + Duration::from_secs(10));
    assert_eq!(
        apply_retention(Some(&current), Some(&requested), Bypass::None, now),
        Ok(Some(lock(
            Mode::Governance,
            now + Duration::from_secs(10),
            LegalHold::Off
        ))),
        "an expired retention does not constrain the replacement"
    );
}

#[test]
fn a_legal_hold_refuses_deletion_in_either_mode() {
    let now = at(2024, Month::January, 1);
    for current in [
        lock(
            Mode::Governance,
            now - Duration::from_secs(1),
            LegalHold::On,
        ),
        lock(
            Mode::Compliance,
            now - Duration::from_secs(1),
            LegalHold::On,
        ),
        hold_only(LegalHold::On),
    ] {
        for bypass in [Bypass::None, Bypass::Governance] {
            let refusal = may_delete(Some(&current), now, bypass)
                .expect_err("a legal hold refuses deletion in either mode");
            assert_eq!(refusal, RefusalReason::LegalHold);
        }
    }
}

#[test]
fn unexpired_compliance_refuses_deletion() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Compliance,
        now + Duration::from_secs(1),
        LegalHold::Off,
    );

    for bypass in [Bypass::None, Bypass::Governance] {
        let refusal =
            may_delete(Some(&current), now, bypass).expect_err("compliance cannot be bypassed");
        assert_eq!(refusal, RefusalReason::Compliance);
        assert_eq!(refusal.to_string(), "compliance");
    }
}

#[test]
fn unexpired_governance_refuses_deletion_without_the_bypass() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now + Duration::from_secs(1),
        LegalHold::Off,
    );

    let refusal = may_delete(Some(&current), now, Bypass::None)
        .expect_err("an unbypassed governance retention refuses deletion");
    assert_eq!(refusal, RefusalReason::Governance);
    assert_eq!(may_delete(Some(&current), now, Bypass::Governance), Ok(()));
}

#[test]
fn expired_retention_is_inert_for_deletion() {
    let now = at(2024, Month::January, 1);
    for mode in [Mode::Governance, Mode::Compliance] {
        for until in [now - Duration::from_secs(1), now] {
            assert_eq!(
                may_delete(Some(&lock(mode, until, LegalHold::Off)), now, Bypass::None),
                Ok(()),
                "a retention that is not in the future no longer protects {mode}"
            );
        }
    }
}

#[test]
fn an_unlocked_version_and_no_version_both_delete() {
    let now = at(2024, Month::January, 1);
    assert_eq!(may_delete(None, now, Bypass::None), Ok(()));
    assert_eq!(
        may_delete(Some(&hold_only(LegalHold::Off)), now, Bypass::None),
        Ok(())
    );
}

#[test]
fn the_bypass_does_not_touch_a_legal_hold() {
    let now = at(2024, Month::January, 1);
    let current = lock(
        Mode::Governance,
        now - Duration::from_secs(1),
        LegalHold::On,
    );

    assert_eq!(
        may_delete(Some(&current), now, Bypass::Governance),
        Err(RefusalReason::LegalHold),
        "the governance bypass is about a governance retention, never a hold"
    );

    let requested = retention(Mode::Compliance, now + Duration::from_secs(10));
    let applied = apply_retention(Some(&current), Some(&requested), Bypass::Governance, now)
        .expect("the expired retention is inert")
        .expect("the hold keeps the lock");
    assert_eq!(applied.legal_hold, LegalHold::On);
}

#[test]
fn refusal_reasons_are_distinct() {
    assert_eq!(RefusalReason::LegalHold.to_string(), "legal hold");
    assert_eq!(RefusalReason::Compliance.to_string(), "compliance");
    assert_eq!(RefusalReason::Governance.to_string(), "governance");
    assert_ne!(RefusalReason::LegalHold, RefusalReason::Compliance);
    assert_ne!(RefusalReason::Compliance, RefusalReason::Governance);
}

// The rule wire.

#[test]
fn the_rule_wire_is_canonical_and_lossless() {
    for config in [
        config(Mode::Governance, days(1)),
        config(Mode::Governance, days(MAX_DAYS)),
        config(Mode::Compliance, years(1)),
        config(Mode::Compliance, years(MAX_YEARS)),
        Config {
            enabled: false,
            rule: Some(rule(Mode::Governance, days(30))),
        },
    ] {
        let wire = config.rule_wire();
        assert_eq!(
            Config::from_rule_wire(config.enabled, &wire),
            config,
            "{wire} round-trips"
        );
    }
}

#[test]
fn the_rule_wire_spells_mode_then_period_then_count() {
    assert_eq!(
        config(Mode::Governance, days(30)).rule_wire(),
        "GOVERNANCE,DAYS,30"
    );
    assert_eq!(
        config(Mode::Compliance, years(7)).rule_wire(),
        "COMPLIANCE,YEARS,7"
    );
}

#[test]
fn an_absent_rule_is_the_empty_wire() {
    assert_eq!(
        Config {
            enabled: true,
            rule: None
        }
        .rule_wire(),
        ""
    );
    assert_eq!(
        Config::from_rule_wire(false, ""),
        Config {
            enabled: false,
            rule: None
        }
    );
}

#[test]
fn a_corrupt_rule_wire_loses_the_rule_not_the_enabled_bit() {
    for wire in [
        "garbage",
        "GOVERNANCE",
        "GOVERNANCE,DAYS",
        "GOVERNANCE,DAYS,",
        "GOVERNANCE,DAYS,abc",
        "GOVERNANCE,DAYS,0",
        "GOVERNANCE,DAYS,36501",
        "GOVERNANCE,YEARS,0",
        "GOVERNANCE,YEARS,101",
        "BOGUS,DAYS,1",
        "governance,days,1",
        "GOVERNANCE,days,1",
        "GOVERNANCE,DAYS,1,EXTRA",
        "GOVERNANCE,DAYS,YEARS,1",
        " GOVERNANCE,DAYS,1",
        "GOVERNANCE,DAYS,-1",
    ] {
        let decoded = Config::from_rule_wire(true, wire);
        assert_eq!(decoded.rule, None, "{wire} is not a rule");
        assert!(decoded.enabled, "{wire}: the enabled bit survives");
    }
}

// The per-version columns.

#[test]
fn from_columns_reads_a_retention_and_a_hold() {
    let until = at(2025, Month::January, 1);
    assert_eq!(
        Lock::from_columns(Some("COMPLIANCE"), Some(nanos(until)), Some("ON")),
        Ok(Some(lock(Mode::Compliance, until, LegalHold::On)))
    );
    assert_eq!(
        Lock::from_columns(Some("GOVERNANCE"), Some(nanos(until)), Some("OFF")),
        Ok(Some(lock(Mode::Governance, until, LegalHold::Off)))
    );
}

#[test]
fn from_columns_reads_a_legal_hold_only() {
    assert_eq!(
        Lock::from_columns(None, None, Some("ON")),
        Ok(Some(hold_only(LegalHold::On)))
    );
}

#[test]
fn from_columns_treats_all_absent_as_no_row() {
    assert_eq!(Lock::from_columns(None, None, None), Ok(None));
}

#[test]
fn from_columns_treats_an_all_off_row_as_no_lock() {
    assert_eq!(
        Lock::from_columns(None, None, Some("OFF")),
        Ok(None),
        "a row that carries neither a retention nor a hold is not a lock"
    );
}

#[test]
fn carries_fact_matches_the_decode_rule() {
    assert!(!hold_only(LegalHold::Off).carries_fact());
    assert!(hold_only(LegalHold::On).carries_fact());
    let until = at(2025, Month::January, 1);
    assert!(
        lock(Mode::Governance, until, LegalHold::Off).carries_fact(),
        "a retention is a fact even when it is later expired"
    );
    assert_eq!(
        Lock::from_columns(None, None, Some("OFF")),
        Ok(None),
        "the encoder's predicate and the decoder's no-lock arm agree"
    );
}

#[test]
fn from_columns_refuses_a_partial_retention_pair() {
    for (mode, retain_until) in [(Some("GOVERNANCE"), None), (None, Some(1))] {
        let refusal = Lock::from_columns(mode, retain_until, Some("OFF"))
            .expect_err("a partial mode/date pair fails closed");
        assert_eq!(refusal.reason, RefusalReason::Compliance);
        assert_eq!(refusal.detail, PARTIAL_RETENTION);
    }
}

#[test]
fn from_columns_refuses_a_corrupt_mode() {
    let refusal = Lock::from_columns(Some("BOGUS"), Some(1), Some("OFF"))
        .expect_err("an unknown mode fails closed");
    assert_eq!(refusal.reason, RefusalReason::Compliance);
    assert_eq!(refusal.detail, CORRUPT_MODE);
}

#[test]
fn from_columns_refuses_a_corrupt_or_missing_hold() {
    for hold in [Some("MAYBE"), Some("on"), None] {
        let refusal = Lock::from_columns(Some("GOVERNANCE"), Some(1), hold)
            .expect_err("a corrupt hold fails closed");
        assert_eq!(refusal.reason, RefusalReason::LegalHold);
        assert_eq!(refusal.detail, CORRUPT_LEGAL_HOLD);
    }
}

#[test]
fn from_columns_accepts_before_epoch_nanos() {
    let until = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
    assert_eq!(
        Lock::from_columns(Some("GOVERNANCE"), Some(-1_000_000_000), Some("OFF")),
        Ok(Some(lock(Mode::Governance, until, LegalHold::Off)))
    );
}

// The plane JSON.

#[test]
fn the_plane_json_round_trips_the_stored_spellings() {
    for value in [
        config(Mode::Governance, days(30)),
        config(Mode::Compliance, years(7)),
        Config {
            enabled: true,
            rule: None,
        },
    ] {
        let json = serde_json::to_string(&value).expect("a configuration is JSON");
        assert_eq!(
            serde_json::from_str::<Config>(&json).expect("the model reads its own JSON"),
            value,
            "{json}"
        );
    }
    assert_eq!(
        serde_json::to_string(&config(Mode::Compliance, years(7))).expect("json"),
        r#"{"enabled":true,"rule":{"default_retention":{"mode":"COMPLIANCE","period":{"years":7}}}}"#
    );
}

#[test]
fn a_retention_is_the_mode_and_an_rfc3339_instant() {
    let value = lock(
        Mode::Compliance,
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_893_456_000),
        LegalHold::On,
    );
    let json = serde_json::to_string(&value).expect("json");
    assert_eq!(
        json,
        r#"{"retention":{"mode":"COMPLIANCE","retain_until":"2030-01-01T00:00:00Z"},"legal_hold":"ON"}"#
    );
    assert_eq!(
        serde_json::from_str::<Lock>(&json).expect("the model reads its own JSON"),
        value
    );
}

#[test]
fn a_lock_without_a_retention_serializes_a_null_member() {
    let json = serde_json::to_string(&hold_only(LegalHold::Off)).expect("json");
    assert_eq!(json, r#"{"retention":null,"legal_hold":"OFF"}"#);
}

// The stored columns' encoder.

#[test]
fn columns_round_trip_through_from_columns() {
    let until = at(2025, Month::January, 1);
    for value in [
        lock(Mode::Governance, until, LegalHold::Off),
        lock(Mode::Compliance, until, LegalHold::On),
        hold_only(LegalHold::On),
        hold_only(LegalHold::Off),
    ] {
        let (mode, retain_until, legal_hold) = value.columns();
        assert_eq!(
            Lock::from_columns(mode.as_deref(), retain_until, Some(&legal_hold)),
            Ok(wrap(value.clone())),
            "{value:?}"
        );
    }
}

#[test]
fn columns_clamp_an_instant_past_the_i64_range() {
    // 400 years is past the column's ~2262 ceiling; 300 years is past its ~1678 floor.
    let (_, ahead, _) = lock(
        Mode::Governance,
        SystemTime::UNIX_EPOCH + Duration::from_secs(400 * 365 * 86_400),
        LegalHold::Off,
    )
    .columns();
    assert_eq!(ahead, Some(i64::MAX));

    let (_, behind, _) = lock(
        Mode::Governance,
        SystemTime::UNIX_EPOCH - Duration::from_secs(300 * 365 * 86_400),
        LegalHold::Off,
    )
    .columns();
    assert_eq!(behind, Some(i64::MIN));
}
