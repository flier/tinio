//! Calendar arithmetic for the timestamps the workspace renders.
//!
//! [`civil_from_days`] is Howard Hinnant's `civil_from_days`: the exact conversion of a days-since-1970-01-01 count into a proleptic Gregorian (year, month, day). The tier verbs' RFC 3339 report spelling and the test signer's SigV4 `YYYYMMDDTHHMMSSZ` timestamp both need it, and each carried its own copy; this is the one definer. It is `std`-only, so the tier verb compiles in a build without the `object-lock` feature (whose `time` dependency formats other timestamps).

/// The civil (year, month, day) date of a count of days since 1970-01-01.
///
/// Negative counts are dates before the epoch. The result is proleptic Gregorian and exact over the whole `i64` range the callers can produce.
///
/// # Examples
///
/// ```
/// use tinio_util::time::civil_from_days;
///
/// assert_eq!(civil_from_days(0), (1970, 1, 1));
/// assert_eq!(civil_from_days(-1), (1969, 12, 31));
/// assert_eq!(civil_from_days(11_017), (2000, 3, 1));
/// ```
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let month_position = (5 * day_of_year + 2) / 153; // [0, 11]
    let day = (day_of_year - (153 * month_position + 2) / 5 + 1) as u32; // [1, 31]
    let month = (if month_position < 10 {
        month_position + 3
    } else {
        month_position - 9
    }) as u32; // [1, 12]
    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch, the day before it, leap-day boundaries and a far date: the four cases where a
    /// hand-rolled conversion goes wrong (the negative shift, February in a leap year, and a
    /// year boundary whose month is January or February).
    #[test]
    fn civil_from_days_matches_the_calendar() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(59), (1970, 3, 1));
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(11_017), (2000, 3, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
    }
}
