//! The verdict rule: what a series of interleaved rounds is allowed to say.
//!
//! The protocol's rule (`docs/benches.md`) is that **a single run never
//! establishes a difference**, and that the answer to "did this change make
//! the operation slower" is an *interval* until the rounds agree on a side.
//! This module is that rule, as arithmetic over what the legs reported.
//!
//! A [`Round`] is one back-to-back run of both legs, each in its own process,
//! each carrying its own flush control. Three things are then decided, in
//! order:
//!
//! 1. **Is the round usable?** The two legs of a round ran a few seconds
//!    apart, so they are only comparable if the host's flush regime held
//!    across both: a round whose two flush controls differ by more than
//!    [`Rules::flush_band`] is dropped, and the drop is reported rather than
//!    hidden.
//! 2. **Is there an interval?** The surviving rounds give one ratio each
//!    (leg B over leg A); their minimum and maximum are the interval the
//!    verdict is reported with. Fewer than [`Rules::min_rounds`] surviving
//!    rounds is [`Verdict::Unmeasurable`], not a weak verdict.
//! 3. **Which side?** Every surviving round on the same side of one is
//!    [`Verdict::Resolved`] — the sign of the difference survived the
//!    interleaving, which is all this rule ever claims. An interval that
//!    swallows one is [`Verdict::NoDifference`]: on this host, with these
//!    rounds, the two legs are not separable.
//!
//! What no verdict here claims is *causation*. Two legs are two binaries; a
//! stable difference may come from the change under test or from anything else
//! that differs between the two builds, and the rounds cannot tell them apart.
//! The allocation channel is the one number beside the times that a change of
//! the host cannot fake — it is reported separately, as a fact about the legs
//! rather than as a verdict about them.

/// One round: both legs run back to back, each in its own process.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Round {
    /// Leg A's median, nanoseconds per operation.
    pub a_ns: f64,
    /// Leg B's median, nanoseconds per operation.
    pub b_ns: f64,
    /// Leg A's flush control, nanoseconds.
    pub a_flush_ns: f64,
    /// Leg B's flush control, nanoseconds.
    pub b_flush_ns: f64,
    /// Leg A's allocation requests per operation.
    pub a_allocs: f64,
    /// Leg B's allocation requests per operation.
    pub b_allocs: f64,
}

/// The thresholds a decision is made against; [`Rules::default`] is what the
/// protocol prescribes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rules {
    /// The factor by which a round's two flush controls may differ before the
    /// round is dropped as regime-mixed.
    pub flush_band: f64,
    /// The surviving rounds a verdict needs.
    pub min_rounds: usize,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            flush_band: 2.0,
            min_rounds: 5,
        }
    }
}

/// What the surviving rounds say about the two legs.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict {
    /// Every surviving round put leg B on one side of leg A.
    Resolved {
        /// The median ratio, leg B over leg A.
        ratio: f64,
        /// The lowest ratio of any surviving round.
        lo: f64,
        /// The highest ratio of any surviving round.
        hi: f64,
    },
    /// At least one surviving round put leg B above one and at least one put it
    /// below: the interval spans "no difference".
    NoDifference {
        /// The lowest ratio of any surviving round.
        lo: f64,
        /// The highest ratio of any surviving round.
        hi: f64,
    },
    /// The rounds were not enough to say anything, and why.
    Unmeasurable(&'static str),
}

/// Why a round was dropped: its two legs did not run in one flush regime.
pub const MIXED_REGIME: &str =
    "the legs' flush controls differ by more than the band — the two runs were not in one regime";

/// Why a round was dropped: a leg reported a time no operation can have.
pub const NON_POSITIVE_TIME: &str =
    "a leg reported a non-positive time — the reading is not a measurement";

/// Why the verdict is [`Verdict::Unmeasurable`]: too few rounds survived.
pub const TOO_FEW_ROUNDS: &str =
    "too few rounds survived to say anything — rerun with more rounds when the host is quiet";

/// The verdict and the evidence behind it.
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    /// The rounds that counted, by their 1-based number.
    pub valid: Vec<usize>,
    /// The rounds that did not, by their 1-based number, with the reason.
    pub discarded: Vec<(usize, &'static str)>,
    /// The ratio series over the surviving rounds, leg B over leg A.
    pub ratios: Vec<f64>,
    /// The verdict.
    pub verdict: Verdict,
    /// The legs' allocation counts per operation, when they differ: an exact
    /// difference the flush regime cannot move.
    pub allocation_delta: Option<(f64, f64)>,
    /// The run's flush controls over the surviving rounds, lowest and highest,
    /// when the run spans more than the band — the numbers are still
    /// interleaved-valid, but no stored baseline from another regime is
    /// comparable with them.
    pub regime_span: Option<(f64, f64)>,
}

/// Decide what `rounds` say, against `rules`.
pub fn decide(rounds: &[Round], rules: &Rules) -> Decision {
    let mut valid = Vec::new();
    let mut discarded = Vec::new();
    let mut ratios = Vec::new();
    for (index, round) in rounds.iter().enumerate() {
        let number = index + 1;
        if round.a_ns <= 0.0 || round.b_ns <= 0.0 {
            discarded.push((number, NON_POSITIVE_TIME));
            continue;
        }
        if !one_regime(round.a_flush_ns, round.b_flush_ns, rules.flush_band) {
            discarded.push((number, MIXED_REGIME));
            continue;
        }
        valid.push(number);
        ratios.push(round.b_ns / round.a_ns);
    }

    let verdict = if valid.len() < rules.min_rounds {
        Verdict::Unmeasurable(TOO_FEW_ROUNDS)
    } else {
        let lo = ratios.iter().copied().fold(f64::INFINITY, f64::min);
        let hi = ratios.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        if lo <= 1.0 && 1.0 <= hi {
            Verdict::NoDifference { lo, hi }
        } else {
            Verdict::Resolved {
                ratio: median(&ratios),
                lo,
                hi,
            }
        }
    };

    Decision {
        allocation_delta: allocation_delta(rounds, &valid),
        regime_span: regime_span(rounds, &valid, rules.flush_band),
        valid,
        discarded,
        ratios,
        verdict,
    }
}

/// Whether two flush controls are close enough to call one regime.
fn one_regime(a: f64, b: f64, band: f64) -> bool {
    let (low, high) = if a <= b { (a, b) } else { (b, a) };
    if low <= 0.0 {
        return high <= 0.0;
    }
    high / low <= band
}

/// The two legs' allocation counts when they differ, over the rounds that
/// counted: an exact difference, and the one the flush regime cannot move.
fn allocation_delta(rounds: &[Round], valid: &[usize]) -> Option<(f64, f64)> {
    let counted = || valid.iter().filter_map(|n| rounds.get(n - 1));
    let a = median(&counted().map(|r| r.a_allocs).collect::<Vec<_>>());
    let b = median(&counted().map(|r| r.b_allocs).collect::<Vec<_>>());
    (a != b).then_some((a, b))
}

/// The flush controls' spread over the rounds that counted, when the run spans
/// more than the band.
fn regime_span(rounds: &[Round], valid: &[usize], band: f64) -> Option<(f64, f64)> {
    let controls: Vec<f64> = valid
        .iter()
        .filter_map(|n| rounds.get(n - 1))
        .flat_map(|r| [r.a_flush_ns, r.b_flush_ns])
        .collect();
    let lo = controls.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = controls.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    (lo > 0.0 && hi / lo > band).then_some((lo, hi))
}

/// The nearest-rank median of an unsorted series; `0.0` for an empty one.
fn median(series: &[f64]) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let mut sorted = series.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::{MIXED_REGIME, Round, Rules, TOO_FEW_ROUNDS, Verdict, decide};

    /// A round whose legs ran in one regime, at the given times and counts.
    fn round(a_ns: f64, b_ns: f64, allocs: (f64, f64)) -> Round {
        Round {
            a_ns,
            b_ns,
            a_flush_ns: 1_000_000.0,
            b_flush_ns: 1_050_000.0,
            a_allocs: allocs.0,
            b_allocs: allocs.1,
        }
    }

    /// `count` rounds, each with the given ratio and a jitter that keeps them
    /// on one side of one.
    fn series(count: usize, ratio: f64) -> Vec<Round> {
        (0..count)
            .map(|i| {
                let jitter = 1.0 + (i as f64 % 5.0) / 1000.0;
                round(1000.0, 1000.0 * ratio * jitter, (0.0, 0.0))
            })
            .collect()
    }

    #[test]
    fn one_sided_rounds_resolve_the_difference() {
        let decision = decide(&series(7, 1.5), &Rules::default());
        let Verdict::Resolved { ratio, lo, hi } = decision.verdict else {
            panic!("seven rounds on one side of one must resolve");
        };
        assert!(ratio > 1.4 && ratio < 1.6, "median ratio {ratio}");
        assert!(lo > 1.0, "the interval must not reach one: {lo}");
        assert!(hi > ratio, "the interval must reach the highest round");
        assert_eq!(decision.valid.len(), 7);
        assert!(decision.discarded.is_empty());
    }

    #[test]
    fn a_ratio_interval_spanning_one_is_no_difference() {
        let rounds = vec![
            round(1000.0, 1010.0, (0.0, 0.0)),
            round(1000.0, 990.0, (0.0, 0.0)),
            round(1000.0, 1005.0, (0.0, 0.0)),
            round(1000.0, 995.0, (0.0, 0.0)),
            round(1000.0, 1002.0, (0.0, 0.0)),
        ];
        let decision = decide(&rounds, &Rules::default());
        let Verdict::NoDifference { lo, hi } = decision.verdict else {
            panic!("a straddling interval must not resolve");
        };
        assert!(lo < 1.0 && hi > 1.0, "[{lo}, {hi}]");
    }

    #[test]
    fn a_regime_mixed_round_is_dropped_and_reported() {
        let mut rounds = series(6, 1.5);
        rounds[2].b_flush_ns = rounds[2].a_flush_ns * 3.0;
        let decision = decide(&rounds, &Rules::default());
        assert_eq!(decision.discarded, vec![(3, MIXED_REGIME)]);
        assert_eq!(decision.valid, vec![1, 2, 4, 5, 6]);
        assert!(matches!(
            decision.verdict,
            Verdict::Resolved { .. } | Verdict::Unmeasurable(_)
        ));
    }

    #[test]
    fn dropping_rounds_below_the_minimum_is_unmeasurable_not_a_weak_verdict() {
        let mut rounds = series(5, 1.5);
        for round in rounds.iter_mut().take(2) {
            round.b_flush_ns = round.a_flush_ns * 10.0;
        }
        let decision = decide(&rounds, &Rules::default());
        assert_eq!(decision.valid.len(), 3);
        assert_eq!(decision.verdict, Verdict::Unmeasurable(TOO_FEW_ROUNDS));
    }

    #[test]
    fn an_allocation_difference_is_reported_beside_the_verdict() {
        let rounds: Vec<Round> = (0..5).map(|_| round(1000.0, 1000.0, (0.0, 4.0))).collect();
        let decision = decide(&rounds, &Rules::default());
        assert_eq!(decision.allocation_delta, Some((0.0, 4.0)));
        assert!(matches!(decision.verdict, Verdict::NoDifference { .. }));
    }

    #[test]
    fn equal_allocation_counts_report_nothing() {
        let decision = decide(&series(5, 1.5), &Rules::default());
        assert_eq!(decision.allocation_delta, None);
    }

    #[test]
    fn a_run_that_spans_regimes_says_so() {
        let mut rounds = series(6, 1.5);
        rounds[5].a_flush_ns = 8_000_000.0;
        rounds[5].b_flush_ns = 8_100_000.0;
        let decision = decide(&rounds, &Rules::default());
        let span = decision.regime_span.expect("the run spans regimes");
        assert!(span.0 <= 1_000_000.0 && span.1 >= 8_000_000.0);
    }

    #[test]
    fn a_non_positive_reading_is_not_a_measurement() {
        let rounds = vec![round(0.0, 1.0, (0.0, 0.0))];
        let decision = decide(&rounds, &Rules::default());
        assert_eq!(decision.discarded.len(), 1);
        assert_eq!(decision.valid.len(), 0);
        assert_eq!(decision.verdict, Verdict::Unmeasurable(TOO_FEW_ROUNDS));
    }
}
