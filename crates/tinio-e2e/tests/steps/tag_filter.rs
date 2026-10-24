//! The runner's default tag filter: the platform-tag rule, and the exclusion
//! string composed from it.
//!
//! Shared by the cucumber binary (`tests/cucumber.rs` writes
//! `CUCUMBER_FILTER_TAGS` from [`default_filter`]) and the `traceability`
//! target, which `#[path]`-includes this file so the composition's unit tests
//! ride its libtest, because the cucumber binary is `harness = false` and
//! carries no test harness.
//!
//! **The rule** (`plans/2026-09-29-sandbox-bdd-e2e.md` §3): a scenario
//! carrying platform tags runs on a host whose target matches **at least one**
//! of them, and is excluded when none does. `@unix` is the umbrella for both
//! unix platforms, so it keeps its meaning — excluded on Windows, kept on
//! Linux and macOS — and a scenario carrying `@macos @windows` runs on both
//! launcher platforms and nowhere else.
//!
//! The rule needs one clause, not one exclusion per tag: a list of
//! `not <tag>` clauses excludes a scenario carrying *any* excluded tag, so
//! `not @windows` on macOS would drop the `@macos @windows` scenario the rule
//! keeps. [`platform_clause`] is the "carries a platform tag but none of
//! mine" test instead.

use tinio_util::switch::parse_switch;

/// The three states of `TINIO_E2E_CONFINEMENT`: the variable is a switch with
/// a value that matters, not a presence test.
///
/// Three states rather than a bool because "unset" and "on" must stay
/// distinguishable: a macOS CI pass forces the family *out* of the parallel
/// default run ([`Confinement::Exclude`]), where that host's own default would
/// keep it, while a Linux or Windows operator forces it *in*
/// ([`Confinement::Include`]) — the host default is the state neither of them
/// spells.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Confinement {
    /// The variable is unset: the target's own default — `@confinement` is
    /// kept on macOS and excluded everywhere else.
    Platform,
    /// `1`/`true`: the family is kept on every target.
    Include,
    /// `0`/`false`: the family is excluded on every target, macOS included.
    Exclude,
}

/// Parse a `TINIO_E2E_CONFINEMENT` value: `None` (the variable unset) is
/// [`Confinement::Platform`], `1`/`true` is [`Confinement::Include`] and
/// `0`/`false` is [`Confinement::Exclude`].
///
/// **Anything else is refused, never defaulted** — the grammar and the refusal
/// shape are `tinio_util::switch::parse_switch`'s, the one definer behind
/// `TINIO_SANDBOX`/`TINIO_READ_ONLY`: a typo in a switch that decides what runs
/// must stop the run rather than pick a side (the plan's G7 rule).
pub fn parse_confinement(raw: Option<&str>) -> Result<Confinement, String> {
    match raw {
        None => Ok(Confinement::Platform),
        Some(value) => match parse_switch(value)? {
            true => Ok(Confinement::Include),
            false => Ok(Confinement::Exclude),
        },
    }
}

/// One platform tag and the targets it may run on: a host whose target is in
/// the set matches the tag, every other host does not (`@linux`, `@macos` and
/// `@windows` are exclusive; `@unix` is both unix platforms).
const PLATFORM_TAGS: &[(&str, &[&str])] = &[
    ("@linux", &["linux"]),
    ("@macos", &["macos"]),
    ("@windows", &["windows"]),
    ("@unix", &["linux", "macos"]),
];

/// The platform axis for `target_os`: the rule as one clause, excluding the
/// scenarios that carry a platform tag none of which matches the target.
///
/// The parentheses are load-bearing: in a tag expression `and` binds tighter
/// than `or`, so an unwrapped tag list would make the conjunction bind to the
/// list's last tag (`@unix` here) instead of to the whole "carries a platform
/// tag" test.
///
/// A target that matches no platform tag (neither linux, macos nor windows)
/// excludes every platform-bound scenario, `@unix` included — the clause is
/// then the "carries a platform tag" test alone.
fn platform_clause(target_os: &str) -> String {
    let any = PLATFORM_TAGS
        .iter()
        .map(|(tag, _)| *tag)
        .collect::<Vec<_>>()
        .join(" or ");
    let matching = PLATFORM_TAGS
        .iter()
        .filter(|(_, targets)| targets.contains(&target_os))
        .map(|(tag, _)| *tag)
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [] => format!("not ({any})"),
        [tag] => format!("not (({any}) and not {tag})"),
        tags => format!("not (({any}) and not ({}))", tags.join(" or ")),
    }
}

/// The exclusion string for a build: a conjunction of clauses, empty when
/// nothing is excluded (the caller then leaves `CUCUMBER_FILTER_TAGS` unset,
/// which is cucumber's own default).
///
/// Six independent axes, in composition order: `@parquet` unless the parquet
/// cargo feature is on; `@object-lock` unless the object-lock cargo feature is
/// on (the corpus compiles against the server's gated surface, so a build
/// without it can only answer NotImplemented); the platform axis
/// ([`platform_clause`]); the `@external` umbrella (`@interop`/`@boto3`/`@mc`)
/// unless the external opt-in is set; `@confinement`, which follows the
/// [`Confinement`] state — the target's own default (excluded off macOS) when
/// the variable is unset, and the explicit states forcing it in or out on every
/// target; and `@pg`, always excluded from the default run (PG17) — the `@pg`
/// pass is its own run, driven by an explicit `--tags` (which is also what sets
/// `TINIO_E2E_BACKEND=pg`), and no other pass wants the DB-backed scenarios.
///
/// The target, the three opt-in flags and the confinement state are parameters
/// rather than `cfg!`/env reads, so one function composes every host's filter
/// and the composition is assertable without a host of each kind
/// (`tests/traceability.rs` includes this file for exactly that). `@pg` is not
/// a parameter: it is excluded from every default run, whatever the host.
pub fn default_filter(
    target_os: &str,
    parquet: bool,
    object_lock: bool,
    external: bool,
    confinement: Confinement,
) -> String {
    let mut excluded: Vec<String> = Vec::new();
    if !parquet {
        excluded.push("not @parquet".to_owned());
    }
    if !object_lock {
        excluded.push("not @object-lock".to_owned());
    }
    excluded.push(platform_clause(target_os));
    if !external {
        excluded.push("not @interop and not @boto3 and not @mc".to_owned());
    }
    let confinement_excluded = match confinement {
        Confinement::Platform => target_os != "macos",
        Confinement::Include => false,
        Confinement::Exclude => true,
    };
    if confinement_excluded {
        excluded.push("not @confinement".to_owned());
    }
    excluded.push("not @pg".to_owned());
    excluded.join(" and ")
}

#[cfg(test)]
mod tests {
    //! The platform rule's own legs: the literal filter each target composes,
    //! and what cucumber's own parser and evaluator select with it.

    use cucumber::{gherkin::tagexpr::TagOperation, tag::Ext as _};

    // `parse_confinement` is reached only by the refusal test, which the
    // cucumber binary compiles but never runs (harness disabled — the same
    // reason `selected` carries its dead-code allow below).
    #[allow(
        unused_imports,
        clippy::allow_attributes,
        reason = "test support: `parse_confinement` is reached only by the refusal test, which the traceability binary runs but the cucumber binary compiles without running (harness disabled) — one attribute cannot be `expect` in both"
    )]
    use super::{Confinement, default_filter, parse_confinement};

    /// The filter a target composes when only the platform axis can exclude
    /// anything, parsed and evaluated against one scenario's tag set by the
    /// runner's own code — the syntax and the meaning are both asserted, and
    /// neither is re-implemented here. A scenario's parsed tags keep no `@`
    /// (gherkin's `tag()` strips it), so the literals below are stripped too.
    /// `allow(dead_code)`: the tests module also compiles into the cucumber
    /// test binary (harness disabled — never runs there), where no `#[test]`
    /// reaches this helper.
    #[allow(
        dead_code,
        clippy::allow_attributes,
        reason = "test support: no `#[test]` reaches this helper in the cucumber binary (harness disabled — never runs there), while the traceability binary's tests do — one attribute cannot be `expect` in both"
    )]
    fn selected(target_os: &str, tags: &[&str]) -> bool {
        let filter: TagOperation =
            default_filter(target_os, true, true, true, Confinement::Include)
                .parse()
                .unwrap_or_else(|e| panic!("the filter for {target_os} parses: {e}"));
        filter.eval(tags.iter().map(|tag| tag.trim_start_matches('@')))
    }

    /// The platform axis alone (parquet on, both opt-ins set), target by
    /// target: the exclusion set is the "no tag of mine" test, never a list of
    /// `not <tag>` clauses.
    #[test]
    fn the_platform_axis_is_the_rule_for_each_target() {
        for (target, expected) in [
            (
                "linux",
                "not ((@linux or @macos or @windows or @unix) and not (@linux or @unix)) and not @pg",
            ),
            (
                "macos",
                "not ((@linux or @macos or @windows or @unix) and not (@macos or @unix)) and not @pg",
            ),
            (
                "windows",
                "not ((@linux or @macos or @windows or @unix) and not @windows) and not @pg",
            ),
        ] {
            assert_eq!(
                default_filter(target, true, true, true, Confinement::Include),
                expected,
                "the composed filter for {target}"
            );
        }
    }

    /// The rule's consequences for one scenario per tag.
    #[test]
    fn a_scenario_runs_where_one_of_its_platform_tags_matches() {
        for target in ["linux", "macos", "windows"] {
            let own = format!("@{target}");
            // No platform tag: the filter never touches the scenario.
            assert!(selected(target, &[]), "an untagged scenario on {target}");
            assert!(
                selected(target, &["@fs", "@cli"]),
                "a scenario with no platform tag on {target}"
            );
            // The host's own tag.
            assert!(selected(target, &[own.as_str()]), "the {own} scenario");
            // Every other platform's tag, `@unix` excepted (below).
            for other in ["linux", "macos", "windows"] {
                if other == target {
                    continue;
                }
                let tag = format!("@{other}");
                assert!(
                    !selected(target, &[tag.as_str()]),
                    "the {tag} scenario on {target}"
                );
            }
        }
    }

    /// `@unix` is both unix platforms: kept on Linux and macOS, out on Windows.
    #[test]
    fn unix_is_both_unix_platforms() {
        for (target, selected_on) in [("linux", true), ("macos", true), ("windows", false)] {
            assert_eq!(
                selected(target, &["@unix"]),
                selected_on,
                "the @unix scenario on {target}"
            );
        }
    }

    /// The case the old single-exclusion rule could not express: one scenario,
    /// two launcher-platform tags, running on both of them.
    #[test]
    fn a_shared_launcher_scenario_runs_on_both_launcher_platforms() {
        for (target, selected_on) in [("linux", false), ("macos", true), ("windows", true)] {
            assert_eq!(
                selected(target, &["@macos", "@windows"]),
                selected_on,
                "the @macos @windows scenario on {target}"
            );
        }
    }

    /// A target that is neither linux, macos nor windows matches no platform
    /// tag, so no platform-bound scenario runs there.
    #[test]
    fn another_target_selects_no_platform_bound_scenario() {
        for tag in ["@linux", "@macos", "@windows", "@unix"] {
            assert!(
                !selected("freebsd", &[tag]),
                "the {tag} scenario on a host that matches no platform tag"
            );
        }
        assert!(
            selected("freebsd", &["@fs"]),
            "a scenario with no platform tag on a host that matches no platform tag"
        );
    }

    /// The three non-platform axes follow their own inputs unchanged.
    #[test]
    fn the_other_axes_follow_their_inputs() {
        assert_eq!(
            default_filter("linux", false, true, false, Confinement::Platform),
            "not @parquet and not ((@linux or @macos or @windows or @unix) and not (@linux or @unix)) and not @interop and not @boto3 and not @mc and not @confinement and not @pg"
        );
        // `@confinement` follows the state, not the host alone: on macOS the
        // platform default emits nothing for the family, and the `0` state
        // emits the exclusion — the two composed strings, whole.
        assert_eq!(
            default_filter("macos", true, true, true, Confinement::Platform),
            "not ((@linux or @macos or @windows or @unix) and not (@macos or @unix)) and not @pg"
        );
        assert_eq!(
            default_filter("macos", true, true, true, Confinement::Exclude),
            "not ((@linux or @macos or @windows or @unix) and not (@macos or @unix)) and not @confinement and not @pg"
        );
        assert!(
            !default_filter("macos", true, true, true, Confinement::Include)
                .contains("@confinement")
        );
        assert!(
            default_filter("windows", true, true, true, Confinement::Platform)
                .contains("not @confinement")
        );
        assert!(
            !default_filter("windows", true, true, true, Confinement::Include)
                .contains("@confinement")
        );
        assert!(
            default_filter("windows", true, true, true, Confinement::Exclude)
                .contains("not @confinement")
        );
    }

    /// The confinement axis is three-state on every target: the platform
    /// default, then the two explicit states overriding it either way.
    #[test]
    fn the_confinement_axis_has_three_states() {
        fn excluded(target_os: &str, confinement: Confinement) -> bool {
            default_filter(target_os, true, true, true, confinement).contains("not @confinement")
        }
        for (target, platform_excludes) in [("linux", true), ("macos", false), ("windows", true)] {
            assert_eq!(
                excluded(target, Confinement::Platform),
                platform_excludes,
                "the platform default on {target}"
            );
            assert!(
                !excluded(target, Confinement::Include),
                "forced in on {target}"
            );
            assert!(
                excluded(target, Confinement::Exclude),
                "forced out on {target}"
            );
        }
    }

    /// The `@pg` axis is the fifth, and unlike the other four it follows no input: it is excluded
    /// from every default run, whatever the host and whatever the opt-ins say, because the DB-backed
    /// pass is its own explicit-`--tags` run (PG17). `selected` composes with parquet on and both
    /// opt-ins set, so only this axis can drop a `@pg` scenario.
    #[test]
    fn the_pg_axis_is_excluded_from_every_default_run() {
        for target in ["linux", "macos", "windows"] {
            let filter = default_filter(target, true, true, true, Confinement::Include);
            assert!(
                filter.ends_with("not @pg"),
                "the composed filter for {target}: {filter}"
            );
            assert!(!selected(target, &["@pg"]), "a @pg scenario on {target}");
            // The axis drops only `@pg`: a backend-neutral scenario stays selected.
            assert!(
                selected(target, &["@fs", "@cli"]),
                "a neutral scenario on {target}"
            );
        }
    }

    /// The `@lifecycle` corpus is a subject of this suite like any other: no
    /// axis names it, so every backend-neutral lifecycle scenario runs in the
    /// default pass.
    #[test]
    fn the_lifecycle_corpus_runs_by_default() {
        for target in ["linux", "macos", "windows"] {
            let filter = default_filter(target, true, true, true, Confinement::Include);
            assert!(!filter.contains("@lifecycle"), "the filter for {target}");
            assert!(
                selected(target, &["@lifecycle"]),
                "a @lifecycle scenario on {target}"
            );
        }
    }

    /// The `@object-lock` corpus follows its cargo feature, the way `@parquet`
    /// does: without `--features object-lock` the server's six-operation
    /// surface is compiled out, so the default run excludes the corpus rather
    /// than failing every scenario on a 501.
    #[test]
    fn the_object_lock_corpus_follows_its_cargo_feature() {
        for target in ["linux", "macos", "windows"] {
            let off = default_filter(target, true, false, true, Confinement::Include);
            assert!(
                off.contains("not @object-lock"),
                "the filter for {target}: {off}"
            );
            let on = default_filter(target, true, true, true, Confinement::Include);
            assert!(
                !on.contains("@object-lock"),
                "the filter for {target}: {on}"
            );
        }
    }

    /// The switch's grammar: unset is the platform default, the four tolerant
    /// spellings parse, and everything else is refused with the offending value
    /// quoted — never defaulted to one of the states.
    #[test]
    fn the_confinement_switch_parses_or_refuses() {
        assert_eq!(
            parse_confinement(None).unwrap(),
            Confinement::Platform,
            "an unset variable"
        );
        for raw in ["1", "true", " 1 ", "\ttrue\n"] {
            assert_eq!(
                parse_confinement(Some(raw)).unwrap(),
                Confinement::Include,
                "{raw:?}"
            );
        }
        for raw in ["0", "false", " 0 "] {
            assert_eq!(
                parse_confinement(Some(raw)).unwrap(),
                Confinement::Exclude,
                "{raw:?}"
            );
        }
        for raw in ["", "2", "yes", "TRUE", "on"] {
            let refusal = parse_confinement(Some(raw)).unwrap_err();
            assert!(refusal.contains("0/1/true/false"), "{raw:?}: {refusal}");
            assert!(refusal.contains(raw), "{raw:?}: {refusal}");
        }
    }
}
