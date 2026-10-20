    // The matcher and the day arithmetic (LC10).

    /// The fixed instant every matcher test shares: 2023-11-14T10:30:00Z. No
    /// test reads the wall clock, so none of them can flake at a midnight.
    const CREATED: i64 = MIDNIGHT + 10 * 3_600 + 30 * 60;

    /// The instant a `Date` names, for the fixture instants these tests use
    /// (all of which the platform's clock can hold).
    fn at(seconds: i64) -> SystemTime {
        from_unix_seconds(seconds).expect("the fixture's instants are in range")
    }

    /// One object's matched facts: its key, its size, and its tags.
    struct Object {
        key: object::Key,
        tags: object::Tags,
    }

    impl Object {
        fn new(key: &str, tags: &[(&str, &str)]) -> Self {
            Self {
                key: object::key(key).expect("a valid object key"),
                tags: object::Tags::from_pairs(
                    tags.iter()
                        .map(|&(key, value)| (key.to_owned(), value.to_owned())),
                )
                .expect("the fixture's tags are valid"),
            }
        }

        /// The listing facts at `size` bytes, created at [`CREATED`].
        fn facts(&self, size: u64) -> Facts<'_> {
            Facts {
                key: &self.key,
                size,
                last_modified: at(CREATED),
                tags: &self.tags,
            }
        }
    }

    #[test]
    fn prefix_matches_by_prefix() {
        let cfg = one_rule(
            None,
            Filter {
                prefix: Some("logs/".to_owned()),
                ..Filter::default()
            },
        );
        for (key, expected) in [
            ("logs/a.txt", 1),
            ("logs/", 1),
            ("logs/2024/01/a.txt", 1),
            ("logs", 0),
            ("lettuce", 0),
        ] {
            assert_eq!(
                cfg.rules_for(&Object::new(key, &[]).facts(0)).count(),
                expected,
                "{key}"
            );
        }
    }

    #[test]
    fn a_tag_predicate_needs_the_tag() {
        // `tag("k")` is key `k`, value `v`.
        let cfg = one_rule(
            None,
            Filter {
                tag: Some(tag("k")),
                ..Filter::default()
            },
        );
        assert_eq!(
            cfg.rules_for(&Object::new("a", &[("k", "v")]).facts(0))
                .count(),
            1
        );
        // Additional tags do not exclude the object: matching is exact per tag.
        assert_eq!(
            cfg.rules_for(&Object::new("a", &[("k", "v"), ("j", "w")]).facts(0))
                .count(),
            1
        );
        // The key alone is not the predicate: the value is half of it.
        assert_eq!(
            cfg.rules_for(&Object::new("a", &[("k", "other"), ("j", "v")]).facts(0))
                .count(),
            0
        );
    }

    #[test]
    fn an_untagged_fact_never_matches_a_tag_rule() {
        let cfg = one_rule(
            None,
            Filter {
                tag: Some(tag("k")),
                ..Filter::default()
            },
        );
        let untagged = Object::new("a", &[]);
        assert!(untagged.facts(0).tags.is_empty());
        assert_eq!(cfg.rules_for(&untagged.facts(0)).count(), 0);
        assert_eq!(cfg.expiry_for(&untagged.facts(0)), None);
    }

    #[test]
    fn and_requires_every_predicate() {
        let cfg = one_rule(
            None,
            Filter {
                and: Some(And {
                    prefix: Some("logs/".to_owned()),
                    tags: vec![tag("k")],
                    size_greater_than: Some(100),
                    size_less_than: None,
                }),
                ..Filter::default()
            },
        );
        assert_eq!(cfg.validate(), Ok(()));
        let matching = Object::new("logs/a", &[("k", "v")]);
        assert_eq!(cfg.rules_for(&matching.facts(101)).count(), 1);
        // Any single predicate vetoes the match.
        assert_eq!(cfg.rules_for(&matching.facts(100)).count(), 0);
        assert_eq!(
            cfg.rules_for(&Object::new("other/a", &[("k", "v")]).facts(101))
                .count(),
            0
        );
        assert_eq!(
            cfg.rules_for(&Object::new("logs/a", &[]).facts(101))
                .count(),
            0
        );
    }

    #[test]
    fn size_bounds_are_strict() {
        let object = Object::new("a", &[]);
        let greater = one_rule(
            None,
            Filter {
                size_greater_than: Some(100),
                ..Filter::default()
            },
        );
        assert_eq!(greater.rules_for(&object.facts(100)).count(), 0);
        assert_eq!(greater.rules_for(&object.facts(101)).count(), 1);
        let less = one_rule(
            None,
            Filter {
                size_less_than: Some(100),
                ..Filter::default()
            },
        );
        assert_eq!(less.rules_for(&object.facts(100)).count(), 0);
        assert_eq!(less.rules_for(&object.facts(99)).count(), 1);
    }

    #[test]
    fn a_disabled_rule_never_matches() {
        let disabled = Rule {
            status: Status::Disabled,
            expiration: expiration(Some(RULE_DAYS)),
            noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                noncurrent_days: Some(1),
                newer_noncurrent_versions: None,
            }),
            abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                days_after_initiation: 1,
            }),
            ..rule(None, Filter::default(), None)
        };
        let cfg = Config {
            rules: vec![disabled],
        };
        assert_eq!(cfg.validate(), Ok(()));
        let object = Object::new("a", &[]);
        let facts = object.facts(0);
        // The filter is empty, so only the status keeps these four at bay.
        assert_eq!(cfg.rules_for(&facts).count(), 0);
        assert_eq!(cfg.expiry_for(&facts), None);
        assert_eq!(cfg.noncurrent_plans(&facts).count(), 0);
        assert_eq!(cfg.abort_after(&facts), None);
    }

    #[test]
    fn the_earliest_expiry_wins() {
        let later = rule(
            Some("later"),
            Filter::default(),
            Some(Expiration {
                days: None,
                date: Some(MIDNIGHT + 30 * DAY),
                expired_object_delete_marker: None,
            }),
        );
        let earlier = rule(Some("earlier"), Filter::default(), expiration(Some(1)));
        let marker_only = rule(
            Some("marker"),
            Filter::default(),
            Some(Expiration {
                days: None,
                date: None,
                expired_object_delete_marker: Some(true),
            }),
        );
        // The vector's order is the opposite of the dates on purpose: a
        // matcher that took the first matching rule would answer `later`.
        let cfg = Config {
            rules: vec![later, earlier, marker_only],
        };
        assert_eq!(cfg.validate(), Ok(()));
        let object = Object::new("a", &[]);
        let facts = object.facts(0);
        // `days = 1` from 2023-11-14T10:30Z rounds to the midnight of the 16th.
        assert_eq!(cfg.expiry_for(&facts), Some(at(MIDNIGHT + 2 * DAY)));

        // A `Date` on its own is that absolute instant, and a rule carrying
        // only `ExpiredObjectDeleteMarker` names no instant at all (the
        // `min` above would otherwise answer the epoch).
        let dated = Config {
            rules: vec![rule(
                Some("dated"),
                Filter::default(),
                Some(Expiration {
                    days: None,
                    date: Some(MIDNIGHT + 30 * DAY),
                    expired_object_delete_marker: None,
                }),
            )],
        };
        assert_eq!(dated.expiry_for(&facts), Some(at(MIDNIGHT + 30 * DAY)));
    }

    #[test]
    fn days_round_up_to_the_next_midnight() {
        // AWS's own example: 10:30 plus 3 days rounds to the 4th midnight.
        assert_eq!(midnight_utc(at(CREATED), 3), at(MIDNIGHT + 4 * DAY));
        // One second before a midnight, one day rounds past it, not onto it.
        assert_eq!(midnight_utc(at(MIDNIGHT - 1), 1), at(MIDNIGHT + DAY));
    }

    #[test]
    fn zero_days_pins_the_arithmetic_not_a_rule_shape() {
        // Zero is the arithmetic's own edge, not a shape a rule can carry:
        // `Expiration.Days` is at least 1 (`validate`), and the count-only
        // noncurrent form is an age the caller compares, never an instant
        // through here. `midnight_utc` still rounds it forward.
        assert_eq!(midnight_utc(at(MIDNIGHT - 1), 0), at(MIDNIGHT));
        assert_eq!(midnight_utc(at(MIDNIGHT), 0), at(MIDNIGHT + DAY));
    }

    #[test]
    fn a_date_outside_the_clock_range_never_panics() {
        // `validate` floors a `Date` only at midnight UTC, so a stored value can
        // be far outside the platform's clock range (Windows starts at 1601 and
        // stops in 30828). Where the platform can hold the instant, it is on the
        // side the value names — never wrapped — and either way there is no
        // panic.
        for date in [
            i64::MIN,
            -1_000_000_000_000_000,
            1_000_000_000_000_000,
            i64::MAX,
        ] {
            if let Some(instant) = from_unix_seconds(date) {
                assert_eq!(instant >= SystemTime::UNIX_EPOCH, date >= 0, "{date}");
            }
        }
    }

    #[test]
    fn a_date_the_clock_cannot_hold_names_no_instant() {
        // The same out-of-range dates, now asked of the rule that carries them:
        // a date the clock cannot hold names no instant, so that rule expires
        // nothing and the object is retained.
        let object = Object::new("a", &[]);
        for date in [92_233_720_368_000, -92_233_720_368_000] {
            let cfg = Config {
                rules: vec![rule(
                    None,
                    Filter::default(),
                    Some(Expiration {
                        days: None,
                        date: Some(date),
                        expired_object_delete_marker: None,
                    }),
                )],
            };
            assert_eq!(
                cfg.expiry_for(&object.facts(0)),
                from_unix_seconds(date),
                "{date}"
            );
        }

        // Nor can such a rule win the min: the matching days rule still sets the
        // expiry, so nothing expires at the epoch. The date here is `i64::MAX`,
        // which no clock can hold as an instant, so the "this rule names none"
        // branch is what runs rather than a representable far-future value.
        let cfg = Config {
            rules: vec![
                rule(
                    None,
                    Filter::default(),
                    Some(Expiration {
                        days: None,
                        date: Some(i64::MAX),
                        expired_object_delete_marker: None,
                    }),
                ),
                rule(None, Filter::default(), expiration(Some(1))),
            ],
        };
        assert_eq!(
            cfg.expiry_for(&object.facts(0)),
            Some(at(MIDNIGHT + 2 * DAY))
        );

        // A date the clock *can* hold still expires at that instant, however far
        // out. The second value is the discriminating one: year 3000 is inside
        // the calendar too, so only a date past the calendar's own year-9999
        // ceiling catches an over-fix that routes the conversion through
        // `OffsetDateTime` instead of plain checked arithmetic.
        let dated = |date: i64| Config {
            rules: vec![rule(
                None,
                Filter::default(),
                Some(Expiration {
                    days: None,
                    date: Some(date),
                    expired_object_delete_marker: None,
                }),
            )],
        };
        for date in [32_503_680_000, 8_000_000 * DAY] {
            assert_eq!(
                dated(date).expiry_for(&object.facts(0)),
                Some(SystemTime::UNIX_EPOCH + Duration::from_secs(date.unsigned_abs())),
                "{date}"
            );
        }
    }

    #[test]
    fn a_count_only_plan_is_zero_days() {
        // MinIO's documented extension, accepted per LC10: keep 3 noncurrent
        // versions and no day count means zero days, so a version past the
        // count is due at once.
        let cfg = Config {
            rules: vec![Rule {
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: None,
                    newer_noncurrent_versions: Some(3),
                }),
                ..rule(
                    None,
                    Filter {
                        prefix: Some("logs/".to_owned()),
                        ..Filter::default()
                    },
                    None,
                )
            }],
        };
        assert_eq!(cfg.validate(), Ok(()));
        assert_eq!(
            cfg.noncurrent_plans(&Object::new("logs/a", &[]).facts(0))
                .collect::<Vec<_>>(),
            vec![NoncurrentPlan {
                days: 0,
                keep: Some(3)
            }]
        );
        assert_eq!(
            cfg.noncurrent_plans(&Object::new("other/a", &[]).facts(0))
                .count(),
            0
        );
    }

    #[test]
    fn both_thresholds_are_required() {
        // AWS deletes a noncurrent version only when *both* of a rule's
        // thresholds are exceeded, and decides that per rule: the union of the
        // rules' answers, never a merged pair.
        let scoped = || Filter {
            prefix: Some("logs/".to_owned()),
            ..Filter::default()
        };
        let rule_with = |noncurrent: NoncurrentVersionExpiration| Rule {
            noncurrent_version_expiration: Some(noncurrent),
            ..rule(None, scoped(), None)
        };
        // One rule states its own pair; an absent count is "no keep count".
        let one = Config {
            rules: vec![rule_with(NoncurrentVersionExpiration {
                noncurrent_days: Some(30),
                newer_noncurrent_versions: None,
            })],
        };
        assert_eq!(
            one.noncurrent_plans(&Object::new("logs/a", &[]).facts(0))
                .collect::<Vec<_>>(),
            vec![NoncurrentPlan {
                days: 30,
                keep: None
            }]
        );

        // A allows 10 days and keeps 5, B allows 30 days and keeps 2. A version
        // 20 days old with 3 newer versions is past A's age but within A's keep
        // count, and short of B's age — so no rule lets it go, although a
        // merged `{days: 10, keep: 2}` would.
        let cfg = Config {
            rules: vec![
                rule_with(NoncurrentVersionExpiration {
                    noncurrent_days: Some(10),
                    newer_noncurrent_versions: Some(5),
                }),
                rule_with(NoncurrentVersionExpiration {
                    noncurrent_days: Some(30),
                    newer_noncurrent_versions: Some(2),
                }),
            ],
        };
        assert_eq!(cfg.validate(), Ok(()));
        let plans: Vec<NoncurrentPlan> = cfg
            .noncurrent_plans(&Object::new("logs/a", &[]).facts(0))
            .collect();
        assert_eq!(
            plans,
            vec![
                NoncurrentPlan {
                    days: 10,
                    keep: Some(5)
                },
                NoncurrentPlan {
                    days: 30,
                    keep: Some(2)
                },
            ]
        );
        // The question the pass asks of the union: does any rule let it go?
        let due = |age_days: u32, newer: u32| {
            plans
                .iter()
                .any(|plan| age_days >= plan.days && plan.keep.is_none_or(|keep| newer >= keep))
        };
        assert!(!due(20, 3), "no rule authorizes this version: {plans:?}");
        // Past both of B's thresholds, so B authorizes it.
        assert!(due(31, 3), "{plans:?}");
    }

    #[test]
    fn abort_after_takes_the_smallest_days() {
        let abort = |days: i32| Rule {
            abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                days_after_initiation: days,
            }),
            ..rule(
                None,
                Filter {
                    prefix: Some("logs/".to_owned()),
                    ..Filter::default()
                },
                None,
            )
        };
        let cfg = Config {
            rules: vec![abort(30), abort(7)],
        };
        assert_eq!(cfg.validate(), Ok(()));
        assert_eq!(
            cfg.abort_after(&Object::new("logs/a", &[]).facts(0)),
            Some(7)
        );
        assert_eq!(cfg.abort_after(&Object::new("other/a", &[]).facts(0)), None);
    }

    #[test]
    fn a_noncurrent_rule_governs_its_action() {
        let carrying = |status: Status| Config {
            rules: vec![Rule {
                status,
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(30),
                    newer_noncurrent_versions: None,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        assert!(carrying(Status::Enabled).governs_noncurrent_version_expiration());
        assert!(
            !carrying(Status::Disabled).governs_noncurrent_version_expiration(),
            "a Disabled rule is stored and governs nothing"
        );
        assert!(
            !one_rule(None, Filter::default()).governs_noncurrent_version_expiration(),
            "an Enabled rule carrying only Expiration governs another action"
        );
    }

    #[test]
    fn an_abort_rule_governs_its_action() {
        let carrying = |status: Status| Config {
            rules: vec![Rule {
                status,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        assert!(carrying(Status::Enabled).governs_abort_incomplete_multipart_upload());
        assert!(
            !carrying(Status::Disabled).governs_abort_incomplete_multipart_upload(),
            "a Disabled rule is stored and governs nothing"
        );
        assert!(
            !carrying(Status::Enabled).governs_noncurrent_version_expiration(),
            "the abandoned-upload action is not the noncurrent one"
        );
        assert!(
            !one_rule(None, Filter::default()).governs_abort_incomplete_multipart_upload(),
            "an Enabled rule carrying only Expiration governs another action"
        );
    }

    #[test]
    fn an_expiration_rule_governs_its_action() {
        let carrying = |status: Status, expiration: Option<Expiration>| Config {
            rules: vec![Rule {
                status,
                ..rule(None, Filter::default(), expiration)
            }],
        };
        assert!(
            one_rule(None, Filter::default()).governs_expiration(),
            "a Days rule expires objects"
        );
        assert!(
            carrying(
                Status::Enabled,
                Some(Expiration {
                    days: None,
                    date: Some(86_400),
                    expired_object_delete_marker: None,
                }),
            )
            .governs_expiration(),
            "a Date rule expires objects too"
        );
        assert!(
            !carrying(Status::Disabled, expiration(Some(RULE_DAYS))).governs_expiration(),
            "a Disabled rule is stored and governs nothing"
        );
        let aborting = Config {
            rules: vec![Rule {
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        assert!(
            !aborting.governs_expiration(),
            "an Enabled rule carrying only the abandoned-upload action governs another action"
        );
    }

    #[test]
    fn a_delete_marker_rule_governs_its_action() {
        let carrying = |status: Status, marker: Option<bool>| Config {
            rules: vec![Rule {
                status,
                expiration: Some(Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: marker,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        assert!(carrying(Status::Enabled, Some(true)).governs_expired_object_delete_marker());
        assert!(
            !carrying(Status::Disabled, Some(true)).governs_expired_object_delete_marker(),
            "a Disabled rule is stored and governs nothing"
        );
        assert!(
            !carrying(Status::Enabled, Some(false)).governs_expired_object_delete_marker(),
            "Some(false) takes no action, so it governs nothing"
        );
        assert!(
            !one_rule(None, Filter::default()).governs_expired_object_delete_marker(),
            "an Enabled rule carrying only Days governs another action"
        );
    }
