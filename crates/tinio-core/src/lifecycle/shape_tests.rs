use crate::Class;

    /// A midnight UTC `Expiration` date (2023-11-14T00:00:00Z). It is in the
    /// past, so the accepted cases also pin that a past date is accepted.
    const MIDNIGHT: i64 = 1_699_920_000;

    /// The `Expiration.days` the helper-built rules carry, so a test names
    /// only the part it is about.
    const RULE_DAYS: i32 = 30;

    /// The day length the fixture arithmetic uses.
    const DAY: i64 = SECONDS_PER_DAY as i64;

    fn expiration(days: Option<i32>) -> Option<Expiration> {
        Some(Expiration {
            days,
            date: None,
            expired_object_delete_marker: None,
        })
    }

    fn rule(id: Option<&str>, filter: Filter, expiration: Option<Expiration>) -> Rule {
        Rule {
            id: id.map(str::to_owned),
            status: Status::Enabled,
            filter,
            expiration,
            transitions: Vec::new(),
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: None,
        }
    }

    fn one_rule(id: Option<&str>, filter: Filter) -> Config {
        Config {
            rules: vec![rule(id, filter, expiration(Some(RULE_DAYS)))],
        }
    }

    fn refusal(cfg: &Config) -> Invalid {
        match cfg.validate() {
            Err(invalid) => invalid,
            Ok(()) => panic!("the configuration must be refused"),
        }
    }

    fn tag(key: &str) -> Tag {
        Tag {
            key: key.to_owned(),
            value: "v".to_owned(),
        }
    }

    #[test]
    fn two_top_level_predicates_are_malformed() {
        let cfg = one_rule(
            Some("two"),
            Filter {
                prefix: Some("logs/".to_owned()),
                size_greater_than: Some(500),
                ..Filter::default()
            },
        );
        let invalid = refusal(&cfg);
        assert_eq!(invalid.reason, FILTER_ARITY);
        assert_eq!(invalid.kind, InvalidKind::Shape);
        assert_eq!(invalid.rule.as_deref(), Some("two"));
    }

    #[test]
    fn an_and_needs_two_predicates() {
        for and in [
            And::default(),
            And {
                prefix: Some("logs/".to_owned()),
                ..And::default()
            },
        ] {
            let cfg = one_rule(
                None,
                Filter {
                    and: Some(and),
                    ..Filter::default()
                },
            );
            let invalid = refusal(&cfg);
            assert_eq!(invalid.reason, AND_ARITY);
            assert_eq!(invalid.kind, InvalidKind::Shape);
        }
    }

    #[test]
    fn a_size_range_needs_and() {
        let cfg = one_rule(
            None,
            Filter {
                size_greater_than: Some(500),
                size_less_than: Some(2000),
                ..Filter::default()
            },
        );
        let invalid = refusal(&cfg);
        assert_eq!(invalid.reason, FILTER_ARITY);
        assert_eq!(invalid.kind, InvalidKind::Shape);

        // The same range inside `And` is the shape AWS accepts.
        let wrapped = one_rule(
            None,
            Filter {
                and: Some(And {
                    size_greater_than: Some(500),
                    size_less_than: Some(2000),
                    ..And::default()
                }),
                ..Filter::default()
            },
        );
        assert_eq!(wrapped.validate(), Ok(()));
    }

    #[test]
    fn the_greater_bound_must_be_below_the_less() {
        for (greater, less) in [(2000, 500), (500, 500)] {
            let cfg = one_rule(
                None,
                Filter {
                    and: Some(And {
                        size_greater_than: Some(greater),
                        size_less_than: Some(less),
                        ..And::default()
                    }),
                    ..Filter::default()
                },
            );
            let invalid = refusal(&cfg);
            assert_eq!(invalid.reason, SIZE_ORDER, "{greater} then {less}");
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
    }

    #[test]
    fn a_size_predicate_stays_under_the_object_ceiling() {
        let cfg = one_rule(
            None,
            Filter {
                size_greater_than: Some(MAX_SIZE + 1),
                ..Filter::default()
            },
        );
        assert_eq!(refusal(&cfg).reason, SIZE_CEILING);
        let at_the_ceiling = one_rule(
            None,
            Filter {
                size_greater_than: Some(MAX_SIZE),
                ..Filter::default()
            },
        );
        assert_eq!(at_the_ceiling.validate(), Ok(()));
    }

    #[test]
    fn duplicate_tags_in_an_and_are_refused() {
        let cfg = one_rule(
            None,
            Filter {
                and: Some(And {
                    prefix: Some("logs/".to_owned()),
                    tags: vec![tag("a"), tag("a")],
                    ..And::default()
                }),
                ..Filter::default()
            },
        );
        assert_eq!(refusal(&cfg).reason, TAG_KEYS_UNIQUE);

        let distinct = one_rule(
            None,
            Filter {
                and: Some(And {
                    prefix: Some("logs/".to_owned()),
                    tags: vec![tag("a"), tag("b")],
                    ..And::default()
                }),
                ..Filter::default()
            },
        );
        assert_eq!(distinct.validate(), Ok(()));
    }

    #[test]
    fn an_empty_filter_is_everything() {
        assert_eq!(one_rule(None, Filter::default()).validate(), Ok(()));
    }

    #[test]
    fn a_rule_needs_at_least_one_action() {
        // An action member that is present but empty is not an action: an
        // `Expiration {}` or a `NoncurrentVersionExpiration {}` (reachable from
        // a plane body and from `<Expiration/>`) would otherwise read as
        // "expire everything" one step later (LC8, LC10).
        let empty_expiration = Config {
            rules: vec![rule(
                None,
                Filter::default(),
                Some(Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: None,
                }),
            )],
        };
        let empty_noncurrent = Config {
            rules: vec![Rule {
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: None,
                    newer_noncurrent_versions: None,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        // `AbortIncompleteMultipartUpload` has no empty form — one plain
        // `days_after_initiation` field, whose floor is
        // `the_action_day_counts_are_at_least_one`'s.
        let no_action = Config {
            rules: vec![rule(None, Filter::default(), None)],
        };
        for cfg in [no_action, empty_expiration, empty_noncurrent] {
            let invalid = refusal(&cfg);
            assert_eq!(invalid.reason, NO_ACTION);
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
    }

    #[test]
    fn a_configuration_carries_at_least_one_rule() {
        let invalid = refusal(&Config { rules: vec![] });
        assert_eq!(invalid.reason, NO_RULES);
        assert_eq!(invalid.kind, InvalidKind::Semantic);
        assert_eq!(invalid.rule, None);
    }

    #[test]
    fn the_action_day_counts_are_at_least_one() {
        let noncurrent = |days: i32| Config {
            rules: vec![Rule {
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(days),
                    newer_noncurrent_versions: None,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        for days in [0, -5] {
            assert_eq!(refusal(&noncurrent(days)).reason, NONCURRENT_DAYS_MIN);
        }
        assert_eq!(noncurrent(1).validate(), Ok(()));

        let abort = |days: i32| Config {
            rules: vec![Rule {
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: days,
                }),
                ..rule(None, Filter::default(), None)
            }],
        };
        for days in [0, -5] {
            assert_eq!(refusal(&abort(days)).reason, ABORT_DAYS_MIN);
        }
        assert_eq!(abort(1).validate(), Ok(()));
    }

    #[test]
    fn several_actions_in_one_rule_are_legal() {
        let cfg = Config {
            rules: vec![Rule {
                id: Some("several".to_owned()),
                status: Status::Enabled,
                filter: Filter::default(),
                expiration: expiration(Some(RULE_DAYS)),
                transitions: Vec::new(),
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(10),
                    newer_noncurrent_versions: None,
                }),
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
            }],
        };
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn id_is_at_most_255_and_unique() {
        let longest = "x".repeat(MAX_RULE_ID);
        assert_eq!(
            one_rule(Some(&longest), Filter::default()).validate(),
            Ok(())
        );

        let too_long = "x".repeat(MAX_RULE_ID + 1);
        let invalid = refusal(&one_rule(Some(&too_long), Filter::default()));
        assert_eq!(invalid.reason, RULE_ID_TOO_LONG);
        assert_eq!(invalid.rule.as_deref(), Some(too_long.as_str()));

        let duplicate = Config {
            rules: vec![
                rule(Some("a"), Filter::default(), expiration(Some(1))),
                rule(Some("a"), Filter::default(), expiration(Some(2))),
            ],
        };
        let invalid = refusal(&duplicate);
        assert_eq!(invalid.reason, RULE_ID_DUPLICATE);
        assert_eq!(invalid.rule.as_deref(), Some("a"));
    }

    #[test]
    fn a_configuration_carries_at_most_a_thousand_rules() {
        let once = rule(None, Filter::default(), expiration(Some(RULE_DAYS)));
        assert_eq!(
            Config {
                rules: vec![once.clone(); MAX_RULES]
            }
            .validate(),
            Ok(())
        );
        assert_eq!(
            refusal(&Config {
                rules: vec![once; MAX_RULES + 1]
            })
            .reason,
            TOO_MANY_RULES
        );
    }

    #[test]
    fn days_is_at_least_one() {
        for days in [0, -1] {
            let invalid = refusal(&Config {
                rules: vec![rule(None, Filter::default(), expiration(Some(days)))],
            });
            assert_eq!(invalid.reason, DAYS_MIN, "Expiration Days {days}");
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
    }

    #[test]
    fn a_date_must_be_midnight_utc() {
        assert_eq!(MIDNIGHT.rem_euclid(DAY), 0);
        let at_midnight = |date: i64| Config {
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
        // A midnight, and in the past: accepted, as AWS accepts it.
        assert_eq!(at_midnight(MIDNIGHT).validate(), Ok(()));
        let invalid = refusal(&at_midnight(MIDNIGHT + 1));
        assert_eq!(invalid.reason, DATE_MIDNIGHT);
        assert_eq!(invalid.kind, InvalidKind::Semantic);
    }

    #[test]
    fn days_and_date_are_exclusive() {
        let cfg = Config {
            rules: vec![rule(
                None,
                Filter::default(),
                Some(Expiration {
                    days: Some(1),
                    date: Some(MIDNIGHT),
                    expired_object_delete_marker: None,
                }),
            )],
        };
        assert_eq!(refusal(&cfg).reason, DAYS_AND_DATE);
    }

    #[test]
    fn expired_delete_marker_excludes_days_and_date() {
        let marker = |days: Option<i32>, date: Option<i64>| Config {
            rules: vec![rule(
                None,
                Filter::default(),
                Some(Expiration {
                    days,
                    date,
                    expired_object_delete_marker: Some(true),
                }),
            )],
        };
        assert_eq!(refusal(&marker(Some(1), None)).reason, DELETE_MARKER_DAYS);
        assert_eq!(
            refusal(&marker(None, Some(MIDNIGHT))).reason,
            DELETE_MARKER_DAYS
        );
        // The marker alone is the action.
        assert_eq!(marker(None, None).validate(), Ok(()));
    }

    #[test]
    fn a_tag_filter_excludes_abort_and_delete_marker() {
        let tagged = Filter {
            tag: Some(tag("k")),
            ..Filter::default()
        };
        let marker = Config {
            rules: vec![rule(
                None,
                tagged.clone(),
                Some(Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: Some(true),
                }),
            )],
        };
        assert_eq!(refusal(&marker).reason, TAG_FILTER_ACTION);

        let abort = Config {
            rules: vec![Rule {
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                ..rule(None, tagged.clone(), expiration(Some(RULE_DAYS)))
            }],
        };
        assert_eq!(refusal(&abort).reason, TAG_FILTER_ACTION);

        // A tag inside `And` is a tag filter too.
        let nested = Config {
            rules: vec![Rule {
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
                ..rule(
                    None,
                    Filter {
                        and: Some(And {
                            prefix: Some("logs/".to_owned()),
                            tags: vec![tag("k")],
                            ..And::default()
                        }),
                        ..Filter::default()
                    },
                    expiration(Some(RULE_DAYS)),
                )
            }],
        };
        assert_eq!(refusal(&nested).reason, TAG_FILTER_ACTION);

        // A tag-filtered `Expiration` by days is the legal combination.
        let by_days = Config {
            rules: vec![rule(None, tagged, expiration(Some(RULE_DAYS)))],
        };
        assert_eq!(by_days.validate(), Ok(()));
    }

    #[test]
    fn newer_noncurrent_versions_needs_a_filter() {
        let noncurrent = |filter: Filter| Config {
            rules: vec![Rule {
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(30),
                    newer_noncurrent_versions: Some(3),
                }),
                ..rule(None, filter, None)
            }],
        };
        assert_eq!(
            refusal(&noncurrent(Filter::default())).reason,
            NEWER_NEEDS_FILTER
        );
        assert_eq!(
            noncurrent(Filter {
                prefix: Some("logs/".to_owned()),
                ..Filter::default()
            })
            .validate(),
            Ok(())
        );
    }

    #[test]
    fn newer_noncurrent_versions_is_one_through_a_hundred() {
        let newer = |count: i32| Config {
            rules: vec![Rule {
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(30),
                    newer_noncurrent_versions: Some(count),
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
        for count in [0, -1, 101] {
            assert_eq!(refusal(&newer(count)).reason, NEWER_RANGE, "{count}");
        }
        for count in [1, 100] {
            assert_eq!(newer(count).validate(), Ok(()), "{count}");
        }
    }

    fn transition(days: Option<i32>, date: Option<i64>, storage_class: Class) -> Transition {
        Transition {
            days,
            date,
            storage_class,
        }
    }

    /// A rule whose only action is the given transitions.
    fn transition_rule(transitions: Vec<Transition>) -> Config {
        Config {
            rules: vec![Rule {
                transitions,
                ..rule(None, Filter::default(), None)
            }],
        }
    }

    #[test]
    fn a_transition_only_rule_is_valid() {
        // T11: a transition is an action on its own — a rule may carry one and
        // no expiration at all.
        let cfg = transition_rule(vec![transition(Some(30), None, Class::StandardIa)]);
        assert_eq!(cfg.validate(), Ok(()));

        // With no transitions and no other action the rule still carries no
        // action: an empty list is not an action.
        let bare = Config {
            rules: vec![rule(None, Filter::default(), None)],
        };
        assert_eq!(refusal(&bare).reason, NO_ACTION);
    }

    #[test]
    fn a_standard_target_is_refused() {
        let cfg = transition_rule(vec![transition(Some(30), None, Class::Standard)]);
        let invalid = refusal(&cfg);
        assert_eq!(invalid.reason, TRANSITION_CLASS);
        assert_eq!(invalid.kind, InvalidKind::Semantic);
        // `STANDARD_IA` is the only accepted target:
        // `a_transition_only_rule_is_valid` stores exactly that one.
    }

    #[test]
    fn a_transition_day_is_at_least_thirty() {
        let by_days =
            |days: i32| transition_rule(vec![transition(Some(days), None, Class::StandardIa)]);
        for days in [0, 1, 29] {
            let invalid = refusal(&by_days(days));
            assert_eq!(invalid.reason, TRANSITION_DAYS_MIN, "Transition Days {days}");
            assert_eq!(invalid.kind, InvalidKind::Semantic);
        }
        for days in [30, 31] {
            assert_eq!(by_days(days).validate(), Ok(()), "Transition Days {days}");
        }
        // A transition names Days or Date, never both.
        let both = transition_rule(vec![transition(Some(30), Some(MIDNIGHT), Class::StandardIa)]);
        assert_eq!(refusal(&both).reason, TRANSITION_DAYS_AND_DATE);
    }

    #[test]
    fn a_past_date_is_accepted() {
        let by_date =
            |date: Option<i64>| transition_rule(vec![transition(None, date, Class::StandardIa)]);
        // 2023-11-14T00:00Z: in the past, accepted as AWS accepts it.
        assert_eq!(by_date(Some(MIDNIGHT)).validate(), Ok(()));
        let invalid = refusal(&by_date(Some(MIDNIGHT + 1)));
        assert_eq!(invalid.reason, TRANSITION_DATE_MIDNIGHT);
        assert_eq!(invalid.kind, InvalidKind::Semantic);
        // Neither Days nor Date names no instant.
        assert_eq!(refusal(&by_date(None)).reason, NO_ACTION);
    }

    #[test]
    fn an_empty_transitions_list_is_omitted() {
        // A rule without transitions keeps the wire it has always had: the
        // empty list is not serialised (T11).
        let expiring = Config {
            rules: vec![rule(None, Filter::default(), expiration(Some(RULE_DAYS)))],
        };
        let wire = expiring.to_wire();
        assert!(!wire.contains("transitions"), "{wire}");

        let transitioning = transition_rule(vec![transition(Some(30), None, Class::StandardIa)]);
        let wire = transitioning.to_wire();
        assert!(
            wire.contains(
                r#""transitions":[{"days":30,"date":null,"storage_class":"STANDARD_IA"}]"#
            ),
            "{wire}"
        );
        assert_eq!(Config::from_wire(&wire), Some(transitioning));

        // A hand-written body carrying the members decodes to the same rule,
        // so the wire is not write-only.
        let hand_written = r#"{"rules":[{"id":null,"status":"Enabled","filter":{},"expiration":null,"noncurrent_version_expiration":null,"abort_incomplete_multipart_upload":null,"transitions":[{"days":null,"date":1699920000,"storage_class":"STANDARD_IA"}]}]}"#;
        let decoded = Config::from_wire(hand_written).expect("the hand-written body decodes");
        assert_eq!(
            decoded.rules[0].transitions,
            vec![transition(None, Some(MIDNIGHT), Class::StandardIa)]
        );
    }

    #[test]
    fn the_class_wire_spelling_is_screaming_snake() {
        assert_eq!(
            serde_json::to_string(&Class::Standard).expect("a class serialises"),
            r#""STANDARD""#
        );
        assert_eq!(
            serde_json::to_string(&Class::StandardIa).expect("a class serialises"),
            r#""STANDARD_IA""#
        );
        assert_eq!(
            serde_json::from_str::<Class>(r#""STANDARD_IA""#).expect("a class decodes"),
            Class::StandardIa
        );
        assert_eq!(Class::StandardIa.to_string(), "STANDARD_IA");
    }

    fn full_config() -> Config {
        Config {
            rules: vec![Rule {
                id: Some("archive/logs".to_owned()),
                status: Status::Enabled,
                filter: Filter {
                    and: Some(And {
                        prefix: Some("logs/".to_owned()),
                        tags: vec![tag("k"), tag("j")],
                        size_greater_than: Some(1),
                        size_less_than: Some(MAX_SIZE),
                    }),
                    ..Filter::default()
                },
                expiration: Some(Expiration {
                    days: None,
                    date: Some(MIDNIGHT),
                    expired_object_delete_marker: None,
                }),
                transitions: Vec::new(),
                noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
                    noncurrent_days: Some(30),
                    newer_noncurrent_versions: Some(100),
                }),
                abort_incomplete_multipart_upload: None,
            }],
        }
    }

    #[test]
    fn the_wire_round_trips() {
        let cfg = full_config();
        assert_eq!(cfg.validate(), Ok(()));
        let wire = cfg.to_wire();
        assert!(wire.contains("\"size_greater_than\""), "{wire}");
        assert!(wire.contains("\"newer_noncurrent_versions\""), "{wire}");
        assert_eq!(Config::from_wire(&wire), Some(cfg));

        // LC17: one shape. A field a later binary adds is carried past this
        // one, not refused — the wire is not strict about unknown members.
        let later = wire.replace("\"Enabled\"", "\"Enabled\",\"future_member\":true");
        assert_ne!(later, wire);
        assert_eq!(
            Config::from_wire(&later),
            Config::from_wire(&wire),
            "{later}"
        );
    }

    #[test]
    fn a_corrupt_wire_decodes_to_none() {
        assert_eq!(Config::from_wire(""), None);
        assert_eq!(Config::from_wire("not json"), None);
        assert_eq!(Config::from_wire("{\"rules\":\"none\"}"), None);
        // `filter` is `#[serde(default)]`, so this case isolates the unknown
        // `status` variant rather than a missing field.
        assert_eq!(
            Config::from_wire("{\"rules\":[{\"id\":null,\"status\":\"Paused\",\"filter\":{}}]}"),
            None
        );
    }

    #[test]
    fn a_missing_filter_is_everything() {
        // A hand-written plane body may omit `filter` (LC8: an absent filter
        // means the whole bucket), so the wire decodes it as `Filter::default`.
        let body =
            "{\"rules\":[{\"id\":\"x\",\"status\":\"Enabled\",\"expiration\":{\"days\":30}}]}";
        let config = Config::from_wire(body).expect("a rule without a filter decodes");
        assert_eq!(config.rules[0].filter, Filter::default());
        assert_eq!(config.validate(), Ok(()));
        // And it round-trips as the spelled-out empty filter.
        assert_eq!(Config::from_wire(&config.to_wire()), Some(config));
    }

