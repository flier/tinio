//! Bucket CORS configuration: domain types, the canonical wire, and
//! preflight matching.
//!
//! Rules are ORDER-PRESERVING and first-origin-match (AWS
//! select-first-origin-rule semantics): the wire codec never sorts or dedupes
//! rules, and [`CorsConfig::preflight`]/[`CorsConfig::rule_for`] select the
//! first rule whose ORIGIN matches, then validate method (and, for preflight,
//! headers) within that rule only — never falling through to a later rule.

/// Maximum number of rules per configuration (AWS: 100).
pub const CORS_RULES_MAX: usize = 100;
/// Maximum length of a rule ID (AWS: 255).
pub const CORS_RULE_ID_MAX: usize = 255;
/// Maximum decoded size of a stored CORS configuration (AWS: 64 KB).
pub const CORS_CONFIG_BYTES_MAX: usize = 64 * 1024;
/// The `AllowedMethod` values AWS accepts (uppercase, exact).
pub const CORS_METHODS: [&str; 5] = ["GET", "PUT", "HEAD", "POST", "DELETE"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Option<Vec<String>>,
    pub expose_headers: Option<Vec<String>>,
    pub max_age_seconds: Option<i32>,
}

impl CorsRule {
    /// Whether `origin` matches any allowed-origin pattern: exact match,
    /// a bare `*`, or a single `*` wildcard (e.g. `https://*.example.net`).
    pub fn origin_matches(&self, origin: &str) -> bool {
        self.allowed_origins
            .iter()
            .any(|p| pattern_matches(p, origin))
    }

    /// Whether `method` is allowed (case-insensitive exact).
    pub fn method_allows(&self, method: &str) -> bool {
        self.allowed_methods
            .iter()
            .any(|m| m.eq_ignore_ascii_case(method))
    }

    /// Whether every requested header is allowed: any pattern match
    /// (`*`-wildcards; HTTP header names are case-insensitive) or, when
    /// no `allowed_headers` is set, none. No allocation — the comparison
    /// is byte-wise case-insensitive.
    pub fn headers_allow(&self, requested: &[String]) -> bool {
        match &self.allowed_headers {
            None => requested.is_empty(),
            Some(patterns) => requested.iter().all(|h| {
                patterns
                    .iter()
                    .any(|p| pattern_matches_ci(p.as_bytes(), h.as_bytes()))
            }),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CorsConfig {
    pub rules: Vec<CorsRule>,
}

/// A preflight decision: the winning rule plus the echoed request values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightMatch {
    pub rule: CorsRule,
    pub origin: String,
    pub method: String,
    pub requested_headers: Vec<String>,
}

impl CorsConfig {
    /// First-origin-match semantics (AWS select-first-rule): iterate rules in
    /// stored order, select the FIRST rule whose `origin` matches, then
    /// validate method AND headers WITHIN that rule only — a rule that
    /// matches origin but fails method/headers returns `None` (deny) and
    /// never falls through to a later rule (see the matching-rules note).
    pub fn preflight(
        &self,
        origin: &str,
        method: &str,
        requested_headers: &[String],
    ) -> Option<PreflightMatch> {
        let rule = self
            .rule_for(origin, method)
            .filter(|r| r.headers_allow(requested_headers))?;
        Some(PreflightMatch {
            rule: rule.clone(),
            origin: origin.to_string(),
            method: method.to_string(),
            requested_headers: requested_headers.to_vec(),
        })
    }

    /// The decoration lookup for actual (non-OPTIONS) responses: the first
    /// origin-matching rule, then method validated within THAT rule only (no
    /// fall-through past an origin-match/method-mismatch rule).
    pub fn rule_for(&self, origin: &str, method: &str) -> Option<&CorsRule> {
        self.rules
            .iter()
            .find(|r| r.origin_matches(origin))
            .filter(|r| r.method_allows(method))
    }

    pub fn to_wire(&self) -> String {
        self.rules
            .iter()
            .map(|r| {
                let methods = encode(&r.allowed_methods.join(","));
                let origins = encode(&r.allowed_origins.join(","));
                let headers = r
                    .allowed_headers
                    .as_ref()
                    .map(|v| encode(&v.join(",")))
                    .unwrap_or_default();
                let expose = r
                    .expose_headers
                    .as_ref()
                    .map(|v| encode(&v.join(",")))
                    .unwrap_or_default();
                let id = r.id.as_deref().map(encode).unwrap_or_default();
                let max_age = r.max_age_seconds.map(|s| s.to_string()).unwrap_or_default();
                [methods, origins, headers, expose, id, max_age].join(",")
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Decode the wire; any parse failure self-heals to an empty config
    /// (the `''` wire — "no CORS configuration" — is `Default`).
    pub fn from_wire(s: &str) -> Self {
        Self::parse_wire(s).unwrap_or_default()
    }

    fn parse_wire(s: &str) -> Option<Self> {
        let mut rules = Vec::new();
        for record in s.split('&').filter(|r| !r.is_empty()) {
            let mut it = record.splitn(6, ',');
            let methods = decode(it.next()?)?;
            let origins = decode(it.next()?)?;
            let headers = decode(it.next()?)?;
            let expose = decode(it.next()?)?;
            let id = decode(it.next()?)?;
            let max_age = decode(it.next()?)?;
            if rules.len() >= CORS_RULES_MAX {
                return None;
            }
            rules.push(CorsRule {
                id: if id.is_empty() { None } else { Some(id) },
                allowed_methods: split_list(&methods),
                allowed_origins: split_list(&origins),
                allowed_headers: if headers.is_empty() {
                    None
                } else {
                    Some(split_list(&headers))
                },
                expose_headers: if expose.is_empty() {
                    None
                } else {
                    Some(split_list(&expose))
                },
                max_age_seconds: if max_age.is_empty() {
                    None
                } else {
                    Some(max_age.parse::<i32>().ok()?)
                },
            });
        }
        Some(Self { rules })
    }
}

fn split_list(s: &str) -> Vec<String> {
    s.split(',').map(str::to_string).collect()
}

/// Match `pattern` (exact, `*`, or one `*` wildcard with prefix+suffix)
/// against `value`.
fn pattern_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some(("*", "")) => true, // "*" alone
        Some((prefix, suffix)) => {
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() > prefix.len() + suffix.len() // at least one wildcard char
        }
    }
}

/// The case-insensitive twin of [`pattern_matches`], byte-wise (HTTP
/// header names; no allocation).
fn pattern_matches_ci(pattern: &[u8], value: &[u8]) -> bool {
    match pattern.iter().position(|b| *b == b'*') {
        None => value.eq_ignore_ascii_case(pattern),
        Some(0) if pattern.len() == 1 => true, // "*" alone
        Some(pos) => {
            let (prefix, rest) = pattern.split_at(pos);
            let suffix = &rest[1..];
            value.len() > prefix.len() + suffix.len() // at least one wildcard char
                && value[..prefix.len()].eq_ignore_ascii_case(prefix)
                && value[value.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
        }
    }
}

// Percent encode/decode. NOTE: the full unreserved-set rule here is
// REQUIRED for the wire grammar (raw `,`/`&`/control bytes would
// mis-frame the 6-field record or smuggle header values — op-review
// C2/S1). The object.rs tags codec escapes only `% = & + space` and must
// NOT be copied as-is; when the ACL plan lands `crate::percent`, its
// encode must adopt this set (one-line coordination point).
const HEX: &[u8; 16] = b"0123456789ABCDEF";

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0x0f) as usize] as char);
            }
        }
    }
    out
}

fn decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex(bytes.get(i + 1)?)?;
            let lo = hex(bytes.get(i + 2)?)?;
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex(b: &u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule() -> CorsRule {
        CorsRule {
            id: Some("allow-example".into()),
            allowed_methods: vec!["GET".into(), "PUT".into()],
            allowed_origins: vec!["https://example.com".into(), "https://*.example.net".into()],
            allowed_headers: Some(vec!["x-amz-*".into(), "content-type".into()]),
            expose_headers: Some(vec!["ETag".into()]),
            max_age_seconds: Some(300),
        }
    }

    #[test]
    fn wire_round_trip_preserves_order_and_fields() {
        let cfg = CorsConfig {
            rules: vec![
                rule(),
                CorsRule {
                    id: None,
                    allowed_methods: vec!["DELETE".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        let wire = cfg.to_wire();
        let back = CorsConfig::from_wire(&wire);
        assert_eq!(back, cfg, "{wire}");
    }

    #[test]
    fn wire_keeps_rule_order_first_match_semantics() {
        // Rules must NOT be sorted or deduped — the wire preserves stored order.
        let cfg = CorsConfig {
            rules: vec![
                CorsRule {
                    id: None,
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
                CorsRule {
                    id: Some("second".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        let back = CorsConfig::from_wire(&cfg.to_wire());
        assert_eq!(back.rules[0].id, None); // the tighter rule stayed first
        assert_eq!(back.rules[1].id.as_deref(), Some("second"));
    }

    #[test]
    fn wire_self_heals_to_empty_on_garbage() {
        assert_eq!(CorsConfig::from_wire("garbage!%"), CorsConfig::default());
        assert_eq!(CorsConfig::from_wire("a,b,c"), CorsConfig::default()); // wrong field count
        assert_eq!(
            CorsConfig::from_wire("a,b,*,*,*,abc"),
            CorsConfig::default()
        ); // bad max_age
        assert_eq!(CorsConfig::from_wire(""), CorsConfig::default());
    }

    #[test]
    fn wire_escapes_field_separators_inside_values() {
        let cfg = CorsConfig {
            rules: vec![CorsRule {
                id: Some("a&b,c;d".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["https://a.example.com/?x=1&y;z".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        let wire = cfg.to_wire();
        assert!(!wire.contains("&,c"), "{wire}"); // the id's ',' must be encoded
        assert!(!wire.contains(';'), "{wire}"); // so must ';' — a tags-style narrow escape set would leak it raw
        assert_eq!(CorsConfig::from_wire(&wire), cfg);
    }

    #[test]
    fn wire_round_trips_percent_in_value() {
        // The escape CHARACTER itself must round-trip (`%` → `%25`) —
        // the one byte the encode/decode pair never passes through raw.
        let cfg = CorsConfig {
            rules: vec![CorsRule {
                id: Some("a%b".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["https://a.example.com/?x=%2F&y=1%".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        let wire = cfg.to_wire();
        assert!(wire.contains("%25"), "{wire}");
        assert_eq!(CorsConfig::from_wire(&wire), cfg, "{wire}");
    }

    #[test]
    fn origin_matching_is_case_sensitive() {
        // Byte-exact origins (design §9): header NAMES are case-insensitive;
        // origins are not — the scheme host path must match exactly.
        let cfg = CorsConfig { rules: vec![rule()] };
        assert!(cfg.preflight("https://example.com", "GET", &[]).is_some());
        assert!(
            cfg.preflight("https://EXAMPLE.com", "GET", &[]).is_none(),
            "uppercase host must not match a mixed-case origin pattern"
        );
    }

    #[test]
    fn wire_round_trips_control_bytes() {
        // op-review S1/C2: control bytes inside a value must be encoded away
        // (the tags codec's `% = & + space`-only escaping cannot do this —
        // the full unreserved-set rule is required by the wire grammar).
        let cfg = CorsConfig {
            rules: vec![CorsRule {
                id: Some("id\t\r\nx".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["https://a.example.com".into()],
                allowed_headers: Some(vec!["x\r\n-amz-b".into()]),
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        let wire = cfg.to_wire();
        assert_eq!(
            CorsConfig::from_wire(&wire),
            cfg,
            "control bytes must round-trip, {wire}"
        );
    }

    #[test]
    fn multi_star_pattern_is_a_safe_non_match() {
        // The put layer rejects >1 `*` (400); a stored row with one
        // (e.g. from a legacy/corrupt row) must never panic — and must
        // not match a plain value.
        let rule = CorsRule {
            id: None,
            allowed_methods: vec!["GET".into()],
            allowed_origins: vec!["a*b*c".into()],
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: None,
        };
        assert!(!rule.origin_matches("abc"));
    }

    #[test]
    fn origin_patterns_exact_wildcard_and_dot_wildcard() {
        assert!(
            CorsConfig {
                rules: vec![rule()]
            }
            .preflight("https://a.example.net", "GET", &[])
            .is_some(),
            "https://*.example.net suffix match"
        );
        assert!(
            CorsConfig {
                rules: vec![rule()]
            }
            .preflight("https://example.com", "GET", &[])
            .is_some()
        );
        assert!(
            CorsConfig {
                rules: vec![rule()]
            }
            .preflight("https://example.net", "GET", &[])
            .is_none(),
            "bare '*.example.net' must not match the apex (no subdomain dot)"
        );
        assert!(
            CorsConfig {
                rules: vec![CorsRule {
                    id: None,
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None
                }]
            }
            .preflight("https://any.where", "GET", &[])
            .is_some(),
            "bare '*' matches any origin"
        );
    }

    #[test]
    fn preflight_matches_first_rule_with_origin_method_and_headers() {
        let cfg = CorsConfig {
            rules: vec![rule()],
        };
        let hit = cfg
            .preflight("https://example.com", "PUT", &["x-amz-foo".into()])
            .unwrap();
        assert_eq!(hit.origin, "https://example.com"); // echoed
        assert_eq!(hit.method, "PUT"); // echoed
        assert_eq!(hit.requested_headers, vec!["x-amz-foo".to_string()]);
        // method not allowed by any rule → no match
        assert!(
            cfg.preflight("https://example.com", "DELETE", &[])
                .is_none()
        );
        // unknown header → no match; `*` header pattern → match
        assert!(
            cfg.preflight("https://example.com", "GET", &["x-evil".into()])
                .is_none()
        );
        assert!(
            cfg.preflight("https://example.com", "GET", &["x-amz-anything".into()])
                .is_some()
        );
        // header matching is case-insensitive (HTTP header names)
        assert!(
            cfg.preflight("https://example.com", "GET", &["X-AmZ-Foo".into()])
                .is_some()
        );
        // no AllowedHeaders = no headers allowed
        let strict = CorsConfig {
            rules: vec![CorsRule {
                id: None,
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        assert!(
            strict
                .preflight("https://example.com", "GET", &["a".into()])
                .is_none()
        );
        assert!(
            strict
                .preflight("https://example.com", "GET", &[])
                .is_some()
        );
    }

    #[test]
    fn rule_for_returns_first_origin_and_method_match() {
        let cfg = CorsConfig {
            rules: vec![CorsRule {
                id: Some("who".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        assert_eq!(
            cfg.rule_for("https://example.com", "GET")
                .unwrap()
                .id
                .as_deref(),
            Some("who")
        );
        assert!(cfg.rule_for("https://example.com", "DELETE").is_none());
    }

    #[test]
    fn method_match_is_case_insensitive() {
        assert!(rule().method_allows("get"));
        assert!(!rule().method_allows("PATCH"));
    }

    #[test]
    fn preflight_does_not_fall_through_past_an_origin_matching_rule() {
        // First-origin-match (AWS): rule1 matches the origin but not the
        // method; rule2 matches everything. AWS DENIES — rule2 must NOT be
        // consulted once rule1 claims the origin.
        let cfg = CorsConfig {
            rules: vec![
                CorsRule {
                    id: Some("r1".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
                CorsRule {
                    id: Some("r2".into()),
                    allowed_methods: vec!["PUT".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        // origin matches r1, but PUT is not in r1's methods → deny (no r2 fall-through)
        assert!(cfg.preflight("https://example.com", "PUT", &[]).is_none());
        // rule_for (decoration) must behave identically: origin matches r1,
        // PUT not allowed by r1 → no rule (r2's origin "*" is never consulted)
        assert!(cfg.rule_for("https://example.com", "PUT").is_none());
        // and a method r1 DOES allow still resolves to r1 (the first origin match)
        assert_eq!(
            cfg.preflight("https://example.com", "GET", &[])
                .unwrap()
                .rule
                .id
                .as_deref(),
            Some("r1")
        );
        assert_eq!(
            cfg.rule_for("https://example.com", "GET")
                .unwrap()
                .id
                .as_deref(),
            Some("r1")
        );
    }
}
