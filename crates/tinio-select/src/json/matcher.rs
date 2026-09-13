//! `PathSeg` compiled to a state machine, and the serde `DeserializeSeed`
//! visitors running it over the streaming event flow (`MapAccess` /
//! `SeqAccess` are the events). Unmatched subtrees go through `Skip`, a
//! validating skip that replaces `IgnoredAny`: nothing materializes, and
//! string contents are validated exactly as today's whole-line `from_slice`
//! validates them.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
};

use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error as DeError, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;

use crate::{error::Error, json::value::key_matches, path::PathSeg, row::NameStyle};

/// serde_json 1.0.151 `src/number.rs:18` (pub(crate) there): with
/// `arbitrary_precision` a non-integer number reaches a custom visitor as a
/// one-key map under this key — detect it and treat the "map" as a scalar.
/// Pinned by `wild_over_float_scalar_is_missing`.
const NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// The FROM path as the matcher walks it: `PathSeg` with the leading `Wild`
/// (the implicit `S3Object[*]` root step) dropped — that root rule is the
/// framing's job, so the compiled path never branches on it and the segments
/// carry everything else. Compiled once per reader.
pub(crate) struct CompiledPath {
    steps: Vec<PathSeg>,
}

impl CompiledPath {
    pub(crate) fn from_segments(segments: &[PathSeg]) -> Self {
        let steps = match segments.first() {
            Some(PathSeg::Wild) => &segments[1..],
            _ => &[],
        };
        Self {
            steps: steps.to_vec(),
        }
    }

    pub(crate) fn steps(&self) -> &[PathSeg] {
        &self.steps
    }
}

/// Per-pull output: the one field the capture end needs, re-borrowed from
/// `StepSeed` by `child()`. No `Budget` here — the cap is armed once per root
/// pull by the handler (Global Constraints), not once per matched value, so
/// the seeds carry no budget state at all. No `failure` either: capture only
/// runs once the steps are exhausted, so it can never record an ambiguity —
/// the handler reads its own `failure` binding, which only `StepSeed` writes.
///
/// There is deliberately no `Sink<'a>` wrapper: with a field of type
/// `&'a mut Sink<'a>` the `Child` enum becomes *invariant* over `'a`, so
/// `fn child(&mut self) -> Child<'_>` cannot return a reborrow of the shorter
/// `&mut self` lifetime (rustc 1.98.1: "lifetime may not live long enough");
/// `-> Child<'a>`, an explicit `'b: 'a` bound and the UFCS spelling all fail
/// as well — the bound variant merely moves the error to the caller. Inlining
/// the fields keeps `Child` covariant, which is what makes the recursion
/// compile.
pub(crate) struct CaptureSeed<'a> {
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
}

/// The steps are exhausted: materialize the current value as the record.
impl<'de> DeserializeSeed<'de> for CaptureSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.pending.push_back(Some(Value::deserialize(d)?));
        Ok(())
    }
}

/// Zero-match rule: the container closed with no match — one MISSING row.
fn push_missing(pending: &mut VecDeque<Option<Value>>) {
    pending.push_back(None);
}

/// A second case-folded match inside one object: recorded for the handler and
/// surfaced as a serde error so the pull aborts; the handler's `map_err`
/// prefers the recorded `Error::Ambiguous`.
fn ambiguous<E: DeError>(failure: &mut Option<Error>, name: &str) -> E {
    *failure = Some(Error::Ambiguous(name.into()));
    E::custom("ambiguous field name")
}

/// A scope step (`steps[0]`) matched against the current value. A container
/// closing with zero matches emits exactly one MISSING row (the zero-match
/// rule); a step over a scalar is a zero-match.
pub(crate) struct StepSeed<'a> {
    pub(crate) steps: &'a [PathSeg],
    pub(crate) pending: &'a mut VecDeque<Option<Value>>,
    pub(crate) failure: &'a mut Option<Error>,
}

pub(crate) enum Child<'a> {
    Capture(CaptureSeed<'a>),
    Step(StepSeed<'a>),
}

impl<'de> DeserializeSeed<'de> for Child<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        match self {
            Child::Capture(c) => c.deserialize(d),
            Child::Step(s) => s.deserialize(d),
        }
    }
}

/// The seed for the value a step selects, given what remains *after* that
/// step: the next step's seed, or the capture once nothing remains. Called
/// with `steps[1..]` from `StepSeed` (the child of the current step) and with
/// the whole path from a handler's root pull — one rule, one place.
///
/// A free function, not a method: the returned `Child` reborrows the failure
/// slot for as long as the caller holds it, which a `&mut self` receiver plus
/// `&mut *self.pending` cannot express (E0499).
pub(crate) fn child_into<'q>(
    rest: &'q [PathSeg],
    failure: &'q mut Option<Error>,
    pending: &'q mut VecDeque<Option<Value>>,
) -> Child<'q> {
    if rest.is_empty() {
        Child::Capture(CaptureSeed { pending })
    } else {
        Child::Step(StepSeed {
            steps: rest,
            pending,
            failure,
        })
    }
}

impl StepSeed<'_> {
    /// The seed for the value selected by the current step: one level
    /// deeper, or the capture once the steps run out.
    fn child(&mut self) -> Child<'_> {
        child_into(&self.steps[1..], &mut *self.failure, &mut *self.pending)
    }
}

impl<'de> DeserializeSeed<'de> for StepSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for StepSeed<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut matched = false;
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(PathSeg::Wild) => {
                while let Some(()) = seq.next_element_seed(self.child())? {
                    matched = true;
                }
            }
            Some(PathSeg::Index(i)) => {
                let target = *i;
                let mut ord = 0usize;
                loop {
                    if ord == target {
                        match seq.next_element_seed(self.child())? {
                            Some(()) => {
                                matched = true;
                                ord += 1;
                            }
                            None => break,
                        }
                    } else {
                        match seq.next_element_seed(Skip)? {
                            Some(()) => ord += 1,
                            None => break,
                        }
                    }
                }
            }
            Some(PathSeg::Name(_)) => {
                // A by-name step over an array is a zero-match — consume.
                while seq.next_element_seed(Skip)?.is_some() {}
            }
        }
        if !matched {
            push_missing(self.pending);
        }
        Ok(())
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // First key read doubles as the number-map check: with
        // arbitrary_precision a non-integer number is a scalar in disguise.
        let key0 = map.next_key::<String>()?;
        if key0.as_deref() == Some(NUMBER_TOKEN) {
            map.next_value_seed(Skip)?;
            // Genuine token maps have exactly one entry (`NumberDeserializer`
            // is synthetic, number.rs:653-677), but a *real* object with this
            // literal first key arrives through the raw `MapAccess` —
            // `deserialize_any` makes no token check at `{`
            // (de.rs:1444-1454) — and collides here. serde_json documents
            // that collision and today's `from_slice::<Value>` rejects such
            // objects (value/de.rs:125-129), so error like today instead of
            // asserting: the input is untrusted and a `debug_assert!` would
            // panic debug builds. A single-entry "token map" is still taken
            // as the number without re-validating the value (today's `Value`
            // re-parses and rejects non-numeric strings — an accepted
            // divergence on a key serde_json itself reserves).
            if map.next_key_seed(Skip)?.is_some() {
                return Err(DeError::custom("invalid number-token object"));
            }
            push_missing(self.pending);
            return Ok(());
        }
        match self.steps.first() {
            None => unreachable!("CaptureSeed handles the exhausted steps"),
            Some(PathSeg::Wild) => {
                // One row group per DISTINCT key, in first-occurrence order,
                // with a repeated key replacing its earlier group in place.
                // The deleted whole-document pipeline materialized a
                // serde_json::Value first, whose preserve_order map (an
                // IndexMap) collapsed byte-identical duplicate keys last-wins
                // without moving them; the raw `MapAccess` stream shows every
                // pair, so the collapse is reproduced here. Keys are decoded
                // with `next_key::<String>`, which validates UTF-8 and escapes
                // exactly as the skipped-key path did. Memory: this buffers one
                // object level's match rows for a single wildcard step — inside
                // the pull's own per-root cap (every served byte is charged)
                // and the spec's "one root's match set" promise, not an
                // unbounded buffer. The `index` side map is what keeps the
                // in-place replace O(1) per member: the per-root 1 MB cap does
                // not bound the member count usefully (~10^5 one-character keys
                // fit), so a linear scan would be quadratic on attacker-
                // controlled input, where the `IndexMap` this replaces was O(1)
                // per key.
                let mut groups: Vec<(String, VecDeque<Option<Value>>)> = Vec::new();
                let mut index: HashMap<String, usize> = HashMap::new();
                let mut key = key0;
                while let Some(k) = key {
                    let mut rows = VecDeque::new();
                    map.next_value_seed(child_into(
                        &self.steps[1..],
                        &mut *self.failure,
                        &mut rows,
                    ))?;
                    match index.get(&k).copied() {
                        Some(i) => groups[i].1 = rows,
                        None => {
                            index.insert(k.clone(), groups.len());
                            groups.push((k, rows));
                        }
                    }
                    key = map.next_key::<String>()?;
                }
                if groups.is_empty() {
                    push_missing(self.pending);
                } else {
                    self.pending
                        .extend(groups.into_iter().flat_map(|(_, rows)| rows));
                }
                Ok(())
            }
            Some(PathSeg::Index(_)) => {
                // An index step over an object is a zero-match — consume.
                let mut has_key = key0.is_some();
                while has_key {
                    map.next_value_seed(Skip)?;
                    has_key = map.next_key_seed(Skip)?.is_some();
                }
                push_missing(self.pending);
                Ok(())
            }
            Some(PathSeg::Name(name)) => {
                let mut matched = false;
                let mut held: Option<Value> = None;
                let mut key = key0;
                while let Some(k) = key {
                    if key_matches(&k, name, NameStyle::Bare) {
                        if matched {
                            return Err(ambiguous(self.failure, name));
                        }
                        matched = true;
                        if self.steps.len() == 1 {
                            // Final Name: hold the value until the object
                            // closes — the ambiguity verdict needs the full
                            // member scan (R10 parity, bounded by one value,
                            // which the pull's own budget already caps).
                            held = Some(map.next_value::<Value>()?);
                        } else {
                            map.next_value_seed(self.child())?;
                        }
                    } else {
                        map.next_value_seed(Skip)?;
                    }
                    key = map.next_key::<String>()?;
                }
                if !matched {
                    push_missing(self.pending);
                } else if let Some(v) = held {
                    self.pending.push_back(Some(v));
                }
                Ok(())
            }
        }
    }

    // A step over any scalar is a zero-match — exactly one MISSING row.
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        push_missing(self.pending);
        Ok(())
    }
}

/// Validating skip for every unmatched subtree, replacing
/// `serde::de::IgnoredAny`.
///
/// `IgnoredAny` materializes nothing, but it also does not fully validate:
/// serde_json's `IoRead::ignore_str` (`read.rs:348-366`) checks escape *shape*
/// and the four hex digits of `\uXXXX`, yet never runs the surrogate-pairing
/// check (`ignore_escape` does not reach `parse_unicode_escape`) and never runs
/// `str::from_utf8` over the raw bytes. A string `from_slice` rejects today — a
/// lone `\uD800`, a raw non-UTF-8 byte — would therefore pass silently inside a
/// subtree the query never reads, which is precisely the validation gap that
/// ruled out the hand-written scanner (spec: full serde validation wins).
///
/// `Skip` takes `deserialize_any` — never `deserialize_ignored_any`, which
/// consumes the value before any visitor runs — so strings arrive as `&str`,
/// get validated, and are dropped. Memory stays O(1): scalars are dropped in
/// place, children recurse with `Skip`, and `from_reader` hands strings over in
/// serde's reusable scratch buffer. Numbers, literals, structure and control
/// characters were already validated; this closes the string-content half.
///
/// The first key of every map is probed for the reserved
/// `$serde_json::private::Number` token (see `NUMBER_TOKEN`) and a skipped map
/// carrying it is handled exactly as the traversed `visit_map` above handles
/// it, so today's whole-line `from_slice::<Value>` — which rejects any object
/// continuing past that key (`value/de.rs:125-129`) — stays the reference. One
/// divergence survives, on both paths alike: a single-entry object whose
/// reserved-key value is not a numeric string is taken as the number rather
/// than re-parsed.
pub(crate) struct Skip;

/// First-key probe for `Skip::visit_map`: does this key name
/// `arbitrary_precision`'s reserved number token? Compares in `visit_str`, so
/// the hot skip path allocates nothing.
struct TokenProbe;

impl<'de> DeserializeSeed<'de> for TokenProbe {
    type Value = bool;

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `deserialize_any`, matching how `Skip` reads a key.
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for TokenProbe {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON object key")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(v == NUMBER_TOKEN)
    }
}

impl<'de> DeserializeSeed<'de> for Skip {
    type Value = ();

    fn deserialize<D>(self, d: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Skip {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element_seed(Skip)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        // The reserved number token is read exactly as the traversed
        // `visit_map` reads it: consume its value, then require the token
        // map's single entry. Today's `from_slice::<Value>` rejects a real
        // object that continues past that key, so a skipped one must too.
        match map.next_key_seed(TokenProbe)? {
            Some(true) => {
                map.next_value_seed(Skip)?;
                if map.next_key_seed(TokenProbe)?.is_some() {
                    return Err(DeError::custom("invalid number-token object"));
                }
                return Ok(());
            }
            // Keys go through `Skip` too: today's whole-line `from_slice`
            // validates every key, and a skipped key must not become the one
            // exception. This first one was validated by `TokenProbe`.
            Some(false) => map.next_value_seed(Skip)?,
            None => return Ok(()),
        }
        while map.next_entry_seed(Skip, Skip)?.is_some() {}
        Ok(())
    }

    // Scalars: the visit *is* the validation; the value itself is dropped.
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_borrowed_str<E>(self, _: &'de str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_string<E>(self, _: String) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        io::{BufReader, Cursor},
        rc::Rc,
    };

    use serde_json::Value;

    use super::*;
    use crate::{
        error::Error,
        json::framing::{Budget, BudgetedRead, DocFraming},
        path::PathSeg,
        record::MAX_RECORD,
    };

    fn match_rows(segments: &[PathSeg], input: &str) -> Vec<Option<Value>> {
        let path = CompiledPath::from_segments(segments);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(input.as_bytes());
            child_into(path.steps(), &mut failure, &mut out).deserialize(&mut de)
        };
        assert!(r.is_ok(), "{input}: {:?}", r.err());
        assert!(failure.is_none(), "{input}: {failure:?}");
        out.into_iter().collect()
    }

    #[test]
    fn wild_over_int_scalar_is_missing() {
        assert_eq!(
            match_rows(
                &[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild],
                "{\"x\": 5}"
            ),
            vec![None]
        );
    }

    #[test]
    fn wild_over_float_scalar_is_missing() {
        // arbitrary_precision: a float comes back as a NUMBER_TOKEN one-key
        // map — it must be recognized as a scalar.
        assert_eq!(
            match_rows(
                &[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild],
                "{\"x\": 1.5}"
            ),
            vec![None]
        );
    }

    #[test]
    fn index_over_number_map_is_missing() {
        assert_eq!(
            match_rows(
                &[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Index(0)],
                "{\"x\": 1e309}"
            ),
            vec![None]
        );
    }

    #[test]
    fn number_token_first_key_object_errors_not_panics() {
        // A real object with the reserved first key is not a number — and must
        // never trip a debug assert (untrusted input). Today's
        // `from_slice::<Value>` rejects it (invalid number); so do we.
        let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())]);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(
                br#"{"$serde_json::private::Number": 1, "a": 2}"#,
            );
            StepSeed {
                steps: path.steps(),
                pending: &mut out,
                failure: &mut failure,
            }
            .deserialize(&mut de)
        };
        assert!(r.is_err());
        assert!(out.is_empty());
    }

    #[test]
    fn number_token_single_entry_is_a_scalar() {
        // serde_json's documented `arbitrary_precision` collision: the key is
        // reserved, and today's `from_slice::<Value>` turns this object into a
        // number too (value/de.rs:125-129) — a step over it is a zero-match
        // either way.
        assert_eq!(
            match_rows(
                &[PathSeg::Wild, PathSeg::Name("x".into()), PathSeg::Wild],
                "{\"x\": {\"$serde_json::private::Number\": \"1.5\"}}"
            ),
            vec![None]
        );
    }

    #[test]
    fn escaped_key_matches_case_insensitively() {
        assert_eq!(
            match_rows(
                &[PathSeg::Wild, PathSeg::Name("rules".into()), PathSeg::Wild],
                "{\"R\\u0075les\": [1]}"
            ),
            vec![Some(serde_json::json!(1))]
        );
    }

    #[test]
    fn final_name_holds_value_until_object_close() {
        // Duplicate case-folded key: the held value must not be enqueued
        // early; on the error pending is still empty.
        let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("id".into())]);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(b"{\"id\": 1, \"ID\": 2}");
            StepSeed {
                steps: path.steps(),
                pending: &mut out,
                failure: &mut failure,
            }
            .deserialize(&mut de)
        };
        assert!(r.is_err());
        assert_eq!(failure, Some(Error::Ambiguous("id".into())));
        assert!(out.is_empty());
    }

    #[test]
    fn non_final_name_duplicate_fails_mid_stream() {
        let path =
            CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild]);
        let mut out = VecDeque::new();
        let mut failure = None;
        let r = {
            let mut de = serde_json::Deserializer::from_slice(b"{\"a\": [1], \"A\": [2]}");
            StepSeed {
                steps: path.steps(),
                pending: &mut out,
                failure: &mut failure,
            }
            .deserialize(&mut de)
        };
        assert!(r.is_err());
        assert_eq!(failure, Some(Error::Ambiguous("a".into())));
        // deviation 2: rows already produced before the error survive.
        assert_eq!(
            out.into_iter().collect::<Vec<_>>(),
            vec![Some(serde_json::json!(1))]
        );
    }

    #[test]
    fn zero_match_rules_per_step() {
        // Name miss / Index out of range / Wild over empty / chained MISSING.
        assert_eq!(
            match_rows(&[PathSeg::Wild, PathSeg::Name("a".into())], "{\"b\": 1}"),
            vec![None]
        );
        assert_eq!(
            match_rows(&[PathSeg::Wild, PathSeg::Index(9)], "[1]"),
            vec![None]
        );
        assert_eq!(
            match_rows(&[PathSeg::Wild, PathSeg::Wild], "[]"),
            vec![None]
        );
        assert_eq!(
            match_rows(
                &[
                    PathSeg::Wild,
                    PathSeg::Name("a".into()),
                    PathSeg::Wild,
                    PathSeg::Name("b".into())
                ],
                "{\"c\": 1}"
            ),
            vec![None]
        );
    }

    #[test]
    fn nested_wild_order_matches_dfs() {
        assert_eq!(
            match_rows(
                &[
                    PathSeg::Wild,
                    PathSeg::Name("a".into()),
                    PathSeg::Wild,
                    PathSeg::Name("b".into())
                ],
                "{\"a\": [{\"b\": 1}, {}, {\"b\": 2}]}"
            ),
            vec![Some(serde_json::json!(1)), None, Some(serde_json::json!(2))]
        );
    }

    #[test]
    fn wild_over_object_collapses_duplicate_keys_in_place() {
        // The deleted whole-document pipeline materialized a serde_json::Value
        // first: with preserve_order its map is an IndexMap, so a byte-identical
        // duplicate key collapsed last-wins *in first-occurrence position*. The
        // streaming MapAccess stream sees every raw pair, so the Wild arm must
        // reproduce that collapse per object.
        let path = &[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild];
        assert_eq!(
            match_rows(path, "{\"a\":{\"x\":1,\"x\":2}}"),
            vec![Some(serde_json::json!(2))]
        );
        assert_eq!(
            match_rows(path, "{\"a\":{\"x\":1,\"y\":9,\"x\":2}}"),
            vec![Some(serde_json::json!(2)), Some(serde_json::json!(9))]
        );
        // No duplicate: unchanged document order.
        assert_eq!(
            match_rows(path, "{\"a\":{\"x\":1,\"y\":9}}"),
            vec![Some(serde_json::json!(1)), Some(serde_json::json!(9))]
        );
    }

    #[test]
    fn wild_over_object_collapse_replaces_the_whole_row_group() {
        // The duplicate's value is itself a container the child wildcard fans
        // out: the collapsed map has x = [2], so the inner wildcard yields just
        // 2 — the first group and its earlier fan-out are replaced wholesale.
        assert_eq!(
            match_rows(
                &[
                    PathSeg::Wild,
                    PathSeg::Name("a".into()),
                    PathSeg::Wild,
                    PathSeg::Wild
                ],
                "{\"a\":{\"x\":[1],\"x\":[2]}}"
            ),
            vec![Some(serde_json::json!(2))]
        );
    }

    #[test]
    fn wild_over_object_collapses_across_many_distinct_keys() {
        // The side-index path: 64 distinct keys followed by a duplicate of the
        // first. The duplicate must replace slot 0 in place, so the order stays
        // k00..k63 while k00 carries the last value. A linear scan would give
        // the same answer for this size; the index exists so the lookup does
        // not degrade to a quadratic scan when the member count grows.
        let mut input = String::from("{\"a\":{");
        for i in 0..64 {
            if i > 0 {
                input.push(',');
            }
            input.push_str(&format!("\"k{i:02}\":{i}"));
        }
        input.push_str(",\"k00\":99}}");
        let path = &[PathSeg::Wild, PathSeg::Name("a".into()), PathSeg::Wild];
        let mut expected: Vec<Option<Value>> =
            (0..64).map(|i| Some(serde_json::json!(i))).collect();
        expected[0] = Some(serde_json::json!(99));
        assert_eq!(match_rows(path, &input), expected);
    }

    #[test]
    fn skipped_subtree_still_validates_string_content() {
        // `Skip` replaces `IgnoredAny` precisely so a subtree the path never
        // reads is validated the way today's whole-line `from_slice` validates
        // it: a lone surrogate escape and a raw non-UTF-8 byte must both fail
        // here, and both would have been accepted under `IgnoredAny`.
        for input in [
            &b"{\"b\":\"\\uD800\",\"a\":1}"[..],
            &b"{\"b\":\"\xff\",\"a\":1}"[..],
        ] {
            let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())]);
            let mut out = VecDeque::new();
            let mut failure = None;
            let r = {
                let mut de = serde_json::Deserializer::from_slice(input);
                StepSeed {
                    steps: path.steps(),
                    pending: &mut out,
                    failure: &mut failure,
                }
                .deserialize(&mut de)
            };
            assert!(r.is_err(), "{input:?} must not be accepted");
            assert!(out.is_empty(), "{input:?}");
        }
    }

    #[test]
    fn skipped_subtree_rejects_a_reserved_number_token_object() {
        // The `arbitrary_precision` token map holds exactly one entry, and
        // today's whole-line `from_slice::<Value>` rejects a *real* object that
        // carries the reserved key and keeps going (`value/de.rs:125-129`).
        // `Skip` must reject it too rather than quietly accept what the
        // reference rejects. Each case asserts the reference first, so this
        // pins parity instead of restating it.
        let path = CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Name("a".into())]);
        let pull = |input: &[u8]| -> (bool, Vec<Option<Value>>) {
            let mut out = VecDeque::new();
            let mut failure = None;
            let r = {
                let mut de = serde_json::Deserializer::from_slice(input);
                StepSeed {
                    steps: path.steps(),
                    pending: &mut out,
                    failure: &mut failure,
                }
                .deserialize(&mut de)
            };
            (r.is_ok(), out.into_iter().collect())
        };

        // `b` is off-path, so its value is skipped.
        let reserved = br#"{"b":{"$serde_json::private::Number":1,"c":2},"a":1}"#;
        assert!(
            serde_json::from_slice::<Value>(reserved).is_err(),
            "reference: the reserved key plus a second key is rejected"
        );
        let (ok, out) = pull(reserved);
        assert!(!ok, "a skipped reserved-token object must be rejected");
        assert!(out.is_empty());

        // A genuine token map — how `arbitrary_precision` delivers a float — is
        // still skipped, and the reference agrees on this one.
        let float = br#"{"b":1.5,"a":1}"#;
        assert!(
            serde_json::from_slice::<Value>(float).is_ok(),
            "reference: a float is fine"
        );
        let (ok, out) = pull(float);
        assert!(ok, "a skipped float must still be skipped");
        assert_eq!(out, vec![Some(serde_json::json!(1))]);
    }

    /// One root pull, with the budget reset and the framing wrapped in
    /// `BudgetedRead` exactly as the handlers do it (Task 6/7) — the fix under
    /// test is *where* the reset happens, so the test has to reproduce the
    /// whole charging path. A budget no `BudgetedRead` charges stays untouched:
    /// `fired` would be vacuously false and the boundary assertions below would
    /// prove nothing.
    fn pull_with_budget(framing: &mut DocFraming<Cursor<Vec<u8>>>) -> (Vec<Option<Value>>, bool) {
        let budget = Rc::new(Budget::default());
        let path = CompiledPath::from_segments(&[PathSeg::Wild]);
        let mut out = VecDeque::new();
        let mut failure = None;
        budget.reset();
        {
            let mut de = serde_json::Deserializer::from_reader(BudgetedRead::new(
                &mut *framing,
                Rc::clone(&budget),
            ));
            let r = child_into(path.steps(), &mut failure, &mut out).deserialize(&mut de);
            let _ = r;
        }
        (out.into_iter().collect(), budget.fired())
    }

    #[test]
    fn element_barely_under_cap_is_accepted() {
        // The boundary that decides where the budget reset lives: delivered
        // bytes stay one byte under `MAX_RECORD`, so the record passes only
        // when the budget is reset once for the whole pull. One byte of margin,
        // not zero, because the assertion relies on a serde fact: a string is
        // self-delimiting and nothing past the closing quote is ever read —
        // keep the margin so that fact is not on the critical path. The exact
        // 1 MB line is pinned at the handler level by
        // `document_input_record_cap_exact_allowed` (`json/document.rs`).
        let content = "a".repeat(MAX_RECORD as usize - 3);
        let (vals, fired) = pull_with_budget(&mut DocFraming::new(BufReader::new(Cursor::new(
            format!("[\"{content}\"]").into_bytes(),
        ))));
        assert!(
            !fired,
            "a record of exactly MAX_RECORD delivered bytes must pass"
        );
        assert_eq!(vals.len(), 1);
    }

    #[test]
    fn budget_is_charged_per_root_not_accumulated() {
        // Each root is one pull and one budget reset: four 400 KB elements
        // total ~1.6 MB,
        // yet no single pull exceeds the cap. A cumulative budget would fire on
        // the second element and reject a document the framing accepts.
        // One array of four string elements — four concatenated arrays would be
        // trailing characters after the first root, not a document.
        let elem = |c: char| format!("\"{}\"", c.to_string().repeat(400 * 1024));
        let doc = format!(
            "[{}, {}, {}, {}]",
            elem('a'),
            elem('b'),
            elem('c'),
            elem('d')
        );
        let mut framing = DocFraming::new(BufReader::new(Cursor::new(doc.into_bytes())));
        let mut total = 0usize;
        loop {
            let (vals, fired) = pull_with_budget(&mut framing);
            assert!(!fired, "the cap must count one root, not the document");
            total += vals.len();
            if vals.is_empty() {
                break;
            }
        }
        assert_eq!(total, 4);
    }
}
