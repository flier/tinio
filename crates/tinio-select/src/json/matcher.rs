//! `PathSeg` compiled to a state machine, plus the pausable stepper that runs
//! it over the event flow of a `ValueSource`. The matcher drives
//! `peek`/`has_next`/`name` itself — struson panics when a caller peeks at a
//! member-name position or past the top-level value — and it hands control
//! back as soon as one record is queued, so a caller that takes each record
//! holds one row of `pending` at a time. A caller that finishes a whole root
//! first (the DOCUMENT single-value path, and the LINES line) buffers that
//! root's matches instead; the handler charges them against the 1 MB per-root
//! cap, and its charged `Read` backstop aborts a grossly over-cap value
//! mid-read, so memory is O(cap) with a small constant factor, not the whole
//! document (see `Matcher` for the exact bound).
//!
//! Unmatched subtrees go through `ValueSource::skip_value`: nothing
//! materializes, and string contents are validated exactly as the whole-line
//! parse this replaces validated them.

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque, hash_map::Entry},
};

use serde_json::Value;

use crate::{
    error::Error,
    json::value::key_matches,
    path::PathSeg,
    row::{NameStyle, Record},
};

/// The FROM path as the matcher walks it: `PathSeg` with the leading `Wild`
/// (the implicit `S3Object[*]` root step) dropped — that root rule is the
/// framing's job, so the compiled path never branches on it. Compiled once per
/// reader.
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

    /// Hand the compiled steps over to the matcher, which owns them per run.
    pub(crate) fn into_steps(self) -> Vec<PathSeg> {
        self.steps
    }
}

/// The value type a `ValueSource` reports for the value it is positioned at.
/// A local mirror of struson's `ValueType`, so a test double needs no reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueKind {
    Array,
    Object,
    String,
    Number,
    Bool,
    Null,
}

/// The one seam between the matcher and the bytes. Every method folds a reader
/// failure into the crate's `Error` here, so nothing above ever sees a
/// `struson` error type.
pub(crate) trait ValueSource {
    fn peek(&mut self) -> Result<ValueKind, Error>;
    fn has_next(&mut self) -> Result<bool, Error>;
    /// Borrowed, so the real source does not allocate per object member: the
    /// matcher only compares the name and drops it.
    fn name(&mut self) -> Result<Cow<'_, str>, Error>;
    fn text(&mut self) -> Result<String, Error>;
    fn number(&mut self) -> Result<String, Error>;
    fn boolean(&mut self) -> Result<bool, Error>;
    fn null(&mut self) -> Result<(), Error>;
    fn begin_object(&mut self) -> Result<(), Error>;
    fn end_object(&mut self) -> Result<(), Error>;
    fn begin_array(&mut self) -> Result<(), Error>;
    fn end_array(&mut self) -> Result<(), Error>;
    /// An unmatched subtree: validate, materialize nothing (semantics 8).
    fn skip_value(&mut self) -> Result<(), Error>;
}

/// How far one `next_root` call got. There is no "still in flight" variant: the
/// private `step` reports that as `None`, so a caller can never see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Progress {
    /// One record (MISSING included) is queued for the caller.
    StopRoot,
    /// The current root value is fully consumed.
    End,
}

/// A number token straight from the input, kept verbatim (deviation 8).
///
/// Do NOT use `serde_json::from_str::<Value>` here: with `arbitrary_precision`
/// its scanner injects a `+` into a signless exponent (`de.rs:1027-1043`,
/// `scan_exponent`), so `from_str("1e309")` yields `Number("1e+309")` and
/// deviation 8 fails. `from_string_unchecked` is a safe constructor that wraps
/// the string as-is; struson already validated the token (verified fact 11),
/// so no re-parse is needed.
fn verbatim_number(token: String) -> Value {
    Value::Number(serde_json::Number::from_string_unchecked(token))
}

/// The member row groups of one `Wild`-over-object level.
///
/// Only that scope buffers: its members must be replaceable as a whole when a
/// byte-identical duplicate key appears, and every other scope delivers in
/// document order as each value completes, so a `Wild` over an ARRAY keeps no
/// per-element state at all — the rows go straight to the parent group, which
/// is what keeps a 1 MB array of tiny elements from retaining more than the
/// root it was charged for.
#[derive(Default)]
struct Slots {
    /// One group per member name, in first-occurrence order.
    slots: Vec<VecDeque<Option<Value>>>,
    /// Slot lookup by member name. The per-root 1 MB cap does not bound an
    /// attacker-controlled member count usefully (~10^5 one-character keys
    /// fit), so a linear scan would be quadratic on that input; the map keeps
    /// the in-place duplicate replace O(1) per member, as the `IndexMap` this
    /// reproduces was.
    index: HashMap<String, usize>,
}

impl Slots {
    /// The group for `key`, cleared when the key was already seen so a
    /// byte-identical duplicate replaces it *in place*. One `entry` hashes the
    /// key once, where a lookup-then-insert would hash it twice.
    fn slot_for(&mut self, key: String) -> usize {
        let Slots { slots, index } = self;
        match index.entry(key) {
            Entry::Occupied(seen) => {
                let index = *seen.get();
                slots[index].clear();
                index
            }
            Entry::Vacant(fresh) => {
                let index = slots.len();
                slots.push(VecDeque::new());
                fresh.insert(index);
                index
            }
        }
    }
}

/// What one frame level is doing inside its container.
#[derive(Clone, Copy)]
enum Scope {
    /// A `Name` step: only the matching member is read (semantics 6 and 7).
    Named,
    /// A `Wild` step: every child is read. Over an object, one slot per
    /// member; over an array, nothing per element.
    Wild { object: bool },
    /// An `Index(i)` step: children are counted and only element `target` is
    /// read (semantics 5).
    Indexed { target: usize, ord: usize },
}

/// One open container.
struct Frame {
    scope: Scope,
    /// The step index that applies to this frame's children.
    at: usize,
    /// Row groups being filled: one per `Wild`-over-object member, otherwise
    /// one.
    slots: Slots,
    /// Which slot of the parent's `Slots` receives this frame's rows.
    slot: usize,
    /// Did this container carry any child? A frame that never did emits one
    /// MISSING row (semantics 2).
    children_seen: bool,
    /// The value a final-`Name` scope holds until the object closes — the
    /// ambiguity verdict needs the full member scan (semantics 6).
    held: Option<Option<Value>>,
    /// Did the open `Named` scope already match its case-folded key?
    named_matched: bool,
    /// The container end has been seen; close on the next step.
    closing: bool,
}

impl Frame {
    /// A final `Name` scope: no step follows it, so its value is the record of
    /// the deepest level — held until the object closes, because the ambiguity
    /// verdict needs the full member scan (semantics 6). The one home of that
    /// rule: `record_row` holds on it and `close_frame` releases on it.
    fn is_final_name(&self, steps_len: usize) -> bool {
        matches!(self.scope, Scope::Named) && self.at + 1 == steps_len
    }
}

/// The record-assembly state machine. Pausable: the frame stack survives a
/// return to the caller, so one root may take many `next_root` calls.
///
/// Bound: the buffered rows are bounded by the root, not by one record. A
/// `next_root` returns as soon as a record is queued, so a caller that takes
/// each record keeps `pending` at one row; a caller that finishes a whole root
/// first (the DOCUMENT single-value path, and the LINES line) accumulates that
/// root's matches, which the handler charges against the 1 MB per-root cap.
/// The handler's charged `Read` backstop bounds that accumulation at the cap
/// plus 64 KiB even before the exact span check runs, so memory is O(cap) with
/// a small constant factor, not the whole root. The bound is on *charged* bytes
/// — the bytes the root is served and the rendered length of the rows it queues
/// (`charge`) — so per-row `Value`/`Map`/`Vec` overhead and the `Slots` key
/// index sit outside it. The only other retained state is one row group per
/// `Wild`-over-object member (needed for the in-place duplicate replace), also
/// inside that cap.
pub(crate) struct Matcher {
    steps: Vec<PathSeg>,
    frames: Vec<Frame>,
    /// The records ready for the caller.
    pending: VecDeque<Option<Value>>,
    /// The one record already handed over through `StopRoot` and not yet taken
    /// by the caller: the slot `take_record` serves first, so a caller that
    /// keeps calling without taking (the LINES handler reads a whole line per
    /// pull) does not see `StopRoot` forever instead of reading on.
    handed_over: Option<Option<Value>>,
    done: bool,
    #[cfg(test)]
    pending_high: usize,
    /// Running byte total of the rows in `pending` plus `handed_over`, so the
    /// high-water mark stays O(1) per queued row instead of re-summing the
    /// whole batch (which would be quadratic on a large single-value root).
    #[cfg(test)]
    buffered_bytes: usize,
}

impl Matcher {
    pub(crate) fn new(path: CompiledPath) -> Self {
        Self {
            steps: path.into_steps(),
            frames: Vec::new(),
            pending: VecDeque::new(),
            handed_over: None,
            done: false,
            #[cfg(test)]
            pending_high: 0,
            #[cfg(test)]
            buffered_bytes: 0,
        }
    }

    /// Drive the matcher until one record is queued or the root is over.
    ///
    /// A queued record stops the root immediately; `End` is reported once the
    /// root is complete and nothing is in flight.
    pub(crate) fn next_root(&mut self, source: &mut dyn ValueSource) -> Result<Progress, Error> {
        loop {
            match self.step(source)? {
                // One record is queued: hand it over.
                Some(Progress::StopRoot) => {
                    self.handed_over = self.pending.pop_front();
                    return Ok(Progress::StopRoot);
                }
                // The root's last frame closed; the queued batch stays for the
                // caller and the next root starts from a clean stack.
                Some(Progress::End) => {
                    self.reset();
                    return Ok(Progress::End);
                }
                // The root is still in flight: the loop reads on.
                None => {}
            }
        }
    }

    /// Is a record waiting for the caller? Never read past one the caller has
    /// not taken: the batch is reported one row at a time, and a caller that
    /// wants to finish the root first (LINES) simply keeps calling without
    /// taking.
    fn queued(&self) -> bool {
        self.handed_over.is_none() && !self.pending.is_empty()
    }

    /// One step of the root's event flow: `Some` once the root's `Progress` can
    /// be reported, `None` while it is still in flight.
    fn step(&mut self, source: &mut dyn ValueSource) -> Result<Option<Progress>, Error> {
        if !self.queued() && !self.done {
            self.advance(source)?;
        }
        // A step that closed the root may also have queued that root's last
        // row: the hand-over comes first either way.
        Ok(if self.queued() {
            Some(Progress::StopRoot)
        } else if self.done {
            Some(Progress::End)
        } else {
            None
        })
    }

    /// One traversal step over the root's event flow.
    fn advance(&mut self, source: &mut dyn ValueSource) -> Result<(), Error> {
        if self.frames.is_empty() {
            return self.open_root(source);
        }
        if self.frame()?.closing {
            self.close_frame(source)?;
            return Ok(());
        }
        match self.frame()?.scope {
            Scope::Named | Scope::Wild { object: true } => self.step_container(source),
            Scope::Wild { object: false } | Scope::Indexed { .. } => self.step_array(source),
        }
    }

    /// The first step: the root value is the container that step reads.
    fn open_root(&mut self, source: &mut dyn ValueSource) -> Result<(), Error> {
        if self.steps.is_empty() {
            // `SELECT * FROM S3Object`: the root value itself is the record.
            let value = read_value(source.peek()?, source)?;
            self.push_pending(Some(value));
            self.done = true;
            return Ok(());
        }
        // The block ends the step borrow; `Scope` is owned, so the arms are
        // free to take `&mut self`.
        let scope = {
            let step = &self.steps[0];
            let kind = source.peek()?;
            apply_step(step, kind)
        };
        match scope {
            Some(scope) => self.open(scope, 0, source),
            // The first step meets a value it cannot read (semantics 9): the
            // whole root is one MISSING row. The value is still consumed, or
            // the root never finishes and the trailing probe panics on a
            // top-level value that was never read (probe fact 4).
            None => {
                source.skip_value()?;
                self.push_pending(None);
                self.done = true;
                Ok(())
            }
        }
    }

    /// One step of a `Named`/`Wild`-over-object container. `has_next` always
    /// runs before anything peeks, because after one member's value the reader
    /// sits at a member-name position (struson panics on a peek there).
    fn step_container(&mut self, source: &mut dyn ValueSource) -> Result<(), Error> {
        if !source.has_next()? {
            self.mark_closing()?;
            return Ok(());
        }
        let kind = match self.frame()?.scope {
            Scope::Named => {
                let key = source.name()?;
                let at = self.frame()?.at;
                let PathSeg::Name(name) = &self.steps[at] else {
                    return Err(Error::Invariant("a Named scope is opened by a Name step"));
                };
                if !key_matches(&key, name, NameStyle::Bare) {
                    // A member the step does not want: validate, materialize
                    // nothing (semantics 8).
                    return source.skip_value();
                }
                // Semantics 7: a second case-folded match is ambiguous, and it
                // is reported here rather than at close so rows produced before
                // it survive (deviation 2).
                if self.frame()?.named_matched {
                    return Err(Error::Ambiguous(name.clone()));
                }
                self.frame_mut()?.named_matched = true;
                source.peek()?
            }
            Scope::Wild { object: true } => {
                // A byte-identical duplicate replaces its whole group in place,
                // reproducing the deleted pipeline's preserve_order IndexMap.
                // The slot map owns the key, so the borrow is given up here.
                let key = source.name()?.into_owned();
                let frame = self.frame_mut()?;
                frame.slot = frame.slots.slot_for(key);
                source.peek()?
            }
            Scope::Wild { object: false } | Scope::Indexed { .. } => {
                return Err(Error::Invariant("step_container only serves object scopes"));
            }
        };
        self.frame_mut()?.children_seen = true;
        self.apply_at(self.child_index()?, kind, source)
    }

    /// One step of an array scope: a `Wild` reads every element, an `Index(i)`
    /// counts children and reads only element `target` (semantics 5).
    fn step_array(&mut self, source: &mut dyn ValueSource) -> Result<(), Error> {
        if !source.has_next()? {
            self.mark_closing()?;
            return Ok(());
        }
        let indexed = match &mut self.frame_mut()?.scope {
            Scope::Indexed { target, ord } => {
                let current = *ord;
                *ord += 1;
                Some((*target, current))
            }
            _ => None,
        };
        if let Some((target, ord)) = indexed
            && ord != target
        {
            // An element the step does not want: validate, materialize
            // nothing (semantics 8).
            return source.skip_value();
        }
        // Array scopes keep no per-element state: a `Wild` appends each
        // element's rows to its parent group as that element completes, which is
        // document order, so a 1 MB array of tiny elements retains no more than
        // the root it was charged for.
        self.frame_mut()?.children_seen = true;
        let kind = source.peek()?;
        self.apply_at(self.child_index()?, kind, source)
    }

    /// Apply the step at `at` to the value just peeked, routing its rows into
    /// the frame on top.
    fn apply_at(
        &mut self,
        at: usize,
        kind: ValueKind,
        source: &mut dyn ValueSource,
    ) -> Result<(), Error> {
        if at == self.steps.len() {
            // No step is left: this value is the record of the deepest level.
            let value = read_value(kind, source)?;
            self.record_row(Some(value))?;
            return Ok(());
        }
        let scope = {
            let step = &self.steps[at];
            apply_step(step, kind)
        };
        match scope {
            Some(scope) => self.open(scope, at, source),
            // Semantics 9: a type the step cannot read is a zero-match, and a
            // step applied to a scalar is exactly one MISSING (semantics 3).
            None => {
                // Semantics 3, 8 and 9: a step over a type it cannot read is a
                // zero-match, and a zero match is exactly one MISSING row. The
                // value is still consumed, so the container stays well-formed.
                source.skip_value()?;
                self.emit(None);
                Ok(())
            }
        }
    }

    /// Open the container `scope` reads as a new path level under `at`.
    fn open(&mut self, scope: Scope, at: usize, source: &mut dyn ValueSource) -> Result<(), Error> {
        match scope {
            Scope::Named | Scope::Wild { object: true } => source.begin_object()?,
            Scope::Wild { object: false } | Scope::Indexed { .. } => source.begin_array()?,
        }
        self.frames.push(Frame {
            scope,
            at,
            slots: Slots::default(),
            slot: 0,
            children_seen: false,
            held: None,
            named_matched: false,
            closing: false,
        });
        Ok(())
    }

    /// The frame on top of the stack: the one open container the next step
    /// applies to. Every step runs with a frame open, which the path shape
    /// guarantees — a step that emptied the stack is already `done` — and the
    /// empty case is an error rather than a panic.
    fn frame(&self) -> Result<&Frame, Error> {
        self.frames
            .last()
            .ok_or(Error::Invariant("a frame is open"))
    }

    /// The same frame, mutably.
    fn frame_mut(&mut self) -> Result<&mut Frame, Error> {
        self.frames
            .last_mut()
            .ok_or(Error::Invariant("a frame is open"))
    }

    /// The step index the current frame's children obey.
    fn child_index(&self) -> Result<usize, Error> {
        Ok(self.frame()?.at + 1)
    }

    /// Mark the current frame's container end.
    fn mark_closing(&mut self) -> Result<(), Error> {
        self.frame_mut()?.closing = true;
        Ok(())
    }

    /// Route one row produced at the deepest level.
    ///
    /// A final-`Name` scope holds its row until the object closes (semantics
    /// 6); every other scope emits it straight away.
    fn record_row(&mut self, row: Option<Value>) -> Result<(), Error> {
        let final_name = self.frame()?.is_final_name(self.steps.len());
        let frame = self.frame_mut()?;
        frame.children_seen = true;
        if final_name {
            // The ambiguity verdict needs the whole member scan (semantics 6).
            frame.held = Some(row);
        } else {
            self.emit(row);
        }
        Ok(())
    }

    /// Hand one record to the nearest enclosing Wild-over-object member group,
    /// or straight to the caller when there is none.
    ///
    /// Only a Wild-over-object buffers: its members must be replaceable as a
    /// whole when a byte-identical duplicate key appears, and every other level
    /// delivers in document order anyway.
    fn emit(&mut self, row: Option<Value>) {
        let sink = self
            .frames
            .iter()
            .enumerate()
            .rev()
            .find(|(_, f)| matches!(f.scope, Scope::Wild { object: true }))
            .map(|(index, frame)| (index, frame.slot));
        if let Some((index, slot)) = sink
            && let Some(group) = self.frames[index].slots.slots.get_mut(slot)
        {
            group.push_back(row);
            return;
        }
        self.push_pending(row);
    }

    /// Queue one record for the caller.
    fn push_pending(&mut self, row: Option<Value>) {
        #[cfg(test)]
        self.charge(&row);
        self.pending.push_back(row);
    }

    /// Test-only accounting of the rows that are buffered but not yet
    /// delivered (`pending` plus the one-row hand-over slot), measured in
    /// **bytes** rather than in records: the memory tripwire that replaces the
    /// spec's counting allocator, which `unsafe_code = "forbid"` rules out. An
    /// item count would say nothing about memory — one queued 1 MB value and
    /// 5 000 tiny ones are the same number and three orders of magnitude apart
    /// in size. The bytes are **rendered** ones (`Value::to_string()`), so the
    /// per-row `Value`/`Map`/`Vec` overhead and the `Slots` key index stay
    /// outside the tripwire: it tracks the charged bytes, not RSS. The total is
    /// kept as a running sum so it stays O(1) per queued row, and the high-water
    /// mark is that total at queue time, exactly as the retired `core.rs`
    /// `touch_pending_high` measured it (deviation 12).
    #[cfg(test)]
    fn charge(&mut self, row: &Option<Value>) {
        self.buffered_bytes += bytes(row);
        self.pending_high = self.pending_high.max(self.buffered_bytes);
    }

    /// One record is queued; hand it over.
    pub(crate) fn take_record(&mut self) -> Option<Record> {
        let row = match self.handed_over.take() {
            Some(row) => row,
            None => self.pending.pop_front()?,
        };
        #[cfg(test)]
        {
            self.buffered_bytes -= bytes(&row);
        }
        Some(Record::Json(row))
    }

    /// Queue one MISSING row the handler synthesizes itself: a DOCUMENT root
    /// array that closed with no element never reached the matcher, and the
    /// `S3Object[*]` zero-match rule still owes one row.
    pub(crate) fn queue_missing(&mut self) {
        self.push_pending(None);
    }

    /// Drop every queued record (a terminal pull error).
    pub(crate) fn drop_pending(&mut self) {
        self.pending.clear();
        self.handed_over = None;
        #[cfg(test)]
        {
            self.buffered_bytes = 0;
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.pending.len() + usize::from(self.handed_over.is_some())
    }

    #[cfg(test)]
    pub(crate) fn pending_high(&self) -> usize {
        self.pending_high
    }

    /// Close the frame on top of the stack, promoting its rows upward.
    ///
    /// Closing the container in the reader comes first: the parent's next
    /// has_next() must see the container as finished.
    fn close_frame(&mut self, source: &mut dyn ValueSource) -> Result<(), Error> {
        let Some(frame) = self.frames.pop() else {
            return Err(Error::Invariant("close is reached with a frame"));
        };
        match frame.scope {
            Scope::Named | Scope::Wild { object: true } => source.end_object()?,
            Scope::Wild { object: false } | Scope::Indexed { .. } => source.end_array()?,
        }
        let named_final = frame.is_final_name(self.steps.len());
        if named_final {
            self.emit(frame.held.flatten());
        } else {
            if matches!(frame.scope, Scope::Wild { object: true }) {
                // The member groups surface in first-occurrence order. An empty
                // one drains nothing and falls through to the MISSING rule
                // below, exactly as the array and `Index` scopes do.
                for group in frame.slots.slots {
                    for row in group {
                        self.emit(row);
                    }
                }
            }
            // Semantics 2: a container that carried no child is one MISSING row.
            if !frame.children_seen {
                self.emit(None);
            }
        }
        if self.frames.is_empty() {
            self.done = true;
        }
        Ok(())
    }

    /// Forget the finished root so the next one starts from a clean stack.
    ///
    /// The queued batch is deliberately kept: `next_root` reports `End` as soon
    /// as the root is consumed, and the caller may not have taken every record
    /// of that root yet.
    fn reset(&mut self) {
        self.frames.clear();
        self.done = false;
    }
}

/// The rendered byte width of one queued row; a MISSING row costs nothing.
#[cfg(test)]
fn bytes(row: &Option<Value>) -> usize {
    row.as_ref().map_or(0, |v| v.to_string().len())
}

/// Which scope reads a value of `kind` under `step`, or `None` when the step
/// cannot read that type (a zero-match, semantics 3 and 9).
fn apply_step(step: &PathSeg, kind: ValueKind) -> Option<Scope> {
    match (step, kind) {
        (PathSeg::Name(_), ValueKind::Object) => Some(Scope::Named),
        (PathSeg::Wild, ValueKind::Object) => Some(Scope::Wild { object: true }),
        (PathSeg::Wild, ValueKind::Array) => Some(Scope::Wild { object: false }),
        (PathSeg::Index(target), ValueKind::Array) => Some(Scope::Indexed {
            target: *target,
            ord: 0,
        }),
        _ => None,
    }
}

fn read_value(kind: ValueKind, source: &mut dyn ValueSource) -> Result<Value, Error> {
    Ok(match kind {
        ValueKind::Object => {
            source.begin_object()?;
            let mut map = serde_json::Map::new();
            while source.has_next()? {
                // The map owns the name, so the source borrow is given up
                // before the member value is read.
                let name = source.name()?.into_owned();
                let value = read_value(source.peek()?, source)?;
                map.insert(name, value);
            }
            source.end_object()?;
            Value::Object(map)
        }
        ValueKind::Array => {
            source.begin_array()?;
            let mut items = Vec::new();
            while source.has_next()? {
                items.push(read_value(source.peek()?, source)?);
            }
            source.end_array()?;
            Value::Array(items)
        }
        ValueKind::String => Value::String(source.text()?),
        ValueKind::Number => verbatim_number(source.number()?),
        ValueKind::Bool => Value::Bool(source.boolean()?),
        ValueKind::Null => {
            source.null()?;
            Value::Null
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use derive_more::PartialEq;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        error::Error,
        json::source::{Reader, ReaderSource, settings},
        path::PathSeg,
    };

    // ------------------------------------------------------------------
    // The scripted double.
    // ------------------------------------------------------------------

    /// A scripted `ValueSource`: the instructions are exactly the reader calls
    /// the JSON a real reader would have produced implies, so a test pins the
    /// matcher's semantics — and its `peek`/`has_next`/`name` sequencing —
    /// without a reader. Any instruction the matcher was not supposed to ask
    /// for panics by name and index, which is what makes the double strict.
    #[derive(Default)]
    struct Script {
        steps: Vec<ScriptStep>,
        at: usize,
    }

    /// One reader call the matcher is expected to make. `Kind` is the pending
    /// `peek`; `Begin`/`End` are `begin_object`/`begin_array` and their
    /// counterparts; the rest are the value reads and `skip_value`.
    #[derive(Clone, Debug, PartialEq)]
    enum ScriptStep {
        Kind(ValueKind),
        Name(String),
        Text(String),
        Number(String),
        Bool(bool),
        Null,
        Begin,
        End,
        HasNext(bool),
        Skip,
    }

    impl Script {
        fn new(steps: Vec<ScriptStep>) -> Self {
            Self { steps, at: 0 }
        }

        /// The next instruction, demanding `want` by name.
        fn take(&mut self, want: &str) -> ScriptStep {
            let step = self
                .steps
                .get(self.at)
                .cloned()
                .unwrap_or_else(|| panic!("script exhausted at {}, expected {want}", self.at));
            self.at += 1;
            step
        }

        /// The next instruction, which must be `want`.
        fn take_exact(&mut self, want: ScriptStep) {
            let got = self.take(&format!("{want:?}"));
            assert_eq!(got, want, "script instruction {}", self.at - 1);
        }
    }

    const BEGIN: ScriptStep = ScriptStep::Begin;
    const END: ScriptStep = ScriptStep::End;
    const SKIP: ScriptStep = ScriptStep::Skip;

    fn kind(value: ValueKind) -> ScriptStep {
        ScriptStep::Kind(value)
    }

    fn object() -> ScriptStep {
        kind(ValueKind::Object)
    }

    fn array() -> ScriptStep {
        kind(ValueKind::Array)
    }

    fn number_kind() -> ScriptStep {
        kind(ValueKind::Number)
    }

    fn name(n: &str) -> ScriptStep {
        ScriptStep::Name(n.to_owned())
    }

    fn number(token: &str) -> ScriptStep {
        ScriptStep::Number(token.to_owned())
    }

    fn has(next: bool) -> ScriptStep {
        ScriptStep::HasNext(next)
    }

    fn n(s: &str) -> PathSeg {
        PathSeg::Name(s.into())
    }

    impl ValueSource for Script {
        fn peek(&mut self) -> Result<ValueKind, Error> {
            match self.take("a Kind instruction") {
                ScriptStep::Kind(value) => Ok(value),
                other => panic!("peek got {other:?}, expected a Kind instruction"),
            }
        }

        fn has_next(&mut self) -> Result<bool, Error> {
            match self.take("a HasNext instruction") {
                ScriptStep::HasNext(next) => Ok(next),
                other => panic!("has_next got {other:?}, expected a HasNext instruction"),
            }
        }

        fn name(&mut self) -> Result<Cow<'_, str>, Error> {
            match self.take("a Name instruction") {
                ScriptStep::Name(s) => Ok(Cow::Owned(s)),
                other => panic!("name got {other:?}, expected a Name instruction"),
            }
        }

        fn text(&mut self) -> Result<String, Error> {
            match self.take("a Text instruction") {
                ScriptStep::Text(s) => Ok(s),
                other => panic!("text got {other:?}, expected a Text instruction"),
            }
        }

        fn number(&mut self) -> Result<String, Error> {
            match self.take("a Number instruction") {
                ScriptStep::Number(s) => Ok(s),
                other => panic!("number got {other:?}, expected a Number instruction"),
            }
        }

        fn boolean(&mut self) -> Result<bool, Error> {
            match self.take("a Bool instruction") {
                ScriptStep::Bool(b) => Ok(b),
                other => panic!("boolean got {other:?}, expected a Bool instruction"),
            }
        }

        fn null(&mut self) -> Result<(), Error> {
            self.take_exact(ScriptStep::Null);
            Ok(())
        }

        fn begin_object(&mut self) -> Result<(), Error> {
            self.take_exact(BEGIN);
            Ok(())
        }

        fn end_object(&mut self) -> Result<(), Error> {
            self.take_exact(END);
            Ok(())
        }

        fn begin_array(&mut self) -> Result<(), Error> {
            self.take_exact(BEGIN);
            Ok(())
        }

        fn end_array(&mut self) -> Result<(), Error> {
            self.take_exact(END);
            Ok(())
        }

        fn skip_value(&mut self) -> Result<(), Error> {
            self.take_exact(SKIP);
            Ok(())
        }
    }

    /// Every record the matcher yields for `segments` over `script`.
    fn rows(segments: &[PathSeg], script: Vec<ScriptStep>) -> Vec<Option<Value>> {
        let (out, error) = run(segments, script);
        assert!(error.is_none(), "unexpected error: {error:?}");
        out
    }

    /// Drive `source` over `segments`, collecting the records and the error the
    /// pull ends on. The one loop both sources share; `ctx` prefixes the panic
    /// messages (empty for the script, the input for the real reader).
    fn drive(
        segments: &[PathSeg],
        source: &mut dyn ValueSource,
        ctx: &str,
    ) -> (Vec<Option<Value>>, Option<Error>) {
        let mut matcher = Matcher::new(CompiledPath::from_segments(segments));
        let mut out = Vec::new();
        let error = loop {
            match matcher.next_root(source) {
                Ok(Progress::StopRoot) => match matcher.take_record() {
                    Some(Record::Json(v)) => out.push(v),
                    other => panic!("{ctx}expected a JSON record, got {other:?}"),
                },
                Ok(Progress::End) => break None,
                Err(e) => break Some(e),
            }
        };
        (out, error)
    }

    /// The records and the error of a scripted pull.
    fn run(segments: &[PathSeg], script: Vec<ScriptStep>) -> (Vec<Option<Value>>, Option<Error>) {
        drive(segments, &mut Script::new(script), "")
    }

    // ------------------------------------------------------------------
    // The real reader: the half of the seam a script cannot express.
    // ------------------------------------------------------------------

    /// Drive the real reader and collect the records and the error. The source
    /// is the production `ReaderSource`, so the reader-side half of the seam —
    /// escape decoding, UTF-8 and surrogate validation on the skipped path — is
    /// the one the handlers run.
    fn real_rows_err(segments: &[PathSeg], input: &[u8]) -> (Vec<Option<Value>>, Option<Error>) {
        let mut reader = Reader::new_custom(Cursor::new(input.to_vec()), settings());
        let mut source = ReaderSource::new(&mut reader);
        drive(segments, &mut source, &format!("{input:?}: "))
    }

    // ------------------------------------------------------------------
    // Scalar steps: a step over a scalar is a zero-match (semantics 3).
    // ------------------------------------------------------------------

    #[test]
    fn wild_over_int_scalar_is_missing() {
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("x"), PathSeg::Wild],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    SKIP,
                    has(false),
                    END,
                ],
            ),
            vec![None]
        );
    }

    #[test]
    fn wild_over_float_scalar_is_missing() {
        // The int/float distinction the old test pinned was serde_json's
        // arbitrary_precision number-token quirk (deviation 10), which the new
        // kernel does not have: a Wild step over any number scalar is one
        // MISSING row.
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("x"), PathSeg::Wild],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    SKIP,
                    has(false),
                    END,
                ],
            ),
            vec![None]
        );
    }

    #[test]
    fn index_over_number_map_is_missing() {
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("x"), PathSeg::Index(0)],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    SKIP,
                    has(false),
                    END,
                ],
            ),
            vec![None]
        );
    }

    #[test]
    fn scalar_values_are_materialized() {
        // A matched scalar is assembled by the matching step's own value read,
        // which is what the Text and Bool instructions pin.
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("s")],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("s"),
                    kind(ValueKind::String),
                    ScriptStep::Text("x".into()),
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!("x"))]
        );
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("b")],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("b"),
                    kind(ValueKind::Bool),
                    ScriptStep::Bool(true),
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(true))]
        );
    }

    // ------------------------------------------------------------------
    // Name steps: case folding, holding and the ambiguity rule.
    // ------------------------------------------------------------------

    #[test]
    fn escaped_key_matches_case_insensitively() {
        // The script supplies the member name the reader would have decoded, so
        // this pins the matcher's half: the name folds case-insensitively
        // (semantics 10). The reader's decoding of the escape itself is pinned
        // by real_reader_integration below.
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("rules"), PathSeg::Wild],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("Rules"),
                    array(),
                    BEGIN,
                    has(true),
                    number_kind(),
                    number("1"),
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(1))]
        );
    }

    #[test]
    fn final_name_holds_value_until_object_close() {
        // Duplicate case-folded key: the held value must not be enqueued early;
        // on the error nothing has been emitted.
        let (out, error) = run(
            &[PathSeg::Wild, n("id")],
            vec![
                object(),
                BEGIN,
                has(true),
                name("id"),
                number_kind(),
                number("1"),
                has(true),
                name("ID"),
            ],
        );
        assert_eq!(error, Some(Error::Ambiguous("id".into())));
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn non_final_name_duplicate_fails_mid_stream() {
        let (out, error) = run(
            &[PathSeg::Wild, n("a"), PathSeg::Wild],
            vec![
                object(),
                BEGIN,
                has(true),
                name("a"),
                array(),
                BEGIN,
                has(true),
                number_kind(),
                number("1"),
                has(false),
                END,
                has(true),
                name("A"),
            ],
        );
        assert_eq!(error, Some(Error::Ambiguous("a".into())));
        // deviation 2: rows already produced before the error survive.
        assert_eq!(out, vec![Some(json!(1))]);
    }

    #[test]
    fn zero_match_rules_per_step() {
        // Name miss / Index out of range / Wild over empty / chained MISSING.
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("a")],
                vec![object(), BEGIN, has(true), name("b"), SKIP, has(false), END],
            ),
            vec![None]
        );
        assert_eq!(
            rows(
                &[PathSeg::Wild, PathSeg::Index(9)],
                vec![array(), BEGIN, has(true), SKIP, has(false), END],
            ),
            vec![None]
        );
        assert_eq!(
            rows(
                &[PathSeg::Wild, PathSeg::Wild],
                vec![array(), BEGIN, has(false), END],
            ),
            vec![None]
        );
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("a"), PathSeg::Wild, n("b")],
                vec![object(), BEGIN, has(true), name("c"), SKIP, has(false), END],
            ),
            vec![None]
        );
    }

    // ------------------------------------------------------------------
    // Wild fan-out: DFS order and the object member groups.
    // ------------------------------------------------------------------

    #[test]
    fn nested_wild_order_matches_dfs() {
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("a"), PathSeg::Wild, n("b")],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("a"),
                    array(),
                    BEGIN,
                    has(true),
                    object(),
                    BEGIN,
                    has(true),
                    name("b"),
                    number_kind(),
                    number("1"),
                    has(false),
                    END,
                    has(true),
                    object(),
                    BEGIN,
                    has(false),
                    END,
                    has(true),
                    object(),
                    BEGIN,
                    has(true),
                    name("b"),
                    number_kind(),
                    number("2"),
                    has(false),
                    END,
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(1)), None, Some(json!(2))]
        );
    }

    #[test]
    fn wild_over_object_collapses_duplicate_keys_in_place() {
        // The deleted whole-document pipeline materialized a serde_json::Value
        // first: with preserve_order its map is an IndexMap, so a byte-identical
        // duplicate key collapsed last-wins *in first-occurrence position*. The
        // streaming event flow sees every raw pair, so the Wild arm must
        // reproduce that collapse per object.
        let segments = &[PathSeg::Wild, n("a"), PathSeg::Wild];
        assert_eq!(
            rows(
                segments,
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("a"),
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    number("1"),
                    has(true),
                    name("x"),
                    number_kind(),
                    number("2"),
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(2))]
        );
        assert_eq!(
            rows(
                segments,
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("a"),
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    number("1"),
                    has(true),
                    name("y"),
                    number_kind(),
                    number("9"),
                    has(true),
                    name("x"),
                    number_kind(),
                    number("2"),
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(2)), Some(json!(9))]
        );
        // No duplicate: unchanged document order.
        assert_eq!(
            rows(
                segments,
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("a"),
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    number_kind(),
                    number("1"),
                    has(true),
                    name("y"),
                    number_kind(),
                    number("9"),
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(1)), Some(json!(9))]
        );
    }

    #[test]
    fn wild_over_object_collapse_replaces_the_whole_row_group() {
        // The duplicate's value is itself a container the child wildcard fans
        // out: the collapsed map has x = [2], so the inner wildcard yields just
        // 2 — the first group and its earlier fan-out are replaced wholesale.
        assert_eq!(
            rows(
                &[PathSeg::Wild, n("a"), PathSeg::Wild, PathSeg::Wild],
                vec![
                    object(),
                    BEGIN,
                    has(true),
                    name("a"),
                    object(),
                    BEGIN,
                    has(true),
                    name("x"),
                    array(),
                    BEGIN,
                    has(true),
                    number_kind(),
                    number("1"),
                    has(false),
                    END,
                    has(true),
                    name("x"),
                    array(),
                    BEGIN,
                    has(true),
                    number_kind(),
                    number("2"),
                    has(false),
                    END,
                    has(false),
                    END,
                    has(false),
                    END,
                ],
            ),
            vec![Some(json!(2))]
        );
    }

    #[test]
    fn wild_over_object_collapses_across_many_distinct_keys() {
        // 64 distinct keys followed by a duplicate of the first. The duplicate
        // must replace slot 0 in place, so the order stays k00..k63 while k00
        // carries the last value. This is also the O(1)-lookup case: a linear
        // scan over the member count would be quadratic here.
        let mut script = vec![object(), BEGIN, has(true), name("a"), object(), BEGIN];
        for i in 0..64 {
            script.push(has(true));
            script.push(name(&format!("k{i:02}")));
            script.push(number_kind());
            script.push(number(&i.to_string()));
        }
        script.push(has(true));
        script.push(name("k00"));
        script.push(number_kind());
        script.push(number("99"));
        script.push(has(false));
        script.push(END);
        script.push(has(false));
        script.push(END);
        let mut expected: Vec<Option<Value>> = (0..64).map(|i| Some(json!(i))).collect();
        expected[0] = Some(json!(99));
        assert_eq!(
            rows(&[PathSeg::Wild, n("a"), PathSeg::Wild], script),
            expected
        );
    }

    // ------------------------------------------------------------------
    // Records: delivery, the batch hand-over and the memory tripwire.
    // ------------------------------------------------------------------

    #[test]
    fn verbatim_number_keeps_the_token() {
        assert_eq!(verbatim_number("1e309".to_owned()).to_string(), "1e309");
        assert_eq!(
            verbatim_number("123456789012345678901234567890".to_owned()).to_string(),
            "123456789012345678901234567890"
        );
        // and the path it replaces would NOT:
        assert_eq!(
            serde_json::from_str::<Value>("1e309").unwrap().to_string(),
            "1e+309",
            "serde_json still normalizes; that is why from_string_unchecked is used"
        );
    }

    #[test]
    fn take_record_yields_values_not_offsets() {
        let mut source = Script::new(vec![
            object(),
            BEGIN,
            has(true),
            name("x"),
            number_kind(),
            number("1"),
            has(false),
            END,
        ]);
        let mut matcher = Matcher::new(CompiledPath::from_segments(&[PathSeg::Wild, n("x")]));
        assert_eq!(matcher.next_root(&mut source).unwrap(), Progress::StopRoot);
        assert_eq!(matcher.take_record(), Some(Record::Json(Some(json!(1)))));
        assert_eq!(matcher.take_record(), None);
    }

    #[test]
    fn next_root_stops_after_a_completed_root_value() {
        // One root value completes, so the batch is handed over immediately and
        // pending never grows with the root.
        let mut source = Script::new(vec![
            object(),
            BEGIN,
            has(true),
            name("x"),
            number_kind(),
            number("1"),
            has(false),
            END,
        ]);
        let mut matcher = Matcher::new(CompiledPath::from_segments(&[PathSeg::Wild, n("x")]));
        assert_eq!(matcher.next_root(&mut source).unwrap(), Progress::StopRoot);
        assert_eq!(matcher.pending_len(), 1);
    }

    #[test]
    fn pending_high_stays_bounded_for_a_large_fan_out() {
        // 10_000 tiny matches under one root: each record is taken as it is
        // queued, so the buffered byte total stays at one batch worth.
        let mut script = vec![array(), BEGIN];
        for _ in 0..10_000 {
            script.push(has(true));
            script.push(object());
            script.push(BEGIN);
            script.push(has(true));
            script.push(name("a"));
            script.push(number_kind());
            script.push(number("1"));
            script.push(has(false));
            script.push(END);
        }
        script.push(has(false));
        script.push(END);
        let mut source = Script::new(script);
        let mut matcher =
            Matcher::new(CompiledPath::from_segments(&[PathSeg::Wild, PathSeg::Wild]));
        while let Progress::StopRoot = matcher.next_root(&mut source).unwrap() {
            matcher.take_record();
        }
        assert!(
            matcher.pending_high() <= 1024,
            "pending_high = {}",
            matcher.pending_high()
        );
    }

    // ------------------------------------------------------------------
    // The real reader, end to end.
    // ------------------------------------------------------------------

    #[test]
    fn skipped_subtree_still_validates_string_content() {
        // skip_value replaces IgnoredAny precisely so a subtree the path never
        // reads is validated the way the whole-line parse validates it: a lone
        // surrogate escape and a raw non-UTF-8 byte must both fail here. A
        // scripted Skip has no content to validate, so this one has to drive the
        // real reader.
        for input in [
            &br#"{"b":"\uD800","a":1}"#[..],
            &b"{\"b\":\"\xff\",\"a\":1}"[..],
        ] {
            let (out, error) = real_rows_err(&[PathSeg::Wild, n("a")], input);
            assert!(error.is_some(), "{input:?} must not be accepted");
            assert!(out.is_empty(), "{input:?}");
        }
    }

    #[test]
    fn real_reader_integration() {
        // The seam end to end: the reader decodes the escaped member name and
        // the matcher then folds case on it and fans the array out in document
        // order. This is the half of escaped_key_matches_case_insensitively a
        // script cannot express.
        let (out, error) = real_rows_err(
            &[PathSeg::Wild, n("rules"), PathSeg::Wild],
            br#"{"R\u0075les": [1, 2]}"#,
        );
        assert!(error.is_none(), "unexpected error: {error:?}");
        assert_eq!(out, vec![Some(json!(1)), Some(json!(2))]);
    }
}
