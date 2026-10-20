//! The workspace's one span recorder for tests.
//!
//! Every observability contract test asserts span name, target, level, parent
//! and recorded fields — never log text. This module carries that recorder so
//! no test binary grows a private copy: a `Subscriber` that stores every span
//! it sees, the parent/entered-span fallback `#[instrument]` needs, and the
//! tree queries the assertions are written on.
//!
//! Install it once per test process ([`install`]) or attach a clone as a
//! tracing layer on a per-test registry ([`capture`]). Both spellings share
//! one buffer: the process-global default registry is the point.
//!
//! ```rust
//! use tinio_util::testing::spans;
//!
//! spans::install();
//! let _span = tracing::info_span!("demo", answer = 42).entered();
//! drop(_span);
//! let recorded = spans::spans();
//! assert!(
//!     recorded
//!         .iter()
//!         .any(|span| span.field("answer") == Some("42"))
//! );
//! ```

use std::{
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, ThreadId},
};

use tracing::{
    Event, Level, Metadata, Subscriber,
    callsite::rebuild_interest_cache,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::set_global_default,
};
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry, registry::LookupSpan};

/// One captured span: its identity, owning thread, parent and every field
/// recorded on it, creation fields and later `record` calls alike.
#[derive(Clone, Debug)]
pub struct RecordedSpan {
    /// The recorder's creation sequence (the registry's ids are shard-local).
    pub seq: u64,
    /// The registry's id for the span.
    pub id: u64,
    /// The thread that created it (`#[tokio::test]` runs a current-thread
    /// runtime, so a test can keep its own spans apart from other tests').
    pub thread: ThreadId,
    /// The span name.
    pub name: &'static str,
    /// The span target.
    pub target: &'static str,
    /// The span level.
    pub level: Level,
    /// The span's static metadata (what `Span::current()` answers with).
    pub metadata: &'static Metadata<'static>,
    /// The parent's id, when the span was opened inside another.
    pub parent: Option<u64>,
    /// Recorded fields, in the order they were recorded.
    pub fields: Vec<(String, String)>,
}

impl RecordedSpan {
    /// The value recorded under `name`, rendered as the assertion string.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// One captured event: target, rendered fields and the span it fired in.
#[derive(Clone, Debug)]
pub struct RecordedEvent {
    /// The event target.
    pub target: &'static str,
    /// Recorded fields, in the order they were recorded.
    pub fields: Vec<(String, String)>,
    /// The id of the span the event fired inside, when there was one.
    pub span: Option<u64>,
}

impl RecordedEvent {
    /// The value recorded under `name`, rendered as the assertion string.
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The one buffer every capture clone writes into.
#[derive(Default)]
struct Buffer {
    spans: Mutex<Vec<RecordedSpan>>,
    events: Mutex<Vec<RecordedEvent>>,
    next: AtomicU64,
}

impl Buffer {
    fn spans(&self) -> MutexGuard<'_, Vec<RecordedSpan>> {
        self.spans.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The recorder. Cheap to clone; every clone writes the same buffer.
#[derive(Clone, Default)]
pub struct Capture {
    buffer: Arc<Buffer>,
}

impl Capture {
    /// A recorder with an empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every span recorded so far, in creation order.
    pub fn spans(&self) -> Vec<RecordedSpan> {
        self.buffer.spans().clone()
    }

    /// The spans this thread created, in creation order.
    pub fn spans_on_this_thread(&self) -> Vec<RecordedSpan> {
        let mine = thread::current().id();
        self.spans()
            .into_iter()
            .filter(|span| span.thread == mine)
            .collect()
    }

    /// The creation-sequence watermark a leg snapshots before its own work.
    pub fn watermark(&self) -> u64 {
        self.buffer.next.load(Ordering::SeqCst)
    }

    /// The spans recorded after `watermark` on the calling thread, in creation
    /// order. The registry's ids are shard-local, so the recorder's own
    /// sequence is the watermark.
    pub fn spans_since(&self, watermark: u64) -> Vec<RecordedSpan> {
        let mine = thread::current().id();
        self.spans()
            .into_iter()
            .filter(|span| span.seq > watermark && span.thread == mine)
            .collect()
    }

    /// The spans with this name, in creation order.
    pub fn named(&self, name: &str) -> Vec<RecordedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.name == name)
            .collect()
    }

    /// Every event recorded so far, in creation order.
    pub fn events(&self) -> Vec<RecordedEvent> {
        self.buffer
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Drop every recorded span and event; the next recording starts clean.
    pub fn clear(&self) {
        self.buffer.spans().clear();
        self.buffer
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let id = id.into_u64();
        // The registry decides the parent; an explicit root stays a root. The
        // fallback is the entered span, which is what `#[instrument]` and a
        // hand-entered scope provide.
        let parent = if attrs.is_root() {
            None
        } else {
            attrs
                .parent()
                .map(|parent| parent.clone().into_u64())
                .or_else(|| ctx.current_span().id().map(|id| id.clone().into_u64()))
        };
        let mut fields = FieldValues::default();
        attrs.record(&mut fields);
        let seq = self.buffer.next.fetch_add(1, Ordering::SeqCst) + 1;
        self.buffer.spans().push(RecordedSpan {
            seq,
            id,
            thread: thread::current().id(),
            name: attrs.metadata().name(),
            target: attrs.metadata().target(),
            level: *attrs.metadata().level(),
            metadata: attrs.metadata(),
            parent,
            fields: fields.0,
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = FieldValues::default();
        event.record(&mut fields);
        let span = ctx.event_span(event).map(|span| span.id().into_u64());
        self.buffer
            .events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(RecordedEvent {
                target: event.metadata().target(),
                fields: fields.0,
                span,
            });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _: Context<'_, S>) {
        let mut fields = FieldValues::default();
        values.record(&mut fields);
        let id = id.into_u64();
        if let Some(span) = self.buffer.spans().iter_mut().find(|span| span.id == id) {
            span.fields.extend(fields.0);
        }
    }
}

/// The span's fields, as (name, rendered value) pairs — the assertion shape,
/// not a formatter.
#[derive(Default)]
struct FieldValues(Vec<(String, String)>);

impl Visit for FieldValues {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name().to_string(), value.to_string()));
    }
}

/// The process's capture: every test binary has one.
static CAPTURE: OnceLock<Capture> = OnceLock::new();

/// The process's capture, created on first use.
pub fn capture() -> Capture {
    CAPTURE.get_or_init(Capture::new).clone()
}

/// Install the capture as the process's default subscriber, once, before any
/// span callsite is hit in this binary.
///
/// The callsite interest cache is rebuilt after installing: a callsite another
/// thread registered while no subscriber was installed is cached `never` for
/// the process, and a thread-scoped capture cannot reliably turn it back on.
pub fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let _ = set_global_default(registry().with(capture()));
        rebuild_interest_cache();
    });
}

/// Install the recorder and open the binary's recording window.
///
/// The guard serializes exact assertions against every other recording leg in
/// the same test binary; the buffer is cleared on entry so a failed test cannot
/// bleed into the next one.
pub fn recording() -> (Capture, MutexGuard<'static, ()>) {
    static GATE: Mutex<()> = Mutex::new(());
    let guard = GATE.lock().unwrap_or_else(PoisonError::into_inner);
    install();
    let capture = capture();
    capture.clear();
    (capture, guard)
}

/// Every span recorded by the process's capture, in creation order.
pub fn spans() -> Vec<RecordedSpan> {
    capture().spans()
}

/// The spans the calling thread created, in creation order.
pub fn spans_on_this_thread() -> Vec<RecordedSpan> {
    capture().spans_on_this_thread()
}

/// The one span named `name` in `spans`, panicking when there is none or when more than one was
/// recorded — the per-unit assertion the span legs share.
pub fn one_named<'a>(spans: &'a [RecordedSpan], name: &str) -> &'a RecordedSpan {
    let mut matching = spans.iter().filter(|span| span.name == name);
    let span = matching
        .next()
        .unwrap_or_else(|| panic!("no {name} span in {spans:?}"));
    assert!(matching.next().is_none(), "more than one {name} span");
    span
}

/// The spans whose parent is `id`.
pub fn children(spans: &[RecordedSpan], id: u64) -> Vec<&RecordedSpan> {
    spans
        .iter()
        .filter(|span| span.parent == Some(id))
        .collect()
}

/// Every span below `id`, breadth first.
pub fn descendants(spans: &[RecordedSpan], id: u64) -> Vec<&RecordedSpan> {
    let mut found = Vec::new();
    let mut frontier = vec![id];
    while let Some(id) = frontier.pop() {
        for child in children(spans, id) {
            frontier.push(child.id);
            found.push(child);
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use tracing::{field, subscriber::with_default};

    use super::*;

    #[test]
    fn a_field_recorded_after_creation_lands_on_the_span() {
        let capture = Capture::new();
        with_default(registry().with(capture.clone()), || {
            let span = tracing::info_span!("later", answer = field::Empty);
            span.record("answer", 42u64);
        });
        let spans = capture.spans();
        assert_eq!(spans.len(), 1, "{spans:#?}");
        assert_eq!(spans[0].field("answer"), Some("42"), "{spans:#?}");
    }

    /// A child opened inside an entered span records that span as its parent,
    /// and the two tree queries walk it: `children` is one level,
    /// `descendants` is the whole subtree below the root.
    #[test]
    fn children_record_their_parent_and_the_tree_queries_walk_the_subtree() {
        let capture = Capture::new();
        with_default(registry().with(capture.clone()), || {
            rebuild_interest_cache();
            let parent = tracing::info_span!("parent-probe");
            let child = {
                let _entered = parent.enter();
                tracing::info_span!("child-probe")
            };
            let _child_entered = child.enter();
            let _grandchild = tracing::info_span!("grandchild-probe");
        });

        let spans = capture.spans();
        let by_name = |name: &str| {
            spans
                .iter()
                .find(|span| span.name == name)
                .unwrap_or_else(|| panic!("{name} was recorded: {spans:#?}"))
        };
        let parent = by_name("parent-probe");
        let child = by_name("child-probe");
        let grandchild = by_name("grandchild-probe");
        assert_eq!(parent.parent, None, "the root has no parent");
        assert_eq!(child.parent, Some(parent.id));
        assert_eq!(grandchild.parent, Some(child.id));

        let children = children(&spans, parent.id);
        assert_eq!(children.len(), 1, "{children:#?}");
        assert_eq!(children[0].id, child.id);
        let descendants = descendants(&spans, parent.id);
        assert_eq!(descendants.len(), 2, "{descendants:#?}");
        assert!(
            descendants.iter().any(|span| span.id == grandchild.id),
            "{descendants:#?}"
        );
    }

    /// `named` filters by name and `clear` empties the one shared buffer —
    /// the next recording starts clean.
    #[test]
    fn named_filters_the_buffer_and_clear_empties_it() {
        let capture = Capture::new();
        with_default(registry().with(capture.clone()), || {
            rebuild_interest_cache();
            let _named = tracing::info_span!("named-probe");
            let _other = tracing::info_span!("other-probe");
        });

        assert_eq!(capture.named("named-probe").len(), 1);
        assert_eq!(capture.named("absent-probe").len(), 0);
        assert_eq!(capture.spans().len(), 2);
        capture.clear();
        assert!(capture.spans().is_empty());
    }
}
