//! The `@tier` scenarios' own steps (T25): the moves, the layer's passes and
//! the root facts no other feature drives.
//!
//! Everything else is the existing corpus — the CRUD, range, tagging, ACL,
//! listing, attributes and lifecycle steps run unchanged over the tier
//! backend, which is the point of a decorator: the wire surface is the
//! storage contract's, not the layer's. What a scenario cannot express over
//! the wire is here — "this object is cold now" and "the pass has run" — so
//! an assertion never races a background cadence. The metrics legs read the
//! same process-global registry the management `/metrics` renders (the
//! in-process plane serves no management route), and the idle leg polls the
//! placement only until the product's own background pass moves it.

use std::{
    array::from_fn,
    time::{Duration, SystemTime},
};

use cucumber::{gherkin::Step, given, then, when};
use tinio_core::{bucket, object};
use tinio_tier::{MoveOutcome, State, Tier as IndexTier};
use tinio_util::testing::metrics::{counter, exposition};

use super::{
    Tier, World,
    common::{Server, eventually_within},
};

/// Split a step path ("bucket/key") into the layer's two newtypes.
fn split(path: &str) -> (bucket::Name, object::Key) {
    let (bucket, key) = path
        .split_once('/')
        .unwrap_or_else(|| panic!("`{path}` is not `bucket/key`"));
    (
        bucket::name(bucket).unwrap_or_else(|err| panic!("`{bucket}`: {err}")),
        object::key(key).unwrap_or_else(|err| panic!("`{key}`: {err}")),
    )
}

/// The scenario's tier handle — every step in this module runs over a tier server.
fn tier(world: &World) -> &Tier {
    world
        .server
        .as_ref()
        .expect("a @tier scenario's server")
        .tier_handle()
}

/// Demote one object into the cold plane now (T13): the deterministic driver for the cold-read,
/// heat, copy, range and large-object scenarios.
#[given(expr = "I demote object {string}")]
#[when(expr = "I demote object {string}")]
async fn demote(world: &mut World, path: String) {
    let (bucket, key) = split(&path);
    let summary = tier(world)
        .storage()
        .demote(&bucket, &key, SystemTime::now())
        .await
        .unwrap_or_else(|err| panic!("demote `{path}`: {err}"));
    assert!(
        matches!(summary, MoveOutcome::Moved { .. }),
        "the demote of `{path}` moved nothing"
    );
}

/// Run one capacity-and-heat pass now: the promotion queue the reads filled is drained inside it.
#[given("the tier mover runs a pass")]
#[when("the tier mover runs a pass")]
async fn mover_pass(world: &mut World) {
    let summary = tier(world)
        .mover()
        .pass()
        .await
        .expect("the tier mover's pass runs");
    assert_eq!(
        summary.failed, 0,
        "the mover pass failed a move: {summary:?}"
    );
}

/// Run one collection pass now: a demotion's leftover warm file is removed after the (zero) grace
/// this harness configures.
#[given("the tier collector runs")]
#[when("the tier collector runs")]
async fn collector_pass(world: &mut World) {
    tier(world)
        .storage()
        .gc(SystemTime::now())
        .await
        .expect("the tier collection pass runs");
}

/// The batch buffer still holds the key: the write was acknowledged without reaching the
/// filesystem, which is the half a 200 over the wire cannot show.
#[then(expr = "the tier buffer holds {string}")]
async fn buffer_holds(world: &mut World, path: String) {
    let (bucket, key) = split(&path);
    assert!(
        tier(world).storage().buffer().contains(&bucket, &key),
        "the write of `{path}` was not buffered"
    );
}

/// Drain the batch write buffer now — the public `shutdown` entry point is the layer's flush
/// (T8), so the scenario observes the file only after this step, never on a timer.
#[given("the tier buffer flushes")]
#[when("the tier buffer flushes")]
async fn flush_buffer(world: &mut World) {
    tier(world)
        .storage()
        .shutdown()
        .await
        .expect("the tier buffer drains");
}

/// Run one lifecycle pass now: the transition scenario's driver, so the demotion a rule asks for
/// is asserted instead of waited for.
#[given("the lifecycle pass runs")]
#[when("the lifecycle pass runs")]
async fn lifecycle_pass(world: &mut World) {
    let summary = world
        .server
        .as_ref()
        .expect("a @tier scenario's server")
        .lifecycle_pass()
        .await;
    assert_eq!(summary.errors, 0, "the lifecycle pass failed: {summary:?}");
}

/// Reopen the scenario's tier root: the index is exclusive, so the planes must be constructed
/// over the same tree and the same cold rows once every task of the first server has ended.
#[given("the tier server is restarted")]
#[when("the tier server is restarted")]
async fn restart(world: &mut World) {
    reopen(world, false).await;
}

/// The same reopen, read-only (T27): the reads are served and every write is refused.
#[given("the tier server is restarted read-only")]
#[when("the tier server is restarted read-only")]
async fn restart_read_only(world: &mut World) {
    reopen(world, true).await;
}

/// The one reopen body the two steps share.
///
/// The first server is stopped and **awaited** before the guard is handed on: the tier tasks
/// hold the index and the cold plane, and the reopened construction takes the index's exclusive
/// file lock. The shared database's permit is **moved, never released** — releasing it would let
/// the next scenario acquire it between the two servers, and its reset would truncate this
/// scenario's cold rows out from under the reopened server.
async fn reopen(world: &mut World, read_only: bool) {
    let mut old = world.server.take().expect("a running tier server");
    let root = old
        .take_root()
        .expect("a tier server serves a temp root it owns");
    let path = root.path().to_path_buf();
    let permit = old
        .take_permit()
        .expect("the tier scenario's shared-database permit");
    old.stop().await;
    drop(old);
    world.tier_root = Some(root);
    let server = Server::tier_on(&path, read_only, permit).await;
    world.client.bind(server.addr());
    world.server = Some(server);
}

/// The served root holds `path` as a file: the batch flush's destination, and the warm half of a
/// demotion's two planes.
#[then(expr = "the served root holds the file {string}")]
async fn root_holds_file(world: &mut World, path: String) {
    let root = world
        .server
        .as_ref()
        .and_then(Server::root)
        .expect("a tier scenario's served root");
    assert!(
        root.join(&path).is_file(),
        "the served root holds no file `{path}`"
    );
}

/// The served root holds no file at `path`: a cold object's representation is absent (T4), and
/// the collection pass is what removes a demotion's leftover.
#[then(expr = "the served root does not hold the file {string}")]
async fn root_omits_file(world: &mut World, path: String) {
    let root = world
        .server
        .as_ref()
        .and_then(Server::root)
        .expect("a tier scenario's served root");
    assert!(
        !root.join(&path).exists(),
        "the served root still holds `{path}`"
    );
}

/// The tier series the metrics scenario samples and asserts: one entry per `(family, tier)`
/// pair the scenario's table names. A wrong implementation that never books a plane, books it
/// twice, or books the wrong plane's bytes moves one of these series away from its expected
/// delta, so the table is the assertion.
const TIER_SERIES: &[(&str, &str)] = &[
    ("tinio_tier_hits_total", "warm"),
    ("tinio_tier_misses_total", "warm"),
    ("tinio_tier_bytes_total", "warm"),
    ("tinio_tier_hits_total", "cold"),
    ("tinio_tier_misses_total", "cold"),
    ("tinio_tier_bytes_total", "cold"),
];

/// The counters sampled before the scenario's reads.
///
/// **A baseline, never an absolute total**: the registry is process-global and `--retry` leaves
/// an earlier attempt's samples in it, so only the difference between this sample and the one
/// the assertion takes is safe.
#[derive(Debug, Clone, Copy)]
pub struct MetricsSample {
    values: [f64; TIER_SERIES.len()],
}

impl MetricsSample {
    /// Read every series in [`TIER_SERIES`].
    fn read() -> Self {
        Self {
            values: from_fn(|index| {
                let (family, plane) = TIER_SERIES[index];
                counter(family, &[("tier", plane)])
            }),
        }
    }

    /// One sampled series, or `None` when the table names a pair the sample does not cover.
    fn get(&self, family: &str, plane: &str) -> Option<f64> {
        TIER_SERIES
            .iter()
            .position(|(known_family, known_plane)| {
                *known_family == family && *known_plane == plane
            })
            .map(|index| self.values[index])
    }
}

/// Take the baseline the reads' deltas are measured from — after the fixture is written and
/// demoted, so the moves' own counters cannot be attributed to the reads.
#[when("I sample the tier metric counters")]
async fn sample_metric_counters(world: &mut World) {
    world.tier_metrics = Some(MetricsSample::read());
}

/// Scrape the tier registry — [`exposition`] is the shared testing helper over the same default
/// registry the management `/metrics` endpoint gathers, and the in-process plane serves no
/// management route of its own.
#[when("I scrape the tier metrics")]
async fn scrape_metrics(world: &mut World) {
    world.tier_metrics_text = Some(exposition());
}

/// Every table row's series exists in the scrape and moved by exactly `delta` since the sample.
///
/// The table is the whole expected movement: a warm GET books one warm hit and its bytes, a cold
/// GET books one warm miss, one cold hit and the cold bytes, and no other family moves for these
/// reads. An exact delta (not `>=`) catches an implementation that double-books or books the
/// other plane.
#[then("the tier metrics report the served planes")]
async fn metrics_report_served_planes(world: &mut World, step: &Step) {
    let before = world
        .tier_metrics
        .as_ref()
        .expect("a preceding `I sample the tier metric counters` step");
    let text = world
        .tier_metrics_text
        .as_deref()
        .expect("a preceding `I scrape the tier metrics` step");
    let table = step.table().expect("the metrics step carries a table");
    for row in table.rows.iter().skip(1) {
        assert_eq!(
            row.len(),
            3,
            "the metrics row is `family | tier | delta`: {row:?}"
        );
        let (family, plane, delta) = (row[0].as_str(), row[1].as_str(), row[2].as_str());
        let expected: f64 = delta
            .parse()
            .unwrap_or_else(|_| panic!("`{delta}` is not a numeric delta"));
        let baseline = before
            .get(family, plane)
            .unwrap_or_else(|| panic!("the sample does not cover {family}{{tier=\"{plane}\"}}"));
        let after = counter(family, &[("tier", plane)]);
        assert_eq!(
            after - baseline,
            expected,
            "{family}{{tier=\"{plane}\"}} must move by {expected}: {baseline} -> {after}"
        );
        assert!(
            text.contains(&format!("{family}{{tier=\"{plane}\"}}")),
            "the scraped exposition carries no {family}{{tier=\"{plane}\"}} series:\n{text}"
        );
    }
}

/// The run's read-only series is present with the access mode the server was built with: the
/// harness's testing constructor books a writable run (`false`), so the series exists and reads
/// zero.
#[then(expr = "the metrics exposition reports read-only tier {int}")]
async fn exposition_read_only(world: &mut World, expected: u64) {
    let text = world
        .tier_metrics_text
        .as_deref()
        .expect("a preceding `I scrape the tier metrics` step");
    let line = format!("tinio_storage_read_only{{backend=\"tier\"}} {expected}");
    assert!(
        text.contains(&line),
        "the scraped exposition carries no `{line}` line:\n{text}"
    );
}

/// Poll the placement until the product's own background pass has demoted `path` — the idle
/// scenario's deterministic wait.
///
/// **The cadence is the thing under test**, so no step drives a pass here: the scenario's
/// `@tier-idle` configuration carries a three-second `demote_idle` and a 100 ms `scan_interval`,
/// and a bounded 15 s poll absorbs a loaded host while still failing a mover that never demotes.
/// The poll reads the index this process already owns; the wire assertions that follow are the
/// ones that pin the observable answer.
#[when(expr = "the tier idle pass demotes {string}")]
async fn idle_pass_demotes(world: &mut World, path: String) {
    let (bucket, key) = split(&path);
    let storage = tier(world).storage();
    let demoted = eventually_within(
        || {
            storage
                .index()
                .placement(&bucket, &key)
                .ok()
                .flatten()
                .is_some_and(|placement| {
                    placement.tier == IndexTier::Pg && placement.state == State::Live
                })
        },
        Duration::from_secs(15),
    )
    .await;
    assert!(
        demoted,
        "the background pass did not demote `{path}` inside the poll's budget"
    );
}
