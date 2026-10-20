//! The pg pool's connection gauge, declared in the crate that observes it.
//!
//! The family is scrape-time: a [`Collector`] on the default registry reads
//! the live pools when the management plane gathers, so no operation pays for a
//! sample and a scrape never waits on a statement. `state` is the closed set
//! `max` / `idle` / `in_use`, summed over the pools this process holds open,
//! which is the process's whole connection use.
//!
//! `specs/001-s3-local-server/data-model.md` §Metrics owns the name and labels.

#![expect(
    clippy::expect_used,
    reason = "argued: every error these registrations can answer is about the family's own name \
              — the process-global default registry refuses a name it already holds or holds with \
              a different label set or help. The name has one registration site in the repo (this \
              module's `LazyLock`, whose closure runs once per process), against the family list \
              `specs/001-s3-local-server/data-model.md`'s `## Metrics (read-only views, not \
              persisted)` owns; a collision is a source-level mistake, not a runtime condition, \
              and `LazyLock::new` has no `Result` to return it through. The module's tests read \
              the three states back out of the gathered exposition, so a registration that did \
              not land fails a test."
)]

use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};

use prometheus::{
    IntGaugeVec, Opts,
    core::{Collector, Desc},
    proto::MetricFamily,
    register,
};
use sqlx::PgPool;

/// The family's name (`data-model.md` §Metrics).
const NAME: &str = "tinio_storage_pool_connections";

/// The gauge's help line.
const HELP: &str = "PostgreSQL pool connections by state";

/// The `state` label's closed domain.
const STATES: [&str; 3] = ["max", "idle", "in_use"];

/// The live pools a gather reads, weakly: a dropped pool is pruned on the next
/// attach.
static POOLS: LazyLock<Mutex<Vec<Weak<PoolHandle>>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// The family is registered on the first pool and never again; a build with no
/// pg pool exposes none of it.
static REGISTERED: LazyLock<()> = LazyLock::new(|| {
    register(Box::new(PoolCollector::new())).expect("register tinio_storage_pool_connections");
});

/// One live pool the collector reads. The [`Pool`](crate::pool::Pool) holds the
/// strong handle, so dropping the pool takes it out of the family.
#[derive(Debug)]
pub(crate) struct PoolHandle {
    /// The driver's pool: `size` and `num_idle` are the observation.
    pool: PgPool,
    /// The configured maximum.
    max: u32,
}

/// Attach `pool` to the family and answer the handle its owner keeps.
pub(crate) fn attach(pool: PgPool, max_connections: u32) -> Arc<PoolHandle> {
    let handle = Arc::new(PoolHandle {
        pool,
        max: max_connections.max(1),
    });
    LazyLock::force(&REGISTERED);
    let mut pools = POOLS.lock().unwrap_or_else(PoisonError::into_inner);
    pools.retain(|pool| pool.strong_count() > 0);
    pools.push(Arc::downgrade(&handle));
    handle
}

/// The registry's view of the family: the three states filled from the live
/// pools on every gather.
struct PoolCollector {
    connections: IntGaugeVec,
}

impl PoolCollector {
    fn new() -> Self {
        Self {
            connections: IntGaugeVec::new(Opts::new(NAME, HELP), &["state"])
                .expect("build tinio_storage_pool_connections"),
        }
    }

    /// Fill the samples: `max` is the configured capacity, `idle` the
    /// connections waiting, `in_use` the checked-out ones (`size` counts the
    /// established connections, so `size - idle` is what a statement holds).
    fn sample(&self) {
        let pools = POOLS.lock().unwrap_or_else(PoisonError::into_inner);
        let mut max = 0_u32;
        let mut idle = 0_u32;
        let mut in_use = 0_u32;
        for handle in pools.iter().filter_map(Weak::upgrade) {
            let idle_now = u32::try_from(handle.pool.num_idle()).unwrap_or(u32::MAX);
            max = max.saturating_add(handle.max);
            idle = idle.saturating_add(idle_now);
            in_use = in_use.saturating_add(handle.pool.size().saturating_sub(idle_now));
        }
        set_state_samples(&self.connections, max, idle, in_use);
    }
}

/// Write one sample per state. A test builds its own vector and calls this, so
/// the values are asserted exactly rather than through the process's registry.
fn set_state_samples(connections: &IntGaugeVec, max: u32, idle: u32, in_use: u32) {
    for (state, value) in STATES.into_iter().zip([max, idle, in_use]) {
        connections
            .with_label_values(&[state])
            .set(i64::from(value));
    }
}

impl Collector for PoolCollector {
    fn desc(&self) -> Vec<&Desc> {
        Collector::desc(&self.connections)
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.sample();
        Collector::collect(&self.connections)
    }
}

#[cfg(test)]
mod tests {
    use prometheus::{IntGaugeVec, Opts};
    /// What `/metrics` renders: the default registry, gathered and encoded.
    use tinio_util::testing::metrics::{assert_registered, exposition};

    use super::{NAME, STATES, set_state_samples};
    use crate::pool::Pool;

    #[test]
    fn the_three_states_report_their_values() {
        let connections = IntGaugeVec::new(
            Opts::new("tinio_storage_pool_connections_probe", "probe"),
            &["state"],
        )
        .expect("build the probe family");
        set_state_samples(&connections, 8, 3, 5);
        assert_eq!(connections.with_label_values(&["max"]).get(), 8);
        assert_eq!(connections.with_label_values(&["idle"]).get(), 3);
        assert_eq!(connections.with_label_values(&["in_use"]).get(), 5);
    }

    #[tokio::test]
    async fn a_live_pool_registers_the_family_with_its_three_states() {
        let pool = Pool::open(
            "postgres://user:password@db.example:5432/tinio?sslmode=disable",
            4,
        )
        .expect("the dsn parses");
        // A gather is the scrape: the collector reads the live handle.
        assert_registered(&["tinio_storage_pool_connections"]);
        let text = exposition();
        for state in STATES {
            assert!(
                text.contains(&format!("{NAME}{{state=\"{state}\"}}")),
                "the pool gauge declares the {state} state: {text}"
            );
        }
        assert!(
            text.contains("tinio_storage_pool_connections{state=\"max\"} "),
            "the max state carries a sample: {text}"
        );
        drop(pool);
    }
}
