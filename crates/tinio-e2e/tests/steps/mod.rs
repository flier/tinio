//! Step definitions and shared per-scenario state for the cucumber suite.
//!
//! Every step module in `tests/steps/` is declared here; the cucumber
//! attribute macros register the steps into the binary's registry. The
//! `#[before]`/`#[after]` hooks ([`configure`]) spawn the in-process
//! server per scenario and tear it down after.

use std::{env, path::Path};

use cucumber::{
    Cucumber,
    event::ScenarioFinished,
    gherkin,
    gherkin::{Feature, Scenario},
};
pub mod acl;
pub mod buckets;
pub mod cli;
pub mod clients;
pub mod common;
pub mod conditions;
pub mod doctor;
pub mod errors;
pub mod lifecycle;
pub mod listing;
pub mod multipart;
pub mod objects;
pub mod reserved_paths;
pub mod select;
pub mod tag_filter;
pub mod versioning;

use std::collections::HashMap;

pub use cli::Cli;
pub use clients::{External, SpawnedServer};
pub use common::{Backend, Client, FsKind, LastResponse, Server};
use cucumber::{
    World as _,
    cli::Empty as CliEmpty,
    parser::Basic,
    runner::{
        Basic as RunnerBasic,
        basic::{AfterHookFn, BeforeHookFn, WhichScenarioFn},
    },
    writer::{Basic as WriterBasic, Normalize, Summarize},
};
use futures::future::LocalBoxFuture;
pub use listing::ListingState;
pub use multipart::MultipartState;
use tokio::time::Duration;

use crate::_server::Capabilities;

/// Shared per-scenario state; cucumber builds one via `Default` per
/// scenario and the `#[before]`/`#[after]` hooks manage the server.
#[derive(Debug, Default, cucumber::World)]
pub struct World {
    pub server: Option<Server>,
    pub client: Client,
    pub last: LastResponse,
    /// The scenario's multipart-upload state (multipart.rs) — one rebuild
    /// per started upload, so no upload-scoped field can leak across
    /// uploads.
    pub mp: MultipartState,
    /// All header values captured by `the response header … is stored`,
    /// by name (errors.rs: the `{name}` and `{etag}` substitutions in
    /// later steps — the stored names are matched case-insensitively).
    pub stored_headers: HashMap<String, String>,
    /// Bytes of the scenario's last uploaded body (objects.rs: the
    /// body-equality assertion compares the served body against these;
    /// the ETag assertions hash the served body, not this buffer).
    pub last_upload: Vec<u8>,
    /// The last ListObjectsV2 request's parameters (listing.rs: the
    /// pagination step resumes from these).
    pub last_listing: ListingState,
    /// @external scenarios only: a spawned `tinio server` + one client
    /// session (clients.rs); None for the in-process scenarios.
    pub ext: Option<External>,
    /// The ephemeral `--port 0` second server (journey feature).
    pub ext_second: Option<SpawnedServer>,
    /// The last external client run's stdout (clients.rs Then steps).
    pub ext_output: String,
    /// The last external client run's stderr (the error-path legs).
    pub ext_error: String,
    /// The captured (trimmed) client output, substituted for `{captured}`
    /// in later commands (the pagination continuation token).
    pub ext_captured: String,
    /// The `@cli` scenarios' harness (cli.rs): the scratch root, the spawned
    /// binary and the last run's captured streams. None for every other
    /// scenario — those spawn the binary themselves and use neither the
    /// in-process server nor the wire client, which is why the `#[before]`
    /// hook leaves this world alone entirely.
    pub cli: Option<Cli>,
    /// The served root a step restricted to owner read and traverse, with the
    /// mode to give back on drop (common.rs: the `@unix` read-only scenario's
    /// `chmod 0500` fixture). Unix only — the plant is a permission bit —
    /// and `None` for every scenario that plants none.
    #[cfg(unix)]
    pub restricted: Option<common::RestrictedRoot>,
}

/// Whether `tags` contains exactly `tag` — the scenario-classification
/// test shared by the in-process hook, the @external spawn, and the
/// client-presence checks (one semantics, one place).
pub fn has_tag(tags: &[String], tag: &str) -> bool {
    tags.iter().any(|x| x == tag)
}

/// Tag → server configuration, shared by the in-process hook and the
/// @external spawn (Task 7). One mapping, one place: a scenario tag means
/// the same configuration whichever way the server runs.
pub fn config_from_tags(tags: &[String]) -> (Backend, Capabilities, FsKind) {
    let tagged = |t: &str| has_tag(tags, t);
    // Backend: an explicit @pg/@mem/@fs scenario tag wins; otherwise the
    // TINIO_E2E_BACKEND env override (the CI mem and pg passes); default fs.
    let env_backend = env::var("TINIO_E2E_BACKEND").ok();
    let backend = if tagged("pg") {
        Backend::Pg
    } else if tagged("mem") {
        Backend::Mem
    } else if tagged("fs") {
        Backend::Fs
    } else if env_backend.as_deref() == Some("mem") {
        Backend::Mem
    } else if env_backend.as_deref() == Some("pg") {
        Backend::Pg
    } else {
        Backend::Fs
    };

    // Capability toggles (specs/001-s3-local-server/contracts/s3-surface.md
    // §Object tagging — the tagging capability toggle of FR-030; grilling Q4).
    let mut caps = Capabilities::default();
    if tagged("checksum-on") {
        caps.checksum = true;
    }
    if tagged("tagging-off") {
        caps.tagging = false;
    }
    if tagged("cors-off") {
        caps.cors = false;
    }
    if tagged("minimal-caps") {
        caps.multipart = false;
        caps.copy_object = false;
        caps.list_objects_v1 = false;
        caps.list_objects_v2 = false;
        caps.delete_objects = false;
        caps.tagging = false;
        caps.cors = false;
        // The seventh capability (Task 15, Task 10 carry): the ACL
        // subsystem's runtime toggle — off answers NotImplemented on the
        // four ACL ops (the legacy no-identity plane, like the other
        // off-mode scenarios).
        caps.acl = false;
        // The lifecycle plan's capability-off leg (Task 6): off answers
        // NotImplemented on the three lifecycle operations through the
        // gate, not through s3s's trait default.
        caps.lifecycle = false;
    }
    if tagged("acl-off") {
        // The @acl-off scenarios' own toggle: same semantics as the
        // @minimal-caps entry above, spelled out for the acl.feature
        // headings (the 12 legacy feature files use @minimal-caps).
        caps.acl = false;
    }
    if tagged("versioning-off") {
        // The @versioning-off scenario (spec 2026-10-03-s3-versioning):
        // the three version operations are refused with NotImplemented
        // and a version-addressed request is refused outright — never
        // answered from the current version (the fail-open guard).
        caps.versioning = false;
    }
    if tagged("max-buckets-3") {
        // The ListBuckets-pagination scenarios need a page cap below the
        // bucket count (the boto3 script asserts ≥ 2 pages; the default
        // 10,000 cap returns everything in one page).
        caps.max_buckets = 3;
    }

    let fs_kind = if tagged("nested-root") {
        FsKind::NestedRoot
    } else if tagged("cold-listing") {
        FsKind::ColdListing(Duration::from_millis(100))
    } else {
        FsKind::Plain
    };
    (backend, caps, fs_kind)
}

/// The `#[before]` hook: spawn the scenario's server and bind the client.
///
/// The `@external` branch (`@interop`/`@boto3`/`@mc`): skip the in-process
/// server, check the client binaries, and spawn the real `tinio server`
/// binary instead — configured from the same `config_from_tags` mapping
/// the in-process hook uses (clients.rs translates `Capabilities` into the
/// `--config` file and `FsKind` into the `TINIO_SCANNER` toggle). An
/// `@external` scenario tagged `@confinement` takes this branch too, with
/// the confinement added to the spawn ([`clients::Confinement`]) — its
/// `@read-only` tag is read there and not by the branch below, which it
/// therefore never reaches.
///
/// The `@read-only` branch (FR-023) spawns a read-only fs plane with its
/// state in a scratch home ([`Server::read_only`]) — every mutating
/// operation answers `AccessDenied`.
///
/// The `@cli` scenarios (cli.rs) take none of the three: they spawn the real
/// binary and observe it from outside, so the hook spawns nothing and binds
/// nothing — the world's harness is the scenario's own `Given a storage root`.
fn before_hook<'a>(
    _feature: &'a Feature,
    _rule: Option<&'a gherkin::Rule>,
    scenario: &'a Scenario,
    world: &'a mut World,
) -> LocalBoxFuture<'a, ()> {
    Box::pin(async move {
        if has_tag(&scenario.tags, "cli") {
            return;
        }
        let (backend, caps, fs_kind) = config_from_tags(&scenario.tags);
        let external = ["interop", "boto3", "mc"]
            .iter()
            .any(|t| has_tag(&scenario.tags, t));
        if external {
            clients::check_presence(&scenario.tags);
            // The confinement the scenario asks for is a tag of its own (`@confinement`, and
            // `@read-only` for FR-023's mode): the spawn happens here, before any step runs, so a
            // step could not request one — see `clients::Confinement`.
            world.ext = Some(clients::External::start(
                &caps,
                fs_kind,
                clients::Confinement::from_tags(&scenario.tags),
            ));
            return;
        }
        let acl = has_tag(&scenario.tags, "acl");
        let server = if !acl && has_tag(&scenario.tags, "read-only") {
            // The read-only plane (FR-023): every mutating operation
            // answers AccessDenied. Checked ahead of the backend
            // dispatch, which would otherwise need a read-only arm per
            // `FsKind`; an `@acl` scenario outranks it, as the branch
            // below is the wider plane.
            Server::read_only(caps).await
        } else {
            // The two tag axes are independent — the backend says which
            // storage to serve, `@acl` whether to serve it on the
            // identity-wired plane (Task 15, grilling Q2: the
            // config-driven Identity from the fixture
            // `[auth]`/`[owner]`/`[[users]]`, through
            // `DataPlane::new_with_acl`). `@acl-off` is NOT the acl arm
            // (the caps gate off runs the legacy no-identity plane, like
            // the other @minimal-caps off-mode scenarios).
            Server::from_tags(backend, fs_kind, caps, acl).await
        };
        world.client.bind(server.addr());
        world.server = Some(server);
    })
}

/// The `#[after]` hook: drop the server (sends the watch-channel
/// shutdown; the spawned serve binary is killed synchronously), and end what
/// the `@cli` scenarios spawned — killed and reaped, with a daemon the
/// scenario left behind asked to stop, because no handle here reaches it
/// (cli.rs). The world is `None` when the scenario never initialized it.
fn after_hook<'a>(
    _feature: &'a Feature,
    _rule: Option<&'a gherkin::Rule>,
    _scenario: &'a Scenario,
    _ev: &'a ScenarioFinished,
    world: Option<&'a mut World>,
) -> LocalBoxFuture<'a, ()> {
    Box::pin(async move {
        if let Some(world) = world {
            world.ext.take();
            world.ext_second.take();
            if let Some(mut cli) = world.cli.take() {
                cli.teardown();
                // A `@pg` scenario's scratch database goes last: PG12's ownership lock is
                // process-lifetime, so it may only be dropped once `teardown` above has ended the
                // servers that hold it — and the shared `tinio` fixture belongs to every other leg.
                #[cfg(feature = "pg")]
                cli.drop_pg_scratch().await;
            }
            // Before the server, whose tempdir may be the restricted root: a directory at
            // mode 0500 refuses `remove_dir_all`, so the mode has to be back first
            // (common.rs's `RestrictedRoot` owns the restore, and its `Drop` is this take).
            #[cfg(unix)]
            world.restricted.take();
            world.server.take();
        }
    })
}

/// The configured cucumber runner type: the default runner with the
/// lifecycle hooks as plain fn pointers (the hook closure types are
/// unnameable; the fn-pointer aliases are not).
type ConfiguredRunner<I> = Cucumber<
    World,
    Basic,
    I,
    RunnerBasic<World, WhichScenarioFn, BeforeHookFn<World>, AfterHookFn<World>>,
    Summarize<Normalize<World, WriterBasic>>,
    CliEmpty,
>;

/// The cucumber runner with the lifecycle hooks attached.
pub fn configure<I: AsRef<Path>>() -> ConfiguredRunner<I> {
    World::cucumber()
        .before::<BeforeHookFn<World>>(before_hook)
        .after::<AfterHookFn<World>>(after_hook)
}
