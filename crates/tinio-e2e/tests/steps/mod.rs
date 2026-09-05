//! Step definitions and shared per-scenario state for the cucumber suite.
//!
//! Every step module in `tests/steps/` is declared here; the cucumber
//! attribute macros register the steps into the binary's registry. The
//! `#[before]`/`#[after]` hooks ([`configure`]) spawn the in-process
//! server per scenario and tear it down after.

pub mod buckets;
pub mod clients;
pub mod common;
pub mod conditions;
pub mod errors;
pub mod listing;
pub mod multipart;
pub mod objects;
pub mod reserved_paths;

use std::collections::HashMap;

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
    /// @external scenarios only: a spawned `serve` binary + one client
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
    // Backend: an explicit @fs/@mem scenario tag wins; otherwise the
    // TINIO_E2E_BACKEND env override (the CI mem pass); default fs.
    let env_backend = std::env::var("TINIO_E2E_BACKEND").ok();
    let backend = if tagged("mem") {
        Backend::Mem
    } else if tagged("fs") {
        Backend::Fs
    } else if env_backend.as_deref() == Some("mem") {
        Backend::Mem
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
/// server, check the client binaries, and spawn the real `serve` example
/// binary instead — configured from the same `config_from_tags` mapping
/// the in-process hook uses (clients.rs translates `Capabilities` into the
/// `--config` file and `FsKind` into the `TINIO_SCANNER` toggle).
fn before_hook<'a>(
    _feature: &'a cucumber::gherkin::Feature,
    _rule: Option<&'a cucumber::gherkin::Rule>,
    scenario: &'a cucumber::gherkin::Scenario,
    world: &'a mut World,
) -> LocalBoxFuture<'a, ()> {
    Box::pin(async move {
        let (backend, caps, fs_kind) = config_from_tags(&scenario.tags);
        let external = ["interop", "boto3", "mc"]
            .iter()
            .any(|t| has_tag(&scenario.tags, t));
        if external {
            clients::check_presence(&scenario.tags);
            world.ext = Some(clients::External::start(&caps, fs_kind));
            return;
        }
        let server = match (backend, fs_kind) {
            (Backend::Mem, _) => Server::mem(caps).await,
            (Backend::Fs, FsKind::NestedRoot) => Server::fs_nested(caps).await,
            (Backend::Fs, FsKind::ColdListing(interval)) => {
                Server::fs_with_scanner_interval(caps, interval).await
            }
            (Backend::Fs, FsKind::Plain) => Server::fs(caps).await,
        };
        world.client.bind(server.addr());
        world.server = Some(server);
    })
}

/// The `#[after]` hook: drop the server (sends the watch-channel
/// shutdown; the spawned serve binary is killed synchronously). The world
/// is `None` when the scenario never initialized it.
fn after_hook<'a>(
    _feature: &'a cucumber::gherkin::Feature,
    _rule: Option<&'a cucumber::gherkin::Rule>,
    _scenario: &'a cucumber::gherkin::Scenario,
    _ev: &'a cucumber::event::ScenarioFinished,
    world: Option<&'a mut World>,
) -> LocalBoxFuture<'a, ()> {
    Box::pin(async move {
        if let Some(world) = world {
            world.ext.take();
            world.ext_second.take();
            world.server.take();
        }
    })
}

/// The configured cucumber runner type: the default runner with the
/// lifecycle hooks as plain fn pointers (the hook closure types are
/// unnameable; the fn-pointer aliases are not).
type ConfiguredRunner<I> = cucumber::Cucumber<
    World,
    Basic,
    I,
    RunnerBasic<World, WhichScenarioFn, BeforeHookFn<World>, AfterHookFn<World>>,
    Summarize<Normalize<World, WriterBasic>>,
    CliEmpty,
>;

/// The cucumber runner with the lifecycle hooks attached.
pub fn configure<I: AsRef<std::path::Path>>() -> ConfiguredRunner<I> {
    World::cucumber()
        .before::<BeforeHookFn<World>>(before_hook)
        .after::<AfterHookFn<World>>(after_hook)
}
