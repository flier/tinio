//! Configuration for tinio.
//!
//! Single source of truth for the server's configuration: the TOML config
//! file schema (`version = 1` with `[server]`, `[scanner]`, `[auth]`, `[log]`,
//! `[s3]`, `[storage]`, `[api]`, `[telemetry]` sections), fail-fast
//! validation, source precedence resolution (CLI flags > process env > `.env`
//! > config file), and credential generation.
//!
//! Module layout is populated by the Phase 2 foundational tasks and US2/US3
//! (lib, error, sources, credentials); nothing is public yet.
