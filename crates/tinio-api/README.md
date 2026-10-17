# `tinio-api` — the management plane

`tinio-api` is the management plane: the axum router that serves `/status`, `/stop`, `/metrics` and `/openapi.json`, the control-channel transports it serves them over, the `state` file that lets a client find that channel, and the `status`/`stop` client the CLI drives against it. It implements the management-API contract (`contracts/management-api.md`) — the endpoint set, the token rule that follows the **bind address**, the mutual exclusivity of the transports, and single-instance enforcement, which is the bind itself rather than a check beside it. It owns no data-plane surface: the S3 protocol, routing, SigV4 and the error mapping are `tinio-server`'s, and the crate deliberately does not depend on that crate. It owns no configuration schema either — the transport selection, the state directory and the served root all arrive as its caller's `Options`, resolved by `tinio-cli`. The whole crate sits behind the facade's `api` feature; its own `openapi` and `tls` features are both on by default.

## The state file and the single-instance claim

`<state_dir>/state` is the plane's record of a running server; the fields it carries, and what each one means, are `../../CONTEXT.md`'s "State file" term. It is written atomically, and private on unix — mode `0600`, because the token that guards `/status` and `/stop` is inside it — while on Windows the create installs no descriptor (`specs/001-s3-local-server/data-model.md`'s file-permissions row holds that clause). That token is redacted from any rendering of the value.

The file's presence means "a server claims this root". The **effective** lock is the control channel, so a second start for the same root fails at the bind, before it can touch anything else: the unix socket is unlinked only after a probe proves nobody is listening behind it, and the Windows pipe is created with `FILE_FLAG_FIRST_PIPE_INSTANCE`. What a client dials comes from `state.control_name` and is never re-derived, and both platforms derive their default channel name from the **canonical** served root — so two roots sharing one state directory, which `TINIO_STATE_DIR` exists to allow, cannot collide.

## The transports

Exactly one of three is enabled. None is a startup error, two are a usage error.

| Form | Platform | Notes |
|---|---|---|
| Local | unix socket on Linux and macOS; named pipe on Windows | always the strict local token rule; the socket path is `[api.unix] path` or a name derived from the served root |
| TCP HTTP | every platform | the rule follows the **bound** address: a loopback bind keeps only `/metrics` open, a reachable one guards every endpoint |
| TCP HTTPS | every platform, `tls` feature | the same rule, no client authentication — the token is the only scheme |

The bind happens **before** the process's runtime exists — the listeners are `std` ones, which is what lets this be the single-instance claim — and serving converts them inside the runtime, stopping on the plane's one shutdown channel. When a launcher confined the process, that launcher bound the channel and the plane serves what it inherited (`Plane::adopt`) instead of binding a second one; the enablement and refusal side of confinement is `tinio-cli`'s.

## The endpoints

| Endpoint | What it answers |
|---|---|
| `GET /status` | the running server's state, as the client prints it |
| `POST /stop` | `202`, and flips the shutdown channel that a termination signal and the data plane's drain also end at |
| `GET /metrics` | the registry gather, fed by a gauge source the caller injects and a background reader that keeps the storage scan off the serving path |
| `GET /openapi.json` | the OpenAPI document (`openapi` feature); the spec JSON only, no UI |

The token is `X-Tinio-Token`, with `Authorization: Bearer` as an alias so a standard scraper can send it. Which endpoints require it is the bind-address rule applied as an exception list on the open side, so an unrouted path leaks nothing and answers the contract's plain `404`.

## Layout

| File | What it is |
|---|---|
| `src/bind.rs` | binding the control channel — also the single-instance lock; runtime-free listeners |
| `src/transport.rs` | serving the router over the bound transports, the three-choose-one rule, TLS termination |
| `src/router.rs` | the routes, the token rule and the open-path list |
| `src/state.rs` | the `state` file, its token, and the derived channel names |
| `src/client.rs` | the `status`/`stop` client over all three local forms |
| `src/metrics.rs` | the gauge-source contract, the registry gather, and its reader task |
| `src/openapi.rs` | the OpenAPI document (`openapi` feature) |
| `src/pipe.rs` | the crate's one raw-Windows module: named-pipe creation and adoption; the sentence a failed `*W` call is reported with is `tinio-util`'s `win32::error_message` |
| `src/error.rs`, `src/lib.rs` | the HTTP-mapped error, and the crate's re-exports |
| `src/test_support.rs` | test-only fixtures (the TLS pair, the metrics double), `#[cfg(test)]` |

## Gates

```console
cargo test -p tinio-api
```

The default graph has `tls` and `openapi` on, and the named-pipe pool, its sequential-connection leg and the second-instance refusal are Windows legs that run only there; what the pipe-pool legs require, and how such a leg is driven, are `docs/tests.md`'s.
