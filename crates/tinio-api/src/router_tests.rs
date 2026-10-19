use std::path::Path;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use tokio::sync::watch;
use tower::ServiceExt;

use super::*;
use crate::{
    state::State,
    test_support::{self, Counting},
};

/// `state` records the control name and the router only passes it through — the value
/// here is an arbitrary recorded name (the derived default is Task 1's, and no arm
/// re-derives one).
fn state() -> State {
    State::new(
        "tinio-0123456789abcdef.sock",
        Some(9000),
        Path::new("/srv/data"),
    )
    .expect("state")
}

fn shared(auth: Auth) -> (Shared, watch::Receiver<bool>) {
    let (tx, rx) = watch::channel(false);
    (Arc::new(Context::new(state(), "127.0.0.1", auth, tx)), rx)
}

/// [`shared`] with a gauge source attached — the state `/metrics` answers `200` from, and
/// the reason `Context::new`'s `None` default is the 404 its own test pins. The reader task
/// is deliberately **not** started: nothing here asserts the cadence, and the double and its
/// gauges are [`test_support`]'s, so there is one definition of both.
fn gauged(auth: Auth) -> (Shared, watch::Receiver<bool>, Arc<Counting>) {
    let (tx, rx) = watch::channel(false);
    let (shared, source) = test_support::gauged(Context::new(state(), "127.0.0.1", auth, tx));
    (shared, rx, source)
}

fn get(path: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(path).method("GET");
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    builder.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn status_answers_with_the_state_on_the_local_channel() {
    let (shared, _rx) = shared(Auth::Local);
    let token = shared.state.token.expose_secret().as_str().to_owned();
    let response = router(shared)
        .oneshot(get("/status", Some(&token)))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(text.contains("\"running\":true"), "{text}");
    assert!(text.contains("\"pid\""), "{text}");
    assert!(
        text.contains("\"host\":\"127.0.0.1\""),
        "the contract's body carries host: {text}"
    );
}

#[tokio::test]
async fn status_refuses_a_missing_or_wrong_token() {
    let (shared, _rx) = shared(Auth::Local);
    for request in [get("/status", None), get("/status", Some("wrong"))] {
        let response = router(shared.clone())
            .oneshot(request)
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}

#[tokio::test]
async fn a_token_that_differs_in_one_byte_is_refused() {
    // The comparison is fixed-length and branch-free past the length check (`tokens_match`
    // below), so the last byte is as expensive to guess as the first: a token that is
    // correct except for its final character is refused like any other. The replacement
    // character is chosen against the one this test removes — comparing it with the
    // character now at the end would leave the token unchanged in one run out of 256.
    let (shared, _rx) = shared(Auth::Local);
    let mut token = shared.state.token.expose_secret().as_str().to_owned();
    let last = token.pop().expect("a 64-character token");
    token.push(if last == 'a' { 'b' } else { 'a' });
    assert!(!tokens_match(
        &token,
        shared.state.token.expose_secret().as_str()
    ));
    assert!(tokens_match(
        shared.state.token.expose_secret().as_str(),
        shared.state.token.expose_secret().as_str()
    ));
    let response = router(shared)
        .oneshot(get("/status", Some(&token)))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_local_channel_does_not_guard_metrics() {
    // The route exists now, so the middleware's rule and the handler are asserted together:
    // a guarded route answers 401 before its handler runs, an open one reaches it — and the
    // scrape drove the cheap half, the counter the folded integration leg read back.
    let (shared, _rx, source) = gauged(Auth::Local);
    let response = router(shared)
        .oneshot(get("/metrics", None))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        source.refreshes() >= 1,
        "the scrape refreshed the cheap half"
    );
}

#[tokio::test]
async fn metrics_answers_200_with_gauges_attached_and_follows_the_bind_rule() {
    // The route's first fixed point: with a source attached, the open side of the rule
    // serves a scrape without a token (`management-api.md:23`), and the handler is what
    // answers — the status is the concrete `200`, not "anything but a 401". What the scrape
    // runs is the cheap half alone: the storage scan is the reader task's, and no request
    // ever reaches it.
    for auth in [Auth::Local, Auth::Loopback] {
        let (shared, _rx, source) = gauged(auth);
        let response = router(shared.clone())
            .oneshot(get("/metrics", None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK, "{auth:?}");
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("the exposition's content type"),
            "text/plain; version=0.0.4; charset=utf-8",
            "{auth:?}"
        );
        assert_eq!(
            source.refreshes(),
            1,
            "one scrape refreshes the cheap half once: {auth:?}"
        );
        assert_eq!(source.scans(), 0, "and never scans: {auth:?}");

        // **...and the flood does not move either number**: the burst the retired
        // `management.rs` leg drove, folded in here — every one of the eight is the handler's
        // cheap half, and the scan count stays where the reader task's cadence put it (none,
        // for a source whose reader is deliberately not started).
        for _ in 0..8 {
            let response = router(shared.clone())
                .oneshot(get("/metrics", None))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{auth:?}");
        }
        assert_eq!(
            source.refreshes(),
            9,
            "every scrape refreshes the cheap half: {auth:?}"
        );
        assert_eq!(source.scans(), 0, "no scrape scans the storage: {auth:?}");
    }
    // A reachable bind requires the token, and presenting it reaches the same handler —
    // the leg that makes the 401 a guard rather than a missing route.
    let (shared, _rx, _source) = gauged(Auth::Tcp);
    let untokened = router(shared.clone())
        .oneshot(get("/metrics", None))
        .await
        .expect("response");
    assert_eq!(untokened.status(), StatusCode::UNAUTHORIZED);
    let token = shared.state.token.expose_secret().as_str().to_owned();
    let tokened = router(shared)
        .oneshot(get("/metrics", Some(&token)))
        .await
        .expect("response");
    assert_eq!(tokened.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_plane_without_a_gauge_source_has_no_metrics_endpoint() {
    // The route's second fixed point (decision 8's round-2 ruling): a plane whose source was
    // never attached answers the contract's `404` rather than an empty `200`, so a scraper
    // sees the endpoint's absence. It holds on the open side — where nothing but the handler
    // can be answering — and behind the token on a reachable bind, which is also what shows
    // the token check runs *before* the handler.
    for auth in [Auth::Local, Auth::Loopback] {
        let (shared, _rx) = shared(auth);
        let response = router(shared)
            .oneshot(get("/metrics", None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{auth:?}");
        // The body is what makes this the handler's answer rather than an unrouted path's
        // (both are `404`): the absence keeps this crate's error shape.
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8");
        assert!(
            text.contains(r#""error""#) && text.contains("metrics are not attached"),
            "the absence keeps this crate's error shape: {text}"
        );
    }
    let (shared, _rx) = shared(Auth::Tcp);
    let token = shared.state.token.expose_secret().as_str().to_owned();
    let response = router(shared)
        .oneshot(get("/metrics", Some(&token)))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_loopback_tcp_plane_opens_metrics_and_nothing_else() {
    // Loopback opens `/metrics` for one stated reason — a Prometheus scrape cannot send a
    // custom header — and that reason does not cover the two control endpoints, which stay
    // guarded exactly as they are on a remote bind (`/stop` by the same path-only rule,
    // asserted under `Auth::Tcp` below). `/openapi.json` is registered only under the
    // `openapi` feature, so it cannot stand in this loop: in the default graph it is
    // unrouted and answers axum's built-in 404 instead of reaching the token check at all.
    // The route's own rule is asserted by the `openapi`-gated test below.
    let (shared, _rx, _source) = gauged(Auth::Loopback);
    let open = router(shared.clone())
        .oneshot(get("/metrics", None))
        .await
        .expect("response");
    assert_eq!(open.status(), StatusCode::OK);
    for path in ["/status", "/stop"] {
        let guarded = router(shared.clone())
            .oneshot(get(path, None))
            .await
            .expect("response");
        assert_eq!(guarded.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
}

#[tokio::test]
async fn tcp_requires_the_token_on_every_registered_endpoint() {
    // `/metrics` joins this loop here, the task that registers the route — an unrouted path
    // answers axum's built-in 404 and never reaches the token check (see the module doc), so
    // a guarded `401` on it is only meaningful now that the route exists.
    let (shared, _rx, _source) = gauged(Auth::Tcp);
    for path in ["/status", "/stop", "/metrics"] {
        let response = router(shared.clone())
            .oneshot(get(path, None))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
}

#[tokio::test]
async fn a_bearer_token_is_accepted_wherever_the_header_is() {
    // A Prometheus `scrape_config` can send `Authorization: Bearer` and never a custom
    // header (`management-api.md:15`), so the alias holds on every transport.
    for auth in [Auth::Local, Auth::Loopback, Auth::Tcp] {
        let (shared, _rx) = shared(auth);
        let token = shared.state.token.expose_secret().as_str().to_owned();
        let request = Request::builder()
            .uri("/status")
            .method("GET")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .expect("request");
        let response = router(shared).oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK, "{auth:?}");
    }
}

#[tokio::test]
async fn stop_flips_the_shutdown_channel_and_answers_202() {
    let (shared, mut rx) = shared(Auth::Local);
    let token = shared.state.token.expose_secret().as_str().to_owned();
    let response = router(shared)
        .oneshot(
            Request::builder()
                .uri("/stop")
                .method("POST")
                .header("x-tinio-token", token)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    // The contract's `202` body, not just the status: the drain the CLI reports is this
    // string.
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(text.contains("\"draining\""), "{text}");
    rx.changed().await.expect("the channel flips");
    assert!(*rx.borrow());
}

#[tokio::test]
async fn an_unknown_path_is_404_with_or_without_the_token() {
    // An unmatched path never reaches the middleware — the `route_layer` rule is the module
    // doc's — so both requests get the contract's plain `404` (`management-api.md:50`) and
    // nothing about the plane leaks.
    let (shared, _rx) = shared(Auth::Local);
    let token = shared.state.token.expose_secret().as_str().to_owned();
    let with_token = router(shared.clone())
        .oneshot(get("/nope", Some(&token)))
        .await
        .expect("response");
    assert_eq!(with_token.status(), StatusCode::NOT_FOUND);
    let without_token = router(shared)
        .oneshot(get("/nope", None))
        .await
        .expect("response");
    assert_eq!(without_token.status(), StatusCode::NOT_FOUND);
}

#[cfg(feature = "openapi")]
#[tokio::test]
async fn openapi_json_serves_the_document_under_the_paths_own_rule() {
    // Task 8's route, registered above the token layer like the others, so the rule
    // `Auth::guards` already carried for the path is enforced: open on the local channel,
    // token on a reachable bind, and what a permitted caller receives is the document.
    let (local, _rx) = shared(Auth::Local);
    let open = router(local)
        .oneshot(get("/openapi.json", None))
        .await
        .expect("response");
    assert_eq!(open.status(), StatusCode::OK);
    let body = open.into_body().collect().await.expect("body").to_bytes();
    let text = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(
        text.contains("/openapi.json"),
        "the route answers the document: {text}"
    );
    // The middle rule: a loopback bind opens `/metrics` and nothing else, and the schema is
    // not a scrape — it needs the token there, exactly as it does on a reachable bind.
    let (loopback, _rx) = shared(Auth::Loopback);
    let untokened = router(loopback)
        .oneshot(get("/openapi.json", None))
        .await
        .expect("response");
    assert_eq!(
        untokened.status(),
        StatusCode::UNAUTHORIZED,
        "/openapi.json"
    );
    let (remote, _rx) = shared(Auth::Tcp);
    let token = remote.state.token.expose_secret().as_str().to_owned();
    let untokened = router(remote.clone())
        .oneshot(get("/openapi.json", None))
        .await
        .expect("response");
    assert_eq!(untokened.status(), StatusCode::UNAUTHORIZED);
    let tokened = router(remote)
        .oneshot(get("/openapi.json", Some(&token)))
        .await
        .expect("response");
    assert_eq!(tokened.status(), StatusCode::OK);
}
