//! The management plane's spans, asserted from a dedicated binary: the lib test binary runs
//! every other router test in parallel, and a callsite another thread registers while no
//! subscriber is installed is cached `never` for the process — a thread-scoped capture cannot
//! reliably turn it back on. A global recorder in a binary with nothing else to race is the
//! deterministic shape.

use std::sync::Arc;

use axum::{body::Body, http::Request};
use tempfile::TempDir;
use tinio_api::{
    _core::mode::AccessMode,
    Options, Plane,
    router::{Auth, Context, Shared, router},
    state::State,
    transport::Selection,
};
use tinio_util::testing::spans;
use tokio::{sync::watch, task::yield_now};
use tower::ServiceExt;
use tracing::Level;

/// The token the test plane serves with: 32 bytes, hex, exactly what `State::new` would mint.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

#[expect(
    clippy::expect_used,
    reason = "test support: every part of these literal requests is valid by construction, so a builder error is this test's own typo rather than a runtime condition"
)]
fn request(method: &str, target: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(target).method(method);
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    builder.body(Body::empty()).expect("request")
}

#[tokio::test]
async fn the_management_plane_opens_its_request_state_and_shutdown_spans() {
    spans::install();
    let root = TempDir::new().expect("temp dir");
    let state = State::with_token(
        "tinio-0123456789abcdef.sock",
        Some(9000),
        root.path(),
        TOKEN,
    )
    .expect("state");
    let (stop, _stop) = watch::channel(false);
    let shared: Shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, stop));

    let cases = [
        ("GET", "/status?probe=1", "/status", Some(TOKEN), 200),
        ("GET", "/status", "/status", None, 401),
        ("GET", "/metrics", "/metrics", None, 200),
        ("POST", "/stop", "/stop", Some(TOKEN), 202),
    ];
    for (method, target, _, token, status) in cases {
        let answer = router(shared.clone())
            .oneshot(request(method, target, token))
            .await
            .expect("response");
        assert_eq!(answer.status().as_u16(), status, "{method} {target}");
    }
    shared.state.write(root.path()).expect("state write");

    // The shutdown trigger: `serve` awaits the plane's channel through `until_stopped`, so
    // flipping the sender the plane owns is what closes the debug span; no request is involved.
    let (plane_stop, plane_rx) = watch::channel(false);
    let trigger_stop = plane_stop.clone();
    let plane = Plane::bind(Options {
        selection: Selection {
            http: Some("127.0.0.1:0".parse().expect("addr")),
            ..Selection::default()
        },
        state_dir: root.path().to_path_buf(),
        root: root.path().to_path_buf(),
        host: "127.0.0.1".to_string(),
        stop: plane_stop,
        metrics_requires_token: false,
        access: AccessMode::ReadWrite,
        token: None,
        #[cfg(feature = "lifecycle")]
        lifecycle: None,
        #[cfg(feature = "object-lock")]
        object_lock: None,
    })
    .expect("bind");
    let serve = plane.serve(plane_rx);
    let trigger = async move {
        yield_now().await;
        trigger_stop.send(true).expect("the plane is listening");
    };
    let (served, ()) = tokio::join!(serve, trigger);
    served.expect("serve ends on the stop");

    let recorded = spans::spans();
    let requests: Vec<&spans::RecordedSpan> = recorded
        .iter()
        .filter(|span| span.name == "tinio.api.request")
        .collect();
    assert_eq!(requests.len(), cases.len(), "{recorded:#?}");
    for (span, (method, _, path, _, status)) in requests.iter().zip(cases) {
        assert_eq!(span.target, "tinio::api");
        assert_eq!(span.level, Level::INFO);
        assert_eq!(span.field("method"), Some(method));
        assert_eq!(span.field("path"), Some(path));
        assert_eq!(
            span.field("status")
                .and_then(|value| value.parse::<u16>().ok()),
            Some(status),
            "{span:?}"
        );
        // The request itself is skipped: its headers carry the control token.
        assert!(span.field("request").is_none(), "{span:?}");
        assert!(
            !span.fields.iter().any(|(_, value)| value.contains(TOKEN)),
            "the control token never enters the span: {span:?}"
        );
    }

    let states: Vec<&spans::RecordedSpan> = recorded
        .iter()
        .filter(|span| span.name == "tinio.api.state_write")
        .collect();
    assert_eq!(states.len(), 1, "{recorded:#?}");
    assert_eq!(states[0].target, "tinio::api");
    assert_eq!(states[0].level, Level::DEBUG);

    let shutdowns: Vec<&spans::RecordedSpan> = recorded
        .iter()
        .filter(|span| span.name == "tinio.api.shutdown")
        .collect();
    assert_eq!(shutdowns.len(), 1, "{recorded:#?}");
    assert_eq!(shutdowns[0].target, "tinio::api");
    assert_eq!(shutdowns[0].level, Level::DEBUG);
}
