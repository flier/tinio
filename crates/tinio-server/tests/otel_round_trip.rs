//! The OTLP round trip, driven by a real request (plan Task 9 Step 3).
//!
//! A T0 span started by the data plane must reach an exporter with its parent
//! chain intact. The in-memory exporter is the collector: no network, no
//! collector process, and the assertion is on the exported `SpanData`, not on
//! log text.
//!
//! `#[tokio::test]` runs a current-thread runtime, so the plane's spawned
//! connection task polls on the test thread and sees the thread-local
//! subscriber installed here.

#![cfg(feature = "otel")]

mod common;

use opentelemetry::trace::{SpanId, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tinio_util::testing::http::exchange;
use tracing::subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn a_request_span_and_its_operation_export_with_the_parent_chain() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(OpenTelemetryLayer::new(
        provider.tracer("tinio-server-test"),
    ));
    let _guard = subscriber::set_default(subscriber);

    let plane = common::spawn_plane().await;
    let addr = plane.addr;

    let put = exchange("localhost", addr, "PUT", "/data/key", &[], b"hello", 0)
        .await
        .status;
    assert_eq!(put, 200, "the PUT stores the object");
    plane.stop().await;

    provider.force_flush().expect("the exporter flushes");
    let spans = exporter
        .get_finished_spans()
        .expect("the in-memory exporter reads its finished spans");
    let find = |name: &str| {
        spans
            .iter()
            .find(|span| span.name.as_ref() == name)
            .unwrap_or_else(|| panic!("the exported {name} span: {spans:#?}"))
    };
    let request = find("tinio.http.request");
    let operation = find("tinio.s3.operation");
    assert_eq!(
        request.parent_span_id,
        SpanId::INVALID,
        "the request is the root"
    );
    assert_ne!(
        operation.parent_span_id,
        SpanId::INVALID,
        "the operation is not a root"
    );
    let attribute = |span: &SpanData, name: &str| {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == name)
            .map(|kv| kv.value.as_str().into_owned())
    };
    assert_eq!(attribute(operation, "op").as_deref(), Some("PutObject"));
    assert_eq!(attribute(operation, "bucket").as_deref(), Some("data"));
    assert_eq!(attribute(operation, "key").as_deref(), Some("key"));

    // Walk up from the operation; the request must be on the chain. s3s opens
    // its own `call` span between them, so the request is an ancestor, not
    // necessarily the direct parent.
    let request_id = request.span_context.span_id();
    let mut current = operation;
    let mut reached = false;
    while current.parent_span_id != SpanId::INVALID {
        if current.parent_span_id == request_id {
            reached = true;
            break;
        }
        let Some(parent) = spans
            .iter()
            .find(|span| span.span_context.span_id() == current.parent_span_id)
        else {
            break;
        };
        current = parent;
    }
    assert!(
        reached,
        "the operation's parent chain reaches the request span: {spans:#?}"
    );

    provider
        .shutdown()
        .expect("the in-memory provider shuts down");
}
