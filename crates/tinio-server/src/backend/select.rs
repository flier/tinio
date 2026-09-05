#![cfg(feature = "select")]

//! SelectObjectContent of the mapping layer (spec 2026-09-04).
//!
//! The op validates the request (expression, serialization combinations,
//! single-byte delimiters, ScanRange, JSON-output alias rule), fetches the
//! object through the read path, and bridges the synchronous
//! [`tinio_select::events::select_iter`] onto s3s (spec §1.1): a forwarder
//! task pumps the storage body stream into a bounded channel, `spawn_blocking`
//! drives the engine over a [`ChannelReader`], and the response is a
//! hand-rolled `futures::Stream` over a second channel (`Receiver::poll_recv`
//! — no `tokio-stream`) mapping [`SelectEvent`] → `dto` events and
//! [`SelectError`] → `S3Error` items. CPU-bound work never runs on a tokio
//! worker; cancellation unwinds the channel topology (the s3s stream drop
//! ends each stage).
//!
//! Runtime errors mapping (§3): `Ambiguous` → `AmbiguousFieldName`,
//! `MissingHeader` → `Custom("MissingHeaderName")`, everything else →
//! `Custom("S3QueryError")` — all in-stream items inside the 200 response.
//! Request-level `Custom` errors ([`S3QueryParsingError`]) MUST carry
//! `set_status_code(BAD_REQUEST)` — s3s serializes a `Custom` with no status
//! override as HTTP 500.

use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex, OnceLock},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::StatusCode;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto, s3_error};
use tinio_select::{
    SelectError,
    events::{ContPolicy, InputFormat, JsonParams, ParquetParams, SelectConfig, SelectEvent},
    json::JsonType,
    output::{CsvOutputParams, JsonOutputParams, OutputMode, QuoteFields},
    record::{Compression, CsvHeader, CsvParams},
    sql,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{
    _core::storage::{GetObjectResult, Storage},
    backend::{S3Backend, map_backend_error},
};

/// Bound of both bridge channels (spec §1.1: capacity 4) — backpressure on
/// the storage read, bounded memory mid-pipeline.
const CHANNEL_CAP: usize = 4;

/// Concurrency cap on streaming select jobs (review 2026-09-05 #4): at most
/// [`SELECT_CONCURRENCY`] responses stream at once, so a pile of concurrent
/// selects cannot saturate tokio workers on top of `spawn_blocking`. The
/// permit is held for the whole response (dropped on stream end/cancel).
const SELECT_CONCURRENCY: usize = 4;

/// The select concurrency cap, shared across backend instances (one process
/// serves one cluster).
fn select_semaphore() -> Arc<Semaphore> {
    static SEM: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| Arc::new(Semaphore::new(SELECT_CONCURRENCY)))
        .clone()
}

/// The sync bridge input (spec §1.1): `std::io::Read` over the forwarder's
/// bounded channel — `blocking_recv` blocks the calling thread, so this
/// reader is used ONLY inside `spawn_blocking` (never on a tokio worker, the
/// runtime's documented panic). EOF when the forwarder drops the sender;
/// a storage read error passes through as `io::Error` (the engine surfaces
/// it as an in-stream error item).
struct ChannelReader {
    rx: mpsc::Receiver<io::Result<Bytes>>,
    /// The unpumped tail of a chunk larger than the caller's buffer.
    pending: Option<Bytes>,
}

impl io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let bytes = match self.pending.take() {
            Some(b) => b,
            None => match self.rx.blocking_recv() {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(e),
                // The forwarder dropped: EOF.
                None => return Ok(0),
            },
        };
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        if n < bytes.len() {
            self.pending = Some(bytes.slice(n..));
        }
        Ok(n)
    }
}

/// The s3s response stream: a hand-rolled `futures::Stream` over the output
/// receiver's `poll_recv` (spec §1.1 — the server has no `tokio-stream` and
/// none is added). The `std::sync::Mutex` exists only to satisfy the
/// `Send + Sync` bound of `SelectObjectContentEventStream::new`;
/// `poll_recv` takes `&mut self` and is exclusive by construction. The
/// semaphore permit rides here: dropped when the stream ends or the client
/// drops it — which unwinds the pipeline (the engine's next send fails, its
/// output drops the `ChannelReader`, the forwarder stops).
struct EventStream {
    rx: StdMutex<mpsc::Receiver<Result<SelectEvent, SelectError>>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Stream for EventStream {
    type Item = S3Result<dto::SelectObjectContentEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let item = {
            let mut rx = this.rx.lock().expect("select event stream poisoned");
            rx.poll_recv(cx)
        };
        match item {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                // The engine finished: the stream is over — release the cap.
                this.permit.take();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(map_event(event)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(map_stream_error(e)))),
        }
    }
}

/// [`SelectEvent`] → s3s event (spec §1.1 mapping).
fn map_event(event: SelectEvent) -> dto::SelectObjectContentEvent {
    match event {
        SelectEvent::Records(bytes) => dto::SelectObjectContentEvent::Records(dto::RecordsEvent {
            payload: Some(dto::Body::from(bytes)),
        }),
        SelectEvent::Progress {
            bytes_scanned,
            bytes_processed,
            bytes_returned,
        } => dto::SelectObjectContentEvent::Progress(dto::ProgressEvent {
            details: Some(dto::Progress {
                bytes_scanned: Some(bytes_scanned as i64),
                bytes_processed: Some(bytes_processed as i64),
                bytes_returned: Some(bytes_returned as i64),
            }),
        }),
        SelectEvent::Stats {
            bytes_scanned,
            bytes_processed,
            bytes_returned,
        } => dto::SelectObjectContentEvent::Stats(dto::StatsEvent {
            details: Some(dto::Stats {
                bytes_scanned: Some(bytes_scanned as i64),
                bytes_processed: Some(bytes_processed as i64),
                bytes_returned: Some(bytes_returned as i64),
            }),
        }),
        SelectEvent::Cont => dto::SelectObjectContentEvent::Cont(dto::ContinuationEvent {}),
        SelectEvent::End => dto::SelectObjectContentEvent::End(dto::EndEvent {}),
    }
}

/// [`SelectError`] → in-stream s3s error (design §3). These items sit inside
/// the 200 event stream — no status override (a `Custom` status only matters
/// for request-level errors, which must be set explicitly, below).
fn map_stream_error(e: SelectError) -> S3Error {
    match e {
        SelectError::Ambiguous(m) => S3Error::with_message(S3ErrorCode::AmbiguousFieldName, m),
        SelectError::MissingHeader(m) => {
            S3Error::with_message(S3ErrorCode::Custom("MissingHeaderName".into()), m)
        }
        e => S3Error::with_message(S3ErrorCode::Custom("S3QueryError".into()), e.to_string()),
    }
}

/// The request-level SQL error (design §3 row 1): `S3QueryParsingError` does
/// not exist as an s3s variant — a `Custom` whose default status is HTTP 500
/// (`Custom.status_code() == None` → 500 on serialize) — so every one of
/// these MUST set BAD_REQUEST explicitly (review 2026-09-05 #2/#11).
fn query_parsing_error(e: SelectError) -> S3Error {
    let message = match e {
        SelectError::Parse(m) | SelectError::Unsupported(m) => m,
        e => e.to_string(),
    };
    let mut err = S3Error::with_message(S3ErrorCode::Custom("S3QueryParsingError".into()), message);
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

impl<S: Storage> S3Backend<S> {
    /// SelectObjectContent — request-level validation first (nothing is
    /// streamed before every 400 below), then the §2 read path and the §1.1
    /// bridge. The static checks (expression type/parse/limit, exactly one
    /// input and one output serialization, parquet compression/ScanRange
    /// constraints, single-byte delimiters, JSON-output alias rule) run
    /// before the fetch; the size-dependent ones (ScanRange window resolved
    /// against `info.size`, the parquet memory bound) run on the fetch's own
    /// size — one storage round trip, no `head_object` — before any body
    /// byte is consumed.
    pub(crate) async fn op_select_object_content(
        &self,
        req: S3Request<dto::SelectObjectContentInput>,
    ) -> S3Result<S3Response<dto::SelectObjectContentOutput>> {
        Self::require_cap(self.caps.select, "SelectObjectContent")?;
        let permit = select_semaphore()
            .acquire_owned()
            .await
            .map_err(|_| s3_error!(InternalError, "select semaphore closed"))?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let (plan, mut config, raw_scan) = build_config(&req.input.request)?;

        let fetched = self
            .storage
            .get_object(&bucket, &key, None)
            .await
            .map_err(map_backend_error)?;
        let size = fetched.info.size;
        // Size-dependent checks — still before a body byte is consumed.
        config.scan_range = match raw_scan {
            Some(scan) => Some(resolve_scan_range(&scan, size)?),
            None => None,
        };
        if matches!(config.input_format, InputFormat::Parquet(_)) && size > config.max_parquet_bytes
        {
            return Err(s3_error!(
                InvalidRequestParameter,
                "parquet object of {} bytes exceeds the {} byte memory bound",
                size,
                config.max_parquet_bytes
            ));
        }
        config.size = Some(size);

        // The bridge (spec §1.1): a forwarder feeds the body stream into a
        // bounded channel; `spawn_blocking` runs the synchronous engine over
        // the ChannelReader; results flow out over a second bounded channel.
        let GetObjectResult { body, .. } = fetched;
        let (in_tx, in_rx) = mpsc::channel::<io::Result<Bytes>>(CHANNEL_CAP);
        tokio::spawn(async move {
            let mut body = body;
            while let Some(item) = body.next().await {
                if in_tx.send(item).await.is_err() {
                    // The engine exited (cancelled): stop pumping.
                    return;
                }
            }
            // The sender drops here — the ChannelReader sees EOF.
        });
        let (out_tx, out_rx) = mpsc::channel::<Result<SelectEvent, SelectError>>(CHANNEL_CAP);
        tokio::task::spawn_blocking(move || {
            let input = Box::new(ChannelReader {
                rx: in_rx,
                pending: None,
            });
            for item in tinio_select::events::select_iter(plan, config, input) {
                if out_tx.blocking_send(item).is_err() {
                    // The response stream dropped: the client is gone.
                    break;
                }
            }
        });

        Ok(S3Response::new(dto::SelectObjectContentOutput {
            payload: Some(dto::SelectObjectContentEventStream::new(EventStream {
                rx: StdMutex::new(out_rx),
                permit: Some(permit),
            })),
        }))
    }
}

/// The request's static validation + `SelectConfig` build. `scan_range` is
/// returned unresolved — the op resolves it against the fetched object size —
/// and `config.scan_range`/`config.size` are left for the op.
fn build_config(
    request: &dto::SelectObjectContentRequest,
) -> S3Result<(
    tinio_select::sql::QueryPlan,
    SelectConfig,
    Option<dto::ScanRange>,
)> {
    // ExpressionType: only SQL exists.
    if request.expression_type.as_str() != dto::ExpressionType::SQL {
        return Err(s3_error!(
            InvalidRequestParameter,
            "ExpressionType must be SQL"
        ));
    }
    // The expression parse + grammar gate runs here, request-level — the
    // engine receives the pre-built plan (rejected constructs → §3 row 1).
    let plan = sql::parse(&request.expression).map_err(query_parsing_error)?;

    // Exactly one input serialization (CSV/JSON/Parquet).
    let inputs = [
        request.input_serialization.csv.is_some(),
        request.input_serialization.json.is_some(),
        request.input_serialization.parquet.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if inputs != 1 {
        return Err(s3_error!(
            InvalidRequestParameter,
            "exactly one of CSV, JSON, or Parquet input serialization must be specified"
        ));
    }
    // Exactly one output serialization (CSV/JSON).
    if request.output_serialization.csv.is_some() == request.output_serialization.json.is_some() {
        return Err(s3_error!(
            InvalidRequestParameter,
            "exactly one of CSV or JSON output serialization must be specified"
        ));
    }

    let compression = request
        .input_serialization
        .compression_type
        .as_ref()
        .map(|c| c.as_str())
        .unwrap_or(dto::CompressionType::NONE);
    let parquet = request.input_serialization.parquet.is_some();
    // Parquet ⇒ no compression (the engine reads parquet out of the raw
    // stream), and no ScanRange (row-group positions are meaningless for it).
    if parquet && compression != dto::CompressionType::NONE {
        return Err(s3_error!(
            InvalidRequestParameter,
            "Parquet input must not be compressed"
        ));
    }
    if parquet && request.scan_range.is_some() {
        return Err(s3_error!(
            InvalidRequestParameter,
            "ScanRange is not supported for Parquet input"
        ));
    }
    // ScanRange ⇒ no compression (AWS semantics for the combination are
    // ambiguous; refuse rather than guess).
    if request.scan_range.is_some() && compression != dto::CompressionType::NONE {
        return Err(s3_error!(
            InvalidRequestParameter,
            "ScanRange does not support compressed input"
        ));
    }

    let input_format = if let Some(csv) = request.input_serialization.csv.as_ref() {
        InputFormat::Csv(csv_input_params(csv)?)
    } else if let Some(json) = request.input_serialization.json.as_ref() {
        InputFormat::Json(json_input_params(json)?)
    } else {
        // Parquet: project down to the columns the plan references.
        InputFormat::Parquet(ParquetParams {
            projection: sql::referenced_columns(&plan),
        })
    };
    let output = if let Some(csv) = request.output_serialization.csv.as_ref() {
        OutputMode::Csv(csv_output_params(csv)?)
    } else {
        // JSON output names its columns from aliases / plain field names
        // (grilling Q6): any other expression without an alias would need a
        // guessed key — rejected request-level, CSV output is positional and
        // never gated.
        if plan.projections.iter().any(sql::projection_needs_alias) {
            return Err(s3_error!(
                InvalidRequestParameter,
                "a JSON output projection that is not a column reference must have an alias"
            ));
        }
        OutputMode::Json(json_output_params(
            request
                .output_serialization
                .json
                .as_ref()
                .expect("checked above"),
        )?)
    };

    Ok((
        plan,
        SelectConfig {
            input_format,
            output,
            compression: match compression {
                dto::CompressionType::GZIP => Compression::Gzip,
                dto::CompressionType::BZIP2 => Compression::Bzip2,
                _ => Compression::None_,
            },
            // Resolved by the op against the fetched object size.
            scan_range: None,
            request_progress: request
                .request_progress
                .as_ref()
                .and_then(|p| p.enabled)
                .unwrap_or(false),
            cont: ContPolicy::default(),
            size: None,
            max_parquet_bytes: tinio_select::events::MAX_PARQUET_BYTES,
        },
        request.scan_range.clone(),
    ))
}

/// A single-character wire parameter: one byte after the checks, else the
/// AWS default. Delimiters and quote chars are single-character per AWS; the
/// engine is `u8`-based (no multi-byte, documented non-goal).
fn single_char(value: &Option<String>, default: u8, name: &str) -> S3Result<u8> {
    match value {
        None => Ok(default),
        Some(text) => {
            let bytes = text.as_bytes();
            if bytes.len() != 1 {
                return Err(s3_error!(
                    InvalidRequestParameter,
                    "{name} must be a single character"
                ));
            }
            Ok(bytes[0])
        }
    }
}

/// The comment character: AWS default `#` (the engine's own default —
/// [`SelectConfig`] — is None, the crate's raw-pipeline pick; the wire
/// default is what the op applies).
fn single_char_opt(value: &Option<String>, default: u8, name: &str) -> S3Result<Option<u8>> {
    match value {
        None => Ok(Some(default)),
        Some(text) => {
            let bytes = text.as_bytes();
            if bytes.len() != 1 {
                return Err(s3_error!(
                    InvalidRequestParameter,
                    "{name} must be a single character"
                ));
            }
            Ok(Some(bytes[0]))
        }
    }
}

/// dto `CSVInput` → engine params (unset fields take the AWS documented
/// defaults: `,`, `\n`, `"`, `"`, `#`, `NONE`).
fn csv_input_params(csv: &dto::CSVInput) -> S3Result<CsvParams> {
    let header = match csv.file_header_info.as_ref() {
        None => CsvHeader::None_,
        Some(info) => match info.as_str() {
            dto::FileHeaderInfo::USE => CsvHeader::Use,
            dto::FileHeaderInfo::IGNORE => CsvHeader::Ignore,
            // AWS accepts NONE (also the catch-all for an unrecognized
            // value — the wire enum is closed).
            _ => CsvHeader::None_,
        },
    };
    Ok(CsvParams {
        field_delimiter: single_char(&csv.field_delimiter, b',', "FieldDelimiter")?,
        record_delimiter: single_char(&csv.record_delimiter, b'\n', "RecordDelimiter")?,
        quote: single_char(&csv.quote_character, b'"', "QuoteCharacter")?,
        escape: single_char(&csv.quote_escape_character, b'"', "QuoteEscapeCharacter")?,
        comments: single_char_opt(&csv.comments, b'#', "Comments")?,
        header,
        // Known deviation (#7): the flag is not enforced by the engine —
        // permissive (AWS true) behavior applies either way; documented and
        // pinned by the tinio-select tests, never silently ignored.
        allow_quoted_record_delimiter: csv.allow_quoted_record_delimiter.unwrap_or(true),
    })
}

/// dto `JSONInput` → engine params (unset type = `LINES`).
fn json_input_params(json: &dto::JSONInput) -> S3Result<JsonParams> {
    let ty = match json.type_.as_ref() {
        None => JsonType::Lines,
        Some(t) => match t.as_str() {
            dto::JSONType::DOCUMENT => JsonType::Document,
            _ => JsonType::Lines,
        },
    };
    Ok(JsonParams { ty })
}

/// dto `CSVOutput` → engine params (unset fields take the AWS documented
/// defaults: `,`, `\n`, `"`, `"`, `ASNEEDED`).
fn csv_output_params(csv: &dto::CSVOutput) -> S3Result<CsvOutputParams> {
    Ok(CsvOutputParams {
        field_delimiter: single_char(&csv.field_delimiter, b',', "FieldDelimiter")?,
        record_delimiter: single_char(&csv.record_delimiter, b'\n', "RecordDelimiter")?,
        quote: single_char(&csv.quote_character, b'"', "QuoteCharacter")?,
        escape: single_char(&csv.quote_escape_character, b'"', "QuoteEscapeCharacter")?,
        quote_fields: match csv.quote_fields.as_ref() {
            None => QuoteFields::AsNeeded,
            Some(q) => match q.as_str() {
                dto::QuoteFields::ALWAYS => QuoteFields::Always,
                _ => QuoteFields::AsNeeded,
            },
        },
    })
}

/// dto `JSONOutput` → engine params (unset record delimiter = `\n`).
fn json_output_params(json: &dto::JSONOutput) -> S3Result<JsonOutputParams> {
    Ok(JsonOutputParams {
        record_delimiter: single_char(&json.record_delimiter, b'\n', "RecordDelimiter")?,
    })
}

/// The ScanRange window resolved against the object size (review 2026-09-05
/// #6): `start`-only → `[start, size-1]`; both → `[start, end]` with
/// `end < size`; `end`-only (AWS "scan the last N bytes") → the trailing
/// `[size-end, size-1]`. Every arithmetic path is checked — an out-of-range
/// or underflowing window errors 400 instead of wrapping or panicking (the
/// `end`-only `size - end` is guarded by `end ≤ size`).
fn resolve_scan_range(scan: &dto::ScanRange, size: u64) -> S3Result<(u64, Option<u64>)> {
    let window = match (scan.start, scan.end) {
        // "must not be empty" (AWS).
        (None, None) => Err(s3_error!(
            InvalidRequestParameter,
            "ScanRange must not be empty"
        )),
        (Some(start), None) => {
            if start < 0 {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange values must be non-negative"
                ))
            } else if start as u64 >= size {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange start is beyond the object size"
                ))
            } else {
                Ok((start as u64, None))
            }
        }
        (None, Some(end)) => {
            if end < 1 {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange end must be at least 1"
                ))
            } else if end as u64 > size {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange end is beyond the object size"
                ))
            } else {
                // The trailing window; `size - end` cannot underflow (checked
                // above), `size - 1` needs size ≥ 1 (end ≥ 1 ≤ size).
                Ok((size - end as u64, Some(size - 1)))
            }
        }
        (Some(start), Some(end)) => {
            if start < 0 || end < 0 {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange values must be non-negative"
                ))
            } else if start > end {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange start must not exceed end"
                ))
            } else if end as u64 >= size {
                Err(s3_error!(
                    InvalidRequestParameter,
                    "ScanRange end is beyond the object size"
                ))
            } else {
                Ok((start as u64, Some(end as u64)))
            }
        }
    };
    window
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use s3s::S3;
    use tinio_select::{events::ContPolicy, output::CsvOutputParams};

    use super::*;
    use crate::{
        _core::{bucket, storage::ObjectOps},
        _mem::MemoryStorage,
        _util::testing::body,
        backend::{
            Capabilities,
            testutil::{s3_request, setup, setup_with_caps},
        },
    };

    async fn setup_name() -> (S3Backend<MemoryStorage>, bucket::Name) {
        let (backend, b) = setup().await;
        (backend, bucket::name(b.as_str()).unwrap())
    }

    fn request(expression: &str) -> dto::SelectObjectContentRequest {
        dto::SelectObjectContentRequest {
            expression: expression.into(),
            expression_type: dto::ExpressionType::from_static(dto::ExpressionType::SQL),
            input_serialization: dto::InputSerialization {
                csv: Some(dto::CSVInput::default()),
                ..Default::default()
            },
            output_serialization: dto::OutputSerialization {
                csv: Some(dto::CSVOutput::default()),
                ..Default::default()
            },
            request_progress: None,
            scan_range: None,
        }
    }

    async fn select(
        backend: &S3Backend<MemoryStorage>,
        b: &bucket::Name,
        request: dto::SelectObjectContentRequest,
    ) -> S3Result<S3Response<dto::SelectObjectContentOutput>> {
        backend
            .select_object_content(s3_request(dto::SelectObjectContentInput {
                bucket: b.to_string(),
                key: "data.csv".into(),
                request,
                expected_bucket_owner: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            }))
            .await
    }

    /// Drain an event stream, joining the Records payload bytes.
    async fn collect_records(stream: dto::SelectObjectContentEventStream) -> Vec<u8> {
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            if let dto::SelectObjectContentEvent::Records(r) = item.unwrap() {
                out.extend_from_slice(&r.payload.unwrap());
            }
        }
        out
    }

    /// Drive the event stream to its first error item.
    async fn stream_error(stream: dto::SelectObjectContentEventStream) -> S3Error {
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                return e;
            }
        }
        panic!("event stream ended without an error item");
    }

    /// The named-body test object.
    async fn put(backend: &S3Backend<MemoryStorage>, b: &bucket::Name, key: &str, data: &[u8]) {
        backend
            .storage()
            .put_object(b, &object_key(key), body(data.to_vec()))
            .await
            .unwrap();
    }

    fn object_key(key: &str) -> crate::_core::object::Key {
        crate::_core::object::key(key).unwrap()
    }

    #[tokio::test]
    async fn select_filters_by_column() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"a,b\n1,x\n2,y\n").await;
        let resp = select(
            &backend,
            &b,
            request("SELECT s._1, s._2 FROM S3Object s WHERE s._1 = '1'"),
        )
        .await
        .unwrap();
        let records = collect_records(resp.output.payload.unwrap()).await;
        assert_eq!(records, b"1,x\n");
    }

    #[tokio::test]
    async fn select_csv_header_use_names_columns() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"name,age\nalice,30\nbob,25\n").await;
        let mut req = request("SELECT s.name FROM S3Object s WHERE s.age > 26");
        req.input_serialization.csv = Some(dto::CSVInput {
            file_header_info: Some(dto::FileHeaderInfo::from_static(dto::FileHeaderInfo::USE)),
            ..Default::default()
        });
        let resp = select(&backend, &b, req).await.unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"alice\n"
        );
    }

    #[tokio::test]
    async fn select_emits_records_stats_end_in_order() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"1\n2\n").await;
        let resp = select(&backend, &b, request("SELECT * FROM S3Object s"))
            .await
            .unwrap();
        let mut stream = resp.output.payload.unwrap();
        let mut kinds = Vec::new();
        while let Some(item) = stream.next().await {
            match item.unwrap() {
                dto::SelectObjectContentEvent::Records(r) => {
                    assert_eq!(&r.payload.unwrap()[..], b"1\n2\n");
                    kinds.push("Records");
                }
                dto::SelectObjectContentEvent::Stats(_) => kinds.push("Stats"),
                dto::SelectObjectContentEvent::End(_) => kinds.push("End"),
                dto::SelectObjectContentEvent::Cont(_) => kinds.push("Cont"),
                dto::SelectObjectContentEvent::Progress(_) => kinds.push("Progress"),
                _ => kinds.push("Other"),
            }
        }
        assert_eq!(kinds, ["Records", "Stats", "End"]);
    }

    #[tokio::test]
    async fn select_missing_object_is_no_such_key() {
        let (backend, b) = setup_name().await;
        let err = select(&backend, &b, request("SELECT * FROM S3Object s"))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NoSuchKey");
    }

    #[tokio::test]
    async fn select_capability_off_is_not_implemented() {
        let (backend, _) = setup_with_caps(Capabilities {
            select: false,
            ..Default::default()
        })
        .await;
        let err = backend
            .select_object_content(s3_request(dto::SelectObjectContentInput {
                bucket: "data".into(),
                key: "data.csv".into(),
                request: request("SELECT * FROM S3Object s"),
                expected_bucket_owner: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented");
    }

    #[tokio::test]
    async fn request_level_errors_are_400() {
        let (backend, b) = setup_name().await;
        // JOIN in the expression → the parse gate (§3 row 1).
        let bad = request("SELECT s._1 FROM S3Object s JOIN S3Object t");
        // ExpressionType != SQL → InvalidRequestParameter.
        let mut non_sql = request("SELECT * FROM S3Object s");
        non_sql.expression_type = "PAX".parse().unwrap();
        // Neither input serialization present.
        let mut no_input = request("SELECT * FROM S3Object s");
        no_input.input_serialization = dto::InputSerialization::default();
        // Neither output serialization present.
        let mut no_output = request("SELECT * FROM S3Object s");
        no_output.output_serialization = dto::OutputSerialization::default();
        // Both CSV and JSON input present.
        let mut both_input = request("SELECT * FROM S3Object s");
        both_input.input_serialization.json = Some(dto::JSONInput::default());
        // A multi-byte field delimiter.
        let mut multi_byte = request("SELECT * FROM S3Object s");
        multi_byte.input_serialization.csv = Some(dto::CSVInput {
            field_delimiter: Some("::".into()),
            ..Default::default()
        });

        let err = select(&backend, &b, bad).await.unwrap_err();
        assert_eq!(err.code().as_str(), "S3QueryParsingError", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
        for (req, name) in [
            (non_sql, "non-SQL expression type"),
            (no_input, "no input serialization"),
            (no_output, "no output serialization"),
            (both_input, "both input serializations"),
            (multi_byte, "multi-byte delimiter"),
        ] {
            let err = select(&backend, &b, req).await.unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidRequestParameter",
                "{name}: {err:?}"
            );
            assert_eq!(err.status_code().unwrap().as_u16(), 400, "{name}: {err:?}");
        }
    }

    #[tokio::test]
    async fn select_expression_over_256k_is_query_parsing_error_400() {
        let (backend, b) = setup_name().await;
        let expression = format!(
            "SELECT * FROM S3Object s WHERE s.a = '{}'",
            "x".repeat(256 * 1024)
        );
        let err = select(&backend, &b, request(&expression))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "S3QueryParsingError", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
        assert!(err.message().unwrap().contains("256 KiB"), "{err:?}");
    }

    #[tokio::test]
    async fn select_parquet_constraints_are_400() {
        let (backend, b) = setup_name().await;
        // Parquet + GZIP.
        let mut gz = request("SELECT * FROM S3Object s");
        gz.input_serialization = dto::InputSerialization {
            parquet: Some(dto::ParquetInput {}),
            compression_type: Some(dto::CompressionType::from_static(
                dto::CompressionType::GZIP,
            )),
            ..Default::default()
        };
        let err = select(&backend, &b, gz).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
        // Parquet + ScanRange.
        let mut scan = request("SELECT * FROM S3Object s");
        scan.input_serialization = dto::InputSerialization {
            parquet: Some(dto::ParquetInput {}),
            ..Default::default()
        };
        scan.scan_range = Some(dto::ScanRange {
            start: Some(0),
            end: None,
        });
        let err = select(&backend, &b, scan).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
    }

    #[tokio::test]
    async fn select_scan_range_with_gzip_is_400() {
        let (backend, b) = setup_name().await;
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.compression_type = Some(dto::CompressionType::from_static(
            dto::CompressionType::GZIP,
        ));
        req.scan_range = Some(dto::ScanRange {
            start: Some(0),
            end: None,
        });
        let err = select(&backend, &b, req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
    }

    #[tokio::test]
    async fn select_scan_range_windows_resolve_against_size() {
        // "0,1\n2,3\n4,5\n" — records start at 0, 4, 8 of a 12-byte object.
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"0,1\n2,3\n4,5\n").await;
        let scan_range = |start: Option<i64>, end: Option<i64>| dto::SelectObjectContentRequest {
            scan_range: Some(dto::ScanRange { start, end }),
            ..request("SELECT * FROM S3Object s")
        };
        // start-only: from byte 4 to EOF → rows 2+3.
        let resp = select(&backend, &b, scan_range(Some(4), None))
            .await
            .unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"2,3\n4,5\n"
        );
        // both: [4, 8] → the rows starting at 4 and 8.
        let resp = select(&backend, &b, scan_range(Some(4), Some(8)))
            .await
            .unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"2,3\n4,5\n"
        );
        // end-only: the last 4 bytes [8, 11] → the row at 8.
        let resp = select(&backend, &b, scan_range(None, Some(4)))
            .await
            .unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"4,5\n"
        );
        // Bad windows: empty, negative, start > end, end past the size.
        for (start, end) in [
            (None, None),
            (Some(-1), None),
            (Some(5), Some(1)),
            (Some(0), Some(12)),
        ] {
            let err = select(&backend, &b, scan_range(start, end))
                .await
                .unwrap_err();
            assert_eq!(
                err.code().as_str(),
                "InvalidRequestParameter",
                "{start:?}/{end:?}: {err:?}"
            );
            assert_eq!(
                err.status_code().unwrap().as_u16(),
                400,
                "{start:?}/{end:?}"
            );
        }
    }

    #[tokio::test]
    async fn select_json_output_requires_an_alias_on_expressions() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"1\n").await;
        // JSON output + a bare expression (no alias) → 400.
        let mut req = request("SELECT s._1 + 1 FROM S3Object s");
        req.output_serialization = dto::OutputSerialization {
            json: Some(dto::JSONOutput::default()),
            ..Default::default()
        };
        let err = select(&backend, &b, req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
        // CSV output positional — the same expression is allowed.
        let resp = select(
            &backend,
            &b,
            request("SELECT s._1 + 1 FROM S3Object s"), // alias rule is JSON-scoped
        )
        .await
        .unwrap();
        assert_eq!(collect_records(resp.output.payload.unwrap()).await, b"2\n");
    }

    #[tokio::test]
    async fn select_parquet_over_max_bytes_is_400() {
        let (backend, b) = setup_name().await;
        // Any bytes: the size check runs before a body byte is read (the
        // key must be the `select` helper's "data.csv").
        put(&backend, &b, "data.csv", &vec![0; 256 * 1024 * 1024 + 1]).await;
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization = dto::InputSerialization {
            parquet: Some(dto::ParquetInput {}),
            ..Default::default()
        };
        let err = select(&backend, &b, req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
    }

    #[tokio::test]
    async fn select_ambiguous_field_is_in_stream_error() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"a,a\n1,2\n").await;
        let mut req = request("SELECT s.a FROM S3Object s");
        req.input_serialization.csv = Some(dto::CSVInput {
            file_header_info: Some(dto::FileHeaderInfo::from_static(dto::FileHeaderInfo::USE)),
            ..Default::default()
        });
        let err = stream_error(
            select(&backend, &b, req)
                .await
                .unwrap()
                .output
                .payload
                .unwrap(),
        )
        .await;
        assert_eq!(err.code(), &S3ErrorCode::AmbiguousFieldName, "{err:?}");
        assert!(err.message().unwrap().contains("a"), "{err:?}");
    }

    #[tokio::test]
    async fn select_missing_header_is_in_stream_error() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"a,b\n1,2\n").await;
        let mut req = request("SELECT s.c FROM S3Object s");
        req.input_serialization.csv = Some(dto::CSVInput {
            file_header_info: Some(dto::FileHeaderInfo::from_static(dto::FileHeaderInfo::USE)),
            ..Default::default()
        });
        let err = stream_error(
            select(&backend, &b, req)
                .await
                .unwrap()
                .output
                .payload
                .unwrap(),
        )
        .await;
        assert_eq!(err.code().as_str(), "MissingHeaderName", "{err:?}");
        assert!(err.message().unwrap().contains("c"), "{err:?}");
    }

    #[tokio::test]
    async fn select_input_record_over_1mb_is_s3_query_error() {
        let (backend, b) = setup_name().await;
        let mut data = vec![b'a'; 1024 * 1024];
        data.push(b'\n');
        put(&backend, &b, "data.csv", &data).await;
        let err = stream_error(
            select(&backend, &b, request("SELECT * FROM S3Object s"))
                .await
                .unwrap()
                .output
                .payload
                .unwrap(),
        )
        .await;
        assert_eq!(err.code().as_str(), "S3QueryError", "{err:?}");
        assert!(
            err.message().unwrap().contains("input record exceeds 1 MB"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn select_runtime_value_errors_are_s3_query_error() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"99999999999999999999999999999\n").await;
        let err = stream_error(
            select(&backend, &b, request("SELECT min(s._1) FROM S3Object s"))
                .await
                .unwrap()
                .output
                .payload
                .unwrap(),
        )
        .await;
        assert_eq!(err.code().as_str(), "S3QueryError", "{err:?}");
        assert!(
            err.message().unwrap().contains("28-digit precision"),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn select_concurrency_cap_waits_and_releases_at_stream_end() {
        let (backend, b) = setup_name().await;
        put(&backend, &b, "data.csv", b"1\n2\n3\n").await;
        // Hold four live responses (the permit rides the response stream).
        let mut held = Vec::new();
        for _ in 0..SELECT_CONCURRENCY {
            held.push(
                select(&backend, &b, request("SELECT * FROM S3Object s"))
                    .await
                    .unwrap(),
            );
        }
        // The fifth must wait for the cap.
        let fifth = select(&backend, &b, request("SELECT * FROM S3Object s"));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), fifth)
                .await
                .is_err(),
            "the fifth select must wait for the concurrency cap"
        );
        // Draining one response to its stream end releases its permit.
        let _ = collect_records(held.remove(0).output.payload.unwrap()).await;
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            select(&backend, &b, request("SELECT * FROM S3Object s")),
        )
        .await
        .expect("the released permit must admit the waiting select");
        assert!(resp.is_ok());
    }

    #[test]
    fn select_config_applies_aws_defaults_for_unset_dto_fields() {
        // The dto → SelectConfig builder's unwrap defaults (plan Global
        // Constraints): FieldDelimiter `,`, RecordDelimiter `\n`,
        // QuoteCharacter/QuoteEscapeCharacter `"`, Comments `#`,
        // FileHeaderInfo `NONE`, JSONType `LINES`; CSV output `,`, `\n`,
        // `"`, `"`, ASNEEDED; JSON output `\n`; no compression; no
        // ScanRange; no request progress; the default Cont policy; the
        // 256 MiB parquet bound.
        let (_, config, raw_scan) = build_config(&request("SELECT * FROM S3Object s")).unwrap();
        let InputFormat::Csv(csv) = config.input_format else {
            panic!("default input must be CSV");
        };
        assert_eq!(csv.field_delimiter, b',');
        assert_eq!(csv.record_delimiter, b'\n');
        assert_eq!(csv.quote, b'"');
        assert_eq!(csv.escape, b'"');
        assert_eq!(csv.comments, Some(b'#'));
        assert_eq!(csv.header, CsvHeader::None_);
        assert!(csv.allow_quoted_record_delimiter);
        let OutputMode::Csv(out) = config.output else {
            panic!("default output must be CSV");
        };
        assert_eq!(
            out,
            CsvOutputParams {
                quote_fields: QuoteFields::AsNeeded,
                ..Default::default()
            }
        );
        assert!(!config.request_progress);
        assert_eq!(config.cont, ContPolicy::default());
        assert_eq!(config.compression, Compression::None_);
        assert_eq!(config.scan_range, None);
        assert_eq!(config.size, None);
        assert_eq!(config.max_parquet_bytes, 256 * 1024 * 1024);
        assert_eq!(raw_scan, None);

        // JSON input defaults to LINES; JSON output to `\n`.
        let mut json = request("SELECT s.a FROM S3Object s");
        json.input_serialization = dto::InputSerialization {
            json: Some(dto::JSONInput::default()),
            ..Default::default()
        };
        json.output_serialization = dto::OutputSerialization {
            json: Some(dto::JSONOutput::default()),
            ..Default::default()
        };
        let (_, config, _) = build_config(&json).unwrap();
        assert_eq!(
            config.input_format,
            InputFormat::Json(JsonParams {
                ty: JsonType::Lines
            })
        );
        assert_eq!(
            config.output,
            OutputMode::Json(JsonOutputParams {
                record_delimiter: b'\n',
            })
        );
    }

    #[test]
    fn select_config_rejects_non_single_byte_characters() {
        // The single-character check is byte-based (the engine is `u8`-based)
        // — a 2-byte UTF-8 character is rejected even though it is one
        // character (no multi-byte delimiters, documented non-goal).
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.csv = Some(dto::CSVInput {
            field_delimiter: Some("é".into()),
            ..Default::default()
        });
        let err = build_config(&req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
    }
}
