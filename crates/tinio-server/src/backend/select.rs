#![cfg(feature = "select")]

//! SelectObjectContent of the mapping layer (spec 2026-09-04).
//!
//! The op validates the request (expression, serialization combinations,
//! single-byte delimiters, ScanRange, JSON-output alias rule), fetches the
//! object through the read path, and bridges the synchronous
//! [`select_iter`] onto s3s (spec §1.1): a forwarder
//! task pumps the storage body stream into a bounded channel, a pre-sized
//! rayon pool (16 MiB stacks — parquet schema recursion is unbounded
//! upstream, X3) drives the engine over a [`ChannelReader`], and the response
//! is a hand-rolled `futures::Stream` over a second channel (`Receiver::poll_recv`
//! — no `tokio-stream`) mapping [`SelectEvent`] → `dto` events and
//! [`Error`] → `S3Error` items. CPU-bound work never runs on a tokio
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
    io, panic,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use cfg_if::cfg_if;
use futures::{Stream, StreamExt};
use http::StatusCode;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto, s3_error};
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc},
    task,
};

use crate::{
    _core::storage::{GetObjectResult, Storage},
    _select::{
        Error, csv,
        events::{
            ByteCounters, ContPolicy, InputFormat, MAX_PARQUET_BYTES, ParquetParams, SelectConfig,
            SelectEvent, select_iter,
        },
        json,
        output::{CsvOutputParams, JsonOutputParams, OutputMode, QuoteFields},
        record::{Compression, ScanRange},
        sql,
    },
    backend::{S3Backend, map_backend_error},
};

/// Bound of both bridge channels (spec §1.1: capacity 4) — backpressure on
/// the storage read, bounded memory mid-pipeline.
const CHANNEL_CAP: usize = 4;

/// Stream-send deadline (X5): a client that stopped reading must not pin a
/// concurrency permit forever. After [`SEND_TIMEOUT`] of a full out channel
/// the engine gives up and unwinds — the drop unwinds the whole topology.
const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// `try_send` with a wall-clock deadline: a `Full` channel hands the item
/// back for the retry (never cloned); `false` = client gone (closed) or
/// deadline hit — the caller unwinds so the permit is released.
fn send_or_unwind(
    tx: &mpsc::Sender<Result<SelectEvent, Error>>,
    mut item: Result<SelectEvent, Error>,
) -> bool {
    let deadline = Instant::now() + SEND_TIMEOUT;
    loop {
        match tx.try_send(item) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(buf)) => {
                item = buf;
                if Instant::now() >= deadline {
                    return false;
                }
                thread::sleep(SEND_POLL);
            }
        }
    }
}

/// The retry interval of [`send_or_unwind`].
const SEND_POLL: Duration = Duration::from_millis(25);

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
    rx: StdMutex<mpsc::Receiver<Result<SelectEvent, Error>>>,
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
    // The three counters as the dto's `Option<i64>` fields — the
    // `Progress`/`Stats` detail structs carry the same trio.
    let count = |c: ByteCounters| {
        (
            Some(c.bytes_scanned as i64),
            Some(c.bytes_processed as i64),
            Some(c.bytes_returned as i64),
        )
    };
    match event {
        SelectEvent::Records(bytes) => dto::SelectObjectContentEvent::Records(dto::RecordsEvent {
            payload: Some(dto::Body::from(bytes)),
        }),
        SelectEvent::Progress(counters) => {
            dto::SelectObjectContentEvent::Progress(dto::ProgressEvent {
                details: Some({
                    let (bytes_scanned, bytes_processed, bytes_returned) = count(counters);
                    dto::Progress {
                        bytes_scanned,
                        bytes_processed,
                        bytes_returned,
                    }
                }),
            })
        }
        SelectEvent::Stats(counters) => dto::SelectObjectContentEvent::Stats(dto::StatsEvent {
            details: Some({
                let (bytes_scanned, bytes_processed, bytes_returned) = count(counters);
                dto::Stats {
                    bytes_scanned,
                    bytes_processed,
                    bytes_returned,
                }
            }),
        }),
        SelectEvent::Cont => dto::SelectObjectContentEvent::Cont(dto::ContinuationEvent {}),
        SelectEvent::End => dto::SelectObjectContentEvent::End(dto::EndEvent {}),
    }
}

/// [`Error`] → in-stream s3s error (design §3). These items sit inside
/// the 200 event stream — no status override (a `Custom` status only matters
/// for request-level errors, which must be set explicitly, below).
fn map_stream_error(e: Error) -> S3Error {
    match e {
        Error::Ambiguous(m) => S3Error::with_message(S3ErrorCode::AmbiguousFieldName, m),
        Error::MissingHeader(m) => {
            S3Error::with_message(S3ErrorCode::Custom("MissingHeaderName".into()), m)
        }
        // X9: `Io` details can embed on-disk paths — the wire sees a fixed
        // message, the detail is logged server-side.
        Error::Io(err) => {
            tracing::warn!("select stream io error: {err}");
            S3Error::with_message(
                S3ErrorCode::Custom("S3QueryError".into()),
                "S3 select: io error",
            )
        }
        e => S3Error::with_message(S3ErrorCode::Custom("S3QueryError".into()), e.to_string()),
    }
}

/// The request-level SQL error (design §3 row 1): `S3QueryParsingError` does
/// not exist as an s3s variant — a `Custom` whose default status is HTTP 500
/// (`Custom.status_code() == None` → 500 on serialize) — so every one of
/// these MUST set BAD_REQUEST explicitly (review 2026-09-05 #2/#11).
fn query_parsing_error(e: Error) -> S3Error {
    let message = match e {
        Error::Parse(m) | Error::Unsupported(m) => m,
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
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        // X6: the parse+validate is CPU-bound — a 256 KiB adversarial
        // expression (token walk, positions table) must not run unbounded
        // on a tokio worker; the §1.1 "CPU never on a worker" rule applies
        // here too.
        let (plan, mut config, raw_scan) =
            task::spawn_blocking(move || build_config(req.input.request))
                .await
                .map_err(|_| s3_error!(InternalError, "select request build task panicked"))??;

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

        // Admit only after every request-level check (review 2026-09-06b R9):
        // invalid / oversize requests must not hold slots. The permit rides
        // the response stream for the job's lifetime.
        let permit = Arc::clone(&self.select_semaphore)
            .acquire_owned()
            .await
            .map_err(|_| s3_error!(InternalError, "select semaphore closed"))?;

        // The bridge (spec §1.1): a forwarder feeds the body stream into a
        // bounded channel; the pre-sized select rayon pool runs the sync
        // engine over the ChannelReader; results flow out over a second
        // bounded channel.
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
        let (out_tx, out_rx) = mpsc::channel::<Result<SelectEvent, Error>>(CHANNEL_CAP);
        // X3: pool threads carry a 16 MiB stack — parquet footer schema
        // recursion (upstream parquet 59.3, unbounded) and our own
        // `arrow_json` can nest thousands of levels, and a stack overflow is
        // NOT a panic: `catch_unwind` cannot hold it and the process would
        // abort. Width matches the admission semaphore so jobs wait outside
        // the pool (R9) rather than stacking unbounded in rayon.
        self.select_pool.spawn(move || {
            let input = Box::new(ChannelReader {
                rx: in_rx,
                pending: None,
            });
            // Review 2026-09-06b R8: without this, a panic in the engine
            // ends the stream silently — no error item, no Stats/End.
            // Unwind-capture and surface it as an in-stream S3QueryError.
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                for item in select_iter(plan, config, input) {
                    if !send_or_unwind(&out_tx, item) {
                        // X5: client gone (closed channel) or stalled
                        // past the deadline — unwind the topology.
                        return;
                    }
                }
            }));
            if result.is_err() {
                let _ = send_or_unwind(
                    &out_tx,
                    Err(Error::Value("internal: select engine panicked".into())),
                );
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
/// and `config.scan_range` is left for the op.
fn build_config(
    request: dto::SelectObjectContentRequest,
) -> S3Result<(sql::QueryPlan, SelectConfig, Option<dto::ScanRange>)> {
    let dto::SelectObjectContentRequest {
        expression,
        expression_type,
        input_serialization,
        output_serialization,
        request_progress,
        scan_range,
    } = request;

    // ExpressionType: only SQL exists on the wire.
    require_expression_type_sql(&expression_type)?;

    // Exactly one input (CSV/JSON/Parquet) and one output (CSV/JSON).
    require_one_input(&input_serialization)?;
    require_one_output(&output_serialization)?;

    // Unset / wire `NONE` → no codec; unknown values refused (never silently
    // become none — that would transparently read compressed input as plain).
    let compression = parse_compression(&input_serialization.compression_type)?;

    // Parquet ⇒ feature gate (FR-021), no compression (engine reads raw),
    // no ScanRange (row-group positions are meaningless for it).
    if input_serialization.parquet.is_some() {
        // Compile-time stripped: with `select` but without `select-parquet`
        // the engine has no parquet reader — refuse request-level (501)
        // instead of failing in-stream after the 200.
        cfg_if! {
            if #[cfg(feature = "select-parquet")] {
                if compression.is_some() {
                    return Err(s3_error!(
                        InvalidRequestParameter,
                        "Parquet input must not be compressed"
                    ));
                }
                if scan_range.is_some() {
                    return Err(s3_error!(
                        InvalidRequestParameter,
                        "ScanRange is not supported for Parquet input"
                    ));
                }
            } else {
                return Err(s3_error!(
                    NotImplemented,
                    "Parquet input requires the select-parquet feature"
                ));
            }
        }
    }

    // ScanRange ⇒ no compression (AWS semantics for the combination are
    // ambiguous; refuse rather than guess).
    if scan_range.is_some() && compression.is_some() {
        return Err(s3_error!(
            InvalidRequestParameter,
            "ScanRange does not support compressed input"
        ));
    }

    // The expression parse + grammar gate runs here, request-level — the
    // engine receives the pre-built plan (rejected constructs → §3 row 1).
    let plan = sql::parse(&expression).map_err(query_parsing_error)?;

    let input_format = if let Some(csv) = input_serialization.csv {
        InputFormat::Csv(csv_input_params(csv)?)
    } else if let Some(json) = input_serialization.json.as_ref() {
        InputFormat::Json(json_input_params(json)?)
    } else {
        // Parquet: project down to the columns the plan references.
        InputFormat::Parquet(ParquetParams {
            projection: sql::referenced_columns(&plan),
        })
    };
    let output = if let Some(csv) = output_serialization.csv {
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
            output_serialization.json.as_ref().expect("checked above"),
        )?)
    };

    Ok((
        plan,
        SelectConfig {
            input_format,
            output,
            compression,
            // Resolved by the op against the fetched object size.
            scan_range: None,
            request_progress: request_progress.and_then(|p| p.enabled).unwrap_or(false),
            cont: ContPolicy::default(),
            max_parquet_bytes: MAX_PARQUET_BYTES,
        },
        scan_range,
    ))
}

/// ExpressionType: only SQL exists on the wire.
fn require_expression_type_sql(ty: &dto::ExpressionType) -> S3Result<()> {
    if ty.as_str() != dto::ExpressionType::SQL {
        return Err(s3_error!(
            InvalidRequestParameter,
            "ExpressionType must be SQL"
        ));
    }
    Ok(())
}

/// Exactly one of CSV / JSON / Parquet in `InputSerialization`.
fn require_one_input(input: &dto::InputSerialization) -> S3Result<()> {
    let n = [
        input.csv.is_some(),
        input.json.is_some(),
        input.parquet.is_some(),
    ]
    .into_iter()
    .filter(|&present| present)
    .count();
    if n != 1 {
        return Err(s3_error!(
            InvalidRequestParameter,
            "exactly one of CSV, JSON, or Parquet input serialization must be specified"
        ));
    }
    Ok(())
}

/// Exactly one of CSV / JSON in `OutputSerialization`.
fn require_one_output(output: &dto::OutputSerialization) -> S3Result<()> {
    if output.csv.is_some() == output.json.is_some() {
        return Err(s3_error!(
            InvalidRequestParameter,
            "exactly one of CSV or JSON output serialization must be specified"
        ));
    }
    Ok(())
}

/// Unset / wire `NONE` → `None`; known codec → `Some`; unknown → 400.
fn parse_compression(value: &Option<dto::CompressionType>) -> S3Result<Option<Compression>> {
    Ok(value
        .as_ref()
        .map(|c| {
            Compression::from_wire(c.as_str()).map_err(|_| {
                s3_error!(
                    InvalidRequestParameter,
                    "unknown CompressionType: {}",
                    c.as_str()
                )
            })
        })
        .transpose()?
        .flatten())
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

/// Quote/escape characters equal to a delimiter are ambiguous (X10): the
/// `csv` crate would silently tolerate the clash — refused request-level,
/// dotted names per parameter so the message names the conflict.
fn validate_quote_escape(
    field_delimiter: &u8,
    record_delimiter: &u8,
    quote: &u8,
    escape: &u8,
) -> S3Result<()> {
    for (name, ch) in [
        ("QuoteCharacter", *quote),
        ("QuoteEscapeCharacter", *escape),
    ] {
        if ch == *field_delimiter {
            return Err(s3_error!(
                InvalidRequestParameter,
                "{name} must differ from FieldDelimiter"
            ));
        }
        if ch == *record_delimiter {
            return Err(s3_error!(
                InvalidRequestParameter,
                "{name} must differ from RecordDelimiter"
            ));
        }
    }
    Ok(())
}

/// The comment character: AWS default `#` (the engine's own default —
/// [`SelectConfig`] — is None, the crate's raw-pipeline pick; the wire
/// default is what the op applies). The byte check is `single_char`'s;
/// `None` takes the default. (review 2026-09-06 simplify: one validator.)
fn single_char_opt(value: &Option<String>, default: u8, name: &str) -> S3Result<Option<u8>> {
    Ok(Some(single_char(value, default, name)?))
}

/// dto `CSVInput` → engine params (unset fields take the AWS documented
/// defaults: `,`, `\n`, `"`, `"`, `#`, `NONE`).
fn csv_input_params(
    dto::CSVInput {
        file_header_info,
        field_delimiter,
        record_delimiter,
        quote_character,
        quote_escape_character,
        comments,
        allow_quoted_record_delimiter,
    }: dto::CSVInput,
) -> S3Result<csv::Params> {
    let header = match file_header_info.as_ref() {
        None => None,
        // Wire `NONE` is absence — not a header-mode variant.
        Some(i) if i.as_str() == dto::FileHeaderInfo::NONE => None,
        // The wire enum is closed (AWS): an unrecognized value is refused,
        // never defaulted.
        Some(i) => Some(i.as_str().parse().map_err(|_| {
            s3_error!(
                InvalidRequestParameter,
                "unknown FileHeaderInfo: {}",
                i.as_str()
            )
        })?),
    };
    let field_delimiter = single_char(&field_delimiter, b',', "FieldDelimiter")?;
    let record_delimiter = single_char(&record_delimiter, b'\n', "RecordDelimiter")?;
    let quote = single_char(&quote_character, b'"', "QuoteCharacter")?;
    let escape = single_char(&quote_escape_character, b'"', "QuoteEscapeCharacter")?;
    validate_quote_escape(&field_delimiter, &record_delimiter, &quote, &escape)?;
    Ok(csv::Params {
        field_delimiter,
        record_delimiter,
        quote,
        escape,
        comments: single_char_opt(&comments, b'#', "Comments")?,
        header,
        // Known deviation (#7): the flag is not enforced by the engine —
        // permissive (AWS true) behavior applies either way; documented and
        // pinned by the tinio-select tests, never silently ignored.
        allow_quoted_record_delimiter: allow_quoted_record_delimiter.unwrap_or(true),
    })
}

/// dto `JSONInput` → engine params (unset type = `LINES`).
fn json_input_params(input: &dto::JSONInput) -> S3Result<json::Params> {
    let ty = match input.type_.as_ref().map(|t| t.as_str()) {
        None => json::Type::Lines,
        Some(dto::JSONType::DOCUMENT) => json::Type::Document,
        Some(dto::JSONType::LINES) => json::Type::Lines,
        Some(other) => {
            return Err(s3_error!(
                InvalidRequestParameter,
                "unknown JSONType: {other}"
            ));
        }
    };
    Ok(json::Params { ty })
}

/// dto `CSVOutput` → engine params (unset fields take the AWS documented
/// defaults: `,`, `\n`, `"`, `"`, `ASNEEDED`).
fn csv_output_params(
    dto::CSVOutput {
        field_delimiter,
        record_delimiter,
        quote_character,
        quote_escape_character,
        quote_fields,
    }: dto::CSVOutput,
) -> S3Result<CsvOutputParams> {
    let field_delimiter = single_char(&field_delimiter, b',', "FieldDelimiter")?;
    let record_delimiter = single_char(&record_delimiter, b'\n', "RecordDelimiter")?;
    let quote = single_char(&quote_character, b'"', "QuoteCharacter")?;
    let escape = single_char(&quote_escape_character, b'"', "QuoteEscapeCharacter")?;
    validate_quote_escape(&field_delimiter, &record_delimiter, &quote, &escape)?;
    Ok(CsvOutputParams {
        field_delimiter,
        record_delimiter,
        quote,
        escape,
        quote_fields: match quote_fields.as_ref().map(|q| q.as_str()) {
            None => QuoteFields::AsNeeded,
            Some(dto::QuoteFields::ALWAYS) => QuoteFields::Always,
            Some(dto::QuoteFields::ASNEEDED) => QuoteFields::AsNeeded,
            Some(other) => {
                return Err(s3_error!(
                    InvalidRequestParameter,
                    "unknown QuoteFields: {other}"
                ));
            }
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
fn resolve_scan_range(scan: &dto::ScanRange, size: u64) -> S3Result<ScanRange> {
    match (scan.start, scan.end) {
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
                Ok(ScanRange {
                    start: start as u64,
                    end: None,
                })
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
                Ok(ScanRange {
                    start: size - end as u64,
                    end: Some(size - 1),
                })
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
                Ok(ScanRange {
                    start: start as u64,
                    end: Some(end as u64),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, time::Duration};

    use s3s::S3;
    use tokio::time::timeout;

    use super::*;
    use crate::{
        _core::{bucket, object, storage::ObjectOps},
        _mem::MemoryStorage,
        _select::{events::ContPolicy, output::CsvOutputParams},
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

    fn object_key(key: &str) -> object::Key {
        object::key(key).unwrap()
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
    #[cfg(feature = "select-parquet")]
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
    #[cfg(feature = "select-parquet")]
    async fn select_parquet_over_max_bytes_is_400() {
        let (backend, b) = setup_name().await;
        // Any bytes: the size check runs before a body byte is read (the
        // key must be the `select` helper's "data.csv"). Only reachable with
        // the parquet feature — without it the request-level feature gate
        // (501) fires before the fetch.
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

    /// The committed fixture `tinio-select`'s integration test reads and
    /// asserts (`crates/tinio-select/tests/parquet.rs`; regenerate with its
    /// ignored generator). Both layers embed the same file, so they cannot
    /// drift onto different fixtures.
    #[cfg(feature = "select-parquet")]
    const PARQUET_FIXTURE: &[u8] =
        include_bytes!("../../../tinio-select/tests/fixtures/select.parquet");

    /// The parquet read path end to end: a real parquet object through the
    /// storage read path, the engine, and the CSV output. The three parquet
    /// tests beside this one pin only the request-level constraints — none
    /// of them ever lands a parquet body in the reader.
    #[tokio::test]
    #[cfg(feature = "select-parquet")]
    async fn select_parquet_object_projects_and_filters() {
        let (backend, b) = setup_name().await;
        // The `select` helper's key is fixed ("data.csv"): the
        // `InputSerialization`, not the suffix, picks the reader.
        put(&backend, &b, "data.csv", PARQUET_FIXTURE).await;
        let parquet = |expression: &str| {
            let mut req = request(expression);
            req.input_serialization = dto::InputSerialization {
                parquet: Some(dto::ParquetInput {}),
                ..Default::default()
            };
            req
        };
        // Every column of every row: the file's schema order, the float's
        // raw carrier text, and the present null as an empty CSV cell.
        let resp = select(&backend, &b, parquet("SELECT * FROM S3Object s"))
            .await
            .unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"1,alice,3.5,true,12.34\n2,bob,,false,5.67\n3,carol,1.25,true,9.99\n"
        );
        // A projection pruned to two columns and filtered on a third (a
        // column read for the predicate alone still resolves).
        let resp = select(
            &backend,
            &b,
            parquet("SELECT s.name, s.score FROM S3Object s WHERE s.id >= 2"),
        )
        .await
        .unwrap();
        assert_eq!(
            collect_records(resp.output.payload.unwrap()).await,
            b"bob,\ncarol,1.25\n"
        );
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
        let cap = backend.capabilities().select_concurrency as usize;
        // Hold `cap` live responses (the permit rides the response stream).
        let mut held = Vec::new();
        for _ in 0..cap {
            held.push(
                select(&backend, &b, request("SELECT * FROM S3Object s"))
                    .await
                    .unwrap(),
            );
        }
        // The next must wait for the cap.
        let waiting = select(&backend, &b, request("SELECT * FROM S3Object s"));
        assert!(
            timeout(Duration::from_millis(200), waiting).await.is_err(),
            "the next select must wait for the concurrency cap"
        );
        // Draining one response to its stream end releases its permit.
        let _ = collect_records(held.remove(0).output.payload.unwrap()).await;
        let resp = timeout(
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
        let (_, config, raw_scan) = build_config(request("SELECT * FROM S3Object s")).unwrap();
        let InputFormat::Csv(csv) = config.input_format else {
            panic!("default input must be CSV");
        };
        assert_eq!(csv.field_delimiter, b',');
        assert_eq!(csv.record_delimiter, b'\n');
        assert_eq!(csv.quote, b'"');
        assert_eq!(csv.escape, b'"');
        assert_eq!(csv.comments, Some(b'#'));
        assert_eq!(csv.header, None);
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
        assert_eq!(config.compression, None);
        assert_eq!(config.scan_range, None);
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
        let (_, config, _) = build_config(json).unwrap();
        assert_eq!(
            config.input_format,
            InputFormat::Json(json::Params {
                ty: json::Type::Lines
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
    fn select_config_maps_gzip_and_bzip2_compression() {
        // The dto compression type lands in the config as the engine's
        // enum (NONE stays None, the Option carries it).
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.compression_type = Some(dto::CompressionType::from_static(
            dto::CompressionType::GZIP,
        ));
        let (_, config, _) = build_config(req.clone()).unwrap();
        assert_eq!(config.compression, Some(Compression::Gzip));

        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.compression_type = Some(dto::CompressionType::from_static(
            dto::CompressionType::BZIP2,
        ));
        let (_, config, _) = build_config(req).unwrap();
        assert_eq!(config.compression, Some(Compression::Bzip2));
    }

    #[test]
    fn stream_io_error_is_fixed_message() {
        // X9: Io errors can embed on-disk paths — the wire sees a fixed
        // message, the detail is logged server-side.
        let err = map_stream_error(Error::Io(
            io::Error::new(ErrorKind::PermissionDenied, "/srv/secret/object.csv").into(),
        ));
        assert_eq!(err.code().as_str(), "S3QueryError", "{err:?}");
        assert_eq!(err.message().unwrap(), "S3 select: io error", "{err:?}");
    }

    #[test]
    fn send_or_unwind_closed_channel_returns_false() {
        // X5: a dropped receiver (client gone) is detected by try_send —
        // no retry loop, immediate unwind.
        let (tx, rx) = mpsc::channel::<Result<SelectEvent, Error>>(CHANNEL_CAP);
        drop(rx);
        assert!(!send_or_unwind(&tx, Err(Error::Value("boom".into()))));
    }

    #[test]
    fn select_config_rejects_quote_escape_delimiter_conflicts() {
        // X10: a quote/escape character equal to a delimiter is ambiguous —
        // the csv crate would tolerate it silently; refused request-level.
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.csv = Some(dto::CSVInput {
            quote_character: Some(",".into()),
            ..Default::default()
        });
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert!(err.message().unwrap().contains("QuoteCharacter"), "{err:?}");

        let mut req = request("SELECT * FROM S3Object s");
        req.output_serialization = dto::OutputSerialization {
            csv: Some(dto::CSVOutput {
                quote_escape_character: Some("\n".into()),
                ..Default::default()
            }),
            json: None,
        };
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert!(
            err.message().unwrap().contains("QuoteEscapeCharacter"),
            "{err:?}"
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
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
    }

    #[test]
    fn select_config_rejects_unknown_wire_enum_values() {
        // The wire enums are closed (AWS): an unknown value is refused, never
        // silently defaulted (unknown compression must not read compressed
        // input as plain bytes; unknown header/json/quote values must not
        // silently pick the default mode).
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.compression_type = Some(dto::CompressionType::from_static("LZO"));
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 400, "{err:?}");
        assert!(
            err.message().unwrap().contains("CompressionType"),
            "{err:?}"
        );

        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization.csv = Some(dto::CSVInput {
            file_header_info: Some(dto::FileHeaderInfo::from_static("WEIRD")),
            ..Default::default()
        });
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert!(err.message().unwrap().contains("FileHeaderInfo"), "{err:?}");

        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization = dto::InputSerialization {
            json: Some(dto::JSONInput {
                type_: Some(dto::JSONType::from_static("WILD")),
            }),
            ..Default::default()
        };
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert!(err.message().unwrap().contains("JSONType"), "{err:?}");

        let mut req = request("SELECT * FROM S3Object s");
        req.output_serialization = dto::OutputSerialization {
            csv: Some(dto::CSVOutput {
                quote_fields: Some(dto::QuoteFields::from_static("WILD")),
                ..Default::default()
            }),
            json: None,
        };
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequestParameter", "{err:?}");
        assert!(err.message().unwrap().contains("QuoteFields"), "{err:?}");
    }

    #[test]
    #[cfg(not(feature = "select-parquet"))]
    fn select_config_rejects_parquet_without_parquet_feature() {
        // Compile-time strip (FR-021): on a `select`-only build a parquet
        // request is a request-level 501 — never an in-stream failure after
        // the 200 has begun (the engine has no parquet reader).
        let mut req = request("SELECT * FROM S3Object s");
        req.input_serialization = dto::InputSerialization {
            parquet: Some(dto::ParquetInput {}),
            ..Default::default()
        };
        let err = build_config(req).unwrap_err();
        assert_eq!(err.code().as_str(), "NotImplemented", "{err:?}");
        assert_eq!(err.status_code().unwrap().as_u16(), 501, "{err:?}");
        assert!(err.message().unwrap().contains("select-parquet"), "{err:?}");
    }
}
