//! Framing filters: `Read` adapters between serde_json and the input stream.
//! They reshape the byte stream so each serde pull parses exactly one record
//! root, enforce the 1 MB input-record cap mid-read, and account offsets in
//! uncompressed bytes.

use std::{
    cell::Cell,
    io::{self, BufRead, BufReader, Read},
    rc::Rc,
};

use crate::record::{CAP_MESSAGE, MAX_RECORD};

/// JSON whitespace per RFC 8259.
pub(crate) fn ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Per-root-pull budget: the handler resets it before a root pull, so every
/// byte serde is served during that pull is charged — matched, skipped and
/// injected-delimiter alike (bytes the framing consumes internally —
/// top-level `[` `]` `,`, whitespace skipped by `skip_ws`, and the LINES `\n`
/// — never reach this layer). One pull is exactly one record root, so the cap
/// this enforces is "one root ≤ `MAX_RECORD`" (an array element in DOCUMENT
/// array mode, the whole document in single mode). The handler maps `fired`
/// to `Error::Format(CAP_MESSAGE)`.
#[derive(Default)]
pub(crate) struct Budget {
    remaining: Cell<u64>,
    fired: Cell<bool>,
}

impl Budget {
    /// Start a root pull from a full budget. `BudgetedRead` — the only thing
    /// that charges — is built inside the pull and dropped with it, so
    /// `charge` is never reached between pulls.
    pub(crate) fn reset(&self) {
        self.fired.set(false);
        self.remaining.set(MAX_RECORD);
    }

    pub(crate) fn fired(&self) -> bool {
        self.fired.get()
    }

    fn charge(&self, n: usize) -> io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let r = self.remaining.get();
        if n as u64 > r {
            self.remaining.set(0);
            self.fired.set(true);
            return Err(io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE));
        }
        self.remaining.set(r - n as u64);
        Ok(())
    }
}

/// `Read` wrapper charging every served byte to the shared `Budget` — reset
/// by the handler around a whole root pull, so the charge covers everything
/// serde is served: matched values, skipped subtrees and the injected
/// delimiter alike. Bytes the framing consumes internally (top-level
/// `[` `]` `,`, whitespace skipped by `skip_ws`, the LINES `\n`) never pass
/// through here; they are bounded by the framing's own span accounting
/// instead.
pub(crate) struct BudgetedRead<R> {
    inner: R,
    budget: Rc<Budget>,
}

impl<R: Read> BudgetedRead<R> {
    pub(crate) fn new(inner: R, budget: Rc<Budget>) -> Self {
        Self { inner, budget }
    }
}

impl<R: Read> Read for BudgetedRead<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(out)?;
        self.budget.charge(n)?;
        Ok(n)
    }
}

/// What `start_line` found at the stream position.
pub(crate) enum LineStart {
    /// A line exists (blank included); carries its start offset.
    Line(u64),
    /// Clean end of input: no more lines.
    Eof,
}

/// Serves one LINES line at a time: the line's content bytes (without the
/// `\n`), then EOF until `start_line` advances. The span (content +
/// terminator) is counted while serving and errors mid-serve past
/// `MAX_RECORD` — a line is never buffered whole.
pub(crate) struct LineFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    span: u64,
    eol: bool,
    pub(crate) capped: bool,
}

impl<R: Read> LineFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self {
            inner,
            offset: 0,
            span: 0,
            eol: true,
            capped: false,
        }
    }

    pub(crate) fn start_line(&mut self) -> io::Result<LineStart> {
        debug_assert!(self.eol, "start_line before the previous line drained");
        if self.inner.fill_buf()?.is_empty() {
            return Ok(LineStart::Eof);
        }
        self.eol = false;
        self.span = 0;
        Ok(LineStart::Line(self.offset))
    }

    fn cap(&mut self) -> io::Error {
        self.capped = true;
        io::Error::new(io::ErrorKind::InvalidData, CAP_MESSAGE)
    }
}

impl<R: Read> Read for LineFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.eol || out.is_empty() {
            return Ok(0);
        }
        let avail = self.inner.fill_buf()?;
        if avail.is_empty() {
            // Unterminated final line: EOF ends its content.
            self.eol = true;
            return Ok(0);
        }
        // Only the bytes this call can serve are worth scanning: serde's
        // `IoRead` reads one byte at a time, so scanning all of `avail` here
        // would make serving a line O(L × min(L, BufReader capacity)). One
        // byte past `out` suffices — a `\n` at exactly `out.len()` still ends
        // the line, and is consumed below without being copied.
        let w = avail.len().min(out.len() + 1);
        let nl = avail[..w].iter().position(|&b| b == b'\n');
        let content = nl.unwrap_or(avail.len());
        let n = content.min(out.len());
        out[..n].copy_from_slice(&avail[..n]);
        self.inner.consume(n);
        self.offset += n as u64;
        self.span += n as u64;
        if n == content && nl.is_some() {
            self.inner.consume(1);
            self.offset += 1;
            self.span += 1;
            self.eol = true;
        }
        if self.span > MAX_RECORD {
            return Err(self.cap());
        }
        Ok(n)
    }
}

/// The document's root shape, resolved lazily on the first read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DocMode {
    Unknown,
    /// Zero bytes of input.
    Empty,
    /// Only whitespace before EOF.
    WsOnly,
    /// Root is an array: elements are served one per pull.
    Array,
    /// Root is any other value: served verbatim, one pull.
    Single,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Start,
    ExpectElement { after_comma: bool },
    ServingElement,
    InjectSpace,
    ExpectSep,
    AfterRoot,
    ServeSingle,
    Done,
}

/// Reshapes a DOCUMENT stream so each serde pull parses exactly one root
/// value. Array mode swallows (and validates) the top-level `[` `]` `,`,
/// injects one virtual space at each element end (a clean terminator for
/// number tokens), and records each element's first-byte offset. Single
/// mode passes the stream through verbatim.
pub(crate) struct DocFraming<R: Read> {
    inner: BufReader<R>,
    offset: u64,
    state: State,
    in_string: bool,
    escape: bool,
    depth: u64,
    pub(crate) mode: DocMode,
    pub(crate) element_start: u64,
    pub(crate) framing_msg: Option<String>,
}

impl<R: Read> DocFraming<R> {
    pub(crate) fn new(inner: BufReader<R>) -> Self {
        Self {
            inner,
            offset: 0,
            state: State::Start,
            in_string: false,
            escape: false,
            depth: 0,
            mode: DocMode::Unknown,
            element_start: 0,
            framing_msg: None,
        }
    }

    fn peek(&mut self) -> io::Result<Option<u8>> {
        Ok(self.inner.fill_buf()?.first().copied())
    }

    fn consume(&mut self, n: usize) {
        self.inner.consume(n);
        self.offset += n as u64;
    }

    fn skip_ws(&mut self) -> io::Result<()> {
        loop {
            // Borrow scope: `avail` borrows `self.inner`, so only scalars may
            // cross out of it — `self.consume` needs `&mut self`.
            let (n, done) = {
                let avail = self.inner.fill_buf()?;
                let n = avail.iter().position(|&b| !ws(b)).unwrap_or(avail.len());
                (n, avail.is_empty() || n < avail.len())
            };
            self.consume(n);
            if done {
                return Ok(());
            }
        }
    }

    fn framing_err(&mut self, msg: &str) -> io::Error {
        self.framing_msg = Some(msg.to_string());
        io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
    }

    /// Resolve the root shape and cache it without consuming the first
    /// significant byte. The handler asks *before* the pull — whether the
    /// trailing probe must run depends on the mode, and once the
    /// `Deserializer` exists it holds `&mut self.framing`, so the mode can no
    /// longer be read. Idempotent: the `Start` arm calls it too, so callers
    /// that never ask still get the mode resolved.
    pub(crate) fn peek_mode(&mut self) -> io::Result<DocMode> {
        if self.mode != DocMode::Unknown {
            return Ok(self.mode);
        }
        self.skip_ws()?;
        self.mode = match self.peek()? {
            None if self.offset == 0 => DocMode::Empty,
            None => DocMode::WsOnly,
            Some(b'[') => DocMode::Array,
            Some(_) => DocMode::Single,
        };
        Ok(self.mode)
    }

    /// Whether the root array has closed cleanly at the top level (the `]` was
    /// consumed). The handler needs this to tell `[]` — the one MISSING row of
    /// the `S3Object[*]` zero-match rule — from an EOF that struck *inside* a
    /// value, which is a malformed document and must error like today's
    /// whole-document `from_slice` did.
    pub(crate) fn closed(&self) -> bool {
        matches!(self.state, State::AfterRoot | State::Done)
    }

    /// Advance the machine after `n` served bytes have been copied out. The
    /// copy itself lives in the caller, inside the borrow scope of `avail`:
    /// `avail` borrows `self.inner`, so no `&mut self` method is callable
    /// while it is live (rustc 1.98.1 reports E0499 for the `serve(out, avail,
    /// …)` shape this replaces).
    fn advance(&mut self, n: usize, next: State) -> usize {
        self.consume(n);
        self.state = next;
        n
    }
}

impl<R: Read> Read for DocFraming<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::InjectSpace => {
                    out[0] = b' ';
                    self.state = State::ExpectSep;
                    return Ok(1);
                }
                State::Start => match self.peek_mode()? {
                    DocMode::Array => {
                        self.consume(1);
                        self.state = State::ExpectElement { after_comma: false };
                    }
                    DocMode::Single => {
                        self.element_start = self.offset;
                        self.state = State::ServeSingle;
                    }
                    // Empty / WsOnly end the stream; `Unknown` cannot occur
                    // (peek_mode always resolves it).
                    _ => self.state = State::Done,
                },
                State::ExpectElement { after_comma } => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(
                                self.framing_err("unexpected end of input inside the root array")
                            );
                        }
                        Some(b']') if !after_comma => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(b']') => {
                            return Err(self.framing_err("trailing comma in the root array"));
                        }
                        Some(b',') => return Err(self.framing_err("expected an array element")),
                        Some(_) => {
                            self.element_start = self.offset;
                            self.depth = 0;
                            self.state = State::ServingElement;
                        }
                    }
                }
                State::ServingElement => {
                    // Scan and copy live in one borrow scope: `avail` borrows
                    // `self.inner`, and only scalars may cross out of it so the
                    // `&mut self` advance stays callable (E0499 otherwise).
                    // Field writes (`depth`/`in_string`/`escape`/`state`) are
                    // disjoint from `self.inner` and therefore fine in scope.
                    let (n, next) = {
                        let avail = self.inner.fill_buf()?;
                        if avail.is_empty() {
                            // EOF: depth > 0 → serde reports the truncated
                            // value; depth == 0 → the scalar is complete, so
                            // serve the injected virtual terminator first: a
                            // bare `Ok(0)` here makes serde's number scanner
                            // peek once more, reach ExpectSep and lose the
                            // completed element. ExpectSep then flags the
                            // unterminated array on the next pull.
                            if self.depth == 0 {
                                self.state = State::InjectSpace;
                                continue;
                            }
                            return Ok(0);
                        }
                        let mut n = 0;
                        let mut next = State::ServingElement;
                        while n < avail.len() && n < out.len() {
                            let b = avail[n];
                            if self.in_string {
                                if self.escape {
                                    self.escape = false;
                                } else if b == b'\\' {
                                    self.escape = true;
                                } else if b == b'"' {
                                    self.in_string = false;
                                }
                                n += 1;
                                continue;
                            }
                            match b {
                                b'"' => {
                                    self.in_string = true;
                                    n += 1;
                                }
                                b'{' | b'[' => {
                                    self.depth += 1;
                                    n += 1;
                                }
                                b'}' | b']' if self.depth > 0 => {
                                    self.depth -= 1;
                                    n += 1;
                                    if self.depth == 0 {
                                        next = State::InjectSpace;
                                        break;
                                    }
                                }
                                b',' | b']' | b'}' | b' ' | b'\t' | b'\n' | b'\r'
                                    if self.depth == 0 =>
                                {
                                    // A scalar element ends BEFORE this byte
                                    // (delimiter left in the stream for
                                    // ExpectSep). n == 0 only for a malformed
                                    // empty element (`[}]`): serving nothing
                                    // lets serde report the truncated value —
                                    // no debug_assert: it would panic debug
                                    // builds on untrusted input.
                                    next = State::InjectSpace;
                                    break;
                                }
                                _ => n += 1,
                            }
                        }
                        out[..n].copy_from_slice(&avail[..n]);
                        (n, next)
                    };
                    // `de::IoRead` pulls one byte per `read`, so an element's
                    // final byte and its terminator arrive in different calls:
                    // here `n == 0` means only the injected terminator is
                    // pending. Serve it rather than returning `Ok(0)`, which
                    // serde would read as EOF and report as a truncated value.
                    if n == 0 {
                        self.state = State::InjectSpace;
                        continue;
                    }
                    return Ok(self.advance(n, next));
                }
                State::ExpectSep => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => {
                            return Err(
                                self.framing_err("unexpected end of input inside the root array")
                            );
                        }
                        Some(b',') => {
                            self.consume(1);
                            self.state = State::ExpectElement { after_comma: true };
                        }
                        Some(b']') => {
                            self.consume(1);
                            self.state = State::AfterRoot;
                        }
                        Some(_) => {
                            return Err(self.framing_err("expected `,` or `]` in the root array"));
                        }
                    }
                }
                State::AfterRoot => {
                    self.skip_ws()?;
                    match self.peek()? {
                        None => self.state = State::Done,
                        Some(_) => {
                            return Err(
                                self.framing_err("trailing characters after the JSON document")
                            );
                        }
                    }
                }
                State::ServeSingle => {
                    let n = self.inner.read(out)?;
                    self.offset += n as u64;
                    if n == 0 {
                        self.state = State::Done;
                    }
                    return Ok(n);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, rc::Rc};

    use serde::Deserialize;

    use super::*;

    #[test]
    fn budgeted_read_fires_mid_stream() {
        let budget = Rc::new(Budget::default());
        budget.reset();
        let mut r = BudgetedRead::new(
            Cursor::new(vec![b'a'; (MAX_RECORD + 1) as usize]),
            budget.clone(),
        );
        let mut sink = Vec::new();
        let err = r.read_to_end(&mut sink).unwrap_err();
        assert_eq!(err.to_string(), CAP_MESSAGE);
        assert!(budget.fired());
    }

    #[test]
    fn budgeted_read_exact_cap_allowed() {
        let budget = Rc::new(Budget::default());
        let mut r = BudgetedRead::new(Cursor::new(vec![b'a'; MAX_RECORD as usize]), budget.clone());
        budget.reset();
        let mut sink = Vec::new();
        r.read_to_end(&mut sink).unwrap();
        assert_eq!(sink.len() as u64, MAX_RECORD);
        assert!(!budget.fired());
    }

    fn framing(bytes: &[u8]) -> LineFraming<Cursor<Vec<u8>>> {
        LineFraming::new(BufReader::new(Cursor::new(bytes.to_vec())))
    }

    #[test]
    fn lines_serve_content_then_eof_until_next_line() {
        let mut f = framing(b"{\"a\":1}\n{\"b\":2}\n");
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap(); // the line's end reads as EOF
        assert_eq!(s, "{\"a\":1}");
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(8)));
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        assert_eq!(s, "{\"b\":2}");
        assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
    }

    #[test]
    fn lines_blank_line_serves_immediate_eof_but_exists() {
        let mut f = framing(b"\n");
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
        let mut buf = [0u8; 4];
        assert_eq!(f.read(&mut buf).unwrap(), 0); // blank line = exists, no content
        assert!(matches!(f.start_line().unwrap(), LineStart::Eof));
    }

    #[test]
    fn lines_cap_fires_mid_serve_and_exact_span_allowed() {
        let mut f = framing(&vec![b'a'; (MAX_RECORD + 1) as usize]); // no terminator
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
        let mut sink = Vec::new();
        assert_eq!(
            f.read_to_end(&mut sink).unwrap_err().to_string(),
            CAP_MESSAGE
        );
        assert!(f.capped);

        let mut f = framing(&{
            let mut v = vec![b'a'; (MAX_RECORD - 1) as usize];
            v.push(b'\n');
            v
        });
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
        let mut sink = Vec::new();
        f.read_to_end(&mut sink).unwrap();
        assert_eq!(sink.len() as u64, MAX_RECORD - 1);
        assert!(!f.capped);
    }

    /// Every root value a DOCUMENT pull serves from `bytes`, with each value's
    /// first-byte offset and the mode the framing resolved.
    fn pull_values(bytes: &str) -> (Vec<serde_json::Value>, Vec<u64>, DocMode) {
        let mut f = DocFraming::new(BufReader::new(Cursor::new(bytes.as_bytes().to_vec())));
        let mut vals = Vec::new();
        let mut starts = Vec::new();
        loop {
            let r = {
                let mut de = serde_json::Deserializer::from_reader(&mut f);
                serde_json::Value::deserialize(&mut de)
            };
            match r {
                Ok(v) => {
                    vals.push(v);
                    starts.push(f.element_start);
                }
                Err(e) if e.is_eof() => break,
                Err(e) => panic!("{bytes}: {e}"),
            }
            if f.mode == DocMode::Single {
                break;
            }
        }
        (vals, starts, f.mode)
    }

    #[test]
    fn doc_array_elements_pull_one_at_a_time_with_offsets() {
        let (vals, starts, mode) = pull_values("[{\"id\": 1}, {\"id\": 2}]");
        assert_eq!(
            vals,
            vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})]
        );
        assert_eq!(mode, DocMode::Array);
        assert_eq!(starts[0], 1);
        assert!(starts[1] > starts[0]);
    }

    #[test]
    fn doc_scalar_and_nested_elements() {
        let (vals, ..) = pull_values("[1, \"two\", true, null, [1, 2], {\"a\": \"x]y\"}]");
        assert_eq!(
            vals,
            vec![
                serde_json::json!(1),
                serde_json::json!("two"),
                serde_json::json!(true),
                serde_json::Value::Null,
                serde_json::json!([1, 2]),
                serde_json::json!({"a": "x]y"}),
            ]
        );
    }

    #[test]
    fn doc_single_object_pulls_once() {
        let (vals, starts, mode) = pull_values("{\n \"a\": 1\n}");
        assert_eq!(vals, vec![serde_json::json!({"a": 1})]);
        assert_eq!(mode, DocMode::Single);
        assert_eq!(starts, vec![0]);
    }

    #[test]
    fn doc_separator_errors_are_framing_errors() {
        for (input, msg) in [
            ("[1,]", "trailing comma"),
            ("[1 2]", "expected `,` or `]`"),
            ("[1", "unexpected end of input"),
            ("[1] x", "trailing characters"),
        ] {
            let mut f = DocFraming::new(BufReader::new(Cursor::new(input.as_bytes().to_vec())));
            let mut errs = 0;
            loop {
                let r = {
                    let mut de = serde_json::Deserializer::from_reader(&mut f);
                    serde_json::Value::deserialize(&mut de)
                };
                match r {
                    Ok(_) => {}
                    Err(e) if e.is_eof() => break,
                    Err(_) => {
                        errs += 1;
                        assert!(
                            f.framing_msg.as_deref().is_some_and(|m| m.contains(msg)),
                            "{input}: {:?}",
                            f.framing_msg
                        );
                        break;
                    }
                }
            }
            assert_eq!(errs, 1, "{input}");
        }
    }

    #[test]
    fn doc_empty_and_ws_only_modes() {
        let mut f = DocFraming::new(BufReader::new(Cursor::new(Vec::<u8>::new())));
        let r = {
            let mut de = serde_json::Deserializer::from_reader(&mut f);
            serde_json::Value::deserialize(&mut de)
        };
        assert!(r.unwrap_err().is_eof());
        assert_eq!(f.mode, DocMode::Empty);

        let mut f = DocFraming::new(BufReader::new(Cursor::new(b"  \n".to_vec())));
        let r = {
            let mut de = serde_json::Deserializer::from_reader(&mut f);
            serde_json::Value::deserialize(&mut de)
        };
        assert!(r.unwrap_err().is_eof());
        assert_eq!(f.mode, DocMode::WsOnly);
    }
}
