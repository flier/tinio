//! LINES framing: serves exactly one line content bytes (terminator
//! excluded) and reports EOF at the newline, so the parser consumes a line
//! as a stream. The span cap (content + terminator <= 1 MB) is enforced here,
//! mid-serve, and remains the authority on how wide a line is.

use std::io::{self, BufRead, Read};

use crate::record::{MAX_RECORD, cap_io_error};

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
pub(crate) struct LineFraming<R> {
    inner: R,
    offset: u64,
    span: u64,
    eol: bool,
}

impl<R> LineFraming<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner,
            offset: 0,
            span: 0,
            eol: true,
        }
    }
}

impl<R: BufRead> LineFraming<R> {
    pub(crate) fn start_line(&mut self) -> io::Result<LineStart> {
        debug_assert!(self.eol, "start_line before the previous line drained");
        if self.inner.fill_buf()?.is_empty() {
            return Ok(LineStart::Eof);
        }
        self.eol = false;
        self.span = 0;
        Ok(LineStart::Line(self.offset))
    }
}

impl<R: BufRead> Read for LineFraming<R> {
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
        // Only the bytes this call can serve are worth scanning: struson
        // fills its fixed 1 KiB reader buffer with one `read`, so scanning all
        // of `avail` here would rescan the buffered tail on every call and
        // make serving a line quadratic in the buffered prefix. One byte past
        // `out` suffices — a `\n` at exactly `out.len()` still ends the line,
        // and is consumed below without being copied.
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
            // Struson surfaces this read failure as an `IoError` (its only
            // production read site, `fill_buffer`, wraps every non-`Interrupted`
            // failure), so the pinned pair is LINES' only route to the cap.
            return Err(cap_io_error());
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor};

    use super::*;
    use crate::record::CAP_MESSAGE;

    fn framing(bytes: &[u8]) -> LineFraming<BufReader<Cursor<Vec<u8>>>> {
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
        let err = f.read_to_end(&mut sink).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.to_string(), CAP_MESSAGE);

        let mut f = framing(&{
            let mut v = vec![b'a'; (MAX_RECORD - 1) as usize];
            v.push(b'\n');
            v
        });
        assert!(matches!(f.start_line().unwrap(), LineStart::Line(0)));
        let mut sink = Vec::new();
        f.read_to_end(&mut sink).unwrap();
        assert_eq!(sink.len() as u64, MAX_RECORD - 1);
    }
}
