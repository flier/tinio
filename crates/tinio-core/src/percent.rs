//! RFC 3986 percent-encoding of the ACL grants wire.
//!
//! [`encode`] and [`decode`] are the pair the tags wire used before the
//! dev merge moved it onto the `percent-encoding` crate (extracted from
//! the object module — the tags wire is byte-identical). The ACL grants
//! wire uses [`decode`] too, and `encode_uri` for its `uri=` grantee
//! elements, which carry the RFC 3986 encoding of a full group URI.

/// The hex digits of the wire `%XX` encoding (encode's per-byte lookup).
const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encode the wire-reserved characters (`%`, `=`, `&`, `+`,
/// space). Everything else — the Unicode charset included — passes
/// through untouched.
pub fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' | '=' | '&' | '+' | ' ' => {
                out.push('%');
                out.push(HEX_DIGITS[(c as u32 >> 4) as usize] as char);
                out.push(HEX_DIGITS[(c as u32 & 0xF) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out
}

/// Percent-decode `%XX` sequences (`+` stays literal). The two hex
/// bytes are read as raw bytes — never sliced out of the `&str` — so a
/// `%` followed by a raw non-ASCII char cannot hit a mid-char boundary
/// and panic: the bytes fail UTF-8 or hex validation and the `%` passes
/// through, leaving the caller's domain validation to reject the
/// input.
pub fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(byte) = hex {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encode a URI-valued wire element (`uri=` grantee): every
/// byte outside the RFC 3986 unreserved set (`ALPHA / DIGIT / - . _ ~`)
/// becomes `%XX`. [`encode`] above only covers the tags subset (`%`
/// `=` `&` `+` space) — a group URI's reserved `:` and `/` must be
/// encoded too, giving the canonical `http%3A%2F%2F...` grantee wire.
pub(crate) fn encode_uri(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX_DIGITS[(b >> 4) as usize] as char);
            out.push(HEX_DIGITS[(b & 0xF) as usize] as char);
        }
    }
    out
}
