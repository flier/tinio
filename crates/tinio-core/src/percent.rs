//! The wire codecs over `percent-encoding`: the ACL grants wire's `uri=`
//! grantee elements (`encode_uri`, the RFC 3986 unreserved set — the same
//! set the CORS wire uses) and the [`decode`] the grants parser and the
//! request path share.

use percent_encoding::{percent_decode_str, utf8_percent_encode};

use crate::cors::UNRESERVED;

/// Percent-encode a URI-valued wire element (`uri=` grantee): every byte
/// outside the RFC 3986 unreserved set becomes `%XX`, giving the canonical
/// `http%3A%2F%2F...` grantee wire.
pub(crate) fn encode_uri(s: &str) -> String {
    utf8_percent_encode(s, UNRESERVED).to_string()
}

/// Percent-decode `%XX` sequences (`+` stays literal). An invalid sequence
/// passes through and invalid UTF-8 is replaced — never a panic, leaving
/// the caller's domain validation to reject the result.
pub fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_leaves_plus_literal_and_passes_stray_percent_through() {
        assert_eq!(decode("a+b"), "a+b");
        assert_eq!(decode("%"), "%");
        assert_eq!(decode("%2"), "%2");
        assert_eq!(decode("%ZZ"), "%ZZ");
        // A `%` followed by a raw multi-byte char: the following bytes are
        // not two hex digits, so the `%` passes through and the sequence
        // survives lossy decoding.
        assert_eq!(decode("%é"), "%é");
    }

    #[test]
    fn decode_round_trips_the_uri_wire() {
        for s in ["http://acs.amazonaws.com/groups/global/AllUsers", "é中", ""] {
            assert_eq!(decode(&encode_uri(s)), s);
        }
    }

    #[test]
    fn encode_uri_escapes_every_non_unreserved_byte() {
        assert_eq!(
            encode_uri("http://acs.amazonaws.com/groups/global/AllUsers"),
            "http%3A%2F%2Facs.amazonaws.com%2Fgroups%2Fglobal%2FAllUsers"
        );
        assert_eq!(encode_uri("a-b._~Z9"), "a-b._~Z9");
        // Per-byte, so a multi-byte char becomes two `%XX` triplets.
        assert_eq!(encode_uri("é"), "%C3%A9");
    }
}
