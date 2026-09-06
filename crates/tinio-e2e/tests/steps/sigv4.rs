//! Minimal AWS Signature Version 4 (spec 2026-09-05, Task 15): the
//! hand-rolled signer the acl scenarios use to authenticate requests to
//! the identity-wired plane — `hmac` + `sha2` dev-dependencies, no SDK
//! pull-in. Header-based auth only.
//!
//! The canonical request covers method / URI path / query / signed
//! headers; the payload hash is `UNSIGNED-PAYLOAD` for bodyless requests
//! and the body's SHA-256 for uploads (s3s wraps the latter in its
//! checksum-verifying upload stream — a wrong hash fails the request,
//! the plan's signer cross-check), and the signature is the standard
//! 4-step HMAC chain.
//!
//! Signing rule: `host` + `x-amz-date` + `x-amz-content-sha256` plus
//! every `x-amz-*` header the request carries (and `content-md5` /
//! `content-type` when present) — the AWS header-signing set. s3s sorts
//! the authorization's `SignedHeaders` itself and re-derives the
//! canonical headers from the received request, so the canonicalization
//! here must match what goes over the wire: lowercase names, trimmed
//! values, the AWS URL-encoding table.

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// The signing scope: the region tinio accepts by default (no
/// `expected_region` configured on the plane), service `s3`, terminator
/// `aws4_request`.
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The `x-amz-content-sha256` value for a bodyless request (S3's
/// unsigned-payload convention).
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// One SigV4 credential pair — a configured user of the spawned
/// fixture identity (no request ever carries these outside the step
/// wrappers).
pub struct Signer {
    access_key: String,
    secret: String,
}

impl Signer {
    /// A signer for one configured access-key/secret pair.
    pub fn new(access_key: &str, secret: &str) -> Self {
        Self {
            access_key: access_key.into(),
            secret: secret.into(),
        }
    }

    /// The three signing headers for one request — `x-amz-date`,
    /// `x-amz-content-sha256`, `authorization` — to be merged into and
    /// sent with `headers` (whose own entries the signature covers:
    /// every `x-amz-*` one plus `content-md5`/`content-type`).
    pub fn sign(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
        host: &str,
    ) -> Vec<(String, String)> {
        let amz_date = utc_timestamp();
        let date = &amz_date[..8];
        let payload_hash = if body.is_empty() {
            UNSIGNED_PAYLOAD.to_string()
        } else {
            hex_sha256(body)
        };

        // The signed-header name set of the request as sent: the
        // implicit trio plus every carried `x-amz-*` header and the
        // carried `content-md5`/`content-type` (signed only when sent —
        // a signed-but-absent header would canonicalize as an empty
        // line the server never sees).
        let mut names: Vec<String> = vec![
            "host".into(),
            "x-amz-content-sha256".into(),
            "x-amz-date".into(),
        ];
        for (name, _) in headers {
            let lower = name.to_ascii_lowercase();
            let signable =
                lower.starts_with("x-amz-") || lower == "content-md5" || lower == "content-type";
            if signable && !names.contains(&lower) {
                names.push(lower);
            }
        }
        names.sort();

        let canonical = canonical_request(
            method,
            path,
            headers,
            &names,
            &payload_hash,
            host,
            &amz_date,
        );
        let scope = format!("{date}/{REGION}/{SERVICE}/{TERMINATOR}");
        let string_to_sign = format!(
            "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
            hex_sha256(canonical.as_bytes()),
        );

        let date_key = hmac(
            format!("AWS4{secret}", secret = self.secret).as_bytes(),
            date.as_bytes(),
        );
        let region_key = hmac(&date_key, REGION.as_bytes());
        let service_key = hmac(&region_key, SERVICE.as_bytes());
        let signing_key = hmac(&service_key, TERMINATOR.as_bytes());
        let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());

        let signed_list = names.join(";");
        vec![
            ("x-amz-date".into(), amz_date),
            ("x-amz-content-sha256".into(), payload_hash),
            (
                "authorization".into(),
                format!(
                    "{ALGORITHM} Credential={ak}/{scope}, SignedHeaders={signed_list}, Signature={signature}",
                    ak = self.access_key,
                ),
            ),
        ]
    }
}

/// The SigV4 canonical request (AWS's published construction).
fn canonical_request(
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    signed_names: &[String],
    payload_hash: &str,
    host: &str,
    amz_date: &str,
) -> String {
    let (raw_path, query) = path.split_once('?').unwrap_or((path, ""));
    let mut out = String::with_capacity(320);
    out.push_str(method);
    out.push('\n');
    // Canonical URI: the decoded path re-encoded with the AWS table
    // ('/' preserved) — the scenarios' ASCII paths are identity-encoded.
    out.push_str(&aws_encode(&pct_decode(raw_path, false), true));
    out.push('\n');
    // Canonical query: decoded pairs, re-encoded (query encoding encodes
    // '/' as %2F and '=' as %3D — never the path's table), sorted by
    // name then value — the AWS strict encoding, not a form encode.
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|q| !q.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (
                aws_encode(&pct_decode(k, true), false),
                aws_encode(&pct_decode(v, true), false),
            ),
            None => (aws_encode(&pct_decode(pair, true), false), String::new()),
        })
        .collect();
    pairs.sort();
    if let Some((first, rest)) = pairs.split_first() {
        out.push_str(&first.0);
        out.push('=');
        out.push_str(&first.1);
        for (k, v) in rest {
            out.push('&');
            out.push_str(k);
            out.push('=');
            out.push_str(v);
        }
    }
    out.push('\n');
    // Canonical headers: the signed names in order, values from the
    // request (trimmed; the request values carry no inner whitespace).
    for name in signed_names {
        let value = match name.as_str() {
            "host" => host,
            "x-amz-date" => amz_date,
            "x-amz-content-sha256" => payload_hash,
            _ => headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.trim())
                .unwrap_or_default(),
        };
        out.push_str(name);
        out.push(':');
        out.push_str(value);
        out.push('\n');
    }
    out.push('\n');
    // The signed-headers list.
    out.push_str(&signed_names.join(";"));
    out.push('\n');
    // The payload hash.
    out.push_str(payload_hash);
    out
}

/// The `x-amz-content-sha256` value for a carried body: lowercase hex.
fn hex_sha256(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(out, "{byte:02x}").expect("write to string");
    }
    out
}

/// One HMAC-SHA256 step.
fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// The hex of one HMAC-SHA256 step (the signature).
fn hex_hmac(key: &[u8], data: &[u8]) -> String {
    let digest = hmac(key, data);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(out, "{byte:02x}").expect("write to string");
    }
    out
}

/// Percent-decode `%XX` sequences (`form` also maps `+` to space — the
/// query decode, mirroring s3s's serde_urlencoded parse).
fn pct_decode(input: &str, form: bool) -> String {
    let mut out = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && bytes[i + 1].is_ascii_hexdigit()
            && bytes[i + 2].is_ascii_hexdigit()
        {
            let hi = (bytes[i + 1] as char).to_digit(16).unwrap() as u8;
            let lo = (bytes[i + 2] as char).to_digit(16).unwrap() as u8;
            out.push((hi << 4) | lo);
            i += 3;
        } else if bytes[i] == b'+' && form {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The AWS URL-encoding table (SigV4 canonicalization): every octet
/// except the unreserved set is `%XX`. The canonical URI keeps `/`
/// (`keep_slash`), the canonical query encodes it (`%2F`) — AWS's two
/// distinct uses of the same table.
fn aws_encode(input: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            b'/' if keep_slash => out.push('/'),
            other => {
                use std::fmt::Write as _;
                write!(out, "%{other:02X}").expect("write to string");
            }
        }
    }
    out
}

/// The current UTC time in SigV4's `YYYYMMDDTHHMMSSZ` form — a small
/// civil-from-days conversion (no extra date dependency; the s3s clock
/// skew window is ±15 minutes, so a second-resolution timestamp is
/// ample).
fn utc_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is past the unix epoch")
        .as_secs();
    let days = secs / 86_400;
    let secs_of_day = secs % 86_400;
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        secs_of_day / 3_600,
        secs_of_day % 3_600 / 60,
        secs_of_day % 60,
    )
}

/// The (year, month, day) of a days-since-1970-01-01 count — Howard
/// Hinnant's canonical `civil_from_days` algorithm.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The AWS-published SigV4 example (S3 presigned URL): the same
    /// golden vector s3s's own sig_v4 tests pin, over the canonical
    /// request with a `host`-only signed set and the unsigned payload.
    /// One independent oracle that the canonicalization — query
    /// strict-encoding included — matches AWS byte-for-byte.
    /// `allow(dead_code)`: the tests module also compiles into the
    /// cucumber test binary (harness disabled — never runs there).
    #[allow(dead_code)]
    fn golden_canonical() -> String {
        canonical_request(
            "GET",
            "/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host",
            &[],
            &["host".into()],
            "UNSIGNED-PAYLOAD",
            "examplebucket.s3.amazonaws.com",
            "20130524T000000Z",
        )
    }

    #[test]
    fn canonical_request_matches_the_aws_vector() {
        assert_eq!(
            golden_canonical(),
            concat!(
                "GET\n",
                "/test.txt\n",
                "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\n",
                "host:examplebucket.s3.amazonaws.com\n",
                "\n",
                "host\n",
                "UNSIGNED-PAYLOAD",
            )
        );
    }

    #[test]
    fn signature_matches_the_aws_vector() {
        // The 4-step chain over the golden canonical request with the
        // AWS example secret — the published vector signature.
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\n{}",
            hex_sha256(golden_canonical().as_bytes()),
        );
        let date_key = hmac(b"AWS4wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", b"20130524");
        let region_key = hmac(&date_key, b"us-east-1");
        let service_key = hmac(&region_key, b"s3");
        let signing_key = hmac(&service_key, b"aws4_request");
        let signature = hex_hmac(&signing_key, string_to_sign.as_bytes());
        assert_eq!(
            signature,
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }
}
