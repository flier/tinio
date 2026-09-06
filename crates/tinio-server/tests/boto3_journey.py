"""boto3 basic-journey scenario (task T034) — the SC-001 scenario set via
the boto3 SDK against a running serve endpoint, extended 2026-09-05/06
with the ACL legs (design 2026-09-05-s3-acl-owner, spec verification
item 6): public-read upload + anonymous GET, get-bucket-acl echo,
expected-bucket-owner mismatch, list_buckets per principal. Driven by the
@boto3 cucumber scenario (`interop/journey.feature`): `python3
boto3_journey.py <endpoint>`. Best-effort client per FR-025 (targeted/
manual, NOT CI-gated).
"""

import sys
import tempfile
import urllib.error
import urllib.request

import boto3
from botocore import UNSIGNED
from botocore.client import Config

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

# Basic journey.
s3.create_bucket(Bucket="boto3-bucket")
s3.put_object(Bucket="boto3-bucket", Key="hello.txt", Body=b"hello from boto3")
got = s3.get_object(Bucket="boto3-bucket", Key="hello.txt")["Body"].read()
assert got == b"hello from boto3", "download not byte-identical"

# Zero-byte round-trip.
s3.put_object(Bucket="boto3-bucket", Key="empty", Body=b"")
assert s3.get_object(Bucket="boto3-bucket", Key="empty")["Body"].read() == b""

# List with prefix/delimiter.
s3.put_object(Bucket="boto3-bucket", Key="dir/nested.txt", Body=b"nested")
page = s3.list_objects_v2(Bucket="boto3-bucket", Delimiter="/")
assert "dir/" in [p["Prefix"] for p in page.get("CommonPrefixes", [])]
page = s3.list_objects_v2(Bucket="boto3-bucket", Prefix="dir/")
assert page["KeyCount"] == 1

# Bucket CORS trio (gap-analysis Tier A#2): delete on a fresh config is
# idempotent, the following get answers NoSuchCORSConfiguration, and the
# put/get round trip echoes the configuration.
s3.delete_bucket_cors(Bucket="boto3-bucket")
try:
    s3.get_bucket_cors(Bucket="boto3-bucket")
    raise AssertionError("CORS config should not exist on a fresh bucket")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "NoSuchCORSConfiguration", e.response

import urllib.error
import urllib.request

# boto3 does not compute Content-MD5 for this call; tinio requires it
# (AWS three-state: missing → 400 InvalidRequest). The value is base64 of
# 16 zero bytes — sufficient, since digest equality is not verified.
s3.put_bucket_cors(
    Bucket="boto3-bucket",
    CORSConfiguration={
        "CORSRules": [
            {
                "ID": "allow-example",
                "AllowedOrigins": ["https://example.com"],
                "AllowedMethods": ["GET", "PUT"],
                "ExposeHeaders": ["ETag"],
                "MaxAgeSeconds": 300,
            },
            {"AllowedOrigins": ["*"], "AllowedMethods": ["DELETE"]},
        ]
    },
    ContentMD5="AAAAAAAAAAAAAAAAAAAAAA==",
)
rules = s3.get_bucket_cors(Bucket="boto3-bucket")["CORSRules"]
assert [r.get("ID") for r in rules] == ["allow-example", None], rules
assert rules[0]["AllowedOrigins"] == ["https://example.com"], rules[0]
assert rules[0]["AllowedMethods"] == ["GET", "PUT"], rules[0]
assert rules[0]["MaxAgeSeconds"] == 300, rules[0]
assert rules[1]["AllowedOrigins"] == ["*"], rules[1]
s3.delete_bucket_cors(Bucket="boto3-bucket")
try:
    s3.get_bucket_cors(Bucket="boto3-bucket")
    raise AssertionError("CORS config still exists after delete")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "NoSuchCORSConfiguration", e.response

# Preflight leg: browsers cannot sign an OPTIONS preflight and boto3 has
# no OPTIONS call, so the raw preflight rides the stdlib HTTP client (the
# journey's only dependency beyond boto3). Allowed → 200 with the allow
# headers; denied → 403 AccessDenied without them.
s3.put_bucket_cors(
    Bucket="boto3-bucket",
    CORSConfiguration={
        "CORSRules": [
            {
                "AllowedOrigins": ["https://example.com"],
                "AllowedMethods": ["GET"],
            },
        ]
    },
    ContentMD5="AAAAAAAAAAAAAAAAAAAAAA==",
)


def preflight(origin, method):
    req = urllib.request.Request(
        f"http://{endpoint}/boto3-bucket/hello.txt",
        method="OPTIONS",
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": method,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.headers
    except urllib.error.HTTPError as err:
        return err.code, err.headers


status, headers = preflight("https://example.com", "GET")
assert status == 200, status
assert headers["Access-Control-Allow-Origin"] == "https://example.com", headers
status, headers = preflight("https://foreign.com", "GET")
assert status == 403, status
assert "Access-Control-Allow-Origin" not in headers, headers

# Multipart via upload_file (> 8 MiB -> composed ETag with -N suffix).
big = tempfile.NamedTemporaryFile(delete=False)
big.write(b"x" * (10 * 1024 * 1024))
big.close()
s3.upload_file(big.name, "boto3-bucket", "big.bin")
head = s3.head_object(Bucket="boto3-bucket", Key="big.bin")
assert "-" in head["ETag"].strip('"'), f"composed ETag expected, got {head['ETag']}"
dl = tempfile.NamedTemporaryFile(delete=False)
dl.close()
s3.download_file("boto3-bucket", "big.bin", dl.name)
with open(dl.name, "rb") as f:
    assert f.read() == b"x" * (10 * 1024 * 1024)

# Delete object.
s3.delete_object(Bucket="boto3-bucket", Key="hello.txt")
try:
    s3.head_object(Bucket="boto3-bucket", Key="hello.txt")
    raise AssertionError("object still exists after delete")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "404"

# ACL journey (design 2026-09-05, spec verification item 6). The spawned
# serve runs the default identity: root = the [auth] "minioadmin" pair
# presenting the [owner] element (canonical ID hex(SHA-256("tinio")),
# display name "tinio"); no [[users]] configured, so all "per principal"
# legs compare root vs anonymous.
ROOT_OWNER_ID = "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"

# public-read upload -> anonymous GET works (urllib — the boto3 venv
# carries no `requests`; stdlib keeps the leg dependency-free).
s3.create_bucket(Bucket="boto3-acl", ACL="public-read")
s3.put_object(Bucket="boto3-acl", Key="pub.txt", Body=b"public", ACL="public-read")
with urllib.request.urlopen(f"http://{endpoint}/boto3-acl/pub.txt") as resp:
    assert resp.read() == b"public", "anonymous GET of a public-read object"

# A private object (the default when no ACL header is sent) denies the
# anonymous GET with 403 — the ACL layer's enforced mode.
s3.put_object(Bucket="boto3-acl", Key="priv.txt", Body=b"private")
try:
    urllib.request.urlopen(f"http://{endpoint}/boto3-acl/priv.txt")
    raise AssertionError("anonymous GET of a private object must be denied")
except urllib.error.HTTPError as e:
    assert e.code == 403, f"expected 403, got {e.code}"

# get-bucket-acl echoes the row: the root owner element + the public-read
# grant set (canonical sorted id=,FULL_CONTROL & uri=AllUsers,READ).
acl = s3.get_bucket_acl(Bucket="boto3-acl")
assert acl["Owner"]["ID"] == ROOT_OWNER_ID, f"unexpected owner {acl['Owner']['ID']}"
grants = {(g["Grantee"].get("ID") or g["Grantee"].get("URI"), g["Permission"]) for g in acl["Grants"]}
assert (ROOT_OWNER_ID, "FULL_CONTROL") in grants, "owner FULL_CONTROL grant missing"
assert (
    "http://acs.amazonaws.com/groups/global/AllUsers",
    "READ",
) in grants, "public-read grant missing"

# expected-bucket-owner mismatch -> 403 AccessDenied; the matching owner
# passes.
try:
    s3.head_bucket(Bucket="boto3-acl", ExpectedBucketOwner="f" * 64)
    raise AssertionError("expect a 403 for a mismatching expected bucket owner")
except s3.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "403", e.response["Error"]["Code"]
s3.head_bucket(Bucket="boto3-acl", ExpectedBucketOwner=ROOT_OWNER_ID)

# list_buckets per principal: the signed principal sees its buckets (the
# ACL buckets included); an anonymous request is denied outright. The
# error code is the body's XML code — a GET answer carries the body, so
# botocore surfaces "AccessDenied" (unlike HEAD answers, whose body-less
# 403 surfaces the HTTP status "403" — see the head_bucket leg above).
names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
assert "boto3-acl" in names and "boto3-bucket" in names, names
anon = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    region_name="us-east-1",
    config=Config(signature_version=UNSIGNED),
)
try:
    anon.list_buckets()
    raise AssertionError("anonymous list_buckets must be denied")
except anon.exceptions.ClientError as e:
    assert e.response["Error"]["Code"] == "AccessDenied", e.response["Error"]["Code"]

print("BOTO3 JOURNEY OK")
