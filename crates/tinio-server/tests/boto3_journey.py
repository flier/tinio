"""boto3 basic-journey scenario (task T034) — the SC-001 scenario set via
the boto3 SDK against a running serve endpoint, extended 2026-09-05/06
with the ACL legs (design 2026-09-05-s3-acl-owner, spec verification
item 6): public-read upload + anonymous GET, get-bucket-acl echo,
expected-bucket-owner mismatch, list_buckets per principal. Driven by the
@boto3 cucumber scenario (`interop/journey.feature`): `python3
boto3_journey.py <endpoint>`. Best-effort client per FR-025 (targeted/
manual, NOT CI-gated).

Extended 2026-10-05 with the versioning legs (spec 2026-10-03-s3-versioning,
FR-037/FR-038): the overwrite keeps the superseded version retrievable,
the listing is bucket-addressed, a plain delete leaves a marker the plain
read answers 404 with the marker disclosed for, deleting the marker
version restores the version below it, and purging one version by id is
permanent. The legs ran the same day under the `@boto3` tier (boto3
1.43.108, the conventional `<target>/tinio-e2e-venv` the harness
documents); like the rest of this script they stay opt-in, never CI-run.

A second argument, `--read-only`, swaps the journey for the SDK's own
read-only expectations (G22 of the sandbox BDD plan): the server behind the
endpoint was spawned `--read-only --sandbox`, so every mutating operation
answers `AccessDenied` and this asserts the *SDK's* rendering of that
refusal. The journey body below is untouched by that arm — it `sys.exit`s
before reaching it.
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


def read_only_checks(client):
    """The read-only arm (G22): the SDK's own refusal shape for the mutating
    operations `contracts/s3-surface.md` §Read-only mode lists — sampled one per
    class (bucket and object) rather than swept, because the server-side answer
    itself is `confinement.feature`'s G9 and this leg is about how botocore
    renders it. The fixture is discovered rather than named: the scenario plants
    a bucket with one object on disk, and the read of that object is asserted so
    a dead or unauthenticated endpoint cannot pass this."""

    def refused(label, call):
        """Assert `call()` raises botocore's ClientError carrying the service's
        own refusal — the SDK's shape, not a generic failure."""
        try:
            call()
        except client.exceptions.ClientError as err:
            error = err.response["Error"]
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            assert (error["Code"], status, error["Message"]) == (
                "AccessDenied",
                403,
                "Access Denied",
            ), f"{label}: {error['Code']} {status} {error['Message']!r}"
        else:
            raise AssertionError(f"{label} was not refused")

    buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
    assert buckets, "the read-only fixture planted no bucket"
    bucket = buckets[0]
    keys = [o["Key"] for o in client.list_objects_v2(Bucket=bucket).get("Contents", [])]
    assert keys, f"the read-only fixture planted no object under {bucket}"
    body = client.get_object(Bucket=bucket, Key=keys[0])["Body"].read()
    assert body, "a read of the planted object answered no body"

    refused("CreateBucket", lambda: client.create_bucket(Bucket="boto3-refused"))
    refused(
        "PutObject",
        lambda: client.put_object(Bucket=bucket, Key="new.txt", Body=b"refused"),
    )
    refused(
        "DeleteObject",
        lambda: client.delete_object(Bucket=bucket, Key=keys[0]),
    )
    refused("DeleteBucket", lambda: client.delete_bucket(Bucket=bucket))


if "--read-only" in sys.argv[2:]:
    read_only_checks(s3)
    print("BOTO3 READ-ONLY OK")
    sys.exit(0)

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

# Versioning journey (spec 2026-10-03-s3-versioning, FR-037/FR-038): the
# same journey `versioning.feature` drives in process, seen through the
# SDK. The listing is bucket-addressed (`?versions`, scoped with prefix) —
# the one shape `contracts/s3-surface.md` §Object versioning records for a
# version listing. Added 2026-10-05 and run the same day under the
# `@boto3` tier (`--tags '@boto3 and not @confinement'`, 3/3 scenarios).
versioned = "boto3-versioned"
s3.create_bucket(Bucket=versioned)
s3.put_bucket_versioning(
    Bucket=versioned, VersioningConfiguration={"Status": "Enabled"}
)
assert s3.get_bucket_versioning(Bucket=versioned)["Status"] == "Enabled"

# The overwrite supersedes the first version into the history store, and
# the plain read serves the current one — whose identity the bucket-records-
# versions rule discloses.
first = s3.put_object(Bucket=versioned, Key="notes.txt", Body=b"first")
second = s3.put_object(Bucket=versioned, Key="notes.txt", Body=b"second")
assert first.get("VersionId") and second.get("VersionId"), (first, second)
assert first["VersionId"] != second["VersionId"], (first, second)
current = s3.get_object(Bucket=versioned, Key="notes.txt")
assert current["Body"].read() == b"second", "the plain read serves the current version"
assert current.get("VersionId") == second["VersionId"], current.get("VersionId")

# Both versions list, newest first within the key (`IsLatest` on the first
# entry — the contract's ordering rule).
listing = s3.list_object_versions(Bucket=versioned, Prefix="notes.txt")
assert [v["VersionId"] for v in listing["Versions"]] == [
    second["VersionId"],
    first["VersionId"],
], listing["Versions"]
assert listing["Versions"][0]["IsLatest"] is True, listing["Versions"]
assert not listing.get("DeleteMarkers"), listing.get("DeleteMarkers")

# A version-addressed read serves the immutable snapshot, not the current
# object.
older = s3.get_object(Bucket=versioned, Key="notes.txt", VersionId=first["VersionId"])
assert older["Body"].read() == b"first", "the addressed version is not the current one"
assert older["VersionId"] == first["VersionId"], older["VersionId"]

# A plain delete inserts a marker: the delete answers the marker's own
# identity, the key 404s with the marker disclosed, and every version stays
# in the history.
marker = s3.delete_object(Bucket=versioned, Key="notes.txt")
assert marker.get("DeleteMarker") is True, marker
assert marker.get("VersionId"), marker
try:
    s3.get_object(Bucket=versioned, Key="notes.txt")
    raise AssertionError("a marked key must answer 404")
except s3.exceptions.ClientError as err:
    assert err.response["ResponseMetadata"]["HTTPStatusCode"] == 404, err.response
    assert err.response["Error"]["Code"] == "NoSuchKey", err.response["Error"]
    headers = {
        name.lower(): value
        for name, value in err.response["ResponseMetadata"].get("HTTPHeaders", {}).items()
    }
    assert headers.get("x-amz-delete-marker") == "true", headers
listing = s3.list_object_versions(Bucket=versioned, Prefix="notes.txt")
assert [m["VersionId"] for m in listing["DeleteMarkers"]] == [marker["VersionId"]], listing
assert [v["VersionId"] for v in listing["Versions"]] == [
    second["VersionId"],
    first["VersionId"],
], listing["Versions"]

# Deleting the marker version restores the version below it as current.
s3.delete_object(Bucket=versioned, Key="notes.txt", VersionId=marker["VersionId"])
restored = s3.get_object(Bucket=versioned, Key="notes.txt")
assert restored["Body"].read() == b"second", "the restored current version"

# Purging one content version by id is permanent: the listing loses it and
# the surviving version still serves by its own id.
s3.delete_object(Bucket=versioned, Key="notes.txt", VersionId=first["VersionId"])
listing = s3.list_object_versions(Bucket=versioned, Prefix="notes.txt")
assert [v["VersionId"] for v in listing["Versions"]] == [second["VersionId"]], listing
assert not listing.get("DeleteMarkers"), listing.get("DeleteMarkers")
survivor = s3.get_object(Bucket=versioned, Key="notes.txt", VersionId=second["VersionId"])
assert survivor["Body"].read() == b"second", "the surviving version serves by its id"

print("BOTO3 JOURNEY OK")
