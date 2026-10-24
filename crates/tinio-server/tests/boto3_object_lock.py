"""boto3 Object Lock journey (FR-043) — the S3 Object Lock legs via the
boto3 SDK against a running serve endpoint, driven by the `@boto3` scenario
in `object_lock.feature`: `python3 boto3_object_lock.py <endpoint>`.
Best-effort client per FR-025 (targeted/manual, NOT CI-gated).

The journey is the contract's (`contracts/s3-surface.md` §Object Lock): a
bucket created with Object Lock, its default retention, per-version
retention and legal hold, the GET/HEAD echo, a refused permanent delete,
and the governance bypass the signed owner is entitled to (OL15).

**The `ContentMD5` has two meanings here.** On the Object Lock XML writes
(`put_object_lock_configuration`, `put_object_retention`,
`put_object_legal_hold`) it is the presence-only claim the server
form-checks: s3s consumes the XML body before the handler runs, so the
digest's value is never compared (the deviation the CORS/ACL/lifecycle
surfaces record). On `put_object` it is a **body** checksum: the server
verifies it when `[s3] checksum = true`. This script passes the base64 of 16
zero bytes on both, so it runs against the default server (checksum off);
turn the capability on and the `put_object` calls need the real digest.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.client import Config

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id=os.environ.get("TINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("TINIO_SECRET_KEY", "minioadmin"),
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

# A form-valid Content-MD5 (base64 of 16 zero bytes): presence-only on the
# XML writes, and only accepted as a body digest while `[s3] checksum` is off.
MD5 = "AAAAAAAAAAAAAAAAAAAAAA=="


def future(days=1):
    return datetime.now(timezone.utc) + timedelta(days=days)


def error_of(call):
    """Return `(code, status)` for the ClientError `call` raises."""
    try:
        call()
    except s3.exceptions.ClientError as err:
        error = err.response["Error"]
        return error["Code"], err.response["ResponseMetadata"]["HTTPStatusCode"]
    raise AssertionError("the call was not refused")


bucket = "boto3-object-lock"

# The CreateBucket header writes the bucket, versioning Enabled and the
# configuration in one step (OL1/OL13).
s3.create_bucket(Bucket=bucket, ObjectLockEnabledForBucket=True)
assert s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled", "create header"
config = s3.get_object_lock_configuration(Bucket=bucket)["ObjectLockConfiguration"]
assert config["ObjectLockEnabled"] == "Enabled", config
assert "Rule" not in config, "a header-only enablement carries no default rule"

# The bucket configuration round-trips with a default retention (Days form).
s3.put_object_lock_configuration(
    Bucket=bucket,
    ObjectLockConfiguration={
        "ObjectLockEnabled": "Enabled",
        "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 1}},
    },
    ContentMD5=MD5,
)
rule = s3.get_object_lock_configuration(Bucket=bucket)["ObjectLockConfiguration"]["Rule"]
assert rule["DefaultRetention"] == {"Mode": "GOVERNANCE", "Days": 1}, rule

# A write with no lock headers receives the default retention; an explicit
# COMPLIANCE retention overrides it and a legal hold rides alongside.
defaulted = s3.put_object(Bucket=bucket, Key="defaulted.txt", Body=b"defaulted", ContentMD5=MD5)
default_retention = s3.get_object_retention(
    Bucket=bucket, Key="defaulted.txt", VersionId=defaulted["VersionId"]
)["Retention"]
assert default_retention["Mode"] == "GOVERNANCE", default_retention

explicit = s3.put_object(
    Bucket=bucket,
    Key="explicit.txt",
    Body=b"explicit",
    ObjectLockMode="COMPLIANCE",
    ObjectLockRetainUntilDate=future(3650),
    ObjectLockLegalHoldStatus="ON",
    ContentMD5=MD5,
)
version = explicit["VersionId"]
retention = s3.get_object_retention(Bucket=bucket, Key="explicit.txt", VersionId=version)[
    "Retention"
]
assert retention["Mode"] == "COMPLIANCE", retention
assert s3.get_object_legal_hold(Bucket=bucket, Key="explicit.txt", VersionId=version)[
    "LegalHold"
]["Status"] == "ON"

# GET and HEAD echo the three lock headers for the version they serve.
got = s3.get_object(Bucket=bucket, Key="explicit.txt")
assert got["ObjectLockMode"] == "COMPLIANCE", got
assert got["ObjectLockLegalHoldStatus"] == "ON", got
assert "ObjectLockRetainUntilDate" in got, got
head = s3.head_object(Bucket=bucket, Key="explicit.txt")
assert head["ObjectLockMode"] == "COMPLIANCE", head
assert head["ObjectLockLegalHoldStatus"] == "ON", head
assert "ObjectLockRetainUntilDate" in head, head
older = s3.get_object(Bucket=bucket, Key="explicit.txt", VersionId=version)
assert older["ObjectLockMode"] == "COMPLIANCE", older

# A compliance-protected version refuses a permanent delete even with the
# bypass header; clearing the legal hold does not weaken the retention.
assert error_of(
    lambda: s3.delete_object(Bucket=bucket, Key="explicit.txt", VersionId=version)
) == ("AccessDenied", 403)
s3.put_object_legal_hold(
    Bucket=bucket, Key="explicit.txt", VersionId=version,
    LegalHold={"Status": "OFF"}, ContentMD5=MD5,
)
assert s3.get_object_legal_hold(Bucket=bucket, Key="explicit.txt", VersionId=version)[
    "LegalHold"
]["Status"] == "OFF"
assert error_of(
    lambda: s3.delete_object(
        Bucket=bucket, Key="explicit.txt", VersionId=version, BypassGovernanceRetention=True
    )
) == ("AccessDenied", 403), "compliance has no bypass"

# A governance retention refuses the delete without the header and yields to
# the bucket owner's bypass (OL11/OL15).
governed = s3.put_object(
    Bucket=bucket,
    Key="governed.txt",
    Body=b"governed",
    ObjectLockMode="GOVERNANCE",
    ObjectLockRetainUntilDate=future(3650),
    ContentMD5=MD5,
)
gov_version = governed["VersionId"]
assert error_of(
    lambda: s3.delete_object(Bucket=bucket, Key="governed.txt", VersionId=gov_version)
) == ("AccessDenied", 403)
s3.delete_object(
    Bucket=bucket, Key="governed.txt", VersionId=gov_version, BypassGovernanceRetention=True
)
assert error_of(lambda: s3.head_object(Bucket=bucket, Key="governed.txt", VersionId=gov_version))[
    0
] in ("404", "NoSuchVersion"), "the bypassed version is gone"

# The retention is per version: an overwrite creates a new identity with the
# default, while the superseded version keeps its own explicit window.
old = s3.put_object(
    Bucket=bucket,
    Key="versions.txt",
    Body=b"old",
    ObjectLockMode="GOVERNANCE",
    ObjectLockRetainUntilDate=future(3650),
    ContentMD5=MD5,
)
new = s3.put_object(Bucket=bucket, Key="versions.txt", Body=b"new", ContentMD5=MD5)
old_retention = s3.get_object_retention(
    Bucket=bucket, Key="versions.txt", VersionId=old["VersionId"]
)["Retention"]
new_retention = s3.get_object_retention(
    Bucket=bucket, Key="versions.txt", VersionId=new["VersionId"]
)["Retention"]
assert old_retention["Mode"] == "GOVERNANCE", old_retention
assert new_retention["Mode"] == "GOVERNANCE", new_retention
assert old_retention["RetainUntilDate"] > new_retention["RetainUntilDate"], (
    old_retention,
    new_retention,
)

# A per-version retention update lands on the addressed identity, not the
# current one.
s3.put_object_retention(
    Bucket=bucket,
    Key="versions.txt",
    VersionId=old["VersionId"],
    Retention={"Mode": "GOVERNANCE", "RetainUntilDate": future(3700)},
    ContentMD5=MD5,
)
updated = s3.get_object_retention(
    Bucket=bucket, Key="versions.txt", VersionId=old["VersionId"]
)["Retention"]
assert updated["RetainUntilDate"] > old_retention["RetainUntilDate"], updated

print("BOTO3 OBJECT LOCK OK")
