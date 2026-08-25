"""boto3 basic-journey scenario (task T034) — the SC-001 scenario set via
the boto3 SDK against a running serve endpoint. Driven by the Rust test
tests/boto3.rs: `python3 boto3_journey.py <endpoint>`. Best-effort client
per FR-025 (targeted/manual, NOT CI-gated).
"""

import sys
import tempfile

import boto3
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

print("BOTO3 JOURNEY OK")
