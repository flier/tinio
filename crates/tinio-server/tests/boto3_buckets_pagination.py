"""ListBuckets pagination (2025-03 semantics) against a serve endpoint
whose `[s3] max_buckets = 3` cap forces a small page size: create more
buckets than one page, paginate with the boto3 list_buckets paginator,
assert at least two pages occur (a missing cap — default 10000 — would
return a single page and fail) and every bucket is seen exactly once.
Driven by the Rust test tests/boto3.rs:
`python3 boto3_buckets_pagination.py <endpoint>`.
Best-effort client per FR-025 (targeted/manual, NOT CI-gated).
"""

import sys

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

expected = [f"pag-bucket-{i}" for i in range(7)]
for name in expected:
    s3.create_bucket(Bucket=name)

paginator = s3.get_paginator("list_buckets")
pages = list(paginator.paginate())
seen = []
for page in pages:
    seen.extend(b["Name"] for b in page["Buckets"])
assert len(pages) >= 2, f"single page — the max_buckets cap was not applied: {len(pages)} page(s)"
assert sorted(seen) == sorted(expected), f"pagination lost or duplicated a bucket: {seen}"

print("BUCKET PAGINATION OK")
