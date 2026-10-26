"""boto3 tier journey (Task 26 of the tiered-storage plan).

The SDK leg of the external-client suite: a spawned `tinio server` on the
tiered backend (`[storage] type = "tier"`) is driven through the class, copy,
UploadPartCopy, lifecycle, versioned-refusal and listing surfaces the tier
adds. The assertions are the SDK's own rendering of the facts the design
records (decisions T10, T11 and T24):

* a `STANDARD_IA` PutObject on a plain unversioned bucket demotes before it
  answers, and only `GetObjectAttributes` reports the class;
* `CopyObject` accepts and ignores the class, so a copy of a cold object is
  warm (`STANDARD`);
* `UploadPartCopy` assembles a multipart object from a cold source;
* a transition-only rule with `Days = 30` and `STANDARD_IA` round-trips
  (T11; the tier's one accepted transition class and floor);
* a versioned bucket refuses `STANDARD_IA` from a plain PUT with
  `InvalidRequest`, while a plain write still lands;
* a batched listing pages warm keys with `MaxKeys` and a continuation token.

The script asserts its own journey; the feature asserts the OK marker.
"""

import os
import sys

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id=os.environ.get("TINIO_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.environ.get("TINIO_SECRET_KEY", "minioadmin"),
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

bucket = "tier-boto3"
cold = b"cold payload from boto3"

s3.create_bucket(Bucket=bucket)

# An IA PUT demotes before it answers; the class is GetObjectAttributes' fact,
# and the plain read serves the cold bytes unchanged.
s3.put_object(Bucket=bucket, Key="cold.txt", Body=cold, StorageClass="STANDARD_IA")
assert s3.get_object(Bucket=bucket, Key="cold.txt")["Body"].read() == cold
attrs = s3.get_object_attributes(
    Bucket=bucket, Key="cold.txt", ObjectAttributes=["StorageClass"]
)
assert attrs["StorageClass"] == "STANDARD_IA", attrs

# CopyObject accepts the class header and ignores it (T10): the destination is
# a fresh warm object, never the source's placement.
s3.copy_object(
    Bucket=bucket,
    Key="copy.txt",
    CopySource={"Bucket": bucket, "Key": "cold.txt"},
    StorageClass="STANDARD_IA",
)
attrs = s3.get_object_attributes(
    Bucket=bucket, Key="copy.txt", ObjectAttributes=["StorageClass"]
)
assert attrs["StorageClass"] == "STANDARD", attrs
assert s3.get_object(Bucket=bucket, Key="copy.txt")["Body"].read() == cold

# UploadPartCopy assembles from a cold source: one copied part is all the
# server needs to complete the object with the source's bytes.
upload = s3.create_multipart_upload(Bucket=bucket, Key="assembled.txt")
upload_id = upload["UploadId"]
part = s3.upload_part_copy(
    Bucket=bucket,
    Key="assembled.txt",
    PartNumber=1,
    UploadId=upload_id,
    CopySource={"Bucket": bucket, "Key": "cold.txt"},
)
etag = part["CopyPartResult"]["ETag"]
s3.complete_multipart_upload(
    Bucket=bucket,
    Key="assembled.txt",
    UploadId=upload_id,
    MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
)
assert s3.get_object(Bucket=bucket, Key="assembled.txt")["Body"].read() == cold

# A transition-only rule round-trips: STANDARD_IA only, Days >= 30 (T11).
s3.put_bucket_lifecycle_configuration(
    Bucket=bucket,
    LifecycleConfiguration={
        "Rules": [
            {
                "ID": "ia-at-30",
                "Filter": {"Prefix": "archive/"},
                "Status": "Enabled",
                "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}],
            }
        ]
    },
)
rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
assert [r["ID"] for r in rules] == ["ia-at-30"], rules
transition = rules[0]["Transitions"][0]
assert (transition["Days"], transition["StorageClass"]) == (30, "STANDARD_IA"), transition
s3.delete_bucket_lifecycle(Bucket=bucket)
try:
    s3.get_bucket_lifecycle_configuration(Bucket=bucket)
    raise AssertionError("the lifecycle configuration survived its delete")
except ClientError as err:
    assert err.response["Error"]["Code"] == "NoSuchLifecycleConfiguration", err.response

# A versioned bucket refuses the class on a plain PUT; a class-less write on the
# same bucket still lands.
versioned = "tier-boto3-versioned"
s3.create_bucket(Bucket=versioned)
s3.put_bucket_versioning(
    Bucket=versioned, VersioningConfiguration={"Status": "Enabled"}
)
try:
    s3.put_object(
        Bucket=versioned, Key="ia.txt", Body=b"refused", StorageClass="STANDARD_IA"
    )
    raise AssertionError("a versioned bucket accepted STANDARD_IA")
except ClientError as err:
    assert err.response["Error"]["Code"] == "InvalidRequest", err.response
s3.put_object(Bucket=versioned, Key="plain.txt", Body=b"plain")
assert s3.get_object(Bucket=versioned, Key="plain.txt")["Body"].read() == b"plain"

# A batched listing: three keys, then two pages driven by MaxKeys and the
# continuation token.
for i in range(3):
    s3.put_object(Bucket=bucket, Key=f"batch/{i}.txt", Body=str(i).encode())
listing = s3.list_objects_v2(Bucket=bucket, Prefix="batch/")
assert listing["KeyCount"] == 3, listing
assert [o["Key"] for o in listing["Contents"]] == [
    "batch/0.txt",
    "batch/1.txt",
    "batch/2.txt",
], listing
page = s3.list_objects_v2(Bucket=bucket, Prefix="batch/", MaxKeys=2)
assert page["IsTruncated"] is True and page["KeyCount"] == 2, page
rest = s3.list_objects_v2(
    Bucket=bucket,
    Prefix="batch/",
    MaxKeys=2,
    ContinuationToken=page["NextContinuationToken"],
)
assert rest["KeyCount"] == 1, rest

print("BOTO3 TIER OK")
