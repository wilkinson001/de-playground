"""S3 access to the SeaweedFS object store."""

from __future__ import annotations

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from de_playground.config import Settings


def s3_client(settings: Settings) -> BaseClient:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def ensure_bucket(client: BaseClient, name: str) -> bool:
    """Create the bucket if missing. Returns True if it was created."""
    try:
        client.head_bucket(Bucket=name)
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise
    client.create_bucket(Bucket=name)
    return True


def object_exists(client: BaseClient, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def delete_prefix(client: BaseClient, bucket: str, prefix: str) -> int:
    """Delete every object under a prefix (e.g. a path-based lake table). Returns the count."""
    deleted = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    return deleted


def delta_storage_options(settings: Settings) -> dict[str, str]:
    """Storage options for delta-rs (`deltalake`) against SeaweedFS."""
    return {
        "AWS_ENDPOINT_URL": settings.s3_endpoint,
        "AWS_ACCESS_KEY_ID": settings.s3_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
