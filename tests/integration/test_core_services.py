import uuid

import psycopg
import pytest

from de_playground import storage

pytestmark = pytest.mark.integration


def test_postgres_18_with_databases(settings):
    with psycopg.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        dbname="postgres",
    ) as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        names = {r[0] for r in conn.execute("SELECT datname FROM pg_database")}
    assert version.startswith("18"), version
    assert {"ducklake", "lakekeeper", "dbos", "prefect", "dagster"} <= names


def test_seaweedfs_s3_roundtrip(settings):
    client = storage.s3_client(settings)
    bucket = "smoke"
    storage.ensure_bucket(client, bucket)
    assert storage.ensure_bucket(client, bucket) is False

    key = f"roundtrip/{uuid.uuid4()}.txt"
    client.put_object(Bucket=bucket, Key=key, Body=b"hello")
    assert client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"hello"
    assert storage.object_exists(client, bucket, key)
    client.delete_object(Bucket=bucket, Key=key)
    assert not storage.object_exists(client, bucket, key)
