import uuid

import httpx
import pyarrow as pa
import pytest
from pyiceberg.catalog import load_catalog

from de_playground import bootstrap, storage

pytestmark = pytest.mark.integration


def test_bootstrap_creates_buckets_and_warehouse(settings):
    bootstrap.run(settings)
    client = storage.s3_client(settings)
    for bucket in bootstrap.BUCKETS:
        client.head_bucket(Bucket=bucket)
    r = httpx.get(
        f"{settings.lakekeeper_url}/catalog/v1/config",
        params={"warehouse": settings.iceberg_warehouse},
    )
    assert r.status_code == 200, r.text


def test_bootstrap_is_idempotent(settings):
    bootstrap.run(settings)
    report = bootstrap.run(settings)
    assert report == bootstrap.BootstrapReport(
        buckets_created=[], lakekeeper_bootstrapped=False, warehouse_created=False
    )


def test_iceberg_namespace_roundtrip_via_rest(settings):
    bootstrap.run(settings)
    catalog = load_catalog(
        "lk",
        type="rest",
        uri=f"{settings.lakekeeper_url}/catalog",
        warehouse=settings.iceberg_warehouse,
        **{
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )
    ns = f"smoke_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace(ns)
    try:
        data = pa.table({"id": pa.array([1], pa.int32())})
        table = catalog.create_table(f"{ns}.t", schema=data.schema)
        table.append(data)
        assert table.scan().to_arrow().column("id").to_pylist() == [1]
        catalog.drop_table(f"{ns}.t")
    finally:
        catalog.drop_namespace(ns)
