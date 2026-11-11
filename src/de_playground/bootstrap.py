"""One-time (and safely repeatable) setup: buckets and the Lakekeeper Iceberg warehouse."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import httpx

from de_playground import storage
from de_playground.config import Settings

BUCKETS = ["raw", "ducklake", "iceberg", "delta"]


@dataclass
class BootstrapReport:
    buckets_created: list[str] = field(default_factory=list)
    lakekeeper_bootstrapped: bool = False
    warehouse_created: bool = False


def run(settings: Settings) -> BootstrapReport:
    report = BootstrapReport()
    client = storage.s3_client(settings)
    for bucket in BUCKETS:
        if storage.ensure_bucket(client, bucket):
            report.buckets_created.append(bucket)

    with httpx.Client(base_url=settings.lakekeeper_url, timeout=30) as http:
        _wait_until_up(http)
        if not http.get("/management/v1/info").raise_for_status().json()["bootstrapped"]:
            http.post("/management/v1/bootstrap", json={"accept-terms-of-use": True}).raise_for_status()
            report.lakekeeper_bootstrapped = True

        existing = http.get("/management/v1/warehouse").raise_for_status().json()["warehouses"]
        if not any(w["name"] == settings.iceberg_warehouse for w in existing):
            http.post("/management/v1/warehouse", json=_warehouse_body(settings)).raise_for_status()
            report.warehouse_created = True
    return report


def _warehouse_body(settings: Settings) -> dict:
    # SeaweedFS has no STS, so Lakekeeper neither vends credentials nor remote-signs:
    # every client (Spark, PyIceberg, DuckDB, ClickHouse) brings its own static S3 keys.
    return {
        "warehouse-name": settings.iceberg_warehouse,
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": "iceberg",
            "endpoint": settings.s3_endpoint,
            "region": "us-east-1",
            "path-style-access": True,
            "sts-enabled": False,
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": settings.s3_access_key,
            "aws-secret-access-key": settings.s3_secret_key,
        },
        "delete-profile": {"type": "hard"},
    }


def _wait_until_up(http: httpx.Client, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if http.get("/health").status_code == 200:
                return
        except httpx.TransportError:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"Lakekeeper not healthy at {http.base_url}")
        time.sleep(1)
