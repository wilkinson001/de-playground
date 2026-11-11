"""Point ClickHouse at the gold tables written by Spark (idempotent).

- `iceberg_lake`: a DataLakeCatalog database backed by the Lakekeeper REST catalog — every
  Iceberg table in the `lakehouse` warehouse shows up as `iceberg_lake.`<namespace>.<table>``.
- `delta_gold`: one view per Delta gold table over ClickHouse's `deltaLake` table function.
S3 keys never appear in SQL: Delta views use the `seaweedfs` named collection, and Iceberg data
files are read with the per-endpoint `<s3>` credentials (both in docker/clickhouse/config.d).
"""

from __future__ import annotations

import clickhouse_connect

from de_playground import storage
from de_playground.batch.target import TABLES
from de_playground.config import Settings

S3_INTERNAL = "http://seaweedfs:8333"


def statements(settings: Settings, namespace: str, existing_delta_tables: set[str]) -> list[str]:
    stmts = [
        f"""CREATE DATABASE IF NOT EXISTS iceberg_lake
            ENGINE = DataLakeCatalog('{settings.lakekeeper_url}/catalog')
            SETTINGS catalog_type = 'rest', warehouse = '{settings.iceberg_warehouse}',
                     storage_endpoint = '{S3_INTERNAL}/iceberg'""",
        "CREATE DATABASE IF NOT EXISTS delta_gold",
    ]
    # ClickHouse validates a deltaLake() view against the table's log when the view is created,
    # so on a fresh stack (before the first batch run) the views can't exist yet.
    for table in (t for t in TABLES["gold"] if t in existing_delta_tables):
        url = f"{S3_INTERNAL}/delta/{namespace}_gold/{table}/"
        stmts.append(
            f"CREATE OR REPLACE VIEW delta_gold.{table} AS SELECT * FROM deltaLake(seaweedfs, url = '{url}')"
        )
    return stmts


def existing_delta_gold(settings: Settings, namespace: str) -> set[str]:
    client = storage.s3_client(settings)
    return {
        table
        for table in TABLES["gold"]
        if client.list_objects_v2(Bucket="delta", Prefix=f"{namespace}_gold/{table}/_delta_log/", MaxKeys=1).get("KeyCount")
    }


def run(settings: Settings, namespace: str = "manual") -> None:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port, username="default", password=""
    )
    for stmt in statements(settings, namespace, existing_delta_gold(settings, namespace)):
        client.command(stmt)
