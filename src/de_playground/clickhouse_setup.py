"""Point ClickHouse at the gold tables written by Spark (idempotent).

- `iceberg_lake`: a DataLakeCatalog database backed by the Lakekeeper REST catalog — every
  Iceberg table in the `lakehouse` warehouse shows up as `iceberg_lake.`<namespace>.<table>``.
- `delta_gold`: one view per Delta gold table over ClickHouse's `deltaLake` table function.
S3 keys never appear in SQL: Delta views use the `seaweedfs` named collection, and Iceberg data
files are read with the per-endpoint `<s3>` credentials (both in docker/clickhouse/config.d).
"""

from __future__ import annotations

import clickhouse_connect

from de_playground.batch.target import TABLES
from de_playground.config import Settings

S3_INTERNAL = "http://seaweedfs:8333"


def statements(settings: Settings, namespace: str) -> list[str]:
    stmts = [
        f"""CREATE DATABASE IF NOT EXISTS iceberg_lake
            ENGINE = DataLakeCatalog('{settings.lakekeeper_url}/catalog')
            SETTINGS catalog_type = 'rest', warehouse = '{settings.iceberg_warehouse}',
                     storage_endpoint = '{S3_INTERNAL}/iceberg'""",
        "CREATE DATABASE IF NOT EXISTS delta_gold",
    ]
    for table in TABLES["gold"]:
        url = f"{S3_INTERNAL}/delta/{namespace}_gold/{table}/"
        stmts.append(
            f"CREATE OR REPLACE VIEW delta_gold.{table} AS SELECT * FROM deltaLake(seaweedfs, url = '{url}')"
        )
    return stmts


def run(settings: Settings, namespace: str = "manual") -> None:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port, username="default", password=""
    )
    for stmt in statements(settings, namespace):
        client.command(stmt)
