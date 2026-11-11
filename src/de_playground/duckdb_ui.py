"""DuckDB's web UI over everything the playground has written (`make duckdb-ui`).

Runs in Docker (`make duckdb-ui`, the `duckdb-ui` service) or directly on the host
(`make duckdb-ui-local`). The UI server only listens on the IPv6 loopback, so in Docker a socat
relay publishes it (see compose.yaml). The UI keeps notebooks in its own state database under
~/.duckdb/extension_data/ui (mounted from .data/duckdb-ui/ui-state in Docker); DB_FILE is just
the writable database the UI runs on, which it requires.

Every table format is reachable with the same naming pattern, `<catalog>.<ns>_<layer>.<table>`:
    lake.*     DuckLake (catalog in Postgres, data in SeaweedFS)
    iceberg.*  every Iceberg table, through the Lakekeeper REST catalog
    delta.*    views over the Delta tables found in the `delta` bucket (refreshed at start-up)
"""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from pathlib import Path

from de_playground import storage
from de_playground.config import Settings

DB_FILE = Path(".data/duckdb-ui/playground.duckdb")
UI_URL = "http://localhost:4213"
_HOST_DEFAULTS = {
    "PLAYGROUND_S3_ENDPOINT": "http://localhost:8333",
    "PLAYGROUND_PG_HOST": "localhost",
    "PLAYGROUND_LAKEKEEPER_URL": "http://localhost:8181",
}


def host_settings(env: Mapping[str, str] = os.environ) -> Settings:
    """Settings for code running on the host: services via their 127.0.0.1 ports."""
    return Settings.from_env({**_HOST_DEFAULTS, **env})


def discover_delta_tables(client) -> list[tuple[str, str]]:
    """(schema, table) for every `<schema>/<table>/_delta_log/` in the delta bucket."""
    found = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket="delta"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) >= 4 and parts[2] == "_delta_log":
                found.add((parts[0], parts[1]))
    return sorted(found)


def setup_statements(settings: Settings, delta_tables: list[tuple[str, str]]) -> list[str]:
    stmts = [
        "INSTALL ui; LOAD ui; INSTALL httpfs; LOAD httpfs; INSTALL ducklake; LOAD ducklake; "
        "INSTALL postgres; LOAD postgres; INSTALL iceberg; LOAD iceberg; INSTALL delta; LOAD delta",
        "SET TimeZone = 'UTC'",
        f"""CREATE OR REPLACE SECRET playground_s3 (
            TYPE s3, KEY_ID '{settings.s3_access_key}', SECRET '{settings.s3_secret_key}',
            ENDPOINT '{settings.s3_host_port()}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""",
        f"ATTACH IF NOT EXISTS '{settings.ducklake_attach_uri()}' AS lake (DATA_PATH 's3://ducklake/')",
        # Lakekeeper vends no credentials here: use our own S3 secret for data files.
        f"ATTACH IF NOT EXISTS '{settings.iceberg_warehouse}' AS iceberg (TYPE iceberg, "
        f"ENDPOINT '{settings.lakekeeper_url}/catalog', AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')",
        # Delta has no catalog: an in-memory database of views gives it the same naming as the others.
        "ATTACH IF NOT EXISTS ':memory:' AS delta",
    ]
    for schema in sorted({s for s, _ in delta_tables}):
        stmts.append(f"CREATE SCHEMA IF NOT EXISTS delta.{schema}")
    for schema, table in delta_tables:
        stmts.append(
            f"CREATE OR REPLACE VIEW delta.{schema}.{table} AS SELECT * FROM delta_scan('s3://delta/{schema}/{table}')"
        )
    return stmts


def main() -> None:
    import duckdb

    settings = host_settings()
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_FILE))
    delta_tables = discover_delta_tables(storage.s3_client(settings))
    for statement in setup_statements(settings, delta_tables):
        con.execute(statement)
    con.execute("CALL start_ui_server()")
    print(f"DuckDB UI: {UI_URL}  (catalogs: lake, iceberg, delta with {len(delta_tables)} tables) - Ctrl+C to stop", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        con.execute("CALL stop_ui_server()")
        con.close()


if __name__ == "__main__":
    main()
