"""Probe what each engine can actually do with each table format, against the running stack.

Every engine × format pair runs the same five operations on a 2-column table `(id INT, v STRING)`
in namespace `matrix`: create, append, overwrite (the engine's idiomatic replace-all), merge (a SQL
or API upsert) and read (count a seed table another engine wrote). Whatever raises is recorded as a
failure with its first error line; nothing is assumed from docs.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from de_playground.config import Settings

Op = Literal["create", "append", "overwrite", "merge", "read"]
OPS: tuple[Op, ...] = ("create", "append", "overwrite", "merge", "read")
FORMATS = ("ducklake", "iceberg", "delta")
ENGINES = ("duckdb", "spark", "python", "clickhouse")
SEED_ROWS = 3

_SCHEMA = pa.schema([("id", pa.int32()), ("v", pa.string())])
_MERGE_TAIL = (
    "s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
)


@dataclass(frozen=True)
class ProbeResult:
    engine: str
    format: str
    op: str
    status: Literal["ok", "fail", "n/a"]
    detail: str


def _first_line(e: BaseException) -> str:
    text = str(e).strip() or type(e).__name__
    return text.splitlines()[0][:300]


def _s3_base(settings: Settings) -> str:
    return settings.s3_endpoint


class DuckDBAdapter:
    name = "duckdb"

    def __init__(self, settings: Settings):
        from de_playground.batch.engines import ducklake_engine

        self.engine = ducklake_engine(settings)
        self.engine.execute("INSTALL iceberg; LOAD iceberg; INSTALL delta; LOAD delta")
        self.iceberg_error: str | None = None
        try:
            self.engine.execute(
                f"ATTACH '{settings.iceberg_warehouse}' AS ice "
                f"(TYPE iceberg, ENDPOINT '{settings.lakekeeper_url}/catalog', AUTHORIZATION_TYPE 'none', "
                # Lakekeeper vends no credentials here; without this DuckDB uses the empty vended
                # credential instead of our S3 secret and every data-file access is a 403.
                "ACCESS_DELEGATION_MODE 'none')"
            )
        except Exception as e:  # recorded against every Iceberg op
            self.iceberg_error = f"ATTACH failed: {_first_line(e)}"

    def unsupported(self, fmt: str) -> str | None:
        return None

    def _ref(self, fmt: str, table: str = "duckdb_t") -> str:
        if fmt == "iceberg" and self.iceberg_error:
            raise RuntimeError(self.iceberg_error)
        return {"ducklake": f"lake.matrix.{table}", "iceberg": f"ice.matrix.{table}"}.get(fmt, "")

    def create(self, fmt: str) -> None:
        if fmt == "delta":
            # The delta extension has no CREATE; try writing a new table with COPY.
            self.engine.execute("COPY (SELECT 1::INTEGER AS id, 'a' AS v) TO 's3://delta/matrix/duckdb_t' (FORMAT delta)")
            return
        ref = self._ref(fmt)
        self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {ref.rsplit('.', 1)[0]}")
        self.engine.execute(f"DROP TABLE IF EXISTS {ref}")
        self.engine.execute(f"CREATE TABLE {ref} (id INTEGER, v VARCHAR)")

    def append(self, fmt: str) -> None:
        self.engine.execute(f"INSERT INTO {self._ref(fmt) or 'delta_scan(\'s3://delta/matrix/duckdb_t\')'} VALUES (1, 'a'), (2, 'b')")

    def overwrite(self, fmt: str) -> None:
        ref = self._ref(fmt) or "delta_scan('s3://delta/matrix/duckdb_t')"
        self.engine.execute(f"BEGIN; DELETE FROM {ref}; INSERT INTO {ref} VALUES (10, 'x'); COMMIT")

    def merge(self, fmt: str) -> None:
        ref = self._ref(fmt) or "delta_scan('s3://delta/matrix/duckdb_t')"
        self.engine.execute(f"MERGE INTO {ref} t USING (SELECT 10 AS id, 'y' AS v UNION ALL SELECT 11, 'z') {_MERGE_TAIL}")

    def read(self, fmt: str) -> int:
        ref = self._ref(fmt, "seed") or "delta_scan('s3://delta/matrix/seed')"
        return self.engine.query(f"SELECT count(*) FROM {ref}")[0][0]

    def close(self) -> None:
        try:  # a failed BEGIN..COMMIT can leave a transaction open
            self.engine.execute("ROLLBACK")
        except Exception:
            pass


class SparkAdapter:
    name = "spark"

    def __init__(self, settings: Settings):
        from de_playground.batch.engines import spark_engine

        self.engine = spark_engine(settings)

    def unsupported(self, fmt: str) -> str | None:
        if fmt == "ducklake":
            return "no usable Spark connector (MotherDuck's is write-only/append-only and Spark 3 only)"
        return None

    @staticmethod
    def ref(fmt: str, table: str = "spark_t") -> str:
        return f"iceberg.matrix.{table}" if fmt == "iceberg" else f"delta.`s3a://delta/matrix/{table}`"

    def create(self, fmt: str) -> None:
        if fmt == "iceberg":
            self.engine.execute("CREATE NAMESPACE IF NOT EXISTS iceberg.matrix")
        self.engine.execute(f"CREATE OR REPLACE TABLE {self.ref(fmt)} (id INT, v STRING) USING {fmt}")

    def append(self, fmt: str) -> None:
        self.engine.execute(f"INSERT INTO {self.ref(fmt)} VALUES (1, 'a'), (2, 'b')")

    def overwrite(self, fmt: str) -> None:
        self.engine.execute(f"INSERT OVERWRITE {self.ref(fmt)} VALUES (10, 'x')")

    def merge(self, fmt: str) -> None:
        self.engine.execute(
            f"MERGE INTO {self.ref(fmt)} t USING (SELECT 10 AS id, 'y' AS v UNION ALL SELECT 11, 'z') {_MERGE_TAIL}"
        )

    def read(self, fmt: str) -> int:
        return self.engine.query(f"SELECT count(*) FROM {self.ref(fmt, 'seed')}")[0][0]

    def write_seeds(self) -> None:
        self.engine.execute("CREATE NAMESPACE IF NOT EXISTS iceberg.matrix")
        for fmt in ("iceberg", "delta"):
            self.engine.execute(
                f"CREATE OR REPLACE TABLE {self.ref(fmt, 'seed')} USING {fmt} AS "
                "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS s(id, v)"
            )


class PythonAdapter:
    """Pure-Python libraries: delta-rs (`deltalake`) and PyIceberg — no JVM, no SQL engine."""

    name = "python"

    def __init__(self, settings: Settings):
        from pyiceberg.catalog import load_catalog

        self.storage_options = {
            "AWS_ENDPOINT_URL": settings.s3_endpoint,
            "AWS_ACCESS_KEY_ID": settings.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        self.catalog = load_catalog(
            "lakekeeper",
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

    def unsupported(self, fmt: str) -> str | None:
        if fmt == "ducklake":
            return "DuckLake is used from Python through DuckDB (see the duckdb row)"
        return None

    _URI = "s3://delta/matrix/python_t"

    def create(self, fmt: str) -> None:
        if fmt == "delta":
            from deltalake import DeltaTable

            DeltaTable.create(self._URI, schema=_SCHEMA, mode="overwrite", storage_options=self.storage_options)
        else:
            self.catalog.create_namespace_if_not_exists("matrix")
            if self.catalog.table_exists("matrix.python_t"):
                self.catalog.drop_table("matrix.python_t")
            self.catalog.create_table("matrix.python_t", schema=_SCHEMA)

    def _write(self, fmt: str, rows: list[tuple[int, str]], mode: str) -> None:
        data = pa.table({"id": pa.array([r[0] for r in rows], pa.int32()), "v": [r[1] for r in rows]}, schema=_SCHEMA)
        if fmt == "delta":
            from deltalake import DeltaTable, write_deltalake

            if mode == "merge":
                (
                    DeltaTable(self._URI, storage_options=self.storage_options)
                    .merge(data, predicate="t.id = s.id", source_alias="s", target_alias="t")
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
            else:
                write_deltalake(self._URI, data, mode=mode, storage_options=self.storage_options)
        else:
            table = self.catalog.load_table("matrix.python_t")
            {"append": table.append, "overwrite": table.overwrite}.get(mode, lambda d: table.upsert(d, join_cols=["id"]))(data)

    def append(self, fmt: str) -> None:
        self._write(fmt, [(1, "a"), (2, "b")], "append")

    def overwrite(self, fmt: str) -> None:
        self._write(fmt, [(10, "x")], "overwrite")

    def merge(self, fmt: str) -> None:
        self._write(fmt, [(10, "y"), (11, "z")], "merge")

    def read(self, fmt: str) -> int:
        if fmt == "delta":
            from deltalake import DeltaTable

            return DeltaTable("s3://delta/matrix/seed", storage_options=self.storage_options).to_pyarrow_table().num_rows
        return self.catalog.load_table("matrix.seed").scan().to_arrow().num_rows


class ClickHouseAdapter:
    name = "clickhouse"
    # Experimental write features, passed only if this ClickHouse version knows them.
    _WANTED_SETTINGS = ("allow_experimental_insert_into_iceberg", "allow_experimental_delta_lake_writes")

    def __init__(self, settings: Settings):
        import clickhouse_connect

        from de_playground import clickhouse_setup

        clickhouse_setup.run(settings)  # makes sure the iceberg_lake database exists
        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host, port=settings.clickhouse_port, username="default", password=""
        )
        known = {r[0] for r in self.client.query("SELECT name FROM system.settings").result_rows}
        self.settings = {s: 1 for s in self._WANTED_SETTINGS if s in known}
        self.delta_url = f"{clickhouse_setup.S3_INTERNAL}/delta/matrix"

    def unsupported(self, fmt: str) -> str | None:
        if fmt == "ducklake":
            return "ClickHouse has no DuckLake support"
        return None

    def _cmd(self, sql: str) -> None:
        self.client.command(sql, settings=self.settings)

    def _ref(self, fmt: str) -> str:
        return "iceberg_lake.`matrix.clickhouse_t`" if fmt == "iceberg" else "default.matrix_delta_t"

    def create(self, fmt: str) -> None:
        if fmt == "iceberg":
            self._cmd(f"CREATE TABLE {self._ref(fmt)} (id Int32, v String)")
        else:
            self._cmd(
                f"CREATE OR REPLACE TABLE {self._ref(fmt)} (id Int32, v String) "
                f"ENGINE = DeltaLake(seaweedfs, url = '{self.delta_url}/clickhouse_t/')"
            )

    def append(self, fmt: str) -> None:
        self._cmd(f"INSERT INTO {self._ref(fmt)} VALUES (1, 'a'), (2, 'b')")

    def overwrite(self, fmt: str) -> None:
        self._cmd(f"TRUNCATE TABLE {self._ref(fmt)}")
        self._cmd(f"INSERT INTO {self._ref(fmt)} VALUES (10, 'x')")

    def merge(self, fmt: str) -> None:
        self._cmd(f"MERGE INTO {self._ref(fmt)} t USING (SELECT 10 AS id, 'y' AS v) {_MERGE_TAIL}")

    def read(self, fmt: str) -> int:
        source = (
            "iceberg_lake.`matrix.seed`"
            if fmt == "iceberg"
            else f"deltaLake(seaweedfs, url = '{self.delta_url}/seed/')"
        )
        return self.client.query(f"SELECT count() FROM {source}").result_rows[0][0]


def _probe(adapter, fmt: str, op: Op, create_failed: bool) -> ProbeResult:
    reason = adapter.unsupported(fmt)
    if reason:
        return ProbeResult(adapter.name, fmt, op, "n/a", reason)
    if create_failed and op in ("append", "overwrite", "merge"):
        return ProbeResult(adapter.name, fmt, op, "fail", "skipped: create failed")
    try:
        result = getattr(adapter, op)(fmt)
        if op == "read" and result != SEED_ROWS:
            return ProbeResult(adapter.name, fmt, op, "fail", f"read {result} rows, expected {SEED_ROWS}")
        return ProbeResult(adapter.name, fmt, op, "ok", "")
    except Exception as e:
        if hasattr(adapter, "close"):
            adapter.close()
        return ProbeResult(adapter.name, fmt, op, "fail", _first_line(e))


def run_all(settings: Settings, adapters: dict[str, Callable[[Settings], object]] | None = None) -> list[ProbeResult]:
    factories = adapters or {
        "duckdb": DuckDBAdapter,
        "spark": SparkAdapter,
        "python": PythonAdapter,
        "clickhouse": ClickHouseAdapter,
    }
    built = {name: factory(settings) for name, factory in factories.items()}

    # Seeds that every engine's `read` probe counts: DuckDB writes DuckLake, Spark writes the others.
    if "duckdb" in built:
        built["duckdb"].engine.execute(
            "CREATE SCHEMA IF NOT EXISTS lake.matrix; "
            "CREATE OR REPLACE TABLE lake.matrix.seed AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) s(id, v)"
        )
    if "spark" in built:
        built["spark"].write_seeds()

    results: list[ProbeResult] = []
    for name in ENGINES:
        if name not in built:
            continue
        for fmt in FORMATS:
            create_failed = False
            for op in OPS:
                result = _probe(built[name], fmt, op, create_failed)
                create_failed = create_failed or (op == "create" and result.status == "fail")
                results.append(result)
    return results
