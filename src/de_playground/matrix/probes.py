"""Probe what each engine can actually do with each table format, against the running stack.

Every engine × format pair runs the same five operations on a 2-column table `(id INT, v STRING)`
in namespace `matrix`: create, append, overwrite (the engine's idiomatic replace-all), merge (a SQL
or API upsert) and read (count a seed table another engine wrote). Whatever raises is recorded as a
failure with its first error line; nothing is assumed from docs.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa

from de_playground import storage
from de_playground.config import Settings

Op = Literal["create", "append", "overwrite", "merge", "read"]
OPS: tuple[Op, ...] = ("create", "append", "overwrite", "merge", "read")
FORMATS = ("ducklake", "iceberg", "delta")
ENGINES = ("duckdb", "spark", "flink", "python", "clickhouse")
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

        self.table = {"ducklake": "duckdb_t", "iceberg": "duckdb_t", "delta": "duckdb_t"}

    def unsupported(self, fmt: str) -> str | None:
        return None

    def use_spark_table(self, fmt: str) -> None:
        self.table[fmt] = "duckdb_writable"

    def _ref(self, fmt: str, table: str | None = None) -> str:
        table = table or self.table[fmt]
        if fmt == "ducklake":
            return f"lake.matrix.{table}"
        if fmt == "iceberg":
            if self.iceberg_error:
                raise RuntimeError(self.iceberg_error)
            return f"ice.matrix.{table}"
        # Delta: the delta extension attaches one table (by path) as a database alias.
        alias = f"delta_{table}"
        if not self.engine.query(f"SELECT 1 FROM duckdb_databases() WHERE database_name = '{alias}'"):
            self.engine.execute(f"ATTACH 's3://delta/matrix/{table}' AS {alias} (TYPE delta)")
        return alias

    def create(self, fmt: str) -> None:
        if fmt == "delta":
            # No CREATE TABLE for Delta: attaching a fresh path is the only way to start one.
            self._ref(fmt)
            return
        ref = self._ref(fmt)
        self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {ref.rsplit('.', 1)[0]}")
        self.engine.execute(f"DROP TABLE IF EXISTS {ref}")
        self.engine.execute(f"CREATE TABLE {ref} (id INTEGER, v VARCHAR)")

    def count(self, fmt: str) -> int:
        return self.engine.query(f"SELECT count(*) FROM {self._ref(fmt)}")[0][0]

    def append(self, fmt: str) -> None:
        self.engine.execute(f"INSERT INTO {self._ref(fmt)} VALUES (1, 'a'), (2, 'b')")

    def overwrite(self, fmt: str) -> None:
        ref = self._ref(fmt)
        self.engine.execute(f"BEGIN; DELETE FROM {ref}; INSERT INTO {ref} VALUES (10, 'x'); COMMIT")

    def merge(self, fmt: str) -> None:
        self.engine.execute(
            f"MERGE INTO {self._ref(fmt)} t USING (SELECT 10 AS id, 'y' AS v UNION ALL SELECT 11, 'z') {_MERGE_TAIL}"
        )

    def read(self, fmt: str) -> int:
        return self.engine.query(f"SELECT count(*) FROM {self._ref(fmt, 'seed')}")[0][0]

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

    def count(self, fmt: str) -> int:
        return self.engine.query(f"SELECT count(*) FROM {self.ref(fmt)}")[0][0]

    def use_spark_table(self, fmt: str) -> None:
        pass  # Spark is the engine that creates the fallback tables

    def write_seeds(self) -> None:
        """Seed tables every engine reads, plus an empty `<engine>_writable` table per engine so an
        engine that can't create tables can still have its writes probed."""
        self.engine.execute("CREATE NAMESPACE IF NOT EXISTS iceberg.matrix")
        for fmt in ("iceberg", "delta"):
            self.engine.execute(
                f"CREATE OR REPLACE TABLE {self.ref(fmt, 'seed')} USING {fmt} AS "
                "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS s(id, v)"
            )
            for engine in ("duckdb", "flink", "python", "clickhouse"):
                self.engine.execute(f"CREATE OR REPLACE TABLE {self.ref(fmt, f'{engine}_writable')} (id INT, v STRING) USING {fmt}")


class FlinkAdapter:
    """Flink SQL in batch mode through the SQL Gateway. Delta is sink-only in Flink: the connector
    writes (and creates the table on first write) but can't read, so counts use DuckDB."""

    name = "flink"

    def __init__(self, settings: Settings):
        from de_playground.batch.engines import connect_ducklake
        from de_playground.streaming.flink import statements
        from de_playground.streaming.flink_gateway import FlinkSql

        self.statements = statements
        self.s3_client = storage.s3_client(settings)
        self.sql = FlinkSql(settings, {"execution.runtime-mode": "batch"})
        self.sql.open()
        self.sql.execute(statements.iceberg_catalog_ddl(settings.lakekeeper_url, settings.iceberg_warehouse,
                                                        settings.s3_endpoint, settings.s3_access_key,
                                                        settings.s3_secret_key))
        tmp = tempfile.mkdtemp(prefix="matrix_flink_")
        self.duck = connect_ducklake(f"ducklake:{tmp}/c.ducklake", f"{tmp}/data/", settings)
        self.duck.execute("INSTALL delta; LOAD delta")
        self.table = {"iceberg": "flink_t", "delta": "flink_t"}

    def unsupported(self, fmt: str) -> str | None:
        return "no Flink connector for DuckLake" if fmt == "ducklake" else None

    def use_spark_table(self, fmt: str) -> None:
        self.table[fmt] = "flink_writable"

    def _ref(self, fmt: str, table: str | None = None) -> str:
        table = table or self.table[fmt]
        if fmt == "iceberg":
            return f"lakehouse.matrix.{table}"
        # Delta tables are reached through a temporary table bound to the path.
        alias = f"delta_{table}"
        self.sql.execute(f"CREATE TEMPORARY TABLE IF NOT EXISTS {alias} (id INT, v STRING) WITH ("
                         f"{self.statements.delta_options(f's3a://delta/matrix/{table}')})")
        return alias

    def create(self, fmt: str) -> None:
        if fmt == "delta":
            # No CREATE for Delta: the connector creates the table on its first write.
            storage.delete_prefix(self.s3_client, "delta", "matrix/flink_t/")
            self._ref(fmt)
            return
        self.sql.execute("CREATE DATABASE IF NOT EXISTS lakehouse.matrix")
        self.sql.execute("DROP TABLE IF EXISTS lakehouse.matrix.flink_t")
        self.sql.execute("CREATE TABLE lakehouse.matrix.flink_t (id INT, v STRING)")

    def count(self, fmt: str) -> int:
        if fmt == "delta":
            path = f"delta/matrix/{self.table[fmt]}"
            if not self.s3_client.list_objects_v2(Bucket="delta", Prefix=f"matrix/{self.table[fmt]}/_delta_log/").get("KeyCount"):
                raise RuntimeError("no Delta table exists until the first write: the connector has no CREATE")
            return self.duck.query(f"SELECT count(*) FROM delta_scan('s3://{path}')")[0][0]
        return self.sql.execute(f"SELECT count(*) FROM {self._ref(fmt)}")[0][0]

    def append(self, fmt: str) -> None:
        self.sql.execute(f"INSERT INTO {self._ref(fmt)} VALUES (1, 'a'), (2, 'b')")

    def overwrite(self, fmt: str) -> None:
        self.sql.execute(f"INSERT OVERWRITE {self._ref(fmt)} VALUES (10, 'x')")

    def merge(self, fmt: str) -> None:
        self.sql.execute(
            f"MERGE INTO {self._ref(fmt)} t USING (SELECT 10 AS id, 'y' AS v UNION ALL SELECT 11, 'z') {_MERGE_TAIL}"
        )

    def read(self, fmt: str) -> int:
        return self.sql.execute(f"SELECT count(*) FROM {self._ref(fmt, 'seed')}")[0][0]


class PythonAdapter:
    """Pure-Python libraries: delta-rs (`deltalake`) and PyIceberg — no JVM, no SQL engine."""

    name = "python"

    def __init__(self, settings: Settings):
        self.storage_options = storage.delta_storage_options(settings)
        self.catalog = storage.iceberg_catalog(settings)

    def unsupported(self, fmt: str) -> str | None:
        if fmt == "ducklake":
            return "DuckLake is used from Python through DuckDB (see the duckdb row)"
        return None

    table = "python_t"

    @property
    def _URI(self) -> str:
        return f"s3://delta/matrix/{self.table}"

    def use_spark_table(self, fmt: str) -> None:
        self.table = "python_writable"

    def count(self, fmt: str) -> int:
        if fmt == "delta":
            from deltalake import DeltaTable

            return DeltaTable(self._URI, storage_options=self.storage_options).to_pyarrow_table().num_rows
        return self.catalog.load_table(f"matrix.{self.table}").scan().to_arrow().num_rows

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
            table = self.catalog.load_table(f"matrix.{self.table}")
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
        self.s3 = clickhouse_setup.S3_INTERNAL
        self.s3_client = storage.s3_client(settings)
        self.iceberg_on_spark_table = False

    def unsupported(self, fmt: str) -> str | None:
        if fmt == "ducklake":
            return "ClickHouse has no DuckLake support"
        return None

    # Shown next to a ✅ so the cell isn't over-read.
    caveats = {("iceberg", "create"): "path-based (IcebergS3 engine); not registered in the Lakekeeper catalog"}

    def _cmd(self, sql: str) -> None:
        self.client.command(sql, settings=self.settings)

    def _ref(self, fmt: str) -> str:
        if fmt == "iceberg" and self.iceberg_on_spark_table:
            # Spark-created Iceberg tables live in Lakekeeper: reach them through the catalog database.
            return "iceberg_lake.`matrix.clickhouse_writable`"
        return f"default.matrix_{fmt}_t"

    def _create_over(self, fmt: str, path: str) -> None:
        # A ClickHouse table whose storage is a lake table at `path` (outside the Lakekeeper catalog).
        engine = "IcebergS3" if fmt == "iceberg" else "DeltaLake"
        bucket = "iceberg" if fmt == "iceberg" else "delta"
        self._cmd(
            f"CREATE OR REPLACE TABLE {self._ref(fmt)} (id Int32, v String) "
            f"ENGINE = {engine}(seaweedfs, url = '{self.s3}/{bucket}/{path}/')"
        )

    def create(self, fmt: str) -> None:
        # Path-based lake tables have no DROP that removes files: clear the previous run's table.
        storage.delete_prefix(self.s3_client, "iceberg" if fmt == "iceberg" else "delta", "matrix/clickhouse_t/")
        self._create_over(fmt, "matrix/clickhouse_t")

    def count(self, fmt: str) -> int:
        return self.client.query(f"SELECT count() FROM {self._ref(fmt)}", settings=self.settings).result_rows[0][0]

    def use_spark_table(self, fmt: str) -> None:
        if fmt == "iceberg":
            self.iceberg_on_spark_table = True
        else:
            self._create_over(fmt, "matrix/clickhouse_writable")

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
            else f"deltaLake(seaweedfs, url = '{self.s3}/delta/matrix/seed/')"
        )
        return self.client.query(f"SELECT count() FROM {source}").result_rows[0][0]


SPARK_TABLE_CAVEAT = "on a table created by Spark (this engine couldn't create one)"


def _probe(adapter, fmt: str, op: Op, on_spark_table: bool) -> ProbeResult:
    reason = adapter.unsupported(fmt)
    if reason:
        return ProbeResult(adapter.name, fmt, op, "n/a", reason)
    try:
        result = getattr(adapter, op)(fmt)
        if op == "create":
            adapter.count(fmt)  # a create only counts if the new table can be queried
        if op == "read" and result != SEED_ROWS:
            return ProbeResult(adapter.name, fmt, op, "fail", f"read {result} rows, expected {SEED_ROWS}")
        detail = SPARK_TABLE_CAVEAT if on_spark_table and op in ("append", "overwrite", "merge") else ""
        detail = detail or getattr(adapter, "caveats", {}).get((fmt, op), "")
        return ProbeResult(adapter.name, fmt, op, "ok", detail)
    except Exception as e:
        if hasattr(adapter, "close"):
            adapter.close()
        detail = _first_line(e)
        if on_spark_table and op in ("append", "overwrite", "merge"):
            detail = f"{detail} ({SPARK_TABLE_CAVEAT})"
        return ProbeResult(adapter.name, fmt, op, "fail", detail)


def run_all(settings: Settings, adapters: dict[str, Callable[[Settings], object]] | None = None) -> list[ProbeResult]:
    factories = adapters or {
        "duckdb": DuckDBAdapter,
        "spark": SparkAdapter,
        "flink": FlinkAdapter,
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
            on_spark_table = False
            for op in OPS:
                result = _probe(built[name], fmt, op, on_spark_table)
                if op == "create" and result.status == "fail":
                    try:  # still probe this engine's writes, against a table Spark created for it
                        built[name].use_spark_table(fmt)
                        on_spark_table = True
                    except Exception as e:
                        result = ProbeResult(name, fmt, op, "fail", f"{result.detail}; fallback: {_first_line(e)}")
                results.append(result)
    return results
