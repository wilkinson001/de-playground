"""SQL engines the batch pipeline runs on.

The pipeline itself is engine-agnostic: it renders a SQL template for the engine's dialect and
hands it to `execute`. Each engine only has to know how to run SQL and how to expose raw files
as temporary views.
"""

from __future__ import annotations

import tempfile
from typing import Literal, Protocol

import duckdb

from de_playground.config import Settings

Dialect = Literal["duckdb", "spark"]


class SqlEngine(Protocol):
    dialect: Dialect

    def execute(self, sql: str) -> None: ...

    def query(self, sql: str) -> list[tuple]: ...

    def parquet_columns(self, uri: str) -> list[str]: ...

    def register_parquet_view(self, name: str, uri: str, select_sql: str) -> None:
        """Create temp view `name` as `select_sql`, evaluated over a relation `src` reading the file."""
        ...

    def register_csv_view(self, name: str, uri: str) -> None: ...

    def stage(self, view: str) -> str:
        """Prepare a view to be a MERGE source; returns the relation name to merge from."""
        ...


def split_statements(sql: str) -> list[str]:
    """Split on `;` outside single/double/back-quoted strings; drop empty statements and `--` comments."""
    statements, current, quote = [], [], None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if quote:
            current.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            current.append(ch)
        elif ch == "-" and sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end == -1 else end
            continue
        elif ch == ";":
            statements.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


class DuckDBEngine:
    dialect: Dialect = "duckdb"

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def execute(self, sql: str) -> None:
        for statement in split_statements(sql):
            self.con.execute(statement)

    def query(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()

    def parquet_columns(self, uri: str) -> list[str]:
        rel = self.con.execute("SELECT * FROM read_parquet(?) LIMIT 0", [uri])
        return [d[0] for d in rel.description]

    def register_parquet_view(self, name: str, uri: str, select_sql: str) -> None:
        self.con.execute(
            f"CREATE OR REPLACE TEMP VIEW {name} AS "
            f"WITH src AS (SELECT * FROM read_parquet('{uri}')) {select_sql}"
        )

    def register_csv_view(self, name: str, uri: str) -> None:
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_csv('{uri}', header = true)")

    def stage(self, view: str) -> str:
        # Materialise first: a MERGE whose source is a view evaluates the view's window/dedup
        # inside the MERGE, where DuckDB can't spill, and a full TLC month runs out of memory.
        staged = f"{view}_staged"
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE {staged} AS SELECT * FROM {view}")
        return staged


def connect_ducklake(
    catalog_uri: str, data_path: str, settings: Settings | None = None, memory_limit: str = "2GB"
) -> DuckDBEngine:
    """Open an in-memory DuckDB session with a DuckLake catalog attached as `lake` and selected."""
    con = duckdb.connect()
    # An in-memory database has nowhere to spill by default, so large MERGEs would fail with
    # out-of-memory; give it a spill directory and an explicit cap below the container limit.
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute(f"SET temp_directory = '{tempfile.gettempdir()}/duckdb_spill'")
    for ext in ("ducklake", "postgres", "httpfs"):
        con.execute(f"INSTALL {ext}; LOAD {ext}")  # INSTALL is a no-op when already present
    con.execute("SET TimeZone = 'UTC'")
    if settings is not None:
        con.execute(
            f"""CREATE OR REPLACE SECRET playground_s3 (
                TYPE s3, KEY_ID '{settings.s3_access_key}', SECRET '{settings.s3_secret_key}',
                ENDPOINT '{settings.s3_host_port()}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"""
        )
    con.execute(f"ATTACH '{catalog_uri}' AS lake (DATA_PATH '{data_path}')")
    con.execute("USE lake")
    return DuckDBEngine(con)


def ducklake_engine(settings: Settings) -> DuckDBEngine:
    """DuckLake with its catalog in Postgres and data files in the `ducklake` bucket."""
    return connect_ducklake(settings.ducklake_attach_uri(), "s3://ducklake/", settings)


class SparkEngine:
    """Runs SQL on the Spark Connect server; only a thin gRPC client lives in this process."""

    dialect: Dialect = "spark"

    def __init__(self, spark):  # pyspark.sql.connect.session.SparkSession
        self.spark = spark

    def execute(self, sql: str) -> None:
        for statement in split_statements(sql):
            self.spark.sql(statement).collect()  # collect() forces execution of commands and DML

    def query(self, sql: str) -> list[tuple]:
        return [tuple(row) for row in self.spark.sql(sql).collect()]

    def parquet_columns(self, uri: str) -> list[str]:
        return self.spark.read.parquet(uri).columns

    def register_parquet_view(self, name: str, uri: str, select_sql: str) -> None:
        self.spark.read.parquet(uri).createOrReplaceTempView("src")
        self.spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {select_sql}").collect()

    def stage(self, view: str) -> str:
        return view  # Spark plans the view into the MERGE and spills as needed

    def register_csv_view(self, name: str, uri: str) -> None:
        self.spark.read.option("header", True).option("inferSchema", True).csv(uri).createOrReplaceTempView(name)


def spark_engine(settings: Settings) -> SparkEngine:
    from pyspark.sql import SparkSession

    return SparkEngine(SparkSession.builder.remote(settings.spark_remote).getOrCreate())
