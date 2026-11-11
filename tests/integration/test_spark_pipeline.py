"""The same fixture month through Spark → Iceberg / Delta, checked against DuckDB → DuckLake."""

import math
import uuid
from datetime import datetime

import pytest

from de_playground import storage
from de_playground.batch.engines import ducklake_engine, spark_engine
from de_playground.batch.pipeline import RawSources, run_month
from de_playground.batch.schemas import SCHEMAS
from de_playground.batch.target import Target, table_ref
from de_playground.months import Month
from tests.fixtures.make_fixtures import sample_rows, trip_row, write_trips_parquet, write_zones_csv

pytestmark = pytest.mark.integration

JAN = Month.parse("2025-01")
GOLD = ["daily_zone_stats", "hourly_borough_demand", "payment_tip_stats"]


def cleaning_rows() -> list[dict]:
    """Same mix as the DuckDB unit tests: 6 rule-breakers, an out-of-month trip, a duplicate."""
    t = datetime(2025, 1, 10, 9, 0)
    return [
        trip_row(t, minutes=-5),
        trip_row(t, minutes=7 * 60),
        trip_row(t, trip_distance=0.0),
        trip_row(t, trip_distance=250.0),
        trip_row(t, fare_amount=-3.0),
        trip_row(t, passenger_count=9),
        trip_row(datetime(2009, 1, 1)),
        trip_row(t),
        trip_row(t),  # duplicate
        trip_row(t, PULocationID=999),  # unknown zone -> NULL borough
        *sample_rows(JAN),
    ]


@pytest.fixture(scope="module")
def spark(settings):
    return spark_engine(settings)


@pytest.fixture(scope="module")
def raw(settings, tmp_path_factory):
    """Upload fixture files to s3://raw/fixtures/<id>/ and yield (duckdb sources, spark sources)."""
    tmp = tmp_path_factory.mktemp("raw")
    prefix = f"fixtures/{uuid.uuid4().hex[:8]}"
    client = storage.s3_client(settings)
    trips = write_trips_parquet(tmp / "trips.parquet", cleaning_rows(), airport_fee_name="airport_fee")
    zones = write_zones_csv(tmp / "zones.csv")
    client.upload_file(str(trips), "raw", f"{prefix}/trips.parquet")
    client.upload_file(str(zones), "raw", f"{prefix}/zones.csv")
    yield (
        RawSources(f"s3://raw/{prefix}/trips.parquet", f"s3://raw/{prefix}/zones.csv"),
        RawSources(f"s3a://raw/{prefix}/trips.parquet", f"s3a://raw/{prefix}/zones.csv"),
    )
    for key in ("trips.parquet", "zones.csv"):
        client.delete_object(Bucket="raw", Key=f"{prefix}/{key}")


def _drop(engine, target: Target, settings) -> None:
    if target.format == "delta":
        client = storage.s3_client(settings)
        for page in client.get_paginator("list_objects_v2").paginate(Bucket="delta", Prefix=f"{target.namespace}_"):
            for obj in page.get("Contents", []):
                client.delete_object(Bucket="delta", Key=obj["Key"])
        return
    for schema in SCHEMAS.values():
        engine.execute(f"DROP TABLE IF EXISTS {table_ref(target, schema.layer, schema.name)}")
    for layer in ("bronze", "silver", "gold"):
        kind = "SCHEMA" if target.format == "ducklake" else "NAMESPACE"
        catalog = "lake" if target.format == "ducklake" else "iceberg"
        engine.execute(f"DROP {kind} IF EXISTS {catalog}.{target.namespace}_{layer}")


@pytest.fixture(params=["iceberg", "delta"])
def spark_target(request, spark, settings):
    target = Target("spark", request.param, f"itest_{request.param}_{uuid.uuid4().hex[:6]}")
    yield target
    _drop(spark, target, settings)


@pytest.fixture
def duck_target(settings):
    engine = ducklake_engine(settings)
    target = Target("duckdb", "ducklake", f"itest_duck_{uuid.uuid4().hex[:6]}")
    yield engine, target
    _drop(engine, target, settings)


def test_spark_pipeline_layer_counts(spark, spark_target, raw):
    counts = run_month(spark, spark_target, JAN, raw[1])
    # 15 raw rows; silver keeps sample_rows (5) + one of the duplicates + the unknown-zone trip.
    assert counts["bronze"] == 15
    assert counts["silver"] == 7


def test_spark_rerun_month_is_idempotent(spark, spark_target, raw):
    run_month(spark, spark_target, JAN, raw[1])
    before = {s.name: spark.query(f"SELECT count(*) FROM {table_ref(spark_target, s.layer, s.name)}")
              for s in SCHEMAS.values()}
    run_month(spark, spark_target, JAN, raw[1])
    after = {s.name: spark.query(f"SELECT count(*) FROM {table_ref(spark_target, s.layer, s.name)}")
             for s in SCHEMAS.values()}
    assert after == before


def _rows(engine, target: Target, table: str) -> list[tuple]:
    schema = SCHEMAS[table]
    cols = ", ".join(schema.column_names)
    keys = ", ".join(f"{k} NULLS LAST" for k in schema.keys)  # engines differ on default NULL order
    return engine.query(f"SELECT {cols} FROM {table_ref(target, schema.layer, table)} ORDER BY {keys}")


def _same(a, b) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        return (a is None and b is None) or (a is not None and b is not None and math.isclose(a, b, abs_tol=1e-9))
    return a == b


def test_spark_gold_matches_duckdb_gold(spark, spark_target, duck_target, raw):
    duck, dtarget = duck_target
    run_month(duck, dtarget, JAN, raw[0])
    run_month(spark, spark_target, JAN, raw[1])

    for table in GOLD:
        d_rows, s_rows = _rows(duck, dtarget, table), _rows(spark, spark_target, table)
        assert len(d_rows) == len(s_rows), table
        for d, s in zip(d_rows, s_rows):
            assert all(_same(x, y) for x, y in zip(d, s)), (table, d, s)

    d_ids = {r[0] for r in duck.query(f"SELECT trip_id FROM {table_ref(dtarget, 'silver', 'trips')}")}
    s_ids = {r[0] for r in spark.query(f"SELECT trip_id FROM {table_ref(spark_target, 'silver', 'trips')}")}
    assert d_ids == s_ids
