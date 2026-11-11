import uuid

import pytest

from de_playground import storage
from de_playground.batch.engines import spark_engine
from de_playground.months import Month
from de_playground.sources import tlc

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(settings):
    return spark_engine(settings)


def test_spark_iceberg_roundtrip(spark):
    ns = f"iceberg.smoke_{uuid.uuid4().hex[:8]}"
    spark.execute(f"CREATE NAMESPACE IF NOT EXISTS {ns}; CREATE TABLE {ns}.t (id INT, v STRING) USING iceberg")
    try:
        spark.execute(f"INSERT INTO {ns}.t VALUES (1, 'a'), (2, 'b')")
        spark.execute(
            f"MERGE INTO {ns}.t t USING (SELECT 2 AS id, 'B' AS v UNION ALL SELECT 3, 'c') s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )
        assert spark.query(f"SELECT id, v FROM {ns}.t ORDER BY id") == [(1, "a"), (2, "B"), (3, "c")]
    finally:
        # No PURGE: Lakekeeper's hard delete profile removes the files itself, and a client-side
        # PURGE races it (reads manifests Lakekeeper is deleting -> intermittent S3 404s).
        spark.execute(f"DROP TABLE IF EXISTS {ns}.t; DROP NAMESPACE IF EXISTS {ns}")


def test_spark_delta_path_roundtrip(spark, settings):
    prefix = f"smoke_{uuid.uuid4().hex[:8]}"
    ref = f"delta.`s3a://delta/{prefix}/t`"
    spark.execute(f"CREATE TABLE IF NOT EXISTS {ref} (id INT, v STRING) USING delta")
    spark.execute(f"INSERT INTO {ref} VALUES (1, 'a'), (2, 'b')")
    spark.execute(
        f"MERGE INTO {ref} t USING (SELECT 2 AS id, 'B' AS v) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    )
    assert spark.query(f"SELECT id, v FROM {ref} ORDER BY id") == [(1, "a"), (2, "B")]
    listed = storage.s3_client(settings).list_objects_v2(Bucket="delta", Prefix=f"{prefix}/t/_delta_log/")
    assert listed.get("KeyCount", 0) > 0


def test_spark_reads_raw_parquet(spark, settings):
    jan = Month.parse("2025-01")
    if not storage.object_exists(storage.s3_client(settings), tlc.RAW_BUCKET, tlc.raw_trips_key(jan)):
        pytest.skip("run `make download MONTHS=2025-01` first")
    cols = spark.parquet_columns(f"s3a://raw/{tlc.raw_trips_key(jan)}")
    assert "tpep_pickup_datetime" in cols
