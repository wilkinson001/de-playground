"""ClickHouse reads the gold tables written by `make batch` (namespace `manual`)."""

import clickhouse_connect
import pytest

from de_playground import clickhouse_setup
from de_playground.batch.engines import spark_engine

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def ch(settings):
    clickhouse_setup.run(settings, namespace="manual")
    return clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                         username="default", password="")


@pytest.fixture(scope="module")
def spark(settings):
    return spark_engine(settings)


def _spark_sum(spark, ref: str) -> int:
    try:
        return spark.query(f"SELECT sum(trips) FROM {ref}")[0][0]
    except Exception:
        pytest.skip("gold tables missing - run `make batch` first")


def test_setup_is_idempotent(settings, ch):
    clickhouse_setup.run(settings, namespace="manual")


def test_clickhouse_reads_delta_gold(ch, spark):
    expected = _spark_sum(spark, "delta.`s3a://delta/manual_gold/daily_zone_stats`")
    assert ch.query("SELECT sum(trips) FROM delta_gold.daily_zone_stats").result_rows[0][0] == expected


def test_clickhouse_reads_iceberg_gold(ch, spark):
    expected = _spark_sum(spark, "iceberg.manual_gold.daily_zone_stats")
    got = ch.query("SELECT sum(trips) FROM iceberg_lake.`manual_gold.daily_zone_stats`").result_rows[0][0]
    assert got == expected
