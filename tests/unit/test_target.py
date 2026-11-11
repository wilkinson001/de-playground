import pytest

from de_playground.batch.target import TABLES, Target, schema_name, table_ref


@pytest.mark.parametrize(
    "engine,fmt,ns",
    [("spark", "ducklake", "x"), ("duckdb", "iceberg", "x"), ("duckdb", "delta", "x"), ("spark", "delta", "Bad-Name"),
     ("spark", "delta", "")],
)
def test_target_validation(engine, fmt, ns):
    with pytest.raises(ValueError):
        Target(engine, fmt, ns)


def test_table_ref_per_format():
    assert table_ref(Target("duckdb", "ducklake", "manual"), "silver", "trips") == "lake.manual_silver.trips"
    assert table_ref(Target("spark", "iceberg", "manual"), "silver", "trips") == "iceberg.manual_silver.trips"
    assert table_ref(Target("spark", "delta", "manual"), "silver", "trips") == "delta.`s3a://delta/manual_silver/trips`"


def test_schema_name_and_tables():
    assert schema_name(Target("spark", "iceberg", "dagster"), "gold") == "dagster_gold"
    assert TABLES == {
        "bronze": ["yellow_trips", "taxi_zones"],
        "silver": ["trips"],
        "gold": ["daily_zone_stats", "hourly_borough_demand", "payment_tip_stats"],
    }
