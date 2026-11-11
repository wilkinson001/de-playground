from datetime import datetime

import pytest

from de_playground.batch.engines import connect_ducklake
from de_playground.batch.pipeline import RawSources, run_month
from de_playground.batch.target import Target
from de_playground.months import Month
from tests.fixtures.make_fixtures import sample_rows, trip_row, write_trips_parquet, write_zones_csv

TARGET = Target("duckdb", "ducklake", "t")
JAN = Month.parse("2025-01")
FEB = Month.parse("2025-02")


@pytest.fixture
def engine(tmp_path):
    return connect_ducklake(f"ducklake:{tmp_path}/cat.ducklake", f"{tmp_path}/data/")


@pytest.fixture
def load(tmp_path, engine):
    """Write a fixture file for a month and run the whole pipeline on it."""
    zones = str(write_zones_csv(tmp_path / "zones.csv"))

    def _load(month: Month, rows: list[dict], **parquet_kwargs) -> dict:
        trips = write_trips_parquet(tmp_path / f"trips_{month}.parquet", rows, **parquet_kwargs)
        return run_month(engine, TARGET, month, RawSources(str(trips), zones))

    return _load


def count(engine, table: str, where: str = "true") -> int:
    return engine.query(f"SELECT count(*) FROM {table} WHERE {where}")[0][0]


def test_bronze_loads_month_missing_cbd_fee(engine, load):
    rows = sample_rows(JAN)
    counts = load(JAN, rows, include_cbd_fee=False, airport_fee_name="airport_fee")
    assert counts["bronze"] == len(rows)
    assert count(engine, "t_bronze.yellow_trips") == len(rows)
    assert count(engine, "t_bronze.yellow_trips", "cbd_congestion_fee IS NULL") == len(rows)
    assert count(engine, "t_bronze.yellow_trips", "airport_fee = 0") == len(rows)
    assert count(engine, "t_bronze.taxi_zones") == 8


def test_silver_drops_out_of_month_pickups(engine, load):
    rows = [
        trip_row(datetime(2009, 1, 1, 0, 0)),
        trip_row(datetime(2025, 2, 1, 0, 0, 0)),
        trip_row(datetime(2025, 1, 31, 23, 59, 59)),
    ]
    load(JAN, rows)
    assert count(engine, "t_bronze.yellow_trips") == 3
    assert count(engine, "t_silver.trips") == 1
    assert engine.query("SELECT pickup_at FROM t_silver.trips") == [(datetime(2025, 1, 31, 23, 59, 59),)]


def test_silver_cleaning_rules(engine, load):
    t = datetime(2025, 1, 10, 9, 0)
    rows = [
        trip_row(t, minutes=-5),  # dropoff before pickup
        trip_row(t, minutes=7 * 60),  # longer than 6 hours
        trip_row(t, trip_distance=0.0),
        trip_row(t, trip_distance=250.0),
        trip_row(t, fare_amount=-3.0),
        trip_row(t, passenger_count=9),
        trip_row(t),
        trip_row(t, minutes=360, PULocationID=132),  # exactly 6 hours is allowed
    ]
    counts = load(JAN, rows)
    assert counts["silver"] == 2
    assert count(engine, "t_silver.trips") == 2


def test_silver_dedupes_identical_trips(engine, load):
    t = datetime(2025, 1, 10, 9, 0)
    load(JAN, [trip_row(t), trip_row(t)])
    assert count(engine, "t_silver.trips") == 1


def test_silver_enriches_zones(engine, load):
    load(JAN, [trip_row(datetime(2025, 1, 3, 7, 30), PULocationID=132, DOLocationID=161)])
    assert engine.query(
        "SELECT pu_borough, pu_zone, do_zone, pickup_date, pickup_hour, duration_minutes, pickup_month FROM t_silver.trips"
    ) == [("Queens", "JFK Airport", "Midtown Center", datetime(2025, 1, 3).date(), 7, 15.0, "2025-01")]


def test_gold_values(engine, load):
    day = datetime(2025, 1, 5, 10, 0)
    rows = [
        trip_row(day.replace(hour=10), PULocationID=132, fare_amount=10.0, tip_amount=1.0, total_amount=12.0),
        trip_row(day.replace(hour=11), PULocationID=132, fare_amount=20.0, tip_amount=2.0, total_amount=24.0),
        trip_row(day.replace(hour=11, minute=30), PULocationID=132, fare_amount=30.0, tip_amount=3.0, total_amount=36.0),
        trip_row(day.replace(hour=12), PULocationID=161, payment_type=2, tip_amount=0.0, total_amount=22.5),
    ]
    load(JAN, rows)

    (trips, revenue, tip_pct, borough), = engine.query(
        "SELECT trips, total_revenue, avg_tip_pct, pu_borough FROM t_gold.daily_zone_stats "
        "WHERE pu_location_id = 132 AND pickup_date = DATE '2025-01-05'"
    )
    assert (trips, str(revenue), borough) == (3, "72.00", "Queens")
    assert tip_pct == pytest.approx(0.1)

    # Cash trips carry no recorded tip, so zone 161's tip % is NULL.
    assert engine.query("SELECT avg_tip_pct FROM t_gold.daily_zone_stats WHERE pu_location_id = 161") == [(None,)]

    assert engine.query("SELECT sum(trips) FROM t_gold.hourly_borough_demand") == [(4,)]
    assert engine.query(
        "SELECT trips FROM t_gold.hourly_borough_demand WHERE pickup_hour = 11 AND pu_borough = 'Queens'"
    ) == [(2,)]
    assert engine.query(
        "SELECT payment_type, payment_type_name, trips, pickup_month FROM t_gold.payment_tip_stats ORDER BY 1"
    ) == [(1, "Credit card", 3, datetime(2025, 1, 1).date()), (2, "Cash", 1, datetime(2025, 1, 1).date())]


def _snapshot(engine) -> dict:
    tables = ["t_bronze.yellow_trips", "t_bronze.taxi_zones", "t_silver.trips",
              "t_gold.daily_zone_stats", "t_gold.hourly_borough_demand", "t_gold.payment_tip_stats"]
    snap = {t: count(engine, t) for t in tables}
    snap["gold"] = engine.query("SELECT * FROM t_gold.daily_zone_stats ORDER BY ALL")
    return snap


def test_rerun_month_is_idempotent(engine, load):
    rows = sample_rows(JAN)
    load(JAN, rows)
    first = _snapshot(engine)
    load(JAN, rows)
    assert _snapshot(engine) == first


def test_months_accumulate(engine, load):
    load(JAN, sample_rows(JAN))
    jan_gold = engine.query("SELECT * FROM t_gold.daily_zone_stats ORDER BY ALL")
    load(FEB, sample_rows(FEB))
    assert count(engine, "t_silver.trips", "pickup_month = '2025-01'") == 5
    assert count(engine, "t_silver.trips", "pickup_month = '2025-02'") == 5
    assert engine.query(
        "SELECT * FROM t_gold.daily_zone_stats WHERE pickup_date < DATE '2025-02-01' ORDER BY ALL"
    ) == jan_gold
