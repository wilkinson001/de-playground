"""Tiny TLC-shaped files for pipeline tests.

Column names and Arrow types mirror the real NYC TLC yellow-taxi Parquet files, including the
quirks the pipeline must cope with (mixed-case names, `Airport_fee` vs `airport_fee`,
`cbd_congestion_fee` only present from 2025).
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from de_playground.months import Month

_TYPES: list[tuple[str, pa.DataType]] = [
    ("VendorID", pa.int32()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    ("passenger_count", pa.int64()),
    ("trip_distance", pa.float64()),
    ("RatecodeID", pa.int64()),
    ("store_and_fwd_flag", pa.string()),
    ("PULocationID", pa.int32()),
    ("DOLocationID", pa.int32()),
    ("payment_type", pa.int64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
    ("Airport_fee", pa.float64()),
    ("cbd_congestion_fee", pa.float64()),
]

ZONES = [
    (1, "EWR", "Newark Airport", "EWR"),
    (132, "Queens", "JFK Airport", "Airports"),
    (138, "Queens", "LaGuardia Airport", "Airports"),
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
    (237, "Manhattan", "Upper East Side South", "Yellow Zone"),
    (264, "Unknown", "N/A", "N/A"),
    (265, "N/A", "Outside of NYC", "N/A"),
]


def trip_row(pickup: datetime, minutes: float = 15, **overrides) -> dict:
    """One valid card trip from Midtown to the UES; override any TLC column by name."""
    fare, tip = 20.0, 4.0
    row = {
        "VendorID": 2,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=minutes),
        "passenger_count": 1,
        "trip_distance": 2.5,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": fare,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": tip,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": fare + tip + 2.5,
        "congestion_surcharge": 2.5,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.75,
    }
    row.update(overrides)
    return row


def sample_rows(month: Month) -> list[dict]:
    """A handful of valid trips spread over the month, varied zones/payment types."""
    base = datetime(month.year, month.month, 1, 8, 0)
    return [
        trip_row(base),
        trip_row(base + timedelta(days=1, hours=3), PULocationID=132, DOLocationID=161, trip_distance=17.2),
        trip_row(base + timedelta(days=2, hours=10), payment_type=2, tip_amount=0.0),
        trip_row(base + timedelta(days=9, hours=14), PULocationID=138, passenger_count=None),
        trip_row(base + timedelta(days=20, hours=22), PULocationID=237, DOLocationID=1),
    ]


def write_trips_parquet(
    path: Path,
    rows: list[dict],
    *,
    include_cbd_fee: bool = True,
    airport_fee_name: str = "Airport_fee",
) -> Path:
    fields = []
    for name, typ in _TYPES:
        if name == "cbd_congestion_fee" and not include_cbd_fee:
            continue
        fields.append((airport_fee_name if name == "Airport_fee" else name, name, typ))
    columns = {out: pa.array([r.get(src) for r in rows], typ) for out, src, typ in fields}
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)
    return path


def write_zones_csv(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        w.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        w.writerows(ZONES)
    return path
