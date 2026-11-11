"""Normalising raw TLC columns into one stable bronze schema.

TLC files drift between years: `airport_fee` vs `Airport_fee`, int vs double types,
`cbd_congestion_fee` only from 2025. Bronze always has the same columns with the same types;
anything a file lacks becomes NULL.
"""

from __future__ import annotations

from de_playground.batch.engines import Dialect

# (canonical name, logical type, TLC source name)
_COLUMNS: list[tuple[str, str, str]] = [
    ("vendor_id", "int", "VendorID"),
    ("pickup_at", "timestamp", "tpep_pickup_datetime"),
    ("dropoff_at", "timestamp", "tpep_dropoff_datetime"),
    ("passenger_count", "bigint", "passenger_count"),
    ("trip_distance", "double", "trip_distance"),
    ("ratecode_id", "bigint", "RatecodeID"),
    ("store_and_fwd_flag", "string", "store_and_fwd_flag"),
    ("pu_location_id", "int", "PULocationID"),
    ("do_location_id", "int", "DOLocationID"),
    ("payment_type", "bigint", "payment_type"),
    ("fare_amount", "double", "fare_amount"),
    ("extra", "double", "extra"),
    ("mta_tax", "double", "mta_tax"),
    ("tip_amount", "double", "tip_amount"),
    ("tolls_amount", "double", "tolls_amount"),
    ("improvement_surcharge", "double", "improvement_surcharge"),
    ("total_amount", "double", "total_amount"),
    ("congestion_surcharge", "double", "congestion_surcharge"),
    ("airport_fee", "double", "Airport_fee"),
    ("cbd_congestion_fee", "double", "cbd_congestion_fee"),
]

BRONZE_COLUMNS: list[tuple[str, str]] = [(name, typ) for name, typ, _ in _COLUMNS]

_SQL_TYPES: dict[Dialect, dict[str, str]] = {
    "duckdb": {"int": "INTEGER", "bigint": "BIGINT", "double": "DOUBLE", "timestamp": "TIMESTAMP", "string": "VARCHAR"},
    "spark": {"int": "INT", "bigint": "BIGINT", "double": "DOUBLE", "timestamp": "TIMESTAMP", "string": "STRING"},
}
_QUOTE: dict[Dialect, str] = {"duckdb": '"', "spark": "`"}


def sql_type(logical: str, dialect: Dialect) -> str:
    return _SQL_TYPES[dialect][logical]


def bronze_select_list(available: list[str], dialect: Dialect) -> str:
    """SELECT-list mapping whatever columns a raw file has onto the canonical bronze columns."""
    actual_by_lower = {c.lower(): c for c in available}
    q = _QUOTE[dialect]
    parts = []
    for name, logical, source in _COLUMNS:
        typ = sql_type(logical, dialect)
        actual = actual_by_lower.get(source.lower())
        expr = f"src.{q}{actual}{q}" if actual else "NULL"
        parts.append(f"CAST({expr} AS {typ}) AS {name}")
    return ",\n    ".join(parts)
