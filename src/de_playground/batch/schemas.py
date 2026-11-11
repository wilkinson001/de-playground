"""Table definitions for every layer, and the DDL / MERGE statements generated from them.

Defined once here so DuckLake, Iceberg and Delta tables always have identical schemas.
"""

from __future__ import annotations

from dataclasses import dataclass

from de_playground.batch.columns import BRONZE_COLUMNS, sql_type
from de_playground.batch.target import Layer, Target, schema_name, table_ref


@dataclass(frozen=True)
class TableSchema:
    layer: Layer
    name: str
    columns: list[tuple[str, str]]  # (name, logical type: int|bigint|double|timestamp|string|date|DECIMAL(p,s))
    keys: list[str]  # MERGE keys; empty for tables loaded by delete + insert
    partition_by: str | None = None

    @property
    def column_names(self) -> list[str]:
        return [c for c, _ in self.columns]


SCHEMAS: dict[str, TableSchema] = {
    s.name: s
    for s in [
        TableSchema(
            "bronze",
            "yellow_trips",
            [*BRONZE_COLUMNS, ("source_month", "string"), ("source_file", "string"), ("ingested_at", "timestamp")],
            keys=[],
            partition_by="source_month",
        ),
        TableSchema(
            "bronze",
            "taxi_zones",
            [("location_id", "int"), ("borough", "string"), ("zone", "string"), ("service_zone", "string")],
            keys=[],
        ),
        TableSchema(
            "silver",
            "trips",
            [
                ("trip_id", "string"),
                ("pickup_month", "string"),
                ("pickup_at", "timestamp"),
                ("dropoff_at", "timestamp"),
                ("pickup_date", "date"),
                ("pickup_hour", "int"),
                ("duration_minutes", "double"),
                ("vendor_id", "int"),
                ("passenger_count", "bigint"),
                ("trip_distance", "DECIMAL(8,2)"),
                ("pu_location_id", "int"),
                ("do_location_id", "int"),
                ("pu_borough", "string"),
                ("pu_zone", "string"),
                ("do_borough", "string"),
                ("do_zone", "string"),
                ("payment_type", "bigint"),
                ("fare_amount", "DECIMAL(10,2)"),
                ("tip_amount", "DECIMAL(10,2)"),
                ("total_amount", "DECIMAL(10,2)"),
                ("airport_fee", "DECIMAL(10,2)"),
                ("cbd_congestion_fee", "DECIMAL(10,2)"),
            ],
            keys=["trip_id"],
            partition_by="pickup_month",
        ),
        TableSchema(
            "gold",
            "daily_zone_stats",
            [
                ("pickup_date", "date"),
                ("pu_location_id", "int"),
                ("pu_borough", "string"),
                ("pu_zone", "string"),
                ("trips", "bigint"),
                ("total_revenue", "DECIMAL(14,2)"),
                ("avg_trip_distance", "double"),
                ("avg_tip_pct", "double"),
            ],
            keys=["pickup_date", "pu_location_id"],
        ),
        TableSchema(
            "gold",
            "hourly_borough_demand",
            [("pickup_date", "date"), ("pickup_hour", "int"), ("pu_borough", "string"), ("trips", "bigint")],
            keys=["pickup_date", "pickup_hour", "pu_borough"],
        ),
        TableSchema(
            "gold",
            "payment_tip_stats",
            [
                ("pickup_month", "date"),
                ("payment_type", "bigint"),
                ("payment_type_name", "string"),
                ("trips", "bigint"),
                ("avg_tip_amount", "double"),
                ("avg_tip_pct", "double"),
            ],
            keys=["pickup_month", "payment_type"],
        ),
    ]
}


def _type(logical: str, dialect: str) -> str:
    return logical if logical.startswith("DECIMAL") else sql_type(logical, dialect)


def ddl_statements(target: Target) -> list[str]:
    """Idempotent CREATE statements for every schema and table of a target."""
    dialect = target.engine
    statements: list[str] = []
    for layer in ("bronze", "silver", "gold"):
        if target.format == "ducklake":
            statements.append(f"CREATE SCHEMA IF NOT EXISTS lake.{schema_name(target, layer)}")
        elif target.format == "iceberg":
            statements.append(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{schema_name(target, layer)}")
        # Delta tables are addressed by path: no schema object to create.

    for schema in SCHEMAS.values():
        ref = table_ref(target, schema.layer, schema.name)
        cols = ", ".join(f"{c} {_type(t, dialect)}" for c, t in schema.columns)
        if target.format == "ducklake":
            statements.append(f"CREATE TABLE IF NOT EXISTS {ref} ({cols})")
            if schema.partition_by:
                statements.append(f"ALTER TABLE {ref} SET PARTITIONED BY ({schema.partition_by})")
        else:
            using = "iceberg" if target.format == "iceberg" else "delta"
            partition = f" PARTITIONED BY ({schema.partition_by})" if schema.partition_by else ""
            statements.append(f"CREATE TABLE IF NOT EXISTS {ref} ({cols}) USING {using}{partition}")
    return statements


def merge_statements(target: Target, table: str, source_view: str, scope: str) -> list[str]:
    """Make the month's slice of a keyed table equal `source_view`.

    Matched rows are updated, new rows inserted, and rows inside `scope` (a predicate on the target
    alias `t`, e.g. this month's partition) that the source no longer produces are deleted, so
    re-running a month after its input or the SQL changed leaves no stale rows behind.
    """
    schema = SCHEMAS[table]
    ref = table_ref(target, schema.layer, table)
    # Null-safe key match: e.g. trips from an unknown zone have a NULL borough, and a plain `=`
    # would never match them, so re-running a month would insert them again.
    keys = " AND ".join(f"t.{k} IS NOT DISTINCT FROM s.{k}" for k in schema.keys)
    non_keys = [c for c in schema.column_names if c not in schema.keys]
    sets = ", ".join(f"{c} = s.{c}" for c in non_keys)
    cols = ", ".join(schema.column_names)
    vals = ", ".join(f"s.{c}" for c in schema.column_names)
    # The scope also goes in ON as a literal so Iceberg/Delta can prune the target to one month.
    merge = (
        f"MERGE INTO {ref} t USING {source_view} s ON ({scope}) AND {keys} "
        f"WHEN MATCHED THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
    )
    if target.format == "ducklake":
        # DuckLake's MERGE allows only one UPDATE/DELETE action, so the stale-row delete is a
        # separate statement in the same transaction.
        delete = f"DELETE FROM {ref} t WHERE ({scope}) AND NOT EXISTS (SELECT 1 FROM {source_view} s WHERE {keys})"
        return ["BEGIN TRANSACTION", delete, merge, "COMMIT"]
    return [merge + f" WHEN NOT MATCHED BY SOURCE AND ({scope}) THEN DELETE"]
