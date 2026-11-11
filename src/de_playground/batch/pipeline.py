"""The NYC taxi medallion pipeline, processed incrementally one month at a time.

`run_layer` and `run_month` are the contract the orchestrators (Dagster, Prefect, DBOS) wrap:
every engine × format variant runs the same SQL, rendered for the engine's dialect.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

from de_playground.batch.columns import bronze_select_list, sql_type
from de_playground.batch.engines import Dialect, SqlEngine
from de_playground.batch.schemas import SCHEMAS, ddl_statements, merge_statements
from de_playground.batch.target import LAYERS, Layer, Target, table_ref
from de_playground.months import Month
from de_playground.sources import tlc

_EPOCH_US = {"duckdb": "epoch_us", "spark": "unix_micros"}

# Per layer: (table, temp view the layer's template produces, the month's slice of that table).
_MONTH_DAYS = "t.pickup_date >= DATE '{month_start}' AND t.pickup_date < DATE '{next_month_start}'"
_MERGES: dict[str, list[tuple[str, str, str]]] = {
    "bronze": [],
    "silver": [("trips", "silver_batch", "t.pickup_month = '{month}'")],
    "gold": [
        ("daily_zone_stats", "gold_daily_zone_batch", _MONTH_DAYS),
        ("hourly_borough_demand", "gold_hourly_borough_batch", _MONTH_DAYS),
        ("payment_tip_stats", "gold_payment_tip_batch", "t.pickup_month = DATE '{month_start}'"),
    ],
}


@dataclass(frozen=True)
class RawSources:
    trips_uri: str
    zones_uri: str


def raw_sources(month: Month, dialect: Dialect) -> RawSources:
    scheme = "s3" if dialect == "duckdb" else "s3a"
    base = f"{scheme}://{tlc.RAW_BUCKET}"
    return RawSources(f"{base}/{tlc.raw_trips_key(month)}", f"{base}/{tlc.RAW_ZONES_KEY}")


def ensure_tables(engine: SqlEngine, target: Target) -> None:
    for statement in ddl_statements(target):
        engine.execute(statement)


def run_layer(engine: SqlEngine, target: Target, layer: Layer, month: Month, sources: RawSources) -> int:
    """Build one layer for one month. Returns the layer's row count for that month."""
    ensure_tables(engine, target)
    if layer == "bronze":
        columns = engine.parquet_columns(sources.trips_uri)
        engine.register_parquet_view(
            "raw_trips", sources.trips_uri, f"SELECT {bronze_select_list(columns, engine.dialect)} FROM src"
        )
        engine.register_csv_view("raw_zones", sources.zones_uri)

    context = _context(engine.dialect, target, month, sources)
    engine.execute(_template(layer).format(**context))
    for table, view, scope in _MERGES[layer]:
        for statement in merge_statements(target, table, engine.stage(view), scope.format(**context)):
            engine.execute(statement)
    return _month_count(engine, layer, context)


def run_month(
    engine: SqlEngine,
    target: Target,
    month: Month,
    sources: RawSources,
    layers: tuple[Layer, ...] = LAYERS,
) -> dict[Layer, int]:
    return {layer: run_layer(engine, target, layer, month, sources) for layer in layers}


def _template(layer: Layer) -> str:
    return resources.files("de_playground.batch").joinpath("sql", f"{layer}.sql").read_text()


def _context(dialect: Dialect, target: Target, month: Month, sources: RawSources) -> dict[str, str]:
    return {
        "bronze_trips": table_ref(target, "bronze", "yellow_trips"),
        "bronze_zones": table_ref(target, "bronze", "taxi_zones"),
        "silver_trips": table_ref(target, "silver", "trips"),
        "gold_daily_zone": table_ref(target, "gold", "daily_zone_stats"),
        "gold_hourly_borough": table_ref(target, "gold", "hourly_borough_demand"),
        "gold_payment_tip": table_ref(target, "gold", "payment_tip_stats"),
        "bronze_trips_cols": ", ".join(SCHEMAS["yellow_trips"].column_names),
        "month": str(month),
        "month_start": month.start.isoformat(),
        "next_month_start": month.next_start.isoformat(),
        "source_file": sources.trips_uri,
        "epoch_us": _EPOCH_US[dialect],
        "int_type": sql_type("int", dialect),
        "string_type": sql_type("string", dialect),
    }


def _month_count(engine: SqlEngine, layer: Layer, ctx: dict[str, str]) -> int:
    where = {
        "bronze": (ctx["bronze_trips"], f"source_month = '{ctx['month']}'"),
        "silver": (ctx["silver_trips"], f"pickup_month = '{ctx['month']}'"),
        "gold": (ctx["gold_daily_zone"], f"pickup_date >= DATE '{ctx['month_start']}' "
                                         f"AND pickup_date < DATE '{ctx['next_month_start']}'"),
    }[layer]
    return int(engine.query(f"SELECT count(*) FROM {where[0]} WHERE {where[1]}")[0][0])
