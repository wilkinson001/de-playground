"""Where a pipeline run writes: engine × table format × namespace.

The namespace lets several runners (manual CLI runs, Dagster, Prefect, DBOS) write the same
pipeline side by side without colliding; each layer gets its own schema `{namespace}_{layer}`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

Engine = Literal["duckdb", "spark"]
Format = Literal["ducklake", "iceberg", "delta"]
Layer = Literal["bronze", "silver", "gold"]

LAYERS: tuple[Layer, ...] = ("bronze", "silver", "gold")

TABLES: dict[str, list[str]] = {
    "bronze": ["yellow_trips", "taxi_zones"],
    "silver": ["trips"],
    "gold": ["daily_zone_stats", "hourly_borough_demand", "payment_tip_stats"],
}

# DuckLake is DuckDB's home format; Spark has no usable DuckLake connector, so it writes
# Iceberg and Delta instead (see the design doc's research note).
SUPPORTED: set[tuple[str, str]] = {("duckdb", "ducklake"), ("spark", "iceberg"), ("spark", "delta")}

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_]{0,30}$")


@dataclass(frozen=True)
class Target:
    engine: Engine
    format: Format
    namespace: str

    def __post_init__(self) -> None:
        if (self.engine, self.format) not in SUPPORTED:
            raise ValueError(f"unsupported engine/format pair {self.engine}/{self.format}; use one of {sorted(SUPPORTED)}")
        if not _NAMESPACE_RE.match(self.namespace):
            raise ValueError(f"namespace {self.namespace!r} must match {_NAMESPACE_RE.pattern}")

    def __str__(self) -> str:
        return f"{self.engine}:{self.format}"


def schema_name(target: Target, layer: Layer) -> str:
    return f"{target.namespace}_{layer}"


def table_ref(target: Target, layer: Layer, table: str) -> str:
    schema = schema_name(target, layer)
    if target.format == "ducklake":
        return f"lake.{schema}.{table}"
    if target.format == "iceberg":
        return f"iceberg.{schema}.{table}"
    return f"delta.`s3a://delta/{schema}/{table}`"
