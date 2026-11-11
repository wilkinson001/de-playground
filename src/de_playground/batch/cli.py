"""`python -m de_playground.batch run ...` — run the medallion pipeline by hand.

Processes targets in turn; within a target, months in order and layers in order (a month's
silver needs its bronze). Stops at the first failure and says where it happened.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable

from de_playground.batch.engines import SqlEngine, ducklake_engine, spark_engine
from de_playground.batch.pipeline import raw_sources, run_layer
from de_playground.batch.target import LAYERS, Target
from de_playground.config import Settings
from de_playground.months import Month

ALL_TARGETS = ["duckdb:ducklake", "spark:iceberg", "spark:delta"]


class BatchFailed(Exception):
    pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="python -m de_playground.batch")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="run the pipeline for some months")
    run_p.add_argument("--target", default="all", help=f"one of {ALL_TARGETS} or 'all'")
    run_p.add_argument("--months", required=True, help="YYYY-MM or YYYY-MM..YYYY-MM")
    run_p.add_argument("--namespace", default="manual")
    run_p.add_argument("--layers", default=",".join(LAYERS), help="comma-separated subset of bronze,silver,gold")
    args = parser.parse_args(argv)

    try:
        args.months = Month.range(args.months)
        specs = ALL_TARGETS if args.target == "all" else [args.target]
        args.targets = [Target(*spec.split(":", 1), args.namespace) for spec in specs]
    except (ValueError, TypeError) as e:
        parser.error(str(e))
    layers = tuple(args.layers.split(","))
    if not layers or any(layer not in LAYERS for layer in layers):
        parser.error(f"--layers must be a comma-separated subset of {','.join(LAYERS)}")
    args.layers = tuple(layer for layer in LAYERS if layer in layers)  # always run in pipeline order
    return args


def run(
    args: argparse.Namespace,
    engine_factory: Callable[[Target], SqlEngine],
    out: Callable[[str], None] = print,
) -> None:
    for target in args.targets:
        engine = engine_factory(target)
        for month in args.months:
            sources = raw_sources(month, target.engine)
            for layer in args.layers:
                where = f"{target} {month} {layer}"
                started = time.monotonic()
                try:
                    rows = run_layer(engine, target, layer, month, sources)
                except Exception as e:
                    raise BatchFailed(f"{where}: {e}") from e
                out(f"{where:<28} {rows:>12,} rows  {time.monotonic() - started:6.1f}s")


def _engine_factory(settings: Settings) -> Callable[[Target], SqlEngine]:
    engines: dict[str, SqlEngine] = {}

    def factory(target: Target) -> SqlEngine:
        if target.engine not in engines:
            engines[target.engine] = ducklake_engine(settings) if target.engine == "duckdb" else spark_engine(settings)
        return engines[target.engine]

    return factory


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        run(args, _engine_factory(Settings.from_env()))
    except BatchFailed as e:
        print(f"FAILED {e}", file=sys.stderr)
        sys.exit(1)
