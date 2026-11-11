"""`python -m de_playground <command>` — infrastructure helpers."""

from __future__ import annotations

import argparse

from de_playground import bootstrap
from de_playground.config import Settings
from de_playground.months import Month
from de_playground.sources import tlc


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="de_playground")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("bootstrap", help="create buckets and the Lakekeeper warehouse (idempotent)")
    dl = sub.add_parser("download", help="download TLC months + zone lookup into the raw bucket")
    dl.add_argument("--months", required=True, help="YYYY-MM or YYYY-MM..YYYY-MM")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    if args.command == "bootstrap":
        print(bootstrap.run(settings))
    elif args.command == "download":
        for month in Month.range(args.months):
            print(tlc.download_month(month, settings))
        print(tlc.download_zones(settings))


if __name__ == "__main__":
    main()
