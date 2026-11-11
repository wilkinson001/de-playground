"""`python -m de_playground <command>` — infrastructure helpers."""

from __future__ import annotations

import argparse

from de_playground import bootstrap
from de_playground.config import Settings


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="de_playground")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("bootstrap", help="create buckets and the Lakekeeper warehouse (idempotent)")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    if args.command == "bootstrap":
        print(bootstrap.run(settings))


if __name__ == "__main__":
    main()
