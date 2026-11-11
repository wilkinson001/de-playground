"""`python -m de_playground.streaming <command>` — streaming track helpers."""

from __future__ import annotations

import argparse

from de_playground.config import Settings
from de_playground.streaming import topics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m de_playground.streaming")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="create the Kafka topics (idempotent)")
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    if args.command == "setup":
        created = topics.ensure_topics(settings)
        print(f"Kafka topics created: {created or 'none (all exist)'}")


if __name__ == "__main__":
    main()
