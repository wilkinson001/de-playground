"""`python -m de_playground.streaming <command>` — streaming track helpers."""

from __future__ import annotations

import argparse
import logging

from de_playground.config import Settings
from de_playground.streaming import producer, topics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m de_playground.streaming")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="create the Kafka topics (idempotent)")
    prod = sub.add_parser("produce", help="stream Wikimedia (or synthetic) events into Kafka")
    prod.add_argument("--source", choices=["auto", "wikimedia", "synthetic"], default="auto")
    prod.add_argument("--max-events", type=int, default=None)
    prod.add_argument("--topic", default=topics.RAW_TOPIC)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = Settings.from_env()
    if args.command == "setup":
        created = topics.ensure_topics(settings)
        print(f"Kafka topics created: {created or 'none (all exist)'}")
    elif args.command == "produce":
        sent = producer.run(producer.KafkaSink(settings, args.topic), args.source, max_events=args.max_events)
        print(f"sent {sent} records to {args.topic}")


if __name__ == "__main__":
    main()
