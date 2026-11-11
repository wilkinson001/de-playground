"""`python -m de_playground.streaming <command>` — streaming track helpers."""

from __future__ import annotations

import argparse
import logging
import os

from de_playground.config import Settings
from de_playground.streaming import flink_control, producer, spark_stream, topics
from de_playground.streaming.flink.statements import JobConfig


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m de_playground.streaming")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("setup", help="create the Kafka topics (idempotent)")
    prod = sub.add_parser("produce", help="stream Wikimedia (or synthetic) events into Kafka")
    prod.add_argument("--source", choices=["auto", "wikimedia", "synthetic"], default="auto")
    prod.add_argument("--max-events", type=int, default=None)
    prod.add_argument("--topic", default=topics.RAW_TOPIC)
    sub.add_parser("spark-stream", help="run the Spark Structured Streaming queries until stopped")
    sub.add_parser("job-status", help="list running Flink streaming jobs")
    sub.add_parser("job-cancel", help="cancel the Flink streaming job (so a resubmit replaces it)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = Settings.from_env()
    if args.command == "setup":
        created = topics.ensure_topics(settings)
        print(f"Kafka topics created: {created or 'none (all exist)'}")
    elif args.command == "spark-stream":
        spark_stream.run(settings, spark_stream.SparkStreamConfig.from_env(os.environ))
    elif args.command in ("job-status", "job-cancel"):
        name = JobConfig.from_env(os.environ).job_name
        if args.command == "job-cancel":
            print(f"cancelled {name}: {flink_control.cancel_jobs(settings, name) or 'nothing running'}")
        else:
            print(f"{name}: {flink_control.running_jobs(settings, name) or 'not running'}")
    elif args.command == "produce":
        sent = producer.run(producer.KafkaSink(settings, args.topic), args.source, max_events=args.max_events)
        print(f"sent {sent} records to {args.topic}")


if __name__ == "__main__":
    main()
