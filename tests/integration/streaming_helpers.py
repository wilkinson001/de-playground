"""Shared helpers for the streaming integration tests (Flink and Spark)."""

from __future__ import annotations

import contextlib
import json
import random
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from de_playground import storage
from de_playground.config import Settings
from de_playground.streaming import synthetic, topics
from de_playground.streaming.events import to_record

DAY = datetime(2026, 1, 15, tzinfo=timezone.utc)


def at(hh: int, mm: int, ss: int = 0) -> datetime:
    return DAY.replace(hour=hh, minute=mm, second=ss)


def records(start: datetime, count: int, interval_s: float = 1.0, seed: int = 11) -> list[dict]:
    """Normalised records with event times start, start+interval, ... (fixed past times)."""
    return [to_record(e) for e in synthetic.raw_events(random.Random(seed), start, count, interval_s)]


def record_at(when: datetime, wiki: str = "enwiki", seed: int = 99) -> dict:
    rec = records(when, 1, seed=seed)[0]
    return {**rec, "wiki": wiki}


def flush_records(when: datetime) -> list[dict]:
    """One event per wiki at `when`. Keyed by wiki, so it reaches every partition and moves every
    partition's watermark, not just one. (A test's data is all read at once, so every partition
    goes quiet together and idleness alone can't release the watermark.)"""
    return [record_at(when, wiki=wiki, seed=1000 + i) for i, wiki in enumerate(synthetic.WIKIS)]


@contextlib.contextmanager
def ticking(settings: Settings, topic: str, start: datetime, key: str, every_s: float = 1.0):
    """Keep one partition active, as a live stream would: an event every `every_s` seconds with
    event time advancing 1 s per tick from `start`."""
    stop = threading.Event()

    def run() -> None:
        producer, tick = Producer({"bootstrap.servers": settings.kafka_bootstrap}), 0
        while not stop.wait(every_s):
            rec = record_at(start + timedelta(seconds=tick), seed=5000 + tick)
            producer.produce(topic, key=key, value=json.dumps(rec))
            producer.poll(0)
            tick += 1
        producer.flush(10)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(15)


def produce(settings: Settings, topic: str, recs: list[dict], key: Callable[[dict], str] = lambda r: r["wiki"]) -> None:
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    for rec in recs:
        producer.produce(topic, key=key(rec), value=json.dumps(rec))
    producer.flush(30)


def new_topics(settings: Settings, *names: str) -> None:
    topics.ensure_topics(settings, list(names))


def drop_topics(settings: Settings, *names: str) -> None:
    AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).delete_topics(list(names))


def consume_all(settings: Settings, topic: str, settle_s: float = 5.0) -> list[dict]:
    """Everything currently on a topic (stops after `settle_s` without new messages)."""
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": f"itest-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])
    out, idle_since = [], time.monotonic()
    while time.monotonic() - idle_since < settle_s:
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            out.append(json.loads(msg.value()))
            idle_since = time.monotonic()
    consumer.close()
    return out


def wait_until(check: Callable[[], object], timeout_s: float = 180, every_s: float = 3) -> object:
    """Poll until `check()` returns something truthy (exceptions count as not-yet)."""
    deadline, last_error = time.monotonic() + timeout_s, None
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except Exception as e:  # tables may not exist until the first checkpoint commits
            last_error = e
        time.sleep(every_s)
    raise TimeoutError(f"condition not met within {timeout_s}s (last error: {last_error})")


def expected_edits(recs: list[dict], minute: datetime) -> dict[str, int]:
    """edits per wiki for the 1-minute window starting at `minute`."""
    prefix = minute.strftime("%Y-%m-%dT%H:%M")
    return dict(Counter(r["wiki"] for r in recs if r["event_time"].startswith(prefix)))


def iceberg_count(settings: Settings, ident: str) -> int:
    return storage.iceberg_catalog(settings).load_table(ident).scan().to_arrow().num_rows


def drop_iceberg_namespace(settings: Settings, namespace: str) -> None:
    catalog = storage.iceberg_catalog(settings)
    if not catalog.namespace_exists(namespace):
        return
    for ident in catalog.list_tables(namespace):
        catalog.drop_table(ident)  # no purge: Lakekeeper's hard delete removes the files
    catalog.drop_namespace(namespace)
