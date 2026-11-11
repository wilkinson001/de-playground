"""Push Wikimedia recent changes (or synthetic look-alikes) into Kafka as normalised records.

`auto` mode reads the live EventStreams feed and switches to synthetic events if the feed stays
unreachable, so the streaming track always has data.
"""

from __future__ import annotations

import json
import logging
import random
import time
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from typing import Literal, Protocol

import httpx

from de_playground.config import Settings
from de_playground.streaming import synthetic
from de_playground.streaming.events import decode, parse_sse, to_record

log = logging.getLogger(__name__)

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks clients to identify themselves; the repo is the contact point.
USER_AGENT = "de-playground/0.1 (https://github.com/wilkinson001/de-playground)"

Source = Literal["auto", "wikimedia", "synthetic"]
Sleep = Callable[[float], None]


class FeedUnavailable(Exception):
    pass


class Sink(Protocol):
    def send(self, key: str, value: dict) -> None: ...

    def flush(self) -> None: ...


class KafkaSink:
    def __init__(self, settings: Settings, topic: str):
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "acks": "all", "linger.ms": 50})

    def send(self, key: str, value: dict) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=key, value=json.dumps(value))
                break
            except BufferError:  # local queue full: let deliveries drain, then retry
                self.producer.poll(1)
        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush(30)


def wikimedia_records(
    http: httpx.Client, max_retries: int = 5, backoff_s: float = 2.0, sleep: Sleep = time.sleep
) -> Iterator[dict]:
    """Stream normalised records, reconnecting (and resuming via Last-Event-ID) on errors.

    Raises FeedUnavailable after `max_retries` consecutive failed connections.
    """
    last_id: str | None = None
    failures = 0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
        if last_id:
            headers["Last-Event-ID"] = last_id
        got_events = False
        try:
            with http.stream("GET", WIKIMEDIA_URL, headers=headers) as response:
                if response.status_code != 200:
                    raise httpx.HTTPStatusError(
                        f"HTTP {response.status_code}", request=response.request, response=response
                    )
                for event in parse_sse(response.iter_lines()):
                    got_events, failures = True, 0
                    if event.id:
                        last_id = event.id
                    if (record := decode(event)) is not None:
                        yield record
            if got_events:
                continue  # the server closed a working stream (it does, periodically): reconnect now
            error: Exception = RuntimeError("stream closed without sending any events")
        except (httpx.TransportError, httpx.HTTPStatusError) as e:
            error = e
        # A failed or empty connection: back off, so a misbehaving feed is never hammered.
        failures += 1
        if failures >= max_retries:
            raise FeedUnavailable(f"{WIKIMEDIA_URL}: {error}") from error
        log.warning("Wikimedia stream error (%s), reconnecting (attempt %d)", error, failures)
        sleep(backoff_s * failures)


def synthetic_records(rng: random.Random, interval_s: float = 0.2, sleep: Sleep = time.sleep) -> Iterator[dict]:
    """Synthetic records paced in real time and stamped with the current time."""
    for raw in synthetic.raw_events(rng, datetime.now(timezone.utc), interval_s=interval_s):
        yield to_record(raw)
        sleep(interval_s)


def run(
    sink: Sink,
    source: Source,
    max_events: int | None = None,
    http: httpx.Client | None = None,
    rng_seed: int | None = None,
    sleep: Sleep = time.sleep,
) -> int:
    rng = random.Random(rng_seed)
    if source == "synthetic":
        records = synthetic_records(rng, sleep=sleep)
    else:
        http = http or httpx.Client(timeout=httpx.Timeout(30, read=60))
        records = wikimedia_records(http, sleep=sleep)

    sent = 0
    try:
        while max_events is None or sent < max_events:
            try:
                record = next(records)
            except FeedUnavailable as e:
                if source != "auto":
                    raise
                log.warning("Wikimedia feed unavailable (%s); switching to synthetic events", e)
                records = synthetic_records(rng, sleep=sleep)
                continue
            sink.send(record["wiki"], record)
            sent += 1
    finally:
        sink.flush()
    return sent
