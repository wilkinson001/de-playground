"""Wikimedia EventStreams: Server-Sent Events parsing and normalisation to flat records.

The producer turns each `recentchange` event into a small flat record (RECORD_FIELDS) before it
goes to Kafka, so Flink, Spark and ClickHouse all read one simple, stable JSON shape.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone

RECORD_FIELDS = [
    "event_id",
    "event_time",
    "wiki",
    "server_name",
    "type",
    "namespace",
    "title",
    "user",
    "bot",
    "minor",
    "length_old",
    "length_new",
    "byte_delta",
]


@dataclass(frozen=True)
class SseEvent:
    id: str | None
    data: str


def parse_sse(lines: Iterable[str]) -> Iterator[SseEvent]:
    """Standard SSE framing: `data:` lines accumulate until a blank line dispatches the event."""
    event_id: str | None = None
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data:
                yield SseEvent(event_id, "\n".join(data))
            data = []
            continue
        if line.startswith(":"):  # comment / heartbeat
            continue
        field, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value
        # `event:` and `retry:` aren't needed here


def to_record(raw: dict) -> dict | None:
    """Flatten a recentchange event; None for canary events or events missing essentials."""
    meta = raw.get("meta") or {}
    if meta.get("domain") == "canary" or not meta.get("id") or not meta.get("dt") or not raw.get("wiki"):
        return None
    try:
        event_time = _utc_millis(meta["dt"])
    except ValueError:
        return None
    length = raw.get("length") or {}
    old, new = length.get("old"), length.get("new")
    return {
        "event_id": meta["id"],
        "event_time": event_time,
        "wiki": raw["wiki"],
        "server_name": raw.get("server_name"),
        "type": raw.get("type"),
        "namespace": raw.get("namespace"),
        "title": raw.get("title"),
        "user": raw.get("user"),
        "bot": bool(raw.get("bot", False)),
        "minor": bool(raw.get("minor", False)),
        "length_old": old,
        "length_new": new,
        "byte_delta": (new or 0) - (old or 0),
    }


def decode(event: SseEvent) -> dict | None:
    try:
        return to_record(json.loads(event.data))
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None


def _utc_millis(dt: str) -> str:
    parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    utc = parsed.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"
