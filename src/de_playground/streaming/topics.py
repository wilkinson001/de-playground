"""Kafka topics for the streaming track."""

from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic

from de_playground.config import Settings

RAW_TOPIC = "wikimedia.recentchange"
# Each streaming engine writes its aggregates to its own topic so they can be compared.
AGG_TOPICS = {
    "flink": "wikimedia.edits_per_minute.flink",
    "spark": "wikimedia.edits_per_minute.spark",
}


def ensure_topics(settings: Settings, names: list[str] | None = None, partitions: int = 3) -> list[str]:
    """Create any missing topics (replication 1: single broker). Returns the names created."""
    names = names if names is not None else [RAW_TOPIC, *AGG_TOPICS.values()]
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [n for n in names if n not in existing]
    if not missing:
        return []
    futures = admin.create_topics([NewTopic(n, num_partitions=partitions, replication_factor=1) for n in missing])
    for future in futures.values():
        future.result(timeout=30)
    return missing
