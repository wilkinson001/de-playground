import json
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from de_playground.streaming import topics

pytestmark = pytest.mark.integration


def test_ensure_topics_idempotent(settings):
    name = f"itest.topics.{uuid.uuid4().hex[:8]}"
    assert topics.ensure_topics(settings, [name]) == [name]
    assert topics.ensure_topics(settings, [name]) == []
    meta = AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).list_topics(timeout=10)
    assert len(meta.topics[name].partitions) == 3
    AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).delete_topics([name])


def test_default_topics():
    assert topics.RAW_TOPIC == "wikimedia.recentchange"
    assert topics.AGG_TOPICS == {
        "flink": "wikimedia.edits_per_minute.flink",
        "spark": "wikimedia.edits_per_minute.spark",
    }


def test_produce_consume_roundtrip(settings):
    name = f"itest.roundtrip.{uuid.uuid4().hex[:8]}"
    topics.ensure_topics(settings, [name])
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    for i in range(3):
        producer.produce(name, key=str(i), value=json.dumps({"i": i}))
    producer.flush(10)

    consumer = Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": f"itest-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([name])
    got = []
    for _ in range(60):
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            got.append(json.loads(msg.value())["i"])
        if len(got) == 3:
            break
    consumer.close()
    assert sorted(got) == [0, 1, 2]
    AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).delete_topics([name])
