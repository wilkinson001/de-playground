import json
import uuid

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

from de_playground.streaming import producer, topics
from de_playground.streaming.events import RECORD_FIELDS

pytestmark = pytest.mark.integration


def test_synthetic_producer_to_kafka(settings):
    topic = f"itest.producer.{uuid.uuid4().hex[:8]}"
    topics.ensure_topics(settings, [topic])
    sent = producer.run(producer.KafkaSink(settings, topic), "synthetic", max_events=20, rng_seed=3,
                        sleep=lambda s: None)
    assert sent == 20

    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": f"itest-{uuid.uuid4().hex[:8]}",
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])
    got = []
    for _ in range(120):
        msg = consumer.poll(0.5)
        if msg is not None and not msg.error():
            got.append((msg.key().decode(), json.loads(msg.value())))
        if len(got) == 20:
            break
    consumer.close()
    AdminClient({"bootstrap.servers": settings.kafka_bootstrap}).delete_topics([topic])

    assert len(got) == 20
    assert all(list(value) == RECORD_FIELDS and key == value["wiki"] for key, value in got)
