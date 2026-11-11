import json
import uuid

import clickhouse_connect
import pytest
from confluent_kafka import Producer

from de_playground.streaming import clickhouse_streaming
from tests.integration.streaming_helpers import at, drop_topics, new_topics, produce, records, wait_until

pytestmark = pytest.mark.integration


@pytest.fixture
def ch_db(settings):
    db = f"itest_ch_{uuid.uuid4().hex[:6]}"
    raw, aggs = f"itest.raw.{db}", {"flink": f"itest.agg.{db}.flink", "spark": f"itest.agg.{db}.spark"}
    new_topics(settings, raw, *aggs.values())
    clickhouse_streaming.run(settings, raw_topic=raw, agg_topics=aggs, database=db)
    client = clickhouse_connect.get_client(host=settings.clickhouse_host, port=settings.clickhouse_port,
                                           username="default", password="")
    yield db, raw, aggs, client
    client.command(f"DROP DATABASE IF EXISTS {db} SYNC")
    drop_topics(settings, raw, *aggs.values())


def test_clickhouse_ingests_raw_topic(settings, ch_db):
    db, raw, _, client = ch_db
    produce(settings, raw, records(at(10, 0), 10))
    wait_until(lambda: client.query(f"SELECT count() FROM {db}.recentchange FINAL").result_rows == [(10,)], 60, 2)
    (ts,), = client.query(f"SELECT min(event_time) FROM {db}.recentchange").result_rows
    assert ts.replace(tzinfo=None) == at(10, 0).replace(tzinfo=None)


def test_duplicate_messages_collapse(settings, ch_db):
    db, raw, _, client = ch_db
    recs = records(at(10, 0), 5)
    produce(settings, raw, recs)
    produce(settings, raw, recs)  # e.g. a producer or job restart re-sending
    # Wait on what the Kafka engine consumed, not on row counts: background merges may already
    # have collapsed the duplicates.
    consumed = (f"SELECT sum(num_messages_read) FROM system.kafka_consumers "
                f"WHERE database = '{db}' AND table = 'recentchange_queue'")
    wait_until(lambda: (client.query(consumed).result_rows[0][0] or 0) >= 10, 60, 2)
    wait_until(lambda: client.query(f"SELECT count() FROM {db}.recentchange FINAL").result_rows == [(5,)], 30, 2)


def test_clickhouse_keeps_engines_apart(settings, ch_db):
    db, _, aggs, client = ch_db
    # The two engines serialise timestamps differently (Flink: SQL style, Spark: ISO-8601).
    flink = {"window_start": "2026-01-15 10:00:00", "window_end": "2026-01-15 10:01:00"}
    spark = {"window_start": "2026-01-15T10:00:00.000Z", "window_end": "2026-01-15T10:01:00.000Z"}
    counts = {"wiki": "enwiki", "edits": 5, "bot_edits": 1, "human_edits": 4, "new_pages": 0,
              "bytes_added": 10, "bytes_removed": 2}
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    producer.produce(aggs["flink"], value=json.dumps({**flink, **counts}))
    producer.produce(aggs["spark"], value=json.dumps({**spark, **counts}))
    producer.flush(10)
    rows = wait_until(lambda: (r := client.query(
        f"SELECT engine, window_start, edits FROM {db}.edits_per_minute FINAL ORDER BY engine").result_rows)
        and len(r) == 2 and r, 60, 2)
    assert [r[0] for r in rows] == ["flink", "spark"]
    assert rows[0][1] == rows[1][1] and rows[0][2] == rows[1][2] == 5
