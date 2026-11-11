"""Spike: one Flink image can write Iceberg and Delta on SeaweedFS and read Kafka."""

import json
import uuid

import pytest
from confluent_kafka import Producer

from de_playground import storage
from de_playground.matrix.probes import PythonAdapter
from de_playground.streaming import topics
from de_playground.streaming.flink import statements
from de_playground.streaming.flink_gateway import FlinkSql

pytestmark = pytest.mark.integration


@pytest.fixture
def ns():
    return f"itest_flink_{uuid.uuid4().hex[:6]}"


@pytest.fixture
def flink(settings):
    with FlinkSql(settings) as sql:
        sql.execute("SET 'execution.runtime-mode' = 'batch'")
        yield sql


def test_flink_writes_iceberg(flink, settings, ns):
    flink.execute(statements.iceberg_catalog_ddl(settings.lakekeeper_url, settings.iceberg_warehouse,
                                                 settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key))
    flink.execute(f"CREATE DATABASE IF NOT EXISTS lakehouse.{ns}")
    flink.execute(f"CREATE TABLE lakehouse.{ns}.t (id INT, v STRING)")
    flink.execute(f"INSERT INTO lakehouse.{ns}.t VALUES (1, 'a'), (2, 'b')")

    catalog = PythonAdapter(settings).catalog
    try:
        assert catalog.load_table(f"{ns}.t").scan().to_arrow().num_rows == 2
    finally:
        catalog.drop_table(f"{ns}.t")
        catalog.drop_namespace(ns)


def test_flink_writes_delta(flink, settings, ns, delta_reader):
    path = f"s3a://delta/{ns}/t"
    flink.execute(f"CREATE TEMPORARY TABLE d (id INT, v STRING) WITH ({statements.delta_options(path)})")
    flink.execute("INSERT INTO d VALUES (1, 'a'), (2, 'b')")
    try:
        assert delta_reader(f"delta/{ns}/t", "SELECT id, v FROM t ORDER BY id") == [(1, "a"), (2, "b")]
    finally:
        storage.delete_prefix(storage.s3_client(settings), "delta", f"{ns}/")


def test_flink_reads_kafka(flink, settings):
    topic = f"itest.flink.{uuid.uuid4().hex[:8]}"
    topics.ensure_topics(settings, [topic])
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap})
    for i in range(3):
        producer.produce(topic, value=json.dumps({"i": i}))
    producer.flush(10)

    flink.execute(f"""CREATE TEMPORARY TABLE k (i INT) WITH (
        'connector' = 'kafka', 'topic' = '{topic}', 'properties.bootstrap.servers' = '{settings.kafka_bootstrap}',
        'format' = 'json', 'scan.startup.mode' = 'earliest-offset', 'scan.bounded.mode' = 'latest-offset')""")
    assert flink.execute("SELECT count(*) FROM k") == [[3]]
