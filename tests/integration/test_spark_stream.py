"""Spark Structured Streaming doing the same work as the Flink job, and agreeing with it."""

import json
import os
import signal
import subprocess
import time
import uuid

import pytest
from pyspark.sql import SparkSession

from de_playground.streaming import flink_control, spark_stream
from de_playground.streaming.flink.statements import JobConfig
from tests.integration.streaming_helpers import (
    at, consume_all, drop_iceberg_namespace, drop_topics, expected_edits, flush_records, iceberg_count, new_topics,
    produce, record_at, records, wait_until,
)
from de_playground import storage

pytestmark = pytest.mark.integration

GOLD_SQL = ("SELECT CAST(window_start AS TIMESTAMP) AS ws, wiki, edits, bot_edits, human_edits, new_pages, "
            "bytes_added, bytes_removed FROM t ORDER BY ws, wiki")


def stream_config(settings, label: str, **overrides) -> spark_stream.SparkStreamConfig:
    ns = f"itest_sp_{label}_{uuid.uuid4().hex[:6]}"
    return spark_stream.SparkStreamConfig(
        namespace=ns, raw_topic=f"itest.raw.{ns}", agg_topic=f"itest.agg.{ns}",
        kafka_bootstrap=settings.kafka_bootstrap, checkpoint_root=f"/opt/spark/checkpoints/itest/{ns}",
        trigger="2 seconds", **overrides,
    )


def cleanup(settings, cfg: spark_stream.SparkStreamConfig, queries=()) -> None:
    for q in queries:
        q.stop()
    for layer in ("bronze", "gold"):
        drop_iceberg_namespace(settings, f"{cfg.namespace}_{layer}")
    storage.delete_prefix(storage.s3_client(settings), "delta", f"{cfg.namespace}_")
    drop_topics(settings, cfg.raw_topic, cfg.agg_topic)


@pytest.fixture(scope="module")
def spark(settings):
    return SparkSession.builder.remote(settings.spark_remote).getOrCreate()


@pytest.fixture(scope="module")
def stream(settings, spark):
    cfg = stream_config(settings, "main")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    recs = records(at(10, 0), 120)
    produce(settings, cfg.raw_topic, recs + flush_records(at(10, 5)))
    spark_stream.ensure_tables(spark, cfg)
    queries = spark_stream.start_queries(spark, cfg)
    yield cfg, recs
    cleanup(settings, cfg, queries)


def spark_gold(delta_reader, cfg, minute) -> dict[str, int]:
    naive = minute.replace(tzinfo=None)
    return {wiki: edits for ws, wiki, edits, *_ in delta_reader(f"delta/{cfg.namespace}_gold/edits_per_minute", GOLD_SQL)
            if ws == naive}


def test_spark_parse_and_aggregate_static(spark):
    recs = records(at(10, 0), 120)
    raw = spark.createDataFrame([(json.dumps(r),) for r in recs], "value string")
    gold = spark_stream.aggregate(spark_stream.parse_events(raw), "30 seconds").collect()
    first = {r["wiki"]: r["edits"] for r in gold if r["window_start"].replace(tzinfo=None) == at(10, 0).replace(tzinfo=None)}
    assert first == expected_edits(recs, at(10, 0))
    assert list(gold[0].asDict()) == spark_stream.GOLD_COLUMNS


def test_spark_stream_lands_bronze_and_gold(stream, settings, delta_reader):
    cfg, recs = stream
    wait_until(lambda: iceberg_count(settings, f"{cfg.namespace}_bronze.recentchange") == 126)
    wait_until(lambda: delta_reader(f"delta/{cfg.namespace}_bronze/recentchange") == [(126,)])
    for minute in (at(10, 0), at(10, 1)):
        assert wait_until(lambda: spark_gold(delta_reader, cfg, minute)) == expected_edits(recs, minute)
    iceberg_gold = storage.iceberg_catalog(settings).load_table(f"{cfg.namespace}_gold.edits_per_minute")
    assert iceberg_gold.scan().to_arrow().num_rows == 2 * len(expected_edits(recs, at(10, 0)))
    on_topic = consume_all(settings, cfg.agg_topic)
    assert {m["wiki"]: m["edits"] for m in on_topic if m["window_start"].startswith("2026-01-15T10:00:00")} == (
        expected_edits(recs, at(10, 0)))


def test_spark_late_event_kept_in_bronze_excluded_from_window(stream, settings, delta_reader):
    cfg, recs = stream
    before = wait_until(lambda: spark_gold(delta_reader, cfg, at(10, 0)))
    produce(settings, cfg.raw_topic, [record_at(at(10, 0, 30), seed=7)])
    wait_until(lambda: iceberg_count(settings, f"{cfg.namespace}_bronze.recentchange") == 127)
    assert spark_gold(delta_reader, cfg, at(10, 0)) == before


def test_spark_and_flink_gold_match(settings, spark, delta_reader):
    """Both engines read the same raw topic; every completed window must be identical."""
    scfg = stream_config(settings, "eq")
    fcfg = JobConfig(access_key=settings.s3_access_key, secret_key=settings.s3_secret_key,
                     namespace=f"itest_fl_eq_{uuid.uuid4().hex[:6]}", raw_topic=scfg.raw_topic,
                     agg_topic=f"itest.agg.{scfg.namespace}.flink", kafka_bootstrap=settings.kafka_bootstrap,
                     group_id=f"itest-{scfg.namespace}", lakekeeper_url=settings.lakekeeper_url,
                     warehouse=settings.iceberg_warehouse, s3_endpoint=settings.s3_endpoint,
                     checkpoint_interval="5s", job_name=f"itest-{scfg.namespace}")
    new_topics(settings, scfg.raw_topic, scfg.agg_topic, fcfg.agg_topic)
    queries = []
    try:
        produce(settings, scfg.raw_topic, records(at(10, 0), 120, seed=21) + flush_records(at(10, 5)))
        flink_control.submit(settings, fcfg)
        spark_stream.ensure_tables(spark, scfg)
        queries = spark_stream.start_queries(spark, scfg)

        def both_have_two_windows():
            f = delta_reader(f"delta/{fcfg.namespace}_gold/edits_per_minute", GOLD_SQL)
            s = delta_reader(f"delta/{scfg.namespace}_gold/edits_per_minute", GOLD_SQL)
            return (f, s) if len({r[0] for r in f}) == 2 and len({r[0] for r in s}) == 2 else None

        flink_rows, spark_rows = wait_until(both_have_two_windows)
        assert flink_rows == spark_rows
    finally:
        flink_control.cancel_jobs(settings, fcfg.job_name)
        for layer in ("bronze", "gold"):
            drop_iceberg_namespace(settings, f"{fcfg.namespace}_{layer}")
        storage.delete_prefix(storage.s3_client(settings), "delta", f"{fcfg.namespace}_")
        drop_topics(settings, fcfg.agg_topic)
        cleanup(settings, scfg, queries)


def test_restart_replaces_spark_queries(settings, spark):
    cfg = stream_config(settings, "restart")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    other = SparkSession.builder.remote(settings.spark_remote).create()  # a second, separate session
    first, second = [], []
    try:
        spark_stream.ensure_tables(spark, cfg)
        first = spark_stream.start_queries(spark, cfg)
        second = spark_stream.start_queries(other, cfg)
        wait_until(lambda: not any(q.isActive for q in first), 60, 2)
        assert sum(q.isActive for q in second) == 5
    finally:
        cleanup(settings, cfg, second)


def test_sigterm_stops_spark_queries(settings):
    """Stopping the spark-stream process must stop its queries: Spark Connect would otherwise keep
    a disconnected session's queries running until the session times out."""
    cfg = stream_config(settings, "sigterm")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    env = {**os.environ, "STREAM_NAMESPACE": cfg.namespace, "STREAM_RAW_TOPIC": cfg.raw_topic,
           "STREAM_AGG_TOPIC": cfg.agg_topic, "STREAM_CHECKPOINT_ROOT": cfg.checkpoint_root,
           "STREAM_TRIGGER": "2 seconds"}
    proc = subprocess.Popen(["python", "-m", "de_playground.streaming", "spark-stream"], env=env)
    bronze = f"{cfg.namespace}_bronze.recentchange"
    try:
        produce(settings, cfg.raw_topic, records(at(10, 0), 10))
        wait_until(lambda: iceberg_count(settings, bronze) == 10)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0

        produce(settings, cfg.raw_topic, records(at(10, 1), 10, seed=3))
        time.sleep(15)  # several trigger intervals: a still-running query would have picked these up
        assert iceberg_count(settings, bronze) == 10
    finally:
        if proc.poll() is None:
            proc.kill()
        cleanup(settings, cfg)
