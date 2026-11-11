"""The Flink streaming job end to end: Kafka → bronze (Iceberg + Delta) and gold (Iceberg, Delta, Kafka)."""

import uuid
from datetime import datetime

import pytest

from de_playground import storage
from de_playground.streaming import flink_control
from de_playground.streaming.flink.statements import JobConfig
from tests.integration.streaming_helpers import (
    at, consume_all, drop_iceberg_namespace, drop_topics, expected_edits, flush_records, iceberg_count, new_topics,
    produce, record_at, records, ticking, wait_until,
)

pytestmark = pytest.mark.integration


def job_config(settings, label: str) -> JobConfig:
    ns = f"itest_fl_{label}_{uuid.uuid4().hex[:6]}"
    return JobConfig(
        access_key=settings.s3_access_key, secret_key=settings.s3_secret_key, namespace=ns,
        raw_topic=f"itest.raw.{ns}", agg_topic=f"itest.agg.{ns}", kafka_bootstrap=settings.kafka_bootstrap,
        group_id=f"itest-{ns}", lakekeeper_url=settings.lakekeeper_url, warehouse=settings.iceberg_warehouse,
        s3_endpoint=settings.s3_endpoint, checkpoint_interval="5s", job_name=f"itest-{ns}",
    )


def cleanup(settings, cfg: JobConfig) -> None:
    flink_control.cancel_jobs(settings, cfg.job_name)
    for layer in ("bronze", "gold"):
        drop_iceberg_namespace(settings, f"{cfg.namespace}_{layer}")
    storage.delete_prefix(storage.s3_client(settings), "delta", f"{cfg.namespace}_")
    drop_topics(settings, cfg.raw_topic, cfg.agg_topic)


@pytest.fixture(scope="module")
def job(settings):
    """One job over two full minutes (10:00, 10:01) plus one event per wiki at 10:05 that moves
    every partition's watermark past both windows."""
    cfg = job_config(settings, "main")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    recs = records(at(10, 0), 120)
    produce(settings, cfg.raw_topic, recs + flush_records(at(10, 5)))
    flink_control.submit(settings, cfg)
    yield cfg, recs
    cleanup(settings, cfg)


def gold_rows(delta_reader, settings, cfg, minute: datetime) -> dict[str, dict[str, int]]:
    """{format: {wiki: edits}} for one window, from Iceberg and Delta."""
    naive = minute.replace(tzinfo=None)
    table = storage.iceberg_catalog(settings).load_table(f"{cfg.namespace}_gold.edits_per_minute").scan().to_arrow()
    iceberg = {r["wiki"]: r["edits"] for r in table.to_pylist() if r["window_start"] == naive}
    delta = dict(delta_reader(f"delta/{cfg.namespace}_gold/edits_per_minute",
                              f"SELECT wiki, edits FROM t WHERE window_start = TIMESTAMP '{naive:%Y-%m-%d %H:%M:%S}'"))
    return {"iceberg": iceberg, "delta": delta}


def test_job_lands_bronze_in_iceberg_and_delta(job, settings, delta_reader):
    cfg, _ = job
    wait_until(lambda: iceberg_count(settings, f"{cfg.namespace}_bronze.recentchange") == 126)
    wait_until(lambda: delta_reader(f"delta/{cfg.namespace}_bronze/recentchange") == [(126,)])


def test_job_aggregates_per_minute(job, settings, delta_reader):
    cfg, recs = job
    for minute in (at(10, 0), at(10, 1)):
        expected = expected_edits(recs, minute)
        got = wait_until(lambda: (g := gold_rows(delta_reader, settings, cfg, minute))["iceberg"]
                         and g["delta"] and g)
        assert got["iceberg"] == expected, minute
        assert got["delta"] == expected, minute

    on_topic = consume_all(settings, cfg.agg_topic)
    first_window = {m["wiki"]: m["edits"] for m in on_topic if m["window_start"].startswith("2026-01-15 10:00:00")}
    assert first_window == expected_edits(recs, at(10, 0))


def test_late_event_kept_in_bronze_excluded_from_window(job, settings, delta_reader):
    cfg, recs = job
    before = wait_until(lambda: gold_rows(delta_reader, settings, cfg, at(10, 0))["iceberg"])
    # 10:00:30 arrives after the watermark has passed 10:04:30: too late for its window.
    produce(settings, cfg.raw_topic, [record_at(at(10, 0, 30), seed=7)])
    wait_until(lambda: iceberg_count(settings, f"{cfg.namespace}_bronze.recentchange") == 127)
    wait_until(lambda: delta_reader(f"delta/{cfg.namespace}_bronze/recentchange") == [(127,)])
    assert gold_rows(delta_reader, settings, cfg, at(10, 0))["iceberg"] == before


def test_windows_emit_with_idle_partitions(settings, delta_reader):
    """All events share one key, so two of the topic's three partitions never get data. The live
    partition keeps ticking (as a real stream does); the empty ones must be marked idle or the
    watermark - the minimum across partitions - never moves."""
    cfg = job_config(settings, "idle")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    try:
        recs = records(at(10, 0), 30)
        produce(settings, cfg.raw_topic, recs, key=lambda r: "one-key")
        flink_control.submit(settings, cfg)
        with ticking(settings, cfg.raw_topic, at(10, 2), key="one-key"):
            got = wait_until(lambda: gold_rows(delta_reader, settings, cfg, at(10, 0))["iceberg"])
        assert got == expected_edits(recs, at(10, 0))
    finally:
        cleanup(settings, cfg)


def test_resubmit_replaces_running_job(settings):
    cfg = job_config(settings, "resubmit")
    new_topics(settings, cfg.raw_topic, cfg.agg_topic)
    try:
        flink_control.submit(settings, cfg)
        flink_control.submit(settings, cfg)
        assert len(wait_until(lambda: flink_control.running_jobs(settings, cfg.job_name), 60)) == 1
    finally:
        cleanup(settings, cfg)
