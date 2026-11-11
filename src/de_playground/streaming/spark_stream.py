"""Spark Structured Streaming doing the same work as the Flink job.

Same raw topic, same bronze and gold columns, same window semantics (1-minute tumbling windows,
30 s watermark, each window emitted once in append mode), written to its own namespace
(`spark_stream_*`) and aggregates topic so the two engines can be compared.

The queries run on the Spark Connect server; this process only starts them and keeps the session
open. Spark Connect keeps a *disconnected* session's queries running until the session times out,
so `run` stops its queries on SIGTERM/SIGINT.
"""

from __future__ import annotations

import logging
import signal
import threading
from collections.abc import Mapping
from dataclasses import dataclass

from de_playground.config import Settings
from de_playground.streaming import topics
from de_playground.streaming.events import RECORD_FIELDS

log = logging.getLogger(__name__)

# Spark SQL types for the producer's record, in RECORD_FIELDS order.
_RECORD_TYPES = {
    "event_id": "STRING", "event_time": "STRING", "wiki": "STRING", "server_name": "STRING",
    "type": "STRING", "namespace": "INT", "title": "STRING", "user": "STRING", "bot": "BOOLEAN",
    "minor": "BOOLEAN", "length_old": "INT", "length_new": "INT", "byte_delta": "INT",
}
_BRONZE_TYPES = {**_RECORD_TYPES, "event_ts": "TIMESTAMP", "event_date": "DATE"}
_GOLD_TYPES = {
    "window_start": "TIMESTAMP", "window_end": "TIMESTAMP", "wiki": "STRING", "edits": "BIGINT",
    "bot_edits": "BIGINT", "human_edits": "BIGINT", "new_pages": "BIGINT", "bytes_added": "BIGINT",
    "bytes_removed": "BIGINT",
}
BRONZE_COLUMNS = list(_BRONZE_TYPES)
GOLD_COLUMNS = list(_GOLD_TYPES)
assert list(_RECORD_TYPES) == RECORD_FIELDS


@dataclass(frozen=True)
class SparkStreamConfig:
    namespace: str = "spark_stream"
    raw_topic: str = topics.RAW_TOPIC
    agg_topic: str = topics.AGG_TOPICS["spark"]
    kafka_bootstrap: str = "kafka:29092"
    checkpoint_root: str = "/opt/spark/checkpoints"  # on the Spark Connect server (sparkcheckpoints volume)
    watermark: str = "30 seconds"
    trigger: str = "10 seconds"
    starting_offsets: str = "earliest"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> SparkStreamConfig:
        return cls(
            namespace=env.get("STREAM_NAMESPACE", cls.namespace),
            raw_topic=env.get("STREAM_RAW_TOPIC", cls.raw_topic),
            agg_topic=env.get("STREAM_AGG_TOPIC", cls.agg_topic),
            kafka_bootstrap=env.get("PLAYGROUND_KAFKA_BOOTSTRAP", cls.kafka_bootstrap),
            checkpoint_root=env.get("STREAM_CHECKPOINT_ROOT", cls.checkpoint_root),
            trigger=env.get("STREAM_TRIGGER", cls.trigger),
        )


def query_names(cfg: SparkStreamConfig) -> list[str]:
    return [f"{cfg.namespace}-{layer}-{sink}"
            for layer, sinks in (("bronze", ("iceberg", "delta")), ("gold", ("iceberg", "delta", "kafka")))
            for sink in sinks]


def checkpoint_path(cfg: SparkStreamConfig, query_name: str) -> str:
    return f"{cfg.checkpoint_root}/{cfg.namespace}/{query_name}"


def _refs(cfg: SparkStreamConfig) -> dict[str, str]:
    ns = cfg.namespace
    return {
        "bronze-iceberg": f"iceberg.{ns}_bronze.recentchange",
        "bronze-delta": f"s3a://delta/{ns}_bronze/recentchange",
        "gold-iceberg": f"iceberg.{ns}_gold.edits_per_minute",
        "gold-delta": f"s3a://delta/{ns}_gold/edits_per_minute",
    }


def _ddl(types: dict[str, str]) -> str:
    return ", ".join(f"`{name}` {typ}" for name, typ in types.items())


def ensure_tables(spark, cfg: SparkStreamConfig) -> None:
    """Create the four lake tables (partitioned like the Flink job's) if they don't exist."""
    refs = _refs(cfg)
    for layer in ("bronze", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{cfg.namespace}_{layer}").collect()
    for key, types, partition in (("bronze", _BRONZE_TYPES, " PARTITIONED BY (event_date)"), ("gold", _GOLD_TYPES, "")):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {refs[f'{key}-iceberg']} ({_ddl(types)}) USING iceberg{partition}").collect()
        spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{refs[f'{key}-delta']}` ({_ddl(types)}) USING delta{partition}").collect()


def parse_events(kafka_df):
    """Kafka rows (binary or string `value`) → bronze columns, with event time parsed as UTC."""
    from pyspark.sql import functions as F

    schema = ", ".join(f"`{name}` {typ}" for name, typ in _RECORD_TYPES.items())
    parsed = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("r")).select("r.*")
    event_ts = F.to_timestamp(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    return parsed.withColumn("event_ts", event_ts).withColumn("event_date", F.to_date(event_ts)).select(*BRONZE_COLUMNS)


def aggregate(events, watermark: str):
    """1-minute tumbling windows per wiki; same columns and sums as the Flink job's gold."""
    from pyspark.sql import functions as F

    flag = lambda cond: F.sum(F.when(cond, 1).otherwise(0)).cast("bigint")  # noqa: E731
    delta = F.col("byte_delta").cast("bigint")
    return (
        events.withWatermark("event_ts", watermark)
        .groupBy(F.window("event_ts", "1 minute"), F.col("wiki"))
        .agg(
            F.count(F.lit(1)).alias("edits"),
            flag(F.col("bot")).alias("bot_edits"),
            flag(~F.col("bot")).alias("human_edits"),
            flag(F.col("type") == "new").alias("new_pages"),
            F.sum(F.when(delta > 0, delta).otherwise(0)).cast("bigint").alias("bytes_added"),
            F.sum(F.when(delta < 0, -delta).otherwise(0)).cast("bigint").alias("bytes_removed"),
        )
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                *GOLD_COLUMNS[2:])
    )


def start_queries(spark, cfg: SparkStreamConfig) -> list:
    """Start the five streaming queries. Starting a query whose checkpoint another run is using
    stops that run (spark.sql.streaming.stopActiveRunOnRestart, on by default)."""
    from pyspark.sql import functions as F

    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.raw_topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze = parse_events(source)
    gold = aggregate(bronze, cfg.watermark)
    refs = _refs(cfg)
    names = dict(zip(("bronze-iceberg", "bronze-delta", "gold-iceberg", "gold-delta", "gold-kafka"), query_names(cfg)))

    def writer(df, key: str):
        return (df.writeStream.queryName(names[key]).outputMode("append")
                .trigger(processingTime=cfg.trigger)
                .option("checkpointLocation", checkpoint_path(cfg, names[key])))

    queries = [
        writer(bronze, "bronze-iceberg").toTable(refs["bronze-iceberg"]),
        writer(bronze, "bronze-delta").format("delta").start(refs["bronze-delta"]),
        writer(gold, "gold-iceberg").toTable(refs["gold-iceberg"]),
        writer(gold, "gold-delta").format("delta").start(refs["gold-delta"]),
        writer(gold.select(F.col("wiki").alias("key"), F.to_json(F.struct(*GOLD_COLUMNS)).alias("value")), "gold-kafka")
        .format("kafka").option("kafka.bootstrap.servers", cfg.kafka_bootstrap).option("topic", cfg.agg_topic)
        .start(),
    ]
    log.info("started %s", [q.name for q in queries])
    return queries


def run(settings: Settings, cfg: SparkStreamConfig) -> None:
    """Entry point of the spark-stream service: start the queries, watch them, stop them on SIGTERM."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.remote(settings.spark_remote).getOrCreate()
    ensure_tables(spark, cfg)
    queries = start_queries(spark, cfg)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        # Poll rather than awaitAnyTermination(): a blocking gRPC call wouldn't let the signal
        # handler run until it returned.
        while not stop.wait(5):
            for q in queries:
                if not q.isActive:
                    raise RuntimeError(f"streaming query {q.name} stopped: {q.exception()}")
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        log.info("stopped %s", [q.name for q in queries])
