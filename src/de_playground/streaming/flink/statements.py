"""Flink SQL for the streaming job, rendered from plain values.

Runs inside the Flink containers too, so it imports only the standard library (and pyflink).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

# Mirrors de_playground.streaming.topics (which needs confluent-kafka, absent in the Flink image);
# a unit test keeps the two in step.
RAW_TOPIC = "wikimedia.recentchange"
AGG_TOPIC = "wikimedia.edits_per_minute.flink"

# Bronze columns: the producer's record (RECORD_FIELDS) plus event-time columns. Backticked
# because `type` and `user` are reserved words in Flink SQL.
_RECORD_COLUMNS = [
    ("event_id", "STRING"), ("event_time", "STRING"), ("wiki", "STRING"), ("server_name", "STRING"),
    ("type", "STRING"), ("namespace", "INT"), ("title", "STRING"), ("user", "STRING"),
    ("bot", "BOOLEAN"), ("minor", "BOOLEAN"), ("length_old", "INT"), ("length_new", "INT"),
    ("byte_delta", "INT"),
]
_BRONZE_EXTRA = [("event_ts", "TIMESTAMP_LTZ(3)"), ("event_date", "DATE")]
_GOLD_COLUMNS = [
    ("window_start", "TIMESTAMP(3)"), ("window_end", "TIMESTAMP(3)"), ("wiki", "STRING"),
    ("edits", "BIGINT"), ("bot_edits", "BIGINT"), ("human_edits", "BIGINT"), ("new_pages", "BIGINT"),
    ("bytes_added", "BIGINT"), ("bytes_removed", "BIGINT"),
]


@dataclass(frozen=True)
class JobConfig:
    access_key: str
    secret_key: str
    namespace: str = "flink_stream"
    raw_topic: str = RAW_TOPIC
    agg_topic: str = AGG_TOPIC
    kafka_bootstrap: str = "kafka:29092"
    group_id: str = "flink-wikimedia"
    lakekeeper_url: str = "http://lakekeeper:8181"
    warehouse: str = "lakehouse"
    s3_endpoint: str = "http://seaweedfs:8333"
    checkpoint_interval: str = "30s"
    watermark_delay_s: int = 30
    job_name: str = "wikimedia-streaming"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> JobConfig:
        """Same env vars as de_playground.config.Settings, plus STREAM_* overrides (used by tests)."""
        return cls(
            access_key=env["S3_ACCESS_KEY"],
            secret_key=env["S3_SECRET_KEY"],
            namespace=env.get("STREAM_NAMESPACE", cls.namespace),
            raw_topic=env.get("STREAM_RAW_TOPIC", cls.raw_topic),
            agg_topic=env.get("STREAM_AGG_TOPIC", cls.agg_topic),
            kafka_bootstrap=env.get("PLAYGROUND_KAFKA_BOOTSTRAP", cls.kafka_bootstrap),
            group_id=env.get("STREAM_GROUP_ID", cls.group_id),
            lakekeeper_url=env.get("PLAYGROUND_LAKEKEEPER_URL", cls.lakekeeper_url),
            warehouse=env.get("PLAYGROUND_ICEBERG_WAREHOUSE", cls.warehouse),
            s3_endpoint=env.get("PLAYGROUND_S3_ENDPOINT", cls.s3_endpoint),
            checkpoint_interval=env.get("STREAM_CHECKPOINT_INTERVAL", cls.checkpoint_interval),
            job_name=env.get("STREAM_JOB_NAME", cls.job_name),
        )


def _with(options: dict[str, str]) -> str:
    return ",\n    ".join(f"'{k}' = '{v}'" for k, v in options.items())


def iceberg_catalog_ddl(lakekeeper_url: str, warehouse: str, s3_endpoint: str, access_key: str, secret_key: str) -> str:
    """Catalog `lakehouse` → the Lakekeeper REST catalog; data files via S3FileIO with static keys."""
    return "CREATE CATALOG lakehouse WITH (\n    " + _with({
        "type": "iceberg",
        "catalog-type": "rest",
        "uri": f"{lakekeeper_url}/catalog",
        "warehouse": warehouse,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": s3_endpoint,
        "s3.path-style-access": "true",
        "client.region": "us-east-1",
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
    }) + "\n)"


def delta_options(path: str, partitions: str | None = None) -> str:
    """WITH-clause body for a path-based Delta sink (the connector creates the table on first write).

    S3 endpoint and credentials are not table options: the released connector rejects them, so
    they come from the image's Hadoop config (docker/flink/core-site.xml).
    """
    options = {"connector": "delta", "table_path": path}
    if partitions:
        options["partitions"] = partitions
    return _with(options)


def _micros(columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Millisecond timestamp types → microsecond ones, for the Delta sinks.

    delta-flink 4.4.1 reads Flink's compact millisecond timestamps (precision <= 3) as if they
    were the wide format and fails every checkpoint (ArrayIndexOutOfBoundsException in
    BinarySegmentUtils.readTimestampData). Delta stores microseconds anyway.
    """
    return [(name, typ.replace("(3)", "(6)")) for name, typ in columns]


def _select_cast(columns: list[tuple[str, str]], exprs: dict[str, str] | None = None) -> str:
    """SELECT list casting every column to its declared type (exprs override the source expression)."""
    exprs = exprs or {}
    return ", ".join(
        f"CAST({exprs.get(name, f'`{name}`')} AS {typ})" if "TIMESTAMP" in typ else exprs.get(name, f"`{name}`")
        for name, typ in columns
    )


def _cols(columns: list[tuple[str, str]]) -> str:
    return ",\n    ".join(f"`{name}` {typ}" for name, typ in columns)


def _names(columns: list[tuple[str, str]]) -> str:
    return ", ".join(f"`{name}`" for name, _ in columns)


def build_statements(cfg: JobConfig) -> tuple[list[str], list[str]]:
    """(setup DDL, INSERTs). The INSERTs run together as one statement set, i.e. one Flink job that
    reads Kafka once and fans out to five sinks."""
    ns = cfg.namespace
    bronze, gold = f"lakehouse.{ns}_bronze.recentchange", f"lakehouse.{ns}_gold.edits_per_minute"
    bronze_cols, gold_cols = _RECORD_COLUMNS + _BRONZE_EXTRA, _GOLD_COLUMNS

    # Option dicts live outside the f-strings below: the Flink image runs Python 3.10, which
    # doesn't allow comments (or reused quotes) inside f-string expressions.
    source_options = {
        "connector": "kafka",
        "topic": cfg.raw_topic,
        "properties.bootstrap.servers": cfg.kafka_bootstrap,
        "properties.group.id": cfg.group_id,
        "scan.startup.mode": "group-offsets",
        "properties.auto.offset.reset": "earliest",
        "format": "json",
        "json.ignore-parse-errors": "true",
        # Keyed traffic leaves some partitions quiet. The watermark is the minimum across
        # partitions, so a quiet one would hold every window open: mark it idle after 10 s.
        "scan.watermark.idle-timeout": "10s",
    }
    kafka_sink_options = {
        "connector": "kafka",
        "topic": cfg.agg_topic,
        "properties.bootstrap.servers": cfg.kafka_bootstrap,
        "key.format": "json",
        "key.fields": "wiki",
        "value.format": "json",
        "value.fields-include": "ALL",
        "sink.delivery-guarantee": "at-least-once",
    }

    setup = [
        iceberg_catalog_ddl(cfg.lakekeeper_url, cfg.warehouse, cfg.s3_endpoint, cfg.access_key, cfg.secret_key),
        f"CREATE DATABASE IF NOT EXISTS lakehouse.{ns}_bronze",
        f"CREATE DATABASE IF NOT EXISTS lakehouse.{ns}_gold",
        f"CREATE TABLE IF NOT EXISTS {bronze} (\n    {_cols(bronze_cols)}\n) PARTITIONED BY (`event_date`)",
        f"CREATE TABLE IF NOT EXISTS {gold} (\n    {_cols(gold_cols)}\n)",
        # Source: event time parsed from the record's ISO string (UTC); late data tolerated for
        # watermark_delay_s. A new consumer group starts from the earliest offset.
        f"""CREATE TEMPORARY TABLE recentchange_src (
    {_cols(_RECORD_COLUMNS)},
    event_ts AS CAST(TO_TIMESTAMP(REPLACE(REPLACE(`event_time`, 'T', ' '), 'Z', ''), 'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP_LTZ(3)),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '{cfg.watermark_delay_s}' SECOND
) WITH (
    {_with(source_options)}
)""",
        f"CREATE TEMPORARY TABLE delta_bronze (\n    {_cols(_micros(bronze_cols))}\n) WITH (\n    "
        f"{delta_options(f's3a://delta/{ns}_bronze/recentchange', partitions='event_date')}\n)",
        f"CREATE TEMPORARY TABLE delta_gold (\n    {_cols(_micros(gold_cols))}\n) WITH (\n    "
        f"{delta_options(f's3a://delta/{ns}_gold/edits_per_minute')}\n)",
        f"""CREATE TEMPORARY TABLE kafka_gold (
    {_cols(gold_cols)}
) WITH (
    {_with(kafka_sink_options)}
)""",
        # 1-minute tumbling windows per wiki. A window is emitted once, when the watermark passes
        # its end; events arriving later than that are dropped here (but still land in bronze).
        f"""CREATE TEMPORARY VIEW gold_view AS
SELECT
    window_start,
    window_end,
    `wiki`,
    COUNT(*) AS edits,
    SUM(CASE WHEN `bot` THEN 1 ELSE 0 END) AS bot_edits,
    SUM(CASE WHEN `bot` THEN 0 ELSE 1 END) AS human_edits,
    SUM(CASE WHEN `type` = 'new' THEN 1 ELSE 0 END) AS new_pages,
    SUM(CASE WHEN `byte_delta` > 0 THEN CAST(`byte_delta` AS BIGINT) ELSE 0 END) AS bytes_added,
    SUM(CASE WHEN `byte_delta` < 0 THEN CAST(-`byte_delta` AS BIGINT) ELSE 0 END) AS bytes_removed
FROM TABLE(TUMBLE(TABLE recentchange_src, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, `wiki`""",
    ]

    bronze_exprs = {"event_ts": "event_ts", "event_date": "CAST(event_ts AS DATE)"}
    bronze_select = f"SELECT {_names(_RECORD_COLUMNS)}, event_ts, CAST(event_ts AS DATE) FROM recentchange_src"
    gold_select = f"SELECT {_names(gold_cols)} FROM gold_view"
    inserts = [
        f"INSERT INTO {bronze} {bronze_select}",
        f"INSERT INTO delta_bronze SELECT {_select_cast(_micros(bronze_cols), bronze_exprs)} FROM recentchange_src",
        f"INSERT INTO {gold} {gold_select}",
        f"INSERT INTO delta_gold SELECT {_select_cast(_micros(gold_cols))} FROM gold_view",
        f"INSERT INTO kafka_gold {gold_select}",
    ]
    return setup, inserts
