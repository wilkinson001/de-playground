"""ClickHouse ingests the streaming topics: Kafka engine table → materialized view → MergeTree.

The usual ClickHouse pattern: a `Kafka` engine table is a consumer (reading it consumes
messages), and a materialized view moves each batch into a real table. Delivery from Kafka is
at-least-once, so the tables are ReplacingMergeTree keyed on the event / window: duplicates are
collapsed at merge time, or immediately when queried with FINAL.
"""

from __future__ import annotations

import clickhouse_connect

from de_playground.config import Settings
from de_playground.streaming.topics import AGG_TOPICS, RAW_TOPIC

_RAW_QUEUE_COLUMNS = """
    event_id String, event_time String, wiki String, server_name Nullable(String),
    type Nullable(String), namespace Nullable(Int32), title Nullable(String), user Nullable(String),
    bot Bool, minor Bool, length_old Nullable(Int64), length_new Nullable(Int64), byte_delta Int64"""

_AGG_QUEUE_COLUMNS = """
    window_start String, window_end String, wiki String, edits UInt64, bot_edits UInt64,
    human_edits UInt64, new_pages UInt64, bytes_added UInt64, bytes_removed UInt64"""


def _queue(table: str, columns: str, topic: str, group: str, kafka: str) -> str:
    return f"""CREATE TABLE IF NOT EXISTS {table} ({columns}
) ENGINE = Kafka SETTINGS
    kafka_broker_list = '{kafka}', kafka_topic_list = '{topic}', kafka_group_name = '{group}',
    kafka_format = 'JSONEachRow', kafka_skip_broken_messages = 100"""


def statements(raw_topic: str, agg_topics: dict[str, str], kafka: str = "kafka:29092",
               database: str = "wikimedia") -> list[str]:
    db = database
    stmts = [
        f"""CREATE TABLE IF NOT EXISTS {db}.recentchange (
    event_id String, event_time DateTime64(3, 'UTC'), wiki LowCardinality(String),
    server_name Nullable(String), type LowCardinality(Nullable(String)), namespace Nullable(Int32),
    title Nullable(String), user Nullable(String), bot Bool, minor Bool,
    length_old Nullable(Int64), length_new Nullable(Int64), byte_delta Int64
) ENGINE = ReplacingMergeTree PARTITION BY toDate(event_time) ORDER BY (wiki, event_id)""",
        _queue(f"{db}.recentchange_queue", _RAW_QUEUE_COLUMNS, raw_topic, f"clickhouse-{db}-raw", kafka),
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.recentchange_mv TO {db}.recentchange AS
SELECT event_id, parseDateTime64BestEffort(event_time, 3, 'UTC') AS event_time, wiki, server_name, type,
       namespace, title, user, bot, minor, length_old, length_new, byte_delta
FROM {db}.recentchange_queue""",
        f"""CREATE TABLE IF NOT EXISTS {db}.edits_per_minute (
    engine LowCardinality(String), window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
    wiki LowCardinality(String), edits UInt64, bot_edits UInt64, human_edits UInt64, new_pages UInt64,
    bytes_added UInt64, bytes_removed UInt64
) ENGINE = ReplacingMergeTree ORDER BY (engine, wiki, window_start)""",
    ]
    for engine, topic in agg_topics.items():
        queue = f"{db}.edits_per_minute_{engine}_queue"
        stmts += [
            _queue(queue, _AGG_QUEUE_COLUMNS, topic, f"clickhouse-{db}-{engine}", kafka),
            # The engines serialise timestamps differently (Flink "2026-01-15 10:00:00", Spark
            # ISO-8601 with Z): parseDateTime64BestEffort reads both.
            f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.edits_per_minute_{engine}_mv TO {db}.edits_per_minute AS
SELECT '{engine}' AS engine,
       parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
       parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
       wiki, edits, bot_edits, human_edits, new_pages, bytes_added, bytes_removed
FROM {queue}""",
        ]
    return stmts


def run(settings: Settings, raw_topic: str = RAW_TOPIC, agg_topics: dict[str, str] = AGG_TOPICS,
        database: str = "wikimedia") -> None:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port, username="default", password=""
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    for stmt in statements(raw_topic, agg_topics, settings.kafka_bootstrap, database):
        client.command(stmt)
