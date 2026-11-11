# Kafka (KRaft) and Kafka UI

A single Apache Kafka 4.3 node running in **KRaft** mode: one process is both broker and
controller, with no ZooKeeper. Profile: `streaming` (`make up-stream`).

| Port | What |
|---|---|
| `localhost:9092` | Kafka, for clients on the host |
| `kafka:29092` | Kafka, for containers on the Compose network |
| http://localhost:8080 | Kafka UI (kafbat): topics, messages, consumer groups |

## Why two listeners

A Kafka client first asks the broker "where are you?" and then connects to the address the broker
*advertises*. Containers must be told `kafka:29092`, while the host must be told
`localhost:9092`, so the broker advertises one listener for each (`KAFKA_ADVERTISED_LISTENERS`
in `compose.yaml`). A third listener, `CONTROLLER` on 9093, is KRaft's internal quorum traffic.

## Topics

`make stream-setup` creates these (3 partitions each, replication 1 because there's one broker):

| Topic | Written by | Read by |
|---|---|---|
| `wikimedia.recentchange` | the producer (`src/de_playground/streaming/producer.py`) | Flink, Spark, ClickHouse |
| `wikimedia.edits_per_minute.flink` | the Flink job | ClickHouse |
| `wikimedia.edits_per_minute.spark` | the Spark Structured Streaming job | ClickHouse |

## Things to try

- Kafka UI → Topics → `wikimedia.recentchange` → Messages, to watch events arrive live.
- Kafka UI → Consumers, to see Flink, Spark and ClickHouse each tracking their own offsets on
  the same topic.
- Inside the container:
  `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic wikimedia.recentchange --max-messages 5`

## The producer

The `producer` service (`src/de_playground/streaming/producer.py`) starts with `make up-stream`.
It reads Wikimedia's public EventStreams feed
(`https://stream.wikimedia.org/v2/stream/recentchange`), flattens each event to a small record
(`streaming/events.py`) and sends it keyed by `wiki`.

- **Reconnects** resume where they left off, using the SSE `Last-Event-ID` header.
- **Skipped:** canary (health-check) events and malformed JSON.
- **Fallback:** if the feed is unreachable for 5 attempts in a row, `--source auto` (the default)
  switches to synthetic events with the same shape (`streaming/synthetic.py`).
  Use `--source synthetic` to force it.

Checked when this was built: the live feed delivered about 40 events/s, but **unevenly across
partitions** (573 / 384 / 40 after ~25 s). Messages are keyed by wiki and a handful of wikis
(Wikidata, Commons, English Wikipedia) dominate. That skew is why the Flink job sets a source
idle timeout: see `docker/flink/README.md`.
