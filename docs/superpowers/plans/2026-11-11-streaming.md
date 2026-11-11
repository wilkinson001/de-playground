# Streaming (Phase 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream Wikimedia recent changes, falling back to synthetic events, through Kafka into two streaming engines, a PyFlink job and Spark Structured Streaming, that do the same work. Each lands raw events in Iceberg **and** Delta (bronze) and writes per-minute windowed aggregates to Iceberg, Delta and its own Kafka topic (gold), and a test proves they agree. ClickHouse ingests the topics for live, side-by-side queries, and Flink is added to the engine × format matrix.

**Architecture:** A new Compose profile `streaming` adds Kafka (KRaft), Kafka UI, a producer service, and Flink (JobManager, TaskManager, SQL Gateway) built on one custom image with Kafka, Iceberg and Delta connectors. All Python is in the existing `de_playground` package under `streaming/`. The Flink job is Flink SQL rendered by a pure-stdlib module and run by a PyFlink entrypoint inside the Flink containers. Spark Structured Streaming runs on the existing Spark Connect server: a small `spark-stream` service (runner image) starts its queries and keeps the session open. Everything else (producer, setup, tests) runs in the existing `runner`.

**Tech Stack:** Apache Kafka 4.3 (KRaft), kafbat Kafka UI, confluent-kafka (Python), Apache Flink 2.0.1 + PyFlink, `iceberg-flink-runtime-2.0` 1.10.2, `delta-flink_2.0` 4.4.1 (experimental, sink-only), `flink-sql-connector-kafka` 4.0.1-2.0, Spark 4.0.1 Structured Streaming + `spark-sql-kafka-0-10_2.13:4.0.1`, ClickHouse Kafka engine, pyiceberg + deltalake for verification.

**Spec:** `docs/superpowers/specs/2026-11-11-de-playground-design.md` (Streaming track, and the research note on Flink 2.0). This plan builds on the phase 1–2 code on `main`; read `README.md` and `docs/architecture.md` first.

## Global Constraints

- arm64-native images, each pinned to an exact tag at least 2 weeks old: `apache/kafka:4.3.2`, `kafbat/kafka-ui:v1.5.0`, `flink:2.0.1-java17`. Python: `confluent-kafka==2.15.0`, `apache-flink==2.0.1` (only in the Flink image, never in the runner).
- Flink connector set. Bump the versions only together, and only after Task 4's smoke test passes:
  - `iceberg-flink-runtime-2.0:1.10.2`
  - `iceberg-aws-bundle:1.10.2`
  - `delta-flink_2.0:4.4.1`, with its transitive runtime deps
  - `flink-sql-connector-kafka:4.0.1-2.0`
- Flink must write **both Iceberg and Delta** (owner's decision). Flink cannot read Delta (the connector is sink-only), so Delta output is verified from Python (`deltalake`) or Spark.
- **Spark Structured Streaming does the same operations as Flink** (owner's decision): same source topic, same bronze columns, same gold columns and window semantics (1-minute tumbling, 30 s watermark, append mode). Spark's Kafka jars are added to the existing Spark image: `spark-sql-kafka-0-10_2.13:4.0.1`, `spark-token-provider-kafka-0-10_2.13:4.0.1`, plus the `kafka-clients` and `commons-pool2` versions named in that artifact's pom.
- Every published port binds to `127.0.0.1`. Memory limits:

  | Service | Limit |
  |---|---|
  | kafka | 1g |
  | kafka-ui | 512m |
  | producer | 256m |
  | flink-jobmanager | 1536m |
  | flink-taskmanager | 3g |
  | flink-sql-gateway | 1g |
  | spark-stream | 512m |

  `make up-stream` (core + streaming + clickhouse) must stay under ~16 GB; adding Spark (`make stream-spark`) must stay under ~24 GB.
- Kafka listeners: containers use `kafka:29092`; the host uses `localhost:9092`.
- Topics, 3 partitions each: `wikimedia.recentchange` (normalised raw events, read by both engines), `wikimedia.edits_per_minute.flink` and `wikimedia.edits_per_minute.spark` (each engine's aggregates).
- Table naming follows phase 2. Each engine has its own namespace, so the two never collide: Flink uses `flink_stream` and Spark uses `spark_stream`.
  - Iceberg: `<ns>_bronze.recentchange` and `<ns>_gold.edits_per_minute` in the Lakekeeper warehouse `lakehouse`.
  - Delta: `s3a://delta/<ns>_bronze/recentchange` and `s3a://delta/<ns>_gold/edits_per_minute`.
  - Integration tests use their own namespaces and topics, and remove them afterwards.
- The Wikimedia `User-Agent` names the project and repo (`de-playground/0.1 (https://github.com/wilkinson001/de-playground)`). Never put a personal email in it.
- Modules under `src/de_playground/streaming/flink/` import only the stdlib and `pyflink`. They run inside the Flink image, which doesn't have the runner's dependencies.
- Event time is UTC. Watermark delay is 30 s. Tumbling windows are 1 minute. `table.exec.source.idle-timeout` is 10 s. The checkpoint interval is 30 s by default (5 s in tests), stored in `file:///opt/flink/checkpoints` on a volume shared by the JobManager and TaskManager. Spark: `withWatermark("event_ts", "30 seconds")`, trigger every 10 s, checkpoints under `/opt/spark/checkpoints/<ns>/<query>` on a `sparkcheckpoints` volume mounted in `spark-connect`.
- Each new component gets a `docker/<component>/README.md` (what it is, wiring, things to try), as in phases 1–2.

## Review Focus

1. **Late or out-of-order events.** An event arriving more than 30 s behind the watermark is dropped from the window aggregates but still lands in bronze. Test: `test_late_event_kept_in_bronze_excluded_from_window` (Task 5).
2. **Idle Kafka partitions stall the watermark.** With low volume or keyed traffic, some partitions get nothing, and windows must still emit. Test: `test_windows_emit_with_idle_partitions` (Task 5).
3. **Wikimedia feed hiccups.** After a disconnect the producer reconnects and resumes from `Last-Event-ID`. Canary events (`meta.domain == "canary"`) and malformed JSON are skipped, and the producer doesn't crash. Tests: `test_canary_and_malformed_skipped` (Task 2) and `test_reconnect_resumes_from_last_event_id` (Task 3).
4. **Duplicates after restarts.** At-least-once delivery means a restarted Flink job, Spark query or producer can resend messages. ClickHouse tables collapse those duplicates (`ReplacingMergeTree`, queried with `FINAL`). Test: `test_duplicate_messages_collapse` (Task 7).
5. **Restarting either streaming job** must replace it rather than run two writers. For Spark this includes stopping the `spark-stream` container, since Spark Connect keeps a disconnected session's queries running until the session times out. Tests: `test_resubmit_replaces_running_job` (Task 5), `test_restart_replaces_spark_queries` and `test_sigterm_stops_spark_queries` (Task 6).

---

## File Structure

```
compose.yaml                               # + kafka, kafka-ui, producer, flink-* (profile streaming); spark-stream (profile spark-stream)
Makefile                                   # + up-stream, stream-setup, stream-job, stream-status, stream-stop, stream-spark, stream-spark-stop
docker/spark/Dockerfile                    # + Spark Kafka connector jars
docker/kafka/README.md
docker/flink/Dockerfile                    # multi-stage: maven resolves connector jars → flink:2.0.1-java17 + python3 + apache-flink
docker/flink/pom.xml                       # connector dependency list (flink-* excluded / provided)
docker/flink/config.yaml                   # checkpointing, memory, python.executable
docker/flink/README.md
src/de_playground/config.py                # + kafka_bootstrap, flink_url, flink_sql_gateway_url
src/de_playground/streaming/__init__.py
src/de_playground/streaming/__main__.py    # setup | produce | job-status | job-cancel
src/de_playground/streaming/topics.py      # ensure_topics
src/de_playground/streaming/events.py      # SSE parsing + normalisation
src/de_playground/streaming/synthetic.py   # synthetic Wikimedia-shaped events
src/de_playground/streaming/producer.py    # SSE → Kafka (auto/wikimedia/synthetic)
src/de_playground/streaming/flink_control.py  # REST: list/cancel jobs (runner side)
src/de_playground/streaming/flink_gateway.py  # SQL Gateway client (runner side; tests + matrix)
src/de_playground/streaming/flink/__init__.py
src/de_playground/streaming/flink/statements.py  # stdlib-only: renders the job's Flink SQL
src/de_playground/streaming/flink/job.py   # PyFlink entrypoint (runs in Flink containers)
src/de_playground/streaming/spark_stream.py  # Spark Structured Streaming: same ops as the Flink job
src/de_playground/streaming/clickhouse_streaming.py  # Kafka-engine ingestion DDL
src/de_playground/matrix/probes.py         # + FlinkAdapter (via SQL Gateway)
docs/architecture.md, README.md            # + streaming
tests/unit/test_streaming_*.py, tests/integration/test_streaming_*.py
```

---

### Task 1: Kafka, Kafka UI and topics

**Files:**
- Modify: `src/de_playground/config.py`, `compose.yaml`, `Makefile`, `pyproject.toml` (+ `confluent-kafka==2.15.0`)
- Create: `src/de_playground/streaming/{__init__,__main__,topics}.py`, `docker/kafka/README.md`
- Test: `tests/unit/test_config.py` (extend), `tests/integration/test_kafka.py`

**Interfaces:**
- Produces:
  - New `Settings` fields:

    | Field | Env var | Default |
    |---|---|---|
    | `kafka_bootstrap` | `PLAYGROUND_KAFKA_BOOTSTRAP` | `kafka:29092` |
    | `flink_url` | `PLAYGROUND_FLINK_URL` | `http://flink-jobmanager:8081` |
    | `flink_sql_gateway_url` | `PLAYGROUND_FLINK_SQL_GATEWAY_URL` | `http://flink-sql-gateway:8083` |

  - `topics.RAW_TOPIC = "wikimedia.recentchange"`, `topics.AGG_TOPICS = {"flink": "wikimedia.edits_per_minute.flink", "spark": "wikimedia.edits_per_minute.spark"}`.
  - `topics.ensure_topics(settings, names: list[str] | None = None, partitions: int = 3) -> list[str]` returns the topics it created. It is idempotent and defaults to all three topics.
  - `python -m de_playground.streaming setup` runs `ensure_topics`; Task 6 extends it to also set up ClickHouse.
  - Compose (profile `streaming`):
    - `kafka`: `apache/kafka:4.3.2`, KRaft combined broker+controller. Listeners `PLAINTEXT://kafka:29092`, `HOST://localhost:9092` and `CONTROLLER`. Port `127.0.0.1:9092`. Healthcheck with `kafka-broker-api-versions.sh`. Named volume `kafkadata`.
    - `kafka-ui`: `kafbat/kafka-ui:v1.5.0` on `127.0.0.1:8080`, cluster `playground` → `kafka:29092`.
  - Make targets:
    - Split the Makefile's profile list: `ALL_PROFILES` (every profile: core, spark, clickhouse, streaming, spark-stream) is used by `RUN`, `down`, `nuke`, `ps` and `logs`, so the runner can resolve every dependency. `up-batch` keeps starting only core + spark + clickhouse.
    - `make up-stream` = profiles core + streaming + clickhouse, `up -d --wait`.
    - `make stream-setup` = `$(RUN) uv run python -m de_playground.streaming setup`.
- [ ] **Step 1: Write the failing tests.**
  - Extend `test_settings_from_env_defaults` with the three new defaults.
  - Integration `test_ensure_topics_idempotent`: `ensure_topics(settings, ["itest.topics.<uuid>"])` returns that name; a second call returns `[]`; the topic has 3 partitions per the AdminClient metadata.
  - Integration `test_produce_consume_roundtrip`: produce 3 JSON messages to a fresh topic and consume them back with a fresh group, `auto.offset.reset=earliest`.
- [ ] **Step 2: Run** `uv run pytest tests/unit -q` and `make up-stream && make test-integration ARGS=tests/integration/test_kafka.py`. Expect FAIL.
- [ ] **Step 3: Implement.** In the README, cover listener naming (why two listeners), KRaft, and things to try (Kafka UI, `kafka-console-consumer.sh` inside the container).
- [ ] **Step 4: Run.** Expect PASS. Then run `make nuke && make up-stream && make stream-setup && make stream-setup`; the second run should report no topics created.
- [ ] **Step 5: Commit.** `feat: kafka (KRaft), kafka-ui and topic setup`

---

### Task 2: Event parsing, normalisation and synthetic events

**Files:**
- Create: `src/de_playground/streaming/events.py`, `src/de_playground/streaming/synthetic.py`
- Test: `tests/unit/test_streaming_events.py`, `tests/unit/test_streaming_synthetic.py`

**Interfaces:**
- Produces:
  - `SseEvent(id: str | None, data: str)` (frozen dataclass).
  - `parse_sse(lines: Iterable[str]) -> Iterator[SseEvent]`. This is standard SSE:
    - `data:` lines accumulate, joined with `\n`, and a blank line dispatches the event;
    - `id:` sets the id;
    - lines starting with `:` (comments and heartbeats) and the `event:` and `retry:` fields are ignored;
    - a trailing event with no blank line after it is not dispatched.
  - `RECORD_FIELDS`, the normalised record keys, in order: `event_id` (str, the Wikimedia `meta.id`), `event_time` (str, UTC ISO-8601 with milliseconds, e.g. `2026-11-11T10:00:05.123Z`, from `meta.dt`), `wiki`, `server_name`, `type` (`edit|new|log|categorize|external`), `namespace` (int), `title`, `user`, `bot` (bool), `minor` (bool, default False), `length_old` (int|None), `length_new` (int|None), `byte_delta` (int: `length_new - length_old`, with missing lengths treated as 0).
  - `to_record(raw: dict) -> dict | None` returns a dict with exactly `RECORD_FIELDS`, or None when the event is a canary (`meta.domain == "canary"`) or is missing `meta.id`, `meta.dt` or `wiki`.
  - `decode(event: SseEvent) -> dict | None` returns `to_record(json.loads(event.data))`, or None on invalid JSON.
  - `synthetic.WIKIS = ["enwiki", "dewiki", "frwiki", "jawiki", "commonswiki", "wikidatawiki"]`.
  - `synthetic.raw_events(rng: random.Random, start: datetime, count: int | None = None, interval_s: float = 0.2) -> Iterator[dict]` yields Wikimedia-shaped raw dicts (with `meta.id`, `meta.dt`, `meta.domain`, `wiki`, `server_name`, `type`, `namespace`, `title`, `user`, `bot`, `minor`, `length.old/new`), with event times advancing by `interval_s`. About 30% are bots, about 10% are `new`, and the rest are `edit`. It is deterministic for a given seed.
- [ ] **Step 1: Write the failing tests.**
  - `test_parse_sse_multiline_ids_and_comments`: a stream with a `:heartbeat` line, an event with `id:` and two `data:` lines, and a final unterminated event. It yields exactly one `SseEvent("x1", "a\nb")`.
  - `test_to_record_edit`: a realistic Wikimedia edit dict (fixture JSON in the test) maps to the exact expected record, including `event_time` with a `Z` suffix and `byte_delta = new - old`.
  - `test_to_record_new_page_without_old_length`: `byte_delta == length_new`.
  - `test_canary_and_malformed_skipped`: the canary returns None; `decode(SseEvent(None, "{not json"))` returns None; the missing-`meta.dt` case returns None.
  - `test_synthetic_deterministic_and_valid`: two generators seeded 7 produce identical first 50 events; every `to_record(e)` is not None; event times strictly increase by `interval_s`.
- [ ] **Step 2: Run.** Expect FAIL. **Step 3: Implement.** **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit.** `feat: wikimedia SSE parsing, normalisation and synthetic events`

---

### Task 3: Producer service

**Files:**
- Create: `src/de_playground/streaming/producer.py`
- Modify: `src/de_playground/streaming/__main__.py` (`produce`), `compose.yaml` (service `producer`), `docker/kafka/README.md`
- Test: `tests/unit/test_streaming_producer.py`, `tests/integration/test_streaming_producer.py`

**Interfaces:**
- Consumes: `parse_sse`, `decode`, `synthetic.raw_events`, `to_record`, `topics.RAW_TOPIC`.
- Produces:
  - `WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"`, `USER_AGENT` per the Global Constraints.
  - `class Sink(Protocol): def send(self, key: str, value: dict) -> None; def flush(self) -> None`. `KafkaSink(settings, topic)` sends JSON values with `wiki` as the key, `acks=all`, `linger.ms=50`.
  - `wikimedia_records(http: httpx.Client, max_retries: int = 5, backoff_s: float = 2.0) -> Iterator[dict]` streams with an `Accept: text/event-stream` header. On any transport error or non-200, it reconnects and sends `Last-Event-ID` with the last seen SSE id. After `max_retries` consecutive failures it raises `FeedUnavailable`.
  - `run(sink: Sink, source: Literal["auto", "wikimedia", "synthetic"], max_events: int | None = None, http: httpx.Client | None = None, rng_seed: int | None = None) -> int` returns the number of records sent. `auto` uses Wikimedia and switches permanently to synthetic on `FeedUnavailable`, logging one warning. Synthetic mode paces events in real time (`interval_s`) and stamps them with the current time.
  - `python -m de_playground.streaming produce [--source auto|wikimedia|synthetic] [--max-events N] [--topic T]`.
  - Compose service `producer` (profile `streaming`): runner image, command `uv run python -m de_playground.streaming produce --source auto`, `restart: unless-stopped`, depends on healthy kafka, `mem_limit` 256m. The producer does not create topics; `make stream-setup` does.
- [ ] **Step 1: Write the failing unit tests**, using `httpx.MockTransport` and an in-memory `ListSink`.
  - `test_run_wikimedia_sends_normalised_records`: an SSE body with 3 events, one of them a canary, gives 2 sends keyed by wiki.
  - `test_reconnect_resumes_from_last_event_id`: the first response yields one event with id `e1` and then raises `httpx.ReadError`. The second request carries the header `Last-Event-ID: e1`.
  - `test_auto_falls_back_to_synthetic`: the transport always returns 503, so `run(..., source="auto", max_events=5)` sends 5 synthetic records.
  - `test_user_agent_has_no_email`: the request's User-Agent equals `USER_AGENT` and does not contain `@`.
- [ ] **Step 2: Run.** Expect FAIL. **Step 3: Implement.**
- [ ] **Step 4: Integration** `test_synthetic_producer_to_kafka`: `run(KafkaSink(settings, test_topic), "synthetic", max_events=20)`, then consume the 20 messages, each with all `RECORD_FIELDS`. This is not tied to the live feed. Expect PASS.
- [ ] **Step 5: Manual check.** Run `make up-stream && make stream-setup`, and check that Kafka UI shows `wikimedia.recentchange` growing. Note in the README whether the live feed or the synthetic fallback was used.
- [ ] **Step 6: Commit.** `feat: wikimedia producer with reconnect and synthetic fallback`

---

### Task 4: Flink image and cluster — connector spike (de-risk first)

**Files:**
- Create: `docker/flink/{Dockerfile,pom.xml,config.yaml,README.md}`, `src/de_playground/streaming/flink_gateway.py`, `src/de_playground/streaming/flink/{__init__,statements}.py`
- Modify: `compose.yaml` (`flink-jobmanager`, `flink-taskmanager`, `flink-sql-gateway`, volume `flinkcheckpoints`), `Makefile`
- Test: `tests/integration/test_flink_formats.py`, `tests/unit/test_streaming_statements.py`

**Interfaces:**
- Produces:
  - Image `de-playground/flink:2.0.1`. Stage 1 (`maven:3.9-eclipse-temurin-17`) runs `mvn dependency:copy-dependencies` on `pom.xml`. The pom lists the four connector artifacts plus `hadoop-client-api`/`hadoop-client-runtime` (needed by Delta Kernel and Iceberg's Hadoop config) and the AWS SDK bundle that `delta-flink` expects. `org.apache.flink:*` is excluded (the image provides it). Stage 2 is `flink:2.0.1-java17` with `python3`, `python3-pip`, `pip install apache-flink==2.0.1`, and the jars copied into `/opt/flink/lib`. If a jar conflict appears (duplicate Hadoop/AWS classes between `iceberg-aws-bundle` and Delta's deps), resolve it in the pom with exclusions and record the decision in the README.
  - `config.yaml`: `taskmanager.numberOfTaskSlots: 4`, `taskmanager.memory.process.size: 2560m`, `jobmanager.memory.process.size: 1280m`, checkpointing `30s` to `file:///opt/flink/checkpoints`, `python.executable: python3`, `table.exec.source.idle-timeout: 10s`.
  - Compose (profile `streaming`):
    - `flink-jobmanager` (`jobmanager`) on `127.0.0.1:8081`, with the repo `src/` mounted read-only at `/opt/playground/src`.
    - `flink-taskmanager` (`taskmanager`), with the same mount.
    - `flink-sql-gateway`: `sql-gateway.sh start-foreground -Dsql-gateway.endpoint.rest.address=0.0.0.0 -Drest.address=flink-jobmanager`, on `127.0.0.1:8083`.
    - All three use `env_file: .env`, share the `flinkcheckpoints` volume, have healthchecks on their REST endpoints, and depend on healthy kafka, lakekeeper and seaweedfs.
  - `flink_gateway.FlinkSql(settings)` manages a session over SQL Gateway REST v3:
    - `open() -> session handle`;
    - `execute(sql: str, timeout_s: float = 120) -> list[list]` submits a statement and polls results until FINISHED. For `INSERT` in batch mode it waits for the job to finish.
    - `close()`.
    - Context-manager support.
  - Shared helpers in `streaming/flink/statements.py`, introduced here and used by Tasks 5 and 7:
    - `iceberg_catalog_ddl(lakekeeper_url: str, warehouse: str, s3_endpoint: str, access_key: str, secret_key: str) -> str` renders `CREATE CATALOG lakehouse WITH ('type'='iceberg','catalog-type'='rest','uri'=…/catalog,'warehouse'=…,'io-impl'='org.apache.iceberg.aws.s3.S3FileIO','s3.endpoint'=…,'s3.path-style-access'='true','client.region'='us-east-1','s3.access-key-id'=…,'s3.secret-access-key'=…)`.
    - `delta_options(path: str, s3_endpoint: str, access_key: str, secret_key: str, partitions: str | None = None) -> str` renders the `WITH (...)` body: `'connector'='delta'`, `'table_path'=path`, `'credentials.source'='ambient'`, plus per-table `'fs.s3a.endpoint'`, `'fs.s3a.access.key'`, `'fs.s3a.secret.key'`, `'fs.s3a.path.style.access'='true'`, `'fs.s3a.connection.ssl.enabled'='false'`, and `'partitions'` when given.
- [ ] **Step 1: Write the failing integration tests.** Each runs in batch mode (`SET 'execution.runtime-mode' = 'batch'`) through `FlinkSql`, in namespace `itest_flink_<uuid>`, and cleans up.
  - `test_flink_writes_iceberg`: create the catalog, create database `lakehouse.itest_flink_x`, create table `t (id INT, v STRING)`, `INSERT INTO … VALUES (1,'a'),(2,'b')`. Reading with pyiceberg gives 2 rows.
  - `test_flink_writes_delta`: `CREATE TEMPORARY TABLE d (id INT, v STRING) WITH (<delta_options('s3a://delta/itest_flink_x/t')>)`, then insert 2 rows. Reading with `deltalake.DeltaTable(...).to_pyarrow_table()` gives 2 rows.
  - `test_flink_reads_kafka`: produce 3 JSON messages to a fresh topic; a bounded Kafka source (`'scan.bounded.mode'='latest-offset'`) counts 3.
  - Unit `test_statements_helpers_render`: exact substrings for `iceberg_catalog_ddl` and `delta_options`, and no secrets other than those passed in.
- [ ] **Step 2: Run** `make up-stream && make test-integration ARGS=tests/integration/test_flink_formats.py`. Expect FAIL.
- [ ] **Step 3: Implement.** This is the spike: iterate on the pom until all three tests pass, and record every exclusion or added jar, with the reason, in `docker/flink/README.md`. If `delta-flink_2.0:4.4.1` cannot write to SeaweedFS after a genuine attempt, stop and report to the owner with the error. Do not silently drop Delta: it is an owner requirement.
- [ ] **Step 4: Run.** Expect PASS. Check that `docker stats` shows the Flink containers under their limits.
- [ ] **Step 5: Commit.** `feat: flink 2.0 cluster with kafka, iceberg and delta connectors`

---

### Task 5: The Flink streaming job

**Files:**
- Modify: `src/de_playground/streaming/flink/statements.py`
- Create: `src/de_playground/streaming/flink/job.py`, `src/de_playground/streaming/flink_control.py`
- Modify: `src/de_playground/streaming/__main__.py` (`job-status`, `job-cancel`), `Makefile` (`stream-job`, `stream-status`, `stream-stop`)
- Test: `tests/unit/test_streaming_statements.py`, `tests/integration/test_streaming_job.py`

**Interfaces:**
- Consumes: the Task 4 helpers, the Task 2 record schema, and the topics.
- Produces:
  - `JobConfig` (frozen dataclass): `namespace="flink_stream"`, `raw_topic`, `agg_topic` (default `AGG_TOPICS["flink"]`), `kafka_bootstrap`, `group_id="flink-wikimedia"`, `lakekeeper_url`, `warehouse`, `s3_endpoint`, `access_key`, `secret_key`, `checkpoint_interval="30s"`, `watermark_delay_s=30`, `job_name="wikimedia-streaming"`. `JobConfig.from_env(env)` reads the same env vars as `Settings`, plus `STREAM_NAMESPACE`, `STREAM_RAW_TOPIC`, `STREAM_AGG_TOPIC`, `STREAM_GROUP_ID` and `STREAM_CHECKPOINT_INTERVAL`, so tests can override them.
  - `build_statements(cfg: JobConfig) -> tuple[list[str], list[str]]` returns `(setup_ddl, inserts)`. The inserts run together as one `StatementSet` (one job).
    - **source** `recentchange_src`: Kafka, JSON, `RECORD_FIELDS` columns; `event_ts AS TO_TIMESTAMP_LTZ(…)` parsed from `event_time`; `WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND`; `'scan.startup.mode'='group-offsets'`, `'properties.auto.offset.reset'='earliest'`.
    - **bronze**: Iceberg `lakehouse.<ns>_bronze.recentchange` (all fields + `event_date` DATE, partitioned by `event_date`) and Delta `s3a://delta/<ns>_bronze/recentchange` (same columns, `partitions='event_date'`). Both are append-only.
    - **gold**: a 1-minute `TUMBLE` on `event_ts`, grouped by `wiki`, with columns `window_start`, `window_end` (TIMESTAMP(3)), `wiki`, `edits` (count of all), `bot_edits`, `human_edits`, `new_pages` (`type='new'`), `bytes_added` (sum of positive `byte_delta`), `bytes_removed` (sum of the absolute value of negative `byte_delta`). It is written to Iceberg `lakehouse.<ns>_gold.edits_per_minute`, Delta `s3a://delta/<ns>_gold/edits_per_minute`, and Kafka `agg_topic` (JSON, at-least-once).
  - `job.py`: `main()` builds a streaming `TableEnvironment`, sets the job name, checkpoint interval and idle timeout, runs the setup DDL, and submits the inserts as one `StatementSet`. Submitted with `flink run -d -py /opt/playground/src/de_playground/streaming/flink/job.py -pyfs /opt/playground/src`.
  - `flink_control.running_jobs(settings, name: str) -> list[str]` (ids in RUNNING/CREATED/RESTARTING) and `cancel_jobs(settings, name: str) -> list[str]`.
  - Make targets:
    - `stream-job`: runner `job-cancel` first, then `$(COMPOSE) exec flink-jobmanager flink run …`. This is how re-submitting replaces the running job.
    - `stream-status`: list the jobs.
    - `stream-stop`: cancel.
- [ ] **Step 1: Write the failing unit tests.**
  - `test_build_statements_tables_and_watermark`: the rendered SQL contains the watermark clause with `INTERVAL '30' SECOND`, `TUMBLE(`, all five sink targets for namespace `flink_stream`, and `'scan.startup.mode' = 'group-offsets'`.
  - `test_build_statements_respects_namespace_and_topics`: a custom namespace and topics appear, and the defaults don't.
  - `test_statements_module_is_stdlib_only`: parse `statements.py` with `ast`. Its imports must be stdlib only, or `pyflink`.
- [ ] **Step 2: Write the failing integration tests.** Each uses fresh topics and namespace (env overrides passed to `flink run` via `-D`/env), `STREAM_CHECKPOINT_INTERVAL=5s`, synthetic events with fixed past event times produced by the test, and polling with a 120 s timeout. Verify with pyiceberg and deltalake.
  - `test_job_lands_bronze_in_iceberg_and_delta`: 60 events in minute 10:00 and 60 in 10:01 (UTC, a fixed date), then one event at 10:05 to advance the watermark. Bronze has 121 rows in both formats.
  - `test_job_aggregates_per_minute`: gold `edits` for (10:00, each wiki) matches the counts computed in the test from the produced records, in both formats and on `agg_topic`.
  - `test_late_event_kept_in_bronze_excluded_from_window`: after 10:05 has advanced the watermark, send one more event stamped 10:00:30. It appears in bronze, and the 10:00 window counts do not change.
  - `test_windows_emit_with_idle_partitions`: all events carry the same key, so they land in a single partition of a 3-partition topic. Windows still emit.
  - `test_resubmit_replaces_running_job`: submit twice. `running_jobs(name)` has length 1.
- [ ] **Step 3: Implement.** **Step 4: Run** unit and integration tests. Expect PASS.
- [ ] **Step 5: Manual check.** `make stream-job`, wait about 2 minutes, then check the Flink UI (`:8081`) for a running job and Iceberg/Delta gold rows (`make shell` → pyiceberg/deltalake snippet in the README). Record a sample in `docker/flink/README.md`.
- [ ] **Step 6: Commit.** `feat: pyflink streaming job to iceberg, delta and kafka`

---

### Task 6: Spark Structured Streaming doing the same work

**Files:**
- Modify: `docker/spark/Dockerfile` (+ Kafka connector jars), `docker/spark/README.md`, `compose.yaml` (`sparkcheckpoints` volume on `spark-connect`; service `spark-stream`), `Makefile` (`stream-spark`, `stream-spark-stop`)
- Create: `src/de_playground/streaming/spark_stream.py`
- Modify: `src/de_playground/streaming/__main__.py` (`spark-stream`)
- Test: `tests/integration/test_spark_stream.py`, `tests/unit/test_spark_stream_config.py`

**Interfaces:**
- Consumes: `RECORD_FIELDS` (Task 2), `RAW_TOPIC`/`AGG_TOPICS` (Task 1), `spark_engine` / the Spark Connect server (phase 2), the Task 5 job and its `JobConfig` (for the equivalence test).
- Produces:
  - `SparkStreamConfig` (frozen dataclass): `namespace="spark_stream"`, `raw_topic=RAW_TOPIC`, `agg_topic=AGG_TOPICS["spark"]`, `kafka_bootstrap`, `checkpoint_root="/opt/spark/checkpoints"`, `watermark="30 seconds"`, `trigger="10 seconds"`, `starting_offsets="earliest"`. `SparkStreamConfig.from_env(env)` takes the same `STREAM_*` overrides as `JobConfig`.
  - `QUERY_NAMES(cfg) -> list[str]` returns `<ns>-bronze-iceberg`, `<ns>-bronze-delta`, `<ns>-gold-iceberg`, `<ns>-gold-delta` and `<ns>-gold-kafka`. Each query checkpoints to `<checkpoint_root>/<ns>/<query name>`.
  - `parse_events(kafka_df: DataFrame) -> DataFrame` uses `from_json(value)` with a schema built from `RECORD_FIELDS`, and returns the same bronze columns as Flink: all fields, `event_ts` (timestamp from `event_time`) and `event_date`.
  - `aggregate(events: DataFrame, watermark: str) -> DataFrame` applies `withWatermark("event_ts", watermark)`, then `groupBy(window("event_ts", "1 minute"), "wiki")`, producing exactly the Flink gold columns: `window_start`, `window_end`, `wiki`, `edits`, `bot_edits`, `human_edits`, `new_pages`, `bytes_added`, `bytes_removed`. Output mode is `append`, so a window is emitted once, after the watermark passes it, as in Flink.
  - `ensure_tables(spark, cfg) -> None` creates the Iceberg namespaces and tables (partitioned like Flink's) and the Delta tables by path, with `CREATE … IF NOT EXISTS`.
  - `start_queries(spark, cfg) -> list[StreamingQuery]` starts the five queries.
    - Bronze: `toTable` for Iceberg and `format("delta").start(path)` for Delta.
    - Gold: the same two sinks, plus `format("kafka")` with `to_json(struct(*))` as the value and `wiki` as the key.
    - `spark.sql.streaming.stopActiveRunOnRestart` stays true (the Spark default), so starting with the same checkpoint stops an older run of that query.
  - `run(settings, cfg) -> None` (the `spark-stream` service entrypoint) creates the session, calls `ensure_tables` and `start_queries`, installs a SIGTERM/SIGINT handler that `stop()`s every query before exiting, and blocks on `awaitAnyTermination()`. If any query fails, it exits non-zero (Compose restarts it).
  - Compose service `spark-stream`, in its own profile `spark-stream` so `up-batch` never starts it:
    - runner image;
    - command `uv run python -m de_playground.streaming spark-stream`;
    - `restart: unless-stopped`, `stop_grace_period: 30s`;
    - depends on healthy `spark-connect` and `kafka`;
    - `mem_limit` 512m.
  - Make targets:
    - `make stream-spark` = profiles core + streaming + clickhouse + spark + spark-stream, `up -d --wait spark-connect spark-stream`;
    - `make stream-spark-stop` = `$(COMPOSE) … stop spark-stream`. The SIGTERM handler stops the queries.
- [ ] **Step 1: Write the failing tests.**
  - Unit `test_spark_stream_config_and_query_names`: defaults, `from_env` overrides, and the exact query names and checkpoint paths for namespace `spark_stream`.
  - Integration `test_spark_parse_and_aggregate_static`: run `parse_events` and `aggregate` on a *static* DataFrame of JSON strings (the same synthetic records as the Flink test). The watermark is a no-op on static data, and gold matches the counts computed in the test.
  - Integration `test_spark_stream_lands_bronze_and_gold`: fresh topics, namespace, and a `checkpoint_root` under the test id. Start the queries with a `trigger` of 2 seconds and produce the same 10:00 / 10:01 / 10:05 events as the Flink test. Bronze has 121 rows in Iceberg and Delta, and gold for 10:00 is in Iceberg, Delta and the Spark agg topic.
  - Integration `test_spark_late_event_kept_in_bronze_excluded_from_window`: same scenario as the Flink test.
  - Integration `test_spark_and_flink_gold_match`: run the Flink job (Task 5, test config) and the Spark queries over the **same** test raw topic. Gold for every completed window (10:00, 10:01) is identical across the two engines, compared through `deltalake` on each engine's Delta gold.
  - Integration `test_restart_replaces_spark_queries`: call `start_queries` from session A, then again from session B with the same config. Session A's queries are no longer active, and B has 5 active queries.
  - Integration `test_sigterm_stops_spark_queries`: start `run` in a subprocess (`python -m de_playground.streaming spark-stream` with test env), wait until it is running, then send SIGTERM. Within 30 s, a fresh session's `start_queries` with the same config starts cleanly and the subprocess has exited 0.
- [ ] **Step 2: Run** `make stream-spark` (to build the image with the Kafka jars), then the tests. Expect FAIL.
- [ ] **Step 3: Implement.** Confirm the `kafka-clients`/`commons-pool2` versions from `spark-sql-kafka-0-10_2.13-4.0.1.pom` and pin them in the Dockerfile with a comment, the same way the phase 2 jars are pinned.
- [ ] **Step 4: Run.** Expect PASS. Then re-run the phase 2 Spark integration tests (`test_spark_formats.py`, `test_spark_pipeline.py`) to confirm the image change broke nothing.
- [ ] **Step 5: Manual check.** `make stream-spark`, wait about 2 minutes, then check the Structured Streaming tab in the Spark UI (`:4040`) for 5 active queries and rows in `spark_stream_gold`.
- [ ] **Step 6: Commit.** `feat: spark structured streaming mirroring the flink job`

---

### Task 7: ClickHouse ingestion from Kafka

**Files:**
- Create: `src/de_playground/streaming/clickhouse_streaming.py`
- Modify: `src/de_playground/streaming/__main__.py` (setup also runs this when ClickHouse is up), `docker/clickhouse/README.md`
- Test: `tests/unit/test_clickhouse_streaming.py`, `tests/integration/test_clickhouse_streaming.py`

**Interfaces:**
- Produces:
  - `statements(raw_topic: str, agg_topics: dict[str, str], kafka: str = "kafka:29092", database: str = "wikimedia") -> list[str]`, idempotent (`IF NOT EXISTS`). It creates:
    - `{db}.recentchange_queue`: `ENGINE = Kafka` on `raw_topic`, `JSONEachRow`, group `clickhouse-{db}-raw`.
    - `{db}.recentchange`: `ReplacingMergeTree`, `ORDER BY (wiki, event_id)`, `PARTITION BY toDate(event_time)`, with `event_time DateTime64(3, 'UTC')`.
    - A materialized view `{db}.recentchange_mv` from the queue into the table.
    - `{db}.edits_per_minute`: `ReplacingMergeTree`, `ORDER BY (engine, wiki, window_start)`, with an `engine LowCardinality(String)` column.
    - For each `(engine, topic)` in `agg_topics`: `{db}.edits_per_minute_{engine}_queue` (`ENGINE = Kafka`, group `clickhouse-{db}-{engine}`) and a materialized view that inserts with `'{engine}' AS engine`. One table holds both engines' aggregates, so they can be compared with a `GROUP BY engine`.
  - `run(settings, raw_topic=RAW_TOPIC, agg_topics=AGG_TOPICS, database="wikimedia") -> None`.
- [ ] **Step 1: Write the failing tests.**
  - Unit `test_statements_shape`: with both engines there are 8 statements, `ENGINE = Kafka` 3 times, `ReplacingMergeTree` twice, both agg topics substituted, `'spark' AS engine` and `'flink' AS engine` present, and all statements use `IF NOT EXISTS`.
  - Integration `test_clickhouse_keeps_engines_apart`: produce the same aggregate record to both test agg topics. `SELECT engine, count() … FINAL GROUP BY engine` gives one row per engine.
  - Integration `test_clickhouse_ingests_raw_topic`, in a test database with test topics: produce 10 records, and within 60 s `SELECT count() FROM db.recentchange FINAL` returns 10.
  - Integration `test_duplicate_messages_collapse`: produce the same 5 records twice. `count() … FINAL` returns 5.
- [ ] **Step 2: Run.** Expect FAIL. **Step 3: Implement.** **Step 4: Run.** Expect PASS.
- [ ] **Step 5: Commit.** `feat: clickhouse ingests the streaming topics`

---

### Task 8: Flink in the engine × format matrix

**Files:**
- Modify: `src/de_playground/matrix/probes.py` (`FlinkAdapter`, `ENGINES` adds `"flink"`), `tests/integration/test_matrix_smoke.py`
- Regenerate: `docs/engine-format-matrix.md`

**Interfaces:**
- Consumes: `FlinkSql` and `iceberg_catalog_ddl`/`delta_options` (Task 4); the matrix adapter protocol (`unsupported`, `create`, `count`, `append`, `overwrite`, `merge`, `read`, `use_spark_table`).
- Produces: `FlinkAdapter` (batch mode via SQL Gateway).
  - DuckLake is `n/a` ("no Flink connector").
  - **Iceberg:**
    - `create`: `CREATE TABLE lakehouse.matrix.flink_t`;
    - `append`: `INSERT INTO`;
    - `overwrite`: `INSERT OVERWRITE`;
    - `merge`: attempts `MERGE INTO`, expected to fail since Flink SQL has none;
    - `read`: `SELECT count(*) FROM lakehouse.matrix.seed`;
    - `count`: count of own table.
  - **Delta:** `create` and `append` through a temporary table with `delta_options('s3a://delta/matrix/flink_t')` (the connector creates the table on first write). `overwrite`, `merge` and `read` are attempted and recorded as they fall. The connector is sink-only, so `read` is expected to fail. `count` uses `deltalake` from the runner.
  - The smoke test count becomes 5 engines × 3 formats × 5 ops = 75. `flink/iceberg` create, append and read must be `ok`.
- [ ] **Step 1: Update the failing smoke test.** Expect 75 results and `flink/iceberg/{create,append,read}` ok.
- [ ] **Step 2: Run.** Expect FAIL. **Step 3: Implement.** **Step 4: Run** the smoke test and `make matrix`. Expect PASS. Commit the regenerated doc.
- [ ] **Step 5: Commit.** `feat: flink in the engine x format matrix`

---

### Task 9: Documentation and diagrams

**Files:**
- Modify: `README.md` (streaming quick start, reference sample, lessons), `docs/architecture.md` (a streaming stages diagram in the same style as the batch stages diagram: source → producer → Kafka → two lanes, Flink and Spark, each → bronze/gold in Iceberg and Delta and its own agg topic, plus Kafka → ClickHouse; the component diagram gains the streaming profiles), `docker/flink/README.md`, `docker/kafka/README.md`
- Test: Mermaid parse check (render each block with the Mermaid parser, as in the phase 1–2 docs work)

- [ ] **Step 1:** Write the docs. The README quick start is `make up-stream` → `make stream-setup` → `make stream-job` (Flink) and/or `make stream-spark` (Spark), then where to look:
  - Kafka UI on `:8080`;
  - Flink UI on `:8081`, Spark UI on `:4040` (Structured Streaming tab);
  - ClickHouse `SELECT engine, wiki, sum(edits) FROM wikimedia.edits_per_minute FINAL GROUP BY ALL`;
  - Lakekeeper `flink_stream_gold` and `spark_stream_gold`.

  Also document the main Flink-vs-Spark differences the build surfaced: per-partition vs global watermarks, checkpoints vs micro-batch commits, job submission vs a long-lived client session.
- [ ] **Step 2:** Validate that every Mermaid block parses and render the new stages diagram to check the layout. Expect all blocks to parse.
- [ ] **Step 3:** Run the full verification from nothing:

  ```
  make nuke && make up-stream && make bootstrap && make stream-setup && make stream-job && make stream-spark
  ```

  Then run `make test && make test-integration`. Expect everything green. The batch integration tests need `make up-batch` and gold present, so run `make up-batch && make batch` first. Profiles can run together, at about 26 GB peak; if memory is tight, run streaming and batch suites separately and record that in the README.
- [ ] **Step 4: Commit.** `docs: streaming track docs and diagrams`

---

## Out of scope for this plan

- Orchestrators (phase 4). They will submit and check `stream-job` and run table maintenance (Iceberg snapshot expiry, Delta optimize/vacuum).
- Debezium/CDC (phase 5).
- Top-edited-pages aggregation (the spec's "e.g." list). Per-minute counts by wiki cover the windowing lessons. Add it later if wanted.
- Spark Structured Streaming into Kafka-based CDC or `foreachBatch` MERGE patterns. Phase 5 (Debezium) is the natural place for upserting streams.
