# de-playground

A local playground for trying out modern data engineering tools on realistic data. Everything
runs in Docker Compose on one laptop; services are grouped into profiles so each exercise starts
only what it needs. Design: `docs/superpowers/specs/2026-11-11-de-playground-design.md`.

## What's here so far (phases 1–3)

| Piece | What it does | Docs |
|---|---|---|
| SeaweedFS | S3-compatible object store for every table format | `docker/seaweedfs/README.md` |
| Postgres 18 | One instance, a database per tool (DuckLake catalog, Lakekeeper, …) | `docker/postgres/README.md` |
| Lakekeeper | Iceberg REST catalog | `docker/lakekeeper/README.md` |
| Spark 4 (Spark Connect) | Writes Iceberg and Delta | `docker/spark/README.md` |
| ClickHouse | Reads the gold tables | `docker/clickhouse/README.md` |
| DuckDB | Embedded; writes DuckLake | `src/de_playground/batch/engines.py` |
| Kafka 4.3 (KRaft) + Kafka UI | The streaming backbone | `docker/kafka/README.md` |
| producer | Live Wikimedia edits (or synthetic) into Kafka | `docker/kafka/README.md` |
| Flink 2.0 (PyFlink) | Streaming job writing Iceberg, Delta and Kafka | `docker/flink/README.md` |
| Spark Structured Streaming | The same streaming work as Flink, for comparison | `docker/spark/README.md` |

**The batch pipeline** takes NYC TLC yellow-taxi trips through bronze → silver → gold,
**incrementally, one month at a time**, in three variants that share the same SQL:

| Target | Engine | Table format | Catalog |
|---|---|---|---|
| `duckdb:ducklake` | DuckDB | DuckLake | Postgres (`ducklake` db) |
| `spark:iceberg` | Spark | Iceberg | Lakekeeper |
| `spark:delta` | Spark | Delta Lake | none — tables by path |

- **Bronze** — the month's raw rows, normalised to one schema (TLC columns drift between years).
  Re-running a month replaces it.
- **Silver** — cleaned (out-of-month pickups, impossible times/distances/fares dropped), keyed
  with a derived `trip_id`, de-duplicated, enriched with zone names, then `MERGE`d.
- **Gold** — `daily_zone_stats`, `hourly_borough_demand`, `payment_tip_stats`, `MERGE`d per month.

SQL lives in `src/de_playground/batch/sql/`; table schemas in `batch/schemas.py`.

**The streaming track** takes live Wikimedia edits (every edit to every wiki, ~40/s) through
Kafka into **two streaming engines doing the same work**, Flink and Spark Structured Streaming:

- **Bronze**: every event, in Iceberg *and* Delta (`<engine>_stream_bronze.recentchange`).
- **Gold**: 1-minute windows per wiki (edits, bot/human, new pages, bytes added/removed) in
  Iceberg, Delta and a Kafka topic per engine. Event time, 30 s watermark; late events are dropped
  from their window but kept in bronze.
- **ClickHouse** ingests the raw topic and both engines' aggregates from Kafka, for live queries
  and a side-by-side engine comparison.

## Architecture

```mermaid
flowchart LR
    tlc["NYC TLC trip data<br/>(public CloudFront)"]
    wm["Wikimedia EventStreams<br/>recentchange (live SSE)"]

    subgraph tools["profile: tools"]
        runner["runner (Python 3.12)<br/>pipeline CLI · bootstrap · tests<br/>embedded DuckDB"]
    end

    subgraph streaming_p["profile: streaming"]
        producer["producer<br/>SSE → Kafka<br/>(synthetic fallback)"]
        kafka[("Kafka 4.3 KRaft :9092<br/>+ Kafka UI :8080")]
        flink["Flink 2.0 :8081<br/>JobManager · TaskManager<br/>SQL Gateway :8083"]
    end

    subgraph spark_p["profile: spark"]
        spark["Spark 4 Connect server :15002<br/>Iceberg + Delta connectors"]
    end

    subgraph spark_stream_p["profile: spark-stream"]
        sstream["spark-stream<br/>Structured Streaming client"]
    end

    subgraph core["profile: core"]
        pg[("Postgres 18 :5432<br/>dbs: ducklake · lakekeeper<br/>dbos · prefect · dagster")]
        lk["Lakekeeper :8181<br/>Iceberg REST catalog"]
        s3[("SeaweedFS S3 :8333<br/>buckets: raw · ducklake<br/>iceberg · delta")]
    end

    subgraph ch_p["profile: clickhouse"]
        ch["ClickHouse :8123<br/>iceberg_lake · delta_gold"]
    end

    tlc -- "download (once per month)" --> runner
    runner -- "raw files" --> s3
    runner -- "DuckLake catalog" --> pg
    runner -- "DuckLake data files" --> s3
    runner -- "SQL over gRPC (pyspark-client)" --> spark
    spark -- "Iceberg tables" --> lk
    spark -- "Iceberg + Delta files, raw reads" --> s3
    lk -- "catalog state" --> pg
    lk -. "validates warehouse" .-> s3
    ch -- "DataLakeCatalog" --> lk
    ch -- "reads Iceberg + Delta files" --> s3

    wm -- "SSE" --> producer
    producer -- "raw events" --> kafka
    kafka -- "raw topic" --> flink
    flink -- "aggregates topic" --> kafka
    flink -- "Iceberg tables" --> lk
    flink -- "Iceberg + Delta files" --> s3
    runner -- "SQL (tests, matrix)" --> flink
    sstream -- "starts queries via Connect" --> spark
    kafka -- "raw topic" --> spark
    spark -- "aggregates topic" --> kafka
    kafka -- "Kafka engine: raw + aggregates" --> ch
```

More in [`docs/architecture.md`](docs/architecture.md):
stage-by-layer views of what processes the data at each step, for batch (ingest → bronze →
silver → gold → serve, per engine and format) and for streaming (producer → Kafka → Flink /
Spark → bronze and gold), where each table format keeps its metadata, and the month-by-month flow.

## First run

```bash
cp .env.example .env        # local-only credentials
make up-batch               # core + Spark + ClickHouse (≈19 GB Docker memory at most; ports on 127.0.0.1)
make bootstrap              # buckets, Lakekeeper warehouse, ClickHouse databases (safe to re-run)
# (make batch also refreshes the ClickHouse Delta views once the gold tables exist)
make batch                  # download Jan–Mar 2025, run all three targets
```

Useful variations: `make batch TARGET=spark:delta MONTHS=2025-02` (re-running a month makes
silver and gold for that month match its current input and SQL — rows no longer produced are
deleted), `make batch NS=dev` (write to a separate `dev_*` namespace), `make help`.

Reference row counts for Jan–Mar 2025 (identical in all three formats):

| Month | Bronze | Silver | Gold `daily_zone_stats` |
|---|---:|---:|---:|
| 2025-01 | 3,475,226 | 3,251,811 | 7,216 |
| 2025-02 | 3,577,543 | 3,305,647 | 6,650 |
| 2025-03 | 4,145,257 | 3,826,289 | 7,479 |

On an M5 Pro a month takes ~4s end to end in DuckDB and ~25–30s in Spark.

## Streaming run

```bash
make up-stream              # core + Kafka + producer + Flink + ClickHouse (≈16 GB)
make bootstrap              # if not done yet
make stream-setup           # Kafka topics + ClickHouse's Kafka ingestion tables (safe to re-run)
make stream-job             # submit the PyFlink job (re-running replaces it)
make stream-spark           # + Spark and its Structured Streaming queries (≈24 GB in total)
```

`make stream-status`, `make stream-stop` (Flink, with a savepoint) and `make stream-spark-stop`
manage them. Re-running `make stream-job` resumes from the last savepoint, so restarting never
loses an open window or duplicates bronze (`docker/flink/README.md`). Within a minute or two:

- **Flink UI**: http://localhost:8081, the running job's graph, watermarks and checkpoints.
- **Spark UI**: http://localhost:4040/StreamingQuery/, the five Structured Streaming queries.
- **Kafka UI**: http://localhost:8080, messages and each consumer group's lag.
- **ClickHouse**: http://localhost:8123/play, e.g.
  `SELECT engine, window_start, wiki, edits FROM wikimedia.edits_per_minute FINAL ORDER BY window_start DESC LIMIT 20`.
- **Lakekeeper UI**: `flink_stream_gold` and `spark_stream_gold`.

When this was built, the live feed gave about 125k events in the first 2½ minutes. Flink and Spark
agreed on every shared window (0 differences over 4,692 (window, wiki) rows).

## Where to look afterwards (batch)

- **Lakekeeper UI** — http://localhost:8181/ui → `lakehouse` → `manual_silver.trips` snapshots.
- **SeaweedFS filer** — http://localhost:8888/buckets/ to compare each format's file layout.
- **DuckLake catalog** — `psql postgresql://playground:playground@localhost:5432/ducklake`, `\dt`.
- **Spark UI** — http://localhost:4040, SQL tab, to watch a `MERGE`.
- **ClickHouse** — http://localhost:8123/play, query `delta_gold.daily_zone_stats`.
- **Engine × format matrix** — `docs/engine-format-matrix.md` (`make matrix` regenerates it).
- **DuckDB UI** — `make duckdb-ui`, every format in one place (see below).

## Querying everything: the DuckDB UI

```bash
make duckdb-ui              # http://localhost:4213, in Docker (make duckdb-ui-stop to stop)
make duckdb-ui-local        # or the same UI directly on your Mac (Ctrl+C to stop)
```

DuckDB's web UI gives you notebooks, a schema browser and result charts. Only the data lake needs
to be up (`make up`), not the engines that wrote the data. The UI server only listens on its
container's IPv6 loopback, so the `duckdb-ui` service runs a small `socat` relay that Compose
publishes on `127.0.0.1:4213`. Every table format is under the same naming pattern:

| Catalog | What | Example |
|---|---|---|
| `lake` | DuckLake (catalog in Postgres) | `lake.manual_gold.daily_zone_stats` |
| `iceberg` | every Iceberg table in Lakekeeper | `iceberg.spark_stream_gold.edits_per_minute` |
| `delta` | a view per Delta table found in the `delta` bucket (refreshed each start) | `delta.flink_stream_gold.edits_per_minute` |

Notebooks live in the UI's own state database (`~/.duckdb/extension_data/ui`), which the Docker
service mounts from `.data/duckdb-ui/ui-state` (git-ignored) so they survive restarts. The UI
downloads its web assets from `ui.duckdb.org`; your queries and data stay local. Code:
`src/de_playground/duckdb_ui.py`.

Try comparing the same gold table across all three formats:

```sql
SELECT 'ducklake' AS fmt, count(*) FROM lake.manual_gold.daily_zone_stats
UNION ALL SELECT 'iceberg', count(*) FROM iceberg.manual_gold.daily_zone_stats
UNION ALL SELECT 'delta', count(*) FROM delta.manual_gold.daily_zone_stats;
```

## Saving battery

`make pipelines-down` stops every engine (Kafka, producer, Flink, Spark, ClickHouse) and the
DuckDB UI, and leaves only the data lake (Postgres, SeaweedFS, Lakekeeper: ~1.6 GB, idle CPU)
running. `make duckdb-ui` needs nothing more than that. It saves the Flink job's state (a
savepoint) before stopping Flink, and Spark keeps its state in checkpoints, so to carry on later:
`make up-stream && make stream-job && make stream-spark`. `make down` stops everything, keeping the data; `make nuke` also deletes it.

## Tests

- `make test` — unit tests on the host (pipeline logic runs on a local DuckLake).
- `make test-integration` — against the running stack, inside the `runner` container. The
  batch tests need `make up-batch` (and `make batch` for the ClickHouse gold tests); the streaming
  tests need `make up-stream` + `make stream-spark`. Both stacks fit in Docker together (~26 GB peak).

## Lessons recorded along the way

- DuckLake inlines small inserts into the catalog instead of writing Parquet (`docker/postgres`).
- Don't `DROP TABLE … PURGE` Iceberg tables in Lakekeeper — it races Lakekeeper's own purge (`docker/spark`).
- In-memory DuckDB can't spill unless given a `temp_directory`, and a `MERGE` whose source is a
  window-heavy view can't spill at all — the pipeline stages sources into temp tables first.
- DuckLake's `MERGE` allows only one UPDATE/DELETE action, so on DuckLake the "delete rows the
  month no longer produces" step is a separate `DELETE` in the same transaction; Iceberg and Delta
  do it in one `MERGE … WHEN NOT MATCHED BY SOURCE`.
- ClickHouse can create (path-based) Iceberg tables and append to Iceberg and existing Delta tables,
  but has no `MERGE` and no `TRUNCATE` for lake tables; DuckDB writes Iceberg fully but only reads
  Delta (see the matrix).
- SeaweedFS needs headroom (2 GB + `GOMEMLIMIT`) under parallel Spark I/O or it gets OOM-killed.
- Streaming connector "jar hell" is real. Flink's Delta connector needed four extra or excluded
  jars, a Hadoop `core-site.xml` instead of table options, a per-bucket path-style override, and
  microsecond timestamps to dodge a bug (all in `docker/flink/README.md`).
- Replacing Flink's `config.yaml` silently drops its Java 17 module flags, and every checkpoint
  then fails. Iceberg and Delta only commit on checkpoints, so nothing reached the tables.
- Flink's watermark is the **minimum** across Kafka partitions. A quiet partition holds every
  window open unless the source marks it idle (`scan.watermark.idle-timeout`). Spark's single
  global watermark doesn't have that problem.
- Flink-written Delta tables use the `v2Checkpoint` feature: DuckDB, ClickHouse and Spark read
  them, delta-rs (`deltalake`) can't yet.
- Spark Connect keeps a disconnected session's streaming queries running, so the client stops
  its queries on SIGTERM.
- The Flink image's Python is 3.10, so code that runs inside it is compiled with 3.10 in a test.
