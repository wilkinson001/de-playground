# Data Engineering Playground — Design

Date: 2026-11-11
Status: draft, awaiting approval

## Intent

A personal, local playground for **learning** modern data engineering tools by
using them on realistic data. The owner wants to trial tools hands-on, see how
they interact, and compare alternatives (orchestrators, table formats, engines)
doing the same job.

It is a learning environment, not a product. Success = the owner can bring up a
slice of the stack with one command, run an example pipeline end to end, poke at
every tool through its UI / CLI / Python API, and understand what each one is
doing.

## Goals

1. Two example workloads that exercise the stack:
   - **Batch:** a public dataset processed through bronze → silver → gold.
   - **Streaming:** a public live feed processed continuously.
2. Write to and read from **three table formats** — DuckLake, Apache Iceberg,
   Delta Lake — on the same object store, to learn how each is written,
   catalogued and queried, and which engines can do what with each.
3. Run the **same pipelines under three orchestrators** — Dagster, Prefect,
   DBOS — each written idiomatically for that orchestrator, sharing one set of
   transformation code.
4. An AI layer (built last): a local LLM, a LangGraph agent that answers
   questions over the gold data, and NVIDIA Auto Ontology as a semantic layer
   that grounds the agent.
5. Everything is runnable on one laptop within the Docker resource limit, with
   heavy pieces started only when needed.

## Non-goals

- Production hardening: HA, real auth, TLS, backups, secrets managers.
- Cloud deployment or Kubernetes.
- Performance benchmarking at scale (small-to-medium data only).
- Running every service at once as the normal mode of use.

## Anti-goals (would make this a failure even if it "works")

- Tools wired together in non-idiomatic ways that teach the wrong pattern (e.g.
  forcing Spark or Flink through DuckLake via hacks, or orchestrator code that
  ignores each orchestrator's own model).
- Opaque magic: setup that works but the owner can't see or understand why.
  Each component needs a short README explaining what it is, how it's wired,
  and things to try.
- Fragile bring-up: if starting a profile regularly needs manual fixes, the
  playground won't get used.

## Constraints

- **Host:** MacBook, Apple M5 Pro (arm64), 64GB RAM / 18 cores. **Docker is
  limited to 32GB RAM / 9 cores.** All images must run on arm64 (native
  preferred; emulation only as a last resort, called out where used).
- **Runtime:** Docker Compose. Services grouped into **Compose profiles** so a
  given exercise starts only what it needs.
- **Secrets:** a single `.env` at the repo root (git-ignored; already set up).
  Currently holds `NVIDIA_API_KEY`. A committed `.env.example` documents all
  variables. No Anthropic API key — the owner uses a Claude Max plan, which
  does not provide API access.
- **LLM:** local model via **Ollama in Docker**. The agent's model must be
  switchable by configuration so Claude (via API) could be added later.
- **Data:** well-known public datasets only. Public streaming APIs are fine.

## Decisions made by the owner

- Docker Compose on the local Mac, with profiles; it's fine to run pieces
  separately.
- Batch medallion (bronze/silver/gold) track + streaming track.
- Batch pipeline is **incremental, month by month**, with merge/upsert into
  silver and gold.
- Flink writes **both Iceberg and Delta** in the streaming track.
- **Spark Structured Streaming** runs the same streaming operations as Flink
  (read the raw topic; bronze to Iceberg + Delta; per-minute aggregates to
  Iceberg + Delta + Kafka), so the two streaming engines can be compared like
  the batch engines are.
- Public datasets; public streaming APIs acceptable.
- Table formats: **DuckLake, Iceberg and Delta** all included, specifically to
  learn writing to and interacting with each.
- Orchestrators: **Prefect, Dagster and DBOS all run the same pipelines**, each
  in that orchestrator's idiomatic style.
- LLM: **local model** (Ollama). Not Claude for now.
- NVIDIA Auto Ontology: use it; owner has an NVIDIA API key.
- **Build order:** AI pieces (Ollama, LangGraph, Auto Ontology) are the **last**
  phase.
- **dbt, Debezium (or another CDC tool) and OpenLineage are definitely wanted,
  added after the core stack** is working.

## Component list

Owner-requested: SeaweedFS, PostgreSQL, DuckLake (Postgres catalog), DBOS
(Postgres), Prefect, Dagster, LangChain/LangGraph, NVIDIA Auto Ontology,
ClickHouse, PySpark, PyFlink, Kafka, DuckDB.

Added by agreement: Apache Iceberg, Delta Lake, Ollama; later dbt, Debezium,
OpenLineage (+ Marquez UI).

Added as *my call* (cheap, supporting): Iceberg REST catalog (Lakekeeper,
Postgres-backed), a Kafka web UI (e.g. Kafka UI / Redpanda Console).

Explicitly **not** in scope for now (offered, not chosen; candidates for later):
JupyterLab / notebooks (owner: this isn't for interactive analytics), Airflow,
Trino, data-quality tooling (Soda / Great Expectations), BI
(Metabase / Superset), Prometheus/Grafana, Schema Registry (likely arrives with
Debezium).

## Architecture

### Storage and catalogs

- **SeaweedFS** with its S3 gateway is the single object store. Buckets
  (*my call*): `raw` (landing files), `ducklake`, `iceberg`, `delta`.
- **PostgreSQL 18** (owner's decision), one instance with pgvector (*my call*,
  so Auto Ontology can share it later), hosts separate databases for: the
  DuckLake catalog, the Lakekeeper/Iceberg catalog, DBOS system tables,
  Prefect, Dagster, and (later) Auto Ontology. Rule: use 18 unless a tool
  doesn't support it. Checked so far: Debezium supports 18 (3.x; use the
  latest 3.4.x). Not yet confirmed: Lakekeeper and Auto Ontology (whose own
  stack ships Postgres 17) — the Phase 1 smoke check must exercise Lakekeeper
  against 18; if Auto Ontology won't run on 18, it keeps its own bundled
  Postgres 17 rather than downgrading the shared instance.
- **Table formats:**
  - **DuckLake** — catalog in Postgres, data in `ducklake` bucket. Home engine:
    DuckDB.
  - **Iceberg** — REST catalog (Lakekeeper, *my call*), data in `iceberg`
    bucket. Written by Spark and Flink; readable by DuckDB and ClickHouse.
  - **Delta** — path-based tables in `delta` bucket (*my call*; Unity Catalog
    OSS is a possible later addition). Written by Spark (delta-spark), Python
    (delta-rs) and, where the connector allows, Flink. Readable by DuckDB and
    ClickHouse.
- Research note: DuckLake has no Flink connector and only a limited,
  write-only, append-only, Spark-3-only Spark connector (MotherDuck's). This is
  why Iceberg and Delta exist here; do **not** force Spark/Flink into DuckLake.

### Engines

- **DuckDB** — embedded in Python; primary engine for the DuckLake path.
- **PySpark** — Spark in a container. *My call:* expose it via **Spark Connect**
  so orchestrator containers and pipeline scripts only need a thin PySpark
  client.
  Builder to confirm Iceberg + Delta versions compatible with the chosen Spark
  version, and fall back to Spark 3.5 if Spark 4 compatibility is not there.
- **PyFlink** — Flink JobManager + TaskManager in a custom image with Python and
  the Kafka / Iceberg / Delta connector jars baked in.
- **Kafka** — single broker, KRaft mode, plus a web UI.
- **ClickHouse** — real-time analytics store for streaming results (via its
  Kafka table engine into MergeTree tables) and a reader of gold
  Iceberg/Delta tables via its table functions/engines.

### Batch track: NYC TLC taxi trips

- **Dataset:** NYC TLC Yellow Taxi trip records (Parquet) for ~3 recent months
  (*my call*; a few million rows each) plus the taxi zone lookup CSV.
- **Raw:** files downloaded once into `raw` bucket.
- **Bronze:** raw records loaded as-is into a table, with ingestion metadata
  (source file, load timestamp).
- **Silver:** typed, cleaned (drop impossible fares/distances/timestamps),
  de-duplicated, joined to zone names.
- **Gold:** analytics tables, e.g. daily trips & revenue per zone, hourly
  demand by borough, airport trip stats, tip behaviour by payment type
  (*my call* — builder may adjust).
- **Incremental, month by month (owner's decision):** each month is a
  partition that "arrives" and is processed on its own. Bronze appends the
  month's records; silver and gold are updated with **merge/upsert** (so
  de-duplication and aggregates stay correct across loads, and re-running a
  month is idempotent). This drives Dagster monthly partitions + backfills,
  Prefect parameterised per-month runs, and DBOS workflows that resume at the
  month/step where they stopped. Merge is a first-class part of the pipelines,
  not only of the engine/format matrix.
- **Engine × format variants** (the same logical pipeline):
  - DuckDB → DuckLake
  - PySpark → Iceberg
  - PySpark → Delta
  The shared transform code expresses each layer's logic once per engine
  (SQL for DuckDB; DataFrame/SQL for Spark), parameterised by target format
  where the engine supports more than one.
- **Engine/format matrix:** a doc (and ideally a runnable script) recording,
  for each engine × format, whether read / write / append / overwrite / merge
  works, verified against the running stack. This is an explicit learning
  output.

### Streaming track: Wikimedia recent changes

- **Source:** Wikimedia EventStreams `recentchange` (public SSE feed).
  A small Python producer pushes events into Kafka. A synthetic generator is
  the fallback if the feed is unavailable.
- **Processing:** PyFlink job(s) doing windowed aggregations (e.g. edits per
  wiki per minute, bot vs human ratio, top edited pages), writing results to a
  Kafka output topic, an Iceberg table **and a Delta table** (owner's decision,
  2026-11-11: Flink must write Delta too) (*my call* on specific aggregations).
- Research note (2026-11-11): Flink 2.0.x is the common ground — Iceberg
  `iceberg-flink-runtime-2.0` (1.10.x, same Iceberg line as Spark) and the
  Kernel-based `delta-flink_2.0` (4.4.x, experimental, **sink-only**: Flink can
  write Delta but not read it; exposes a Flink SQL `'connector' = 'delta'`).
- **Serving:** ClickHouse consumes the raw and/or aggregated topics into
  MergeTree tables for fast queries.

### Orchestration

- One shared Python package holds the transformation logic; **orchestrator
  projects are thin, idiomatic wrappers** around it:
  - **Dagster:** software-defined assets per layer (bronze/silver/gold) per
    variant, with resources for DuckDB/Spark/storage, schedules and sensors.
  - **Prefect:** flows and tasks, deployments, a worker.
  - **DBOS:** durable workflows and steps backed by Postgres, showing exactly-
    once / resumable execution (e.g. kill mid-run and resume).
- All three run the full batch pipeline (all engine × format variants).
- **Each orchestrator writes to its own namespace** (*my call*): e.g. a
  schema/namespace per orchestrator (`dagster_*`, `prefect_*`, `dbos_*`) in
  each catalog, so outputs can be compared side by side and runs never
  collide. The shared package takes the target namespace as a parameter.
- Streaming runs continuously, so for the streaming track the orchestrators run
  the **surrounding jobs** (*my call*): submit/check the Flink job, table
  maintenance (Iceberg snapshot expiry / compaction, Delta optimize/vacuum),
  and periodic ClickHouse roll-ups.

### AI layer (last phase)

- **Ollama** container serving a local model (builder picks a model that fits
  the memory budget and does decent text-to-SQL).
- **LangGraph agent** that answers natural-language questions over gold data in
  two modes, for comparison:
  1. raw text-to-SQL against gold tables (DuckDB / ClickHouse);
  2. grounded via **NVIDIA Auto Ontology**'s MCP server.
  Model provider is configuration-switchable (Ollama now, Claude later).
- **NVIDIA Auto Ontology**: its own stack (Next.js UI, FastAPI backend,
  ingestion service, MCP server, Postgres+pgvector). Uses NVIDIA hosted NIM
  models via `NVIDIA_API_KEY`. Connects to Postgres / DuckDB / ClickHouse gold
  data to build the semantic layer. Whether it runs from its own upstream
  compose file or is merged into ours is left to the builder.
  It is public on GitHub (`NVIDIA/auto-ontology`, Apache 2.0) with a Docker
  Compose stack. **Unverified:** arm64 image availability — check at the start
  of Phase 6; build images locally from source if only amd64 is published.

## Phases

1. **Foundation:** repo layout, Compose skeleton with profiles, SeaweedFS,
   Postgres, Lakekeeper, `.env.example`, a `Makefile` (or `just`) for
   bring-up/teardown/smoke checks.
2. **Batch:** dataset download, DuckDB→DuckLake pipeline, Spark (Iceberg +
   Delta) pipelines, ClickHouse reading gold, engine/format matrix.
3. **Streaming:** Kafka + UI, Wikimedia producer, PyFlink job, ClickHouse
   ingestion.
4. **Orchestration:** Dagster, Prefect, DBOS wrappers over the batch pipelines
   and streaming-adjacent jobs.
5. **Extensions:** dbt, Debezium (Postgres CDC → Kafka), OpenLineage + Marquez.
   *My calls, to revisit when this phase is planned:* dbt sits **alongside**
   the hand-written transforms as another way to build silver/gold (starting
   with dbt-duckdb → DuckLake), invoked by the orchestrators (e.g. Dagster's
   dbt integration) — Phase 2 SQL is not written as dbt models. Debezium
   captures from a **separate small "app" Postgres database** seeded with
   demo data and a simulator producing inserts/updates/deletes — never from
   the catalog/orchestrator databases.
6. **AI:** Ollama, LangGraph agent, NVIDIA Auto Ontology.

Each phase ends with the owner able to bring it up and run its example end to
end. Phases are planned one or two at a time rather than all up front.

## Resource budget

Docker has 32GB / 9 cores. Profiles should be sized so that any single
exercise (e.g. "batch with Spark", "streaming with Flink", "orchestration with
Dagster") fits comfortably, with explicit memory limits on heavy services
(Spark, Flink, ClickHouse, Kafka, Ollama). Running everything simultaneously is
not required.

## Left to the builder

- Exact image versions (pin them; arm64; ≥2 weeks old), and verifying
  Spark/Iceberg/Delta/Flink connector compatibility.
- Repo/package layout and Python tooling (*suggest* `uv` workspace).
- Exact profile names and groupings, ports, bucket/database names.
- Specific gold tables and streaming aggregations.
- How smoke checks are expressed (Makefile targets, scripts, pytest).
- Ollama model choice.
