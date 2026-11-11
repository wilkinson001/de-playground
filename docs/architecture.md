# Architecture

What runs where in the playground (phases 1–2), how a month of taxi data moves through it, and
what does the work at each stage.
Diagrams are [Mermaid](https://mermaid.js.org) and render on GitHub; edit them as text.

## Components

<!-- This diagram is also in README.md: keep the two copies in sync. -->

Everything runs in Docker Compose on one machine. Boxes are grouped by Compose profile
(`make up` starts `core`; `make up-batch` adds `spark` and `clickhouse`; `tools` is the runner
used by `make` targets). Every published port is bound to `127.0.0.1`.

```mermaid
flowchart LR
    tlc["NYC TLC trip data<br/>(public CloudFront)"]

    subgraph tools["profile: tools"]
        runner["runner (Python 3.12)<br/>pipeline CLI · bootstrap · tests<br/>embedded DuckDB"]
    end

    subgraph spark_p["profile: spark"]
        spark["Spark 4 Connect server :15002<br/>Iceberg + Delta connectors"]
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
```

| Component | Role | Where it's described |
|---|---|---|
| runner | Runs the pipeline (`make batch`), bootstrap and tests on the Compose network. DuckDB is embedded here; Spark work is sent to the Connect server. | `docker/runner/Dockerfile`, `src/de_playground/` |
| Postgres 18 | One instance, one database per tool. Holds the DuckLake catalog and Lakekeeper's state; `dbos`/`prefect`/`dagster` are reserved for phase 4. | `docker/postgres/README.md` |
| SeaweedFS | The only object store. Every format's data files live here. | `docker/seaweedfs/README.md` |
| Lakekeeper | Iceberg REST catalog (warehouse `lakehouse` → bucket `iceberg`). No credential vending: clients bring their own S3 keys. | `docker/lakekeeper/README.md` |
| Spark Connect | Spark 4.0.1 in local mode with Iceberg 1.10 and Delta 4.0; clients only need a thin gRPC client. | `docker/spark/README.md` |
| ClickHouse | Reads the gold tables in place: Iceberg through the Lakekeeper catalog, Delta through `deltaLake()` views. | `docker/clickhouse/README.md` |

## Processing stages and data layers

Columns are the stages a month of data goes through; rows are the three targets `make batch`
runs. Cylinders are **data at rest** (which layer, which table, where it's stored); rounded boxes
are **processing**, coloured by what does the work: blue = Python in the runner, amber = DuckDB,
orange = Spark, green = ClickHouse, purple = orchestration. Dotted lines are control (what
triggers each step), solid lines are data.

```mermaid
flowchart LR
    orch{{"Orchestration<br/>make batch → run_month / run_layer<br/>(phase 4: Dagster · Prefect · DBOS)"}}

    subgraph s_src["Source"]
        tlc[("NYC TLC<br/>monthly Parquet<br/>+ zone lookup CSV")]
    end

    subgraph s_ingest["Ingest"]
        dl("download<br/>Python in runner<br/>httpx → boto3")
        raw[("Raw layer<br/>s3://raw/tlc/…<br/>files as published")]
    end

    subgraph lane_dl["duckdb:ducklake — DuckDB embedded in the runner"]
        direction LR
        b_dl("bronze.sql<br/>DuckDB")
        bz_dl[("Bronze<br/>lake.manual_bronze.*<br/>catalog: Postgres · data: s3://ducklake")]
        s_dl("silver.sql + MERGE<br/>DuckDB")
        sv_dl[("Silver<br/>lake.manual_silver.trips")]
        g_dl("gold.sql + MERGE<br/>DuckDB")
        gd_dl[("Gold<br/>lake.manual_gold.*")]
        b_dl --> bz_dl --> s_dl --> sv_dl --> g_dl --> gd_dl
    end

    subgraph lane_ice["spark:iceberg — Spark Connect server"]
        direction LR
        b_ice("bronze.sql<br/>Spark")
        bz_ice[("Bronze<br/>iceberg.manual_bronze.*<br/>catalog: Lakekeeper · data: s3://iceberg")]
        s_ice("silver.sql + MERGE<br/>Spark")
        sv_ice[("Silver<br/>iceberg.manual_silver.trips")]
        g_ice("gold.sql + MERGE<br/>Spark")
        gd_ice[("Gold<br/>iceberg.manual_gold.*")]
        b_ice --> bz_ice --> s_ice --> sv_ice --> g_ice --> gd_ice
    end

    subgraph lane_delta["spark:delta — Spark Connect server"]
        direction LR
        b_de("bronze.sql<br/>Spark")
        bz_de[("Bronze<br/>s3a://delta/manual_bronze/…<br/>no catalog: _delta_log")]
        s_de("silver.sql + MERGE<br/>Spark")
        sv_de[("Silver<br/>s3a://delta/manual_silver/trips")]
        g_de("gold.sql + MERGE<br/>Spark")
        gd_de[("Gold<br/>s3a://delta/manual_gold/…")]
        b_de --> bz_de --> s_de --> sv_de --> g_de --> gd_de
    end

    subgraph s_serve["Serve"]
        q_duck("DuckDB<br/>ad-hoc SQL")
        q_ch("ClickHouse<br/>iceberg_lake · delta_gold")
    end

    tlc --> dl --> raw
    raw --> b_dl
    raw --> b_ice
    raw --> b_de
    gd_dl --> q_duck
    gd_ice --> q_ch
    gd_de --> q_ch

    orch -.-> dl
    orch -.-> lane_dl
    orch -.-> lane_ice
    orch -.-> lane_delta

    classDef data fill:#eef2f7,stroke:#64748b,color:#0f172a
    classDef python fill:#dbeafe,stroke:#2563eb,color:#0f172a
    classDef duckdb fill:#fef3c7,stroke:#d97706,color:#0f172a
    classDef spark fill:#fde2d4,stroke:#ea580c,color:#0f172a
    classDef clickhouse fill:#dcfce7,stroke:#16a34a,color:#0f172a
    classDef control fill:#ede9fe,stroke:#7c3aed,color:#0f172a
    class tlc,raw,bz_dl,sv_dl,gd_dl,bz_ice,sv_ice,gd_ice,bz_de,sv_de,gd_de data
    class dl python
    class b_dl,s_dl,g_dl,q_duck duckdb
    class b_ice,s_ice,g_ice,b_de,s_de,g_de spark
    class q_ch clickhouse
    class orch control
```

| Stage | What does the work | Code | Reads | Writes | Re-running the month |
|---|---|---|---|---|---|
| **Ingest** | Python in the runner (`httpx` → `boto3`) | `sources/tlc.py` | TLC CloudFront | `s3://raw/tlc/…` | Skipped if the file is already there |
| **Bronze** | DuckDB (DuckLake) or Spark (Iceberg, Delta) | `batch/sql/bronze.sql`, `batch/columns.py` | the month's raw file + zone CSV | `<ns>_bronze.yellow_trips`, `taxi_zones` | Replaces the month (DELETE + INSERT) |
| **Silver** | the same engine | `batch/sql/silver.sql`, `batch/schemas.py` | the month's bronze rows | `<ns>_silver.trips` | MERGE: upsert + delete rows no longer produced |
| **Gold** | the same engine | `batch/sql/gold.sql` | the month's silver rows | `<ns>_gold.daily_zone_stats`, `hourly_borough_demand`, `payment_tip_stats` | MERGE, same as silver |
| **Serve** | ClickHouse for Iceberg/Delta; DuckDB for DuckLake | `clickhouse_setup.py` | gold tables in place | nothing (reads only) | — |

`run_layer` in `batch/pipeline.py` drives each layer: it renders the layer's SQL for the engine,
runs it, then MERGEs the result. Phase 4's orchestrators will call the same function, so this
picture stays the same; only the box at the top changes.

## Where each table format keeps its metadata

The three formats split "catalog" and "data" differently — the main thing the playground
lets you compare.

```mermaid
flowchart TB
    subgraph ducklake["DuckLake"]
        dl_meta[("Postgres db ducklake<br/>all metadata as SQL tables<br/>(and small inlined inserts)")]
        dl_data[("s3://ducklake/…<br/>Parquet files")]
        dl_meta --> dl_data
    end

    subgraph iceberg["Iceberg"]
        ice_cat[("Postgres db lakekeeper<br/>pointer to current metadata.json")]
        ice_meta[("s3://iceberg/…/metadata<br/>metadata.json · manifests")]
        ice_data[("s3://iceberg/…/data<br/>Parquet files")]
        ice_cat --> ice_meta --> ice_data
    end

    subgraph delta["Delta Lake"]
        d_log[("s3://delta/…/_delta_log<br/>JSON commits + checkpoints")]
        d_data[("s3://delta/…<br/>Parquet files")]
        d_log --> d_data
    end
```

DuckLake keeps everything except data files in a database; Iceberg keeps metadata as files and
uses the catalog only to find the current version; Delta needs no catalog at all here (tables are
addressed by path).

## Batch data flow (one month)

`make batch` runs this for each month, in order, for each target. The SQL is the same for every
target (`src/de_playground/batch/sql/`); only the engine and table format differ.

```mermaid
flowchart LR
    raw["raw bucket<br/>yellow_tripdata_YYYY-MM.parquet<br/>taxi_zone_lookup.csv"]

    subgraph targets["one target per run"]
        direction TB
        t1["duckdb:ducklake"]
        t2["spark:iceberg"]
        t3["spark:delta"]
    end

    bronze["Bronze<br/>yellow_trips · taxi_zones<br/>normalised schema, all rows kept"]
    silver["Silver<br/>trips<br/>cleaned · trip_id · deduped · zone names"]
    gold["Gold<br/>daily_zone_stats<br/>hourly_borough_demand<br/>payment_tip_stats"]
    ch["ClickHouse<br/>(Iceberg + Delta gold)"]

    raw --> targets
    targets -- "replace the month<br/>(DELETE + INSERT)" --> bronze
    bronze -- "clean · dedup · MERGE<br/>(upsert + delete stale rows)" --> silver
    silver -- "aggregate · MERGE<br/>(upsert + delete stale rows)" --> gold
    gold -. "read in place" .-> ch
```

What each step guarantees:

- **Bronze** replaces the month's rows, so re-running never duplicates. Raw columns are mapped
  onto one schema (TLC files drift between years); missing columns become NULL.
- **Silver** keeps only valid trips whose pickup is inside the month, derives `trip_id`, keeps one
  row per `trip_id` with a deterministic tie-break, joins zone names, then `MERGE`s. Rows the month
  no longer produces are deleted (on DuckLake via a separate `DELETE` in the same transaction,
  since its `MERGE` allows only one update/delete action).
- **Gold** recomputes the month's aggregates from silver and `MERGE`s them the same way.
- Tables are named `{namespace}_{layer}.{table}`; `make batch NS=dev` writes a separate copy.
  Phase 4 orchestrators will each use their own namespace (`dagster`, `prefect`, `dbos`).

## Coming later

Phase 3 adds a streaming track (Kafka, a Wikimedia producer, PyFlink, ClickHouse ingestion);
phase 4 the orchestrators (Dagster, Prefect, DBOS) wrapping `run_layer`/`run_month`; phase 5
dbt, Debezium and OpenLineage; phase 6 the AI layer (Ollama, LangGraph, NVIDIA Auto Ontology).
See the design doc: `docs/superpowers/specs/2026-11-11-de-playground-design.md`.
