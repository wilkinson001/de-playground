# de-playground

A local playground for trying out modern data engineering tools on realistic data. Everything
runs in Docker Compose on one laptop; services are grouped into profiles so each exercise starts
only what it needs. Design: `docs/superpowers/specs/2026-11-11-de-playground-design.md`.

## What's here so far (phases 1–2)

| Piece | What it does | Docs |
|---|---|---|
| SeaweedFS | S3-compatible object store for every table format | `docker/seaweedfs/README.md` |
| Postgres 18 | One instance, a database per tool (DuckLake catalog, Lakekeeper, …) | `docker/postgres/README.md` |
| Lakekeeper | Iceberg REST catalog | `docker/lakekeeper/README.md` |
| Spark 4 (Spark Connect) | Writes Iceberg and Delta | `docker/spark/README.md` |
| ClickHouse | Reads the gold tables | `docker/clickhouse/README.md` |
| DuckDB | Embedded; writes DuckLake | `src/de_playground/batch/engines.py` |

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

## Architecture

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

More in [`docs/architecture.md`](docs/architecture.md): a stage-by-layer view of what processes
the data at each step (ingest → bronze → silver → gold → serve, per engine and format), where each
table format keeps its metadata, and the month-by-month flow.

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

## Where to look afterwards

- **Lakekeeper UI** — http://localhost:8181/ui → `lakehouse` → `manual_silver.trips` snapshots.
- **SeaweedFS filer** — http://localhost:8888/buckets/ to compare each format's file layout.
- **DuckLake catalog** — `psql postgresql://playground:playground@localhost:5432/ducklake`, `\dt`.
- **Spark UI** — http://localhost:4040, SQL tab, to watch a `MERGE`.
- **ClickHouse** — http://localhost:8123/play, query `delta_gold.daily_zone_stats`.
- **Engine × format matrix** — `docs/engine-format-matrix.md` (`make matrix` regenerates it).

## Tests

- `make test` — unit tests on the host (pipeline logic runs on a local DuckLake).
- `make test-integration` — against the running stack, inside the `runner` container.

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
