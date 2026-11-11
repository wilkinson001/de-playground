# Foundation + Batch (Phases 1–2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Docker Compose playground with SeaweedFS, Postgres 18, Lakekeeper (Iceberg REST), Spark Connect and ClickHouse, plus an incremental month-by-month NYC taxi bronze → silver → gold pipeline that runs as DuckDB→DuckLake, Spark→Iceberg and Spark→Delta, and a generated engine × format capability matrix.

**Architecture:** Infra lives in one `compose.yaml` with profiles (`core`, `spark`, `clickhouse`). All Python (pipelines, bootstrap, tests) is one `uv` project, `de_playground`, run on the host for unit tests and inside a `runner` container on the Compose network for anything touching services. Pipeline logic is SQL templates per dialect (`duckdb`, `spark`) driven by one engine-agnostic `run_month()`, parameterised by a `Target` (engine, format, namespace) so the orchestrators in Phase 4 can wrap it.

**Tech Stack:** Docker Compose, SeaweedFS (S3 gateway), Postgres 18 + pgvector, Lakekeeper, DuckDB + DuckLake extension, Apache Spark 4.0 (Spark Connect) + Iceberg 1.10 + Delta 4.0, ClickHouse, Python 3.12, uv, pytest, boto3, httpx, pyspark-client, deltalake, pyiceberg, clickhouse-connect.

**Spec:** `docs/superpowers/specs/2026-11-11-de-playground-design.md`

## Global Constraints

- Host is Apple Silicon (arm64); every image must run natively on arm64. Emulation only as a last resort, called out in that component's README.
- Docker budget 32GB RAM / 9 cores. Every heavy service gets an explicit `mem_limit`: postgres 1g, seaweedfs 1g, lakekeeper 512m, spark-connect 8g, clickhouse 4g, runner 2g.
- Postgres **18** (`pgvector/pgvector:0.8.1-pg18` or later pg18 tag). Use 18 unless a tool fails on it; record any failure in `docker/postgres/README.md`.
- Spark/connector baseline (known-compatible set; bump only together and only after the Task 8 smoke test passes): Spark `4.0.1` (Scala 2.13, Java 17), `iceberg-spark-runtime-4.0_2.13:1.10.0`, `iceberg-aws-bundle:1.10.0`, `delta-spark_2.13:4.0.0`, `hadoop-aws:3.4.1` + matching AWS SDK v2 bundle. Python client `pyspark-client==4.0.1`.
- All other image versions pinned to an exact tag at least 2 weeks old. No `latest`.
- Secrets only in repo-root `.env` (git-ignored). `.env.example` committed with every variable and safe local defaults; `NVIDIA_API_KEY` present but unused in these phases.
- Table naming: schema/namespace = `{namespace}_{layer}` (e.g. `manual_bronze`); Delta path = `s3a://delta/{namespace}_{layer}/{table}`. CLI runs use namespace `manual`; Phase 4 will use `dagster`, `prefect`, `dbos`.
- Buckets: `raw`, `ducklake`, `iceberg`, `delta`.
- Postgres databases: `ducklake`, `lakekeeper`, `dbos`, `prefect`, `dagster`.
- Spark session time zone `UTC`; DuckDB `SET TimeZone='UTC'`.
- Don't force Spark or Flink into DuckLake.
- Each component directory under `docker/` gets a short `README.md`: what it is, how it's wired, things to try.

## Review Focus

1. **Schema drift between TLC months** (column case such as `Airport_fee`/`airport_fee`, int vs double types, `cbd_congestion_fee` only from 2025). Expected: every month loads into the same bronze schema; missing columns become NULL. Test: `test_bronze_select_list_handles_missing_and_case_variants` (Task 6) and `test_bronze_loads_month_missing_cbd_fee` (Task 7).
2. **Re-running a month** must not duplicate rows in any layer. Test: `test_rerun_month_is_idempotent` (Task 7, DuckDB) and `test_spark_rerun_month_is_idempotent` (Task 9).
3. **Trips whose pickup falls outside the file's month** (TLC files contain stray 2008/2009 and next-month timestamps). Expected: dropped in silver, kept in bronze. Test: `test_silver_drops_out_of_month_pickups` (Task 7).
4. **Same input gives same gold across engines** (timestamp/time zone handling, decimal formatting in `trip_id`). Test: `test_spark_gold_matches_duckdb_gold` (Task 9).
5. **Bring-up from nothing, repeated.** `make nuke && make up && make bootstrap` works every time, and `make bootstrap` run twice is a no-op. Test: `test_bootstrap_is_idempotent` (Task 3).

---

## File Structure

```
compose.yaml                         # all services, profiles core|spark|clickhouse|tools
Makefile                             # up/down/nuke/bootstrap/test/download/batch/matrix
.env.example
pyproject.toml                       # uv project "de-playground", package de_playground
README.md                            # overview + first-run walkthrough
docker/
  postgres/init/01-databases.sql     # CREATE DATABASE ducklake, lakekeeper, dbos, prefect, dagster
  postgres/README.md
  seaweedfs/s3.json                  # S3 identity from env (rendered at start)
  seaweedfs/README.md
  lakekeeper/README.md
  runner/Dockerfile                  # python3.12 + uv + project deps + preinstalled DuckDB extensions
  spark/Dockerfile                   # apache/spark 4.0.1 + baked connector jars
  spark/spark-defaults.conf          # S3A, Iceberg REST catalog "iceberg", Delta extensions
  spark/README.md
  clickhouse/config.d/*.xml          # enable experimental Iceberg catalog DB, memory limits
  clickhouse/README.md
src/de_playground/
  config.py                          # Settings.from_env()
  months.py                          # Month
  storage.py                         # s3 client, bucket helpers
  bootstrap.py                       # buckets + Lakekeeper bootstrap/warehouse (idempotent)
  sources/tlc.py                     # TLC download → raw bucket
  batch/target.py                    # Target, table_ref, schema_name
  batch/columns.py                   # bronze_select_list
  batch/engines.py                   # SqlEngine protocol, DuckDBEngine, SparkEngine
  batch/pipeline.py                  # run_layer, run_month
  batch/sql/{duckdb,spark}/{ddl,bronze,silver,gold}.sql
  batch/cli.py, batch/__main__.py
  matrix/probes.py, matrix/report.py, matrix/__main__.py
tests/
  unit/...                           # host, no services
  integration/...                    # @pytest.mark.integration, run in runner container
  fixtures/make_fixtures.py          # builds tiny TLC-shaped parquet + zones csv
docs/engine-format-matrix.md         # generated by `make matrix`
```

---

### Task 1: Project skeleton, config and months

**Files:**
- Create: `pyproject.toml`, `.env.example`, `Makefile`, `README.md` (stub: purpose + "make up" placeholder), `src/de_playground/__init__.py`, `src/de_playground/config.py`, `src/de_playground/months.py`
- Test: `tests/unit/test_config.py`, `tests/unit/test_months.py`, `tests/conftest.py`

**Interfaces:**
- Produces:
  - `Month` (frozen dataclass `year: int, month: int`): `Month.parse(s: str) -> Month` (accepts `"YYYY-MM"`, raises `ValueError` otherwise), `start -> datetime.date`, `next_start -> datetime.date`, `__str__ -> "YYYY-MM"`, `Month.range(spec: str) -> list[Month]` accepting `"2025-01"` or `"2025-01..2025-03"` (inclusive; `ValueError` if end < start).
  - `Settings` (frozen dataclass) with `Settings.from_env(env: Mapping[str,str] = os.environ)`. Fields and env vars (defaults in parentheses are the in-Compose values):
    `s3_endpoint` `PLAYGROUND_S3_ENDPOINT` (`http://seaweedfs:8333`), `s3_access_key` `S3_ACCESS_KEY`, `s3_secret_key` `S3_SECRET_KEY`, `pg_host` `PLAYGROUND_PG_HOST` (`postgres`), `pg_port` (`5432`), `pg_user` `POSTGRES_USER`, `pg_password` `POSTGRES_PASSWORD`, `lakekeeper_url` `PLAYGROUND_LAKEKEEPER_URL` (`http://lakekeeper:8181`), `iceberg_warehouse` (`lakehouse`), `spark_remote` `PLAYGROUND_SPARK_REMOTE` (`sc://spark-connect:15002`), `clickhouse_host` (`clickhouse`), `clickhouse_port` (`8123`).
    Derived: `ducklake_attach_uri() -> str` returning `ducklake:postgres:dbname=ducklake host={pg_host} port={pg_port} user={pg_user} password={pg_password}`; `s3_host_port() -> str` (endpoint without scheme).
  - pytest marker `integration` registered; default `addopts = -m "not integration"`.

- [ ] **Step 1: Write failing tests**
  - `test_month_parse_and_bounds`: `Month.parse("2025-01").start == date(2025,1,1)`, `.next_start == date(2025,2,1)`; `Month.parse("2025-12").next_start == date(2026,1,1)`; `str(Month.parse("2025-03")) == "2025-03"`.
  - `test_month_parse_rejects_bad_input`: `"2025-13"`, `"2025-1"`, `"25-01"`, `""` all raise `ValueError`.
  - `test_month_range`: `Month.range("2025-01..2025-03")` gives three months in order; `Month.range("2025-02")` gives one; `"2025-03..2025-01"` raises.
  - `test_settings_from_env_defaults_and_overrides`: with only the three required secrets set, the defaults above apply; `ducklake_attach_uri()` contains `dbname=ducklake host=postgres`; missing `S3_ACCESS_KEY` raises `KeyError` naming the var.
- [ ] **Step 2: Run `uv run pytest tests/unit -v`** — expect FAIL (import errors).
- [ ] **Step 3: Implement** `pyproject.toml` (Python `>=3.12,<3.13`; deps listed in Tech Stack; dev deps pytest), `config.py`, `months.py`. `.env.example` holds: `POSTGRES_USER=playground`, `POSTGRES_PASSWORD=playground`, `S3_ACCESS_KEY=playground`, `S3_SECRET_KEY=playground-secret`, `LAKEKEEPER_ENCRYPTION_KEY=change-me-32-chars-minimum-000000`, `NVIDIA_API_KEY=`. Makefile: `.env` check target that fails with a clear message if `.env` is missing, and a `test` target (`uv run pytest`).
- [ ] **Step 4: Run `uv run pytest tests/unit -v`** — expect PASS.
- [ ] **Step 5: Commit** `feat: project skeleton, settings and Month`.

---

### Task 2: Core infra — Postgres 18 and SeaweedFS, plus the runner container

**Files:**
- Create: `compose.yaml`, `docker/postgres/init/01-databases.sql`, `docker/postgres/README.md`, `docker/seaweedfs/s3.json` (or an entrypoint that renders it from env), `docker/seaweedfs/README.md`, `docker/runner/Dockerfile`, `src/de_playground/storage.py`
- Modify: `Makefile` (`up`, `down`, `nuke`, `ps`, `logs`, `shell`, `test-integration`)
- Test: `tests/integration/test_core_services.py`

**Interfaces:**
- Consumes: `Settings` (Task 1).
- Produces:
  - Compose services (profile `core`): `postgres` (port 5432, healthcheck `pg_isready`), `seaweedfs` (`weed server -s3` with S3 on 8333; master 9333/filer 8888 exposed for the UI, plus a healthcheck). Profile `tools`: `runner` (built from `docker/runner/Dockerfile`, repo mounted at `/workspace`, `env_file: .env`, depends on healthy postgres+seaweedfs).
  - `storage.s3_client(settings) -> botocore client` (path-style addressing, region `us-east-1`); `storage.ensure_bucket(client, name) -> bool` (True if created); `storage.object_exists(client, bucket, key) -> bool`.
  - Makefile: `make up` = `docker compose --profile core up -d --wait`; `make nuke` = `down -v`; `make test-integration` = `docker compose --profile tools run --rm runner uv run pytest -m integration`.
- Runner image: `python:3.12-slim-bookworm`, install `uv`, `uv sync --frozen` at build (deps cached in image layer; source is mounted), and at build run DuckDB `INSTALL ducklake; INSTALL postgres; INSTALL httpfs; INSTALL iceberg; INSTALL delta;` so runs need no extension downloads.

- [ ] **Step 1: Write failing integration tests**
  - `test_postgres_18_with_databases`: `SHOW server_version` starts with `18`; databases `ducklake, lakekeeper, dbos, prefect, dagster` all exist.
  - `test_seaweedfs_s3_roundtrip`: `ensure_bucket(c, "smoke")`, put/get an object, `object_exists` True then False after delete; `ensure_bucket` twice → second returns False.
- [ ] **Step 2: Run `make up && make test-integration`** — expect FAIL (missing files / services).
- [ ] **Step 3: Implement** compose services, init SQL, SeaweedFS S3 identity (one admin identity, keys from `S3_ACCESS_KEY`/`S3_SECRET_KEY`, actions `Admin, Read, Write, List, Tagging`), runner Dockerfile, `storage.py`. Named volumes `pgdata`, `seaweeddata`. Postgres image `pgvector/pgvector:0.8.1-pg18`. Note: the PG18 image's data dir moved to `/var/lib/postgresql` (mount the volume there, not `/var/lib/postgresql/data`). READMEs: ports, how to connect (`psql`, SeaweedFS filer UI at http://localhost:8888), things to try.
- [ ] **Step 4: Run `make nuke && make up && make test-integration`** — expect PASS.
- [ ] **Step 5: Commit** `feat: core compose with postgres 18, seaweedfs and runner`.

---

### Task 3: Lakekeeper and idempotent bootstrap

**Files:**
- Create: `src/de_playground/bootstrap.py`, `src/de_playground/__main__.py` (dispatch `bootstrap`), `docker/lakekeeper/README.md`
- Modify: `compose.yaml` (services `lakekeeper-migrate`, `lakekeeper`), `Makefile` (`bootstrap`)
- Test: `tests/integration/test_bootstrap.py`

**Interfaces:**
- Consumes: `Settings`, `storage.ensure_bucket`.
- Produces:
  - Compose (profile `core`): `lakekeeper-migrate` (one-shot `migrate`, depends on healthy postgres) → `lakekeeper` (`serve`, port 8181, depends on `lakekeeper-migrate` completing successfully; healthcheck on `/health`). Env: read/write PG URLs pointing at db `lakekeeper`, `LAKEKEEPER__PG_ENCRYPTION_KEY` from `.env`. Auth disabled (local playground).
  - `bootstrap.run(settings) -> BootstrapReport` (dataclass: `buckets_created: list[str]`, `lakekeeper_bootstrapped: bool`, `warehouse_created: bool`). Steps: ensure the four buckets; `POST /management/v1/bootstrap` (accept-terms) if `GET /management/v1/info` reports not bootstrapped; create warehouse `lakehouse` if absent, with an S3 storage profile: bucket `iceberg`, endpoint `http://seaweedfs:8333`, path-style access, region `us-east-1`, static credentials from settings, **STS off and remote signing off** so clients use their own static S3 credentials (SeaweedFS has no STS). If Lakekeeper rejects that combination, use whatever its docs give for S3-compatible stores without STS, and record it in the README.
  - `make bootstrap` = `docker compose --profile tools run --rm runner uv run python -m de_playground bootstrap`, printing the report.

- [ ] **Step 1: Write failing tests**
  - `test_bootstrap_creates_buckets_and_warehouse`: after `bootstrap.run`, all four buckets exist and `GET {lakekeeper}/catalog/v1/config?warehouse=lakehouse` returns 200.
  - `test_bootstrap_is_idempotent`: a second `bootstrap.run` returns an empty `buckets_created` list, `lakekeeper_bootstrapped=False` and `warehouse_created=False`, and doesn't raise.
  - `test_iceberg_namespace_roundtrip_via_rest`: with `pyiceberg` `load_catalog("lk", type="rest", uri=f"{url}/catalog", warehouse="lakehouse", **s3 props)`, create namespace `smoke`, create a 1-column table, append one row with pyarrow, read it back, then drop both. This proves Lakekeeper on PG18 plus SeaweedFS storage end to end.
- [ ] **Step 2: Run `make up && make test-integration`** — expect FAIL.
- [ ] **Step 3: Implement** the compose services and `bootstrap.py` (httpx, with retries while Lakekeeper is starting). Pin the Lakekeeper image to an exact release tag. README: UI at http://localhost:8181/ui, what the warehouse is, and how storage credentials are configured and why.
- [ ] **Step 4: Run `make nuke && make up && make bootstrap && make bootstrap && make test-integration`** — expect PASS, with the second bootstrap printing nothing created.
- [ ] **Step 5: Commit** `feat: lakekeeper iceberg catalog and idempotent bootstrap`.

---

### Task 4: TLC source download to the raw bucket, and test fixtures

**Files:**
- Create: `src/de_playground/sources/__init__.py`, `src/de_playground/sources/tlc.py`, `tests/fixtures/make_fixtures.py`
- Modify: `src/de_playground/__main__.py` (dispatch `download`), `Makefile` (`download MONTHS=2025-01..2025-03`)
- Test: `tests/unit/test_tlc.py`, `tests/integration/test_tlc_download.py`

**Interfaces:**
- Consumes: `Month`, `Settings`, `storage`.
- Produces:
  - `TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"`; `tlc_trip_url(month) -> f"{TLC_BASE}/trip-data/yellow_tripdata_{month}.parquet"`; `ZONES_URL = f"{TLC_BASE}/misc/taxi_zone_lookup.csv"`.
  - Raw keys: `raw_trips_key(month) -> f"tlc/yellow/yellow_tripdata_{month}.parquet"`; `RAW_ZONES_KEY = "tlc/taxi_zone_lookup.csv"` (both in bucket `raw`).
  - `download_month(month, settings, *, client=None, http=None) -> DownloadResult` (`key: str, skipped: bool, bytes: int`). Skips if the object already exists. Streams the HTTP body through a local temp file into S3 with multipart upload; never holds the whole file in memory. A non-200 (e.g. a month not yet published → 403/404) raises `SourceUnavailable(month, status)`. `download_zones(...)` behaves the same way.
  - `tests/fixtures/make_fixtures.py`: `write_trips_parquet(path, rows: list[dict], *, include_cbd_fee=True, airport_fee_name="Airport_fee")` writes TLC-shaped parquet using the real column names and types (`VendorID` int32, `tpep_pickup_datetime`/`tpep_dropoff_datetime` timestamp[us], `passenger_count` int64, `trip_distance` double, `RatecodeID` int64, `store_and_fwd_flag` string, `PULocationID`/`DOLocationID` int32, `payment_type` int64, and the amount columns as double). Also `write_zones_csv(path)` (columns `LocationID,Borough,Zone,service_zone`; include at least zones 1, 132, 138, 161, 236, 237, 264, 265), and a `sample_rows(month) -> list[dict]` builder used by later tasks.

- [ ] **Step 1: Write failing tests** (unit tests use `httpx.MockTransport` and `moto`'s in-memory S3, or a stub client):
  - `test_urls_and_keys`: exact strings for `Month.parse("2025-01")`.
  - `test_download_skips_existing_object`: an existing key gives `skipped=True` and makes no HTTP request.
  - `test_download_raises_on_missing_month`: a 403 response raises `SourceUnavailable` and leaves no partial object.
  - Integration `test_download_real_month`: `download_month(Month.parse("2025-01"))` then `object_exists` is True and the size is > 10 MB; a second call gives `skipped=True`.
- [ ] **Step 2: Run** `uv run pytest tests/unit/test_tlc.py -v` — expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** unit tests, then `make download MONTHS=2025-01 && make test-integration` — expect PASS.
- [ ] **Step 5: Commit** `feat: TLC month download to raw bucket`.

---

### Task 5: DuckDB / DuckLake connection

**Files:**
- Create: `src/de_playground/batch/__init__.py`, `src/de_playground/batch/engines.py` (DuckDB part)
- Test: `tests/unit/test_duckdb_engine.py`, `tests/integration/test_ducklake_on_seaweedfs.py`

**Interfaces:**
- Consumes: `Settings`.
- Produces:
  - `SqlEngine` Protocol: `dialect: Literal["duckdb","spark"]`; `execute(sql: str) -> None` (runs one or more `;`-separated statements); `query(sql: str) -> list[tuple]`; `parquet_columns(uri: str) -> list[str]` (column names as stored); `register_parquet_view(name: str, uri: str, select_sql: str) -> None` (temp view `name` = `select_sql` evaluated over a relation called `src` that reads the parquet); `register_csv_view(name: str, uri: str) -> None` (header row, inferred types).
  - `DuckDBEngine(con: duckdb.DuckDBPyConnection)` implements `SqlEngine`, with `dialect="duckdb"`.
  - `connect_ducklake(catalog_uri: str, data_path: str, settings: Settings | None = None) -> DuckDBEngine`: loads extensions, `SET TimeZone='UTC'`, creates an S3 secret when `settings` is given (`ENDPOINT` = `settings.s3_host_port()`, `URL_STYLE 'path'`, `USE_SSL false`, region `us-east-1`), then `ATTACH '{catalog_uri}' AS lake (DATA_PATH '{data_path}')` and `USE lake`.
  - `ducklake_engine(settings) -> DuckDBEngine` = `connect_ducklake("ducklake:postgres:…" via settings.ducklake_attach_uri(), "s3://ducklake/", settings)`.
- [ ] **Step 1: Write failing tests**
  - Unit `test_local_ducklake_roundtrip(tmp_path)`: `connect_ducklake(f"ducklake:{tmp_path}/cat.ducklake", f"{tmp_path}/data/")`; create schema and table, insert, query; then reconnect a new engine to the same paths and the row is still there.
  - Unit `test_parquet_columns_and_views(tmp_path)`: with a fixture parquet, `parquet_columns` returns the TLC names (case preserved), and `register_csv_view` exposes the zones columns.
  - Integration `test_ducklake_postgres_catalog_on_s3`: with `ducklake_engine(settings)`, create schema `smoke`, a table, and insert; the parquet files appear under bucket `ducklake`; drop the schema.
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** unit tests and `make test-integration` — expect PASS.
- [ ] **Step 5: Commit** `feat: DuckDB engine with DuckLake on Postgres + SeaweedFS`.

---

### Task 6: Targets, table naming and bronze column normalisation

**Files:**
- Create: `src/de_playground/batch/target.py`, `src/de_playground/batch/columns.py`
- Test: `tests/unit/test_target.py`, `tests/unit/test_columns.py`

**Interfaces:**
- Produces:
  - `Engine = Literal["duckdb","spark"]`, `Format = Literal["ducklake","iceberg","delta"]`, `Layer = Literal["bronze","silver","gold"]`.
  - `Target(engine, format, namespace)` (frozen dataclass). Validation: allowed pairs are `duckdb/ducklake`, `spark/iceberg`, `spark/delta`, anything else raises `ValueError`; namespace must match `^[a-z][a-z0-9_]{0,30}$`.
  - `schema_name(target, layer) -> f"{namespace}_{layer}"`.
  - `table_ref(target, layer, table) -> str`: ducklake → `lake.{schema}.{table}`; iceberg → `iceberg.{schema}.{table}`; delta → ``delta.`s3a://delta/{schema}/{table}` ``.
  - `TABLES = {"bronze": ["yellow_trips","taxi_zones"], "silver": ["trips"], "gold": ["daily_zone_stats","hourly_borough_demand","payment_tip_stats"]}`.
  - `BRONZE_COLUMNS: list[tuple[str, str]]`: canonical lowercase snake_case name and logical type, in this order: `vendor_id int`, `pickup_at timestamp`, `dropoff_at timestamp`, `passenger_count bigint`, `trip_distance double`, `ratecode_id bigint`, `store_and_fwd_flag string`, `pu_location_id int`, `do_location_id int`, `payment_type bigint`, `fare_amount double`, `extra double`, `mta_tax double`, `tip_amount double`, `tolls_amount double`, `improvement_surcharge double`, `total_amount double`, `congestion_surcharge double`, `airport_fee double`, `cbd_congestion_fee double`. Source names map case-insensitively: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `Airport_fee`, `cbd_congestion_fee`.
  - `bronze_select_list(available: list[str], dialect) -> str`: a comma-joined `CAST(src."<actual name>" AS <dialect type>) AS <canonical>` for each canonical column, using the actual source spelling (matched case-insensitively), or `CAST(NULL AS <type>) AS <canonical>` when the column is absent. Identifier quoting is `"…"` for duckdb and `` `…` `` for spark. Type mapping: `string` → `VARCHAR`/`STRING`; `int` → `INTEGER`/`INT`; the rest are identical.
- [ ] **Step 1: Write failing tests**
  - `test_target_validation`: `Target("spark","ducklake","x")` and `Target("duckdb","iceberg","x")` raise; `Target("spark","delta","Bad-Name")` raises.
  - `test_table_ref_per_format`: the exact strings for `Target("duckdb","ducklake","manual")`, `("spark","iceberg","manual")` and `("spark","delta","manual")` with `("silver","trips")`; the delta one is ``delta.`s3a://delta/manual_silver/trips` ``.
  - `test_bronze_select_list_handles_missing_and_case_variants`: given available columns with `airport_fee` (lowercase) and no `cbd_congestion_fee`, the duckdb output contains `CAST(src."airport_fee" AS DOUBLE) AS airport_fee` and `CAST(NULL AS DOUBLE) AS cbd_congestion_fee`; the spark output uses backticks and `STRING` for `store_and_fwd_flag`.
- [ ] **Step 2: Run** — expect FAIL. **Step 3: Implement.** **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** `feat: batch targets, table refs and bronze column mapping`.

---

### Task 7: Medallion pipeline on DuckDB → DuckLake

**Files:**
- Create: `src/de_playground/batch/pipeline.py`, `src/de_playground/batch/sql/duckdb/{ddl,bronze,silver,gold}.sql`
- Test: `tests/unit/test_duckdb_pipeline.py`

**Interfaces:**
- Consumes: `SqlEngine`, `DuckDBEngine`, `connect_ducklake`, `Target`, `table_ref`, `schema_name`, `TABLES`, `bronze_select_list`, `Month`, fixtures.
- Produces:
  - `RawSources(trips_uri: str, zones_uri: str)`; `raw_sources(month, dialect) -> RawSources`: scheme `s3://` for duckdb and `s3a://` for spark, bucket `raw`, keys from Task 4.
  - `ensure_tables(engine, target) -> None`: runs `ddl.sql`, creating the schemas and all six tables if they don't exist. Partitioning: bronze `yellow_trips` by `source_month`, silver `trips` by `pickup_month`. Tables not listed here are unpartitioned.
  - `run_layer(engine, target, layer, month, sources) -> int` (rows written or affected in the main table of that layer); `run_month(engine, target, month, sources, layers=("bronze","silver","gold")) -> dict[Layer,int]`. Templates are loaded with `importlib.resources` and rendered with `str.format` using keys `{bronze_trips} {bronze_zones} {silver_trips} {gold_daily_zone} {gold_hourly_borough} {gold_payment_tip} {month} {month_start} {next_month_start} {select_list}`. Phase 4 orchestrators call these two functions, so their signatures are the contract.
  - Table schemas:
    - bronze `yellow_trips`: the `BRONZE_COLUMNS` plus `source_month VARCHAR`, `source_file VARCHAR` and `ingested_at TIMESTAMP`. bronze `taxi_zones`: `location_id INT, borough VARCHAR, zone VARCHAR, service_zone VARCHAR`.
    - silver `trips`: `trip_id VARCHAR` (key), `pickup_month VARCHAR`, `pickup_at`, `dropoff_at`, `pickup_date DATE`, `pickup_hour INT`, `duration_minutes DOUBLE`, `vendor_id`, `passenger_count`, `trip_distance DECIMAL(8,2)`, `pu_location_id`, `do_location_id`, `pu_borough`, `pu_zone`, `do_borough`, `do_zone`, `payment_type`, `fare_amount DECIMAL(10,2)`, `tip_amount DECIMAL(10,2)`, `total_amount DECIMAL(10,2)`, `airport_fee DECIMAL(10,2)`, `cbd_congestion_fee DECIMAL(10,2)`.
    - gold `daily_zone_stats` (key `pickup_date, pu_location_id`): `pu_borough, pu_zone, trips BIGINT, total_revenue DECIMAL(14,2), avg_trip_distance DOUBLE, avg_tip_pct DOUBLE`.
    - gold `hourly_borough_demand` (key `pickup_date, pickup_hour, pu_borough`): `trips BIGINT`.
    - gold `payment_tip_stats` (key `pickup_month, payment_type`): `payment_type_name VARCHAR, trips BIGINT, avg_tip_amount DOUBLE, avg_tip_pct DOUBLE`.
  - Layer rules (both dialects implement these exactly):
    - **Bronze:** `DELETE` the month's rows (`source_month = '{month}'`), then `INSERT` every row from `src` with the normalised select list and metadata. Zones are `DELETE` all then `INSERT`. Bronze keeps out-of-month and invalid rows.
    - **Silver:** take rows from bronze for `source_month='{month}'`. Keep a row only if all of these hold:
      - `pickup_at >= month_start AND pickup_at < next_month_start`
      - `dropoff_at > pickup_at`
      - duration ≤ 360 minutes
      - `trip_distance > 0 AND trip_distance <= 200`
      - `fare_amount >= 0 AND total_amount >= 0`
      - `passenger_count IS NULL OR passenger_count <= 8`

      `trip_id = md5(concat_ws('|', vendor_id, <pickup epoch micros>, <dropoff epoch micros>, pu_location_id, do_location_id, CAST(trip_distance AS DECIMAL(8,2)), CAST(total_amount AS DECIMAL(10,2))))`, where epoch micros is `epoch_us()` in duckdb and `unix_micros()` in spark, and every element is cast to string. Remove duplicates within the batch by `trip_id`, keeping one row (lowest `ingested_at`, then any). Left-join zones for PU and DO. Then `MERGE INTO` silver on `trip_id`: `WHEN MATCHED THEN UPDATE SET *`-equivalent (explicit column list), `WHEN NOT MATCHED THEN INSERT`.
    - **Gold:** recompute the three aggregates from silver `WHERE pickup_month='{month}'` and `MERGE` each table on its key (update when matched, insert when not).
      - `avg_tip_pct = avg(tip_amount / fare_amount)` over rows with `payment_type = 1 AND fare_amount > 0` (NULL if there are none).
      - `total_revenue = sum(total_amount)`.
      - `payment_type_name` comes from: `0 Flex Fare, 1 Credit card, 2 Cash, 3 No charge, 4 Dispute, 5 Unknown, 6 Voided trip`, else `Other`.
      - `pickup_month` in gold is a `DATE` (first of the month).
- [ ] **Step 1: Write failing tests** (each one uses a local DuckLake in `tmp_path` and fixture files written with `make_fixtures`, with `Target("duckdb","ducklake","t")`):
  - `test_bronze_loads_month_missing_cbd_fee`: a fixture without `cbd_congestion_fee` and with `airport_fee` in lowercase loads, and the bronze row count equals the fixture row count with `cbd_congestion_fee` all NULL.
  - `test_silver_drops_out_of_month_pickups`: a fixture for 2025-01 with rows at `2009-01-01`, `2025-02-01 00:00:00` and `2025-01-31 23:59:59` gives exactly one surviving row.
  - `test_silver_cleaning_rules`: one fixture row breaking each rule (dropoff before pickup, a 7h trip, distance 0, distance 250, negative fare, 9 passengers) plus two valid rows gives 2 silver rows.
  - `test_silver_dedupes_identical_trips`: two identical rows give 1 silver row.
  - `test_gold_values`: a hand-built fixture where zone 132 has 3 card trips on 2025-01-05 with fares 10/20/30, tips 1/2/3 and totals 12/24/36 gives `daily_zone_stats` with `trips=3`, `total_revenue=72.00` and `avg_tip_pct≈0.1`; `hourly_borough_demand` sums to the silver row count; `payment_tip_stats` has `payment_type_name='Credit card'`.
  - `test_rerun_month_is_idempotent`: running `run_month` twice for the same month leaves the row counts in all six tables unchanged, with gold values identical.
  - `test_months_accumulate`: running 2025-01 and then 2025-02 fixtures makes silver hold both months, and the gold rows for January are unchanged after the February run.
- [ ] **Step 2: Run `uv run pytest tests/unit/test_duckdb_pipeline.py -v`** — expect FAIL.
- [ ] **Step 3: Implement** `pipeline.py` and the four duckdb SQL templates.
- [ ] **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** `feat: incremental medallion pipeline on DuckDB/DuckLake`.

---

### Task 8: Spark Connect with Iceberg and Delta

**Files:**
- Create: `docker/spark/Dockerfile`, `docker/spark/spark-defaults.conf`, `docker/spark/README.md`
- Modify: `compose.yaml` (service `spark-connect`, profile `spark`), `src/de_playground/batch/engines.py` (Spark part), `Makefile` (`up-batch` = profiles core+spark+clickhouse)
- Test: `tests/integration/test_spark_formats.py`

**Interfaces:**
- Consumes: `Settings`, `SqlEngine`.
- Produces:
  - Image: `apache/spark:4.0.1-scala2.13-java17-python3-ubuntu` with these jars downloaded at build time into `/opt/spark/jars`: Iceberg runtime, Iceberg aws-bundle, delta-spark, delta-storage, hadoop-aws and the AWS SDK v2 bundle (versions from Global Constraints). Spark Connect is bundled with Spark 4. The command runs `start-connect-server.sh` in the foreground in `local[6]` mode with `spark.driver.memory=6g`, on port 15002, with the Spark UI on 4040.
  - Config: S3A endpoint `http://seaweedfs:8333`, path style, no SSL, keys from env. `spark.sql.extensions` set to both the Delta and Iceberg extensions. `spark_catalog` = `DeltaCatalog`. Catalog `iceberg` = `org.apache.iceberg.spark.SparkCatalog`, `type=rest`, `uri=http://lakekeeper:8181/catalog`, `warehouse=lakehouse`, `io-impl=org.apache.iceberg.aws.s3.S3FileIO`, with `s3.endpoint`, `s3.path-style-access=true` and static keys. `spark.sql.session.timeZone=UTC`. S3 keys come in through env, not baked into the image.
  - `SparkEngine(spark: pyspark.sql.SparkSession)` implements `SqlEngine` with `dialect="spark"`. It calls `.collect()` on `spark.sql` to force execution and splits multi-statement strings on `;` outside quotes. `spark_engine(settings) -> SparkEngine` uses `SparkSession.builder.remote(settings.spark_remote).getOrCreate()`.
- [ ] **Step 1: Write failing tests** (integration):
  - `test_spark_iceberg_roundtrip`: `CREATE NAMESPACE IF NOT EXISTS iceberg.smoke`, then create a table, insert, `MERGE INTO`, select, and drop.
  - `test_spark_delta_path_roundtrip`: run ``CREATE TABLE IF NOT EXISTS delta.`s3a://delta/smoke/t` (id INT, v STRING) USING delta``, then insert, merge and select; afterwards `_delta_log/` exists in bucket `delta`.
  - `test_spark_reads_raw_parquet`: `parquet_columns("s3a://raw/tlc/yellow/yellow_tripdata_2025-01.parquet")` contains `tpep_pickup_datetime` (assumes `make download` has run; skip with a clear message otherwise).
- [ ] **Step 2: Run** `make up-batch && make test-integration` — expect FAIL.
- [ ] **Step 3: Implement.** If Iceberg 1.10 or Delta 4.0 can't load on Spark 4.0.1, drop to the documented Spark 3.5.x set (Iceberg `-3.5_2.12`, Delta `3.3.x`, `pyspark==3.5.x` with `[connect]`), and record why in `docker/spark/README.md`.
- [ ] **Step 4: Run** — expect PASS. Check that `docker stats` shows spark-connect under its 8g limit.
- [ ] **Step 5: Commit** `feat: spark connect with iceberg rest catalog and delta on seaweedfs`.

---

### Task 9: Medallion pipeline on Spark → Iceberg and Delta

**Files:**
- Create: `src/de_playground/batch/sql/spark/{ddl,bronze,silver,gold}.sql`
- Modify: `src/de_playground/batch/pipeline.py` (only if a dialect branch is needed; the templates should carry the differences)
- Test: `tests/integration/test_spark_pipeline.py`

**Interfaces:**
- Consumes: `run_month`, `run_layer`, `ensure_tables`, `SparkEngine`, `ducklake_engine`, fixtures, and the rules from Task 7, which are unchanged.
- Produces: Spark templates that produce the same table schemas and layer rules as Task 7. Partitioning: Iceberg uses `PARTITIONED BY (source_month)` / `(pickup_month)`; Delta uses the same columns in `PARTITIONED BY`. For delta, `CREATE SCHEMA` is a no-op because paths are used.
- Tests upload fixture files to `s3://raw/fixtures/{test-id}/…` and pass a matching `RawSources`, using namespaces `itest_{format}` that are dropped on teardown.
- [ ] **Step 1: Write failing tests**, parametrised over `format in ["iceberg","delta"]`:
  - `test_spark_pipeline_layer_counts`: the fixture month from `test_silver_cleaning_rules` gives the same bronze and silver counts as the DuckDB test.
  - `test_spark_rerun_month_is_idempotent`.
  - `test_spark_gold_matches_duckdb_gold`: run the same fixture month through `ducklake_engine` (namespace `itest_duck`) and Spark. All three gold tables, ordered by key, match exactly on keys and counts, and to within 1e-9 on doubles. Silver `trip_id` sets are identical.
- [ ] **Step 2: Run** — expect FAIL. **Step 3: Implement.** **Step 4: Run** — expect PASS.
- [ ] **Step 5: Commit** `feat: medallion pipeline on spark for iceberg and delta`.

---

### Task 10: Batch CLI and a real three-month run

**Files:**
- Create: `src/de_playground/batch/cli.py`, `src/de_playground/batch/__main__.py`
- Modify: `Makefile`, `README.md` (first-run walkthrough)
- Test: `tests/unit/test_batch_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `python -m de_playground.batch run --target duckdb:ducklake|spark:iceberg|spark:delta|all --months 2025-01..2025-03 [--namespace manual] [--layers bronze,silver,gold]`. It processes months in order and the layers within each month in order, and prints one line per (target, month, layer) with the row count and elapsed time. It exits non-zero on the first failure and says which (target, month, layer) failed. `all` runs the three targets in turn.
  - `make batch TARGET=all MONTHS=2025-01..2025-03` (defaults are those values), which runs `download` for the months first.
  - `README.md` walkthrough: `cp .env.example .env` → `make up-batch` → `make bootstrap` → `make batch` → where to look (the Lakekeeper UI, the SeaweedFS filer, `psql` into the DuckLake catalog, the Spark UI).
- [ ] **Step 1: Write failing unit tests**: argument parsing (`--target all` expands to three `Target`s; a bad `--months` exits 2 with the `ValueError` message); a stub engine factory records the call order (month-major, then layer).
- [ ] **Step 2–4: Implement and pass.**
- [ ] **Step 5: Real run:** `make nuke && make up-batch && make bootstrap && make batch`. Expect the 9 target-month combinations × 3 layers to succeed. Then run `make batch MONTHS=2025-02` again and check that the silver counts are unchanged. Record the real row counts per layer per month in `README.md` as a reference.
- [ ] **Step 6: Commit** `feat: batch CLI and end-to-end three-month run`.

---

### Task 11: ClickHouse reading gold

**Files:**
- Create: `docker/clickhouse/config.d/playground.xml`, `docker/clickhouse/init/01-gold.sql.tmpl` (or Python-driven setup), `docker/clickhouse/README.md`, `src/de_playground/clickhouse_setup.py`
- Modify: `compose.yaml` (service `clickhouse`, profile `clickhouse`, ports 8123 and 9000, `mem_limit` 4g), `src/de_playground/__main__.py` (`clickhouse-setup`, also called by `make bootstrap` when ClickHouse is up)
- Test: `tests/integration/test_clickhouse_gold.py`

**Interfaces:**
- Consumes: gold tables from Task 10 (namespace `manual`).
- Produces:
  - `clickhouse_setup.run(settings, namespace="manual") -> None`, which is idempotent and creates:
    - Database `iceberg_lake` using the `DataLakeCatalog` engine pointed at Lakekeeper (`catalog_type='rest'`, `warehouse='lakehouse'`, storage endpoint for SeaweedFS), with any experimental setting it needs turned on in `config.d`.
    - Database `delta_gold` with one view per gold table: `CREATE VIEW … AS SELECT * FROM deltaLake('http://seaweedfs:8333/delta/{ns}_gold/{table}/', key, secret)`.
  - If `DataLakeCatalog` doesn't work against Lakekeeper with this ClickHouse version, fall back to `icebergS3` views over each table's metadata location fetched from the REST catalog, and record which approach is used in the README.
- [ ] **Step 1: Write failing tests**: in ClickHouse, `SELECT sum(trips) FROM delta_gold.daily_zone_stats` equals Spark's sum over the delta gold table; the same holds for the Iceberg `manual_gold.daily_zone_stats` reached through `iceberg_lake`.
- [ ] **Step 2–4: Implement and pass.**
- [ ] **Step 5: Commit** `feat: clickhouse reads iceberg and delta gold`.

---

### Task 12: Engine × format capability matrix

**Files:**
- Create: `src/de_playground/matrix/{__init__,probes,report,__main__}.py`, `docs/engine-format-matrix.md` (generated)
- Modify: `Makefile` (`matrix`)
- Test: `tests/unit/test_matrix_report.py`, `tests/integration/test_matrix_smoke.py`

**Interfaces:**
- Consumes: `ducklake_engine`, `spark_engine`, `Settings`, the ClickHouse client.
- Produces:
  - `Op = Literal["create","append","overwrite","merge","read"]`; `ProbeResult(engine, format, op, status: Literal["ok","fail","n/a"], detail: str)`.
  - `EngineAdapter` Protocol: `name: str`; `supports(format) -> bool`; and `create(format, table)`, `append(...)`, `overwrite(...)`, `merge(...)` and `read(...) -> int`, all working on a 2-column test table `(id INT, v STRING)` in namespace `matrix`. A method that raises counts as `fail`, with the first line of the exception as `detail`. `supports() == False` gives `n/a`.
  - Adapters: `duckdb` (ducklake natively, plus Iceberg via the DuckDB `iceberg` extension attached to the Lakekeeper REST catalog, plus Delta via the `delta` extension), `spark` (iceberg, delta; ducklake `n/a`, linking to the spec's research note), `python` (`deltalake` for delta, `pyiceberg` for iceberg; ducklake `n/a`), and `clickhouse` (iceberg and delta, which it can read and possibly write; ducklake `n/a`).
  - `run_all() -> list[ProbeResult]`, which runs the probes in order create → append → overwrite → merge → read for each engine × format pair, and `render_markdown(results) -> str`. The output has one table per format, with engines as rows, ops as columns, and cells `✅` / `❌` / `—`. A "Notes" list gives each failure's detail and the date run. `make matrix` writes `docs/engine-format-matrix.md`.
- [ ] **Step 1: Write failing tests**: unit `test_render_markdown` takes fixed results and checks the exact table layout, including `—` for n/a and failure notes. Integration `test_matrix_smoke` checks that `run_all()` returns 4 engines × 3 formats × 5 ops = 60 results and that `duckdb/ducklake/*` and `spark/iceberg/*` are all `ok`.
- [ ] **Step 2–4: Implement and pass.**
- [ ] **Step 5:** run `make matrix` and commit the generated doc together with the code: `feat: engine x format capability matrix`.

---

## Out of scope for this plan (later plans)

Streaming (Kafka, Kafka UI, Wikimedia producer, PyFlink, ClickHouse ingestion), the orchestrators (Dagster, Prefect, DBOS), dbt, Debezium, OpenLineage, and the AI layer. `run_month`/`run_layer` and `Target.namespace` are the hooks that the Phase 4 plan builds on.
