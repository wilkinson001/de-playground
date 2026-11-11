# ClickHouse

A columnar analytics database. In phases 1–2 it **reads the gold tables Spark wrote**, straight
from the lakehouse, without copying data. In phase 3 it will also ingest the streaming track.

- HTTP: http://localhost:8123 — built-in SQL UI at http://localhost:8123/play
- Native protocol: `localhost:9000` (`clickhouse client` / DBeaver etc.)
- User `default`, no password (local only). Profile: `clickhouse`.

## How it reaches the lakehouse

`make bootstrap` (when ClickHouse is running) calls `src/de_playground/clickhouse_setup.py`:

| Database       | Engine / mechanism | What you get |
|----------------|--------------------|--------------|
| `iceberg_lake` | `DataLakeCatalog` → Lakekeeper REST catalog | every Iceberg table, as `` iceberg_lake.`manual_gold.daily_zone_stats` `` |
| `delta_gold`   | views over the `deltaLake()` table function | `delta_gold.daily_zone_stats` etc. |

Credentials never appear in SQL:
- `config.d/playground.xml` defines a `seaweedfs` **named collection** (used by the Delta views) and
  per-endpoint `<s3>` credentials for `http://seaweedfs:8333` (used for Iceberg data files, since
  Lakekeeper doesn't vend credentials here).
- `users.d/playground.xml` enables `allow_experimental_database_iceberg` and
  `s3_allow_server_credentials_in_user_queries` — ClickHouse otherwise refuses to use
  server-side keys for user queries (error 497).

DuckLake can't be read by ClickHouse (no support), which is one of the cells in
`docs/engine-format-matrix.md`.

## Things to try

```sql
SHOW TABLES FROM iceberg_lake;
SELECT pu_borough, sum(trips) FROM iceberg_lake.`manual_gold.daily_zone_stats` GROUP BY ALL ORDER BY 2 DESC;
-- Compare query time for the same question over Delta and over Iceberg:
SELECT pickup_hour, sum(trips) FROM delta_gold.hourly_borough_demand GROUP BY ALL ORDER BY 1;
-- Read a raw file straight from object storage:
SELECT count() FROM s3(seaweedfs, url = 'http://seaweedfs:8333/raw/tlc/yellow/yellow_tripdata_2025-01.parquet');
```
