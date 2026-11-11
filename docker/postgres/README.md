# Postgres 18

One Postgres 18 instance (`pgvector/pgvector` image, so the pgvector extension is available for the
AI phase) shared by every tool, each in its own database:

| Database     | Used by                                  |
|--------------|------------------------------------------|
| `ducklake`   | DuckLake catalog (table metadata)        |
| `lakekeeper` | Lakekeeper — the Iceberg REST catalog    |
| `dbos`       | DBOS system tables (Phase 4)             |
| `prefect`    | Prefect server (Phase 4)                 |
| `dagster`    | Dagster run/event storage (Phase 4)      |

Databases are created on first start by `init/01-databases.sql` (only runs on an empty volume;
`make nuke` resets it). Note the Postgres 18 image keeps data under `/var/lib/postgresql`, not
`/var/lib/postgresql/data` as older versions did.

## Connect

- From the host: `psql postgresql://playground:playground@localhost:5432/postgres`
- From the runner: host `postgres`, port 5432.

## Things to try

- After a DuckLake pipeline run, look at the catalog: `\c ducklake` then `\dt` — DuckLake's
  `ducklake_table`, `ducklake_snapshot`, `ducklake_data_file`… are plain SQL tables. Query them.
- `\c lakekeeper` and compare: Lakekeeper stores Iceberg *pointers* to metadata files in S3,
  whereas DuckLake stores *all* metadata in Postgres.

## Version notes

Postgres 18 is used everywhere unless a tool fails on it; record any such failure here.
