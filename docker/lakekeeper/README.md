# Lakekeeper (Iceberg REST catalog)

[Lakekeeper](https://docs.lakekeeper.io) implements the Apache Iceberg REST catalog spec. Spark,
Flink, PyIceberg, DuckDB and ClickHouse all talk to it over HTTP to find Iceberg tables; the
table data and metadata files live in the `iceberg` bucket on SeaweedFS.

- REST catalog: `http://localhost:8181/catalog` (inside Compose: `http://lakekeeper:8181/catalog`)
- Web UI: http://localhost:8181/ui
- Catalog state: Postgres database `lakekeeper` (schema migrated by the one-shot
  `lakekeeper-migrate` service before `lakekeeper` starts).
- Auth is disabled — local playground only.

## Warehouse and storage credentials

`make bootstrap` accepts the terms of use (bootstrap) and creates warehouse **`lakehouse`**
pointing at bucket `iceberg` (`src/de_playground/bootstrap.py`).

SeaweedFS has no STS service, so the storage profile sets `sts-enabled: false` and
`remote-signing-enabled: false`. That means Lakekeeper does **not** hand out credentials: every
client must be configured with its own static S3 keys, endpoint and path-style access.
On a real cloud setup you would normally enable vended credentials so clients never hold keys.

## Things to try

- Open the UI, browse `lakehouse` after a Spark pipeline run, and look at a table's snapshots.
- `curl 'http://localhost:8181/catalog/v1/config?warehouse=lakehouse'` — the config every client
  fetches first.
- In Postgres: `\c lakekeeper` — note it stores pointers to `metadata.json` files, not the
  metadata itself (compare with DuckLake, which keeps everything in Postgres).

## Version notes

Runs fine on Postgres 18 (`v0.14.2`, verified by `tests/integration/test_bootstrap.py`).
