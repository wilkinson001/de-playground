# Spark (Spark Connect) with Iceberg and Delta

One container runs Spark 4.0.1 in `local[6]` mode (driver and executors in one JVM, 6 GB
driver memory under an 8 GB container limit) and serves it over **Spark Connect** on port 15002.
Clients — pipeline code, and later the orchestrators — use only the thin `pyspark-client`
package (`SparkSession.builder.remote("sc://spark-connect:15002")`); all the JVM work happens here.

- Spark Connect: `sc://localhost:15002` from the host, `sc://spark-connect:15002` in Compose
- Spark UI: http://localhost:4040 (jobs, stages, SQL plans of everything clients run)
- Profile: `spark` (`make up-batch` starts it with core + ClickHouse)

## Formats in one session

| Catalog          | Format  | How tables are addressed                           |
|------------------|---------|----------------------------------------------------|
| `iceberg`        | Iceberg | `iceberg.<namespace>.<table>` via Lakekeeper REST  |
| `spark_catalog`  | Delta   | by path: ``delta.`s3a://delta/<schema>/<table>` `` |

Connector jars are baked into the image (`Dockerfile`), so start-up never downloads packages.
Versions move together: Iceberg `1.10.0` (`-4.0_2.13` runtime), Delta `4.0.0`, `hadoop-aws`
`3.4.1` + AWS SDK bundle `2.24.6` matching Spark's Hadoop. All verified by
`tests/integration/test_spark_formats.py`.

S3 credentials are passed in at start-up from `.env` (`entrypoint.sh`), never baked into the image.
Iceberg uses its own `S3FileIO`; Delta and raw-file reads use Hadoop's S3A.

## Gotcha: don't `DROP TABLE … PURGE` Iceberg tables

The Lakekeeper warehouse uses a *hard* delete profile, so Lakekeeper deletes a dropped table's
files itself. A client-side `PURGE` reads the manifests at the same time and races it,
intermittently failing with S3 404s. Use plain `DROP TABLE`.

## Things to try

- Connect from the host: `uv run python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.remote('sc://localhost:15002').getOrCreate(); s.sql('SHOW NAMESPACES IN iceberg').show()"`
- After a pipeline run, `SELECT * FROM iceberg.manual_silver.trips.snapshots` — Iceberg metadata tables.
- `DESCRIBE HISTORY delta.`s3a://delta/manual_silver/trips`` — Delta's transaction log as a table.
- Watch a MERGE in the Spark UI's SQL tab and compare how Iceberg and Delta rewrite files.
