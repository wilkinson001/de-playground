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

## Structured Streaming (the streaming track)

`make stream-spark` starts Spark Connect plus the `spark-stream` service (profile `spark-stream`),
which runs `src/de_playground/streaming/spark_stream.py`. That does the **same work as the Flink
job**: it reads `wikimedia.recentchange` from Kafka, writes bronze to Iceberg and Delta, and writes
1-minute per-wiki aggregates (30 s watermark, append mode) to Iceberg, Delta and
`wikimedia.edits_per_minute.spark`. Its tables are under `spark_stream_*`. `make stream-spark-stop`
stops it.

- **Five queries, one per sink**, each with its own checkpoint under `/opt/spark/checkpoints`
  (the `sparkcheckpoints` volume) and its own Kafka read of the source topic. Flink, by contrast,
  runs one job that reads Kafka once and fans out.
- **Micro-batches every 10 s.** Iceberg and Delta commit per micro-batch; the Kafka sink is
  at-least-once.
- **One global watermark.** Spark tracks the maximum event time seen across all partitions minus
  30 s. Unlike Flink, which takes the minimum across partitions, a quiet Kafka partition can't
  hold Spark's windows open, but a single far-future event can close them early.
- **The client keeps the session alive.** The queries run on the Connect server, and Spark
  Connect keeps a disconnected session's queries running until the session times out. So the
  `spark-stream` process stops its queries on SIGTERM (`stop_grace_period: 30s`). Restarting with
  the same checkpoints stops any older run (`spark.sql.streaming.stopActiveRunOnRestart`).
- **Kafka connector jars** (`spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`,
  `kafka-clients` 3.9.1, `commons-pool2` 2.12.0) are baked into the image like the other connectors.

**Flink and Spark agree.** `test_spark_and_flink_gold_match` checks it on a test topic, and on the
live Wikimedia feed the two engines' Delta gold matched on **66 one-minute windows, 4,365
(window, wiki) rows, 0 differences**.

Things to try: the Spark UI's **Structured Streaming** tab (http://localhost:4040/StreamingQuery/)
shows each query's input rate, batch duration and watermark. Compare it with the Flink UI's
job graph for the same work.
