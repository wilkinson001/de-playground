# Flink 2.0 (with PyFlink, Kafka, Iceberg and Delta)

A Flink 2.0.1 session cluster, profile `streaming` (`make up-stream`):

| Service | What | Port |
|---|---|---|
| `flink-jobmanager` | Schedules jobs; web UI + REST API | http://localhost:8081 |
| `flink-taskmanager` | Runs the work (4 slots) | — |
| `flink-sql-gateway` | REST API for running Flink SQL (used by tests and the matrix) | http://localhost:8083 |

All three use one image, `de-playground/flink:2.0.1` (`Dockerfile`), with connector jars in
`/opt/flink/lib`. Checkpoints go to the shared `flinkcheckpoints` volume. The repo's `src/` is
mounted read-only at `/opt/playground/src`, so PyFlink jobs run straight from the working tree.

## Connectors and why each jar is there

The jars are resolved by Maven from `pom.xml` in the first build stage. Everything that took a
debugging step to find is recorded here, because each one is a typical "connector jar hell" lesson:

| Jar(s) | Why |
|---|---|
| `flink-sql-connector-kafka` 4.0.1-2.0 | Kafka source and sink (the `-2.0` suffix is the Flink version). |
| `iceberg-flink-runtime-2.0` 1.10.2 | Iceberg; same 1.10 line as Spark's. Includes `S3FileIO` but not the AWS SDK. |
| `delta-flink_2.0` 4.4.1 | The Kernel-based Delta connector: **experimental and sink-only** (Flink can write Delta, not read it). |
| `delta-kernel-api` | Missing from `delta-flink`'s POM → `ClassNotFoundException: io.delta.kernel.internal.util.Preconditions`. |
| `hadoop-aws` 3.4.2 + AWS SDK `bundle` 2.29.52 | S3A filesystem for Delta. The **one** AWS SDK in the image also serves Iceberg's `S3FileIO`, which is why `iceberg-aws-bundle` is *not* added (it would put a second SDK on the classpath). |
| `hadoop-client-api` 3.4.2 | Delta Kernel brings the shaded runtime half only; Iceberg's catalog needs Hadoop's `Configuration`. |
| `guava` 33.3.1 | Delta's S3 log store uses unshaded Guava → `ClassNotFoundException: com.google.common.io.CountingOutputStream`. |
| Jackson pinned to 2.17.0 (BOM) | Delta's dependencies mixed databind 2.13 with core 2.17. |
| *excluded*: `org.slf4j`, Log4j | `delta-kernel-api` drags in SLF4J 1.7, which shadowed Flink's SLF4J 2 and stopped the JobManager starting (`NoClassDefFoundError: org/slf4j/spi/LoggingEventBuilder`). |

PyFlink: there are no Linux arm64 wheels for `apache-flink` 2.0.1, so the image compiles it (and
`pemja`, its Java↔Python bridge) from source. That's why the build installs a compiler and JDK
headers temporarily, why the first build takes ~6 minutes, and why the image is ~6 GB.
`flink-python-2.0.1.jar` is copied from `opt/` to `lib/` so the cluster can run Python jobs.

## S3 for Delta: Hadoop config, not table options

The released Delta connector rejects per-table `fs.s3a.*` options (`Unsupported options found
for 'delta'`). The README on Delta's `master` branch documents them, but 4.4.1 doesn't have them.
So S3A is configured in `core-site.xml`, which is on `HADOOP_CONF_DIR` and so on Flink's
classpath. Keys come from the container environment through Hadoop's `${env.S3_ACCESS_KEY}`
substitution.

One more catch: the connector **sets `fs.s3a.path.style.access` itself**, overriding
`core-site.xml`. With that, it addressed `delta.seaweedfs` (virtual-host style, which has no DNS),
not `seaweedfs/delta`. S3A's **per-bucket** options are applied over the base keys when the
filesystem is created, so `fs.s3a.bucket.delta.path.style.access=true` wins.

A Delta sink in Flink SQL is then just a path:

```sql
CREATE TEMPORARY TABLE d (id INT, v STRING)
WITH ('connector' = 'delta', 'table_path' = 's3a://delta/some/table');
```

## Reading what Flink wrote to Delta

Flink's Delta tables use the `v2Checkpoint` reader feature. **delta-rs (`deltalake` in
Python) can't read them yet**, but **DuckDB (`delta_scan`), ClickHouse (`deltaLake()`) and Spark
can**. The streaming tests verify Delta output with DuckDB for that reason.

## Other settings (`config.yaml`)

- `table.exec.source.idle-timeout: 10s`: Wikimedia traffic is keyed by wiki and some Kafka
  partitions get very little (see `docker/kafka/README.md`). Flink's watermark is the minimum
  across partitions, so a quiet partition would stop windows from ever closing.
- `classloader.check-leaked-classloader: false`: Hadoop's `Configuration` remembers the thread's
  classloader when it's created, and Hadoop's codec pool caches one. A later job (e.g. Iceberg
  gzip-compressing metadata after a Delta write in the same SQL Gateway) then touches the earlier
  job's closed classloader and fails with "Trying to access closed classloader". All connector
  jars are in `lib/`, so there's no real user code to leak. This is Flink's documented setting
  for Hadoop-backed connectors.
- Checkpoints every 30 s to `file:///opt/flink/checkpoints` (Iceberg and Delta commit on checkpoints).

## Things to try

- Flink UI → Running Jobs, to see the streaming job's graph, watermarks per operator and checkpoint history.
- Run SQL by hand: `docker compose exec flink-jobmanager ./bin/sql-client.sh gateway --endpoint flink-sql-gateway:8083`
  then `CREATE CATALOG lakehouse WITH (...)` (see `src/de_playground/streaming/flink/statements.py`).

## Sample from the live feed

`make stream-job`, about 2½ minutes after submitting against the live Wikimedia stream (the job
first worked through the producer's backlog from the earliest offset):

- bronze: **125,825** events in `flink_stream_bronze.recentchange` (Delta copy read with DuckDB)
- gold: **3,563** rows in `flink_stream_gold.edits_per_minute`
- the latest closed window (22:05 UTC), busiest wikis first:

| wiki | edits | bot | human | new pages |
|---|---:|---:|---:|---:|
| commonswiki | 964 | 632 | 332 | 5 |
| enwiki | 273 | 79 | 194 | 4 |
| wikidatawiki | 206 | 8 | 198 | 3 |
| elwiktionary | 117 | 117 | 0 | 0 |
| arwiki | 77 | 75 | 2 | 1 |

Query it yourself: `make shell`, then DuckDB's
`delta_scan('s3://delta/flink_stream_gold/edits_per_minute')` (see `tests/integration/conftest.py`
for the connection setup), or ClickHouse's `deltaLake(seaweedfs, url = 'http://seaweedfs:8333/delta/flink_stream_gold/edits_per_minute/')`.

## Restarting the job without losing or duplicating data

`make stream-job` **replaces** a running job via a savepoint: `make stream-stop` (also run by
`make stream-job` and `make pipelines-down`) stops the job with a savepoint, recording its path
in `.data/flink/savepoints/`, and the next `make stream-job` resumes from it with
`flink run -s` (used once).

A plain cancel + resubmit is wrong in either of two ways, depending on whether the old job had
committed its Kafka offsets yet. It either **loses the windows still open** (gold undercounted)
or **re-reads events already written** (duplicate bronze); the tests caught the second (96 bronze
rows for 66 events). `make stream-cancel` still does a hard cancel, for when you want to throw
the state away.

Restoring a savepoint needs **stable operator ids**, which took two fixes:

- **Compiled plan.** Planned on the fly, Flink SQL numbers its operators from a counter in the
  planning JVM. The SQL Gateway is long-lived, so every submission got new ids and the savepoint
  "cannot map state … operator not available in the new program". The job now runs as
  `COMPILE PLAN … IF NOT EXISTS FOR STATEMENT SET` + `EXECUTE PLAN`, with the plan file name
  hashed from the SQL, so changing the job's SQL gets a fresh plan instead of a stale one.
- **Delta sink `uid`.** Without the connector's `'uid'` option, the Delta sink's operators got
  new random ids every submission. Each Delta table now has a fixed one (`<ns>-delta-bronze/gold`).

If you change the job's SQL so its state is no longer compatible, run `make stream-cancel` first
(no savepoint), and the new job starts from its consumer group's committed offsets.
