#!/bin/bash
# Run the Spark Connect server in the foreground. S3 keys come from the environment (.env),
# never from the image.
set -euo pipefail
export SPARK_NO_DAEMONIZE=1
exec /opt/spark/sbin/start-connect-server.sh \
  --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}" \
  --conf "spark.sql.catalog.iceberg.s3.access-key-id=${S3_ACCESS_KEY}" \
  --conf "spark.sql.catalog.iceberg.s3.secret-access-key=${S3_SECRET_KEY}"
