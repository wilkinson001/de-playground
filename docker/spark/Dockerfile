# Spark Connect server with Iceberg + Delta + S3A baked in (no runtime package downloads).
FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu

ARG MAVEN=https://repo1.maven.org/maven2
USER root
# Versions must move together: Iceberg's runtime is built per Spark minor, Delta per Spark major,
# hadoop-aws must match Spark's bundled Hadoop (3.4.1) and its AWS SDK v2 bundle (2.24.6).
ADD --chmod=644 \
    ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
    ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar \
    ${MAVEN}/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar \
    ${MAVEN}/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar \
    ${MAVEN}/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    ${MAVEN}/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    /opt/spark/jars/

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/playground/entrypoint.sh
RUN chmod 755 /opt/playground/entrypoint.sh && mkdir -p /tmp/spark-events && chown spark /tmp/spark-events

USER spark
ENTRYPOINT ["/opt/playground/entrypoint.sh"]
