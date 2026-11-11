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

# Kafka source/sink for Structured Streaming. kafka-clients and commons-pool2 are the versions
# spark-sql-kafka-0-10_2.13:4.0.1's POM (and Spark 4.0.1's parent POM) name.
ADD --chmod=644 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    /opt/spark/jars/

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/playground/entrypoint.sh
RUN chmod 755 /opt/playground/entrypoint.sh && mkdir -p /tmp/spark-events /opt/spark/checkpoints \
 && chown spark /tmp/spark-events /opt/spark/checkpoints

USER spark
ENTRYPOINT ["/opt/playground/entrypoint.sh"]
