# Flink 2.0.1 with PyFlink and the Kafka, Iceberg and Delta connectors.

# Stage 1: resolve the connector jars and their runtime dependencies with Maven.
FROM maven:3.9-eclipse-temurin-17 AS jars
WORKDIR /build
COPY pom.xml .
# Flink provides its own logging (SLF4J 2 + Log4j 2): connector copies of those would shadow it.
RUN mvn -q dependency:copy-dependencies -DincludeScope=runtime \
      -DexcludeGroupIds=org.apache.flink.shaded,org.slf4j,org.apache.logging.log4j \
      -DoutputDirectory=/build/lib \
 && cp ~/.m2/repository/org/apache/flink/flink-sql-connector-kafka/*/flink-sql-connector-kafka-*.jar /build/lib/ \
 && ls /build/lib | grep -E '^flink-(core|streaming|table|runtime|clients)' && exit 1 || true

# Stage 2: Flink + Python. There are no Linux arm64 wheels for apache-flink 2.0.1, so pip builds
# it (and pemja, its Java/Python bridge) from source: that needs a compiler and JDK headers.
FROM flink:2.0.1-java17
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev build-essential openjdk-17-jdk-headless \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && JAVA_HOME=/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) \
    pip3 install --no-cache-dir apache-flink==2.0.1 \
 && apt-get purge -y build-essential python3-dev openjdk-17-jdk-headless && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*
COPY --from=jars /build/lib/ /opt/flink/lib/
# PyFlink's runtime jar ships in opt/; Python jobs need it on the cluster classpath.
RUN cp /opt/flink/opt/flink-python-2.0.1.jar /opt/flink/lib/
# Baked in rather than mounted: the image's entrypoint rewrites conf/config.yaml at start-up.
COPY config.yaml /opt/flink/conf/config.yaml
COPY core-site.xml /opt/flink/conf/hadoop/core-site.xml
ENV HADOOP_CONF_DIR=/opt/flink/conf/hadoop
RUN mkdir -p /opt/flink/checkpoints && chown -R flink:flink /opt/flink/checkpoints /opt/flink/lib
USER flink
