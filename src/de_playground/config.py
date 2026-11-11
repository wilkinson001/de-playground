"""Connection settings for the playground services, read from the environment.

Defaults are the hostnames/ports on the Compose network, so code running in the
`runner` container needs only the secrets from `.env`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    lakekeeper_url: str
    iceberg_warehouse: str
    spark_remote: str
    clickhouse_host: str
    clickhouse_port: int
    kafka_bootstrap: str
    flink_url: str
    flink_sql_gateway_url: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        def required(name: str) -> str:
            if name not in env:
                raise KeyError(f"missing required environment variable {name}")
            return env[name]

        return cls(
            s3_endpoint=env.get("PLAYGROUND_S3_ENDPOINT", "http://seaweedfs:8333"),
            s3_access_key=required("S3_ACCESS_KEY"),
            s3_secret_key=required("S3_SECRET_KEY"),
            pg_host=env.get("PLAYGROUND_PG_HOST", "postgres"),
            pg_port=int(env.get("PLAYGROUND_PG_PORT", "5432")),
            pg_user=required("POSTGRES_USER"),
            pg_password=required("POSTGRES_PASSWORD"),
            lakekeeper_url=env.get("PLAYGROUND_LAKEKEEPER_URL", "http://lakekeeper:8181"),
            iceberg_warehouse=env.get("PLAYGROUND_ICEBERG_WAREHOUSE", "lakehouse"),
            spark_remote=env.get("PLAYGROUND_SPARK_REMOTE", "sc://spark-connect:15002"),
            clickhouse_host=env.get("PLAYGROUND_CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(env.get("PLAYGROUND_CLICKHOUSE_PORT", "8123")),
            kafka_bootstrap=env.get("PLAYGROUND_KAFKA_BOOTSTRAP", "kafka:29092"),
            flink_url=env.get("PLAYGROUND_FLINK_URL", "http://flink-jobmanager:8081"),
            flink_sql_gateway_url=env.get("PLAYGROUND_FLINK_SQL_GATEWAY_URL", "http://flink-sql-gateway:8083"),
        )

    def s3_host_port(self) -> str:
        return urlparse(self.s3_endpoint).netloc

    def ducklake_attach_uri(self) -> str:
        return (
            f"ducklake:postgres:dbname=ducklake host={self.pg_host} port={self.pg_port} "
            f"user={self.pg_user} password={self.pg_password}"
        )
