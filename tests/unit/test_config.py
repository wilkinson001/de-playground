import pytest

from de_playground.config import Settings

REQUIRED = {
    "S3_ACCESS_KEY": "ak",
    "S3_SECRET_KEY": "sk",
    "POSTGRES_USER": "pguser",
    "POSTGRES_PASSWORD": "pgpass",
}


def test_settings_from_env_defaults():
    s = Settings.from_env(REQUIRED)
    assert s.s3_endpoint == "http://seaweedfs:8333"
    assert s.s3_access_key == "ak"
    assert s.pg_host == "postgres"
    assert s.pg_port == 5432
    assert s.lakekeeper_url == "http://lakekeeper:8181"
    assert s.iceberg_warehouse == "lakehouse"
    assert s.spark_remote == "sc://spark-connect:15002"
    assert s.clickhouse_host == "clickhouse"
    assert s.clickhouse_port == 8123
    assert s.s3_host_port() == "seaweedfs:8333"
    assert s.ducklake_attach_uri() == (
        "ducklake:postgres:dbname=ducklake host=postgres port=5432 user=pguser password=pgpass"
    )


def test_settings_from_env_overrides():
    s = Settings.from_env(
        {**REQUIRED, "PLAYGROUND_S3_ENDPOINT": "http://localhost:8333", "PLAYGROUND_PG_HOST": "localhost"}
    )
    assert s.s3_host_port() == "localhost:8333"
    assert "host=localhost" in s.ducklake_attach_uri()


def test_settings_missing_required_names_var():
    env = {k: v for k, v in REQUIRED.items() if k != "S3_ACCESS_KEY"}
    with pytest.raises(KeyError, match="S3_ACCESS_KEY"):
        Settings.from_env(env)
