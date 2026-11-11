import boto3
from moto import mock_aws

from de_playground import duckdb_ui

SECRETS = {"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"}


def test_host_settings_default_to_localhost():
    s = duckdb_ui.host_settings(SECRETS)
    assert (s.s3_endpoint, s.pg_host, s.lakekeeper_url) == ("http://localhost:8333", "localhost", "http://localhost:8181")
    overridden = duckdb_ui.host_settings({**SECRETS, "PLAYGROUND_PG_HOST": "db.example"})
    assert overridden.pg_host == "db.example"


def test_setup_statements_attach_every_format():
    sql = "\n".join(duckdb_ui.setup_statements(duckdb_ui.host_settings(SECRETS), [("manual_gold", "daily_zone_stats")]))
    assert "ENDPOINT 'localhost:8333'" in sql and "URL_STYLE 'path'" in sql
    assert "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=ducklake host=localhost" in sql
    assert "AS lake (DATA_PATH 's3://ducklake/')" in sql
    assert "AS iceberg (TYPE iceberg, ENDPOINT 'http://localhost:8181/catalog'" in sql
    assert "ACCESS_DELEGATION_MODE 'none'" in sql
    assert "ATTACH IF NOT EXISTS ':memory:' AS delta" in sql
    assert "CREATE SCHEMA IF NOT EXISTS delta.manual_gold" in sql
    assert ("CREATE OR REPLACE VIEW delta.manual_gold.daily_zone_stats AS "
            "SELECT * FROM delta_scan('s3://delta/manual_gold/daily_zone_stats')") in sql


def test_discover_delta_tables_finds_only_tables_with_a_log():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="delta")
        for key in ["manual_gold/daily_zone_stats/_delta_log/00000000000000000000.json",
                    "manual_gold/daily_zone_stats/part-0.parquet",
                    "flink_stream_bronze/recentchange/_delta_log/00000000000000000000.json",
                    "matrix/half_written/part-0.parquet"]:
            client.put_object(Bucket="delta", Key=key, Body=b"x")
        assert duckdb_ui.discover_delta_tables(client) == [
            ("flink_stream_bronze", "recentchange"), ("manual_gold", "daily_zone_stats")]
