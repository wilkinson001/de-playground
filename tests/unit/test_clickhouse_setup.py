from de_playground import clickhouse_setup
from de_playground.config import Settings

SETTINGS = Settings.from_env(
    {"S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"}
)


def test_fresh_stack_creates_databases_but_no_delta_views():
    stmts = clickhouse_setup.statements(SETTINGS, "manual", existing_delta_tables=set())
    assert any("iceberg_lake" in s for s in stmts)
    assert "CREATE DATABASE IF NOT EXISTS delta_gold" in stmts
    assert not any("deltaLake(" in s for s in stmts)


def test_delta_views_only_for_existing_tables():
    stmts = clickhouse_setup.statements(SETTINGS, "manual", existing_delta_tables={"daily_zone_stats"})
    views = [s for s in stmts if "deltaLake(" in s]
    assert len(views) == 1
    assert "delta_gold.daily_zone_stats" in views[0]
    assert "http://seaweedfs:8333/delta/manual_gold/daily_zone_stats/" in views[0]
