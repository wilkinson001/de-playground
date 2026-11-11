import uuid

import pytest

from de_playground import storage
from de_playground.batch.engines import ducklake_engine

pytestmark = pytest.mark.integration


def test_ducklake_postgres_catalog_on_s3(settings):
    engine = ducklake_engine(settings)
    schema = f"smoke_{uuid.uuid4().hex[:8]}"
    engine.execute(f"CREATE SCHEMA {schema}; CREATE TABLE {schema}.t (id INTEGER)")
    # Enough rows to exceed DuckLake's data-inlining limit, so a Parquet file is really written
    # (tiny inserts are stored inline in the Postgres catalog instead).
    engine.execute(f"INSERT INTO {schema}.t SELECT range FROM range(10000)")
    try:
        assert engine.query(f"SELECT count(*) FROM {schema}.t") == [(10000,)]
        listed = storage.s3_client(settings).list_objects_v2(Bucket="ducklake", Prefix=schema)
        assert any(o["Key"].endswith(".parquet") for o in listed.get("Contents", []))
    finally:
        engine.execute(f"DROP TABLE {schema}.t; DROP SCHEMA {schema}")
