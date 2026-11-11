import pytest

from de_playground.config import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()


@pytest.fixture(scope="session")
def delta_reader(settings, tmp_path_factory):
    """Query a Delta table with DuckDB's delta extension (delta-kernel-rs).

    Used to verify streaming output: tables written by the Kernel-based Flink connector use the
    v2Checkpoint reader feature, which delta-rs (`deltalake`) can't read yet; DuckDB can.
    """
    from de_playground.batch.engines import connect_ducklake

    tmp = tmp_path_factory.mktemp("delta_reader")
    engine = connect_ducklake(f"ducklake:{tmp}/c.ducklake", f"{tmp}/data/", settings)
    engine.execute("INSTALL delta; LOAD delta")

    def query(path: str, sql: str = "SELECT count(*) FROM t") -> list[tuple]:
        """`path` is the table's location without scheme, e.g. "delta/ns_bronze/t"; `t` names it in `sql`."""
        return engine.query(f"WITH t AS (SELECT * FROM delta_scan('s3://{path}')) {sql}")

    return query
