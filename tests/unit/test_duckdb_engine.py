from de_playground.batch.engines import DuckDBEngine, connect_ducklake
from de_playground.months import Month
from tests.fixtures.make_fixtures import sample_rows, write_trips_parquet, write_zones_csv


def _engine(tmp_path) -> DuckDBEngine:
    return connect_ducklake(f"ducklake:{tmp_path}/cat.ducklake", f"{tmp_path}/data/")


def test_local_ducklake_roundtrip(tmp_path):
    engine = _engine(tmp_path)
    assert engine.dialect == "duckdb"
    engine.execute("CREATE SCHEMA IF NOT EXISTS s; CREATE TABLE s.t (id INTEGER, v VARCHAR)")
    engine.execute("INSERT INTO s.t VALUES (1, 'a'), (2, 'b')")
    assert engine.query("SELECT count(*) FROM s.t") == [(2,)]
    engine.con.close()

    reopened = _engine(tmp_path)
    assert reopened.query("SELECT v FROM lake.s.t ORDER BY id") == [("a",), ("b",)]


def test_parquet_columns_and_views(tmp_path):
    engine = _engine(tmp_path)
    trips = write_trips_parquet(tmp_path / "trips.parquet", sample_rows(Month.parse("2025-01")))
    zones = write_zones_csv(tmp_path / "zones.csv")

    cols = engine.parquet_columns(str(trips))
    assert cols[:3] == ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"]
    assert "Airport_fee" in cols

    engine.register_parquet_view("raw_trips", str(trips), 'SELECT src."VendorID" AS vendor_id FROM src')
    assert engine.query("SELECT count(*), min(vendor_id) FROM raw_trips") == [(5, 2)]

    engine.register_csv_view("raw_zones", str(zones))
    assert engine.query("SELECT \"Zone\" FROM raw_zones WHERE \"LocationID\" = 132") == [("JFK Airport",)]


def test_split_statements_respects_quotes_and_comments():
    from de_playground.batch.engines import split_statements

    sql = """
    -- a comment; with a semicolon
    SELECT 'a;b' AS x;
    SELECT `c;d`, "e;f" FROM t;;
    """
    assert split_statements(sql) == ["SELECT 'a;b' AS x", 'SELECT `c;d`, "e;f" FROM t']


def test_duckdb_can_spill_to_disk(tmp_path):
    engine = connect_ducklake(f"ducklake:{tmp_path}/cat.ducklake", f"{tmp_path}/data/", memory_limit="1GB")
    assert engine.query("SELECT current_setting('memory_limit')") == [("953.6 MiB",)]
    assert engine.query("SELECT current_setting('temp_directory')")[0][0].endswith("duckdb_spill")


def test_stage_materialises_view_into_temp_table(tmp_path):
    engine = _engine(tmp_path)
    engine.execute("CREATE TEMPORARY VIEW v AS SELECT range AS id FROM range(3)")
    staged = engine.stage("v")
    assert staged != "v"
    assert engine.query(
        f"SELECT table_type FROM information_schema.tables WHERE table_name = '{staged}'"
    ) == [("LOCAL TEMPORARY",)]
    assert engine.query(f"SELECT count(*) FROM {staged}") == [(3,)]
