import ast
import sys
from pathlib import Path

from de_playground.streaming.flink import statements

CAT = statements.iceberg_catalog_ddl("http://lakekeeper:8181", "lakehouse", "http://seaweedfs:8333", "AK", "SK")


def test_iceberg_catalog_ddl():
    assert CAT.startswith("CREATE CATALOG lakehouse WITH (")
    for part in ["'type' = 'iceberg'", "'catalog-type' = 'rest'", "'uri' = 'http://lakekeeper:8181/catalog'",
                 "'warehouse' = 'lakehouse'", "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'",
                 "'s3.endpoint' = 'http://seaweedfs:8333'", "'s3.path-style-access' = 'true'",
                 "'client.region' = 'us-east-1'", "'s3.access-key-id' = 'AK'", "'s3.secret-access-key' = 'SK'"]:
        assert part in CAT, part


def test_delta_options():
    # The released connector (4.4.1) rejects per-table fs.* options: S3 settings come from the
    # image's Hadoop core-site.xml instead, so only the table itself is described here.
    opts = statements.delta_options("s3a://delta/ns_bronze/t", partitions="event_date")
    assert opts == "'connector' = 'delta',\n    'table_path' = 's3a://delta/ns_bronze/t',\n    'partitions' = 'event_date'"
    assert "partitions" not in statements.delta_options("s3a://delta/x")
    assert "fs.s3a" not in opts


def test_statements_module_is_stdlib_only():
    tree = ast.parse(Path(statements.__file__).read_text())
    modules = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    modules |= {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    assert modules - set(sys.stdlib_module_names) - {"pyflink", "__future__"} == set()
