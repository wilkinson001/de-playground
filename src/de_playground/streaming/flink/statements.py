"""Flink SQL for the streaming job, rendered from plain values.

Runs inside the Flink containers too, so it imports only the standard library (and pyflink).
"""

from __future__ import annotations


def _with(options: dict[str, str]) -> str:
    return ",\n    ".join(f"'{k}' = '{v}'" for k, v in options.items())


def iceberg_catalog_ddl(lakekeeper_url: str, warehouse: str, s3_endpoint: str, access_key: str, secret_key: str) -> str:
    """Catalog `lakehouse` → the Lakekeeper REST catalog; data files via S3FileIO with static keys."""
    return "CREATE CATALOG lakehouse WITH (\n    " + _with({
        "type": "iceberg",
        "catalog-type": "rest",
        "uri": f"{lakekeeper_url}/catalog",
        "warehouse": warehouse,
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "s3.endpoint": s3_endpoint,
        "s3.path-style-access": "true",
        "client.region": "us-east-1",
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
    }) + "\n)"


def delta_options(path: str, partitions: str | None = None) -> str:
    """WITH-clause body for a path-based Delta sink (the connector creates the table on first write).

    S3 endpoint and credentials are not table options: the released connector rejects them, so
    they come from the image's Hadoop config (docker/flink/core-site.xml).
    """
    options = {"connector": "delta", "table_path": path}
    if partitions:
        options["partitions"] = partitions
    return _with(options)
