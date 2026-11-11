import pytest

from de_playground.matrix.probes import run_all

pytestmark = pytest.mark.integration


def test_matrix_smoke(settings):
    results = run_all(settings)
    assert len(results) == 4 * 3 * 5
    by = {(r.engine, r.format, r.op): r for r in results}
    for engine, fmt in [("duckdb", "ducklake"), ("spark", "iceberg"), ("spark", "delta")]:
        for op in ("create", "append", "overwrite", "merge", "read"):
            r = by[(engine, fmt, op)]
            assert r.status == "ok", r
