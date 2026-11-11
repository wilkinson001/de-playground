import pytest

from de_playground.matrix.probes import run_all

pytestmark = pytest.mark.integration


def test_matrix_smoke(settings):
    results = run_all(settings)
    assert len(results) == 5 * 3 * 5
    by = {(r.engine, r.format, r.op): r for r in results}
    for engine, fmt in [("duckdb", "ducklake"), ("spark", "iceberg"), ("spark", "delta")]:
        for op in ("create", "append", "overwrite", "merge", "read"):
            r = by[(engine, fmt, op)]
            assert r.status == "ok", r
    for op in ("create", "append", "read"):
        assert by[("flink", "iceberg", op)].status == "ok", by[("flink", "iceberg", op)]
    assert by[("flink", "ducklake", "create")].status == "n/a"


def test_matrix_is_rerunnable(settings):
    # Run twice: no probe may fail just because a previous run left its table behind.
    run_all(settings)
    leftovers = [r for r in run_all(settings) if "already exists" in r.detail.lower()]
    assert leftovers == []
