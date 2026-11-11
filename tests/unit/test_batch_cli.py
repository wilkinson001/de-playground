import pytest

from de_playground.batch import cli
from de_playground.batch.target import Target


def test_parse_all_targets_and_months():
    args = cli.parse_args(["run", "--target", "all", "--months", "2025-01..2025-02"])
    assert args.targets == [
        Target("duckdb", "ducklake", "manual"),
        Target("spark", "iceberg", "manual"),
        Target("spark", "delta", "manual"),
    ]
    assert [str(m) for m in args.months] == ["2025-01", "2025-02"]
    assert args.layers == ("bronze", "silver", "gold")


def test_parse_single_target_namespace_and_layers():
    args = cli.parse_args(["run", "--target", "spark:delta", "--months", "2025-03", "--namespace", "dev",
                           "--layers", "bronze,silver"])
    assert args.targets == [Target("spark", "delta", "dev")]
    assert args.layers == ("bronze", "silver")


@pytest.mark.parametrize("bad", [
    ["run", "--target", "all", "--months", "2025-13"],
    ["run", "--target", "spark:ducklake", "--months", "2025-01"],
    ["run", "--target", "all", "--months", "2025-01", "--layers", "platinum"],
])
def test_parse_rejects_bad_input(bad, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.parse_args(bad)
    assert exc.value.code == 2
    assert capsys.readouterr().err  # argparse printed the reason


def test_run_order_is_target_then_month_then_layer(monkeypatch):
    calls = []

    def fake_run_layer(engine, target, layer, month, sources):
        calls.append((str(target), str(month), layer))
        return 1

    monkeypatch.setattr(cli, "run_layer", fake_run_layer)
    args = cli.parse_args(["run", "--target", "all", "--months", "2025-01..2025-02", "--layers", "bronze,gold"])
    lines = []
    cli.run(args, engine_factory=lambda target: object(), out=lines.append)

    assert calls[:4] == [
        ("duckdb:ducklake", "2025-01", "bronze"),
        ("duckdb:ducklake", "2025-01", "gold"),
        ("duckdb:ducklake", "2025-02", "bronze"),
        ("duckdb:ducklake", "2025-02", "gold"),
    ]
    assert len(calls) == 12
    assert len(lines) == 12 and "duckdb:ducklake 2025-01 bronze" in lines[0]


def test_run_stops_and_names_the_failure(monkeypatch):
    def failing(engine, target, layer, month, sources):
        if layer == "silver":
            raise RuntimeError("boom")
        return 1

    monkeypatch.setattr(cli, "run_layer", failing)
    args = cli.parse_args(["run", "--target", "spark:iceberg", "--months", "2025-01"])
    with pytest.raises(cli.BatchFailed, match="spark:iceberg 2025-01 silver: boom"):
        cli.run(args, engine_factory=lambda target: object(), out=lambda line: None)
