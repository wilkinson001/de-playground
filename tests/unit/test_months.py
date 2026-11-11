from datetime import date

import pytest

from de_playground.months import Month


def test_month_parse_and_bounds():
    jan = Month.parse("2025-01")
    assert jan.start == date(2025, 1, 1)
    assert jan.next_start == date(2025, 2, 1)
    assert Month.parse("2025-12").next_start == date(2026, 1, 1)
    assert str(Month.parse("2025-03")) == "2025-03"


@pytest.mark.parametrize("bad", ["2025-13", "2025-1", "25-01", "", "2025-00", "2025/01"])
def test_month_parse_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        Month.parse(bad)


def test_month_range():
    assert [str(m) for m in Month.range("2025-01..2025-03")] == ["2025-01", "2025-02", "2025-03"]
    assert [str(m) for m in Month.range("2024-12..2025-01")] == ["2024-12", "2025-01"]
    assert [str(m) for m in Month.range("2025-02")] == ["2025-02"]
    with pytest.raises(ValueError):
        Month.range("2025-03..2025-01")
