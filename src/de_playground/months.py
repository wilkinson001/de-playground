"""Calendar months, the unit the batch pipeline processes data in."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

_MONTH_RE = re.compile(r"^(\d{4})-(\d{2})$")


@dataclass(frozen=True, order=True)
class Month:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"month must be 1-12, got {self.month}")

    @classmethod
    def parse(cls, s: str) -> Month:
        m = _MONTH_RE.match(s)
        if not m:
            raise ValueError(f"expected YYYY-MM, got {s!r}")
        return cls(int(m.group(1)), int(m.group(2)))

    @classmethod
    def range(cls, spec: str) -> list[Month]:
        """Parse "YYYY-MM" or an inclusive range "YYYY-MM..YYYY-MM"."""
        start_s, _, end_s = spec.partition("..")
        start = cls.parse(start_s)
        end = cls.parse(end_s) if end_s else start
        if end < start:
            raise ValueError(f"range end {end} is before start {start}")
        months = [start]
        while months[-1] < end:
            months.append(months[-1].next())
        return months

    def next(self) -> Month:
        return Month(self.year + 1, 1) if self.month == 12 else Month(self.year, self.month + 1)

    @property
    def start(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def next_start(self) -> date:
        return self.next().start

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"
