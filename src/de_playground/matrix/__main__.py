"""`python -m de_playground.matrix` — probe every engine × format and write the report."""

from datetime import date
from pathlib import Path

from de_playground.config import Settings
from de_playground.matrix.probes import run_all
from de_playground.matrix.report import render_markdown

OUT = Path("docs/engine-format-matrix.md")

results = run_all(Settings.from_env())
OUT.write_text(render_markdown(results, date.today()))
ok = sum(r.status == "ok" for r in results)
fail = sum(r.status == "fail" for r in results)
print(f"{len(results)} probes: {ok} ok, {fail} failed, {len(results) - ok - fail} n/a -> {OUT}")
