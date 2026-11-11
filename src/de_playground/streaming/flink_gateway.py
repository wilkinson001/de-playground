"""Run Flink SQL from the runner through the Flink SQL Gateway's REST API (v3).

Used by integration tests and the engine × format matrix. The streaming job itself is submitted
with `flink run` (see Makefile `stream-job`), not through here.
"""

from __future__ import annotations

import time

import httpx

from de_playground.config import Settings


class FlinkSqlError(RuntimeError):
    pass


class FlinkSql:
    """One SQL Gateway session. DML runs synchronously (`table.dml-sync`), so INSERTs wait for their job."""

    def __init__(self, settings: Settings, properties: dict[str, str] | None = None):
        self.http = httpx.Client(base_url=settings.flink_sql_gateway_url, timeout=60)
        self.properties = {"table.dml-sync": "true", **(properties or {})}
        self.session: str | None = None

    def open(self) -> str:
        r = self.http.post("/v3/sessions", json={"properties": self.properties})
        r.raise_for_status()
        self.session = r.json()["sessionHandle"]
        return self.session

    def execute(self, sql: str, timeout_s: float = 120) -> list[list]:
        """Run one statement; return its result rows (empty for DDL/DML)."""
        if self.session is None:
            self.open()
        base = f"/v3/sessions/{self.session}"
        r = self.http.post(f"{base}/statements", json={"statement": sql})
        r.raise_for_status()
        op = r.json()["operationHandle"]

        rows: list[list] = []
        uri = f"{base}/operations/{op}/result/0?rowFormat=JSON"
        deadline = time.monotonic() + timeout_s
        while uri:
            r = self.http.get(uri)
            if r.status_code >= 400:
                raise FlinkSqlError(_error_text(r, sql))
            body = r.json()
            kind = body.get("resultType")
            if kind == "PAYLOAD":
                rows += [row["fields"] for row in body["results"]["data"]]
            if kind == "EOS":
                break
            uri = body.get("nextResultUri")
            if kind == "NOT_READY":
                if time.monotonic() > deadline:
                    raise FlinkSqlError(f"timed out after {timeout_s}s: {sql[:200]}")
                time.sleep(0.5)
        return rows

    def close(self) -> None:
        if self.session:
            self.http.delete(f"/v3/sessions/{self.session}")
            self.session = None
        self.http.close()

    def __enter__(self) -> FlinkSql:
        self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _error_text(response: httpx.Response, sql: str) -> str:
    try:
        errors = response.json().get("errors", [])
    except ValueError:
        errors = [response.text]
    # The gateway returns a full Java stack trace; the root cause is usually the last "Caused by".
    text = "\n".join(errors)
    causes = [line for line in text.splitlines() if line.startswith("Caused by:")]
    summary = causes[-1] if causes else text.splitlines()[0] if text else f"HTTP {response.status_code}"
    return f"{summary}\n  in: {sql[:200]}"
