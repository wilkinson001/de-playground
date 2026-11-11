"""Start, find and stop the Flink streaming job from the runner (Flink REST + SQL Gateway).

`make stream-job` submits the PyFlink entrypoint (`flink/job.py`) inside the JobManager container;
`submit` here runs the *same* statements through the SQL Gateway, which is what tests (and later
the orchestrators) use because they can't exec into the Flink containers.
"""

from __future__ import annotations

import httpx

from de_playground.config import Settings
from de_playground.streaming.flink.statements import JobConfig, build_statements
from de_playground.streaming.flink_gateway import FlinkSql

ACTIVE_STATES = {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "RECONCILING"}


def job_properties(cfg: JobConfig) -> dict[str, str]:
    return {
        "execution.runtime-mode": "streaming",
        "pipeline.name": cfg.job_name,
        "execution.checkpointing.interval": cfg.checkpoint_interval,
        "table.local-time-zone": "UTC",
        "table.exec.source.idle-timeout": "10s",
        "table.dml-sync": "false",  # streaming INSERTs never finish: return once submitted
    }


def running_jobs(settings: Settings, name: str) -> list[str]:
    r = httpx.get(f"{settings.flink_url}/jobs/overview", timeout=30)
    r.raise_for_status()
    return [j["jid"] for j in r.json()["jobs"] if j["name"] == name and j["state"] in ACTIVE_STATES]


def cancel_jobs(settings: Settings, name: str) -> list[str]:
    cancelled = running_jobs(settings, name)
    for jid in cancelled:
        httpx.patch(f"{settings.flink_url}/jobs/{jid}", params={"mode": "cancel"}, timeout=30).raise_for_status()
    return cancelled


def submit(settings: Settings, cfg: JobConfig) -> str:
    """Replace any running job of the same name, then submit. Returns the new job id."""
    cancel_jobs(settings, cfg.job_name)
    setup, inserts = build_statements(cfg)
    with FlinkSql(settings, job_properties(cfg)) as sql:
        for statement in setup:
            sql.execute(statement)
        result = sql.execute("EXECUTE STATEMENT SET BEGIN\n" + ";\n".join(inserts) + ";\nEND")
    return result[0][0]
