"""Start, find and stop the Flink streaming job from the runner (Flink REST + SQL Gateway).

`make stream-job` submits the PyFlink entrypoint (`flink/job.py`) inside the JobManager container;
`submit` here runs the *same* statements through the SQL Gateway, which is what tests (and later
the orchestrators) use because they can't exec into the Flink containers.

Replacing a running job goes through a **savepoint**: the old job stops with its window state and
Kafka offsets saved, and the new one resumes from it. A plain cancel would either lose the windows
still open (undercounted gold) or re-read events already written (duplicate bronze), depending on
whether the old job had committed its Kafka offsets yet.
"""

from __future__ import annotations

import time
from pathlib import Path

import httpx

from de_playground.config import Settings
from de_playground.streaming.flink.statements import (
    SAVEPOINT_DIR, JobConfig, build_statements, job_settings, plan_statements,
)
from de_playground.streaming.flink_gateway import FlinkSql

ACTIVE_STATES = {"RUNNING", "CREATED", "RESTARTING", "INITIALIZING", "RECONCILING", "CANCELLING", "FAILING"}
# Where the last stop's savepoint is recorded, per job name. Inside the repo so both the runner
# (which writes it) and `make stream-job` on the host (which reads it) see the same file.
SAVEPOINTS = Path(".data/flink/savepoints")


def running_jobs(settings: Settings, name: str) -> list[str]:
    r = httpx.get(f"{settings.flink_url}/jobs/overview", timeout=30)
    r.raise_for_status()
    return [j["jid"] for j in r.json()["jobs"] if j["name"] == name and j["state"] in ACTIVE_STATES]


def cancel_jobs(settings: Settings, name: str) -> list[str]:
    """Hard cancel, no savepoint (used for test clean-up). Waits until the jobs have gone."""
    cancelled = running_jobs(settings, name)
    for jid in cancelled:
        httpx.patch(f"{settings.flink_url}/jobs/{jid}", params={"mode": "cancel"}, timeout=30).raise_for_status()
    _wait_gone(settings, name)
    return cancelled


def stop_with_savepoint(settings: Settings, name: str, timeout_s: float = 120) -> str | None:
    """Stop the running job, saving its state. Records and returns the savepoint path (None if
    nothing was running)."""
    jids = running_jobs(settings, name)
    location = None
    for jid in jids:
        _wait_tasks_running(settings, jid)  # Flink refuses a savepoint until every task is up
        r = httpx.post(f"{settings.flink_url}/jobs/{jid}/stop",
                       json={"targetDirectory": SAVEPOINT_DIR, "drain": False}, timeout=30)
        r.raise_for_status()
        trigger = r.json()["request-id"]
        deadline = time.monotonic() + timeout_s
        while True:
            status = httpx.get(f"{settings.flink_url}/jobs/{jid}/savepoints/{trigger}", timeout=30).json()
            if status["status"]["id"] == "COMPLETED":
                op = status["operation"]
                if "failure-cause" in op:
                    raise RuntimeError(f"savepoint for {name} failed: {op['failure-cause'].get('class')}")
                location = op["location"]
                break
            if time.monotonic() > deadline:
                raise TimeoutError(f"savepoint for {name} not done after {timeout_s}s")
            time.sleep(1)
    _wait_gone(settings, name)
    if location:
        SAVEPOINTS.mkdir(parents=True, exist_ok=True)
        (SAVEPOINTS / name).write_text(location)
    return location


def take_savepoint_path(name: str) -> str | None:
    """The savepoint recorded by the last stop, consumed so it is resumed from at most once."""
    path = SAVEPOINTS / name
    if not path.exists():
        return None
    location = path.read_text().strip()
    path.unlink()
    return location or None


def submit(settings: Settings, cfg: JobConfig) -> str:
    """Replace any running job of the same name (via a savepoint), then submit. Returns the job id."""
    stop_with_savepoint(settings, cfg.job_name)
    properties = {
        "execution.runtime-mode": "streaming",
        "table.dml-sync": "false",  # streaming INSERTs never finish: return once submitted
        **job_settings(cfg),
    }
    if savepoint := take_savepoint_path(cfg.job_name):
        properties["execution.state-recovery.path"] = savepoint
    setup, _ = build_statements(cfg)
    compile_sql, execute_sql = plan_statements(cfg)
    with FlinkSql(settings, properties) as sql:
        for statement in setup:
            sql.execute(statement)
        sql.execute(compile_sql)
        result = sql.execute(execute_sql)
    return result[0][0]


def _wait_tasks_running(settings: Settings, jid: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        job = httpx.get(f"{settings.flink_url}/jobs/{jid}", timeout=30).json()
        if job["state"] == "RUNNING" and all(v["status"] == "RUNNING" for v in job["vertices"]):
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {jid} tasks not all running after {timeout_s}s")
        time.sleep(1)


def _wait_gone(settings: Settings, name: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    while running_jobs(settings, name):
        if time.monotonic() > deadline:
            raise TimeoutError(f"{name} still running after {timeout_s}s")
        time.sleep(1)
