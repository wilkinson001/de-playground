"""PyFlink entrypoint for the Wikimedia streaming job (runs inside the Flink containers).

Submitted by `make stream-job`:
    flink run -d -py /opt/playground/src/de_playground/streaming/flink/job.py
Configuration comes from the environment (see JobConfig.from_env).
"""

from __future__ import annotations

import os
import sys

# Make `de_playground` importable from the mounted source tree without installing it.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from pyflink.table import EnvironmentSettings, TableEnvironment  # noqa: E402

from de_playground.streaming.flink.statements import JobConfig, build_statements  # noqa: E402


def main() -> None:
    cfg = JobConfig.from_env(os.environ)
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = t_env.get_config()
    conf.set("pipeline.name", cfg.job_name)
    conf.set("execution.checkpointing.interval", cfg.checkpoint_interval)
    conf.set("table.local-time-zone", "UTC")
    conf.set("table.exec.source.idle-timeout", "10s")

    setup, inserts = build_statements(cfg)
    for statement in setup:
        t_env.execute_sql(statement)
    statement_set = t_env.create_statement_set()
    for insert in inserts:
        statement_set.add_insert_sql(insert)
    result = statement_set.execute()
    print(f"submitted {cfg.job_name}: {result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
