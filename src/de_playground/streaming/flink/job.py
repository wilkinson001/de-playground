"""PyFlink entrypoint for the Wikimedia streaming job (runs inside the Flink containers).

Submitted by `make stream-job`:
    flink run -d [-s <savepoint>] -py /opt/playground/src/de_playground/streaming/flink/job.py
Configuration comes from the environment (see JobConfig.from_env).
"""

from __future__ import annotations

import os
import sys

# Make `de_playground` importable from the mounted source tree without installing it.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from pyflink.table import EnvironmentSettings, TableEnvironment  # noqa: E402

from de_playground.streaming.flink.statements import JobConfig, build_statements, job_settings, plan_statements  # noqa: E402


def main() -> None:
    cfg = JobConfig.from_env(os.environ)
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    conf = t_env.get_config()
    for key, value in job_settings(cfg).items():
        conf.set(key, value)

    setup, _ = build_statements(cfg)
    for statement in setup:
        t_env.execute_sql(statement)
    # Run the inserts as one job from a compiled plan: stable operator ids let a savepoint from the
    # previous submission (`flink run -s`) be restored.
    compile_sql, execute_sql = plan_statements(cfg)
    t_env.execute_sql(compile_sql)
    result = t_env.execute_sql(execute_sql)
    print(f"submitted {cfg.job_name}: {result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
