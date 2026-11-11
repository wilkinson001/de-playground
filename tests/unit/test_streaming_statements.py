import ast
import sys
from pathlib import Path

from de_playground.streaming.flink import statements

CAT = statements.iceberg_catalog_ddl("http://lakekeeper:8181", "lakehouse", "http://seaweedfs:8333", "AK", "SK")


def test_iceberg_catalog_ddl():
    assert CAT.startswith("CREATE CATALOG lakehouse WITH (")
    for part in ["'type' = 'iceberg'", "'catalog-type' = 'rest'", "'uri' = 'http://lakekeeper:8181/catalog'",
                 "'warehouse' = 'lakehouse'", "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'",
                 "'s3.endpoint' = 'http://seaweedfs:8333'", "'s3.path-style-access' = 'true'",
                 "'client.region' = 'us-east-1'", "'s3.access-key-id' = 'AK'", "'s3.secret-access-key' = 'SK'"]:
        assert part in CAT, part


def test_delta_options():
    # The released connector (4.4.1) rejects per-table fs.* options: S3 settings come from the
    # image's Hadoop core-site.xml instead, so only the table itself is described here.
    opts = statements.delta_options("s3a://delta/ns_bronze/t", partitions="event_date", uid="delta-ns-bronze")
    assert opts == ("'connector' = 'delta',\n    'table_path' = 's3a://delta/ns_bronze/t',\n    "
                    "'partitions' = 'event_date',\n    'uid' = 'delta-ns-bronze'")
    assert "partitions" not in statements.delta_options("s3a://delta/x")
    assert "fs.s3a" not in opts


def test_statements_module_is_stdlib_only():
    tree = ast.parse(Path(statements.__file__).read_text())
    modules = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    modules |= {node.module.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module}
    assert modules - set(sys.stdlib_module_names) - {"pyflink", "__future__"} == set()


ENV = {"S3_ACCESS_KEY": "AK", "S3_SECRET_KEY": "SK"}


def _sql(cfg) -> str:
    setup, inserts = statements.build_statements(cfg)
    return "\n".join(setup + inserts)


def test_job_config_defaults_match_topics_module():
    from de_playground.streaming import topics

    cfg = statements.JobConfig.from_env(ENV)
    assert cfg.namespace == "flink_stream"
    assert cfg.raw_topic == topics.RAW_TOPIC
    assert cfg.agg_topic == topics.AGG_TOPICS["flink"]
    assert (cfg.kafka_bootstrap, cfg.group_id, cfg.checkpoint_interval, cfg.watermark_delay_s, cfg.job_name) == (
        "kafka:29092", "flink-wikimedia", "30s", 30, "wikimedia-streaming")
    assert (cfg.access_key, cfg.secret_key) == ("AK", "SK")


def test_job_config_env_overrides():
    cfg = statements.JobConfig.from_env({**ENV, "STREAM_NAMESPACE": "t_ns", "STREAM_RAW_TOPIC": "r",
                                         "STREAM_AGG_TOPIC": "a", "STREAM_GROUP_ID": "g",
                                         "STREAM_CHECKPOINT_INTERVAL": "5s", "STREAM_JOB_NAME": "j"})
    assert (cfg.namespace, cfg.raw_topic, cfg.agg_topic, cfg.group_id, cfg.checkpoint_interval, cfg.job_name) == (
        "t_ns", "r", "a", "g", "5s", "j")


def test_build_statements_tables_and_watermark():
    sql = _sql(statements.JobConfig.from_env(ENV))
    assert "WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND" in sql
    assert "TUMBLE(TABLE recentchange_src, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)" in sql
    assert "'scan.startup.mode' = 'group-offsets'" in sql
    assert "'properties.auto.offset.reset' = 'earliest'" in sql
    for target in ["lakehouse.flink_stream_bronze.recentchange", "lakehouse.flink_stream_gold.edits_per_minute",
                   "'table_path' = 's3a://delta/flink_stream_bronze/recentchange'",
                   "'table_path' = 's3a://delta/flink_stream_gold/edits_per_minute'",
                   "'topic' = 'wikimedia.edits_per_minute.flink'"]:
        assert target in sql, target


def test_build_statements_inserts_one_per_sink():
    _, inserts = statements.build_statements(statements.JobConfig.from_env(ENV))
    assert len(inserts) == 5
    assert all(i.startswith("INSERT INTO ") for i in inserts)


def test_build_statements_respects_namespace_and_topics():
    cfg = statements.JobConfig.from_env({**ENV, "STREAM_NAMESPACE": "itest_x", "STREAM_RAW_TOPIC": "raw.t",
                                         "STREAM_AGG_TOPIC": "agg.t"})
    sql = _sql(cfg)
    assert "lakehouse.itest_x_bronze.recentchange" in sql and "'topic' = 'raw.t'" in sql and "'topic' = 'agg.t'" in sql
    assert "flink_stream" not in sql and "wikimedia." not in sql


def test_delta_sinks_use_microsecond_timestamps():
    # delta-flink 4.4.1 misreads Flink's compact millisecond timestamps (precision <= 3) and fails
    # every checkpoint; Delta stores microseconds anyway, so its sinks use precision 6.
    setup, inserts = statements.build_statements(statements.JobConfig.from_env(ENV))
    delta_ddl = [s for s in setup if s.startswith("CREATE TEMPORARY TABLE delta_")]
    assert len(delta_ddl) == 2
    assert all("(3)" not in ddl for ddl in delta_ddl)
    assert "`event_ts` TIMESTAMP_LTZ(6)" in delta_ddl[0] and "`window_start` TIMESTAMP(6)" in delta_ddl[1]
    delta_inserts = [i for i in inserts if i.startswith("INSERT INTO delta_")]
    assert "CAST(event_ts AS TIMESTAMP_LTZ(6))" in delta_inserts[0]
    assert "CAST(`window_start` AS TIMESTAMP(6))" in delta_inserts[1]


def test_source_marks_quiet_partitions_idle():
    # Per-partition (split) idleness belongs on the source itself: the job-wide
    # table.exec.source.idle-timeout didn't free the watermark when one source task reads all
    # partitions, so a quiet partition held windows open forever.
    sql = _sql(statements.JobConfig.from_env(ENV))
    assert "'scan.watermark.idle-timeout' = '10s'" in sql


def test_flink_modules_compile_on_the_flink_images_python():
    # The Flink image has Python 3.10 (Ubuntu 22.04); this repo runs 3.12, which accepts syntax
    # 3.10 doesn't (e.g. comments inside f-string expressions, PEP 701). Compile with a real 3.10.
    import shutil
    import subprocess

    import pytest

    if shutil.which("uv") is None:
        pytest.skip("uv not available to fetch Python 3.10")
    flink_dir = Path(statements.__file__).parent
    files = [str(p) for p in sorted(flink_dir.glob("*.py"))]
    result = subprocess.run(
        ["uv", "run", "--no-project", "--python", "3.10", "python", "-m", "py_compile", *files],
        capture_output=True, text=True, timeout=300,
    )
    assert result.returncode == 0, result.stderr


def test_plan_statements_use_one_stable_plan_file():
    # Operator ids must be the same across submissions so a savepoint can be restored: a compiled
    # plan fixes them (the SQL Gateway's long-lived JVM would otherwise number operators afresh).
    cfg = statements.JobConfig.from_env(ENV)
    compile_sql, execute_sql = statements.plan_statements(cfg)
    path = execute_sql.removeprefix("EXECUTE PLAN '").removesuffix("'")
    assert path.startswith("file:///opt/flink/checkpoints/plans/wikimedia-streaming-") and path.endswith(".json")
    assert compile_sql.startswith(f"COMPILE PLAN '{path}' IF NOT EXISTS FOR STATEMENT SET BEGIN")
    _, inserts = statements.build_statements(cfg)
    assert all(i in compile_sql for i in inserts) and compile_sql.endswith("END")
    assert statements.plan_statements(cfg) == (compile_sql, execute_sql)  # deterministic


def test_changed_sql_gets_a_new_plan_file():
    a = statements.plan_statements(statements.JobConfig.from_env(ENV))[1]
    b = statements.plan_statements(statements.JobConfig.from_env({**ENV, "STREAM_NAMESPACE": "other"}))[1]
    assert a != b


def test_delta_sinks_have_stable_uids():
    # Without a fixed uid the Delta sink's operators get new ids each submission, and a savepoint
    # can't be restored ("operator is not available in the new program").
    setup, _ = statements.build_statements(statements.JobConfig.from_env(ENV))
    delta_ddl = [st for st in setup if st.startswith("CREATE TEMPORARY TABLE delta_")]
    assert "'uid' = 'flink_stream-delta-bronze'" in delta_ddl[0]
    assert "'uid' = 'flink_stream-delta-gold'" in delta_ddl[1]
