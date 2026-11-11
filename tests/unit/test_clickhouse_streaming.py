from de_playground.streaming import clickhouse_streaming, topics


def test_statements_shape():
    stmts = clickhouse_streaming.statements(topics.RAW_TOPIC, topics.AGG_TOPICS)
    assert len(stmts) == 8
    assert sum("ENGINE = Kafka" in s for s in stmts) == 3
    assert sum("ENGINE = ReplacingMergeTree" in s for s in stmts) == 2
    assert all("IF NOT EXISTS" in s for s in stmts)
    text = "\n".join(stmts)
    for topic in (topics.RAW_TOPIC, *topics.AGG_TOPICS.values()):
        assert f"kafka_topic_list = '{topic}'" in text
    assert "'flink' AS engine" in text and "'spark' AS engine" in text
    assert "ORDER BY (engine, wiki, window_start)" in text
    assert "ORDER BY (wiki, event_id)" in text


def test_targets_created_before_their_views():
    stmts = clickhouse_streaming.statements("raw", {"flink": "a"}, database="db")
    order = [s.split("(")[0].split(" AS ")[0] for s in stmts]
    assert stmts.index(next(s for s in stmts if "TABLE IF NOT EXISTS db.recentchange " in s)) < stmts.index(
        next(s for s in stmts if "VIEW IF NOT EXISTS db.recentchange_mv" in s)), order
    assert all(s.find("db.") > 0 for s in stmts)
