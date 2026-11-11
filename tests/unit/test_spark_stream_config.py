from de_playground.streaming import spark_stream, topics

ENV = {"S3_ACCESS_KEY": "AK", "S3_SECRET_KEY": "SK", "POSTGRES_USER": "u", "POSTGRES_PASSWORD": "p"}


def test_spark_stream_config_defaults():
    cfg = spark_stream.SparkStreamConfig.from_env(ENV)
    assert cfg.namespace == "spark_stream"
    assert cfg.raw_topic == topics.RAW_TOPIC
    assert cfg.agg_topic == topics.AGG_TOPICS["spark"]
    assert (cfg.kafka_bootstrap, cfg.checkpoint_root, cfg.watermark, cfg.trigger, cfg.starting_offsets) == (
        "kafka:29092", "/opt/spark/checkpoints", "30 seconds", "10 seconds", "earliest")


def test_spark_stream_config_env_overrides():
    cfg = spark_stream.SparkStreamConfig.from_env({**ENV, "STREAM_NAMESPACE": "t", "STREAM_RAW_TOPIC": "r",
                                                   "STREAM_AGG_TOPIC": "a", "STREAM_TRIGGER": "2 seconds",
                                                   "STREAM_CHECKPOINT_ROOT": "/tmp/cp"})
    assert (cfg.namespace, cfg.raw_topic, cfg.agg_topic, cfg.trigger, cfg.checkpoint_root) == (
        "t", "r", "a", "2 seconds", "/tmp/cp")


def test_query_names_and_checkpoints():
    cfg = spark_stream.SparkStreamConfig.from_env(ENV)
    assert spark_stream.query_names(cfg) == [
        "spark_stream-bronze-iceberg", "spark_stream-bronze-delta",
        "spark_stream-gold-iceberg", "spark_stream-gold-delta", "spark_stream-gold-kafka",
    ]
    assert spark_stream.checkpoint_path(cfg, "spark_stream-gold-kafka") == (
        "/opt/spark/checkpoints/spark_stream/spark_stream-gold-kafka")


def test_bronze_and_gold_columns_match_flink():
    from de_playground.streaming.events import RECORD_FIELDS
    from de_playground.streaming.flink import statements

    assert spark_stream.BRONZE_COLUMNS == [*RECORD_FIELDS, "event_ts", "event_date"]
    assert spark_stream.GOLD_COLUMNS == [name for name, _ in statements._GOLD_COLUMNS]
