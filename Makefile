SHELL := /bin/bash
COMPOSE := docker compose
# Every profile, so the runner (started with --no-deps) can resolve any service it talks to.
ALL_PROFILES := --profile core --profile spark --profile clickhouse --profile streaming --profile spark-stream --profile ui
BATCH_PROFILES := --profile core --profile spark --profile clickhouse
STREAM_PROFILES := --profile core --profile streaming --profile clickhouse
RUN := $(COMPOSE) $(ALL_PROFILES) --profile tools run --rm --no-deps runner

.PHONY: help env test

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

env: ## Check .env exists
	@test -f .env || { echo "Missing .env - run: cp .env.example .env"; exit 1; }

test: ## Unit tests (host, no services needed)
	uv run pytest

.PHONY: up down nuke ps logs shell test-integration
up: env ## Start core services (postgres, seaweedfs, lakekeeper)
	$(COMPOSE) --profile core up -d --wait

up-batch: env ## Start core + Spark + ClickHouse (batch exercises)
	$(COMPOSE) $(BATCH_PROFILES) up -d --wait

up-stream: env ## Start core + Kafka + Flink + ClickHouse (streaming exercises)
	$(COMPOSE) $(STREAM_PROFILES) up -d --wait

down: ## Stop all services (keeps data)
	$(COMPOSE) --profile '*' down

nuke: ## Stop all services and DELETE all data volumes
	$(COMPOSE) --profile '*' down -v

ps: ## Show running services
	$(COMPOSE) --profile '*' ps

logs: ## Tail logs (S=service to filter)
	$(COMPOSE) --profile '*' logs -f $(S)

shell: env ## Shell in the runner container
	$(RUN) bash

test-integration: env ## Integration tests against the running stack
	$(RUN) uv run pytest -m integration $(ARGS)

.PHONY: bootstrap
bootstrap: env ## Create buckets + Lakekeeper warehouse (+ ClickHouse databases if running); safe to re-run
	$(RUN) uv run python -m de_playground bootstrap
	@if [ -n "$$($(COMPOSE) $(ALL_PROFILES) ps --status running -q clickhouse)" ]; then \
		$(RUN) uv run python -m de_playground clickhouse-setup; fi

MONTHS ?= 2025-01..2025-03
.PHONY: download
download: env ## Download TLC months into the raw bucket (MONTHS=2025-01..2025-03)
	$(RUN) uv run python -m de_playground download --months $(MONTHS)

TARGET ?= all
NS ?= manual
.PHONY: batch
batch: download ## Run the medallion pipeline (TARGET=all|duckdb:ducklake|spark:iceberg|spark:delta MONTHS=... NS=manual)
	$(RUN) uv run python -m de_playground.batch run --target $(TARGET) --months $(MONTHS) --namespace $(NS)
	@if [ -n "$$($(COMPOSE) $(ALL_PROFILES) ps --status running -q clickhouse)" ]; then \
		$(RUN) uv run python -m de_playground clickhouse-setup --namespace $(NS); fi

.PHONY: matrix
matrix: env ## Probe every engine x table format and write docs/engine-format-matrix.md
	$(RUN) uv run python -m de_playground.matrix

.PHONY: stream-setup
stream-setup: env ## Create Kafka topics (+ ClickHouse streaming tables); safe to re-run
	$(RUN) uv run python -m de_playground.streaming setup
	@if [ -n "$$($(COMPOSE) $(ALL_PROFILES) ps --status running -q clickhouse)" ]; then \
		$(RUN) uv run python -m de_playground.streaming clickhouse-setup; fi

FLINK_JOB := /opt/playground/src/de_playground/streaming/flink/job.py
# Written by `job-stop` (via the runner, into the repo's .data/), read once by `stream-job`.
FLINK_SAVEPOINT := .data/flink/savepoints/wikimedia-streaming
.PHONY: stream-job stream-status stream-stop stream-cancel
stream-job: env ## (Re)submit the PyFlink streaming job, resuming the previous one's state from a savepoint
	$(RUN) uv run python -m de_playground.streaming job-stop
	@SP=$$(cat $(FLINK_SAVEPOINT) 2>/dev/null); rm -f $(FLINK_SAVEPOINT); \
	  if [ -n "$$SP" ]; then echo "resuming from $$SP"; fi; \
	  $(COMPOSE) $(STREAM_PROFILES) exec flink-jobmanager /opt/flink/bin/flink run -d $${SP:+-s $$SP} -py $(FLINK_JOB)

stream-status: env ## Show the Flink streaming job
	$(RUN) uv run python -m de_playground.streaming job-status

stream-stop: env ## Stop the Flink streaming job with a savepoint (make stream-job resumes it)
	$(RUN) uv run python -m de_playground.streaming job-stop

stream-cancel: env ## Cancel the Flink streaming job WITHOUT a savepoint (its window state is lost)
	$(RUN) uv run python -m de_playground.streaming job-cancel

SPARK_STREAM_PROFILES := $(STREAM_PROFILES) --profile spark --profile spark-stream
.PHONY: stream-spark stream-spark-stop
stream-spark: env ## Start Spark + the Spark Structured Streaming queries (alongside the streaming stack)
	$(COMPOSE) $(SPARK_STREAM_PROFILES) up -d --wait spark-connect spark-stream

stream-spark-stop: env ## Stop the Spark Structured Streaming queries
	$(COMPOSE) $(SPARK_STREAM_PROFILES) stop spark-stream

.PHONY: duckdb-ui
duckdb-ui: env ## DuckDB web UI in Docker (http://localhost:4213) over DuckLake, Iceberg and Delta
	$(COMPOSE) --profile core --profile ui up -d --wait duckdb-ui
	@echo "DuckDB UI: http://localhost:4213  (make duckdb-ui-stop to stop)"

.PHONY: duckdb-ui-stop duckdb-ui-local
duckdb-ui-stop: ## Stop the DuckDB UI container
	$(COMPOSE) --profile core --profile ui stop duckdb-ui

duckdb-ui-local: env ## The same DuckDB UI, run directly on the host instead of in Docker
	set -a && . ./.env && set +a && uv run python -m de_playground.duckdb_ui

PIPELINE_SERVICES := duckdb-ui producer flink-jobmanager flink-taskmanager flink-sql-gateway spark-stream spark-connect kafka-ui kafka clickhouse
.PHONY: pipelines-down
pipelines-down: ## Stop every engine/pipeline service but keep the data lake (postgres, seaweedfs, lakekeeper) up
	@if [ -n "$$($(COMPOSE) $(ALL_PROFILES) ps --status running -q flink-jobmanager)" ]; then \
		$(RUN) uv run python -m de_playground.streaming job-stop; fi
	$(COMPOSE) $(ALL_PROFILES) stop $(PIPELINE_SERVICES)
