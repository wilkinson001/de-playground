SHELL := /bin/bash
COMPOSE := docker compose
PROFILES ?= --profile core --profile spark --profile clickhouse
RUN := $(COMPOSE) $(PROFILES) --profile tools run --rm --no-deps runner

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
	$(COMPOSE) $(PROFILES) up -d --wait

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
	@if [ -n "$$($(COMPOSE) $(PROFILES) ps --status running -q clickhouse)" ]; then \
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
