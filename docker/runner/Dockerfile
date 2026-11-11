# Python environment for pipelines, bootstrap and integration tests, on the Compose network.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /usr/local/bin/

# Keep the venv outside /workspace so the host's macOS .venv (mounted with the repo) is never used.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /workspace
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Pre-install DuckDB extensions so pipeline runs never download them.
RUN /opt/venv/bin/python -c "import duckdb; [duckdb.sql(f'INSTALL {e}') for e in ('ducklake','postgres','httpfs','iceberg','delta','avro')]"
