FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY shared-contracts /app/shared-contracts
COPY services/fetcher-service /app/services/fetcher-service

WORKDIR /app/services/fetcher-service
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim

ENV PATH="/app/services/fetcher-service/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 crawler \
    && useradd --uid 10001 --gid crawler --create-home crawler

WORKDIR /app/services/fetcher-service
COPY --from=builder --chown=crawler:crawler /app/services/fetcher-service/.venv .venv

USER crawler
