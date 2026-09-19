FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY shared-contracts /app/shared-contracts
COPY services/content-service /app/services/content-service

WORKDIR /app/services/content-service
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim

ENV PATH="/app/services/content-service/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 crawler \
    && useradd --uid 10001 --gid crawler --create-home crawler

WORKDIR /app/services/content-service
COPY --from=builder --chown=crawler:crawler /app/services/content-service/.venv .venv

USER crawler
