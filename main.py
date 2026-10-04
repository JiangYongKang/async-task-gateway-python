"""Service entrypoint: assemble the gateway and expose the FastAPI app.

Run locally:  uv run uvicorn main:app --reload
Configuration is built in :func:`build_app`; tests build their own
executors with tailored :class:`GatewayConfig` values.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from gateway.api import create_app
from gateway.executor import TaskExecutor
from gateway.handlers import build_default_registry
from gateway.idempotency import IdempotencyStore
from gateway.models import GatewayConfig, QuotaConfig, RetryPolicy
from gateway.quotas import QuotaManager


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


def build_config() -> GatewayConfig:
    # The default quota is deliberately small; override per caller via config.
    default_quota = QuotaConfig(
        max_concurrency=_env_int("GATEWAY_MAX_CONCURRENCY", 4),
        rate_per_second=_env_float("GATEWAY_RATE_PER_SECOND", 20.0),
        queue_capacity=_env_int("GATEWAY_QUEUE_CAPACITY", 16),
    )
    return GatewayConfig(
        idempotency_ttl=_env_float("GATEWAY_IDEMPOTENCY_TTL", 300.0),
        shutdown_grace=_env_float("GATEWAY_SHUTDOWN_GRACE", 10.0),
        default_retry=RetryPolicy(
            max_attempts=_env_int("GATEWAY_MAX_ATTEMPTS", 3),
            base_delay=_env_float("GATEWAY_RETRY_BASE_DELAY", 0.2),
            max_delay=_env_float("GATEWAY_RETRY_MAX_DELAY", 5.0),
            multiplier=2.0,
        ),
        default_quota=default_quota,
        persistence_path=os.environ.get("GATEWAY_PERSISTENCE_PATH") or None,
        history_retention=_env_float("GATEWAY_HISTORY_RETENTION", 3600.0),
        retention_sweep_interval=_env_float("GATEWAY_RETENTION_SWEEP_INTERVAL", 60.0),
        tombstone_retention=_env_float("GATEWAY_TOMBSTONE_RETENTION", 86400.0),
    )


def build_executor(config: GatewayConfig | None = None) -> TaskExecutor:
    config = config or build_config()
    persistence = None
    if config.persistence_path is not None:
        from gateway.persistence import SqlitePersistence
        persistence = SqlitePersistence(config.persistence_path)
    return TaskExecutor(
        config=config,
        quota_manager=QuotaManager(config.default_quota, config.quotas),
        idempotency_store=IdempotencyStore(config.idempotency_ttl,
                                           persistence=persistence),
        registry=build_default_registry(),
        persistence=persistence,
    )


def build_app() -> FastAPI:
    logging.basicConfig(
        level=os.environ.get("GATEWAY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    executor = build_executor()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await executor.start()
        try:
            yield
        finally:
            await executor.shutdown()

    app = create_app(executor)
    app.router.lifespan_context = lifespan
    return app


app = build_app()
