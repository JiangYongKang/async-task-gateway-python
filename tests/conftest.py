"""Shared fixtures and test helpers.

Only the local event loop and existing dependencies are used; no network
sockets are opened (HTTP tests use FastAPI's in-process ASGI transport).
"""

from __future__ import annotations

import logging

import pytest

from gateway.executor import TaskExecutor
from gateway.handlers import build_default_registry, flaky_handler
from gateway.idempotency import IdempotencyStore
from gateway.models import GatewayConfig, QuotaConfig, RetryPolicy
from gateway.quotas import QuotaManager

log = logging.getLogger("gateway.test")


@pytest.fixture
def make_executor():
    """Factory so each test can choose quota/retry parameters."""
    started: list[TaskExecutor] = []

    def _make(
        *,
        max_concurrency: int = 2,
        queue_capacity: int = 4,
        rate_per_second: float = float("inf"),
        max_attempts: int = 3,
        base_delay: float = 0.01,
        idempotency_ttl: float = 300.0,
        shutdown_grace: float = 5.0,
        quotas: dict[str, QuotaConfig] | None = None,
        persistence_path: str | None = None,
        history_retention: float = 3600.0,
        retention_sweep_interval: float = 60.0,
        tombstone_retention: float = 86400.0,
    ) -> TaskExecutor:
        cfg = GatewayConfig(
            idempotency_ttl=idempotency_ttl,
            shutdown_grace=shutdown_grace,
            default_retry=RetryPolicy(
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=base_delay * 10,
                multiplier=2.0,
            ),
            default_quota=QuotaConfig(
                max_concurrency=max_concurrency,
                rate_per_second=rate_per_second,
                queue_capacity=queue_capacity,
            ),
            quotas=quotas or {},
            persistence_path=persistence_path,
            history_retention=history_retention,
            retention_sweep_interval=retention_sweep_interval,
            tombstone_retention=tombstone_retention,
        )
        persistence = None
        if persistence_path is not None:
            from gateway.persistence import SqlitePersistence
            persistence = SqlitePersistence(persistence_path)
        executor = TaskExecutor(
            config=cfg,
            quota_manager=QuotaManager(cfg.default_quota, cfg.quotas),
            idempotency_store=IdempotencyStore(idempotency_ttl, persistence=persistence),
            registry=build_default_registry(),
            persistence=persistence,
        )
        started.append(executor)
        return executor

    yield _make

    # No automatic shutdown: tests that care call shutdown() explicitly to
    # assert its semantics; leftover workers are torn down with the loop.
    for executor in started:
        if not executor.is_shutting_down:
            executor._shutting_down.set()


@pytest.fixture(autouse=True)
def reset_flaky():
    flaky_handler.__dict__["_counters"] = {}
    yield
