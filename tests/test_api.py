"""HTTP contract tests via httpx in-process ASGI transport (no real sockets)."""

from __future__ import annotations

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from gateway.api import create_app
from gateway.handlers import build_default_registry
from gateway.idempotency import IdempotencyStore
from gateway.models import GatewayConfig, QuotaConfig, RetryPolicy
from gateway.quotas import QuotaManager

log = logging.getLogger("gateway.test.api")


@pytest.fixture
async def make_client(make_executor):
    async def _make(**kw):
        params = dict(max_concurrency=2, queue_capacity=1, rate_per_second=2.0,
                      max_attempts=2, base_delay=0.01)
        params.update(kw)
        executor = make_executor(**params)
        app = create_app(executor)

        async def lifespan_start():
            await executor.start()

        async def lifespan_stop():
            await executor.shutdown()

        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://t")
        await lifespan_start()
        return client, executor, lifespan_stop

    created = []

    async def factory(**kw):
        client, executor, stop = await _make(**kw)
        created.append((client, stop))
        return client, executor

    try:
        yield factory
    finally:
        for client, stop in created:
            await stop()
            await client.aclose()


@pytest.fixture
async def client(make_client):
    c, ex = await make_client()
    yield c, ex


@pytest.mark.asyncio
async def test_submit_get_and_succeed(client):
    c, _ = client
    resp = await c.post("/tasks", json={"task_type": "echo", "payload": {"v": 7}},
                        headers={"X-Caller": "alice", "X-Idempotency-Key": "k1"})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    log.info("submit -> %s reused=%s", body["state"], body["reused"])
    assert body["state"] in {"PENDING", "SCHEDULED", "RUNNING", "SUCCEEDED"}
    assert body["reused"] is False
    task_id = body["task_id"]

    # Idempotent replay over HTTP returns the same task.
    resp2 = await c.post("/tasks", json={"task_type": "echo", "payload": {"v": 7}},
                         headers={"X-Caller": "alice", "X-Idempotency-Key": "k1"})
    assert resp2.status_code == 201
    assert resp2.json()["task_id"] == task_id
    assert resp2.json()["reused"] is True

    for _ in range(30):
        got = await c.get(f"/tasks/{task_id}")
        if got.json()["state"] == "SUCCEEDED":
            break
        await asyncio.sleep(0.02)
    got = (await c.get(f"/tasks/{task_id}")).json()
    log.info("final view=%s", got)
    assert got["state"] == "SUCCEEDED"
    assert got["result"] == {"echo": {"v": 7}}


@pytest.mark.asyncio
async def test_conflict_and_quota_errors_are_distinguishable(client):
    c, _ = client
    # Bind a key.
    await c.post("/tasks", json={"task_type": "echo", "payload": {}},
                 headers={"X-Caller": "bob", "X-Idempotency-Key": "kk"})
    conflict = await c.post("/tasks", json={"task_type": "echo", "payload": {"x": 1}},
                            headers={"X-Caller": "bob", "X-Idempotency-Key": "kk"})
    log.info("conflict status=%s body=%s", conflict.status_code, conflict.json())
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # Key conflicts are designed NOT to consume submit tokens; spend the
    # 2-token burst on two genuinely new submits, then the third is limited.
    await c.post("/tasks", json={"task_type": "echo", "payload": {"a": 1}},
                 headers={"X-Caller": "bob"})
    await c.post("/tasks", json={"task_type": "echo", "payload": {"a": 2}},
                 headers={"X-Caller": "bob"})
    limited = await c.post("/tasks", json={"task_type": "echo", "payload": {}},
                           headers={"X-Caller": "bob"})
    log.info("rate limit status=%s body=%s", limited.status_code, limited.json())
    assert limited.status_code == 429
    assert limited.json()["error"]["code"] == "RATE_LIMIT_REJECTED"
    assert "retry_after" in limited.json()["error"]


@pytest.mark.asyncio
async def test_concurrency_and_queue_full_over_http(make_client):
    # No rate limit so the queue/concurrency rejection is the only gate.
    c, ex = await make_client(max_concurrency=2, queue_capacity=1,
                              rate_per_second=float("inf"))
    submitted = []
    for _ in range(3):  # cap 2 running + 1 queued
        r = await c.post("/tasks", json={"task_type": "sleep", "payload": {"seconds": 30}},
                         headers={"X-Caller": "carol"})
        assert r.status_code == 201, r.text
        submitted.append(r.json()["task_id"])
    await asyncio.sleep(0.1)
    full = await c.post("/tasks", json={"task_type": "sleep", "payload": {"seconds": 30}},
                        headers={"X-Caller": "carol"})
    log.info("4th submit status=%s body=%s", full.status_code, full.json())
    assert full.status_code == 429
    assert full.json()["error"]["code"] == "QUEUE_FULL_REJECTED"
    # Cancel so the managed shutdown stays quick and deterministic.
    for tid in submitted:
        await c.post(f"/tasks/{tid}/cancel")
    for tid in submitted:
        await ex.wait_terminal(tid)


@pytest.mark.asyncio
async def test_cancel_and_404_and_unknown_type(client):
    c, _ = client
    r = await c.post("/tasks", json={"task_type": "sleep", "payload": {"seconds": 30}},
                     headers={"X-Caller": "dave"})
    tid = r.json()["task_id"]
    await asyncio.sleep(0.1)
    cancelled = await c.post(f"/tasks/{tid}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["state"] == "RUNNING"

    for _ in range(30):
        got = (await c.get(f"/tasks/{tid}")).json()
        if got["state"] == "CANCELLED":
            break
        await asyncio.sleep(0.02)
    assert got["state"] == "CANCELLED"
    log.info("cancelled view=%s", got)

    again = await c.post(f"/tasks/{tid}/cancel")
    assert again.status_code == 409
    assert again.json()["error"]["code"] == "TASK_NOT_CANCELLABLE"

    missing = await c.get("/tasks/nope")
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "TASK_NOT_FOUND"

    bad_type = await c.post("/tasks", json={"task_type": "ghost", "payload": {}},
                            headers={"X-Caller": "dave"})
    assert bad_type.status_code == 400
    assert bad_type.json()["error"]["code"] == "UNKNOWN_TASK_TYPE"


@pytest.mark.asyncio
async def test_healthz(client):
    c, _ = client
    resp = await c.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    log.info("healthz=%s", resp.json())
