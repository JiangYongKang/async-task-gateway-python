"""Idempotency: reuse, content conflict, expiry and concurrent dedup."""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import GatewayError
from gateway.handlers import echo_handler
from gateway.idempotency import IdempotencyConflict, IdempotencyStore

log = logging.getLogger("gateway.test.idem")


@pytest.mark.asyncio
async def test_duplicate_submit_returns_same_task_and_executes_once(make_executor):
    ex = make_executor()
    await ex.start()
    log.info("key=dup-1: submit echo twice with identical content")
    r1 = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                         idempotency_key="dup-1")
    t1 = await ex.wait_terminal(r1.task.task_id)

    r2 = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                         idempotency_key="dup-1")
    log.info("judgement: reused=%s same_task=%s", r2.reused,
             r2.task.task_id == r1.task.task_id)

    assert r1.reused is False
    assert r2.reused is True
    assert r2.task.task_id == r1.task.task_id
    assert t1.state.value == "SUCCEEDED"

    # The handler registry can prove execution count if we wrap the handler.
    counts = {"n": 0}

    async def counting(payload, event):
        counts["n"] += 1
        return {"n": counts["n"]}

    ex._registry.register("counting", counting)
    for _ in range(5):
        await ex.submit(caller="c", task_type="counting", payload={},
                        idempotency_key="dup-count")
    # Allow processing, then all five map to the one task.
    task = await ex.wait_terminal(
        (await ex.submit(caller="c", task_type="counting", payload={},
                         idempotency_key="dup-count")).task.task_id)
    log.info("judgement: six identical submits -> executions=%s", counts["n"])
    assert task.state.value == "SUCCEEDED"
    assert counts["n"] == 1

    await ex.shutdown()


@pytest.mark.asyncio
async def test_same_key_different_payload_is_conflict(make_executor):
    ex = make_executor()
    await ex.start()
    r = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                        idempotency_key="k")
    await ex.wait_terminal(r.task.task_id)

    log.info("same key k with different payload must be rejected, not silently reused")
    with pytest.raises(IdempotencyConflict) as ei:
        await ex.submit(caller="c", task_type="echo", payload={"v": 2},
                        idempotency_key="k")
    log.info("judgement: error_code=%s existing=%s",
             ei.value.code, ei.value.existing_task_id)
    assert ei.value.code == "IDEMPOTENCY_CONFLICT"
    assert ei.value.existing_task_id == r.task.task_id

    # Different task_type with same key is also a content conflict.
    with pytest.raises(IdempotencyConflict):
        await ex.submit(caller="c", task_type="fail", payload={},
                        idempotency_key="k")

    # The original task must remain untouched.
    unchanged = await ex.get_task(r.task.task_id)
    assert unchanged.state.value == "SUCCEEDED"
    assert unchanged.result == {"echo": {"v": 1}}
    await ex.shutdown()


@pytest.mark.asyncio
async def test_idempotency_window_expiry_allows_reuse(make_executor):
    ex = make_executor(idempotency_ttl=0.05)
    await ex.start()
    r1 = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                         idempotency_key="exp")
    await ex.wait_terminal(r1.task.task_id)
    log.info("key=exp submitted task=%s, sleeping past ttl=0.05s", r1.task.task_id)
    await asyncio.sleep(0.08)

    r2 = await ex.submit(caller="c", task_type="echo", payload={"v": 9},
                         idempotency_key="exp")
    log.info("judgement after expiry: reused=%s new_task=%s",
             r2.reused, r2.task.task_id != r1.task.task_id)
    assert r2.reused is False
    assert r2.task.task_id != r1.task.task_id
    await ex.wait_terminal(r2.task.task_id)
    assert r2.task.result == {"echo": {"v": 9}}
    await ex.shutdown()


@pytest.mark.asyncio
async def test_concurrent_identical_submits_dedup_to_one_execution(make_executor):
    ex = make_executor(max_concurrency=1, queue_capacity=0)
    await ex.start()
    started = asyncio.Event()
    release = asyncio.Event()
    counts = {"n": 0}

    async def gate(payload, event):
        counts["n"] += 1
        started.set()
        await release.wait()
        return {"done": True}

    ex._registry.register("gate", gate)

    async def submit_one():
        return await ex.submit(caller="c", task_type="gate", payload={},
                               idempotency_key="race")

    first = await submit_one()
    await started.wait()  # first execution occupies the single slot

    log.info("firing 20 concurrent identical submits against key=race")
    results = await asyncio.gather(*[submit_one() for _ in range(20)])
    release.set()
    await ex.wait_terminal(first.task.task_id)

    task_ids = {r.task.task_id for r in results} | {first.task.task_id}
    reused = [r for r in results if r.reused]
    log.info("judgement: unique_tasks=%s reused_results=%s executions=%s",
             len(task_ids), len(reused), counts["n"])
    assert task_ids == {first.task.task_id}
    assert len(reused) == 20
    assert counts["n"] == 1
    await ex.shutdown()


@pytest.mark.asyncio
async def test_store_expiry_and_reservation_semantics():
    t = {"now": 0.0}
    store = IdempotencyStore(ttl=10.0, clock=lambda: t["now"])
    await store.reserve("k", "t1", "h")
    r = await store.lookup("k")
    assert r.reused is True and r.entry.task_id == "t1"
    t["now"] = 11
    r = await store.lookup("k")
    assert r.reused is False  # expired -> may rebind
    await store.reserve("k", "t2", "h2")
    r = await store.lookup("k")
    assert r.reused is True and r.entry.task_id == "t2"
    log.info("store TTL semantics verified")
