"""Per-caller quotas: concurrency cap, bounded queue, rate limit, isolation."""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import GatewayError
from gateway.models import QuotaConfig

log = logging.getLogger("gateway.test.quotas")


@pytest.mark.asyncio
async def test_concurrency_never_exceeds_limit_under_burst(make_executor):
    cap = 3
    ex = make_executor(max_concurrency=cap, queue_capacity=50, rate_per_second=float("inf"))
    await ex.start()
    observed = []
    gate = asyncio.Event()
    seen = 0

    async def tracked(payload, event):
        nonlocal seen
        seen += 1
        observed.append(ex._quotas.get("c").running)
        await gate.wait()
        return seen

    ex._registry.register("tracked", tracked)
    n = 25
    log.info("submitting %s tasks with concurrency cap=%s", n, cap)
    results = await asyncio.gather(*[
        ex.submit(caller="c", task_type="tracked", payload={}) for _ in range(n)
    ])
    # Wait until the cap number of tasks are running simultaneously.
    await asyncio.sleep(0.1)
    running_now = ex._quotas.get("c").running
    log.info("judgement: running_now=%s cap=%s (must be <=)", running_now, cap)
    assert running_now == cap
    assert max(observed) <= cap
    gate.set()
    await asyncio.gather(*[ex.wait_terminal(r.task.task_id) for r in results])
    assert max(observed) == cap
    assert all(t.state.value == "SUCCEEDED" for t in
               [await ex.get_task(r.task.task_id) for r in results])
    log.info("all %s tasks succeeded; observed max concurrency=%s", n, max(observed))
    await ex.shutdown()


@pytest.mark.asyncio
async def test_concurrency_rejected_with_distinct_code_when_queue_disabled(make_executor):
    ex = make_executor(max_concurrency=2, queue_capacity=0)
    await ex.start()
    gate = asyncio.Event()

    async def block(payload, event):
        await gate.wait()
        return True

    ex._registry.register("block", block)
    accepted = await asyncio.gather(*[
        ex.submit(caller="c", task_type="block", payload={}) for _ in range(2)
    ])
    await asyncio.sleep(0.05)
    log.info("2 slots occupied; third submit must be rejected, not piled up")
    with pytest.raises(GatewayError) as ei:
        await ex.submit(caller="c", task_type="block", payload={})
    log.info("judgement: code=%s (expect CONCURRENCY_LIMIT_REJECTED)", ei.value.code)
    assert ei.value.code == "CONCURRENCY_LIMIT_REJECTED"
    gate.set()
    await asyncio.gather(*[ex.wait_terminal(r.task.task_id) for r in accepted])
    await ex.shutdown()


@pytest.mark.asyncio
async def test_bounded_queue_overflow_returns_queue_full(make_executor):
    # cap 1 + queue 2 -> at most 3 admitted; 4th is QUEUE_FULL_REJECTED
    ex = make_executor(max_concurrency=1, queue_capacity=2)
    await ex.start()
    gate = asyncio.Event()

    async def block(payload, event):
        await gate.wait()
        return True

    ex._registry.register("block", block)
    admitted = []
    for i in range(3):
        admitted.append(await ex.submit(caller="c", task_type="block", payload={}))
    assert ex._quotas.get("c").inflight == 3
    log.info("inflight=3 reached cap+queue; next submit must fail fast")
    with pytest.raises(GatewayError) as ei:
        await ex.submit(caller="c", task_type="block", payload={})
    log.info("judgement: code=%s (expect QUEUE_FULL_REJECTED)", ei.value.code)
    assert ei.value.code == "QUEUE_FULL_REJECTED"
    gate.set()
    await asyncio.gather(*[ex.wait_terminal(r.task.task_id) for r in admitted])
    assert ex._quotas.get("c").inflight == 0
    await ex.shutdown()


@pytest.mark.asyncio
async def test_queued_tasks_execute_in_fifo_order(make_executor):
    ex = make_executor(max_concurrency=1, queue_capacity=10)
    await ex.start()
    order = []

    async def numbered(payload, event):
        order.append(payload["i"])
        await asyncio.sleep(0.005)
        return payload["i"]

    ex._registry.register("numbered", numbered)
    results = await asyncio.gather(*[
        ex.submit(caller="c", task_type="numbered", payload={"i": i}) for i in range(6)
    ])
    await asyncio.gather(*[ex.wait_terminal(r.task.task_id) for r in results])
    log.info("judgement: execution order=%s (expect 0..5)", order)
    assert order == [0, 1, 2, 3, 4, 5]
    await ex.shutdown()


@pytest.mark.asyncio
async def test_rate_limit_rejects_burst_with_retry_after(make_executor):
    ex = make_executor(rate_per_second=2.0, max_concurrency=100, queue_capacity=100)
    await ex.start()
    ex._quotas.get("c").check_rate()
    ex._quotas.get("c").check_rate()
    log.info("2/s bucket drained by 2 submits; third must be rate-limited")
    with pytest.raises(GatewayError) as ei:
        await ex.submit(caller="c", task_type="echo", payload={})
    log.info("judgement: code=%s retry_after=%.3f", ei.value.code, ei.value.retry_after)
    assert ei.value.code == "RATE_LIMIT_REJECTED"
    assert ei.value.retry_after > 0

    # Tokens refill: after ~0.6s at 2/s at least one submit token exists.
    await asyncio.sleep(0.6)
    r = await ex.submit(caller="c", task_type="echo", payload={})
    await ex.wait_terminal(r.task.task_id)
    await ex.shutdown()


@pytest.mark.asyncio
async def test_idempotent_reuse_does_not_consume_rate_tokens(make_executor):
    ex = make_executor(rate_per_second=1.0, max_concurrency=10, queue_capacity=10)
    await ex.start()
    await ex.submit(caller="c", task_type="echo", payload={}, idempotency_key="rk")
    await asyncio.sleep(0.02)
    log.info("bucket now holds <1 new token; duplicate key submits must still pass")
    for _ in range(5):
        r = await ex.submit(caller="c", task_type="echo", payload={},
                            idempotency_key="rk")
        assert r.reused is True
    log.info("five reuses admitted without rate rejection")
    await ex.shutdown()


@pytest.mark.asyncio
async def test_quotas_are_isolated_per_caller(make_executor):
    ex = make_executor(
        quotas={
            "a": QuotaConfig(max_concurrency=1, queue_capacity=0),
            "b": QuotaConfig(max_concurrency=1, queue_capacity=0),
        },
    )
    await ex.start()
    gate = asyncio.Event()

    async def block(payload, event):
        await gate.wait()
        return True

    ex._registry.register("block", block)
    ra = await ex.submit(caller="a", task_type="block", payload={})
    rb = await ex.submit(caller="b", task_type="block", payload={})
    await asyncio.sleep(0.05)
    log.info("caller a and b each hold their own slot; both run simultaneously")
    assert ex._quotas.get("a").running == 1
    assert ex._quotas.get("b").running == 1
    with pytest.raises(GatewayError) as ei:
        await ex.submit(caller="a", task_type="block", payload={})
    assert ei.value.code == "CONCURRENCY_LIMIT_REJECTED"
    gate.set()
    await asyncio.gather(ex.wait_terminal(ra.task.task_id),
                         ex.wait_terminal(rb.task.task_id))
    await ex.shutdown()
