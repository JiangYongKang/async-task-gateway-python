"""Shutdown semantics: drain within grace, force cancel beyond grace, no loss."""

from __future__ import annotations

import asyncio
import logging
import time

import pytest

from gateway.errors import GatewayError
from gateway.states import TaskState

log = logging.getLogger("gateway.test.shutdown")


@pytest.mark.asyncio
async def test_graceful_shutdown_lets_running_work_finish(make_executor):
    ex = make_executor(max_concurrency=2, shutdown_grace=5.0)
    await ex.start()

    async def quick(payload, event):
        await asyncio.sleep(0.1)
        return {"done": True}

    ex._registry.register("quick", quick)
    results = await asyncio.gather(*[
        ex.submit(caller="c", task_type="quick", payload={}) for _ in range(4)
    ])
    t0 = time.monotonic()
    await ex.shutdown()
    elapsed = time.monotonic() - t0
    states = [(await ex.get_task(r.task.task_id)).state for r in results]
    log.info("graceful shutdown took %.2fs; states=%s", elapsed,
             [s.value for s in states])
    assert elapsed < 1.0  # drained, did not wait out the whole grace
    assert all(s is TaskState.SUCCEEDED for s in states)
    assert ex.stats()["terminal"] == ex.stats()["tasks_total"]


@pytest.mark.asyncio
async def test_shutdown_force_cancels_tasks_exceeding_grace(make_executor):
    ex = make_executor(max_concurrency=2, shutdown_grace=0.2)
    await ex.start()
    r = await ex.submit(caller="c", task_type="sleep", payload={"seconds": 30})
    await asyncio.sleep(0.05)  # let it start running

    async def submit_during_drain():
        await asyncio.sleep(0.02)
        log.info("submitting during shutdown must be rejected as SHUTTING_DOWN")
        with pytest.raises(GatewayError) as ei:
            await ex.submit(caller="c", task_type="echo", payload={})
        return ei.value.code

    t0 = time.monotonic()
    reject_code, _ = await asyncio.gather(submit_during_drain(), ex.shutdown())
    elapsed = time.monotonic() - t0
    task = await ex.get_task(r.task.task_id)
    log.info("forced shutdown took %.2fs task_state=%s error=%s reject=%s",
             elapsed, task.state.value, task.error_code, reject_code)
    assert task.state is TaskState.CANCELLED
    assert task.error_code == "SHUTTING_DOWN"
    assert reject_code == "SHUTTING_DOWN"
    assert elapsed < 3.0
    assert ex.stats()["terminal"] == ex.stats()["tasks_total"]


@pytest.mark.asyncio
async def test_shutdown_cancels_queued_tasks_without_executing(make_executor):
    ex = make_executor(max_concurrency=1, queue_capacity=3, shutdown_grace=0.1)
    await ex.start()
    gate = asyncio.Event()
    executed = []

    async def block(payload, event):
        executed.append(payload["id"])
        await gate.wait()  # never released: only shutdown can clear it
        return True

    ex._registry.register("block", block)
    head = await ex.submit(caller="c", task_type="block", payload={"id": "head"})
    queued = await asyncio.gather(*[
        ex.submit(caller="c", task_type="block", payload={"id": f"q{i}"})
        for i in range(3)
    ])
    await asyncio.sleep(0.05)
    log.info("1 running + 3 queued; shutting down with 0.1s grace")
    await ex.shutdown()

    final_head = await ex.get_task(head.task.task_id)
    final_queued = [await ex.get_task(r.task.task_id) for r in queued]
    log.info("head=%s queued=%s executed=%s",
             final_head.state.value, [t.state.value for t in final_queued], executed)
    assert final_head.state is TaskState.CANCELLED
    assert all(t.state is TaskState.CANCELLED for t in final_queued)
    assert executed == ["head"]  # queued tasks never ran
    assert ex.stats()["terminal"] == ex.stats()["tasks_total"]


@pytest.mark.asyncio
async def test_shutdown_with_no_tasks_is_clean(make_executor):
    ex = make_executor(shutdown_grace=0.5)
    await ex.start()
    await ex.shutdown()
    log.info("stats=%s", ex.stats())
    assert ex.stats() == {"tasks_total": 0, "terminal": 0, "shutting_down": True}


@pytest.mark.asyncio
async def test_retries_in_flight_are_cancelled_on_shutdown(make_executor):
    ex = make_executor(max_attempts=5, base_delay=5.0, shutdown_grace=0.2)
    await ex.start()
    r = await ex.submit(caller="c", task_type="fail", payload={})
    # Enter the long RETRY_WAIT backoff, then shut down.
    for _ in range(50):
        await asyncio.sleep(0.02)
        if (await ex.get_task(r.task.task_id)).state is TaskState.RETRY_WAIT:
            break
    assert (await ex.get_task(r.task.task_id)).state is TaskState.RETRY_WAIT
    await ex.shutdown()
    task = await ex.get_task(r.task.task_id)
    log.info("retry-wait task after shutdown: state=%s attempt=%s",
             task.state.value, task.attempt)
    assert task.state is TaskState.CANCELLED
    assert task.attempt == 1  # no further attempt was scheduled
