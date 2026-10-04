"""Restart recovery: persisted tasks and idempotency keys survive a crash.

A "restart" is simulated by ``halt()`` (stops processing without finalizing
records, like a killed process) followed by a brand-new executor opened on
the same storage path. Logs show the key inputs (task ids, keys, attempts)
and the recovery judgements.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import ErrorCode, GatewayError
from gateway.handlers import MapHandlerRegistry
from gateway.states import TaskState

log = logging.getLogger("gateway.test.persistence")


def _registry(**handlers) -> MapHandlerRegistry:
    registry = MapHandlerRegistry()
    for task_type, handler in handlers.items():
        registry.register(task_type, handler)
    return registry


async def _wait_state(executor, task_id: str, state: TaskState,
                      timeout: float = 2.0) -> None:
    async def _poll():
        while True:
            task = await executor.get_task(task_id)
            if task.state is state:
                return task
            await asyncio.sleep(0.005)

    return await asyncio.wait_for(_poll(), timeout)


@pytest.mark.asyncio
async def test_restart_resumes_pending_and_interrupted_tasks(make_executor, tmp_path):
    """A RUNNING task interrupted mid-attempt and a PENDING task both reach
    a terminal state after restart, with stable ids and attempt counts."""
    path = str(tmp_path / "state.db")
    entered = asyncio.Event()

    async def blocking(payload, cancel_event):
        entered.set()
        await asyncio.Event().wait()  # never returns before halt()

    async def quick(payload, cancel_event):
        quick.calls += 1
        return {"ok": True, "call": quick.calls}
    quick.calls = 0

    ex_a = make_executor(storage_path=path, max_concurrency=1, base_delay=0.01,
                         registry=_registry(block=blocking, quick=quick))
    await ex_a.start()
    t1 = (await ex_a.submit(caller="c", task_type="block", payload={})).task
    await asyncio.wait_for(entered.wait(), 2.0)
    t2 = (await ex_a.submit(caller="c", task_type="quick", payload={})).task
    log.info("pre-crash: t1=%s RUNNING attempt=1, t2=%s PENDING", t1.task_id, t2.task_id)
    await ex_a.halt()
    assert quick.calls == 0

    async def block_now_succeeds(payload, cancel_event):
        block_now_succeeds.calls += 1
        return {"recovered": True}
    block_now_succeeds.calls = 0

    ex_b = make_executor(storage_path=path, max_concurrency=1, base_delay=0.01,
                         registry=_registry(block=block_now_succeeds, quick=quick))
    await ex_b.start()
    r1 = await ex_b.wait_terminal(t1.task_id, timeout=5.0)
    r2 = await ex_b.wait_terminal(t2.task_id, timeout=5.0)
    log.info("post-restart: t1 state=%s attempt=%s; t2 state=%s attempt=%s",
             r1.state.value, r1.attempt, r2.state.value, r2.attempt)

    # t1's interrupted attempt counted; it resumed at attempt 2 and the
    # (new process) handler ran it exactly once more.
    assert r1.state is TaskState.SUCCEEDED
    assert r1.task_id == t1.task_id and r1.attempt == 2
    assert r1.result == {"recovered": True}
    assert block_now_succeeds.calls == 1
    # t2 never ran before the crash and ran exactly once after.
    assert r2.state is TaskState.SUCCEEDED and r2.attempt == 1
    assert quick.calls == 1
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_does_not_rerun_succeeded_task(make_executor, tmp_path):
    path = str(tmp_path / "state.db")

    async def counted(payload, cancel_event):
        counted.calls += 1
        return {"n": counted.calls}
    counted.calls = 0

    ex_a = make_executor(storage_path=path, registry=_registry(count=counted))
    await ex_a.start()
    task = (await ex_a.submit(caller="c", task_type="count", payload={})).task
    done = await ex_a.wait_terminal(task.task_id, timeout=5.0)
    assert done.state is TaskState.SUCCEEDED and done.result == {"n": 1}
    log.info("pre-crash: task=%s SUCCEEDED attempt=%s result=%s",
             task.task_id, done.attempt, done.result)
    await ex_a.halt()

    calls_before = counted.calls
    ex_b = make_executor(storage_path=path, registry=_registry(count=counted))
    await ex_b.start()
    view = await ex_b.get_task(task.task_id)
    log.info("post-restart: state=%s attempt=%s result=%s handler_calls=%s",
             view.state.value, view.attempt, view.result, counted.calls)
    assert view.state is TaskState.SUCCEEDED
    assert view.attempt == 1 and view.result == {"n": 1}
    assert counted.calls == calls_before  # never re-executed
    await asyncio.sleep(0.1)  # give any rogue resume a chance to run
    assert counted.calls == calls_before
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_preserves_idempotency_reuse_and_conflict(make_executor, tmp_path):
    path = str(tmp_path / "state.db")

    async def counted(payload, cancel_event):
        counted.calls += 1
        return {"echo": payload}
    counted.calls = 0

    ex_a = make_executor(storage_path=path, registry=_registry(echo=counted))
    await ex_a.start()
    first = await ex_a.submit(caller="c", task_type="echo",
                              payload={"v": 1}, idempotency_key="k1")
    await ex_a.wait_terminal(first.task.task_id, timeout=5.0)
    log.info("pre-crash: key=k1 -> task=%s", first.task.task_id)
    await ex_a.halt()

    ex_b = make_executor(storage_path=path, registry=_registry(echo=counted))
    await ex_b.start()
    calls_before = counted.calls

    same = await ex_b.submit(caller="c", task_type="echo",
                             payload={"v": 1}, idempotency_key="k1")
    log.info("post-restart same key+content: reused=%s task=%s",
             same.reused, same.task.task_id)
    assert same.reused is True
    assert same.task.task_id == first.task.task_id
    assert counted.calls == calls_before  # still executed only once

    with pytest.raises(GatewayError) as err:
        await ex_b.submit(caller="c", task_type="echo",
                          payload={"v": 2}, idempotency_key="k1")
    log.info("post-restart same key+other content: code=%s bound_task=%s",
             err.value.code, err.value.existing_task_id)
    assert err.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert err.value.existing_task_id == first.task.task_id
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_idempotency_window_expiry_allows_rebind(make_executor, tmp_path):
    path = str(tmp_path / "state.db")
    ex_a = make_executor(storage_path=path, idempotency_ttl=0.15)
    await ex_a.start()
    first = await ex_a.submit(caller="c", task_type="echo",
                              payload={"v": 1}, idempotency_key="k2")
    await ex_a.wait_terminal(first.task.task_id, timeout=5.0)
    await ex_a.halt()

    await asyncio.sleep(0.2)  # let the TTL window expire while "down"
    ex_b = make_executor(storage_path=path, idempotency_ttl=0.15)
    await ex_b.start()
    rebound = await ex_b.submit(caller="c", task_type="echo",
                                payload={"v": 1}, idempotency_key="k2")
    log.info("expired key rebound: old_task=%s new_task=%s reused=%s",
             first.task.task_id, rebound.task.task_id, rebound.reused)
    assert rebound.reused is False
    assert rebound.task.task_id != first.task.task_id
    # The old record is still queryable (retention has not elapsed).
    old = await ex_b.get_task(first.task.task_id)
    assert old.state is TaskState.SUCCEEDED
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_resumes_retry_wait_task(make_executor, tmp_path):
    """A task halted during retry backoff resumes and finishes after restart."""
    path = str(tmp_path / "state.db")
    ex_a = make_executor(storage_path=path, max_attempts=3, base_delay=0.3)
    await ex_a.start()
    task = (await ex_a.submit(caller="c", task_type="flaky",
                              payload={"key": "rw", "succeed_after": 1})).task
    await _wait_state(ex_a, task.task_id, TaskState.RETRY_WAIT)
    log.info("pre-crash: task=%s in RETRY_WAIT after attempt=%s",
             task.task_id, (await ex_a.get_task(task.task_id)).attempt)
    await ex_a.halt()

    ex_b = make_executor(storage_path=path, max_attempts=3, base_delay=0.01)
    await ex_b.start()
    done = await ex_b.wait_terminal(task.task_id, timeout=5.0)
    log.info("post-restart: state=%s attempt=%s result=%s",
             done.state.value, done.attempt, done.result)
    assert done.state is TaskState.SUCCEEDED
    assert done.attempt == 2  # 1 failed attempt before crash + 1 after
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_interrupted_attempt_with_exhausted_budget_fails(
        make_executor, tmp_path):
    """RUNNING at crash with no retry budget left -> deterministic FAILED,
    never silently re-run."""
    path = str(tmp_path / "state.db")
    entered = asyncio.Event()

    async def blocking(payload, cancel_event):
        blocking.calls += 1
        entered.set()
        await asyncio.Event().wait()
    blocking.calls = 0

    ex_a = make_executor(storage_path=path, max_attempts=1,
                         registry=_registry(block=blocking))
    await ex_a.start()
    task = (await ex_a.submit(caller="c", task_type="block", payload={})).task
    await asyncio.wait_for(entered.wait(), 2.0)
    await ex_a.halt()

    ex_b = make_executor(storage_path=path, max_attempts=1,
                         registry=_registry(block=blocking))
    await ex_b.start()
    done = await ex_b.wait_terminal(task.task_id, timeout=5.0)
    log.info("post-restart: state=%s attempt=%s error=%s calls=%s",
             done.state.value, done.attempt, done.error_code, blocking.calls)
    assert done.state is TaskState.FAILED
    assert done.attempt == 1
    assert done.error_code == ErrorCode.EXECUTION_INTERRUPTED
    assert blocking.calls == 1  # not re-executed
    await ex_b.shutdown()
