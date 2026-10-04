"""Restart recovery and idempotency continuity across process restarts.

A "restart" is simulated by ``simulate_crash()`` (freezes the executor and
stops persisting, like an abrupt process death) followed by building a new
executor on the same persistence path.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import GatewayError
from gateway.states import TaskState

log = logging.getLogger("gateway.test.persistence")


def _db(tmp_path) -> str:
    return str(tmp_path / "gateway.db")


@pytest.mark.asyncio
async def test_restart_resumes_running_and_pending_tasks(make_executor, tmp_path):
    gate = asyncio.Event()
    executed: list[str] = []

    async def gate_handler(payload, cancel_event):
        executed.append(payload["id"])
        await gate.wait()
        return {"id": payload["id"]}

    # First "process": one RUNNING task (holds the only slot) + one PENDING.
    ex_a = make_executor(persistence_path=_db(tmp_path),
                         max_concurrency=1, queue_capacity=4)
    ex_a._registry.register("gate", gate_handler)
    await ex_a.start()
    t1 = (await ex_a.submit(caller="c", task_type="gate", payload={"id": "t1"})).task
    t2 = (await ex_a.submit(caller="c", task_type="gate", payload={"id": "t2"})).task
    for _ in range(100):
        await asyncio.sleep(0.02)
        if (await ex_a.get_task(t1.task_id)).state is TaskState.RUNNING:
            break
    log.info("pre-crash: t1=%s t2=%s attempt=%s",
             (await ex_a.get_task(t1.task_id)).state.value,
             (await ex_a.get_task(t2.task_id)).state.value, t1.attempt)
    ex_a.simulate_crash()
    await asyncio.sleep(0.1)

    # Second "process" on the same journal.
    ex_b = make_executor(persistence_path=_db(tmp_path),
                         max_concurrency=1, queue_capacity=4)
    ex_b._registry.register("gate", gate_handler)
    await ex_b.start()
    r1 = await ex_b.get_task(t1.task_id)
    r2 = await ex_b.get_task(t2.task_id)
    log.info("post-restart: t1=%s attempt=%s t2=%s (ids must be unchanged)",
             r1.state.value, r1.attempt, r2.state.value)
    assert not r1.state.is_terminal and not r2.state.is_terminal
    assert r1.attempt == 1  # interrupted attempt is re-driven, not reset

    gate.set()
    f1 = await ex_b.wait_terminal(t1.task_id, timeout=5)
    f2 = await ex_b.wait_terminal(t2.task_id, timeout=5)
    log.info("final: t1=%s attempt=%s result=%s t2=%s executed=%s",
             f1.state.value, f1.attempt, f1.result, f2.state.value, executed)
    assert f1.state is TaskState.SUCCEEDED and f1.result == {"id": "t1"}
    assert f2.state is TaskState.SUCCEEDED and f2.result == {"id": "t2"}
    assert f1.attempt == 1 and f2.attempt == 1
    # t1 ran twice in total: the interrupted pre-crash attempt plus the
    # re-driven one; t2 ran exactly once.
    assert executed.count("t1") == 2 and executed.count("t2") == 1
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_does_not_reexecute_succeeded_task(make_executor, tmp_path):
    calls: list[str] = []

    async def counting_handler(payload, cancel_event):
        calls.append(payload["id"])
        return {"done": payload["id"]}

    ex_a = make_executor(persistence_path=_db(tmp_path))
    ex_a._registry.register("count", counting_handler)
    await ex_a.start()
    r = await ex_a.submit(caller="c", task_type="count", payload={"id": "x"},
                          idempotency_key="done-key")
    task = await ex_a.wait_terminal(r.task.task_id)
    assert task.state is TaskState.SUCCEEDED
    ex_a.simulate_crash()
    await asyncio.sleep(0.1)

    ex_b = make_executor(persistence_path=_db(tmp_path))
    ex_b._registry.register("count", counting_handler)
    await ex_b.start()
    restored = await ex_b.get_task(r.task.task_id)
    log.info("restored terminal task: id=%s state=%s attempt=%s result=%s",
             restored.task_id, restored.state.value, restored.attempt, restored.result)
    assert restored.state is TaskState.SUCCEEDED
    assert restored.attempt == 1 and restored.result == {"done": "x"}

    # Same idempotency key after restart -> reuse, still no re-execution.
    again = await ex_b.submit(caller="c", task_type="count", payload={"id": "x"},
                              idempotency_key="done-key")
    log.info("re-submit after restart: reused=%s task=%s calls=%s",
             again.reused, again.task.task_id, calls)
    assert again.reused and again.task.task_id == r.task.task_id
    await asyncio.sleep(0.2)
    assert calls == ["x"]  # executed exactly once across both processes
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_restart_resumes_retry_wait_with_next_attempt(make_executor, tmp_path):
    attempts: list[str] = []

    async def fail_once(payload, cancel_event):
        attempts.append(payload["id"])
        if len(attempts) == 1:
            from gateway.handlers import TaskExecutionError
            raise TaskExecutionError("first attempt fails")
        return {"ok": True}

    # Long backoff so the task is parked in RETRY_WAIT when the crash hits.
    ex_a = make_executor(persistence_path=_db(tmp_path),
                         max_attempts=3, base_delay=30.0)
    ex_a._registry.register("fail-once", fail_once)
    await ex_a.start()
    r = await ex_a.submit(caller="c", task_type="fail-once", payload={"id": "r"})
    for _ in range(100):
        await asyncio.sleep(0.02)
        if (await ex_a.get_task(r.task.task_id)).state is TaskState.RETRY_WAIT:
            break
    assert (await ex_a.get_task(r.task.task_id)).state is TaskState.RETRY_WAIT
    log.info("pre-crash: state=RETRY_WAIT attempt=%s", r.task.attempt)
    ex_a.simulate_crash()
    await asyncio.sleep(0.1)

    ex_b = make_executor(persistence_path=_db(tmp_path),
                         max_attempts=3, base_delay=30.0)
    ex_b._registry.register("fail-once", fail_once)
    await ex_b.start()
    final = await ex_b.wait_terminal(r.task.task_id, timeout=5)
    log.info("post-restart: state=%s attempt=%s attempts_seen=%s",
             final.state.value, final.attempt, attempts)
    assert final.state is TaskState.SUCCEEDED
    assert final.attempt == 2          # the scheduled next attempt ran
    assert attempts == ["r", "r"]      # one pre-crash + one post-recovery
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_idempotency_reuse_and_conflict_survive_restart(make_executor, tmp_path):
    ex_a = make_executor(persistence_path=_db(tmp_path))
    await ex_a.start()
    r = await ex_a.submit(caller="c", task_type="echo", payload={"v": 1},
                          idempotency_key="k-restart")
    await ex_a.wait_terminal(r.task.task_id)
    ex_a.simulate_crash()
    await asyncio.sleep(0.1)

    ex_b = make_executor(persistence_path=_db(tmp_path))
    await ex_b.start()
    same = await ex_b.submit(caller="c", task_type="echo", payload={"v": 1},
                             idempotency_key="k-restart")
    log.info("same key+content after restart: reused=%s task=%s (original=%s)",
             same.reused, same.task.task_id, r.task.task_id)
    assert same.reused and same.task.task_id == r.task.task_id

    with pytest.raises(GatewayError) as ei:
        await ex_b.submit(caller="c", task_type="echo", payload={"v": 2},
                          idempotency_key="k-restart")
    log.info("same key+different content after restart: code=%s bound_task=%s",
             ei.value.code, getattr(ei.value, "existing_task_id", None))
    assert ei.value.code == "IDEMPOTENCY_CONFLICT"
    assert ei.value.existing_task_id == r.task.task_id
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_expired_idempotency_window_rebinds_after_restart(make_executor, tmp_path):
    ex_a = make_executor(persistence_path=_db(tmp_path), idempotency_ttl=0.2)
    await ex_a.start()
    r1 = await ex_a.submit(caller="c", task_type="echo", payload={"v": 1},
                           idempotency_key="k-ttl")
    await ex_a.wait_terminal(r1.task.task_id)
    ex_a.simulate_crash()
    await asyncio.sleep(0.3)  # let the TTL window lapse while "down"

    ex_b = make_executor(persistence_path=_db(tmp_path), idempotency_ttl=0.2)
    await ex_b.start()
    r2 = await ex_b.submit(caller="c", task_type="echo", payload={"v": 1},
                           idempotency_key="k-ttl")
    log.info("expired key rebind after restart: reused=%s old=%s new=%s",
             r2.reused, r1.task.task_id, r2.task.task_id)
    assert not r2.reused and r2.task.task_id != r1.task.task_id
    final = await ex_b.wait_terminal(r2.task.task_id)
    assert final.state is TaskState.SUCCEEDED
    await ex_b.shutdown()
