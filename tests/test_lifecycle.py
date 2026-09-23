"""Task lifecycle: state transitions, retries, non-retryable, cancellation."""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import GatewayError
from gateway.states import IllegalTransition, TaskState
from gateway.models import TaskRecord

log = logging.getLogger("gateway.test.lifecycle")


@pytest.mark.asyncio
async def test_state_machine_rejects_illegal_transitions():
    t = TaskRecord("t", None, "c", "echo", {}, TaskState.PENDING)
    t.transition_to(TaskState.SCHEDULED)
    t.transition_to(TaskState.RUNNING)
    t.transition_to(TaskState.RETRY_WAIT)
    t.transition_to(TaskState.RUNNING)
    t.transition_to(TaskState.SUCCEEDED)
    with pytest.raises(IllegalTransition):
        t.transition_to(TaskState.RUNNING)
    log.info("legal path PENDING->SCHEDULED->RUNNING->RETRY_WAIT->RUNNING->SUCCEEDED ok")


@pytest.mark.asyncio
async def test_failed_task_retries_until_exhausted_then_terminal(make_executor):
    ex = make_executor(max_attempts=3, base_delay=0.01)
    await ex.start()
    r = await ex.submit(caller="c", task_type="fail",
                        payload={"message": "boom"})
    log.info("fail task submitted; expecting 3 attempts then FAILED")
    task = await ex.wait_terminal(r.task.task_id)
    assert task.state is TaskState.FAILED
    assert task.attempt == 3
    assert task.error_code == "HANDLER_FAILED"
    assert task.error_message == "boom"
    log.info("judgement: terminal=%s attempts=%s error=%s",
             task.state.value, task.attempt, task.error_code)
    await ex.shutdown()


@pytest.mark.asyncio
async def test_non_retryable_failure_fails_immediately(make_executor):
    ex = make_executor(max_attempts=5, base_delay=0.01)
    await ex.start()
    r = await ex.submit(caller="c", task_type="fail",
                        payload={"message": "permanent", "retryable": False})
    task = await ex.wait_terminal(r.task.task_id)
    log.info("non-retryable failure: attempts=%s state=%s", task.attempt, task.state.value)
    assert task.state is TaskState.FAILED
    assert task.attempt == 1
    assert task.error_message == "permanent"
    await ex.shutdown()


@pytest.mark.asyncio
async def test_eventual_success_records_attempts_and_result(make_executor):
    ex = make_executor(max_attempts=4, base_delay=0.01)
    await ex.start()
    r = await ex.submit(caller="c", task_type="flaky",
                        payload={"key": "f", "succeed_after": 2})
    task = await ex.wait_terminal(r.task.task_id)
    log.info("flaky task: state=%s attempts=%s result=%s",
             task.state.value, task.attempt, task.result)
    assert task.state is TaskState.SUCCEEDED
    assert task.attempt == 3
    assert task.result == {"ok": True, "attempts": 3}
    await ex.shutdown()


@pytest.mark.asyncio
async def test_cancel_running_task_reaches_cancelled(make_executor):
    ex = make_executor()
    await ex.start()
    r = await ex.submit(caller="c", task_type="sleep",
                        payload={"seconds": 30}, idempotency_key="cancel-run")
    await asyncio.sleep(0.1)
    task = await ex.get_task(r.task.task_id)
    assert task.state is TaskState.RUNNING
    await ex.cancel(r.task.task_id)
    final = await ex.wait_terminal(r.task.task_id)
    log.info("running cancel: final=%s error=%s", final.state.value, final.error_code)
    assert final.state is TaskState.CANCELLED
    assert final.error_code == "CANCELLED"

    # Idempotent second cancel of a terminal task -> stable distinct error.
    with pytest.raises(GatewayError) as ei:
        await ex.cancel(r.task.task_id)
    assert ei.value.code == "TASK_NOT_CANCELLABLE"
    await ex.shutdown()


@pytest.mark.asyncio
async def test_cancel_during_retry_wait(make_executor):
    ex = make_executor(max_attempts=5, base_delay=1.0)
    await ex.start()
    r = await ex.submit(caller="c", task_type="fail", payload={"delay": 0.0})
    # Wait until the task enters RETRY_WAIT (1s backoff).
    for _ in range(50):
        await asyncio.sleep(0.02)
        if (await ex.get_task(r.task.task_id)).state is TaskState.RETRY_WAIT:
            break
    state = (await ex.get_task(r.task.task_id)).state
    log.info("task state before cancel=%s", state.value)
    assert state is TaskState.RETRY_WAIT
    await ex.cancel(r.task.task_id)
    final = await ex.wait_terminal(r.task.task_id)
    assert final.state is TaskState.CANCELLED
    log.info("cancel during RETRY_WAIT -> %s (no further attempts, attempt=%s)",
             final.state.value, final.attempt)
    assert final.attempt == 1
    await ex.shutdown()


@pytest.mark.asyncio
async def test_cancel_queued_task_does_not_execute(make_executor):
    # cap=1, queue=3: occupy the slot, then cancel a queued task.
    ex = make_executor(max_concurrency=1, queue_capacity=3)
    await ex.start()
    gate = asyncio.Event()
    executed = []

    async def block(payload, event):
        executed.append(payload.get("id"))
        await gate.wait()
        return True

    ex._registry.register("block", block)
    head = await ex.submit(caller="c", task_type="block", payload={"id": "head"})
    queued = []
    for i in range(2):
        queued.append(await ex.submit(caller="c", task_type="block",
                                      payload={"id": f"q{i}"}))
    await asyncio.sleep(0.05)
    target = queued[0].task
    log.info("cancelling queued task=%s before it gets a slot", target.task_id)
    await ex.cancel(target.task_id)
    final = await ex.wait_terminal(target.task_id)
    assert final.state is TaskState.CANCELLED
    gate.set()
    await ex.wait_terminal(head.task.task_id)
    await ex.wait_terminal(queued[1].task.task_id)
    log.info("executed=%s (cancelled task must be absent)", executed)
    assert "q0" not in executed
    assert "q1" in executed
    await ex.shutdown()


@pytest.mark.asyncio
async def test_get_unknown_task_is_not_found(make_executor):
    ex = make_executor()
    await ex.start()
    with pytest.raises(GatewayError) as ei:
        await ex.get_task("missing")
    assert ei.value.code == "TASK_NOT_FOUND"
    log.info("missing task query -> TASK_NOT_FOUND")
    await ex.shutdown()


@pytest.mark.asyncio
async def test_unknown_task_type_rejected_before_admission(make_executor):
    ex = make_executor()
    await ex.start()
    with pytest.raises(GatewayError) as ei:
        await ex.submit(caller="c", task_type="does-not-exist", payload={})
    assert ei.value.code == "UNKNOWN_TASK_TYPE"
    assert ex.stats()["tasks_total"] == 0
    log.info("unknown type rejected without creating a task record")
    await ex.shutdown()
