"""History retention: reclamation boundaries, tombstones, concurrency."""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.errors import GatewayError
from gateway.states import TaskState

log = logging.getLogger("gateway.test.retention")


@pytest.mark.asyncio
async def test_old_terminal_tasks_reclaimed_with_explainable_lookup(make_executor):
    ex = make_executor(history_retention=0.2, retention_sweep_interval=60.0)
    await ex.start()
    old = (await ex.submit(caller="c", task_type="echo", payload={"n": 1})).task
    await ex.wait_terminal(old.task_id)
    await asyncio.sleep(0.3)
    fresh = (await ex.submit(caller="c", task_type="echo", payload={"n": 2})).task
    await ex.wait_terminal(fresh.task_id)

    reclaimed = await ex.sweep_now()
    log.info("sweep reclaimed=%s; old=%s fresh=%s", reclaimed, old.task_id, fresh.task_id)
    assert reclaimed == 1

    with pytest.raises(GatewayError) as ei:
        await ex.get_task(old.task_id)
    assert ei.value.code == "TASK_EXPIRED"
    log.info("reclaimed task lookup -> TASK_EXPIRED (distinguishable)")

    # The fresh terminal task is below the retention age and survives.
    assert (await ex.get_task(fresh.task_id)).state is TaskState.SUCCEEDED

    # A never-existing id stays TASK_NOT_FOUND - different from TASK_EXPIRED.
    with pytest.raises(GatewayError) as ei2:
        await ex.get_task("never-existed")
    assert ei2.value.code == "TASK_NOT_FOUND"
    await ex.shutdown()


@pytest.mark.asyncio
async def test_inflight_tasks_are_never_reclaimed(make_executor):
    gate = asyncio.Event()
    executed: list[str] = []

    async def blocker(payload, cancel_event):
        await gate.wait()
        executed.append(payload["id"])
        return True

    ex = make_executor(history_retention=0.2, retention_sweep_interval=60.0)
    ex._registry.register("blocker", blocker)
    await ex.start()
    r = await ex.submit(caller="c", task_type="blocker", payload={"id": "live"})
    for _ in range(100):
        await asyncio.sleep(0.02)
        if (await ex.get_task(r.task.task_id)).state is TaskState.RUNNING:
            break
    await asyncio.sleep(0.3)  # older than retention, but still in-flight
    reclaimed = await ex.sweep_now()
    state = (await ex.get_task(r.task.task_id)).state
    log.info("in-flight task after sweep: reclaimed=%s state=%s", reclaimed, state.value)
    assert reclaimed == 0 and state is TaskState.RUNNING
    gate.set()
    final = await ex.wait_terminal(r.task.task_id)
    assert final.state is TaskState.SUCCEEDED and executed == ["live"]
    await ex.shutdown()


@pytest.mark.asyncio
async def test_live_idempotency_window_protects_terminal_record(make_executor):
    ex = make_executor(history_retention=0.2, idempotency_ttl=10.0,
                       retention_sweep_interval=60.0)
    await ex.start()
    r = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                        idempotency_key="k-protected")
    await ex.wait_terminal(r.task.task_id)
    await asyncio.sleep(0.3)  # older than retention, window still live
    reclaimed = await ex.sweep_now()
    log.info("sweep with live idempotency window: reclaimed=%s", reclaimed)
    assert reclaimed == 0

    again = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                            idempotency_key="k-protected")
    log.info("re-submit inside window: reused=%s task=%s", again.reused,
             again.task.task_id)
    assert again.reused and again.task.task_id == r.task.task_id
    await ex.shutdown()


@pytest.mark.asyncio
async def test_expired_window_allows_reclaim_and_rebind(make_executor):
    ex = make_executor(history_retention=0.2, idempotency_ttl=0.3,
                       retention_sweep_interval=60.0)
    await ex.start()
    r = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                        idempotency_key="k-expiring")
    await ex.wait_terminal(r.task.task_id)
    await asyncio.sleep(0.4)  # retention AND idempotency window both lapsed
    reclaimed = await ex.sweep_now()
    log.info("sweep after window expiry: reclaimed=%s", reclaimed)
    assert reclaimed == 1
    with pytest.raises(GatewayError) as ei:
        await ex.get_task(r.task.task_id)
    assert ei.value.code == "TASK_EXPIRED"

    # The expired key may be rebound to a brand-new task.
    r2 = await ex.submit(caller="c", task_type="echo", payload={"v": 1},
                         idempotency_key="k-expiring")
    log.info("rebind after reclaim: reused=%s old=%s new=%s",
             r2.reused, r.task.task_id, r2.task.task_id)
    assert not r2.reused and r2.task.task_id != r.task.task_id
    await ex.wait_terminal(r2.task.task_id)
    await ex.shutdown()


@pytest.mark.asyncio
async def test_tombstone_expiry_degrades_to_not_found(make_executor):
    ex = make_executor(history_retention=0.1, tombstone_retention=0.2,
                       retention_sweep_interval=60.0)
    await ex.start()
    r = await ex.submit(caller="c", task_type="echo", payload={})
    await ex.wait_terminal(r.task.task_id)
    await asyncio.sleep(0.15)
    await ex.sweep_now()
    with pytest.raises(GatewayError) as ei:
        await ex.get_task(r.task.task_id)
    assert ei.value.code == "TASK_EXPIRED"
    await asyncio.sleep(0.25)  # tombstone itself now expires
    await ex.sweep_now()
    with pytest.raises(GatewayError) as ei2:
        await ex.get_task(r.task.task_id)
    log.info("after tombstone expiry: code=%s", ei2.value.code)
    assert ei2.value.code == "TASK_NOT_FOUND"
    await ex.shutdown()


@pytest.mark.asyncio
async def test_tombstones_survive_restart(make_executor, tmp_path):
    db = str(tmp_path / "gateway.db")
    ex_a = make_executor(persistence_path=db, history_retention=0.1,
                         retention_sweep_interval=60.0)
    await ex_a.start()
    r = await ex_a.submit(caller="c", task_type="echo", payload={})
    await ex_a.wait_terminal(r.task.task_id)
    await asyncio.sleep(0.15)
    assert await ex_a.sweep_now() == 1
    ex_a.simulate_crash()
    await asyncio.sleep(0.1)

    ex_b = make_executor(persistence_path=db, history_retention=0.1,
                         retention_sweep_interval=60.0)
    await ex_b.start()
    with pytest.raises(GatewayError) as ei:
        await ex_b.get_task(r.task.task_id)
    log.info("reclaimed task lookup after restart: code=%s", ei.value.code)
    assert ei.value.code == "TASK_EXPIRED"
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_sweep_concurrent_with_submit_execute_and_query(make_executor):
    """Reclamation racing normal traffic must not corrupt or confuse state."""
    ex = make_executor(history_retention=0.15, retention_sweep_interval=60.0,
                       max_concurrency=2, queue_capacity=64)
    await ex.start()
    submitted: list[str] = []
    observations: list[str] = []
    stop = asyncio.Event()

    async def submitter():
        for i in range(20):
            r = await ex.submit(caller="c", task_type="echo", payload={"i": i})
            submitted.append(r.task.task_id)
            await asyncio.sleep(0.02)

    async def querier():
        while not stop.is_set():
            for tid in list(submitted):
                try:
                    task = await ex.get_task(tid)
                    observations.append(f"{tid[:6]}:{task.state.value}")
                except GatewayError as exc:
                    # Reclaimed is explainable; NOT_FOUND for a known id is not.
                    assert exc.code == "TASK_EXPIRED", f"unexpected code {exc.code}"
                    observations.append(f"{tid[:6]}:EXPIRED")
            await asyncio.sleep(0.01)

    async def sweeper():
        while not stop.is_set():
            await ex.sweep_now()
            await asyncio.sleep(0.03)

    sq = asyncio.gather(submitter(), querier(), sweeper())
    while len(submitted) < 20:
        await asyncio.sleep(0.05)
    # Wait until every task is either terminal or already reclaimed; both
    # are explainable outcomes, and terminal states must all be SUCCEEDED.
    final_states: dict[str, str] = {}
    for _ in range(200):
        await asyncio.sleep(0.05)
        for tid in submitted:
            if tid in final_states:
                continue
            try:
                task = await ex.get_task(tid)
                if task.state.is_terminal:
                    final_states[tid] = task.state.value
            except GatewayError as exc:
                assert exc.code == "TASK_EXPIRED"
                final_states[tid] = "EXPIRED"
        if len(final_states) == 20:
            break
    stop.set()
    await sq

    log.info("final states=%s", sorted(set(final_states.values())))
    assert len(final_states) == 20
    assert set(final_states.values()) <= {"SUCCEEDED", "EXPIRED"}

    # Post-mortem consistency: every task is either queryable as SUCCEEDED
    # or reclaimed with a tombstone - never silently gone.
    expired = 0
    for tid in submitted:
        try:
            assert (await ex.get_task(tid)).state is TaskState.SUCCEEDED
        except GatewayError as exc:
            assert exc.code == "TASK_EXPIRED"
            expired += 1
    log.info("post-mortem: queryable=%s expired=%s observations=%s",
             20 - expired, expired, len(observations))
    await ex.shutdown()
