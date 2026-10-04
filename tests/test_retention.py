"""History retention: terminal tasks are reclaimed after a bounded window,
tombstones keep lookups explainable, and reclaiming is safe under
concurrent submits/queries. Logs show ages, windows and judgements.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
from httpx import ASGITransport, AsyncClient

from gateway.api import create_app
from gateway.errors import ErrorCode, GatewayError
from gateway.states import TaskState

log = logging.getLogger("gateway.test.retention")


@pytest.mark.asyncio
async def test_reclaim_terminal_task_leaves_explainable_tombstone(make_executor):
    ex = make_executor(retention_seconds=0.05, tombstone_retention=0.2)
    await ex.start()
    task = (await ex.submit(caller="c", task_type="echo", payload={"v": 1})).task
    done = await ex.wait_terminal(task.task_id, timeout=5.0)
    assert done.state is TaskState.SUCCEEDED

    await asyncio.sleep(0.08)  # older than retention_seconds
    reclaimed = await ex.reclaim_once()
    log.info("reclaim pass: reclaimed=%s task=%s", reclaimed, task.task_id)
    assert reclaimed == 1

    with pytest.raises(GatewayError) as err:
        await ex.get_task(task.task_id)
    log.info("lookup after reclaim: code=%s message=%s",
             err.value.code, err.value.message)
    assert err.value.code == ErrorCode.TASK_RECLAIMED
    assert "SUCCEEDED" in err.value.message  # explainable, not "never existed"

    await asyncio.sleep(0.25)  # tombstone itself expires
    await ex.reclaim_once()
    with pytest.raises(GatewayError) as err2:
        await ex.get_task(task.task_id)
    log.info("lookup after tombstone expiry: code=%s", err2.value.code)
    assert err2.value.code == ErrorCode.TASK_NOT_FOUND
    await ex.shutdown()


@pytest.mark.asyncio
async def test_tombstone_survives_restart(make_executor, tmp_path):
    path = str(tmp_path / "state.db")
    ex_a = make_executor(storage_path=path, retention_seconds=0.05,
                         tombstone_retention=60.0)
    await ex_a.start()
    task = (await ex_a.submit(caller="c", task_type="echo", payload={})).task
    await ex_a.wait_terminal(task.task_id, timeout=5.0)
    await asyncio.sleep(0.08)
    assert await ex_a.reclaim_once() == 1
    await ex_a.halt()

    ex_b = make_executor(storage_path=path, retention_seconds=0.05,
                         tombstone_retention=60.0)
    await ex_b.start()
    with pytest.raises(GatewayError) as err:
        await ex_b.get_task(task.task_id)
    log.info("post-restart lookup of reclaimed task: code=%s", err.value.code)
    assert err.value.code == ErrorCode.TASK_RECLAIMED
    await ex_b.shutdown()


@pytest.mark.asyncio
async def test_reclaim_never_touches_non_terminal(make_executor):
    entered = asyncio.Event()

    async def blocking(payload, cancel_event):
        entered.set()
        await asyncio.Event().wait()

    from gateway.handlers import MapHandlerRegistry
    registry = MapHandlerRegistry()
    registry.register("block", blocking)

    ex = make_executor(retention_seconds=0.02, registry=registry)
    await ex.start()
    task = (await ex.submit(caller="c", task_type="block", payload={})).task
    await asyncio.wait_for(entered.wait(), 2.0)
    await asyncio.sleep(0.05)  # far older than retention, but still RUNNING

    reclaimed = await ex.reclaim_once()
    log.info("reclaim with RUNNING task: reclaimed=%s state=%s",
             reclaimed, (await ex.get_task(task.task_id)).state.value)
    assert reclaimed == 0
    assert (await ex.get_task(task.task_id)).state is TaskState.RUNNING

    await ex.cancel(task.task_id)
    done = await ex.wait_terminal(task.task_id, timeout=5.0)
    assert done.state is TaskState.CANCELLED
    await asyncio.sleep(0.05)
    reclaimed = await ex.reclaim_once()
    log.info("reclaim after cancel: reclaimed=%s", reclaimed)
    assert reclaimed == 1
    with pytest.raises(GatewayError) as err:
        await ex.get_task(task.task_id)
    assert err.value.code == ErrorCode.TASK_RECLAIMED
    assert "CANCELLED" in err.value.message
    await ex.shutdown()


@pytest.mark.asyncio
async def test_reclaim_respects_live_idempotency_window(make_executor):
    ex = make_executor(retention_seconds=0.05, idempotency_ttl=0.3)
    await ex.start()
    first = await ex.submit(caller="c", task_type="echo",
                            payload={"v": 1}, idempotency_key="keep")
    await ex.wait_terminal(first.task.task_id, timeout=5.0)

    await asyncio.sleep(0.1)  # past retention, still inside the TTL window
    reclaimed = await ex.reclaim_once()
    log.info("reclaim inside idempotency window: reclaimed=%s", reclaimed)
    assert reclaimed == 0
    same = await ex.submit(caller="c", task_type="echo",
                           payload={"v": 1}, idempotency_key="keep")
    assert same.reused and same.task.task_id == first.task.task_id

    await asyncio.sleep(0.25)  # now the TTL window has expired too
    reclaimed = await ex.reclaim_once()
    log.info("reclaim after window expiry: reclaimed=%s", reclaimed)
    assert reclaimed == 1
    with pytest.raises(GatewayError) as err:
        await ex.get_task(first.task.task_id)
    assert err.value.code == ErrorCode.TASK_RECLAIMED

    rebound = await ex.submit(caller="c", task_type="echo",
                              payload={"v": 1}, idempotency_key="keep")
    log.info("key rebound after reclaim: new_task=%s", rebound.task.task_id)
    assert rebound.reused is False
    assert rebound.task.task_id != first.task.task_id
    await ex.shutdown()


@pytest.mark.asyncio
async def test_reclaim_concurrent_with_reads_and_writes(make_executor):
    """Reclaim passes racing submits/queries must not corrupt state:
    keyed resubmits always resolve to their original task, unkeyed tasks
    are always either queryable or explainably reclaimed."""
    ex = make_executor(retention_seconds=0.05, idempotency_ttl=300.0,
                       max_concurrency=4, queue_capacity=32)
    await ex.start()
    stop = asyncio.Event()

    async def reaper():
        while not stop.is_set():
            await ex.reclaim_once()
            await asyncio.sleep(0.005)

    reaper_task = asyncio.create_task(reaper())

    keyed_ids = {}
    for i in range(6):
        r = await ex.submit(caller="keyed", task_type="echo",
                            payload={"i": i}, idempotency_key=f"ck-{i}")
        keyed_ids[i] = r.task.task_id

    async def resubmit_keyed(i: int):
        for _ in range(15):
            r = await ex.submit(caller="keyed", task_type="echo",
                                payload={"i": i}, idempotency_key=f"ck-{i}")
            assert r.reused is True
            assert r.task.task_id == keyed_ids[i]
            await asyncio.sleep(0.004)

    async def submit_unkeyed(j: int):
        r = await ex.submit(caller="plain", task_type="echo", payload={"j": j})
        for _ in range(400):
            try:
                t = await ex.get_task(r.task.task_id)
            except GatewayError as exc:
                # Reclaimed under our feet: explainable, never NOT_FOUND.
                assert exc.code == ErrorCode.TASK_RECLAIMED
                return "reclaimed"
            if t.state.is_terminal:
                return "terminal"
            await asyncio.sleep(0.005)
        pytest.fail(f"task {r.task.task_id} never reached a terminal state")

    results = await asyncio.gather(
        *(resubmit_keyed(i) for i in range(6)),
        *(submit_unkeyed(j) for j in range(12)),
    )
    stop.set()
    await reaper_task
    unkeyed_outcomes = results[6:]
    log.info("concurrent run done: keyed resubmits consistent; "
             "unkeyed outcomes terminal=%s reclaimed=%s",
             unkeyed_outcomes.count("terminal"),
             unkeyed_outcomes.count("reclaimed"))

    # Keyed tasks were protected by their live idempotency window.
    for i, task_id in keyed_ids.items():
        t = await ex.get_task(task_id)
        assert t.state is TaskState.SUCCEEDED
    await ex.shutdown()


@pytest.mark.asyncio
async def test_reclaimed_task_returns_410_over_http(make_executor):
    ex = make_executor(retention_seconds=0.05, tombstone_retention=60.0)
    app = create_app(ex)
    await ex.start()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://t") as client:
        resp = await client.post("/tasks", headers={"X-Caller": "c"},
                                 json={"task_type": "echo", "payload": {}})
        assert resp.status_code == 201
        task_id = resp.json()["task_id"]
        await ex.wait_terminal(task_id, timeout=5.0)
        await asyncio.sleep(0.08)
        assert await ex.reclaim_once() == 1

        gone = await client.get(f"/tasks/{task_id}")
        log.info("http lookup of reclaimed task: status=%s body=%s",
                 gone.status_code, gone.json())
        assert gone.status_code == 410
        assert gone.json()["error"]["code"] == "TASK_RECLAIMED"

        missing = await client.get("/tasks/never-existed")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "TASK_NOT_FOUND"
    await ex.shutdown()
