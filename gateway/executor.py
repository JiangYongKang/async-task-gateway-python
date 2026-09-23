"""Task lifecycle executor.

Owns every state mutation (via a single lock) and one worker task per
caller that drains that caller's bounded queue, spawning one processor
coroutine per admitted task (bounded by ``max_concurrency + queue_capacity``);
the per-caller semaphore inside the processor enforces the concurrency cap.

Admission order on submit: idempotency -> rate limit -> concurrency/queue.
Only legal state transitions are applied (see gateway.states).  Retries use
exponential backoff with a fixed budget.  Cancellation has one stable
meaning per state (PENDING/SCHEDULED/RETRY_WAIT -> immediate CANCELLED as
soon as observed; RUNNING -> the handler coroutine is cancelled and the
task ends CANCELLED; terminal -> TASK_NOT_CANCELLABLE).  Shutdown stops
admission, drains within the grace period, then deterministically cancels
every task still alive.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from gateway.errors import ErrorCode, GatewayError
from gateway.handlers import HandlerRegistry, TaskExecutionError
from gateway.idempotency import IdempotencyConflict, IdempotencyStore
from gateway.models import GatewayConfig, TaskRecord, canonical_request_hash
from gateway.quotas import ConcurrencyLimitRejected, QuotaManager, QueueFullRejected
from gateway.states import IllegalTransition, TaskState

logger = logging.getLogger("gateway.executor")

# Upper bound for in-flight handler cancellation during forced shutdown,
# after which still-alive tasks are finalized directly as a safety net.
_FORCE_CANCEL_WAIT = 1.0


@dataclass(frozen=True)
class SubmitResult:
    task: TaskRecord
    reused: bool  # True when an existing idempotent task was returned


class TaskNotFound(GatewayError):
    def __init__(self, task_id: str):
        super().__init__(ErrorCode.TASK_NOT_FOUND, f"task {task_id!r} not found")


class TaskNotCancellable(GatewayError):
    def __init__(self, task_id: str, state: TaskState):
        super().__init__(
            ErrorCode.TASK_NOT_CANCELLABLE,
            f"task {task_id!r} in state {state.value} cannot be cancelled",
        )


class ShuttingDownError(GatewayError):
    def __init__(self) -> None:
        super().__init__(
            ErrorCode.SHUTTING_DOWN,
            "gateway is shutting down; new submissions are rejected",
        )


class TaskExecutor:
    def __init__(
        self,
        config: GatewayConfig,
        quota_manager: QuotaManager,
        idempotency_store: IdempotencyStore,
        registry: HandlerRegistry,
        *,
        clock: Callable[[], float] | None = None,
    ):
        self._config = config
        self._quotas = quota_manager
        self._idem = idempotency_store
        self._registry = registry
        self._clock = clock or time.time
        self._lock = asyncio.Lock()
        self._terminal_cv = asyncio.Condition(self._lock)
        self._tasks: dict[str, TaskRecord] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._handler_tasks: dict[str, asyncio.Task] = {}
        self._shutting_down = asyncio.Event()

    # ------------------------------------------------------------------
    async def start(self) -> None:
        logger.info("executor started: idempotency_ttl=%ss grace=%ss",
                    self._config.idempotency_ttl, self._config.shutdown_grace)

    @property
    def is_shutting_down(self) -> bool:
        return self._shutting_down.is_set()

    # ------------------------------------------------------------------
    # Submission
    # ------------------------------------------------------------------
    async def submit(
        self,
        *,
        caller: str,
        task_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> SubmitResult:
        if self._shutting_down.is_set():
            raise ShuttingDownError()

        # Reject unknown types before any state or quota is consumed.
        self._registry.get(task_type)
        request_hash = canonical_request_hash(task_type, payload)
        logger.info("submit input: caller=%s type=%s key=%s hash=%s payload=%s",
                    caller, task_type, idempotency_key, request_hash[:12], payload)

        async with self._lock:
            # 1) Idempotency: duplicate keys never consume quota or execute twice.
            if idempotency_key is not None:
                lookup = await self._idem.lookup(idempotency_key)
                if lookup is not None and lookup.reused:
                    existing = self._tasks[lookup.entry.task_id]
                    if lookup.entry.request_hash != request_hash:
                        logger.warning(
                            "idempotency conflict: key=%s existing_task=%s "
                            "bound_hash=%s incoming_hash=%s",
                            idempotency_key, existing.task_id,
                            lookup.entry.request_hash[:12], request_hash[:12])
                        raise IdempotencyConflict(idempotency_key, existing.task_id)
                    logger.info("idempotent reuse: key=%s -> task=%s state=%s",
                                idempotency_key, existing.task_id, existing.state.value)
                    return SubmitResult(task=existing, reused=True)

            # 2) Submit rate limit (only charged for genuinely new intent).
            quota = self._quotas.get(caller)
            quota.check_rate()

            # 3) Create record and reserve the key.
            task = TaskRecord(
                task_id=uuid.uuid4().hex,
                idempotency_key=idempotency_key,
                caller=caller,
                task_type=task_type,
                payload=dict(payload),
                state=TaskState.PENDING,
                cancel_event=asyncio.Event(),
                created_at=self._clock(),
                updated_at=self._clock(),
            )
            if idempotency_key is not None:
                try:
                    await self._idem.reserve(idempotency_key, task.task_id, request_hash)
                except IdempotencyConflict:
                    lookup = await self._idem.lookup(idempotency_key)
                    winner = self._tasks.get(lookup.entry.task_id) if lookup else None
                    if winner is not None and lookup.reused:
                        logger.info("reserve race lost; reusing winner task=%s", winner.task_id)
                        return SubmitResult(task=winner, reused=True)
                    raise

            # 4) Concurrency cap / bounded queue admission.
            try:
                await quota.enqueue(task.task_id)
            except (ConcurrencyLimitRejected, QueueFullRejected) as exc:
                logger.warning("admission rejected: task=%s code=%s inflight=%s/%s",
                               task.task_id, exc.code, quota.inflight,
                               quota.config.max_concurrency + quota.config.queue_capacity)
                if idempotency_key is not None:
                    await self._idem.release(idempotency_key, task.task_id)
                raise

            self._tasks[task.task_id] = task
            self._ensure_worker_locked(caller)
            logger.info("task accepted: task=%s caller=%s state=PENDING inflight=%s queued=%s",
                        task.task_id, caller, quota.inflight, quota.queued)
            return SubmitResult(task=task, reused=False)

    # ------------------------------------------------------------------
    # Queries / cancellation
    # ------------------------------------------------------------------
    async def get_task(self, task_id: str) -> TaskRecord:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFound(task_id)
            return task

    async def cancel(self, task_id: str) -> TaskRecord:
        """Request cancellation. Terminal tasks raise TASK_NOT_CANCELLABLE;
        repeated cancellation of a live task is idempotent (same record)."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                raise TaskNotFound(task_id)
            if not task.state.is_cancellable:
                logger.info("cancel rejected (terminal): task=%s state=%s",
                            task_id, task.state.value)
                raise TaskNotCancellable(task_id, task.state)
            task.cancel_event.set()
            if task.state is TaskState.RUNNING:
                handler_task = self._handler_tasks.get(task_id)
                if handler_task is not None:
                    handler_task.cancel()
            logger.info("cancel requested: task=%s state=%s attempt=%s",
                        task_id, task.state.value, task.attempt)
            return task

    async def wait_terminal(self, task_id: str, timeout: float | None = None) -> TaskRecord:
        async with self._terminal_cv:
            await asyncio.wait_for(
                self._terminal_cv.wait_for(
                    lambda: task_id in self._tasks and self._tasks[task_id].state.is_terminal
                ),
                timeout=timeout,
            )
            return self._tasks[task_id]

    def stats(self) -> dict[str, Any]:
        return {
            "tasks_total": len(self._tasks),
            "terminal": sum(1 for t in self._tasks.values() if t.state.is_terminal),
            "shutting_down": self._shutting_down.is_set(),
        }

    # ------------------------------------------------------------------
    # Per-caller worker + processors
    # ------------------------------------------------------------------
    def _ensure_worker_locked(self, caller: str) -> None:
        worker = self._workers.get(caller)
        if worker is None or worker.done():
            worker = asyncio.create_task(self._worker(caller), name=f"worker-{caller}")
            self._workers[caller] = worker

    async def _worker(self, caller: str) -> None:
        """Drain the caller queue; every admitted task gets a processor.
        Processor concurrency is bounded by the caller semaphore."""
        quota = self._quotas.get(caller)
        processors: set[asyncio.Task] = set()
        logger.info("worker started: caller=%s concurrency=%s queue=%s",
                    caller, quota.config.max_concurrency, quota.config.queue_capacity)
        try:
            while True:
                task_id = await quota.next_task()
                processor = asyncio.create_task(
                    self._process_one(caller, quota, task_id), name=f"proc-{task_id[:8]}"
                )
                processors.add(processor)
                processor.add_done_callback(processors.discard)
        except asyncio.CancelledError:
            pass
        finally:
            await asyncio.gather(*processors, return_exceptions=True)
            logger.info("worker exited: caller=%s", caller)

    async def _process_one(self, caller: str, quota: Any, task_id: str) -> None:
        async with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.state is not TaskState.PENDING:
                quota.task_done()
                return
            task.transition_to(TaskState.SCHEDULED)
            cancelled_early = task.cancel_event.is_set()
            if cancelled_early:
                self._finalize_locked(
                    task, TaskState.CANCELLED,
                    ErrorCode.SHUTTING_DOWN if self._shutting_down.is_set()
                    else ErrorCode.CANCELLED,
                    "cancelled while queued")
        if cancelled_early:
            quota.task_done()
            async with self._terminal_cv:
                self._terminal_cv.notify_all()
            return

        shutdown = self._shutting_down.is_set()
        async with quota.hold_slot(task.cancel_event) as granted:
            if not granted:
                # Cancel event fired while waiting for a concurrency slot.
                async with self._lock:
                    self._finalize_locked(
                        task, TaskState.CANCELLED,
                        ErrorCode.SHUTTING_DOWN if shutdown else ErrorCode.CANCELLED,
                        "cancelled while waiting for a slot")
                quota.task_done()
                async with self._terminal_cv:
                    self._terminal_cv.notify_all()
                return
            async with self._lock:
                if task.cancel_event.is_set():
                    self._finalize_locked(
                        task, TaskState.CANCELLED,
                        ErrorCode.SHUTTING_DOWN if self._shutting_down.is_set()
                        else ErrorCode.CANCELLED,
                        "cancelled just after slot grant")
                else:
                    task.transition_to(TaskState.RUNNING)
                    task.attempt = 1
            if task.state is TaskState.CANCELLED:
                quota.task_done()
                async with self._terminal_cv:
                    self._terminal_cv.notify_all()
                return
            policy = self._config.retry_for(task.task_type)
            logger.info("task running: task=%s attempt=1/%s", task_id, policy.max_attempts)
            await self._run_attempts(task, policy)
        quota.task_done()

    # ------------------------------------------------------------------
    # Attempts / retries
    # ------------------------------------------------------------------
    async def _run_attempts(self, task: TaskRecord, policy: Any) -> None:
        try:
            while True:
                handler = self._registry.get(task.task_type)
                handler_task = asyncio.create_task(
                    handler(task.payload, task.cancel_event), name=f"handler-{task.task_id[:8]}"
                )
                async with self._lock:
                    self._handler_tasks[task.task_id] = handler_task
                try:
                    result = await handler_task
                except asyncio.CancelledError:
                    code = (ErrorCode.SHUTTING_DOWN if self._shutting_down.is_set()
                            else ErrorCode.CANCELLED)
                    async with self._lock:
                        self._finalize_locked(task, TaskState.CANCELLED, code,
                                              "cancelled during execution")
                    logger.info("task cancelled while running: task=%s code=%s",
                                task.task_id, code)
                    return
                except TaskExecutionError as exc:
                    attempt_no = task.attempt
                    exhausted = attempt_no >= policy.max_attempts
                    logger.warning(
                        "attempt failed: task=%s attempt=%s/%s retryable=%s "
                        "exhausted=%s reason=%s",
                        task.task_id, attempt_no, policy.max_attempts,
                        exc.retryable, exhausted, exc.message)
                    if exhausted or not exc.retryable:
                        async with self._lock:
                            self._finalize_locked(task, TaskState.FAILED,
                                                  ErrorCode.HANDLER_FAILED, exc.message)
                        return
                    async with self._lock:
                        task.transition_to(TaskState.RETRY_WAIT,
                                           error_code=ErrorCode.HANDLER_FAILED,
                                           error_message=exc.message)
                    cancelled = await self._backoff(task, policy.delay_for(attempt_no + 1))
                    async with self._lock:
                        if cancelled:
                            self._finalize_locked(
                                task, TaskState.CANCELLED,
                                ErrorCode.SHUTTING_DOWN if self._shutting_down.is_set()
                                else ErrorCode.CANCELLED,
                                "cancelled during retry backoff")
                            return
                        task.attempt += 1
                        task.transition_to(TaskState.RUNNING)
                    logger.info("task retrying: task=%s attempt=%s",
                                task.task_id, task.attempt)
                    continue
                except Exception as exc:  # defensive: unexpected handler crash
                    logger.exception("unexpected handler error: task=%s", task.task_id)
                    async with self._lock:
                        self._finalize_locked(task, TaskState.FAILED,
                                              ErrorCode.HANDLER_FAILED, repr(exc))
                    return
                else:
                    async with self._lock:
                        task.result = result
                        self._finalize_locked(task, TaskState.SUCCEEDED)
                    logger.info("task succeeded: task=%s attempts=%s result=%s",
                                task.task_id, task.attempt, result)
                    return
        finally:
            async with self._lock:
                self._handler_tasks.pop(task.task_id, None)
                if task.state.is_terminal:
                    self._terminal_cv.notify_all()

    async def _backoff(self, task: TaskRecord, delay: float) -> bool:
        """Wait ``delay`` seconds; return True if cancellation interrupted."""
        try:
            await asyncio.wait_for(task.cancel_event.wait(), timeout=max(delay, 0.0))
            return True
        except asyncio.TimeoutError:
            return False

    def _finalize_locked(self, task: TaskRecord, state: TaskState,
                         code: str | None = None, message: str | None = None) -> None:
        try:
            task.transition_to(state, error_code=code, error_message=message)
        except IllegalTransition:
            logger.warning("redundant finalize ignored: task=%s already=%s target=%s",
                           task.task_id, task.state.value, state.value)

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("shutdown initiated: rejecting new submissions")
        self._shutting_down.set()

        alive = [t for t in self._tasks.values() if not t.state.is_terminal]
        logger.info("shutdown: %s alive task(s); grace=%ss",
                    len(alive), self._config.shutdown_grace)
        try:
            await asyncio.wait_for(self._await_all_terminal(), self._config.shutdown_grace)
            logger.info("shutdown: all tasks drained cleanly")
        except asyncio.TimeoutError:
            logger.warning("shutdown grace expired; force-cancelling remaining work")
            await self._force_cancel_remaining()

        await self._join_workers()
        remaining = [t for t in self._tasks.values() if not t.state.is_terminal]
        logger.info("shutdown complete: total=%s terminal=%s remaining_alive=%s",
                    len(self._tasks),
                    sum(1 for t in self._tasks.values() if t.state.is_terminal),
                    len(remaining))

    async def _force_cancel_remaining(self) -> None:
        async with self._lock:
            alive = [t for t in self._tasks.values() if not t.state.is_terminal]
            for task in alive:
                task.cancel_event.set()
                if task.state is TaskState.RUNNING:
                    handler_task = self._handler_tasks.get(task.task_id)
                    if handler_task is not None:
                        handler_task.cancel()
                # PENDING/SCHEDULED/RETRY_WAIT processors observe the event and
                # finalize themselves, keeping all transitions legal.
            self._terminal_cv.notify_all()

        try:
            await asyncio.wait_for(self._await_all_terminal(), _FORCE_CANCEL_WAIT)
            return
        except asyncio.TimeoutError:
            logger.error("tasks still alive after forced cancellation; finalizing directly")

        # Safety net: a stuck processor must not leave a non-terminal record.
        async with self._lock:
            for task in self._tasks.values():
                if not task.state.is_terminal:
                    self._finalize_locked(task, TaskState.CANCELLED,
                                          ErrorCode.SHUTTING_DOWN,
                                          "force-finalized after shutdown timeout")
            self._terminal_cv.notify_all()

    async def _await_all_terminal(self) -> None:
        async with self._terminal_cv:
            await self._terminal_cv.wait_for(
                lambda: all(t.state.is_terminal for t in self._tasks.values())
            )

    async def _join_workers(self, timeout: float = _FORCE_CANCEL_WAIT) -> None:
        """Cancel per-caller workers and bound the wait.

        A handler that swallows ``CancelledError`` could leave a processor
        alive; records are already finalized by the safety net, so we never
        block shutdown forever on a rogue coroutine.
        """
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        if not workers:
            return
        _, pending = await asyncio.wait(
            [asyncio.gather(*workers, return_exceptions=True)],
            timeout=timeout,
        )
        for fut in pending:
            fut.cancel()
