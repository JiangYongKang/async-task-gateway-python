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
from gateway.models import GatewayConfig, TaskRecord, Tombstone, canonical_request_hash
from gateway.quotas import ConcurrencyLimitRejected, QuotaManager, QueueFullRejected
from gateway.states import IllegalTransition, TaskState
from gateway.storage import SQLiteTaskStore

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


class TaskReclaimed(GatewayError):
    """The task existed, finished, and was reclaimed after the retention
    window; its tombstone is kept so this is distinguishable from
    TASK_NOT_FOUND."""

    def __init__(self, tombstone: Tombstone):
        super().__init__(
            ErrorCode.TASK_RECLAIMED,
            f"task {tombstone.task_id!r} finished as {tombstone.final_state.value} "
            f"and was reclaimed after the retention window "
            f"(reclaimed_at={tombstone.reclaimed_at:.3f})",
        )
        self.tombstone = tombstone


class TaskExecutor:
    def __init__(
        self,
        config: GatewayConfig,
        quota_manager: QuotaManager,
        idempotency_store: IdempotencyStore,
        registry: HandlerRegistry,
        *,
        clock: Callable[[], float] | None = None,
        store: SQLiteTaskStore | None = None,
    ):
        self._config = config
        self._quotas = quota_manager
        self._idem = idempotency_store
        self._registry = registry
        self._clock = clock or time.time
        if store is None and config.storage_path:
            store = SQLiteTaskStore(config.storage_path)
        self._store = store
        if store is not None:
            self._idem.attach_persister(store)
        self._lock = asyncio.Lock()
        self._terminal_cv = asyncio.Condition(self._lock)
        self._tasks: dict[str, TaskRecord] = {}
        self._tombstones: dict[str, Tombstone] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._processors: set[asyncio.Task] = set()
        self._handler_tasks: dict[str, asyncio.Task] = {}
        self._shutting_down = asyncio.Event()
        self._halted = False  # crash-drill: stop without finalizing records
        self._reaper_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    def _save(self, task: TaskRecord) -> None:
        """Persist the current record; a restart must see exactly this."""
        if self._store is not None:
            self._store.upsert_task(task)

    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._store is not None:
            await self._recover_from_store()
        self._reaper_task = asyncio.create_task(self._reaper_loop(), name="reaper")
        logger.info("executor started: idempotency_ttl=%ss grace=%ss "
                    "retention=%ss reaper_interval=%ss",
                    self._config.idempotency_ttl, self._config.shutdown_grace,
                    self._config.retention_seconds, self._config.reaper_interval)

    # ------------------------------------------------------------------
    # Restart recovery
    # ------------------------------------------------------------------
    async def _recover_from_store(self) -> None:
        """Reload persisted state and resume every non-terminal task.

        * terminal records are only reloaded for queries; never re-executed;
        * PENDING/SCHEDULED are re-queued (SCHEDULED had not started yet);
        * RUNNING means the in-flight attempt was interrupted: it counts
          against the retry budget and the task resumes at the next attempt
          via RETRY_WAIT (or FAILED if the budget is already exhausted);
        * RETRY_WAIT resumes its backoff and then the next attempt.
        """
        assert self._store is not None
        store = self._store
        now = self._clock()

        entries = store.load_idempotency()
        live = await self._idem.restore(entries)
        for key, entry in entries.items():
            if entry.expires_at <= now:
                store.delete_idempotency(key)

        async with self._lock:
            for ts in store.load_tombstones():
                if ts.expires_at > now:
                    self._tombstones[ts.task_id] = ts
                else:
                    store.delete_tombstone(ts.task_id)

            recovered: list[TaskRecord] = []
            terminal = 0
            for rec in store.load_tasks():  # ordered by created_at
                rec.cancel_event = asyncio.Event()
                self._tasks[rec.task_id] = rec
                if rec.state.is_terminal:
                    terminal += 1
                else:
                    recovered.append(rec)
            logger.info("recovery: loaded tasks=%s (terminal=%s, to_resume=%s) "
                        "idempotency_live=%s tombstones=%s",
                        len(self._tasks), terminal, len(recovered),
                        live, len(self._tombstones))
            for rec in recovered:
                await self._recover_one_locked(rec)

    async def _recover_one_locked(self, rec: TaskRecord) -> None:
        policy = self._config.retry_for(rec.task_type)
        if rec.state is TaskState.SCHEDULED:
            # Dequeued but never started: put it back in the queue.
            rec.transition_to(TaskState.PENDING)
            self._save(rec)
        elif rec.state is TaskState.RUNNING:
            if rec.attempt >= policy.max_attempts:
                self._finalize_locked(
                    rec, TaskState.FAILED, ErrorCode.EXECUTION_INTERRUPTED,
                    f"attempt {rec.attempt} interrupted by process restart; "
                    "retry budget exhausted")
                logger.warning("recovery: task=%s interrupted at attempt=%s/%s; "
                               "budget exhausted -> FAILED", rec.task_id,
                               rec.attempt, policy.max_attempts)
                return
            rec.transition_to(
                TaskState.RETRY_WAIT,
                error_code=ErrorCode.EXECUTION_INTERRUPTED,
                error_message=f"attempt {rec.attempt} interrupted by process "
                              "restart; resuming")
            self._save(rec)
            logger.warning("recovery: task=%s interrupted mid-attempt=%s/%s; "
                           "resuming at attempt %s",
                           rec.task_id, rec.attempt, policy.max_attempts,
                           rec.attempt + 1)
        else:
            logger.info("recovery: task=%s state=%s attempt=%s re-queued",
                        rec.task_id, rec.state.value, rec.attempt)
        quota = self._quotas.get(rec.caller)
        await quota.enqueue_recovered(rec.task_id)
        self._ensure_worker_locked(rec.caller)

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
            self._save(task)
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
            if task is not None:
                return task
            tombstone = self._tombstones.get(task_id)
            if tombstone is not None:
                logger.info("lookup hit tombstone: task=%s final=%s reclaimed_at=%.3f",
                            task_id, tombstone.final_state.value,
                            tombstone.reclaimed_at)
                raise TaskReclaimed(tombstone)
            raise TaskNotFound(task_id)

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

    # ------------------------------------------------------------------
    # History retention / reclaim
    # ------------------------------------------------------------------
    async def reclaim_once(self) -> int:
        """One retention pass. Returns the number of tasks reclaimed.

        A task is reclaimed only when ALL of these hold:
          * it is terminal (running work is never touched);
          * ``now - updated_at >= retention_seconds``;
          * its idempotency key (if any) is no longer inside the live TTL
            window, so a same-key resubmit can never resolve to a record
            that disappeared.
        Reclaimed tasks leave a tombstone so lookups answer TASK_RECLAIMED
        instead of TASK_NOT_FOUND; tombstones themselves expire after
        ``tombstone_retention`` seconds.
        """
        async with self._lock:
            now = self._clock()
            for task_id, ts in list(self._tombstones.items()):
                if ts.expires_at <= now:
                    del self._tombstones[task_id]
                    if self._store is not None:
                        self._store.delete_tombstone(task_id)
                    logger.info("tombstone expired: task=%s final=%s",
                                task_id, ts.final_state.value)

            purged = await self._idem.purge_expired()
            reclaimed = 0
            for task in list(self._tasks.values()):
                if not task.state.is_terminal:
                    continue
                age = now - task.updated_at
                if age < self._config.retention_seconds:
                    continue
                if task.idempotency_key is not None:
                    lookup = await self._idem.lookup(task.idempotency_key)
                    if lookup is not None and lookup.reused:
                        logger.info("reclaim skipped (idempotency window live): "
                                    "task=%s key=%s", task.task_id,
                                    task.idempotency_key)
                        continue
                tombstone = Tombstone(
                    task_id=task.task_id,
                    caller=task.caller,
                    task_type=task.task_type,
                    final_state=task.state,
                    attempt=task.attempt,
                    error_code=task.error_code,
                    error_message=task.error_message,
                    created_at=task.created_at,
                    finished_at=task.updated_at,
                    reclaimed_at=now,
                    expires_at=now + self._config.tombstone_retention,
                )
                del self._tasks[task.task_id]
                self._tombstones[task.task_id] = tombstone
                if self._store is not None:
                    self._store.delete_task(task.task_id)
                    self._store.put_tombstone(tombstone)
                reclaimed += 1
                logger.info("task reclaimed: task=%s final=%s attempt=%s "
                            "age=%.3fs tombstone_ttl=%.0fs",
                            task.task_id, task.state.value, task.attempt,
                            age, self._config.tombstone_retention)
            if reclaimed or purged:
                logger.info("reclaim pass done: reclaimed=%s "
                            "idempotency_purged=%s remaining=%s",
                            reclaimed, purged, len(self._tasks))
            return reclaimed

    async def _reaper_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._config.reaper_interval)
                try:
                    await self.reclaim_once()
                except Exception:
                    logger.exception("reaper pass failed; will retry next interval")
        except asyncio.CancelledError:
            pass

    def stats(self) -> dict[str, Any]:
        # Shape kept stable for existing consumers; tombstone count is
        # reported via logs and the reclaim API instead.
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
                self._processors.add(processor)
                processor.add_done_callback(self._processors.discard)
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
            if task is None or task.state not in (TaskState.PENDING,
                                                  TaskState.RETRY_WAIT):
                quota.task_done()
                return
            # A RETRY_WAIT task can only arrive here via restart recovery;
            # it keeps its attempt count and resumes below.
            recovered = task.state is TaskState.RETRY_WAIT
            if not recovered:
                task.transition_to(TaskState.SCHEDULED)
                self._save(task)
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
                elif not recovered:
                    task.transition_to(TaskState.RUNNING)
                    task.attempt = 1
                    self._save(task)
            if task.state is TaskState.CANCELLED:
                quota.task_done()
                async with self._terminal_cv:
                    self._terminal_cv.notify_all()
                return
            policy = self._config.retry_for(task.task_type)
            if recovered:
                resumed = await self._resume_recovered(task, policy)
                if not resumed:
                    quota.task_done()
                    async with self._terminal_cv:
                        self._terminal_cv.notify_all()
                    return
            logger.info("task running: task=%s attempt=%s/%s",
                        task_id, task.attempt, policy.max_attempts)
            await self._run_attempts(task, policy)
        quota.task_done()

    async def _resume_recovered(self, task: TaskRecord, policy: Any) -> bool:
        """Finish a recovered task's backoff and start the next attempt.

        Returns False when the task was finalized instead (cancelled, or the
        retry budget no longer covers another attempt). The backoff restarts
        in full after a reboot: the pre-restart wait happened in a dead
        process and its remaining time is unknowable.
        """
        if task.attempt >= policy.max_attempts:
            async with self._lock:
                self._finalize_locked(
                    task, TaskState.FAILED, ErrorCode.EXECUTION_INTERRUPTED,
                    f"interrupted by process restart after attempt "
                    f"{task.attempt}; retry budget exhausted")
            logger.warning("recovered task has no budget left: task=%s "
                           "attempt=%s/%s -> FAILED",
                           task.task_id, task.attempt, policy.max_attempts)
            return False
        delay = policy.delay_for(task.attempt + 1)
        logger.info("resuming recovered task: task=%s next_attempt=%s/%s "
                    "backoff=%.3fs",
                    task.task_id, task.attempt + 1, policy.max_attempts, delay)
        cancelled = await self._backoff(task, delay)
        async with self._lock:
            if cancelled:
                self._finalize_locked(
                    task, TaskState.CANCELLED,
                    ErrorCode.SHUTTING_DOWN if self._shutting_down.is_set()
                    else ErrorCode.CANCELLED,
                    "cancelled during recovered backoff")
                return False
            task.attempt += 1
            task.transition_to(TaskState.RUNNING)
            self._save(task)
        return True

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
                    if self._halted:
                        # Crash drill: leave the persisted record exactly as
                        # it was (RUNNING); the next process recovers it.
                        raise
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
                        self._save(task)
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
                        self._save(task)
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
            return
        self._save(task)

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    async def halt(self) -> None:
        """Crash drill: stop all local processing WITHOUT finalizing records.

        Unlike :meth:`shutdown`, persisted state is left exactly as last
        saved (e.g. RUNNING mid-attempt), so a fresh executor opened on the
        same store recovers those tasks. Used to rehearse restart recovery;
        the halted executor must be discarded afterwards.
        """
        logger.warning("halt: simulating abrupt crash; records left as-is")
        self._halted = True
        self._shutting_down.set()
        if self._reaper_task is not None:
            self._reaper_task.cancel()
        handler_tasks = list(self._handler_tasks.values())
        for handler_task in handler_tasks:
            handler_task.cancel()
        processors = list(self._processors)
        for processor in processors:
            processor.cancel()
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, *processors, *handler_tasks,
                             return_exceptions=True)
        logger.warning("halt complete: %s task record(s) left non-terminal",
                       sum(1 for t in self._tasks.values()
                           if not t.state.is_terminal))

    async def shutdown(self) -> None:
        logger.info("shutdown initiated: rejecting new submissions")
        self._shutting_down.set()
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            self._reaper_task = None

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
