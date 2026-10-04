"""Per-caller quotas: submit rate limit, concurrency cap and bounded queue.

Three independently observable rejection reasons:
  RATE_LIMIT_REJECTED        token bucket empty (too many submits/second)
  CONCURRENCY_LIMIT_REJECTED concurrency slots full and ``queue_capacity == 0``
  QUEUE_FULL_REJECTED        concurrency slots full and the bounded queue full

Admitted-but-unfinished tasks are bounded by
``max_concurrency + queue_capacity``, so a caller can never pile up work
without limit even under concurrent submission.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
import contextlib
from contextlib import asynccontextmanager

from gateway.errors import ErrorCode, GatewayError
from gateway.models import QuotaConfig


class RateLimitRejected(GatewayError):
    def __init__(self, caller: str, retry_after: float):
        super().__init__(
            ErrorCode.RATE_LIMIT_REJECTED,
            f"caller {caller!r} exceeded submit rate; retry after {retry_after:.3f}s",
            retry_after=retry_after,
        )


class ConcurrencyLimitRejected(GatewayError):
    def __init__(self, caller: str, limit: int):
        super().__init__(
            ErrorCode.CONCURRENCY_LIMIT_REJECTED,
            f"caller {caller!r} is already running {limit} task(s) and queueing "
            "is disabled (queue_capacity=0)",
        )


class QueueFullRejected(GatewayError):
    def __init__(self, caller: str, capacity: int):
        super().__init__(
            ErrorCode.QUEUE_FULL_REJECTED,
            f"caller {caller!r} queue is full ({capacity} waiting); retry later",
        )


class CallerQuota:
    def __init__(
        self,
        caller: str,
        config: QuotaConfig,
        *,
        clock: Callable[[], float] = time.time,
    ):
        self.caller = caller
        self.config = config
        self._clock = clock
        # Token bucket: burst of one second worth of tokens.
        self._capacity = config.rate_per_second
        self._tokens = config.rate_per_second
        self._last_refill = clock()
        self._inflight = 0  # queued + waiting-for-slot + running
        self._running = 0
        self._admit_cap = config.max_concurrency + config.queue_capacity
        self._slots = asyncio.Semaphore(config.max_concurrency)
        # Unbounded container: admission is controlled solely by _inflight
        # vs _admit_cap, so recovery can re-admit previously accepted tasks
        # even if the configured capacity changed across a restart.
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._lock = asyncio.Lock()

    # ----- stats -----------------------------------------------------------
    @property
    def running(self) -> int:
        return self._running

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def queued(self) -> int:
        return self._queue.qsize()

    # ----- submission admission -------------------------------------------
    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._last_refill)
        if elapsed > 0:
            if math.isfinite(self._capacity):
                self._tokens = min(self._capacity, self._tokens + elapsed * self.config.rate_per_second)
            else:
                self._tokens = float("inf")
            self._last_refill = now

    def check_rate(self) -> None:
        """Consume one submit token or raise :class:`RateLimitRejected`."""
        self._refill()
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return
        retry_after = (1.0 - self._tokens) / self.config.rate_per_second
        raise RateLimitRejected(self.caller, max(0.0, retry_after))

    async def enqueue(self, task_id: str) -> None:
        """Admit a submitted task into the bounded queue."""
        async with self._lock:
            if self._inflight >= self._admit_cap:
                if self.config.queue_capacity == 0:
                    raise ConcurrencyLimitRejected(self.caller, self.config.max_concurrency)
                raise QueueFullRejected(self.caller, self.config.queue_capacity)
            self._inflight += 1
            self._queue.put_nowait(task_id)

    async def next_task(self) -> str:
        return await self._queue.get()

    async def enqueue_recovered(self, task_id: str) -> None:
        """Re-admit a task that was accepted before a restart.

        Bypasses the admission cap on purpose: the task consumed its slot
        when it was originally submitted, and recovery must never drop it.
        """
        async with self._lock:
            self._inflight += 1
            self._queue.put_nowait(task_id)

    def task_done(self) -> None:
        self._queue.task_done()
        self._inflight -= 1

    @asynccontextmanager
    async def hold_slot(self, cancel_event: asyncio.Event | None = None):
        """Acquire a concurrency slot, yielding ``granted: bool``.

        When ``cancel_event`` is supplied and fires before a slot is
        available, the wait is abandoned and the context yields ``False``
        without ever holding a permit, so a cancelled queued task does not
        wait behind the whole queue. A permit won in the cancellation race
        is released immediately to avoid leaks.
        """
        if cancel_event is None:
            await self._slots.acquire()
            self._running += 1
            try:
                yield True
            finally:
                self._running -= 1
                self._slots.release()
            return

        acquire = asyncio.create_task(self._slots.acquire())
        cancelled = asyncio.create_task(cancel_event.wait())
        await asyncio.wait({acquire, cancelled}, return_when=asyncio.FIRST_COMPLETED)

        if not acquire.done() or acquire.cancelled():
            acquire.cancel()
            try:
                await acquire
            except asyncio.CancelledError:
                pass
            else:  # race: the permit was granted just as we gave up
                self._slots.release()
            cancelled.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancelled
            yield False
            return

        acquired = True
        cancelled.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancelled
        self._running += 1
        try:
            yield True
        finally:
            self._running -= 1
            if acquired:
                self._slots.release()


class QuotaManager:
    def __init__(
        self,
        default: QuotaConfig,
        overrides: dict[str, QuotaConfig],
        *,
        clock: Callable[[], float] = time.time,
    ):
        self._default = default
        self._overrides = dict(overrides)
        self._clock = clock
        self._quotas: dict[str, CallerQuota] = {}

    def get(self, caller: str) -> CallerQuota:
        quota = self._quotas.get(caller)
        if quota is None:
            quota = CallerQuota(
                caller,
                self._overrides.get(caller, self._default),
                clock=self._clock,
            )
            self._quotas[caller] = quota
        return quota
