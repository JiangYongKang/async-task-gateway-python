"""Idempotency key store with a finite validity window.

Rules
-----
* A key maps, for at most ``ttl`` seconds, to one task and one request hash.
* Same key + same content while valid  -> the existing task is returned
  (``reused=True``); it must never be executed a second time.
* Same key + different content while valid -> :class:`IdempotencyConflict`.
* Expired keys are lazily removed and may be reserved again.

All operations are guarded by one lock because they interleave awaits;
in a real deployment this would be a Redis SETNX-style record.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable

from gateway.errors import ErrorCode, GatewayError


class IdempotencyConflict(GatewayError):
    def __init__(self, key: str, existing_task_id: str):
        super().__init__(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            f"idempotency key {key!r} is already bound to task {existing_task_id} "
            "with a different request payload",
        )
        self.existing_task_id = existing_task_id


@dataclass(frozen=True)
class IdempotencyEntry:
    task_id: str
    request_hash: str
    expires_at: float


@dataclass(frozen=True)
class LookupResult:
    entry: IdempotencyEntry
    reused: bool          # True: live key; False: entry expired, caller may rebind


class IdempotencyStore:
    def __init__(self, ttl: float, *, clock: Callable[[], float] = time.time):
        self._ttl = ttl
        self._clock = clock
        self._entries: dict[str, IdempotencyEntry] = {}
        self._lock = asyncio.Lock()

    async def lookup(self, key: str) -> LookupResult | None:
        """Return the live entry for ``key``, an expired one, or ``None``."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            now = self._clock()
            if entry.expires_at > now:
                return LookupResult(entry=entry, reused=True)
            # Expired: drop it and report so the caller can rebind the key.
            del self._entries[key]
            return LookupResult(entry=entry, reused=False)

    async def reserve(self, key: str, task_id: str, request_hash: str) -> None:
        """Bind ``key`` to ``(task_id, request_hash)`` for the TTL window.

        A concurrent reservation of the *same* key wins; a different live
        binding is a conflict. Expired bindings are simply overwritten.
        """
        async with self._lock:
            now = self._clock()
            existing = self._entries.get(key)
            if existing is not None and existing.expires_at > now:
                if existing.task_id == task_id and existing.request_hash == request_hash:
                    return
                raise IdempotencyConflict(key, existing.task_id)
            self._entries[key] = IdempotencyEntry(
                task_id=task_id, request_hash=request_hash, expires_at=now + self._ttl
            )


    async def release(self, key: str, task_id: str) -> None:
        """Roll back a reservation, but only if it still belongs to us."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.task_id == task_id:
                del self._entries[key]

    async def purge_expired(self) -> int:
        async with self._lock:
            now = self._clock()
            dead = [k for k, e in self._entries.items() if e.expires_at <= now]
            for k in dead:
                del self._entries[k]
            return len(dead)

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)
