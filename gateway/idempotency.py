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
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from gateway.errors import ErrorCode, GatewayError

logger = logging.getLogger("gateway.idempotency")


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
    def __init__(
        self,
        ttl: float,
        *,
        clock: Callable[[], float] = time.time,
        persistence: Any = None,
    ):
        self._ttl = ttl
        self._clock = clock
        self._entries: dict[str, IdempotencyEntry] = {}
        self._lock = asyncio.Lock()
        # Optional write-through journal (gateway.persistence backend);
        # None keeps the original in-memory-only behaviour.
        self._persistence = persistence

    async def load(self) -> int:
        """Restore persisted bindings (called once at executor startup)."""
        if self._persistence is None:
            return 0
        entries = await self._persistence.load_idempotency()
        async with self._lock:
            now = self._clock()
            live = {k: e for k, e in entries.items() if e.expires_at > now}
            expired = len(entries) - len(live)
            self._entries.update(live)
        logger.info(
            "idempotency bindings restored: live=%s expired_dropped=%s ttl=%ss",
            len(live), expired, self._ttl,
        )
        return len(live)

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
            await self._persist_delete(key)
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
            await self._persist_save(key, self._entries[key])


    async def release(self, key: str, task_id: str) -> None:
        """Roll back a reservation, but only if it still belongs to us."""
        async with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.task_id == task_id:
                del self._entries[key]
                await self._persist_delete(key)

    async def purge_expired(self) -> int:
        async with self._lock:
            now = self._clock()
            dead = [k for k, e in self._entries.items() if e.expires_at <= now]
            for k in dead:
                del self._entries[k]
                await self._persist_delete(k)
            return len(dead)

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)

    # ------------------------------------------------------------------
    async def _persist_save(self, key: str, entry: IdempotencyEntry) -> None:
        if self._persistence is not None:
            await self._persistence.save_idempotency(key, entry)

    async def _persist_delete(self, key: str) -> None:
        if self._persistence is not None:
            await self._persistence.delete_idempotency(key)
