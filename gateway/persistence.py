"""Durable storage for tasks, idempotency bindings and tombstones.

The executor keeps its in-memory state machine exactly as before; a
persistence backend mirrors every committed mutation so a process restart
can recover.  Backends are selected by configuration:

* ``NullPersistence``   - default; pure in-memory behaviour (unchanged).
* ``SqlitePersistence`` - local single-file durable store (stdlib sqlite3).

Records are stored as JSON documents keyed by id; the in-memory objects
remain the source of truth at runtime, the store is a write-through journal
that is only read once, at startup, for recovery.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from typing import Any, Protocol

from gateway.idempotency import IdempotencyEntry
from gateway.models import TaskRecord
from gateway.states import TaskState

logger = logging.getLogger("gateway.persistence")


class PersistenceBackend(Protocol):
    """Write-through journal used by the executor and idempotency store."""

    async def save_task(self, record: TaskRecord) -> None: ...
    async def delete_task(self, task_id: str) -> None: ...
    async def load_tasks(self) -> list[TaskRecord]: ...
    async def save_idempotency(self, key: str, entry: IdempotencyEntry) -> None: ...
    async def delete_idempotency(self, key: str) -> None: ...
    async def load_idempotency(self) -> dict[str, IdempotencyEntry]: ...
    async def save_tombstone(self, task_id: str, expired_at: float) -> None: ...
    async def delete_tombstone(self, task_id: str) -> None: ...
    async def load_tombstones(self) -> dict[str, float]: ...
    async def close(self) -> None: ...


class NullPersistence:
    """No-op backend: keeps the original in-memory-only behaviour."""

    async def save_task(self, record: TaskRecord) -> None:
        return None

    async def delete_task(self, task_id: str) -> None:
        return None

    async def load_tasks(self) -> list[TaskRecord]:
        return []

    async def save_idempotency(self, key: str, entry: IdempotencyEntry) -> None:
        return None

    async def delete_idempotency(self, key: str) -> None:
        return None

    async def load_idempotency(self) -> dict[str, IdempotencyEntry]:
        return {}

    async def save_tombstone(self, task_id: str, expired_at: float) -> None:
        return None

    async def delete_tombstone(self, task_id: str) -> None:
        return None

    async def load_tombstones(self) -> dict[str, float]:
        return {}

    async def close(self) -> None:
        return None


def task_record_to_dict(record: TaskRecord) -> dict[str, Any]:
    """JSON-serialisable snapshot of a task (runtime-only fields excluded)."""
    return {
        "task_id": record.task_id,
        "idempotency_key": record.idempotency_key,
        "caller": record.caller,
        "task_type": record.task_type,
        "payload": record.payload,
        "state": record.state.value,
        "attempt": record.attempt,
        "result": record.result,
        "error_code": record.error_code,
        "error_message": record.error_message,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def task_record_from_dict(data: dict[str, Any]) -> TaskRecord:
    """Rebuild a record from its snapshot; runtime fields are re-bound."""
    return TaskRecord(
        task_id=data["task_id"],
        idempotency_key=data["idempotency_key"],
        caller=data["caller"],
        task_type=data["task_type"],
        payload=dict(data["payload"]),
        state=TaskState(data["state"]),
        attempt=int(data["attempt"]),
        result=data["result"],
        error_code=data["error_code"],
        error_message=data["error_message"],
        created_at=float(data["created_at"]),
        updated_at=float(data["updated_at"]),
    )


class SqlitePersistence:
    """Single-file durable backend built on the stdlib sqlite3 module.

    Writes are small upserts committed immediately (synchronous=NORMAL),
    so a committed state mutation survives a process crash.  sqlite3 calls
    are blocking but tiny; they run on the event loop thread, which is
    acceptable for a local single-process gateway.
    """

    def __init__(self, path: str):
        self._path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                data    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS idempotency (
                key  TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tombstones (
                task_id    TEXT PRIMARY KEY,
                expired_at REAL NOT NULL
            );
            """
        )
        self._conn.commit()
        logger.info("sqlite persistence opened: path=%s", path)

    # -- tasks ------------------------------------------------------------
    async def save_task(self, record: TaskRecord) -> None:
        data = json.dumps(task_record_to_dict(record), ensure_ascii=False)
        self._conn.execute(
            "INSERT INTO tasks(task_id, data) VALUES(?, ?) "
            "ON CONFLICT(task_id) DO UPDATE SET data=excluded.data",
            (record.task_id, data),
        )
        self._conn.commit()

    async def delete_task(self, task_id: str) -> None:
        self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
        self._conn.commit()

    async def load_tasks(self) -> list[TaskRecord]:
        rows = self._conn.execute("SELECT data FROM tasks").fetchall()
        records = [task_record_from_dict(json.loads(row[0])) for row in rows]
        logger.info("loaded %s persisted task(s) from %s", len(records), self._path)
        return records

    # -- idempotency --------------------------------------------------------
    async def save_idempotency(self, key: str, entry: IdempotencyEntry) -> None:
        data = json.dumps(
            {
                "task_id": entry.task_id,
                "request_hash": entry.request_hash,
                "expires_at": entry.expires_at,
            }
        )
        self._conn.execute(
            "INSERT INTO idempotency(key, data) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
            (key, data),
        )
        self._conn.commit()

    async def delete_idempotency(self, key: str) -> None:
        self._conn.execute("DELETE FROM idempotency WHERE key = ?", (key,))
        self._conn.commit()

    async def load_idempotency(self) -> dict[str, IdempotencyEntry]:
        rows = self._conn.execute("SELECT key, data FROM idempotency").fetchall()
        entries = {
            row[0]: IdempotencyEntry(
                task_id=(d := json.loads(row[1]))["task_id"],
                request_hash=d["request_hash"],
                expires_at=float(d["expires_at"]),
            )
            for row in rows
        }
        logger.info("loaded %s persisted idempotency entrie(s)", len(entries))
        return entries

    # -- tombstones ---------------------------------------------------------
    async def save_tombstone(self, task_id: str, expired_at: float) -> None:
        self._conn.execute(
            "INSERT INTO tombstones(task_id, expired_at) VALUES(?, ?) "
            "ON CONFLICT(task_id) DO UPDATE SET expired_at=excluded.expired_at",
            (task_id, expired_at),
        )
        self._conn.commit()

    async def delete_tombstone(self, task_id: str) -> None:
        self._conn.execute("DELETE FROM tombstones WHERE task_id = ?", (task_id,))
        self._conn.commit()

    async def load_tombstones(self) -> dict[str, float]:
        rows = self._conn.execute("SELECT task_id, expired_at FROM tombstones").fetchall()
        return {row[0]: float(row[1]) for row in rows}

    async def close(self) -> None:
        self._conn.close()
        logger.info("sqlite persistence closed: path=%s", self._path)
