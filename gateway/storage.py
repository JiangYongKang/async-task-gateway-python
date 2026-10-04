"""Durable storage for tasks, idempotency keys and reclaimed-task tombstones.

SQLite (stdlib) in WAL mode: every state mutation is persisted before the
executor moves on, so a process restart can reload the exact same records.
This module is synchronous and deliberately small; all callers run on the
event loop thread and each operation is a single short transaction.

Tables
------
tasks              one row per task record (terminal and non-terminal)
idempotency_keys   key -> (task_id, request_hash, expires_at)
tombstones         reclaimed terminal tasks: proof the task *did* exist,
                   so lookups can answer "reclaimed" instead of "not found"
"""

from __future__ import annotations

import json
import logging
import sqlite3

from gateway.idempotency import IdempotencyEntry
from gateway.models import TaskRecord, Tombstone
from gateway.states import TaskState

logger = logging.getLogger("gateway.storage")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT,
    caller TEXT NOT NULL,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    result TEXT,
    error_code TEXT,
    error_message TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS tombstones (
    task_id TEXT PRIMARY KEY,
    caller TEXT NOT NULL,
    task_type TEXT NOT NULL,
    final_state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    error_code TEXT,
    error_message TEXT,
    created_at REAL NOT NULL,
    finished_at REAL NOT NULL,
    reclaimed_at REAL NOT NULL,
    expires_at REAL NOT NULL
);
"""


def _dumps(value: object) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        # Results are meant to be JSON-able; keep a readable fallback rather
        # than losing the record.
        return json.dumps(repr(value), ensure_ascii=False)


class SQLiteTaskStore:
    """SQLite-backed durable store. ``path=':memory:'`` is allowed."""

    def __init__(self, path: str):
        self._path = path
        self._conn = sqlite3.connect(path, isolation_level=None)  # autocommit
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        logger.info("task store opened: path=%s", path)

    # ----- tasks -----------------------------------------------------------
    def upsert_task(self, task: TaskRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO tasks (task_id, idempotency_key, caller, task_type,
                               payload, state, attempt, result, error_code,
                               error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                idempotency_key=excluded.idempotency_key,
                state=excluded.state, attempt=excluded.attempt,
                result=excluded.result, error_code=excluded.error_code,
                error_message=excluded.error_message,
                updated_at=excluded.updated_at
            """,
            (
                task.task_id, task.idempotency_key, task.caller, task.task_type,
                _dumps(task.payload) or "{}", task.state.value, task.attempt,
                _dumps(task.result), task.error_code, task.error_message,
                task.created_at, task.updated_at,
            ),
        )

    def delete_task(self, task_id: str) -> None:
        self._conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))

    def load_tasks(self) -> list[TaskRecord]:
        rows = self._conn.execute(
            "SELECT task_id, idempotency_key, caller, task_type, payload, state,"
            " attempt, result, error_code, error_message, created_at, updated_at"
            " FROM tasks ORDER BY created_at, task_id"
        ).fetchall()
        tasks: list[TaskRecord] = []
        for (task_id, idem_key, caller, task_type, payload, state, attempt,
             result, error_code, error_message, created_at, updated_at) in rows:
            tasks.append(TaskRecord(
                task_id=task_id,
                idempotency_key=idem_key,
                caller=caller,
                task_type=task_type,
                payload=json.loads(payload),
                state=TaskState(state),
                attempt=attempt,
                result=json.loads(result) if result is not None else None,
                error_code=error_code,
                error_message=error_message,
                created_at=created_at,
                updated_at=updated_at,
            ))
        return tasks

    # ----- idempotency keys -------------------------------------------------
    def put_idempotency(self, key: str, entry: IdempotencyEntry) -> None:
        self._conn.execute(
            "INSERT INTO idempotency_keys (key, task_id, request_hash, expires_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(key) DO UPDATE SET task_id=excluded.task_id,"
            " request_hash=excluded.request_hash, expires_at=excluded.expires_at",
            (key, entry.task_id, entry.request_hash, entry.expires_at),
        )

    def delete_idempotency(self, key: str) -> None:
        self._conn.execute("DELETE FROM idempotency_keys WHERE key = ?", (key,))

    def load_idempotency(self) -> dict[str, IdempotencyEntry]:
        rows = self._conn.execute(
            "SELECT key, task_id, request_hash, expires_at FROM idempotency_keys"
        ).fetchall()
        return {
            key: IdempotencyEntry(task_id=task_id, request_hash=request_hash,
                                  expires_at=expires_at)
            for key, task_id, request_hash, expires_at in rows
        }

    # ----- tombstones --------------------------------------------------------
    def put_tombstone(self, tombstone: Tombstone) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO tombstones (task_id, caller, task_type,"
            " final_state, attempt, error_code, error_message, created_at,"
            " finished_at, reclaimed_at, expires_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                tombstone.task_id, tombstone.caller, tombstone.task_type,
                tombstone.final_state.value, tombstone.attempt,
                tombstone.error_code, tombstone.error_message,
                tombstone.created_at, tombstone.finished_at,
                tombstone.reclaimed_at, tombstone.expires_at,
            ),
        )

    def delete_tombstone(self, task_id: str) -> None:
        self._conn.execute("DELETE FROM tombstones WHERE task_id = ?", (task_id,))

    def load_tombstones(self) -> list[Tombstone]:
        rows = self._conn.execute(
            "SELECT task_id, caller, task_type, final_state, attempt, error_code,"
            " error_message, created_at, finished_at, reclaimed_at, expires_at"
            " FROM tombstones"
        ).fetchall()
        return [
            Tombstone(
                task_id=task_id, caller=caller, task_type=task_type,
                final_state=TaskState(final_state), attempt=attempt,
                error_code=error_code, error_message=error_message,
                created_at=created_at, finished_at=finished_at,
                reclaimed_at=reclaimed_at, expires_at=expires_at,
            )
            for (task_id, caller, task_type, final_state, attempt, error_code,
                 error_message, created_at, finished_at, reclaimed_at,
                 expires_at) in rows
        ]

    # ----- lifecycle ---------------------------------------------------------
    def close(self) -> None:
        self._conn.close()
        logger.info("task store closed: path=%s", self._path)
