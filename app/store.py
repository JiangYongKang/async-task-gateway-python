"""任务存储：内存索引 + 磁盘持久化，保证关停不丢任务。"""
from __future__ import annotations

import json
import os
import threading
from typing import Optional

from .models import Task, TaskStatus


def _task_to_dict(t: Task) -> dict:
    return {
        "id": t.id,
        "caller": t.caller,
        "idempotency_key": t.idempotency_key,
        "payload_hash": t.payload_hash,
        "payload": t.payload,
        "handler": t.handler,
        "status": t.status.value,
        "attempts": t.attempts,
        "max_attempts": t.max_attempts,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
        "error": t.error,
        "result": t.result,
        "cancel_requested": t.cancel_requested,
    }


def _task_from_dict(d: dict) -> Task:
    t = Task(
        id=d["id"],
        caller=d["caller"],
        idempotency_key=d["idempotency_key"],
        payload_hash=d["payload_hash"],
        payload=d["payload"],
        handler=d["handler"],
        status=TaskStatus(d["status"]),
        attempts=d["attempts"],
        max_attempts=d["max_attempts"],
        created_at=d["created_at"],
        updated_at=d["updated_at"],
        error=d["error"],
        result=d["result"],
        cancel_requested=d["cancel_requested"],
    )
    return t


class TaskStore:
    def __init__(self, persistence_path: str):
        self._path = persistence_path
        self._lock = threading.Lock()
        self._tasks: dict[str, Task] = {}
        self._by_idem: dict[tuple[str, str], str] = {}  # (caller, idem_key) -> task_id

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._tasks.get(task_id)

    def get_by_idempotency(self, caller: str, key: str) -> Optional[Task]:
        with self._lock:
            tid = self._by_idem.get((caller, key))
            return self._tasks.get(tid) if tid else None

    def put_if_absent(self, task: Task) -> tuple[Task, bool]:
        """幂等键不存在则写入；返回 (任务, 是否新写入)。"""
        with self._lock:
            k = (task.caller, task.idempotency_key)
            existing = self._by_idem.get(k)
            if existing is not None:
                return self._tasks[existing], False
            self._tasks[task.id] = task
            self._by_idem[k] = task.id
            self._persist_locked()
            return task, True

    def save(self, task: Task) -> None:
        with self._lock:
            self._tasks[task.id] = task
            self._persist_locked()


    def remove(self, task_id: str) -> None:
        """移除任务及其幂等索引（用于提交被限流拒绝后的回滚）。"""
        with self._lock:
            t = self._tasks.pop(task_id, None)
            if t is not None:
                self._by_idem.pop((t.caller, t.idempotency_key), None)
                self._persist_locked()

    def evict_idempotency(self, caller: str, key: str) -> None:
        """幂等窗口过期后解除键映射，允许同键再次提交为新任务。"""
        with self._lock:
            self._by_idem.pop((caller, key), None)

    def all_tasks(self) -> list[Task]:
        with self._lock:
            return list(self._tasks.values())

    def flush(self) -> None:
        with self._lock:
            self._persist_locked()

    def load(self) -> None:
        """启动时恢复；上次关停时处于 RUNNING 的任务重置为 PENDING 以便重跑。"""
        with self._lock:
            if not os.path.exists(self._path):
                return
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for d in data:
                t = _task_from_dict(d)
                if t.status == TaskStatus.RUNNING:
                    t.status = TaskStatus.PENDING
                self._tasks[t.id] = t
                self._by_idem[(t.caller, t.idempotency_key)] = t.id

    def _persist_locked(self) -> None:
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump([_task_to_dict(t) for t in self._tasks.values()], f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)
