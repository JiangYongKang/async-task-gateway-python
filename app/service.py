"""任务服务：对外门面，组合存储、限流与执行器。"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Optional

from .config import Settings
from .executor import Executor
from .limiter import ConcurrencyLimiter, RateLimiter
from .models import (TERMINAL_STATES, RejectReason, SubmitRejected, Task,
                     TaskStatus)
from .store import TaskStore

logger = logging.getLogger(__name__)


def _hash_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


class TaskService:
    def __init__(self, settings: Optional[Settings] = None):
        self._settings = settings or Settings()
        self._store = TaskStore(self._settings.persistence_path)
        self._rate = RateLimiter(self._settings)
        self._concurrency = ConcurrencyLimiter(self._settings)
        self._executor = Executor(self._settings, self._store, self._concurrency)
        self._shutting_down = False

    @property
    def executor(self) -> Executor:
        return self._executor

    @property
    def concurrency(self) -> ConcurrencyLimiter:
        return self._concurrency

    def start(self) -> None:
        """恢复持久化任务并重新入队未完成任务，然后启动 worker。"""
        self._store.load()
        for t in self._store.all_tasks():
            if t.status == TaskStatus.PENDING and not t.cancel_requested:
                self._executor.submit(t)
        self._executor.start()

    def register_handler(self, name: str, fn) -> None:
        self._executor.register_handler(name, fn)

    def submit(self, caller: str, idempotency_key: str, payload: dict[str, Any],
               handler: str = "default") -> Task:
        if self._shutting_down:
            raise SubmitRejected(RejectReason.SHUTTING_DOWN, "服务关停中")
        payload_hash = _hash_payload(payload)

        # 1) 幂等检查（先于限流，重复提交不消耗配额）
        existing = self._store.get_by_idempotency(caller, idempotency_key)
        if existing is not None:
            if time.time() - existing.created_at <= self._settings.idempotency_window_sec:
                if existing.payload_hash != payload_hash:
                    raise SubmitRejected(
                        RejectReason.IDEMPOTENCY_CONFLICT,
                        "幂等键已存在但请求内容不同",
                    )
                return existing  # 窗口内重复提交：映射到同一任务，不重复执行
            self._store.evict_idempotency(caller, idempotency_key)

        # 2) 速率配额
        if not self._rate.allow(caller):
            raise SubmitRejected(RejectReason.RATE_LIMITED, "单位时间提交次数超限")

        # 3) 原子占位（并发下同键只有一个胜出）
        task = Task.new(caller, idempotency_key, payload, payload_hash, handler,
                        self._settings.default_max_attempts)
        task, created = self._store.put_if_absent(task)
        if not created:
            if task.payload_hash != payload_hash:
                raise SubmitRejected(RejectReason.IDEMPOTENCY_CONFLICT,
                                     "幂等键已存在但请求内容不同")
            return task

        # 4) 进入执行队列；失败则回滚占位，避免留下永不执行的记录
        try:
            self._executor.submit(task)
        except SubmitRejected:
            self._store.remove(task.id)
            raise
        return task

    def get(self, task_id: str) -> Optional[Task]:
        return self._store.get(task_id)

    def cancel(self, task_id: str) -> Task:
        task = self._store.get(task_id)
        if task is None:
            raise KeyError(task_id)
        if task.status in TERMINAL_STATES:
            return task  # 已结束：取消为空操作，语义稳定
        if task.status == TaskStatus.PENDING:
            task.transition(TaskStatus.CANCELLED)
            self._store.save(task)
            return task
        # RUNNING：协作式取消，等当前尝试结束后转入 CANCELLED
        self._executor.request_cancel(task)
        return task

    def shutdown(self) -> None:
        self._shutting_down = True
        self._executor.shutdown(self._settings.shutdown_timeout_sec)
        self._store.flush()
