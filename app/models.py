"""任务领域模型与状态机定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


class TaskStatus(str, enum.Enum):
    """任务状态机。

    合法转换：
      PENDING   -> RUNNING | CANCELLED
      RUNNING   -> SUCCEEDED | FAILED | CANCELLED | PENDING(重试)
      FAILED    -> PENDING(重试) | DEAD(重试耗尽)
      终态: SUCCEEDED / DEAD / CANCELLED
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD = "DEAD"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = {TaskStatus.SUCCEEDED, TaskStatus.DEAD, TaskStatus.CANCELLED}

ALLOWED_TRANSITIONS = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.PENDING,  # 重试重新入队
    },
    TaskStatus.FAILED: {TaskStatus.PENDING, TaskStatus.DEAD, TaskStatus.CANCELLED},
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.DEAD: set(),
    TaskStatus.CANCELLED: set(),
}


class RejectReason(str, enum.Enum):
    """提交被拒绝时的可区分原因。"""

    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"  # 幂等键相同但内容不同
    RATE_LIMITED = "RATE_LIMITED"  # 单位时间提交次数超限
    CONCURRENCY_LIMITED = "CONCURRENCY_LIMITED"  # 并发执行数超限且队列已满
    SHUTTING_DOWN = "SHUTTING_DOWN"  # 服务关停中，不再接受新任务


class SubmitRejected(Exception):
    """提交被拒绝，携带可区分的原因。"""

    def __init__(self, reason: RejectReason, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


class InvalidTransition(Exception):
    """非法状态转换。"""


@dataclass
class Task:
    id: str
    caller: str
    idempotency_key: str
    payload_hash: str  # 请求内容指纹，用于幂等冲突检测
    payload: dict[str, Any]
    handler: str = "default"
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: Optional[str] = None
    result: Optional[Any] = None
    cancel_requested: bool = False

    @staticmethod
    def new(caller: str, idempotency_key: str, payload: dict[str, Any],
            payload_hash: str, handler: str, max_attempts: int) -> "Task":
        return Task(
            id=uuid.uuid4().hex,
            caller=caller,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            payload=payload,
            handler=handler,
            max_attempts=max_attempts,
        )

    def transition(self, target: TaskStatus) -> None:
        if target not in ALLOWED_TRANSITIONS[self.status]:
            raise InvalidTransition(f"{self.status} -> {target} 非法")
        self.status = target
        self.updated_at = time.time()
