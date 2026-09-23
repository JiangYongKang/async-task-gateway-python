"""Configuration value objects, request hashing and the task record.

Everything in this module is plain data + small pure functions; no async
scheduling lives here so it can be unit-tested in isolation.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any

from gateway.states import TaskState, check_transition


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff policy.

    ``max_attempts`` counts the *first* execution: ``1`` means no retry.
    Delay before attempt ``n`` (n >= 2) is
    ``min(max_delay, base_delay * multiplier ** (n - 2))``.
    """

    max_attempts: int = 1
    base_delay: float = 0.0
    max_delay: float = 0.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("retry delays must be non-negative")
        if self.multiplier < 1.0:
            raise ValueError("multiplier must be >= 1.0")

    def delay_for(self, attempt: int) -> float:
        """Backoff delay (seconds) before the given 1-based attempt."""
        if attempt <= 1:
            return 0.0
        raw = self.base_delay * (self.multiplier ** (attempt - 2))
        if self.max_delay > 0:
            return min(self.max_delay, raw)
        return raw


@dataclass(frozen=True)
class QuotaConfig:
    """Per-caller quota.

    max_concurrency  tasks executing simultaneously
    rate_per_second  sustained submit-token rate (token bucket)
    queue_capacity   PENDING tasks allowed to wait beyond the concurrency
                     limit; 0 means excess submits are rejected immediately
    """

    max_concurrency: int = 1
    rate_per_second: float = float("inf")
    queue_capacity: int = 0

    def __post_init__(self) -> None:
        if self.max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if self.queue_capacity < 0:
            raise ValueError("queue_capacity must be >= 0")
        if self.rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")

    def effective_queue_capacity(self) -> int:
        return self.queue_capacity


@dataclass(frozen=True)
class GatewayConfig:
    idempotency_ttl: float = 300.0
    shutdown_grace: float = 10.0
    default_retry: RetryPolicy = field(default_factory=RetryPolicy)
    quotas: dict[str, QuotaConfig] = field(default_factory=dict)
    default_quota: QuotaConfig = field(default_factory=QuotaConfig)

    def __post_init__(self) -> None:
        if self.idempotency_ttl <= 0:
            raise ValueError("idempotency_ttl must be > 0")
        if self.shutdown_grace < 0:
            raise ValueError("shutdown_grace must be >= 0")

    def retry_for(self, task_type: str) -> RetryPolicy:
        """Retry policies may later be keyed by task type; default for now."""
        return self.default_retry


def canonical_request_hash(task_type: str, payload: dict[str, Any]) -> str:
    """Stable hash of the request *content* for idempotency conflict checks.

    Two submits with the same idempotency key but different hashes are a
    conflict (the caller reused a key for a different request).
    """
    canonical = json.dumps(
        {"task_type": task_type, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class TaskRecord:
    task_id: str
    idempotency_key: str | None
    caller: str
    task_type: str
    payload: dict[str, Any]
    state: TaskState
    attempt: int = 0
    result: Any = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    cancel_event: Any = None  # asyncio.Event, bound lazily

    def transition_to(
        self,
        state: TaskState,
        *,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Apply a state transition, rejecting illegal ones loudly."""
        check_transition(self.state, state)
        self.state = state
        if error_code is not None:
            self.error_code = error_code
        if error_message is not None:
            self.error_message = error_message
        self.updated_at = time.time()

    def to_view(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "idempotency_key": self.idempotency_key,
            "caller": self.caller,
            "task_type": self.task_type,
            "state": self.state.value,
            "terminal": self.state.is_terminal,
            "attempt": self.attempt,
            "result": self.result,
            "error": (
                None
                if self.error_code is None
                else {"code": self.error_code, "message": self.error_message}
            ),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
