"""Error codes and domain exceptions for the task gateway.

This file is intentionally limited to type definitions in the skeleton stage.
"""

from __future__ import annotations


class ErrorCode:
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    CONCURRENCY_LIMIT_REJECTED = "CONCURRENCY_LIMIT_REJECTED"
    RATE_LIMIT_REJECTED = "RATE_LIMIT_REJECTED"
    QUEUE_FULL_REJECTED = "QUEUE_FULL_REJECTED"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    TASK_NOT_CANCELLABLE = "TASK_NOT_CANCELLABLE"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    CANCELLED = "CANCELLED"
    UNKNOWN_TASK_TYPE = "UNKNOWN_TASK_TYPE"
    HANDLER_FAILED = "HANDLER_FAILED"
    TASK_RECLAIMED = "TASK_RECLAIMED"
    EXECUTION_INTERRUPTED = "EXECUTION_INTERRUPTED"


class GatewayError(Exception):
    """Base class for all gateway-level errors with a stable machine code."""

    def __init__(self, code: str, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retry_after = retry_after

    def to_dict(self) -> dict:
        payload = {"error": {"code": self.code, "message": self.message}}
        if self.retry_after is not None:
            payload["error"]["retry_after"] = self.retry_after
        return payload
