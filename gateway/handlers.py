"""Task handler registry and built-in handlers used by tests/demos.

A handler is ``async def handler(payload, cancel_event) -> result``.
Handlers should cooperatively observe ``cancel_event`` (an asyncio.Event)
where they can; cancellation is also delivered by cancelling the task.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from gateway.errors import ErrorCode, GatewayError


class TaskExecutionError(GatewayError):
    """Failure raised by a handler; counted against the retry budget."""

    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(ErrorCode.HANDLER_FAILED, message)
        self.retryable = retryable


TaskHandler = Callable[[dict[str, Any], "asyncio.Event"], Awaitable[Any]]


class HandlerRegistry(Protocol):
    def register(self, task_type: str, handler: TaskHandler) -> None: ...
    def get(self, task_type: str) -> TaskHandler: ...


class MapHandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(self, task_type: str, handler: TaskHandler) -> None:
        if task_type in self._handlers:
            raise ValueError(f"handler for task_type {task_type!r} already registered")
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> TaskHandler:
        try:
            return self._handlers[task_type]
        except KeyError:
            raise GatewayError(
                ErrorCode.UNKNOWN_TASK_TYPE, f"unknown task_type {task_type!r}"
            ) from None


# --------------------------------------------------------------------------
# Built-in handlers
# --------------------------------------------------------------------------
async def echo_handler(payload: dict[str, Any], cancel_event: asyncio.Event) -> Any:
    return {"echo": payload}


async def fail_handler(payload: dict[str, Any], cancel_event: asyncio.Event) -> Any:
    """Always fail. ``retryable`` defaults to true to exercise retries."""
    await asyncio.sleep(payload.get("delay", 0.0))
    raise TaskExecutionError(
        payload.get("message", "forced failure"),
        retryable=bool(payload.get("retryable", True)),
    )


async def flaky_handler(payload: dict[str, Any], cancel_event: asyncio.Event) -> Any:
    """Succeed once a stateful counter reaches ``succeed_after`` failures."""
    key = payload["key"]
    succeed_after = int(payload.get("succeed_after", 1))
    counter = flaky_handler.__dict__.setdefault("_counters", {})
    counter[key] = counter.get(key, 0) + 1
    await asyncio.sleep(payload.get("delay", 0.0))
    if counter[key] <= succeed_after:
        raise TaskExecutionError(f"flaky failure {counter[key]}/{succeed_after}")
    return {"ok": True, "attempts": counter[key]}


async def sleep_handler(payload: dict[str, Any], cancel_event: asyncio.Event) -> Any:
    """Sleep up to ``seconds``; cooperatively returns early when cancelled."""
    seconds = float(payload.get("seconds", 1.0))
    try:
        while seconds > 0:
            step = min(0.05, seconds)
            await asyncio.sleep(step)
            seconds -= step
            if cancel_event.is_set():
                raise asyncio.CancelledError
    except asyncio.CancelledError:
        raise
    return {"slept": True}


def build_default_registry() -> MapHandlerRegistry:
    registry = MapHandlerRegistry()
    registry.register("echo", echo_handler)
    registry.register("fail", fail_handler)
    registry.register("flaky", flaky_handler)
    registry.register("sleep", sleep_handler)
    return registry
