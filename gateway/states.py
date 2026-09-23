"""Task lifecycle states and the legal transition table.

States
------
PENDING     accepted and waiting in the caller's bounded queue
SCHEDULED   dequeued by the caller worker, waiting for a concurrency slot
RUNNING     the handler coroutine is executing an attempt
RETRY_WAIT  an attempt failed; backing off before the next attempt
SUCCEEDED   terminal: handler returned a result
FAILED      terminal: all attempts exhausted (or a non-retryable failure)
CANCELLED   terminal: cancelled by the caller or by shutdown draining

Only transitions present in ``LEGAL_TRANSITIONS`` are allowed; any other
transition raises ``IllegalTransition`` so an inconsistent state can never
be persisted silently.
"""

from __future__ import annotations

from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL_STATES

    @property
    def is_cancellable(self) -> bool:
        return self in _CANCELLABLE_STATES


_TERMINAL_STATES = frozenset({TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED})
_CANCELLABLE_STATES = frozenset(
    {TaskState.PENDING, TaskState.SCHEDULED, TaskState.RUNNING, TaskState.RETRY_WAIT}
)


class IllegalTransition(Exception):
    """Raised when a state transition violates the state machine."""

    def __init__(self, src: TaskState, dst: TaskState):
        super().__init__(f"illegal task state transition: {src.value} -> {dst.value}")
        self.src = src
        self.dst = dst


# Legal forward transitions for the task state machine.
LEGAL_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset({TaskState.SCHEDULED, TaskState.CANCELLED}),
    TaskState.SCHEDULED: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.RUNNING: frozenset(
        {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.RETRY_WAIT, TaskState.CANCELLED}
    ),
    TaskState.RETRY_WAIT: frozenset({TaskState.RUNNING, TaskState.CANCELLED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


def check_transition(src: TaskState, dst: TaskState) -> None:
    """Raise :class:`IllegalTransition` if ``src -> dst`` is not allowed."""
    if dst not in LEGAL_TRANSITIONS.get(src, frozenset()):
        raise IllegalTransition(src, dst)
