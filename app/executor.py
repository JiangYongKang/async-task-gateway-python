"""执行器：后台线程池执行任务，支持重试、取消与优雅关停。

不变式：每次入队（submit / 重入队）都会使 queue 的未完成计数 +1，
worker 处理完一个条目后 task_done -1。因此
`queue.unfinished_tasks == 0` 等价于"没有待执行或在途的任务"，
wait_idle 与关停都依赖这一不变式。
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Callable

from .config import Settings
from .limiter import ConcurrencyLimiter
from .models import RejectReason, SubmitRejected, Task, TaskStatus
from .store import TaskStore

logger = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any]], Any]


def _default_handler(payload: dict[str, Any]) -> Any:
    return {"echo": payload}


class Executor:
    def __init__(self, settings: Settings, store: TaskStore,
                 concurrency: ConcurrencyLimiter):
        self._settings = settings
        self._store = store
        self._concurrency = concurrency
        self._handlers: dict[str, Handler] = {"default": _default_handler}
        self._queue: queue.Queue[str] = queue.Queue()
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._started = False

    def register_handler(self, name: str, fn: Handler) -> None:
        self._handlers[name] = fn

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for i in range(self._settings.max_global_concurrency):
            t = threading.Thread(target=self._worker_loop, name=f"worker-{i}",
                                 daemon=True)
            t.start()
            self._workers.append(t)

    def queue_size(self) -> int:
        return self._queue.qsize()

    def submit(self, task: Task) -> None:
        """将任务放入执行队列；关停中或队列满则抛出可区分原因的拒绝。"""
        if self._stop.is_set():
            raise SubmitRejected(RejectReason.SHUTTING_DOWN, "服务关停中，不再接受新任务")
        if self._queue.qsize() >= self._settings.max_queue_size:
            raise SubmitRejected(RejectReason.CONCURRENCY_LIMITED, "等待队列已满")
        self._queue.put(task.id)

    def request_cancel(self, task: Task) -> None:
        task.cancel_requested = True
        self._store.save(task)

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """等待所有已入队任务处理完毕（含重试退避中的任务）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._queue.unfinished_tasks == 0:
                return True
            time.sleep(0.005)
        return False

    def shutdown(self, timeout: float) -> None:
        """停止接收新任务，等待在途任务结束；未执行的保持 PENDING 并落盘。"""
        self._stop.set()
        deadline = time.time() + timeout
        for w in self._workers:
            w.join(max(0.0, deadline - time.time()))
        self._store.flush()

    # ---- 内部 ----

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                task_id = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                self._run_one(task_id)
            except Exception:  # 兜底，绝不让 worker 线程死掉
                logger.exception("worker 处理任务 %s 出现未预期异常", task_id)
            finally:
                self._queue.task_done()

    def _run_one(self, task_id: str) -> None:
        task = self._store.get(task_id)
        if task is None or task.status != TaskStatus.PENDING:
            return  # 已取消或已处理
        if not self._concurrency.try_acquire(task.caller):
            self._requeue(task.id, delay=0.01)  # 调用方并发已满，稍后重试入队
            return
        try:
            if task.cancel_requested:
                task.transition(TaskStatus.CANCELLED)
                self._store.save(task)
                return
            task.transition(TaskStatus.RUNNING)
            task.attempts += 1
            self._store.save(task)
            handler = self._handlers.get(task.handler, _default_handler)
            try:
                result = handler(task.payload)
            except Exception as exc:  # 业务异常进入重试链路
                self._on_failure(task, exc)
                return
            if task.cancel_requested:
                task.transition(TaskStatus.CANCELLED)
            else:
                task.transition(TaskStatus.SUCCEEDED)
                task.result = result
            self._store.save(task)
        finally:
            self._concurrency.release(task.caller)

    def _on_failure(self, task: Task, exc: Exception) -> None:
        task.error = f"{type(exc).__name__}: {exc}"
        task.transition(TaskStatus.FAILED)
        if task.cancel_requested:
            task.transition(TaskStatus.CANCELLED)  # 取消优先于重试
        elif task.attempts >= task.max_attempts:
            task.transition(TaskStatus.DEAD)
            logger.info("任务 %s 重试耗尽进入 DEAD: %s", task.id, task.error)
        else:
            task.transition(TaskStatus.PENDING)
            backoff = self._settings.retry_backoff_base_sec * (2 ** (task.attempts - 1))
            self._requeue(task.id, delay=backoff)
        self._store.save(task)

    def _requeue(self, task_id: str, delay: float) -> None:
        """同步重入队：先分段睡眠（可响应关停），再 put。

        在 worker 内同步完成，保证 queue 未完成计数不会出现
        "旧条目已 task_done、新条目尚未 put" 的空窗。
        """
        deadline = time.time() + delay
        while not self._stop.is_set() and time.time() < deadline:
            time.sleep(min(0.01, deadline - time.time()))
        if not self._stop.is_set():
            self._queue.put(task_id)
