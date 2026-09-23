"""按调用方维度的并发与速率配额。"""
from __future__ import annotations

import threading
import time

from .config import Settings


class RateLimiter:
    """固定窗口计数限流：窗口内提交次数超限即拒绝。"""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._lock = threading.Lock()
        self._windows: dict[str, tuple[int, int]] = {}  # caller -> (window_start_sec, count)

    def allow(self, caller: str) -> bool:
        now = time.time()
        w = int(now // self._settings.rate_limit_window_sec)
        with self._lock:
            start, count = self._windows.get(caller, (w, 0))
            if start != w:
                start, count = w, 0
            if count >= self._settings.rate_limit_per_caller:
                self._windows[caller] = (start, count)
                return False
            self._windows[caller] = (start, count + 1)
            return True


class ConcurrencyLimiter:
    """按调用方 + 全局的并发计数信号量（非阻塞获取）。"""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._lock = threading.Lock()
        self._running: dict[str, int] = {}
        self._total = 0

    def try_acquire(self, caller: str) -> bool:
        with self._lock:
            cur = self._running.get(caller, 0)
            if cur >= self._settings.max_concurrency_per_caller:
                return False
            if self._total >= self._settings.max_global_concurrency:
                return False
            self._running[caller] = cur + 1
            self._total += 1
            return True

    def release(self, caller: str) -> None:
        with self._lock:
            self._running[caller] -= 1
            self._total -= 1

    def running_of(self, caller: str) -> int:
        with self._lock:
            return self._running.get(caller, 0)

    def running_total(self) -> int:
        with self._lock:
            return self._total
