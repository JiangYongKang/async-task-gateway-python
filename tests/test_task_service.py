"""异步任务网关测试：并发提交、幂等、配额、取消与关停。

仅依赖标准库与被测代码；日志打印关键输入与判定依据。
"""
import logging
import os
import tempfile
import threading
import time
import unittest

from app.config import Settings
from app.models import RejectReason, SubmitRejected, TaskStatus, TERMINAL_STATES
from app.service import TaskService
from app.store import TaskStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("test")


def make_service(**overrides):
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    os.remove(path)
    settings = Settings(persistence_path=path, retry_backoff_base_sec=0.01,
                        **overrides)
    svc = TaskService(settings)
    svc.start()
    return svc, path


def wait_status(svc, task_id, targets, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        t = svc.get(task_id)
        if t.status in targets:
            return t
        time.sleep(0.01)
    raise AssertionError(f"任务 {task_id} 未在 {timeout}s 内进入 {targets}，"
                         f"当前 {svc.get(task_id).status}")


class IdempotencyTest(unittest.TestCase):
    def test_concurrent_same_key_executes_once(self):
        svc, path = make_service()
        self.addCleanup(svc.shutdown)
        runs = []
        lock = threading.Lock()

        def handler(payload):
            with lock:
                runs.append(payload)
            time.sleep(0.02)
            return "ok"

        svc.register_handler("h", handler)
        n = 16
        log.info("输入: %d 个线程并发提交相同幂等键 key=same payload={'v': 1}", n)
        results, errors = [], []

        def submit():
            try:
                results.append(svc.submit("alice", "same", {"v": 1}, handler="h").id)
            except SubmitRejected as e:
                errors.append(e)

        threads = [threading.Thread(target=submit) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        svc.executor.wait_idle()

        unique_ids = set(results)
        log.info("判定依据: 返回的不同任务数=%d, 处理器实际执行次数=%d, 拒绝数=%d",
                 len(unique_ids), len(runs), len(errors))
        self.assertEqual(len(unique_ids), 1, "同一幂等键必须映射到同一任务")
        self.assertEqual(len(runs), 1, "同一幂等键只能被执行一次")
        self.assertEqual(wait_status(svc, results[0], {TaskStatus.SUCCEEDED}).status,
                         TaskStatus.SUCCEEDED)

    def test_same_key_different_payload_rejected(self):
        svc, path = make_service()
        self.addCleanup(svc.shutdown)
        t = svc.submit("alice", "k", {"v": 1})
        log.info("输入: 幂等键 k 已绑定 payload={'v': 1}，再以 payload={'v': 2} 提交")
        with self.assertRaises(SubmitRejected) as ctx:
            svc.submit("alice", "k", {"v": 2})
        log.info("判定依据: 拒绝原因=%s", ctx.exception.reason.value)
        self.assertEqual(ctx.exception.reason, RejectReason.IDEMPOTENCY_CONFLICT)
        # 原任务未被覆盖
        self.assertEqual(svc.get(t.id).payload, {"v": 1})

    def test_same_key_same_payload_returns_same_task(self):
        svc, path = make_service()
        self.addCleanup(svc.shutdown)
        t1 = svc.submit("alice", "k", {"v": 1})
        t2 = svc.submit("alice", "k", {"v": 1})
        log.info("判定依据: 两次提交返回 id 分别为 %s / %s", t1.id, t2.id)
        self.assertEqual(t1.id, t2.id)


class QuotaTest(unittest.TestCase):
    def test_rate_limit_rejected_with_distinct_reason(self):
        svc, path = make_service(rate_limit_per_caller=3)
        self.addCleanup(svc.shutdown)
        log.info("输入: 配额=3，连续提交 5 个不同幂等键任务")
        for i in range(3):
            svc.submit("alice", f"k{i}", {})
        with self.assertRaises(SubmitRejected) as ctx:
            svc.submit("alice", "k3", {})
        log.info("判定依据: 第 4 次提交拒绝原因=%s", ctx.exception.reason.value)
        self.assertEqual(ctx.exception.reason, RejectReason.RATE_LIMITED)
        # 其他调用方不受限
        svc.submit("bob", "k0", {})

    def test_concurrency_never_exceeds_limit(self):
        svc, path = make_service(max_concurrency_per_caller=2,
                                 max_global_concurrency=8)
        self.addCleanup(svc.shutdown)
        current = 0
        peak = 0
        lock = threading.Lock()

        def handler(payload):
            nonlocal current, peak
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1

        svc.register_handler("h", handler)
        n = 12
        log.info("输入: 单调用方并发上限=2，提交 %d 个任务", n)
        for i in range(n):
            svc.submit("alice", f"c{i}", {}, handler="h")
        svc.executor.wait_idle(timeout=15)
        log.info("判定依据: 观测到的并发峰值=%d，上限=2", peak)
        self.assertLessEqual(peak, 2)
        self.assertGreaterEqual(peak, 1)

    def test_queue_full_rejected_with_distinct_reason(self):
        svc, path = make_service(max_concurrency_per_caller=1,
                                 max_global_concurrency=1,
                                 max_queue_size=2)
        blocker = threading.Event()

        def handler(payload):
            blocker.wait(timeout=5)

        svc.register_handler("h", handler)
        log.info("输入: 队列上限=2、并发=1、处理器阻塞，连续提交直至拒绝")
        reasons = []
        try:
            for i in range(20):
                try:
                    svc.submit("alice", f"q{i}", {}, handler="h")
                except SubmitRejected as e:
                    reasons.append(e.reason)
        finally:
            blocker.set()
        log.info("判定依据: 拒绝原因集合=%s", {r.value for r in reasons})
        self.assertTrue(reasons, "队列满应产生拒绝")
        self.assertTrue(all(r == RejectReason.CONCURRENCY_LIMITED for r in reasons))
        svc.shutdown()


class LifecycleTest(unittest.TestCase):
    def test_retry_exhaustion_goes_dead(self):
        svc, path = make_service(default_max_attempts=3)
        self.addCleanup(svc.shutdown)
        attempts = []

        def boom(payload):
            attempts.append(1)
            raise ValueError("boom")

        svc.register_handler("boom", boom)
        t = svc.submit("alice", "r1", {}, handler="boom")
        log.info("输入: 处理器始终抛错，max_attempts=3")
        t = wait_status(svc, t.id, {TaskStatus.DEAD})
        log.info("判定依据: 终态=%s, attempts=%d, error=%s",
                 t.status, t.attempts, t.error)
        self.assertEqual(t.status, TaskStatus.DEAD)
        self.assertEqual(t.attempts, 3)
        self.assertEqual(len(attempts), 3)
        self.assertIn("boom", t.error)

    def test_cancel_pending_task(self):
        svc, path = make_service(max_concurrency_per_caller=1,
                                 max_global_concurrency=1)
        self.addCleanup(svc.shutdown)
        blocker = threading.Event()
        ran = []
        svc.register_handler("h", lambda p: (blocker.wait(timeout=5), ran.append(p)))
        first = svc.submit("alice", "c1", {}, handler="h")  # 占住唯一并发位
        wait_status(svc, first.id, {TaskStatus.RUNNING})
        second = svc.submit("alice", "c2", {}, handler="h")
        log.info("输入: 取消排队中的任务 %s（状态=%s）", second.id, second.status)
        cancelled = svc.cancel(second.id)
        blocker.set()
        svc.executor.wait_idle()
        log.info("判定依据: 取消后状态=%s, 处理器收到的 payload 数=%d",
                 cancelled.status, len(ran))
        self.assertEqual(cancelled.status, TaskStatus.CANCELLED)
        self.assertEqual(svc.get(second.id).status, TaskStatus.CANCELLED)
        self.assertEqual(len(ran), 1, "被取消的任务不得执行")

    def test_cancel_terminal_task_is_noop(self):
        svc, path = make_service()
        self.addCleanup(svc.shutdown)
        t = svc.submit("alice", "t1", {})
        t = wait_status(svc, t.id, {TaskStatus.SUCCEEDED})
        log.info("输入: 取消已终态任务（状态=%s）", t.status)
        again = svc.cancel(t.id)
        log.info("判定依据: 取消返回状态=%s", again.status)
        self.assertEqual(again.status, TaskStatus.SUCCEEDED)


class ShutdownTest(unittest.TestCase):
    def test_shutdown_rejects_new_and_recovers_pending(self):
        svc, path = make_service(max_concurrency_per_caller=1,
                                 max_global_concurrency=1)
        slow = threading.Event()

        def handler(payload):
            slow.wait(timeout=5)
            return "done"

        svc.register_handler("h", handler)
        t1 = svc.submit("alice", "s1", {}, handler="h")
        wait_status(svc, t1.id, {TaskStatus.RUNNING})
        t2 = svc.submit("alice", "s2", {}, handler="h")
        log.info("输入: t1 执行中、t2 排队时关停服务")
        slow.set()
        svc.shutdown()

        with self.assertRaises(SubmitRejected) as ctx:
            svc.submit("alice", "s3", {})
        log.info("判定依据: 关停后提交拒绝原因=%s", ctx.exception.reason.value)
        self.assertEqual(ctx.exception.reason, RejectReason.SHUTTING_DOWN)

        # 落盘状态必须一致：不存在 RUNNING 残留
        store = TaskStore(path)
        store.load()
        states = {t.id: t.status for t in store.all_tasks()}
        log.info("判定依据: 恢复后各任务状态=%s", states)
        for st in states.values():
            self.assertNotEqual(st, TaskStatus.RUNNING)
            self.assertIn(st, TERMINAL_STATES | {TaskStatus.PENDING})

        # 新实例恢复后，未完成任务继续执行至成功，不丢失
        svc2 = TaskService(Settings(persistence_path=path,
                                    retry_backoff_base_sec=0.01))
        svc2.register_handler("h", lambda p: "done")
        svc2.start()
        self.addCleanup(svc2.shutdown)
        for tid in (t1.id, t2.id):
            t = wait_status(svc2, tid, {TaskStatus.SUCCEEDED})
            log.info("判定依据: 任务 %s 恢复后终态=%s", tid, t.status)


if __name__ == "__main__":
    unittest.main()
