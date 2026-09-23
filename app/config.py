"""可配置项。"""
from dataclasses import dataclass


@dataclass
class Settings:
    max_concurrency_per_caller: int = 2      # 单调用方同时执行上限
    max_global_concurrency: int = 8          # 全局同时执行上限
    rate_limit_per_caller: int = 20          # 单调用方窗口内提交上限
    rate_limit_window_sec: float = 60.0      # 限流窗口（秒）
    max_queue_size: int = 100                # 等待队列上限，超出即拒绝
    idempotency_window_sec: float = 3600.0   # 幂等键有效窗口
    default_max_attempts: int = 3            # 默认最大尝试次数（含首次）
    retry_backoff_base_sec: float = 0.05     # 重试退避基数
    shutdown_timeout_sec: float = 10.0       # 关停等待超时
    persistence_path: str = "tasks.json"     # 任务持久化文件
