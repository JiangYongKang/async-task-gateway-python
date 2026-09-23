# async-task-gateway-python

基于 FastAPI 的异步任务网关：任务在提交、执行、重试、取消与关停等环节都有一致且可解释的状态。

## 架构

```
HTTP (main.py) → TaskService (app/service.py)
                      ├─ TaskStore          内存索引 + JSON 落盘（app/store.py）
                      ├─ RateLimiter        按调用方固定窗口限流（app/limiter.py）
                      ├─ ConcurrencyLimiter 按调用方 + 全局并发信号量
                      └─ Executor           后台线程池执行 / 重试 / 取消（app/executor.py）
```

## 状态机

```
PENDING → RUNNING | CANCELLED
RUNNING → SUCCEEDED | FAILED | CANCELLED | PENDING(重试重新入队)
FAILED  → PENDING(重试) | DEAD(重试耗尽) | CANCELLED(取消优先)
SUCCEEDED / DEAD / CANCELLED 为终态，不再转换
```

非法转换会抛出 `InvalidTransition`。每次转换即落盘，保证关停后状态可恢复。

## 幂等规则

- 提交时以 `(caller, idempotency_key)` 作为幂等键，并对 `payload` 计算 SHA-256 指纹。
- 有效窗口（`idempotency_window_sec`）内：
  - 键相同且内容相同 → 返回同一个任务，**不会重复执行**（并发提交由存储层 `put_if_absent` 原子占位保证只有一个胜出）。
  - 键相同但内容不同 → 拒绝，原因 `IDEMPOTENCY_CONFLICT`，不覆盖、不静默返回旧结果。
- 窗口过期后解除键映射，允许同键作为新任务提交。
- 幂等检查先于限流，重复提交不消耗配额。

## 配额规则

| 维度 | 超限行为 | 拒绝原因 |
|---|---|---|
| 单调用方单位时间提交次数 | 拒绝 | `RATE_LIMITED` |
| 单调用方 / 全局并发执行数 | 排队（worker 内延迟重入队，不会越过上限） | — |
| 等待队列长度 | 拒绝并回滚占位，不留永不执行的记录 | `CONCURRENCY_LIMITED` |
| 服务关停中 | 拒绝 | `SHUTTING_DOWN` |

HTTP 层对被拒绝的提交返回 `429`，`detail.reason` 携带上述可区分原因。

## 取消语义

- `PENDING` → 立即转 `CANCELLED`，处理器不会执行。
- `RUNNING` → 协作式取消：置 `cancel_requested`，当前尝试结束后转 `CANCELLED`（取消优先于重试）。
- 终态 → 空操作，返回当前状态，语义稳定。

## 关停语义

- 停止接受新任务（`SHUTTING_DOWN`）。
- 在途任务执行完毕或等待 `shutdown_timeout_sec`；未执行的保持 `PENDING`。
- 全量落盘后退出；恢复时 `RUNNING` 重置为 `PENDING` 重新入队，不丢任务、不留不一致状态。

## 可配置项（app/config.py `Settings`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `max_concurrency_per_caller` | 2 | 单调用方同时执行上限 |
| `max_global_concurrency` | 8 | 全局同时执行上限（worker 数） |
| `rate_limit_per_caller` | 20 | 单调用方窗口内提交上限 |
| `rate_limit_window_sec` | 60 | 限流窗口（秒） |
| `max_queue_size` | 100 | 等待队列上限 |
| `idempotency_window_sec` | 3600 | 幂等键有效窗口 |
| `default_max_attempts` | 3 | 最大尝试次数（含首次） |
| `retry_backoff_base_sec` | 0.05 | 重试退避基数（指数退避） |
| `shutdown_timeout_sec` | 10 | 关停等待超时 |
| `persistence_path` | tasks.json | 任务持久化文件 |

## HTTP 接口

- `POST /tasks` `{caller, idempotency_key, payload, handler}` → 201 / 429(reason)
- `GET /tasks/{task_id}` → 任务状态
- `POST /tasks/{task_id}/cancel` → 取消

## 本地验证

```bash
uv sync                                  # 安装依赖
python -m unittest discover -s tests -v  # 运行测试（仅标准库，日志含关键输入与判定依据）
uv run uvicorn main:app                  # 启动服务
```

测试覆盖：并发提交同幂等键（断言只执行一次）、幂等冲突拒绝、速率/并发/队列配额（断言并发峰值不超限、拒绝原因可区分）、失败重试至 `DEAD`、取消排队/终态任务、关停后拒绝与恢复续跑。
