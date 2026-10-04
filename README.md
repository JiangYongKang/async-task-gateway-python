# Async Task Gateway (Python)

一套基于 FastAPI + asyncio 的异步任务执行服务，在 **提交、执行、重试、取消、关停**
各环节提供一致且可解释的状态，并对调用方做 **幂等** 与 **配额** 管控。

- 仅依赖本地运行与 `pyproject.toml` 中已有依赖（fastapi / pytest / pytest-asyncio / httpx）。
- 所有失败都返回稳定机器码 `error.code`，原因彼此可区分。

## 目录结构

```
gateway/
  states.py       状态枚举与合法状态转换表
  models.py       RetryPolicy / QuotaConfig / GatewayConfig / TaskRecord、请求哈希
  errors.py       ErrorCode 与 GatewayError（统一错误体）
  persistence.py  持久化后端：NullPersistence（默认纯内存）/ SqlitePersistence（重启恢复）
  idempotency.py  幂等键存储（TTL 窗口、内容冲突、并发抢占、写穿透持久化）
  quotas.py       按调用方：令牌桶限频 + 并发信号量 + 有界队列
  handlers.py     handler 注册表与内置 handler（echo/fail/flaky/sleep）
  executor.py     核心：提交准入、调度、重试、取消、关停、重启恢复、历史回收
  api.py          FastAPI 路由与错误码 -> HTTP 状态映射
main.py           组装执行器与 app（lifespan 负责 start/shutdown）
tests/            43 个单测（幂等 / 配额 / 生命周期 / 关停 / 持久化 / 回收 / HTTP）
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交任务。Header `X-Caller`（必填）、`X-Idempotency-Key`（可选） |
| GET  | `/tasks/{task_id}` | 查询任务状态视图 |
| POST | `/tasks/{task_id}/cancel` | 请求取消 |
| GET  | `/healthz` | 存活与统计（关停中返回 `draining`） |

提交体：`{"task_type": "echo", "payload": {...}}`。响应包含 `state`、`terminal`、
`attempt`、`result`、`error`，提交额外包含 `reused`（是否命中幂等复用）。

内置 task_type：`echo`（原样返回）、`fail`（必失败，可配 `retryable`/`delay`/`message`）、
`flaky`（按 `key` 计数，失败 `succeed_after` 次后成功）、`sleep`（睡 `seconds` 秒，
可被取消及时打断）。自定义 handler 通过 `MapHandlerRegistry.register` 注册，
签名 `async def handler(payload: dict, cancel_event: asyncio.Event)`。

## 状态机

```
PENDING ──> SCHEDULED ──> RUNNING ──> SUCCEEDED        (终态)
   │            │           │
   │            │           ├──────> FAILED            (终态：不可重试错误或重试耗尽)
   │            │           └──────> RETRY_WAIT ──> RUNNING ...
   │            │           │
   └────────────┴───────────┴──────> CANCELLED         (终态)
```

- `PENDING`：已被有界队列接纳，等待调用方 worker 取出。
- `SCHEDULED`：已出队，正在等待该调用方的并发槽（可在此阶段被取消，等待会立刻放弃）。
- `RUNNING`：handler 协程正在执行；失败后按策略进入 `RETRY_WAIT` 或直接 `FAILD`。
- `RETRY_WAIT`：指数退避中，取消会立刻终止，不再安排后续尝试。
- 只有表中列出的转换合法，任何非法转换抛 `IllegalTransition`，不会静默落库。

任务状态随时可通过 `GET /tasks/{id}` 查询；`attempt` 为已发起的尝试次数（从 1 起）。

## 幂等规则

1. 同一 `X-Idempotency-Key` 在 TTL 窗口（`GATEWAY_IDEMPOTENCY_TTL`，默认 300s）内：
   - **同内容**重复提交 → 映射到**同一个任务**，响应 `reused=true`，
     任务只执行一次；重复提交**不消耗**限频令牌。
   - **不同内容**（payload 或 task_type 不同；内容用规范化 JSON 的 SHA-256 比对）
     → `409 IDEMPOTENCY_CONFLICT`，错误体给出已绑定的 task_id；
     既不静默返回旧结果，也不覆盖旧任务。
2. 窗口过期后键可重新绑定（惰性清理）。
3. 并发相同提交：存储用锁保证只有一个抢占成功，其余并发请求复用胜者任务。
4. 若任务在准入阶段被配额拒绝，会回滚其幂等占位，避免“占位但不存在任务”。
5. **重启延续**：开启持久化后，键→任务绑定随每次变更写穿透落盘，
   重启后在剩余 TTL 窗口内仍然有效（过期判定用绝对时间戳，与重启前边界一致）。

## 持久化与重启恢复

默认（不配置 `GATEWAY_PERSISTENCE_PATH`）行为与之前完全一致：纯内存、重启即清空。
配置 `GATEWAY_PERSISTENCE_PATH=/path/to/gateway.db` 后启用 `SqlitePersistence`
（stdlib sqlite3，WAL 模式，每次状态变更同步提交）：

- **写穿透**：任务每次状态转换、attempt 递增、结果落账，以及幂等键的
  绑定/释放/过期，都在同一把执行器锁内同步写入 sqlite；
  已提交的变更在进程崩溃（含 `kill -9`）后不丢。
- **重启恢复**（`start()` 时一次性加载）：
  - 终态任务（SUCCEEDED/FAILED/CANCELLED）原样恢复，**绝不会被再次执行**；
    任务编号、attempt、result/error 与重启前完全一致。
  - `PENDING` / `SCHEDULED`：重新入队，正常调度。
  - `RUNNING`：说明该次 attempt 被重启打断、未提交结果。按**原 attempt 号**
    重新驱动（attempt 不回退、不重复计数），日志会明确打印
    `attempt N was interrupted by the restart; re-driving`。
    注意这是 at-least-once 语义：被打断的那次 attempt 若已产生外部副作用，
    重跑可能重复该副作用——只有已落库为终态的任务才能保证不重跑。
  - `RETRY_WAIT`：失败的那次 attempt 已定论，直接调度**下一次** attempt
    （attempt+1）并重新入队，不再等待剩余退避。
  - 恢复准入走 `quota.restore`，绕过容量检查：重启前已被接纳的任务
    不会因为重启后配置容量变小而被遗弃。
- **幂等延续**：重启后同键同内容仍复用原任务（`reused=true`，只执行一次）；
  同键异内容仍 `409 IDEMPOTENCY_CONFLICT`；窗口过期后仍可重新绑定，
  边界与重启前完全一致（绝对过期时间，不随重启顺延或提前）。

## 历史保留与回收

开启 `GATEWAY_HISTORY_RETENTION`（默认 3600s，`0` 表示完全关闭回收）后，
后台清扫器每 `GATEWAY_RETENTION_SWEEP_INTERVAL`（默认 60s）运行一次，
回收**同时满足**两个条件的任务：

1. 已处于终态（在途任务永不回收）；
2. 终态停留时间超过保留期，**且**其幂等键不在有效窗口内
   （窗口仍活的记录会被跳过，避免破坏“窗口内复用”语义）。

回收规则与可观测性：

- 被回收的任务留下**墓碑**（tombstone，同样持久化），保留
  `GATEWAY_TOMBSTONE_RETENTION`（默认 86400s）。期间按任务编号查询返回
  `410 TASK_EXPIRED`——明确区别于从未存在过的 `404 TASK_NOT_FOUND`；
  墓碑过期后才退化为 `TASK_NOT_FOUND`。
- 清扫的全部读写都在执行器同一把锁内完成，与提交、执行、查询并发时
  不会出现状态错乱；日志逐条打印回收判定依据（任务号、终态、停留时长、
  幂等窗口是否存活）。
- 幂等键过期条目随清扫惰性清理，历史不会无限增长。

## 配额规则（按调用方 `X-Caller` 维度，互相隔离）

准入顺序固定为：**幂等 → 限频 → 并发/队列**。

- **提交限频**：令牌桶，速率 `rate_per_second`，突发容量为 1 秒令牌量。
  无令牌时拒绝 `429 RATE_LIMIT_REJECTED`，并带 `retry_after`（秒）。
- **并发上限** `max_concurrency`：同一调用方同时 `RUNNING` 的任务数绝不越界；
  等待槽的任务数也计入在途。
- **有界排队** `queue_capacity`：
  - 超出 `max_concurrency` 的任务进入 PENDING 队列，**FIFO** 执行；
  - 队列关闭（容量 0）且槽满 → `429 CONCURRENCY_LIMIT_REJECTED`；
  - 队列也满 → `429 QUEUE_FULL_REJECTED`。
  - 在途任务总数硬性上限为 `max_concurrency + queue_capacity`，
    因此任何调用方都不会无序堆积直至耗尽资源。

三类拒绝原因分别有独立错误码，调用方可以区别处理。

## 重试策略

`RetryPolicy(max_attempts, base_delay, max_delay, multiplier)`：

- `max_attempts` 含首次执行；`1` 表示不重试。
- 第 n 次尝试前的退避：`min(max_delay, base_delay * multiplier**(n-2))`。
- handler 抛 `TaskExecutionError(retryable=True)` 才计入重试预算；
  `retryable=False` 立即 `FAILED`；重试耗尽进入确定终态 `FAILED`，
  并保留最后的 `error.code=HANDLER_FAILED` 与错误信息。
- 其他意外异常同样兜底为 `FAILED`，不会让任务滞留非终态。

## 取消语义

- 对 `PENDING / SCHEDULED / RETRY_WAIT / RUNNING`：`POST .../cancel` 立即生效返回；
  - 排队/等槽中的任务尽快进入 `CANCELLED`（等槽动作可被取消唤醒，不排在队尾死等）；
  - 运行中的 handler 协程会被 `cancel()`，任务进入 `CANCELLED`；
  - 退避中的等待立即结束，且不再发起后续尝试。
- 对已终态任务再次取消 → `409 TASK_NOT_CANCELLABLE`（稳定、可区分）。
- 对不存在的任务 → `404 TASK_NOT_FOUND`。
- `CANCELLED` 是终态，取消不可逆、不重试。

## 关停语义

收到 SIGTERM / lifespan 关闭时执行 `shutdown()`：

1. 立即进入关停态，新提交一律 `503 SHUTTING_DOWN`；
2. 在 `GATEWAY_SHUTDOWN_GRACE`（默认 10s）宽限内等待所有在途任务自然到终态
   （成功/失败/已取消都会被等待，不丢任务）；
3. 宽限到期仍存活的任务：设置取消事件、取消运行中的 handler 协程，
   排队/退避中的任务直接终结为 `CANCELLED`（error 标记 `SHUTTING_DOWN`）；
4. 再给一个短暂强制窗口，极端情况下用安全网把残留非终态记录直接终结，
   保证“不留不可恢复的不一致状态”。关停完成后所有任务均为终态。

## 错误码一览

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `IDEMPOTENCY_CONFLICT` | 409 | 幂等键相同但请求内容不同 |
| `RATE_LIMIT_REJECTED` | 429 | 提交频率超限（含 `retry_after`） |
| `CONCURRENCY_LIMIT_REJECTED` | 429 | 并发已满且不允许排队 |
| `QUEUE_FULL_REJECTED` | 429 | 并发与有界队列均满 |
| `TASK_NOT_FOUND` | 404 | 任务不存在（含墓碑已过期） |
| `TASK_EXPIRED` | 410 | 任务曾存在，终态记录已过保留期被回收 |
| `TASK_NOT_CANCELLABLE` | 409 | 取消已终态任务 |
| `SHUTTING_DOWN` | 503 | 关停期间提交 |
| `UNKNOWN_TASK_TYPE` | 400 | 未注册的 task_type（准入前拒绝） |
| `HANDLER_FAILED` | —（记录在任务 `error` 中） | handler 执行失败 |
| `CANCELLED` | —（任务终态） | 被用户取消 |

## 可配置项（环境变量）

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `GATEWAY_IDEMPOTENCY_TTL` | 300 | 幂等键有效窗口（秒） |
| `GATEWAY_SHUTDOWN_GRACE` | 10 | 关停宽限（秒），超时强制取消 |
| `GATEWAY_MAX_CONCURRENCY` | 4 | 每调用方并发上限 |
| `GATEWAY_RATE_PER_SECOND` | 20 | 每调用方每秒提交令牌 |
| `GATEWAY_QUEUE_CAPACITY` | 16 | 每调用方有界队列容量（0 = 不排队） |
| `GATEWAY_MAX_ATTEMPTS` | 3 | 最大尝试次数（含首次） |
| `GATEWAY_RETRY_BASE_DELAY` | 0.2 | 首次重试退避基准（秒） |
| `GATEWAY_RETRY_MAX_DELAY` | 5.0 | 退避上限（秒） |
| `GATEWAY_LOG_LEVEL` | INFO | 日志级别 |
| `GATEWAY_PERSISTENCE_PATH` | 空（关闭） | sqlite 持久化文件路径；不设置则保持纯内存行为 |
| `GATEWAY_HISTORY_RETENTION` | 3600 | 终态任务保留期（秒），0 = 不回收 |
| `GATEWAY_RETENTION_SWEEP_INTERVAL` | 60 | 回收清扫间隔（秒） |
| `GATEWAY_TOMBSTONE_RETENTION` | 86400 | 回收后墓碑保留期（秒），期间查询返回 `TASK_EXPIRED` |

## 兼容范围

- 不设置 `GATEWAY_PERSISTENCE_PATH` 时，所有运行时行为与上一版完全一致
  （纯内存、重启清空），原有接口、错误码、`stats()` 字段均未改变。
- 新增能力全部为**可选配置**：持久化路径、保留期、清扫间隔、墓碑保留期。
- 接口层面只新增了一个错误码 `TASK_EXPIRED`（HTTP 410），
  仅在开启回收且任务确实被回收后才可能出现；原有错误码语义不变。
- `GatewayConfig` / `TaskExecutor` / `IdempotencyStore` 的新参数均有默认值，
  既有代码构造方式无需修改。

需要按调用方差异化配额时，用代码构造 `GatewayConfig(quotas={"caller-a": ...})`
（见测试 `test_quotas_are_isolated_per_caller`）。

## 本地验证

```bash
# 依赖由 uv 管理（已在 .venv 中），全部测试仅使用本地事件循环/ASGI 内存传输
uv run pytest -q                 # 43 个测试；-s 下打印关键输入与判定依据
uv run pytest tests/test_idempotency.py -q
uv run pytest tests/test_quotas.py -q
uv run pytest tests/test_lifecycle.py -q
uv run pytest tests/test_shutdown.py -q
uv run pytest tests/test_persistence.py -q   # 重启恢复 / 幂等延续
uv run pytest tests/test_retention.py -q     # 历史回收边界与并发
uv run pytest tests/test_api.py -q

# 启动真实服务（开启持久化）
export GATEWAY_PERSISTENCE_PATH=/tmp/gateway.db
uv run uvicorn main:app --port 18099

curl -s -X POST localhost:18099/tasks \
  -H 'X-Caller: demo' -H 'X-Idempotency-Key: k1' -H 'Content-Type: application/json' \
  -d '{"task_type":"echo","payload":{"v":1}}'
```

手动复现重启恢复（以上服务已启动、提交了一个长时间 `sleep` 任务后）：

```bash
# 1. 提交一个 30s 的任务并确认 RUNNING
TID=$(curl -s -X POST localhost:18099/tasks -H 'X-Caller: demo' \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"sleep","payload":{"seconds":30}}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["task_id"])')
curl -s localhost:18099/tasks/$TID   # state=RUNNING

# 2. 模拟崩溃（不是优雅关停）
kill -9 $(pgrep -f "uvicorn main:app")

# 3. 用同一 GATEWAY_PERSISTENCE_PATH 重启
uv run uvicorn main:app --port 18099
#    日志可见: recovery: task=... attempt=1 was interrupted ... re-driving
#             recovery complete: restored=N requeued=1

# 4. 任务编号不变，被续跑到确定终态
curl -s localhost:18099/tasks/$TID   # 30s 后 state=SUCCEEDED, attempt=1
```

测试覆盖与断言要点：

- **并发提交**：25 任务突发，断言观测到的并发执行数始终 `<= max_concurrency`，
  且全部成功；FIFO 顺序；按调用方配额隔离。
- **重复幂等键**：6 次相同提交只执行 1 次；20 个并发相同提交只产生 1 个任务；
  同键异内容返回可区分的 `IDEMPOTENCY_CONFLICT`；TTL 过期后可重新绑定。
- **配额超限**：分别断言 `CONCURRENCY_LIMIT_REJECTED`、`QUEUE_FULL_REJECTED`、
  `RATE_LIMIT_REJECTED`（含 `retry_after`）三种不同理由；幂等复用不消耗令牌。
- **取消**：RUNNING / RETRY_WAIT / 排队中取消均到 `CANCELLED`，
  排队任务不被执行，终态取消报 `TASK_NOT_CANCELLABLE`。
- **关停**：宽限内自然排空、超时强制取消、排队任务不执行、退避中任务取消、
  关停中提交报 `SHUTTING_DOWN`，且结束后所有任务均为终态。
- **重启恢复**（`test_persistence.py`，用 `simulate_crash()` 模拟进程崩溃后
  在同一 sqlite 路径上重建执行器）：RUNNING/PENDING 任务续跑到终态且编号、
  attempt 不变；RETRY_WAIT 任务调度下一次 attempt；已成功任务零重跑
  （handler 调用计数断言）；同键复用与同键异内容冲突在重启后依旧可区分；
  TTL 过期键重启后可重新绑定。
- **历史回收**（`test_retention.py`）：超保留期终态任务被回收且查询返回
  `TASK_EXPIRED`（区别于 `TASK_NOT_FOUND`）；在途任务永不回收；
  幂等窗口仍活的记录被跳过、窗口过期后才可回收重绑；墓碑过期退化为
  `TASK_NOT_FOUND`；墓碑跨重启保留；回收与提交/执行/查询并发压测下
  全部任务终态一致、无编号混淆、无静默丢失。
- 各用例通过 `logging` 打印关键输入（caller/key/hash/payload）与判定依据
  （reused、error code、观测并发数、恢复决策、回收决策、最终状态）。
