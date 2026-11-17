# 制品元数据熔断网关

部署系统读取制品元数据时，上游故障会引发密集重试。本项目是一个 **FastAPI + SQLite
的双 worker 熔断网关**：按制品名把 `GET` 查询转发给本地目录服务，慢查询不会阻塞其他
查询，两个 worker 进程共享同一份熔断状态，重启后状态仍然保留。

打开 <http://localhost:8000/> 即可使用网页：并发查询、查看元数据与熔断状态、切换示例
上游行为（正常 / 延迟 / 503），并对比「网关转发次数」与「上游实际访问次数」。

## 快速启动

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/
```

需要 Docker 与 Docker Compose v2。Compose 会构建两个服务：

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `upstream` | 示例本地目录服务，提供 `/artifacts/{name}`、模式控制与访问计数 | 仅集群内 9000 |
| `gateway` | 熔断网关，`uvicorn --workers 2`，SQLite 位于命名卷 `breaker-data` | 宿主机 8000 |

`gateway` 使用命名卷挂载 `/data`，容器重启或重建后熔断状态仍在。

### 不使用 Docker 的本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export UPSTREAM_URL=http://127.0.0.1:9000 GATEWAY_DB=./data/breaker.db
python -m uvicorn upstream:app --port 9000 &
python -m uvicorn app:app --port 8000 --workers 2
```

## 熔断语义

状态按**制品名**独立维护，存放在 SQLite（WAL、写事务 `BEGIN IMMEDIATE`），因此对两个
worker 进程共享，并对重启持久。

* **CLOSED**：转发请求。失败（连接失败 / 超时 / 5xx）按**完成顺序**连续计数；
  其它响应（2xx/3xx/4xx）把计数清零。连续第 3 次失败立即熔断。
* **OPEN**：所有请求直接返回 **503**（带 `Retry-After`），**不访问上游**。
  冷却 **2 秒**后，恰好一个请求通过原子 CAS 成为唯一的**恢复探测**；其余请求继续 503。
* **HALF_OPEN**：只有一个探测在飞。探测成功 → CLOSED 并清零；探测失败或超时 →
  重新 OPEN 并重新计算 2 秒冷却。探测进程崩溃后，探测槽位在租约到期 / 重启时释放，
  可以再次探测。
* 每次调用总时长上限 **1 秒**（`asyncio.wait_for`），连接、读取、解析合计计算。

### 旧结果不会覆盖新结论

* 准入时记录该次调用所属的 **epoch**。若调用期间熔断已打开（epoch 变化），这次结果
  被视为过期：**熔断前发出、熔断后才成功的慢请求既不会清零失败数，也不会让熔断恢复**
  （响应中标记 `stale_success_ignored`）。
* 探测结果携带 **probe generation**。旧代次探测迟到时被丢弃（`stale_probe_ignored`），
  不会覆盖新代次的判定。
* 探测者进程退出后，`HALF_OPEN` 记录被识别为无主，冷却结束后可再次探测。

### 并发与阻塞

* 每个 worker 是独立的 asyncio 事件循环，上游调用全程异步；慢查询不会阻塞同 worker
  内的其他查询。
* 上游 HTTP 连接在进程内复用连接池。
* SQLite 写事务放在线程池执行；常态放行与熔断拒绝都是只读快路径，不占用写锁。

## 在途查询合并（single-flight）

部署高峰时多个 worker 会同时收到同名制品查询。网关在熔断准入**之后**再加一层按制品名的
在途合并，把重复的目录调用收敛成一次：

1. **先按原熔断规则准入。** 被 OPEN/HALF_OPEN 拒绝的请求直接返回 503，既不加入合并也
   不访问上游；恢复探测（probe）天然全局唯一，不参与合并。
2. **获准且在一次目录调用完成前到达的同名请求共享该次调用与结果。** 不同制品各自独立，
   仍然完全并行。合并跨越两个 worker 进程：领导权通过 SQLite 表 `shared_calls` 上的原子
   compare-and-swap（`BEGIN IMMEDIATE`）选举，因此两个 worker 同时打到同名制品时，目录
   服务也只收到一次请求。
3. **一次共享调用的成败只记账一次。** 由 leader 在同一个写事务里推进熔断状态与转发计数；
   若这次失败触发熔断，所有已加入的等待者都收到本次失败（响应里 `breaker_effect=opened`），
   熔断之后到达的查询按原规则被 503 拒绝。
4. **等待者不会无限挂起。** 每个共享调用受 1 秒上游超时与租约约束；leader 所在进程死亡或
   租约过期时，等待者会以新的 call generation 接管。等待本身也有硬上限
   （`SHARED_WAIT_BUDGET`），到点返回本地失败而不是永久挂起。
5. **断连安全、可安全重试。** 驱动共享调用的任务与具体请求解耦（detached task）：发起者或
   某个等待者断连、worker 退出，都不会取消仍有其他参与者的共享调用；未确认的调用之后可以
   安全重试。worker 关闭时未完成的调用会把熔断记账补上（计一次失败），不会丢账。
6. **旧调用迟到不能改写新结论。** 每个制品的每次调用都有独立的 call generation，完成行保留
   为历史；等待者只读取自己加入的那一代。被接管的旧代次迟到结果会被 fencing
   （`stale_shared_call_ignored`），既不覆盖新调用的结果，也不改变熔断判定。

响应体新增字段：`shared`（是否搭了本 worker 已有调用的便车）、`call_leader`（是否为真正
发起目录调用的那一个请求）、`call_gen`（调用代次）；响应头新增 `X-Coalesced`、
`X-Call-Generation`。`/api/breakers` 的 `gateway_upstream` 增加 `shared_joins`（由在途合并
直接服务、未产生额外上游访问的请求数）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/meta/{artifact}` | 经熔断器查询元数据。成功 200，上游失败 502，熔断 503 |
| GET | `/api/breakers` | 所有制品的熔断状态、最近事件、转发计数 |
| POST | `/api/reset` | 清空熔断状态与计数（演示用） |
| GET/POST | `/api/upstream-mode` | 读取 / 设置示例上游模式：`normal`、`delay`、`503`，可带 `delay` 秒数 |
| GET | `/api/upstream-stats` | 上游实际收到的 `/artifacts` 请求数（地面真值） |
| POST | `/api/upstream-stats/reset` | 清零上游计数 |
| GET | `/healthz` | 健康检查 |

说明：`/api/meta` 的 4xx（例如制品不存在）视为上游**健康**响应，原样返回并清零计数；
仅连接失败、超时和 5xx 计入熔断失败。

示例制品：`example-app.json`、`shared-lib.json`、`reports-tool.json`
（位于 `artifacts/`；请求任意不存在的名字会得到 404，可用来观察“健康 4xx”行为）。

## 建议的手动验证

1. 将上游切到 **503**，连续查询同一制品 3 次：前两次 502，第三次之后变 503（熔断）。
   再查几次，观察「上游实际访问次数」不再增长。
2. 把上游切回**正常**，等待 2 秒后查询：该请求作为“恢复探测”成功，熔断恢复。
3. 切到**延迟**并设置为 `0.8`，把“并发请求数”调到 8 后查询同一制品：全部成功，而
   「上游收到的 /artifacts 请求」只增加 1 次，页面会标出 1 个“调用发起者”和其余
   “同进程合并 / 跨进程等待”；把延迟设为 `3` 则每次查询约 1 秒超时并计为失败。
4. 熔断后在浏览器刷新页面，或 `docker compose restart gateway`：熔断状态仍为 open。
5. 并发查询多个**不同**制品名（逗号分隔）：它们各自独立并行，上游访问次数等于制品数。

## 自动化测试

```bash
pip install -r requirements-dev.txt
# 需要先启动 gateway 与 upstream（Compose 或本机进程）
GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/
```

* `tests/test_unit.py`：直接驱动状态机，覆盖失败计数/清零、熔断、单探测选举、
  探测失败重冷却、慢成功不能恢复、旧探测代次不能覆盖、无主探测释放、超时与错误分类。
* `tests/test_coalescing.py`：进程内驱动在途合并，覆盖同名并发只打一次上游、不同制品并行、
  共享失败只计一次并在三次后熔断、等待者都收到熔断失败、代次推进、旧代次结果被 fencing、
  leader 死亡接管、等待者断连不取消共享调用、reset 清理。
* `tests/test_breaker.py`：通过 HTTP 覆盖真实双 worker 的熔断行为（含两个 worker 都被用到、
  超时上限、并发探测只放行一个，以及设置 `GATEWAY_RESTART_CMD` 时的重启持久化）。
* `tests/test_coalescing_http.py`：通过 HTTP 覆盖真实双 worker 的在途合并，包括跨进程
  合并为一次上游调用、不同制品并行、熔断只记一次、探测不被合并、断连后共享调用仍存活、
  转发计数与合并计数的区分。

## 可调环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `UPSTREAM_URL` | `http://upstream:9000` | 上游目录服务地址 |
| `GATEWAY_DB` | `./data/breaker.db` | SQLite 路径 |
| `REQUEST_TIMEOUT` | `1.0` | 单次上游调用总超时（秒） |
| `COOLDOWN` | `2.0` | 熔断冷却（秒） |
| `FAILURE_THRESHOLD` | `3` | 连续失败阈值 |

在途合并的内部常量（`SHARED_CALL_LEASE`、`SHARED_POLL_INTERVAL`、`SHARED_WAIT_BUDGET`、
`SHARED_DONE_RETENTION`、`SHARED_HISTORY_KEEP`）定义在 `app.py` 顶部，随 `REQUEST_TIMEOUT`
推导默认值，一般无需调整；它们只影响跨进程等待与历史行保留，不改变熔断语义。
