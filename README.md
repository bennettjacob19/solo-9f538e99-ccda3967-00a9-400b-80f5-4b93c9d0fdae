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
3. 切到**延迟**并设置为 `0.8`，用“并发请求数”发起多次查询：全部在 1 秒内成功，
   说明慢查询之间互不阻塞；把延迟设为 `3` 则每次查询约 1 秒超时并计为失败。
4. 熔断后在浏览器刷新页面，或 `docker compose restart gateway`：熔断状态仍为 open。

## 自动化测试

```bash
pip install -r requirements-dev.txt
# 需要先启动 gateway 与 upstream（Compose 或本机进程）
GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/
```

* `tests/test_unit.py`：直接驱动状态机，覆盖失败计数/清零、熔断、单探测选举、
  探测失败重冷却、慢成功不能恢复、旧探测代次不能覆盖、无主探测释放、超时与错误分类。
* `tests/test_breaker.py`：通过 HTTP 覆盖真实双 worker 行为，包括两个 worker 都被用到、
  超时上限、并发探测只放行一个、慢查询并发、以及（设置 `GATEWAY_RESTART_CMD` 时）
  重启后状态保留。

## 可调环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `UPSTREAM_URL` | `http://upstream:9000` | 上游目录服务地址 |
| `GATEWAY_DB` | `./data/breaker.db` | SQLite 路径 |
| `REQUEST_TIMEOUT` | `1.0` | 单次上游调用总超时（秒） |
| `COOLDOWN` | `2.0` | 熔断冷却（秒） |
| `FAILURE_THRESHOLD` | `3` | 连续失败阈值 |
