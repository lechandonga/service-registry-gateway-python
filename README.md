# Service Registry & Dynamic Upstream Gateway（本地可验证版）

一套**不依赖真实后端**即可完整验证的服务注册 + 动态上游路由网关。上游用本地模拟器
（`LocalUpstreamPool`）代替，可脚本化产出“正常 / 超时 / 不可达 / DNS 失败 / 5xx”等结果，
使注册续约、健康探测、路由选择、重试熔断与背压限流在各入口下结论一致、原因可区分、状态可解释。

## 能力总览

| 能力 | 说明 |
| --- | --- |
| 注册 / 注销 / 心跳续约 | 租约过期实例不再被选中；重复注册返回 `409 registry_duplicate`，重复注销/未知实例返回 `404 registry_not_found`，不做静默覆盖 |
| 路径 + 请求头路由 | 按优先级、最长前缀匹配，请求头需全部命中；加权随机分配 |
| 下线 / 零权重隔离 | 权重 0 标记为 offline，不参与选择 |
| 主动 + 被动健康检查 | 连续失败达阈值才摘除（抖动容忍），恢复需连续成功且过冷却期 |
| 失败重试 | 仅对幂等请求重试，且重试尽量落到其他健康实例 |
| 熔断 | CLOSED/OPEN/HALF_OPEN；OPEN 快速失败，冷却后**并发下只放一个**恢复探测 |
| 并发 / 连接上限、超时、背压 | 超限立即拒绝并区分原因；超时归一化；任何失败路径计数都在 `finally` 释放 |
| 并发一致性 | 注册表、健康状态、连接计数全部在锁下变更；注册与摘除竞争时不会选到已摘除实例 |
| 失败归一化 | 上游不可达 / DNS / 超时 / 5xx / 熔断 / 限流均为可区分错误码；内部异常与堆栈不外泄 |

## 目录结构

```
gateway/
  errors.py     归一化错误类型 ErrorType + GatewayError
  models.py     Instance / RouteRule / Request / Response 等模型
  registry.py   注册表：注册、注销、心跳、租约过期、后台 reaper
  health.py     HealthChecker：被动反馈 + 周期主动探测、抖动容忍、恢复冷却
  upstream.py   LocalUpstreamPool：本地模拟上游（可配置行为/结果脚本）
  router.py     规则匹配 + 加权选择 + 不可用原因诊断
  circuit.py    每实例熔断器（含 HALF_OPEN 并发去重）
  limiter.py    全局/每实例并发上限与背压
  gateway.py    编排层：选路→熔断→限流→调用→健康反馈→安全重试
  api.py        FastAPI：管理面 + 代理面 + 统一错误处理
  factory.py    组件组装
tests/          pytest 套件（37 个用例，异常链路均有断言与状态日志）
docs/DESIGN.md  详细设计与判定规则
```

## 快速开始

```bash
uv sync
uv run uvicorn main:app --port 8000
```

## 注册协议（管理面）

| 方法与路径 | 作用 | 关键语义 |
| --- | --- | --- |
| `POST /admin/instances` | 注册实例 | 同 id 仍在线时返回 `409 registry_duplicate`；body 可带 `weight`、`lease_seconds`、`behavior` |
| `DELETE /admin/instances/{id}` | 注销 | 未知/已注销返回 `404 registry_not_found`（错误体带 `op`） |
| `POST /admin/instances/{id}/heartbeat` | 续约 | body 可选新的 `lease_seconds`；返回 `expires_at` |
| `PUT /admin/routes` | 安装/替换路由规则 | `path_prefix`、`match_headers`、`targets`、`priority` |
| `GET /admin/status` | 状态面 | 每个实例的注册状态、健康状态及判定依据、熔断态、连接数、租约剩余 |
| `POST /admin/instances/{id}/script` | 配置模拟结果序列（仅本地验证） | `{"results": ["healthy","timeout","http_503", ...]}` |

注册 body 示例：

```json
{
  "id": "cart-1",
  "host": "10.0.0.8",
  "port": 8080,
  "weight": 5,
  "lease_seconds": 30,
  "behavior": "healthy",
  "metadata": {"zone": "a"}
}
```

## 代理面

`/proxy/{path}` 支持 GET/POST/PUT/DELETE/HEAD/OPTIONS：

```bash
curl localhost:8000/proxy/orders/42
# 响应头：x-upstream-id=<实例id>  x-gateway-attempts=<尝试次数>
# 查询参数 timeout 可覆盖本次上游超时：/proxy/x?timeout=0.2
```

### 幂等与重试

- 默认 GET/HEAD/OPTIONS/PUT/DELETE 视为幂等，可重试；POST 默认**不重试**。
- 请求模型可用 `idempotent: true` 显式声明某 POST 可安全重试。
- 重试时排除已失败实例；没有其他实例可选时，保留首个连接类错误（不会被
  `route_no_available` 覆盖）。
- 5xx/408 是上游的**真实响应**：可重试则换实例，预算用尽后把最后一次响应透传。

## 路由规则

1. 规则按 `priority` 降序，其次按 `path_prefix` 长度降序（最长前缀优先）。
2. 请求路径必须以 `path_prefix` 开头。
3. `match_headers` 中的头（大小写不敏感）必须**全部**命中。
4. `targets` 非空时只在列出的实例中选择；为空则在该规则下所有可用实例中加权随机。
5. 选择失败原因可区分：
   - 无规则匹配 → `404 route_no_match`
   - 规则匹配但零可用 → `503 route_no_available`，错误体 `detail.candidates`
     给出每个候选实例的不可用原因：`lease_expired / health_evicted:<原因> / zero_weight / deregistered`。

## 健康与熔断规则

### 健康检查

- 被动：每次真实请求都反馈结果。连续失败达到 `fail_threshold`（默认 3）才摘除为 `sick`；
  中途一次成功即清零，因此短暂抖动不会反复摘除。
- 主动：后台周期探测（`probe_interval`）。被摘除实例必须先经过
  `recover_cooldown`（默认 0.2s）才允许探测；连续成功 `recover_threshold`（默认 2）次
  才恢复为 `online`。恢复期间一次失败会重新计数。
- 探测中的超时/连接错误/DNS 错误全部归一化记录，内部异常不外泄。

### 熔断（每实例独立）

- CLOSED 下连续失败达阈值（默认 5）→ OPEN。
- OPEN 期间所有请求**快速失败** `503 circuit_open`，不打到上游。
- 经过 `recovery_timeout`（默认 5s）后，下一批并发请求中**恰好一个**进入 HALF_OPEN 探测，
  其余仍快速失败；探测成功 → CLOSED，探测失败 → 重新 OPEN 并重新计时冷却。

## 失败类型对照表

| ErrorType | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `registry_duplicate` | 409 | 实例已在线，重复注册 |
| `registry_not_found` | 404 | 注销/续约的实例不存在或已注销 |
| `route_no_match` | 404 | 无匹配路由规则 |
| `route_no_available` | 503 | 规则匹配但无可用上游（带候选原因） |
| `circuit_open` | 503 | 熔断打开快速失败 |
| `limit_concurrency` | 503 | 网关全局并发上限 |
| `limit_connections` | 503 | 单实例连接上限 |
| `upstream_timeout` / `limit_timeout` | 504 | 上游超时 / 等待超时 |
| `upstream_unreachable` | 502 | 连接被拒/不可达 |
| `dns_error` | 502 | 主机名无法解析 |
| `upstream_http_error` / `retries_exhausted` / `upstream_error` | 502 | 上游错误归一化 |
| `bad_request` | 400 | 请求体不合法 |

所有错误体统一为 `{"error": <code>, "message": <safe message>, "detail": {...}}`，
不含内部堆栈。

## 模拟上游行为

| behavior / 脚本令牌 | 行为 |
| --- | --- |
| `healthy` | 200 `ok:<id>` |
| `timeout` | 长延迟，由调用方超时截断为 `upstream_timeout` |
| `unreachable` | 直接抛 `upstream_unreachable`（模拟连接拒绝） |
| `dns_fail` | 抛 `dns_error` |
| `http_5xx` | 稳定返回 503 |
| `http_<code>` | 返回指定状态码，如 `http_429`、`http_500` |
| `flaky` | 按 `fail_rate` 随机失败（默认随机种子，可复现） |

## 本地验证

```bash
# 全量单测（-s 可看到每个用例打印的上游状态与判定依据日志）
uv run pytest -q

# 手工端到端
uv run uvicorn main:app --port 8000
# 另开终端：
curl -X POST localhost:8000/admin/instances -H 'content-type: application/json' \
  -d '{"id":"a","host":"10.0.0.1","port":80,"behavior":"healthy"}'
curl -X PUT localhost:8000/admin/routes -H 'content-type: application/json' \
  -d '{"name":"all","path_prefix":"/proxy/"}'
curl localhost:8000/proxy/demo
curl localhost:8000/admin/status

# 注入故障：让实例 a 的下一次请求超时
curl -X POST localhost:8000/admin/instances/a/script \
  -H 'content-type: application/json' -d '{"results":["timeout","healthy"]}'
curl 'localhost:8000/proxy/demo?timeout=0.2'
```

测试覆盖的异常链路：租约过期、探测抖动与恢复、熔断打开/冷却/并发单探测恢复、
并发注册摘除一致性、超时背压计数残留、DNS/不可达/5xx 失败归一化。

## 设计文档

详见 [`docs/DESIGN.md`](docs/DESIGN.md)。
