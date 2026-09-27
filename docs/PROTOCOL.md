# 服务注册与路由协议

所有接口均返回 JSON。成功返回业务字段；失败统一返回：

```json
{ "code": "FAILURE_CODE", "message": "可读原因（不含内部堆栈）" }
```

## 1. 实例管理

### 注册 `POST /admin/register`

```json
{
  "service": "users",
  "host": "127.0.0.1",
  "port": 9001,
  "weight": 3,
  "lease_seconds": 15.0,
  "metadata": {"zone": "a"}
}
```

- 成功：`200 {"registered": true, "instance": {...}}`
- **重复注册**：HTTP `409`，`code=REG_DUPLICATE`，**不会静默覆盖**已存在且租约有效的实例。
- `weight<=0` 允许注册但永不参与选路；`lease_seconds` 为租约时长。

### 注销 `DELETE /admin/instances/{service}/{host}/{port}`

- 成功：`200 {"deregistered": true}`，同时清理健康状态、熔断状态与连接计数。
- **重复注销 / 实例不存在**：HTTP `404`，`code=UNREG_NOT_FOUND`（与成功注销明确区分）。

### 心跳续约 `POST /admin/instances/{service}/{host}/{port}/heartbeat`

```json
{ "lease_seconds": 20.0 }   // 可选，不传则沿用原租约
```

- 成功：`200 {"renewed": true, "expires_at": ...}`，重置到期时间。
- **实例不存在或租约已过期**：HTTP `404`，`code=HEARTBEAT_UNKNOWN`。
  过期实例不会因迟到的心跳复活，必须重新注册。
- **实例正在平滑下线（DRAINING）**：HTTP `409`，`code=INSTANCE_DRAINING`；
  下线宣告不会被续约推翻。

### 平滑下线 `POST /admin/instances/{service}/{host}/{port}/drain`

宣告后实例不再接新流量，等在途请求跑完、连接计数归零后才真正注销。

- 正常受理：`200 {"status":"DRAINING_STARTED","instance":...,"in_flight":N,"deregistered":bool}`；
  无在途时 `deregistered=true` 立即退出，有在途时为 `false` 并在归零后自动退出。
- 已在 DRAINING 中重复宣告：HTTP `409`，`code=INSTANCE_ALREADY_DRAINING`（幂等）。
- 实例不存在 / 已下线完成 / 已过期：HTTP `404`，`code=INSTANCE_DRAIN_NOT_FOUND`。

### 列举 `GET /admin/instances`

返回各服务当前**租约有效**的实例（过期实例不出现在结果中）。

## 2. 路由规则

### 添加规则 `POST /admin/rules`

```json
{
  "service": "orders",
  "path_prefix": "/proxy/orders",
  "require_headers": {"x-canary": "true"},
  "name": "orders-canary"
}
```

匹配规则：按添加顺序，第一个同时满足以下条件的规则胜出：

1. 请求路径以 `path_prefix` 开头；
2. `require_headers` 中所有请求头（键自动转小写）都精确匹配。

列举规则：`GET /admin/rules`。

## 3. 代理转发

`ANY /proxy/{path}`（GET/POST/PUT/DELETE/PATCH/HEAD/OPTIONS）

请求被转发给按规则与权重选出的本地模拟上游。失败时按归一化 `code` 返回（见 `docs/RULES.md`）。

**会话保持**：请求携带 `x-session-id`（合法字符 `A-Za-z0-9._:-@`，长度 ≤128）时，
同一会话在粘性 TTL（默认 300s，滑动续期）内稳定路由到同一实例；标识缺失或非法时
退化为普通加权选择，不报错。绑定实例退出/不可用时自动切换到其他可用实例。

## 4. 本地模拟上游控制（仅用于本地验证）

`POST /admin/instances/{service}/{host}/{port}/mode`

```json
{ "mode": "unreachable" }
```

`mode` 取值：

| mode | 含义 | 归一化 code |
| --- | --- | --- |
| `ok` | 正常 200 | - |
| `http_error` | 返回异常状态（默认 500） | `UPSTREAM_ERROR` |
| `unreachable` | 连接被拒 | `UPSTREAM_UNREACHABLE` |
| `unresolvable` | 名称无法解析 | `NAME_NOT_RESOLVED` |
| `timeout` | 挂起不响应 | `UPSTREAM_TIMEOUT` |
| `crash` | 上游内部异常 | `UPSTREAM_ERROR`（堆栈不外泄） |

## 5. 状态观测

`GET /admin/status/{service}` 返回每个实例的：

- `weight`、租约到期时间、`draining` 标记；
- `health`：`HEALTHY/SUSPECT/DOWN`、连续失败数、恢复探测成功数、判定 `reason`；
- `circuit`：`CLOSED/OPEN/HALF_OPEN`、失败数、冷却剩余、判定 `reason`；
- `drain`：`SERVING/DRAINING/DRAINED`、当前在途连接数与判定 `reason`；
- `active_connections`：当前在途连接数；
- 顶层 `sessions`：该服务当前全部有效会话绑定（会话、实例、建立时间、剩余 TTL）。

### 会话绑定查询 `GET /admin/sessions/{service}/{session_id}`

- 有效绑定：`{"bound": true, "binding": {"instance":..., "bound_at":..., "expires_at":..., "ttl_remaining":...}}`；
- 无有效绑定：`{"bound": false}`。
