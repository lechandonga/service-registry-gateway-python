# 路由、健康检查与熔断规则

## 1. 选路与加权分配

一次 `dispatch` 的判定顺序，每一步都产出可解释依据（日志与 `/admin/status` 可见）：

1. **规则匹配**：首个 `path_prefix + require_headers` 命中的规则生效；无任何命中
   -> `NO_ROUTE_RULE`（404）。
2. **注册表快照**：在注册表锁内取出该服务租约有效的实例；为空
   -> `NO_AVAILABLE_UPSTREAM`（503）。
3. **候选过滤**，剔除任一不满足者：
   - `weight <= 0`；
   - 健康状态为 `DOWN`（`SUSPECT` 仍保留在候选中）；
   - 熔断非 `CLOSED`（`OPEN/HALF_OPEN`）；
   - **平滑下线中**（`DRAINING/DRAINED`，不接新流量）；
   - **本次请求中已失败过的实例**（重试不反复落在同一失败实例上）。

   全部被剔除 -> `ALL_UPSTREAMS_DOWN`（503），响应与日志中带每个实例的剔除原因。
4. **会话粘性**：见第 2 节；无合法会话标识时直接进入下一步。
5. **加权随机**：在候选中按权重做区间抽样，权重为相对值（如 3:1 长期约 75%:25%）。

> 选路、健康判断、熔断判断读取的是一致快照；注册与摘除同时发生时，选出的实例
> 在返回时必然仍在注册表中（见 `tests/test_concurrency.py`）。

## 2. 会话粘性（会话保持）

- **会话标识**：请求头 `x-session-id`。合法形态为 1~128 位
  `[A-Za-z0-9._~-]`；**缺失或不合法时按普通加权选择处理**，不报错、不影响可用性判定。
- **绑定规则**：首次携带合法会话标识的请求按加权选择落点并建立绑定；绑定有效期内，
  同一会话的请求在**绑定实例仍在候选中**时稳定落到同一实例。
- **有效期语义**：滑动过期。每次命中绑定都把到期时间顺延 `ttl_seconds`（默认 300s），
  `bound_at` 保留首次建立时间；超过有效期未访问的绑定被惰性清除，下次按新会话处理。
- **自动切换**：绑定实例被健康摘除、熔断、平滑下线、租约到期或注销时，请求自动
  加权落到其他可用实例并**改写绑定**，日志与 `SelectionContext` 记录可区分原因：
  `bound_instance_unhealthy` / `bound_instance_circuit_open` / `bound_instance_draining`
  / `bound_instance_zero_weight` / `bound_instance_gone`。
  只要服务还有可服务实例，就不会因为"原来绑定的那个不在了"而整体失败；
  `NO_AVAILABLE_UPSTREAM`（一个实例都没有）与 `ALL_UPSTREAMS_DOWN`（有但都不可用）
  仍然可区分。
- **观测**：`GET /admin/sessions/{key}` 返回绑定实例、`bound_at`、剩余秒数；
  `GET /admin/sessions` 列出全部有效绑定。
- **清理**：实例注销、租约摘除、平滑下线完成时，所有指向它的绑定被同步清除，
  不留下会导致错误命中的陈旧记录。

## 3. 平滑下线（优雅退出）

状态流转：`SERVING --start_drain--> DRAINING --在途归零--> DRAINED`。

- **发起**：`POST /admin/instances/{svc}/{host}/{port}/drain`，结果面向调用方可区分：

  | code | HTTP | 含义 |
  | --- | --- | --- |
  | `DRAIN_STARTED` | 200 | 进入下线流程，等待在途请求完成（响应含 `in_flight`） |
  | `DRAIN_COMPLETED` | 200 | 发起时在途即为零，已直接完成下线 |
  | `ALREADY_DRAINING` | 409 | 拒绝：该实例已在下线流程中 |
  | `ALREADY_DRAINED` | 409 | 拒绝：该实例此前已完成下线 |
  | `NOT_FOUND` | 404 | 拒绝：实例不存在或已过期 |

- **DRAINING 期间**：路由层不再把新请求选到它（含粘性绑定它的会话，自动切换）；
  已在处理的请求**正常跑完并返回**。每次连接释放后检查在途计数，归零即完成下线。
- **续约语义**：下线中的实例心跳被明确拒绝（`409 HEARTBEAT_DRAINING`），
  不存在"宣布下线又被续约复活"的状态；同时租约到期**不会**把它提前摘除——
  它必须在在途归零后由下线流程主动注销，保证在途请求跑完。
- **完成（DRAINED）**：从注册表注销，并联动清理健康、熔断、连接计数与指向它的
  会话绑定；收尾在并发释放下**恰好执行一次**。
- **竞态防护**：选路到获取连接之间实例开始下线的，获取连接后会再复核一次，
  命中则释放连接立即重选（不计入重试次数）。
- **观测**：`GET /admin/instances/{svc}/{host}/{port}/drain` 返回状态、发起/完成
  时间与当前在途数；`GET /admin/status/{service}` 每个实例带 `drain` 字段。

## 4. 租约

- 注册即起租；心跳把 `last_heartbeat` 刷新为当前时刻，到期时间 = 心跳 + `lease_seconds`。
- 后台 `lease_loop` 每秒摘除到期实例，并联动清理健康、熔断、连接计数。
- 注册表所有读取都会再次校验到期，**到期实例不会被选中、不能用心跳复活**。
- 重复注册 -> `REG_DUPLICATE`；重复注销 -> `UNREG_NOT_FOUND`；对未知/过期实例心跳
  -> `HEARTBEAT_UNKNOWN`，三者可区分，不做静默覆盖。

## 5. 健康检查（主动 + 被动）

三态：`HEALTHY`（参与选路）/ `SUSPECT`（仍参与，已记录失败）/ `DOWN`（摘除）。

- **被动反馈**：每次真实请求成功记一次成功，失败记一次失败。
  - 连续失败 `< failure_threshold`：`SUSPECT`，**不摘除**（短暂抖动容忍）；
  - 连续失败达到 `failure_threshold`（默认 3）：`DOWN`，从候选摘除；
  - `SUSPECT/HEALTHY` 下任意一次成功立即把失败计数清零，单次抖动不累积。
- **主动探测**：后台 `probe_loop` 每秒对 `DOWN` 实例通过本地模拟上游发探测。
  - 必须**连续** `recovery_threshold`（默认 2）次成功才回到 `HEALTHY`；
  - 恢复过程中任意一次失败立即清零恢复计数并保持 `DOWN`，
    避免抖动造成反复摘除/错误恢复；
  - 探测本身抛异常按失败处理，不外泄异常。

## 6. 熔断

每实例三态（默认 `failure_threshold=5`，`open_seconds=5s`）：

- `CLOSED`：正常放行；连续失败达到阈值 -> `OPEN`。
- `OPEN`：`allow` **快速失败**，`code=CIRCUIT_OPEN`（503），不建立连接、不调用上游；
  冷却期内每次拒绝都给出剩余冷却时间。
- 冷却结束后的**首个**请求把状态切到 `HALF_OPEN` 并占用唯一探测名额；并发的其他
  请求在探测结论出来前全部快速失败（`半开探测进行中，并发探测被拒绝`），
  **恢复探测在并发下不会被重复放行**。
  - 探测成功 -> `CLOSED`；
  - 探测失败 -> 重新 `OPEN` 并重置冷却计时。

## 7. 重试

- 仅**幂等只读方法**（GET/HEAD/OPTIONS）允许重试；POST 等非幂等请求只尝试一次，
  避免重复副作用（`is_safe_to_retry`）。
- 仅瞬时失败可重试：`UPSTREAM_UNREACHABLE / UPSTREAM_TIMEOUT / NAME_NOT_RESOLVED /
  NO_AVAILABLE_UPSTREAM / ALL_UPSTREAMS_DOWN`。业务错误（`UPSTREAM_ERROR`）不重试。
- 默认最多再试 2 次，每次重新选路（倾向选到其他实例）。重试期间若故障实例已被
  摘除导致 `ALL_UPSTREAMS_DOWN`，对外仍保留**首次尝试的真实根因**（记录在
  `masked_by` 字段），避免失败原因被掩盖。

## 8. 连接上限、超时与背压

- 每上游并发上限（默认 8）与全局并发上限（默认 64）。申请时在同一把锁内先检查后
  计数，超限直接抛 `CONCURRENCY_LIMIT`（HTTP 429），**抛错前不产生任何计数变更**。
- 连接以租约对象持有，`release()` 幂等；请求成功、失败、重试、取消的所有路径都在
  `finally` 精确释放一次，不残留计数、不出现负数或重复扣减。
- 上游调用统一 `asyncio.wait_for(timeout=upstream_timeout)`，超时归一化为
  `UPSTREAM_TIMEOUT`（504）。

## 9. 失败类型归一化（对外可区分）

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `REG_DUPLICATE` | 409 | 重复注册 |
| `UNREG_NOT_FOUND` | 404 | 注销不存在的实例 |
| `HEARTBEAT_UNKNOWN` | 404 | 对未知/过期实例续约 |
| `NO_ROUTE_RULE` | 404 | 路径+请求头无匹配规则 |
| `NO_AVAILABLE_UPSTREAM` | 503 | 服务没有租约有效实例 |
| `ALL_UPSTREAMS_DOWN` | 503 | 有实例但全部健康 DOWN / 熔断 / 权重 0 |
| `CIRCUIT_OPEN` | 503 | 熔断打开快速失败 / 半开探测在途 |
| `CONCURRENCY_LIMIT` | 429 | 上游或全局并发超限 |
| `UPSTREAM_UNREACHABLE` | 502 | 连接被拒 / 上游不可达 |
| `NAME_NOT_RESOLVED` | 502 | 主机名无法解析 |
| `UPSTREAM_TIMEOUT` | 504 | 上游响应超时 |
| `UPSTREAM_ERROR` | 502 | 上游内部异常或返回非 2xx |
| `HEARTBEAT_DRAINING` | 409 | 对平滑下线中的实例续约（明确拒绝，不会复活） |
| `INTERNAL_ERROR` | 500 | 网关未知异常（仅返回 request_id，堆栈只进服务端日志） |
