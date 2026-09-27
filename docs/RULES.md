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
   - 正在**平滑下线**（`DRAINING`）或已退出（`DRAINED`）。

   全部被剔除 -> `ALL_UPSTREAMS_DOWN`（503），响应与日志中带每个实例的剔除原因。
4. **会话粘性（启用时）**：请求携带合法 `x-session-id` 且绑定仍有效 -> 稳定命中绑定
   实例并滑动续期；绑定实例不可用/退出/过期 -> 自动切换到其他可用实例并记录
   `switch_reason`；标识缺失或非法 -> 静默退化为加权选择。
5. **加权随机**：在（排除本请求已失败实例后的）候选中按权重做区间抽样。

> 选路、健康判断、熔断判断、下线判断读取的是一致快照；注册/摘除/下线同时发生时，
> 选出的实例在返回时必然仍在注册表且未在退出（见 `tests/test_concurrency.py`、
> `tests/test_sticky_drain_concurrency.py`）。
>
> 会话粘性有效期语义、平滑下线状态流转与兼容范围详见 [STICKY_DRAIN.md](STICKY_DRAIN.md)。

## 2. 租约

- 注册即起租；心跳把 `last_heartbeat` 刷新为当前时刻，到期时间 = 心跳 + `lease_seconds`。
- 后台 `lease_loop` 每秒摘除到期实例，并联动清理健康、熔断、连接计数。
- 注册表所有读取都会再次校验到期，**到期实例不会被选中、不能用心跳复活**。
- 重复注册 -> `REG_DUPLICATE`；重复注销 -> `UNREG_NOT_FOUND`；对未知/过期实例心跳
  -> `HEARTBEAT_UNKNOWN`，三者可区分，不做静默覆盖。

## 3. 健康检查（主动 + 被动）

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

## 4. 熔断

每实例三态（默认 `failure_threshold=5`，`open_seconds=5s`）：

- `CLOSED`：正常放行；连续失败达到阈值 -> `OPEN`。
- `OPEN`：`allow` **快速失败**，`code=CIRCUIT_OPEN`（503），不建立连接、不调用上游；
  冷却期内每次拒绝都给出剩余冷却时间。
- 冷却结束后的**首个**请求把状态切到 `HALF_OPEN` 并占用唯一探测名额；并发的其他
  请求在探测结论出来前全部快速失败（`半开探测进行中，并发探测被拒绝`），
  **恢复探测在并发下不会被重复放行**。
  - 探测成功 -> `CLOSED`；
  - 探测失败 -> 重新 `OPEN` 并重置冷却计时。

## 5. 重试

- 仅**幂等只读方法**（GET/HEAD/OPTIONS）允许重试；POST 等非幂等请求只尝试一次，
  避免重复副作用（`is_safe_to_retry`）。
- 仅瞬时失败可重试：`UPSTREAM_UNREACHABLE / UPSTREAM_TIMEOUT / NAME_NOT_RESOLVED /
  NO_AVAILABLE_UPSTREAM / ALL_UPSTREAMS_DOWN`。业务错误（`UPSTREAM_ERROR`）不重试。
- 默认最多再试 2 次，每次重新选路（倾向选到其他实例）。重试期间若故障实例已被
  摘除导致 `ALL_UPSTREAMS_DOWN`，对外仍保留**首次尝试的真实根因**（记录在
  `masked_by` 字段），避免失败原因被掩盖。
- **本请求已失败的实例会被加入排除集合**，重试不会再落在同一个失败实例上。
- `INSTANCE_EXITING`（并发窗口内选到正在平滑下线的实例）发生在真正调用上游之前，
  对任意 HTTP 方法（含 POST）改投重试都没有重复副作用。

## 6. 会话保持与平滑下线

完整规则见 [STICKY_DRAIN.md](STICKY_DRAIN.md)，要点：

- 会话标识取自 `x-session-id`，默认粘性 TTL 300s，**滑动续期**；缺失/非法标识
  退化为加权选择，不报错。绑定实例健康 DOWN/熔断/下线/注销/过期时自动切换并记录
  `BOUND_INSTANCE_UNAVAILABLE / BOUND_INSTANCE_FAILED / BOUND_INSTANCE_GONE /
  STICKY_TTL_EXPIRED` 等可解释原因。
- 平滑下线：`SERVING -> DRAINING -> DRAINED`。DRAINING 时不接新流量、豁免租约
  过期、拒绝续约；在途请求正常跑完，连接计数归零后才真正注销并清理会话绑定。
  受理结果 `DRAINING_STARTED / INSTANCE_ALREADY_DRAINING(409) /
  INSTANCE_DRAIN_NOT_FOUND(404)` 三者对调用方可区分。

## 7. 连接上限、超时与背压

- 每上游并发上限（默认 8）与全局并发上限（默认 64）。申请时在同一把锁内先检查后
  计数，超限直接抛 `CONCURRENCY_LIMIT`（HTTP 429），**抛错前不产生任何计数变更**。
- 连接以租约对象持有，`release()` 幂等；请求成功、失败、重试、取消的所有路径都在
  `finally` 精确释放一次，不残留计数、不出现负数或重复扣减。
- 上游调用统一 `asyncio.wait_for(timeout=upstream_timeout)`，超时归一化为
  `UPSTREAM_TIMEOUT`（504）。

## 8. 失败类型归一化（对外可区分）

| code | HTTP | 触发场景 |
| --- | --- | --- |
| `REG_DUPLICATE` | 409 | 重复注册（含对下线中实例重新注册） |
| `UNREG_NOT_FOUND` | 404 | 注销不存在的实例 |
| `HEARTBEAT_UNKNOWN` | 404 | 对未知/过期/已退出实例续约 |
| `INSTANCE_DRAINING` | 409 | 对**平滑下线过程中**的实例续约（拒绝复活） |
| `INSTANCE_DRAIN_NOT_FOUND` | 404 | 对不存在/已下线完成/已过期实例发起下线 |
| `INSTANCE_ALREADY_DRAINING` | 409 | 对已在 DRAINING 的实例重复宣告下线 |
| `INSTANCE_EXITING` | 503 | 并发窗口选到正在退出实例（内部改投，通常不透出） |
| `NO_ROUTE_RULE` | 404 | 路径+请求头无匹配规则 |
| `NO_AVAILABLE_UPSTREAM` | 503 | 服务没有租约有效实例 |
| `ALL_UPSTREAMS_DOWN` | 503 | 有实例但全部健康 DOWN / 熔断 / 权重 0 / 下线中 |
| `CIRCUIT_OPEN` | 503 | 熔断打开快速失败 / 半开探测在途 |
| `CONCURRENCY_LIMIT` | 429 | 上游或全局并发超限 |
| `UPSTREAM_UNREACHABLE` | 502 | 连接被拒 / 上游不可达 |
| `NAME_NOT_RESOLVED` | 502 | 主机名无法解析 |
| `UPSTREAM_TIMEOUT` | 504 | 上游响应超时 |
| `UPSTREAM_ERROR` | 502 | 上游内部异常或返回非 2xx |
| `INTERNAL_ERROR` | 500 | 网关未知异常（仅返回 request_id，堆栈只进服务端日志） |
