# 设计说明：一致性判定与异常处置

本文说明各组件如何保证“各入口下结论一致且可解释”。

## 1. 实例生命周期与租约

```
register ─► ONLINE ◄──┐  heartbeat 仅刷新 last_heartbeat
            │  │      │
   weight=0 │  │ 连续失败达阈值（健康层）
            ▼  ▼
         OFFLINE  SICK
            └──┬──┘
               │ 健康恢复（冷却后连续成功）且 weight>0
               └──► ONLINE
   任意活跃态 ──now-last_heartbeat > lease──► EXPIRED
   任意状态   ──deregister──────────────────► DEREGISTERED
```

- 重复注册：仅当同 id 已处于 ONLINE/SICK/OFFLINE 才判重，返回 `registry_duplicate`；
  EXPIRED/DEREGISTERED 的 id 允许重新注册（这是“复活”的唯一合法途径）。
- 续约不改变 SICK/OFFLINE：心跳只表示“进程还活着”，不健康状态要靠健康检查恢复。
- 过期判定集中在注册表锁内完成：`snapshot()` 与后台 reaper 走同一 `_expire_locked`，
  不会出现两个入口对同一实例给出不同结论。

## 2. 可路由集合的定义（唯一口径）

一个实例可被选中，当且仅当（`HealthChecker.is_available`）：

1. `registry.status == ONLINE`；
2. `weight > 0`；
3. 该实例健康标记为 true（未被健康层摘除）。

`Router.select` 与 `/admin/status` 使用同一口径；当集合为空时，
`diagnose()` 逐实例给出 `reason`，因此调用方能区分：全部过期、全部被健康摘除、
全部权重为零，或仅仅是规则没有目标。

## 3. 并发模型

- 全组件运行在单个 asyncio 事件循环内；所有“读-判-写”复合操作在各自的
  `asyncio.Lock` 内原子完成。
- 注册表锁：register / deregister / heartbeat / snapshot（含过期扫描）。
- 熔断器锁：`allow()` 内部完成“是否到冷却 + 状态转 HALF_OPEN + 探测计数”这一复合动作，
  因此 N 个并发请求同时发现冷却结束，也只有 1 个拿到放行。
- 限流器锁：名额获取顺序固定（先全局后实例），实例名额被拒时尚未增加任何计数；
  释放在上下文管理器的 `finally` 中，保证异常、超时、取消都不漏减/重减。
- 选路一致性：`select` 取的是注册表一致性快照；即便 deregister 紧接其后发生，
  请求要么完全没看到该实例，要么在调用前由健康/熔断状态拒绝——测试
  `test_select_never_returns_deregistered` 以 2000 轮循环复核此不变量。

## 4. 请求编排顺序（gateway.Gateway._attempt）

```
select(rule, exclude=tried)
  └─ 无规则/无可用：route_no_match / route_no_available（带候选原因）
circuit.allow(id)
  └─ OPEN/HALF_OPEN 占用：circuit_open（快速失败，不占连接名额）
limiter.acquire(id)
  └─ 全局满：limit_concurrency；实例满：limit_connections
wait_for(upstream.invoke, request.timeout)
  └─ 超时：upstream_timeout；连接/DNS：保留原始归一化错误；其他：兜底 upstream_error
结果反馈：
  2xx/3xx → circuit.success + health(ok)
  408/5xx → circuit.failure + health(fail, http_<code>)，响应仍透传给调用方
```

失败计数在连接类错误与可重试 5xx 两条路径都会反馈，保证熔断和被动健康检查
不会因为错误类型不同而漏记。

## 5. 重试安全

- 幂等口径：方法白名单（GET/HEAD/OPTIONS/PUT/DELETE）或显式 `idempotent=true`。
  非幂等请求最多尝试一次，避免重复提交。
- 重试换实例：每次失败的实例 id 加入 `tried`，下一轮从候选中排除。
- 错误保持：重试期间若已没有其他实例（`route_no_available`）：
  - 拿到过上游 5xx 响应 → 返回最后一次真实响应；
  - 否则 → 抛出第一次的连接类错误（DNS/超时/不可达可区分）。
- 重试只针对连接类错误（超时、不可达、DNS、上游错误）与 5xx 响应；
  熔断打开、限流拒绝、请求不合法不重试。

## 6. 健康抖动与恢复

- 摘除需要“连续”失败计数达到阈值；任意一次成功清零，容忍间歇性失败。
- 摘除后进入恢复冷却（recover cooldown）：冷却期内主动探测直接跳过，
  避免“刚摘除就探测、刚好成功又纳回、下一秒又摘除”的抖动。
- 恢复需要连续成功计数达到阈值；恢复过程中一次失败立即把成功计数清零。
- 权重 0 / 主动 OFFLINE 的实例即便探测成功也不会被路由（健康恢复不等于重新上线）。

## 7. 熔断状态机

```
CLOSED ──failures>=N──► OPEN ──冷却到期, 恰好1个请求──► HALF_OPEN
  ▲                       ▲                              │
  └──── probe success ────┴──── probe failure ◄──────────┘
```

- OPEN 期间不触达上游，直接 `circuit_open`。
- HALF_OPEN 期间除探测请求外全部拒绝；因此“恢复探测在并发下不会被重复放行”。
- 探测失败重新打开并刷新冷却起点，防止坏实例被反复尝试。

## 8. 失败归一化与信息边界

- 模拟上游只产出标准错误（`upstream_unreachable` / `dns_error` / `upstream_timeout`）
  或标准 HTTP 响应；`invoke()` 对未知结果也归一化为 `upstream_error`。
- 编排层对未预期异常兜底为 `upstream_error`，并用 `raise ... from None` 切断链条。
- FastAPI 注册两个异常处理器：`GatewayError` 映射到固定状态码与错误体；
  其余 `Exception` 只在日志记录堆栈，对外返回固定的 internal error 响应。
- 错误码设计即“可区分的失败原因”，`detail` 仅含实例 id、地址、计数等安全字段。

## 9. 可解释性与可观测性

- `GET /admin/status` 返回每个实例的：注册态、权重、健康标记、连续成败计数、
  最后判定原因、熔断态、当前连接数、租约剩余秒数。
- 日志：摘除、恢复、探测结果、每次请求失败原因均以结构化字段输出。
- 测试中的 `log_states` 在关键断言点打印同一行汇总
  （`status/healthy/available/circuit/fails/succ/last_reason/conns`），
  便于核对“状态未被污染”。
