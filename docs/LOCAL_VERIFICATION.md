# 本地验证指南

无需任何真实后端：上游全部由进程内 `MockUpstream` 模拟，可任意编排
成功 / 不可达 / 解析失败 / 超时 / 500 / 内部崩溃等形态。

## 1. 环境准备

```bash
uv sync
```

需要 Python 3.14（见 `pyproject.toml`）。

## 2. 运行测试

```bash
uv run pytest -v
```

测试覆盖（共 80+ 用例）：

| 文件 | 覆盖链路 |
| --- | --- |
| `tests/test_registry.py` | 注册、重复注册、重复注销、心跳续约（含下线中拒绝续约）、**租约过期**、并发 |
| `tests/test_health.py` | **探测抖动**不摘除、阈值摘除、连续探测恢复、恢复被失败打断、探测异常归一化 |
| `tests/test_circuit.py` | 阈值打开、快速失败原因、冷却后半开、**并发恢复只放行一个探测** |
| `tests/test_pool.py` | 上游/全局上限、背压拒绝、退出门禁、异常路径无泄漏、重复释放幂等 |
| `tests/test_router.py` | 无规则/无实例/全不可用三类原因可区分、权重 0 与下线过滤、加权分布 |
| `tests/test_transport.py` | 不可达/解析失败/超时/崩溃/500 的归一化与可区分 code |
| `tests/test_gateway.py` | 非幂等不重试、只读请求重试换实例、熔断实例零调用、超时、背压、资源清理 |
| `tests/test_sticky.py` | **粘性命中/滑动 TTL/非法标识降级/绑定实例 DOWN·熔断·注销·过期自动切换带原因/重试排除** |
| `tests/test_drain.py` | **下线结果三态可区分、续约拒绝、过期豁免、在途跑完才退出、计数与绑定无残留** |
| `tests/test_sticky_drain_gateway.py` | **dispatch 级粘性、下线切换、在途收尾、门禁改投（POST 安全）、单实例退出与全服务不可用区分** |
| `tests/test_drain_api.py` | **HTTP：下线 200/409/404 可区分、续约 409、会话查询、状态观测** |
| `tests/test_sticky_drain_concurrency.py` | **下线/摘除/选路/真实 dispatch 并发：零误选已退出实例、计数归零、无陈旧绑定** |
| `tests/test_api.py` | HTTP 全链路：注册协议、代理、头匹配、模式注入、状态观测 |
| `tests/test_concurrency.py` | **并发注册摘除绝不选到已摘除实例**、混合流量计数一致、续约与过期竞争 |

测试日志会打印上游状态与判定依据，例如：

```text
[连续三次失败] 上游=svc://h:1 状态与判定={'state': 'DOWN', 'reason': '连续失败 3 次达到阈值，摘除', ...}
[50并发恢复探测 放行1 拒绝49] 上游=... 熔断判定={'state': 'HALF_OPEN', 'probe_in_flight': True, ...}
```

开启实时日志：`uv run pytest -v --log-cli-level=INFO`。

## 3. 启动本地服务

```bash
uv run uvicorn main:app --port 8077
# 或 uv run fastapi dev main.py --port 8077
```

启动时自动预置：

- `users` 服务：`127.0.0.1:9001`（权重 3）、`127.0.0.1:9002`（权重 1），租约 15s；
- `orders` 服务：`127.0.0.1:9101`，要求请求头 `x-canary: true`；
- 后台租约摘除循环（1s）与主动健康探测循环（1s）。

> 注意预置实例租约仅 15s。若要长时间实验，请用更长 `lease_seconds` 重新注册，
> 或周期性调用心跳接口。

## 4. 手工验证脚本

```bash
# 4.1 代理成功（加权落在两个 users 实例上）
curl -s localhost:8077/proxy/users/42

# 4.2 路由规则：无头 404 NO_ROUTE_RULE，带头 200
curl -i localhost:8077/proxy/orders/1
curl -i -H 'x-canary: true' localhost:8077/proxy/orders/1

# 4.3 注册一个长租约实例 + 规则
curl -s -X POST localhost:8077/admin/register \
  -H 'content-type: application/json' \
  -d '{"service":"demo","host":"h1","port":1,"weight":1,"lease_seconds":300}'
curl -s -X POST localhost:8077/admin/rules \
  -H 'content-type: application/json' \
  -d '{"service":"demo","path_prefix":"/proxy/demo"}'

# 4.4 心跳续约
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/heartbeat \
  -H 'content-type: application/json' -d '{"lease_seconds":600}'

# 4.5 注入故障 -> 连续请求 -> 观察健康从 SUSPECT 到 DOWN
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/mode \
  -H 'content-type: application/json' -d '{"mode":"unreachable"}'
for i in 1 2 3 4; do curl -s -o /dev/null -w '%{http_code} ' \
  localhost:8077/proxy/demo/x; done; echo
curl -s localhost:8077/admin/status/demo
# 前几次 502 UPSTREAM_UNREACHABLE；摘除后 503 ALL_UPSTREAMS_DOWN

# 4.6 恢复 -> 后台连续 2 次主动探测成功后自动重新纳入
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/mode \
  -H 'content-type: application/json' -d '{"mode":"ok"}'
sleep 3
curl -s localhost:8077/proxy/demo/x   # 200
```

### 其他失败形态

```bash
# 超时（504 UPSTREAM_TIMEOUT）
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/mode \
  -H 'content-type: application/json' -d '{"mode":"timeout","hang_seconds":10}'
# 名称无法解析（502 NAME_NOT_RESOLVED）
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/mode \
  -H 'content-type: application/json' -d '{"mode":"unresolvable"}'
# 上游内部崩溃（502 UPSTREAM_ERROR，响应不含堆栈）
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/mode \
  -H 'content-type: application/json' -d '{"mode":"crash"}'
```

## 4.7 会话保持（粘性）

```bash
# 再注册一个实例，制造权重倾斜也不影响粘性
curl -s -X POST localhost:8077/admin/register \
  -H 'content-type: application/json' \
  -d '{"service":"demo","host":"h2","port":1,"weight":1,"lease_seconds":300}'

# 带同一会话标识连发，始终落到同一实例（响应体含实例名）
for i in $(seq 1 8); do
  curl -s -H 'x-session-id: user-42' localhost:8077/proxy/demo/x; echo
done

# 查询该会话当前固定在哪个实例、建立时间与剩余 TTL
curl -s localhost:8077/admin/sessions/demo/user-42 | python -m json.tool

# 非法会话标识（含空格）不报错，退化为加权选择
curl -s -H 'x-session-id: "bad id!!"' localhost:8077/proxy/demo/x; echo
```

## 4.8 平滑下线

```bash
# 1) 宣告 h1 平滑下线（无在途时立即退出）
curl -s -X POST localhost:8077/admin/instances/demo/h1/1/drain | python -m json.tool
#    -> {"status":"DRAINING_STARTED","deregistered":true,"in_flight":0,...}

# 2) 重复宣告：404 INSTANCE_DRAIN_NOT_FOUND（已退出，与成功可区分）
curl -i -X POST localhost:8077/admin/instances/demo/h1/1/drain

# 3) 下线过程中续约：若仍有在途（DRAINING）-> 409 INSTANCE_DRAINING；
#    已退出 -> 404 HEARTBEAT_UNKNOWN，结论确定、不会被续约复活
curl -i -X POST localhost:8077/admin/instances/demo/h1/1/heartbeat \
  -H 'content-type: application/json' -d '{}'

# 4) 带会话的请求：绑定实例 h1 退出后，自动切换到 h2 继续成功
curl -s -H 'x-session-id: user-42' localhost:8077/proxy/demo/x; echo
curl -s localhost:8077/admin/sessions/demo/user-42 | python -m json.tool
#    binding.instance 已变为 demo://h2:1，旧绑定被清理
```

> 观察“在途请求跑完才退出”：先用较低频率制造一个长请求（可把某实例 mode 设为
> `timeout` 之外的自定义延迟，或直接运行
> `uv run pytest tests/test_drain.py::test_inflight_request_runs_to_completion_then_instance_exits -v`
> 与 `tests/test_sticky_drain_gateway.py::test_inflight_dispatch_completes_after_drain_announcement`）。
> 日志会依次出现“平滑下线开始 … 在途请求=N”→“实例平滑退出完成 … 清理连接=0”。

## 5. 可解释性入口

- `GET /admin/status/{service}`：实例的健康/熔断/连接数/平滑下线状态（`drain`）
  与文字判定依据，以及该服务当前全部会话绑定（`sessions`）；
- `GET /admin/sessions/{service}/{session_id}`：某会话固定实例、建立时间与剩余 TTL；
- 服务端 JSON 日志：每次 dispatch 输出 `record`（每次尝试的上游、状态、错误 code、
  `sticky` 粘性动作与 `switch_reason`）与 `candidates`（每个候选被保留或剔除、
  含 `drain` 状态的原因）；下线流程输出“平滑下线开始/实例平滑退出完成/清理会话绑定”；
- 错误响应只包含 `code` 与 `message`，内部异常仅以 `INTERNAL_ERROR + request_id`
  返回，堆栈只写服务端日志。

会话粘性规则、平滑下线状态流转与兼容范围见 [STICKY_DRAIN.md](STICKY_DRAIN.md)。
