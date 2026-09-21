# 服务注册与动态上游路由网关（本地可验证）

在 FastAPI 框架内实现的一套**服务注册 + 动态上游路由网关**，使用进程内模拟上游
（`MockUpstream`）替代真实后端，使注册续约、健康探测、路由选择与故障处置在
HTTP、单测、日志三个入口下结论一致且可解释。

## 能力一览

- **注册 / 注销 / 心跳续约**：租约过期实例不再被选中且不能复活；重复注册
  （`REG_DUPLICATE`）与重复注销（`UNREG_NOT_FOUND`）给出可区分结果，不静默覆盖。
- **路径 + 请求头路由与加权分配**：权重为 0、健康下线、熔断打开的上游不参与；
  无规则（`NO_ROUTE_RULE`）、无实例（`NO_AVAILABLE_UPSTREAM`）、全不可用
  （`ALL_UPSTREAMS_DOWN`）三类失败原因可区分。
- **主动 + 被动健康检查**：三态（HEALTHY/SUSPECT/DOWN）+ 抖动容忍，连续失败才摘除，
  连续探测成功才恢复。
- **失败重试 + 熔断**：仅幂等只读请求、仅瞬时失败可重试；熔断 OPEN 快速失败；
  HALF_OPEN 恢复探测在并发下只放行一个。
- **连接/并发上限、上游超时与背压**：超限 429、超时 504；租约式连接计数在任何
  路径下都精确释放，无残留、无重复扣减。
- **并发一致性**：注册表、健康状态、连接计数均有锁保护；注册与摘除同时发生时
  不会选到已摘除实例。
- **失败归一化**：不可达 / 名称无法解析 / 超时 / 上游错误 / 内部异常归一化为不同
  code，内部堆栈不透出对外接口。

## 快速开始

```bash
uv sync
uv run pytest -v                      # 50+ 用例，含租约/抖动/熔断恢复/并发摘除
uv run uvicorn main:app --port 8077   # 启动后访问 /docs
```

## 模块结构

```
gateway/
  errors.py      # 可区分的失败 code 与异常层次
  clock.py       # 可注入时钟（测试控制租约/抖动/熔断时间）
  models.py      # Instance / Lease / RouteRule / Request / Response
  registry.py    # 注册表：注册/注销/心跳/租约过期（asyncio.Lock）
  health.py      # 三态健康检查与主动探测（抖动容忍、恢复阈值）
  circuit.py     # CLOSED/OPEN/HALF_OPEN 熔断与单飞恢复探测
  pool.py        # 每上游/全局连接上限，幂等租约
  upstream.py    # 本地模拟上游（ok/不可达/解析失败/超时/500/崩溃）
  transport.py   # 异常归一化 + 超时控制
  router.py      # 规则匹配、健康/权重/熔断过滤、加权随机
  retry.py       # 安全重试判定
  engine.py      # 编排：选路→熔断→连接→超时→反馈→重试→清理
  api.py         # HTTP 接口与统一错误响应
  bootstrap.py   # 本地装配 + 后台租约/探测循环
tests/           # 按模块的单测与并发/HTTP 集成测试
docs/            # 协议、规则与本地验证指南
```

## 文档

- [注册与路由协议](docs/PROTOCOL.md)
- [路由、健康检查与熔断规则](docs/RULES.md)
- [本地验证指南](docs/LOCAL_VERIFICATION.md)
