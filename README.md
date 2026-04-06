# Brain 服务（统一 BFF 与任务控制面）

## 作用
`brain` 是当前前端唯一入口，负责：
- 为 `web/app` 提供统一的 `/api/v1/ui/*` BFF 接口
- 统一提供 `/api/v1/task-jobs*` 任务控制面
- 提供鉴权、调度、系统数据运维、用户与权限管理
- 在内部对接 `macro`、`execution`、`portfolio`、`flowhub`

这份 README 只描述**当前活跃合同**。已经移除或废弃的旧系统面、旧代理面、旧 `web/frontend` 对接路径不再作为正式接口说明。

## 依赖
- TimescaleDB
- Redis
- 下游服务：
  - `macro`
  - `execution`
  - `portfolio`
  - `flowhub`
- 运行时依赖：
  - `external/econdb`
  - `external/asyncron`

## 运行方式
### Docker（推荐）
```bash
docker-compose up -d brain
```

### 本地启动
```bash
cd services/brain
pip install -r requirements.txt
python main.py
```

## 端口
- HTTP: `8088`

## 配置
- Docker / 本地运行时配置统一以环境变量为准，定义见 `services/brain/config.py`
- 关键下游地址：
  - `BRAIN_MACRO_SERVICE_URL`
  - `BRAIN_PORTFOLIO_SERVICE_URL`
  - `BRAIN_EXECUTION_SERVICE_URL`
  - `BRAIN_FLOWHUB_SERVICE_URL`
- 鉴权配置：
  - `BRAIN_AUTH_ISSUER`
  - `BRAIN_AUTH_JWT_SECRET`
  - `BRAIN_AUTH_ACCESS_TOKEN_TTL_SECONDS`
  - `BRAIN_AUTH_REFRESH_TOKEN_TTL_SECONDS`
- 调度/初始化配置：
  - `BRAIN_SCHEDULER_TIMEZONE`
  - `BRAIN_INIT_DATA_ON_STARTUP`
  - `BRAIN_INIT_WAIT_DEPENDENCIES`
  - `BRAIN_DEFAULT_PORTFOLIO_ID`
- 控制面业务超时：
  - `BRAIN_CYCLE_TIMEOUT`
  - `BRAIN_STRATEGY_ANALYSIS_TIMEOUT_SECONDS`
  - `BRAIN_STRATEGY_VALIDATION_TIMEOUT_SECONDS`

## 当前正式公开接口

### 基础接口
- `GET /health`

### 鉴权接口
- `POST /api/v1/ui/auth/login`
- `POST /api/v1/ui/auth/refresh`
- `POST /api/v1/ui/auth/logout`
- `GET /api/v1/ui/auth/me`

### 统一任务控制面
- `POST /api/v1/task-jobs`
- `GET /api/v1/task-jobs`
- `GET /api/v1/task-jobs/types`
- `GET /api/v1/task-jobs/analytics`
- `GET /api/v1/task-jobs/{task_job_id}`
- `POST /api/v1/task-jobs/{task_job_id}/cancel`
- `GET /api/v1/task-jobs/{task_job_id}/history`

说明：
- 当前没有公开的 `/api/v1/task-jobs/{task_job_id}/lineage`
- lineage 视图通过 `GET /api/v1/ui/system/jobs/{task_job_id}/lineage` 暴露

### 控制面硬约束
- Brain 是唯一异步任务创建入口；Brain 内部任何下游异步任务都必须通过 `TaskOrchestrator.create_task_job(...)` 创建。
- `StrategyAdapter`、`ExecutionAdapter`、`FlowhubAdapter` 只保留只读查询或结果标准化语义，不再允许自己直发下游 `POST /api/v1/jobs`。
- `/api/v1/task-jobs/{task_job_id}/history` 返回的是合并视图：
  - 控制面快照事件来源 `brain_control_plane`
  - 下游执行面 `checkpoint / progress / heartbeat` 等真实运行事件来源 `service_runtime`
- `UnifiedScheduler` 对到期窗口先 claim、写下一次触发时间、再入队；schedule 持久化会保存 `dispatch_token / last_enqueued_at / pending_dispatch`，防止重启窗口重复造任务。
- 内部 `SystemCoordinator`、`SignalRouter` 涉及 strategy 分析/验证的流程，也必须通过 `TaskOrchestrator` 派发 execution task，不能再直接提交下游 job。

### 活跃 BFF 入口
- `GET|POST|PUT|PATCH|DELETE /api/v1/ui/*`

说明：
- 读取类接口同步返回
- 变更类接口通过任务控制面异步返回 `task_job_id`
- `brain` 不再对前端暴露旧的 `/api/v1/services*`、`/api/v1/system*`、`/api/v1/monitoring*`、`/api/v1/dataflow*`、原始 `/api/v1/signals*`、原始 `/api/v1/schedules*` 等旧接口面

## web/app 主链路

| `web/app` 业务域 | 前端路径族 | Brain 角色 | 下游归属 | 方式 |
|---|---|---|---|---|
| 登录与会话 | `/api/v1/ui/auth/*` | Brain 直接处理 | `brain` | 同步 |
| 基础可达性 | `/health` | Brain 直接处理 | `brain` | 同步 |
| 任务中心 | `/api/v1/task-jobs*` | Brain 直接处理 | `brain` | 同步 |
| 顶栏上下文/搜索 | `/api/v1/ui/shell/context`, `/api/v1/ui/search` | Brain 聚合 | `brain` | 同步 |
| 市场快照 | `/api/v1/ui/market-snapshot/*` | BFF 转发/编排 | `macro` | 读同步，写异步 |
| 宏观周期 | `/api/v1/ui/macro-cycle/*` | BFF 转发/编排 | `macro` | 读同步，写异步 |
| 结构轮动 | `/api/v1/ui/structure-rotation/*` | BFF 转发/编排 | `macro` | 读同步，写异步 |
| 候选池 | `/api/v1/ui/candidates/*` | BFF 转发/编排 | `execution` | 读同步，写异步 |
| 标的研究 | `/api/v1/ui/research/*` | BFF 转发/编排 | `execution` | 读同步，写异步 |
| 策略与报告 | `/api/v1/ui/strategy/*` | BFF 转发/编排 | `execution` | 读同步，写异步 |
| 模拟执行 | `/api/v1/ui/execution/sim/orders*`, `/api/v1/ui/execution/sim/positions` | BFF 转发/编排 | `portfolio` | `GET` 同步，`POST` 异步 |
| 系统任务/调度/告警/审计 | `/api/v1/ui/system/jobs/*`, `/api/v1/ui/system/schedules/*`, `/api/v1/ui/system/alerts*`, `/api/v1/ui/system/audit`, `/api/v1/ui/system/rules`, `/api/v1/ui/system/health` | Brain 内部处理 | `brain` | 同步 |
| 系统数据运维 | `/api/v1/ui/system/data/*` | Brain 主导，必要时调下游 | `brain` / `flowhub` | 混合 |
| 系统设置 | `/api/v1/ui/system/settings*` | Brain 内部处理 | `brain` | 同步 |
| 用户与权限 | `/api/v1/ui/system/users*` | Brain 内部处理 | `brain` | 同步 |

## 附录：接口清单（按活跃前端调用归档）

### 1. Brain 直接处理

| 路径族 | 方式 | 主要页面/调用方 | 说明 |
|---|---|---|---|
| `/health` | 同步 | `LoginPage` | 基础可达性检查 |
| `/api/v1/task-jobs*` | 同步 | `uiClient`、`SystemJobsPanel`、`WatchlistPanel` | 统一任务控制面 |
| `/api/v1/ui/auth*` | 同步 | `uiClient` | 登录、刷新、退出、当前用户 |
| `/api/v1/ui/shell/context` | 同步 | `App.tsx` | 顶栏上下文 |
| `/api/v1/ui/search` | 同步 | `TopBar` | 全局搜索 |
| `/api/v1/ui/system/jobs*` | 同步 | `SystemJobsPanel`、`SystemSettingsPanel` | Brain 内部任务视图 |
| `/api/v1/ui/system/schedules*` | 同步 | `SystemJobsPanel` | Brain 内部调度视图 |
| `/api/v1/ui/system/alerts*` | 同步 | `SystemJobsPanel` | Brain 内部告警视图 |
| `/api/v1/ui/system/health` | 同步 | `SystemJobsPanel`、`SystemSettingsPanel` | Brain 系统健康聚合 |
| `/api/v1/ui/system/audit` | 同步 | `SystemJobsPanel`、`SystemSettingsPanel`、`SystemUsersPanel` | 审计日志 |
| `/api/v1/ui/system/rules` | 同步 | `SystemJobsPanel`、`SystemSettingsPanel` | 规则管理 |
| `/api/v1/ui/system/settings*` | 同步 | `SystemSettingsPanel` | 系统设置 |
| `/api/v1/ui/system/users*` | 同步 | `SystemUsersPanel` | 用户、角色、会话、审批、令牌、策略 |

### 2. Brain 主导，必要时调下游

| 路径族 | 方式 | 主要页面/调用方 | 下游 |
|---|---|---|---|
| `/api/v1/ui/system/data*` | 混合 | `SystemDataPanel`、`SystemSettingsPanel` | `brain` / `flowhub` |

说明：
- 列表、详情、日志、质量、通知等读取类接口同步返回
- 数据同步、回填、流水线控制等操作通过 `brain` 发起，再统一回到任务控制面

### 3. Brain 转发到 macro

| 路径族 | 方式 | 主要页面/调用方 |
|---|---|---|
| `/api/v1/ui/market-snapshot*` | 读同步，写异步 | `OverviewPanel` |
| `/api/v1/ui/macro-cycle*` | 读同步，写异步 | `MacroPanel` |
| `/api/v1/ui/structure-rotation*` | 读同步，写异步 | `StructurePanel`、`FlowMigrationPanel` |

### 4. Brain 转发到 execution

| 路径族 | 方式 | 主要页面/调用方 |
|---|---|---|
| `/api/v1/ui/candidates*` | 读同步，写异步 | `WatchlistPanel` |
| `/api/v1/ui/research*` | 读同步，写异步 | `StockPanel`、`WatchlistPanel` |
| `/api/v1/ui/strategy*` | 读同步，写异步 | `StrategyPanel` |

### 5. Brain 转发到 portfolio

| 路径族 | 方式 | 主要页面/调用方 |
|---|---|---|
| `/api/v1/ui/execution/sim/orders*` | `GET` 同步，`POST` 异步 | `StrategyPanel` |
| `/api/v1/ui/execution/sim/positions` | 同步 | `StrategyPanel` |

## 非活跃前端代码说明
- `web/app` 中存在一个未引用的旧客户端文件：`web/app/src/api/marketSnapshotApi.ts`
- 该文件使用的是 `/api/v1/market/snapshot/*` 直连旧接口风格，不属于当前活跃合同
- 当前 README 不将其视为正式前端对接面

## 设计边界
- `brain` 是前端唯一 BFF，活跃前端对接以 `web/app` 为准
- `brain` 不再为旧 `web/frontend` 维护兼容接口面
- `brain` 不负责定义下游服务的业务资源契约，只负责消费正式契约并向前端收口
- `brain` 不应新增占位成功接口；不支持的能力必须显式报错

## 当前实现备注
- `ui_bff.py` 只保留当前活跃主链入口，其余大块逻辑已拆到：
  - `handlers/ui_bff_readiness.py`
  - `handlers/ui_bff_execution.py`
  - `handlers/ui_bff_system_ops.py`
  - `handlers/ui_bff_system_data.py`
- `task_orchestrator.py` 保留任务入口与公共控制逻辑，自动链与补偿恢复逻辑拆到 `task_orchestrator_auto_chain.py`
- `SystemCoordinator` 的完整分析周期语义保持不变：strategy 分析/验证阶段会通过控制面创建 execution task，并通过控制面等待任务终态后再继续下一阶段
- `SignalRouter` 的 strategy 路由阶段会通过控制面创建 execution task，由上层调用方按需要消费该异步结果
- `brain -> portfolio` 已不再假设 `default_portfolio` 必然存在；会优先使用显式 `portfolio_id`，再尝试配置与自动解析

## 验证
常用回归命令：
```bash
cd services/brain
python -m pytest tests -q
python -m compileall .
```
