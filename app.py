"""
Integration Service 应用工厂

创建和配置 aiohttp 应用实例，集成所有必要的组件。
"""

import logging
import asyncio
import inspect
import os
from aiohttp import web
from aiohttp_cors import setup as cors_setup, ResourceOptions

from asyncron import UnifiedScheduler
import redis.asyncio as redis
from config import IntegrationConfig
from container import setup_container
from interfaces import ISystemCoordinator, ISignalRouter, IDataFlowManager
from middleware import setup_middleware
from routes import setup_routes
from adapters.service_registry import ServiceRegistry
from adapters.macro_adapter import MacroAdapter
from adapters.execution_adapter import ExecutionAdapter
from monitors.system_monitor import SystemMonitor
from initializers.data_initializer import DataInitializationCoordinator
from task_orchestrator import TaskOrchestrator
from auth_service import AuthService

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _cors_allowed_origins() -> list[str]:
    raw = os.getenv("BRAIN_CORS_ALLOWED_ORIGINS", "").strip()
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    return [
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:4173",
        "http://localhost:4173",
    ]


def _flowhub_bootstrap_schedule_specs() -> list[dict]:
    return [
        {
            "bootstrap_key": "research_daily_stock_basic",
            "job_type": "stock_basic_data",
            "cron": os.getenv("FLOWHUB_RESEARCH_STOCK_BASIC_CRON", "20 17 * * 1-5"),
            "params": {"update_mode": "incremental"},
        },
        {
            "bootstrap_key": "strategy_daily_index_daily",
            "job_type": "index_daily_data",
            "cron": os.getenv("FLOWHUB_STRATEGY_INDEX_DAILY_CRON", "30 18 * * 1-5"),
            "params": {
                "update_mode": "incremental",
                "index_codes": ["000300.SH", "000905.SH", "000852.SH", "000001.SH", "399001.SZ", "399006.SZ"],
            },
        },
        {
            "bootstrap_key": "strategy_daily_trade_calendar",
            "job_type": "trade_calendar_data",
            "cron": os.getenv("FLOWHUB_STRATEGY_TRADE_CAL_CRON", "35 18 * * 1-5"),
            "params": {"exchange": "SSE", "update_mode": "incremental"},
        },
        {
            "bootstrap_key": "strategy_weekly_index_components",
            "job_type": "index_components",
            "cron": os.getenv("FLOWHUB_STRATEGY_INDEX_COMPONENTS_CRON", "30 16 * * 6"),
            "params": {
                "update_mode": "snapshot",
                "index_codes": ["000300.SH", "000905.SH", "000852.SH", "000001.SH", "399001.SZ", "399006.SZ"],
            },
        },
        {
            "bootstrap_key": "strategy_monthly_sw_industry",
            "job_type": "sw_industry_data",
            "cron": os.getenv("FLOWHUB_STRATEGY_SW_INDUSTRY_CRON", "30 16 1 * *"),
            "params": {"src": "SW2021", "update_mode": "incremental", "include_members": True},
        },
        {
            "bootstrap_key": "research_daily_suspend",
            "job_type": "suspend_data",
            "cron": os.getenv("FLOWHUB_RESEARCH_SUSPEND_CRON", "40 18 * * 1-5"),
            "params": {"update_mode": "incremental"},
        },
        {
            "bootstrap_key": "research_daily_st_status",
            "job_type": "st_status_data",
            "cron": os.getenv("FLOWHUB_RESEARCH_ST_STATUS_CRON", "45 18 * * 1-5"),
            "params": {"update_mode": "incremental"},
        },
        {
            "bootstrap_key": "research_daily_stk_limit",
            "job_type": "stk_limit_data",
            "cron": os.getenv("FLOWHUB_RESEARCH_STK_LIMIT_CRON", "50 18 * * 1-5"),
            "params": {"update_mode": "incremental"},
        },
    ]


def _is_legacy_dependent_stock_bootstrap_schedule(item: dict) -> bool:
    if not isinstance(item, dict):
        return False
    if not bool(item.get("enabled", True)):
        return False
    job_type = str(item.get("job_type") or "").strip().lower()
    if job_type not in {"batch_daily_ohlc", "batch_daily_basic"}:
        return False
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    bootstrap_key = str(metadata.get("bootstrap_key") or "").strip().lower()
    bootstrap_source = str(metadata.get("bootstrap_source") or "").strip().lower()
    return (
        bootstrap_key in {"strategy_daily_batch_ohlc", "research_daily_batch_basic"}
        or bootstrap_source == "brain_startup"
    )


async def _list_all_flowhub_schedules(orchestrator: TaskOrchestrator) -> list[dict]:
    limit = 100
    offset = 0
    items: list[dict] = []
    while True:
        payload = await orchestrator.list_schedules(service="flowhub", limit=limit, offset=offset)
        page = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(page, list) or not page:
            break
        items.extend(page)
        offset += len(page)
        total = int(payload.get("total") or 0) if isinstance(payload, dict) else 0
        if total and offset >= total:
            break
        if len(page) < limit:
            break
    return items


async def _ensure_flowhub_bootstrap_schedules(app: web.Application) -> None:
    if not _env_bool("BRAIN_FLOWHUB_BOOTSTRAP_ENABLED", True):
        logger.info("Skip flowhub bootstrap schedules: BRAIN_FLOWHUB_BOOTSTRAP_ENABLED=false")
        return

    orchestrator = app.get("task_orchestrator")
    if not orchestrator:
        return

    existing = await _list_all_flowhub_schedules(orchestrator)
    for item in existing:
        if not _is_legacy_dependent_stock_bootstrap_schedule(item):
            continue
        schedule_id = str(item.get("id") or "").strip()
        if not schedule_id:
            continue
        try:
            await orchestrator.patch_schedule(schedule_id, {"enabled": False})
            item["enabled"] = False
            logger.info(
                "Disabled legacy bootstrap schedule that bypassed stock_basic_data prerequisite: %s (%s)",
                item.get("job_type"),
                schedule_id,
            )
        except Exception as exc:
            logger.warning(f"Failed to disable legacy bootstrap schedule {schedule_id}: {exc}")

    existing_keys: dict[str, dict] = {}
    for item in existing:
        if not isinstance(item, dict):
            continue
        if not bool(item.get("enabled", True)):
            continue
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        key = str(metadata.get("bootstrap_key") or "").strip()
        if key:
            existing_keys[key] = item

    created_ids: list[str] = []
    for spec in _flowhub_bootstrap_schedule_specs():
        key = spec["bootstrap_key"]
        if key in existing_keys:
            continue
        payload = {
            "service": "flowhub",
            "job_type": spec["job_type"],
            "trigger": "cron",
            "cron": spec["cron"],
            "enabled": True,
            "params": dict(spec.get("params") or {}),
            "metadata": {
                "bootstrap_key": key,
                "bootstrap_source": "brain_startup",
            },
        }
        try:
            created = await orchestrator.create_schedule(payload)
            created_id = str(created.get("id") or "").strip()
            if created_id:
                created_ids.append(created_id)
                logger.info(f"Bootstrap schedule created: {key} ({created_id})")
        except Exception as exc:
            logger.warning(f"Failed to create flowhub bootstrap schedule {key}: {exc}")

    if not created_ids:
        return

    config = app.get("config")
    init_on_startup = bool(getattr(getattr(config, "service", None), "init_data_on_startup", True))
    if init_on_startup:
        logger.info(
            "Skip one-shot bootstrap schedule trigger because DataInitializationCoordinator is enabled"
        )
        return

    if not _env_bool("BRAIN_FLOWHUB_BOOTSTRAP_TRIGGER_CREATED", True):
        return

    for schedule_id in created_ids:
        try:
            await orchestrator.trigger_schedule(schedule_id)
            logger.info(f"Triggered bootstrap flowhub schedule once: {schedule_id}")
        except Exception as exc:
            logger.warning(f"Failed to trigger bootstrap schedule {schedule_id}: {exc}")


async def create_app(config: IntegrationConfig) -> web.Application:
    """创建 aiohttp 应用实例

    Args:
        config: 集成配置对象

    Returns:
        web.Application: 配置完成的应用实例
    """
    logger.info("Creating Integration Service application")

    # 创建应用
    app = web.Application()

    # 存储配置
    app['config'] = config
    app['cors_allowed_origins'] = _cors_allowed_origins()

    # 设置CORS
    cors = cors_setup(app, defaults={
        origin: ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
        for origin in app['cors_allowed_origins']
    })
    app['cors_enabled'] = True

    # 设置中间件
    setup_middleware(app)

    # 设置路由
    setup_routes(app, cors)

    # 初始化核心组件
    await init_components(app, config)

    # 设置启动和清理钩子
    app.on_startup.append(startup_handler)
    app.on_cleanup.append(cleanup_handler)

    logger.info("Integration Service application created successfully")
    return app


async def init_components(app: web.Application, config: IntegrationConfig):
    """初始化核心组件

    Args:
        app: aiohttp 应用实例
        config: 集成配置对象
    """
    logger.info("Initializing core components")

    try:
        # 依赖注入容器
        app['container'] = await setup_container(config)

        # 服务注册表
        app['service_registry'] = ServiceRegistry(config)
        app['task_orchestrator'] = TaskOrchestrator(app)
        app['task_runtime_api'] = app['task_orchestrator']._get_task_api()

        # Redis客户端（供调度器/分析触发使用）
        try:
            app['redis'] = redis.from_url(
                config.redis.url,
                encoding='utf-8',
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=10,
                retry_on_timeout=True,
                max_connections=50,
                health_check_interval=15,
            )
            await app['redis'].ping()
            logger.info(f"Redis client initialized: {config.redis.url}")
        except Exception as redis_err:
            logger.warning(f"Failed to initialize Redis client: {redis_err}")
            app['redis'] = None

        # 鉴权服务（JWT + Refresh + Redis 会话索引）
        app['auth_service'] = AuthService(config=config, redis_client=app.get('redis'))
        await app['auth_service'].initialize()
        logger.info("Auth service initialized")

        async def _dispatch_schedule(payload: dict) -> None:
            schedule = payload.get("schedule") if isinstance(payload, dict) else {}
            if not isinstance(schedule, dict):
                return
            schedule_id = str(payload.get("schedule_id") or schedule.get("id") or "").strip()
            if not schedule_id:
                return
            orchestrator = app.get("task_orchestrator")
            scheduler = app.get("unified_scheduler")
            if not orchestrator or not scheduler:
                return
            metadata = dict(schedule.get("metadata") or {})
            metadata["schedule_id"] = schedule_id
            created = await orchestrator.create_task_job(
                service=str(schedule.get("service") or ""),
                job_type=str(schedule.get("job_type") or ""),
                params=dict(schedule.get("params") or {}),
                metadata=metadata,
                lineage_context={
                    "root_schedule_id": schedule_id,
                    "trigger_kind": "schedule",
                },
            )
            task_job_id = str(created.get("id") or "").strip()
            if not task_job_id:
                raise RuntimeError(f"schedule dispatch did not return task job id: {schedule_id}")
            await scheduler.mark_triggered(
                schedule_id,
                task_job_id,
                dispatch_token=str(payload.get("dispatch_token") or "").strip() or None,
            )

        app['unified_scheduler'] = UnifiedScheduler(
            redis_client=app.get('redis'),
            namespace='asyncron:v2:brain',
            storage_api=app.get("task_runtime_api"),
            on_dispatch=_dispatch_schedule,
            poll_interval_seconds=1,
            timezone_name=config.service.scheduler_timezone,
        )

        # 核心组件（通过DI解析）
        app['coordinator'] = await app['container'].resolve(ISystemCoordinator)
        app['signal_router'] = await app['container'].resolve(ISignalRouter)
        app['data_flow_manager'] = await app['container'].resolve(IDataFlowManager)
        if hasattr(app['coordinator'], 'set_task_orchestrator'):
            app['coordinator'].set_task_orchestrator(app.get('task_orchestrator'))
        if hasattr(app['signal_router'], 'set_task_orchestrator'):
            app['signal_router'].set_task_orchestrator(app.get('task_orchestrator'))

        # 系统监控器
        app['system_monitor'] = SystemMonitor(config)

        # 服务适配器
        # Macro服务适配器
        app['macro_adapter'] = MacroAdapter(config)
        logger.info(f"MacroAdapter initialized with URL: {config.service.macro_service_url}")

        # Execution服务适配器
        app['execution_adapter'] = ExecutionAdapter(config)
        logger.info(f"ExecutionAdapter initialized with URL: {config.service.execution_service_url}")

        # 监控器组件引用
        app['system_monitor'].set_component_references(
            system_coordinator=app.get('coordinator'),
            signal_router=app.get('signal_router'),
            data_flow_manager=app.get('data_flow_manager'),
            macro_adapter=app.get('macro_adapter')
        )

        logger.info("Core components initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize components: {e}")
        raise


async def startup_handler(app: web.Application):
    """应用启动处理器"""
    logger.info("Starting Integration Service components")

    # 优先调度数据初始化任务（即使后续组件启动失败也不影响初始化尝试）
    try:
        config = app['config']
        if getattr(config.service, 'init_data_on_startup', True) and 'data_initializer' not in app:
            initializer = DataInitializationCoordinator(
                config,
                task_orchestrator=app.get("task_orchestrator"),
            )
            app['data_initializer'] = initializer
            asyncio.create_task(initializer.run())
            logger.info("Background data initialization task scheduled")
    except Exception as e:
        logger.warning(f"Failed to schedule data initialization: {e}")

    # 清除旧 boot cycle 的 auto_chain claims，防止跨重启残留导致链式触发被阻塞
    try:
        redis_client = app.get('redis')
        if redis_client:
            cleared = 0
            async for key in redis_client.scan_iter(match="brain:auto_chain:*"):
                await redis_client.delete(key)
                cleared += 1
            if cleared:
                logger.info("Cleared %d stale auto_chain claims from previous boot cycle", cleared)
    except Exception as clear_err:
        logger.warning(f"Failed to clear auto_chain claims: {clear_err}")

    try:
        # 启动服务注册表
        await app['service_registry'].start()

        # 预热统一任务类型目录（以各服务 /api/v1/jobs/types 为唯一来源）
        try:
            orchestrator = app.get("task_orchestrator")
            if orchestrator:
                await orchestrator.list_task_job_types()
                logger.info("Task job types catalog warmed up")
        except Exception as warmup_err:
            logger.warning(f"Task job types warmup failed: {warmup_err}")

        scheduler_enabled = bool(getattr(app["config"].service, "scheduler_enabled", True))
        if "unified_scheduler" in app and scheduler_enabled:
            await app["unified_scheduler"].start()
            logger.info("Unified scheduler started")
        elif "unified_scheduler" in app:
            logger.info("Unified scheduler startup skipped: SCHEDULER_ENABLED=false")

        orchestrator = app.get("task_orchestrator")
        if orchestrator:
            async def _orphan_task_monitor() -> None:
                while True:
                    try:
                        result = await orchestrator.scan_orphaned_task_jobs_once()
                        if any(int(result.get(key) or 0) > 0 for key in ("cancelled", "recreated", "capped")):
                            logger.info(
                                "Orphan task scan completed: scanned=%s cancelled=%s recreated=%s capped=%s skipped=%s",
                                result.get("scanned"),
                                result.get("cancelled"),
                                result.get("recreated"),
                                result.get("capped"),
                                result.get("skipped"),
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception as scan_err:
                        logger.warning(f"Orphan task monitor scan failed: {scan_err}")
                    await asyncio.sleep(getattr(orchestrator, "ORPHAN_MONITOR_INTERVAL_SECONDS", 30))

            app["orphan_task_monitor"] = asyncio.create_task(_orphan_task_monitor())
            logger.info("Orphan task monitor started")

            async def _task_retention_monitor() -> None:
                while True:
                    try:
                        deleted = await orchestrator.cleanup_completed_task_jobs(max_age_hours=24 * 30)
                        if deleted:
                            logger.info("Brain task retention cleanup removed %s completed task jobs", deleted)
                    except asyncio.CancelledError:
                        raise
                    except Exception as cleanup_err:
                        logger.warning(f"Brain task retention cleanup failed: {cleanup_err}")
                    await asyncio.sleep(3600)

            app["task_retention_monitor"] = asyncio.create_task(_task_retention_monitor())
            logger.info("Brain task retention monitor started")

        # Flowhub 抓取任务由 Brain 统一补齐与触发，Flowhub 本身不再自建调度。
        try:
            await _ensure_flowhub_bootstrap_schedules(app)
        except Exception as bootstrap_err:
            logger.warning(f"Flowhub bootstrap schedules initialization failed: {bootstrap_err}")

        # 启动系统协调器
        await app['coordinator'].start()

        # 启动信号路由器
        await app['signal_router'].start()

        # 启动数据流管理器
        await app['data_flow_manager'].start()

        # 启动系统监控器
        await app['system_monitor'].start()

        logger.info("All components started successfully")

        # 执行系统启动协调
        startup_result = await app['coordinator'].coordinate_system_startup()
        if startup_result:
            logger.info("System startup coordination completed successfully")
        else:
            logger.warning("System startup coordination completed with warnings")

    except Exception as e:
        logger.error(f"Failed to start components: {e}")
        # 不再向上抛出异常，避免阻断服务整体启动（初始化任务已在上方调度）
        # raise


async def cleanup_handler(app: web.Application):
    """应用清理处理器"""
    logger.info("Stopping Integration Service components")

    try:
        orphan_task_monitor = app.get("orphan_task_monitor")
        if orphan_task_monitor:
            orphan_task_monitor.cancel()
            try:
                await orphan_task_monitor
            except asyncio.CancelledError:
                pass

        task_retention_monitor = app.get("task_retention_monitor")
        if task_retention_monitor:
            task_retention_monitor.cancel()
            try:
                await task_retention_monitor
            except asyncio.CancelledError:
                pass

        if "unified_scheduler" in app:
            await app["unified_scheduler"].stop()

        # 停止系统监控器
        if 'system_monitor' in app:
            await app['system_monitor'].stop()

        # 停止数据流管理器
        if 'data_flow_manager' in app:
            await app['data_flow_manager'].stop()

        # 停止信号路由器
        if 'signal_router' in app:
            await app['signal_router'].stop()

        # 停止系统协调器
        if 'coordinator' in app:
            await app['coordinator'].stop()

        # 停止服务注册表
        if 'service_registry' in app:
            await app['service_registry'].stop()

        # 关闭Redis客户端
        redis_client = app.get('redis')
        if redis_client:
            close_result = redis_client.close()
            if inspect.isawaitable(close_result):
                await close_result

        logger.info("All components stopped successfully")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


def create_test_app(config: IntegrationConfig = None) -> web.Application:
    """创建测试应用实例（同步版本）

    Args:
        config: 可选的配置对象，如果不提供则使用测试配置

    Returns:
        web.Application: 测试应用实例
    """
    if config is None:
        config = IntegrationConfig(environment="testing")

    # 在新的事件循环中创建应用
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        app = loop.run_until_complete(create_app(config))
        return app
    finally:
        loop.close()
