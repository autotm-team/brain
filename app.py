"""
Integration Service 应用工厂

创建和配置 aiohttp 应用实例，集成所有必要的组件。
"""

import logging
import asyncio
import inspect
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

    # 设置CORS
    cors = cors_setup(app, defaults={
        "*": ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
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

        # Redis客户端（供调度器/分析触发使用）
        try:
            app['redis'] = redis.from_url(
                config.redis.url,
                encoding='utf-8',
                decode_responses=True
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
            task_job_id = None
            try:
                created = await orchestrator.create_task_job(
                    service=str(schedule.get("service") or ""),
                    job_type=str(schedule.get("job_type") or ""),
                    params=dict(schedule.get("params") or {}),
                    metadata=metadata,
                )
                task_job_id = created.get("id")
            finally:
                await scheduler.mark_triggered(schedule_id, task_job_id)

        app['unified_scheduler'] = UnifiedScheduler(
            redis_client=app.get('redis'),
            namespace='asyncron:v2:brain',
            on_dispatch=_dispatch_schedule,
            poll_interval_seconds=1,
        )

        # 核心组件（通过DI解析）
        app['coordinator'] = await app['container'].resolve(ISystemCoordinator)
        app['signal_router'] = await app['container'].resolve(ISignalRouter)
        app['data_flow_manager'] = await app['container'].resolve(IDataFlowManager)

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
            initializer = DataInitializationCoordinator(config)
            app['data_initializer'] = initializer
            asyncio.create_task(initializer.run())
            logger.info("Background data initialization task scheduled")
    except Exception as e:
        logger.warning(f"Failed to schedule data initialization: {e}")

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

        if "unified_scheduler" in app:
            await app["unified_scheduler"].start()
            logger.info("Unified scheduler started")

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
