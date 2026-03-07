"""
服务管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.services import ServiceHandler


def setup_service_routes(app: web.Application, cors: CorsConfig = None):
    """设置服务管理路由

    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """

    # 创建处理器实例
    service_handler = ServiceHandler()

    # 存储到应用上下文
    app['service_handler'] = service_handler

    # 获取服务列表
    route = app.router.add_get('/api/v1/services', service_handler.list_services)
    if cors:
        cors.add(route)

    # 获取服务状态
    route = app.router.add_get('/api/v1/services/{service}/status', service_handler.get_service_status)
    if cors:
        cors.add(route)

    # 服务健康检查
    route = app.router.add_post('/api/v1/services/{service}/health-check', service_handler.health_check)
    if cors:
        cors.add(route)

    # 重启服务连接
    route = app.router.add_post('/api/v1/services/{service}/reconnect', service_handler.reconnect_service)
    if cors:
        cors.add(route)

    # 获取服务配置
    route = app.router.add_get('/api/v1/services/{service}/config', service_handler.get_service_config)
    if cors:
        cors.add(route)

    # 兼容前端操作（占位实现）
    route = app.router.add_post('/api/v1/services/{service}/start', service_handler.start_service)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/services/{service}/stop', service_handler.stop_service)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/services/{service}/restart', service_handler.restart_service)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/services/deploy', service_handler.deploy_service)
    if cors:
        cors.add(route)

    # Execution 回调（批量分析/回测完成通知）
    route = app.router.add_post('/api/v1/services/execution/callback/analyze', service_handler.execution_analyze_callback)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/services/execution/callback/backtest', service_handler.execution_backtest_callback)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/services/execution/callback/last', service_handler.get_last_execution_callback)
    if cors:
        cors.add(route)
