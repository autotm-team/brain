"""
健康检查路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.health import HealthHandler


def setup_health_routes(app: web.Application, cors: CorsConfig = None):
    """设置健康检查路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    health_handler = HealthHandler()
    
    # 存储到应用上下文
    app['health_handler'] = health_handler
    
    # 基础健康检查
    route = app.router.add_get('/health', health_handler.health_check)
    if cors:
        cors.add(route)
