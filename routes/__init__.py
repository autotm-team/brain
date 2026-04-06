"""
Integration Service 路由模块

设置所有API路由和WebSocket连接。
"""

from typing import Any

from aiohttp import web

from .health import setup_health_routes
from .tasks import setup_task_routes
from .ui_bff import setup_ui_bff_routes
from .auth import setup_auth_routes


def setup_routes(app: web.Application, cors: Any = None):
    """设置所有路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 健康检查路由
    setup_health_routes(app, cors)
    
    # 定时任务管理路由
    setup_task_routes(app, cors)

    # Auth 路由（需要优先于 UI BFF catch-all 注册）
    setup_auth_routes(app, cors)

    # UI BFF 路由（前端统一入口）
    setup_ui_bff_routes(app, cors)
    
    # 静态文件路由（如果需要）
    # app.router.add_static('/', path='static', name='static')
