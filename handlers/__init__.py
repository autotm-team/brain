"""
Integration Service 处理器模块

包含当前仍启用的API请求处理器。
"""

from handlers.health import HealthHandler
from handlers.tasks import TaskHandler

__all__ = [
    'HealthHandler',
    'TaskHandler',
]
