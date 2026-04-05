"""
健康检查处理器
"""

import time
from datetime import datetime

from aiohttp import web

from handlers.base import BaseHandler


class HealthHandler(BaseHandler):
    """健康检查处理器"""
    
    def __init__(self):
        super().__init__()
        self.start_time = time.time()
    
    async def health_check(self, request: web.Request) -> web.Response:
        """基础健康检查
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 健康状态响应
        """
        try:
            # 检查基本组件
            app = request.app
            config = app.get('config')
            
            health_data = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'uptime': time.time() - self.start_time,
                'version': '1.0.0',
                'environment': config.environment if config else 'unknown'
            }
            
            return self.success_response(health_data)
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return self.error_response("Health check failed", 503)
