"""
健康检查处理器
"""

import time
from datetime import datetime

from aiohttp import web

from handlers.base import BaseHandler
from api_info import get_api_info


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
    
    async def detailed_status(self, request: web.Request) -> web.Response:
        """详细状态信息
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: 详细状态响应
        """
        try:
            app = request.app
            config = app.get('config')
            
            # 检查各个组件状态
            components_status = {}
            
            # 检查系统协调器
            if 'coordinator' in app:
                coordinator = app['coordinator']
                components_status['coordinator'] = {
                    'status': 'running' if coordinator._is_running else 'stopped',
                    'active_cycles': len(coordinator._active_cycles)
                }
            
            # 检查信号路由器
            if 'signal_router' in app:
                signal_router = app['signal_router']
                components_status['signal_router'] = {
                    'status': 'running' if signal_router._is_running else 'stopped'
                }
            
            # 检查数据流管理器
            if 'data_flow_manager' in app:
                data_flow_manager = app['data_flow_manager']
                components_status['data_flow_manager'] = {
                    'status': 'running' if data_flow_manager._is_running else 'stopped'
                }
            
            # 检查统一调度器
            if 'unified_scheduler' in app:
                unified_scheduler = app['unified_scheduler']
                components_status['unified_scheduler'] = {
                    'status': 'running' if getattr(unified_scheduler, '_running', False) else 'stopped'
                }
            
            # 检查系统监控器
            if 'system_monitor' in app:
                system_monitor = app['system_monitor']
                components_status['system_monitor'] = {
                    'status': 'running' if system_monitor._is_running else 'stopped'
                }
            
            status_data = {
                'service': 'integration-service',
                'version': '1.0.0',
                'environment': config.environment if config else 'unknown',
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'uptime': time.time() - self.start_time,
                'components': components_status,
                'configuration': {
                    'host': config.service.host if config else 'unknown',
                    'port': config.service.port if config else 'unknown',
                    'debug': config.service.debug if config else False,
                    'scheduler_enabled': config.service.scheduler_enabled if config else False
                }
            }
            
            return self.success_response(status_data)
            
        except Exception as e:
            self.logger.error(f"Detailed status check failed: {e}")
            return self.error_response("Status check failed", 500)
    
    async def api_info(self, request: web.Request) -> web.Response:
        """API信息
        
        Args:
            request: HTTP请求对象
            
        Returns:
            web.Response: API信息响应
        """
        try:
            api_info = get_api_info()
            return self.success_response(api_info)
            
        except Exception as e:
            self.logger.error(f"API info failed: {e}")
            return self.error_response("API info failed", 500)
