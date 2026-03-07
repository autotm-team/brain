"""
服务管理处理器
"""

from aiohttp import web

from handlers.base import BaseHandler


class ServiceHandler(BaseHandler):
    """服务管理处理器"""

    async def list_services(self, request: web.Request) -> web.Response:
        """获取服务列表"""
        try:
            service_registry = self.get_app_component(request, 'service_registry')
            services = await service_registry.get_all_services()
            return self.success_response(services)
        except Exception as e:
            self.logger.error(f"List services failed: {e}")
            return self.error_response("获取服务列表失败", 500)

    async def get_service_status(self, request: web.Request) -> web.Response:
        """获取服务状态"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            status = await service_registry.get_service_status(service_name)
            return self.success_response(status)
        except Exception as e:
            self.logger.error(f"Get service status failed: {e}")
            return self.error_response("获取服务状态失败", 500)

    async def health_check(self, request: web.Request) -> web.Response:
        """服务健康检查"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            health = await service_registry.health_check_service(service_name)
            return self.success_response(health)
        except Exception as e:
            self.logger.error(f"Service health check failed: {e}")
            return self.error_response("服务健康检查失败", 500)

    async def reconnect_service(self, request: web.Request) -> web.Response:
        """重连服务"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            result = await service_registry.reconnect_service(service_name)
            return self.success_response(result, "服务重连成功")
        except Exception as e:
            self.logger.error(f"Service reconnect failed: {e}")
            return self.error_response("服务重连失败", 500)

    async def get_service_config(self, request: web.Request) -> web.Response:
        """获取服务配置"""
        try:
            service_name = self.get_path_params(request)['service']
            service_registry = self.get_app_component(request, 'service_registry')
            config = await service_registry.get_service_config(service_name)
            return self.success_response(config)
        except Exception as e:
            self.logger.error(f"Get service config failed: {e}")
            return self.error_response("获取服务配置失败", 500)

    async def start_service(self, request: web.Request) -> web.Response:
        """启动服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'start'}, "服务启动请求已受理")
        except Exception as e:
            self.logger.error(f"Start service failed: {e}")
            return self.error_response("服务启动失败", 500)

    async def stop_service(self, request: web.Request) -> web.Response:
        """停止服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'stop'}, "服务停止请求已受理")
        except Exception as e:
            self.logger.error(f"Stop service failed: {e}")
            return self.error_response("服务停止失败", 500)

    async def restart_service(self, request: web.Request) -> web.Response:
        """重启服务（占位实现）"""
        try:
            service_name = self.get_path_params(request)['service']
            return self.success_response({'service': service_name, 'action': 'restart'}, "服务重启请求已受理")
        except Exception as e:
            self.logger.error(f"Restart service failed: {e}")
            return self.error_response("服务重启失败", 500)

    async def deploy_service(self, request: web.Request) -> web.Response:
        """部署服务（占位实现）"""
        try:
            payload = await self.get_request_json(request)
            return self.success_response({'deployment': payload}, "部署请求已受理")
        except Exception as e:
            self.logger.error(f"Deploy service failed: {e}")
            return self.error_response("服务部署失败", 500)

    async def execution_analyze_callback(self, request: web.Request) -> web.Response:
        """ Execution 
        app['last_execution_callback']
        """
        try:
            payload = await request.json()
            request.app['last_execution_callback'] = payload
            # 
            event_engine = request.app.get('event_engine')
            if event_engine:
                await event_engine.publish('execution.analyze.completed', payload)
            return self.success_response({'received': True})
        except Exception as e:
            self.logger.error(f"Execution analyze callback failed: {e}")
            return self.error_response("", 500)

    async def execution_backtest_callback(self, request: web.Request) -> web.Response:
        """ Execution 
        """
        try:
            payload = await request.json()
            request.app['last_execution_callback'] = payload
            event_engine = request.app.get('event_engine')
            if event_engine:
                await event_engine.publish('execution.backtest.completed', payload)
            return self.success_response({'received': True})
        except Exception as e:
            self.logger.error(f"Execution backtest callback failed: {e}")
            return self.error_response("", 500)

    async def get_last_execution_callback(self, request: web.Request) -> web.Response:
        """调试用途：获取最近一次接收到的Execution回调载荷"""
        try:
            payload = request.app.get('last_execution_callback')
            return self.success_response(payload or {})
        except Exception as e:
            self.logger.error(f"Get last execution callback failed: {e}")
            return self.error_response("获取回调信息失败", 500)
