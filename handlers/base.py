"""
基础处理器类

提供通用的请求处理功能。
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from aiohttp import web
from serializers import to_jsonable


class BaseHandler:
    """基础处理器类"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def success_response(self, data: Any = None, message: str = "操作成功") -> web.Response:
        """成功响应
        
        Args:
            data: 响应数据
            message: 响应消息
            
        Returns:
            web.Response: JSON响应
        """
        response_data = {
            'success': True,
            'data': to_jsonable(data),
            'message': message,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        return web.json_response(response_data)
    
    def error_response(self, message: str, code: int = 400, error_code: str = None) -> web.Response:
        """错误响应
        
        Args:
            message: 错误消息
            code: HTTP状态码
            error_code: 业务错误码
            
        Returns:
            web.Response: JSON错误响应
        """
        response_data = {
            'success': False,
            'error': message,
            'error_code': error_code,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }

        return web.json_response(response_data, status=code)

    def not_implemented_response(self, message: str = "当前接口尚未实现") -> web.Response:
        """统一的未实现响应。"""
        return self.error_response(message, 501, "NOT_IMPLEMENTED")
    
    async def get_request_json(self, request: web.Request) -> Dict[str, Any]:
        """获取请求JSON数据

        Args:
            request: HTTP请求对象

        Returns:
            Dict[str, Any]: JSON数据

        Raises:
            web.HTTPBadRequest: JSON解析失败
        """
        try:
            ct = request.headers.get('Content-Type', '')
            if 'application/json' in ct:
                raw = await request.text()
                try:
                    return json.loads(raw) if raw else {}
                except json.JSONDecodeError as e:
                    self.logger.error(f"JSON decode error: {e}; raw body: {raw[:200]}")
                    raise web.HTTPBadRequest(text="Invalid JSON format")
            else:
                return {}
        except Exception as e:
            self.logger.error(f"Error reading request data: {e}")
            raise web.HTTPBadRequest(text="Error reading request data")

    def get_query_params(self, request: web.Request) -> Dict[str, str]:
        """获取查询参数
        
        Args:
            request: HTTP请求对象
            
        Returns:
            Dict[str, str]: 查询参数字典
        """
        return dict(request.query)
    
    def get_path_params(self, request: web.Request) -> Dict[str, str]:
        """获取路径参数
        
        Args:
            request: HTTP请求对象
            
        Returns:
            Dict[str, str]: 路径参数字典
        """
        return dict(request.match_info)
    
    def validate_required_fields(self, data: Dict[str, Any], required_fields: list) -> Optional[str]:
        """验证必需字段
        
        Args:
            data: 数据字典
            required_fields: 必需字段列表
            
        Returns:
            Optional[str]: 错误消息，如果验证通过则返回None
        """
        missing_fields = []
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            return f"Missing required fields: {', '.join(missing_fields)}"
        
        return None
    
    def get_app_component(self, request: web.Request, component_name: str) -> Any:
        """获取应用组件
        
        Args:
            request: HTTP请求对象
            component_name: 组件名称
            
        Returns:
            Any: 组件实例
            
        Raises:
            web.HTTPInternalServerError: 组件不存在
        """
        app = request.app
        if component_name not in app:
            self.logger.error(f"Component '{component_name}' not found in app")
            raise web.HTTPInternalServerError(text=f"Component '{component_name}' not available")
        
        return app[component_name]
    
    async def handle_async_operation(self, operation, error_message: str = "Operation failed"):
        """处理异步操作
        
        Args:
            operation: 异步操作函数
            error_message: 错误消息
            
        Returns:
            操作结果或抛出HTTP异常
        """
        try:
            return await operation()
        except Exception as e:
            self.logger.error(f"{error_message}: {e}")
            raise web.HTTPInternalServerError(text=error_message)
