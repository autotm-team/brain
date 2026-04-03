"""
Integration Service 中间件

提供请求处理、日志记录、错误处理等中间件功能。
"""

import time
import uuid
import logging
import traceback
from datetime import datetime
from typing import Callable

from aiohttp import web
from aiohttp.web_middlewares import middleware
from auth_middleware import ui_auth_guard_middleware

logger = logging.getLogger(__name__)


@middleware
async def request_id_middleware(request: web.Request, handler: Callable) -> web.Response:
    """请求ID中间件
    
    为每个请求生成唯一ID，用于链路追踪。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    request_id = str(uuid.uuid4())
    request['request_id'] = request_id
    
    try:
        response = await handler(request)
        response.headers['X-Request-ID'] = request_id
        return response
    except Exception as e:
        # 确保异常响应也包含请求ID
        if hasattr(e, 'headers'):
            e.headers['X-Request-ID'] = request_id
        raise


@middleware
async def logging_middleware(request: web.Request, handler: Callable) -> web.Response:
    """日志中间件
    
    记录请求和响应的详细信息。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    start_time = time.time()
    request_id = request.get('request_id', 'unknown')
    
    # 记录请求开始
    logger.info(
        f"Request started - {request.method} {request.path} - "
        f"Remote: {request.remote} - "
        f"User-Agent: {request.headers.get('User-Agent', 'unknown')} - "
        f"Request-ID: {request_id}"
    )
    
    try:
        response = await handler(request)
        
        # 计算响应时间
        duration = time.time() - start_time
        
        # 记录成功响应
        logger.info(
            f"Request completed - {request.method} {request.path} - "
            f"Status: {response.status} - "
            f"Duration: {duration:.3f}s - "
            f"Request-ID: {request_id}"
        )
        
        # 添加响应时间头
        response.headers['X-Response-Time'] = f"{duration:.3f}s"
        
        return response
        
    except Exception as e:
        # 计算错误响应时间
        duration = time.time() - start_time
        
        # 记录错误响应
        logger.error(
            f"Request failed - {request.method} {request.path} - "
            f"Error: {str(e)} - "
            f"Duration: {duration:.3f}s - "
            f"Request-ID: {request_id}"
        )
        
        raise


@middleware
async def error_handling_middleware(request: web.Request, handler: Callable) -> web.Response:
    """错误处理中间件
    
    统一处理应用中的异常，返回标准化的错误响应。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    request_id = request.get('request_id', 'unknown')
    
    try:
        return await handler(request)
        
    except web.HTTPException:
        # HTTP异常直接抛出，由aiohttp处理
        raise
        
    except Exception as e:
        # 记录详细错误信息
        logger.exception(
            f"Unhandled error in {request.path} - "
            f"Error: {str(e)} - "
            f"Request-ID: {request_id}"
        )
        
        # 返回标准化错误响应
        error_response = {
            'success': False,
            'error': 'Internal server error',
            'error_type': type(e).__name__,
            'request_id': request_id,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        # 在调试模式下包含详细错误信息
        config = request.app.get('config')
        if config and config.service.debug:
            error_response['error_detail'] = str(e)
            error_response['traceback'] = traceback.format_exc()
        
        return web.json_response(error_response, status=500)


@middleware
async def cors_middleware(request: web.Request, handler: Callable) -> web.Response:
    """CORS中间件
    
    处理跨域请求。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    # 处理预检请求
    if request.method == 'OPTIONS':
        response = web.Response()
    else:
        response = await handler(request)
    
    allowed_origins = request.app.get('cors_allowed_origins') or []
    origin = request.headers.get('Origin', '')
    allow_origin = origin if origin and origin in allowed_origins else ''
    if allow_origin:
        response.headers['Access-Control-Allow-Origin'] = allow_origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
    response.headers['Access-Control-Expose-Headers'] = 'X-Request-ID, X-Response-Time'
    response.headers['Access-Control-Max-Age'] = '86400'
    response.headers['Vary'] = 'Origin'
    
    return response


@middleware
async def security_middleware(request: web.Request, handler: Callable) -> web.Response:
    """安全中间件
    
    添加安全相关的HTTP头。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    response = await handler(request)
    
    # 添加安全头
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    
    return response


@middleware
async def rate_limiting_middleware(request: web.Request, handler: Callable) -> web.Response:
    """限流中间件
    
    简单的基于IP的限流实现。
    
    Args:
        request: HTTP请求对象
        handler: 请求处理器
        
    Returns:
        web.Response: HTTP响应
    """
    # 获取客户端IP
    client_ip = request.remote
    
    # 简化的限流逻辑（实际应该使用Redis等外部存储）
    app = request.app
    if 'rate_limit_cache' not in app:
        app['rate_limit_cache'] = {}
    
    cache = app['rate_limit_cache']
    current_time = time.time()
    
    # 清理过期记录
    for ip in list(cache.keys()):
        if current_time - cache[ip]['last_request'] > 60:  # 1分钟窗口
            del cache[ip]
    
    # 检查当前IP的请求频率
    if client_ip in cache:
        ip_data = cache[client_ip]
        if current_time - ip_data['last_request'] < 1:  # 1秒内不能超过1个请求
            if ip_data['request_count'] >= 60:  # 每分钟最多60个请求
                logger.warning(f"Rate limit exceeded for IP: {client_ip}")
                return web.json_response(
                    {
                        'success': False,
                        'error': 'Rate limit exceeded',
                        'retry_after': 60
                    },
                    status=429
                )
        else:
            ip_data['request_count'] = 1
        
        ip_data['last_request'] = current_time
        ip_data['request_count'] += 1
    else:
        cache[client_ip] = {
            'last_request': current_time,
            'request_count': 1
        }
    
    return await handler(request)


def setup_middleware(app: web.Application):
    """设置中间件
    
    按照正确的顺序添加所有中间件。
    
    Args:
        app: aiohttp应用实例
    """
    # 中间件的执行顺序很重要
    # 请求时从上到下执行，响应时从下到上执行
    
    # 1. 错误处理（最外层）
    app.middlewares.append(error_handling_middleware)
    
    # 2. 请求ID生成
    app.middlewares.append(request_id_middleware)
    
    # 3. 日志记录
    app.middlewares.append(logging_middleware)
    
    # 4. 安全头
    app.middlewares.append(security_middleware)
    
    # 5. CORS处理（aiohttp_cors 已统一处理时跳过）
    if not app.get('cors_enabled'):
        app.middlewares.append(cors_middleware)

    # 6. UI 鉴权门禁（仅作用于 /api/v1/ui/*）
    app.middlewares.append(ui_auth_guard_middleware)

    # 7. 限流（可选，根据需要启用）
    config = app.get('config')
    if config and hasattr(config.service, 'enable_rate_limiting') and config.service.enable_rate_limiting:
        app.middlewares.append(rate_limiting_middleware)
    
    logger.info("Middleware setup completed")
