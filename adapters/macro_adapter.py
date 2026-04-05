"""
宏观系统适配器实现

负责与宏观战略系统的HTTP接口适配和通信管理。
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from interfaces import ISystemAdapter
from config import IntegrationConfig
from exceptions import AdapterException, ConnectionException, HealthCheckException
from adapters.http_client import HttpClient

logger = logging.getLogger(__name__)


class MacroAdapter(ISystemAdapter):
    """宏观系统适配器实现类

    提供与宏观战略系统的标准化HTTP接口，
    负责宏观分析请求的发送和结果的接收处理。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化宏观适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._http_client = HttpClient('macro', config)
        self._is_connected = False
        self._last_health_check = None

        logger.info("MacroAdapter initialized with HTTP client")

    async def connect_to_system(self) -> bool:
        """连接到宏观系统

        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to macro system...")

            # 启动HTTP客户端
            await self._http_client.start()

            # 执行健康检查
            health_result = await self.health_check()

            if health_result:
                self._is_connected = True
                logger.info("Successfully connected to macro system")
                return True
            else:
                logger.error("Failed to connect to macro system - health check failed")
                return False

        except Exception as e:
            logger.error(f"Failed to connect to macro system: {e}")
            self._is_connected = False
            return False

    async def disconnect_from_system(self) -> bool:
        """断开与宏观系统的连接

        Returns:
            bool: 断开是否成功
        """
        try:
            logger.info("Disconnecting from macro system...")

            # 停止HTTP客户端
            await self._http_client.stop()

            self._is_connected = False
            logger.info("Successfully disconnected from macro system")
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from macro system: {e}")
            return False

    async def health_check(self) -> bool:
        """执行健康检查

        Returns:
            bool: 系统是否健康
        """
        try:
            if self._http_client and getattr(self._http_client, "_session", None) is None:
                await self._http_client.start()
            health_data = await self._http_client.health_check()
            self._last_health_check = datetime.now()

            if self._validate_health_response(health_data):
                logger.debug("Macro system health check passed")
                return True
            else:
                logger.warning(f"Macro system health check failed: {health_data}")
                return False

        except Exception as e:
            logger.error(f"Macro system health check error: {e}")
            raise HealthCheckException(f"Health check failed: {e}")

    def _validate_health_response(self, response: Dict[str, Any]) -> bool:
        """验证健康检查响应，兼容多种返回格式"""
        if not isinstance(response, dict):
            return False
        status_top = response.get('status')
        data = response.get('data', {}) if isinstance(response.get('data'), dict) else {}
        status_data = data.get('status')
        service_name = response.get('service') or data.get('service')
        timestamp = response.get('timestamp') or data.get('timestamp')

        is_ok = (
            status_top == 'healthy' or
            status_data == 'healthy' or
            (status_top == 'success' and status_data == 'healthy')
        )
        has_service = service_name in ('macro-strategy', 'macro_strategy', 'macro')
        return bool(is_ok and has_service and timestamp is not None)

    async def send_request(self, request: Any) -> Any:
        """发送请求到宏观系统

        Args:
            request: 请求对象

        Returns:
            Any: 响应结果
        """
        if not self._is_connected:
            raise ConnectionException("Not connected to macro system")

        try:
            # 根据请求类型选择合适的端点
            action = request.get('action', 'unknown')

            if action == 'get_state':
                return await self._http_client.get('state')
            elif action == 'trigger_analysis':
                return await self._http_client.post('analysis', request.get('params', {}))
            elif action == 'get_indicators':
                params = self._serialize_indicator_params(request.get('indicators'))
                return await self._http_client.get('indicators', params)
            elif action == 'get_cycle_position':
                return await self._http_client.get('cycle/position')
            else:
                raise AdapterException(f"Unknown action: {action}")

        except Exception as e:
            logger.error(f"Macro request failed: {e}")
            raise AdapterException(f"Request failed: {e}")

    async def get_macro_state(self) -> Dict[str, Any]:
        """获取宏观状态

        Returns:
            Dict[str, Any]: 宏观状态数据
        """
        try:
            response = await self._http_client.get('state')
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get macro state: {e}")
            raise

    async def trigger_macro_analysis(self, analysis_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """触发宏观分析

        Args:
            analysis_params: 分析参数

        Returns:
            Dict[str, Any]: 分析结果
        """
        try:
            response = await self._http_client.post('analysis', analysis_params or {})
            job_id = response.get('task_id') if isinstance(response, dict) else None
            if job_id:
                job = await self.get_job_status(job_id)
                return {
                    'job_id': job_id,
                    'status': response.get('status', 'accepted'),
                    'job': job
                }
            return response.get('data', response)

        except Exception as e:
            logger.error(f"Failed to trigger macro analysis: {e}")
            raise

    async def get_economic_indicators(self, indicators: List[str] = None) -> Dict[str, Any]:
        """获取经济指标

        Args:
            indicators: 指标列表

        Returns:
            Dict[str, Any]: 经济指标数据
        """
        try:
            params = self._serialize_indicator_params(indicators)
            response = await self._http_client.get('indicators', params)
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get economic indicators: {e}")
            raise

    @staticmethod
    def _serialize_indicator_params(indicators: Optional[List[Any]]) -> Dict[str, Any]:
        tokens = [str(item).strip() for item in (indicators or []) if str(item).strip()]
        return {'indicators': ",".join(tokens)} if tokens else {}

    async def get_market_cycle_position(self) -> Dict[str, Any]:
        """获取市场周期位置

        Returns:
            Dict[str, Any]: 市场周期位置数据
        """
        try:
            response = await self._http_client.get('cycle/position')
            return response.get('data', {})

        except Exception as e:
            logger.error(f"Failed to get market cycle position: {e}")
            raise

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """获取宏观任务状态（统一Job接口）"""
        try:
            response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            return self._unwrap_job(response)
        except Exception as e:
            logger.error(f"Failed to get macro job status: {e}")
            raise

    async def list_jobs(self, status: str = None, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """获取宏观任务列表（统一Job接口）"""
        try:
            params = {'limit': limit, 'offset': offset}
            if status:
                params['status'] = status
            response = await self._http_client.get('/api/v1/jobs', params)
            return self._unwrap_jobs_response(response)
        except Exception as e:
            logger.error(f"Failed to list macro jobs: {e}")
            raise

    @staticmethod
    def _unwrap_job(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                if isinstance(data.get("job"), dict):
                    return data["job"]
                return data
            if isinstance(payload.get("job"), dict):
                return payload["job"]
            return payload
        return {}

    @classmethod
    def _unwrap_jobs_response(cls, payload: Any) -> Dict[str, Any]:
        body = cls._unwrap_job(payload)
        if isinstance(body.get("jobs"), list):
            return body
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict) and isinstance(data.get("jobs"), list):
                return data
            if isinstance(payload.get("jobs"), list):
                return payload
        return {"jobs": [], "total": 0, "limit": 0, "offset": 0}

    async def wait_for_job_completion(
        self,
        job_id: str,
        timeout: int = 21600,
        poll_interval: int = 15
    ) -> Dict[str, Any]:
        """等待宏观分析任务完成"""
        end_time = time.time() + timeout
        while time.time() < end_time:
            job = await self.get_job_status(job_id)
            status = (job.get('status') or '').lower()
            if status in {'completed', 'succeeded'}:
                return job
            if status in {'failed', 'cancelled', 'canceled'}:
                raise RuntimeError(f"Macro job {job_id} failed with status={status}")
            await asyncio.sleep(poll_interval)
        raise TimeoutError(f"Macro job {job_id} timeout after {timeout}s")

    def get_connection_statistics(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        stats = self._http_client.get_statistics()
        return {
            'adapter_type': 'macro',
            'is_connected': self._is_connected,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'http_statistics': stats
        }



















    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        try:
            healthy = await self.health_check()
        except Exception:
            healthy = False
        return {
            'system': 'macro',
            'status': 'healthy' if healthy else 'unhealthy',
            'is_connected': self._is_connected,
            'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
            'http_statistics': self._http_client.get_statistics()
        }

    async def handle_response(self, response: Any) -> Any:
        """处理响应: 基础格式校验后透传"""
        if not isinstance(response, dict):
            raise AdapterException("MacroAdapter", "Invalid response format")
        return response
