"""
Flowhub数据抓取服务适配器实现

负责与Flowhub数据抓取服务的接口适配和通信管理。
提供批量数据抓取、任务状态监控、结果获取等功能。
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

from asyncron import request_envelope

try:
    from ..interfaces import ISystemAdapter
    from ..config import IntegrationConfig
    from ..exceptions import AdapterException, ConnectionException, HealthCheckException
    from .http_client import HttpClient
    from .flowhub_request_mapper import FlowhubRequestMapper
except Exception:
    from interfaces import ISystemAdapter
    from config import IntegrationConfig
    from exceptions import AdapterException, ConnectionException, HealthCheckException
    from adapters.http_client import HttpClient
    from adapters.flowhub_request_mapper import FlowhubRequestMapper

logger = logging.getLogger(__name__)


class FlowhubAdapter(ISystemAdapter):
    """Flowhub数据抓取服务适配器实现类

    提供与Flowhub数据抓取服务的标准化接口，
    负责批量数据抓取任务的创建、监控和结果获取。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化Flowhub适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_connected = False
        self._last_health_check = None

        # HTTP客户端和请求映射器
        self._http_client: Optional[HttpClient] = None
        self._request_mapper = FlowhubRequestMapper()

        # Flowhub服务配置
        self._flowhub_system_config = {
            'endpoint': 'data_fetch',
            'timeout': self.config.adapter.request_timeout,
            'max_retries': self.config.adapter.max_retries,
            'retry_delay': self.config.adapter.retry_delay
        }

        # 请求统计
        self._request_statistics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'average_response_time': 0.0
        }

        # 任务状态缓存
        self._job_status_cache = {}

        logger.info("FlowhubAdapter initialized with HTTP client integration")

    async def _ensure_http_client(self) -> HttpClient:
        if not self._http_client:
            self._http_client = HttpClient('flowhub', self.config)
        await self._http_client.start()
        return self._http_client

    @staticmethod
    def _unwrap_data(payload: Any) -> Any:
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload.get("data")
        return payload

    @classmethod
    def _normalize_data_type(cls, token: str) -> str:
        raw = (token or "").strip()
        if not raw:
            return raw
        if "-" in raw:
            raise AdapterException("FlowhubAdapter", f"Legacy job_type/data_type is not allowed: {raw}")
        return raw

    @staticmethod
    def _job_request(job_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return request_envelope({
            "job_type": job_type,
            "params": dict(params or {}),
        })

    async def list_tasks(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        raise AdapterException("FlowhubAdapter", "Flowhub task schedules were removed; use brain /api/v1/schedules")

    async def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise AdapterException("FlowhubAdapter", "Flowhub task schedules were removed; use brain /api/v1/schedules")

    async def run_task(self, task_id: str) -> Dict[str, Any]:
        raise AdapterException("FlowhubAdapter", "Flowhub task schedules were removed; use brain /api/v1/schedules/{id}/trigger")

    async def ensure_task(
        self,
        name: str,
        data_type: str,
        params: Dict[str, Any],
        schedule_type: Optional[str] = None,
        schedule_value: Any = None,
        enabled: Optional[bool] = None,
        allow_overlap: Optional[bool] = None,
    ) -> Dict[str, Any]:
        raise AdapterException("FlowhubAdapter", "Flowhub task schedules were removed; use brain /api/v1/schedules")

    async def connect_to_system(self) -> bool:
        """连接到Flowhub数据抓取服务

        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to Flowhub data fetch service...")

            # 初始化HTTP客户端
            if not self._http_client:
                self._http_client = HttpClient('flowhub', self.config)
                await self._http_client.start()
                logger.info("HTTP client for flowhub service initialized")

            # 执行初始健康检查
            health_status = await self.health_check()
            if not health_status:
                raise ConnectionException("FlowhubAdapter", "data_fetch", "Health check failed")

            self._is_connected = True
            logger.info("Successfully connected to Flowhub data fetch service")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Flowhub service: {e}")
            self._is_connected = False
            raise ConnectionException("FlowhubAdapter", "data_fetch", str(e))

    async def disconnect_from_system(self) -> bool:
        """断开与Flowhub服务的连接

        Returns:
            bool: 断开是否成功
        """
        try:
            logger.info("Disconnecting from Flowhub data fetch service...")

            if self._http_client:
                await self._http_client.stop()
                self._http_client = None

            self._is_connected = False
            logger.info("Successfully disconnected from Flowhub service")
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from Flowhub service: {e}")
            return False

    async def health_check(self) -> bool:
        """检查Flowhub服务健康状态

        Returns:
            bool: 服务是否健康
        """
        try:
            if not self._http_client:
                return False

            # 调用健康检查端点
            response = await self._http_client.get('/health')

            # 检查响应状态
            is_healthy = response.get('status') == 'healthy'

            self._last_health_check = datetime.now()

            if is_healthy:
                logger.debug("Flowhub service health check passed")
            else:
                logger.warning(f"Flowhub service health check failed: {response}")

            return is_healthy

        except Exception as e:
            logger.error(f"Flowhub service health check failed: {e}")
            return False

    async def create_batch_stock_data_job(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """创建批量股票数据抓取任务

        Args:
            request: 批量股票数据抓取请求

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            await self._ensure_http_client()

            # 映射请求格式
            flowhub_request = self._request_mapper.map_batch_stock_data_request(request)
            logger.info(f"Creating batch stock data job with request: {flowhub_request}")

            # 发送统一任务创建请求
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request('batch_daily_ohlc', flowhub_request))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 映射响应格式
            mapped_response = self._request_mapper.map_job_response(response_data or {}, 'batch_stock_data')

            # 更新统计
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Batch stock data job created successfully: {mapped_response['job_id']}")
            return mapped_response

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create batch stock data job: {e}")
            raise AdapterException("FlowhubAdapter", f"Batch stock data job creation failed: {e}")

    async def create_batch_basic_data_job(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """创建批量基础数据抓取任务

        Args:
            request: 批量基础数据抓取请求

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            await self._ensure_http_client()

            # 映射请求格式
            flowhub_request = self._request_mapper.map_batch_basic_data_request(request)
            logger.info(f"Creating batch basic data job with request: {flowhub_request}")

            # 发送统一任务创建请求
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request('batch_daily_basic', flowhub_request))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 映射响应格式
            mapped_response = self._request_mapper.map_job_response(response_data or {}, 'batch_basic_data')

            # 更新统计
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Batch basic data job created successfully: {mapped_response['job_id']}")
            return mapped_response

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create batch basic data job: {e}")
            raise AdapterException("FlowhubAdapter", f"Batch basic data job creation failed: {e}")

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """获取任务执行状态

        Args:
            job_id: 任务ID

        Returns:
            Dict[str, Any]: 任务状态信息
        """
        try:
            await self._ensure_http_client()

            logger.debug(f"Getting status for job: {job_id}")

            # 统一接口：GET /api/v1/jobs/{job_id}
            response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 映射响应格式
            mapped_response = self._request_mapper.map_status_response(response_data or {})

            # 缓存状态
            self._job_status_cache[job_id] = mapped_response

            logger.debug(f"Job {job_id} status: {mapped_response['status']}")
            return mapped_response

        except Exception as e:
            logger.error(f"Failed to get job status for {job_id}: {e}")
            raise AdapterException("FlowhubAdapter", f"Get job status failed: {e}")

    async def get_job_result(self, job_id: str) -> Dict[str, Any]:
        """获取任务执行结果

        Args:
            job_id: 任务ID

        Returns:
            Dict[str, Any]: 任务执行结果
        """
        try:
            await self._ensure_http_client()

            logger.debug(f"Getting result for job: {job_id}")

            # 统一接口下结果字段从任务详情读取
            response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 映射响应格式
            mapped_response = self._request_mapper.map_result_response(response_data or {})

            logger.info(f"Job {job_id} result retrieved successfully")
            return mapped_response

        except Exception as e:
            logger.error(f"Failed to get job result for {job_id}: {e}")
            raise AdapterException("FlowhubAdapter", f"Get job result failed: {e}")

    async def list_jobs(self, status: str = None, limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        """获取任务列表"""
        try:
            await self._ensure_http_client()

            params = {'limit': limit, 'offset': offset}
            if status:
                params['status'] = status
            response = await self._http_client.get('/api/v1/jobs', params=params)
            return self._extract_jobs(response)
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            raise AdapterException("FlowhubAdapter", f"List jobs failed: {e}")

    @staticmethod
    def _extract_jobs(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, dict):
            direct = payload.get("jobs")
            if isinstance(direct, list):
                return direct
            data = payload.get("data")
            if isinstance(data, dict):
                jobs = data.get("jobs")
                if isinstance(jobs, list):
                    return jobs
            if isinstance(data, list):
                return data
        if isinstance(payload, list):
            return payload
        return []

    async def wait_for_job_completion(self, job_id: str, timeout: int = 172800,
                                    check_interval: int = 30,
                                    max_status_failures: int = 5) -> Dict[str, Any]:
        """等待任务完成

        Args:
            job_id: 任务ID
            timeout: 超时时间（秒）
            check_interval: 检查间隔（秒）

        Returns:
            Dict[str, Any]: 最终任务结果
        """
        try:
            logger.info(f"Waiting for job {job_id} to complete (timeout: {timeout}s)")

            start_time = datetime.now()

            status_failures = 0
            while True:
                # 检查任务状态
                try:
                    status_response = await self.get_job_status(job_id)
                    status = status_response.get('status')
                    status_failures = 0
                except AdapterException as e:
                    logger.warning(f"Get job status failed for {job_id}, retrying: {e}")
                    status_failures += 1
                    if status_failures >= max_status_failures:
                        raise AdapterException(
                            "FlowhubAdapter",
                            f"Job {job_id} status unavailable after {status_failures} attempts"
                        )
                    await asyncio.sleep(min(check_interval, 10))
                    status_response = None
                    status = None

                if status in ['succeeded', 'failed']:
                    # 任务完成，获取结果
                    result = await self.get_job_result(job_id)
                    logger.info(f"Job {job_id} completed with status: {status}")
                    return result
                if status in ['cancelled', 'canceled']:
                    logger.warning(f"Job {job_id} was cancelled")
                    return status_response

                # 检查超时
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed >= timeout:
                    logger.warning(f"Job {job_id} timed out after {timeout} seconds")
                    raise AdapterException("FlowhubAdapter", f"Job {job_id} timed out")

                # 等待下次检查
                await asyncio.sleep(check_interval)

        except Exception as e:
            logger.error(f"Failed to wait for job completion: {e}")
            raise AdapterException("FlowhubAdapter", f"Wait for job completion failed: {e}")

    def get_request_statistics(self) -> Dict[str, Any]:
        """获取请求统计信息

        Returns:
            Dict[str, Any]: 统计信息
        """
        return self._request_statistics.copy()

    def get_cached_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取缓存的任务状态

        Args:
            job_id: 任务ID

        Returns:
            Optional[Dict[str, Any]]: 缓存的状态信息
        """
        return self._job_status_cache.get(job_id)

    async def cancel_job(self, job_id: str) -> bool:
        """取消任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 取消是否成功
        """
        try:
            await self._ensure_http_client()

            logger.info(f"Cancelling job: {job_id}")

            # 统一接口：POST /api/v1/jobs/{job_id}/cancel
            await self._http_client.post(f'/api/v1/jobs/{job_id}/cancel', data={})

            # 从缓存中移除
            self._job_status_cache.pop(job_id, None)

            logger.info(f"Job {job_id} cancelled successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            return False

    async def create_daily_data_fetch_job(self, symbols: List[str] = None,
                                        incremental: bool = True) -> Dict[str, Any]:
        """创建每日股票日K数据抓取任务（用于定时调度，批量抓取OHLC）

        Args:
            symbols: 股票代码列表，None表示所有股票
            incremental: 是否增量更新

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求（日K需要日期范围，使用 batch_stock_data 的默认日期窗口）
            request = self._request_mapper.get_default_batch_request('batch_stock_data')
            request['incremental'] = incremental

            if symbols:
                request['symbols'] = symbols

            # 创建批量股票日K（OHLC）数据抓取任务
            return await self.create_batch_stock_data_job(request)

        except Exception as e:
            logger.error(f"Failed to create daily data fetch job: {e}")
            raise AdapterException("FlowhubAdapter", f"Daily data fetch job creation failed: {e}")

    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态信息

        Returns:
            Dict[str, Any]: 系统状态
        """
        try:
            if not self._http_client:
                return {'status': 'disconnected', 'message': 'HTTP client not initialized'}

            # 调用详细状态端点
            response = await self._http_client.get('/api/v1/status')

            # 添加适配器统计信息
            response['adapter_statistics'] = self._request_statistics
            response['cached_jobs'] = len(self._job_status_cache)
            response['last_health_check'] = self._last_health_check.isoformat() if self._last_health_check else None

            return response

        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'adapter_statistics': self._request_statistics
            }
    # ============== ISystemAdapter 通用实现 ==============
    async def send_request(self, request: Dict[str, Any]) -> Any:
        """通用请求发送接口（满足 ISystemAdapter 抽象方法）

        Args:
            request: 包含 method/endpoint 与 payload 的请求字典

        Returns:
            Any: 响应数据
        """
        await self._ensure_http_client()
        method = str(request.get('method', 'GET')).upper()
        endpoint = request.get('endpoint') or request.get('path') or '/'
        payload = request.get('payload') or request.get('data') or request.get('json')
        endpoint = str(endpoint)

        if method == 'GET':
            return await self._http_client.get(endpoint, params=payload)
        if method == 'POST':
            resp = await self._http_client.post(endpoint, data=payload)
            data = self._unwrap_data(resp)
            return data if isinstance(data, dict) else resp
        if method == 'PUT':
            return await self._http_client.put(endpoint, data=payload)
        if method == 'DELETE':
            return await self._http_client.delete(endpoint)
        raise AdapterException("FlowhubAdapter", f"Unsupported method: {method}")

    async def handle_response(self, response: Any) -> Any:
        """通用响应处理接口（满足 ISystemAdapter 抽象方法）

        Args:
            response: 原始响应

        Returns:
            Any: 处理后的响应（此处直接返回）
        """
        return response


    # ==================== Portfolio数据任务创建方法 ====================

    async def create_portfolio_data_job(self, data_type: str, update_mode: str = 'incremental', **kwargs) -> Dict[str, Any]:
        """创建Portfolio数据抓取任务（复权因子、指数成分股等）

        Args:
            data_type: 数据类型 ('adj_factors', 'index_components')
            update_mode: 更新模式 ('incremental', 'full_update', 'snapshot')
            **kwargs: 数据类型特定参数
                - symbols: 股票代码列表 (for adj_factors)
                - index_code: 指数代码 (for index_components)
                - index_codes: 指数代码列表 (for index_components)
                - start_date: 开始日期
                - end_date: 结束日期
                - trade_date: 交易日期

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求参数
            params = {
                'update_mode': update_mode
            }

            # 添加数据类型特定参数
            if 'symbols' in kwargs:
                params['symbols'] = kwargs['symbols']
            if 'index_code' in kwargs:
                # 单个指数代码转换为列表
                params['index_codes'] = [kwargs['index_code']]
            if 'index_codes' in kwargs:
                params['index_codes'] = kwargs['index_codes']
            if 'start_date' in kwargs:
                params['start_date'] = kwargs['start_date']
            if 'end_date' in kwargs:
                params['end_date'] = kwargs['end_date']
            if 'trade_date' in kwargs:
                params['trade_date'] = kwargs['trade_date']

            normalized_type = self._normalize_data_type(data_type)
            if normalized_type not in {'adj_factors', 'index_components'}:
                raise ValueError(f"Unsupported portfolio data type: {data_type}")

            await self._ensure_http_client()
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request(normalized_type, params))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Portfolio data job created for {data_type}: {(response_data or {}).get('job_id')}")
            return response_data if isinstance(response_data, dict) else {}

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create portfolio data job for {data_type}: {e}")
            raise AdapterException("FlowhubAdapter", f"Portfolio data job creation failed for {data_type}: {e}")

    # ==================== 市场数据任务创建方法 ====================

    async def create_index_daily_data_job(self, index_codes: List[str] = None,
                                         start_date: str = None, end_date: str = None,
                                         update_mode: str = 'incremental') -> Dict[str, Any]:
        """创建指数日线数据抓取任务

        Args:
            index_codes: 指数代码列表，None表示主要指数
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            update_mode: 更新模式 ('incremental', 'full_update')

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求参数
            params = {
                'update_mode': update_mode
            }

            # 默认主要指数
            if index_codes is None:
                index_codes = ['000001.SH', '399001.SZ', '000300.SH', '399006.SZ', '000016.SH', '399005.SZ']

            params['index_codes'] = index_codes

            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date

            # 发送请求
            await self._ensure_http_client()
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request('index_daily_data', params))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Index daily data job created: {(response_data or {}).get('job_id')}")
            return response_data if isinstance(response_data, dict) else {}

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create index daily data job: {e}")
            raise AdapterException("FlowhubAdapter", f"Index daily data job creation failed: {e}")

    async def create_industry_board_job(self, source: str = 'ths',
                                       update_mode: str = 'incremental') -> Dict[str, Any]:
        """创建行业板块数据抓取任务

        Args:
            source: 数据源 ('ths' 同花顺)
            update_mode: 更新模式 ('incremental', 'full_update')

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求参数
            params = {
                'source': source,
                'update_mode': update_mode
            }

            # 发送请求
            await self._ensure_http_client()
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request('industry_board', params))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Industry board job created: {(response_data or {}).get('job_id')}")
            return response_data if isinstance(response_data, dict) else {}

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create industry board job: {e}")
            raise AdapterException("FlowhubAdapter", f"Industry board job creation failed: {e}")

    async def create_concept_board_job(self, source: str = 'ths',
                                      update_mode: str = 'incremental') -> Dict[str, Any]:
        """创建概念板块数据抓取任务

        Args:
            source: 数据源 ('ths' 同花顺)
            update_mode: 更新模式 ('incremental', 'full_update')

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            # 构建请求参数
            params = {
                'source': source,
                'update_mode': update_mode
            }

            # 发送请求
            await self._ensure_http_client()
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request('concept_board', params))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Concept board job created: {(response_data or {}).get('job_id')}")
            return response_data if isinstance(response_data, dict) else {}

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create concept board job: {e}")
            raise AdapterException("FlowhubAdapter", f"Concept board job creation failed: {e}")

    # ==================== 宏观数据任务创建方法 ====================

    async def create_macro_data_job(self, data_type: str, incremental: bool = True, **kwargs) -> Dict[str, Any]:
        """创建宏观数据抓取任务

        注意：不再传递日期范围参数。Flowhub 会根据数据库状态和配置自动确定抓取范围：
        - 数据库为空时：从配置的历史起始日期开始（MacroDataConfig）
        - 数据库有数据时：从最新日期+1开始增量抓取

        Args:
            data_type: 数据类型 (e.g. 'gdp_data', 'price_index_data')
            incremental: 是否增量更新（默认 True）
            **kwargs: 数据类型特定参数（如 index_types, rate_types 等）

        Returns:
            Dict[str, Any]: 任务创建结果
        """
        try:
            await self._ensure_http_client()
            # 构建请求参数 - 只包含 incremental 标志
            normalized_type = self._normalize_data_type(data_type)
            params = {
                'incremental': incremental,
            }

            # 不再传递任何日期范围参数（start_date, end_date, start_quarter, end_quarter,
            # start_month, end_month, start_year, end_year）
            # 所有日期范围由 Flowhub 的 econdb_client 根据数据库状态和 MacroDataConfig 自动确定

            # 添加数据类型特定参数（非日期参数）
            if 'index_types' in kwargs:
                params['index_types'] = kwargs['index_types']
            if 'rate_types' in kwargs:
                params['rate_types'] = kwargs['rate_types']
            if 'index_codes' in kwargs:
                params['index_codes'] = kwargs['index_codes']
            if 'flow_types' in kwargs:
                params['flow_types'] = kwargs['flow_types']
            if 'commodity_types' in kwargs:
                params['commodity_types'] = kwargs['commodity_types']
            if 'indicators' in kwargs:
                params['indicators'] = kwargs['indicators']

            # 发送统一任务创建请求
            response = await self._http_client.post('/api/v1/jobs', data=self._job_request(normalized_type, params))
            response_data = self._unwrap_data(response) if isinstance(response, dict) else {}

            # 更新统计信息
            self._request_statistics['total_requests'] += 1
            self._request_statistics['successful_requests'] += 1

            logger.info(f"Macro data job created for {data_type}: {(response_data or {}).get('job_id')}")
            return response_data if isinstance(response_data, dict) else {}

        except Exception as e:
            self._request_statistics['total_requests'] += 1
            self._request_statistics['failed_requests'] += 1
            logger.error(f"Failed to create macro data job for {data_type}: {e}")
            raise AdapterException("FlowhubAdapter", f"Macro data job creation failed for {data_type}: {e}")
