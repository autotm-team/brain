"""
策略分析系统适配器实现

负责与策略分析系统(execution服务)的接口适配和通信管理。
提供策略分析、回测验证、实时验证等功能。
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from asyncron import request_envelope

from interfaces import ISystemAdapter
from config import IntegrationConfig
from exceptions import AdapterException, ConnectionException, HealthCheckException
from adapters.http_client import HttpClient
from adapters.execution_request_mapper import ExecutionRequestMapper

logger = logging.getLogger(__name__)


class StrategyAdapter(ISystemAdapter):
    """策略分析系统适配器实现类

    提供与策略分析系统的标准化接口，
    负责策略分析、回测验证、实时验证请求的发送和结果的接收处理。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化策略适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_connected = False
        self._last_health_check = None

        # HTTP客户端和请求映射器
        self._http_client: Optional[HttpClient] = None
        self._request_mapper = ExecutionRequestMapper()

        # 策略系统配置
        self._strategy_system_config = {
            'endpoint': 'strategy_analysis',
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

        # 分析器状态 (映射到execution服务的分析器名称)
        self._analyzer_status = {
            'livermore': 'active',
            'multi_indicator': 'active',
            'dow_theory': 'active',
            'hong_hao': 'active'
        }

        logger.info("StrategyAdapter initialized with HTTP client integration")

    async def connect_to_system(self) -> bool:
        """连接到策略分析系统

        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to strategy analysis system...")

            # 初始化HTTP客户端
            if not self._http_client:
                self._http_client = HttpClient('execution', self.config)

            # 检查分析器状态
            await self._check_analyzers_status()

            # 执行初始健康检查
            health_status = await self.health_check()
            if not health_status:
                raise ConnectionException("StrategyAdapter", "strategy_analysis", "Health check failed")

            self._is_connected = True
            logger.info("Successfully connected to strategy analysis system")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to strategy analysis system: {e}")
            self._is_connected = False
            raise ConnectionException("StrategyAdapter", "strategy_analysis", str(e))

    async def disconnect_from_system(self) -> bool:
        """断开与策略分析系统的连接

        Returns:
            bool: 断开是否成功
        """
        try:
            if self._http_client:
                await self._http_client.close()
                self._http_client = None

            self._is_connected = False
            logger.info("Disconnected from strategy analysis system")
            return True

        except Exception as e:
            logger.error(f"Failed to disconnect from strategy analysis system: {e}")
            return False

    async def health_check(self) -> bool:
        """健康检查

        Returns:
            bool: 系统是否健康
        """
        try:
            if not self._http_client:
                raise HealthCheckException("StrategyAdapter", "strategy_analysis", "HTTP client not initialized")

            start_time = datetime.now()

            # 发送健康检查请求到execution服务
            response = await self._http_client.get('/health')

            # 验证响应
            is_healthy = self._validate_health_response(response)

            self._last_health_check = datetime.now()
            response_time = (self._last_health_check - start_time).total_seconds()

            if is_healthy:
                logger.debug(f"Strategy system health check passed, response time: {response_time:.3f}s")
            else:
                logger.warning("Strategy system health check failed")
                raise HealthCheckException("StrategyAdapter", "strategy_analysis", "unhealthy")

            return is_healthy

        except HealthCheckException:
            raise
        except Exception as e:
            logger.error(f"Strategy system health check error: {e}")
            raise HealthCheckException("StrategyAdapter", "strategy_analysis", str(e))

    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态

        Returns:
            Dict[str, Any]: 系统状态信息
        """
        try:
            if not self._http_client:
                raise AdapterException("StrategyAdapter", "HTTP client not initialized")

            # 获取execution服务的系统状态
            response = await self._http_client.get('system/status')

            return {
                'system': 'strategy_analysis',
                'status': response.get('status', 'unknown'),
                'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
                'is_connected': self._is_connected,
                'analyzer_status': self._analyzer_status.copy(),
                'statistics': self._request_statistics.copy(),
                'response_data': response
            }

        except Exception as e:
            logger.error(f"Failed to get strategy system status: {e}")
            return {
                'system': 'strategy_analysis',
                'status': 'error',
                'error': str(e),
                'is_connected': self._is_connected,
                'analyzer_status': self._analyzer_status.copy()
            }

    async def send_request(self, request: Any) -> Any:
        """发送请求

        Args:
            request: 请求对象

        Returns:
            Any: 响应结果
        """
        if not self._is_connected:
            raise AdapterException("StrategyAdapter", "Not connected to strategy system")

        if not self._http_client:
            raise AdapterException("StrategyAdapter", "HTTP client not initialized")

        start_time = datetime.now()
        retry_count = 0
        max_retries = self._strategy_system_config['max_retries']

        while retry_count <= max_retries:
            try:
                self._request_statistics['total_requests'] += 1

                # 使用请求映射器转换请求格式
                mapped_request = self._request_mapper.map_strategy_analysis_request(request)

                response = await self._http_client.post(
                    '/api/v1/jobs',
                    request_envelope(
                        {
                            'job_type': 'batch_analyze',
                            'params': mapped_request,
                        }
                    ),
                )

                # 如果是异步任务，等待 Job 完成后再处理结果
                job_id = self._extract_job_id(response)
                if job_id:
                    job = await self._wait_for_job_completion(job_id, timeout=1800)
                    execution_payload = self._build_analysis_payload(job.get('result'))
                    processed_response = await self.handle_response({
                        'success': True,
                        'data': execution_payload
                    })
                else:
                    processed_response = await self.handle_response(response)

                # 更新统计
                response_time = (datetime.now() - start_time).total_seconds()
                self._update_request_statistics(True, response_time)

                logger.debug(f"Strategy request completed successfully, response time: {response_time:.3f}s")
                return processed_response

            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    self._update_request_statistics(False, 0)
                    logger.error(f"Strategy request failed after {max_retries} retries: {e}")
                    raise AdapterException("StrategyAdapter", f"Request failed: {e}")

                # 等待重试
                await asyncio.sleep(self._strategy_system_config['retry_delay'] * retry_count)
                logger.warning(f"Strategy request failed, retrying ({retry_count}/{max_retries}): {e}")

    async def handle_response(self, response: Any) -> Any:
        """处理响应

        Args:
            response: 响应对象

        Returns:
            Any: 处理后的结果
        """
        try:
            # 使用请求映射器转换响应格式
            mapped_response = self._request_mapper.map_analysis_response(response)

            # 验证响应格式
            if not self._validate_response_format(mapped_response):
                raise AdapterException("StrategyAdapter", "Invalid response format")

            # 提取分析结果数据
            analysis_results = self._extract_analysis_results(mapped_response)

            # 标准化数据格式
            standardized_results = self._standardize_analysis_results(analysis_results)

            return standardized_results

        except Exception as e:
            logger.error(f"Failed to handle strategy response: {e}")
            raise AdapterException("StrategyAdapter", f"Response handling failed: {e}")

    def get_request_statistics(self) -> Dict[str, Any]:
        """获取请求统计信息

        Returns:
            Dict[str, Any]: 请求统计
        """
        return self._request_statistics.copy()

    def get_analyzer_status(self) -> Dict[str, str]:
        """获取分析器状态

        Returns:
            Dict[str, str]: 分析器状态
        """
        return self._analyzer_status.copy()

    def _update_request_statistics(self, success: bool, response_time: float) -> None:
        """更新请求统计"""
        if success:
            self._request_statistics['successful_requests'] += 1
        else:
            self._request_statistics['failed_requests'] += 1

        # 更新平均响应时间
        total_successful = self._request_statistics['successful_requests']
        if total_successful > 0 and success:
            current_avg = self._request_statistics['average_response_time']
            self._request_statistics['average_response_time'] = (
                (current_avg * (total_successful - 1) + response_time) / total_successful
            )

    # 主要业务方法
    async def request_strategy_analysis(self, portfolio_instruction: Dict[str, Any],
                                      symbols: List[str], callback_url: Optional[str] = None) -> List[Dict[str, Any]]:
        """请求策略分析

        Args:
            portfolio_instruction: 组合指令
            symbols: 股票代码列表
            callback_url: 批量分析完成后用于回调通知的URL（可选）

        Returns:
            List[Dict[str, Any]]: 分析结果列表
        """
        try:
            request = {
                'type': 'strategy_analysis',
                'portfolio_instruction': portfolio_instruction,
                'symbols': symbols,
                'timestamp': datetime.now().isoformat(),
                'request_id': f"strategy_req_{datetime.now().timestamp()}"
            }

            if callback_url:
                request['callback_url'] = callback_url

            response = await self.send_request(request)

            logger.info(f"Strategy analysis completed for {len(symbols)} symbols, request: {request['request_id']}")
            return response

        except Exception as e:
            logger.error(f"Strategy analysis request failed: {e}")
            raise AdapterException("StrategyAdapter", f"Analysis request failed: {e}")

    async def request_backtest_validation(self, symbols: List[str],
                                        strategy_config: Dict[str, Any], callback_url: Optional[str] = None) -> Dict[str, Any]:
        """请求回测验证

        Args:
            symbols: 股票代码列表
            strategy_config: 策略配置

        Returns:
            Dict[str, Any]: 回测验证结果
        """
        try:
            if not self._http_client:
                raise AdapterException("StrategyAdapter", "HTTP client not initialized")

            # 构造回测请求
            backtest_request_payload = {
                'symbols': symbols,
                'strategy_config': strategy_config
            }
            if callback_url:
                backtest_request_payload['callback_url'] = callback_url
            backtest_request = self._request_mapper.map_backtest_request(backtest_request_payload)

            response = await self._http_client.post(
                '/api/v1/jobs',
                request_envelope(
                    {
                        'job_type': 'run_backtest',
                        'params': backtest_request,
                    }
                ),
            )

            job_id = self._extract_job_id(response)
            if job_id:
                job = await self._wait_for_job_completion(job_id, timeout=1800)
                result = {
                    'success': True,
                    'data': job.get('result', {})
                }
            else:
                result = response

            mapped_result = self._request_mapper.map_backtest_response(result)

            logger.info(f"Backtest validation completed for {len(symbols)} symbols")
            return mapped_result

        except Exception as e:
            logger.error(f"Backtest validation request failed: {e}")
            raise AdapterException("StrategyAdapter", f"Backtest validation failed: {e}")

    async def request_realtime_validation(self, pool_config: Dict[str, Any]) -> Dict[str, Any]:
        """请求实时验证

        Args:
            pool_config: 股票池配置

        Returns:
            Dict[str, Any]: 实时验证结果
        """
        try:
            if not self._http_client:
                raise AdapterException("StrategyAdapter", "HTTP client not initialized")

            # 构造股票池创建请求
            pool_request = self._request_mapper.map_quantum_pool_request({
                'pool_config': pool_config
            })

            # 创建股票池
            response = await self._http_client.post('quantum/pool/create', pool_request)

            if response.get('success'):
                pool_id = response.get('data', {}).get('pool_id')
                if pool_id:
                    # 获取股票池状态
                    status_response = await self._http_client.get(f'quantum/pool/{pool_id}/status')
                    return {
                        'status': 'success',
                        'data': {
                            'pool_id': pool_id,
                            'pool_status': status_response.get('data', {}),
                            'validation_type': 'realtime'
                        }
                    }

            return response

        except Exception as e:
            logger.error(f"Realtime validation request failed: {e}")
            raise AdapterException("StrategyAdapter", f"Realtime validation failed: {e}")

    def _extract_job_id(self, response: Dict[str, Any]) -> Optional[str]:
        data = response.get('data') if isinstance(response, dict) else None
        if isinstance(data, dict):
            return data.get('job_id')
        return response.get('job_id') if isinstance(response, dict) else None

    async def _wait_for_job_completion(self, job_id: str, timeout: int = 600, poll_interval: int = 2) -> Dict[str, Any]:
        start_time = time.monotonic()
        while True:
            job_response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            job = job_response.get('data') if isinstance(job_response, dict) else job_response
            status = (job.get('status') or '').lower()
            if status in ('succeeded', 'success', 'completed'):
                return job
            if status in ('failed', 'cancelled'):
                raise AdapterException("StrategyAdapter", f"Job {job_id} failed with status {status}")
            if time.monotonic() - start_time > timeout:
                raise AdapterException("StrategyAdapter", f"Job {job_id} timed out")
            await asyncio.sleep(poll_interval)

    def _build_analysis_payload(self, job_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not job_result:
            return {}
        if 'results' in job_result:
            return job_result
        results_map: Dict[str, Any] = {}
        for item in job_result.get('data', []) or []:
            if item.get('status') != 'success':
                continue
            symbol = item.get('symbol')
            result = item.get('result') or {}
            aggregated = result.get('aggregated') or {}
            analyzer_scores: Dict[str, Any] = {}
            for name, score in (result.get('results') or {}).items():
                if isinstance(score, dict):
                    analyzer_scores[name] = score.get('strength', score.get('confidence', 0.0))
                else:
                    analyzer_scores[name] = score
            if symbol:
                results_map[symbol] = {
                    'overall_score': aggregated.get('strength', aggregated.get('confidence', 0.5)),
                    'confidence': aggregated.get('confidence', 0.5),
                    'analyzer_scores': analyzer_scores
                }
        return {'results': results_map}


    async def query_analysis_history(self, **params) -> Dict[str, Any]:
        """通过 Execution 查询分析历史（分页/排序/过滤），透传所有筛选参数"""
        if not self._http_client:
            raise AdapterException("StrategyAdapter", "HTTP client not initialized")
        return await self._http_client.get('ui/execution/history', params=params)

    async def query_signal_stream(self, **params) -> Dict[str, Any]:
        """通过 Execution 查询信号流（游标分页）"""
        if not self._http_client:
            raise AdapterException("StrategyAdapter", "HTTP client not initialized")
        return await self._http_client.get('ui/execution/signals/stream', params=params)

    async def query_backtest_history(self,
                                     strategy_type: Optional[str] = None,
                                     start_date: Optional[str] = None,
                                     end_date: Optional[str] = None,
                                     page: int = 1,
                                     page_size: int = 20,
                                     sort_by: str = 'created_at',
                                     sort_order: str = 'desc',
                                     min_total_return: Optional[float] = None,
                                     max_drawdown: Optional[float] = None) -> Dict[str, Any]:
        """通过 Execution 查询回测历史（分页/排序/过滤）"""
        if not self._http_client:
            raise AdapterException("StrategyAdapter", "HTTP client not initialized")
        params: Dict[str, Any] = {
            'page': page,
            'page_size': page_size,
            'sort_by': sort_by,
            'sort_order': sort_order
        }
        if strategy_type:
            params['strategy_type'] = strategy_type
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if min_total_return is not None:
            params['min_total_return'] = min_total_return
        if max_drawdown is not None:
            params['max_drawdown'] = max_drawdown
        return await self._http_client.get('ui/execution/backtests/history', params=params)

    # 私有辅助方法
    async def _poll_backtest_result(self, task_id: str, max_wait_time: int = 300) -> Dict[str, Any]:
        """轮询回测结果

        Args:
            task_id: 任务ID
            max_wait_time: 最大等待时间(秒)

        Returns:
            Dict[str, Any]: 回测结果
        """
        start_time = datetime.now()

        while (datetime.now() - start_time).total_seconds() < max_wait_time:
            try:
                response = await self._http_client.get(f'backtest/result/{task_id}')

                if response.get('success'):
                    data = response.get('data', {})
                    status = data.get('status', 'pending')

                    if status == 'completed':
                        return response
                    elif status == 'failed':
                        raise AdapterException("StrategyAdapter", f"Backtest failed: {data.get('error', 'Unknown error')}")

                # 等待后重试
                await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Error polling backtest result: {e}")
                await asyncio.sleep(5)

        raise AdapterException("StrategyAdapter", f"Backtest timeout after {max_wait_time} seconds")

    async def _check_analyzers_status(self):
        """检查分析器状态"""
        try:
            if self._http_client:
                # 获取execution服务的分析能力
                response = await self._http_client.get('ui/execution/analyzers')
                if response.get('success'):
                    capabilities = response.get('data', {})
                    available_analyzers = capabilities.get('available_analyzers', [])

                    # 更新分析器状态
                    for analyzer in self._analyzer_status:
                        if analyzer in available_analyzers:
                            self._analyzer_status[analyzer] = 'active'
                        else:
                            self._analyzer_status[analyzer] = 'inactive'
            else:
                # 如果HTTP客户端未初始化，保持默认状态
                for analyzer in self._analyzer_status:
                    self._analyzer_status[analyzer] = 'unknown'

        except Exception as e:
            logger.warning(f"Failed to check analyzer status: {e}")
            # 出错时设置为未知状态
            for analyzer in self._analyzer_status:
                self._analyzer_status[analyzer] = 'unknown'

    def _validate_health_response(self, response: Dict[str, Any]) -> bool:
        """验证健康检查响应"""
        return (response.get('status') == 'healthy' and
                'timestamp' in response)

    def _validate_response_format(self, response: Any) -> bool:
        """验证响应格式"""
        if not isinstance(response, dict):
            return False

        required_fields = ['status']
        return all(field in response for field in required_fields)

    def _extract_analysis_results(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取分析结果数据"""
        if response.get('status') != 'success':
            raise AdapterException("StrategyAdapter", f"Strategy analysis failed: {response.get('error', 'Unknown error')}")

        data = response.get('data', {})
        return data.get('analysis_results', [])

    def _standardize_analysis_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """标准化分析结果数据"""
        standardized_results = []

        for result in results:
            standardized = {
                'symbol': result.get('symbol', ''),
                'signal_type': result.get('signal_type', 'hold'),
                'strength': float(result.get('strength', 0.5)),
                'confidence': float(result.get('confidence', 0.5)),
                'target_weight': float(result.get('target_weight', 0.0)),
                'timestamp': result.get('timestamp', datetime.now().isoformat()),
                'source': result.get('source', 'strategy_analysis_system'),
                'analyzer_scores': result.get('analyzer_scores', {}),
                'metadata': {
                    'original_result': result
                }
            }

            # 验证数据范围
            if not 0 <= standardized['strength'] <= 1:
                standardized['strength'] = max(0, min(1, standardized['strength']))

            if not 0 <= standardized['confidence'] <= 1:
                standardized['confidence'] = max(0, min(1, standardized['confidence']))

            if not 0 <= standardized['target_weight'] <= 1:
                standardized['target_weight'] = max(0, min(1, standardized['target_weight']))

            standardized_results.append(standardized)

        return standardized_results
