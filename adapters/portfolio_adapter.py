"""
组合系统适配器实现

负责与组合管理系统的接口适配和通信管理。
"""

import asyncio
import importlib.util
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

def _load_local_module(module_filename: str, module_name: str):
    module_path = Path(__file__).resolve().parents[1] / f"{module_filename}.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load local module: {module_filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


try:
    from ..interfaces import ISystemAdapter
    from ..config import IntegrationConfig
    from ..exceptions import AdapterException, ConnectionException, HealthCheckException
    from .http_client import HttpClient
    from .portfolio_request_mapper import PortfolioRequestMapper
except Exception:
    interfaces_module = _load_local_module("interfaces", "brain_interfaces")
    config_module = _load_local_module("config", "brain_config")
    exceptions_module = _load_local_module("exceptions", "brain_exceptions")

    ISystemAdapter = interfaces_module.ISystemAdapter
    IntegrationConfig = config_module.IntegrationConfig
    AdapterException = exceptions_module.AdapterException
    ConnectionException = exceptions_module.ConnectionException
    HealthCheckException = exceptions_module.HealthCheckException

    from adapters.http_client import HttpClient
    from adapters.portfolio_request_mapper import PortfolioRequestMapper

logger = logging.getLogger(__name__)


class PortfolioAdapter(ISystemAdapter):
    """组合系统适配器实现类
    
    提供与组合管理系统的标准化接口，
    负责组合管理请求的发送和结果的接收处理。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化组合适配器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_connected = False
        self._connection_pool = None
        self._last_health_check = None
        
        # HTTP客户端和请求映射器
        self._http_client: Optional[HttpClient] = None
        self._request_mapper = PortfolioRequestMapper(
            default_portfolio_id=getattr(config.adapter, 'default_portfolio_id', None)
        )

        # 组合系统配置
        self._portfolio_system_config = {
            'endpoint': 'portfolio_management',
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
        self._resolved_portfolio_id: Optional[str] = None

        logger.info("PortfolioAdapter initialized with HTTP client integration")
    
    async def connect_to_system(self) -> bool:
        """连接到组合系统
        
        Returns:
            bool: 连接是否成功
        """
        try:
            logger.info("Connecting to portfolio management system...")
            
            # 初始化HTTP客户端
            if self._http_client is None:
                self._http_client = HttpClient('portfolio', self.config)
                await self._http_client.start()
                logger.info("HTTP client for portfolio service initialized")
            
            # 初始化连接池
            if self.config.adapter.enable_connection_pooling:
                self._connection_pool = await self._create_connection_pool()
            
            # 执行初始健康检查
            health_status = await self.health_check()
            if not health_status:
                raise ConnectionException("PortfolioAdapter", "portfolio_management", "Health check failed")
            
            self._is_connected = True
            logger.info("Successfully connected to portfolio management system")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to portfolio management system: {e}")
            self._is_connected = False
            raise ConnectionException("PortfolioAdapter", "portfolio_management", str(e))
    
    async def disconnect_from_system(self) -> bool:
        """断开与组合系统的连接
        
        Returns:
            bool: 断开是否成功
        """
        try:
            # 关闭HTTP客户端
            if self._http_client:
                await self._http_client.stop()
                self._http_client = None
                logger.info("HTTP client for portfolio service stopped")

            if self._connection_pool:
                await self._close_connection_pool()
                self._connection_pool = None

            self._is_connected = False
            logger.info("Disconnected from portfolio management system")
            return True
            
        except Exception as e:
            logger.error(f"Failed to disconnect from portfolio management system: {e}")
            return False
    
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 系统是否健康
        """
        try:
            start_time = datetime.now()
            
            # 构造健康检查请求
            health_request = {
                'type': 'health_check',
                'timestamp': start_time.isoformat(),
                'adapter': 'portfolio_adapter'
            }

            # 使用HTTP客户端发送健康检查请求
            if self._http_client is None:
                logger.warning("HTTP client not initialized, cannot perform health check")
                return False

            # 映射健康检查请求
            method, api_path, request_body = self._request_mapper.map_request(health_request)
            response = await self._http_client.request(method, api_path, request_body)
            
            # 验证响应
            is_healthy = self._validate_health_response(response)
            
            self._last_health_check = datetime.now()
            response_time = (self._last_health_check - start_time).total_seconds()
            
            if is_healthy:
                logger.debug(f"Portfolio system health check passed, response time: {response_time:.3f}s")
            else:
                logger.warning("Portfolio system health check failed")
                raise HealthCheckException("PortfolioAdapter", "portfolio_management", "unhealthy")
            
            return is_healthy
            
        except HealthCheckException:
            raise
        except Exception as e:
            logger.error(f"Portfolio system health check error: {e}")
            raise HealthCheckException("PortfolioAdapter", "portfolio_management", str(e))

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """获取组合任务状态（统一Job接口）"""
        try:
            response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            return self._unwrap_job(response)
        except Exception as e:
            logger.error(f"Failed to get portfolio job status: {e}")
            raise

    async def list_jobs(self, status: str = None, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """获取组合任务列表（统一Job接口）"""
        try:
            params = {'limit': limit, 'offset': offset}
            if status:
                params['status'] = status
            response = await self._http_client.get('/api/v1/jobs', params)
            return self._unwrap_jobs_response(response)
        except Exception as e:
            logger.error(f"Failed to list portfolio jobs: {e}")
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
    
    async def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态
        
        Returns:
            Dict[str, Any]: 系统状态信息
        """
        try:
            status_request = {
                'type': 'status_request',
                'timestamp': datetime.now().isoformat()
            }
            
            response = await self.send_request(status_request)
            
            return {
                'system': 'portfolio_management',
                'status': response.get('status', 'unknown'),
                'last_health_check': self._last_health_check.isoformat() if self._last_health_check else None,
                'is_connected': self._is_connected,
                'statistics': self._request_statistics.copy(),
                'response_data': response
            }
            
        except Exception as e:
            logger.error(f"Failed to get portfolio system status: {e}")
            return {
                'system': 'portfolio_management',
                'status': 'error',
                'error': str(e),
                'is_connected': self._is_connected
            }
    
    async def send_request(self, request: Any) -> Any:
        """发送请求
        
        Args:
            request: 请求对象
            
        Returns:
            Any: 响应结果
        """
        if not self._is_connected:
            raise AdapterException("PortfolioAdapter", "Not connected to portfolio system")
        
        start_time = datetime.now()
        retry_count = 0
        max_retries = self._portfolio_system_config['max_retries']
        
        while retry_count <= max_retries:
            try:
                self._request_statistics['total_requests'] += 1
                
                # 发送请求到组合系统
                response = await self._send_portfolio_request(request)
                
                # 处理响应
                processed_response = await self.handle_response(response, request.get('type', 'unknown'))
                
                # 更新统计
                response_time = (datetime.now() - start_time).total_seconds()
                self._update_request_statistics(True, response_time)
                
                logger.debug(f"Portfolio request completed successfully, response time: {response_time:.3f}s")
                return processed_response
                
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    self._update_request_statistics(False, 0)
                    logger.error(f"Portfolio request failed after {max_retries} retries: {e}")
                    raise AdapterException("PortfolioAdapter", f"Request failed: {e}")
                
                # 等待重试
                await asyncio.sleep(self._portfolio_system_config['retry_delay'] * retry_count)
                logger.warning(f"Portfolio request failed, retrying ({retry_count}/{max_retries}): {e}")
    
    async def handle_response(self, response: Any, request_type: str) -> Any:
        """处理响应
        
        Args:
            response: 响应对象
            
        Returns:
            Any: 处理后的结果
        """
        try:
            # 验证响应格式
            if not self._validate_response_format(response):
                raise AdapterException("PortfolioAdapter", "Invalid response format")
            
            response_data = self._extract_response_data(response)

            if request_type == 'portfolio_optimization':
                return self._standardize_portfolio_instruction(response_data)
            if request_type == 'risk_assessment':
                return self._standardize_risk_assessment(response_data)
            if request_type == 'status_request':
                return self._standardize_system_status(response_data)
            return response_data
            
        except Exception as e:
            logger.error(f"Failed to handle portfolio response: {e}")
            raise AdapterException("PortfolioAdapter", f"Response handling failed: {e}")
    
    async def request_portfolio_optimization(self, macro_state: Dict[str, Any], 
                                           constraints: Dict[str, Any],
                                           portfolio_id: Optional[str] = None) -> Dict[str, Any]:
        """请求组合优化
        
        Args:
            macro_state: 宏观状态
            constraints: 组合约束
            
        Returns:
            Dict[str, Any]: 组合优化结果
        """
        try:
            resolved_portfolio_id = await self._resolve_portfolio_id(portfolio_id)
            request = {
                'type': 'portfolio_optimization',
                'portfolio_id': resolved_portfolio_id,
                'macro_state': macro_state,
                'constraints': constraints,
                'timestamp': datetime.now().isoformat(),
                'request_id': f"portfolio_req_{datetime.now().timestamp()}"
            }
            
            response = await self.send_request(request)
            
            logger.info(f"Portfolio optimization completed for request: {request['request_id']}")
            return response
            
        except Exception as e:
            logger.error(f"Portfolio optimization request failed: {e}")
            raise AdapterException("PortfolioAdapter", f"Optimization request failed: {e}")
    
    async def request_risk_assessment(self, portfolio_data: Dict[str, Any], portfolio_id: Optional[str] = None) -> Dict[str, Any]:
        """请求风险评估
        
        Args:
            portfolio_data: 组合数据
            
        Returns:
            Dict[str, Any]: 风险评估结果
        """
        try:
            resolved_portfolio_id = await self._resolve_portfolio_id(portfolio_id or portfolio_data.get('portfolio_id'))
            request = {
                'type': 'risk_assessment',
                'portfolio_id': resolved_portfolio_id,
                'portfolio_data': portfolio_data,
                'timestamp': datetime.now().isoformat(),
                'request_id': f"risk_req_{datetime.now().timestamp()}"
            }
            
            response = await self.send_request(request)
            
            logger.info(f"Risk assessment completed for request: {request['request_id']}")
            return response
            
        except Exception as e:
            logger.error(f"Risk assessment request failed: {e}")
            raise AdapterException("PortfolioAdapter", f"Risk assessment failed: {e}")
    
    def get_request_statistics(self) -> Dict[str, Any]:
        """获取请求统计信息
        
        Returns:
            Dict[str, Any]: 请求统计
        """
        return self._request_statistics.copy()
    
    # 私有方法实现
    async def _create_connection_pool(self):
        """创建连接池"""
        # HTTP客户端内部管理连接池，这里只记录日志
        logger.debug("Connection pool managed by HTTP client for portfolio system")
        return {"pool_size": self.config.adapter.pool_size, "active_connections": 0}

    async def _close_connection_pool(self):
        """关闭连接池"""
        # HTTP客户端内部管理连接池，这里只记录日志
        logger.debug("Connection pool closed by HTTP client for portfolio system")
    
    async def _send_health_check_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """发送健康检查请求"""
        if not self._http_client:
            raise AdapterException("PortfolioAdapter", "HTTP client not initialized")

        try:
            # 直接调用健康检查端点
            response = await self._http_client.get('health')
            return response
        except Exception as e:
            logger.error(f"Health check request failed: {e}")
            raise AdapterException("PortfolioAdapter", f"Health check failed: {e}")
    
    def _validate_health_response(self, response: Dict[str, Any]) -> bool:
        """验证健康检查响应，兼容多种健康检查返回格式"""
        try:
            # 兼容 portfolio-service 返回: {status: success, data: {status: healthy, service: ...}}
            status_top = response.get('status')
            data = response.get('data', {}) if isinstance(response, dict) else {}
            status_data = data.get('status')
            service_name = response.get('system') or data.get('service')
            timestamp = response.get('timestamp') or data.get('timestamp')

            is_ok = (
                (status_top == 'healthy') or (status_data == 'healthy') or (status_top == 'success' and status_data == 'healthy')
            )
            has_system = service_name in ('portfolio_management', 'portfolio-management', 'portfolio')
            has_ts = timestamp is not None
            return bool(is_ok and has_ts and has_system)
        except Exception:
            return False

    async def _send_portfolio_request(self, request: Any) -> Any:
        """发送组合系统请求"""
        if not self._http_client:
            raise AdapterException("PortfolioAdapter", "HTTP client not initialized")

        if not isinstance(request, dict):
            raise AdapterException("PortfolioAdapter", "Invalid request format")

        try:
            # 使用请求映射器将通用请求转换为具体的API调用
            method, api_path, request_body = self._request_mapper.map_request(request)

            logger.debug(f"Sending portfolio request: {method} {api_path}")

            # 发送HTTP请求
            response = await self._http_client.request(method, api_path, request_body)

            # 映射响应格式
            mapped_response = self._request_mapper.map_response(response, request.get('type', 'unknown'))

            logger.debug(f"Portfolio request completed successfully")
            return mapped_response

        except Exception as e:
            logger.error(f"Portfolio request failed: {e}")
            raise AdapterException("PortfolioAdapter", f"Request failed: {e}")
    
    def _validate_response_format(self, response: Any) -> bool:
        """验证响应格式"""
        if not isinstance(response, dict):
            return False
        
        required_fields = ['status']
        return all(field in response for field in required_fields)
    
    def _extract_response_data(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """提取成功响应的数据内容"""
        if response.get('status') != 'success':
            raise AdapterException("PortfolioAdapter", f"Portfolio operation failed: {response.get('error', 'Unknown error')}")
        
        return response.get('data', {})
    
    def _standardize_portfolio_instruction(self, instruction: Dict[str, Any]) -> Dict[str, Any]:
        """标准化组合指令数据"""
        standardized = {
            'target_position': float(instruction.get('target_position', 0.5)),
            'sector_weights': instruction.get('sector_weights', {}),
            'risk_constraints': instruction.get('risk_constraints', {}),
            'weights': instruction.get('weights', {}),
            'positions': instruction.get('positions', {}),
            'symbols': instruction.get('symbols', []),
            'rebalance_threshold': float(instruction.get('rebalance_threshold', 0.05)),
            'execution_priority': instruction.get('execution_priority', 3),
            'expected_return': float(instruction.get('expected_return', 0.0)),
            'expected_volatility': float(instruction.get('expected_volatility', 0.0)),
            'job_id': instruction.get('job_id'),
            'job_status': instruction.get('job_status', 'succeeded'),
            'timestamp': datetime.now().isoformat(),
            'source': 'portfolio_management_system'
        }
        
        # 验证数据范围
        if not 0 <= standardized['target_position'] <= 1:
            standardized['target_position'] = max(0, min(1, standardized['target_position']))
        
        if not 0 <= standardized['rebalance_threshold'] <= 1:
            standardized['rebalance_threshold'] = max(0, min(1, standardized['rebalance_threshold']))
        
        # 验证权重总和
        sector_weights = standardized['sector_weights']
        if sector_weights:
            total_weight = sum(sector_weights.values())
            if abs(total_weight - 1.0) > 0.01:  # 允许1%的误差
                logger.warning(f"Sector weights sum to {total_weight}, normalizing...")
                if total_weight > 0:
                    standardized['sector_weights'] = {
                        sector: weight / total_weight 
                        for sector, weight in sector_weights.items()
                    }
        
        return standardized

    def _standardize_risk_assessment(self, risk_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'var_1d': float(risk_data.get('var_1d', 0.0)),
            'var_5d': float(risk_data.get('var_5d', 0.0)),
            'expected_shortfall': float(risk_data.get('expected_shortfall', 0.0)),
            'beta': float(risk_data.get('beta', 0.0)),
            'tracking_error': float(risk_data.get('tracking_error', 0.0)),
            'information_ratio': float(risk_data.get('information_ratio', 0.0)),
            'risk_score': float(risk_data.get('risk_score', 0.0)),
            'raw_metrics': risk_data.get('raw_metrics', {}),
        }

    def _standardize_system_status(self, status_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'status': status_data.get('status', 'unknown'),
            'last_optimization': status_data.get('last_optimization'),
            'active_portfolios': int(status_data.get('active_portfolios', 0)),
            'total_aum': float(status_data.get('total_aum', 0.0)),
            'portfolio_ids': status_data.get('portfolio_ids', []),
        }
    
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

    async def _resolve_portfolio_id(self, requested_portfolio_id: Optional[str] = None) -> str:
        """解析要发送给 portfolio 服务的目标组合 ID。"""
        explicit_id = requested_portfolio_id or getattr(self.config.adapter, 'default_portfolio_id', None)
        if explicit_id:
            if await self._portfolio_exists(explicit_id):
                self._resolved_portfolio_id = explicit_id
                return explicit_id
            raise AdapterException("PortfolioAdapter", f"Configured portfolio_id does not exist: {explicit_id}")

        if self._resolved_portfolio_id and await self._portfolio_exists(self._resolved_portfolio_id):
            return self._resolved_portfolio_id

        portfolios = await self._list_portfolios()
        if not portfolios:
            raise AdapterException(
                "PortfolioAdapter",
                "No portfolio exists in portfolio service. Create one first or configure adapter.default_portfolio_id.",
            )

        active_portfolios = [
            item for item in portfolios
            if str(item.get('status', '')).lower() in {'active', 'pending_execution', 'rebalancing'}
        ]
        candidates = active_portfolios or portfolios

        if len(candidates) == 1:
            portfolio_id = str(candidates[0].get('portfolio_id') or '').strip()
            if portfolio_id:
                self._resolved_portfolio_id = portfolio_id
                return portfolio_id

        raise AdapterException(
            "PortfolioAdapter",
            "Multiple portfolios exist. Specify portfolio_id explicitly or configure adapter.default_portfolio_id.",
        )

    async def _portfolio_exists(self, portfolio_id: str) -> bool:
        if not self._http_client:
            raise AdapterException("PortfolioAdapter", "HTTP client not initialized")
        try:
            response = await self._http_client.get(f'/api/v1/portfolios/{portfolio_id}')
            return isinstance(response, dict) and response.get('status') == 'success'
        except Exception:
            return False

    async def _list_portfolios(self) -> List[Dict[str, Any]]:
        if not self._http_client:
            raise AdapterException("PortfolioAdapter", "HTTP client not initialized")
        response = await self._http_client.get('/api/v1/portfolios')
        if not isinstance(response, dict):
            return []
        data = response.get('data', {})
        items = data.get('items') if isinstance(data, dict) else []
        return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
