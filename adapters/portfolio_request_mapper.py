"""
Portfolio请求映射器

负责将brain层的通用请求格式转换为portfolio服务的RESTful API格式。
"""

import logging
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class PortfolioRequestMapper:
    """Portfolio请求映射器
    
    将brain层期望的通用请求格式转换为portfolio服务的具体RESTful API调用。
    """
    
    def __init__(self, default_portfolio_id: Optional[str] = None):
        """
        初始化映射器
        
        Args:
            default_portfolio_id: 默认的portfolio ID
        """
        self.default_portfolio_id = default_portfolio_id
        logger.info(f"PortfolioRequestMapper initialized with default portfolio: {default_portfolio_id}")
    
    def map_request(self, request: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """
        映射请求到具体的API端点
        
        Args:
            request: brain层的通用请求格式
            
        Returns:
            Tuple[str, str, Dict[str, Any]]: (HTTP方法, API路径, 请求体)
        """
        request_type = request.get('type', 'unknown')
        
        if request_type == 'portfolio_optimization':
            return self._map_portfolio_optimization(request)
        elif request_type == 'risk_assessment':
            return self._map_risk_assessment(request)
        elif request_type == 'status_request':
            return self._map_status_request(request)
        elif request_type == 'health_check':
            return self._map_health_check(request)
        else:
            raise ValueError(f"Unknown request type: {request_type}")
    
    def _map_portfolio_optimization(self, request: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """映射组合优化请求"""
        portfolio_id = request.get('portfolio_id') or self.default_portfolio_id
        if not portfolio_id:
            raise ValueError("portfolio_id is required for portfolio optimization requests")
        macro_state = request.get('macro_state', {})
        constraints = request.get('constraints', {})
        
        # 选择优化算法（默认使用Markowitz）
        optimization_method = self._select_optimization_method(macro_state, constraints)
        
        # 构造请求体
        request_body = {
            'objective': 'max_sharpe',  # 默认目标
            'constraints': {
                'max_weight': constraints.get('max_single_stock'),
                'max_portfolio_risk': constraints.get('var_limit'),
                'sector_limits': constraints.get('sector_limits', {}),
            },
            'expected_returns': {},  # 可以从macro_state中提取
            'macro_context': macro_state  # 传递宏观背景信息
        }
        
        # 如果有宏观状态信息，调整优化参数
        if macro_state:
            request_body = self._adjust_optimization_params(request_body, macro_state)
        
        api_path = f"portfolios/{portfolio_id}/optimize/{optimization_method}"
        
        logger.debug(f"Mapped portfolio optimization to: POST {api_path}")
        return ("POST", api_path, request_body)
    
    def _map_risk_assessment(self, request: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """映射风险评估请求"""
        portfolio_data = request.get('portfolio_data', {})
        portfolio_id = request.get('portfolio_id') or portfolio_data.get('portfolio_id') or self.default_portfolio_id
        if not portfolio_id:
            raise ValueError("portfolio_id is required for risk assessment requests")
        
        # 默认获取风险指标，如果需要特定风险计算，可以扩展
        api_path = f"portfolios/{portfolio_id}/risk/metrics"
        
        # 构造查询参数（如果需要）
        request_body = {}
        if portfolio_data:
            request_body['portfolio_data'] = portfolio_data
        
        logger.debug(f"Mapped risk assessment to: GET {api_path}")
        return ("GET", api_path, request_body)
    
    def _map_status_request(self, request: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """映射状态查询请求"""
        api_path = "portfolios"
        
        logger.debug(f"Mapped status request to: GET {api_path}")
        return ("GET", api_path, {})
    
    def _map_health_check(self, request: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        """映射健康检查请求"""
        api_path = "health"
        
        logger.debug("Mapped health check to: GET health")
        return ("GET", api_path, {})
    
    def _select_optimization_method(self, macro_state: Dict[str, Any], 
                                  constraints: Dict[str, Any]) -> str:
        """
        根据宏观状态和约束选择优化方法
        
        Args:
            macro_state: 宏观状态
            constraints: 约束条件
            
        Returns:
            str: 优化方法名称
        """
        # 简单的选择逻辑，可以根据需要扩展
        risk_level = macro_state.get('risk_level', 'medium')
        
        if risk_level == 'high':
            return 'risk-parity'  # 高风险环境使用风险平价
        elif risk_level == 'low':
            return 'markowitz'    # 低风险环境使用马科维茨
        else:
            return 'markowitz'    # 默认使用马科维茨
    
    def _adjust_optimization_params(self, request_body: Dict[str, Any], 
                                  macro_state: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据宏观状态调整优化参数
        
        Args:
            request_body: 原始请求体
            macro_state: 宏观状态
            
        Returns:
            Dict[str, Any]: 调整后的请求体
        """
        # 根据宏观状态调整优化目标和约束
        economic_cycle = macro_state.get('economic_cycle', 'neutral')
        risk_level = macro_state.get('risk_level', 'medium')
        
        # 调整优化目标
        if economic_cycle == 'recession':
            request_body['objective'] = 'min_risk'  # 衰退期优先降低风险
        elif economic_cycle == 'expansion':
            request_body['objective'] = 'max_return'  # 扩张期优先提高收益
        else:
            request_body['objective'] = 'max_sharpe'  # 默认最大化夏普比率
        
        # 调整风险约束
        if risk_level == 'high':
            if 'constraints' not in request_body:
                request_body['constraints'] = {}
            request_body['constraints']['max_volatility'] = 0.15  # 限制波动率
        
        return request_body
    
    def map_response(self, response: Dict[str, Any], original_request_type: str) -> Dict[str, Any]:
        """
        映射响应格式，将portfolio服务的响应转换为brain层期望的格式
        
        Args:
            response: portfolio服务的原始响应
            original_request_type: 原始请求类型
            
        Returns:
            Dict[str, Any]: 转换后的响应
        """
        if original_request_type == 'portfolio_optimization':
            return self._map_optimization_response(response)
        elif original_request_type == 'risk_assessment':
            return self._map_risk_response(response)
        elif original_request_type == 'status_request':
            return self._map_status_response(response)
        elif original_request_type == 'health_check':
            return self._map_health_response(response)
        else:
            return response
    
    def _map_optimization_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """映射优化响应"""
        payload = self._unwrap_success_data(response)
        optimization_result = payload.get('optimization_result') if isinstance(payload, dict) else None
        if isinstance(optimization_result, dict):
            optimal_weights = optimization_result.get('optimal_weights', {}) or {}
            success = bool(optimization_result.get('success', False))
            return {
                'status': 'success' if success else 'error',
                'data': {
                    'target_position': float(sum(float(weight) for weight in optimal_weights.values())) if optimal_weights else 0.0,
                    'weights': optimal_weights,
                    'positions': optimal_weights,
                    'symbols': list(optimal_weights.keys()),
                    'sector_weights': payload.get('sector_weights', {}),
                    'risk_constraints': {
                        'constraints_satisfied': optimization_result.get('constraints_satisfied', False),
                        'constraint_violations': optimization_result.get('constraint_violations', []),
                    },
                    'expected_return': optimization_result.get('expected_return', 0.0),
                    'expected_volatility': optimization_result.get('expected_risk', 0.0),
                    'sharpe_ratio': optimization_result.get('sharpe_ratio', 0.0),
                    'rebalance_threshold': payload.get('rebalance_threshold', 0.05)
                },
                'error': None if success else optimization_result.get('convergence_info', {}).get('message', 'optimization failed'),
                'timestamp': response.get('timestamp', datetime.now().isoformat())
            }
        return response
    
    def _map_risk_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """映射风险评估响应"""
        payload = self._unwrap_success_data(response)
        risk_metrics = payload.get('risk_metrics') if isinstance(payload, dict) else None
        if isinstance(risk_metrics, dict):
            var_1d = float(risk_metrics.get('portfolio_var_1d', risk_metrics.get('var_1d', 0.0)))
            var_5d = float(risk_metrics.get('portfolio_var_5d', risk_metrics.get('var_5d', 0.0)))
            expected_shortfall = float(risk_metrics.get('portfolio_cvar_1d', risk_metrics.get('expected_shortfall', 0.0)))
            concentration = float(risk_metrics.get('concentration_ratio', 0.0))
            correlation = float(risk_metrics.get('avg_correlation', 0.0))
            risk_score = min(1.0, max(var_1d / 0.05, concentration / 0.1, correlation / 0.8))
            return {
                'status': 'success',
                'data': {
                    'var_1d': var_1d,
                    'var_5d': var_5d,
                    'expected_shortfall': expected_shortfall,
                    'beta': risk_metrics.get('beta', 1.0),
                    'tracking_error': risk_metrics.get('tracking_error', 0.03),
                    'information_ratio': risk_metrics.get('information_ratio', 0.5),
                    'risk_score': risk_score,
                    'raw_metrics': risk_metrics
                },
                'timestamp': response.get('timestamp', datetime.now().isoformat())
            }
        return response
    
    def _map_status_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """映射状态响应"""
        payload = self._unwrap_success_data(response)
        if isinstance(payload, dict) and isinstance(payload.get('portfolio'), dict):
            portfolio = payload['portfolio']
            return {
                'status': 'success',
                'data': {
                    'status': portfolio.get('status', 'running'),
                    'last_optimization': portfolio.get('updated_time', datetime.now().isoformat()),
                    'active_portfolios': 1,
                    'total_aum': float(portfolio.get('total_value', 0.0)),
                    'portfolio_ids': [portfolio.get('portfolio_id')] if portfolio.get('portfolio_id') else []
                },
                'timestamp': response.get('timestamp', datetime.now().isoformat())
            }
        if isinstance(payload, dict) and isinstance(payload.get('items'), list):
            items = [item for item in payload.get('items', []) if isinstance(item, dict)]
            updated_times = [str(item.get('updated_time') or '') for item in items if item.get('updated_time')]
            return {
                'status': 'success',
                'data': {
                    'status': 'running',
                    'last_optimization': max(updated_times) if updated_times else datetime.now().isoformat(),
                    'active_portfolios': len(items),
                    'total_aum': float(sum(float(item.get('total_value', 0.0)) for item in items)),
                    'portfolio_ids': [item.get('portfolio_id') for item in items if item.get('portfolio_id')]
                },
                'timestamp': response.get('timestamp', datetime.now().isoformat())
            }
        return response
    
    def _map_health_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """映射健康检查响应（兼容portfolio-service返回结构）"""
        data = response.get('data', {}) if isinstance(response, dict) else {}
        return {
            'status': data.get('status', response.get('status', 'unknown')),
            'timestamp': data.get('timestamp', response.get('timestamp', datetime.now().isoformat())),
            'system': data.get('service', 'portfolio_management'),
            'version': data.get('version', response.get('version', '1.0.0'))
        }

    @staticmethod
    def _unwrap_success_data(response: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(response, dict):
            return {}
        data = response.get('data', {})
        return data if isinstance(data, dict) else {}
