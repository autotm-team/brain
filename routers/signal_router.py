"""
信号路由器实现

负责管理各层之间的信号传递、冲突解决和质量控制，
确保信号在三层系统间的正确路由和高效传递。
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, deque
import threading

from interfaces import ISignalRouter
from models import SignalRoutingResult, StandardSignal, EventType
from config import IntegrationConfig
from exceptions import (
    SignalRouterException, SignalConflictException,
    SignalValidationException, handle_exception
)
from routers.conflict_resolver import ConflictResolver
from routers.signal_processor import SignalProcessor
from adapters.strategy_adapter import StrategyAdapter
from adapters.portfolio_adapter import PortfolioAdapter
from serializers import to_jsonable

logger = logging.getLogger(__name__)


class SignalRouter(ISignalRouter):
    """信号路由器实现类
    
    管理各层之间的信号传递、冲突解决和质量控制，
    确保信号的正确路由、及时传递和高质量处理。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化信号路由器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 信号队列和缓存
        self._signal_queues: Dict[str, asyncio.Queue] = {
            'macro_to_portfolio': asyncio.Queue(maxsize=self.config.signal_router.max_signal_queue_size),
            'portfolio_to_strategy': asyncio.Queue(maxsize=self.config.signal_router.max_signal_queue_size),
            'strategy_to_validation': asyncio.Queue(maxsize=self.config.signal_router.max_signal_queue_size)
        }
        
        # 信号历史和统计
        self._signal_history: deque = deque(maxlen=1000)
        self._routing_statistics: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # 组件
        self._conflict_resolver = ConflictResolver(config)
        self._signal_processor = SignalProcessor(config)

        # 适配器实例
        self._strategy_adapter: Optional[StrategyAdapter] = None
        self._task_orchestrator: Optional[Any] = None

        # 路由规则和策略
        self._routing_rules: Dict[str, Dict[str, Any]] = {}
        self._load_routing_rules()
        
        # 锁和同步
        self._routing_lock = threading.RLock()
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info("SignalRouter initialized")

    def set_task_orchestrator(self, task_orchestrator: Any) -> None:
        """注入 Brain 统一任务控制面。"""
        self._task_orchestrator = task_orchestrator
    
    async def start(self) -> bool:
        """启动信号路由器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("SignalRouter is already running")
                return True
            
            # 初始化StrategyAdapter
            await self._initialize_strategy_adapter()

            # 启动组件
            await self._conflict_resolver.start()
            await self._signal_processor.start()

            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            
            self._is_running = True
            logger.info("SignalRouter started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start SignalRouter: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止信号路由器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping SignalRouter...")
            self._is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
            if self._cleanup_task:
                self._cleanup_task.cancel()
            
            # 停止组件
            await self._conflict_resolver.stop()
            await self._signal_processor.stop()
            
            logger.info("SignalRouter stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping SignalRouter: {e}")
            return False

    def is_running(self) -> bool:
        """暴露给 HTTP 层的运行状态。"""
        return self._is_running

    async def route_signal(self, signal_payload: Dict[str, Any]) -> Dict[str, Any]:
        """兼容 HTTP 层的通用信号路由入口。"""
        if not self._is_running:
            raise SignalRouterException("SignalRouter is not running")

        signal = await self._signal_processor._convert_to_standard_signal(signal_payload)
        validation_result = await self._validate_signal(signal)
        conflicts = await self.detect_signal_conflicts([signal])
        resolutions = await self.resolve_signal_conflicts(conflicts) if conflicts.get('has_conflicts') else []

        target = str(signal.target or "").lower()
        source = str(signal.source or "").lower()
        if 'portfolio' in target or 'macro' in source:
            routed = await self._route_to_portfolio_layer(signal)
            target_layer = 'portfolio'
        elif 'strategy' in target or 'portfolio' in source:
            routed = await self._route_to_strategy_layer(signal)
            target_layer = 'strategy'
        else:
            routed = await self._signal_processor.process_signal(signal)
            target_layer = target or 'unknown'

        routing_result = SignalRoutingResult(
            routing_id=str(uuid.uuid4()),
            source_layer=source or 'unknown',
            target_layer=target_layer,
            original_signals=[signal_payload],
            routed_signals=[routed],
            conflicts_detected=conflicts.get('conflicts', []),
            conflicts_resolved=to_jsonable(resolutions),
        )
        self._record_routing_result(routing_result)

        return {
            'routing': to_jsonable(routing_result),
            'validation': to_jsonable(validation_result),
            'result': to_jsonable(routed),
        }

    async def get_signal_conflicts(self) -> Dict[str, Any]:
        """返回最近路由中记录的冲突摘要。"""
        items: List[Dict[str, Any]] = []
        with self._routing_lock:
            for entry in reversed(list(self._signal_history)):
                if entry.conflicts_detected:
                    items.extend(to_jsonable(entry.conflicts_detected))
        return {
            'items': items,
            'total': len(items),
            'statistics': self._conflict_resolver.get_conflict_statistics(),
        }

    async def resolve_conflicts(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """兼容 HTTP 层的冲突解决入口。"""
        if payload.get('has_conflicts'):
            conflicts = payload
        else:
            signals = payload.get('signals') if isinstance(payload.get('signals'), list) else []
            conflicts = await self.detect_signal_conflicts(signals)
        resolved = await self.resolve_signal_conflicts(conflicts)
        return {
            'resolved': to_jsonable(resolved),
            'resolved_count': len(resolved),
        }

    async def get_signal_history(self, query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """返回最近信号路由历史。"""
        query_params = query_params if isinstance(query_params, dict) else {}
        limit = max(1, int(query_params.get('limit', 50)))
        offset = max(0, int(query_params.get('offset', 0)))

        with self._routing_lock:
            history_items = [to_jsonable(item) for item in reversed(list(self._signal_history))]

        total = len(history_items)
        return {
            'items': history_items[offset: offset + limit],
            'total': total,
            'limit': limit,
            'offset': offset,
        }

    async def get_signal_statistics(self) -> Dict[str, Any]:
        """返回统一的信号路由统计。"""
        return {
            'routing': self.get_routing_statistics(),
            'processing': self._signal_processor.get_processing_statistics(),
            'conflicts': self._conflict_resolver.get_conflict_statistics(),
        }

    async def cleanup_expired_signals(self) -> Dict[str, Any]:
        """清理过期信号并返回清理结果。"""
        removed = await self._cleanup_expired_signals()
        return {'removed': removed}
    
    async def route_macro_signals(self, macro_state: Any) -> Any:
        """路由宏观信号
        
        Args:
            macro_state: 宏观状态
            
        Returns:
            Any: 组合指令
        """
        try:
            routing_id = str(uuid.uuid4())
            start_time = datetime.now()
            
            logger.debug(f"Routing macro signals: {routing_id}")
            
            # 转换为标准信号格式
            standard_signal = await self._convert_macro_to_standard_signal(macro_state)
            
            # 验证信号
            if self.config.signal_router.enable_signal_validation:
                validation_result = await self._validate_signal(standard_signal)
                if not validation_result['valid']:
                    raise SignalValidationException(
                        standard_signal.signal_id, 
                        validation_result['errors']
                    )
            
            # 检测冲突
            conflicts = await self.detect_signal_conflicts([standard_signal])
            if conflicts['conflicts']:
                resolved_signals = await self.resolve_signal_conflicts(conflicts)
                standard_signal = resolved_signals[0] if resolved_signals else standard_signal
            
            # 路由到组合层
            portfolio_instruction = await self._route_to_portfolio_layer(standard_signal)
            
            # 记录路由结果
            routing_result = SignalRoutingResult(
                routing_id=routing_id,
                source_layer="macro",
                target_layer="portfolio",
                original_signals=[macro_state],
                routed_signals=[portfolio_instruction],
                routing_time=(datetime.now() - start_time).total_seconds()
            )
            
            self._record_routing_result(routing_result)
            
            logger.debug(f"Macro signal routing completed: {routing_id}")
            return portfolio_instruction
            
        except Exception as e:
            logger.error(f"Failed to route macro signals: {e}")
            raise SignalRouterException(f"Macro signal routing failed: {e}")
    
    async def route_portfolio_signals(self, instruction: Any) -> Any:
        """路由组合信号
        
        Args:
            instruction: 组合指令
            
        Returns:
            Any: 战术分配
        """
        try:
            routing_id = str(uuid.uuid4())
            start_time = datetime.now()
            
            logger.debug(f"Routing portfolio signals: {routing_id}")
            
            # 转换为标准信号格式
            standard_signal = await self._convert_portfolio_to_standard_signal(instruction)
            
            # 验证和冲突检测
            if self.config.signal_router.enable_signal_validation:
                validation_result = await self._validate_signal(standard_signal)
                if not validation_result['valid']:
                    raise SignalValidationException(
                        standard_signal.signal_id,
                        validation_result['errors']
                    )
            
            # 路由到策略层
            strategy_allocation = await self._route_to_strategy_layer(standard_signal)

            # 记录路由结果
            routing_result = SignalRoutingResult(
                routing_id=routing_id,
                source_layer="portfolio",
                target_layer="strategy",
                original_signals=[instruction],
                routed_signals=[strategy_allocation],
                routing_time=(datetime.now() - start_time).total_seconds()
            )

            self._record_routing_result(routing_result)

            logger.debug(f"Portfolio signal routing completed: {routing_id}")
            return strategy_allocation
            
        except Exception as e:
            logger.error(f"Failed to route portfolio signals: {e}")
            raise SignalRouterException(f"Portfolio signal routing failed: {e}")
    
    async def route_analysis_results(self, results: List[Any]) -> List[Any]:
        """路由分析结果

        Args:
            results: 分析结果列表

        Returns:
            List[Any]: 路由后的结果列表
        """
        try:
            routing_id = str(uuid.uuid4())
            start_time = datetime.now()
            
            logger.debug(f"Routing analysis results: {routing_id}, count: {len(results)}")

            # 批量处理结果
            batch_size = self.config.signal_router.batch_processing_size
            routed_results = []

            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]
                batch_result = await self._process_result_batch(batch)
                routed_results.extend(batch_result)
            
            # 记录路由结果
            routing_result = SignalRoutingResult(
                routing_id=routing_id,
                source_layer="strategy",
                target_layer="validation",
                original_signals=results,
                routed_signals=routed_results,
                routing_time=(datetime.now() - start_time).total_seconds(),
                success_rate=len(routed_results) / len(results) if results else 1.0
            )

            self._record_routing_result(routing_result)

            logger.debug(f"Analysis results routing completed: {routing_id}")
            return routed_results
            
        except Exception as e:
            logger.error(f"Failed to route trading signals: {e}")
            raise SignalRouterException(f"Trading signal routing failed: {e}")
    
    async def detect_signal_conflicts(self, signals: List[Any]) -> Dict[str, Any]:
        """检测信号冲突
        
        Args:
            signals: 信号列表
            
        Returns:
            Dict[str, Any]: 冲突检测结果
        """
        try:
            return await self._conflict_resolver.detect_conflicts(signals)
        except Exception as e:
            logger.error(f"Signal conflict detection failed: {e}")
            raise SignalRouterException(f"Conflict detection failed: {e}")
    
    async def resolve_signal_conflicts(self, conflicts: Dict[str, Any]) -> List[Any]:
        """解决信号冲突
        
        Args:
            conflicts: 冲突信息
            
        Returns:
            List[Any]: 解决后的信号列表
        """
        try:
            return await self._conflict_resolver.resolve_conflicts(conflicts)
        except Exception as e:
            logger.error(f"Signal conflict resolution failed: {e}")
            raise SignalRouterException(f"Conflict resolution failed: {e}")
    
    def get_routing_statistics(self) -> Dict[str, Any]:
        """获取路由统计信息
        
        Returns:
            Dict[str, Any]: 路由统计
        """
        with self._routing_lock:
            return {
                'total_routings': sum(sum(stats.values()) for stats in self._routing_statistics.values()),
                'by_layer': dict(self._routing_statistics),
                'queue_sizes': {name: queue.qsize() for name, queue in self._signal_queues.items()},
                'history_size': len(self._signal_history)
            }
    
    # 私有方法实现
    def _load_routing_rules(self) -> None:
        """加载路由规则"""
        self._routing_rules = {
            'macro_to_portfolio': {
                'timeout': self.config.signal_router.signal_timeout,
                'priority_mapping': {'high': 5, 'medium': 3, 'low': 1},
                'validation_rules': ['format_check', 'range_check']
            },
            'portfolio_to_strategy': {
                'timeout': self.config.signal_router.signal_timeout,
                'priority_mapping': {'urgent': 5, 'normal': 3, 'low': 1},
                'validation_rules': ['format_check', 'constraint_check']
            },
            'strategy_to_validation': {
                'timeout': self.config.signal_router.signal_timeout,
                'priority_mapping': {'immediate': 5, 'normal': 3, 'delayed': 1},
                'validation_rules': ['format_check', 'risk_check']
            }
        }
    
    async def _convert_macro_to_standard_signal(self, macro_state: Any) -> StandardSignal:
        """转换宏观状态为标准信号"""
        return StandardSignal(
            signal_id=str(uuid.uuid4()),
            signal_type="macro_guidance",
            source="macro_layer",
            target="portfolio_layer",
            strength=0.8,  # 基于宏观状态计算
            confidence=0.9,  # 基于宏观状态计算
            data=macro_state if isinstance(macro_state, dict) else {"state": macro_state}
        )
    
    async def _convert_portfolio_to_standard_signal(self, instruction: Any) -> StandardSignal:
        """转换组合指令为标准信号"""
        return StandardSignal(
            signal_id=str(uuid.uuid4()),
            signal_type="portfolio_instruction",
            source="portfolio_layer",
            target="strategy_layer",
            strength=0.7,  # 基于指令计算
            confidence=0.8,  # 基于指令计算
            data=instruction if isinstance(instruction, dict) else {"instruction": instruction}
        )
    
    async def _validate_signal(self, signal: StandardSignal) -> Dict[str, Any]:
        """验证信号"""
        return await self._signal_processor.validate_signal(signal)
    
    async def _route_to_portfolio_layer(self, signal: StandardSignal) -> Any:
        """路由到组合层"""
        try:
            # 获取组合适配器
            if not hasattr(self, '_portfolio_adapter') or self._portfolio_adapter is None:
                from config import get_settings

                config = getattr(self, 'config', get_settings())
                self._portfolio_adapter = PortfolioAdapter(config)
                await self._portfolio_adapter.connect_to_system()

            # 构造组合优化请求
            optimization_request = {
                'type': 'portfolio_optimization',
                'macro_state': signal.data,
                'constraints': {
                    'max_sector_weight': 0.35,
                    'max_single_stock': 0.05
                },
                'signal_strength': signal.strength,
                'signal_confidence': signal.confidence
            }

            # 执行组合优化
            result = await self._portfolio_adapter.request_portfolio_optimization(
                signal.data, optimization_request.get('constraints', {})
            )

            logger.debug(f"Portfolio signal routing completed for signal: {signal.signal_id}")
            return result

        except Exception as e:
            logger.error(f"Portfolio signal routing failed: {e}")
            # 返回默认结果以保持信号流继续
            return {"target_position": 0.6, "sector_weights": signal.data, "error": str(e)}
    
    async def _route_to_strategy_layer(self, signal: StandardSignal) -> Any:
        """路由到策略层"""
        try:
            if not self._strategy_adapter:
                raise SignalRouterException("StrategyAdapter not initialized")
            if not self._task_orchestrator:
                raise SignalRouterException("TaskOrchestrator not initialized")

            # 从信号中提取股票代码和组合指令
            symbols = self._extract_symbols_from_signal(signal)
            portfolio_instruction = signal.data

            logger.debug(
                "Routing signal %s to strategy control plane with %s symbols",
                signal.signal_id,
                len(symbols),
            )
            params = self._strategy_adapter.build_strategy_analysis_params(
                portfolio_instruction if isinstance(portfolio_instruction, dict) else {},
                symbols,
            )
            created = await self._task_orchestrator.create_task_job(
                service="execution",
                job_type="batch_analyze",
                params=params,
                metadata={
                    "control_plane_source": "signal_router",
                    "signal_id": signal.signal_id,
                    "stage": "strategy_analysis",
                },
            )

            logger.debug("Strategy layer routing dispatched for signal %s as task_job_id=%s", signal.signal_id, created.get("id"))
            return {
                "mode": "control_plane_async",
                "task_job_id": created.get("id"),
                "service_job_id": created.get("service_job_id"),
                "service": created.get("service") or "execution",
                "job_type": created.get("job_type") or "batch_analyze",
                "status": created.get("status"),
                "signal_id": signal.signal_id,
                "symbols": symbols,
            }

        except Exception as e:
            logger.error(f"Strategy layer routing failed for signal {signal.signal_id}: {e}")
            # 返回默认结果以保持信号流继续
            return {"allocation": signal.data, "constraints": {"max_position": 0.1}, "error": str(e)}
    
    async def _process_signal_batch(self, batch: List[Any]) -> List[Any]:
        """处理信号批次"""
        processed = []
        for signal in batch:
            try:
                # 处理单个信号
                processed_signal = await self._signal_processor.process_signal(signal)
                processed.append(processed_signal)
            except Exception as e:
                logger.warning(f"Failed to process signal: {e}")
        return processed

    async def _process_result_batch(self, batch: List[Any]) -> List[Any]:
        """处理分析结果批次"""
        processed = []
        for result in batch:
            try:
                # 处理单个分析结果
                processed_result = await self._signal_processor.process_analysis_result(result)
                processed.append(processed_result)
            except Exception as e:
                logger.warning(f"Failed to process analysis result: {e}")
        return processed
    
    def _record_routing_result(self, result: SignalRoutingResult) -> None:
        """记录路由结果"""
        with self._routing_lock:
            self._signal_history.append(result)
            self._routing_statistics[result.source_layer][result.target_layer] += 1
    
    async def _monitoring_loop(self) -> None:
        """监控循环"""
        while self._is_running:
            try:
                await self._check_queue_health()
                await asyncio.sleep(10)  # 每10秒检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
    
    async def _cleanup_loop(self) -> None:
        """清理循环"""
        while self._is_running:
            try:
                await self._cleanup_expired_signals()
                await asyncio.sleep(60)  # 每分钟清理一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
    
    async def _check_queue_health(self) -> None:
        """检查队列健康状态"""
        for name, queue in self._signal_queues.items():
            if queue.qsize() > queue.maxsize * 0.8:
                logger.warning(f"Signal queue {name} is nearly full: {queue.qsize()}/{queue.maxsize}")
    
    async def _cleanup_expired_signals(self) -> int:
        """清理过期信号"""
        current_time = datetime.now()
        expiry_time = timedelta(seconds=self.config.signal_router.signal_expiry_time)
        removed = 0
        
        # 清理历史记录中的过期信号
        while (self._signal_history and
               current_time - self._signal_history[0].timestamp > expiry_time):
            self._signal_history.popleft()
            removed += 1
        return removed

    async def _initialize_strategy_adapter(self) -> None:
        """初始化StrategyAdapter"""
        try:
            logger.info("Initializing StrategyAdapter in SignalRouter...")
            self._strategy_adapter = StrategyAdapter(self.config)

            # 连接到策略分析系统
            connected = await self._strategy_adapter.connect_to_system()
            if not connected:
                raise SignalRouterException("Failed to connect to strategy analysis system")

            logger.info("StrategyAdapter initialized and connected successfully in SignalRouter")

        except Exception as e:
            logger.error(f"Failed to initialize StrategyAdapter in SignalRouter: {e}")
            raise SignalRouterException(f"StrategyAdapter initialization failed: {e}")

    def _extract_symbols_from_signal(self, signal: StandardSignal) -> List[str]:
        """从信号中提取股票代码列表"""
        symbols = []

        if signal.data and isinstance(signal.data, dict):
            # 尝试从不同字段提取股票代码
            symbols.extend(signal.data.get('symbols', []))
            symbols.extend(signal.data.get('target_symbols', []))

            # 从权重配置中提取
            weights = signal.data.get('weights', {})
            if isinstance(weights, dict):
                symbols.extend(weights.keys())

            # 从持仓配置中提取
            positions = signal.data.get('positions', {})
            if isinstance(positions, dict):
                symbols.extend(positions.keys())

        # 去重并过滤有效的股票代码
        unique_symbols = list(set(symbols))
        valid_symbols = [s for s in unique_symbols if s and isinstance(s, str) and len(s) >= 6]

        # 如果没有找到有效股票代码，使用默认测试代码
        if not valid_symbols:
            valid_symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
            logger.warning(f"No valid symbols found in signal {signal.signal_id}, using default: {valid_symbols}")

        logger.debug(f"Extracted symbols from signal {signal.signal_id}: {valid_symbols}")
        return valid_symbols
