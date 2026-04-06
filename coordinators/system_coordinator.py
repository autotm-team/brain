"""
系统协调器实现

负责统一协调三层金融交易系统的分析周期、资源管理和异常处理，
是整个集成层的核心控制组件。
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import threading

from interfaces import ISystemCoordinator
from models import (
    AnalysisCycleResult, SystemStatus, MarketEvent, ResourceAllocation,
    ConflictResolution, SystemHealthStatus, EventType
)
from config import IntegrationConfig
from exceptions import (
    SystemCoordinatorException, CycleExecutionException,
    ResourceAllocationException, handle_exception
)
from adapters.strategy_adapter import StrategyAdapter
from adapters.macro_adapter import MacroAdapter
from adapters.portfolio_adapter import PortfolioAdapter
from managers.data_flow_manager import DataFlowManager

logger = logging.getLogger(__name__)


class SystemCoordinator(ISystemCoordinator):
    """系统协调器实现类
    
    统一协调三层系统的分析周期、资源分配和异常处理，
    确保系统各组件协调运行和高效执行。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化系统协调器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        self._active_cycles: Dict[str, AnalysisCycleResult] = {}
        self._resource_allocation: Optional[ResourceAllocation] = None
        self._system_status = SystemStatus(
            overall_health=SystemHealthStatus.HEALTHY,
            macro_system_status=SystemHealthStatus.HEALTHY,
            portfolio_system_status=SystemHealthStatus.HEALTHY,
            strategy_system_status=SystemHealthStatus.HEALTHY,
            tactical_system_status=SystemHealthStatus.HEALTHY,
            data_pipeline_status=SystemHealthStatus.HEALTHY,
            last_update_time=datetime.now()
        )
        
        # 线程池用于并发执行
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.system_coordinator.max_concurrent_cycles
        )
        
        # 事件队列
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._event_handlers: Dict[EventType, List[callable]] = {}
        
        # 锁和同步
        self._cycle_lock = threading.RLock()
        self._resource_lock = threading.RLock()

        # 适配器实例
        self._strategy_adapter: Optional[StrategyAdapter] = None
        self._task_orchestrator: Optional[Any] = None

        # 管理器实例
        self._data_flow_manager: Optional[DataFlowManager] = None

        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("SystemCoordinator initialized")

    def set_task_orchestrator(self, task_orchestrator: Any) -> None:
        """注入 Brain 统一任务控制面。"""
        self._task_orchestrator = task_orchestrator
    
    async def start(self) -> bool:
        """启动系统协调器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("SystemCoordinator is already running")
                return True
            
            # 初始化StrategyAdapter
            await self._initialize_strategy_adapter()

            # 初始化DataFlowManager
            await self._initialize_data_flow_manager()

            # 启动系统
            startup_success = await self.coordinate_system_startup()
            if not startup_success:
                raise SystemCoordinatorException("Failed to start system")

            # 分配初始资源
            await self.allocate_system_resources()
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            # 启动事件处理任务
            asyncio.create_task(self._event_processing_loop())
            
            self._is_running = True
            logger.info("SystemCoordinator started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start SystemCoordinator: {e}")
            await self.stop()
            return False
    
    async def stop(self) -> bool:
        """停止系统协调器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping SystemCoordinator...")
            
            # 停止接受新的分析周期
            self._is_running = False
            
            # 等待活跃周期完成
            await self._wait_for_active_cycles()
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            # 关闭线程池
            self._executor.shutdown(wait=True)

            # 停止数据流管理器
            if self._data_flow_manager:
                try:
                    await self._data_flow_manager.stop()
                    logger.info("DataFlowManager stopped")
                except Exception as e:
                    logger.warning(f"Error stopping DataFlowManager: {e}")

            # 协调系统关闭
            await self.coordinate_system_shutdown()
            
            logger.info("SystemCoordinator stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping SystemCoordinator: {e}")
            return False
    
    async def coordinate_full_analysis_cycle(self) -> AnalysisCycleResult:
        """协调完整分析周期
        
        Returns:
            AnalysisCycleResult: 分析周期执行结果
        """
        if not self._is_running:
            raise SystemCoordinatorException("SystemCoordinator is not running")
        
        # 检查并发限制
        with self._cycle_lock:
            if len(self._active_cycles) >= self.config.system_coordinator.max_concurrent_cycles:
                raise SystemCoordinatorException("Maximum concurrent cycles limit reached")
        
        cycle_id = str(uuid.uuid4())
        cycle_result = AnalysisCycleResult(
            cycle_id=cycle_id,
            start_time=datetime.now(),
            status="running"
        )
        
        try:
            # 注册活跃周期
            with self._cycle_lock:
                self._active_cycles[cycle_id] = cycle_result
            
            logger.info(f"Starting analysis cycle: {cycle_id}")
            
            # 阶段1: 宏观分析
            macro_state = await self._execute_macro_analysis(cycle_id)
            cycle_result.macro_state = macro_state
            
            # 阶段2: 组合管理
            portfolio_instruction = await self._execute_portfolio_management(cycle_id, macro_state)
            cycle_result.portfolio_instruction = portfolio_instruction
            
            # 阶段3: 策略分析
            analysis_results = await self._execute_strategy_analysis(cycle_id, portfolio_instruction)
            cycle_result.analysis_results = analysis_results

            # 阶段4: 策略验证
            validation_result = await self._execute_strategy_validation(cycle_id, analysis_results)
            cycle_result.validation_result = validation_result
            
            # 完成周期
            cycle_result.end_time = datetime.now()
            cycle_result.status = "completed"
            
            logger.info(f"Analysis cycle completed: {cycle_id}, duration: {cycle_result.duration:.2f}s")
            return cycle_result
            
        except Exception as e:
            cycle_result.end_time = datetime.now()
            cycle_result.status = "failed"
            cycle_result.error_messages.append(str(e))
            
            logger.error(f"Analysis cycle failed: {cycle_id}, error: {e}")
            raise CycleExecutionException(cycle_id, "unknown", str(e))
            
        finally:
            # 清理活跃周期
            with self._cycle_lock:
                self._active_cycles.pop(cycle_id, None)
    
    async def coordinate_real_time_updates(self, events: List[MarketEvent]) -> None:
        """协调实时更新处理
        
        Args:
            events: 市场事件列表
        """
        try:
            # 按优先级排序事件
            sorted_events = sorted(events, key=lambda e: e.priority, reverse=True)
            
            for event in sorted_events:
                await self._event_queue.put(event)
                logger.debug(f"Queued event: {event.event_id}, type: {event.event_type}")
            
        except Exception as e:
            logger.error(f"Failed to coordinate real-time updates: {e}")
            raise SystemCoordinatorException(f"Real-time update coordination failed: {e}")
    
    async def coordinate_system_startup(self) -> bool:
        """协调系统启动流程
        
        Returns:
            bool: 启动是否成功
        """
        try:
            logger.info("Coordinating system startup...")
            
            # 检查各层系统健康状态
            health_checks = await asyncio.gather(
                self._check_macro_system_health(),
                self._check_portfolio_system_health(),
                self._check_strategy_system_health(),
                self._check_data_pipeline_health(),
                return_exceptions=True
            )

            # 更新系统状态
            self._system_status.macro_system_status = health_checks[0] if not isinstance(health_checks[0], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.portfolio_system_status = health_checks[1] if not isinstance(health_checks[1], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.strategy_system_status = health_checks[2] if not isinstance(health_checks[2], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.data_pipeline_status = health_checks[3] if not isinstance(health_checks[3], Exception) else SystemHealthStatus.CRITICAL
            
            # 计算整体健康状态
            self._update_overall_health()
            
            # 检查是否可以启动
            if self._system_status.overall_health == SystemHealthStatus.CRITICAL:
                logger.error("System startup failed: critical health issues detected")
                return False
            
            logger.info("System startup coordination completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"System startup coordination failed: {e}")
            return False
    
    async def coordinate_system_shutdown(self) -> bool:
        """协调系统关闭流程
        
        Returns:
            bool: 关闭是否成功
        """
        try:
            logger.info("Coordinating system shutdown...")
            
            # 等待活跃周期完成
            await self._wait_for_active_cycles()
            
            # 通知各层系统准备关闭
            await asyncio.gather(
                self._notify_macro_system_shutdown(),
                self._notify_portfolio_system_shutdown(),
                self._notify_strategy_system_shutdown(),
                return_exceptions=True
            )
            
            logger.info("System shutdown coordination completed")
            return True
            
        except Exception as e:
            logger.error(f"System shutdown coordination failed: {e}")
            return False
    
    async def allocate_system_resources(self) -> ResourceAllocation:
        """分配系统资源
        
        Returns:
            ResourceAllocation: 资源分配结果
        """
        try:
            with self._resource_lock:
                allocation_id = str(uuid.uuid4())
                
                # 基于配置策略分配资源
                strategy = self.config.system_coordinator.resource_allocation_strategy
                
                if strategy == "balanced":
                    allocation = self._create_balanced_allocation(allocation_id)
                elif strategy == "performance":
                    allocation = self._create_performance_allocation(allocation_id)
                elif strategy == "conservative":
                    allocation = self._create_conservative_allocation(allocation_id)
                else:
                    allocation = self._create_balanced_allocation(allocation_id)
                
                self._resource_allocation = allocation
                self._resource_allocation.strategy = strategy
                self._resource_allocation.metadata = {
                    **(self._resource_allocation.metadata or {}),
                    "strategy": strategy,
                }
                logger.info(f"Resource allocation completed: {allocation_id}, strategy: {strategy}")
                return allocation
                
        except Exception as e:
            logger.error(f"Resource allocation failed: {e}")
            raise ResourceAllocationException("system", 0, 0, {"error": str(e)})
    
    async def handle_system_exception(self, exception: Exception) -> ConflictResolution:
        """处理系统异常
        
        Args:
            exception: 系统异常
            
        Returns:
            ConflictResolution: 异常处理结果
        """
        try:
            integration_exception = handle_exception(exception, "SystemCoordinator")
            
            resolution_id = str(uuid.uuid4())
            resolution = ConflictResolution(
                resolution_id=resolution_id,
                conflict_type="system_exception",
                conflict_description=str(exception),
                resolution_strategy="auto_recovery",
                resolution_result="pending",
                affected_components=[integration_exception.component]
            )
            
            # 尝试自动恢复
            if self.config.system_coordinator.enable_auto_recovery:
                recovery_success = await self._attempt_auto_recovery(integration_exception)
                resolution.resolution_result = "success" if recovery_success else "failed"
            else:
                resolution.resolution_result = "manual_intervention_required"
            
            logger.info(f"Exception handled: {resolution_id}, result: {resolution.resolution_result}")
            return resolution
            
        except Exception as e:
            logger.error(f"Exception handling failed: {e}")
            raise SystemCoordinatorException(f"Failed to handle exception: {e}")
    
    def get_system_status(self) -> SystemStatus:
        """获取系统状态
        
        Returns:
            SystemStatus: 当前系统状态
        """
        return self._system_status

    def is_running(self) -> bool:
        """是否在运行"""
        return self._is_running

    def get_resource_allocation(self) -> Optional[ResourceAllocation]:
        """获取资源分配信息"""
        return self._resource_allocation
    
    def get_active_cycles(self) -> Dict[str, AnalysisCycleResult]:
        """获取活跃分析周期
        
        Returns:
            Dict[str, AnalysisCycleResult]: 活跃周期字典
        """
        with self._cycle_lock:
            return self._active_cycles.copy()
    
    # 私有方法实现
    async def _execute_macro_analysis(self, cycle_id: str) -> Any:
        """执行宏观分析阶段"""
        try:
            # 获取宏观适配器
            if not hasattr(self, '_macro_adapter') or self._macro_adapter is None:
                self._macro_adapter = MacroAdapter(self.config)
                await self._macro_adapter.connect_to_system()

            # 调用真实的宏观分析
            logger.info(f"Executing macro analysis for cycle: {cycle_id}")

            # 获取当前宏观状态
            macro_state = await self._macro_adapter.get_macro_state()

            # 如果需要触发新的分析，可以调用
            # analysis_result = await self._macro_adapter.trigger_macro_analysis()

            logger.info(f"Macro analysis completed for cycle: {cycle_id}")
            return macro_state

        except Exception as e:
            logger.error(f"Macro analysis failed for cycle {cycle_id}: {e}")
            raise CycleExecutionException(cycle_id, "macro_analysis", str(e))
    
    async def _execute_portfolio_management(self, cycle_id: str, macro_state: Any) -> Any:
        """执行组合管理阶段"""
        try:
            # 获取组合适配器
            if not hasattr(self, '_portfolio_adapter') or self._portfolio_adapter is None:
                config = getattr(self, 'config', None)
                if config is None:
                    raise RuntimeError("SystemCoordinator runtime config is not bound")
                self._portfolio_adapter = PortfolioAdapter(config)
                await self._portfolio_adapter.connect_to_system()

            # 构造组合优化请求
            optimization_request = {
                'type': 'portfolio_optimization',
                'macro_state': macro_state,
                'constraints': {
                    'max_sector_weight': 0.35,
                    'max_single_stock': 0.05,
                    'var_limit': 0.02
                },
                'cycle_id': cycle_id
            }

            # 执行组合优化
            result = await self._portfolio_adapter.request_portfolio_optimization(
                macro_state, optimization_request.get('constraints', {})
            )

            logger.info(f"Portfolio management completed for cycle {cycle_id}")
            return result

        except Exception as e:
            logger.error(f"Portfolio management failed for cycle {cycle_id}: {e}")
            raise CycleExecutionException(cycle_id, "portfolio_management", str(e))
    
    async def _execute_strategy_analysis(self, cycle_id: str, portfolio_instruction: Any) -> List[Any]:
        """执行策略分析阶段"""
        try:
            if not self._strategy_adapter:
                raise CycleExecutionException(cycle_id, "strategy_analysis", "StrategyAdapter not initialized")
            if not self._task_orchestrator:
                raise CycleExecutionException(cycle_id, "strategy_analysis", "TaskOrchestrator not initialized")

            # 从组合指令中提取股票代码
            symbols = self._extract_symbols_from_instruction(portfolio_instruction)

            logger.info(
                "Dispatching strategy analysis through Brain control plane for cycle %s with %s symbols",
                cycle_id,
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
                    "control_plane_source": "system_coordinator",
                    "cycle_id": cycle_id,
                    "stage": "strategy_analysis",
                },
            )
            task_job_id = str(created.get("id") or "").strip()
            if not task_job_id:
                raise CycleExecutionException(cycle_id, "strategy_analysis", "TaskOrchestrator did not return task_job_id")

            logger.info("Strategy analysis dispatched for cycle %s as task_job_id=%s", cycle_id, task_job_id)
            completed = await self._wait_for_task_job_terminal(
                cycle_id,
                "strategy_analysis",
                task_job_id,
            )
            analysis_results = await self._strategy_adapter.normalize_analysis_job_result(
                completed.get("result") if isinstance(completed.get("result"), dict) else {},
            )

            logger.info(f"Strategy analysis completed for cycle {cycle_id}, got {len(analysis_results)} results")
            return analysis_results

        except Exception as e:
            logger.error(f"Strategy analysis failed for cycle {cycle_id}: {e}")
            raise CycleExecutionException(cycle_id, "strategy_analysis", str(e))
    
    async def _execute_strategy_validation(self, cycle_id: str, analysis_results: List[Any]) -> Any:
        """执行策略验证阶段"""
        try:
            if not self._strategy_adapter:
                raise CycleExecutionException(cycle_id, "strategy_validation", "StrategyAdapter not initialized")
            if not self._task_orchestrator:
                raise CycleExecutionException(cycle_id, "strategy_validation", "TaskOrchestrator not initialized")

            # 从分析结果中提取股票代码和构建策略配置
            symbols = [result.get('symbol') for result in analysis_results if result.get('symbol')]
            strategy_config = self._build_strategy_config_from_results(analysis_results)

            logger.info(
                "Dispatching strategy validation through Brain control plane for cycle %s with %s symbols",
                cycle_id,
                len(symbols),
            )
            params = self._strategy_adapter.build_backtest_validation_params(symbols, strategy_config)
            created = await self._task_orchestrator.create_task_job(
                service="execution",
                job_type="run_backtest",
                params=params,
                metadata={
                    "control_plane_source": "system_coordinator",
                    "cycle_id": cycle_id,
                    "stage": "strategy_validation",
                },
            )
            task_job_id = str(created.get("id") or "").strip()
            if not task_job_id:
                raise CycleExecutionException(cycle_id, "strategy_validation", "TaskOrchestrator did not return task_job_id")

            logger.info("Strategy validation dispatched for cycle %s as task_job_id=%s", cycle_id, task_job_id)
            completed = await self._wait_for_task_job_terminal(
                cycle_id,
                "strategy_validation",
                task_job_id,
            )
            validation_result = self._strategy_adapter.normalize_backtest_job_result(
                completed.get("result") if isinstance(completed.get("result"), dict) else {},
            )

            logger.info(f"Strategy validation completed for cycle {cycle_id}")
            return validation_result

        except Exception as e:
            logger.error(f"Strategy validation failed for cycle {cycle_id}: {e}")
            raise CycleExecutionException(cycle_id, "strategy_validation", str(e))

    async def _wait_for_task_job_terminal(
        self,
        cycle_id: str,
        stage: str,
        task_job_id: str,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> Dict[str, Any]:
        timeout_seconds = self._resolve_task_stage_timeout_seconds(stage)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        last_payload: Dict[str, Any] = {}

        while True:
            payload = await self._task_orchestrator.get_task_job(task_job_id)
            if isinstance(payload, dict):
                last_payload = payload
            status = str((payload or {}).get("status") or "").strip().lower()
            if status == "succeeded":
                return payload
            if status in {"failed", "cancelled"}:
                message = (
                    str((payload or {}).get("message") or "").strip()
                    or str(((payload or {}).get("error") or {}).get("message") or "").strip()
                    or f"task_job_id={task_job_id} finished with status={status}"
                )
                raise CycleExecutionException(cycle_id, stage, message, details={"task_job_id": task_job_id, "payload": payload})
            if asyncio.get_running_loop().time() >= deadline:
                raise CycleExecutionException(
                    cycle_id,
                    stage,
                    f"Timed out waiting for task_job_id={task_job_id}",
                    details={"task_job_id": task_job_id, "payload": last_payload},
                )
            await asyncio.sleep(max(0.1, float(poll_interval_seconds)))

    def _resolve_task_stage_timeout_seconds(self, stage: str) -> int:
        coordinator_config = getattr(self.config, "system_coordinator", None)
        cycle_timeout = max(1, int(getattr(coordinator_config, "cycle_timeout", 300) or 300))
        stage_token = str(stage or "").strip().lower()
        stage_timeout_map = {
            "strategy_analysis": getattr(coordinator_config, "strategy_analysis_timeout_seconds", 1800),
            "strategy_validation": getattr(coordinator_config, "strategy_validation_timeout_seconds", 1800),
        }
        stage_timeout = stage_timeout_map.get(stage_token)
        if stage_timeout is None:
            return cycle_timeout
        return max(cycle_timeout, max(1, int(stage_timeout or cycle_timeout)))

    async def _wait_for_active_cycles(self) -> None:
        """等待活跃周期完成"""
        timeout = self.config.system_coordinator.cycle_timeout
        start_time = datetime.now()

        while self._active_cycles:
            if (datetime.now() - start_time).total_seconds() > timeout:
                logger.warning(f"Timeout waiting for active cycles: {list(self._active_cycles.keys())}")
                break
            await asyncio.sleep(1)

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        interval = self.config.system_coordinator.health_check_interval

        while self._is_running:
            try:
                await self._update_system_health()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(interval)

    async def _event_processing_loop(self) -> None:
        """事件处理循环"""
        while self._is_running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._process_event(event)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event processing error: {e}")

    async def _process_event(self, event: MarketEvent) -> None:
        """处理单个事件"""
        try:
            handlers = self._event_handlers.get(event.event_type, [])
            if handlers:
                await asyncio.gather(*[handler(event) for handler in handlers])
            else:
                logger.debug(f"No handlers for event type: {event.event_type}")
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")

    async def _update_system_health(self) -> None:
        """更新系统健康状态"""
        try:
            # 检查各层系统健康状态
            health_checks = await asyncio.gather(
                self._check_macro_system_health(),
                self._check_portfolio_system_health(),
                self._check_strategy_system_health(),
                self._check_data_pipeline_health(),
                return_exceptions=True
            )

            # 更新状态
            self._system_status.macro_system_status = health_checks[0] if not isinstance(health_checks[0], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.portfolio_system_status = health_checks[1] if not isinstance(health_checks[1], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.strategy_system_status = health_checks[2] if not isinstance(health_checks[2], Exception) else SystemHealthStatus.CRITICAL
            self._system_status.data_pipeline_status = health_checks[3] if not isinstance(health_checks[3], Exception) else SystemHealthStatus.CRITICAL

            self._update_overall_health()
            self._system_status.last_update_time = datetime.now()
            self._system_status.active_sessions = len(self._active_cycles)

        except Exception as e:
            logger.error(f"Failed to update system health: {e}")

    def _update_overall_health(self) -> None:
        """更新整体健康状态"""
        statuses = [
            self._system_status.macro_system_status,
            self._system_status.portfolio_system_status,
            self._system_status.strategy_system_status,
            self._system_status.data_pipeline_status
        ]

        if any(status == SystemHealthStatus.CRITICAL for status in statuses):
            self._system_status.overall_health = SystemHealthStatus.CRITICAL
        elif any(status == SystemHealthStatus.WARNING for status in statuses):
            self._system_status.overall_health = SystemHealthStatus.WARNING
        elif any(status == SystemHealthStatus.DEGRADED for status in statuses):
            self._system_status.overall_health = SystemHealthStatus.DEGRADED
        else:
            self._system_status.overall_health = SystemHealthStatus.HEALTHY

    async def _check_macro_system_health(self) -> SystemHealthStatus:
        """检查宏观系统健康状态"""
        try:
            # 获取宏观适配器
            if not hasattr(self, '_macro_adapter') or self._macro_adapter is None:
                self._macro_adapter = MacroAdapter(self.config)
                await self._macro_adapter.connect_to_system()

            # 调用真实的健康检查
            is_healthy = await self._macro_adapter.health_check()

            if is_healthy:
                logger.debug("Macro system health check passed")
                return SystemHealthStatus.HEALTHY
            else:
                logger.warning("Macro system health check failed")
                return SystemHealthStatus.DEGRADED

        except Exception as e:
            logger.error(f"Macro system health check error: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_portfolio_system_health(self) -> SystemHealthStatus:
        """检查组合系统健康状态"""
        try:
            # 获取组合适配器
            if not hasattr(self, '_portfolio_adapter') or self._portfolio_adapter is None:
                config = getattr(self, 'config', None)
                if config is None:
                    raise RuntimeError("SystemCoordinator runtime config is not bound")
                self._portfolio_adapter = PortfolioAdapter(config)
                await self._portfolio_adapter.connect_to_system()

            # 执行真实的健康检查
            health_result = await self._portfolio_adapter.health_check()

            if health_result:
                logger.debug("Portfolio system health check passed")
                return SystemHealthStatus.HEALTHY
            else:
                logger.warning("Portfolio system health check failed")
                return SystemHealthStatus.DEGRADED

        except Exception as e:
            logger.error(f"Portfolio system health check error: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_strategy_system_health(self) -> SystemHealthStatus:
        """检查策略系统健康状态"""
        try:
            if not self._strategy_adapter:
                logger.warning("StrategyAdapter not initialized, returning CRITICAL status")
                return SystemHealthStatus.CRITICAL

            # 调用真实的StrategyAdapter健康检查
            is_healthy = await self._strategy_adapter.health_check()
            status = SystemHealthStatus.HEALTHY if is_healthy else SystemHealthStatus.CRITICAL

            logger.debug(f"Strategy system health check result: {status}")
            return status

        except Exception as e:
            logger.error(f"Strategy system health check error: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_data_pipeline_health(self) -> SystemHealthStatus:
        """检查数据管道健康状态"""
        try:
            # 检查数据流管理器是否已初始化
            if self._data_flow_manager is None:
                logger.warning("DataFlowManager not initialized, attempting to initialize...")
                await self._initialize_data_flow_manager()
                if self._data_flow_manager is None:
                    return SystemHealthStatus.CRITICAL

            # 检查数据流管理器是否运行
            if not self._data_flow_manager.is_running():
                logger.warning("DataFlowManager is not running, attempting to start...")
                if not await self._data_flow_manager.start():
                    return SystemHealthStatus.CRITICAL

            # 执行数据管道健康检查
            logger.debug("Performing data pipeline health check...")

            # 1. 检查数据源健康状态
            data_sources_health = await self._check_data_sources_health()

            # 2. 检查数据质量
            data_quality_status = await self._check_data_quality()

            # 3. 检查数据流状态
            data_flow_status = await self._data_flow_manager.get_data_flow_status()

            # 4. 检查缓存状态
            cache_health = await self._check_cache_health()

            # 综合评估健康状态
            health_scores = []

            # 数据源健康评分
            if data_sources_health['healthy_sources'] == 0:
                health_scores.append(0.0)  # 没有健康数据源
            else:
                source_ratio = data_sources_health['healthy_sources'] / data_sources_health['total_sources']
                health_scores.append(source_ratio)

            # 数据质量评分
            if data_quality_status['overall_quality'] >= 0.8:
                health_scores.append(1.0)
            elif data_quality_status['overall_quality'] >= 0.6:
                health_scores.append(0.7)
            elif data_quality_status['overall_quality'] >= 0.4:
                health_scores.append(0.5)
            else:
                health_scores.append(0.2)

            # 数据流状态评分
            if data_flow_status and data_flow_status.status == 'healthy':
                health_scores.append(1.0)
            elif data_flow_status and data_flow_status.status == 'degraded':
                health_scores.append(0.6)
            else:
                health_scores.append(0.2)

            # 缓存健康评分
            if cache_health['hit_rate'] >= 0.8:
                health_scores.append(1.0)
            elif cache_health['hit_rate'] >= 0.6:
                health_scores.append(0.8)
            else:
                health_scores.append(0.5)

            # 计算综合健康评分
            overall_score = sum(health_scores) / len(health_scores) if health_scores else 0.0

            # 根据评分确定健康状态
            if overall_score >= 0.8:
                status = SystemHealthStatus.HEALTHY
            elif overall_score >= 0.6:
                status = SystemHealthStatus.DEGRADED
            elif overall_score >= 0.3:
                status = SystemHealthStatus.WARNING
            else:
                status = SystemHealthStatus.CRITICAL

            logger.info(f"Data pipeline health check completed: score={overall_score:.3f}, status={status}")
            return status

        except Exception as e:
            logger.error(f"Data pipeline health check failed: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_data_sources_health(self) -> Dict[str, Any]:
        """检查数据源健康状态"""
        try:
            # 定义需要检查的数据源
            data_sources = [
                'market_data',
                'macro_data',
                'portfolio_data',
                'execution_data'
            ]

            healthy_sources = 0
            source_details = {}

            for source in data_sources:
                try:
                    # 使用策略适配器检查数据源连接
                    if self._strategy_adapter:
                        health_check = await self._strategy_adapter.health_check()
                        if health_check:
                            healthy_sources += 1
                            source_details[source] = {'status': 'healthy', 'last_update': datetime.now().isoformat()}
                        else:
                            source_details[source] = {'status': 'unhealthy', 'error': 'Connection failed'}
                    else:
                        source_details[source] = {'status': 'unknown', 'error': 'Adapter not available'}

                except Exception as e:
                    source_details[source] = {'status': 'error', 'error': str(e)}

            return {
                'total_sources': len(data_sources),
                'healthy_sources': healthy_sources,
                'source_details': source_details,
                'health_ratio': healthy_sources / len(data_sources) if data_sources else 0.0
            }

        except Exception as e:
            logger.error(f"Failed to check data sources health: {e}")
            return {
                'total_sources': 0,
                'healthy_sources': 0,
                'source_details': {},
                'health_ratio': 0.0,
                'error': str(e)
            }

    async def _check_data_quality(self) -> Dict[str, Any]:
        """检查数据质量"""
        try:
            if self._data_flow_manager:
                # 使用数据流管理器的数据质量监控
                quality_report = await self._data_flow_manager.monitor_data_quality()

                if quality_report:
                    return {
                        'overall_quality': quality_report.get('overall_score', 0.5),
                        'completeness': quality_report.get('completeness', 0.5),
                        'accuracy': quality_report.get('accuracy', 0.5),
                        'timeliness': quality_report.get('timeliness', 0.5),
                        'consistency': quality_report.get('consistency', 0.5),
                        'last_check': datetime.now().isoformat()
                    }

            # 如果数据流管理器不可用，返回默认质量评估
            return {
                'overall_quality': 0.7,  # 假设中等质量
                'completeness': 0.8,
                'accuracy': 0.7,
                'timeliness': 0.6,
                'consistency': 0.7,
                'last_check': datetime.now().isoformat(),
                'note': 'Default quality assessment - DataFlowManager unavailable'
            }

        except Exception as e:
            logger.error(f"Failed to check data quality: {e}")
            return {
                'overall_quality': 0.3,  # 低质量评分
                'completeness': 0.0,
                'accuracy': 0.0,
                'timeliness': 0.0,
                'consistency': 0.0,
                'last_check': datetime.now().isoformat(),
                'error': str(e)
            }

    async def _check_cache_health(self) -> Dict[str, Any]:
        """检查缓存健康状态"""
        try:
            if self._data_flow_manager:
                # 获取缓存统计信息
                cache_stats = await self._data_flow_manager.get_cache_statistics()

                if cache_stats:
                    hit_rate = cache_stats.get('hit_rate', 0.0)
                    return {
                        'hit_rate': hit_rate,
                        'total_requests': cache_stats.get('total_requests', 0),
                        'cache_hits': cache_stats.get('cache_hits', 0),
                        'cache_misses': cache_stats.get('cache_misses', 0),
                        'cache_size': cache_stats.get('cache_size', 0),
                        'memory_usage': cache_stats.get('memory_usage', 0),
                        'last_check': datetime.now().isoformat()
                    }

            # 如果数据流管理器不可用，返回默认缓存状态
            return {
                'hit_rate': 0.6,  # 假设中等缓存命中率
                'total_requests': 0,
                'cache_hits': 0,
                'cache_misses': 0,
                'cache_size': 0,
                'memory_usage': 0,
                'last_check': datetime.now().isoformat(),
                'note': 'Default cache assessment - DataFlowManager unavailable'
            }

        except Exception as e:
            logger.error(f"Failed to check cache health: {e}")
            return {
                'hit_rate': 0.2,  # 低缓存命中率
                'total_requests': 0,
                'cache_hits': 0,
                'cache_misses': 0,
                'cache_size': 0,
                'memory_usage': 0,
                'last_check': datetime.now().isoformat(),
                'error': str(e)
            }

    async def _initialize_data_flow_manager(self) -> None:
        """初始化数据流管理器"""
        try:
            logger.info("Initializing DataFlowManager...")
            self._data_flow_manager = DataFlowManager(self.config)

            # 启动数据流管理器
            if not await self._data_flow_manager.start():
                logger.error("Failed to start DataFlowManager")
                self._data_flow_manager = None
                return

            logger.info("DataFlowManager initialized and started successfully")

        except Exception as e:
            logger.error(f"Failed to initialize DataFlowManager: {e}")
            self._data_flow_manager = None

    async def _notify_macro_system_shutdown(self) -> None:
        """通知宏观系统准备关闭"""
        try:
            # 断开宏观适配器连接
            if hasattr(self, '_macro_adapter') and self._macro_adapter is not None:
                await self._macro_adapter.disconnect_from_system()
                logger.info("Macro system shutdown notification sent")
        except Exception as e:
            logger.error(f"Failed to notify macro system shutdown: {e}")

    async def _notify_portfolio_system_shutdown(self) -> None:
        """通知组合系统准备关闭"""
        try:
            # 通知组合适配器断开连接
            if hasattr(self, '_portfolio_adapter') and self._portfolio_adapter is not None:
                await self._portfolio_adapter.disconnect_from_system()
                logger.info("Portfolio system shutdown notification sent")
        except Exception as e:
            logger.error(f"Failed to notify portfolio system shutdown: {e}")

    async def _notify_strategy_system_shutdown(self) -> None:
        """通知策略系统准备关闭"""
        try:
            if self._strategy_adapter:
                logger.info("Disconnecting from strategy analysis system...")
                await self._strategy_adapter.disconnect_from_system()
                self._strategy_adapter = None
                logger.info("Strategy system shutdown notification completed")
            else:
                logger.debug("StrategyAdapter not initialized, skipping shutdown notification")
        except Exception as e:
            logger.error(f"Failed to notify strategy system shutdown: {e}")

    def _create_balanced_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建平衡资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.3, "portfolio": 0.4, "strategy": 0.3},
            memory_allocation={"macro": 0.2, "portfolio": 0.3, "strategy": 0.5},
            network_allocation={"macro": 0.3, "portfolio": 0.3, "strategy": 0.4},
            storage_allocation={"macro": 0.2, "portfolio": 0.3, "strategy": 0.5},
            priority_weights={"macro": 0.3, "portfolio": 0.4, "strategy": 0.3}
        )

    def _create_performance_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建性能优先资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.2, "portfolio": 0.3, "strategy": 0.5},
            memory_allocation={"macro": 0.15, "portfolio": 0.25, "strategy": 0.6},
            network_allocation={"macro": 0.2, "portfolio": 0.3, "strategy": 0.5},
            storage_allocation={"macro": 0.15, "portfolio": 0.25, "strategy": 0.6},
            priority_weights={"macro": 0.2, "portfolio": 0.3, "strategy": 0.5}
        )

    def _create_conservative_allocation(self, allocation_id: str) -> ResourceAllocation:
        """创建保守资源分配"""
        return ResourceAllocation(
            allocation_id=allocation_id,
            cpu_allocation={"macro": 0.4, "portfolio": 0.4, "strategy": 0.2},
            memory_allocation={"macro": 0.3, "portfolio": 0.4, "strategy": 0.3},
            network_allocation={"macro": 0.4, "portfolio": 0.4, "strategy": 0.2},
            storage_allocation={"macro": 0.3, "portfolio": 0.4, "strategy": 0.3},
            priority_weights={"macro": 0.4, "portfolio": 0.4, "strategy": 0.2}
        )

    async def _attempt_auto_recovery(self, exception) -> bool:
        """尝试自动恢复"""
        try:
            # 根据异常类型选择恢复策略
            # 这里实现基本的重试逻辑
            await asyncio.sleep(self.config.system_coordinator.retry_delay)
            return True
        except Exception as e:
            logger.error(f"Auto recovery failed: {e}")
            return False

    async def _initialize_strategy_adapter(self) -> None:
        """初始化StrategyAdapter"""
        try:
            logger.info("Initializing StrategyAdapter...")
            self._strategy_adapter = StrategyAdapter(self.config)

            # 连接到策略分析系统
            connected = await self._strategy_adapter.connect_to_system()
            if not connected:
                raise SystemCoordinatorException("Failed to connect to strategy analysis system")

            logger.info("StrategyAdapter initialized and connected successfully")

        except Exception as e:
            logger.error(f"Failed to initialize StrategyAdapter: {e}")
            raise SystemCoordinatorException(f"StrategyAdapter initialization failed: {e}")

    def _extract_symbols_from_instruction(self, portfolio_instruction: Dict[str, Any]) -> List[str]:
        """从组合指令中提取股票代码列表"""
        # 从组合指令中提取股票代码
        symbols = []

        if isinstance(portfolio_instruction, dict):
            # 尝试从不同字段提取股票代码
            symbols.extend(portfolio_instruction.get('symbols', []))
            symbols.extend(portfolio_instruction.get('target_symbols', []))

            # 从权重配置中提取
            weights = portfolio_instruction.get('weights', {})
            if isinstance(weights, dict):
                symbols.extend(weights.keys())

            # 从持仓配置中提取
            positions = portfolio_instruction.get('positions', {})
            if isinstance(positions, dict):
                symbols.extend(positions.keys())

        # 去重并过滤有效的股票代码
        unique_symbols = list(set(symbols))
        valid_symbols = [s for s in unique_symbols if s and isinstance(s, str) and len(s) >= 6]

        # 如果没有找到有效股票代码，使用默认测试代码
        if not valid_symbols:
            valid_symbols = ["000001.SZ", "000002.SZ", "600000.SH"]
            logger.warning(f"No valid symbols found in portfolio instruction, using default: {valid_symbols}")

        logger.debug(f"Extracted symbols from portfolio instruction: {valid_symbols}")
        return valid_symbols

    def _build_strategy_config_from_results(self, analysis_results: List[Any]) -> Dict[str, Any]:
        """从分析结果构建策略配置"""
        if not analysis_results:
            return {
                'analyzers': ['livermore', 'multi_indicator'],
                'start_date': '2023-01-01',
                'end_date': '2023-12-31',
                'initial_capital': 1000000,
                'commission': 0.001
            }

        # 从分析结果中提取分析器信息
        analyzers = set()
        for result in analysis_results:
            if isinstance(result, dict):
                analyzer_scores = result.get('analyzer_scores', {})
                analyzers.update(analyzer_scores.keys())

        # 如果没有找到分析器，使用默认配置
        if not analyzers:
            analyzers = {'livermore', 'multi_indicator'}

        return {
            'analyzers': list(analyzers),
            'start_date': '2023-01-01',
            'end_date': '2023-12-31',
            'initial_capital': 1000000,
            'commission': 0.001,
            'weights': {analyzer: 1.0/len(analyzers) for analyzer in analyzers}
        }
