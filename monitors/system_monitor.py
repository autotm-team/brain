"""
系统监控器实现

负责监控三层系统的健康状态、性能指标和异常情况。
"""

import asyncio
import logging
import os
import resource
import shutil
from collections import deque, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from dataclasses import asdict

from models import SystemStatus, PerformanceMetrics, AlertInfo, SystemHealthStatus
from config import IntegrationConfig
from exceptions import MonitoringException

logger = logging.getLogger(__name__)


class SystemMonitor:
    """系统监控器实现类
    
    监控三层系统的健康状态、性能指标和异常情况，
    提供实时监控和历史数据分析功能。
    """
    
    def __init__(self, config: IntegrationConfig):
        """初始化系统监控器
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False
        
        # 监控数据存储
        self._performance_history: deque = deque(maxlen=1000)
        self._health_history: deque = deque(maxlen=100)
        self._alert_history: deque = deque(maxlen=500)
        self._alert_rules: List[Dict[str, Any]] = []
        
        # 当前状态
        self._current_system_status = SystemStatus(
            overall_health=SystemHealthStatus.HEALTHY,
            macro_system_status=SystemHealthStatus.HEALTHY,
            portfolio_system_status=SystemHealthStatus.HEALTHY,
            strategy_system_status=SystemHealthStatus.HEALTHY,
            tactical_system_status=SystemHealthStatus.HEALTHY,
            data_pipeline_status=SystemHealthStatus.HEALTHY,
            last_update_time=datetime.now()
        )
        
        # 性能阈值
        self._performance_thresholds = self.config.monitoring.performance_threshold
        
        # 监控统计
        self._monitoring_statistics = {
            'total_checks': 0,
            'health_issues_detected': 0,
            'performance_alerts': 0,
            'system_restarts': 0,
            'uptime_start': datetime.now()
        }
        
        # 组件监控状态
        self._component_status = {
            'system_coordinator': {'status': 'unknown', 'last_check': None},
            'signal_router': {'status': 'unknown', 'last_check': None},
            'data_flow_manager': {'status': 'unknown', 'last_check': None},
            'macro_adapter': {'status': 'unknown', 'last_check': None},
            'portfolio_adapter': {'status': 'unknown', 'last_check': None},
            'strategy_adapter': {'status': 'unknown', 'last_check': None}
        }
        
        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        self._performance_task: Optional[asyncio.Task] = None

        # 组件引用（在启动时设置）
        self._system_coordinator = None
        self._signal_router = None
        self._data_flow_manager = None
        self._strategy_adapter = None
        self._portfolio_adapter = None
        self._macro_adapter = None

        logger.info("SystemMonitor initialized")

    def set_component_references(self, **components):
        """设置组件引用

        Args:
            **components: 组件实例的关键字参数
        """
        self._system_coordinator = components.get('system_coordinator')
        self._signal_router = components.get('signal_router')
        self._data_flow_manager = components.get('data_flow_manager')
        self._strategy_adapter = components.get('strategy_adapter')
        self._portfolio_adapter = components.get('portfolio_adapter')
        self._macro_adapter = components.get('macro_adapter')

        logger.info("Component references set for SystemMonitor")

    async def start(self) -> bool:
        """启动系统监控器
        
        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("SystemMonitor is already running")
                return True
            
            if not self.config.monitoring.enable_system_monitoring:
                logger.info("System monitoring is disabled in config")
                return True
            
            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            
            if self.config.monitoring.enable_performance_monitoring:
                self._performance_task = asyncio.create_task(self._performance_monitoring_loop())
            
            self._is_running = True
            self._monitoring_statistics['uptime_start'] = datetime.now()
            
            logger.info("SystemMonitor started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start SystemMonitor: {e}")
            return False
    
    async def stop(self) -> bool:
        """停止系统监控器
        
        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True
            
            logger.info("Stopping SystemMonitor...")
            self._is_running = False
            
            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass
            
            if self._performance_task:
                self._performance_task.cancel()
                try:
                    await self._performance_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("SystemMonitor stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping SystemMonitor: {e}")
            return False
    
    async def check_system_health(self) -> SystemStatus:
        """检查系统健康状态
        
        Returns:
            SystemStatus: 系统健康状态
        """
        try:
            current_time = datetime.now()
            
            # 检查各组件健康状态
            component_health = await self._check_all_components_health()
            
            # 更新系统状态
            self._current_system_status.macro_system_status = component_health.get('macro_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.portfolio_system_status = component_health.get('portfolio_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.strategy_system_status = component_health.get('strategy_adapter', SystemHealthStatus.CRITICAL)
            self._current_system_status.data_pipeline_status = component_health.get('data_flow_manager', SystemHealthStatus.CRITICAL)
            
            # 计算整体健康状态
            self._update_overall_health_status()
            
            # 更新时间戳
            self._current_system_status.last_update_time = current_time
            
            # 记录健康历史
            self._health_history.append({
                'timestamp': current_time,
                'overall_health': self._current_system_status.overall_health,
                'component_health': component_health
            })
            
            # 更新统计
            self._monitoring_statistics['total_checks'] += 1
            
            # 检查是否有健康问题
            if self._current_system_status.overall_health in [SystemHealthStatus.WARNING, SystemHealthStatus.CRITICAL]:
                self._monitoring_statistics['health_issues_detected'] += 1
            
            return self._current_system_status
            
        except Exception as e:
            logger.error(f"System health check failed: {e}")
            raise MonitoringException(f"Health check failed: {e}")
    
    async def collect_performance_metrics(self) -> Dict[str, Any]:
        """收集性能指标
        
        Returns:
            PerformanceMetrics: 性能指标
        """
        try:
            current_time = datetime.now()
            
            # 收集系统资源使用情况（优先使用系统数据）
            cpu_usage, memory_usage, disk_usage = self._get_system_resource_usage()
            
            # 应用层性能指标
            response_time = await self._measure_response_time()
            throughput = await self._measure_throughput()
            error_rate = await self._calculate_error_rate()
            availability = await self._calculate_availability()
            
            # 创建性能指标对象
            metrics = PerformanceMetrics(
                metric_id=f"metrics_{current_time.timestamp()}",
                component_name="integration_layer",
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                response_time=response_time,
                throughput=throughput,
                error_rate=error_rate,
                availability=availability,
                additional_metrics={
                    'disk_usage': disk_usage,
                    'active_connections': self._get_active_connections(),
                    'queue_sizes': self._get_queue_sizes()
                }
            )
            
            # 记录性能历史
            self._performance_history.append(metrics)
            
            # 检查性能阈值
            await self._check_performance_thresholds(metrics)
            
            return self._serialize_performance_metrics(metrics)
            
        except Exception as e:
            logger.error(f"Performance metrics collection failed: {e}")
            raise MonitoringException(f"Metrics collection failed: {e}")
    
    def get_system_status(self) -> SystemStatus:
        """获取当前系统状态
        
        Returns:
            SystemStatus: 当前系统状态
        """
        return self._current_system_status

    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """获取未解决告警"""
        return [
            self._serialize_alert(alert)
            for alert in self._alert_history
            if not alert.is_resolved
        ]

    def get_alert_rules(self) -> List[Dict[str, Any]]:
        """获取告警规则"""
        return list(self._alert_rules)

    def get_system_performance(self, hours: int = 1) -> Dict[str, Any]:
        """获取系统性能摘要"""
        history = self.get_performance_history(hours=hours)
        latest = history[-1] if history else None

        summary = {
            'latest': self._serialize_performance_metrics(latest) if latest else None,
            'history_size': len(history),
            'averages': {}
        }

        if history:
            summary['averages'] = {
                'cpu_usage': sum(m.cpu_usage for m in history) / len(history),
                'memory_usage': sum(m.memory_usage for m in history) / len(history),
                'response_time': sum(m.response_time for m in history) / len(history),
                'throughput': sum(m.throughput for m in history) / len(history),
                'error_rate': sum(m.error_rate for m in history) / len(history),
                'availability': sum(m.availability for m in history) / len(history)
            }

        return summary

    def acknowledge_alert(self, alert_id: str, notes: str = "") -> Dict[str, Any]:
        """确认告警"""
        for alert in self._alert_history:
            if alert.alert_id == alert_id:
                if not alert.is_resolved:
                    metadata = dict(alert.metadata or {})
                    metadata["acked"] = True
                    metadata["acknowledged"] = True
                    metadata["acknowledged_at"] = datetime.now().isoformat()
                    if notes:
                        metadata["acknowledge_notes"] = notes
                    alert.metadata = metadata
                    alert.resolution_notes = notes or alert.resolution_notes
                return self._serialize_alert(alert)
        raise MonitoringException(f"Alert not found: {alert_id}")

    def silence_alert(
        self,
        alert_id: str,
        reason: str = "",
        duration_minutes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """静默告警（不结束告警，仅标记为静默态）。"""
        for alert in self._alert_history:
            if alert.alert_id != alert_id:
                continue
            if alert.is_resolved:
                return self._serialize_alert(alert)

            metadata = dict(alert.metadata or {})
            metadata["silenced"] = True
            metadata["silence_reason"] = reason or "manual_silence"
            metadata["silenced_at"] = datetime.now().isoformat()
            if duration_minutes and duration_minutes > 0:
                metadata["silenced_until"] = (datetime.now() + timedelta(minutes=duration_minutes)).isoformat()
            alert.metadata = metadata
            return self._serialize_alert(alert)
        raise MonitoringException(f"Alert not found: {alert_id}")

    def set_alert_rule(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        """设置告警规则"""
        existing_rule = None
        rule_id = rule.get('rule_id') or rule.get('id')
        if rule_id:
            existing_rule = next((item for item in self._alert_rules if item.get('rule_id') == rule_id), None)
        if not rule_id:
            rule_id = f"rule_{datetime.now().timestamp()}"

        created_at = (
            existing_rule.get('created_at')
            if isinstance(existing_rule, dict) and existing_rule.get('created_at')
            else datetime.now().isoformat()
        )
        normalized = {
            'rule_id': rule_id,
            'name': rule.get('name'),
            'component': rule.get('component') or (existing_rule.get('component') if isinstance(existing_rule, dict) else None) or 'integration_layer',
            'condition': rule.get('condition'),
            'threshold': rule.get('threshold'),
            'alert_level': rule.get('alert_level') or (existing_rule.get('alert_level') if isinstance(existing_rule, dict) else None) or 'P2',
            'enabled': bool(rule.get('enabled', True)),
            'created_at': created_at,
            'updated_at': datetime.now().isoformat(),
            'metadata': rule.get('metadata', {}) if isinstance(rule.get('metadata'), dict) else {},
        }

        # 同步到性能阈值（若规则是标准指标）
        condition = normalized.get('condition')
        if condition in self._performance_thresholds and normalized.get('threshold') is not None:
            try:
                self._performance_thresholds[condition] = float(normalized['threshold'])
            except (TypeError, ValueError):
                pass

        if existing_rule is not None:
            idx = self._alert_rules.index(existing_rule)
            self._alert_rules[idx] = normalized
        else:
            self._alert_rules.append(normalized)
        return normalized
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """获取监控统计信息
        
        Returns:
            Dict[str, Any]: 监控统计
        """
        current_time = datetime.now()
        uptime = (current_time - self._monitoring_statistics['uptime_start']).total_seconds()
        
        stats = self._monitoring_statistics.copy()
        stats.update({
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'performance_history_size': len(self._performance_history),
            'health_history_size': len(self._health_history),
            'alert_history_size': len(self._alert_history),
            'component_status': self._component_status.copy()
        })
        
        return stats
    
    def get_performance_history(self, hours: int = 1) -> List[PerformanceMetrics]:
        """获取性能历史数据
        
        Args:
            hours: 获取最近几小时的数据
            
        Returns:
            List[PerformanceMetrics]: 性能历史数据
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            metrics for metrics in self._performance_history
            if metrics.timestamp >= cutoff_time
        ]
    
    def get_health_history(self, hours: int = 1) -> List[Dict[str, Any]]:
        """获取健康状态历史
        
        Args:
            hours: 获取最近几小时的数据
            
        Returns:
            List[Dict[str, Any]]: 健康状态历史
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)
        return [
            record for record in self._health_history
            if record['timestamp'] >= cutoff_time
        ]
    
    # 私有方法实现
    async def _monitoring_loop(self) -> None:
        """主监控循环"""
        interval = self.config.monitoring.monitoring_interval
        
        while self._is_running:
            try:
                # 执行健康检查
                await self.check_system_health()
                
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(interval)
    
    async def _performance_monitoring_loop(self) -> None:
        """性能监控循环"""
        interval = self.config.monitoring.monitoring_interval * 2  # 性能监控频率较低
        
        while self._is_running:
            try:
                # 收集性能指标
                await self.collect_performance_metrics()
                
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Performance monitoring loop error: {e}")
                await asyncio.sleep(interval)
    
    async def _check_all_components_health(self) -> Dict[str, SystemHealthStatus]:
        """检查所有组件健康状态"""
        component_health = {}
        current_time = datetime.now()
        
        for component_name in self._component_status.keys():
            try:
                # 执行真实的组件健康检查
                health_status = await self._check_component_health(component_name)
                component_health[component_name] = health_status
                
                # 更新组件状态
                self._component_status[component_name] = {
                    'status': health_status.value,
                    'last_check': current_time
                }
                
            except Exception as e:
                logger.warning(f"Health check failed for {component_name}: {e}")
                component_health[component_name] = SystemHealthStatus.CRITICAL
                self._component_status[component_name] = {
                    'status': 'error',
                    'last_check': current_time
                }
        
        return component_health
    
    async def _check_component_health(self, component_name: str) -> SystemHealthStatus:
        """检查单个组件健康状态"""
        try:
            logger.debug(f"Checking health of component: {component_name}")

            # 根据组件名称执行相应的健康检查
            if component_name == 'system_coordinator':
                return await self._check_system_coordinator_health()
            elif component_name == 'signal_router':
                return await self._check_signal_router_health()
            elif component_name == 'data_flow_manager':
                return await self._check_data_flow_manager_health()
            elif component_name == 'strategy_adapter':
                return await self._check_strategy_adapter_health()
            elif component_name == 'portfolio_adapter':
                return await self._check_portfolio_adapter_health()
            elif component_name == 'macro_adapter':
                return await self._check_macro_adapter_health()
            else:
                logger.warning(f"Unknown component: {component_name}")
                return SystemHealthStatus.WARNING

        except Exception as e:
            logger.error(f"Health check failed for component {component_name}: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_system_coordinator_health(self) -> SystemHealthStatus:
        """检查系统协调器健康状态"""
        try:
            if self._system_coordinator is None:
                return SystemHealthStatus.CRITICAL

            # 检查系统协调器是否运行
            if hasattr(self._system_coordinator, '_is_running') and not self._system_coordinator._is_running:
                return SystemHealthStatus.CRITICAL

            # 检查活跃周期数量
            if hasattr(self._system_coordinator, '_active_cycles'):
                active_cycles = len(self._system_coordinator._active_cycles)
                max_cycles = getattr(self._system_coordinator.config.system_coordinator, 'max_concurrent_cycles', 10)

                if active_cycles >= max_cycles:
                    return SystemHealthStatus.WARNING
                elif active_cycles > max_cycles * 0.8:
                    return SystemHealthStatus.DEGRADED

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"System coordinator health check failed: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_signal_router_health(self) -> SystemHealthStatus:
        """检查信号路由器健康状态"""
        try:
            if self._signal_router is None:
                return SystemHealthStatus.CRITICAL

            # 检查信号路由器是否运行
            if hasattr(self._signal_router, '_is_running') and not self._signal_router._is_running:
                return SystemHealthStatus.CRITICAL

            # 检查信号队列状态
            if hasattr(self._signal_router, '_signal_queues'):
                max_queue_size = getattr(self._signal_router.config.signal_router, 'max_signal_queue_size', 1000)
                for queue in self._signal_router._signal_queues.values():
                    queue_size = queue.qsize()
                    if queue_size >= max_queue_size:
                        return SystemHealthStatus.CRITICAL
                    if queue_size > max_queue_size * 0.8:
                        return SystemHealthStatus.WARNING

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Signal router health check failed: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_data_flow_manager_health(self) -> SystemHealthStatus:
        """检查数据流管理器健康状态"""
        try:
            if self._data_flow_manager is None:
                return SystemHealthStatus.CRITICAL

            # 检查数据流管理器是否运行
            if hasattr(self._data_flow_manager, 'is_running') and not self._data_flow_manager.is_running():
                return SystemHealthStatus.CRITICAL

            # 检查数据流状态
            if hasattr(self._data_flow_manager, 'get_data_flow_status'):
                try:
                    flow_status = await self._data_flow_manager.get_data_flow_status()
                    if flow_status:
                        if flow_status.status == 'healthy':
                            return SystemHealthStatus.HEALTHY
                        elif flow_status.status == 'degraded':
                            return SystemHealthStatus.DEGRADED
                        else:
                            return SystemHealthStatus.WARNING
                except Exception:
                    pass

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Data flow manager health check failed: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_strategy_adapter_health(self) -> SystemHealthStatus:
        """检查策略适配器健康状态"""
        try:
            if self._strategy_adapter is None:
                return SystemHealthStatus.CRITICAL

            # 检查适配器连接状态
            if hasattr(self._strategy_adapter, 'health_check'):
                health_ok = await self._strategy_adapter.health_check()
                if health_ok:
                    return SystemHealthStatus.HEALTHY
                else:
                    return SystemHealthStatus.CRITICAL

            # 检查连接状态
            if hasattr(self._strategy_adapter, '_is_connected') and not self._strategy_adapter._is_connected:
                return SystemHealthStatus.CRITICAL

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Strategy adapter health check failed: {e}")
            return SystemHealthStatus.CRITICAL

    async def _check_portfolio_adapter_health(self) -> SystemHealthStatus:
        """检查组合适配器健康状态"""
        try:
            if self._portfolio_adapter is None:
                return SystemHealthStatus.WARNING  # 组合适配器可能是可选的

            # 检查适配器连接状态
            if hasattr(self._portfolio_adapter, 'health_check'):
                health_ok = await self._portfolio_adapter.health_check()
                if health_ok:
                    return SystemHealthStatus.HEALTHY
                else:
                    return SystemHealthStatus.DEGRADED

            # 检查连接状态
            if hasattr(self._portfolio_adapter, '_is_connected') and not self._portfolio_adapter._is_connected:
                return SystemHealthStatus.DEGRADED

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Portfolio adapter health check failed: {e}")
            return SystemHealthStatus.DEGRADED

    async def _check_macro_adapter_health(self) -> SystemHealthStatus:
        """检查宏观适配器健康状态"""
        try:
            if self._macro_adapter is None:
                return SystemHealthStatus.WARNING  # 宏观适配器可能是可选的

            # 检查适配器连接状态
            if hasattr(self._macro_adapter, 'health_check'):
                health_ok = await self._macro_adapter.health_check()
                if health_ok:
                    return SystemHealthStatus.HEALTHY
                else:
                    return SystemHealthStatus.DEGRADED

            # 检查连接状态
            if hasattr(self._macro_adapter, '_is_connected') and not self._macro_adapter._is_connected:
                return SystemHealthStatus.DEGRADED

            return SystemHealthStatus.HEALTHY

        except Exception as e:
            logger.error(f"Macro adapter health check failed: {e}")
            return SystemHealthStatus.DEGRADED

    def _update_overall_health_status(self) -> None:
        """更新整体健康状态"""
        component_statuses = [
            self._current_system_status.macro_system_status,
            self._current_system_status.portfolio_system_status,
            self._current_system_status.strategy_system_status,
            self._current_system_status.tactical_system_status,
            self._current_system_status.data_pipeline_status
        ]
        
        # 如果有任何组件处于关键状态，整体状态为关键
        if any(status == SystemHealthStatus.CRITICAL for status in component_statuses):
            self._current_system_status.overall_health = SystemHealthStatus.CRITICAL
        # 如果有任何组件处于警告状态，整体状态为警告
        elif any(status == SystemHealthStatus.WARNING for status in component_statuses):
            self._current_system_status.overall_health = SystemHealthStatus.WARNING
        # 否则整体状态为健康
        else:
            self._current_system_status.overall_health = SystemHealthStatus.HEALTHY
    
    async def _measure_response_time(self) -> float:
        """测量响应时间"""
        if self._data_flow_manager and hasattr(self._data_flow_manager, 'get_pipeline_status'):
            status = self._data_flow_manager.get_pipeline_status()
            if status and status.latency:
                return float(status.latency)
        return 0.0
    
    async def _measure_throughput(self) -> float:
        """测量吞吐量"""
        if self._data_flow_manager and hasattr(self._data_flow_manager, 'get_pipeline_status'):
            status = self._data_flow_manager.get_pipeline_status()
            if status and status.throughput:
                return float(status.throughput)
        return 0.0
    
    async def _calculate_error_rate(self) -> float:
        """计算错误率"""
        if self._data_flow_manager and hasattr(self._data_flow_manager, 'get_pipeline_status'):
            status = self._data_flow_manager.get_pipeline_status()
            if status and status.error_rate is not None:
                return float(status.error_rate)
        return 0.0
    
    async def _calculate_availability(self) -> float:
        """计算可用性"""
        if self._current_system_status.overall_health == SystemHealthStatus.HEALTHY:
            return 1.0
        if self._current_system_status.overall_health == SystemHealthStatus.WARNING:
            return 0.995
        if self._current_system_status.overall_health == SystemHealthStatus.CRITICAL:
            return 0.95
        return 0.98
    
    def _get_active_connections(self) -> int:
        """获取活跃连接数"""
        if self._data_flow_manager and hasattr(self._data_flow_manager, 'get_pipeline_status'):
            status = self._data_flow_manager.get_pipeline_status()
            if status and status.active_connections is not None:
                return int(status.active_connections)
        return 0
    
    def _get_queue_sizes(self) -> Dict[str, int]:
        """获取队列大小"""
        queue_sizes: Dict[str, int] = {}
        if self._signal_router and hasattr(self._signal_router, '_signal_queues'):
            for name, queue in self._signal_router._signal_queues.items():
                try:
                    queue_sizes[name] = queue.qsize()
                except Exception:
                    queue_sizes[name] = 0
        return queue_sizes

    def _get_system_resource_usage(self) -> tuple[float, float, float]:
        """获取系统资源使用率（CPU/内存/磁盘）"""
        cpu_usage = 0.0
        memory_usage = 0.0
        disk_usage = 0.0

        try:
            cpu_count = os.cpu_count() or 1
            if hasattr(os, "getloadavg"):
                load1, _, _ = os.getloadavg()
                cpu_usage = min(1.0, float(load1) / float(cpu_count))

            # 内存使用率（优先取系统总量 + 进程占用）
            total_mem = None
            if hasattr(os, "sysconf"):
                try:
                    page_size = os.sysconf("SC_PAGE_SIZE")
                    phys_pages = os.sysconf("SC_PHYS_PAGES")
                    total_mem = float(page_size) * float(phys_pages)
                except (ValueError, OSError):
                    total_mem = None

            try:
                rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                rss_bytes = float(rss_kb) * 1024.0
                if total_mem and total_mem > 0:
                    memory_usage = min(1.0, rss_bytes / total_mem)
            except Exception:
                pass

            usage = shutil.disk_usage("/")
            if usage.total > 0:
                disk_usage = (usage.total - usage.free) / usage.total

        except Exception:
            pass

        return cpu_usage, memory_usage, disk_usage

    def _serialize_performance_metrics(self, metrics: Optional[PerformanceMetrics]) -> Optional[Dict[str, Any]]:
        if not metrics:
            return None
        data = asdict(metrics)
        data['timestamp'] = metrics.timestamp.isoformat()
        return data

    def _serialize_alert(self, alert: AlertInfo) -> Dict[str, Any]:
        data = asdict(alert)
        data['timestamp'] = alert.timestamp.isoformat()
        data['resolution_time'] = alert.resolution_time.isoformat() if alert.resolution_time else None
        data['duration'] = alert.duration
        return data
    
    async def _check_performance_thresholds(self, metrics: PerformanceMetrics) -> None:
        """检查性能阈值"""
        alerts = []
        
        # 检查CPU使用率
        if metrics.cpu_usage > self._performance_thresholds['cpu_usage']:
            alerts.append(f"High CPU usage: {metrics.cpu_usage:.1%}")
        
        # 检查内存使用率
        if metrics.memory_usage > self._performance_thresholds['memory_usage']:
            alerts.append(f"High memory usage: {metrics.memory_usage:.1%}")
        
        # 检查响应时间
        if metrics.response_time > self._performance_thresholds['response_time']:
            alerts.append(f"High response time: {metrics.response_time:.1f}ms")
        
        # 检查错误率
        if metrics.error_rate > self._performance_thresholds['error_rate']:
            alerts.append(f"High error rate: {metrics.error_rate:.1%}")
        
        # 生成告警
        for alert_message in alerts:
            alert = AlertInfo(
                alert_id=f"perf_alert_{datetime.now().timestamp()}",
                alert_level="P2",
                alert_type="performance",
                component="integration_layer",
                message=alert_message
            )
            self._alert_history.append(alert)
            self._monitoring_statistics['performance_alerts'] += 1
            
            logger.warning(f"Performance alert: {alert_message}")
