"""
数据流管理器实现

负责统一管理三层系统的数据流、缓存策略和质量控制，
确保数据的高效传输和可靠性。
"""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict, deque
import threading

from interfaces import IDataFlowManager
from models import DataFlowStatus
from config import IntegrationConfig
from exceptions import DataFlowException, DataQualityException, CacheException
from managers.cache_manager import CacheManager

logger = logging.getLogger(__name__)


class DataFlowManager(IDataFlowManager):
    """数据流管理器实现类

    统一管理三层系统的数据流、缓存策略和质量控制，
    确保数据传输的高效性和可靠性。
    """

    def __init__(self, config: IntegrationConfig):
        """初始化数据流管理器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._is_running = False

        # 数据管道状态
        self._pipeline_status = DataFlowStatus(
            pipeline_id=str(uuid.uuid4()),
            status="stopped",
            data_sources=[],
            data_quality_score=0.0,
            throughput=0.0,
            latency=0.0,
            cache_hit_rate=0.0,
            error_rate=0.0,
            last_update_time=datetime.now()
        )

        # 数据源管理
        self._data_sources: Dict[str, Dict[str, Any]] = {}
        self._data_dependencies: Dict[str, Set[str]] = defaultdict(set)

        # 数据质量监控
        self._quality_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._quality_thresholds = {
            'completeness': 0.95,
            'accuracy': 0.90,
            'timeliness': 0.85,
            'consistency': 0.90
        }

        # 缓存管理器
        self._cache_manager = CacheManager(config)

        # 数据流统计
        self._flow_statistics = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_bytes_transferred': 0,
            'average_latency': 0.0
        }

        # 锁和同步
        self._flow_lock = threading.RLock()

        # 监控任务
        self._monitoring_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None

        logger.info("DataFlowManager initialized")

    async def start(self) -> bool:
        """启动数据流管理器

        Returns:
            bool: 启动是否成功
        """
        try:
            if self._is_running:
                logger.warning("DataFlowManager is already running")
                return True

            # 启动缓存管理器
            await self._cache_manager.start()

            # 初始化数据源
            await self._initialize_data_sources()

            # 启动监控任务
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

            # 更新状态
            self._pipeline_status.status = "running"
            self._pipeline_status.last_update_time = datetime.now()

            self._is_running = True
            logger.info("DataFlowManager started successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to start DataFlowManager: {e}")
            return False

    async def stop(self) -> bool:
        """停止数据流管理器

        Returns:
            bool: 停止是否成功
        """
        try:
            if not self._is_running:
                return True

            logger.info("Stopping DataFlowManager...")
            self._is_running = False

            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
            if self._cleanup_task:
                self._cleanup_task.cancel()

            # 停止缓存管理器
            await self._cache_manager.stop()

            # 更新状态
            self._pipeline_status.status = "stopped"
            self._pipeline_status.last_update_time = datetime.now()

            logger.info("DataFlowManager stopped successfully")
            return True

        except Exception as e:
            logger.error(f"Error stopping DataFlowManager: {e}")
            return False

    async def orchestrate_data_pipeline(self) -> DataFlowStatus:
        """编排数据管道

        Returns:
            DataFlowStatus: 数据管道状态
        """
        try:
            if not self._is_running:
                raise DataFlowException("DataFlowManager is not running")
            if self._pipeline_status.status == "paused":
                return self._pipeline_status

            logger.debug("Orchestrating data pipeline...")

            # 检查数据源健康状态
            source_health = await self._check_data_sources_health()

            # 更新数据依赖关系
            await self._update_data_dependencies()

            # 优化数据流路径
            await self._optimize_data_flow_paths()

            # 执行数据质量检查
            quality_report = await self.monitor_data_quality()

            # 更新管道状态
            with self._flow_lock:
                previous_update_time = self._pipeline_status.last_update_time
                self._pipeline_status.data_quality_score = quality_report.get('overall_score', 0.0)
                self._pipeline_status.cache_hit_rate = self._cache_manager.get_hit_rate()
                current_time = datetime.now()

                # 计算吞吐量和延迟
                if self._flow_statistics['total_requests'] > 0:
                    elapsed = max((current_time - previous_update_time).total_seconds(), 1.0)
                    self._pipeline_status.throughput = (
                        self._flow_statistics['successful_requests'] /
                        elapsed
                    )
                    self._pipeline_status.latency = self._flow_statistics['average_latency']
                    self._pipeline_status.error_rate = (
                        self._flow_statistics['failed_requests'] /
                        self._flow_statistics['total_requests']
                    )

                # 根据当前指标更新健康状态
                if self._pipeline_status.error_rate <= 0.1 and self._pipeline_status.data_quality_score >= 0.8:
                    self._pipeline_status.status = 'healthy'
                elif self._pipeline_status.error_rate <= 0.2 and self._pipeline_status.data_quality_score >= 0.6:
                    self._pipeline_status.status = 'degraded'
                else:
                    self._pipeline_status.status = 'warning'
                self._pipeline_status.last_update_time = current_time

            logger.debug("Data pipeline orchestration completed")
            return self._pipeline_status

        except Exception as e:
            logger.error(f"Data pipeline orchestration failed: {e}")
            raise DataFlowException(f"Pipeline orchestration failed: {e}")

    async def manage_data_dependencies(self) -> Dict[str, Any]:
        """管理数据依赖关系

        Returns:
            Dict[str, Any]: 依赖关系映射
        """
        try:
            dependency_map = {}

            with self._flow_lock:
                for source, dependencies in self._data_dependencies.items():
                    dependency_map[source] = {
                        'dependencies': list(dependencies),
                        'dependency_count': len(dependencies),
                        'is_critical': len(dependencies) > 3,
                        'last_updated': datetime.now().isoformat()
                    }

            # 检测循环依赖
            circular_dependencies = await self._detect_circular_dependencies()
            if circular_dependencies:
                logger.warning(f"Circular dependencies detected: {circular_dependencies}")
                dependency_map['circular_dependencies'] = circular_dependencies

            return dependency_map

        except Exception as e:
            logger.error(f"Data dependency management failed: {e}")
            raise DataFlowException(f"Dependency management failed: {e}")

    async def optimize_data_flow(self) -> Dict[str, Any]:
        """优化数据流

        Returns:
            Dict[str, Any]: 优化结果
        """
        try:
            optimization_results = {}

            # 缓存优化
            cache_optimization = await self._optimize_cache_strategy()
            optimization_results['cache_optimization'] = cache_optimization

            # 数据预加载优化
            preload_optimization = await self._optimize_data_preloading()
            optimization_results['preload_optimization'] = preload_optimization

            # 并发优化
            concurrency_optimization = await self._optimize_concurrency()
            optimization_results['concurrency_optimization'] = concurrency_optimization

            # 压缩优化
            if self.config.data_flow_manager.enable_data_compression:
                compression_optimization = await self._optimize_data_compression()
                optimization_results['compression_optimization'] = compression_optimization

            logger.info("Data flow optimization completed")
            return optimization_results

        except Exception as e:
            logger.error(f"Data flow optimization failed: {e}")
            raise DataFlowException(f"Flow optimization failed: {e}")

    async def manage_intelligent_cache(self) -> Dict[str, Any]:
        """管理智能缓存

        Returns:
            Dict[str, Any]: 缓存状态
        """
        try:
            return await self._cache_manager.get_cache_status()
        except Exception as e:
            logger.error(f"Cache management failed: {e}")
            raise CacheException("get_status", "all", str(e))

    async def monitor_data_quality(self) -> Dict[str, Any]:
        """监控数据质量

        Returns:
            Dict[str, Any]: 质量报告
        """
        try:
            quality_report = {
                'timestamp': datetime.now(),
                'overall_score': 0.0,
                'metrics': {},
                'issues': [],
                'recommendations': []
            }

            # 检查各个质量维度
            completeness_score = await self._check_data_completeness()
            accuracy_score = await self._check_data_accuracy()
            timeliness_score = await self._check_data_timeliness()
            consistency_score = await self._check_data_consistency()

            quality_report['metrics'] = {
                'completeness': completeness_score,
                'accuracy': accuracy_score,
                'timeliness': timeliness_score,
                'consistency': consistency_score
            }

            # 计算总体质量评分
            quality_report['overall_score'] = (
                completeness_score + accuracy_score +
                timeliness_score + consistency_score
            ) / 4

            # 检查质量阈值
            for metric, score in quality_report['metrics'].items():
                threshold = self._quality_thresholds.get(metric, 0.8)
                if score < threshold:
                    quality_report['issues'].append({
                        'metric': metric,
                        'score': score,
                        'threshold': threshold,
                        'severity': 'high' if score < threshold * 0.8 else 'medium'
                    })

            # 生成改进建议
            if quality_report['issues']:
                quality_report['recommendations'] = await self._generate_quality_recommendations(
                    quality_report['issues']
                )

            # 更新质量历史
            for metric, score in quality_report['metrics'].items():
                self._quality_metrics[metric].append({
                    'score': score,
                    'timestamp': datetime.now()
                })

            # 检查质量异常
            if quality_report['overall_score'] < self.config.data_flow_manager.data_quality_threshold:
                raise DataQualityException(
                    "overall",
                    quality_report['overall_score'],
                    self.config.data_flow_manager.data_quality_threshold
                )

            return quality_report

        except DataQualityException:
            raise
        except Exception as e:
            logger.error(f"Data quality monitoring failed: {e}")
            raise DataFlowException(f"Quality monitoring failed: {e}")

    def get_pipeline_status(self) -> DataFlowStatus:
        """获取管道状态

        Returns:
            DataFlowStatus: 当前管道状态
        """
        return self._pipeline_status

    def is_running(self) -> bool:
        """是否在运行"""
        return self._is_running

    async def get_data_flow_status(self) -> DataFlowStatus:
        """获取数据流状态（异步接口以兼容调用方）"""
        return self._pipeline_status

    async def get_cache_statistics(self) -> Dict[str, Any]:
        """获取缓存统计（异步接口以兼容调用方）"""
        return self._cache_manager.get_statistics()

    async def collect_performance_metrics(self) -> Dict[str, Any]:
        """兼容 HTTP 层的性能指标收集入口。"""
        status = await self.orchestrate_data_pipeline()
        return {
            'pipeline': {
                'pipeline_id': status.pipeline_id,
                'status': status.status,
                'throughput': status.throughput,
                'latency': status.latency,
                'error_rate': status.error_rate,
                'cache_hit_rate': status.cache_hit_rate,
                'data_quality_score': status.data_quality_score,
                'active_connections': status.active_connections,
                'processed_records': status.processed_records,
                'failed_records': status.failed_records,
                'last_update_time': status.last_update_time.isoformat(),
            },
            'flow_statistics': self.get_flow_statistics(),
        }

    async def clear_cache(self) -> Dict[str, Any]:
        """兼容 HTTP 层的缓存清理入口。"""
        cleared = await self._cache_manager.clear()
        return {
            'cleared': bool(cleared),
            'cache_statistics': await self.get_cache_statistics(),
        }

    async def trigger_data_sync(self, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """触发一次数据同步与状态刷新。"""
        payload = payload if isinstance(payload, dict) else {}
        targets = payload.get('sources') if isinstance(payload.get('sources'), list) else []
        now = datetime.now()

        with self._flow_lock:
            if targets:
                for source in targets:
                    if source in self._data_sources:
                        self._data_sources[source]['status'] = 'active'
                        self._data_sources[source]['last_update'] = now
            else:
                for source_info in self._data_sources.values():
                    source_info['status'] = 'active'
                    source_info['last_update'] = now

        status = await self.orchestrate_data_pipeline()
        return {
            'triggered': True,
            'targets': targets or list(self._data_sources.keys()),
            'status': status.status,
            'last_update_time': status.last_update_time.isoformat(),
        }

    async def list_dataflows(self) -> List[Dict[str, Any]]:
        """返回 Brain 当前维护的数据流视图。"""
        items = []
        with self._flow_lock:
            items.append(
                {
                    'flow_id': 'pipeline',
                    'type': 'pipeline',
                    'status': self._pipeline_status.status,
                    'data_sources': list(self._pipeline_status.data_sources),
                    'last_update_time': self._pipeline_status.last_update_time.isoformat(),
                }
            )
            for source_name, source_info in self._data_sources.items():
                last_update = source_info.get('last_update')
                items.append(
                    {
                        'flow_id': source_name,
                        'type': 'source',
                        'status': source_info.get('status', 'unknown'),
                        'health_score': source_info.get('health_score', 0.0),
                        'connection_count': source_info.get('connection_count', 0),
                        'last_update_time': last_update.isoformat() if hasattr(last_update, 'isoformat') else None,
                    }
                )
        return items

    async def control_dataflow(self, flow_id: Optional[str], action: Optional[str]) -> Dict[str, Any]:
        """执行最小可用的数据流控制动作。"""
        flow_id = str(flow_id or '').strip()
        action = str(action or '').strip().lower()
        if not flow_id:
            raise DataFlowException("Missing flow_id")
        if action not in {'pause', 'resume', 'start', 'stop', 'enable', 'disable', 'sync'}:
            raise DataFlowException(f"Unsupported dataflow action: {action}")

        if flow_id in {'pipeline', self._pipeline_status.pipeline_id}:
            if action == 'pause':
                self._pipeline_status.status = 'paused'
            elif action == 'resume':
                self._pipeline_status.status = 'running'
            elif action == 'start':
                await self.start()
            elif action == 'stop':
                await self.stop()
            elif action == 'sync':
                return await self.trigger_data_sync({})
            else:
                raise DataFlowException(f"Action '{action}' is not supported for pipeline")
            self._pipeline_status.last_update_time = datetime.now()
            return {
                'flow_id': flow_id,
                'action': action,
                'status': self._pipeline_status.status,
            }

        if flow_id not in self._data_sources:
            raise DataFlowException(f"Unknown dataflow: {flow_id}")

        if action in {'enable', 'start', 'resume', 'sync'}:
            self._data_sources[flow_id]['status'] = 'active'
            self._data_sources[flow_id]['last_update'] = datetime.now()
            if action == 'sync':
                return await self.trigger_data_sync({'sources': [flow_id]})
        elif action in {'disable', 'stop', 'pause'}:
            self._data_sources[flow_id]['status'] = 'inactive'

        return {
            'flow_id': flow_id,
            'action': action,
            'status': self._data_sources[flow_id]['status'],
        }

    async def delete_dataflow(self, flow_id: Optional[str]) -> Dict[str, Any]:
        """删除一个非核心数据源流。"""
        flow_id = str(flow_id or '').strip()
        if not flow_id:
            raise DataFlowException("Missing flow_id")
        if flow_id in {'pipeline', self._pipeline_status.pipeline_id}:
            raise DataFlowException("Pipeline flow cannot be deleted")
        if flow_id not in self._data_sources:
            raise DataFlowException(f"Unknown dataflow: {flow_id}")

        with self._flow_lock:
            self._data_sources.pop(flow_id, None)
            self._data_dependencies.pop(flow_id, None)
            for deps in self._data_dependencies.values():
                deps.discard(flow_id)
            self._pipeline_status.data_sources = list(self._data_sources.keys())

        return {
            'flow_id': flow_id,
            'deleted': True,
            'remaining': list(self._data_sources.keys()),
        }


    def get_flow_statistics(self) -> Dict[str, Any]:
        """获取数据流统计

        Returns:
            Dict[str, Any]: 流统计信息
        """
        with self._flow_lock:
            stats = self._flow_statistics.copy()
            stats['cache_statistics'] = self._cache_manager.get_statistics()
            stats['quality_history'] = {
                metric: list(history)[-10:]  # 最近10个记录
                for metric, history in self._quality_metrics.items()
            }
            return stats

    # 私有方法实现
    async def _initialize_data_sources(self) -> None:
        """初始化数据源"""
        # 注册默认数据源
        default_sources = [
            'macro_data_source',
            'market_data_source',
            'portfolio_data_source',
            'tactical_data_source'
        ]

        for source in default_sources:
            self._data_sources[source] = {
                'status': 'active',
                'last_update': datetime.now(),
                'health_score': 1.0,
                'connection_count': 0
            }

        self._pipeline_status.data_sources = list(self._data_sources.keys())

    async def _check_data_sources_health(self) -> Dict[str, float]:
        """检查数据源健康状态"""
        health_scores = {}
        now = datetime.now()
        ttl = max(self.config.data_flow_manager.cache_ttl, 1)

        for source_name, source_info in self._data_sources.items():
            try:
                status = source_info.get('status', 'inactive')
                last_update = source_info.get('last_update')
                if not last_update:
                    last_update = now
                age_seconds = (now - last_update).total_seconds()

                if status != 'active':
                    health_score = 0.0
                elif age_seconds <= ttl:
                    health_score = 1.0
                elif age_seconds <= ttl * 2:
                    health_score = 0.6
                else:
                    health_score = 0.2

                health_scores[source_name] = health_score
                source_info['health_score'] = health_score
                source_info['last_update'] = now
            except Exception as e:
                logger.warning(f"Health check failed for {source_name}: {e}")
                health_scores[source_name] = 0.0
                source_info['health_score'] = 0.0

        return health_scores

    async def _update_data_dependencies(self) -> None:
        """更新数据依赖关系"""
        # 定义数据依赖关系
        dependencies = {
            'portfolio_data': {'macro_data', 'market_data'},
            'tactical_data': {'portfolio_data', 'market_data'},
            'execution_data': {'tactical_data', 'market_data'},
            'performance_data': {'execution_data', 'portfolio_data'}
        }

        with self._flow_lock:
            self._data_dependencies.clear()
            for source, deps in dependencies.items():
                self._data_dependencies[source] = deps

    async def _optimize_data_flow_paths(self) -> None:
        """优化数据流路径"""
        try:
            logger.debug("Starting data flow path optimization...")

            with self._flow_lock:
                # 1. 构建依赖图
                dependency_graph = await self._build_dependency_graph()

                # 2. 检测循环依赖
                circular_deps = await self._detect_circular_dependencies()
                if circular_deps:
                    logger.warning(f"Detected circular dependencies: {circular_deps}")
                    await self._resolve_circular_dependencies(circular_deps)

                # 3. 计算最优执行顺序
                optimal_order = await self._calculate_optimal_execution_order(dependency_graph)

                # 4. 优化并行处理路径
                parallel_groups = await self._identify_parallel_processing_groups(dependency_graph, optimal_order)

                # 5. 优化缓存策略
                cache_optimization = await self._optimize_cache_placement(dependency_graph)

                # 6. 更新数据流配置
                await self._update_flow_configuration(optimal_order, parallel_groups, cache_optimization)

                # 7. 记录优化结果
                optimization_result = {
                    'execution_order': optimal_order,
                    'parallel_groups': parallel_groups,
                    'cache_strategy': cache_optimization,
                    'circular_dependencies_resolved': len(circular_deps),
                    'optimization_timestamp': datetime.now().isoformat()
                }

                self._flow_statistics['last_optimization'] = optimization_result
                logger.info(f"Data flow path optimization completed: {len(optimal_order)} nodes optimized")

        except Exception as e:
            logger.error(f"Data flow path optimization failed: {e}")
            raise DataFlowException(f"Path optimization failed: {e}")

    async def _detect_circular_dependencies(self) -> List[List[str]]:
        """检测循环依赖"""
        try:
            circular_deps = []
            visited = set()
            rec_stack = set()

            def dfs(node: str, path: List[str]) -> bool:
                """深度优先搜索检测循环"""
                if node in rec_stack:
                    # 找到循环，提取循环路径
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:] + [node]
                    circular_deps.append(cycle)
                    return True

                if node in visited:
                    return False

                visited.add(node)
                rec_stack.add(node)

                # 检查所有依赖
                dependencies = self._data_dependencies.get(node, set())
                for dep in dependencies:
                    if dfs(dep, path + [node]):
                        return True

                rec_stack.remove(node)
                return False

            # 对所有节点执行DFS
            for node in self._data_dependencies.keys():
                if node not in visited:
                    dfs(node, [])

            return circular_deps

        except Exception as e:
            logger.error(f"Circular dependency detection failed: {e}")
            return []

    async def _build_dependency_graph(self) -> Dict[str, Set[str]]:
        """构建依赖图"""
        # 返回当前的依赖关系图
        return dict(self._data_dependencies)

    async def _calculate_optimal_execution_order(self, dependency_graph: Dict[str, Set[str]]) -> List[str]:
        """计算最优执行顺序（拓扑排序）"""
        try:
            # 计算入度
            in_degree = defaultdict(int)
            all_nodes = set(dependency_graph.keys())

            # 添加所有被依赖的节点
            for deps in dependency_graph.values():
                all_nodes.update(deps)

            # 初始化入度
            for node in all_nodes:
                in_degree[node] = 0

            # 计算入度
            for node, deps in dependency_graph.items():
                for dep in deps:
                    in_degree[node] += 1

            # 拓扑排序
            queue = deque([node for node in all_nodes if in_degree[node] == 0])
            result = []

            while queue:
                node = queue.popleft()
                result.append(node)

                # 更新依赖此节点的其他节点的入度
                for dependent, deps in dependency_graph.items():
                    if node in deps:
                        in_degree[dependent] -= 1
                        if in_degree[dependent] == 0:
                            queue.append(dependent)

            return result

        except Exception as e:
            logger.error(f"Failed to calculate optimal execution order: {e}")
            return list(dependency_graph.keys())

    async def _identify_parallel_processing_groups(self, dependency_graph: Dict[str, Set[str]], execution_order: List[str]) -> List[List[str]]:
        """识别可并行处理的组"""
        try:
            parallel_groups = []
            processed = set()

            for node in execution_order:
                if node in processed:
                    continue

                # 找到所有可以与当前节点并行处理的节点
                parallel_group = [node]
                node_deps = dependency_graph.get(node, set())

                for other_node in execution_order:
                    if other_node in processed or other_node == node:
                        continue

                    other_deps = dependency_graph.get(other_node, set())

                    # 检查是否可以并行：没有相互依赖关系
                    if (node not in other_deps and other_node not in node_deps and
                        not node_deps.intersection(other_deps)):
                        parallel_group.append(other_node)

                parallel_groups.append(parallel_group)
                processed.update(parallel_group)

            return parallel_groups

        except Exception as e:
            logger.error(f"Failed to identify parallel processing groups: {e}")
            return [[node] for node in execution_order]

    async def _optimize_cache_placement(self, dependency_graph: Dict[str, Set[str]]) -> Dict[str, Any]:
        """优化缓存放置策略"""
        try:
            cache_strategy = {}

            # 计算节点的访问频率（基于依赖关系）
            access_frequency = defaultdict(int)
            for node, deps in dependency_graph.items():
                for dep in deps:
                    access_frequency[dep] += 1

            # 根据访问频率确定缓存策略
            for node, frequency in access_frequency.items():
                if frequency >= 3:
                    cache_strategy[node] = {
                        'cache_enabled': True,
                        'cache_ttl': 300,  # 5分钟
                        'cache_size': 'large',
                        'priority': 'high'
                    }
                elif frequency >= 2:
                    cache_strategy[node] = {
                        'cache_enabled': True,
                        'cache_ttl': 180,  # 3分钟
                        'cache_size': 'medium',
                        'priority': 'medium'
                    }
                else:
                    cache_strategy[node] = {
                        'cache_enabled': True,
                        'cache_ttl': 60,   # 1分钟
                        'cache_size': 'small',
                        'priority': 'low'
                    }

            return cache_strategy

        except Exception as e:
            logger.error(f"Failed to optimize cache placement: {e}")
            return {}

    async def _resolve_circular_dependencies(self, circular_deps: List[List[str]]) -> None:
        """解决循环依赖"""
        try:
            for cycle in circular_deps:
                logger.warning(f"Resolving circular dependency: {' -> '.join(cycle)}")

                # 简单策略：移除循环中最后一个依赖关系
                if len(cycle) >= 2:
                    source = cycle[-2]
                    target = cycle[-1]

                    if source in self._data_dependencies and target in self._data_dependencies[source]:
                        self._data_dependencies[source].remove(target)
                        logger.info(f"Removed dependency: {source} -> {target}")

        except Exception as e:
            logger.error(f"Failed to resolve circular dependencies: {e}")

    async def _update_flow_configuration(self, execution_order: List[str], parallel_groups: List[List[str]], cache_strategy: Dict[str, Any]) -> None:
        """更新数据流配置"""
        try:
            # 更新执行顺序配置
            self._flow_statistics['execution_order'] = execution_order
            self._flow_statistics['parallel_groups'] = parallel_groups
            self._flow_statistics['cache_strategy'] = cache_strategy

            # 应用缓存策略
            for node, strategy in cache_strategy.items():
                await self._cache_manager.update_cache_policy(node, strategy)

            logger.info("Data flow configuration updated successfully")

        except Exception as e:
            logger.error(f"Failed to update flow configuration: {e}")

    async def _optimize_cache_strategy(self) -> Dict[str, Any]:
        """优化缓存策略"""
        return await self._cache_manager.optimize_cache_strategy()

    async def _optimize_data_preloading(self) -> Dict[str, Any]:
        """优化数据预加载"""
        cache_stats = self._cache_manager.get_statistics()
        max_size_mb = self.config.data_flow_manager.cache_size_mb
        hit_rate = cache_stats.get('hit_rate', 0.0)
        preload_enabled = hit_rate < 0.85
        preload_size_mb = max(10.0, min(max_size_mb * 0.1, max_size_mb))
        return {
            'preload_enabled': preload_enabled,
            'preload_size': f"{preload_size_mb:.1f}MB",
            'preload_hit_rate': hit_rate
        }

    async def _optimize_concurrency(self) -> Dict[str, Any]:
        """优化并发处理"""
        max_concurrent = self.config.data_flow_manager.max_concurrent_requests
        current_concurrent = self._flow_statistics.get('active_requests', 0)
        return {
            'max_concurrent_requests': max_concurrent,
            'current_concurrent': current_concurrent,
            'optimization_applied': current_concurrent <= max_concurrent
        }

    async def _optimize_data_compression(self) -> Dict[str, Any]:
        """优化数据压缩"""
        hit_rate = self._cache_manager.get_hit_rate()
        compression_ratio = 0.6 if hit_rate < 0.5 else 0.7
        return {
            'compression_enabled': self.config.data_flow_manager.enable_data_compression,
            'compression_ratio': compression_ratio,
            'compression_algorithm': 'gzip'
        }

    async def _check_data_completeness(self) -> float:
        """检查数据完整性"""
        if not self._data_sources:
            return 0.0
        scores = [info.get('health_score', 0.0) for info in self._data_sources.values()]
        return sum(scores) / len(scores)

    async def _check_data_accuracy(self) -> float:
        """检查数据准确性"""
        total = self._flow_statistics.get('total_requests', 0)
        failed = self._flow_statistics.get('failed_requests', 0)
        if total <= 0:
            return 1.0
        error_rate = failed / total
        return max(0.0, min(1.0, 1.0 - error_rate))

    async def _check_data_timeliness(self) -> float:
        """检查数据及时性"""
        if not self._data_sources:
            return 0.0
        now = datetime.now()
        ttl = max(self.config.data_flow_manager.cache_ttl, 1)
        freshness_scores = []
        for info in self._data_sources.values():
            last_update = info.get('last_update')
            if not last_update:
                freshness_scores.append(0.0)
                continue
            age = (now - last_update).total_seconds()
            freshness = max(0.0, 1.0 - min(age / ttl, 1.0))
            freshness_scores.append(freshness)
        return sum(freshness_scores) / len(freshness_scores)

    async def _check_data_consistency(self) -> float:
        """检查数据一致性"""
        if not self._data_sources:
            return 0.0
        scores = [info.get('health_score', 0.0) for info in self._data_sources.values()]
        max_score = max(scores)
        min_score = min(scores)
        if max_score <= 0:
            return 0.0
        return min_score / max_score

    async def _generate_quality_recommendations(self, issues: List[Dict[str, Any]]) -> List[str]:
        """生成质量改进建议"""
        recommendations = []

        for issue in issues:
            metric = issue['metric']
            if metric == 'completeness':
                recommendations.append("增加数据源冗余，提高数据完整性")
            elif metric == 'accuracy':
                recommendations.append("加强数据验证规则，提高数据准确性")
            elif metric == 'timeliness':
                recommendations.append("优化数据更新频率，提高数据及时性")
            elif metric == 'consistency':
                recommendations.append("统一数据格式标准，提高数据一致性")

        return recommendations

    async def _monitoring_loop(self) -> None:
        """监控循环"""
        interval = 30  # 30秒监控间隔

        while self._is_running:
            try:
                await self.orchestrate_data_pipeline()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(interval)

    async def _cleanup_loop(self) -> None:
        """清理循环"""
        interval = self.config.data_flow_manager.cleanup_interval

        while self._is_running:
            try:
                if self.config.data_flow_manager.enable_auto_cleanup:
                    await self._cleanup_expired_data()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")
                await asyncio.sleep(interval)

    async def _cleanup_expired_data(self) -> None:
        """清理过期数据"""
        # 清理过期的质量指标历史
        cutoff_time = datetime.now() - timedelta(hours=24)

        for metric_history in self._quality_metrics.values():
            while (metric_history and
                   metric_history[0]['timestamp'] < cutoff_time):
                metric_history.popleft()

        # 清理缓存中的过期数据
        await self._cache_manager.cleanup_expired_data()
