"""
集成层数据模型定义

定义系统协调、信号路由、验证等过程中使用的标准化数据模型，
确保各组件之间的数据交换标准化和类型安全。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from enum import Enum


class EventType(Enum):
    """事件类型枚举"""
    MACRO_DATA_UPDATE = "macro_data_update"
    PRICE_SIGNIFICANT_CHANGE = "price_significant_change"
    NEWS_EVENT = "news_event"
    POLICY_ANNOUNCEMENT = "policy_announcement"
    EARNINGS_RELEASE = "earnings_release"
    TECHNICAL_SIGNAL = "technical_signal"
    SYSTEM_EXCEPTION = "system_exception"


class SystemHealthStatus(Enum):
    """系统健康状态枚举"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    WARNING = "warning"
    CRITICAL = "critical"
    OFFLINE = "offline"


class ValidationStatus(Enum):
    """验证状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class MarketEvent:
    """市场事件数据模型"""
    event_id: str
    event_type: EventType
    timestamp: datetime
    symbols: List[str]
    data: Dict[str, Any]
    priority: int = 3  # 1-5, 5为最高优先级
    source: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """数据验证"""
        if not 1 <= self.priority <= 5:
            raise ValueError("Priority must be between 1 and 5")
        if not self.event_id:
            raise ValueError("Event ID cannot be empty")


@dataclass
class SystemStatus:
    """系统状态数据模型"""
    overall_health: SystemHealthStatus
    macro_system_status: SystemHealthStatus
    portfolio_system_status: SystemHealthStatus
    strategy_system_status: SystemHealthStatus
    tactical_system_status: SystemHealthStatus
    data_pipeline_status: SystemHealthStatus
    last_update_time: datetime
    active_sessions: int = 0
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    error_count: int = 0
    warning_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnalysisCycleResult:
    """分析周期结果数据模型"""
    cycle_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    macro_state: Optional[Any] = None
    portfolio_instruction: Optional[Any] = None
    analysis_results: List[Any] = field(default_factory=list)
    validation_result: Optional[Any] = None
    validation_results: List[Any] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    status: str = "pending"
    error_messages: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """计算执行时长（秒）"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def is_completed(self) -> bool:
        """检查是否完成"""
        return self.status == "completed" and self.end_time is not None

    @property
    def success(self) -> bool:
        """兼容旧调用方：completed 视为成功。"""
        return self.status == "completed"


@dataclass
class ValidationResult:
    """验证结果数据模型"""
    validation_id: str
    validation_type: str  # historical, forward, cross_layer
    validation_status: ValidationStatus
    validation_score: float  # 0-1
    confidence_adjustment: float  # -1 to 1
    risk_assessment: Dict[str, float] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)
    execution_time: float = 0.0  # 执行时间（秒）
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """数据验证"""
        if not 0 <= self.validation_score <= 1:
            raise ValueError("Validation score must be between 0 and 1")
        if not -1 <= self.confidence_adjustment <= 1:
            raise ValueError("Confidence adjustment must be between -1 and 1")


@dataclass
class SignalRoutingResult:
    """信号路由结果数据模型"""
    routing_id: str
    source_layer: str  # macro, portfolio, strategy
    target_layer: str
    original_signals: List[Any]
    routed_signals: List[Any]
    conflicts_detected: List[Dict[str, Any]] = field(default_factory=list)
    conflicts_resolved: List[Dict[str, Any]] = field(default_factory=list)
    routing_time: float = 0.0  # 路由时间（秒）
    success_rate: float = 1.0  # 成功率
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataFlowStatus:
    """数据流状态数据模型"""
    pipeline_id: str
    status: str  # running, completed, failed, paused
    data_sources: List[str]
    data_quality_score: float  # 0-1
    throughput: float  # 数据吞吐量
    latency: float  # 延迟（毫秒）
    cache_hit_rate: float  # 缓存命中率
    error_rate: float  # 错误率
    last_update_time: datetime
    active_connections: int = 0
    processed_records: int = 0
    failed_records: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResourceAllocation:
    """资源分配数据模型"""
    allocation_id: str
    cpu_allocation: Dict[str, float]  # 各组件CPU分配
    memory_allocation: Dict[str, float]  # 各组件内存分配
    network_allocation: Dict[str, float]  # 各组件网络带宽分配
    storage_allocation: Dict[str, float]  # 各组件存储分配
    priority_weights: Dict[str, float]  # 优先级权重
    strategy: str = "balanced"
    is_active: bool = True
    allocation_time: datetime = field(default_factory=datetime.now)
    effectiveness_score: float = 0.0  # 分配效果评分
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def created_time(self) -> datetime:
        """兼容旧调用方的别名。"""
        return self.allocation_time


@dataclass
class ConflictResolution:
    """冲突解决数据模型"""
    resolution_id: str
    conflict_type: str
    conflict_description: str
    resolution_strategy: str
    resolution_result: str  # success, partial, failed
    affected_components: List[str]
    resolution_time: float = 0.0  # 解决时间（秒）
    timestamp: datetime = field(default_factory=datetime.now)
    follow_up_actions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """性能指标数据模型"""
    metric_id: str
    component_name: str
    cpu_usage: float  # CPU使用率 (0-1)
    memory_usage: float  # 内存使用率 (0-1)
    response_time: float  # 响应时间（毫秒）
    throughput: float  # 吞吐量
    error_rate: float  # 错误率 (0-1)
    availability: float  # 可用性 (0-1)
    timestamp: datetime = field(default_factory=datetime.now)
    additional_metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class AlertInfo:
    """告警信息数据模型"""
    alert_id: str
    alert_level: str  # P1, P2, P3, P4
    alert_type: str
    component: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)
    is_resolved: bool = False
    resolution_time: Optional[datetime] = None
    resolution_notes: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """计算告警持续时间（秒）"""
        if self.resolution_time:
            return (self.resolution_time - self.timestamp).total_seconds()
        return None


# 标准化信号数据模型
@dataclass
class StandardSignal:
    """标准化信号数据模型"""
    signal_id: str
    signal_type: str
    source: str
    target: str
    strength: float  # 信号强度 (0-1)
    confidence: float  # 置信度 (0-1)
    timestamp: datetime = field(default_factory=datetime.now)
    expiry_time: Optional[datetime] = None
    priority: int = 3  # 1-5
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """数据验证"""
        if not 0 <= self.strength <= 1:
            raise ValueError("Signal strength must be between 0 and 1")
        if not 0 <= self.confidence <= 1:
            raise ValueError("Signal confidence must be between 0 and 1")
        if not 1 <= self.priority <= 5:
            raise ValueError("Signal priority must be between 1 and 5")
    
    @property
    def is_expired(self) -> bool:
        """检查信号是否过期"""
        if self.expiry_time:
            return datetime.now() > self.expiry_time
        return False
