"""
集成层配置管理模块

提供集成层各组件的配置管理功能，支持多环境配置、
动态配置更新和配置验证。
"""

import os
import yaml
import json
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass, field
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class SystemCoordinatorConfig:
    """系统协调器配置"""
    max_concurrent_cycles: int = 3
    cycle_timeout: int = 300  # 秒
    strategy_analysis_timeout_seconds: int = 1800  # 秒
    strategy_validation_timeout_seconds: int = 1800  # 秒
    retry_attempts: int = 3
    retry_delay: float = 1.0  # 秒
    health_check_interval: int = 30  # 秒
    resource_allocation_strategy: str = "balanced"  # balanced, performance, conservative
    enable_auto_recovery: bool = True
    emergency_stop_threshold: float = 0.1  # 系统健康度阈值


@dataclass
class SignalRouterConfig:
    """信号路由器配置"""
    max_signal_queue_size: int = 1000
    signal_timeout: int = 60  # 秒
    conflict_resolution_strategy: str = "priority_based"  # priority_based, confidence_based, hybrid
    enable_signal_validation: bool = True
    signal_expiry_time: int = 300  # 秒
    batch_processing_size: int = 50
    enable_signal_compression: bool = True


@dataclass
class DataFlowManagerConfig:
    """数据流管理器配置"""
    cache_size_mb: int = 512
    cache_ttl: int = 300  # 秒
    max_concurrent_requests: int = 100
    request_timeout: int = 30  # 秒
    enable_data_compression: bool = True
    data_quality_threshold: float = 0.8
    enable_auto_cleanup: bool = True
    cleanup_interval: int = 3600  # 秒


@dataclass
class ValidationCoordinatorConfig:
    """验证协调器配置"""
    enable_dual_validation: bool = True
    validation_timeout: int = 120  # 秒
    confidence_threshold: float = 0.6
    enable_parallel_validation: bool = True
    max_validation_workers: int = 4
    validation_cache_size: int = 100
    enable_validation_learning: bool = True


@dataclass
class MonitoringConfig:
    """监控配置"""
    enable_system_monitoring: bool = True
    enable_performance_monitoring: bool = True
    enable_alerting: bool = True
    monitoring_interval: int = 10  # 秒
    alert_cooldown: int = 300  # 秒
    performance_threshold: Dict[str, float] = field(default_factory=lambda: {
        'cpu_usage': 0.8,
        'memory_usage': 0.8,
        'response_time': 1000,  # 毫秒
        'error_rate': 0.05
    })


@dataclass
class AdapterConfig:
    """适配器配置"""
    connection_timeout: int = 30  # 秒
    request_timeout: int = 60  # 秒
    max_retries: int = 3
    retry_delay: float = 1.0  # 秒
    enable_connection_pooling: bool = True
    pool_size: int = 10
    enable_health_check: bool = True
    health_check_interval: int = 60  # 秒
    default_portfolio_id: Optional[str] = None


@dataclass
class ServiceConfig:
    """微服务配置"""
    host: str = "0.0.0.0"
    port: int = 8088
    debug: bool = False

    # 服务发现配置
    macro_service_url: str = "http://macro:8080"
    portfolio_service_url: str = "http://portfolio:8080"
    execution_service_url: str = "http://execution:8087"
    flowhub_service_url: str = "http://flowhub:8080"

    # 定时任务配置
    scheduler_enabled: bool = True
    scheduler_timezone: str = "Asia/Shanghai"
    daily_data_fetch_cron: Optional[str] = "at:16:30"
    daily_index_fetch_retry_interval_seconds: int = 1800
    daily_index_fetch_retry_attempts: int = 3
    monthly_sw_industry_full_fetch_cron: Optional[str] = "at:16:30"
    daily_data_fetch_timeout: int = 86400
    strategy_plan_generation_cron: Optional[str] = "at:18:50"
    strategy_plan_generation_timeout: int = 7200
    strategy_plan_lookback_days: int = 180
    strategy_plan_initial_cash: float = 3000000.0
    strategy_plan_benchmark: str = "000300.SH"
    strategy_plan_cost: str = "5bp"

    # 监控配置
    monitoring_enabled: bool = True
    metrics_port: int = 9090

    # 启动数据初始化配置
    init_data_on_startup: bool = True
    init_wait_dependencies: List[str] = field(default_factory=lambda: ["flowhub"])
    init_max_history_first_run: bool = True
    init_retry: Dict[str, Any] = field(default_factory=lambda: {"max_retries": 10, "backoff": [1, 2, 3, 5, 8, 13], "timeout": 300})
    init_concurrency: int = 2

    # Auth 配置
    auth_issuer: str = "autotm-brain"
    auth_jwt_secret: str = "CHANGE_ME_AUTOTM_BRAIN_SECRET"
    auth_access_token_ttl_seconds: int = 900
    auth_refresh_token_ttl_seconds: int = 604800
    auth_admin_default_password: str = "admin123!"
    auth_lock_enabled: bool = False
    auth_lock_threshold: int = 5
    auth_lock_seconds: int = 600


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    file: str = "logs/integration.log"
    max_size: int = 10485760  # 10MB
    backup_count: int = 5
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class DatabaseConfig:
    """数据库配置"""
    url: str = "postgresql://postgres:password@localhost:5432/stock_data"
    pool_size: int = 20
    max_overflow: int = 30
    pool_timeout: int = 30
    pool_recycle: int = 3600


@dataclass
class RedisConfig:
    """Redis配置"""
    url: str = "redis://localhost:6379/0"
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str = None
    max_connections: int = 50


class IntegrationConfig:
    """集成层主配置类"""

    def __init__(self, config_file: Optional[str] = None, environment: str = "development"):
        """初始化配置

        Args:
            config_file: 配置文件路径
            environment: 环境名称 (development, testing, production)
        """
        self.environment = environment
        self.config_file = config_file
        self._config_data: Dict[str, Any] = {}

        # 默认配置
        self.system_coordinator = SystemCoordinatorConfig()
        self.signal_router = SignalRouterConfig()
        self.data_flow_manager = DataFlowManagerConfig()
        self.validation_coordinator = ValidationCoordinatorConfig()
        self.monitoring = MonitoringConfig()
        self.adapter = AdapterConfig()

        # 新增微服务配置
        self.service = ServiceConfig()
        self.logging = LoggingConfig()
        self.database = DatabaseConfig()
        self.redis = RedisConfig()

        # 加载配置
        self._load_config()

        logger.info(f"Integration config initialized for environment: {environment}")

    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            # 加载默认配置
            self._load_default_config()

            # 加载环境特定配置
            self._load_environment_config()

            # 加载用户指定配置文件
            if self.config_file:
                self._load_file_config(self.config_file)

            # 加载环境变量配置
            self._load_env_config()

            # 应用配置到各组件
            self._apply_config()

        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    def _load_default_config(self) -> None:
        """加载默认配置"""
        default_config = {
            'system_coordinator': {
                'max_concurrent_cycles': 3,
                'cycle_timeout': 300,
                'strategy_analysis_timeout_seconds': 1800,
                'strategy_validation_timeout_seconds': 1800,
                'retry_attempts': 3,
                'retry_delay': 1.0,
                'health_check_interval': 30,
                'resource_allocation_strategy': 'balanced',
                'enable_auto_recovery': True,
                'emergency_stop_threshold': 0.1
            },
            'signal_router': {
                'max_signal_queue_size': 1000,
                'signal_timeout': 60,
                'conflict_resolution_strategy': 'priority_based',
                'enable_signal_validation': True,
                'signal_expiry_time': 300,
                'batch_processing_size': 50,
                'enable_signal_compression': True
            },
            'data_flow_manager': {
                'cache_size_mb': 512,
                'cache_ttl': 300,
                'max_concurrent_requests': 100,
                'request_timeout': 30,
                'enable_data_compression': True,
                'data_quality_threshold': 0.8,
                'enable_auto_cleanup': True,
                'cleanup_interval': 3600
            },
            'validation_coordinator': {
                'enable_dual_validation': True,
                'validation_timeout': 120,
                'confidence_threshold': 0.6,
                'enable_parallel_validation': True,
                'max_validation_workers': 4,
                'validation_cache_size': 100,
                'enable_validation_learning': True
            },
            'monitoring': {
                'enable_system_monitoring': True,
                'enable_performance_monitoring': True,
                'enable_alerting': True,
                'monitoring_interval': 10,
                'alert_cooldown': 300,
                'performance_threshold': {
                    'cpu_usage': 0.8,
                    'memory_usage': 0.8,
                    'response_time': 1000,
                    'error_rate': 0.05
                }
            },
            'adapter': {
                'connection_timeout': 30,
                'request_timeout': 60,
                'max_retries': 3,
                'retry_delay': 1.0,
                'enable_connection_pooling': True,
                'pool_size': 10,
                'enable_health_check': True,
                'health_check_interval': 60,
                'default_portfolio_id': None
            },
            'service': {
                'host': '0.0.0.0',
                'port': 8088,
                'debug': False,
                'macro_service_url': 'http://macro:8080',
                'portfolio_service_url': 'http://portfolio:8080',
                'execution_service_url': 'http://execution:8087',
                'flowhub_service_url': 'http://flowhub:8080',
                'scheduler_enabled': True,
                'scheduler_timezone': 'Asia/Shanghai',
                'daily_data_fetch_cron': 'at:16:30',
                'daily_index_fetch_retry_interval_seconds': 1800,
                'daily_index_fetch_retry_attempts': 3,
                'monthly_sw_industry_full_fetch_cron': 'at:16:30',
                'daily_data_fetch_timeout': 86400,
                'strategy_plan_generation_cron': 'at:18:50',
                'strategy_plan_generation_timeout': 7200,
                'strategy_plan_lookback_days': 180,
                'strategy_plan_initial_cash': 3000000.0,
                'strategy_plan_benchmark': '000300.SH',
                'strategy_plan_cost': '5bp',
                'monitoring_enabled': True,
                'metrics_port': 9090,
                'init_data_on_startup': True,
                'init_wait_dependencies': ['flowhub'],
                'init_max_history_first_run': True,
                'init_retry': { 'max_retries': 10, 'backoff': [1, 2, 3, 5, 8, 13], 'timeout': 300 },
                'init_concurrency': 2,
                'auth_issuer': 'autotm-brain',
                'auth_jwt_secret': 'CHANGE_ME_AUTOTM_BRAIN_SECRET',
                'auth_access_token_ttl_seconds': 900,
                'auth_refresh_token_ttl_seconds': 604800,
                'auth_admin_default_password': 'admin123!',
                'auth_lock_enabled': False,
                'auth_lock_threshold': 5,
                'auth_lock_seconds': 600,
            },
            'logging': {
                'level': 'INFO',
                'file': 'logs/integration.log',
                'max_size': 10485760,
                'backup_count': 5,
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'database': {
                'url': 'postgresql://postgres:password@localhost:5432/stock_data',
                'pool_size': 20,
                'max_overflow': 30,
                'pool_timeout': 30,
                'pool_recycle': 3600
            },
            'redis': {
                'url': 'redis://localhost:6379/0',
                'host': 'localhost',
                'port': 6379,
                'db': 0,
                'password': None,
                'max_connections': 50
            }
        }

        self._config_data.update(default_config)

    def _load_environment_config(self) -> None:
        """加载环境特定配置"""
        env_config_file = f"config/integration_{self.environment}.yaml"
        if os.path.exists(env_config_file):
            self._load_file_config(env_config_file)

    def _load_file_config(self, config_file: str) -> None:
        """加载配置文件"""
        try:
            config_path = Path(config_file)
            if not config_path.exists():
                logger.warning(f"Config file not found: {config_file}")
                return

            with open(config_path, 'r', encoding='utf-8') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    file_config = yaml.safe_load(f)
                elif config_path.suffix.lower() == '.json':
                    file_config = json.load(f)
                else:
                    logger.warning(f"Unsupported config file format: {config_file}")
                    return

            if file_config:
                self._merge_config(self._config_data, file_config)
                logger.info(f"Loaded config from: {config_file}")

        except Exception as e:
            logger.error(f"Failed to load config file {config_file}: {e}")
            raise

    def _load_env_config(self) -> None:
        """加载环境变量配置"""
        env_mappings = {
            # 原有配置
            'INTEGRATION_MAX_CONCURRENT_CYCLES': ('system_coordinator', 'max_concurrent_cycles', int),
            'INTEGRATION_CYCLE_TIMEOUT': ('system_coordinator', 'cycle_timeout', int),
            'INTEGRATION_STRATEGY_ANALYSIS_TIMEOUT_SECONDS': ('system_coordinator', 'strategy_analysis_timeout_seconds', int),
            'INTEGRATION_STRATEGY_VALIDATION_TIMEOUT_SECONDS': ('system_coordinator', 'strategy_validation_timeout_seconds', int),
            'INTEGRATION_SIGNAL_TIMEOUT': ('signal_router', 'signal_timeout', int),
            'INTEGRATION_CACHE_SIZE_MB': ('data_flow_manager', 'cache_size_mb', int),
            'INTEGRATION_ENABLE_MONITORING': ('monitoring', 'enable_system_monitoring', bool),

            # 微服务配置
            'INTEGRATION_HOST': ('service', 'host', str),
            'INTEGRATION_PORT': ('service', 'port', int),
            'INTEGRATION_DEBUG': ('service', 'debug', bool),
            'MACRO_SERVICE_URL': ('service', 'macro_service_url', str),
            'PORTFOLIO_SERVICE_URL': ('service', 'portfolio_service_url', str),
            'EXECUTION_SERVICE_URL': ('service', 'execution_service_url', str),
            'FLOWHUB_SERVICE_URL': ('service', 'flowhub_service_url', str),
            'DEFAULT_PORTFOLIO_ID': ('adapter', 'default_portfolio_id', str),
            'SCHEDULER_ENABLED': ('service', 'scheduler_enabled', bool),
            'SCHEDULER_TIMEZONE': ('service', 'scheduler_timezone', str),
            'SCHEDULER_DAILY_CRON': ('service', 'daily_data_fetch_cron', str),
            'SCHEDULER_DAILY_INDEX_RETRY_INTERVAL_SECONDS': ('service', 'daily_index_fetch_retry_interval_seconds', int),
            'SCHEDULER_DAILY_INDEX_RETRY_ATTEMPTS': ('service', 'daily_index_fetch_retry_attempts', int),
            'SCHEDULER_MONTHLY_SW_FULL_CRON': ('service', 'monthly_sw_industry_full_fetch_cron', str),
            'SCHEDULER_DAILY_FETCH_TIMEOUT': ('service', 'daily_data_fetch_timeout', int),
            'SCHEDULER_STRATEGY_PLAN_CRON': ('service', 'strategy_plan_generation_cron', str),
            'SCHEDULER_STRATEGY_PLAN_TIMEOUT': ('service', 'strategy_plan_generation_timeout', int),
            'SCHEDULER_STRATEGY_PLAN_LOOKBACK_DAYS': ('service', 'strategy_plan_lookback_days', int),
            'SCHEDULER_STRATEGY_PLAN_INITIAL_CASH': ('service', 'strategy_plan_initial_cash', float),
            'SCHEDULER_STRATEGY_PLAN_BENCHMARK': ('service', 'strategy_plan_benchmark', str),
            'SCHEDULER_STRATEGY_PLAN_COST': ('service', 'strategy_plan_cost', str),
            'BRAIN_AUTH_ISSUER': ('service', 'auth_issuer', str),
            'BRAIN_AUTH_JWT_SECRET': ('service', 'auth_jwt_secret', str),
            'BRAIN_AUTH_ACCESS_TOKEN_TTL_SECONDS': ('service', 'auth_access_token_ttl_seconds', int),
            'BRAIN_AUTH_REFRESH_TOKEN_TTL_SECONDS': ('service', 'auth_refresh_token_ttl_seconds', int),
            'BRAIN_AUTH_ADMIN_PASSWORD': ('service', 'auth_admin_default_password', str),
            'BRAIN_AUTH_LOCK_ENABLED': ('service', 'auth_lock_enabled', bool),
            'BRAIN_AUTH_LOCK_THRESHOLD': ('service', 'auth_lock_threshold', int),
            'BRAIN_AUTH_LOCK_SECONDS': ('service', 'auth_lock_seconds', int),
            # 启动数据初始化配置（环境变量覆盖）
            'INIT_DATA_ON_STARTUP': ('service', 'init_data_on_startup', bool),
            'INIT_WAIT_DEPENDENCIES': ('service', 'init_wait_dependencies', str),  # 逗号分隔
            'INIT_MAX_HISTORY_FIRST_RUN': ('service', 'init_max_history_first_run', bool),
            'INIT_CONCURRENCY': ('service', 'init_concurrency', int),

            # 日志配置
            'LOG_LEVEL': ('logging', 'level', str),
            'LOG_FILE': ('logging', 'file', str),

            # 数据库配置
            'DATABASE_URL': ('database', 'url', str),
            'DATABASE_POOL_SIZE': ('database', 'pool_size', int),

            # Redis配置
            'REDIS_URL': ('redis', 'url', str),
            'REDIS_HOST': ('redis', 'host', str),
            'REDIS_PORT': ('redis', 'port', int),
            'REDIS_DB': ('redis', 'db', int),
            'REDIS_PASSWORD': ('redis', 'password', str),
        }

        for env_var, (section, key, type_func) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    if type_func == bool:
                        value = value.lower() in ('true', '1', 'yes', 'on')
                    elif type_func == str and value.lower() == 'none':
                        value = None
                    else:
                        value = type_func(value)

                    if section not in self._config_data:
                        self._config_data[section] = {}
                    self._config_data[section][key] = value

                    logger.info(f"Applied env config: {env_var}={value}")

                except (ValueError, TypeError) as e:
                    logger.warning(f"Invalid env config value for {env_var}: {value}, error: {e}")

        #    INIT_WAIT_DEPENDENCIES 
        #   
        if os.getenv('INIT_WAIT_DEPENDENCIES') is not None:
            try:
                deps_raw = os.getenv('INIT_WAIT_DEPENDENCIES', '')
                deps = [x.strip() for x in deps_raw.split(',') if x.strip()]
                if 'service' not in self._config_data:
                    self._config_data['service'] = {}
                self._config_data['service']['init_wait_dependencies'] = deps
                logger.info(f"Applied env config: INIT_WAIT_DEPENDENCIES={deps}")
            except Exception as e:
                logger.warning(f"Invalid INIT_WAIT_DEPENDENCIES: {e}")

    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """合并配置字典"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value

    def _apply_config(self) -> None:
        """应用配置到各组件"""
        try:
            # 应用系统协调器配置
            if 'system_coordinator' in self._config_data:
                config = self._config_data['system_coordinator']
                for key, value in config.items():
                    if hasattr(self.system_coordinator, key):
                        setattr(self.system_coordinator, key, value)

            # 应用信号路由器配置
            if 'signal_router' in self._config_data:
                config = self._config_data['signal_router']
                for key, value in config.items():
                    if hasattr(self.signal_router, key):
                        setattr(self.signal_router, key, value)

            # 应用数据流管理器配置
            if 'data_flow_manager' in self._config_data:
                config = self._config_data['data_flow_manager']
                for key, value in config.items():
                    if hasattr(self.data_flow_manager, key):
                        setattr(self.data_flow_manager, key, value)

            # 应用验证协调器配置
            if 'validation_coordinator' in self._config_data:
                config = self._config_data['validation_coordinator']
                for key, value in config.items():
                    if hasattr(self.validation_coordinator, key):
                        setattr(self.validation_coordinator, key, value)

            # 应用监控配置
            if 'monitoring' in self._config_data:
                config = self._config_data['monitoring']
                for key, value in config.items():
                    if hasattr(self.monitoring, key):
                        setattr(self.monitoring, key, value)

            # 应用适配器配置
            if 'adapter' in self._config_data:
                config = self._config_data['adapter']
                for key, value in config.items():
                    if hasattr(self.adapter, key):
                        setattr(self.adapter, key, value)

            # 应用微服务配置
            if 'service' in self._config_data:
                config = self._config_data['service']
                for key, value in config.items():
                    if hasattr(self.service, key):
                        setattr(self.service, key, value)

            # 应用日志配置
            if 'logging' in self._config_data:
                config = self._config_data['logging']
                for key, value in config.items():
                    if hasattr(self.logging, key):
                        setattr(self.logging, key, value)

            # 应用数据库配置
            if 'database' in self._config_data:
                config = self._config_data['database']
                for key, value in config.items():
                    if hasattr(self.database, key):
                        setattr(self.database, key, value)

            # 应用Redis配置
            if 'redis' in self._config_data:
                config = self._config_data['redis']
                for key, value in config.items():
                    if hasattr(self.redis, key):
                        setattr(self.redis, key, value)

        except Exception as e:
            logger.error(f"Failed to apply config: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值

        Args:
            key: 配置键，支持点号分隔的嵌套键
            default: 默认值

        Returns:
            Any: 配置值
        """
        keys = key.split('.')
        value = self._config_data

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """设置配置值

        Args:
            key: 配置键，支持点号分隔的嵌套键
            value: 配置值
        """
        keys = key.split('.')
        config = self._config_data

        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]

        config[keys[-1]] = value

        # 重新应用配置
        self._apply_config()

    def validate(self) -> bool:
        """验证配置有效性

        Returns:
            bool: 配置是否有效
        """
        try:
            # 验证必要的配置项
            required_sections = [
                'system_coordinator',
                'signal_router',
                'data_flow_manager',
                'validation_coordinator',
                'monitoring',
                'adapter',
                'service',
                'logging',
                'database',
                'redis'
            ]

            for section in required_sections:
                if section not in self._config_data:
                    logger.error(f"Missing required config section: {section}")
                    return False

            # 验证数值范围
            validations = [
                (self.system_coordinator.max_concurrent_cycles > 0, "max_concurrent_cycles must be positive"),
                (self.system_coordinator.cycle_timeout > 0, "cycle_timeout must be positive"),
                (self.system_coordinator.strategy_analysis_timeout_seconds > 0, "strategy_analysis_timeout_seconds must be positive"),
                (self.system_coordinator.strategy_validation_timeout_seconds > 0, "strategy_validation_timeout_seconds must be positive"),
                (0 < self.system_coordinator.emergency_stop_threshold < 1, "emergency_stop_threshold must be between 0 and 1"),
                (self.signal_router.max_signal_queue_size > 0, "max_signal_queue_size must be positive"),
                (self.data_flow_manager.cache_size_mb > 0, "cache_size_mb must be positive"),
                (0 < self.data_flow_manager.data_quality_threshold <= 1, "data_quality_threshold must be between 0 and 1"),
                (0 < self.validation_coordinator.confidence_threshold <= 1, "confidence_threshold must be between 0 and 1"),
                # 微服务配置验证
                (1 <= self.service.port <= 65535, "service port must be between 1 and 65535"),
                (self.service.host is not None, "service host must be specified"),
                (self.database.pool_size > 0, "database pool_size must be positive"),
                (1 <= self.redis.port <= 65535, "redis port must be between 1 and 65535"),
                (0 <= self.redis.db <= 15, "redis db must be between 0 and 15"),
            ]

            for condition, message in validations:
                if not condition:
                    logger.error(f"Config validation failed: {message}")
                    return False

            logger.info("Config validation passed")
            return True

        except Exception as e:
            logger.error(f"Config validation error: {e}")
            return False

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式

        Returns:
            Dict[str, Any]: 配置字典
        """
        return self._config_data.copy()

    def save_to_file(self, file_path: str) -> None:
        """保存配置到文件

        Args:
            file_path: 文件路径
        """
        try:
            config_path = Path(file_path)
            config_path.parent.mkdir(parents=True, exist_ok=True)

            with open(config_path, 'w', encoding='utf-8') as f:
                if config_path.suffix.lower() in ['.yaml', '.yml']:
                    yaml.dump(self._config_data, f, default_flow_style=False, allow_unicode=True)
                elif config_path.suffix.lower() == '.json':
                    json.dump(self._config_data, f, indent=2, ensure_ascii=False)
                else:
                    raise ValueError(f"Unsupported file format: {config_path.suffix}")

            logger.info(f"Config saved to: {file_path}")

        except Exception as e:
            logger.error(f"Failed to save config to {file_path}: {e}")
            raise
