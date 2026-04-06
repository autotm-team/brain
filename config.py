"""
Brain runtime settings.

运行时权威配置来自环境变量；不再从仓库内 YAML 加载运行配置。
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import yaml
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_CORS_ALLOWED_ORIGINS = [
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:4173",
    "http://localhost:4173",
]

COMMON_MODEL_CONFIG = SettingsConfigDict(
    case_sensitive=False,
    extra="ignore",
)


def _split_csv(raw: Optional[str], *, default: Optional[List[str]] = None) -> List[str]:
    if raw is None:
        return list(default or [])
    items = [item.strip() for item in str(raw).split(",") if item.strip()]
    if items:
        return items
    return list(default or [])


def _split_int_csv(raw: Optional[str], *, default: List[int]) -> List[int]:
    if raw is None:
        return list(default)
    values: List[int] = []
    for item in str(raw).split(","):
        item = item.strip()
        if not item:
            continue
        values.append(int(item))
    return values or list(default)


def _mask_url(url: Optional[str]) -> Optional[str]:
    if not url or "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    creds, host = rest.split("@", 1)
    if ":" not in creds:
        return f"{scheme}://***@{host}"
    user, _ = creds.split(":", 1)
    return f"{scheme}://{user}:***@{host}"


class SystemCoordinatorConfig(BaseSettings):
    max_concurrent_cycles: int = Field(
        default=3,
        validation_alias="BRAIN_MAX_CONCURRENT_CYCLES",
    )
    cycle_timeout: int = Field(
        default=300,
        validation_alias="BRAIN_CYCLE_TIMEOUT",
    )
    strategy_analysis_timeout_seconds: int = Field(
        default=1800,
        validation_alias="BRAIN_STRATEGY_ANALYSIS_TIMEOUT_SECONDS",
    )
    strategy_validation_timeout_seconds: int = Field(
        default=1800,
        validation_alias="BRAIN_STRATEGY_VALIDATION_TIMEOUT_SECONDS",
    )
    retry_attempts: int = 3
    retry_delay: float = 1.0
    health_check_interval: int = 30
    resource_allocation_strategy: str = "balanced"
    enable_auto_recovery: bool = True
    emergency_stop_threshold: float = 0.1

    model_config = COMMON_MODEL_CONFIG


class SignalRouterConfig(BaseSettings):
    max_signal_queue_size: int = 1000
    signal_timeout: int = Field(
        default=60,
        validation_alias="BRAIN_SIGNAL_TIMEOUT",
    )
    conflict_resolution_strategy: str = "priority_based"
    enable_signal_validation: bool = True
    signal_expiry_time: int = 300
    batch_processing_size: int = 50
    enable_signal_compression: bool = True

    model_config = COMMON_MODEL_CONFIG


class DataFlowManagerConfig(BaseSettings):
    cache_size_mb: int = Field(
        default=512,
        validation_alias="BRAIN_CACHE_SIZE_MB",
    )
    cache_ttl: int = 300
    max_concurrent_requests: int = 100
    request_timeout: int = 30
    enable_data_compression: bool = True
    data_quality_threshold: float = 0.8
    enable_auto_cleanup: bool = True
    cleanup_interval: int = 3600

    model_config = COMMON_MODEL_CONFIG


class ValidationCoordinatorConfig(BaseSettings):
    enable_dual_validation: bool = True
    validation_timeout: int = 120
    confidence_threshold: float = 0.6
    enable_parallel_validation: bool = True
    max_validation_workers: int = 4
    validation_cache_size: int = 100
    enable_validation_learning: bool = True

    model_config = COMMON_MODEL_CONFIG


class MonitoringConfig(BaseSettings):
    enable_system_monitoring: bool = Field(
        default=True,
        validation_alias="BRAIN_ENABLE_MONITORING",
    )
    enable_performance_monitoring: bool = True
    enable_alerting: bool = True
    monitoring_interval: int = 10
    alert_cooldown: int = 300
    performance_threshold: Dict[str, float] = Field(
        default_factory=lambda: {
            "cpu_usage": 0.8,
            "memory_usage": 0.8,
            "response_time": 1000,
            "error_rate": 0.05,
        }
    )

    model_config = COMMON_MODEL_CONFIG


class AdapterConfig(BaseSettings):
    connection_timeout: int = 30
    request_timeout: int = 60
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_connection_pooling: bool = True
    pool_size: int = 10
    enable_health_check: bool = True
    health_check_interval: int = 60
    default_portfolio_id: Optional[str] = Field(
        default=None,
        validation_alias="BRAIN_DEFAULT_PORTFOLIO_ID",
    )

    model_config = COMMON_MODEL_CONFIG


class ServiceConfig(BaseSettings):
    host: str = Field(default="0.0.0.0", validation_alias="BRAIN_HOST")
    port: int = Field(default=8088, validation_alias="BRAIN_PORT")
    debug: bool = Field(default=False, validation_alias="BRAIN_DEBUG")
    config_proxy_token: str = Field(
        default="",
        validation_alias="AUTOTM_CONFIG_PROXY_TOKEN",
        exclude=True,
    )

    macro_service_url: str = Field(
        default="http://macro:8080",
        validation_alias="BRAIN_MACRO_SERVICE_URL",
    )
    portfolio_service_url: str = Field(
        default="http://portfolio:8080",
        validation_alias="BRAIN_PORTFOLIO_SERVICE_URL",
    )
    execution_service_url: str = Field(
        default="http://execution:8087",
        validation_alias="BRAIN_EXECUTION_SERVICE_URL",
    )
    flowhub_service_url: str = Field(
        default="http://flowhub:8080",
        validation_alias="BRAIN_FLOWHUB_SERVICE_URL",
    )

    scheduler_enabled: bool = Field(
        default=True,
        validation_alias="BRAIN_SCHEDULER_ENABLED",
    )
    scheduler_timezone: str = Field(
        default="Asia/Shanghai",
        validation_alias="BRAIN_SCHEDULER_TIMEZONE",
    )

    monitoring_enabled: bool = Field(
        default=True,
        validation_alias="BRAIN_MONITORING_ENABLED",
    )
    metrics_port: int = Field(
        default=9090,
        validation_alias="BRAIN_METRICS_PORT",
    )

    init_data_on_startup: bool = Field(
        default=True,
        validation_alias="BRAIN_INIT_DATA_ON_STARTUP",
    )
    init_wait_dependencies_raw: str = Field(
        default="flowhub",
        validation_alias="BRAIN_INIT_WAIT_DEPENDENCIES",
    )
    init_max_history_first_run: bool = Field(
        default=True,
        validation_alias="BRAIN_INIT_MAX_HISTORY_FIRST_RUN",
    )
    init_concurrency: int = Field(
        default=2,
        validation_alias="BRAIN_INIT_CONCURRENCY",
    )
    init_retry_max_retries: int = Field(default=10, validation_alias="BRAIN_INIT_RETRY_MAX_RETRIES")
    init_retry_backoff_raw: str = Field(default="1,2,3,5,8,13", validation_alias="BRAIN_INIT_RETRY_BACKOFF")
    init_retry_timeout: int = Field(default=300, validation_alias="BRAIN_INIT_RETRY_TIMEOUT")

    auth_issuer: str = Field(default="autotm-brain", validation_alias="BRAIN_AUTH_ISSUER")
    auth_jwt_secret: str = Field(default="", validation_alias="BRAIN_AUTH_JWT_SECRET")
    auth_access_token_ttl_seconds: int = Field(default=900, validation_alias="BRAIN_AUTH_ACCESS_TOKEN_TTL_SECONDS")
    auth_refresh_token_ttl_seconds: int = Field(default=604800, validation_alias="BRAIN_AUTH_REFRESH_TOKEN_TTL_SECONDS")
    auth_admin_default_password: str = Field(default="", validation_alias="BRAIN_AUTH_ADMIN_PASSWORD")
    auth_lock_enabled: bool = Field(default=False, validation_alias="BRAIN_AUTH_LOCK_ENABLED")
    auth_lock_threshold: int = Field(default=5, validation_alias="BRAIN_AUTH_LOCK_THRESHOLD")
    auth_lock_seconds: int = Field(default=600, validation_alias="BRAIN_AUTH_LOCK_SECONDS")
    auth_refresh_cookie_name: str = Field(
        default="autotm_refresh_token",
        validation_alias="BRAIN_AUTH_REFRESH_COOKIE_NAME",
    )
    auth_refresh_cookie_path: str = Field(
        default="/api/v1/ui/auth",
        validation_alias="BRAIN_AUTH_REFRESH_COOKIE_PATH",
    )
    auth_refresh_cookie_samesite: str = Field(
        default="Lax",
        validation_alias="BRAIN_AUTH_REFRESH_COOKIE_SAMESITE",
    )
    auth_refresh_cookie_domain: Optional[str] = Field(
        default=None,
        validation_alias="BRAIN_AUTH_REFRESH_COOKIE_DOMAIN",
    )

    cors_allowed_origins_raw: str = Field(default="", validation_alias="BRAIN_CORS_ALLOWED_ORIGINS")

    model_config = COMMON_MODEL_CONFIG

    @property
    def init_wait_dependencies(self) -> List[str]:
        return _split_csv(self.init_wait_dependencies_raw, default=["flowhub"])

    @property
    def init_retry(self) -> Dict[str, Any]:
        return {
            "max_retries": self.init_retry_max_retries,
            "backoff": _split_int_csv(self.init_retry_backoff_raw, default=[1, 2, 3, 5, 8, 13]),
            "timeout": self.init_retry_timeout,
        }

    @property
    def cors_allowed_origins(self) -> List[str]:
        return _split_csv(self.cors_allowed_origins_raw, default=DEFAULT_CORS_ALLOWED_ORIGINS)


class LoggingConfig(BaseSettings):
    level: str = Field(default="INFO", validation_alias="BRAIN_LOG_LEVEL")
    file: str = Field(default="logs/integration.log", validation_alias="BRAIN_LOG_FILE")
    max_size: int = 10485760
    backup_count: int = 5
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    model_config = COMMON_MODEL_CONFIG


class DatabaseConfig(BaseSettings):
    url: Optional[str] = Field(
        default=None,
        validation_alias="DATABASE_URL",
    )
    pool_size: int = Field(default=20, validation_alias=AliasChoices("DATABASE_POOL_SIZE", "BRAIN_DATABASE_POOL_SIZE"))
    max_overflow: int = Field(
        default=30,
        validation_alias=AliasChoices("DATABASE_MAX_OVERFLOW", "BRAIN_DATABASE_MAX_OVERFLOW"),
    )
    pool_timeout: int = Field(
        default=30,
        validation_alias=AliasChoices("DATABASE_POOL_TIMEOUT", "BRAIN_DATABASE_POOL_TIMEOUT"),
    )
    pool_recycle: int = Field(
        default=3600,
        validation_alias=AliasChoices("DATABASE_POOL_RECYCLE", "BRAIN_DATABASE_POOL_RECYCLE"),
    )

    model_config = COMMON_MODEL_CONFIG


class EconDBRuntimeConfig(BaseSettings):
    url: Optional[str] = Field(
        default=None,
        validation_alias="ECONDB_DATABASE_URL",
    )
    host: str = Field(default="localhost", validation_alias=AliasChoices("ECONDB_DB_HOST", "BRAIN_ECONDB_DB_HOST"))
    port: int = Field(default=5432, validation_alias=AliasChoices("ECONDB_DB_PORT", "BRAIN_ECONDB_DB_PORT"))
    name: str = Field(default="stock_data", validation_alias=AliasChoices("ECONDB_DB_NAME", "BRAIN_ECONDB_DB_NAME"))
    user: str = Field(default="postgres", validation_alias=AliasChoices("ECONDB_DB_USER", "BRAIN_ECONDB_DB_USER"))
    password: str = Field(default="", validation_alias=AliasChoices("ECONDB_DB_PASSWORD", "BRAIN_ECONDB_DB_PASSWORD"))
    pool_size: int = Field(default=10, validation_alias=AliasChoices("ECONDB_DB_POOL_SIZE", "BRAIN_ECONDB_DB_POOL_SIZE"))
    max_overflow: int = Field(
        default=10,
        validation_alias=AliasChoices("ECONDB_DB_MAX_OVERFLOW", "BRAIN_ECONDB_DB_MAX_OVERFLOW"),
    )
    pool_recycle: int = Field(
        default=1800,
        validation_alias=AliasChoices("ECONDB_DB_POOL_RECYCLE", "BRAIN_ECONDB_DB_POOL_RECYCLE"),
    )
    pool_timeout: int = Field(
        default=30,
        validation_alias=AliasChoices("ECONDB_DB_POOL_TIMEOUT", "BRAIN_ECONDB_DB_POOL_TIMEOUT"),
    )

    model_config = COMMON_MODEL_CONFIG

    def model_post_init(self, __context: Any) -> None:
        if not self.url:
            return
        parsed = urlparse(self.url)
        if parsed.hostname and self.host == "localhost":
            self.host = parsed.hostname
        if parsed.port and self.port == 5432:
            self.port = parsed.port
        if parsed.path and self.name == "stock_data":
            self.name = parsed.path.lstrip("/") or self.name
        if parsed.username and self.user == "postgres":
            self.user = parsed.username
        if parsed.password is not None and self.password == "":
            self.password = parsed.password

    @property
    def database_url(self) -> str:
        if self.url:
            return str(self.url)
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"

    def econdb_override(self) -> Dict[str, Any]:
        return {
            "database_url": self.database_url,
            "db_host": self.host,
            "db_port": self.port,
            "db_name": self.name,
            "db_user": self.user,
            "db_password": self.password,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_timeout": self.pool_timeout,
        }


class RedisConfig(BaseSettings):
    url: Optional[str] = Field(default=None, validation_alias="REDIS_URL")
    host: str = Field(default="localhost", validation_alias=AliasChoices("REDIS_HOST", "BRAIN_REDIS_HOST"))
    port: int = Field(default=6379, validation_alias=AliasChoices("REDIS_PORT", "BRAIN_REDIS_PORT"))
    db: int = Field(default=0, validation_alias=AliasChoices("REDIS_DB", "BRAIN_REDIS_DB"))
    password: Optional[str] = Field(default=None, validation_alias=AliasChoices("REDIS_PASSWORD", "BRAIN_REDIS_PASSWORD"))
    max_connections: int = Field(
        default=50,
        validation_alias=AliasChoices("REDIS_MAX_CONNECTIONS", "BRAIN_REDIS_MAX_CONNECTIONS"),
    )

    model_config = COMMON_MODEL_CONFIG

    def model_post_init(self, __context: Any) -> None:
        if not self.url:
            return
        parsed = urlparse(self.url)
        if parsed.hostname and self.host == "localhost":
            self.host = parsed.hostname
        if parsed.port and self.port == 6379:
            self.port = parsed.port
        db_part = (parsed.path or "/0").lstrip("/")
        if db_part and self.db == 0:
            self.db = int(db_part)
        if parsed.password is not None and self.password is None:
            self.password = parsed.password


class SchemaConfig(BaseSettings):
    enforce: bool = Field(default=True, validation_alias="DB_SCHEMA_ENFORCE")
    required_version: str = Field(default="V001", validation_alias="DB_SCHEMA_REQUIRED_VERSION")
    exit_on_failure: bool = Field(default=True, validation_alias="DB_SCHEMA_EXIT_ON_FAILURE")
    allow_runtime_ddl: bool = Field(default=False, validation_alias="DB_ALLOW_RUNTIME_DDL")

    model_config = COMMON_MODEL_CONFIG


class ControlPlaneDefaultsConfig(BaseSettings):
    daily_data_fetch_cron: Optional[str] = Field(
        default="at:16:30",
        validation_alias="BRAIN_SCHEDULER_DAILY_CRON",
    )
    daily_index_fetch_retry_interval_seconds: int = Field(
        default=1800,
        validation_alias="BRAIN_SCHEDULER_DAILY_INDEX_RETRY_INTERVAL_SECONDS",
    )
    daily_index_fetch_retry_attempts: int = Field(
        default=3,
        validation_alias="BRAIN_SCHEDULER_DAILY_INDEX_RETRY_ATTEMPTS",
    )
    monthly_sw_industry_full_fetch_cron: Optional[str] = Field(
        default="at:16:30",
        validation_alias="BRAIN_SCHEDULER_MONTHLY_SW_FULL_CRON",
    )
    daily_data_fetch_timeout: int = Field(
        default=86400,
        validation_alias="BRAIN_SCHEDULER_DAILY_FETCH_TIMEOUT",
    )
    strategy_plan_generation_cron: Optional[str] = Field(
        default="at:18:50",
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_CRON",
    )
    strategy_plan_generation_timeout: int = Field(
        default=7200,
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_TIMEOUT",
    )
    strategy_plan_lookback_days: int = Field(
        default=180,
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_LOOKBACK_DAYS",
    )
    strategy_plan_initial_cash: float = Field(
        default=3000000.0,
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_INITIAL_CASH",
    )
    strategy_plan_benchmark: str = Field(
        default="000300.SH",
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_BENCHMARK",
    )
    strategy_plan_cost: str = Field(
        default="5bp",
        validation_alias="BRAIN_SCHEDULER_STRATEGY_PLAN_COST",
    )
    flowhub_bootstrap_enabled: bool = Field(default=True, validation_alias="BRAIN_FLOWHUB_BOOTSTRAP_ENABLED")
    flowhub_bootstrap_trigger_created: bool = Field(
        default=True,
        validation_alias="BRAIN_FLOWHUB_BOOTSTRAP_TRIGGER_CREATED",
    )
    flowhub_stable_wait_seconds: int = Field(default=20, validation_alias="BRAIN_FLOWHUB_STABLE_WAIT_SECONDS")
    flowhub_research_stock_basic_cron: str = Field(
        default="20 17 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_RESEARCH_STOCK_BASIC_CRON",
    )
    flowhub_strategy_index_daily_cron: str = Field(
        default="30 18 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_STRATEGY_INDEX_DAILY_CRON",
    )
    flowhub_strategy_trade_cal_cron: str = Field(
        default="35 18 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_STRATEGY_TRADE_CAL_CRON",
    )
    flowhub_strategy_index_components_cron: str = Field(
        default="30 16 * * 6",
        validation_alias="BRAIN_FLOWHUB_STRATEGY_INDEX_COMPONENTS_CRON",
    )
    flowhub_strategy_sw_industry_cron: str = Field(
        default="30 16 1 * *",
        validation_alias="BRAIN_FLOWHUB_STRATEGY_SW_INDUSTRY_CRON",
    )
    flowhub_research_suspend_cron: str = Field(
        default="40 18 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_RESEARCH_SUSPEND_CRON",
    )
    flowhub_research_st_status_cron: str = Field(
        default="45 18 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_RESEARCH_ST_STATUS_CRON",
    )
    flowhub_research_stk_limit_cron: str = Field(
        default="50 18 * * 1-5",
        validation_alias="BRAIN_FLOWHUB_RESEARCH_STK_LIMIT_CRON",
    )

    model_config = COMMON_MODEL_CONFIG


class BrainSettings:
    def __init__(self, config_file: Optional[str] = None, environment: str = "development"):
        self.environment = environment
        self.config_file = config_file
        self.system_coordinator = SystemCoordinatorConfig()
        self.signal_router = SignalRouterConfig()
        self.data_flow_manager = DataFlowManagerConfig()
        self.validation_coordinator = ValidationCoordinatorConfig()
        self.monitoring = MonitoringConfig()
        self.adapter = AdapterConfig()
        self.service = ServiceConfig()
        self.logging = LoggingConfig()
        self.database = DatabaseConfig()
        self.econdb = EconDBRuntimeConfig()
        if not self.econdb.url and self.database.url:
            self.econdb.url = self.database.url
            self.econdb.model_post_init(None)
        self.redis = RedisConfig()
        self.db_schema = SchemaConfig()
        self.control_plane = ControlPlaneDefaultsConfig()

    def get(self, key: str, default: Any = None) -> Any:
        value: Any = self
        for part in key.split("."):
            if hasattr(value, part):
                value = getattr(value, part)
            else:
                return default
        return value

    def validate(self) -> bool:
        try:
            if not self.service.host:
                raise ValueError("Brain host is required")
            if not self.database.url:
                raise ValueError("DATABASE_URL is required")
            if not self.redis.url:
                raise ValueError("REDIS_URL is required")
            if self.environment == "production" and not self.service.auth_jwt_secret:
                raise ValueError("BRAIN_AUTH_JWT_SECRET is required in production")
            if not self.service.macro_service_url:
                raise ValueError("Brain macro service URL is required")
            if not self.service.execution_service_url:
                raise ValueError("Brain execution service URL is required")
            if not self.service.portfolio_service_url:
                raise ValueError("Brain portfolio service URL is required")
            if not self.service.flowhub_service_url:
                raise ValueError("Brain flowhub service URL is required")
            if not (1 <= self.service.port <= 65535):
                raise ValueError("Brain port must be between 1 and 65535")
            return True
        except Exception:
            return False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "environment": self.environment,
            "system_coordinator": self.system_coordinator.model_dump(),
            "signal_router": self.signal_router.model_dump(),
            "data_flow_manager": self.data_flow_manager.model_dump(),
            "validation_coordinator": self.validation_coordinator.model_dump(),
            "monitoring": self.monitoring.model_dump(),
            "adapter": self.adapter.model_dump(),
            "service": {**self.service.model_dump(), "init_wait_dependencies": self.service.init_wait_dependencies, "init_retry": self.service.init_retry, "cors_allowed_origins": self.service.cors_allowed_origins},
            "logging": self.logging.model_dump(),
            "database": self.database.model_dump(),
            "econdb": self.econdb.model_dump(),
            "redis": self.redis.model_dump(),
            "db_schema": self.db_schema.model_dump(),
            "control_plane": self.control_plane.model_dump(),
        }

    def masked_summary(self) -> Dict[str, Any]:
        return {
            "environment": self.environment,
            "service": {
                "host": self.service.host,
                "port": self.service.port,
                "debug": self.service.debug,
                "scheduler_enabled": self.service.scheduler_enabled,
                "scheduler_timezone": self.service.scheduler_timezone,
                "cors_allowed_origins": self.service.cors_allowed_origins,
            },
            "downstream": {
                "macro": self.service.macro_service_url,
                "portfolio": self.service.portfolio_service_url,
                "execution": self.service.execution_service_url,
                "flowhub": self.service.flowhub_service_url,
            },
            "database": {
                "url": _mask_url(self.database.url),
                "pool_size": self.database.pool_size,
                "max_overflow": self.database.max_overflow,
            },
            "econdb": {
                "database_url": _mask_url(self.econdb.database_url),
                "pool_size": self.econdb.pool_size,
                "max_overflow": self.econdb.max_overflow,
            },
            "redis": {
                "url": _mask_url(self.redis.url),
                "max_connections": self.redis.max_connections,
            },
            "db_schema": self.db_schema.model_dump(),
            "control_plane": {
                "flowhub_bootstrap_enabled": self.control_plane.flowhub_bootstrap_enabled,
                "strategy_plan_generation_cron": self.control_plane.strategy_plan_generation_cron,
            },
        }

    def save_to_file(self, file_path: str) -> None:
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_dict()
        with open(path, "w", encoding="utf-8") as handle:
            if path.suffix.lower() in {".yaml", ".yml"}:
                yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)
            else:
                json.dump(payload, handle, ensure_ascii=False, indent=2)

    def set(self, key: str, value: Any) -> None:
        parts = key.split(".")
        target: Any = self
        for part in parts[:-1]:
            target = getattr(target, part)
        setattr(target, parts[-1], value)


class IntegrationConfig(BrainSettings):
    """Backward-compatible name retained for the rest of the brain service."""


@lru_cache()
def _cached_settings(environment: str = "development") -> IntegrationConfig:
    return IntegrationConfig(environment=environment)


def get_settings(environment: str = "development", force_reload: bool = False) -> IntegrationConfig:
    if force_reload:
        _cached_settings.cache_clear()
    return _cached_settings(environment)
