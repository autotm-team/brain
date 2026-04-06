"""DB-backed control-plane defaults for Brain."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, Optional

try:
    from econdb import UISystemDataAPI, SystemSettingDTO, create_database_manager  # type: ignore
except Exception:  # pragma: no cover - surfaced by caller
    UISystemDataAPI = None  # type: ignore[assignment]
    SystemSettingDTO = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]


class BrainControlPlaneSettingsService:
    SCHEDULER_KEY = "control_plane.scheduler"
    STRATEGY_PLAN_KEY = "control_plane.strategy_plan"
    FLOWHUB_BOOTSTRAP_KEY = "control_plane.flowhub_bootstrap"

    def __init__(self, config, system_api: Optional[Any] = None):
        if system_api is None and (UISystemDataAPI is None or create_database_manager is None):
            raise RuntimeError("econdb runtime dependencies are unavailable for control-plane settings")
        self._config = config
        if system_api is not None:
            self._api = system_api
        else:
            econdb_cfg = getattr(config, "econdb", None)
            db_manager = create_database_manager(
                econdb_cfg.econdb_override() if econdb_cfg is not None else None
            )
            self._api = UISystemDataAPI(db_manager)

    @staticmethod
    def _setting_dto(key: str, value: Dict[str, Any], metadata: Dict[str, Any], updated_by: str) -> Any:
        if SystemSettingDTO is not None:
            return SystemSettingDTO(
                key=key,
                value=value,
                metadata=metadata,
                updated_by=updated_by,
            )
        return SimpleNamespace(
            key=key,
            value=value,
            metadata=metadata,
            updated_by=updated_by,
        )

    def _scheduler_env_defaults(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self._config.service.scheduler_enabled),
            "timezone": str(self._config.service.scheduler_timezone),
            "daily_data_fetch_cron": self._config.control_plane.daily_data_fetch_cron,
            "daily_index_fetch_retry_interval_seconds": int(self._config.control_plane.daily_index_fetch_retry_interval_seconds),
            "daily_index_fetch_retry_attempts": int(self._config.control_plane.daily_index_fetch_retry_attempts),
            "monthly_sw_industry_full_fetch_cron": self._config.control_plane.monthly_sw_industry_full_fetch_cron,
            "daily_data_fetch_timeout": int(self._config.control_plane.daily_data_fetch_timeout),
        }

    def _strategy_plan_env_defaults(self) -> Dict[str, Any]:
        return {
            "generation_cron": self._config.control_plane.strategy_plan_generation_cron,
            "generation_timeout": int(self._config.control_plane.strategy_plan_generation_timeout),
            "lookback_days": int(self._config.control_plane.strategy_plan_lookback_days),
            "initial_cash": float(self._config.control_plane.strategy_plan_initial_cash),
            "benchmark": str(self._config.control_plane.strategy_plan_benchmark),
            "cost": str(self._config.control_plane.strategy_plan_cost),
        }

    def _flowhub_bootstrap_env_defaults(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self._config.control_plane.flowhub_bootstrap_enabled),
            "trigger_created": bool(self._config.control_plane.flowhub_bootstrap_trigger_created),
            "stable_wait_seconds": int(self._config.control_plane.flowhub_stable_wait_seconds),
            "research_stock_basic_cron": self._config.control_plane.flowhub_research_stock_basic_cron,
            "strategy_index_daily_cron": self._config.control_plane.flowhub_strategy_index_daily_cron,
            "strategy_trade_cal_cron": self._config.control_plane.flowhub_strategy_trade_cal_cron,
            "strategy_index_components_cron": self._config.control_plane.flowhub_strategy_index_components_cron,
            "strategy_sw_industry_cron": self._config.control_plane.flowhub_strategy_sw_industry_cron,
            "research_suspend_cron": self._config.control_plane.flowhub_research_suspend_cron,
            "research_st_status_cron": self._config.control_plane.flowhub_research_st_status_cron,
            "research_stk_limit_cron": self._config.control_plane.flowhub_research_stk_limit_cron,
        }

    async def _get_or_seed(self, key: str, defaults: Dict[str, Any]) -> Dict[str, Any]:
        existing = await asyncio.to_thread(self._api.get_setting, key)
        if existing and isinstance(existing.get("value"), dict):
            merged = dict(defaults)
            merged.update(existing["value"])
            return merged

        await asyncio.to_thread(
            self._api.upsert_setting,
            self._setting_dto(
                key=key,
                value=defaults,
                metadata={
                    "seeded_from": "brain_env_defaults",
                    "managed_by": "BrainControlPlaneSettingsService",
                },
                updated_by="system",
            ),
        )
        return dict(defaults)

    async def ensure_seeded(self) -> Dict[str, Dict[str, Any]]:
        return await self.snapshot()

    async def get_scheduler_defaults(self) -> Dict[str, Any]:
        return await self._get_or_seed(self.SCHEDULER_KEY, self._scheduler_env_defaults())

    async def get_strategy_plan_defaults(self) -> Dict[str, Any]:
        return await self._get_or_seed(self.STRATEGY_PLAN_KEY, self._strategy_plan_env_defaults())

    async def get_flowhub_bootstrap_defaults(self) -> Dict[str, Any]:
        return await self._get_or_seed(self.FLOWHUB_BOOTSTRAP_KEY, self._flowhub_bootstrap_env_defaults())

    async def snapshot(self) -> Dict[str, Dict[str, Any]]:
        scheduler, strategy_plan, flowhub_bootstrap = await asyncio.gather(
            self.get_scheduler_defaults(),
            self.get_strategy_plan_defaults(),
            self.get_flowhub_bootstrap_defaults(),
        )
        return {
            "scheduler": scheduler,
            "strategy_plan": strategy_plan,
            "flowhub_bootstrap": flowhub_bootstrap,
        }
