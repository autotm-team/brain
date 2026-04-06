"""DB-backed control-plane defaults for Brain."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, ConfigDict, Field, ValidationError

try:
    from econdb import (  # type: ignore
        UISystemDataAPI,
        SystemAuditDTO,
        SystemSettingDTO,
        create_database_manager,
    )
except Exception:  # pragma: no cover - surfaced by caller
    UISystemDataAPI = None  # type: ignore[assignment]
    SystemAuditDTO = None  # type: ignore[assignment]
    SystemSettingDTO = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]


class SchedulerDefaultsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    timezone: str = Field(min_length=1)
    daily_data_fetch_cron: str = Field(min_length=1)
    daily_index_fetch_retry_interval_seconds: int = Field(ge=1)
    daily_index_fetch_retry_attempts: int = Field(ge=1)
    monthly_sw_industry_full_fetch_cron: str = Field(min_length=1)
    daily_data_fetch_timeout: int = Field(ge=1)


class StrategyPlanDefaultsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generation_cron: str = Field(min_length=1)
    generation_timeout: int = Field(ge=1)
    lookback_days: int = Field(ge=1)
    initial_cash: float = Field(gt=0)
    benchmark: str = Field(min_length=1)
    cost: str = Field(min_length=1)


class FlowhubBootstrapDefaultsModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    trigger_created: bool
    stable_wait_seconds: int = Field(ge=0)
    research_stock_basic_cron: str = Field(min_length=1)
    strategy_index_daily_cron: str = Field(min_length=1)
    strategy_trade_cal_cron: str = Field(min_length=1)
    strategy_index_components_cron: str = Field(min_length=1)
    strategy_sw_industry_cron: str = Field(min_length=1)
    research_suspend_cron: str = Field(min_length=1)
    research_st_status_cron: str = Field(min_length=1)
    research_stk_limit_cron: str = Field(min_length=1)


@dataclass(frozen=True)
class ControlPlaneResolution:
    key: str
    section: str
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    source: str
    normalized_from_legacy: bool


class BrainControlPlaneSettingsService:
    SCHEDULER_KEY = "control_plane.scheduler"
    STRATEGY_PLAN_KEY = "control_plane.strategy_plan"
    FLOWHUB_BOOTSTRAP_KEY = "control_plane.flowhub_bootstrap"

    SCHEMA_VERSION = "v1"
    MANAGED_BY = "BrainControlPlaneSettingsService"
    REQUIRED_METADATA_FIELDS = {
        "schema_version",
        "managed_by",
        "seeded_from",
        "normalized_from_legacy",
    }

    SECTION_NAMES = {
        SCHEDULER_KEY: "scheduler",
        STRATEGY_PLAN_KEY: "strategy_plan",
        FLOWHUB_BOOTSTRAP_KEY: "flowhub_bootstrap",
    }
    MODELS: Dict[str, Type[BaseModel]] = {
        SCHEDULER_KEY: SchedulerDefaultsModel,
        STRATEGY_PLAN_KEY: StrategyPlanDefaultsModel,
        FLOWHUB_BOOTSTRAP_KEY: FlowhubBootstrapDefaultsModel,
    }

    def __init__(self, config, system_api: Optional[Any] = None):
        if system_api is None and (UISystemDataAPI is None or create_database_manager is None):
            raise RuntimeError("econdb runtime dependencies are unavailable for control-plane settings")
        self._config = config
        self._last_summary: Dict[str, Dict[str, Any]] = {}
        if system_api is not None:
            self._api = system_api
        else:
            econdb_cfg = getattr(config, "econdb", None)
            if econdb_cfg is None:
                raise RuntimeError("Brain control-plane settings require explicit econdb runtime settings")
            db_manager = create_database_manager(econdb_cfg.econdb_override())
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

    @staticmethod
    def _audit_dto(action: str, key: str, payload: Dict[str, Any], actor_id: str) -> Any:
        if SystemAuditDTO is None:
            return SimpleNamespace(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action=action,
                target_type="setting",
                target_id=key,
                payload=payload,
                created_at=datetime.utcnow().isoformat(),
            )
        return SystemAuditDTO(
            id=str(uuid.uuid4()),
            actor_id=actor_id,
            action=action,
            target_type="setting",
            target_id=key,
            payload=payload,
            created_at=datetime.utcnow().isoformat(),
        )

    def is_managed_key(self, key: str) -> bool:
        return key in self.MODELS

    def last_startup_summary(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._last_summary)

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

    def _defaults_for_key(self, key: str) -> Dict[str, Any]:
        if key == self.SCHEDULER_KEY:
            return self._scheduler_env_defaults()
        if key == self.STRATEGY_PLAN_KEY:
            return self._strategy_plan_env_defaults()
        if key == self.FLOWHUB_BOOTSTRAP_KEY:
            return self._flowhub_bootstrap_env_defaults()
        raise ValueError(f"Unsupported control-plane setting key: {key}")

    def _validate_payload(self, key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        model = self.MODELS.get(key)
        if model is None:
            raise ValueError(f"Unsupported control-plane setting key: {key}")
        try:
            return model.model_validate(payload).model_dump()
        except ValidationError as exc:
            raise ValueError(f"Invalid control-plane payload for {key}: {exc}") from exc

    def _metadata_payload(
        self,
        existing_metadata: Dict[str, Any],
        *,
        seeded_from: str,
        normalized_from_legacy: bool,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata = {
            key: value
            for key, value in dict(existing_metadata or {}).items()
            if key not in self.REQUIRED_METADATA_FIELDS
        }
        for key, value in dict(extra_metadata or {}).items():
            if key not in self.REQUIRED_METADATA_FIELDS:
                metadata[key] = value
        metadata.update(
            {
                "schema_version": self.SCHEMA_VERSION,
                "managed_by": self.MANAGED_BY,
                "seeded_from": seeded_from,
                "normalized_from_legacy": normalized_from_legacy,
            }
        )
        return metadata

    async def _write_setting(self, key: str, value: Dict[str, Any], metadata: Dict[str, Any], updated_by: str) -> Dict[str, Any]:
        return await asyncio.to_thread(
            self._api.upsert_setting,
            self._setting_dto(
                key=key,
                value=value,
                metadata=metadata,
                updated_by=updated_by,
            ),
        )

    async def _append_audit(self, action: str, key: str, payload: Dict[str, Any], actor_id: str) -> None:
        if not hasattr(self._api, "append_audit_log"):
            return
        audit = self._audit_dto(action, key, payload, actor_id)
        await asyncio.to_thread(self._api.append_audit_log, audit)

    async def _resolve_setting(self, key: str) -> ControlPlaneResolution:
        defaults = self._validate_payload(key, self._defaults_for_key(key))
        section = self.SECTION_NAMES[key]
        existing = await asyncio.to_thread(self._api.get_setting, key)
        if existing is None:
            metadata = self._metadata_payload(
                {},
                seeded_from="brain_env_defaults",
                normalized_from_legacy=False,
            )
            await self._write_setting(key, defaults, metadata, "system")
            await self._append_audit(
                "control_plane.seed",
                key,
                {
                    "section": section,
                    "schema_version": self.SCHEMA_VERSION,
                    "source": "brain_env_defaults",
                },
                "system",
            )
            return ControlPlaneResolution(
                key=key,
                section=section,
                payload=defaults,
                metadata=metadata,
                source="seeded",
                normalized_from_legacy=False,
            )

        raw_value = existing.get("value")
        if not isinstance(raw_value, dict):
            raise ValueError(f"Invalid control-plane payload for {key}: value must be a JSON object")

        existing_metadata = existing.get("metadata")
        metadata_dict = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
        normalized_from_legacy = bool(metadata_dict.get("normalized_from_legacy", False))
        if not isinstance(existing_metadata, dict):
            normalized_from_legacy = True

        seeded_from = str(metadata_dict.get("seeded_from") or "").strip() or "db_existing"
        if seeded_from not in {"brain_env_defaults", "db_existing"}:
            seeded_from = "db_existing"
            normalized_from_legacy = True
        if metadata_dict.get("schema_version") != self.SCHEMA_VERSION:
            normalized_from_legacy = True
        if metadata_dict.get("managed_by") != self.MANAGED_BY:
            normalized_from_legacy = True

        merged = dict(defaults)
        merged.update(raw_value)
        normalized_payload = self._validate_payload(key, merged)
        if normalized_payload != raw_value:
            normalized_from_legacy = True

        target_metadata = self._metadata_payload(
            metadata_dict,
            seeded_from=seeded_from,
            normalized_from_legacy=normalized_from_legacy,
        )
        if normalized_payload != raw_value or target_metadata != metadata_dict:
            updated_by = str(existing.get("updated_by") or "system")
            await self._write_setting(key, normalized_payload, target_metadata, updated_by)
            await self._append_audit(
                "control_plane.normalize",
                key,
                {
                    "section": section,
                    "schema_version": self.SCHEMA_VERSION,
                    "normalized_from_legacy": normalized_from_legacy,
                },
                updated_by,
            )

        return ControlPlaneResolution(
            key=key,
            section=section,
            payload=normalized_payload,
            metadata=target_metadata,
            source="db",
            normalized_from_legacy=normalized_from_legacy,
        )

    async def ensure_seeded(self) -> Dict[str, Dict[str, Any]]:
        return await self.snapshot()

    async def get_scheduler_defaults(self) -> Dict[str, Any]:
        return (await self._resolve_setting(self.SCHEDULER_KEY)).payload

    async def get_strategy_plan_defaults(self) -> Dict[str, Any]:
        return (await self._resolve_setting(self.STRATEGY_PLAN_KEY)).payload

    async def get_flowhub_bootstrap_defaults(self) -> Dict[str, Any]:
        return (await self._resolve_setting(self.FLOWHUB_BOOTSTRAP_KEY)).payload

    async def upsert_setting(
        self,
        key: str,
        value: Dict[str, Any],
        *,
        updated_by: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.is_managed_key(key):
            raise ValueError(f"Unsupported control-plane setting key: {key}")
        if not isinstance(value, dict):
            raise ValueError("Control-plane setting value must be a JSON object")
        defaults = self._validate_payload(key, self._defaults_for_key(key))
        existing = await asyncio.to_thread(self._api.get_setting, key)
        if existing is not None and not isinstance(existing.get("value"), dict):
            raise ValueError(f"Invalid control-plane payload for {key}: value must be a JSON object")
        merged_payload = dict(defaults)
        if existing is not None and isinstance(existing.get("value"), dict):
            merged_payload.update(existing.get("value") or {})
        merged_payload.update(value)
        validated_payload = self._validate_payload(key, merged_payload)
        existing_metadata = existing.get("metadata") if isinstance(existing, dict) and isinstance(existing.get("metadata"), dict) else {}
        seeded_from = str(existing_metadata.get("seeded_from") or "").strip() or "db_existing"
        if seeded_from not in {"brain_env_defaults", "db_existing"}:
            seeded_from = "db_existing"
        target_metadata = self._metadata_payload(
            dict(existing_metadata),
            seeded_from=seeded_from,
            normalized_from_legacy=bool(existing_metadata.get("normalized_from_legacy", False)),
            extra_metadata=metadata,
        )
        setting_payload = await self._write_setting(key, validated_payload, target_metadata, updated_by)
        await self._append_audit(
            "control_plane.upsert",
            key,
            {
                "section": self.SECTION_NAMES[key],
                "schema_version": self.SCHEMA_VERSION,
                "normalized_from_legacy": bool(target_metadata.get("normalized_from_legacy")),
            },
            updated_by,
        )
        return setting_payload

    async def snapshot(self) -> Dict[str, Dict[str, Any]]:
        resolutions = await asyncio.gather(
            self._resolve_setting(self.SCHEDULER_KEY),
            self._resolve_setting(self.STRATEGY_PLAN_KEY),
            self._resolve_setting(self.FLOWHUB_BOOTSTRAP_KEY),
        )
        self._last_summary = {
            item.section: {
                "key": item.key,
                "source": item.source,
                "schema_version": str(item.metadata.get("schema_version") or self.SCHEMA_VERSION),
                "normalized_from_legacy": bool(item.metadata.get("normalized_from_legacy", False)),
            }
            for item in resolutions
        }
        return {item.section: item.payload for item in resolutions}
