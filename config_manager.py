"""Brain 配置管理器。"""

from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from config import IntegrationConfig, get_settings
from config_revision import (
    append_revision,
    blank_revision_envelope,
    make_revision_entry,
    next_revision,
    normalize_revision_envelope,
    revision_entry,
    revision_history,
)

try:
    from econdb import UISystemDataAPI, SystemSettingDTO, create_database_manager
except Exception:  # pragma: no cover - surfaced by caller
    UISystemDataAPI = None  # type: ignore[assignment]
    SystemSettingDTO = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]


DYNAMIC_KEY = "config.service.brain.dynamic"
SCHEMA_VERSION = "v1"
MANAGED_BY = "BrainConfigManager"
SERVICE_NAME = "brain"
REVISION_SCHEMA_VERSION = "v2"


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _deep_get(mapping: Dict[str, Any], dotted_key: str, default: Any = None) -> Any:
    current: Any = mapping
    for part in dotted_key.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def _changed_keys(before: Dict[str, Any], after: Dict[str, Any], prefix: str = "") -> List[str]:
    changes: List[str] = []
    keys = sorted(set(before.keys()) | set(after.keys()))
    for key in keys:
        dotted = f"{prefix}.{key}" if prefix else key
        before_value = before.get(key)
        after_value = after.get(key)
        if isinstance(before_value, dict) and isinstance(after_value, dict):
            changes.extend(_changed_keys(before_value, after_value, dotted))
        elif before_value != after_value:
            changes.append(dotted)
    return changes


def _catalog_item(
    key: str,
    *,
    scope: str,
    type_name: str,
    category: str,
    default: Any,
    description: str,
    mutable: bool,
    secret: bool,
    restart_required: bool,
    hot_reloadable: bool,
    ui_group: str,
) -> Dict[str, Any]:
    return {
        "key": key,
        "scope": scope,
        "type": type_name,
        "category": category,
        "default": default,
        "description": description,
        "mutable": mutable,
        "secret": secret,
        "restart_required": restart_required,
        "hot_reloadable": hot_reloadable,
        "ui_group": ui_group,
    }


class BrainMonitoringDynamicConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enable_system_monitoring: bool = True


class BrainFeatureFlagsDynamicConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    config_proxy_enabled: bool = True


class BrainServiceDynamicSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    monitoring: BrainMonitoringDynamicConfig = Field(default_factory=BrainMonitoringDynamicConfig)
    feature_flags: BrainFeatureFlagsDynamicConfig = Field(default_factory=BrainFeatureFlagsDynamicConfig)


CATALOG_ITEMS: List[Dict[str, Any]] = [
    _catalog_item(
        "service.host",
        scope="runtime",
        type_name="string",
        category="service",
        default="0.0.0.0",
        description="Brain HTTP 监听地址。",
        mutable=False,
        secret=False,
        restart_required=True,
        hot_reloadable=False,
        ui_group="service",
    ),
    _catalog_item(
        "service.port",
        scope="runtime",
        type_name="integer",
        category="service",
        default=8088,
        description="Brain HTTP 监听端口。",
        mutable=False,
        secret=False,
        restart_required=True,
        hot_reloadable=False,
        ui_group="service",
    ),
    _catalog_item(
        "database.url",
        scope="runtime",
        type_name="dsn",
        category="database",
        default="",
        description="Brain 主数据库 DSN。",
        mutable=False,
        secret=True,
        restart_required=True,
        hot_reloadable=False,
        ui_group="database",
    ),
    _catalog_item(
        "redis.url",
        scope="runtime",
        type_name="dsn",
        category="redis",
        default="",
        description="Brain Redis DSN。",
        mutable=False,
        secret=True,
        restart_required=True,
        hot_reloadable=False,
        ui_group="redis",
    ),
    _catalog_item(
        "monitoring.enable_system_monitoring",
        scope="dynamic",
        type_name="boolean",
        category="monitoring",
        default=True,
        description="Brain 系统监控开关。",
        mutable=True,
        secret=False,
        restart_required=False,
        hot_reloadable=True,
        ui_group="monitoring",
    ),
    _catalog_item(
        "feature_flags.config_proxy_enabled",
        scope="dynamic",
        type_name="boolean",
        category="feature_flags",
        default=True,
        description="Brain 配置代理总开关。",
        mutable=False,
        secret=False,
        restart_required=False,
        hot_reloadable=False,
        ui_group="feature_flags",
    ),
    _catalog_item(
        "control_plane.scheduler",
        scope="dynamic",
        type_name="object",
        category="control_plane",
        default={},
        description="Brain scheduler control-plane defaults。",
        mutable=True,
        secret=False,
        restart_required=False,
        hot_reloadable=True,
        ui_group="control_plane",
    ),
    _catalog_item(
        "control_plane.strategy_plan",
        scope="dynamic",
        type_name="object",
        category="control_plane",
        default={},
        description="Brain strategy plan control-plane defaults。",
        mutable=True,
        secret=False,
        restart_required=False,
        hot_reloadable=True,
        ui_group="control_plane",
    ),
    _catalog_item(
        "control_plane.flowhub_bootstrap",
        scope="dynamic",
        type_name="object",
        category="control_plane",
        default={},
        description="Brain flowhub bootstrap control-plane defaults。",
        mutable=True,
        secret=False,
        restart_required=False,
        hot_reloadable=True,
        ui_group="control_plane",
    ),
]


class BrainConfigManager:
    def __init__(
        self,
        config: Optional[IntegrationConfig] = None,
        app: Optional[Any] = None,
        system_api: Optional[Any] = None,
        allow_store_fallback: bool = False,
    ):
        self.config = config
        self._app = app
        self._system_api = system_api
        self._allow_store_fallback = allow_store_fallback
        self._service_dynamic = BrainServiceDynamicSettings()
        self._dynamic_envelope = blank_revision_envelope()
        self.logger = logging.getLogger(__name__)

    def _runtime_dict(self) -> Dict[str, Any]:
        config = self.config or get_settings()
        return config.to_dict()

    def _dynamic_defaults(self) -> Dict[str, Any]:
        config = self.config or get_settings()
        return {
            "monitoring": {
                "enable_system_monitoring": bool(config.monitoring.enable_system_monitoring),
            },
            "feature_flags": {
                "config_proxy_enabled": True,
            },
        }

    def _metadata(self, normalized_from_legacy: bool = False) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "managed_by": MANAGED_BY,
            "service": SERVICE_NAME,
            "config_kind": "dynamic",
            "normalized_from_legacy": normalized_from_legacy,
        }

    def _get_system_api(self) -> Optional[Any]:
        if self._system_api is False:
            return None
        if self._system_api is not None:
            return self._system_api
        if UISystemDataAPI is None or create_database_manager is None:
            if self._allow_store_fallback:
                self.logger.warning("Brain service dynamic config store unavailable; falling back to defaults for explicit test mode")
                self._system_api = False
                return None
            raise RuntimeError("Brain dynamic config store is unavailable in standard runtime")
        config = self.config or get_settings()
        try:
            self._system_api = UISystemDataAPI(create_database_manager(config.econdb.econdb_override()))
        except Exception as exc:
            if self._allow_store_fallback:
                self.logger.warning("Brain service dynamic config store unavailable: %s; falling back to defaults for explicit test mode", exc)
                self._system_api = False
                return None
            raise RuntimeError("Brain dynamic config store is unavailable in standard runtime") from exc
        return self._system_api

    @staticmethod
    def _setting_dto(key: str, value: Dict[str, Any], metadata: Dict[str, Any], updated_by: str) -> Any:
        if SystemSettingDTO is not None:
            return SystemSettingDTO(key=key, value=value, metadata=metadata, updated_by=updated_by)
        return type("_Setting", (), {"key": key, "value": value, "metadata": metadata, "updated_by": updated_by})()

    def _control_plane_service(self) -> Any:
        if self._app is None:
            raise RuntimeError("Brain control-plane settings service is not available")
        service = self._app.get("control_plane_settings")
        if service is None:
            raise RuntimeError("Brain control-plane settings service is not initialized")
        return service

    async def _load_service_dynamic(self) -> BrainServiceDynamicSettings:
        api = self._get_system_api()
        defaults = self._dynamic_defaults()
        control_plane_defaults = await self._control_plane_service().snapshot()
        full_defaults = {**defaults, "control_plane": control_plane_defaults}
        if api is None:
            validated = BrainServiceDynamicSettings.model_validate(defaults)
            self._dynamic_envelope = append_revision(
                blank_revision_envelope(),
                make_revision_entry(
                    revision=1,
                    value={**validated.model_dump(), "control_plane": control_plane_defaults},
                    updated_by="system",
                    source="defaults",
                    reason="fallback_defaults",
                    event="seed",
                ),
            )
            return validated
        existing = await asyncio.to_thread(api.get_setting, DYNAMIC_KEY)
        if existing is None:
            validated = BrainServiceDynamicSettings.model_validate(defaults)
            envelope = append_revision(
                blank_revision_envelope(),
                make_revision_entry(
                    revision=1,
                    value={**validated.model_dump(), "control_plane": control_plane_defaults},
                    updated_by="system",
                    source="seed",
                    reason="defaults",
                    event="seed",
                ),
            )
            await asyncio.to_thread(
                api.upsert_setting,
                self._setting_dto(DYNAMIC_KEY, envelope, self._metadata(False), "system"),
            )
            self._dynamic_envelope = envelope
            return validated
        raw_value = existing.get("value")
        if not isinstance(raw_value, dict):
            raise ValueError("Brain dynamic config payload must be a JSON object")
        normalized_from_legacy = not isinstance(existing.get("metadata"), dict)
        validated_dict, envelope, normalized = normalize_revision_envelope(
            raw_value,
            defaults=full_defaults,
            validate_payload=lambda payload: {
                **BrainServiceDynamicSettings.model_validate(
                    {key: value for key, value in payload.items() if key != "control_plane"}
                ).model_dump(),
                "control_plane": deepcopy(payload.get("control_plane") if isinstance(payload.get("control_plane"), dict) else control_plane_defaults),
            },
            legacy_updated_by=str(existing.get("updated_by") or "system"),
        )
        validated = BrainServiceDynamicSettings.model_validate(
            {key: value for key, value in validated_dict.items() if key != "control_plane"}
        )
        if normalized or normalized_from_legacy:
            await asyncio.to_thread(
                api.upsert_setting,
                self._setting_dto(
                    DYNAMIC_KEY,
                    envelope,
                    self._metadata(True),
                    str(existing.get("updated_by") or "system"),
                ),
            )
        self._dynamic_envelope = envelope
        return validated

    def _apply_service_dynamic(self) -> None:
        if self.config is None:
            return
        payload = self._service_dynamic.model_dump()
        self.config.monitoring.enable_system_monitoring = bool(payload["monitoring"]["enable_system_monitoring"])
        if self._app is None:
            return
        monitor = self._app.get("system_monitor")
        if monitor:
            monitor.config.monitoring.enable_system_monitoring = self.config.monitoring.enable_system_monitoring
        self._app["brain_config_features"] = payload["feature_flags"]

    async def initialize(self) -> None:
        if self.config is None:
            self.config = get_settings()
        self._service_dynamic = await self._load_service_dynamic()
        self._apply_service_dynamic()
        if self._app is not None:
            self._app["config_manager"] = self
            self._app["dynamic_config"] = await self.dynamic_snapshot(masked=True)

    def runtime_snapshot(self, masked: bool = True) -> Dict[str, Any]:
        config = self.config or get_settings()
        return config.masked_summary() if masked else self._runtime_dict()

    async def dynamic_snapshot(self, masked: bool = True) -> Dict[str, Any]:
        return {
            **self._service_dynamic.model_dump(),
            "control_plane": await self._control_plane_service().snapshot(),
        }

    def dynamic_history(self) -> Dict[str, Any]:
        return {
            "service": SERVICE_NAME,
            "current_revision": int(self._dynamic_envelope.get("current_revision") or 0),
            "items": revision_history(self._dynamic_envelope),
        }

    def catalog(self) -> Dict[str, Any]:
        return {"service": SERVICE_NAME, "schema_version": SCHEMA_VERSION, "items": deepcopy(CATALOG_ITEMS)}

    def config_proxy_enabled(self) -> bool:
        return bool(self._service_dynamic.feature_flags.config_proxy_enabled)

    async def get_item(self, key: str) -> Dict[str, Any]:
        runtime = self._runtime_dict()
        dynamic = await self.dynamic_snapshot(masked=False)
        runtime_value = _deep_get(runtime, key)
        if runtime_value is not None:
            return {"scope": "runtime", "key": key, "value": runtime_value}
        dynamic_value = _deep_get(dynamic, key)
        if dynamic_value is not None:
            return {"scope": "dynamic", "key": key, "value": dynamic_value}
        raise KeyError(key)

    async def update_dynamic_config(self, changes: Dict[str, Any], actor_id: str, source: str, expected_revision: Optional[int] = None, reason: str = "") -> Dict[str, Any]:
        if not isinstance(changes, dict) or not changes:
            raise ValueError("changes must be a non-empty JSON object")
        if expected_revision is not None and int(self._dynamic_envelope.get("current_revision") or 0) != int(expected_revision):
            raise ValueError("Revision mismatch")

        before = await self.dynamic_snapshot(masked=False)
        hot_reloaded_keys: List[str] = []
        restart_required_keys: List[str] = []

        if "control_plane" in changes:
            control_plane_changes = changes.get("control_plane")
            if not isinstance(control_plane_changes, dict):
                raise ValueError("control_plane must be a JSON object")
            control_plane_service = self._control_plane_service()
            for key, value in control_plane_changes.items():
                if key == "scheduler":
                    await control_plane_service.upsert_setting(
                        control_plane_service.SCHEDULER_KEY,
                        value,
                        updated_by=actor_id,
                        metadata={"source": source},
                    )
                    hot_reloaded_keys.append("control_plane.scheduler")
                elif key == "strategy_plan":
                    await control_plane_service.upsert_setting(
                        control_plane_service.STRATEGY_PLAN_KEY,
                        value,
                        updated_by=actor_id,
                        metadata={"source": source},
                    )
                    hot_reloaded_keys.append("control_plane.strategy_plan")
                elif key == "flowhub_bootstrap":
                    await control_plane_service.upsert_setting(
                        control_plane_service.FLOWHUB_BOOTSTRAP_KEY,
                        value,
                        updated_by=actor_id,
                        metadata={"source": source},
                    )
                    hot_reloaded_keys.append("control_plane.flowhub_bootstrap")
                else:
                    raise ValueError(f"Unsupported brain control-plane section: {key}")

        service_changes = {key: value for key, value in changes.items() if key != "control_plane"}
        if service_changes:
            feature_flags = service_changes.get("feature_flags")
            if isinstance(feature_flags, dict) and "config_proxy_enabled" in feature_flags:
                raise ValueError("feature_flags.config_proxy_enabled is reserved and cannot be updated via the config platform")
            merged = _deep_merge(self._service_dynamic.model_dump(), service_changes)
            try:
                validated = BrainServiceDynamicSettings.model_validate(merged)
            except ValidationError as exc:
                raise ValueError(f"Invalid brain dynamic config update: {exc}") from exc
            self._service_dynamic = validated
            self._apply_service_dynamic()
            hot_reloaded_keys.extend(_changed_keys(before, await self.dynamic_snapshot(masked=False)))

        after = await self.dynamic_snapshot(masked=False)
        changed_keys = _changed_keys(before, after)
        api = self._get_system_api()
        envelope = append_revision(
            deepcopy(self._dynamic_envelope),
            make_revision_entry(
                revision=next_revision(self._dynamic_envelope),
                value=after,
                updated_by=actor_id,
                source=source,
                reason=reason,
                updated_keys=changed_keys,
                hot_reloaded_keys=sorted(set(hot_reloaded_keys)),
                restart_required_keys=restart_required_keys,
            ),
        )
        if api is not None:
            await asyncio.to_thread(
                api.upsert_setting,
                self._setting_dto(
                    DYNAMIC_KEY,
                    envelope,
                    {**self._metadata(False), "updated_by": actor_id, "source": source},
                    actor_id,
                ),
            )
        self._dynamic_envelope = envelope
        return {
            "applied": True,
            "current_revision": int(self._dynamic_envelope.get("current_revision") or 0),
            "updated_keys": changed_keys,
            "hot_reloaded_keys": sorted(set(hot_reloaded_keys)),
            "restart_required": bool(restart_required_keys),
            "restart_required_keys": restart_required_keys,
        }

    async def rollback_dynamic_config(self, revision: int, actor_id: str, source: str, reason: str = "") -> Dict[str, Any]:
        entry = revision_entry(self._dynamic_envelope, revision)
        if not entry:
            raise ValueError("Revision not found")
        before = await self.dynamic_snapshot(masked=False)
        payload = deepcopy(entry.get("value") or {})
        control_plane_payload = payload.get("control_plane") if isinstance(payload.get("control_plane"), dict) else {}
        service_payload = {key: value for key, value in payload.items() if key != "control_plane"}
        restored = BrainServiceDynamicSettings.model_validate(service_payload)
        control_plane_service = self._control_plane_service()
        if "scheduler" in control_plane_payload:
            await control_plane_service.upsert_setting(
                control_plane_service.SCHEDULER_KEY,
                control_plane_payload["scheduler"],
                updated_by=actor_id,
                metadata={"source": source},
            )
        if "strategy_plan" in control_plane_payload:
            await control_plane_service.upsert_setting(
                control_plane_service.STRATEGY_PLAN_KEY,
                control_plane_payload["strategy_plan"],
                updated_by=actor_id,
                metadata={"source": source},
            )
        if "flowhub_bootstrap" in control_plane_payload:
            await control_plane_service.upsert_setting(
                control_plane_service.FLOWHUB_BOOTSTRAP_KEY,
                control_plane_payload["flowhub_bootstrap"],
                updated_by=actor_id,
                metadata={"source": source},
            )
        self._service_dynamic = restored
        self._apply_service_dynamic()
        after = await self.dynamic_snapshot(masked=False)
        changed_keys = _changed_keys(before, after)
        hot_reloaded_keys = sorted(changed_keys)
        envelope = append_revision(
            deepcopy(self._dynamic_envelope),
            make_revision_entry(
                revision=next_revision(self._dynamic_envelope),
                value=after,
                updated_by=actor_id,
                source=source,
                reason=reason or f"rollback:{revision}",
                updated_keys=changed_keys,
                hot_reloaded_keys=hot_reloaded_keys,
                restart_required_keys=[],
                event="rollback",
            ),
        )
        api = self._get_system_api()
        if api is not None:
            await asyncio.to_thread(
                api.upsert_setting,
                self._setting_dto(
                    DYNAMIC_KEY,
                    envelope,
                    {**self._metadata(False), "updated_by": actor_id, "source": source},
                    actor_id,
                ),
            )
        self._dynamic_envelope = envelope
        return {
            "applied": True,
            "rolled_back_from_revision": int(revision),
            "current_revision": int(self._dynamic_envelope.get("current_revision") or 0),
            "updated_keys": changed_keys,
            "hot_reloaded_keys": hot_reloaded_keys,
            "restart_required": False,
            "restart_required_keys": [],
        }

    async def reload(self) -> Dict[str, Any]:
        previous_runtime = self._runtime_dict()
        self.config = get_settings(force_reload=True)
        if not self.config.validate():
            raise ValueError("Brain runtime configuration validation failed during reload")
        self._service_dynamic = await self._load_service_dynamic()
        self._apply_service_dynamic()
        if self._app is not None:
            self._app["config"] = self.config
            self._app["dynamic_config"] = await self.dynamic_snapshot(masked=True)
        next_runtime = self._runtime_dict()
        updated_sections = [
            section for section in next_runtime.keys() if next_runtime.get(section) != previous_runtime.get(section)
        ]
        restart_required_sections = [
            section for section in updated_sections if section in {"database", "econdb", "redis", "service", "db_schema"}
        ]
        return {
            "applied": True,
            "updated_sections": updated_sections,
            "restart_required_sections": restart_required_sections,
            "service_name": SERVICE_NAME,
        }
