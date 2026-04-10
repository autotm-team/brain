"""Curated Brain-owned system settings for the active settings UI."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SettingSpec:
    key: str
    default: Any
    kind: str
    description: str
    consumer: str
    choices: tuple[str, ...] = ()


class BrainSystemSettingsService:
    """Restrict `/api/v1/ui/system/settings*` to curated Brain-owned settings."""

    STORAGE_SCHEMA_VERSION = "v2"
    HISTORY_LIMIT = 50
    DEFAULT_WORKSPACE_ID = "default"
    DEFAULT_TIMEZONE = "Asia/Shanghai"

    SPECS: Dict[str, SettingSpec] = {
        "system.workspace": SettingSpec(
            key="system.workspace",
            default="Alpha Lab",
            kind="string",
            description="工作区名称，用于壳层上下文展示。",
            consumer="brain.shell_context",
        ),
        "system.name": SettingSpec(
            key="system.name",
            default="AutoTM Quant Research",
            kind="string",
            description="系统/工作台名称，用于壳层标题展示。",
            consumer="brain.shell_context",
        ),
        "system.locale": SettingSpec(
            key="system.locale",
            default="zh-CN",
            kind="choice",
            description="默认 locale，用于壳层上下文展示。",
            consumer="brain.shell_context",
            choices=("zh-CN", "en-US"),
        ),
        "system.timezone": SettingSpec(
            key="system.timezone",
            default="Asia/Shanghai",
            kind="choice",
            description="默认时区，用于壳层上下文与 Brain 默认时间语义。",
            consumer="brain.shell_context",
            choices=("Asia/Shanghai", "Asia/Singapore", "UTC"),
        ),
        "defaults.benchmark": SettingSpec(
            key="defaults.benchmark",
            default="CSI300",
            kind="choice",
            description="Brain 默认 benchmark。",
            consumer="brain.default_request_params",
            choices=("CSI300", "CSI500", "SSE50"),
        ),
        "defaults.scale": SettingSpec(
            key="defaults.scale",
            default="D1",
            kind="choice",
            description="Brain 默认 scale。",
            consumer="brain.default_request_params",
            choices=("D1", "W1"),
        ),
        "defaults.window": SettingSpec(
            key="defaults.window",
            default="180D",
            kind="choice",
            description="Brain 默认 lookback window。",
            consumer="brain.default_request_params",
            choices=("120D", "180D", "252D"),
        ),
        "defaults.inbox_days": SettingSpec(
            key="defaults.inbox_days",
            default=7,
            kind="integer",
            description="候选池/研究默认收件箱天数。",
            consumer="brain.default_request_params",
        ),
        "defaults.fill_mode": SettingSpec(
            key="defaults.fill_mode",
            default="open_next",
            kind="choice",
            description="Brain 默认撮合模式。",
            consumer="brain.default_task_payload",
            choices=("open_next", "close_same_day", "vwap"),
        ),
    }

    MARKET_QUERY_PATHS = {
        "/api/v1/ui/market-snapshot/latest",
        "/api/v1/ui/market-snapshot/list",
        "/api/v1/ui/market-snapshot/alerts",
    }
    MARKET_QUERY_PREFIXES = (
        "/api/v1/ui/market-snapshot/",
    )
    MACRO_QUERY_PATHS = {
        "/api/v1/ui/macro-cycle/inbox",
        "/api/v1/ui/macro-cycle/history",
        "/api/v1/ui/macro-cycle/calendar",
    }
    STRUCTURE_QUERY_PREFIXES = (
        "/api/v1/ui/structure-rotation/",
    )
    CANDIDATE_INBOX_PATHS = {
        "/api/v1/ui/candidates/clusters",
        "/api/v1/ui/candidates/events",
    }
    STRATEGY_MUTATION_PATHS = {
        "/api/v1/ui/strategy/reports/run",
        "/api/v1/ui/strategy/config/apply",
    }

    def is_public_key(self, key: str) -> bool:
        return key in self.SPECS

    def list_settings(self, api: Any, context: Optional[Dict[str, Any]] = None) -> list[Dict[str, Any]]:
        return [self.get_setting(api, key, context=context) for key in self.SPECS]

    def get_setting(self, api: Any, key: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        spec = self.SPECS[key]
        row = api.get_setting(key)
        envelope, should_normalize = self._read_envelope(spec, row)
        envelope, promoted = self._promote_due_pending(envelope)
        if isinstance(row, dict) and (should_normalize or promoted):
            self._persist_envelope(api, key, envelope, row.get("updated_by") or "system", row.get("metadata"))
        resolved = self._resolve_entry(spec, envelope, context=context)
        metadata = self._response_metadata(spec, envelope, resolved)
        return {
            "key": key,
            "value": {"value": resolved["value"]},
            "metadata": metadata,
            "updated_by": resolved.get("updated_by"),
            "created_at": resolved.get("created_at"),
            "updated_at": resolved.get("updated_at"),
        }

    def normalize_update(self, key: str, value: Any) -> Dict[str, Any]:
        spec = self.SPECS[key]
        return self._normalize_value(spec, value)

    def shell_context_settings(self, api: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        snapshot = self.snapshot(api, context=context)
        return {
            "workspace": str(snapshot["system.workspace"]),
            "system_name": str(snapshot["system.name"]),
            "locale": str(snapshot["system.locale"]),
            "timezone": str(snapshot["system.timezone"]),
        }

    def apply_query_defaults(self, path: str, query: Dict[str, Any], api: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        snapshot = self.snapshot(api, context=context)
        next_query = dict(query or {})

        if path in self.MARKET_QUERY_PATHS or any(path.startswith(prefix) and path.endswith("/compare") for prefix in self.MARKET_QUERY_PREFIXES):
            next_query.setdefault("benchmark", snapshot["defaults.benchmark"])
            next_query.setdefault("scale", snapshot["defaults.scale"])

        if path in self.MACRO_QUERY_PATHS:
            next_query.setdefault("benchmark", snapshot["defaults.benchmark"])
            next_query.setdefault("scale", snapshot["defaults.scale"])

        if any(path.startswith(prefix) for prefix in self.STRUCTURE_QUERY_PREFIXES):
            next_query.setdefault("benchmark", snapshot["defaults.benchmark"])
            next_query.setdefault("scale", snapshot["defaults.scale"])

        if path in self.CANDIDATE_INBOX_PATHS and "from" not in next_query and "to" not in next_query:
            days = max(1, int(snapshot["defaults.inbox_days"]))
            today = datetime.utcnow().date()
            next_query["to"] = today.isoformat()
            next_query["from"] = (today - timedelta(days=days)).isoformat()

        return next_query

    def apply_mutation_defaults(self, path: str, payload: Dict[str, Any], api: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if path not in self.STRATEGY_MUTATION_PATHS:
            return dict(payload or {})
        snapshot = self.snapshot(api, context=context)
        next_payload = dict(payload or {})
        config = dict(next_payload.get("config") or {})

        config.setdefault("benchmark", snapshot["defaults.benchmark"])
        config.setdefault("fill", snapshot["defaults.fill_mode"])
        if not config.get("from") and not config.get("to"):
            days = self._window_days(str(snapshot["defaults.window"]))
            end = datetime.utcnow().date()
            config["to"] = end.isoformat()
            config["from"] = (end - timedelta(days=days)).isoformat()

        next_payload["config"] = config
        return next_payload

    def snapshot(self, api: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        values: Dict[str, Any] = {}
        for spec in self.SPECS.values():
            setting = self.get_setting(api, spec.key, context=context)
            values[spec.key] = self._extract_scalar(setting.get("value"), spec.default)
        return values

    def upsert_setting(
        self,
        api: Any,
        key: str,
        value: Any,
        *,
        updated_by: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        spec = self.SPECS[key]
        row = api.get_setting(key)
        envelope, _ = self._read_envelope(spec, row)
        envelope, _ = self._promote_due_pending(envelope)
        normalized_scalar = self._extract_scalar(self._normalize_value(spec, value), spec.default)
        request_meta = dict(metadata or {})
        scope_meta = self._scope_meta(request_meta, updated_by)
        apply_mode = self._normalize_apply_mode(request_meta.get("apply_mode"))
        expected_revision = request_meta.get("expected_revision")
        if expected_revision is not None:
            try:
                expected_revision = int(expected_revision)
            except Exception as exc:
                raise ValueError("expected_revision must be an integer") from exc
        target = self._entry_bucket(envelope, scope_meta["scope"], scope_meta["scope_id"])
        active = target.get("active") if isinstance(target.get("active"), dict) else None
        if expected_revision is not None and int((active or {}).get("revision") or 0) != expected_revision:
            raise ValueError("Revision mismatch")

        next_revision = self._next_revision(target)
        now = self._now_iso()
        entry = {
            "value": normalized_scalar,
            "revision": next_revision,
            "updated_by": str(updated_by),
            "updated_at": now,
            "created_at": active.get("created_at", now) if isinstance(active, dict) else now,
            "scope": scope_meta["scope"],
            "scope_id": scope_meta["scope_id"],
            "apply_mode": apply_mode,
            "reason": str(request_meta.get("reason") or ""),
        }
        if apply_mode == "immediate":
            if isinstance(active, dict):
                self._append_history(target, {**active, "status": "superseded"})
            entry["status"] = "active"
            entry["effective_at"] = now
            target["active"] = entry
        else:
            entry["status"] = "pending"
            entry["effective_at"] = self._resolve_effective_at(request_meta)
            pending = [item for item in self._pending_entries(target) if not self._same_pending_slot(item, entry)]
            pending.append(entry)
            target["pending"] = sorted(pending, key=lambda item: str(item.get("effective_at") or ""))
        self._persist_envelope(api, key, envelope, updated_by, row.get("metadata") if isinstance(row, dict) else None)
        return self.get_setting(api, key, context={"user_id": updated_by, "workspace_id": scope_meta["scope_id"]})

    def list_history(self, api: Any, key: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        spec = self.SPECS[key]
        row = api.get_setting(key)
        envelope, should_normalize = self._read_envelope(spec, row)
        envelope, promoted = self._promote_due_pending(envelope)
        if isinstance(row, dict) and (should_normalize or promoted):
            self._persist_envelope(api, key, envelope, row.get("updated_by") or "system", row.get("metadata"))
        items: list[Dict[str, Any]] = []
        scope_filter = None
        if isinstance(context, dict) and context.get("scope"):
            scope = str(context.get("scope") or "workspace")
            if scope == "global":
                scope_filter = ("global", "global")
            elif scope == "user":
                scope_filter = ("user", str(context.get("user_id") or "anonymous"))
            else:
                scope_filter = ("workspace", str(context.get("workspace_id") or self.DEFAULT_WORKSPACE_ID))

        buckets = dict(envelope.get("entries") or {})
        iterable = []
        if scope_filter is not None:
            token = self._scope_token(scope_filter[0], scope_filter[1])
            iterable = [(token, buckets.get(token))]
        else:
            iterable = list(buckets.items())

        for scope_key, bucket in iterable:
            if not isinstance(bucket, dict):
                continue
            if isinstance(bucket.get("active"), dict):
                items.append({**deepcopy(bucket["active"]), "scope_key": scope_key})
            for item in self._pending_entries(bucket):
                items.append({**deepcopy(item), "scope_key": scope_key})
            for item in list(bucket.get("history") or []):
                if isinstance(item, dict):
                    items.append({**deepcopy(item), "scope_key": scope_key})
        items.sort(key=lambda item: (int(item.get("revision") or 0), str(item.get("updated_at") or "")), reverse=True)
        return {"key": key, "items": items}

    def rollback_setting(
        self,
        api: Any,
        key: str,
        *,
        revision: int,
        updated_by: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        spec = self.SPECS[key]
        row = api.get_setting(key)
        envelope, _ = self._read_envelope(spec, row)
        envelope, _ = self._promote_due_pending(envelope)
        request_meta = dict(metadata or {})
        scope_meta = self._scope_meta(request_meta, updated_by)
        target = self._entry_bucket(envelope, scope_meta["scope"], scope_meta["scope_id"])
        entries = []
        if isinstance(target.get("active"), dict):
            entries.append(target["active"])
        entries.extend(self._pending_entries(target))
        entries.extend([item for item in list(target.get("history") or []) if isinstance(item, dict)])
        source = next((deepcopy(item) for item in entries if int(item.get("revision") or 0) == int(revision)), None)
        if source is None:
            raise ValueError("Revision not found")
        active = target.get("active") if isinstance(target.get("active"), dict) else None
        if isinstance(active, dict):
            self._append_history(target, {**active, "status": "rolled_back_from_active"})
        now = self._now_iso()
        rolled = {
            **source,
            "revision": self._next_revision(target),
            "updated_by": str(updated_by),
            "updated_at": now,
            "effective_at": now,
            "status": "active",
            "apply_mode": "immediate",
            "rolled_back_from_revision": int(revision),
            "reason": str(request_meta.get("reason") or f"rollback:{revision}"),
        }
        target["active"] = rolled
        self._persist_envelope(api, key, envelope, updated_by, row.get("metadata") if isinstance(row, dict) else None)
        return self.get_setting(api, key, context={"user_id": updated_by, "workspace_id": scope_meta["scope_id"]})

    def _synthetic_setting(self, spec: SettingSpec) -> Dict[str, Any]:
        return {
            "key": spec.key,
            "value": {"value": spec.default},
            "metadata": {
                "managed_by": "BrainSystemSettingsService",
                "consumer": spec.consumer,
                "value_type": spec.kind,
                "description": spec.description,
                "source": "default",
                "scope": "default",
                "current_revision": 0,
                "pending_count": 0,
            },
            "updated_by": "system",
            "created_at": None,
            "updated_at": None,
        }

    @staticmethod
    def _extract_scalar(value: Any, default: Any) -> Any:
        if isinstance(value, dict) and "value" in value:
            return value.get("value", default)
        if value is None:
            return default
        return value

    def _normalize_value(self, spec: SettingSpec, value: Any) -> Dict[str, Any]:
        raw = self._extract_scalar(value, spec.default)
        if spec.kind in {"string", "choice"}:
            normalized = str(raw or "").strip() or str(spec.default)
            if spec.kind == "choice" and spec.choices and normalized not in spec.choices:
                normalized = str(spec.default)
            return {"value": normalized}
        if spec.kind == "integer":
            try:
                normalized_int = int(raw)
            except Exception:
                normalized_int = int(spec.default)
            if spec.key == "defaults.inbox_days":
                normalized_int = max(1, normalized_int)
            return {"value": normalized_int}
        return {"value": spec.default}

    @staticmethod
    def _window_days(token: str) -> int:
        text = str(token or "").strip().upper()
        if not text:
            return 180
        if text.endswith("D"):
            try:
                return max(1, int(text[:-1]))
            except Exception:
                return 180
        return 180

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat()

    def _scope_meta(self, metadata: Dict[str, Any], updated_by: str) -> Dict[str, str]:
        scope = str(metadata.get("scope") or "workspace").strip().lower()
        if scope not in {"global", "workspace", "user"}:
            scope = "workspace"
        if scope == "global":
            scope_id = "global"
        elif scope == "user":
            scope_id = str(metadata.get("user_id") or updated_by or "anonymous")
        else:
            scope_id = str(metadata.get("workspace_id") or self.DEFAULT_WORKSPACE_ID)
        return {"scope": scope, "scope_id": scope_id}

    @staticmethod
    def _normalize_apply_mode(value: Any) -> str:
        token = str(value or "immediate").strip().lower()
        return token if token in {"immediate", "next_eod", "scheduled"} else "immediate"

    def _resolve_effective_at(self, metadata: Dict[str, Any]) -> str:
        apply_mode = self._normalize_apply_mode(metadata.get("apply_mode"))
        if apply_mode == "scheduled":
            effective = str(metadata.get("effective_at") or "").strip()
            if not effective:
                raise ValueError("scheduled apply_mode requires metadata.effective_at")
            return effective
        if apply_mode == "next_eod":
            tz = ZoneInfo(str(metadata.get("timezone") or self.DEFAULT_TIMEZONE))
            now_local = datetime.now(tz)
            next_day = (now_local + timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
            return next_day.isoformat()
        return self._now_iso()

    def _response_metadata(self, spec: SettingSpec, envelope: Dict[str, Any], resolved: Dict[str, Any]) -> Dict[str, Any]:
        pending = self._pending_entries_for_scope(envelope, resolved["scope"], resolved["scope_id"])
        return {
            "managed_by": "BrainSystemSettingsService",
            "consumer": spec.consumer,
            "value_type": spec.kind,
            "description": spec.description,
            "storage_schema_version": self.STORAGE_SCHEMA_VERSION,
            "scope": resolved["scope"],
            "scope_id": resolved["scope_id"],
            "current_revision": int(resolved.get("revision") or 0),
            "pending_count": len(pending),
            "pending_effective_at": [item.get("effective_at") for item in pending],
        }

    def _read_envelope(self, spec: SettingSpec, row: Any) -> tuple[Dict[str, Any], bool]:
        if not isinstance(row, dict):
            return self._blank_envelope(), False
        raw_value = row.get("value")
        if isinstance(raw_value, dict) and isinstance(raw_value.get("entries"), dict):
            return deepcopy(raw_value), False
        legacy_value = self._extract_scalar(raw_value, spec.default)
        normalized = self._blank_envelope()
        normalized["entries"]["global:global"] = {
            "scope": "global",
            "scope_id": "global",
            "active": {
                "value": legacy_value,
                "revision": 1,
                "updated_by": row.get("updated_by") or "system",
                "updated_at": row.get("updated_at") or self._now_iso(),
                "created_at": row.get("created_at") or self._now_iso(),
                "effective_at": row.get("updated_at") or self._now_iso(),
                "status": "active",
                "apply_mode": "immediate",
                "reason": "",
            },
            "pending": [],
            "history": [],
        }
        return normalized, True

    def _blank_envelope(self) -> Dict[str, Any]:
        return {"schema_version": self.STORAGE_SCHEMA_VERSION, "entries": {}}

    @staticmethod
    def _scope_token(scope: str, scope_id: str) -> str:
        return f"{scope}:{scope_id}"

    def _entry_bucket(self, envelope: Dict[str, Any], scope: str, scope_id: str) -> Dict[str, Any]:
        entries = envelope.setdefault("entries", {})
        token = self._scope_token(scope, scope_id)
        bucket = entries.get(token)
        if not isinstance(bucket, dict):
            bucket = {"scope": scope, "scope_id": scope_id, "active": None, "pending": [], "history": []}
            entries[token] = bucket
        bucket.setdefault("scope", scope)
        bucket.setdefault("scope_id", scope_id)
        bucket.setdefault("pending", [])
        bucket.setdefault("history", [])
        return bucket

    @staticmethod
    def _pending_entries(bucket: Dict[str, Any]) -> list[Dict[str, Any]]:
        return [item for item in list(bucket.get("pending") or []) if isinstance(item, dict)]

    def _pending_entries_for_scope(self, envelope: Dict[str, Any], scope: str, scope_id: str) -> list[Dict[str, Any]]:
        bucket = self._entry_bucket(envelope, scope, scope_id)
        return self._pending_entries(bucket)

    def _next_revision(self, bucket: Dict[str, Any]) -> int:
        revisions = []
        if isinstance(bucket.get("active"), dict):
            revisions.append(int(bucket["active"].get("revision") or 0))
        revisions.extend(int(item.get("revision") or 0) for item in self._pending_entries(bucket))
        revisions.extend(int(item.get("revision") or 0) for item in list(bucket.get("history") or []) if isinstance(item, dict))
        return (max(revisions) if revisions else 0) + 1

    def _append_history(self, bucket: Dict[str, Any], entry: Dict[str, Any]) -> None:
        history = [item for item in list(bucket.get("history") or []) if isinstance(item, dict)]
        history.insert(0, deepcopy(entry))
        bucket["history"] = history[: self.HISTORY_LIMIT]

    @staticmethod
    def _same_pending_slot(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
        return str(left.get("effective_at") or "") == str(right.get("effective_at") or "")

    def _resolve_entry(self, spec: SettingSpec, envelope: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = dict(context or {})
        workspace_id = str(context.get("workspace_id") or self.DEFAULT_WORKSPACE_ID)
        user_id = str(context.get("user_id") or "").strip()
        candidates = []
        if user_id:
            candidates.append(("user", user_id))
        candidates.append(("workspace", workspace_id))
        candidates.append(("global", "global"))
        for scope, scope_id in candidates:
            bucket = envelope.get("entries", {}).get(self._scope_token(scope, scope_id))
            if isinstance(bucket, dict) and isinstance(bucket.get("active"), dict):
                return {**deepcopy(bucket["active"]), "scope": scope, "scope_id": scope_id}
        return {
            "value": spec.default,
            "revision": 0,
            "updated_by": "system",
            "updated_at": None,
            "created_at": None,
            "effective_at": None,
            "status": "default",
            "apply_mode": "immediate",
            "scope": "default",
            "scope_id": "default",
            "reason": "",
        }

    def _promote_due_pending(self, envelope: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
        mutated = False
        now = datetime.utcnow()
        for bucket in list(dict(envelope.get("entries") or {}).values()):
            if not isinstance(bucket, dict):
                continue
            pending = self._pending_entries(bucket)
            due = [item for item in pending if self._parse_when(item.get("effective_at")) <= now]
            if not due:
                continue
            due.sort(key=lambda item: int(item.get("revision") or 0))
            active = bucket.get("active") if isinstance(bucket.get("active"), dict) else None
            for item in due:
                if isinstance(active, dict):
                    self._append_history(bucket, {**active, "status": "superseded"})
                active = {**item, "status": "active"}
                mutated = True
            bucket["active"] = active
            bucket["pending"] = [item for item in pending if item not in due]
        return envelope, mutated

    @staticmethod
    def _parse_when(value: Any) -> datetime:
        text = str(value or "").strip()
        if not text:
            return datetime.utcnow()
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            return datetime.utcnow()
        return parsed.astimezone(ZoneInfo("UTC")).replace(tzinfo=None) if parsed.tzinfo else parsed

    def _persist_envelope(self, api: Any, key: str, envelope: Dict[str, Any], updated_by: str, metadata: Any = None) -> Dict[str, Any]:
        payload_metadata = dict(metadata or {})
        payload_metadata.update({
            "managed_by": "BrainSystemSettingsService",
            "storage_schema_version": self.STORAGE_SCHEMA_VERSION,
        })
        return api.upsert_setting(
            type(
                "_Setting",
                (),
                {
                    "key": key,
                    "value": envelope,
                    "metadata": payload_metadata,
                    "updated_by": updated_by,
                    "created_at": None,
                    "updated_at": None,
                },
            )()
        )

    @classmethod
    def public_keys(cls) -> Iterable[str]:
        return cls.SPECS.keys()
