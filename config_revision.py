from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def make_revision_entry(
    *,
    revision: int,
    value: Dict[str, Any],
    updated_by: str,
    source: str,
    reason: str = "",
    updated_keys: Optional[List[str]] = None,
    hot_reloaded_keys: Optional[List[str]] = None,
    restart_required_keys: Optional[List[str]] = None,
    event: str = "update",
    updated_at: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "revision": int(revision),
        "value": deepcopy(value),
        "updated_by": str(updated_by),
        "updated_at": updated_at or now_iso(),
        "updated_keys": list(updated_keys or []),
        "hot_reloaded_keys": list(hot_reloaded_keys or []),
        "restart_required_keys": list(restart_required_keys or []),
        "reason": str(reason or ""),
        "source": str(source or ""),
        "event": str(event or "update"),
    }


def blank_revision_envelope() -> Dict[str, Any]:
    return {"schema_version": "v2", "current_revision": 0, "revisions": []}


def current_entry(envelope: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    revisions = [item for item in list(envelope.get("revisions") or []) if isinstance(item, dict)]
    current_revision = int(envelope.get("current_revision") or 0)
    for item in revisions:
        if int(item.get("revision") or 0) == current_revision:
            return item
    if not revisions:
        return None
    return max(revisions, key=lambda item: int(item.get("revision") or 0))


def next_revision(envelope: Dict[str, Any]) -> int:
    revisions = [int(item.get("revision") or 0) for item in list(envelope.get("revisions") or []) if isinstance(item, dict)]
    return (max(revisions) if revisions else 0) + 1


def revision_history(envelope: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = [deepcopy(item) for item in list(envelope.get("revisions") or []) if isinstance(item, dict)]
    return sorted(items, key=lambda item: int(item.get("revision") or 0), reverse=True)


def revision_entry(envelope: Dict[str, Any], revision: int) -> Optional[Dict[str, Any]]:
    for item in list(envelope.get("revisions") or []):
        if isinstance(item, dict) and int(item.get("revision") or 0) == int(revision):
            return deepcopy(item)
    return None


def append_revision(envelope: Dict[str, Any], entry: Dict[str, Any]) -> Dict[str, Any]:
    revisions = [deepcopy(item) for item in list(envelope.get("revisions") or []) if isinstance(item, dict)]
    revisions = [item for item in revisions if int(item.get("revision") or 0) != int(entry.get("revision") or 0)]
    revisions.append(deepcopy(entry))
    envelope["revisions"] = sorted(revisions, key=lambda item: int(item.get("revision") or 0))
    envelope["current_revision"] = int(entry.get("revision") or 0)
    envelope["schema_version"] = "v2"
    return envelope


def normalize_revision_envelope(
    raw_value: Dict[str, Any],
    *,
    defaults: Dict[str, Any],
    validate_payload: Callable[[Dict[str, Any]], Dict[str, Any]],
    legacy_updated_by: str = "system",
    legacy_updated_at: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    if isinstance(raw_value, dict) and isinstance(raw_value.get("revisions"), list):
        envelope = deepcopy(raw_value)
        current = current_entry(envelope)
        merged = _deep_merge(defaults, dict(current.get("value") or {}) if isinstance(current, dict) else {})
        validated = validate_payload(merged)
        changed = False
        if not isinstance(current, dict):
            entry = make_revision_entry(
                revision=1,
                value=validated,
                updated_by=legacy_updated_by,
                source="normalize",
                reason="missing_current_revision",
                event="normalize",
                updated_at=legacy_updated_at,
            )
            envelope = append_revision(blank_revision_envelope(), entry)
            changed = True
        elif current.get("value") != validated or envelope.get("schema_version") != "v2":
            current["value"] = validated
            envelope = append_revision(envelope, current)
            changed = True
        return validated, envelope, changed

    merged = _deep_merge(defaults, raw_value if isinstance(raw_value, dict) else {})
    validated = validate_payload(merged)
    envelope = append_revision(
        blank_revision_envelope(),
        make_revision_entry(
            revision=1,
            value=validated,
            updated_by=legacy_updated_by,
            source="normalize",
            reason="legacy_normalize",
            event="normalize",
            updated_at=legacy_updated_at,
        ),
    )
    return validated, envelope, True


def _deep_merge(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
