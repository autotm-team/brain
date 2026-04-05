"""Shared serialization helpers for Brain HTTP responses."""

from __future__ import annotations

from collections import deque
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time
from enum import Enum
from pathlib import Path
from typing import Any


def to_jsonable(value: Any) -> Any:
    """Convert common Python objects into JSON-safe structures."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return {
            field.name: to_jsonable(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, dict):
        return {
            str(key): to_jsonable(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset, deque)):
        return [to_jsonable(item) for item in value]
    return value


def serialize_system_status(status: Any) -> dict[str, Any]:
    payload = to_jsonable(status) or {}
    return payload if isinstance(payload, dict) else {"value": payload}


def serialize_analysis_cycle_result(result: Any) -> dict[str, Any]:
    payload = to_jsonable(result) or {}
    if not isinstance(payload, dict):
        return {"value": payload}
    payload["success"] = bool(getattr(result, "success", payload.get("status") == "completed"))
    return payload


def serialize_resource_allocation(allocation: Any, *, default_strategy: str | None = None) -> dict[str, Any]:
    payload = to_jsonable(allocation) or {}
    if not isinstance(payload, dict):
        return {"value": payload}

    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    payload["strategy"] = payload.get("strategy") or metadata.get("strategy") or default_strategy
    payload["created_time"] = payload.get("created_time") or payload.get("allocation_time")
    payload["is_active"] = bool(payload.get("is_active", True))
    return payload


def serialize_data_flow_status(status: Any) -> dict[str, Any]:
    payload = to_jsonable(status) or {}
    return payload if isinstance(payload, dict) else {"value": payload}
