import asyncio
import json
import logging
from types import SimpleNamespace

import pytest

from handlers.ui_bff import UIBffHandler
from models import AlertInfo
from monitors.system_monitor import SystemMonitor


def _build_monitor() -> SystemMonitor:
    config = SimpleNamespace(
        monitoring=SimpleNamespace(
            performance_threshold={
                "cpu_usage": 0.8,
                "memory_usage": 0.8,
                "response_time": 1000.0,
                "error_rate": 0.05,
            },
            enable_system_monitoring=False,
            enable_performance_monitoring=False,
        )
    )
    return SystemMonitor(config)


class _DummyRequest:
    can_read_body = True

    def __init__(self):
        self.app = {}


def test_system_alert_ack_all_acknowledges_all_active_alerts():
    acknowledged: list[str] = []

    class _DummyMonitor:
        def get_active_alerts(self):
            return [{"alert_id": "alert-1"}, {"alert_id": "alert-2"}]

        def acknowledge_alert(self, alert_id, notes=""):
            acknowledged.append(alert_id)
            return {"alert_id": alert_id, "notes": notes}

    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-alert-ack-all")
    handler.get_app_component = lambda request, key: _DummyMonitor()

    async def _get_request_json(_request):
        return {}

    handler.get_request_json = _get_request_json

    response = asyncio.run(handler._handle_system_alert_ack_all(_DummyRequest()))
    payload = json.loads(response.text)

    assert payload["success"] is True
    assert payload["data"]["acknowledged"] == 2
    assert acknowledged == ["alert-1", "alert-2"]


def test_acknowledge_alert_marks_alert_as_acked_without_resolving():
    monitor = _build_monitor()
    monitor._alert_history.append(
        AlertInfo(
            alert_id="alert-1",
            alert_level="P2",
            alert_type="performance",
            component="integration_layer",
            message="High CPU usage",
        )
    )

    payload = monitor.acknowledge_alert("alert-1", "checked")

    assert payload["is_resolved"] is False
    assert payload["resolution_time"] is None
    assert payload["resolution_notes"] == "checked"
    assert payload["metadata"]["acked"] is True
    assert payload["metadata"]["acknowledged"] is True
    assert payload["metadata"]["acknowledged_at"]


def test_set_alert_rule_updates_existing_rule_and_preserves_contract_fields():
    monitor = _build_monitor()

    created = monitor.set_alert_rule(
        {
            "rule_id": "rule-1",
            "name": "响应时间过高",
            "component": "flowhub",
            "condition": "response_time",
            "threshold": 1200,
            "alert_level": "P2",
            "enabled": True,
        }
    )
    updated = monitor.set_alert_rule(
        {
            "rule_id": "rule-1",
            "name": "响应时间过高",
            "component": "execution",
            "condition": "response_time",
            "threshold": 1800,
            "alert_level": "P1",
            "enabled": False,
        }
    )

    assert len(monitor.get_alert_rules()) == 1
    assert updated["rule_id"] == "rule-1"
    assert updated["component"] == "execution"
    assert updated["alert_level"] == "P1"
    assert updated["enabled"] is False
    assert updated["created_at"] == created["created_at"]
    assert updated["updated_at"]
    assert monitor._performance_thresholds["response_time"] == 1800.0


def test_schedule_item_normalization_uses_helper_without_type_error():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-schedule-normalization")

    payload = handler._normalize_system_schedule_item(
        {
            "id": "schedule-1",
            "service": "flowhub",
            "job_type": "stock_basic_data",
            "trigger": "interval",
            "interval_seconds": 3600,
            "enabled": True,
            "metadata": {"name": "Stock bootstrap"},
            "params": {"incremental": True},
        }
    )

    assert payload["schedule_id"] == "schedule-1"
    assert payload["name"] == "Stock bootstrap"
    assert payload["service"] == "flowhub"
