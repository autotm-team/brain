import json
import logging

import pytest

from handlers.ui_bff import UIBffHandler


class _DummyOrchestrator:
    async def list_task_jobs(self, limit=20, offset=0):
        return {
            "jobs": [
                {"status": "running", "cancellable": True},
                {"status": "failed", "cancellable": False},
            ]
        }


class _DummySystemAPI:
    def get_user(self, user_id):
        return {"id": user_id, "username": "admin"}

    def list_notifications(self, limit=5):
        return [{"id": "n1"}, {"id": "n2"}]

    def get_setting(self, key):
        values = {
            "system.workspace": {"value": {"value": "Alpha Lab"}},
            "system.name": {"value": {"value": "AutoTM Quant Research"}},
            "system.locale": {"value": {"value": "zh-CN"}},
            "system.timezone": {"value": {"value": "Asia/Shanghai"}},
            "defaults.benchmark": {"value": {"value": "CSI300"}},
            "defaults.scale": {"value": {"value": "D1"}},
            "defaults.window": {"value": {"value": "180D"}},
            "defaults.inbox_days": {"value": {"value": 7}},
        }
        return values.get(key)


class _DummySettingsService:
    def shell_context_settings(self, api, context=None):
        return {"workspace": "Alpha Lab", "system_name": "AutoTM Quant Research", "locale": "zh-CN", "timezone": "Asia/Shanghai"}

    def apply_query_defaults(self, path, query, api, context=None):
        assert path == "/api/v1/ui/candidates/events"
        next_query = dict(query)
        next_query.setdefault("from", "2026-04-04")
        next_query.setdefault("to", "2026-04-11")
        return next_query


class _DummyRequest:
    def __init__(self):
        self.query = {"page": "watchlist"}
        self.headers = {}
        self.app = {}


@pytest.mark.asyncio
async def test_shell_context_returns_real_watchlist_badge():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-shell-context")
    handler.get_app_component = lambda request, key: _DummyOrchestrator()
    handler._get_system_api = lambda: _DummySystemAPI()
    handler._get_system_settings_service = lambda: _DummySettingsService()

    async def _fetch_upstream_json(request, service, path, params=None):
        assert service == "execution"
        assert path == "/api/v1/ui/candidates/events"
        assert params["limit"] == 1
        assert params["offset"] == 0
        assert params["status"] == "pending"
        assert params.get("from")
        assert params.get("to")
        return {"data": {"items": [], "total": 7}}

    handler._fetch_upstream_json = _fetch_upstream_json
    response = await handler._handle_shell_context(_DummyRequest())
    payload = json.loads(response.text)

    assert payload["success"] is True
    assert payload["data"]["sidebar_badges"]["watchlist"] == 7
    assert payload["data"]["sidebar_badges"]["jobs"] == 1
    assert payload["data"]["sidebar_badges"]["alerts"] == 1
    assert payload["data"]["workspace"] == "Alpha Lab"
