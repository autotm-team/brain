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


class _DummyRequest:
    def __init__(self):
        self.query = {"page": "watchlist"}
        self.app = {}


@pytest.mark.asyncio
async def test_shell_context_returns_real_watchlist_badge():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-shell-context")
    handler.get_app_component = lambda request, key: _DummyOrchestrator()
    handler._get_system_api = lambda: _DummySystemAPI()

    async def _fetch_upstream_json(request, service, path, params=None):
        assert service == "execution"
        assert path == "/api/v1/ui/candidates/events"
        assert params == {"limit": 1, "offset": 0, "status": "pending"}
        return {"data": {"items": [], "total": 7}}

    handler._fetch_upstream_json = _fetch_upstream_json
    response = await handler._handle_shell_context(_DummyRequest())
    payload = json.loads(response.text)

    assert payload["success"] is True
    assert payload["data"]["sidebar_badges"]["watchlist"] == 7
    assert payload["data"]["sidebar_badges"]["jobs"] == 1
    assert payload["data"]["sidebar_badges"]["alerts"] == 1
