import json
import logging
from pathlib import Path
import sys
from datetime import datetime, timedelta

import pytest


SERVICE_ROOT = Path(__file__).resolve().parents[1].resolve()
sys.path.insert(0, str(SERVICE_ROOT))

from handlers.ui_bff import UIBffHandler
from system_settings_service import BrainSystemSettingsService


class _FakeSystemApi:
    def __init__(self):
        self.store = {
            "system.workspace": {"key": "system.workspace", "value": {"value": "Research Hub"}, "metadata": {}, "updated_by": "ops"},
            "defaults.benchmark": {"key": "defaults.benchmark", "value": {"value": "CSI500"}, "metadata": {}, "updated_by": "ops"},
            "notify.wecom": {"key": "notify.wecom", "value": {"value": True}, "metadata": {}, "updated_by": "ops"},
        }
        self.audit_logs = []

    def get_setting(self, key):
        return self.store.get(key)

    def upsert_setting(self, setting):
        payload = {
            "key": setting.key,
            "value": dict(setting.value),
            "metadata": dict(setting.metadata),
            "updated_by": setting.updated_by,
        }
        self.store[setting.key] = payload
        return payload

    def append_audit_log(self, audit):
        self.audit_logs.append(audit)

    def list_presets(self):
        return []

    def get_user(self, user_id):
        return {"id": user_id, "username": "admin"}

    def list_notifications(self, limit=5):
        return []


class _DummyOrchestrator:
    async def list_task_jobs(self, limit=20, offset=0):
        return {"jobs": []}


class _DummyRequest(dict):
    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self.path_qs = path
        self.method = "GET"
        self.query = {"page": "overview"}
        self.can_read_body = False
        self.app = {}
        self.match_info = {}
        self.headers = {}


def test_system_settings_service_only_returns_curated_keys():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    keys = [item["key"] for item in service.list_settings(api)]

    assert "system.workspace" in keys
    assert "defaults.benchmark" in keys
    assert "notify.wecom" not in keys


def test_system_settings_service_applies_read_defaults():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    params = service.apply_query_defaults("/api/v1/ui/market-snapshot/latest", {}, api)
    assert params["benchmark"] == "CSI500"
    assert params["scale"] == "D1"

    inbox = service.apply_query_defaults("/api/v1/ui/candidates/events", {}, api)
    assert "from" in inbox and "to" in inbox


def test_system_settings_service_resolves_scope_precedence():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    service.upsert_setting(
        api,
        "system.workspace",
        {"value": "Global Hub"},
        updated_by="ops",
        metadata={"scope": "global"},
    )
    service.upsert_setting(
        api,
        "system.workspace",
        {"value": "Workspace Hub"},
        updated_by="ops",
        metadata={"scope": "workspace", "workspace_id": "alpha"},
    )
    service.upsert_setting(
        api,
        "system.workspace",
        {"value": "Analyst Hub"},
        updated_by="user_1",
        metadata={"scope": "user", "user_id": "user_1"},
    )

    user_setting = service.get_setting(api, "system.workspace", context={"workspace_id": "alpha", "user_id": "user_1"})
    workspace_setting = service.get_setting(api, "system.workspace", context={"workspace_id": "alpha"})
    global_setting = service.get_setting(api, "system.workspace", context={"workspace_id": "beta"})

    assert user_setting["value"]["value"] == "Analyst Hub"
    assert user_setting["metadata"]["scope"] == "user"
    assert workspace_setting["value"]["value"] == "Workspace Hub"
    assert workspace_setting["metadata"]["scope"] == "workspace"
    assert global_setting["value"]["value"] == "Global Hub"
    assert global_setting["metadata"]["scope"] == "global"


def test_system_settings_service_tracks_pending_revision_and_history():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    current = service.upsert_setting(
        api,
        "defaults.benchmark",
        {"value": "CSI300"},
        updated_by="ops",
        metadata={"scope": "workspace", "workspace_id": "alpha"},
    )
    pending = service.upsert_setting(
        api,
        "defaults.benchmark",
        {"value": "SSE50"},
        updated_by="ops",
        metadata={"scope": "workspace", "workspace_id": "alpha", "apply_mode": "scheduled", "effective_at": (datetime.utcnow() + timedelta(days=1)).isoformat()},
    )
    history = service.list_history(api, "defaults.benchmark")

    assert current["metadata"]["current_revision"] == 1
    assert pending["metadata"]["current_revision"] == 1
    assert pending["metadata"]["pending_count"] == 1
    assert any(item["status"] == "pending" for item in history["items"])


def test_system_settings_service_rolls_back_revision():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    service.upsert_setting(api, "system.name", {"value": "AutoTM A"}, updated_by="ops", metadata={"scope": "global"})
    service.upsert_setting(api, "system.name", {"value": "AutoTM B"}, updated_by="ops", metadata={"scope": "global"})
    rolled = service.rollback_setting(api, "system.name", revision=1, updated_by="ops", metadata={"scope": "global"})
    history = service.list_history(api, "system.name")

    assert rolled["value"]["value"] == "AutoTM A"
    assert rolled["metadata"]["current_revision"] == 3
    assert any(int(item.get("rolled_back_from_revision") or 0) == 1 for item in history["items"])


def test_system_settings_service_history_filters_same_scope_bucket():
    service = BrainSystemSettingsService()
    api = _FakeSystemApi()

    service.upsert_setting(api, "system.name", {"value": "Global Name"}, updated_by="ops", metadata={"scope": "global"})
    service.upsert_setting(api, "system.name", {"value": "Workspace Name"}, updated_by="ops", metadata={"scope": "workspace", "workspace_id": "alpha"})
    service.upsert_setting(api, "system.name", {"value": "User Name"}, updated_by="user_1", metadata={"scope": "user", "user_id": "user_1"})

    workspace_history = service.list_history(api, "system.name", context={"scope": "workspace", "workspace_id": "alpha"})
    workspace_scope_keys = {item["scope_key"] for item in workspace_history["items"]}

    assert workspace_scope_keys == {"workspace:alpha"}


@pytest.mark.asyncio
async def test_shell_context_includes_curated_system_settings():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-shell-context-curated")
    handler._system_api = _FakeSystemApi()
    handler.get_app_component = lambda request, key: _DummyOrchestrator()

    async def _fetch_upstream_json(request, service, path, params=None):
        return {"data": {"items": [], "total": 0}}

    handler._fetch_upstream_json = _fetch_upstream_json
    response = await handler._handle_shell_context(_DummyRequest("/api/v1/ui/shell/context"))
    payload = json.loads(response.text)

    assert payload["data"]["workspace"] == "Research Hub"
    assert payload["data"]["system_name"] == "AutoTM Quant Research"
    assert payload["data"]["timezone"] == "Asia/Shanghai"


@pytest.mark.asyncio
async def test_system_settings_upsert_rejects_non_curated_key():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-settings-upsert")
    handler._system_api = _FakeSystemApi()
    handler._app = {}

    async def _fake_get_json(_request):
        return {"value": {"value": True}, "updated_by": "user_admin"}

    handler.get_request_json = _fake_get_json

    request = _DummyRequest("/api/v1/ui/system/settings/notify.wecom")
    request.method = "PUT"
    response = await handler._handle_system_settings_upsert(request, "notify.wecom")
    payload = json.loads(response.text)

    assert response.status == 400
    assert payload["error"] == "Unsupported system settings key"


@pytest.mark.asyncio
async def test_system_settings_upsert_returns_409_on_revision_conflict():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-settings-conflict")
    handler._system_api = _FakeSystemApi()
    handler._app = {}

    async def _first_json(_request):
        return {"value": {"value": "AutoTM V1"}, "updated_by": "user_admin", "metadata": {"scope": "global"}}

    handler.get_request_json = _first_json
    request = _DummyRequest("/api/v1/ui/system/settings/system.name")
    request.method = "PUT"
    await handler._handle_system_settings_upsert(request, "system.name")

    async def _conflict_json(_request):
        return {
            "value": {"value": "AutoTM V2"},
            "updated_by": "user_admin",
            "metadata": {"scope": "global"},
            "expected_revision": 0,
        }

    handler.get_request_json = _conflict_json
    conflict_request = _DummyRequest("/api/v1/ui/system/settings/system.name")
    conflict_request.method = "PUT"
    response = await handler._handle_system_settings_upsert(conflict_request, "system.name")
    payload = json.loads(response.text)

    assert response.status == 409
    assert payload["error_code"] == "SETTING_REVISION_CONFLICT"


@pytest.mark.asyncio
async def test_system_settings_history_and_rollback_routes():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-settings-history")
    handler._system_api = _FakeSystemApi()
    handler._app = {}

    values = iter(["AutoTM V1", "AutoTM V2"])

    async def _upsert_json(_request):
        return {"value": {"value": next(values)}, "updated_by": "user_admin", "metadata": {"scope": "global"}}

    handler.get_request_json = _upsert_json

    for _ in range(2):
        put_request = _DummyRequest("/api/v1/ui/system/settings/system.name")
        put_request.method = "PUT"
        await handler._handle_system_settings_upsert(put_request, "system.name")

    history_request = _DummyRequest("/api/v1/ui/system/settings/system.name/history")
    history_response = await handler._handle_system_settings_history(history_request, "system.name")
    history_payload = json.loads(history_response.text)

    assert history_response.status == 200
    assert history_payload["data"]["key"] == "system.name"

    async def _rollback_json(_request):
        return {"revision": 1, "updated_by": "user_admin", "metadata": {"scope": "global"}}

    handler.get_request_json = _rollback_json
    rollback_request = _DummyRequest("/api/v1/ui/system/settings/system.name/rollback")
    rollback_request.method = "POST"
    rollback_response = await handler._handle_system_settings_rollback(rollback_request, "system.name")
    rollback_payload = json.loads(rollback_response.text)

    assert rollback_response.status == 200
    assert rollback_payload["data"]["value"]["value"] == "AutoTM V1"
