import logging
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SERVICE_ROOT = Path(__file__).resolve().parents[1].resolve()
sys.path.insert(0, str(SERVICE_ROOT))


class _DummyRequest(dict):
    def __init__(self, method: str, path: str):
        super().__init__()
        self.method = method
        self.path = path
        self.path_qs = path
        self.query = {}
        self.match_info = {}
        self.headers = {}
        self.can_read_body = False
        self.app = {}


class _FakeSystemApi:
    def __init__(self):
        self.audit_logs = []

    def append_audit_log(self, audit):
        self.audit_logs.append(
            {
                "action": audit.action,
                "target_id": audit.target_id,
                "payload": dict(audit.payload),
            }
        )


class _FakeBrainConfigManager:
    def __init__(self, enabled: bool = True):
        self._enabled = enabled

    def runtime_snapshot(self, masked: bool = True):
        return {"service": {"host": "0.0.0.0", "port": 8088}}

    async def dynamic_snapshot(self, masked: bool = True):
        return {"monitoring": {"enable_system_monitoring": True}, "control_plane": {"scheduler": {"enabled": True}}}

    def dynamic_history(self):
        return {"service": "brain", "current_revision": 2, "items": [{"revision": 2}, {"revision": 1}]}

    def catalog(self):
        return {"service": "brain", "items": [{"key": "control_plane.scheduler"}]}

    def config_proxy_enabled(self) -> bool:
        return self._enabled

    async def update_dynamic_config(self, changes, actor_id: str, source: str, expected_revision=None, reason: str = ""):
        return {"applied": True, "updated_keys": ["monitoring.enable_system_monitoring"], "hot_reloaded_keys": [], "restart_required": False, "restart_required_keys": []}

    async def rollback_dynamic_config(self, revision: int, actor_id: str, source: str, reason: str = ""):
        return {"applied": True, "rolled_back_from_revision": revision, "current_revision": 3}

    async def reload(self):
        return {"applied": True, "updated_sections": ["service"], "restart_required_sections": []}


@pytest.mark.asyncio
async def test_ui_bff_handles_system_config_routes_for_brain_and_upstream():
    sys.path.insert(0, str(SERVICE_ROOT))
    importlib.invalidate_caches()
    from handlers.ui_bff import UIBffHandler

    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-ui-bff-config-proxy")
    handler._system_api = _FakeSystemApi()
    handler._app = {
        "config": SimpleNamespace(service=SimpleNamespace(config_proxy_token="proxy-token")),
        "config_manager": _FakeBrainConfigManager(),
        "service_registry": SimpleNamespace(
            _services={
                "macro": {"status": "healthy"},
                "execution": {"status": "healthy"},
                "portfolio": {"status": "healthy"},
                "flowhub": {"status": "healthy"},
            }
        ),
    }
    observed = {}

    async def _fake_fetch(_request, service_name: str, path: str, method: str = "GET", params=None, payload=None, headers=None):
        observed["headers"] = headers
        return {"data": {"service": service_name, "path": path, "method": method, "payload": payload}}

    async def _fake_get_json(_request):
        return {"changes": {"logging": {"level": "DEBUG"}}}

    handler._fetch_upstream_json = _fake_fetch
    handler.get_request_json = _fake_get_json

    services_request = _DummyRequest("GET", "/api/v1/ui/system/config/services")
    services_request.app = handler._app
    services_response = await handler._handle_internal_route(services_request)
    assert services_response.status == 200
    services_payload = json.loads(services_response.text)
    assert services_payload["data"]["items"][0]["reload_path"] == "/api/v1/ui/system/config/services/brain/reload"
    assert services_payload["data"]["items"][0]["history_path"] == "/api/v1/ui/system/config/services/brain/history"
    assert services_payload["data"]["items"][0]["rollback_path"] == "/api/v1/ui/system/config/services/brain/rollback"

    brain_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/brain/runtime")
    brain_request.app = handler._app
    brain_runtime = await handler._handle_internal_route(brain_request)
    assert brain_runtime.status == 200

    macro_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/macro/catalog")
    macro_request.app = handler._app
    macro_catalog = await handler._handle_internal_route(macro_request)
    assert macro_catalog.status == 200

    history_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/macro/history")
    history_request.app = handler._app
    history_response = await handler._handle_internal_route(history_request)
    assert history_response.status == 200

    brain_history_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/brain/history")
    brain_history_request.app = handler._app
    brain_history_response = await handler._handle_internal_route(brain_history_request)
    assert brain_history_response.status == 200

    update_request = _DummyRequest("POST", "/api/v1/ui/system/config/services/macro/dynamic")
    update_request.app = handler._app
    update_request["current_user"] = {"id": "user_admin"}
    update_request.can_read_body = True
    update_response = await handler._handle_internal_route(update_request)
    assert update_response.status == 200
    assert handler._system_api.audit_logs[-1]["action"] == "service_config.update_dynamic"
    assert observed["headers"]["X-AutoTM-Config-Proxy-Token"] == "proxy-token"

    reload_request = _DummyRequest("POST", "/api/v1/ui/system/config/services/brain/reload")
    reload_request.app = handler._app
    reload_request["current_user"] = {"id": "user_admin"}
    reload_request.can_read_body = True
    reload_response = await handler._handle_internal_route(reload_request)
    assert reload_response.status == 200
    assert handler._system_api.audit_logs[-1]["action"] == "service_config.reload"

    async def _fake_rollback_json(_request):
        return {"revision": 1, "reason": "manual_rollback"}

    handler.get_request_json = _fake_rollback_json
    rollback_request = _DummyRequest("POST", "/api/v1/ui/system/config/services/macro/rollback")
    rollback_request.app = handler._app
    rollback_request["current_user"] = {"id": "user_admin"}
    rollback_request.can_read_body = True
    rollback_response = await handler._handle_internal_route(rollback_request)
    assert rollback_response.status == 200
    assert handler._system_api.audit_logs[-1]["action"] == "service_config.rollback"

    brain_rollback_request = _DummyRequest("POST", "/api/v1/ui/system/config/services/brain/rollback")
    brain_rollback_request.app = handler._app
    brain_rollback_request["current_user"] = {"id": "user_admin"}
    brain_rollback_request.can_read_body = True
    brain_rollback_response = await handler._handle_internal_route(brain_rollback_request)
    assert brain_rollback_response.status == 200


@pytest.mark.asyncio
async def test_ui_bff_keeps_brain_config_access_when_worker_proxy_is_disabled():
    importlib.invalidate_caches()
    from handlers.ui_bff import UIBffHandler

    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-ui-bff-config-proxy-disabled")
    handler._system_api = _FakeSystemApi()
    handler._app = {
        "config": SimpleNamespace(service=SimpleNamespace(config_proxy_token="proxy-token")),
        "config_manager": _FakeBrainConfigManager(enabled=False),
        "service_registry": SimpleNamespace(_services={"macro": {"status": "healthy"}}),
    }

    async def _fake_fetch(_request, service_name: str, path: str, method: str = "GET", params=None, payload=None, headers=None):
        return {"data": {"service": service_name, "path": path, "method": method, "payload": payload}}

    handler._fetch_upstream_json = _fake_fetch

    brain_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/brain/runtime")
    brain_request.app = handler._app
    brain_response = await handler._handle_internal_route(brain_request)
    assert brain_response.status == 200

    macro_request = _DummyRequest("GET", "/api/v1/ui/system/config/services/macro/catalog")
    macro_request.app = handler._app
    macro_response = await handler._handle_internal_route(macro_request)
    assert macro_response.status == 400


@pytest.mark.asyncio
async def test_brain_config_manager_rejects_platform_self_disable():
    importlib.invalidate_caches()
    from config_manager import BrainConfigManager

    class _FakeControlPlaneService:
        SCHEDULER_KEY = "control_plane.scheduler"
        STRATEGY_PLAN_KEY = "control_plane.strategy_plan"
        FLOWHUB_BOOTSTRAP_KEY = "control_plane.flowhub_bootstrap"

        async def snapshot(self):
            return {"scheduler": {"enabled": True}, "strategy_plan": {}, "flowhub_bootstrap": {}}

    manager = BrainConfigManager(
        app={"control_plane_settings": _FakeControlPlaneService()},
        system_api=False,
        allow_store_fallback=True,
    )

    with pytest.raises(ValueError, match="cannot be updated via the config platform"):
        await manager.update_dynamic_config(
            {"feature_flags": {"config_proxy_enabled": False}},
            actor_id="user_admin",
            source="test",
        )


def test_brain_config_catalog_uses_type_wire_field():
    importlib.invalidate_caches()
    from config_manager import BrainConfigManager

    catalog = BrainConfigManager(system_api=False, allow_store_fallback=True).catalog()
    item = next(entry for entry in catalog["items"] if entry["key"] == "control_plane.scheduler")

    assert item["type"] == "object"
    assert "type_name" not in item


@pytest.mark.asyncio
async def test_ui_bff_maps_brain_revision_conflict_to_409():
    importlib.invalidate_caches()
    from handlers.ui_bff import UIBffHandler

    class _ConflictManager(_FakeBrainConfigManager):
        async def update_dynamic_config(self, changes, actor_id: str, source: str, expected_revision=None, reason: str = ""):
            raise ValueError("Revision mismatch")

    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-ui-bff-config-conflict")
    handler._system_api = _FakeSystemApi()
    handler._app = {
        "config": SimpleNamespace(service=SimpleNamespace(config_proxy_token="proxy-token")),
        "config_manager": _ConflictManager(),
        "service_registry": SimpleNamespace(_services={}),
    }

    async def _fake_get_json(_request):
        return {"changes": {"monitoring": {"enable_system_monitoring": False}}, "expected_revision": 1}

    handler.get_request_json = _fake_get_json
    request = _DummyRequest("POST", "/api/v1/ui/system/config/services/brain/dynamic")
    request.app = handler._app
    request["current_user"] = {"id": "user_admin"}
    request.can_read_body = True
    response = await handler._handle_internal_route(request)
    payload = json.loads(response.text)

    assert response.status == 409
    assert payload["error_code"] == "CONFIG_REVISION_CONFLICT"
