import pytest

from config import IntegrationConfig


@pytest.mark.asyncio
async def test_create_app_registers_brain_contract_routes(monkeypatch):
    pytest.importorskip("aiohttp_cors")
    import app as app_module

    async def _fake_init_components(app, config):
        app["container"] = object()
        app["service_registry"] = object()
        app["task_orchestrator"] = object()
        app["task_runtime_api"] = object()
        app["redis"] = None
        app["auth_service"] = object()
        app["unified_scheduler"] = object()
        app["coordinator"] = object()
        app["signal_router"] = object()
        app["data_flow_manager"] = object()
        app["system_monitor"] = object()

    monkeypatch.setattr(app_module, "init_components", _fake_init_components)

    app = await app_module.create_app(IntegrationConfig(environment="testing"))
    resources = list(app.router.resources())
    canonicals = {resource.canonical for resource in resources}

    assert "/api/v1/task-jobs" in canonicals
    assert "/api/v1/ui/auth/login" in canonicals
    assert "/api/v1/ui/{tail}" in canonicals
