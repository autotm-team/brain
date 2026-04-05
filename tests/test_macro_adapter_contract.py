import pytest

from adapters.macro_adapter import MacroAdapter
from config import IntegrationConfig


def test_brain_service_defaults_use_compose_service_names():
    config = IntegrationConfig(environment="testing")

    assert config.service.macro_service_url == "http://macro:8080"
    assert config.service.portfolio_service_url == "http://portfolio:8080"
    assert config.service.execution_service_url == "http://execution:8087"
    assert config.service.flowhub_service_url == "http://flowhub:8080"


@pytest.mark.asyncio
async def test_macro_adapter_serializes_indicator_list_as_comma_separated(monkeypatch):
    adapter = MacroAdapter(IntegrationConfig(environment="testing"))
    captured = {}

    class _Client:
        async def get(self, endpoint, params=None):
            captured["endpoint"] = endpoint
            captured["params"] = params
            return {"data": {}}

    monkeypatch.setattr(adapter, "_http_client", _Client())

    await adapter.get_economic_indicators(["cpi", "ppi"])

    assert captured["endpoint"] == "indicators"
    assert captured["params"] == {"indicators": "cpi,ppi"}


@pytest.mark.asyncio
async def test_macro_adapter_generic_send_request_uses_same_indicator_serialization(monkeypatch):
    adapter = MacroAdapter(IntegrationConfig(environment="testing"))
    adapter._is_connected = True
    captured = {}

    class _Client:
        async def get(self, endpoint, params=None):
            captured["endpoint"] = endpoint
            captured["params"] = params
            return {"data": {}}

    monkeypatch.setattr(adapter, "_http_client", _Client())

    await adapter.send_request({"action": "get_indicators", "indicators": ["cpi", "ppi"]})

    assert captured["endpoint"] == "indicators"
    assert captured["params"] == {"indicators": "cpi,ppi"}
