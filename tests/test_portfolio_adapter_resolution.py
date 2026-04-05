import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest


SERVICE_ROOT = Path(__file__).resolve().parents[1]
if str(SERVICE_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROOT))

for module_name in ("config", "exceptions", "interfaces"):
    sys.modules.pop(module_name, None)


def _load_brain_module(module_filename: str, module_name: str):
    module_path = SERVICE_ROOT / f"{module_filename}.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module {module_filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeHttpClient:
    def __init__(self, portfolio_items):
        self.portfolio_items = portfolio_items
        self.calls = []

    async def get(self, endpoint, params=None):
        self.calls.append(("GET", endpoint, params))
        if endpoint == "/api/v1/portfolios":
            return {"status": "success", "data": {"items": self.portfolio_items}}
        if endpoint.startswith("/api/v1/portfolios/"):
            portfolio_id = endpoint.rsplit("/", 1)[-1]
            item = next((row for row in self.portfolio_items if row.get("portfolio_id") == portfolio_id), None)
            if item:
                return {"status": "success", "data": {"portfolio": item}}
            raise RuntimeError("not found")
        raise AssertionError(f"Unexpected GET endpoint: {endpoint}")

    async def request(self, method, endpoint, payload=None):
        self.calls.append((method, endpoint, payload))
        return {
            "status": "success",
            "data": {
                "optimization_result": {
                    "success": True,
                    "optimal_weights": {"000001.SZ": 0.6, "000002.SZ": 0.4},
                    "expected_return": 0.12,
                    "expected_risk": 0.08,
                    "sharpe_ratio": 1.5,
                    "constraints_satisfied": True,
                    "constraint_violations": [],
                }
            },
        }


def test_portfolio_adapter_resolves_single_existing_portfolio():
    from adapters.portfolio_adapter import PortfolioAdapter
    IntegrationConfig = _load_brain_module("config", "brain_test_config").IntegrationConfig

    adapter = PortfolioAdapter(IntegrationConfig())
    adapter._http_client = _FakeHttpClient([{"portfolio_id": "p1", "status": "active"}])
    adapter._is_connected = True

    result = asyncio.run(adapter.request_portfolio_optimization({"risk_level": "medium"}, {}))

    assert result["weights"] == {"000001.SZ": 0.6, "000002.SZ": 0.4}
    request_calls = [call for call in adapter._http_client.calls if call[0] == "POST"]
    assert request_calls
    assert request_calls[0][1].startswith("portfolios/p1/optimize/")


def test_portfolio_adapter_fails_cleanly_when_no_portfolios_exist():
    from adapters.portfolio_adapter import PortfolioAdapter
    IntegrationConfig = _load_brain_module("config", "brain_test_config_missing").IntegrationConfig

    adapter = PortfolioAdapter(IntegrationConfig())
    adapter._http_client = _FakeHttpClient([])
    adapter._is_connected = True

    with pytest.raises(Exception, match="No portfolio exists"):
        asyncio.run(adapter.request_portfolio_optimization({"risk_level": "medium"}, {}))
