import asyncio
import json
import logging
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"
for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

if "tushare" not in sys.modules:
    tushare_stub = types.ModuleType("tushare")
    tushare_stub.pro_api = lambda *args, **kwargs: None
    sys.modules["tushare"] = tushare_stub

from handlers.ui_bff import UIBffHandler


class _DummyRequest:
    def __init__(self):
        self.app = {}


class _DummySystemAPI:
    def __init__(self, items):
        self._items = items

    def get_setting(self, key):
        assert key == "system.data.custom_sources"
        return {"value": {"items": self._items}}


def test_system_data_sources_list_merges_runtime_and_custom_sources():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-data-sources-list")
    handler._get_system_api = lambda: _DummySystemAPI(
        [
            {
                "id": "custom-feed",
                "name": "Custom Feed",
                "type": "custom",
                "status": "online",
                "endpoint": "https://example.test/feed",
            }
        ]
    )

    async def _fetch_upstream_json(_request, service, path, **kwargs):
        assert service == "flowhub"
        assert path == "/api/v1/sources"
        return {
            "data": {
                "sources": [
                    {"id": "redis", "name": "Redis", "type": "redis", "status": "online"},
                    {"id": "custom-feed", "name": "Flowhub Placeholder", "type": "runtime", "status": "offline"},
                ]
            }
        }

    handler._fetch_upstream_json = _fetch_upstream_json

    response = asyncio.run(handler._handle_system_data_sources_list(_DummyRequest()))
    payload = json.loads(response.text)

    assert payload["success"] is True
    assert payload["data"]["total"] == 2
    merged = {item["id"]: item for item in payload["data"]["sources"]}
    assert merged["redis"]["status"] == "online"
    assert merged["custom-feed"]["name"] == "Custom Feed"
    assert merged["custom-feed"]["type"] == "custom"


def test_system_data_source_test_falls_back_for_unknown_runtime_source():
    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-system-data-source-test")

    async def _fetch_upstream_json(_request, service, path, **kwargs):
        assert service == "flowhub"
        assert path == "/api/v1/sources/custom-feed/test"
        raise RuntimeError("no runtime tester")

    handler._fetch_upstream_json = _fetch_upstream_json

    response = asyncio.run(handler._handle_system_data_source_test(_DummyRequest(), "custom-feed"))
    payload = json.loads(response.text)

    assert payload["success"] is True
    assert payload["data"]["id"] == "custom-feed"
    assert payload["data"]["status"] == "unsupported"
