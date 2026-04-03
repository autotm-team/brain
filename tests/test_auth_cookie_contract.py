import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
import types

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

import econdb as _econdb  # type: ignore

if not hasattr(_econdb, "TaskRuntimeDataAPI"):
    class _TaskRuntimeDataAPI:  # pragma: no cover - import shim
        pass

    _econdb.TaskRuntimeDataAPI = _TaskRuntimeDataAPI  # type: ignore[attr-defined]

from handlers.auth import AuthHandler


class _DummyRequest:
    def __init__(self, *, cookies=None, headers=None):
        self.app = {}
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.remote = "127.0.0.1"
        self.secure = False
        self.can_read_body = True


def _token_bundle():
    return SimpleNamespace(
        access_token="access-token",
        refresh_token="refresh-token",
        token_type="Bearer",
        access_expires_in=900,
        refresh_expires_in=604800,
        user={"id": "user_admin", "username": "admin"},
    )


def test_login_sets_http_only_refresh_cookie_and_omits_body_refresh_token():
    class _DummyAuthService:
        async def login(self, **kwargs):
            return _token_bundle()

    handler = AuthHandler.__new__(AuthHandler)
    handler.logger = logging.getLogger("test-auth-login-cookie")
    handler.get_app_component = lambda request, key: _DummyAuthService()

    async def _get_request_json(_request):
        return {"username": "admin", "password": "admin123!"}

    handler.get_request_json = _get_request_json

    response = __import__("asyncio").run(handler.login(_DummyRequest()))
    payload = json.loads(response.text)
    cookie = response.cookies[handler.REFRESH_COOKIE_NAME]

    assert payload["success"] is True
    assert "refresh_token" not in payload["data"]
    assert payload["data"]["access_token"] == "access-token"
    assert cookie.value == "refresh-token"
    assert cookie["path"] == handler.REFRESH_COOKIE_PATH
    assert cookie["httponly"]


def test_refresh_and_logout_can_use_cookie_refresh_token():
    class _DummyAuthService:
        def __init__(self):
            self.refresh_calls = []
            self.logout_calls = []

        async def refresh(self, **kwargs):
            self.refresh_calls.append(kwargs)
            return _token_bundle()

        async def logout(self, **kwargs):
            self.logout_calls.append(kwargs)

    service = _DummyAuthService()
    handler = AuthHandler.__new__(AuthHandler)
    handler.logger = logging.getLogger("test-auth-refresh-cookie")
    handler.get_app_component = lambda request, key: service

    async def _get_request_json(_request):
        return {}

    handler.get_request_json = _get_request_json

    refresh_request = _DummyRequest(cookies={handler.REFRESH_COOKIE_NAME: "cookie-refresh-token"})
    refresh_response = __import__("asyncio").run(handler.refresh(refresh_request))
    refresh_payload = json.loads(refresh_response.text)

    assert refresh_payload["success"] is True
    assert service.refresh_calls[0]["refresh_token"] == "cookie-refresh-token"
    assert "refresh_token" not in refresh_payload["data"]

    logout_request = _DummyRequest(
        cookies={handler.REFRESH_COOKIE_NAME: "cookie-refresh-token"},
        headers={"Authorization": "Bearer access-token"},
    )
    logout_response = __import__("asyncio").run(handler.logout(logout_request))
    logout_payload = json.loads(logout_response.text)
    logout_cookie = logout_response.cookies[handler.REFRESH_COOKIE_NAME]

    assert logout_payload["success"] is True
    assert service.logout_calls[0]["refresh_token"] == "cookie-refresh-token"
    assert service.logout_calls[0]["access_token"] == "access-token"
    assert logout_cookie["max-age"] == "0"
