import json
import logging

import pytest


class _DummyRequest:
    def __init__(self):
        self.app = {}
        self.query = {}
        self.match_info = {}
        self.headers = {}
        self.can_read_body = False


@pytest.mark.asyncio
async def test_removed_execution_helper_endpoints_are_not_handled_by_ui_bff():
    from handlers.ui_bff import UIBffHandler

    handler = UIBffHandler.__new__(UIBffHandler)
    handler.logger = logging.getLogger("test-ui-bff-removed-execution-helpers")

    request = _DummyRequest()
    request.method = "GET"

    for path in (
        "/api/v1/ui/execution/analyzers",
        "/api/v1/ui/execution/stocks/top",
        "/api/v1/ui/execution/signals/latest",
        "/api/v1/ui/execution/signals/stream",
        "/api/v1/ui/execution/history",
        "/api/v1/ui/execution/analyzer-backtests",
        "/api/v1/ui/execution/analyzer-backtests/options",
        "/api/v1/ui/execution/backtests/history",
        "/api/v1/ui/execution/backtests/capabilities",
    ):
        request.path = path
        response = await handler._handle_internal_route(request)
        assert response is None
