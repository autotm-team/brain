import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from task_orchestrator import TaskOrchestrator, UpstreamServiceError


@pytest.mark.asyncio
async def test_cancel_prefers_post_cancel_and_falls_back_to_delete_on_405(monkeypatch):
    orchestrator = TaskOrchestrator({"redis": None})
    calls = []

    async def _resolve(_task_job_id):
        return "macro", "svc-job-2", "brain-job-2"

    async def _request(service, method, path, payload=None, params=None):
        calls.append((service, method, path))
        if method == "POST" and path.endswith("/cancel"):
            raise UpstreamServiceError(
                service=service,
                method=method,
                path=path,
                status=405,
                error={"message": "method not allowed"},
            )
        return {"success": True}

    async def _safe_get(*_args, **_kwargs):
        return None

    monkeypatch.setattr(orchestrator, "_resolve_task_job_id", _resolve)
    monkeypatch.setattr(orchestrator, "_request_service", _request)
    monkeypatch.setattr(orchestrator, "_safe_get_service_job", _safe_get)

    result = await orchestrator.cancel_task_job("brain-job-2")

    assert calls[0] == ("macro", "POST", "/api/v1/jobs/svc-job-2/cancel")
    assert calls[1] == ("macro", "DELETE", "/api/v1/jobs/svc-job-2")
    assert result["status"] == "cancelled"
