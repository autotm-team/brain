import pytest

from task_orchestrator import TaskOrchestrator


@pytest.mark.asyncio
async def test_execution_internal_job_types_are_allowed_for_auto_chain():
    orchestrator = TaskOrchestrator(app={})

    async def fake_fetch(service: str, use_cache: bool = True):
        assert service == "execution"
        return [{"job_type": "batch_analyze"}]

    orchestrator._fetch_service_job_types = fake_fetch

    allowed = await orchestrator._fetch_service_job_type_set("execution")

    assert "batch_analyze" in allowed
    assert "ui_candidates_sync_incremental" in allowed
