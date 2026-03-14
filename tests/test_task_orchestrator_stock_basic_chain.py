import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"
for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from task_orchestrator import TaskOrchestrator


@pytest.mark.asyncio
async def test_stock_basic_success_triggers_daily_jobs():
    orchestrator = TaskOrchestrator(app={})
    created = []
    metadata_updates = []
    history_events = []

    async def fake_claim(key):
        return True

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None):
        created.append((service, job_type, params or {}, metadata or {}))
        return {"id": f"{job_type}-task"}

    async def fake_update(task_job_id, metadata):
        metadata_updates.append((task_job_id, metadata))

    async def fake_append(task_job_id, event, normalized, **kwargs):
        history_events.append((task_job_id, event))

    orchestrator._claim_auto_chain = fake_claim
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append

    await orchestrator._maybe_trigger_stock_daily_fetches(
        "task-stock-basic",
        {
            "status": "succeeded",
            "metadata": {},
            "result": {"success": True},
        },
    )

    assert [item[1] for item in created] == ["batch_daily_ohlc", "batch_daily_basic"]
    assert metadata_updates[0][0] == "task-stock-basic"
    assert history_events[0] == ("task-stock-basic", "auto_chain_created")


@pytest.mark.asyncio
async def test_stock_basic_bootstrap_task_does_not_trigger_daily_jobs():
    orchestrator = TaskOrchestrator(app={})
    created = []

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None):
        created.append(job_type)
        return {"id": f"{job_type}-task"}

    orchestrator.create_task_job = fake_create_task_job

    await orchestrator._maybe_trigger_stock_daily_fetches(
        "task-stock-basic",
        {
            "status": "succeeded",
            "metadata": {"bootstrap_source": "brain_initializer"},
            "result": {"success": True},
        },
    )

    assert created == []
