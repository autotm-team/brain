import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"
for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from task_orchestrator import TaskOrchestrator, UpstreamServiceError


def _running_job(orchestrator: TaskOrchestrator, *, task_job_id: str, service: str, service_job_id: str, job_type: str, progress: int = 59):
    return orchestrator._normalize_job(
        service,
        {
            "job_id": service_job_id,
            "job_type": job_type,
            "status": "running",
            "progress": progress,
            "created_at": "2026-03-18T00:00:00Z",
            "updated_at": "2026-03-18T00:10:00Z",
            "metadata": {},
        },
        task_job_id=task_job_id,
    )


@pytest.mark.asyncio
async def test_orphaned_running_task_is_cancelled_and_recreated():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-1"
    record = {
        "task_job_id": task_job_id,
        "service": "execution",
        "service_job_id": "svc-job-1",
        "job_type": "batch_analyze",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {"symbols": ["600519.SH"]},
        "metadata": {},
        "request_payload_hash": "hash-1",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="execution",
        service_job_id="svc-job-1",
        job_type="batch_analyze",
    ))

    created_calls = []

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 404, {"message": "missing"})

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None):
        created_calls.append(
            {
                "service": service,
                "job_type": job_type,
                "params": params or {},
                "metadata": metadata or {},
            }
        )
        return {"id": "brain-task-2", "status": "queued"}

    orchestrator._request_service = fake_request
    orchestrator.create_task_job = fake_create_task_job

    result = await orchestrator.scan_orphaned_task_jobs_once()

    assert result["recreated"] == 1
    assert result["cancelled"] == 1
    assert created_calls[0]["service"] == "execution"
    assert created_calls[0]["job_type"] == "batch_analyze"
    assert created_calls[0]["params"] == {"symbols": ["600519.SH"]}
    assert created_calls[0]["metadata"]["recreated_from_task_job_id"] == task_job_id
    assert created_calls[0]["metadata"]["recovery_root_task_job_id"] == task_job_id
    assert created_calls[0]["metadata"]["recovery_attempt_count"] == 1

    current = await orchestrator.get_task_job(task_job_id)
    assert current["status"] == "cancelled"
    assert current["progress"] == 59
    assert current["metadata"]["replacement_task_job_id"] == "brain-task-2"
    assert current["metadata"]["orphaned_upstream_job"] is True
    assert current["metadata"]["recovery_action"] == "recreated"

    history = await orchestrator.get_task_job_history(task_job_id)
    events = [item["event"] for item in history["history"]]
    assert "orphan_detected" in events
    assert "orphan_cancelled" in events
    assert "auto_recreated" in events


@pytest.mark.asyncio
async def test_upstream_5xx_does_not_trigger_recreate():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-5xx"
    record = {
        "task_job_id": task_job_id,
        "service": "flowhub",
        "service_job_id": "svc-job-5xx",
        "job_type": "batch_daily_ohlc",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {"incremental": True},
        "metadata": {},
        "request_payload_hash": "hash-5xx",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="flowhub",
        service_job_id="svc-job-5xx",
        job_type="batch_daily_ohlc",
    ))

    created_calls = []

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 504, {"message": "timeout"})

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None):
        created_calls.append(job_type)
        return {"id": "unexpected"}

    orchestrator._request_service = fake_request
    orchestrator.create_task_job = fake_create_task_job

    result = await orchestrator.scan_orphaned_task_jobs_once()

    assert result["recreated"] == 0
    assert result["cancelled"] == 0
    assert created_calls == []
    current = await orchestrator.get_task_job(task_job_id)
    assert current["status"] == "running"


@pytest.mark.asyncio
async def test_orphan_recreate_is_capped_after_three_attempts():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-cap"
    record = {
        "task_job_id": task_job_id,
        "service": "execution",
        "service_job_id": "svc-job-cap",
        "job_type": "batch_analyze",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {"symbols": ["000001.SZ"]},
        "metadata": {
            "recovery_root_task_job_id": task_job_id,
            "recovery_attempt_count": 3,
        },
        "request_payload_hash": "hash-cap",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="execution",
        service_job_id="svc-job-cap",
        job_type="batch_analyze",
    ))

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 404, {"message": "missing"})

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None):
        raise AssertionError("should not recreate when capped")

    orchestrator._request_service = fake_request
    orchestrator.create_task_job = fake_create_task_job

    result = await orchestrator.scan_orphaned_task_jobs_once()

    assert result["capped"] == 1
    current = await orchestrator.get_task_job(task_job_id)
    assert current["status"] == "cancelled"
    assert current["metadata"]["recovery_capped"] is True
    history = await orchestrator.get_task_job_history(task_job_id)
    events = [item["event"] for item in history["history"]]
    assert "auto_recreate_capped" in events


@pytest.mark.asyncio
async def test_get_task_job_does_not_fallback_to_fake_queued_on_404():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-history"
    record = {
        "task_job_id": task_job_id,
        "service": "execution",
        "service_job_id": "svc-job-history",
        "job_type": "batch_analyze",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {},
        "metadata": {},
        "request_payload_hash": "hash-history",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="execution",
        service_job_id="svc-job-history",
        job_type="batch_analyze",
        progress=41,
    ))

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 404, {"message": "missing"})

    orchestrator._request_service = fake_request

    current = await orchestrator.get_task_job(task_job_id)

    assert current["status"] == "running"
    assert current["progress"] == 41
    assert current["error"]["upstream_status"] == 404
