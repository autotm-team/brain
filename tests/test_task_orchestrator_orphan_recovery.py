import pytest

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


def test_normalize_job_keeps_top_level_success_when_result_status_is_business_state():
    orchestrator = TaskOrchestrator(app={})

    normalized = orchestrator._normalize_job(
        "macro",
        {
            "job_id": "macro-job-1",
            "job_type": "ui_macro_cycle_freeze",
            "status": "succeeded",
            "progress": 100,
            "completed_at": "2026-04-05T00:00:00Z",
            "result": {"status": "frozen", "id": "macro_snapshot_1"},
            "metadata": {},
        },
        task_job_id="brain-task-macro-freeze",
    )

    assert normalized["status"] == "succeeded"
    assert normalized["progress"] == 100


def test_normalize_job_promotes_completed_job_with_result_even_if_top_level_status_is_queued():
    orchestrator = TaskOrchestrator(app={})

    normalized = orchestrator._normalize_job(
        "execution",
        {
            "job_id": "execution-job-1",
            "job_type": "ui_candidates_promote",
            "status": "queued",
            "progress": 100,
            "completed_at": "2026-04-05T00:00:01Z",
            "result": {"status": "queued", "id": "subject-1"},
            "error": None,
            "metadata": {"message": "ui_candidates_promote completed"},
        },
        task_job_id="brain-task-promote",
    )

    assert normalized["status"] == "succeeded"
    assert normalized["progress"] == 100


@pytest.mark.asyncio
async def test_list_task_jobs_is_pure_read_and_does_not_trigger_followups():
    orchestrator = TaskOrchestrator(app={})
    side_effects = {"history": 0, "followups": 0}

    async def fake_list_service_jobs(service, status=None, max_items=20):
        if service != "execution":
            return []
        return [
            {
                "job_id": "svc-job-1",
                "job_type": "batch_analyze",
                "status": "succeeded",
                "progress": 100,
                "created_at": "2026-04-05T00:00:00Z",
                "updated_at": "2026-04-05T00:00:10Z",
                "metadata": {},
            }
        ]

    async def fake_find_task_job_id(service, service_job_id):
        return "brain-task-1"

    async def fake_merge(task_job_id, normalized):
        return dict(normalized)

    async def fake_append_history_if_changed(task_job_id, normalized):
        side_effects["history"] += 1

    async def fake_followups(task_job_id, normalized):
        side_effects["followups"] += 1

    async def fake_list_local_task_jobs(*, services, status, seen_task_ids):
        return []

    orchestrator._list_service_jobs = fake_list_service_jobs
    orchestrator._find_task_job_id = fake_find_task_job_id
    orchestrator._merge_record_metadata = fake_merge
    orchestrator._append_history_if_changed = fake_append_history_if_changed
    orchestrator._maybe_trigger_followups = fake_followups
    orchestrator._list_local_task_jobs = fake_list_local_task_jobs

    payload = await orchestrator.list_task_jobs(service="execution", limit=10, offset=0)

    assert payload["total"] == 1
    assert side_effects == {"history": 0, "followups": 0}


@pytest.mark.asyncio
async def test_get_task_job_is_pure_read_but_internal_refresh_can_persist_followups():
    orchestrator = TaskOrchestrator(app={})
    side_effects = {"history": 0, "followups": 0}
    upstream_payload = {
        "data": {
            "job_id": "svc-job-1",
            "job_type": "batch_analyze",
            "status": "succeeded",
            "progress": 100,
            "created_at": "2026-04-05T00:00:00Z",
            "updated_at": "2026-04-05T00:00:10Z",
            "metadata": {},
        }
    }

    async def fake_resolve(task_job_id):
        return ("execution", "svc-job-1", "brain-task-1")

    async def fake_request(service, method, path, payload=None, params=None):
        return dict(upstream_payload)

    async def fake_merge(task_job_id, normalized):
        return dict(normalized)

    async def fake_append_history_if_changed(task_job_id, normalized):
        side_effects["history"] += 1

    async def fake_followups(task_job_id, normalized):
        side_effects["followups"] += 1

    orchestrator._resolve_task_job_id = fake_resolve
    orchestrator._request_service = fake_request
    orchestrator._merge_record_metadata = fake_merge
    orchestrator._append_history_if_changed = fake_append_history_if_changed
    orchestrator._maybe_trigger_followups = fake_followups

    payload = await orchestrator.get_task_job("brain-task-1")
    assert payload["status"] == "succeeded"
    assert side_effects == {"history": 0, "followups": 0}

    payload = await orchestrator._refresh_task_job_snapshot(
        "brain-task-1",
        persist_history=True,
        trigger_followups=True,
    )
    assert payload["status"] == "succeeded"
    assert side_effects == {"history": 1, "followups": 1}


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

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
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
async def test_orphan_recreate_failure_keeps_task_active_for_retry():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-retry"
    record = {
        "task_job_id": task_job_id,
        "service": "execution",
        "service_job_id": "svc-job-retry",
        "job_type": "batch_analyze",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {"symbols": ["600519.SH"]},
        "metadata": {},
        "request_payload_hash": "hash-retry",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="execution",
        service_job_id="svc-job-retry",
        job_type="batch_analyze",
    ))

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 404, {"message": "missing"})

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
        raise RuntimeError("temporary recreate failure")

    orchestrator._request_service = fake_request
    orchestrator.create_task_job = fake_create_task_job

    result = await orchestrator.scan_orphaned_task_jobs_once()

    assert result["recreated"] == 0
    assert result["cancelled"] == 0
    assert result["skipped"] == 1
    current = await orchestrator.get_task_job(task_job_id)
    assert current["status"] == "running"
    assert current["metadata"]["recovery_attempt_count"] == 1
    assert current["metadata"]["recovery_action"] == "retry_pending"
    assert current["error"]["code"] == "ORPHAN_RECREATE_FAILED"
    history = await orchestrator.get_task_job_history(task_job_id)
    events = [item["event"] for item in history["history"]]
    assert "orphan_detected" in events
    assert "auto_recreate_failed" in events
    assert "orphan_cancelled" not in events


@pytest.mark.asyncio
async def test_orphan_recreate_replays_service_payload():
    orchestrator = TaskOrchestrator(app={})
    task_job_id = "brain-task-payload"
    record = {
        "task_job_id": task_job_id,
        "service": "flowhub",
        "service_job_id": "svc-job-payload",
        "job_type": "batch_daily_ohlc",
        "created_at": "2026-03-18T00:00:00Z",
        "params": {"incremental": True},
        "metadata": {},
        "service_payload": {"job_type": "batch_daily_ohlc", "params": {"incremental": True, "symbols": ["000001.SZ"]}},
        "request_payload_hash": "hash-payload",
    }
    await orchestrator._save_task_record(record)
    await orchestrator._append_history(task_job_id, "snapshot", _running_job(
        orchestrator,
        task_job_id=task_job_id,
        service="flowhub",
        service_job_id="svc-job-payload",
        job_type="batch_daily_ohlc",
    ))

    created_calls = []

    async def fake_request(service, method, path, payload=None, params=None):
        raise UpstreamServiceError(service, method, path, 404, {"message": "missing"})

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
        created_calls.append(
            {
                "service": service,
                "job_type": job_type,
                "params": params or {},
                "metadata": metadata or {},
                "service_payload": service_payload,
            }
        )
        return {"id": "brain-task-payload-2", "status": "queued"}

    orchestrator._request_service = fake_request
    orchestrator.create_task_job = fake_create_task_job

    result = await orchestrator.scan_orphaned_task_jobs_once()

    assert result["recreated"] == 1
    assert created_calls[0]["service_payload"] == record["service_payload"]


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

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
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

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
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
