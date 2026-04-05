import asyncio

from task_orchestrator import TaskOrchestrator


def test_batch_analyze_success_triggers_candidate_sync_incremental():
    asyncio.run(_test_batch_analyze_success_triggers_candidate_sync_incremental())


async def _test_batch_analyze_success_triggers_candidate_sync_incremental():
    orchestrator = TaskOrchestrator(app={})
    created = []
    metadata_updates = []
    history_events = []

    async def fake_claim(key):
        created.append(("claim", key))
        return True

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
        created.append((service, job_type, params or {}, metadata or {}))
        return {"id": f"{job_type}-task"}

    async def fake_update(task_job_id, metadata):
        metadata_updates.append((task_job_id, metadata))

    async def fake_append(task_job_id, event, normalized, **kwargs):
        history_events.append((task_job_id, event))

    async def fake_child_lineage(task_job_id):
        return {"parent_task_job_id": task_job_id}

    async def fake_try_converge(*args, **kwargs):
        return None

    orchestrator._claim_auto_chain = fake_claim
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append
    orchestrator._build_child_lineage_context = fake_child_lineage
    orchestrator._try_converge_snapshot_prereq = fake_try_converge

    await orchestrator._maybe_trigger_followups(
        "task-batch-analyze",
        {
            "status": "succeeded",
            "service": "execution",
            "job_type": "batch_analyze",
            "result": {"last_trade_date": "2026-03-31"},
        },
    )

    assert created[0] == ("claim", "candidate_sync_incremental:2026-03-31:chanlun,livermore,multi_indicator:30:candidate_sync_v4_per_analyzer")
    _, job_type, params, metadata = created[1]
    assert job_type == "ui_candidates_sync_incremental"
    assert params["mode"] == "resume_repair"
    assert params["from_trade_date"] == "2026-03-31"
    assert params["to_trade_date"] == "2026-03-31"
    assert params["repair_window_days"] == 7
    assert metadata["auto_chain_source"] == "batch_analyze"
    assert history_events[0] == ("task-batch-analyze", "auto_chain_created")


def test_analysis_backfill_success_triggers_candidate_sync_incremental():
    asyncio.run(_test_analysis_backfill_success_triggers_candidate_sync_incremental())


async def _test_analysis_backfill_success_triggers_candidate_sync_incremental():
    orchestrator = TaskOrchestrator(app={})
    created = []

    async def fake_claim(key):
        created.append(("claim", key))
        return True

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
        created.append((service, job_type, params or {}, metadata or {}))
        return {"id": f"{job_type}-task"}

    async def fake_update(task_job_id, metadata):
        return None

    async def fake_append(task_job_id, event, normalized, **kwargs):
        return None

    async def fake_child_lineage(task_job_id):
        return {"parent_task_job_id": task_job_id}

    orchestrator._claim_auto_chain = fake_claim
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append
    orchestrator._build_child_lineage_context = fake_child_lineage

    await orchestrator._maybe_trigger_followups(
        "task-analysis-backfill",
        {
            "status": "succeeded",
            "service": "execution",
            "job_type": "analysis_backfill",
            "result": {
                "coverage_start_date": "2026-03-01",
                "last_trade_date": "2026-03-31",
            },
        },
    )

    _, job_type, params, metadata = created[1]
    assert job_type == "ui_candidates_sync_incremental"
    assert params["mode"] == "resume_repair"
    assert params["from_trade_date"] == "2026-03-01"
    assert params["to_trade_date"] == "2026-03-31"
    assert params["repair_window_days"] == 30
    assert metadata["auto_chain_source"] == "analysis_backfill"
