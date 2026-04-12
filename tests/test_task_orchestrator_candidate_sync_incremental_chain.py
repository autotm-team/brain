import asyncio

import pytest

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

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None, **kwargs):
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
    orchestrator.maybe_converge_snapshot_pipeline = fake_try_converge

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

    async def fake_create_task_job(service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None, **kwargs):
        created.append((service, job_type, params or {}, metadata or {}))
        return {"id": f"{job_type}-task"}

    async def fake_update(task_job_id, metadata):
        return None

    async def fake_append(task_job_id, event, normalized, **kwargs):
        return None

    async def fake_child_lineage(task_job_id):
        return {"parent_task_job_id": task_job_id}

    async def fake_converge(*args, **kwargs):
        return None

    orchestrator._claim_auto_chain = fake_claim
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append
    orchestrator._build_child_lineage_context = fake_child_lineage
    orchestrator.maybe_converge_snapshot_pipeline = fake_converge

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


def test_followup_error_is_recorded_without_raising_by_default():
    asyncio.run(_test_followup_error_is_recorded_without_raising_by_default())


async def _test_followup_error_is_recorded_without_raising_by_default():
    orchestrator = TaskOrchestrator(app={})
    metadata_updates = []
    history_events = []

    async def fake_claim(key):
        return True

    async def fake_create_task_job(*args, **kwargs):
        raise RuntimeError("child creation failed")

    async def fake_update(task_job_id, metadata):
        metadata_updates.append((task_job_id, metadata))

    async def fake_append(task_job_id, event, normalized, **kwargs):
        history_events.append((task_job_id, event))

    async def fake_child_lineage(task_job_id):
        return {"parent_task_job_id": task_job_id}

    orchestrator._claim_auto_chain = fake_claim
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append
    orchestrator._build_child_lineage_context = fake_child_lineage

    await orchestrator._maybe_trigger_followups(
        "task-batch-analyze",
        {
            "status": "succeeded",
            "service": "execution",
            "job_type": "batch_analyze",
            "result": {"last_trade_date": "2026-03-31"},
            "metadata": {},
        },
    )

    assert history_events == [("task-batch-analyze", "auto_chain_error")]
    assert metadata_updates[0][1]["auto_chain_error"] == "child creation failed"


def test_recovery_followup_error_releases_claim_and_raises():
    asyncio.run(_test_recovery_followup_error_releases_claim_and_raises())


async def _test_recovery_followup_error_releases_claim_and_raises():
    orchestrator = TaskOrchestrator(app={})
    released = []

    async def fake_claim(key):
        return True

    async def fake_release(key):
        released.append(key)
        return True

    async def fake_create_task_job(*args, **kwargs):
        raise RuntimeError("child creation failed")

    async def fake_update(*args, **kwargs):
        return None

    async def fake_append(*args, **kwargs):
        return None

    async def fake_child_lineage(task_job_id):
        return {"parent_task_job_id": task_job_id}

    orchestrator._claim_auto_chain = fake_claim
    orchestrator._release_auto_chain = fake_release
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append
    orchestrator._build_child_lineage_context = fake_child_lineage

    with pytest.raises(RuntimeError, match="child creation failed"):
        await orchestrator._maybe_trigger_followups(
            "task-batch-analyze",
            {
                "status": "succeeded",
                "service": "execution",
                "job_type": "batch_analyze",
                "result": {"last_trade_date": "2026-03-31"},
                "metadata": {},
            },
            raise_on_error=True,
        )

    assert released == ["candidate_sync_incremental:2026-03-31:chanlun,livermore,multi_indicator:30:candidate_sync_v4_per_analyzer"]


def test_claim_exists_without_child_raises_in_recovery_mode():
    asyncio.run(_test_claim_exists_without_child_raises_in_recovery_mode())


async def _test_claim_exists_without_child_raises_in_recovery_mode():
    orchestrator = TaskOrchestrator(app={})

    async def fake_claim(key):
        return False

    async def fake_find_child(**kwargs):
        return None

    async def fake_update(*args, **kwargs):
        return None

    async def fake_append(*args, **kwargs):
        return None

    orchestrator._claim_auto_chain = fake_claim
    orchestrator._find_idempotent_child_task = fake_find_child
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append

    with pytest.raises(RuntimeError, match="child job is missing"):
        await orchestrator._maybe_trigger_followups(
            "task-batch-analyze",
            {
                "status": "succeeded",
                "service": "execution",
                "job_type": "batch_analyze",
                "result": {"last_trade_date": "2026-03-31"},
                "metadata": {},
            },
            raise_on_error=True,
        )


def test_claim_exists_with_child_skips_duplicate_create():
    asyncio.run(_test_claim_exists_with_child_skips_duplicate_create())


async def _test_claim_exists_with_child_skips_duplicate_create():
    orchestrator = TaskOrchestrator(app={})
    metadata_updates = []
    create_called = False

    async def fake_claim(key):
        return False

    async def fake_find_child(**kwargs):
        return {"id": "existing-child-task"}

    async def fake_create_task_job(*args, **kwargs):
        nonlocal create_called
        create_called = True
        return {"id": "new-child-task"}

    async def fake_update(task_job_id, metadata):
        metadata_updates.append((task_job_id, metadata))

    async def fake_append(*args, **kwargs):
        return None

    orchestrator._claim_auto_chain = fake_claim
    orchestrator._find_idempotent_child_task = fake_find_child
    orchestrator.create_task_job = fake_create_task_job
    orchestrator._update_task_record_metadata = fake_update
    orchestrator._append_history = fake_append

    await orchestrator._maybe_trigger_followups(
        "task-batch-analyze",
        {
            "status": "succeeded",
            "service": "execution",
            "job_type": "batch_analyze",
            "result": {"last_trade_date": "2026-03-31"},
            "metadata": {},
        },
        raise_on_error=True,
    )

    assert create_called is False
    assert metadata_updates[0][1]["followup_candidate_sync_incremental_task_job_id"] == "existing-child-task"
