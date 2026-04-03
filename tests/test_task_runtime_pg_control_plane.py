import sys
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"
for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

sys.modules.setdefault("tushare", SimpleNamespace())
sys.modules.setdefault("akshare", SimpleNamespace())

from task_orchestrator import TaskOrchestrator


class _FakeTaskRuntimeAPI:
    def __init__(self) -> None:
        self.jobs = {}
        self.history = {}
        self.schedules = {}
        self.links = {}

    def upsert_brain_task_job(self, payload):
        self.jobs[payload["task_job_id"]] = dict(payload)
        return dict(payload)

    def get_brain_task_job(self, task_job_id):
        row = self.jobs.get(task_job_id)
        return dict(row) if row else None

    def get_brain_task_job_by_service_job(self, service, service_job_id):
        for row in self.jobs.values():
            if row.get("service") == service and row.get("service_job_id") == service_job_id:
                return dict(row)
        return None

    def list_brain_task_job_ids(self):
        return list(self.jobs.keys())

    def append_brain_task_job_history(self, payload):
        self.history.setdefault(payload["task_job_id"], []).append(dict(payload))
        return True

    def list_brain_task_job_history(self, task_job_id, limit=200, offset=0):
        rows = [dict(row) for row in self.history.get(task_job_id, [])]
        return rows[offset: offset + limit], len(rows)

    def list_brain_task_jobs_for_cleanup(self, statuses, updated_before, limit=500):
        rows = []
        for row in self.jobs.values():
            if statuses and row.get("status") not in statuses:
                continue
            rows.append({"task_job_id": row["task_job_id"]})
        return rows[:limit]

    def delete_brain_task_jobs(self, task_job_ids):
        deleted = 0
        for task_job_id in task_job_ids:
            if self.jobs.pop(task_job_id, None) is not None:
                self.history.pop(task_job_id, None)
                deleted += 1
        return deleted

    def upsert_brain_schedule(self, payload):
        self.schedules[payload["schedule_id"]] = dict(payload)
        return dict(payload)

    def get_brain_schedule(self, schedule_id):
        row = self.schedules.get(schedule_id)
        return dict(row) if row else None

    def list_brain_schedules(self, service=None, limit=20, offset=0):
        rows = list(self.schedules.values())
        if service:
            rows = [row for row in rows if row.get("service") == service]
        return [dict(row) for row in rows[offset: offset + limit]], len(rows)

    def delete_brain_schedule(self, schedule_id):
        return 1 if self.schedules.pop(schedule_id, None) is not None else 0

    def upsert_brain_task_job_link(self, payload):
        self.links[payload["child_task_job_id"]] = dict(payload)
        return dict(payload)

    def get_brain_task_job_link(self, child_task_job_id):
        row = self.links.get(child_task_job_id)
        return dict(row) if row else None

    def list_brain_task_job_links_by_root(self, root_task_job_id):
        rows = [dict(row) for row in self.links.values() if row.get("root_task_job_id") == root_task_job_id]
        rows.sort(key=lambda row: (int(row.get("depth") or 0), str(row.get("child_task_job_id") or "")))
        return rows

    def list_brain_task_job_root_links_by_schedule(self, root_schedule_id, limit=20, offset=0):
        rows = [
            dict(row)
            for row in self.links.values()
            if row.get("root_schedule_id") == root_schedule_id and int(row.get("depth") or 0) == 0
        ]
        rows.sort(key=lambda row: str(row.get("child_task_job_id") or ""), reverse=True)
        return rows[offset: offset + limit], len(rows)

    def list_brain_task_jobs_by_ids(self, task_job_ids):
        return [dict(self.jobs[item]) for item in task_job_ids if item in self.jobs]


class _FakeServiceRuntime:
    def __init__(self) -> None:
        self.counter = 0
        self.jobs = {}

    async def request(self, service, method, path, payload=None, params=None):
        if method == "POST" and path == "/api/v1/jobs":
            self.counter += 1
            job_id = f"{service}-job-{self.counter}"
            body = payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
            job_type = body.get("job_type") if isinstance(body, dict) else "unknown"
            job_params = body.get("params") if isinstance(body, dict) and isinstance(body.get("params"), dict) else {}
            job = {
                "job_id": job_id,
                "job_type": job_type,
                "status": "queued",
                "progress": 0,
                "params": job_params,
                "metadata": {},
                "created_at": "2026-03-31T00:00:00Z",
                "updated_at": "2026-03-31T00:00:00Z",
            }
            self.jobs[job_id] = job
            return {"data": {"job_id": job_id}}
        if method == "GET" and path.startswith("/api/v1/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            return {"data": dict(self.jobs[job_id])}
        raise AssertionError(f"unexpected request: {service} {method} {path}")


async def _noop_validate(*args, **kwargs):
    return None

def test_task_orchestrator_uses_pg_backed_control_plane_storage():
    asyncio.run(_test_task_orchestrator_uses_pg_backed_control_plane_storage())


async def _test_task_orchestrator_uses_pg_backed_control_plane_storage():
    orchestrator = TaskOrchestrator(app={})
    fake_api = _FakeTaskRuntimeAPI()
    orchestrator._task_api = fake_api

    record = {
        "task_job_id": "brain-task-1",
        "service": "execution",
        "service_job_id": "svc-job-1",
        "job_type": "batch_analyze",
        "status": "queued",
        "progress": 0,
        "params": {"symbols": ["000001.SZ"]},
        "metadata": {"source": "test"},
        "created_at": "2026-03-30T00:00:00Z",
        "updated_at": "2026-03-30T00:00:00Z",
    }
    await orchestrator._save_task_record(record)
    assert fake_api.get_brain_task_job("brain-task-1") is not None

    await orchestrator._append_history(
        "brain-task-1",
        "created",
        {
            "id": "brain-task-1",
            "service": "execution",
            "service_job_id": "svc-job-1",
            "job_type": "batch_analyze",
            "status": "queued",
            "progress": 0,
            "metadata": {"source": "test"},
        },
    )
    history = await orchestrator.get_task_job_history("brain-task-1")
    assert history["history"][0]["event"] == "created"

    resolved = await orchestrator._find_task_job_id("execution", "svc-job-1")
    assert resolved == "brain-task-1"

    deleted = await orchestrator.cleanup_completed_task_jobs(max_age_hours=1)
    assert deleted == 0


def test_task_orchestrator_persists_schedule_and_auto_chain_lineage():
    asyncio.run(_test_task_orchestrator_persists_schedule_and_auto_chain_lineage())


async def _test_task_orchestrator_persists_schedule_and_auto_chain_lineage():
    orchestrator = TaskOrchestrator(app={})
    fake_api = _FakeTaskRuntimeAPI()
    fake_runtime = _FakeServiceRuntime()
    orchestrator._task_api = fake_api
    orchestrator._request_service = fake_runtime.request
    orchestrator._validate_job_type = _noop_validate

    fake_api.upsert_brain_schedule(
        {
            "schedule_id": "schedule-1",
            "service": "execution",
            "job_type": "batch_analyze",
            "trigger": "cron",
            "cron_expr": "30 16 * * 1-5",
            "enabled": True,
            "params": {"incremental": True},
            "metadata": {"name": "daily batch analyze"},
            "created_at": "2026-03-31T00:00:00Z",
            "updated_at": "2026-03-31T00:00:00Z",
        }
    )

    triggered = await orchestrator.trigger_schedule("schedule-1")
    root_job = triggered["job"]
    root_link = fake_api.get_brain_task_job_link(root_job["id"])
    assert root_link["root_schedule_id"] == "schedule-1"
    assert root_link["trigger_kind"] == "schedule"
    assert root_link["depth"] == 0

    child = await orchestrator.create_task_job(
        service="execution",
        job_type="ui_candidates_sync_from_analysis",
        params={"as_of_date": "2026-03-31"},
        metadata={"auto_chain_source": "batch_analyze"},
        lineage_context=await orchestrator._build_child_lineage_context(root_job["id"]),
    )
    grandchild = await orchestrator.create_task_job(
        service="macro",
        job_type="ui_snapshot_refresh",
        params={"benchmark": "CSI300"},
        metadata={"auto_chain_source": "candidate_sync"},
        lineage_context=await orchestrator._build_child_lineage_context(child["id"]),
    )

    child_link = fake_api.get_brain_task_job_link(child["id"])
    grand_link = fake_api.get_brain_task_job_link(grandchild["id"])
    assert child_link["parent_task_job_id"] == root_job["id"]
    assert child_link["root_task_job_id"] == root_job["id"]
    assert child_link["depth"] == 1
    assert grand_link["parent_task_job_id"] == child["id"]
    assert grand_link["root_task_job_id"] == root_job["id"]
    assert grand_link["depth"] == 2

    lineage = await orchestrator.get_task_job_lineage(grandchild["id"])
    assert lineage["root_task_job_id"] == root_job["id"]
    assert lineage["summary"]["max_depth"] == 2
    assert lineage["summary"]["leaf_count"] == 1
    assert len(lineage["edges"]) == 2
    assert lineage["lineage_tree"][0]["task_job_id"] == root_job["id"]


def test_create_task_job_reuses_existing_record_for_deduplicated_upstream_job():
    asyncio.run(_test_create_task_job_reuses_existing_record_for_deduplicated_upstream_job())


async def _test_create_task_job_reuses_existing_record_for_deduplicated_upstream_job():
    orchestrator = TaskOrchestrator(app={})
    fake_api = _FakeTaskRuntimeAPI()
    orchestrator._task_api = fake_api

    existing = {
        "task_job_id": "brain-task-1",
        "service": "flowhub",
        "service_job_id": "svc-job-1",
        "job_type": "batch_daily_ohlc",
        "status": "queued",
        "progress": 0,
        "params": {"incremental": True},
        "metadata": {"source": "old"},
        "created_at": "2026-03-31T00:00:00Z",
        "updated_at": "2026-03-31T00:00:00Z",
    }
    await orchestrator._save_task_record(existing)

    async def fake_validate(service, job_type):
        return None

    async def fake_request(service, method, path, payload=None, params=None):
        return {
            "data": {
                "job_id": "svc-job-1",
                "deduplicated": True,
                "job": {
                    "job_id": "svc-job-1",
                    "job_type": "batch_daily_ohlc",
                    "status": "running",
                    "progress": 15,
                    "params": {"incremental": True},
                    "metadata": {"message": "accepted_existing"},
                },
            }
        }

    async def fake_safe_get(service, service_job_id):
        return {
            "job_id": service_job_id,
            "job_type": "batch_daily_ohlc",
            "status": "running",
            "progress": 15,
            "params": {"incremental": True},
            "metadata": {"message": "accepted_existing"},
        }

    orchestrator._validate_job_type = fake_validate
    orchestrator._request_service = fake_request
    orchestrator._safe_get_service_job = fake_safe_get
    orchestrator._schedule_followup_monitor = lambda *args, **kwargs: None

    created = await orchestrator.create_task_job(
        "flowhub",
        "batch_daily_ohlc",
        params={"incremental": True},
        metadata={"source": "new"},
    )

    assert created["id"] == "brain-task-1"
    assert len(fake_api.jobs) == 1
    assert fake_api.get_brain_task_job("brain-task-1")["metadata"]["source"] == "new"


def test_resolve_snapshot_trade_date_is_strict_by_job_type():
    orchestrator = TaskOrchestrator(app={})

    assert orchestrator._resolve_snapshot_trade_date(
        {
            "service": "execution",
            "job_type": "batch_analyze",
            "result": {"last_trade_date": "2026-03-30", "trade_date": "2026-03-31"},
        }
    ) == "2026-03-30"

    assert orchestrator._resolve_snapshot_trade_date(
        {
            "service": "flowhub",
            "job_type": "index_daily_data",
            "result": {"latest_trade_date": "2026-03-31"},
        }
    ) == "2026-03-31"

    assert orchestrator._resolve_snapshot_trade_date(
        {
            "service": "flowhub",
            "job_type": "industry_board",
            "result": {"trade_date": "2026-03-31"},
        }
    ) == "2026-03-31"

    assert orchestrator._resolve_snapshot_trade_date(
        {
            "service": "flowhub",
            "job_type": "index_daily_data",
            "result": {"target_trade_date": "2026-03-31"},
        }
    ) is None


def test_snapshot_refresh_failure_releases_chain_lock():
    asyncio.run(_test_snapshot_refresh_failure_releases_chain_lock())


async def _test_snapshot_refresh_failure_releases_chain_lock():
    orchestrator = TaskOrchestrator(app={})
    released = []
    events = []

    async def fake_release(key):
        released.append(key)
        return True

    async def fake_append(task_job_id, event, normalized, **kwargs):
        events.append((task_job_id, event))

    orchestrator._release_auto_chain = fake_release
    orchestrator._append_history = fake_append

    await orchestrator._maybe_release_failed_chain_locks(
        "task-snapshot",
        {
            "service": "macro",
            "job_type": "ui_snapshot_refresh",
            "metadata": {"auto_chain_trade_date": "2026-03-30"},
        },
    )

    assert released == ["snapshot_refresh:2026-03-30"]
    assert events == [("task-snapshot", "auto_chain_lock_released")]
