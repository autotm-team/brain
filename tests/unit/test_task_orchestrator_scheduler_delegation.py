import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from task_orchestrator import TaskOrchestrator


class _FakeUnifiedScheduler:
    def __init__(self):
        self.created_payload = None
        self.marked = None

    async def list_schedules(self, **kwargs):
        return {"items": [], "total": 0, "limit": kwargs.get("limit", 20), "offset": kwargs.get("offset", 0)}

    async def create_schedule(self, payload):
        self.created_payload = dict(payload)
        return {"id": "sched-1", **payload}

    async def update_schedule(self, schedule_id, payload):
        return {"id": schedule_id, **payload}

    async def delete_schedule(self, schedule_id):
        return schedule_id == "sched-1"

    async def get_schedule(self, schedule_id):
        if schedule_id != "sched-1":
            return None
        return {
            "id": "sched-1",
            "service": "macro",
            "job_type": "ui_macro_cycle_freeze",
            "params": {"x": 1},
            "metadata": {"source": "test"},
            "enabled": True,
        }

    async def mark_triggered(self, schedule_id, task_job_id):
        self.marked = (schedule_id, task_job_id)
        return {"id": schedule_id, "last_task_job_id": task_job_id, "enabled": True}


@pytest.mark.asyncio
async def test_schedule_methods_delegate_to_unified_scheduler(monkeypatch):
    fake = _FakeUnifiedScheduler()
    orchestrator = TaskOrchestrator({"redis": None, "unified_scheduler": fake})

    async def _types(_service):
        return {"ui_macro_cycle_freeze"}

    async def _create_task_job(*_args, **_kwargs):
        return {"id": "task-job-1"}

    monkeypatch.setattr(orchestrator, "_fetch_service_job_type_set", _types)
    monkeypatch.setattr(orchestrator, "create_task_job", _create_task_job)

    listed = await orchestrator.list_schedules(service="macro", limit=10, offset=0)
    assert listed["total"] == 0

    created = await orchestrator.create_schedule(
        {
            "service": "macro",
            "job_type": "ui_macro_cycle_freeze",
            "trigger": "interval",
            "interval_seconds": 60,
            "enabled": True,
            "params": {"x": 1},
            "metadata": {"source": "test"},
        }
    )
    assert created["id"] == "sched-1"
    assert fake.created_payload is not None
    assert fake.created_payload["service"] == "macro"

    triggered = await orchestrator.trigger_schedule("sched-1")
    assert triggered["job"]["id"] == "task-job-1"
    assert fake.marked == ("sched-1", "task-job-1")
