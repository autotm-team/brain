from pathlib import Path


def test_brain_health_surface_only_keeps_basic_health_route():
    routes_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/routes/health.py").read_text("utf-8")
    assert "/health" in routes_text
    assert "/api/v1/status" not in routes_text
    assert "/api/v1/info" not in routes_text


def test_brain_tasks_surface_does_not_register_raw_schedules():
    routes_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/routes/tasks.py").read_text("utf-8")
    assert "/api/v1/task-jobs" in routes_text
    assert "/api/v1/schedules" not in routes_text
