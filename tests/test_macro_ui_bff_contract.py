from pathlib import Path


def test_brain_ui_bff_keeps_macro_route_contracts():
    ui_bff_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/handlers/ui_bff.py").read_text("utf-8")

    expected_paths = [
        "/api/v1/ui/market-snapshot/list",
        "/api/v1/ui/market-snapshot/alerts",
        "/api/v1/ui/market-snapshot/latest",
        "/api/v1/ui/macro-cycle/inbox",
        "/api/v1/ui/macro-cycle/history",
        "/api/v1/ui/macro-cycle/calendar",
        "/api/v1/ui/structure-rotation/overview",
        "/api/v1/ui/structure-rotation/policy/current",
        "/api/v1/ui/structure-rotation/evidence",
        "/api/v1/ui/structure-rotation/policy/diff",
        "/api/v1/ui/structure-rotation/flow-migration",
    ]

    expected_job_types = [
        "ui_macro_cycle_freeze",
        "ui_macro_cycle_mark_seen",
        "ui_macro_cycle_mark_seen_batch",
        "ui_macro_cycle_apply_portfolio",
        "ui_macro_cycle_apply_snapshot",
        "ui_rotation_policy_freeze",
        "ui_rotation_policy_apply",
        "ui_rotation_policy_save",
    ]

    for path in expected_paths:
        assert path in ui_bff_text
    for job_type in expected_job_types:
        assert job_type in ui_bff_text
