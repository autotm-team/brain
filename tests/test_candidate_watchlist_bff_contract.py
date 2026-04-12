from pathlib import Path


def test_brain_ui_bff_keeps_candidate_watchlist_contracts():
    ui_bff_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/handlers/ui_bff.py").read_text("utf-8")
    task_orchestrator_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/task_orchestrator.py").read_text("utf-8")

    expected_paths = [
        "/api/v1/ui/candidates/watchlist",
        "/api/v1/ui/candidates/watchlist/metrics",
        "/api/v1/ui/candidates/watchlist/(?P<candidate_id>[^/]+)/history",
        "/api/v1/ui/candidates/watchlist/(?P<candidate_id>[^/]+)/transition",
        "/api/v1/ui/candidates/watchlist/(?P<candidate_id>[^/]+)/review",
        "/api/v1/ui/candidates/watchlist/(?P<candidate_id>[^/]+)/promote-to-research",
    ]
    expected_job_types = [
        "ui_candidates_watchlist_create",
        "ui_candidates_watchlist_item_update",
        "ui_candidates_watchlist_transition",
        "ui_candidates_watchlist_review",
        "ui_candidates_watchlist_promote_to_research",
    ]

    for path in expected_paths:
        assert path in ui_bff_text
    for job_type in expected_job_types:
        assert job_type in ui_bff_text
        assert job_type in task_orchestrator_text
