from pathlib import Path
import re


def test_brain_no_longer_registers_raw_signal_routes():
    routes_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/routes/__init__.py").read_text("utf-8")
    assert "setup_signal_routes" not in routes_text
    assert not Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/routes/signals.py").exists()


def test_brain_no_longer_registers_execution_helper_internal_ui_paths():
    ui_bff_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/handlers/ui_bff.py").read_text("utf-8")
    forbidden = [
        "/api/v1/ui/execution/analyzers",
        "/api/v1/ui/execution/stocks/top",
        "/api/v1/ui/execution/signals/latest",
        "/api/v1/ui/execution/signals/stream",
        "/api/v1/ui/execution/history",
        "/api/v1/ui/execution/analyzer-backtests",
        "/api/v1/ui/execution/analyzer-backtests/options",
        "/api/v1/ui/execution/backtests/history",
        "/api/v1/ui/execution/backtests/capabilities",
    ]
    for path in forbidden:
        assert path not in ui_bff_text


def test_brain_keeps_web_app_used_simulation_routes():
    ui_bff_text = Path("/Users/fuxingfu/cursorProjects/AutoTM/services/brain/handlers/ui_bff.py").read_text("utf-8")
    assert re.search(r"/api/v1/ui/execution/sim/orders", ui_bff_text)
    assert re.search(r"/api/v1/ui/execution/sim/positions", ui_bff_text)
    assert "/api/v1/ui/execution/sim/pnl" not in ui_bff_text
