from handlers.ui_bff import UIBffHandler


def test_watchlist_templates_only_emit_stock_basic_root_schedule():
    handler = UIBffHandler.__new__(UIBffHandler)
    templates = handler._build_watchlist_default_templates()
    data_types = {item["data_type"] for item in templates}

    assert "stock_basic_data" in data_types
    assert "batch_daily_ohlc" not in data_types
    assert "batch_daily_basic" not in data_types


def test_structure_rotation_templates_do_not_emit_dependent_stock_root_schedule():
    handler = UIBffHandler.__new__(UIBffHandler)
    templates = handler._build_structure_rotation_default_templates()
    data_types = {item["data_type"] for item in templates}

    assert "batch_daily_basic" not in data_types
