import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"
for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from handlers.ui_bff import UIBffHandler


def test_watchlist_templates_only_emit_stock_basic_root_schedule():
    handler = UIBffHandler.__new__(UIBffHandler)
    templates = handler._build_watchlist_default_templates()
    data_types = {item["data_type"] for item in templates}

    assert "stock_basic_data" in data_types
    assert "batch_daily_ohlc" not in data_types
    assert "batch_daily_basic" not in data_types
