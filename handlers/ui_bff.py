"""
Unified UI BFF proxy handler.
"""

import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import secrets
import sys
from urllib.parse import unquote
import uuid
from typing import Any, Callable, Dict, Optional, Pattern

from aiohttp import web

from handlers.base import BaseHandler
from task_orchestrator import UpstreamServiceError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ECONDB_PATH = PROJECT_ROOT / "external" / "econdb"
if ECONDB_PATH.exists() and str(ECONDB_PATH) not in sys.path:
    sys.path.insert(0, str(ECONDB_PATH))

try:
    from econdb import (  # type: ignore
        UISystemDataAPI,
        SystemAuditDTO,
        SystemRoleDTO,
        SystemSettingDTO,
        SystemUserDTO,
        create_database_manager,
    )
except Exception:  # pragma: no cover - import fallback for old submodule commits
    UISystemDataAPI = None
    SystemAuditDTO = None
    SystemRoleDTO = None
    SystemSettingDTO = None
    SystemUserDTO = None
    create_database_manager = None


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
}

CORS_HEADERS = {
    "access-control-allow-origin",
    "access-control-allow-credentials",
    "access-control-allow-methods",
    "access-control-allow-headers",
    "access-control-expose-headers",
    "access-control-max-age",
}


@dataclass(frozen=True)
class RouteSpec:
    method: str
    pattern: Pattern[str]
    service: str
    is_mutation: bool = False
    job_type: Optional[str] = None
    params_builder: Optional[Callable[[Dict[str, Any], Dict[str, str]], Dict[str, Any]]] = None


class UIBffHandler(BaseHandler):
    """Brain-side frontend BFF for /api/v1/ui/*."""

    ROUTES = (
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/list$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/alerts$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/latest$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)$"), "macro"),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/evidence/(?P<ev_key>[^/]+)$"),
            "macro",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/compare$"),
            "macro",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/market-snapshot/(?P<snapshot_id>[^/]+)/export$"),
            "macro",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/inbox$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/history$"), "macro"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/macro-cycle/freeze$"), "macro", True, "ui_macro_cycle_freeze"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)$"), "macro"),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/evidence/(?P<key>[^/]+)$"),
            "macro",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/macro-cycle/calendar$"), "macro"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/mark-seen$"),
            "macro",
            True,
            "ui_macro_cycle_mark_seen",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/mark-seen/batch$"),
            "macro",
            True,
            "ui_macro_cycle_mark_seen_batch",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/apply/portfolio$"),
            "macro",
            True,
            "ui_macro_cycle_apply_portfolio",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/macro-cycle/(?P<snapshot_id>[^/]+)/apply/snapshot$"),
            "macro",
            True,
            "ui_macro_cycle_apply_snapshot",
            lambda payload, params: {**payload, "snapshot_id": params["snapshot_id"]},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/overview$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/policy/current$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/evidence$"), "macro"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/structure-rotation/policy/freeze$"),
            "macro",
            True,
            "ui_rotation_policy_freeze",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/structure-rotation/policy/apply$"),
            "macro",
            True,
            "ui_rotation_policy_apply",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/structure-rotation/policy/save$"),
            "macro",
            True,
            "ui_rotation_policy_save",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/policy/diff$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/clusters$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/events$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/symbols/(?P<symbol>[^/]+)/chart$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/audit/report$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/history/query$"),
            "execution",
            True,
            "ui_candidates_history_query",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/history/(?P<query_id>[^/]+)$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/candidates/promote$"), "execution", True, "ui_candidates_promote"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/sync-from-analysis$"),
            "execution",
            True,
            "ui_candidates_sync_from_analysis",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/backfill$"),
            "execution",
            True,
            "ui_candidates_backfill_metadata",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/auto-promote$"),
            "execution",
            True,
            "ui_candidates_auto_promote",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/merge$"),
            "execution",
            True,
            "ui_candidates_merge",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/(?P<id>[^/]+)/ignore$"),
            "execution",
            True,
            "ui_candidates_ignore",
            lambda payload, params: {**payload, "event_id": params["id"], "ignore_only": True},
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/mark-read$"),
            "execution",
            True,
            "ui_candidates_mark_read",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/candidates/watchlist/(?P<subject_id>[^/]+)/status$"),
            "execution",
            True,
            "ui_candidates_watchlist_update",
            lambda payload, params: {**payload, "subject_id": params["subject_id"]},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/export$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/subjects$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/decision$"),
            "execution",
            True,
            "ui_research_decision",
            lambda payload, params: {**payload, "subject_id": params["subject_id"]},
        ),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/freeze$"), "execution", True, "ui_research_freeze"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/compare$"), "execution", True, "ui_research_compare"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/archive$"),
            "execution",
            True,
            "ui_research_archive",
            lambda payload, params: {
                **payload,
                "subject_id": params["subject_id"],
                "archive": payload.get("archive", True),
            },
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/research/subjects/(?P<subject_id>[^/]+)/unfreeze$"),
            "execution",
            True,
            "ui_research_unfreeze",
            lambda payload, params: {**payload, "subject_id": params["subject_id"], "unfreeze": True},
        ),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/research/replace-helper$"), "execution", True, "ui_research_replace_helper"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/research/export$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/config/apply$"), "execution", True, "ui_strategy_config_apply"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/presets$"), "execution", True, "ui_strategy_preset_save"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/presets$"), "execution"),
        RouteSpec("POST", re.compile(r"^/api/v1/ui/strategy/reports/run$"), "execution", True, "ui_strategy_report_run"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/reports$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)$"), "execution"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/strategy/reports/compare$"),
            "execution",
            True,
            "ui_strategy_report_compare",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/lineage/export$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/analysis$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/trades/export$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/positions/export$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/logs$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/decisions$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/reports/(?P<report_id>[^/]+)/order-plans$"),
            "execution",
        ),
        RouteSpec(
            "GET",
            re.compile(r"^/api/v1/ui/strategy/order-plans/latest$"),
            "execution",
        ),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/execution/sim/orders$"),
            "portfolio",
            True,
            "ui_sim_order_create",
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/orders$"), "portfolio"),
        RouteSpec(
            "POST",
            re.compile(r"^/api/v1/ui/execution/sim/orders/(?P<order_id>[^/]+)/cancel$"),
            "portfolio",
            True,
            "ui_sim_order_cancel",
            lambda payload, params: {**payload, "order_id": params["order_id"]},
        ),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/positions$"), "portfolio"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/execution/sim/pnl$"), "portfolio"),
    )

    SHELL_CONTEXT_PAGES = {
        "overview": {"phase": "市场快照", "page_title": "市场快照", "service": "macro"},
        "macro": {"phase": "宏观与周期", "page_title": "宏观与周期", "service": "macro"},
        "structure": {"phase": "结构与轮动", "page_title": "结构与轮动", "service": "macro"},
        "watchlist": {"phase": "候选池", "page_title": "候选池", "service": "execution"},
        "stock": {"phase": "标的研究", "page_title": "标的研究", "service": "execution"},
        "strategy": {"phase": "策略与验证", "page_title": "策略与验证", "service": "execution"},
        "jobs": {"phase": "系统任务", "page_title": "任务中心", "service": "brain"},
        "data": {"phase": "系统数据", "page_title": "数据运维", "service": "flowhub"},
        "settings": {"phase": "系统设置", "page_title": "设置中心", "service": "brain"},
        "users": {"phase": "系统用户", "page_title": "用户与权限", "service": "brain"},
    }

    DATA_READINESS_REQUIREMENTS = {
        "market_snapshot": {
            "label": "市场快照",
            "data_types": [
                {"data_type": "stock_index_data", "label": "指数行情"},
                {"data_type": "market_flow_data", "label": "市场资金流"},
                {"data_type": "interest_rate_data", "label": "利率与流动性"},
                {"data_type": "commodity_price_data", "label": "大宗商品"},
            ],
        },
        "macro_cycle": {
            "label": "宏观与周期",
            "data_types": [
                {"data_type": "price_index_data", "label": "价格指数"},
                {"data_type": "money_supply_data", "label": "货币供应"},
                {"data_type": "social_financing_data", "label": "社融"},
                {"data_type": "industrial_data", "label": "工业数据"},
                {"data_type": "sentiment_index_data", "label": "景气与情绪"},
                {"data_type": "gdp_data", "label": "GDP"},
                {"data_type": "macro_calendar_data", "label": "宏观日历"},
            ],
        },
        "structure_rotation": {
            "label": "结构与轮动",
            "data_types": [
                {"data_type": "industry_board", "label": "行业板块行情"},
                {"data_type": "concept_board", "label": "概念板块行情"},
                {"data_type": "industry_board_stocks", "label": "行业板块成分股"},
                {"data_type": "concept_board_stocks", "label": "概念板块成分股"},
                {"data_type": "industry_moneyflow_data", "label": "行业资金流"},
                {"data_type": "concept_moneyflow_data", "label": "概念资金流"},
                {"data_type": "batch_daily_basic", "label": "个股日线基础"},
            ],
        },
        "watchlist": {
            "label": "候选池",
            "data_types": [
                {"data_type": "batch_daily_ohlc", "label": "个股日线行情"},
                {"data_type": "batch_daily_basic", "label": "个股日线基础"},
                {"data_type": "adj_factors", "label": "复权因子"},
                {"data_type": "industry_board_stocks", "label": "行业板块成分股"},
                {"data_type": "industry_moneyflow_data", "label": "行业资金流"},
            ],
        },
    }

    READINESS_LABELS = {
        "ready": "已准备好",
        "no_data": "无有效数据",
        "fetching": "抓取中",
        "waiting_schedule": "等待定时抓取",
        "failed": "抓取失败",
        "missing_task": "未配置任务",
        "backend_error": "后端接口异常",
    }

    MARKET_SNAPSHOT_TEMPLATE_SCHEDULES = {
        "stock_index_data": {"schedule_type": "cron", "schedule_value": "30 16 * * 1-5"},
        "market_flow_data": {"schedule_type": "cron", "schedule_value": "40 16 * * 1-5"},
        "interest_rate_data": {"schedule_type": "cron", "schedule_value": "45 16 * * 1-5"},
        "commodity_price_data": {"schedule_type": "cron", "schedule_value": "50 16 * * 1-5"},
    }

    MARKET_SNAPSHOT_TEMPLATE_NAMES = {
        "stock_index_data": "模板·市场快照·指数行情",
        "market_flow_data": "模板·市场快照·市场资金流",
        "interest_rate_data": "模板·市场快照·利率与流动性",
        "commodity_price_data": "模板·市场快照·大宗商品",
    }

    MARKET_SNAPSHOT_TEMPLATE_PARAMS = {
        "stock_index_data": {"incremental": True},
        "market_flow_data": {"incremental": True},
        "interest_rate_data": {"incremental": True},
        "commodity_price_data": {"incremental": True},
    }

    MACRO_CYCLE_TEMPLATE_SCHEDULES = {
        "macro_calendar_data": {"schedule_type": "cron", "schedule_value": "30 16 * * *"},
        "price_index_data": {"schedule_type": "cron", "schedule_value": "10 19 1 * *"},
        "money_supply_data": {"schedule_type": "cron", "schedule_value": "15 19 1 * *"},
        "social_financing_data": {"schedule_type": "cron", "schedule_value": "20 19 1 * *"},
        "industrial_data": {"schedule_type": "cron", "schedule_value": "25 19 1 * *"},
        "sentiment_index_data": {"schedule_type": "cron", "schedule_value": "30 19 1 * *"},
        "gdp_data": {"schedule_type": "cron", "schedule_value": "40 19 15 1,4,7,10 *"},
    }

    STRUCTURE_ROTATION_TEMPLATE_SCHEDULES = {
        "industry_board": {"schedule_type": "cron", "schedule_value": "30 16 * * 1-5"},
        "concept_board": {"schedule_type": "cron", "schedule_value": "30 16 * * 1-5"},
        "industry_board_stocks": {"schedule_type": "cron", "schedule_value": "40 16 * * 1-5"},
        "concept_board_stocks": {"schedule_type": "cron", "schedule_value": "55 16 * * 1-5"},
        "industry_moneyflow_data": {"schedule_type": "cron", "schedule_value": "10 17 * * 1-5"},
        "concept_moneyflow_data": {"schedule_type": "cron", "schedule_value": "15 17 * * 1-5"},
        "batch_daily_basic": {"schedule_type": "cron", "schedule_value": "30 17 * * 1-5"},
    }

    STRUCTURE_ROTATION_TEMPLATE_NAMES = {
        "industry_board": "模板·结构与轮动·行业板块行情",
        "concept_board": "模板·结构与轮动·概念板块行情",
        "industry_board_stocks": "模板·结构与轮动·行业板块成分股",
        "concept_board_stocks": "模板·结构与轮动·概念板块成分股",
        "industry_moneyflow_data": "模板·结构与轮动·行业资金流",
        "concept_moneyflow_data": "模板·结构与轮动·概念资金流",
        "batch_daily_basic": "模板·结构与轮动·个股日线基础",
    }

    STRUCTURE_ROTATION_TEMPLATE_PARAMS = {
        "industry_board": {"source": "ths", "update_mode": "full_update"},
        "concept_board": {"source": "ths", "update_mode": "full_update"},
        "industry_board_stocks": {"source": "ths", "update_mode": "full_update"},
        "concept_board_stocks": {"source": "ths", "update_mode": "full_update"},
        "industry_moneyflow_data": {"source": "ths", "incremental": True},
        "concept_moneyflow_data": {"source": "em", "incremental": True},
        "batch_daily_basic": {"incremental": True},
    }

    WATCHLIST_TEMPLATE_SCHEDULES = {
        "batch_daily_ohlc": {"schedule_type": "cron", "schedule_value": "10 17 * * 1-5"},
        "batch_daily_basic": {"schedule_type": "cron", "schedule_value": "30 17 * * 1-5"},
        "adj_factors": {"schedule_type": "cron", "schedule_value": "45 17 * * 1-5"},
        "industry_board_stocks": {"schedule_type": "cron", "schedule_value": "40 16 * * 1-5"},
        "industry_moneyflow_data": {"schedule_type": "cron", "schedule_value": "10 17 * * 1-5"},
    }

    WATCHLIST_TEMPLATE_NAMES = {
        "batch_daily_ohlc": "模板·候选池·个股日线行情",
        "batch_daily_basic": "模板·候选池·个股日线基础",
        "adj_factors": "模板·候选池·复权因子",
        "industry_board_stocks": "模板·候选池·行业板块成分股",
        "industry_moneyflow_data": "模板·候选池·行业资金流",
    }

    WATCHLIST_TEMPLATE_PARAMS = {
        "batch_daily_ohlc": {"incremental": True},
        "batch_daily_basic": {"incremental": True},
        "adj_factors": {"update_mode": "incremental"},
        "industry_board_stocks": {"source": "ths", "update_mode": "incremental"},
        "industry_moneyflow_data": {"source": "ths", "incremental": True},
    }

    READINESS_REAL_DATA_TABLES = {
        "stock_index_data": {"table": "macro_stock_index_data", "date_column": "date", "value_kind": "date"},
        "market_flow_data": {"table": "macro_market_flow_data", "date_column": "date", "value_kind": "date"},
        "interest_rate_data": {"table": "macro_interest_rate_data", "date_column": "date", "value_kind": "date"},
        "commodity_price_data": {"table": "macro_commodity_price_data", "date_column": "date", "value_kind": "date"},
        "price_index_data": {"table": "macro_price_index_data", "date_column": "month", "value_kind": "date"},
        "money_supply_data": {"table": "macro_money_supply_data", "date_column": "month", "value_kind": "period"},
        "social_financing_data": {"table": "macro_social_financing_data", "date_column": "month", "value_kind": "period"},
        "industrial_data": {"table": "macro_industrial_data", "date_column": "month", "value_kind": "period"},
        "sentiment_index_data": {"table": "macro_sentiment_index_data", "date_column": "month", "value_kind": "date"},
        "gdp_data": {"table": "macro_gdp_data", "date_column": "quarter", "value_kind": "period"},
        "macro_calendar_data": {"table": "macro_calendar_events", "date_column": "event_date", "value_kind": "date"},
        "industry_board": {"table": "industry_board_daily_data", "date_column": "trade_date"},
        "concept_board": {"table": "concept_board_daily_data", "date_column": "trade_date"},
        "industry_board_stocks": {"table": "industry_board_stocks", "date_column": "trade_date"},
        "concept_board_stocks": {"table": "concept_board_stocks", "date_column": "trade_date"},
        "industry_moneyflow_data": {"table": "industry_moneyflow_daily", "date_column": "trade_date"},
        "concept_moneyflow_data": {"table": "concept_moneyflow_daily", "date_column": "trade_date"},
        "batch_daily_ohlc": {"table": "stock_daily_data", "date_column": "trade_date"},
        "batch_daily_basic": {"table": "stock_daily_basic", "date_column": "trade_date"},
        "adj_factors": {"table": "adj_factors", "date_column": "trade_date"},
    }

    def __init__(self):
        super().__init__()
        self._system_api = None
        self._macro_template_lock = asyncio.Lock()

    @staticmethod
    def _match_route(method: str, path: str) -> tuple[Optional[RouteSpec], Dict[str, str]]:
        effective_method = "GET" if method.upper() == "HEAD" else method.upper()
        for route in UIBffHandler.ROUTES:
            if route.method != effective_method:
                continue
            matched = route.pattern.match(path)
            if matched:
                return route, matched.groupdict()
        return None, {}

    def _build_job_params(
        self,
        route: RouteSpec,
        request_payload: Dict[str, Any],
        path_params: Dict[str, str],
    ) -> Dict[str, Any]:
        payload = dict(request_payload or {})
        if route.params_builder:
            return route.params_builder(payload, path_params)
        return payload

    def _get_system_api(self):
        if UISystemDataAPI is None or create_database_manager is None:
            raise RuntimeError("UISystemDataAPI unavailable; update external/econdb submodule baseline")
        if self._system_api is None:
            db_manager = create_database_manager()
            self._system_api = UISystemDataAPI(db_manager)
            self._system_api.seed_defaults()
        return self._system_api

    @staticmethod
    def _unwrap_response_data(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {"raw": payload}
        if "data" in payload and isinstance(payload["data"], dict):
            return payload["data"]
        return payload

    @staticmethod
    def _now_iso() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _is_future_iso(value: Optional[str]) -> bool:
        if not value:
            return True
        try:
            target = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return target.timestamp() > datetime.utcnow().timestamp()
        except Exception:
            return False

    def _load_active_silence_index(self) -> Dict[str, Dict[str, Any]]:
        try:
            notifications = self._get_system_api().list_notifications(limit=500)
        except Exception as exc:
            self.logger.warning(f"load silence notifications failed: {exc}")
            return {}

        index: Dict[str, Dict[str, Any]] = {}
        for item in notifications:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            if str(payload.get("type") or "") != "alert_silence":
                continue

            alert_id = str(payload.get("alert_id") or "").strip()
            if not alert_id or alert_id in index:
                continue

            status = str(item.get("status") or "").lower()
            if status in {"acked", "resolved", "dismissed", "closed"}:
                continue

            silenced_until = payload.get("silenced_until")
            if not self._is_future_iso(silenced_until):
                continue

            index[alert_id] = {
                "notification_id": item.get("id"),
                "reason": payload.get("reason"),
                "duration_minutes": payload.get("duration_minutes"),
                "silenced_at": payload.get("silenced_at") or item.get("created_at"),
                "silenced_until": silenced_until,
            }
        return index

    @staticmethod
    def _slugify(value: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
        normalized = normalized.strip("_")
        return normalized or "item"

    @staticmethod
    def _password_hash(password: str) -> str:
        iterations = 210000
        salt = secrets.token_bytes(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return (
            f"pbkdf2_sha256${iterations}$"
            f"{base64.b64encode(salt).decode('ascii')}$"
            f"{base64.b64encode(digest).decode('ascii')}"
        )

    @staticmethod
    def _scrub_sensitive_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
        cleaned: Dict[str, Any] = {}
        for key, value in (payload or {}).items():
            key_lower = str(key).lower()
            if key_lower in {"password", "password_hash", "refresh_token", "access_token"}:
                cleaned[key] = "***"
            else:
                cleaned[key] = value
        return cleaned

    @staticmethod
    def _read_setting_items(setting: Optional[Dict[str, Any]]) -> list[Dict[str, Any]]:
        value = (setting or {}).get("value")
        if isinstance(value, dict) and isinstance(value.get("items"), list):
            return [item for item in value["items"] if isinstance(item, dict)]
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        return []

    def _upsert_system_setting(
        self,
        api: Any,
        key: str,
        value: Dict[str, Any],
        updated_by: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return api.upsert_setting(
            SystemSettingDTO(
                key=key,
                value=value,
                metadata=metadata or {},
                updated_by=updated_by,
            )
        )

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _normalize_ymd(value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            return text[:10]
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except Exception:
            return None

    @staticmethod
    def _normalize_period(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        if not text:
            return None
        ymd = UIBffHandler._normalize_ymd(text)
        if ymd:
            return ymd
        if re.fullmatch(r"\d{6}", text):
            return f"{text[0:4]}-{text[4:6]}"
        if re.fullmatch(r"\d{8}", text):
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        if re.fullmatch(r"\d{4}Q[1-4]", text.upper()):
            return text.upper()
        if re.fullmatch(r"\d{4}-Q[1-4]", text.upper()):
            return text.upper()
        return text

    async def _load_readiness_real_data_freshness(self) -> Dict[str, Dict[str, Any]]:
        try:
            api = self._get_system_api()
            if api is None:
                return {}
        except Exception as exc:
            self.logger.warning(f"load readiness real data freshness failed (db unavailable): {exc}")
            return {}

        try:
            raw_snapshot = await asyncio.to_thread(
                api.get_data_readiness_snapshot,
                self.READINESS_REAL_DATA_TABLES,
            )
        except Exception as exc:
            self.logger.warning(f"load readiness data snapshot failed: {exc}")
            return {}

        result: Dict[str, Dict[str, Any]] = {}
        for data_type, payload in (raw_snapshot or {}).items():
            value_kind = str((payload or {}).get("value_kind") or "date").strip().lower()
            raw_latest = (payload or {}).get("latest_date")
            latest_date = self._normalize_period(raw_latest) if value_kind == "period" else self._normalize_ymd(raw_latest)
            result[data_type] = {
                "table": (payload or {}).get("table"),
                "date_column": (payload or {}).get("date_column"),
                "value_kind": value_kind,
                "latest_date": latest_date,
                "rows_on_latest": self._safe_int((payload or {}).get("rows_on_latest"), 0),
            }
        return result

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        token = str(value or "").strip().lower()
        return token in {"1", "true", "yes", "y", "on", "enabled"}

    @classmethod
    def _is_active_periodic_pipeline_task(cls, task: Dict[str, Any]) -> bool:
        if not cls._coerce_bool(task.get("enabled")):
            return False
        schedule_type = str(task.get("schedule_type") or task.get("trigger") or "").strip().lower()
        if schedule_type == "cron":
            return bool(str(task.get("schedule_value") or task.get("cron") or "").strip())
        if schedule_type == "interval":
            return cls._safe_int(task.get("schedule_value") or task.get("interval_seconds"), 0) > 0
        return False

    async def _list_flowhub_tasks(
        self,
        request: web.Request,
        limit: int = 500,
        offset: int = 0,
    ) -> list[Dict[str, Any]]:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.list_schedules(
            service="flowhub",
            limit=max(1, self._safe_int(limit, 500)),
            offset=max(0, self._safe_int(offset, 0)),
        )
        items = payload.get("items", []) if isinstance(payload, dict) else []
        normalized: list[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            trigger = str(item.get("trigger") or "").strip().lower()
            normalized.append(
                {
                    "task_id": item.get("id"),
                    "name": (item.get("metadata") or {}).get("name") if isinstance(item.get("metadata"), dict) else None,
                    "data_type": item.get("job_type"),
                    "schedule_type": trigger,
                    "schedule_value": item.get("cron") if trigger == "cron" else item.get("interval_seconds"),
                    "enabled": bool(item.get("enabled", True)),
                    "allow_overlap": False,
                    "params": item.get("params") if isinstance(item.get("params"), dict) else {},
                    "updated_at": item.get("updated_at"),
                    "created_at": item.get("created_at"),
                    "last_run_at": item.get("last_triggered_at"),
                    "source": "brain_schedule",
                }
            )
        return normalized

    @staticmethod
    def _pipeline_template_to_schedule(template: Dict[str, Any]) -> Dict[str, Any]:
        schedule_type = str(template.get("schedule_type") or "cron").strip().lower()
        schedule_value = template.get("schedule_value")
        payload: Dict[str, Any] = {
            "service": "flowhub",
            "job_type": str(template.get("data_type") or "").strip(),
            "trigger": schedule_type if schedule_type in {"cron", "interval"} else "cron",
            "enabled": bool(template.get("enabled", True)),
            "params": dict(template.get("params") or {}),
            "metadata": {
                "name": template.get("name"),
            },
        }
        if payload["trigger"] == "cron":
            payload["cron"] = str(schedule_value or "").strip() or "30 16 * * 1-5"
        else:
            payload["interval_seconds"] = max(1, int(schedule_value or 60))
        return payload

    def _build_macro_cycle_default_templates(self) -> list[Dict[str, Any]]:
        templates: list[Dict[str, Any]] = []
        macro_cfg = self.DATA_READINESS_REQUIREMENTS.get("macro_cycle") or {}
        data_items = macro_cfg.get("data_types") if isinstance(macro_cfg.get("data_types"), list) else []
        for item in data_items:
            if not isinstance(item, dict):
                continue
            data_type = str(item.get("data_type") or "").strip().lower()
            if not data_type:
                continue
            data_label = str(item.get("label") or data_type).strip() or data_type
            schedule = self.MACRO_CYCLE_TEMPLATE_SCHEDULES.get(data_type, {"schedule_type": "cron", "schedule_value": "0 19 1 * *"})
            templates.append(
                {
                    "name": f"模板·宏观与周期·{data_label}",
                    "data_type": data_type,
                    "schedule_type": schedule.get("schedule_type", "cron"),
                    "schedule_value": schedule.get("schedule_value", "0 19 1 * *"),
                    "enabled": True,
                    "allow_overlap": False,
                    "params": {
                        "data_type": data_type,
                        "incremental": True,
                    },
                }
            )
        return templates

    def _build_structure_rotation_default_templates(self) -> list[Dict[str, Any]]:
        templates: list[Dict[str, Any]] = []
        structure_cfg = self.DATA_READINESS_REQUIREMENTS.get("structure_rotation") or {}
        data_items = structure_cfg.get("data_types") if isinstance(structure_cfg.get("data_types"), list) else []
        for item in data_items:
            if not isinstance(item, dict):
                continue
            data_type = str(item.get("data_type") or "").strip().lower()
            if not data_type:
                continue
            data_label = str(item.get("label") or data_type).strip() or data_type
            schedule = self.STRUCTURE_ROTATION_TEMPLATE_SCHEDULES.get(
                data_type,
                {"schedule_type": "cron", "schedule_value": "0 17 * * 1-5"},
            )
            default_params = self.STRUCTURE_ROTATION_TEMPLATE_PARAMS.get(data_type, {"incremental": True})
            templates.append(
                {
                    "name": self.STRUCTURE_ROTATION_TEMPLATE_NAMES.get(
                        data_type,
                        f"模板·结构与轮动·{data_label}",
                    ),
                    "data_type": data_type,
                    "schedule_type": schedule.get("schedule_type", "cron"),
                    "schedule_value": schedule.get("schedule_value", "0 17 * * 1-5"),
                    "enabled": True,
                    "allow_overlap": False,
                    "params": {
                        "data_type": data_type,
                        **default_params,
                    },
                }
            )
        return templates

    def _build_watchlist_default_templates(self) -> list[Dict[str, Any]]:
        templates: list[Dict[str, Any]] = []
        watchlist_cfg = self.DATA_READINESS_REQUIREMENTS.get("watchlist") or {}
        data_items = watchlist_cfg.get("data_types") if isinstance(watchlist_cfg.get("data_types"), list) else []
        for item in data_items:
            if not isinstance(item, dict):
                continue
            data_type = str(item.get("data_type") or "").strip().lower()
            if not data_type:
                continue
            data_label = str(item.get("label") or data_type).strip() or data_type
            schedule = self.WATCHLIST_TEMPLATE_SCHEDULES.get(
                data_type,
                {"schedule_type": "cron", "schedule_value": "30 17 * * 1-5"},
            )
            default_params = self.WATCHLIST_TEMPLATE_PARAMS.get(data_type, {"incremental": True})
            templates.append(
                {
                    "name": self.WATCHLIST_TEMPLATE_NAMES.get(data_type, f"模板·候选池·{data_label}"),
                    "data_type": data_type,
                    "schedule_type": schedule.get("schedule_type", "cron"),
                    "schedule_value": schedule.get("schedule_value", "30 17 * * 1-5"),
                    "enabled": True,
                    "allow_overlap": False,
                    "params": {
                        "data_type": data_type,
                        **default_params,
                    },
                }
            )
        return templates

    def _build_market_snapshot_default_templates(self) -> list[Dict[str, Any]]:
        templates: list[Dict[str, Any]] = []
        market_cfg = self.DATA_READINESS_REQUIREMENTS.get("market_snapshot") or {}
        data_items = market_cfg.get("data_types") if isinstance(market_cfg.get("data_types"), list) else []
        for item in data_items:
            if not isinstance(item, dict):
                continue
            data_type = str(item.get("data_type") or "").strip().lower()
            if not data_type:
                continue
            data_label = str(item.get("label") or data_type).strip() or data_type
            schedule = self.MARKET_SNAPSHOT_TEMPLATE_SCHEDULES.get(
                data_type,
                {"schedule_type": "cron", "schedule_value": "30 16 * * 1-5"},
            )
            default_params = self.MARKET_SNAPSHOT_TEMPLATE_PARAMS.get(data_type, {"incremental": True})
            templates.append(
                {
                    "name": self.MARKET_SNAPSHOT_TEMPLATE_NAMES.get(
                        data_type,
                        f"模板·市场快照·{data_label}",
                    ),
                    "data_type": data_type,
                    "schedule_type": schedule.get("schedule_type", "cron"),
                    "schedule_value": schedule.get("schedule_value", "30 16 * * 1-5"),
                    "enabled": True,
                    "allow_overlap": False,
                    "params": {
                        "data_type": data_type,
                        **default_params,
                    },
                }
            )
        return templates

    def _build_all_default_pipeline_templates(self) -> list[Dict[str, Any]]:
        return [
            *self._build_market_snapshot_default_templates(),
            *self._build_macro_cycle_default_templates(),
            *self._build_structure_rotation_default_templates(),
            *self._build_watchlist_default_templates(),
        ]

    def _find_default_pipeline_template(self, data_type: str) -> Optional[Dict[str, Any]]:
        token = str(data_type or "").strip().lower()
        if not token:
            return None
        for template in self._build_all_default_pipeline_templates():
            if str(template.get("data_type") or "").strip().lower() == token:
                return template
        return None

    async def _ensure_macro_cycle_default_pipelines(
        self,
        request: web.Request,
        tasks: Optional[list[Dict[str, Any]]] = None,
    ) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
        async with self._macro_template_lock:
            report: Dict[str, Any] = {"created": [], "triggered_runs": [], "errors": []}
            working_tasks = [item for item in (tasks or []) if isinstance(item, dict)]
            if not working_tasks:
                working_tasks = await self._list_flowhub_tasks(request, limit=500, offset=0)

            tasks_by_type: Dict[str, list[Dict[str, Any]]] = {}
            for task in working_tasks:
                token = str(task.get("data_type") or "").strip().lower()
                if not token:
                    continue
                tasks_by_type.setdefault(token, []).append(task)

            templates = self._build_all_default_pipeline_templates()
            for template in templates:
                data_type = str(template.get("data_type") or "").strip().lower()
                if not data_type:
                    continue
                existing_group = tasks_by_type.get(data_type, [])
                if any(self._is_active_periodic_pipeline_task(task) for task in existing_group):
                    continue
                try:
                    orchestrator = self.get_app_component(request, "task_orchestrator")
                    created_task = await orchestrator.create_schedule(self._pipeline_template_to_schedule(template))
                    if not isinstance(created_task, dict):
                        created_task = {"data_type": data_type, "name": template.get("name")}
                    task_id = str(created_task.get("id") or "").strip()
                    report["created"].append(
                        {
                            "data_type": data_type,
                            "task_id": task_id,
                            "name": created_task.get("name") or template.get("name"),
                        }
                    )
                    working_tasks.append(created_task)
                    tasks_by_type.setdefault(data_type, []).append(created_task)
                except Exception as exc:
                    report["errors"].append(
                        {
                            "data_type": data_type,
                            "stage": "create",
                            "message": str(exc),
                        }
                    )

            if report["created"]:
                try:
                    working_tasks = await self._list_flowhub_tasks(request, limit=500, offset=0)
                except Exception as exc:
                    report["errors"].append({"stage": "refresh", "message": str(exc)})
            return working_tasks, report

    @staticmethod
    def _sort_by_time_desc(items: list[Dict[str, Any]], *keys: str) -> list[Dict[str, Any]]:
        def _parse(item: Dict[str, Any]) -> float:
            for key in keys:
                raw = item.get(key)
                if raw is None:
                    continue
                try:
                    return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
                except Exception:
                    continue
            return 0.0

        return sorted(items, key=_parse, reverse=True)

    @staticmethod
    def _parse_any_timestamp(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            raw = float(value)
            if raw <= 0:
                return 0.0
            if raw > 1e12:
                return raw / 1000.0
            if raw > 1e9:
                return raw
            return 0.0
        text = str(value).strip()
        if not text:
            return 0.0
        try:
            raw = float(text)
            if raw > 1e12:
                return raw / 1000.0
            if raw > 1e9:
                return raw
        except Exception:
            pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _get_upstream_raw_error(exc: UpstreamServiceError) -> Dict[str, Any]:
        raw = exc.error.get("raw") if isinstance(exc.error, dict) else {}
        return raw if isinstance(raw, dict) else {}

    @classmethod
    def _resolve_readiness_state_label(cls, state: str) -> str:
        return cls.READINESS_LABELS.get(str(state or "").strip().lower(), "未知")

    @staticmethod
    def _resolve_manual_trigger_policy(state: str) -> tuple[bool, str]:
        normalized_state = str(state or "").strip().lower()
        manual_trigger_allowed = normalized_state not in {"ready", "fetching"}
        if manual_trigger_allowed:
            return True, "当前状态允许手动触发。"
        if normalized_state == "ready":
            return False, "数据已准备好，无需手动触发。"
        return False, "相关任务正在抓取中，请稍后再试。"

    @classmethod
    def _extract_upstream_error_code(cls, exc: UpstreamServiceError) -> str:
        raw = cls._get_upstream_raw_error(exc)
        candidates = [
            raw.get("error_code"),
            raw.get("code"),
        ]
        data = raw.get("data")
        if isinstance(data, dict):
            candidates.extend([data.get("error_code"), data.get("code")])
        for item in candidates:
            if isinstance(item, str) and item.strip():
                return item.strip()
        return ""

    @classmethod
    def _extract_upstream_error_message(cls, exc: UpstreamServiceError) -> str:
        if isinstance(exc.error, dict):
            msg = exc.error.get("message")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
        raw = cls._get_upstream_raw_error(exc)
        msg = raw.get("message") or raw.get("error")
        if isinstance(msg, str) and msg.strip():
            return msg.strip()
        return f"HTTP {exc.status}"

    @staticmethod
    def _latest_job_time(job: Dict[str, Any]) -> float:
        return max(
            UIBffHandler._parse_any_timestamp(job.get("updated_at")),
            UIBffHandler._parse_any_timestamp(job.get("completed_at")),
            UIBffHandler._parse_any_timestamp(job.get("created_at")),
        )

    def _assess_requirement_status(
        self,
        page_key: str,
        page_label: str,
        data_type: str,
        data_label: str,
        jobs_by_type: Dict[str, list[Dict[str, Any]]],
        pipelines_by_type: Dict[str, list[Dict[str, Any]]],
        now_ts: float,
    ) -> Dict[str, Any]:
        def _normalize_job_status(raw: Any) -> str:
            token = str(raw or "").strip().lower()
            if token in {"queued", "pending", "submitted", "accepted", "created", "idle"}:
                return "queued"
            if token in {"running", "in_progress", "processing"}:
                return "running"
            if token in {"succeeded", "success", "completed", "done"}:
                return "succeeded"
            if token in {"failed", "error", "timeout"}:
                return "failed"
            if token in {"cancelled", "canceled"}:
                return "cancelled"
            return token

        def _parse_job_result(job: Dict[str, Any]) -> Dict[str, Any]:
            raw = job.get("result")
            if isinstance(raw, dict):
                return raw
            if isinstance(raw, str):
                text = raw.strip()
                if not text:
                    return {}
                try:
                    parsed = json.loads(text)
                    return parsed if isinstance(parsed, dict) else {}
                except Exception:
                    return {}
            return {}

        def _pick_non_negative_number(payload: Dict[str, Any], keys: tuple[str, ...]) -> Optional[float]:
            for key in keys:
                value = payload.get(key)
                if isinstance(value, (int, float)):
                    number = float(value)
                    if number >= 0:
                        return number
                    continue
                if isinstance(value, str):
                    token = value.strip()
                    if not token:
                        continue
                    try:
                        number = float(token)
                    except Exception:
                        continue
                    if number >= 0:
                        return number
            return None

        def _extract_job_counts(job: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
            payload = _parse_job_result(job)
            saved_count = _pick_non_negative_number(payload, ("saved_count", "saved", "upserted_count", "rows_saved"))
            records_count = _pick_non_negative_number(payload, ("records_count", "count", "rows", "total"))
            return saved_count, records_count

        def _job_has_non_zero_data(job: Dict[str, Any]) -> bool:
            saved_count, records_count = _extract_job_counts(job)
            if saved_count is not None:
                return saved_count > 0
            if records_count is not None:
                return records_count > 0
            return False

        req_jobs = list(jobs_by_type.get(data_type, []))
        req_jobs.sort(key=self._latest_job_time, reverse=True)
        latest_job = req_jobs[0] if req_jobs else {}
        running_jobs = [job for job in req_jobs if _normalize_job_status(job.get("status")) in {"queued", "running"}]
        latest_status = _normalize_job_status(latest_job.get("status"))
        latest_success_job = next((job for job in req_jobs if _normalize_job_status(job.get("status")) == "succeeded"), {})
        latest_success_has_data = _job_has_non_zero_data(latest_success_job) if latest_success_job else False
        history_has_data = any(
            _job_has_non_zero_data(job)
            for job in req_jobs
            if _normalize_job_status(job.get("status")) == "succeeded"
        )
        latest_saved_count, latest_records_count = _extract_job_counts(latest_success_job) if latest_success_job else (None, None)

        req_tasks = list(pipelines_by_type.get(data_type, []))
        enabled_tasks = [task for task in req_tasks if self._coerce_bool(task.get("enabled"))]
        task_pool = enabled_tasks or req_tasks
        task_pool.sort(
            key=lambda item: (
                self._parse_any_timestamp(item.get("updated_at")),
                self._parse_any_timestamp(item.get("next_run_at")),
            ),
            reverse=True,
        )
        selected_task = task_pool[0] if task_pool else {}
        schedule_type = str(selected_task.get("schedule_type") or "")
        schedule_value = selected_task.get("schedule_value")
        next_run_ts = self._parse_any_timestamp(selected_task.get("next_run_at"))
        next_run_at = selected_task.get("next_run_at")
        task_last_status = _normalize_job_status(selected_task.get("last_status"))
        latest_job_id = latest_job.get("id") or selected_task.get("last_job_id")
        latest_job_at = latest_job.get("updated_at") or latest_job.get("completed_at") or latest_job.get("created_at") or selected_task.get("last_run_at")
        running_job_count = len(running_jobs)
        if running_job_count == 0 and task_last_status in {"queued", "running"}:
            running_job_count = 1
        if not latest_status and task_last_status:
            latest_status = task_last_status

        state = "missing_task"
        detail = "未发现对应抓取任务，请在数据与同步中创建并启用任务。"
        if running_jobs or task_last_status in {"queued", "running"}:
            state = "fetching"
            detail = "当前有抓取任务正在执行。"
        elif latest_status == "succeeded":
            if latest_success_job and (not latest_success_has_data) and (not history_has_data):
                state = "no_data"
                detail = "最近一次抓取成功但返回 0 条，且近期历史无有效入库数据。"
            else:
                state = "ready"
                detail = "最近一次抓取成功。"
        elif latest_status == "failed":
            state = "failed"
            detail = "最近一次抓取失败，请查看任务日志与告警。"
        elif selected_task:
            if not self._coerce_bool(selected_task.get("enabled")):
                state = "waiting_schedule"
                detail = "任务已配置但当前处于停用状态。"
            elif schedule_type == "manual":
                state = "waiting_schedule"
                detail = "任务为手动触发，等待用户执行。"
            elif next_run_ts > now_ts:
                state = "waiting_schedule"
                detail = "任务已配置并等待下次定时触发。"
            else:
                state = "waiting_schedule"
                detail = "任务已配置，等待调度器拉起。"

        manual_trigger_allowed, manual_trigger_reason = self._resolve_manual_trigger_policy(state)

        return {
            "kind": "dataset",
            "id": f"{page_key}:{data_type}",
            "page_key": page_key,
            "page_label": page_label,
            "data_type": data_type,
            "label": f"{data_label} ({data_type})",
            "state": state,
            "state_label": self._resolve_readiness_state_label(state),
            "detail": detail,
            "latest_job_id": latest_job_id,
            "latest_job_status": latest_status or "",
            "latest_job_at": latest_job_at,
            "running_job_count": running_job_count,
            "latest_saved_count": latest_saved_count,
            "latest_records_count": latest_records_count,
            "history_has_data": history_has_data,
            "latest_data_date": None,
            "last_fetch_status": latest_status or task_last_status or "",
            "pipeline_task_id": selected_task.get("task_id"),
            "pipeline_enabled": self._coerce_bool(selected_task.get("enabled")) if selected_task else False,
            "pipeline_schedule_type": schedule_type,
            "pipeline_schedule_value": schedule_value,
            "pipeline_next_run_at": next_run_at if next_run_ts > 0 else None,
            "manual_trigger_allowed": manual_trigger_allowed,
            "manual_trigger_reason": manual_trigger_reason,
        }

    async def _build_data_page_checks(
        self,
        request: web.Request,
        dataset_items: list[Dict[str, Any]],
    ) -> list[Dict[str, Any]]:
        checks: list[Dict[str, Any]] = []

        def _apply_related_override(
            state: str,
            detail: str,
            related_items: list[Dict[str, Any]],
            fetching_detail: str,
            failed_detail: str,
            missing_detail: str,
            no_data_detail: str,
        ) -> tuple[str, str]:
            states = [str(item.get("state") or "").strip().lower() for item in related_items]
            if any(item_state == "fetching" for item_state in states):
                return "fetching", fetching_detail
            if any(item_state == "failed" for item_state in states):
                return "failed", failed_detail
            if any(item_state == "missing_task" for item_state in states):
                return "missing_task", missing_detail
            no_data_count = sum(1 for item_state in states if item_state == "no_data")
            has_ready = any(item_state == "ready" for item_state in states)
            if no_data_count > 0 and not has_ready:
                return "no_data", no_data_detail
            return state, detail

        def _append_page_check(
            page_key: str,
            page_label: str,
            state: str,
            detail: str,
            extra: Optional[Dict[str, Any]] = None,
        ) -> None:
            payload = {
                "kind": "page",
                "id": f"page:{page_key}",
                "page_key": page_key,
                "page_label": page_label,
                "state": state,
                "state_label": self._resolve_readiness_state_label(state),
                "detail": detail,
            }
            if isinstance(extra, dict):
                payload.update(extra)
            checks.append(payload)

        market_related = [item for item in dataset_items if item.get("page_key") == "market_snapshot"]
        macro_related = [item for item in dataset_items if item.get("page_key") == "macro_cycle"]
        structure_related = [item for item in dataset_items if item.get("page_key") == "structure_rotation"]
        watchlist_related = [item for item in dataset_items if item.get("page_key") == "watchlist"]

        market_state = "waiting_schedule"
        market_detail = "市场快照接口可达，但尚无可用数据。"
        latest_snapshot_id = None
        try:
            payload = await self._fetch_upstream_json(
                request,
                "macro",
                "/api/v1/ui/market-snapshot/latest",
                params={"benchmark": "CSI300", "scale": "D1"},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, dict) and data.get("id"):
                market_state = "ready"
                latest_snapshot_id = data.get("id")
                market_detail = "市场快照数据已可用于页面展示。"
            else:
                market_state = "waiting_schedule"
                market_detail = "市场快照接口可达，但当前无最新快照。"
        except UpstreamServiceError as exc:
            market_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
            market_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            market_state = "backend_error"
            market_detail = str(exc)
        market_state, market_detail = _apply_related_override(
            market_state,
            market_detail,
            market_related,
            fetching_detail="相关数据正在抓取中，完成后将生成快照。",
            failed_detail="相关数据任务存在失败，需要先修复任务。",
            missing_detail="部分关键数据任务未配置。",
            no_data_detail="相关抓取任务成功执行，但近期未产出有效入库数据。",
        )
        _append_page_check(
            page_key="market_snapshot",
            page_label="市场快照页面",
            state=market_state,
            detail=market_detail,
            extra={"latest_snapshot_id": latest_snapshot_id},
        )

        macro_state = "waiting_schedule"
        macro_detail = "宏观与周期接口可达，但暂无可展示数据。"
        macro_latest_id = None
        try:
            payload = await self._fetch_upstream_json(
                request,
                "macro",
                "/api/v1/ui/macro-cycle/inbox",
                params={"benchmark": "CSI300", "scale": "D1", "limit": 1, "offset": 0},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            items = data.get("items") if isinstance(data, dict) and isinstance(data.get("items"), list) else []
            if items:
                macro_state = "ready"
                macro_latest_id = items[0].get("id")
                macro_detail = "宏观与周期数据已可用于页面展示。"
            else:
                macro_state = "waiting_schedule"
                macro_detail = "宏观与周期接口可达，但暂无可展示数据。"
        except UpstreamServiceError as exc:
            code = self._extract_upstream_error_code(exc).upper()
            if exc.status == 422 and code == "NO_REAL_DATA":
                macro_state = "waiting_schedule"
                macro_detail = "宏观事实数据尚未满足计算覆盖度，等待抓取完成。"
            else:
                macro_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
                macro_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            macro_state = "backend_error"
            macro_detail = str(exc)
        macro_state, macro_detail = _apply_related_override(
            macro_state,
            macro_detail,
            macro_related,
            fetching_detail="宏观相关数据正在抓取中。",
            failed_detail="宏观相关数据任务存在失败，需要先修复任务。",
            missing_detail="部分宏观关键数据任务未配置。",
            no_data_detail="宏观关键抓取任务成功执行，但近期未产出有效入库数据。",
        )
        _append_page_check(
            page_key="macro_cycle",
            page_label="宏观与周期页面",
            state=macro_state,
            detail=macro_detail,
            extra={"latest_snapshot_id": macro_latest_id},
        )

        structure_state = "waiting_schedule"
        structure_detail = "结构与轮动接口可达，但暂无可展示数据。"
        try:
            payload = await self._fetch_upstream_json(
                request,
                "macro",
                "/api/v1/ui/structure-rotation/overview",
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, dict) and data:
                structure_state = "ready"
                structure_detail = "结构与轮动数据已可用于页面展示。"
            else:
                structure_state = "waiting_schedule"
                structure_detail = "结构与轮动等待宏观链路生成结果。"
        except UpstreamServiceError as exc:
            structure_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
            structure_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            structure_state = "backend_error"
            structure_detail = str(exc)
        structure_state, structure_detail = _apply_related_override(
            structure_state,
            structure_detail,
            [*structure_related, *macro_related],
            fetching_detail="结构或宏观链路正在抓取，结构与轮动结果待生成。",
            failed_detail="结构或宏观链路任务失败，结构与轮动暂不可用。",
            missing_detail="结构或宏观关键任务未配置，结构与轮动暂不可用。",
            no_data_detail="结构或宏观关键抓取任务近期无有效入库数据，结构结果可能不完整。",
        )
        _append_page_check(
            page_key="structure_rotation",
            page_label="结构与轮动页面",
            state=structure_state,
            detail=structure_detail,
        )

        watchlist_state = "waiting_schedule"
        watchlist_detail = "候选池接口可达，但暂无候选数据。"
        watchlist_count = 0
        try:
            payload = await self._fetch_upstream_json(
                request,
                "execution",
                "/api/v1/ui/candidates/clusters",
                params={"limit": 1, "offset": 0},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            items = data.get("items") if isinstance(data, dict) and isinstance(data.get("items"), list) else []
            total = data.get("total") if isinstance(data, dict) else None
            watchlist_count = self._safe_int(total, len(items))
            if watchlist_count > 0 or items:
                watchlist_state = "ready"
                watchlist_detail = "候选池数据已可用于页面展示。"
            else:
                watchlist_state = "waiting_schedule"
                watchlist_detail = "候选池暂无数据，请等待分析入池任务完成。"
        except UpstreamServiceError as exc:
            watchlist_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
            watchlist_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            watchlist_state = "backend_error"
            watchlist_detail = str(exc)
        watchlist_state, watchlist_detail = _apply_related_override(
            watchlist_state,
            watchlist_detail,
            watchlist_related,
            fetching_detail="候选池相关数据正在抓取，等待事件化与排序结果刷新。",
            failed_detail="候选池相关抓取任务存在失败，需先修复后再同步入池。",
            missing_detail="候选池关键抓取任务未配置，无法保证入池口径完整。",
            no_data_detail="候选池相关抓取任务无有效新数据，候选事件可能已过期。",
        )
        _append_page_check(
            page_key="watchlist",
            page_label="候选池页面",
            state=watchlist_state,
            detail=watchlist_detail,
            extra={"total_items": watchlist_count},
        )

        stock_state = "waiting_schedule"
        stock_detail = "标的研究接口可达，但暂无研究对象。"
        stock_count = 0
        try:
            payload = await self._fetch_upstream_json(
                request,
                "execution",
                "/api/v1/ui/research/subjects",
                params={"limit": 1, "offset": 0},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            items = data.get("items") if isinstance(data, dict) and isinstance(data.get("items"), list) else []
            total = data.get("total") if isinstance(data, dict) else None
            stock_count = self._safe_int(total, len(items))
            if stock_count > 0 or items:
                stock_state = "ready"
                stock_detail = "标的研究数据已可用于页面展示。"
            else:
                stock_state = "waiting_schedule"
                stock_detail = "标的研究暂无数据，请先形成候选并推进研究流程。"
        except UpstreamServiceError as exc:
            stock_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
            stock_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            stock_state = "backend_error"
            stock_detail = str(exc)
        _append_page_check(
            page_key="stock_research",
            page_label="标的研究页面",
            state=stock_state,
            detail=stock_detail,
            extra={"total_items": stock_count},
        )

        strategy_state = "waiting_schedule"
        strategy_detail = "策略与验证接口可达，但暂无策略报告。"
        strategy_count = 0
        try:
            payload = await self._fetch_upstream_json(
                request,
                "execution",
                "/api/v1/ui/strategy/reports",
                params={"limit": 1, "offset": 0},
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            items = data.get("items") if isinstance(data, dict) and isinstance(data.get("items"), list) else []
            total = data.get("total") if isinstance(data, dict) else None
            strategy_count = self._safe_int(total, len(items))
            if strategy_count > 0 or items:
                strategy_state = "ready"
                strategy_detail = "策略与验证数据已可用于页面展示。"
            else:
                strategy_state = "waiting_schedule"
                strategy_detail = "策略与验证暂无报告，请先执行策略任务。"
        except UpstreamServiceError as exc:
            strategy_state = "backend_error" if exc.status >= 500 else "waiting_schedule"
            strategy_detail = self._extract_upstream_error_message(exc)
        except Exception as exc:
            strategy_state = "backend_error"
            strategy_detail = str(exc)
        _append_page_check(
            page_key="strategy_verify",
            page_label="策略与验证页面",
            state=strategy_state,
            detail=strategy_detail,
            extra={"total_items": strategy_count},
        )

        return checks

    async def _build_data_readiness_payload(self, request: web.Request) -> Dict[str, Any]:
        now_ts = datetime.utcnow().timestamp()
        orchestrator = self.get_app_component(request, "task_orchestrator")
        flowhub_jobs: list[Dict[str, Any]] = []
        flowhub_tasks: list[Dict[str, Any]] = []
        errors: list[Dict[str, Any]] = []

        try:
            jobs_payload = await orchestrator.list_task_jobs(service="flowhub", limit=400, offset=0)
            flowhub_jobs = [item for item in jobs_payload.get("jobs", []) if isinstance(item, dict)]
        except Exception as exc:
            errors.append({"source": "flowhub_jobs", "message": str(exc)})

        try:
            flowhub_tasks = await self._list_flowhub_tasks(request, limit=500, offset=0)
        except Exception as exc:
            errors.append({"source": "flowhub_tasks", "message": str(exc)})
        try:
            flowhub_tasks, template_bootstrap = await self._ensure_macro_cycle_default_pipelines(request, flowhub_tasks)
            for item in template_bootstrap.get("errors", []):
                if isinstance(item, dict):
                    errors.append(
                        {
                            "source": "flowhub_default_templates",
                            "message": f"{item.get('stage') or 'unknown'}::{item.get('data_type') or '-'}::{item.get('message') or '-'}",
                        }
                    )
        except Exception as exc:
            errors.append({"source": "flowhub_default_templates", "message": str(exc)})

        jobs_by_type: Dict[str, list[Dict[str, Any]]] = {}
        for job in flowhub_jobs:
            token = str(job.get("job_type") or "").strip().lower()
            if not token:
                continue
            jobs_by_type.setdefault(token, []).append(job)

        tasks_by_type: Dict[str, list[Dict[str, Any]]] = {}
        for task in flowhub_tasks:
            token = str(task.get("data_type") or "").strip().lower()
            if not token:
                continue
            tasks_by_type.setdefault(token, []).append(task)

        readiness_freshness = await self._load_readiness_real_data_freshness()

        items: list[Dict[str, Any]] = []
        for page_key, page_config in self.DATA_READINESS_REQUIREMENTS.items():
            page_label = str(page_config.get("label") or page_key)
            data_types = page_config.get("data_types") if isinstance(page_config.get("data_types"), list) else []
            for item in data_types:
                if not isinstance(item, dict):
                    continue
                data_type = str(item.get("data_type") or "").strip().lower()
                if not data_type:
                    continue
                data_label = str(item.get("label") or data_type)
                assessed = self._assess_requirement_status(
                    page_key=page_key,
                    page_label=page_label,
                    data_type=data_type,
                    data_label=data_label,
                    jobs_by_type=jobs_by_type,
                    pipelines_by_type=tasks_by_type,
                    now_ts=now_ts,
                )
                freshness = readiness_freshness.get(data_type, {})
                latest_data_date = self._normalize_period(freshness.get("latest_date"))
                rows_on_latest = self._safe_int(freshness.get("rows_on_latest"), 0)
                if latest_data_date:
                    assessed["latest_data_date"] = latest_data_date
                    assessed["latest_data_rows"] = rows_on_latest
                    assessed["history_has_data"] = True
                    if str(assessed.get("state") or "") == "no_data":
                        assessed["state"] = "ready"
                        assessed["state_label"] = self._resolve_readiness_state_label("ready")
                        assessed["detail"] = (
                            f"已检测到真实入库数据（{freshness.get('table')}），最新日期 {latest_data_date}。"
                        )
                        manual_allowed, manual_reason = self._resolve_manual_trigger_policy("ready")
                        assessed["manual_trigger_allowed"] = manual_allowed
                        assessed["manual_trigger_reason"] = manual_reason
                    if not str(assessed.get("last_fetch_status") or "").strip():
                        assessed["last_fetch_status"] = "has_data"
                items.append(assessed)

        page_checks = await self._build_data_page_checks(request, items)
        all_states = [str(item.get("state") or "unknown") for item in [*items, *page_checks]]
        summary: Dict[str, int] = {}
        for state in all_states:
            summary[state] = summary.get(state, 0) + 1

        return {
            "updated_at": self._now_iso(),
            "summary": summary,
            "items": items,
            "page_checks": page_checks,
            "errors": errors,
        }

    async def _fetch_upstream_json(
        self,
        request: web.Request,
        service_name: str,
        path: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        registry = self.get_app_component(request, "service_registry")
        service = getattr(registry, "_services", {}).get(service_name)
        session = getattr(registry, "_session", None)
        if not service or session is None:
            raise RuntimeError(f"Service not available: {service_name}")

        target_url = f"{service['url'].rstrip('/')}{path}"
        async with session.request(method.upper(), target_url, params=params, json=payload) as resp:
            raw = await resp.text()
            body: Dict[str, Any]
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"raw": raw}
            if resp.status >= 400:
                raise UpstreamServiceError(
                    service=service_name,
                    method=method.upper(),
                    path=path,
                    status=resp.status,
                    error={"message": body.get("error") or body.get("message") or "Upstream error", "raw": body},
                )
            if not isinstance(body, dict):
                return {"raw": body}
            return body

    async def _handle_shell_context(self, request: web.Request) -> web.Response:
        page = (request.query.get("page") or "overview").strip().lower()
        context = dict(self.SHELL_CONTEXT_PAGES.get(page, self.SHELL_CONTEXT_PAGES["overview"]))
        orchestrator = self.get_app_component(request, "task_orchestrator")
        task_data = await orchestrator.list_task_jobs(limit=20, offset=0)
        jobs = task_data.get("jobs", [])
        running_count = sum(1 for item in jobs if item.get("status") in ("queued", "running"))
        failed_count = sum(1 for item in jobs if item.get("status") == "failed")
        cancellable_count = sum(1 for item in jobs if bool(item.get("cancellable")))

        user_info: Dict[str, Any] = {
            "id": "user_admin",
            "username": "admin",
            "display_name": "Admin",
            "roles": [{"id": "role_admin", "name": "admin"}],
        }
        notifications = []
        try:
            system_api = self._get_system_api()
            user_info = system_api.get_user("user_admin") or user_info
            notifications = system_api.list_notifications(limit=5)
        except Exception as exc:
            self.logger.warning(f"system api unavailable for shell context: {exc}")

        payload = {
            "page": page,
            "phase": context["phase"],
            "page_title": context["page_title"],
            "service": context["service"],
            "topbar_chips": [
                {"key": "running_jobs", "label": "运行中任务", "value": running_count},
                {"key": "failed_jobs", "label": "失败任务", "value": failed_count},
                {"key": "cancellable_jobs", "label": "可取消", "value": cancellable_count},
            ],
            "toolbar_actions": [
                {"id": "refresh", "label": "刷新", "enabled": True},
                {"id": "search", "label": "搜索", "enabled": True},
                {"id": "open_tasks", "label": "任务中心", "enabled": True},
            ],
            "sidebar_badges": {
                "jobs": running_count,
                "alerts": failed_count,
                "notifications": len(notifications),
            },
            "current_user": user_info,
            "notifications": notifications,
        }
        return self.success_response(payload)

    @staticmethod
    def _contains_query(q: str, *values: Any) -> bool:
        query = q.lower()
        for value in values:
            if value is None:
                continue
            if query in str(value).lower():
                return True
        return False

    async def _handle_search(self, request: web.Request) -> web.Response:
        q = (request.query.get("q") or "").strip()
        scope = (request.query.get("scope") or "all").strip().lower()
        limit = int(request.query.get("limit", 20))
        if not q:
            return self.success_response({"items": [], "total": 0, "scope": scope, "query": q})

        items = []
        if scope in ("all", "research", "subjects"):
            try:
                payload = await self._fetch_upstream_json(request, "execution", "/api/v1/ui/research/subjects", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("symbol"), row.get("title"), row.get("status")):
                        items.append({"type": "research_subject", "id": row.get("id"), "title": row.get("title"), "service": "execution", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search subjects failed: {exc}")

        if scope in ("all", "candidates", "events"):
            try:
                payload = await self._fetch_upstream_json(request, "execution", "/api/v1/ui/candidates/events", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("symbol"), row.get("signal_type"), row.get("status")):
                        items.append({"type": "candidate_event", "id": row.get("id"), "title": row.get("symbol"), "service": "execution", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search events failed: {exc}")

        if scope in ("all", "macro"):
            try:
                payload = await self._fetch_upstream_json(request, "macro", "/api/v1/ui/macro-cycle/history", params={"limit": 200, "offset": 0})
                for row in self._unwrap_response_data(payload).get("items", []):
                    if self._contains_query(q, row.get("id"), row.get("cycle_phase"), row.get("status")):
                        items.append({"type": "macro_cycle", "id": row.get("id"), "title": row.get("cycle_phase"), "service": "macro", "payload": row})
            except Exception as exc:
                self.logger.warning(f"search macro history failed: {exc}")

        items = items[: max(limit, 1)]
        return self.success_response({"items": items, "total": len(items), "scope": scope, "query": q})

    async def _handle_candidate_analyzers(self, request: web.Request) -> web.Response:
        try:
            payload = await self._fetch_upstream_json(request, "execution", "/api/v1/analyze/capabilities")
            data = self._unwrap_response_data(payload)
            raw_items = data.get("analyzers") if isinstance(data.get("analyzers"), list) else []
            items: list[Dict[str, Any]] = []
            for row in raw_items:
                if not isinstance(row, dict):
                    continue
                analyzer_id = str(row.get("name") or "").strip()
                if not analyzer_id:
                    continue
                items.append(
                    {
                        "id": analyzer_id,
                        "name": analyzer_id,
                        "display_name": str(row.get("display_name") or analyzer_id).strip() or analyzer_id,
                        "description": str(row.get("description") or "").strip(),
                        "version": str(row.get("version") or "").strip(),
                        "supported_signals": (
                            [str(item).strip() for item in row.get("supported_signals", []) if str(item).strip()]
                            if isinstance(row.get("supported_signals"), list)
                            else []
                        ),
                        "config": row.get("config") if isinstance(row.get("config"), dict) else {},
                    }
                )
            items.sort(key=lambda item: item["name"])
            return self.success_response(
                {
                    "items": items,
                    "total": len(items),
                    "default_selected": [item["name"] for item in items],
                }
            )
        except UpstreamServiceError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": self._extract_upstream_error_message(exc),
                    "error_code": "UPSTREAM_REQUEST_FAILED",
                    "upstream_status": exc.status,
                    "upstream_service": exc.service,
                    "details": exc.error.get("raw"),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status if 400 <= exc.status < 500 else 502,
            )
        except Exception as exc:
            self.logger.error(f"load candidate analyzers failed: {exc}")
            return self.error_response("Failed to load candidate analyzers", 500)

    async def _handle_system_jobs_overview(self, request: web.Request) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        service = request.query.get("service")
        status = request.query.get("status")
        limit = max(1, min(100, int(request.query.get("limit", 50))))
        offset = max(0, int(request.query.get("offset", 0)))
        payload = await orchestrator.list_task_jobs(service=service, status=status, limit=limit, offset=offset)
        jobs = payload.get("jobs", [])
        summary = {
            "queued": sum(1 for item in jobs if item.get("status") == "queued"),
            "running": sum(1 for item in jobs if item.get("status") == "running"),
            "succeeded": sum(1 for item in jobs if item.get("status") == "succeeded"),
            "failed": sum(1 for item in jobs if item.get("status") == "failed"),
            "cancelled": sum(1 for item in jobs if item.get("status") == "cancelled"),
        }
        return self.success_response({**payload, "summary": summary})

    async def _handle_system_alerts_list(self, request: web.Request) -> web.Response:
        system_monitor = self.get_app_component(request, "system_monitor")
        alerts = system_monitor.get_active_alerts()
        silence_index = self._load_active_silence_index()

        merged_alerts: list[Dict[str, Any]] = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue
            item = dict(alert)
            alert_id = str(item.get("alert_id") or "").strip()
            metadata = item.get("metadata")
            metadata_map = dict(metadata) if isinstance(metadata, dict) else {}
            if alert_id and alert_id in silence_index:
                silence_data = silence_index[alert_id]
                metadata_map["silenced"] = True
                metadata_map["silence_reason"] = silence_data.get("reason")
                metadata_map["silenced_at"] = silence_data.get("silenced_at")
                metadata_map["silenced_until"] = silence_data.get("silenced_until")
                metadata_map["silence_notification_id"] = silence_data.get("notification_id")
            item["metadata"] = metadata_map
            merged_alerts.append(item)

        return self.success_response({"items": merged_alerts, "total": len(merged_alerts)})

    async def _handle_system_alert_ack_all(self, request: web.Request) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)

        alert_ids = []
        if isinstance(body.get("alert_ids"), list):
            alert_ids = [str(item) for item in body["alert_ids"] if item]
        elif body.get("alert_id"):
            alert_ids = [str(body.get("alert_id"))]

        system_monitor = self.get_app_component(request, "system_monitor")
        acknowledged = []
        if not alert_ids:
            return self.success_response({"acknowledged": "all", "items": []}, "Alerts acknowledged")

        for alert_id in alert_ids:
            result = system_monitor.acknowledge_alert(alert_id, str(body.get("notes", "")))
            acknowledged.append(result)

        return self.success_response({"acknowledged": len(acknowledged), "items": acknowledged}, "Alerts acknowledged")

    async def _handle_system_alert_ack(self, request: web.Request, alert_id: str) -> web.Response:
        system_monitor = self.get_app_component(request, "system_monitor")
        result = system_monitor.acknowledge_alert(alert_id, "")
        return self.success_response(result, "Alert acknowledged")

    async def _handle_system_alert_silence(self, request: web.Request, alert_id: str) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)

        reason = str(body.get("reason") or "manual_silence")
        duration_raw = body.get("duration_minutes")
        duration_minutes: Optional[int] = None
        if duration_raw not in (None, ""):
            try:
                duration_minutes = int(duration_raw)
            except Exception:
                return self.error_response("duration_minutes must be an integer", 400)
            if duration_minutes <= 0:
                return self.error_response("duration_minutes must be > 0", 400)

        silenced_until = None
        if duration_minutes:
            silenced_until = (datetime.utcnow() + timedelta(minutes=duration_minutes)).isoformat() + "Z"

        system_monitor = self.get_app_component(request, "system_monitor")
        try:
            silenced_alert = system_monitor.silence_alert(alert_id, reason=reason, duration_minutes=duration_minutes)
        except Exception as exc:
            self.logger.warning(f"silence alert in monitor failed: alert_id={alert_id}, error={exc}")
            silenced_alert = {
                "alert_id": alert_id,
                "metadata": {
                    "silenced": True,
                    "silence_reason": reason,
                    "silenced_until": silenced_until,
                },
            }

        notification_id = None
        try:
            system_api = self._get_system_api()
            notification = system_api.create_notification(
                title=f"Alert silenced: {alert_id}",
                message=f"Alert {alert_id} silenced",
                level="warning",
                user_id=str(body.get("actor_id") or "user_admin"),
                payload={
                    "type": "alert_silence",
                    "alert_id": alert_id,
                    "reason": reason,
                    "duration_minutes": duration_minutes,
                    "silenced_at": self._now_iso(),
                    "silenced_until": silenced_until,
                },
            )
            notification_id = notification.get("id") if isinstance(notification, dict) else None
            if notification_id:
                system_api.update_notification_status(str(notification_id), "silenced")
            if SystemAuditDTO is not None:
                system_api.append_audit_log(
                    SystemAuditDTO(
                        id=str(uuid.uuid4()),
                        actor_id=str(body.get("actor_id") or "user_admin"),
                        action="alert.silence",
                        target_type="monitoring_alert",
                        target_id=alert_id,
                        payload={
                            "reason": reason,
                            "duration_minutes": duration_minutes,
                            "silenced_until": silenced_until,
                            "notification_id": notification_id,
                        },
                        created_at=self._now_iso(),
                    )
                )
        except Exception as exc:
            self.logger.warning(f"persist alert silence failed: alert_id={alert_id}, error={exc}")

        payload = dict(silenced_alert) if isinstance(silenced_alert, dict) else {"alert_id": alert_id}
        metadata = payload.get("metadata")
        metadata_map = dict(metadata) if isinstance(metadata, dict) else {}
        metadata_map["silenced"] = True
        metadata_map["silence_reason"] = reason
        metadata_map["silenced_until"] = silenced_until
        if notification_id:
            metadata_map["silence_notification_id"] = notification_id
        payload["metadata"] = metadata_map
        payload["silenced"] = True
        payload["reason"] = reason
        payload["duration_minutes"] = duration_minutes
        payload["silenced_until"] = silenced_until
        payload["notification_id"] = notification_id
        return self.success_response(payload, "Alert silenced")

    async def _handle_system_rules_list(self, request: web.Request) -> web.Response:
        system_monitor = self.get_app_component(request, "system_monitor")
        rules = system_monitor.get_alert_rules()
        return self.success_response({"items": rules, "total": len(rules)})

    async def _handle_system_rules_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        required = ("name", "condition", "threshold")
        for field in required:
            if field not in body or body.get(field) in (None, ""):
                return self.error_response(f"Missing required field: {field}", 400)
        system_monitor = self.get_app_component(request, "system_monitor")
        rule = system_monitor.set_alert_rule(body)
        return self.success_response(rule, "Rule created")

    async def _handle_system_health(self, request: web.Request) -> web.Response:
        system_monitor = self.get_app_component(request, "system_monitor")
        health = await system_monitor.check_system_health()
        data_readiness = await self._build_data_readiness_payload(request)
        payload = {
            "overall_health": health.overall_health.value,
            "macro_system_status": health.macro_system_status.value,
            "portfolio_system_status": health.portfolio_system_status.value,
            "strategy_system_status": health.strategy_system_status.value,
            "tactical_system_status": health.tactical_system_status.value,
            "data_pipeline_status": health.data_pipeline_status.value,
            "last_update_time": health.last_update_time.isoformat(),
            "active_sessions": health.active_sessions,
            "performance_metrics": health.performance_metrics,
            "error_count": health.error_count,
            "warning_count": health.warning_count,
            "metadata": health.metadata,
            "data_readiness": data_readiness,
        }
        return self.success_response(payload)

    async def _handle_system_cancel(self, request: web.Request, task_job_id: Optional[str] = None) -> web.Response:
        task_job_id = task_job_id or request.match_info.get("task_job_id")
        if not task_job_id:
            return self.error_response("Missing task_job_id", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.cancel_task_job(task_job_id)
        return self.success_response(payload, "Task cancellation requested")

    async def _handle_system_history(self, request: web.Request, task_job_id: Optional[str] = None) -> web.Response:
        task_job_id = task_job_id or request.match_info.get("task_job_id")
        if not task_job_id:
            return self.error_response("Missing task_job_id", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.get_task_job_history(task_job_id)
        return self.success_response(payload)

    @staticmethod
    def _extract_any_job_id(payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        for key in ("job_id", "task_job_id", "id"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("job_id", "task_job_id", "id"):
                value = data.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    async def _run_flowhub_pipeline_task(self, request: web.Request, pipeline_task_id: str) -> Dict[str, Any]:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.trigger_schedule(pipeline_task_id)
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _build_fallback_pipeline_template(data_type: str) -> Dict[str, Any]:
        token = str(data_type or "").strip().lower()
        return {
            "name": f"模板·自动创建·{token}",
            "data_type": token,
            "schedule_type": "cron",
            "schedule_value": "30 16 * * 1-5",
            "enabled": True,
            "allow_overlap": False,
            "params": {
                "data_type": token,
                "incremental": True,
            },
        }

    async def _create_and_run_flowhub_pipeline(
        self,
        request: web.Request,
        data_type: str,
    ) -> tuple[str, Dict[str, Any], Dict[str, Any]]:
        template = self._find_default_pipeline_template(data_type) or self._build_fallback_pipeline_template(data_type)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        created_task = await orchestrator.create_schedule(self._pipeline_template_to_schedule(template))
        pipeline_task_id = str(created_task.get("id") or "").strip() if isinstance(created_task, dict) else ""
        if not pipeline_task_id:
            raise RuntimeError("Failed to create flowhub pipeline: missing task_id")
        run_payload = await self._run_flowhub_pipeline_task(request, pipeline_task_id)
        return pipeline_task_id, run_payload, template

    async def _handle_system_readiness_item_trigger(self, request: web.Request, item_id: str) -> web.Response:
        normalized_item_id = str(item_id or "").strip()
        if not normalized_item_id:
            return self.error_response(
                "Missing readiness item id",
                400,
                error_code="READINESS_ITEM_ID_REQUIRED",
            )

        try:
            readiness_payload = await self._build_data_readiness_payload(request)
            items = readiness_payload.get("items", []) if isinstance(readiness_payload, dict) else []
            target_item = next(
                (
                    item
                    for item in items
                    if isinstance(item, dict) and str(item.get("id") or "").strip() == normalized_item_id
                ),
                None,
            )
            if not isinstance(target_item, dict):
                return self.error_response(
                    f"Readiness item not found: {normalized_item_id}",
                    404,
                    error_code="READINESS_ITEM_NOT_FOUND",
                )

            item_kind = str(target_item.get("kind") or "").strip().lower()
            if item_kind != "dataset":
                return self.error_response(
                    f"Readiness item '{normalized_item_id}' is not a dataset row",
                    409,
                    error_code="READINESS_TRIGGER_NOT_ALLOWED",
                )

            state_before = str(target_item.get("state") or "").strip().lower()
            if state_before in {"ready", "fetching"}:
                return self.error_response(
                    f"Readiness item state '{state_before}' does not allow manual trigger",
                    409,
                    error_code="READINESS_TRIGGER_NOT_ALLOWED",
                )

            data_type = str(target_item.get("data_type") or "").strip().lower()
            if not data_type:
                return self.error_response(
                    f"Readiness item has no data_type: {normalized_item_id}",
                    500,
                    error_code="READINESS_TRIGGER_INTERNAL_ERROR",
                )

            pipeline_task_id = str(target_item.get("pipeline_task_id") or "").strip()
            trigger_mode = "pipeline_run"
            run_payload: Dict[str, Any]
            created_template: Optional[Dict[str, Any]] = None

            if pipeline_task_id:
                run_payload = await self._run_flowhub_pipeline_task(request, pipeline_task_id)
            else:
                trigger_mode = "auto_created_then_run"
                pipeline_task_id, run_payload, created_template = await self._create_and_run_flowhub_pipeline(
                    request,
                    data_type,
                )

            run_job_id = self._extract_any_job_id(run_payload)
            response_payload: Dict[str, Any] = {
                "item_id": normalized_item_id,
                "state_before": state_before,
                "trigger_mode": trigger_mode,
                "pipeline_task_id": pipeline_task_id or None,
                "run_job_id": run_job_id or None,
                "message": "Readiness item trigger requested",
            }
            if created_template:
                response_payload["created_template_name"] = created_template.get("name")
            return self.success_response(response_payload, "Readiness item trigger requested")
        except UpstreamServiceError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": self._extract_upstream_error_message(exc),
                    "error_code": "FLOWHUB_REQUEST_FAILED",
                    "upstream_status": exc.status,
                    "upstream_service": exc.service,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=502,
            )
        except Exception as exc:
            self.logger.error(f"readiness item trigger failed: item_id={normalized_item_id}, error={exc}")
            return self.error_response(
                "Readiness trigger failed",
                500,
                error_code="READINESS_TRIGGER_INTERNAL_ERROR",
            )

    async def _handle_system_data_overview(self, request: web.Request) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        flowhub_jobs = await orchestrator.list_task_jobs(service="flowhub", limit=100, offset=0)
        jobs = flowhub_jobs.get("jobs", [])
        backfill_jobs = [
            item for item in jobs
            if str(item.get("job_type", "")).startswith("backfill_")
        ]
        notifications = []
        try:
            notifications = self._get_system_api().list_notifications(limit=20)
        except Exception as exc:
            self.logger.warning(f"list notifications for data overview failed: {exc}")
        sources: list[dict[str, Any]] = []
        pipelines: list[dict[str, Any]] = []
        recent_logs: list[dict[str, Any]] = []
        template_bootstrap: Dict[str, Any] = {"created": [], "triggered_runs": [], "errors": []}
        try:
            payload = await self._fetch_upstream_json(request, "flowhub", "/api/v1/sources")
            sources = self._unwrap_response_data(payload).get("sources", [])
        except Exception as exc:
            self.logger.warning(f"list sources for data overview failed: {exc}")
        try:
            pipelines, template_bootstrap = await self._ensure_macro_cycle_default_pipelines(request)
        except Exception as exc:
            self.logger.warning(f"list pipelines for data overview failed: {exc}")
        try:
            recent_logs = []
            for job in sorted(
                [item for item in jobs if isinstance(item, dict)],
                key=lambda x: self._to_datetime(x.get("updated_at") or x.get("created_at")) or datetime.min,
                reverse=True,
            )[:50]:
                status = str(job.get("status") or "queued").lower()
                level = "error" if status == "failed" else "info"
                metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
                recent_logs.append(
                    {
                        "id": job.get("id"),
                        "job_id": job.get("id"),
                        "timestamp": job.get("updated_at") or job.get("created_at"),
                        "task_name": metadata.get("schedule_id") or job.get("job_type"),
                        "level": level,
                        "message": str(job.get("message") or status),
                        "status": status,
                    }
                )
        except Exception as exc:
            self.logger.warning(f"build recent logs for data overview failed: {exc}")

        source_total = len(sources)
        source_online = sum(1 for item in sources if str(item.get("status", "")).lower() == "online")
        pipeline_total = len(pipelines)
        pipeline_enabled = sum(1 for item in pipelines if self._coerce_bool(item.get("enabled")))
        pipeline_running = sum(1 for item in pipelines if str(item.get("status", "")).lower() in {"running", "queued"})
        failed_logs = sum(1 for item in recent_logs if str(item.get("level", "")).lower() == "error")

        payload = {
            "flowhub_jobs": jobs,
            "backfill_jobs": backfill_jobs,
            "flowhub_summary": {
                "total": len(jobs),
                "running": sum(1 for item in jobs if item.get("status") in ("queued", "running")),
                "failed": sum(1 for item in jobs if item.get("status") == "failed"),
                "succeeded": sum(1 for item in jobs if item.get("status") == "succeeded"),
            },
            "sources_summary": {
                "total": source_total,
                "online": source_online,
                "offline": max(source_total - source_online, 0),
            },
            "pipelines_summary": {
                "total": pipeline_total,
                "enabled": pipeline_enabled,
                "running": pipeline_running,
                "failed_logs": failed_logs,
            },
            "sources": sources,
            "pipelines": pipelines,
            "recent_logs": recent_logs,
            "notifications": notifications,
            "template_bootstrap": template_bootstrap,
        }
        return self.success_response(payload)

    async def _handle_system_data_sources_list(self, request: web.Request) -> web.Response:
        payload = await self._fetch_upstream_json(request, "flowhub", "/api/v1/sources")
        data = self._unwrap_response_data(payload)

        custom_sources: list[dict[str, Any]] = []
        try:
            setting = self._get_system_api().get_setting("system.data.custom_sources")
            value = (setting or {}).get("value")
            if isinstance(value, dict) and isinstance(value.get("items"), list):
                custom_sources = [item for item in value["items"] if isinstance(item, dict)]
        except Exception as exc:
            self.logger.warning(f"load custom sources failed: {exc}")

        sources = data.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        merged = {str(item.get("id")): item for item in sources if isinstance(item, dict)}
        for item in custom_sources:
            key = str(item.get("id") or "")
            if not key:
                continue
            merged[key] = item

        return self.success_response({"sources": list(merged.values()), "total": len(merged)})

    async def _handle_system_data_sources_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)

        source_id = str(body.get("id") or "").strip()
        name = str(body.get("name") or "").strip()
        source_type = str(body.get("type") or "").strip()
        endpoint = str(body.get("endpoint") or "").strip()
        if not source_id or not name:
            return self.error_response("Missing required field: id/name", 400)

        api = self._get_system_api()
        setting = api.get_setting("system.data.custom_sources")
        value = (setting or {}).get("value")
        items = value.get("items") if isinstance(value, dict) else []
        if not isinstance(items, list):
            items = []

        item_payload: Dict[str, Any] = {
            "id": source_id,
            "name": name,
            "type": source_type or "custom",
            "status": str(body.get("status") or "online"),
            "endpoint": endpoint,
            "owner": body.get("owner"),
            "description": body.get("description"),
            "metadata": body.get("metadata") if isinstance(body.get("metadata"), dict) else {},
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
        replaced = False
        next_items: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("id")) == source_id:
                next_items.append(item_payload)
                replaced = True
            else:
                next_items.append(item)
        if not replaced:
            next_items.append(item_payload)

        api.upsert_setting(
            SystemSettingDTO(
                key="system.data.custom_sources",
                value={"items": next_items},
                metadata={"source": "system_data_sources"},
                updated_by="user_admin",
            )
        )
        return self.success_response(item_payload, "Source saved")

    async def _handle_system_data_source_test(self, request: web.Request, source_id: str) -> web.Response:
        try:
            payload = await self._fetch_upstream_json(
                request,
                "flowhub",
                f"/api/v1/sources/{source_id}/test",
                method="POST",
                payload={},
            )
            return self.success_response(self._unwrap_response_data(payload), "Source test completed")
        except UpstreamServiceError as exc:
            if exc.status != 404:
                raise
        except Exception as exc:
            self.logger.warning(f"flowhub source test failed: source={source_id}, error={exc}")

        return self.success_response(
            {
                "id": source_id,
                "status": "unsupported",
                "latency": 0,
                "message": "No runtime tester registered for this source",
            },
            "Source test completed",
        )

    async def _handle_system_data_pipelines_list(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 200))
        offset = int(request.query.get("offset", 0))
        safe_limit = max(1, self._safe_int(limit, 200))
        safe_offset = max(0, self._safe_int(offset, 0))
        template_bootstrap: Dict[str, Any] = {"created": [], "triggered_runs": [], "errors": []}
        try:
            _, template_bootstrap = await self._ensure_macro_cycle_default_pipelines(request)
        except Exception as exc:
            self.logger.warning(f"ensure default pipelines failed: {exc}")
        all_tasks = await self._list_flowhub_tasks(request, limit=2000, offset=0)
        page_items = all_tasks[safe_offset : safe_offset + safe_limit]
        return self.success_response(
            {
                "items": page_items,
                "total": len(all_tasks),
                "limit": safe_limit,
                "offset": safe_offset,
                "template_bootstrap": template_bootstrap,
            }
        )

    async def _handle_system_data_pipeline_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        payload = self._pipeline_template_to_schedule(body)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        created = await orchestrator.create_schedule(payload)
        return self.success_response(created, "Pipeline created")

    async def _handle_system_data_pipeline_update(self, request: web.Request, task_id: str) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        patch_payload: Dict[str, Any] = {}
        if "enabled" in body:
            patch_payload["enabled"] = bool(body.get("enabled"))
        if "params" in body:
            patch_payload["params"] = body.get("params") if isinstance(body.get("params"), dict) else {}
        if "schedule_value" in body:
            value = body.get("schedule_value")
            if isinstance(value, (int, float)) or (isinstance(value, str) and str(value).isdigit()):
                patch_payload["interval_seconds"] = int(value)
            else:
                patch_payload["cron"] = str(value or "")
        orchestrator = self.get_app_component(request, "task_orchestrator")
        updated = await orchestrator.patch_schedule(task_id, patch_payload)
        return self.success_response(updated, "Pipeline updated")

    async def _handle_system_data_pipeline_run(self, request: web.Request, task_id: str) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.trigger_schedule(task_id)
        return self.success_response(payload, "Pipeline run requested")

    async def _handle_system_data_pipeline_control(self, request: web.Request, task_id: str, action: str) -> web.Response:
        mapped = "disable" if action == "pause" else "enable" if action == "resume" else action
        if mapped not in {"enable", "disable"}:
            return self.error_response("Unsupported pipeline action", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.patch_schedule(task_id, {"enabled": mapped == "enable"})
        return self.success_response(payload, "Pipeline updated")

    async def _handle_system_data_pipeline_history(self, request: web.Request, task_id: str) -> web.Response:
        limit = int(request.query.get("limit", 50))
        offset = int(request.query.get("offset", 0))
        orchestrator = self.get_app_component(request, "task_orchestrator")
        listing = await orchestrator.list_task_jobs(service="flowhub", limit=500, offset=0)
        jobs = listing.get("jobs", []) if isinstance(listing, dict) else []
        matched = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
            if str(metadata.get("schedule_id") or "") == str(task_id):
                matched.append(job)
        matched = self._sort_by_time_desc(matched, "updated_at", "created_at")
        total = len(matched)
        return self.success_response({"items": matched[offset: offset + limit], "total": total, "limit": limit, "offset": offset})

    async def _handle_system_data_logs(self, request: web.Request) -> web.Response:
        limit = min(100, max(1, self._safe_int(request.query.get("limit"), 100)))
        offset = max(0, self._safe_int(request.query.get("offset"), 0))
        orchestrator = self.get_app_component(request, "task_orchestrator")
        listing = await orchestrator.list_task_jobs(service="flowhub", limit=500, offset=0)
        jobs = [item for item in (listing.get("jobs") or []) if isinstance(item, dict)]
        jobs = self._sort_by_time_desc(jobs, "updated_at", "created_at")

        logs: list[Dict[str, Any]] = []
        for job in jobs:
            status = str(job.get("status") or "queued").lower()
            metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
            logs.append(
                {
                    "id": job.get("id"),
                    "job_id": job.get("id"),
                    "timestamp": job.get("updated_at") or job.get("created_at"),
                    "task_name": metadata.get("schedule_id") or job.get("job_type"),
                    "level": "error" if status == "failed" else "info",
                    "message": str(job.get("message") or status),
                    "status": status,
                }
            )

        total = len(logs)
        return self.success_response({"logs": logs[offset: offset + limit], "total": total, "limit": limit, "offset": offset})

    async def _handle_system_data_backfill_run(self, request: web.Request, run_id: str) -> web.Response:
        payload = await self._fetch_upstream_json(
            request,
            "flowhub",
            f"/api/v1/jobs/backfill/{run_id}",
        )
        return self.success_response(self._unwrap_response_data(payload))

    async def _handle_system_data_quality(self, request: web.Request) -> web.Response:
        system_monitor = self.get_app_component(request, "system_monitor")
        alerts = system_monitor.get_active_alerts()
        rules = system_monitor.get_alert_rules()
        health = await system_monitor.check_system_health()

        items = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue
            level = str(alert.get("alert_level") or "warn")
            items.append(
                {
                    "id": alert.get("alert_id"),
                    "object": alert.get("component") or alert.get("alert_type") or "system",
                    "dimension": alert.get("alert_type") or "health",
                    "current": alert.get("message"),
                    "threshold": alert.get("threshold"),
                    "last_check": alert.get("timestamp"),
                    "note": alert.get("resolution_notes") or "",
                    "severity": level.lower(),
                    "status": "resolved" if bool(alert.get("is_resolved")) else "open",
                }
            )

        payload = {
            "items": items,
            "rules": rules,
            "health": {
                "overall_health": health.overall_health.value,
                "data_pipeline_status": health.data_pipeline_status.value,
                "error_count": health.error_count,
                "warning_count": health.warning_count,
                "last_update_time": health.last_update_time.isoformat(),
                "performance_metrics": health.performance_metrics,
                "metadata": health.metadata,
            },
        }
        return self.success_response(payload)

    async def _handle_system_data_notify_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        status = request.query.get("status")
        limit = int(request.query.get("limit", 100))
        items = api.list_notifications(status=status, limit=limit)
        return self.success_response({"items": items, "total": len(items)})

    async def _handle_system_data_notify_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        title = str(body.get("title") or "").strip()
        message = str(body.get("message") or "").strip()
        if not title or not message:
            return self.error_response("Missing required field: title/message", 400)
        api = self._get_system_api()
        created = api.create_notification(
            title=title,
            message=message,
            level=str(body.get("level") or "info"),
            user_id=body.get("user_id"),
            payload=body.get("payload") if isinstance(body.get("payload"), dict) else {},
        )
        return self.success_response(created, "Notification created")

    async def _handle_system_data_notify_status(self, request: web.Request, notification_id: str) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        status = str(body.get("status") or "").strip()
        if not status:
            return self.error_response("Missing required field: status", 400)
        api = self._get_system_api()
        updated = api.update_notification_status(notification_id, status=status)
        if not updated:
            return self.error_response("Notification not found", 404)
        return self.success_response(updated, "Notification updated")

    async def _create_flowhub_job(self, request: web.Request, job_type: str, payload: Optional[Dict[str, Any]] = None) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        created = await orchestrator.create_task_job(
            service="flowhub",
            job_type=job_type,
            params=payload or {},
            metadata={
                "ui_path": request.path,
                "ui_method": request.method,
                "query": dict(request.query),
                "created_at": datetime.utcnow().isoformat() + "Z",
            },
        )
        task_job_id = created["id"]
        return web.json_response(
            {
                "success": True,
                "status": "accepted",
                "message": "Task accepted",
                "data": {
                    "task_job_id": task_job_id,
                    "status_url": f"/api/v1/task-jobs/{task_job_id}",
                    "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel",
                    "history_url": f"/api/v1/task-jobs/{task_job_id}/history",
                    "service": "flowhub",
                    "job_type": job_type,
                },
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
            status=202,
        )

    async def _handle_system_data_sync(self, request: web.Request) -> web.Response:
        payload = {}
        if request.can_read_body:
            payload = await self.get_request_json(request)
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
        return await self._create_flowhub_job(request, "backfill_data_type_history", payload)

    async def _handle_system_data_backfill(self, request: web.Request, action: str) -> web.Response:
        payload = {}
        if request.can_read_body:
            payload = await self.get_request_json(request)
            if payload is None:
                payload = {}
            if not isinstance(payload, dict):
                return self.error_response("Request body must be a JSON object", 400)
        job_type_map = {
            "full-history": "backfill_full_history",
            "resume": "backfill_resume_run",
            "retry-failed": "backfill_retry_failed_shards",
        }
        if action not in job_type_map:
            return self.error_response("Unsupported backfill action", 404)
        return await self._create_flowhub_job(request, job_type_map[action], payload)

    async def _handle_system_settings_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        payload = {
            "settings": api.list_settings(),
            "presets": api.list_presets(),
        }
        return self.success_response(payload)

    async def _handle_system_settings_upsert(self, request: web.Request, key: Optional[str] = None) -> web.Response:
        key = key or request.match_info.get("key")
        if not key:
            return self.error_response("Missing settings key", 400)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        value = body.get("value") if body.get("value") is not None else {}
        metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
        updated_by = body.get("updated_by") or "user_admin"

        api = self._get_system_api()
        setting_payload = api.upsert_setting(
            SystemSettingDTO(
                key=key,
                value=value if isinstance(value, dict) else {"value": value},
                metadata=metadata,
                updated_by=str(updated_by),
            )
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(updated_by),
                action="setting.upsert",
                target_type="setting",
                target_id=key,
                payload={"value": value, "metadata": metadata},
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(setting_payload, "Setting updated")

    async def _handle_system_users_list(self, request: web.Request) -> web.Response:
        limit = int(request.query.get("limit", 100))
        offset = int(request.query.get("offset", 0))
        status = request.query.get("status")
        api = self._get_system_api()
        payload = {
            "items": api.list_users(status=status, limit=limit, offset=offset),
            "roles": api.list_roles(),
            "permissions": api.list_permissions(),
            "limit": limit,
            "offset": offset,
        }
        return self.success_response(payload)

    async def _handle_system_users_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        username = (body.get("username") or "").strip()
        display_name = (body.get("display_name") or "").strip() or username
        if not username:
            return self.error_response("Missing required field: username", 400)
        actor_id = body.get("actor_id") or "user_admin"
        user_id = body.get("id") or str(uuid.uuid4())
        status = str(body.get("status") or "active").strip().lower() or "active"
        password_raw = body.get("password")
        password = str(password_raw).strip() if password_raw is not None else ""
        if status == "active" and not password:
            return self.error_response("Missing required field: password (active user)", 400)
        api = self._get_system_api()
        user = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=username,
                display_name=display_name,
                email=body.get("email"),
                status=status,
                metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else {},
            )
        )
        if password:
            user = api.set_user_password(user_id, self._password_hash(password), actor_id=str(actor_id)) or user
        role_ids = body.get("role_ids") if isinstance(body.get("role_ids"), list) else []
        if role_ids:
            api.replace_user_roles(user_id, [str(role_id) for role_id in role_ids], actor_id=str(actor_id))
            user = api.get_user(user_id) or user
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(actor_id),
                action="user.create",
                target_type="user",
                target_id=user_id,
                payload={
                    "username": username,
                    "status": status,
                    "role_ids": role_ids,
                    "password_set": bool(password),
                },
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(user, "User created")

    async def _handle_system_users_update(self, request: web.Request, user_id: Optional[str] = None) -> web.Response:
        user_id = user_id or request.match_info.get("user_id")
        if not user_id:
            return self.error_response("Missing user_id", 400)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        api = self._get_system_api()
        existing = api.get_user(user_id)
        if not existing:
            return self.error_response("User not found", 404)
        actor_id = body.get("actor_id") or "user_admin"
        next_status = str(body.get("status") or existing.get("status") or "active").strip().lower() or "active"
        if next_status == "active" and not existing.get("password_hash") and body.get("password") is None:
            return self.error_response("Active user requires password; provide password in request body", 400)
        updated = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=(body.get("username") or existing.get("username")),
                display_name=(body.get("display_name") or existing.get("display_name")),
                email=body.get("email", existing.get("email")),
                status=next_status,
                metadata=body.get("metadata") if isinstance(body.get("metadata"), dict) else existing.get("metadata", {}),
            )
        )
        password_raw = body.get("password")
        if password_raw is not None:
            password = str(password_raw).strip()
            if not password:
                return self.error_response("Field password cannot be empty", 400)
            updated = api.set_user_password(user_id, self._password_hash(password), actor_id=str(actor_id)) or updated
        if isinstance(body.get("role_ids"), list):
            api.replace_user_roles(user_id, [str(role_id) for role_id in body["role_ids"]], actor_id=str(actor_id))
            updated = api.get_user(user_id) or updated
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=str(actor_id),
                action="user.update",
                target_type="user",
                target_id=user_id,
                payload={"changes": self._scrub_sensitive_fields(body)},
                created_at=datetime.utcnow().isoformat(),
            )
        )
        return self.success_response(updated, "User updated")

    async def _handle_system_roles_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        items = api.list_roles()
        return self.success_response({"items": items, "total": len(items)})

    async def _handle_system_roles_create(self, request: web.Request) -> web.Response:
        if SystemRoleDTO is None:
            return self.error_response("SystemRoleDTO unavailable in current econdb baseline", 500)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        role_name = str(body.get("name") or "").strip()
        if not role_name:
            return self.error_response("Missing required field: name", 400)
        role_id = str(body.get("id") or f"role_{self._slugify(role_name)}")
        actor_id = str(body.get("actor_id") or "user_admin")
        metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
        description = str(body.get("description") or "")
        permission_ids = body.get("permission_ids") if isinstance(body.get("permission_ids"), list) else []

        api = self._get_system_api()
        role = api.upsert_role(
            SystemRoleDTO(
                id=role_id,
                name=role_name,
                description=description,
                metadata=metadata,
            )
        )
        if permission_ids:
            api.replace_role_permissions(role_id, [str(pid) for pid in permission_ids], actor_id=actor_id)
            role = api.get_role(role_id) or role

        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="role.create",
                target_type="role",
                target_id=role_id,
                payload={"permission_ids": permission_ids, "description": description},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(role, "Role created")

    async def _handle_system_roles_update(self, request: web.Request, role_id: str) -> web.Response:
        if SystemRoleDTO is None:
            return self.error_response("SystemRoleDTO unavailable in current econdb baseline", 500)
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        api = self._get_system_api()
        existing = api.get_role(role_id)
        if not existing:
            return self.error_response("Role not found", 404)

        actor_id = str(body.get("actor_id") or "user_admin")
        role_name = str(body.get("name") or existing.get("name") or role_id).strip()
        description = str(body.get("description") or existing.get("description") or "")
        metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else existing.get("metadata", {})
        permission_ids = body.get("permission_ids") if isinstance(body.get("permission_ids"), list) else None

        role = api.upsert_role(
            SystemRoleDTO(
                id=role_id,
                name=role_name,
                description=description,
                metadata=metadata if isinstance(metadata, dict) else {},
            )
        )
        if permission_ids is not None:
            api.replace_role_permissions(role_id, [str(pid) for pid in permission_ids], actor_id=actor_id)
            role = api.get_role(role_id) or role

        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="role.update",
                target_type="role",
                target_id=role_id,
                payload={"changes": body},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(role, "Role updated")

    async def _handle_system_users_invite(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        email = str(body.get("email") or "").strip()
        username = str(body.get("username") or "").strip()
        if not username:
            username = email.split("@")[0] if email else ""
        if not username:
            return self.error_response("Missing required field: username/email", 400)

        actor_id = str(body.get("actor_id") or "user_admin")
        user_id = str(body.get("id") or f"user_{self._slugify(username)}")
        display_name = str(body.get("display_name") or username)
        api = self._get_system_api()

        metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
        metadata = {
            **metadata,
            "invited_at": self._now_iso(),
            "invited_by": actor_id,
        }

        user = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=username,
                display_name=display_name,
                email=email or None,
                status=str(body.get("status") or "invited"),
                metadata=metadata,
            )
        )
        role_ids = body.get("role_ids") if isinstance(body.get("role_ids"), list) else []
        if role_ids:
            api.replace_user_roles(user_id, [str(role_id) for role_id in role_ids], actor_id=actor_id)
            user = api.get_user(user_id) or user

        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="user.invite",
                target_type="user",
                target_id=user_id,
                payload={"email": email, "role_ids": role_ids},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(user, "Invitation sent")

    async def _handle_system_user_reset_mfa(self, request: web.Request, user_id: str) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        api = self._get_system_api()
        existing = api.get_user(user_id)
        if not existing:
            return self.error_response("User not found", 404)
        metadata = existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {}
        metadata = {
            **metadata,
            "mfa_state": "reset_required",
            "mfa_reset_requested_at": self._now_iso(),
            "mfa_reset_requested_by": actor_id,
        }
        user = api.upsert_user(
            SystemUserDTO(
                id=user_id,
                username=existing.get("username"),
                display_name=existing.get("display_name"),
                email=existing.get("email"),
                status=existing.get("status"),
                metadata=metadata,
            )
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="user.mfa_reset",
                target_type="user",
                target_id=user_id,
                payload={"reason": body.get("reason")},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(user, "MFA reset requested")

    async def _handle_system_users_bulk(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        action = str(body.get("action") or "").strip()
        user_ids = [str(item) for item in body.get("user_ids", []) if item] if isinstance(body.get("user_ids"), list) else []
        if not action:
            return self.error_response("Missing required field: action", 400)
        if not user_ids and action != "export":
            return self.error_response("Missing required field: user_ids", 400)

        actor_id = str(body.get("actor_id") or "user_admin")
        api = self._get_system_api()
        changed: list[Dict[str, Any]] = []

        if action in {"disable", "enable"}:
            next_status = "disabled" if action == "disable" else "active"
            for user_id in user_ids:
                existing = api.get_user(user_id)
                if not existing:
                    continue
                updated = api.upsert_user(
                    SystemUserDTO(
                        id=user_id,
                        username=existing.get("username"),
                        display_name=existing.get("display_name"),
                        email=existing.get("email"),
                        status=next_status,
                        metadata=existing.get("metadata") if isinstance(existing.get("metadata"), dict) else {},
                    )
                )
                changed.append(updated)
        elif action == "assign_role":
            role_ids = [str(item) for item in body.get("role_ids", []) if item] if isinstance(body.get("role_ids"), list) else []
            if not role_ids:
                return self.error_response("Missing required field: role_ids", 400)
            for user_id in user_ids:
                api.replace_user_roles(user_id, role_ids, actor_id=actor_id)
                refreshed = api.get_user(user_id)
                if refreshed:
                    changed.append(refreshed)
        elif action == "export":
            users = api.list_users(limit=1000, offset=0)
            return self.success_response({"items": users, "total": len(users)}, "Users exported")
        else:
            return self.error_response("Unsupported bulk action", 400)

        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="user.bulk_action",
                target_type="user",
                target_id="bulk",
                payload={"action": action, "user_ids": user_ids, "role_ids": body.get("role_ids")},
                created_at=self._now_iso(),
            )
        )
        return self.success_response({"items": changed, "total": len(changed)}, "Bulk action completed")

    async def _handle_system_sessions_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        user_id = request.query.get("user_id")
        status = request.query.get("status")
        limit = self._safe_int(request.query.get("limit"), 200)
        offset = self._safe_int(request.query.get("offset"), 0)

        sessions: list[Dict[str, Any]] = []
        if user_id:
            user = api.get_user(user_id)
            user_sessions = api.list_user_sessions(user_id, status=status, limit=limit + offset)
            for item in user_sessions:
                if not isinstance(item, dict):
                    continue
                sessions.append(
                    {
                        **item,
                        "username": (user or {}).get("username"),
                        "display_name": (user or {}).get("display_name"),
                        "email": (user or {}).get("email"),
                    }
                )
        else:
            users = api.list_users(limit=1000, offset=0)
            for user in users:
                if not isinstance(user, dict):
                    continue
                uid = user.get("id")
                if not uid:
                    continue
                user_sessions = api.list_user_sessions(str(uid), status=status, limit=200)
                for item in user_sessions:
                    if not isinstance(item, dict):
                        continue
                    sessions.append(
                        {
                            **item,
                            "username": user.get("username"),
                            "display_name": user.get("display_name"),
                            "email": user.get("email"),
                            "user_status": user.get("status"),
                        }
                    )

        sessions = self._sort_by_time_desc(sessions, "issued_at", "refreshed_at", "expires_at")
        total = len(sessions)
        items = sessions[offset : offset + limit]
        return self.success_response({"items": items, "total": total, "limit": limit, "offset": offset})

    async def _handle_system_session_revoke(self, request: web.Request, session_id: str) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        reason = str(body.get("reason") or "manual_revoke")
        api = self._get_system_api()
        session = api.revoke_auth_session(session_id, reason=reason)
        if not session:
            return self.error_response("Session not found", 404)
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="session.revoke",
                target_type="auth_session",
                target_id=session_id,
                payload={"reason": reason},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(session, "Session revoked")

    async def _handle_system_approvals_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        status = request.query.get("status")
        setting = api.get_setting("system.users.approvals")
        items = self._read_setting_items(setting)
        if status:
            items = [item for item in items if str(item.get("status", "")).lower() == status.lower()]
        items = self._sort_by_time_desc(items, "updated_at", "created_at")
        return self.success_response({"items": items, "total": len(items)})

    async def _handle_system_approvals_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        title = str(body.get("title") or "").strip()
        request_type = str(body.get("request_type") or "").strip() or "permission_request"
        if not title:
            return self.error_response("Missing required field: title", 400)
        api = self._get_system_api()
        setting = api.get_setting("system.users.approvals")
        items = self._read_setting_items(setting)
        approval_id = str(body.get("id") or f"approval_{uuid.uuid4().hex[:10]}")
        new_item = {
            "id": approval_id,
            "title": title,
            "request_type": request_type,
            "status": str(body.get("status") or "pending"),
            "requester": body.get("requester") or actor_id,
            "target": body.get("target"),
            "payload": body.get("payload") if isinstance(body.get("payload"), dict) else {},
            "comment": body.get("comment"),
            "created_at": self._now_iso(),
            "updated_at": self._now_iso(),
        }
        items = [item for item in items if str(item.get("id")) != approval_id] + [new_item]
        self._upsert_system_setting(
            api,
            "system.users.approvals",
            {"items": items},
            updated_by=actor_id,
            metadata={"source": "users_approvals"},
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="approval.create",
                target_type="approval",
                target_id=approval_id,
                payload={"request_type": request_type, "target": new_item.get("target")},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(new_item, "Approval request created")

    async def _handle_system_approvals_action(self, request: web.Request, approval_id: str, decision: str) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)
        if decision not in {"approve", "reject"}:
            return self.error_response("Unsupported approval action", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        api = self._get_system_api()
        setting = api.get_setting("system.users.approvals")
        items = self._read_setting_items(setting)
        updated_item = None
        next_items: list[Dict[str, Any]] = []
        for item in items:
            if str(item.get("id")) == approval_id:
                updated_item = {
                    **item,
                    "status": "approved" if decision == "approve" else "rejected",
                    "reviewer": actor_id,
                    "review_comment": body.get("comment"),
                    "updated_at": self._now_iso(),
                }
                next_items.append(updated_item)
            else:
                next_items.append(item)
        if updated_item is None:
            return self.error_response("Approval not found", 404)
        self._upsert_system_setting(
            api,
            "system.users.approvals",
            {"items": next_items},
            updated_by=actor_id,
            metadata={"source": "users_approvals"},
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action=f"approval.{decision}",
                target_type="approval",
                target_id=approval_id,
                payload={"comment": body.get("comment")},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(updated_item, "Approval updated")

    async def _handle_system_policies_get(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        setting = api.get_setting("system.users.policy")
        value = setting.get("value") if isinstance(setting, dict) and isinstance(setting.get("value"), dict) else {}
        if not value:
            value = {
                "version": "v1",
                "effect_priority": ["deny", "allow"],
                "default_scope": "workspace",
                "mfa_enforced_actions": ["role.assign", "token.create", "data.credential.view"],
                "rules": [],
                "updated_at": self._now_iso(),
            }
        return self.success_response(value)

    async def _handle_system_policies_upsert(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        api = self._get_system_api()
        current = api.get_setting("system.users.policy")
        current_value = current.get("value") if isinstance(current, dict) and isinstance(current.get("value"), dict) else {}
        value = {
            **current_value,
            **{k: v for k, v in body.items() if k != "actor_id"},
            "updated_at": self._now_iso(),
            "updated_by": actor_id,
        }
        saved = self._upsert_system_setting(
            api,
            "system.users.policy",
            value,
            updated_by=actor_id,
            metadata={"source": "users_policy"},
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="policy.update",
                target_type="policy",
                target_id="system.users.policy",
                payload={"changes": body},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(saved, "Policy updated")

    async def _handle_system_tokens_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        setting = api.get_setting("system.users.tokens")
        items = self._read_setting_items(setting)
        items = self._sort_by_time_desc(items, "updated_at", "created_at")
        return self.success_response({"items": items, "total": len(items)})

    async def _handle_system_tokens_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        owner_id = str(body.get("owner_id") or actor_id)
        token_name = str(body.get("name") or "api-token")
        api = self._get_system_api()
        setting = api.get_setting("system.users.tokens")
        items = self._read_setting_items(setting)
        token_id = str(body.get("id") or f"token_{uuid.uuid4().hex[:10]}")
        item = {
            "id": token_id,
            "name": token_name,
            "owner_id": owner_id,
            "status": "active",
            "scope": body.get("scope") or "workspace",
            "expires_at": body.get("expires_at"),
            "masked": f"{token_id[:8]}********",
            "created_at": self._now_iso(),
            "updated_at": self._now_iso(),
        }
        items = [row for row in items if str(row.get("id")) != token_id] + [item]
        self._upsert_system_setting(
            api,
            "system.users.tokens",
            {"items": items},
            updated_by=actor_id,
            metadata={"source": "users_tokens"},
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action="token.create",
                target_type="token",
                target_id=token_id,
                payload={"owner_id": owner_id, "scope": item.get("scope")},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(item, "Token created")

    async def _handle_system_tokens_action(self, request: web.Request, token_id: str, action: str) -> web.Response:
        body = {}
        if request.can_read_body:
            body = await self.get_request_json(request)
            if body is None:
                body = {}
            if not isinstance(body, dict):
                return self.error_response("Request body must be a JSON object", 400)
        if action not in {"revoke", "rotate", "extend", "reveal"}:
            return self.error_response("Unsupported token action", 400)
        actor_id = str(body.get("actor_id") or "user_admin")
        api = self._get_system_api()
        setting = api.get_setting("system.users.tokens")
        items = self._read_setting_items(setting)
        updated_item = None
        next_items: list[Dict[str, Any]] = []
        for item in items:
            if str(item.get("id")) != token_id:
                next_items.append(item)
                continue
            current = dict(item)
            if action == "revoke":
                current["status"] = "revoked"
                current["revoked_at"] = self._now_iso()
            elif action == "rotate":
                current["masked"] = f"{token_id[:8]}********{uuid.uuid4().hex[:2]}"
                current["rotated_at"] = self._now_iso()
            elif action == "extend":
                current["expires_at"] = body.get("expires_at") or self._now_iso()
            elif action == "reveal":
                current["revealed_once"] = True
                current["revealed_at"] = self._now_iso()
            current["updated_at"] = self._now_iso()
            updated_item = current
            next_items.append(current)
        if updated_item is None:
            return self.error_response("Token not found", 404)
        self._upsert_system_setting(
            api,
            "system.users.tokens",
            {"items": next_items},
            updated_by=actor_id,
            metadata={"source": "users_tokens"},
        )
        api.append_audit_log(
            SystemAuditDTO(
                id=str(uuid.uuid4()),
                actor_id=actor_id,
                action=f"token.{action}",
                target_type="token",
                target_id=token_id,
                payload={"payload": body},
                created_at=self._now_iso(),
            )
        )
        return self.success_response(updated_item, "Token updated")

    async def _handle_system_audit_list(self, request: web.Request) -> web.Response:
        api = self._get_system_api()
        limit = int(request.query.get("limit", 100))
        offset = int(request.query.get("offset", 0))
        actor_id = request.query.get("actor_id")
        action = request.query.get("action")
        logs = api.list_audit_logs(limit=limit, offset=offset, actor_id=actor_id, action=action)
        return self.success_response({"items": logs, "total": len(logs), "limit": limit, "offset": offset})

    async def _handle_internal_route(self, request: web.Request) -> Optional[web.Response]:
        path = request.path
        method = request.method.upper()

        if method == "GET" and path == "/api/v1/ui/shell/context":
            return await self._handle_shell_context(request)
        if method == "GET" and path == "/api/v1/ui/search":
            return await self._handle_search(request)
        if method == "GET" and path == "/api/v1/ui/candidates/analyzers":
            return await self._handle_candidate_analyzers(request)

        if method == "GET" and path == "/api/v1/ui/system/jobs/overview":
            return await self._handle_system_jobs_overview(request)
        if method == "GET" and path == "/api/v1/ui/system/alerts":
            return await self._handle_system_alerts_list(request)
        if method == "POST" and path == "/api/v1/ui/system/alerts/ack":
            return await self._handle_system_alert_ack_all(request)
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/cancel$", path)
            if matched:
                return await self._handle_system_cancel(request, matched.group("task_job_id"))
            matched = re.match(r"^/api/v1/ui/system/jobs/readiness/items/(?P<item_id>[^/]+)/trigger$", path)
            if matched:
                return await self._handle_system_readiness_item_trigger(request, unquote(matched.group("item_id")))
            matched = re.match(r"^/api/v1/ui/system/alerts/(?P<alert_id>[^/]+)/ack$", path)
            if matched:
                return await self._handle_system_alert_ack(request, matched.group("alert_id"))
            matched = re.match(r"^/api/v1/ui/system/alerts/(?P<alert_id>[^/]+)/silence$", path)
            if matched:
                return await self._handle_system_alert_silence(request, matched.group("alert_id"))
            if path == "/api/v1/ui/system/rules":
                return await self._handle_system_rules_create(request)
        if method == "GET":
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/history$", path)
            if matched:
                return await self._handle_system_history(request, matched.group("task_job_id"))
            if path == "/api/v1/ui/system/rules":
                return await self._handle_system_rules_list(request)
            if path == "/api/v1/ui/system/health":
                return await self._handle_system_health(request)

        if method == "GET" and path == "/api/v1/ui/system/data/overview":
            return await self._handle_system_data_overview(request)
        if method == "GET" and path == "/api/v1/ui/system/data/sources":
            return await self._handle_system_data_sources_list(request)
        if method == "POST" and path == "/api/v1/ui/system/data/sources":
            return await self._handle_system_data_sources_create(request)
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/data/sources/(?P<source_id>[^/]+)/test$", path)
            if matched:
                return await self._handle_system_data_source_test(request, matched.group("source_id"))
        if method == "GET" and path == "/api/v1/ui/system/data/pipelines":
            return await self._handle_system_data_pipelines_list(request)
        if method == "POST" and path == "/api/v1/ui/system/data/pipelines":
            return await self._handle_system_data_pipeline_create(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/data/pipelines/(?P<task_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_data_pipeline_update(request, matched.group("task_id"))
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/data/pipelines/(?P<task_id>[^/]+)/run$", path)
            if matched:
                return await self._handle_system_data_pipeline_run(request, matched.group("task_id"))
            matched = re.match(
                r"^/api/v1/ui/system/data/pipelines/(?P<task_id>[^/]+)/(?P<action>pause|resume|enable|disable)$",
                path,
            )
            if matched:
                return await self._handle_system_data_pipeline_control(
                    request,
                    matched.group("task_id"),
                    matched.group("action"),
                )
        if method == "GET":
            matched = re.match(r"^/api/v1/ui/system/data/pipelines/(?P<task_id>[^/]+)/history$", path)
            if matched:
                return await self._handle_system_data_pipeline_history(request, matched.group("task_id"))
        if method == "GET" and path == "/api/v1/ui/system/data/logs":
            return await self._handle_system_data_logs(request)
        if method == "GET" and path == "/api/v1/ui/system/data/quality":
            return await self._handle_system_data_quality(request)
        if method == "GET" and path == "/api/v1/ui/system/data/rules":
            return await self._handle_system_rules_list(request)
        if method == "POST" and path == "/api/v1/ui/system/data/rules":
            return await self._handle_system_rules_create(request)
        if method == "GET" and path == "/api/v1/ui/system/data/notify":
            return await self._handle_system_data_notify_list(request)
        if method == "POST" and path == "/api/v1/ui/system/data/notify":
            return await self._handle_system_data_notify_create(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/data/notify/(?P<notification_id>[^/]+)/status$", path)
            if matched:
                return await self._handle_system_data_notify_status(request, matched.group("notification_id"))
        if method == "GET":
            matched = re.match(r"^/api/v1/ui/system/data/backfill/runs/(?P<run_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_data_backfill_run(request, matched.group("run_id"))
        if method == "POST" and path == "/api/v1/ui/system/data/sync":
            return await self._handle_system_data_sync(request)
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/data/backfill/(?P<action>[^/]+)$", path)
            if matched:
                return await self._handle_system_data_backfill(request, matched.group("action"))

        if method == "GET" and path == "/api/v1/ui/system/settings":
            return await self._handle_system_settings_list(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/settings/(?P<key>[^/]+)$", path)
            if matched:
                return await self._handle_system_settings_upsert(request, matched.group("key"))

        if method == "GET" and path == "/api/v1/ui/system/users":
            return await self._handle_system_users_list(request)
        if method == "POST" and path == "/api/v1/ui/system/users":
            return await self._handle_system_users_create(request)
        if method == "POST" and path == "/api/v1/ui/system/users/invite":
            return await self._handle_system_users_invite(request)
        if method == "POST" and path == "/api/v1/ui/system/users/bulk":
            return await self._handle_system_users_bulk(request)
        if method == "GET" and path == "/api/v1/ui/system/users/roles":
            return await self._handle_system_roles_list(request)
        if method == "POST" and path == "/api/v1/ui/system/users/roles":
            return await self._handle_system_roles_create(request)
        if method == "GET" and path == "/api/v1/ui/system/users/sessions":
            return await self._handle_system_sessions_list(request)
        if method == "GET" and path == "/api/v1/ui/system/users/approvals":
            return await self._handle_system_approvals_list(request)
        if method == "POST" and path == "/api/v1/ui/system/users/approvals":
            return await self._handle_system_approvals_create(request)
        if method == "GET" and path == "/api/v1/ui/system/users/policies":
            return await self._handle_system_policies_get(request)
        if method == "PUT" and path == "/api/v1/ui/system/users/policies":
            return await self._handle_system_policies_upsert(request)
        if method == "GET" and path == "/api/v1/ui/system/users/tokens":
            return await self._handle_system_tokens_list(request)
        if method == "POST" and path == "/api/v1/ui/system/users/tokens":
            return await self._handle_system_tokens_create(request)
        if method == "PUT":
            matched = re.match(r"^/api/v1/ui/system/users/(?P<user_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_users_update(request, matched.group("user_id"))
            matched = re.match(r"^/api/v1/ui/system/users/roles/(?P<role_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_roles_update(request, matched.group("role_id"))
        if method == "POST":
            matched = re.match(r"^/api/v1/ui/system/users/(?P<user_id>[^/]+)/reset-mfa$", path)
            if matched:
                return await self._handle_system_user_reset_mfa(request, matched.group("user_id"))
            matched = re.match(r"^/api/v1/ui/system/users/sessions/(?P<session_id>[^/]+)/revoke$", path)
            if matched:
                return await self._handle_system_session_revoke(request, matched.group("session_id"))
            matched = re.match(r"^/api/v1/ui/system/users/approvals/(?P<approval_id>[^/]+)/(?P<decision>approve|reject)$", path)
            if matched:
                return await self._handle_system_approvals_action(
                    request,
                    matched.group("approval_id"),
                    matched.group("decision"),
                )
            matched = re.match(r"^/api/v1/ui/system/users/tokens/(?P<token_id>[^/]+)/(?P<action>revoke|rotate|extend|reveal)$", path)
            if matched:
                return await self._handle_system_tokens_action(
                    request,
                    matched.group("token_id"),
                    matched.group("action"),
                )

        if method == "GET" and path == "/api/v1/ui/system/audit":
            return await self._handle_system_audit_list(request)

        return None

    async def _proxy_upstream(self, request: web.Request, service_name: str) -> web.Response:
        registry = self.get_app_component(request, "service_registry")
        service = getattr(registry, "_services", {}).get(service_name)
        session = getattr(registry, "_session", None)
        if not service or session is None:
            return self.error_response(f"Service not available: {service_name}", 503)

        target_url = f"{service['url'].rstrip('/')}{request.path_qs}"
        body = await request.read() if request.can_read_body else None
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
        try:
            async with session.request(request.method, target_url, headers=headers, data=body) as resp:
                response_body = await resp.read()
                response_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in HOP_BY_HOP_HEADERS and k.lower() not in CORS_HEADERS
                }
                return web.Response(status=resp.status, headers=response_headers, body=response_body)
        except Exception as exc:
            self.logger.error(f"UI BFF proxy failed: {exc}")
            return self.error_response("UI upstream request failed", 502)

    async def proxy(self, request: web.Request) -> web.Response:
        internal_response = await self._handle_internal_route(request)
        if internal_response is not None:
            return internal_response

        route, path_params = self._match_route(request.method, request.path)
        if not route:
            return self.error_response("Unsupported /api/v1/ui path", 404)

        if not route.is_mutation:
            return await self._proxy_upstream(request, route.service)

        request_payload: Dict[str, Any] = {}
        if request.can_read_body:
            try:
                request_payload = await self.get_request_json(request)
            except web.HTTPBadRequest as exc:
                return self.error_response(exc.text or "Invalid JSON format", 400)
            if not isinstance(request_payload, dict):
                return self.error_response("Request body must be a JSON object", 400)

        try:
            orchestrator = self.get_app_component(request, "task_orchestrator")
            params = self._build_job_params(route, request_payload, path_params)
            metadata = {
                "ui_path": request.path,
                "ui_method": request.method,
                "query": dict(request.query),
                "created_at": datetime.utcnow().isoformat() + "Z",
            }
            created = await orchestrator.create_task_job(
                service=route.service,
                job_type=route.job_type or "unknown",
                params=params,
                metadata=metadata,
            )
            task_job_id = created["id"]
            payload = {
                "task_job_id": task_job_id,
                "status_url": f"/api/v1/task-jobs/{task_job_id}",
                "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel",
                "history_url": f"/api/v1/task-jobs/{task_job_id}/history",
                "service": route.service,
                "job_type": route.job_type,
            }
            return web.json_response(
                {
                    "success": True,
                    "status": "accepted",
                    "message": "Task accepted",
                    "data": payload,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=202,
            )
        except ValueError as exc:
            return self.error_response(str(exc), 400)
        except UpstreamServiceError as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": exc.error.get("message"),
                    "error_code": "UPSTREAM_REQUEST_FAILED",
                    "upstream_status": exc.status,
                    "upstream_service": exc.service,
                    "details": exc.error.get("raw"),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
                status=exc.status if 400 <= exc.status < 500 else 502,
            )
        except Exception as exc:
            self.logger.error(f"UI mutation orchestration failed: {exc}")
            return self.error_response("UI mutation orchestration failed", 502)
