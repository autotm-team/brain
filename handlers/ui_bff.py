"""
Unified UI BFF proxy handler.
"""

import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime, timedelta
import hashlib
import json
import re
import secrets
import time
from urllib.parse import unquote
import uuid
from typing import Any, Callable, Dict, Optional, Pattern

from aiohttp import web

from handlers.base import BaseHandler
from handlers.ui_bff_execution import UIBffExecutionMixin
from handlers.ui_bff_readiness import UIBffReadinessMixin
from handlers.ui_bff_system_data import UIBffSystemDataMixin
from handlers.ui_bff_system_ops import UIBffSystemOpsMixin
from task_orchestrator import UpstreamServiceError

_ECONDB_IMPORT_ERROR: Exception | None = None
try:
    from econdb import (  # type: ignore
        UISystemDataAPI,
        SystemAuditDTO,
        SystemRoleDTO,
        SystemSettingDTO,
        SystemUserDTO,
        create_database_manager,
    )
except Exception as exc:  # pragma: no cover - surfaced only in misconfigured runtime
    UISystemDataAPI = None  # type: ignore[assignment]
    SystemAuditDTO = None  # type: ignore[assignment]
    SystemRoleDTO = None  # type: ignore[assignment]
    SystemSettingDTO = None  # type: ignore[assignment]
    SystemUserDTO = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]
    _ECONDB_IMPORT_ERROR = exc


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


class UIBffHandler(
    UIBffReadinessMixin,
    UIBffExecutionMixin,
    UIBffSystemOpsMixin,
    UIBffSystemDataMixin,
    BaseHandler,
):
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
        RouteSpec("GET", re.compile(r"^/api/v1/ui/structure-rotation/flow-migration$"), "macro"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/clusters$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/events$"), "execution"),
        RouteSpec("GET", re.compile(r"^/api/v1/ui/candidates/symbols/(?P<symbol>[^/]+)/chart$"), "execution"),
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
                {"data_type": "index_info", "label": "指数基础信息"},
                {"data_type": "index_components", "label": "指数成分股"},
                {"data_type": "market_flow_data", "label": "市场资金流"},
                {"data_type": "interest_rate_data", "label": "利率与流动性"},
                {"data_type": "commodity_price_data", "label": "大宗商品"},
                {"data_type": "stock_factor_data", "label": "个股技术因子(专业版)"},
                {"data_type": "daily_info_data", "label": "全市场交易统计"},
                {"data_type": "limit_list_data", "label": "每日涨跌停统计"},
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
                {"data_type": "stock_basic_data", "label": "股票基础信息"},
                {"data_type": "batch_daily_ohlc", "label": "个股日线行情"},
                {"data_type": "batch_daily_basic", "label": "个股日线基础"},
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
        "index_info": {"schedule_type": "cron", "schedule_value": "35 16 * * 1-5"},
        "index_components": {"schedule_type": "cron", "schedule_value": "38 16 * * 1-5"},
        "market_flow_data": {"schedule_type": "cron", "schedule_value": "40 16 * * 1-5"},
        "interest_rate_data": {"schedule_type": "cron", "schedule_value": "45 16 * * 1-5"},
        "commodity_price_data": {"schedule_type": "cron", "schedule_value": "50 16 * * 1-5"},
        "stock_factor_data": {"schedule_type": "cron", "schedule_value": "55 16 * * 1-5"},
        "daily_info_data": {"schedule_type": "cron", "schedule_value": "56 16 * * 1-5"},
        "limit_list_data": {"schedule_type": "cron", "schedule_value": "57 16 * * 1-5"},
    }

    MARKET_SNAPSHOT_TEMPLATE_NAMES = {
        "stock_index_data": "模板·市场快照·指数行情",
        "index_info": "模板·市场快照·指数基础信息",
        "index_components": "模板·市场快照·指数成分股",
        "market_flow_data": "模板·市场快照·市场资金流",
        "interest_rate_data": "模板·市场快照·利率与流动性",
        "commodity_price_data": "模板·市场快照·大宗商品",
        "stock_factor_data": "模板·市场快照·个股技术因子",
        "daily_info_data": "模板·市场快照·全市场交易统计",
        "limit_list_data": "模板·市场快照·每日涨跌停统计",
    }

    MARKET_SNAPSHOT_TEMPLATE_PARAMS = {
        "stock_index_data": {"incremental": True},
        "index_info": {"update_mode": "incremental"},
        "index_components": {"update_mode": "incremental"},
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
        "industry_board": {"source": "ths", "update_mode": "incremental"},
        "concept_board": {"source": "ths", "update_mode": "incremental"},
        "industry_board_stocks": {"source": "ths", "update_mode": "incremental"},
        "concept_board_stocks": {"source": "ths", "update_mode": "incremental"},
        "industry_moneyflow_data": {"source": "ths", "incremental": True},
        "concept_moneyflow_data": {"source": "ths", "incremental": True},
        "batch_daily_basic": {"incremental": True},
    }

    DEPENDENT_STOCK_PIPELINE_TYPES = {"batch_daily_ohlc", "batch_daily_basic"}

    WATCHLIST_TEMPLATE_SCHEDULES = {
        "stock_basic_data": {"schedule_type": "cron", "schedule_value": "20 17 * * 1-5"},
        "batch_daily_ohlc": {"schedule_type": "cron", "schedule_value": "10 17 * * 1-5"},
        "batch_daily_basic": {"schedule_type": "cron", "schedule_value": "30 17 * * 1-5"},
        "industry_board_stocks": {"schedule_type": "cron", "schedule_value": "40 16 * * 1-5"},
        "industry_moneyflow_data": {"schedule_type": "cron", "schedule_value": "10 17 * * 1-5"},
    }

    WATCHLIST_TEMPLATE_NAMES = {
        "stock_basic_data": "模板·候选池·股票基础信息",
        "batch_daily_ohlc": "模板·候选池·个股日线行情",
        "batch_daily_basic": "模板·候选池·个股日线基础",
        "industry_board_stocks": "模板·候选池·行业板块成分股",
        "industry_moneyflow_data": "模板·候选池·行业资金流",
    }

    WATCHLIST_TEMPLATE_PARAMS = {
        "stock_basic_data": {"update_mode": "incremental"},
        "batch_daily_ohlc": {"incremental": True},
        "batch_daily_basic": {"incremental": True},
        "industry_board_stocks": {"source": "ths", "update_mode": "incremental"},
        "industry_moneyflow_data": {"source": "ths", "incremental": True},
    }

    READINESS_REAL_DATA_TABLES = {
        "stock_index_data": {"table": "macro_stock_index_data", "date_column": "date", "value_kind": "date"},
        "index_info": {"table": "index_info", "date_column": "updated_at", "value_kind": "datetime"},
        "index_components": {"table": "index_components", "date_column": "in_date", "value_kind": "date"},
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
        "stock_basic_data": {"table": "stock_basic", "date_column": "updated_at", "value_kind": "datetime"},
        "batch_daily_ohlc": {"table": "stock_daily_data", "date_column": "trade_date"},
        "batch_daily_basic": {"table": "stock_daily_basic", "date_column": "trade_date"},
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
            raise RuntimeError(
                "UISystemDataAPI unavailable; install or update the econdb runtime dependency"
            ) from _ECONDB_IMPORT_ERROR
        if self._system_api is None:
            db_manager = create_database_manager()
            self._system_api = UISystemDataAPI(db_manager)
            self._system_api.seed_defaults(admin_password=None)
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
        if method == "GET" and path == "/api/v1/ui/system/schedules":
            return await self._handle_system_schedules_list(request)
        if method == "GET" and path == "/api/v1/ui/system/alerts":
            return await self._handle_system_alerts_list(request)
        if method == "POST" and path == "/api/v1/ui/system/alerts/ack":
            return await self._handle_system_alert_ack_all(request)
        if method == "POST":
            if path == "/api/v1/ui/system/schedules":
                return await self._handle_system_schedule_create(request)
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/cancel$", path)
            if matched:
                return await self._handle_system_cancel(request, matched.group("task_job_id"))
            matched = re.match(r"^/api/v1/ui/system/jobs/readiness/items/(?P<item_id>[^/]+)/trigger$", path)
            if matched:
                return await self._handle_system_readiness_item_trigger(request, unquote(matched.group("item_id")))
            matched = re.match(r"^/api/v1/ui/system/schedules/(?P<schedule_id>[^/]+)/trigger$", path)
            if matched:
                return await self._handle_system_schedule_trigger(request, matched.group("schedule_id"))
            matched = re.match(r"^/api/v1/ui/system/schedules/(?P<schedule_id>[^/]+)/toggle$", path)
            if matched:
                return await self._handle_system_schedule_toggle(request, matched.group("schedule_id"))
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
            matched = re.match(r"^/api/v1/ui/system/jobs/(?P<task_job_id>[^/]+)/lineage$", path)
            if matched:
                return await self._handle_system_job_lineage(request, matched.group("task_job_id"))
            matched = re.match(r"^/api/v1/ui/system/schedules/(?P<schedule_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_schedule_detail(request, matched.group("schedule_id"))
            if path == "/api/v1/ui/system/rules":
                return await self._handle_system_rules_list(request)
            if path == "/api/v1/ui/system/health":
                return await self._handle_system_health(request)
        if method == "PATCH":
            matched = re.match(r"^/api/v1/ui/system/schedules/(?P<schedule_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_schedule_update(request, matched.group("schedule_id"))
        if method == "DELETE":
            matched = re.match(r"^/api/v1/ui/system/schedules/(?P<schedule_id>[^/]+)$", path)
            if matched:
                return await self._handle_system_schedule_delete(request, matched.group("schedule_id"))

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
