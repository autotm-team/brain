"""
统一任务编排器
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from asyncron import request_envelope
from task_orchestrator_auto_chain import TaskOrchestratorAutoChainMixin

_ECONDB_IMPORT_ERROR: Exception | None = None
try:
    from econdb import TaskRuntimeDataAPI, create_database_manager  # type: ignore
except Exception as exc:  # pragma: no cover - exercised in lightweight tests
    TaskRuntimeDataAPI = None  # type: ignore[assignment]
    create_database_manager = None  # type: ignore[assignment]
    _ECONDB_IMPORT_ERROR = exc


logger = logging.getLogger(__name__)


class UpstreamServiceError(RuntimeError):
    """Raised when upstream service returns non-success HTTP status."""

    def __init__(self, service: str, method: str, path: str, status: int, error: Dict[str, Any]):
        self.service = service
        self.method = method
        self.path = path
        self.status = status
        self.error = error
        super().__init__(f"{service} {method} {path} -> HTTP {status}")


class TaskOrchestrator(TaskOrchestratorAutoChainMixin):
    SERVICES = ("flowhub", "execution", "macro", "portfolio")
    JOB_TYPES_CACHE_TTL_SECONDS = 60
    INTERNAL_JOB_TYPES_BY_SERVICE = {
        "execution": {"ui_candidates_sync_incremental"},
    }
    JOB_TYPE_ZH_MAP = {
        # Flowhub data jobs
        "daily_ohlc": "个股日线行情(单标的)",
        "batch_daily_ohlc": "个股日线行情",
        "daily_basic": "个股日线基础(单标的)",
        "batch_daily_basic": "个股日线基础",
        "stock_basic_data": "股票基础信息",
        "adj_factors": "复权因子",
        "index_daily_data": "指数日线",
        "index_components": "指数成分",
        "index_info": "指数信息",
        "trade_calendar_data": "交易日历",
        "sw_industry_data": "申万行业",
        "industry_board": "行业板块行情",
        "concept_board": "概念板块行情",
        "industry_board_stocks": "行业板块成分股",
        "concept_board_stocks": "概念板块成分股",
        "board_data": "板块数据",
        "index_data": "指数数据",
        "industry_moneyflow_data": "行业资金流",
        "concept_moneyflow_data": "概念资金流",
        "macro_calendar_data": "宏观日历",
        "price_index_data": "价格指数",
        "money_supply_data": "货币供应",
        "social_financing_data": "社会融资",
        "investment_data": "投资数据",
        "industrial_data": "工业数据",
        "sentiment_index_data": "情绪指数",
        "innovation_data": "创新数据",
        "inventory_cycle_data": "库存周期",
        "demographic_data": "人口数据",
        "gdp_data": "GDP",
        "stock_index_data": "股票指数",
        "market_flow_data": "市场资金流",
        "interest_rate_data": "利率数据",
        "commodity_price_data": "大宗商品",
        "suspend_data": "停复牌",
        "st_status_data": "ST状态",
        "stk_limit_data": "涨跌停",
        "backfill_full_history": "全历史回补",
        "backfill_data_type_history": "指定类型历史回补",
        "backfill_resume_run": "历史回补续跑",
        "backfill_retry_failed_shards": "回补失败重试",
        # Execution UI jobs
        "ui_candidates_history_query": "候选池历史查询",
        "ui_candidates_promote": "候选池提升",
        "ui_candidates_auto_promote": "候选池自动提升",
        "ui_candidates_merge": "候选池合并",
        "ui_candidates_ignore": "候选池忽略",
        "ui_candidates_mark_read": "候选池标记已读",
        "ui_candidates_watchlist_update": "候选池状态更新",
        "ui_candidates_sync_from_analysis": "候选池分析同步",
        "ui_candidates_sync_incremental": "候选池增量补齐",
        "ui_candidates_backfill_metadata": "候选池元数据回补",
        "ui_research_decision": "标的研究决策",
        "ui_research_freeze": "标的研究冻结",
        "ui_research_compare": "标的研究对比",
        "ui_research_archive": "标的研究归档",
        "ui_research_unfreeze": "标的研究解冻",
        "ui_research_replace_helper": "研究助手替换",
        "ui_strategy_report_run": "策略报告运行",
        "ui_strategy_report_compare": "策略报告对比",
        "ui_strategy_config_apply": "策略配置应用",
        "ui_strategy_preset_save": "策略预设保存",
        "batch_analyze": "批量分析",
        "analysis_backfill": "分析结果回补",
        # Macro/Portfolio UI jobs
        "ui_macro_cycle_freeze": "宏观周期冻结",
        "ui_macro_cycle_mark_seen": "宏观周期标记已读",
        "ui_macro_cycle_mark_seen_batch": "宏观周期批量标记已读",
        "ui_macro_cycle_apply_portfolio": "宏观周期应用到组合",
        "ui_macro_cycle_apply_snapshot": "宏观周期应用到快照",
        "ui_rotation_policy_freeze": "轮动策略冻结",
        "ui_rotation_policy_apply": "轮动策略应用",
        "ui_rotation_policy_save": "轮动策略保存",
        "ui_snapshot_refresh": "快照刷新",
        "ui_sim_order_create": "模拟下单",
        "ui_sim_order_cancel": "模拟撤单",
    }
    HISTORY_MAX = 200
    # Keep per-service listing lightweight to avoid large payload amplification.
    SERVICE_PAGE_SIZE = 20
    SERVICE_MAX_FETCH = 200
    UPSTREAM_TIMEOUT_SECONDS = 30
    ORPHAN_MONITOR_INTERVAL_SECONDS = 30
    TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}
    ACTIVE_STATUSES = {"queued", "running"}
    RECOVERY_MAX_ATTEMPTS = 3

    def __init__(self, app):
        self._app = app
        self._brain_jobs: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._job_types_cache: Dict[str, Dict[str, Any]] = {}
        self._db_manager = None
        self._task_api = None
        self._local_auto_chain_claims: set[str] = set()
        self._followup_watch_tasks: set[asyncio.Task] = set()

    async def create_task_job(
        self,
        service: str,
        job_type: str,
        params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        service_payload: Optional[Dict[str, Any]] = None,
        lineage_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        service = (service or "").lower()
        if service not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")
        await self._validate_job_type(service, job_type)
        normalized_params = self._normalize_params(params)
        normalized_metadata = self._normalize_metadata(metadata)

        payload = service_payload if isinstance(service_payload, dict) else self._build_create_payload(service, job_type, normalized_params)
        request_payload_hash = self._hash_payload(payload)
        response = await self._request_service(service, "POST", "/api/v1/jobs", payload=payload)
        service_job_id = self._extract_job_id(response)
        if not service_job_id:
            raise RuntimeError(f"Unable to extract job_id from {service} response")

        existing_task_job_id = await self._find_task_job_id(service, service_job_id)
        task_job_id = existing_task_job_id or str(uuid.uuid4())
        existing_record = await self._load_task_record(existing_task_job_id) if existing_task_job_id else None
        record = {
            "task_job_id": task_job_id,
            "service": service,
            "service_job_id": service_job_id,
            "job_type": job_type,
            "created_at": (existing_record or {}).get("created_at") or self._utc_now(),
            "params": normalized_params or ((existing_record or {}).get("params") if isinstance((existing_record or {}).get("params"), dict) else {}),
            "metadata": {
                **(((existing_record or {}).get("metadata")) if isinstance((existing_record or {}).get("metadata"), dict) else {}),
                **normalized_metadata,
            },
            "service_payload": dict(service_payload) if isinstance(service_payload, dict) else None,
            "request_payload_hash": request_payload_hash,
        }
        await self._save_task_record(record)
        await self._save_task_lineage(
            task_job_id,
            metadata=record["metadata"],
            lineage_context=lineage_context,
        )

        job_payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(service, job_payload or {"job_id": service_job_id}, task_job_id=task_job_id)
        normalized["job_type"] = normalized.get("job_type") or job_type
        normalized["metadata"] = {**record["metadata"], **(normalized.get("metadata") or {})}
        await self._append_history(
            task_job_id,
            "deduplicated_reuse" if existing_task_job_id else "created",
            normalized,
            request_payload_hash=request_payload_hash,
            upstream_status=202,
        )
        self._schedule_followup_monitor(task_job_id, service, job_type)
        return normalized

    def _schedule_followup_monitor(self, task_job_id: str, service: str, job_type: str) -> None:
        if service not in {"flowhub", "execution", "macro"}:
            return
        if job_type not in {"stock_basic_data", "batch_daily_ohlc", "batch_daily_basic", "batch_analyze", "index_daily_data", "industry_board", "ui_snapshot_refresh"}:
            return

        async def _watch() -> None:
            deadline = asyncio.get_running_loop().time() + 6 * 3600
            while asyncio.get_running_loop().time() < deadline:
                try:
                    payload = await self._refresh_task_job_snapshot(
                        task_job_id,
                        persist_history=True,
                        trigger_followups=True,
                    )
                    status = str(payload.get("status") or "").lower()
                    if status in {"succeeded", "failed", "cancelled"}:
                        return
                except Exception:
                    return
                await asyncio.sleep(5)

        task = asyncio.create_task(_watch())
        self._followup_watch_tasks.add(task)
        task.add_done_callback(self._followup_watch_tasks.discard)

    async def list_task_job_types(self) -> Dict[str, Any]:
        services_payload: Dict[str, Any] = {}
        merged: Dict[str, Dict[str, Any]] = {}
        errors: list[Dict[str, Any]] = []

        for svc in self.SERVICES:
            try:
                items = await self._fetch_service_job_types(svc, use_cache=False)
                normalized_items = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    job_type = str(item.get("job_type") or "").strip()
                    if not job_type:
                        continue
                    record = {
                        "service": svc,
                        "job_type": job_type,
                    }
                    normalized_items.append(record)
                    merged_key = f"{svc}:{job_type}"
                    merged[merged_key] = record
                services_payload[svc] = normalized_items
            except Exception as exc:
                errors.append({"service": svc, "error": str(exc)})
                services_payload[svc] = []

        items = sorted(merged.values(), key=lambda x: (x["service"], x["job_type"]))
        return {
            "items": items,
            "total": len(items),
            "services": services_payload,
            "errors": errors,
        }

    async def list_schedules(
        self,
        service: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        service_filter = (service or "").strip().lower()
        if service_filter and service_filter not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")

        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            return await scheduler.list_schedules(service=service, limit=limit, offset=offset)

        all_ids = await self._list_schedule_ids()
        items: list[Dict[str, Any]] = []
        for schedule_id in all_ids:
            item = await self._load_schedule(schedule_id)
            if not isinstance(item, dict):
                continue
            if service_filter and item.get("service") != service_filter:
                continue
            items.append(item)

        items.sort(
            key=lambda x: (
                self._as_timestamp(x.get("updated_at") or x.get("created_at")),
                str(x.get("id") or ""),
            ),
            reverse=True,
        )
        total = len(items)
        page = items[offset : offset + limit]
        return {"items": page, "total": total, "limit": limit, "offset": offset}

    async def create_schedule(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Invalid JSON body")
        service = str(payload.get("service") or "").strip().lower()
        job_type = str(payload.get("job_type") or "").strip()
        trigger = str(payload.get("trigger") or "").strip().lower()
        params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        enabled = bool(payload.get("enabled", True))

        if service not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")
        await self._validate_job_type(service, job_type)
        if trigger not in {"cron", "interval"}:
            raise ValueError("trigger must be one of: cron, interval")
        if trigger == "cron":
            cron = str(payload.get("cron") or "").strip()
            if not cron:
                raise ValueError("Missing required field: cron")
        else:
            interval_seconds = int(payload.get("interval_seconds") or 0)
            if interval_seconds <= 0:
                raise ValueError("interval_seconds must be positive")

        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            return await scheduler.create_schedule(
                {
                    "service": service,
                    "job_type": job_type,
                    "trigger": trigger,
                    "cron": str(payload.get("cron") or "").strip() if trigger == "cron" else None,
                    "interval_seconds": int(payload.get("interval_seconds") or 0) if trigger == "interval" else None,
                    "enabled": enabled,
                    "params": dict(params),
                    "metadata": dict(metadata),
                }
            )

        schedule_id = str(uuid.uuid4())
        now = self._utc_now()
        schedule = {
            "id": schedule_id,
            "service": service,
            "job_type": job_type,
            "trigger": trigger,
            "cron": str(payload.get("cron") or "").strip() if trigger == "cron" else None,
            "interval_seconds": int(payload.get("interval_seconds") or 0) if trigger == "interval" else None,
            "enabled": enabled,
            "params": dict(params),
            "metadata": dict(metadata),
            "next_run_at": None,
            "last_triggered_at": None,
            "last_task_job_id": None,
            "dispatch_token": None,
            "last_enqueued_at": None,
            "pending_dispatch": False,
            "created_at": now,
            "updated_at": now,
        }
        if enabled:
            schedule["next_run_at"] = self._compute_next_run(
                trigger=trigger,
                cron=schedule.get("cron"),
                interval_seconds=schedule.get("interval_seconds"),
                base_ts=time.time(),
            )
        await self._save_schedule(schedule)
        return schedule

    async def patch_schedule(self, schedule_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            return await scheduler.update_schedule(schedule_id, payload)

        current = await self._load_schedule(schedule_id)
        if not current:
            raise ValueError("Schedule not found")
        if not isinstance(payload, dict):
            raise ValueError("Invalid JSON body")

        merged = dict(current)
        if "enabled" in payload:
            merged["enabled"] = bool(payload.get("enabled"))
        if "params" in payload:
            if not isinstance(payload.get("params"), dict):
                raise ValueError("params must be a JSON object")
            merged["params"] = dict(payload["params"])
        if "metadata" in payload:
            if not isinstance(payload.get("metadata"), dict):
                raise ValueError("metadata must be a JSON object")
            merged["metadata"] = dict(payload["metadata"])
        if "cron" in payload:
            if merged.get("trigger") != "cron":
                raise ValueError("cron can only be patched when trigger=cron")
            cron = str(payload.get("cron") or "").strip()
            if not cron:
                raise ValueError("cron must be a non-empty string")
            merged["cron"] = cron
        if "interval_seconds" in payload:
            if merged.get("trigger") != "interval":
                raise ValueError("interval_seconds can only be patched when trigger=interval")
            interval_seconds = int(payload.get("interval_seconds") or 0)
            if interval_seconds <= 0:
                raise ValueError("interval_seconds must be positive")
            merged["interval_seconds"] = interval_seconds

        merged["updated_at"] = self._utc_now()
        await self._save_schedule(merged)
        return merged

    async def delete_schedule(self, schedule_id: str) -> bool:
        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            return await scheduler.delete_schedule(schedule_id)

        current = await self._load_schedule(schedule_id)
        if not current:
            return False
        redis = self._redis()
        if not redis:
            self._brain_jobs.pop(f"schedule:{schedule_id}", None)
            return True
        await redis.delete(self._schedule_key(schedule_id))
        await redis.srem(self._schedule_set_key(), schedule_id)
        return True

    async def trigger_schedule(self, schedule_id: str) -> Dict[str, Any]:
        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            current = await scheduler.get_schedule(schedule_id)
        else:
            current = await self._load_schedule(schedule_id)
        if not current:
            raise ValueError("Schedule not found")
        if not current.get("enabled", True):
            raise ValueError("Schedule is disabled")

        metadata = dict(current.get("metadata") or {})
        metadata["schedule_id"] = schedule_id
        created = await self.create_task_job(
            service=str(current.get("service") or ""),
            job_type=str(current.get("job_type") or ""),
            params=dict(current.get("params") or {}),
            metadata=metadata,
            lineage_context={
                "root_schedule_id": schedule_id,
                "trigger_kind": "schedule",
            },
        )
        now = self._utc_now()
        if scheduler is not None:
            current = await scheduler.mark_triggered(
                schedule_id,
                created.get("id"),
                advance_schedule=True,
            )
            if isinstance(current, dict):
                self._brain_jobs[f"schedule:{schedule_id}"] = dict(current)
        else:
            current["last_triggered_at"] = now
            current["last_task_job_id"] = created.get("id")
            current["updated_at"] = now
            await self._save_schedule(current)
        return {"schedule": current, "job": created}

    async def list_task_jobs(
        self,
        service: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> Dict[str, Any]:
        services = [service.lower()] if service else list(self.SERVICES)
        if service and services[0] not in self.SERVICES:
            raise ValueError(f"Unsupported service: {service}")
        jobs: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []
        seen_task_ids: set[str] = set()

        for svc in services:
            try:
                fetch_target = min(
                    max(limit + offset, self.SERVICE_PAGE_SIZE),
                    self.SERVICE_PAGE_SIZE * 2,
                )
                raw_jobs = await self._list_service_jobs(svc, status=status, max_items=fetch_target)
                for raw_job in raw_jobs:
                    service_job_id = self._get_service_job_id(raw_job)
                    if not service_job_id:
                        continue
                    mapped_id = await self._find_task_job_id(svc, service_job_id)
                    task_job_id = mapped_id or f"{svc}:{service_job_id}"
                    normalized = self._normalize_job(svc, raw_job, task_job_id=task_job_id)
                    if mapped_id:
                        normalized = await self._merge_record_metadata(mapped_id, normalized)
                    normalized = self._compact_job_for_list(normalized)
                    if mapped_id:
                        seen_task_ids.add(mapped_id)
                    if status and normalized.get("status") != self._normalize_status(status):
                        continue
                    jobs.append(normalized)
            except UpstreamServiceError as exc:
                errors.append(
                    {
                        "service": svc,
                        "upstream_status": exc.status,
                        "error": exc.error.get("message") or str(exc),
                    }
                )
            except Exception as exc:
                errors.append({"service": svc, "error": str(exc)})

        local_jobs = await self._list_local_task_jobs(services=services, status=status, seen_task_ids=seen_task_ids)
        jobs.extend(local_jobs)
        jobs.sort(
            key=lambda item: (
                self._as_timestamp(item.get("updated_at") or item.get("completed_at") or item.get("created_at")),
                str(item.get("id") or ""),
            ),
            reverse=True,
        )
        total = len(jobs)
        page = jobs[offset: offset + limit]
        return {
            "jobs": page,
            "total": total,
            "limit": limit,
            "offset": offset,
            "errors": errors,
        }

    async def get_task_job_analytics(self, service: Optional[str] = None, window_key: Optional[str] = None) -> Dict[str, Any]:
        task_api = self._get_task_api()
        return {
            "brain": task_api.brain_task_analytics(service=service, window_key=window_key),
            "runtime": task_api.runtime_job_analytics(service_name=service, window_key=window_key),
        }

    async def _list_service_jobs(self, service: str, status: Optional[str], max_items: int) -> List[Dict[str, Any]]:
        jobs: List[Dict[str, Any]] = []
        page_size = self.SERVICE_PAGE_SIZE
        svc_offset = 0
        target = max(self.SERVICE_PAGE_SIZE, int(max_items))

        while svc_offset < min(self.SERVICE_MAX_FETCH, target):
            payload = await self._request_service(
                service,
                "GET",
                "/api/v1/jobs",
                params={
                    "limit": page_size,
                    "offset": svc_offset,
                    **({"status": status} if status else {}),
                },
            )
            page = self._extract_jobs(payload)
            if not page:
                break
            jobs.extend(page)
            if len(jobs) >= target:
                break
            if len(page) < page_size:
                break
            svc_offset += page_size

        return jobs[:target]

    async def _refresh_task_job_snapshot(
        self,
        task_job_id: str,
        *,
        persist_history: bool,
        trigger_followups: bool,
    ) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        try:
            payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
            normalized = self._normalize_job(service, self._extract_data(payload), task_job_id=mapped_task_job_id or task_job_id)
            if mapped_task_job_id:
                normalized = await self._merge_record_metadata(mapped_task_job_id, normalized)
            if mapped_task_job_id and persist_history:
                await self._append_history_if_changed(mapped_task_job_id, normalized)
                if trigger_followups:
                    await self._maybe_trigger_followups(mapped_task_job_id, normalized)
            return normalized
        except UpstreamServiceError as exc:
            if not mapped_task_job_id:
                raise
            fallback = await self._build_local_task_view(mapped_task_job_id)
            if not fallback:
                raise
            fallback["message"] = fallback.get("message") or f"upstream not available: {exc.status}"
            fallback["error"] = fallback.get("error") or {
                "upstream_status": exc.status,
                "message": exc.error.get("message"),
            }
            await self._append_history(
                mapped_task_job_id,
                "upstream_snapshot_error",
                fallback,
                upstream_status=exc.status,
            )
            return fallback

    async def get_task_job(self, task_job_id: str) -> Dict[str, Any]:
        return await self._refresh_task_job_snapshot(
            task_job_id,
            persist_history=False,
            trigger_followups=False,
        )

    async def scan_orphaned_task_jobs_once(self) -> Dict[str, Any]:
        scanned = 0
        recreated = 0
        cancelled = 0
        capped = 0
        skipped = 0
        errors: list[dict[str, Any]] = []

        for task_job_id in await self._list_task_job_ids():
            try:
                record = await self._load_task_record(task_job_id)
                if not isinstance(record, dict):
                    continue
                latest = await self._build_local_task_view(task_job_id)
                if not isinstance(latest, dict):
                    continue
                status = str(latest.get("status") or "").strip().lower()
                if status not in self.ACTIVE_STATUSES:
                    continue
                service = str(record.get("service") or "").strip().lower()
                service_job_id = str(record.get("service_job_id") or "").strip()
                if service not in self.SERVICES or not service_job_id:
                    continue
                scanned += 1
                try:
                    payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
                    normalized = self._normalize_job(
                        service,
                        self._extract_data(payload),
                        task_job_id=task_job_id,
                    )
                    normalized = await self._merge_record_metadata(task_job_id, normalized)
                    await self._append_history_if_changed(task_job_id, normalized)
                    await self._maybe_trigger_followups(task_job_id, normalized)
                    # 每次 upstream API 调用后短暂延时，防止大量活跃任务时产生请求风暴
                    await asyncio.sleep(0.2)
                except UpstreamServiceError as exc:
                    if exc.status == 404:
                        action = await self._recover_orphaned_task(task_job_id, record, latest)
                        if action == "recreated":
                            recreated += 1
                            cancelled += 1
                        elif action == "capped":
                            capped += 1
                            cancelled += 1
                        else:
                            skipped += 1
                    else:
                        await self._append_history(
                            task_job_id,
                            "upstream_snapshot_error",
                            latest,
                            upstream_status=exc.status,
                        )
                except Exception as exc:
                    errors.append({"task_job_id": task_job_id, "error": str(exc)})
            except Exception as exc:
                errors.append({"task_job_id": task_job_id, "error": str(exc)})

        return {
            "scanned": scanned,
            "cancelled": cancelled,
            "recreated": recreated,
            "capped": capped,
            "skipped": skipped,
            "errors": errors,
        }

    async def cancel_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        try:
            await self._request_service(service, "POST", f"/api/v1/jobs/{service_job_id}/cancel")
        except UpstreamServiceError as exc:
            if exc.status in {404, 409}:
                pass
            elif exc.status == 405:
                await self._request_service(service, "DELETE", f"/api/v1/jobs/{service_job_id}")
            else:
                raise
        payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(
            service,
            payload or {"job_id": service_job_id, "status": "cancelled"},
            task_job_id=mapped_task_job_id or task_job_id,
        )
        if mapped_task_job_id:
            if normalized.get("status") == "cancelled":
                await self._append_history(mapped_task_job_id, "cancelled", normalized)
            else:
                await self._append_history_if_changed(mapped_task_job_id, normalized)
        return normalized

    async def get_task_job_history(self, task_job_id: str) -> Dict[str, Any]:
        try:
            service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        except Exception:
            return {"task_job_id": task_job_id, "history": []}

        resolved_id = mapped_task_job_id or task_job_id
        latest_payload = await self._safe_get_service_job(service, service_job_id)
        if latest_payload and mapped_task_job_id:
            normalized = self._normalize_job(service, latest_payload, task_job_id=resolved_id)
            normalized = await self._merge_record_metadata(resolved_id, normalized)
            await self._append_history_if_changed(resolved_id, normalized)

        control_history = await self._get_history(resolved_id)
        if not control_history and latest_payload and mapped_task_job_id:
            control_history.append(
                {
                    "event": "snapshot",
                    "timestamp": self._utc_now(),
                    "job": self._normalize_job(service, latest_payload, task_job_id=resolved_id),
                    "source": "brain_control_plane",
                }
            )

        runtime_history = await self._safe_get_service_history(service, service_job_id)
        history = self._merge_task_job_history(control_history, runtime_history)
        return {"task_job_id": resolved_id, "history": history}

    async def _resolve_task_job_id(self, task_job_id: str) -> Tuple[str, str, Optional[str]]:
        record = await self._load_task_record(task_job_id)
        if record:
            return record["service"], record["service_job_id"], task_job_id

        if ":" in task_job_id:
            service, service_job_id = task_job_id.split(":", 1)
            service = service.lower()
            if service in self.SERVICES and service_job_id:
                return service, service_job_id, None

        raise ValueError(f"Unknown task_job_id: {task_job_id}")

    async def _validate_job_type(self, service: str, job_type: str) -> None:
        value = (job_type or "").strip()
        if not value:
            raise ValueError("Missing required field: job_type")
        allowed = await self._fetch_service_job_type_set(service)
        if value not in allowed:
            raise ValueError(f"Unsupported job_type for {service}: {job_type}")

    @staticmethod
    def _normalize_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ValueError("params must be a JSON object")
        return dict(params)

    @staticmethod
    def _normalize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if metadata is None:
            return {}
        if not isinstance(metadata, dict):
            raise ValueError("metadata must be a JSON object")
        return dict(metadata)

    def _build_create_payload(self, service: str, job_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
        return request_envelope({
            "job_type": job_type,
            "params": params,
        })

    @staticmethod
    def _normalize_upstream_error(payload: Any, status: int) -> Dict[str, Any]:
        if isinstance(payload, dict):
            message = (
                payload.get("error")
                or payload.get("message")
                or (payload.get("data") or {}).get("error")
                if isinstance(payload.get("data"), dict)
                else None
            )
            return {
                "status": status,
                "message": str(message or f"Upstream request failed with status {status}"),
                "raw": payload,
            }
        return {
            "status": status,
            "message": f"Upstream request failed with status {status}",
            "raw": payload,
        }

    @staticmethod
    def _hash_payload(payload: Dict[str, Any]) -> str:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    async def _safe_get_service_job(self, service: str, service_job_id: str) -> Optional[Dict[str, Any]]:
        try:
            payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
            return self._extract_data(payload)
        except Exception:
            return None

    async def _safe_get_service_history(self, service: str, service_job_id: str) -> List[Dict[str, Any]]:
        try:
            payload = await self._request_service(
                service,
                "GET",
                f"/api/v1/jobs/{service_job_id}/history",
                params={"limit": self.HISTORY_MAX, "offset": 0},
            )
        except Exception:
            return []

        items = self._extract_history_items(payload)
        normalized: List[Dict[str, Any]] = []
        for item in items:
            entry = self._normalize_service_history_entry(item, service=service, service_job_id=service_job_id)
            if entry:
                normalized.append(entry)
        return normalized

    async def _request_service(
        self,
        service: str,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        registry = self._app.get("service_registry")
        service_config = getattr(registry, "_services", {}).get(service) if registry else None
        session = getattr(registry, "_session", None) if registry else None
        if not service_config or session is None:
            raise RuntimeError(f"Service {service} not available")

        url = f"{service_config['url'].rstrip('/')}{path}"
        try:
            async with asyncio.timeout(self.UPSTREAM_TIMEOUT_SECONDS):
                async with session.request(method, url, json=payload, params=params) as resp:
                    text = await resp.text()
                    try:
                        data = json.loads(text) if text else {}
                    except Exception:
                        data = {"raw": text}
                    if resp.status >= 400:
                        raise UpstreamServiceError(
                            service=service,
                            method=method,
                            path=path,
                            status=resp.status,
                            error=self._normalize_upstream_error(data, resp.status),
                        )
                    if isinstance(data, dict):
                        return data
                    return {"data": data}
        except TimeoutError:
            raise UpstreamServiceError(
                service=service,
                method=method,
                path=path,
                status=504,
                error={"status": 504, "message": f"Upstream timeout after {self.UPSTREAM_TIMEOUT_SECONDS}s"},
            )

    async def _fetch_service_job_types(self, service: str, use_cache: bool = True) -> List[Dict[str, Any]]:
        now = time.time()
        cache_item = self._job_types_cache.get(service)
        if use_cache and isinstance(cache_item, dict):
            cached_at = float(cache_item.get("cached_at") or 0.0)
            if now - cached_at <= self.JOB_TYPES_CACHE_TTL_SECONDS:
                items = cache_item.get("items")
                if isinstance(items, list):
                    return list(items)

        payload = await self._request_service(service, "GET", "/api/v1/jobs/types")
        data = payload.get("data") if isinstance(payload, dict) else {}
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            items = payload.get("items") if isinstance(payload, dict) else []
        normalized = [item for item in items if isinstance(item, dict) and str(item.get("job_type") or "").strip()]
        self._job_types_cache[service] = {"cached_at": now, "items": list(normalized)}
        return normalized

    async def _fetch_service_job_type_set(self, service: str) -> set[str]:
        items = await self._fetch_service_job_types(service, use_cache=True)
        values = {str(item.get("job_type") or "").strip() for item in items}
        values.update(self.INTERNAL_JOB_TYPES_BY_SERVICE.get(service, set()))
        return {x for x in values if x}

    @staticmethod
    def _extract_data(payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    @classmethod
    def _extract_jobs(cls, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            jobs = data.get("jobs")
            if isinstance(jobs, list):
                return jobs
        if isinstance(payload, dict):
            jobs = payload.get("jobs")
            if isinstance(jobs, list):
                return jobs
        if isinstance(payload, list):
            return payload
        return []

    @staticmethod
    def _extract_history_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            history = data.get("history")
            if isinstance(history, list):
                return [item for item in history if isinstance(item, dict)]
        if isinstance(payload, dict):
            history = payload.get("history")
            if isinstance(history, list):
                return [item for item in history if isinstance(item, dict)]
        return []

    def _extract_job_id(self, payload: Dict[str, Any]) -> Optional[str]:
        candidates: List[Any] = []
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                candidates.extend([data.get("job_id"), data.get("task_id"), data.get("id")])
                job_obj = data.get("job")
                if isinstance(job_obj, dict):
                    candidates.extend([job_obj.get("job_id"), job_obj.get("task_id"), job_obj.get("id")])
            candidates.extend([payload.get("job_id"), payload.get("task_id"), payload.get("id")])
        for value in candidates:
            if isinstance(value, str) and value:
                return value
        return None

    @staticmethod
    def _get_service_job_id(job: Dict[str, Any]) -> Optional[str]:
        value = job.get("job_id")
        return value if isinstance(value, str) and value else None

    async def _find_task_job_id(self, service: str, service_job_id: str) -> Optional[str]:
        for task_job_id, record in self._brain_jobs.items():
            if record.get("service") == service and record.get("service_job_id") == service_job_id:
                return task_job_id

        try:
            row = self._get_task_api().get_brain_task_job_by_service_job(service, service_job_id)
            if isinstance(row, dict):
                task_job_id = str(row.get("task_job_id") or "")
                if task_job_id:
                    self._brain_jobs[task_job_id] = dict(row)
                    return task_job_id
        except Exception:
            pass
        return None

    def _normalize_job(self, service: str, job: Dict[str, Any], task_job_id: str) -> Dict[str, Any]:
        job = job if isinstance(job, dict) else {}
        service_job_id = self._get_service_job_id(job) or task_job_id
        result = job.get("result") if isinstance(job.get("result"), dict) else job.get("result")
        status = self._normalize_status(job.get("status"))
        params = job.get("params") if isinstance(job.get("params"), dict) else {}
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        message = metadata.get("message") or job.get("message")
        job_type = job.get("job_type") or "unknown"
        created_at = job.get("created_at")
        started_at = job.get("started_at")
        updated_at = job.get("updated_at")
        completed_at = job.get("completed_at")
        if updated_at is None:
            updated_at = completed_at or started_at or created_at
        if completed_at is not None and isinstance(result, dict) and status not in self.TERMINAL_STATUSES:
            raw_result_status = str(result.get("status") or "").strip().lower()
            if raw_result_status in self.TERMINAL_STATUSES:
                status = raw_result_status
        if completed_at is not None and status not in self.TERMINAL_STATUSES:
            error_payload = job.get("error")
            has_error = error_payload is not None and error_payload != "" and error_payload != {}
            has_result = result is not None and result != "" and result != {}
            if has_error:
                status = "failed"
            elif has_result:
                status = "succeeded"
        progress = self._normalize_progress(job.get("progress"), status)
        if completed_at is not None and status in {"succeeded", "failed", "cancelled"}:
            progress = 100
        task_name = (
            metadata.get("task_name")
            or params.get("task_name")
            or job.get("task_name")
            or params.get("job_name")
            or job.get("job_name")
        )
        job_type_zh = self._resolve_job_type_zh(
            service=service,
            job_type=str(job_type or ""),
            params=params,
            metadata=metadata,
            task_name=task_name,
        )

        return {
            "id": task_job_id,
            "service": service,
            "service_job_id": service_job_id,
            "job_type": job_type,
            "job_type_zh": job_type_zh,
            "task_name": task_name,
            "status": status,
            "progress": progress,
            "cancellable": status in {"queued", "running"},
            "message": message,
            "error": job.get("error"),
            "result": result,
            "created_at": created_at,
            "started_at": started_at,
            "updated_at": updated_at,
            "completed_at": completed_at,
            "metadata": metadata,
        }

    async def _merge_record_metadata(self, task_job_id: str, normalized: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(normalized or {})
        record = await self._load_task_record(task_job_id)
        if not isinstance(record, dict):
            return merged
        record_metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        current_metadata = merged.get("metadata") if isinstance(merged.get("metadata"), dict) else {}
        merged_metadata = {**record_metadata, **current_metadata}
        merged["metadata"] = merged_metadata

        if not merged.get("job_type_zh"):
            merged["job_type_zh"] = self._resolve_job_type_zh(
                service=str(merged.get("service") or ""),
                job_type=str(merged.get("job_type") or ""),
                params={},
                metadata=merged_metadata,
                task_name=merged.get("task_name"),
            )
        if not merged.get("task_name"):
            merged["task_name"] = (
                merged_metadata.get("task_name")
                or merged_metadata.get("task_name_zh")
                or merged_metadata.get("job_name")
            )
        return merged

    @staticmethod
    def _compact_job_for_list(job: Dict[str, Any]) -> Dict[str, Any]:
        compact = dict(job or {})
        result = compact.get("result")
        if result in (None, "", {}):
            compact["result"] = result
            return compact
        if isinstance(result, dict):
            compact["result"] = {"summary": "result_omitted", "keys": list(result.keys())[:10]}
            return compact
        if isinstance(result, list):
            compact["result"] = {"summary": "result_omitted", "items": len(result)}
            return compact
        compact["result"] = {"summary": "result_omitted"}
        return compact

    @staticmethod
    def _normalize_status(status: Any) -> str:
        if not status:
            return "queued"
        value = str(status).strip().lower()
        if value in {"queued"}:
            return "queued"
        if value in {"running"}:
            return "running"
        if value in {"succeeded"}:
            return "succeeded"
        if value in {"failed"}:
            return "failed"
        if value in {"cancelled"}:
            return "cancelled"
        return "failed"

    @classmethod
    def _resolve_job_type_zh(
        cls,
        *,
        service: str,
        job_type: str,
        params: Dict[str, Any],
        metadata: Dict[str, Any],
        task_name: Any = None,
    ) -> Optional[str]:
        for key in ("job_type_zh", "job_type_label_zh", "job_name_zh", "task_name_zh"):
            value = metadata.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for key in ("job_type_zh", "job_type_label_zh", "job_name_zh", "task_name_zh"):
            value = params.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        token = str(job_type or "").strip().lower()
        mapped = cls.JOB_TYPE_ZH_MAP.get(token)
        if mapped:
            return mapped

        name_token = str(task_name or "").strip()
        if name_token and cls._contains_cjk(name_token):
            return name_token

        if service == "flowhub":
            task_label = str(params.get("task_name") or "").strip()
            if task_label and cls._contains_cjk(task_label):
                return task_label
        return None

    @staticmethod
    def _contains_cjk(value: str) -> bool:
        return any("\u4e00" <= ch <= "\u9fff" for ch in str(value or ""))

    @staticmethod
    def _normalize_progress(progress: Any, status: str) -> int:
        if progress is not None:
            try:
                value = int(float(progress))
                return max(0, min(100, value))
            except Exception:
                pass
        if status == "succeeded":
            return 100
        if status in {"failed", "cancelled"}:
            return 0
        return 0

    def _normalize_service_history_entry(
        self,
        item: Dict[str, Any],
        *,
        service: str,
        service_job_id: str,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(item, dict):
            return None
        entry = dict(item)
        event = entry.get("event") or entry.get("event_type")
        timestamp = entry.get("timestamp") or entry.get("created_at")
        if not event or timestamp is None:
            return None
        payload = entry.get("payload") if isinstance(entry.get("payload"), dict) else {}
        normalized = {
            "event": str(event),
            "timestamp": timestamp,
            "payload": payload,
            "source": "service_runtime",
            "service": service,
            "service_job_id": service_job_id,
        }
        return normalized

    def _merge_task_job_history(
        self,
        control_history: List[Dict[str, Any]],
        runtime_history: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        sortable: List[Tuple[float, int, int, Dict[str, Any]]] = []
        for index, item in enumerate(control_history or []):
            entry = dict(item or {})
            entry["source"] = "brain_control_plane"
            sortable.append((self._as_timestamp(entry.get("timestamp")), 0, index, entry))
        base_index = len(sortable)
        for offset, item in enumerate(runtime_history or []):
            entry = dict(item or {})
            entry["source"] = "service_runtime"
            sortable.append((self._as_timestamp(entry.get("timestamp")), 1, base_index + offset, entry))
        sortable.sort(key=lambda item: (item[0], item[1], item[2]))
        return [item[3] for item in sortable]

    async def _append_history(
        self,
        task_job_id: str,
        event: str,
        normalized_job: Dict[str, Any],
        request_payload_hash: Optional[str] = None,
        upstream_status: Optional[int] = None,
    ) -> None:
        entry = {
            "event": event,
            "timestamp": self._utc_now(),
            "job": dict(normalized_job),
            "source": "brain_control_plane",
        }
        if request_payload_hash:
            entry["request_payload_hash"] = request_payload_hash
        if upstream_status is not None:
            entry["upstream_status"] = upstream_status
        self._history.setdefault(task_job_id, []).append(entry)
        try:
            self._get_task_api().append_brain_task_job_history(
                {
                    "task_job_id": task_job_id,
                    "event_type": event,
                    "job_snapshot": dict(normalized_job),
                    "request_payload_hash": request_payload_hash,
                    "upstream_status": upstream_status,
                    "created_at": datetime.now(timezone.utc),
                }
            )
        except Exception:
            return

    async def _append_history_if_changed(self, task_job_id: str, normalized_job: Dict[str, Any]) -> None:
        history = await self._get_history(task_job_id)
        if not history:
            await self._append_history(task_job_id, "snapshot", normalized_job)
            return
        last_job = history[-1].get("job", {})
        if (
            last_job.get("status") != normalized_job.get("status")
            or last_job.get("progress") != normalized_job.get("progress")
            or last_job.get("updated_at") != normalized_job.get("updated_at")
        ):
            await self._append_history(task_job_id, "snapshot", normalized_job)

    async def _save_task_record(self, record: Dict[str, Any]) -> None:
        task_job_id = record["task_job_id"]
        self._brain_jobs[task_job_id] = dict(record)
        try:
            self._get_task_api().upsert_brain_task_job(record)
        except Exception as exc:
            logger.warning("Persist brain task record failed for %s: %s", task_job_id, exc)
            return

    async def _update_task_record_metadata(self, task_job_id: str, updates: Dict[str, Any]) -> None:
        record = await self._load_task_record(task_job_id)
        if not isinstance(record, dict):
            return
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        metadata = {**metadata, **(updates or {})}
        record["metadata"] = metadata
        await self._save_task_record(record)

    async def _load_task_link(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        cache_key = f"lineage:{task_job_id}"
        local = self._brain_jobs.get(cache_key)
        if isinstance(local, dict):
            return dict(local)
        try:
            link = self._get_task_api().get_brain_task_job_link(task_job_id)
        except Exception:
            return None
        if not isinstance(link, dict):
            return None
        self._brain_jobs[cache_key] = dict(link)
        return dict(link)

    async def _save_task_lineage(
        self,
        task_job_id: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        lineage_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        existing = await self._load_task_link(task_job_id)
        if isinstance(existing, dict):
            return existing
        resolved = await self._resolve_lineage_context(
            task_job_id,
            metadata=metadata,
            lineage_context=lineage_context,
        )
        try:
            saved = self._get_task_api().upsert_brain_task_job_link(resolved)
        except Exception as exc:
            logger.warning("Persist task lineage failed for %s: %s", task_job_id, exc)
            return None
        if isinstance(saved, dict):
            self._brain_jobs[f"lineage:{task_job_id}"] = dict(saved)
            return dict(saved)
        return None

    async def _resolve_lineage_context(
        self,
        task_job_id: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        lineage_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        raw_metadata = metadata if isinstance(metadata, dict) else {}
        raw_context = lineage_context if isinstance(lineage_context, dict) else {}
        parent_task_job_id = str(raw_context.get("parent_task_job_id") or "").strip() or None
        parent_link = await self._load_task_link(parent_task_job_id) if parent_task_job_id else None
        fallback_schedule_id = str(raw_metadata.get("schedule_id") or "").strip() or None
        root_task_job_id = (
            str(raw_context.get("root_task_job_id") or "").strip()
            or (str(parent_link.get("root_task_job_id") or "").strip() if isinstance(parent_link, dict) else "")
            or parent_task_job_id
            or task_job_id
        )
        root_schedule_id = (
            str(raw_context.get("root_schedule_id") or "").strip()
            or (str(parent_link.get("root_schedule_id") or "").strip() if isinstance(parent_link, dict) else "")
            or fallback_schedule_id
            or None
        )
        trigger_kind = str(raw_context.get("trigger_kind") or "").strip().lower()
        if trigger_kind not in {"manual", "schedule", "auto_chain"}:
            if parent_task_job_id:
                trigger_kind = "auto_chain"
            elif root_schedule_id:
                trigger_kind = "schedule"
            else:
                trigger_kind = "manual"
        try:
            depth = int(raw_context.get("depth")) if raw_context.get("depth") is not None else None
        except Exception:
            depth = None
        if depth is None:
            if isinstance(parent_link, dict):
                depth = int(parent_link.get("depth") or 0) + 1
            else:
                depth = 1 if parent_task_job_id else 0
        depth = max(0, depth)
        return {
            "child_task_job_id": task_job_id,
            "parent_task_job_id": parent_task_job_id,
            "root_task_job_id": root_task_job_id,
            "root_schedule_id": root_schedule_id,
            "trigger_kind": trigger_kind,
            "depth": depth,
            "created_at": datetime.utcnow(),
        }

    async def _build_child_lineage_context(self, parent_task_job_id: str) -> Dict[str, Any]:
        parent_id = str(parent_task_job_id or "").strip()
        if not parent_id:
            return {"trigger_kind": "manual"}
        parent_link = await self._load_task_link(parent_id)
        return {
            "parent_task_job_id": parent_id,
            "root_task_job_id": str((parent_link or {}).get("root_task_job_id") or parent_id),
            "root_schedule_id": (parent_link or {}).get("root_schedule_id"),
            "trigger_kind": "auto_chain",
            "depth": int((parent_link or {}).get("depth") or 0) + 1,
        }

    async def _clone_lineage_context(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        link = await self._load_task_link(task_job_id)
        if not isinstance(link, dict):
            return None
        return {
            "parent_task_job_id": link.get("parent_task_job_id"),
            "root_task_job_id": link.get("root_task_job_id"),
            "root_schedule_id": link.get("root_schedule_id"),
            "trigger_kind": link.get("trigger_kind"),
            "depth": int(link.get("depth") or 0),
        }

    async def get_task_job_lineage(self, task_job_id: str) -> Dict[str, Any]:
        try:
            _, _, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
            resolved_id = mapped_task_job_id or task_job_id
        except Exception:
            resolved_id = task_job_id

        link = await self._load_task_link(resolved_id)
        if not isinstance(link, dict):
            return await self._build_single_node_lineage(resolved_id)

        root_task_job_id = str(link.get("root_task_job_id") or resolved_id).strip() or resolved_id
        try:
            link_rows = self._get_task_api().list_brain_task_job_links_by_root(root_task_job_id)
        except Exception:
            link_rows = []
        if not link_rows:
            return await self._build_single_node_lineage(resolved_id)

        jobs_by_id: Dict[str, Dict[str, Any]] = {}
        for row in link_rows:
            child_id = str(row.get("child_task_job_id") or "").strip()
            if not child_id:
                continue
            job_view = await self._build_local_task_view(child_id)
            jobs_by_id[child_id] = dict(job_view) if isinstance(job_view, dict) else {"id": child_id}

        node_map: Dict[str, Dict[str, Any]] = {}
        for row in link_rows:
            child_id = str(row.get("child_task_job_id") or "").strip()
            if not child_id:
                continue
            job = jobs_by_id.get(child_id) or {"id": child_id}
            node_map[child_id] = {
                "task_job_id": child_id,
                "parent_task_job_id": row.get("parent_task_job_id"),
                "root_task_job_id": row.get("root_task_job_id"),
                "root_schedule_id": row.get("root_schedule_id"),
                "trigger_kind": row.get("trigger_kind"),
                "depth": int(row.get("depth") or 0),
                "created_at": row.get("created_at"),
                "service": job.get("service"),
                "job_type": job.get("job_type"),
                "task_name": job.get("task_name"),
                "status": job.get("status"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "updated_at": job.get("updated_at"),
                "job": job,
                "children": [],
            }

        edges: List[Dict[str, Any]] = []
        child_ids_by_parent: Dict[str, List[str]] = {}
        for row in link_rows:
            parent_id = str(row.get("parent_task_job_id") or "").strip()
            child_id = str(row.get("child_task_job_id") or "").strip()
            if not child_id:
                continue
            if parent_id:
                child_ids_by_parent.setdefault(parent_id, []).append(child_id)
                edges.append({"from_task_job_id": parent_id, "to_task_job_id": child_id})

        def _build_tree(node_id: str) -> Dict[str, Any]:
            node = dict(node_map.get(node_id) or {"task_job_id": node_id, "children": []})
            node["children"] = [_build_tree(child_id) for child_id in child_ids_by_parent.get(node_id, [])]
            return node

        tree = [_build_tree(root_task_job_id)] if root_task_job_id in node_map else []
        leaf_count = sum(1 for node in node_map.values() if not child_ids_by_parent.get(node["task_job_id"]))
        max_depth = max((int(node.get("depth") or 0) for node in node_map.values()), default=0)
        return {
            "task_job_id": resolved_id,
            "root_task_job_id": root_task_job_id,
            "root_schedule_id": link.get("root_schedule_id"),
            "nodes": list(node_map.values()),
            "edges": edges,
            "lineage_tree": tree,
            "summary": {
                "node_count": len(node_map),
                "leaf_count": leaf_count,
                "max_depth": max_depth,
            },
        }

    async def _build_single_node_lineage(self, task_job_id: str) -> Dict[str, Any]:
        job = await self._build_local_task_view(task_job_id)
        job = dict(job) if isinstance(job, dict) else {"id": task_job_id}
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        root_schedule_id = str(metadata.get("schedule_id") or "").strip() or None
        trigger_kind = "schedule" if root_schedule_id else "manual"
        node = {
            "task_job_id": task_job_id,
            "parent_task_job_id": None,
            "root_task_job_id": task_job_id,
            "root_schedule_id": root_schedule_id,
            "trigger_kind": trigger_kind,
            "depth": 0,
            "created_at": job.get("created_at"),
            "service": job.get("service"),
            "job_type": job.get("job_type"),
            "task_name": job.get("task_name"),
            "status": job.get("status"),
            "started_at": job.get("started_at"),
            "completed_at": job.get("completed_at"),
            "updated_at": job.get("updated_at"),
            "job": job,
            "children": [],
        }
        return {
            "task_job_id": task_job_id,
            "root_task_job_id": task_job_id,
            "root_schedule_id": root_schedule_id,
            "nodes": [node],
            "edges": [],
            "lineage_tree": [node],
            "summary": {
                "node_count": 1,
                "leaf_count": 1,
                "max_depth": 0,
            },
        }

    async def get_schedule_detail(self, schedule_id: str, recent_limit: int = 20) -> Dict[str, Any]:
        scheduler = self._app.get("unified_scheduler")
        if scheduler is not None:
            schedule = await scheduler.get_schedule(schedule_id)
            if isinstance(schedule, dict):
                self._brain_jobs[f"schedule:{schedule_id}"] = dict(schedule)
        else:
            schedule = None
        if not isinstance(schedule, dict):
            schedule = await self._load_schedule(schedule_id)
        if not isinstance(schedule, dict):
            raise ValueError("Schedule not found")

        last_task_job_id = str(schedule.get("last_task_job_id") or "").strip()
        latest_root_task = None
        lineage_payload = {
            "task_job_id": None,
            "root_task_job_id": None,
            "root_schedule_id": schedule_id,
            "nodes": [],
            "edges": [],
            "lineage_tree": [],
            "summary": {
                "node_count": 0,
                "leaf_count": 0,
                "max_depth": 0,
            },
        }
        if last_task_job_id:
            latest_root_task = await self._build_local_task_view(last_task_job_id)
            lineage_payload = await self.get_task_job_lineage(last_task_job_id)

        try:
            root_links, total = self._get_task_api().list_brain_task_job_root_links_by_schedule(
                schedule_id,
                limit=recent_limit,
                offset=0,
            )
        except Exception:
            root_links, total = [], 0

        recent_task_jobs: List[Dict[str, Any]] = []
        for row in root_links:
            root_id = str(row.get("child_task_job_id") or row.get("root_task_job_id") or "").strip()
            if not root_id:
                continue
            job = await self._build_local_task_view(root_id)
            if isinstance(job, dict):
                recent_task_jobs.append(job)

        return {
            "schedule": schedule,
            "latest_root_task": latest_root_task,
            "recent_task_jobs": recent_task_jobs,
            "lineage_tree": lineage_payload.get("lineage_tree") or [],
            "lineage_nodes": lineage_payload.get("nodes") or [],
            "lineage_edges": lineage_payload.get("edges") or [],
            "lineage_summary": lineage_payload.get("summary") or {},
            "recent_total": total,
        }

    def _get_db_manager(self):
        if self._db_manager is None:
            if create_database_manager is None:
                raise RuntimeError(
                    "econdb runtime dependencies are unavailable; install service dependencies before using task storage"
                ) from _ECONDB_IMPORT_ERROR
            self._db_manager = create_database_manager()
        return self._db_manager

    def _get_task_api(self):
        if self._task_api is None:
            if TaskRuntimeDataAPI is None:
                raise RuntimeError(
                    "TaskRuntimeDataAPI is unavailable; install service dependencies before using task storage"
                ) from _ECONDB_IMPORT_ERROR
            self._task_api = TaskRuntimeDataAPI(self._get_db_manager())
        return self._task_api

    async def cleanup_completed_task_jobs(self, max_age_hours: int = 24 * 30) -> int:
        cutoff = datetime.utcnow() - timedelta(hours=max(1, int(max_age_hours)))
        rows = self._get_task_api().list_brain_task_jobs_for_cleanup(
            statuses=["succeeded", "failed", "cancelled"],
            updated_before=cutoff,
            limit=1000,
        )
        task_job_ids = [str(row.get("task_job_id") or "") for row in rows if row.get("task_job_id")]
        if not task_job_ids:
            return 0
        deleted = self._get_task_api().delete_brain_task_jobs(task_job_ids)
        for task_job_id in task_job_ids:
            self._brain_jobs.pop(task_job_id, None)
            self._brain_jobs.pop(f"lineage:{task_job_id}", None)
            self._history.pop(task_job_id, None)
        return int(deleted or 0)

    def _get_table_latest_date(self, table_name: str, column: str = "trade_date") -> Optional[str]:
        try:
            value = self._get_db_manager().get_table_max_value(table_name, column)
        except Exception:
            return None
        if value is None:
            return None
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    async def _claim_auto_chain(self, key: str) -> bool:
        redis = self._redis()
        if redis:
            try:
                claimed = await redis.set(self._auto_chain_key(key), "1", nx=True, ex=86400)
                return bool(claimed)
            except Exception:
                pass
        if key in self._local_auto_chain_claims:
            return False
        self._local_auto_chain_claims.add(key)
        return True

    async def _release_auto_chain(self, key: str) -> bool:
        """释放 auto-chain 声明锁，允许该链路重新触发。"""
        released = False
        redis = self._redis()
        if redis:
            try:
                result = await redis.delete(self._auto_chain_key(key))
                released = bool(result)
            except Exception:
                pass
        self._local_auto_chain_claims.discard(key)
        if released:
            logger.info("Released auto-chain claim '%s' to allow re-trigger", key)
        return released

    @staticmethod
    def _auto_chain_key(key: str) -> str:
        return f"brain:auto_chain:{key}"

    async def _load_task_record(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        local = self._brain_jobs.get(task_job_id)
        if isinstance(local, dict):
            return dict(local)

        try:
            record = self._get_task_api().get_brain_task_job(task_job_id)
        except Exception:
            return None
        if not isinstance(record, dict):
            return None
        self._brain_jobs[task_job_id] = dict(record)
        return dict(record)

    async def _list_task_job_ids(self) -> List[str]:
        ids = {
            key
            for key in self._brain_jobs.keys()
            if isinstance(key, str) and not key.startswith("schedule:")
        }

        try:
            ids.update(self._get_task_api().list_brain_task_job_ids())
        except Exception:
            pass
        return sorted(ids)

    async def _get_history(self, task_job_id: str) -> List[Dict[str, Any]]:
        local = self._history.get(task_job_id)
        if local:
            normalized = []
            for item in local:
                entry = dict(item)
                entry["source"] = "brain_control_plane"
                normalized.append(entry)
            return normalized

        try:
            rows, _ = self._get_task_api().list_brain_task_job_history(task_job_id, limit=self.HISTORY_MAX, offset=0)
        except Exception:
            return []
        history: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            history.append(
                {
                    "event": row.get("event_type"),
                    "timestamp": row.get("created_at"),
                    "job": row.get("job_snapshot") if isinstance(row.get("job_snapshot"), dict) else {},
                    "request_payload_hash": row.get("request_payload_hash"),
                    "upstream_status": row.get("upstream_status"),
                    "source": "brain_control_plane",
                }
            )
        if history:
            self._history[task_job_id] = list(history)
        return history

    async def _latest_history_entry(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        history = await self._get_history(task_job_id)
        if not history:
            return None
        latest = history[-1]
        return dict(latest) if isinstance(latest, dict) else None

    async def _build_local_task_view(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        record = await self._load_task_record(task_job_id)
        if not isinstance(record, dict):
            return None
        latest = await self._latest_history_entry(task_job_id)
        if isinstance(latest, dict):
            job = latest.get("job")
            if isinstance(job, dict):
                return await self._merge_record_metadata(task_job_id, dict(job))
        normalized = self._normalize_job(
            str(record.get("service") or ""),
            {
                "job_id": record.get("service_job_id"),
                "job_type": record.get("job_type"),
                "status": "queued",
                "metadata": record.get("metadata") if isinstance(record.get("metadata"), dict) else {},
                "created_at": record.get("created_at"),
                "updated_at": record.get("created_at"),
            },
            task_job_id=task_job_id,
        )
        return await self._merge_record_metadata(task_job_id, normalized)

    async def _list_local_task_jobs(
        self,
        *,
        services: List[str],
        status: Optional[str],
        seen_task_ids: set[str],
    ) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        normalized_status = self._normalize_status(status) if status else None
        for task_job_id in await self._list_task_job_ids():
            if task_job_id in seen_task_ids:
                continue
            record = await self._load_task_record(task_job_id)
            if not isinstance(record, dict):
                continue
            service = str(record.get("service") or "").strip().lower()
            if service not in services:
                continue
            local_view = await self._build_local_task_view(task_job_id)
            if not isinstance(local_view, dict):
                continue
            if normalized_status and str(local_view.get("status") or "") != normalized_status:
                continue
            items.append(self._compact_job_for_list(local_view))
        return items

    async def _recover_orphaned_task(
        self,
        task_job_id: str,
        record: Dict[str, Any],
        latest: Dict[str, Any],
    ) -> str:
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        root_task_job_id = str(metadata.get("recovery_root_task_job_id") or task_job_id)
        current_attempts = self._coerce_int(metadata.get("recovery_attempt_count"), default=0)
        recovery_attempt = current_attempts + 1
        base_metadata = dict(metadata)
        await self._append_history(task_job_id, "orphan_detected", latest, upstream_status=404)

        action = "capped" if current_attempts >= self.RECOVERY_MAX_ATTEMPTS else "recreated"
        cancelled_metadata = {
            **base_metadata,
            "recovery_root_task_job_id": root_task_job_id,
            "recovery_attempt_count": current_attempts,
            "orphaned_upstream_job": True,
            "orphaned_reason": "upstream_job_missing",
            "recovery_action": action,
            "recovery_attempt": recovery_attempt,
        }
        if action == "capped":
            cancelled_metadata["recovery_capped"] = True

        cancelled_job = dict(latest)
        cancelled_job["status"] = "cancelled"
        cancelled_job["cancellable"] = False
        cancelled_job["completed_at"] = self._utc_now()
        cancelled_job["updated_at"] = cancelled_job["completed_at"]
        cancelled_job["metadata"] = cancelled_metadata
        cancelled_job["message"] = "upstream job missing; task cancelled by brain orphan recovery"
        cancelled_job["error"] = {
            "code": "UPSTREAM_JOB_MISSING",
            "message": "Upstream job disappeared while brain task was still active",
        }
        await self._save_task_record({**record, "metadata": cancelled_metadata})

        if action == "capped":
            await self._append_history(task_job_id, "orphan_cancelled", cancelled_job, upstream_status=404)
            await self._append_history(task_job_id, "auto_recreate_capped", cancelled_job, upstream_status=404)
            return "capped"

        recreate_metadata = {
            **{
                k: v
                for k, v in base_metadata.items()
                if k
                not in {
                    "replacement_task_job_id",
                    "orphaned_upstream_job",
                    "orphaned_reason",
                    "recovery_action",
                    "recovery_attempt",
                    "recovery_capped",
                }
            },
            "recovery_root_task_job_id": root_task_job_id,
            "recreated_from_task_job_id": task_job_id,
            "recovery_attempt_count": recovery_attempt,
            "auto_recreated": True,
        }
        create_kwargs = {
            "service": str(record.get("service") or ""),
            "job_type": str(record.get("job_type") or ""),
            "params": record.get("params") if isinstance(record.get("params"), dict) else {},
            "metadata": recreate_metadata,
            "service_payload": record.get("service_payload") if isinstance(record.get("service_payload"), dict) else None,
            "lineage_context": await self._clone_lineage_context(task_job_id),
        }
        try:
            created = await self.create_task_job(**create_kwargs)
        except Exception as exc:
            retry_metadata = {
                **base_metadata,
                "recovery_root_task_job_id": root_task_job_id,
                "recovery_attempt_count": recovery_attempt,
                "orphaned_upstream_job": True,
                "orphaned_reason": "upstream_job_missing",
                "recovery_action": "retry_pending",
                "recovery_attempt": recovery_attempt,
                "last_recovery_error": str(exc),
            }
            retry_job = dict(latest)
            retry_job["metadata"] = retry_metadata
            retry_job["message"] = "upstream job missing; auto recreation pending retry"
            retry_job["error"] = {
                "code": "ORPHAN_RECREATE_FAILED",
                "message": str(exc),
            }
            await self._save_task_record({**record, "metadata": retry_metadata})
            await self._append_history(task_job_id, "auto_recreate_failed", retry_job, upstream_status=404)
            return "skipped"

        await self._append_history(task_job_id, "orphan_cancelled", cancelled_job, upstream_status=404)
        replacement_task_job_id = str(created.get("id") or "").strip()
        if replacement_task_job_id:
            await self._update_task_record_metadata(
                task_job_id,
                {
                    **cancelled_metadata,
                    "replacement_task_job_id": replacement_task_job_id,
                },
            )
            cancelled_job["metadata"] = {
                **cancelled_metadata,
                "replacement_task_job_id": replacement_task_job_id,
            }
        await self._append_history(task_job_id, "auto_recreated", cancelled_job, upstream_status=404)
        return "recreated"

    def _redis(self):
        return self._app.get("redis")

    @staticmethod
    def _record_key(task_job_id: str) -> str:
        return f"brain:task_job:{task_job_id}"

    @staticmethod
    def _index_key(service: str, service_job_id: str) -> str:
        return f"brain:task_job:index:{service}:{service_job_id}"

    @staticmethod
    def _history_key(task_job_id: str) -> str:
        return f"brain:task_job:history:{task_job_id}"

    @staticmethod
    def _task_job_set_key() -> str:
        return "brain:task_jobs"

    @staticmethod
    def _schedule_key(schedule_id: str) -> str:
        return f"asyncron:v2:brain:schedule:{schedule_id}"

    @staticmethod
    def _schedule_set_key() -> str:
        return "asyncron:v2:brain:schedules"

    async def _save_schedule(self, schedule: Dict[str, Any]) -> None:
        schedule_id = str(schedule.get("id") or "")
        if not schedule_id:
            return
        self._brain_jobs[f"schedule:{schedule_id}"] = dict(schedule)
        self._get_task_api().upsert_brain_schedule(
            {
                "schedule_id": schedule_id,
                "service": str(schedule.get("service") or ""),
                "job_type": str(schedule.get("job_type") or ""),
                "trigger": str(schedule.get("trigger") or ""),
                "cron_expr": schedule.get("cron"),
                "interval_seconds": schedule.get("interval_seconds"),
                "enabled": bool(schedule.get("enabled", True)),
                "params": dict(schedule.get("params") or {}),
                "metadata": dict(schedule.get("metadata") or {}),
                "next_run_at": self._datetime_or_none(schedule.get("next_run_at")),
                "last_triggered_at": self._datetime_or_none(schedule.get("last_triggered_at")),
                "last_task_job_id": schedule.get("last_task_job_id"),
                "dispatch_token": schedule.get("dispatch_token"),
                "last_enqueued_at": self._datetime_or_none(schedule.get("last_enqueued_at")),
                "pending_dispatch": bool(schedule.get("pending_dispatch", False)),
                "created_at": self._datetime_or_none(schedule.get("created_at")) or datetime.utcnow(),
                "updated_at": self._datetime_or_none(schedule.get("updated_at")) or datetime.utcnow(),
            }
        )

    async def _load_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        local = self._brain_jobs.get(f"schedule:{schedule_id}")
        if isinstance(local, dict):
            return dict(local)
        row = self._get_task_api().get_brain_schedule(schedule_id)
        if not isinstance(row, dict):
            return None
        schedule = {
            "id": row.get("schedule_id"),
            "service": row.get("service"),
            "job_type": row.get("job_type"),
            "trigger": row.get("trigger"),
            "cron": row.get("cron_expr"),
            "interval_seconds": row.get("interval_seconds"),
            "enabled": bool(row.get("enabled", True)),
            "params": dict(row.get("params") or {}),
            "metadata": dict(row.get("metadata") or {}),
            "next_run_at": self._as_timestamp(row.get("next_run_at")) if row.get("next_run_at") else None,
            "last_triggered_at": self._as_timestamp(row.get("last_triggered_at")) if row.get("last_triggered_at") else None,
            "last_task_job_id": row.get("last_task_job_id"),
            "dispatch_token": row.get("dispatch_token"),
            "last_enqueued_at": self._as_timestamp(row.get("last_enqueued_at")) if row.get("last_enqueued_at") else None,
            "pending_dispatch": bool(row.get("pending_dispatch", False)),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }
        self._brain_jobs[f"schedule:{schedule_id}"] = dict(schedule)
        return schedule

    async def _list_schedule_ids(self) -> List[str]:
        ids: list[str] = []
        for key in self._brain_jobs.keys():
            if key.startswith("schedule:"):
                ids.append(key.split("schedule:", 1)[1])
        try:
            rows, _ = self._get_task_api().list_brain_schedules(limit=1000, offset=0)
            ids.extend(str(row.get("schedule_id") or "") for row in rows if row.get("schedule_id"))
        except Exception:
            pass
        return list(dict.fromkeys([schedule_id for schedule_id in ids if schedule_id]))

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    @staticmethod
    def _as_timestamp(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except Exception:
            pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _datetime_or_none(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, (int, float)):
            return datetime.utcfromtimestamp(float(value))
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
