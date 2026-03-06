"""
统一任务编排器
"""

import asyncio
import hashlib
import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from asyncron import request_envelope


class UpstreamServiceError(RuntimeError):
    """Raised when upstream service returns non-success HTTP status."""

    def __init__(self, service: str, method: str, path: str, status: int, error: Dict[str, Any]):
        self.service = service
        self.method = method
        self.path = path
        self.status = status
        self.error = error
        super().__init__(f"{service} {method} {path} -> HTTP {status}")


class TaskOrchestrator:
    SERVICES = ("flowhub", "execution", "macro", "portfolio")
    JOB_TYPES_CACHE_TTL_SECONDS = 60
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
        # Macro/Portfolio UI jobs
        "ui_macro_cycle_freeze": "宏观周期冻结",
        "ui_macro_cycle_mark_seen": "宏观周期标记已读",
        "ui_macro_cycle_mark_seen_batch": "宏观周期批量标记已读",
        "ui_macro_cycle_apply_portfolio": "宏观周期应用到组合",
        "ui_macro_cycle_apply_snapshot": "宏观周期应用到快照",
        "ui_rotation_policy_freeze": "轮动策略冻结",
        "ui_rotation_policy_apply": "轮动策略应用",
        "ui_rotation_policy_save": "轮动策略保存",
        "ui_sim_order_create": "模拟下单",
        "ui_sim_order_cancel": "模拟撤单",
    }
    HISTORY_MAX = 200
    # Keep per-service listing lightweight to avoid large payload amplification.
    SERVICE_PAGE_SIZE = 20
    SERVICE_MAX_FETCH = 200
    UPSTREAM_TIMEOUT_SECONDS = 8

    def __init__(self, app):
        self._app = app
        self._brain_jobs: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._job_types_cache: Dict[str, Dict[str, Any]] = {}

    async def create_task_job(
        self,
        service: str,
        job_type: str,
        params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        service_payload: Optional[Dict[str, Any]] = None,
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

        task_job_id = str(uuid.uuid4())
        record = {
            "task_job_id": task_job_id,
            "service": service,
            "service_job_id": service_job_id,
            "job_type": job_type,
            "created_at": self._utc_now(),
            "metadata": normalized_metadata,
            "request_payload_hash": request_payload_hash,
        }
        await self._save_task_record(record)

        job_payload = await self._safe_get_service_job(service, service_job_id)
        normalized = self._normalize_job(service, job_payload or {"job_id": service_job_id}, task_job_id=task_job_id)
        normalized["job_type"] = normalized.get("job_type") or job_type
        normalized["metadata"] = {**record["metadata"], **(normalized.get("metadata") or {})}
        await self._append_history(
            task_job_id,
            "created",
            normalized,
            request_payload_hash=request_payload_hash,
            upstream_status=202,
        )
        return normalized

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
            "last_triggered_at": None,
            "last_task_job_id": None,
            "created_at": now,
            "updated_at": now,
        }
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
        )
        now = self._utc_now()
        if scheduler is not None:
            current = await scheduler.mark_triggered(schedule_id, created.get("id"))
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
                        await self._append_history_if_changed(mapped_id, normalized)
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

    async def get_task_job(self, task_job_id: str) -> Dict[str, Any]:
        service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        try:
            payload = await self._request_service(service, "GET", f"/api/v1/jobs/{service_job_id}")
            normalized = self._normalize_job(service, self._extract_data(payload), task_job_id=mapped_task_job_id or task_job_id)
            if mapped_task_job_id:
                normalized = await self._merge_record_metadata(mapped_task_job_id, normalized)
            if mapped_task_job_id:
                await self._append_history_if_changed(mapped_task_job_id, normalized)
            return normalized
        except UpstreamServiceError as exc:
            if not mapped_task_job_id:
                raise
            record = await self._load_task_record(mapped_task_job_id) or {}
            fallback = self._normalize_job(
                service,
                {
                    "job_id": service_job_id,
                    "job_type": record.get("job_type"),
                    "status": "queued",
                    "metadata": record.get("metadata") if isinstance(record.get("metadata"), dict) else {},
                    "message": f"upstream not available: {exc.status}",
                },
                task_job_id=mapped_task_job_id,
            )
            fallback = await self._merge_record_metadata(mapped_task_job_id, fallback)
            fallback["error"] = {
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
            await self._append_history(mapped_task_job_id, "cancelled", normalized)
        return normalized

    async def get_task_job_history(self, task_job_id: str) -> Dict[str, Any]:
        try:
            service, service_job_id, mapped_task_job_id = await self._resolve_task_job_id(task_job_id)
        except Exception:
            return {"task_job_id": task_job_id, "history": []}

        resolved_id = mapped_task_job_id or task_job_id
        history = await self._get_history(resolved_id)
        if not history:
            payload = await self._safe_get_service_job(service, service_job_id)
            if payload:
                history.append({
                    "event": "snapshot",
                    "timestamp": self._utc_now(),
                    "job": self._normalize_job(service, payload, task_job_id=resolved_id),
                })

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
        if service == "flowhub":
            flowhub_payload = dict(params)
            flowhub_payload.setdefault("data_type", job_type)
            return flowhub_payload
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
        for key in ("job_id", "task_id", "id"):
            value = job.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    async def _find_task_job_id(self, service: str, service_job_id: str) -> Optional[str]:
        for task_job_id, record in self._brain_jobs.items():
            if record.get("service") == service and record.get("service_job_id") == service_job_id:
                return task_job_id

        redis = self._redis()
        if not redis:
            return None
        key = self._index_key(service, service_job_id)
        try:
            mapped = await redis.get(key)
            return mapped or None
        except Exception:
            return None

    def _normalize_job(self, service: str, job: Dict[str, Any], task_job_id: str) -> Dict[str, Any]:
        job = job if isinstance(job, dict) else {}
        service_job_id = self._get_service_job_id(job) or task_job_id
        status = self._normalize_status(job.get("status") or job.get("state") or job.get("job_status"))
        progress = self._normalize_progress(job.get("progress"), status)
        params = job.get("params") if isinstance(job.get("params"), dict) else {}
        metadata = job.get("metadata") if isinstance(job.get("metadata"), dict) else {}
        message = metadata.get("message") or job.get("message")
        job_type = (
            job.get("job_type")
            or job.get("type")
            or job.get("task_type")
            or job.get("data_type")
            or params.get("job_type")
            or params.get("data_type")
            or "unknown"
        )
        created_at = job.get("created_at")
        started_at = job.get("started_at") or job.get("start_time")
        updated_at = job.get("updated_at") or job.get("last_update")
        completed_at = job.get("completed_at") or job.get("end_time")
        if updated_at is None:
            updated_at = completed_at or started_at or created_at
        task_name = (
            metadata.get("task_name")
            or params.get("task_name")
            or job.get("task_name")
            or params.get("job_name")
            or job.get("job_name")
            or job.get("name")
            or job.get("title")
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
            "result": job.get("result"),
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
        }
        if request_payload_hash:
            entry["request_payload_hash"] = request_payload_hash
        if upstream_status is not None:
            entry["upstream_status"] = upstream_status
        self._history.setdefault(task_job_id, []).append(entry)

        redis = self._redis()
        if not redis:
            return
        try:
            key = self._history_key(task_job_id)
            await redis.rpush(key, json.dumps(entry, ensure_ascii=False))
            await redis.ltrim(key, -self.HISTORY_MAX, -1)
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

        redis = self._redis()
        if not redis:
            return
        try:
            await redis.set(self._record_key(task_job_id), json.dumps(record, ensure_ascii=False))
            await redis.set(self._index_key(record["service"], record["service_job_id"]), task_job_id)
        except Exception:
            return

    async def _load_task_record(self, task_job_id: str) -> Optional[Dict[str, Any]]:
        local = self._brain_jobs.get(task_job_id)
        if isinstance(local, dict):
            return dict(local)

        redis = self._redis()
        if not redis:
            return None
        try:
            raw = await redis.get(self._record_key(task_job_id))
        except Exception:
            return None
        if not raw:
            return None
        try:
            record = json.loads(raw)
        except Exception:
            return None
        if isinstance(record, dict):
            self._brain_jobs[task_job_id] = dict(record)
            return dict(record)
        return None

    async def _get_history(self, task_job_id: str) -> List[Dict[str, Any]]:
        local = self._history.get(task_job_id)
        if local:
            return [dict(item) for item in local]

        redis = self._redis()
        if not redis:
            return []
        try:
            rows = await redis.lrange(self._history_key(task_job_id), 0, -1)
        except Exception:
            return []
        history: List[Dict[str, Any]] = []
        for row in rows:
            try:
                parsed = json.loads(row)
            except Exception:
                continue
            if isinstance(parsed, dict):
                history.append(parsed)
        if history:
            self._history[task_job_id] = list(history)
        return history

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
    def _schedule_key(schedule_id: str) -> str:
        return f"asyncron:v2:brain:schedule:{schedule_id}"

    @staticmethod
    def _schedule_set_key() -> str:
        return "asyncron:v2:brain:schedules"

    async def _save_schedule(self, schedule: Dict[str, Any]) -> None:
        schedule_id = str(schedule.get("id") or "")
        if not schedule_id:
            return
        redis = self._redis()
        if not redis:
            self._brain_jobs[f"schedule:{schedule_id}"] = dict(schedule)
            return
        await redis.set(self._schedule_key(schedule_id), json.dumps(schedule, ensure_ascii=False))
        await redis.sadd(self._schedule_set_key(), schedule_id)

    async def _load_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        redis = self._redis()
        if not redis:
            local = self._brain_jobs.get(f"schedule:{schedule_id}")
            return dict(local) if isinstance(local, dict) else None
        raw = await redis.get(self._schedule_key(schedule_id))
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    async def _list_schedule_ids(self) -> List[str]:
        redis = self._redis()
        if not redis:
            ids: list[str] = []
            for key in self._brain_jobs.keys():
                if key.startswith("schedule:"):
                    ids.append(key.split("schedule:", 1)[1])
            return ids
        values = await redis.smembers(self._schedule_set_key())
        return [str(v) for v in values if isinstance(v, str) and v]

    @staticmethod
    def _utc_now() -> str:
        return datetime.utcnow().isoformat() + "Z"

    @staticmethod
    def _as_timestamp(value: Any) -> float:
        if value is None:
            return 0.0
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
