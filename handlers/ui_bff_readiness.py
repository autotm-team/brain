"""Readiness helpers extracted from the UI BFF handler."""

from __future__ import annotations

import asyncio
from datetime import datetime
import json
from typing import Any, Dict, Optional

from aiohttp import web

from task_orchestrator import UpstreamServiceError


class UIBffReadinessMixin:
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
            if data_type in self.DEPENDENT_STOCK_PIPELINE_TYPES:
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
            if data_type in self.DEPENDENT_STOCK_PIPELINE_TYPES:
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

    @classmethod
    def _is_conflicting_dependent_stock_schedule(cls, task: Dict[str, Any]) -> bool:
        if not isinstance(task, dict):
            return False
        data_type = str(task.get("data_type") or "").strip().lower()
        if data_type not in cls.DEPENDENT_STOCK_PIPELINE_TYPES:
            return False
        return cls._is_active_periodic_pipeline_task(task)

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

            for task in list(working_tasks):
                if not self._is_conflicting_dependent_stock_schedule(task):
                    continue
                task_id = str(task.get("task_id") or "").strip()
                if not task_id:
                    continue
                try:
                    orchestrator = self.get_app_component(request, "task_orchestrator")
                    await orchestrator.patch_schedule(task_id, {"enabled": False})
                    task["enabled"] = False
                    report.setdefault("disabled", []).append(
                        {
                            "data_type": task.get("data_type"),
                            "task_id": task_id,
                            "reason": "dependent_stock_pipeline",
                        }
                    )
                except Exception as exc:
                    report["errors"].append(
                        {
                            "data_type": task.get("data_type"),
                            "task_id": task_id,
                            "stage": "disable",
                            "message": str(exc),
                        }
                    )

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
            UIBffReadinessMixin._parse_any_timestamp(job.get("updated_at")),
            UIBffReadinessMixin._parse_any_timestamp(job.get("completed_at")),
            UIBffReadinessMixin._parse_any_timestamp(job.get("created_at")),
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
            if token == "queued":
                return "queued"
            if token == "running":
                return "running"
            if token == "succeeded":
                return "succeeded"
            if token == "failed":
                return "failed"
            if token == "cancelled":
                return "cancelled"
            return "failed"

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
