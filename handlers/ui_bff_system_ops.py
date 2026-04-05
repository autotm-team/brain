"""System schedule and readiness endpoints extracted from the UI BFF handler."""

from __future__ import annotations

from datetime import datetime, timedelta
import time
from typing import Any, Dict, Optional
import uuid

from aiohttp import web

from task_orchestrator import UpstreamServiceError

try:
    from econdb import SystemAuditDTO  # type: ignore
except Exception:  # pragma: no cover - used only in full runtime
    SystemAuditDTO = None  # type: ignore[assignment]


class UIBffSystemOpsMixin:
    def _schedule_name(item: Dict[str, Any]) -> str:
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        return str(metadata.get("name") or item.get("job_type") or item.get("schedule_id") or "schedule").strip()

    def _normalize_system_schedule_item(
        self,
        schedule: Dict[str, Any],
        *,
        latest_root_task: Optional[Dict[str, Any]] = None,
        lineage_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        latest_root_task = latest_root_task if isinstance(latest_root_task, dict) else {}
        lineage_summary = lineage_summary if isinstance(lineage_summary, dict) else {}
        return {
            "schedule_id": str(schedule.get("id") or schedule.get("schedule_id") or ""),
            "name": self._schedule_name(schedule),
            "service": str(schedule.get("service") or ""),
            "job_type": str(schedule.get("job_type") or ""),
            "trigger": str(schedule.get("trigger") or ""),
            "cron_expr": schedule.get("cron") or schedule.get("cron_expr"),
            "interval_seconds": schedule.get("interval_seconds"),
            "enabled": self._coerce_bool(schedule.get("enabled")),
            "next_run_at": schedule.get("next_run_at"),
            "last_triggered_at": schedule.get("last_triggered_at"),
            "last_task_job_id": schedule.get("last_task_job_id"),
            "last_task_status": latest_root_task.get("status") if latest_root_task else None,
            "last_chain_depth": self._safe_int(lineage_summary.get("max_depth"), 0),
            "last_chain_leaf_count": self._safe_int(lineage_summary.get("leaf_count"), 0),
            "metadata": dict(schedule.get("metadata") or {}) if isinstance(schedule.get("metadata"), dict) else {},
            "params": dict(schedule.get("params") or {}) if isinstance(schedule.get("params"), dict) else {},
            "created_at": schedule.get("created_at"),
            "updated_at": schedule.get("updated_at"),
        }

    def _normalize_system_schedule_detail_payload(self, detail: Dict[str, Any]) -> Dict[str, Any]:
        schedule = detail.get("schedule") if isinstance(detail.get("schedule"), dict) else {}
        return {
            "schedule": self._normalize_system_schedule_item(
                schedule,
                latest_root_task=detail.get("latest_root_task") if isinstance(detail.get("latest_root_task"), dict) else None,
                lineage_summary=detail.get("lineage_summary") if isinstance(detail.get("lineage_summary"), dict) else None,
            ),
            "latest_root_task": detail.get("latest_root_task") if isinstance(detail.get("latest_root_task"), dict) else None,
            "recent_task_jobs": detail.get("recent_task_jobs") if isinstance(detail.get("recent_task_jobs"), list) else [],
            "lineage_tree": detail.get("lineage_tree") if isinstance(detail.get("lineage_tree"), list) else [],
            "lineage_nodes": detail.get("lineage_nodes") if isinstance(detail.get("lineage_nodes"), list) else [],
            "lineage_edges": detail.get("lineage_edges") if isinstance(detail.get("lineage_edges"), list) else [],
            "lineage_summary": detail.get("lineage_summary") if isinstance(detail.get("lineage_summary"), dict) else {},
        }

    def _schedule_sort_key(self, item: Dict[str, Any]) -> tuple[Any, ...]:
        enabled = self._coerce_bool(item.get("enabled"))
        next_run_ts = self._parse_any_timestamp(item.get("next_run_at"))
        updated_ts = self._parse_any_timestamp(item.get("updated_at"))
        has_due_time = enabled and next_run_ts > 0
        overdue = has_due_time and next_run_ts <= time.time()
        return (
            0 if overdue else 1 if has_due_time else 2,
            next_run_ts if has_due_time else float("inf"),
            -updated_ts,
            str(item.get("schedule_id") or ""),
        )

    async def _handle_system_schedules_list(self, request: web.Request) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        query = request.query
        service = str(query.get("service") or "").strip().lower() or None
        trigger = str(query.get("trigger") or "").strip().lower() or None
        enabled_filter = str(query.get("enabled") or "").strip().lower()
        search = str(query.get("search") or "").strip().lower()
        limit = max(1, min(200, self._safe_int(query.get("limit"), 100)))
        offset = max(0, self._safe_int(query.get("offset"), 0))

        payload = await orchestrator.list_schedules(service=service, limit=1000, offset=0)
        items = payload.get("items", []) if isinstance(payload, dict) else []
        filtered: list[Dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if trigger and str(item.get("trigger") or "").strip().lower() != trigger:
                continue
            if enabled_filter in {"enabled", "true", "1"} and not self._coerce_bool(item.get("enabled")):
                continue
            if enabled_filter in {"disabled", "false", "0"} and self._coerce_bool(item.get("enabled")):
                continue
            if search:
                bag = " ".join(
                    [
                        str(item.get("id") or ""),
                        str(item.get("service") or ""),
                        str(item.get("job_type") or ""),
                        str(item.get("trigger") or ""),
                        str(item.get("cron") or ""),
                        str(item.get("interval_seconds") or ""),
                        self._schedule_name(item),
                    ]
                ).lower()
                if search not in bag:
                    continue
            filtered.append(item)

        filtered.sort(key=self._schedule_sort_key)
        total = len(filtered)
        page_items = filtered[offset : offset + limit]
        enriched: list[Dict[str, Any]] = []
        for item in page_items:
            detail = await orchestrator.get_schedule_detail(str(item.get("id") or ""))
            enriched.append(
                self._normalize_system_schedule_item(
                    detail.get("schedule") if isinstance(detail.get("schedule"), dict) else item,
                    latest_root_task=detail.get("latest_root_task") if isinstance(detail.get("latest_root_task"), dict) else None,
                    lineage_summary=detail.get("lineage_summary") if isinstance(detail.get("lineage_summary"), dict) else None,
                )
            )
        return self.success_response({"items": enriched, "total": total, "limit": limit, "offset": offset})

    async def _handle_system_schedule_detail(self, request: web.Request, schedule_id: str) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        detail = await orchestrator.get_schedule_detail(schedule_id)
        return self.success_response(self._normalize_system_schedule_detail_payload(detail))

    async def _handle_system_schedule_create(self, request: web.Request) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        created = await orchestrator.create_schedule(body)
        detail = await orchestrator.get_schedule_detail(str(created.get("id") or ""))
        return self.success_response(self._normalize_system_schedule_detail_payload(detail), "Schedule created")

    async def _handle_system_schedule_update(self, request: web.Request, schedule_id: str) -> web.Response:
        body = await self.get_request_json(request)
        if not isinstance(body, dict):
            return self.error_response("Request body must be a JSON object", 400)
        patch_payload: Dict[str, Any] = {}
        for key in ("enabled", "params", "metadata", "cron", "interval_seconds"):
            if key in body:
                patch_payload[key] = body.get(key)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        updated = await orchestrator.patch_schedule(schedule_id, patch_payload)
        detail = await orchestrator.get_schedule_detail(str(updated.get("id") or schedule_id))
        return self.success_response(self._normalize_system_schedule_detail_payload(detail), "Schedule updated")

    async def _handle_system_schedule_delete(self, request: web.Request, schedule_id: str) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        deleted = await orchestrator.delete_schedule(schedule_id)
        if not deleted:
            return self.error_response("Schedule not found", 404)
        return self.success_response({"schedule_id": schedule_id, "deleted": True}, "Schedule deleted")

    async def _handle_system_schedule_trigger(self, request: web.Request, schedule_id: str) -> web.Response:
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.trigger_schedule(schedule_id)
        return self.success_response(payload, "Schedule trigger requested")

    async def _handle_system_schedule_toggle(self, request: web.Request, schedule_id: str) -> web.Response:
        body = {}
        if request.can_read_body:
            maybe = await self.get_request_json(request)
            if isinstance(maybe, dict):
                body = maybe
        orchestrator = self.get_app_component(request, "task_orchestrator")
        current = await orchestrator.get_schedule_detail(schedule_id)
        current_schedule = current.get("schedule") if isinstance(current.get("schedule"), dict) else {}
        enabled = body.get("enabled")
        next_enabled = bool(enabled) if isinstance(enabled, bool) else not self._coerce_bool(current_schedule.get("enabled"))
        updated = await orchestrator.patch_schedule(schedule_id, {"enabled": next_enabled})
        detail = await orchestrator.get_schedule_detail(str(updated.get("id") or schedule_id))
        return self.success_response(self._normalize_system_schedule_detail_payload(detail), "Schedule updated")

    async def _handle_system_job_lineage(self, request: web.Request, task_job_id: Optional[str] = None) -> web.Response:
        task_job_id = task_job_id or request.match_info.get("task_job_id")
        if not task_job_id:
            return self.error_response("Missing task_job_id", 400)
        orchestrator = self.get_app_component(request, "task_orchestrator")
        payload = await orchestrator.get_task_job_lineage(task_job_id)
        return self.success_response(payload)

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
        if not alert_ids:
            alert_ids = [
                str(item.get("alert_id"))
                for item in system_monitor.get_active_alerts()
                if isinstance(item, dict) and str(item.get("alert_id") or "").strip()
            ]

        acknowledged = []
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
