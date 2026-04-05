"""System data endpoints extracted from the UI BFF handler."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from aiohttp import web

from task_orchestrator import UpstreamServiceError

try:
    from econdb import SystemSettingDTO  # type: ignore
except Exception:  # pragma: no cover - used only in full runtime
    SystemSettingDTO = None  # type: ignore[assignment]


class UIBffSystemDataMixin:
    @staticmethod
    def _to_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None

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
        if SystemSettingDTO is None:
            return self.error_response("SystemSettingDTO unavailable in current econdb baseline", 500)

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
