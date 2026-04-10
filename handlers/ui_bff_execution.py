"""Execution-side helper endpoints extracted from the UI BFF handler."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from aiohttp import web

from task_orchestrator import UpstreamServiceError


class UIBffExecutionMixin:
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

        shell_defaults: Dict[str, Any] = {}
        try:
            shell_defaults = self._get_system_settings_service().shell_context_settings(
                self._get_system_api(),
                context=self._system_settings_context(request),
            )
        except Exception as exc:
            self.logger.warning(f"system settings unavailable for shell context: {exc}")

        watchlist_badge = 0
        try:
            watchlist_payload = await self._fetch_upstream_json(
                request,
                "execution",
                "/api/v1/ui/candidates/events",
                params={"limit": 1, "offset": 0, "status": "pending"},
            )
            watchlist_data = watchlist_payload.get("data") if isinstance(watchlist_payload, dict) else None
            watchlist_total = watchlist_data.get("total") if isinstance(watchlist_data, dict) else None
            watchlist_items = watchlist_data.get("items") if isinstance(watchlist_data, dict) and isinstance(watchlist_data.get("items"), list) else []
            watchlist_badge = self._safe_int(watchlist_total, len(watchlist_items))
        except Exception as exc:
            self.logger.warning(f"watchlist badge unavailable for shell context: {exc}")

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
                "watchlist": watchlist_badge,
            },
            "workspace": shell_defaults.get("workspace", "Alpha Lab"),
            "system_name": shell_defaults.get("system_name", "AutoTM Quant Research"),
            "locale": shell_defaults.get("locale", "zh-CN"),
            "timezone": shell_defaults.get("timezone", "Asia/Shanghai"),
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
            payload = await self._fetch_upstream_json(request, "execution", "/api/v1/ui/execution/analyzers")
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
