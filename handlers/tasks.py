"""
定时任务处理器
"""

from aiohttp import web
from typing import Any, Dict, List

from asyncron import error_payload, response_envelope, unwrap_request_envelope
from handlers.base import BaseHandler
from task_orchestrator import UpstreamServiceError


class TaskHandler(BaseHandler):
    """定时任务处理器"""

    @staticmethod
    def _ok(data: Any, message: str = "ok", status: str = "ok", http_status: int = 200) -> web.Response:
        return web.json_response(
            response_envelope(success=True, status=status, message=message, data=data, error=None),
            status=http_status,
        )

    @staticmethod
    def _err(code: str, message: str, http_status: int = 400, details: Dict[str, Any] | None = None) -> web.Response:
        return web.json_response(
            response_envelope(
                success=False,
                status="error",
                message=message,
                data=None,
                error=error_payload(code, message, details),
            ),
            status=http_status,
        )

    async def list_tasks_overview(self, request: web.Request) -> web.Response:
        """获取跨服务任务概览（统一任务模型输出）"""
        query_params = self.get_query_params(request)
        limit = int(query_params.get('limit', 200))
        offset = int(query_params.get('offset', 0))
        service = query_params.get('service')
        status = query_params.get('status')
        compact = str(query_params.get('compact', 'false')).lower() in ('1', 'true', 'yes')

        orchestrator = self.get_app_component(request, 'task_orchestrator')
        payload = await orchestrator.list_task_jobs(service=service, status=status, limit=limit, offset=offset)
        tasks = payload.get('jobs', [])
        if compact:
            tasks = [self._compact_task(t) for t in tasks]
        return self._ok({
            'tasks': tasks,
            'total': payload.get('total', len(tasks)),
            'limit': payload.get('limit', limit),
            'offset': payload.get('offset', offset),
            'errors': payload.get('errors', []),
        })

    async def create_task_job(self, request: web.Request) -> web.Response:
        """创建统一任务"""
        try:
            raw_payload = await self.get_request_json(request)
            payload = unwrap_request_envelope(raw_payload)
            service = payload.get('service')
            job_type = payload.get('job_type')
            params = payload.get('params') if isinstance(payload.get('params'), dict) else {}
            metadata = payload.get('metadata') if isinstance(payload.get('metadata'), dict) else {}
            service_payload = payload.get('service_payload') if isinstance(payload.get('service_payload'), dict) else None
            if not service:
                return self._err("INVALID_REQUEST", "Missing required field: service", 400)
            if not job_type and not service_payload:
                return self._err("INVALID_REQUEST", "Missing required field: job_type", 400)

            orchestrator = self.get_app_component(request, 'task_orchestrator')
            created = await orchestrator.create_task_job(
                service=service,
                job_type=job_type or "unknown",
                params=params,
                metadata=metadata,
                service_payload=service_payload,
            )
            task_job_id = created.get("id")
            response_data = {
                "task_job_id": task_job_id,
                "status_url": f"/api/v1/task-jobs/{task_job_id}" if task_job_id else None,
                "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel" if task_job_id else None,
                "history_url": f"/api/v1/task-jobs/{task_job_id}/history" if task_job_id else None,
                "job": created,
            }
            return self._ok(response_data, "任务创建成功", "accepted", 202)
        except ValueError as exc:
            return self._err("INVALID_REQUEST", str(exc), 400)
        except UpstreamServiceError as exc:
            return self._err(
                "UPSTREAM_UNAVAILABLE",
                exc.error.get("message") or "Upstream request failed",
                exc.status if 400 <= exc.status < 500 else 502,
                {"upstream_status": exc.status, "upstream_service": exc.service},
            )
        except Exception as e:
            self.logger.error(f"Create task job failed: {e}")
            return self._err("INTERNAL_ERROR", "创建任务失败", 500)

    async def list_task_jobs(self, request: web.Request) -> web.Response:
        """统一任务列表"""
        try:
            query_params = self.get_query_params(request)
            service = query_params.get('service')
            status = query_params.get('status')
            limit = int(query_params.get('limit', 20))
            offset = int(query_params.get('offset', 0))
            orchestrator = self.get_app_component(request, 'task_orchestrator')
            payload = await orchestrator.list_task_jobs(service=service, status=status, limit=limit, offset=offset)
            return self._ok(payload)
        except Exception as e:
            self.logger.error(f"List task jobs failed: {e}")
            return self._err("INTERNAL_ERROR", "获取任务列表失败", 500)

    async def get_task_job(self, request: web.Request) -> web.Response:
        """统一任务详情"""
        try:
            task_job_id = self.get_path_params(request)['task_job_id']
            orchestrator = self.get_app_component(request, 'task_orchestrator')
            payload = await orchestrator.get_task_job(task_job_id)
            return self._ok(payload)
        except ValueError as exc:
            return self._err("JOB_NOT_FOUND", str(exc), 404)
        except Exception as e:
            self.logger.error(f"Get task job failed: {e}")
            return self._err("INTERNAL_ERROR", "获取任务详情失败", 500)

    async def cancel_task_job(self, request: web.Request) -> web.Response:
        """取消统一任务"""
        try:
            task_job_id = self.get_path_params(request)['task_job_id']
            orchestrator = self.get_app_component(request, 'task_orchestrator')
            payload = await orchestrator.cancel_task_job(task_job_id)
            return self._ok(payload, "任务取消成功")
        except ValueError as exc:
            return self._err("JOB_NOT_FOUND", str(exc), 404)
        except Exception as e:
            self.logger.error(f"Cancel task job failed: {e}")
            return self._err("INTERNAL_ERROR", "取消任务失败", 500)

    async def get_task_job_history(self, request: web.Request) -> web.Response:
        """统一任务历史"""
        try:
            task_job_id = self.get_path_params(request)['task_job_id']
            orchestrator = self.get_app_component(request, 'task_orchestrator')
            payload = await orchestrator.get_task_job_history(task_job_id)
            return self._ok(payload)
        except Exception as e:
            self.logger.error(f"Get task job history failed: {e}")
            return self._err("INTERNAL_ERROR", "获取任务历史失败", 500)

    async def list_task_job_types(self, request: web.Request) -> web.Response:
        """统一任务类型目录"""
        try:
            orchestrator = self.get_app_component(request, 'task_orchestrator')
            payload = await orchestrator.list_task_job_types()
            return self._ok(payload)
        except Exception as e:
            self.logger.error(f"List task job types failed: {e}")
            return self._err("INTERNAL_ERROR", "获取任务类型失败", 500)

    async def get_task_job_analytics(self, request: web.Request) -> web.Response:
        try:
            query_params = self.get_query_params(request)
            service = query_params.get("service")
            window_key = query_params.get("window_key")
            orchestrator = self.get_app_component(request, "task_orchestrator")
            payload = await orchestrator.get_task_job_analytics(service=service, window_key=window_key)
            return self._ok(payload)
        except Exception as e:
            self.logger.error(f"Get task job analytics failed: {e}")
            return self._err("INTERNAL_ERROR", "获取任务分析失败", 500)

    async def list_schedules(self, request: web.Request) -> web.Response:
        try:
            query_params = self.get_query_params(request)
            service = query_params.get("service")
            limit = int(query_params.get("limit", 20))
            offset = int(query_params.get("offset", 0))
            orchestrator = self.get_app_component(request, "task_orchestrator")
            payload = await orchestrator.list_schedules(service=service, limit=limit, offset=offset)
            return self._ok(payload)
        except ValueError as exc:
            return self._err("INVALID_REQUEST", str(exc), 400)
        except Exception as e:
            self.logger.error(f"List schedules failed: {e}")
            return self._err("INTERNAL_ERROR", "获取调度列表失败", 500)

    async def create_schedule(self, request: web.Request) -> web.Response:
        try:
            payload = await self.get_request_json(request)
            orchestrator = self.get_app_component(request, "task_orchestrator")
            schedule = await orchestrator.create_schedule(payload)
            return self._ok(schedule, "调度创建成功", "accepted", 202)
        except ValueError as exc:
            return self._err("INVALID_REQUEST", str(exc), 400)
        except Exception as e:
            self.logger.error(f"Create schedule failed: {e}")
            return self._err("INTERNAL_ERROR", "创建调度失败", 500)

    async def patch_schedule(self, request: web.Request) -> web.Response:
        try:
            schedule_id = self.get_path_params(request)['schedule_id']
            orchestrator = self.get_app_component(request, "task_orchestrator")
            data = await self.get_request_json(request)
            schedule = await orchestrator.patch_schedule(schedule_id, data)
            return self._ok(schedule, "调度更新成功")
        except ValueError as exc:
            message = str(exc)
            if message == "Schedule not found":
                return self._err("JOB_NOT_FOUND", message, 404)
            return self._err("INVALID_REQUEST", message, 400)
        except Exception as e:
            self.logger.error(f"Patch schedule failed: {e}")
            return self._err("INTERNAL_ERROR", "更新调度失败", 500)

    async def delete_schedule(self, request: web.Request) -> web.Response:
        try:
            schedule_id = self.get_path_params(request)['schedule_id']
            orchestrator = self.get_app_component(request, "task_orchestrator")
            ok = await orchestrator.delete_schedule(schedule_id)
            if not ok:
                return self._err("JOB_NOT_FOUND", "Schedule not found", 404)
            return self._ok({"schedule_id": schedule_id, "deleted": True}, "调度删除成功")
        except Exception as e:
            self.logger.error(f"Delete schedule failed: {e}")
            return self._err("INTERNAL_ERROR", "删除调度失败", 500)

    async def trigger_schedule(self, request: web.Request) -> web.Response:
        try:
            schedule_id = self.get_path_params(request)['schedule_id']
            orchestrator = self.get_app_component(request, "task_orchestrator")
            result = await orchestrator.trigger_schedule(schedule_id)
            job = result.get("job") if isinstance(result, dict) else {}
            task_job_id = job.get("id") if isinstance(job, dict) else None
            payload = {
                **(result if isinstance(result, dict) else {}),
                "task_job_id": task_job_id,
                "status_url": f"/api/v1/task-jobs/{task_job_id}" if task_job_id else None,
                "cancel_url": f"/api/v1/task-jobs/{task_job_id}/cancel" if task_job_id else None,
                "history_url": f"/api/v1/task-jobs/{task_job_id}/history" if task_job_id else None,
            }
            return self._ok(payload, "调度触发成功")
        except ValueError as exc:
            message = str(exc)
            if message == "Schedule not found":
                return self._err("JOB_NOT_FOUND", message, 404)
            return self._err("INVALID_REQUEST", message, 400)
        except Exception as e:
            self.logger.error(f"Trigger schedule failed: {e}")
            return self._err("INTERNAL_ERROR", "触发调度失败", 500)

    async def _fetch_service_json(
        self,
        request: web.Request,
        service_name: str,
        path: str,
        params: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        registry = self.get_app_component(request, 'service_registry')
        service = getattr(registry, '_services', {}).get(service_name)
        session = getattr(registry, '_session', None)
        if not service or session is None:
            raise RuntimeError(f"Service {service_name} not available")
        url = f"{service['url']}{path}"
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"{service_name} HTTP {resp.status}: {text}")
            return await resp.json()

    def _normalize_flowhub_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(task or {})
        normalized.setdefault('data_source', 'flowhub')
        normalized.setdefault('source', 'flowhub')
        return normalized

    def _compact_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        compacted = dict(task or {})
        params = compacted.get('params') if isinstance(compacted.get('params'), dict) else {}
        if params:
            params = dict(params)
            for key, value in list(params.items()):
                if isinstance(value, list) and len(value) > 50:
                    params[f"{key}_count"] = len(value)
                    params.pop(key, None)
            compacted['params'] = params
        if 'raw' in compacted:
            compacted.pop('raw', None)
        return compacted

    def _normalize_job_task(self, service: str, job: Dict[str, Any]) -> Dict[str, Any]:
        job_id = job.get('job_id') or ''
        job_type = job.get('job_type') or 'job'
        status = job.get('status') or 'failed'
        name = job.get('task_name') or job.get('job_name') or f"{service}:{job_type}"
        created_at = job.get('created_at') or job.get('started_at')
        updated_at = job.get('updated_at') or job.get('completed_at')
        success_count = job.get('success_count')
        failed_count = job.get('failed_count')
        if success_count is None:
            success_count = 1 if status == 'succeeded' else 0
        if failed_count is None:
            failed_count = 1 if status == 'failed' else 0
        return {
            'task_id': job_id,
            'name': name,
            'data_type': job_type,
            'schedule_type': 'manual',
            'schedule_value': None,
            'status': status,
            'enabled': True,
            'run_count': job.get('run_count', 0),
            'success_count': success_count,
            'failed_count': failed_count,
            'created_at': created_at,
            'updated_at': updated_at,
            'last_run_at': updated_at,
            'next_run_at': None,
            'current_job_id': job.get('current_job_id') or job_id,
            'last_job_id': job.get('last_job_id') or job_id,
            'progress': job.get('progress'),
            'data_source': service,
            'source': service,
            'raw': job
        }

    def _normalize_brain_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_id = task.get('task_id') or task.get('id') or ''
        name = task.get('name') or task_id
        enabled = task.get('enabled', True)
        status = task.get('status') or ('disabled' if not enabled else 'idle')
        return {
            'task_id': task_id,
            'name': name,
            'data_type': task.get('data_type') or 'brain_task',
            'schedule_type': 'cron',
            'schedule_value': task.get('cron'),
            'status': status,
            'enabled': enabled,
            'run_count': task.get('run_count', 0),
            'success_count': task.get('success_count', 0),
            'failed_count': task.get('failed_count', 0),
            'created_at': task.get('created_at'),
            'updated_at': task.get('updated_at'),
            'last_run_at': task.get('last_run_at'),
            'next_run_at': task.get('next_run_at'),
            'data_source': 'brain',
            'source': 'brain',
            'raw': task
        }
