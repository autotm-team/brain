"""
定时任务管理路由
"""

from aiohttp import web
from aiohttp_cors import CorsConfig

from handlers.tasks import TaskHandler


def setup_task_routes(app: web.Application, cors: CorsConfig = None):
    """设置定时任务管理路由
    
    Args:
        app: aiohttp应用实例
        cors: CORS配置对象
    """
    
    # 创建处理器实例
    task_handler = TaskHandler()
    
    # 存储到应用上下文
    app['task_handler'] = task_handler
    
    # 统一任务控制面（前端唯一入口）
    route = app.router.add_post('/api/v1/task-jobs', task_handler.create_task_job)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/task-jobs', task_handler.list_task_jobs)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/task-jobs/types', task_handler.list_task_job_types)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/task-jobs/{task_job_id}', task_handler.get_task_job)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/task-jobs/{task_job_id}/cancel', task_handler.cancel_task_job)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/task-jobs/{task_job_id}/history', task_handler.get_task_job_history)
    if cors:
        cors.add(route)

    # 统一调度接口
    route = app.router.add_post('/api/v1/schedules', task_handler.create_schedule)
    if cors:
        cors.add(route)
    route = app.router.add_get('/api/v1/schedules', task_handler.list_schedules)
    if cors:
        cors.add(route)
    route = app.router.add_patch('/api/v1/schedules/{schedule_id}', task_handler.patch_schedule)
    if cors:
        cors.add(route)
    route = app.router.add_delete('/api/v1/schedules/{schedule_id}', task_handler.delete_schedule)
    if cors:
        cors.add(route)
    route = app.router.add_post('/api/v1/schedules/{schedule_id}/trigger', task_handler.trigger_schedule)
    if cors:
        cors.add(route)
