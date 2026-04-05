"""
Execution服务适配器

负责与Execution服务（股票技术分析服务）的通信
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from config import IntegrationConfig
from adapters.http_client import HttpClient

logger = logging.getLogger(__name__)


class ExecutionAdapter:
    """Execution服务适配器"""

    @staticmethod
    def _raise_control_plane_submission_disabled(job_type: str) -> None:
        raise RuntimeError(
            "Brain control plane must create execution jobs via "
            f"TaskOrchestrator.create_task_job(...); direct execution submission is disabled for {job_type}"
        )

    def __init__(self, config: IntegrationConfig):
        """初始化适配器

        Args:
            config: 集成配置对象
        """
        self.config = config
        self._http_client = HttpClient('execution', config)
        logger.info("ExecutionAdapter initialized with HTTP client")
    
    async def trigger_batch_analysis(self, analysis_params: Dict[str, Any]) -> Dict[str, Any]:
        """触发批量股票分析

        支持两种模式：
        1. 全量分析：不传入symbols参数（或传入None），Execution服务会自动从数据库获取所有股票
        2. 指定分析：传入symbols列表，只分析指定的股票

        Args:
            analysis_params: 分析参数
                - symbols: List[str] | None - 股票代码列表（可选，不传入则分析所有股票）
                - analyzers: List[str] - 分析器列表（如['livermore', 'multi_indicator']或['all']）
                - config: Dict - 配置参数
                    - parallel_limit: int - 并发限制
                    - save_all_dates: bool - 是否保存所有历史日期的分析结果
                    - cache_enabled: bool - 是否启用缓存

        Returns:
            Dict[str, Any]: 统一 job 创建结果
                - job_id: str - 任务ID
                - status: str - 任务状态
                - message: str - 消息

        Raises:
            Exception: 调用失败时抛出异常
        """
        self._raise_control_plane_submission_disabled("batch_analyze")

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """获取执行任务状态（统一Job接口）"""
        try:
            response = await self._http_client.get(f'/api/v1/jobs/{job_id}')
            return self._unwrap_job(response)
        except Exception as e:
            logger.error(f"Failed to get execution job status: {e}")
            raise

    async def list_jobs(self, status: str = None, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """获取执行任务列表（统一Job接口）"""
        try:
            params = {'limit': limit, 'offset': offset}
            if status:
                params['status'] = status
            response = await self._http_client.get('/api/v1/jobs', params)
            return self._unwrap_jobs_response(response)
        except Exception as e:
            logger.error(f"Failed to list execution jobs: {e}")
            raise

    async def submit_ui_job(self, job_type: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """提交 execution UI 任务（统一 jobs 契约）。"""
        self._raise_control_plane_submission_disabled(job_type)

    async def sync_candidates_from_analysis(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """触发“分析结果 -> 候选池”同步任务。"""
        return await self.submit_ui_job("ui_candidates_sync_from_analysis", params=params or {})

    @staticmethod
    def _unwrap_job(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                if isinstance(data.get("job"), dict):
                    return data["job"]
                return data
            if isinstance(payload.get("job"), dict):
                return payload["job"]
            return payload
        return {}

    @classmethod
    def _unwrap_jobs_response(cls, payload: Any) -> Dict[str, Any]:
        body = cls._unwrap_job(payload)
        if isinstance(body.get("jobs"), list):
            return body
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict) and isinstance(data.get("jobs"), list):
                return data
            if isinstance(payload.get("jobs"), list):
                return payload
        return {"jobs": [], "total": 0, "limit": 0, "offset": 0}

    async def wait_for_job_completion(
        self,
        job_id: str,
        timeout: int = 43200,
        poll_interval: int = 15
    ) -> Dict[str, Any]:
        """等待执行任务完成"""
        end_time = time.time() + timeout
        while time.time() < end_time:
            job = await self.get_job_status(job_id)
            status = (job.get('status') or '').lower()
            if status in {'completed', 'succeeded'}:
                return job
            if status in {'failed', 'cancelled', 'canceled'}:
                raise RuntimeError(f"Execution job {job_id} failed with status={status}")
            await asyncio.sleep(poll_interval)
        raise TimeoutError(f"Execution job {job_id} timeout after {timeout}s")
    
    async def analyze_single(self, symbol: str, analyzers: List[str] = None, 
                            config: Dict[str, Any] = None) -> Dict[str, Any]:
        """分析单个股票
        
        Args:
            symbol: 股票代码
            analyzers: 分析器列表
            config: 配置参数
        
        Returns:
            Dict[str, Any]: 分析结果
        
        Raises:
            Exception: 调用失败时抛出异常
        """
        try:
            request_body = {
                'symbol': symbol,
                'analyzers': analyzers or ['all'],
                'config': config or {}
            }
            
            response = await self._http_client.post('analyze/single', request_body)
            return response.get('data', {})
            
        except Exception as e:
            logger.error(f"Failed to analyze symbol {symbol}: {e}")
            raise
    
    async def get_analysis_history(self, **params) -> Dict[str, Any]:
        """获取分析历史（透传所有筛选参数）"""
        try:
            response = await self._http_client.get('ui/execution/history', params)
            return response
        except Exception as e:
            logger.error(f"Failed to get analysis history: {e}")
            raise
    
    async def health_check(self) -> bool:
        """健康检查
        
        Returns:
            bool: 服务是否健康
        """
        try:
            response = await self._http_client.get('health')
            return response.get('status') == 'healthy'
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
