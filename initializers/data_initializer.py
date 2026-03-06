"""
Data Initialization Coordinator for brain service

- Waits for dependencies (Flowhub) to be healthy
- Coordinates phased data initialization jobs via FlowhubAdapter
- Persists init state for progress tracking and monitoring (NOT for idempotency)
- Every startup triggers data fetch; Flowhub decides incremental fetch range based on DB state
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    from ..config import IntegrationConfig
    from ..adapters.flowhub_adapter import FlowhubAdapter
except Exception:
    from config import IntegrationConfig
    from adapters.flowhub_adapter import FlowhubAdapter

logger = logging.getLogger(__name__)

# Persist init state under application data directory (writable by 'brain' user)
STATE_PATH = "data/brain/init_state.json"


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


@dataclass
class InitPhaseState:
    name: str
    status: str = "pending"  # pending|running|completed|failed|skipped
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    job_ids: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class InitState:
    """初始化状态

    注意：completed_at 仅用于记录最后一次成功执行的时间，不作为跳过标记。
    每次 Brain 启动都会触发数据拉取，由 Flowhub 根据数据库状态决定增量拉取范围。
    """
    version: str = "1.0"
    started_at: Optional[str] = None  # 当前执行周期的开始时间
    completed_at: Optional[str] = None  # 最后一次成功完成的时间（仅用于监控）
    last_updated: Optional[str] = None  # 状态文件最后更新时间
    phases: Dict[str, InitPhaseState] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "last_updated": self.last_updated,
            "phases": {k: vars(v) for k, v in self.phases.items()},
        }

    @staticmethod
    def from_file(path: str) -> "InitState":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            phases = {k: InitPhaseState(**v) for k, v in (data.get("phases") or {}).items()}
            return InitState(
                version=data.get("version", "1.0"),
                started_at=data.get("started_at"),
                completed_at=data.get("completed_at"),
                last_updated=data.get("last_updated"),
                phases=phases,
            )
        except FileNotFoundError:
            return InitState()
        except Exception as e:
            logger.warning(f"Failed to read init state: {e}")
            return InitState()

    def save(self, path: str) -> None:
        _ensure_dir(path)
        self.last_updated = _now_iso()
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)


class DataInitializationCoordinator:
    def __init__(self, config: IntegrationConfig):
        self.config = config
        self.state_path = STATE_PATH
        self.state = InitState.from_file(self.state_path)
        # define phases
        for name in ("stock_meta", "macro_core", "equities", "macro_rest"):
            if name not in self.state.phases:
                self.state.phases[name] = InitPhaseState(name=name)
        self.state.save(self.state_path)

    async def run(self) -> None:
        """Run initialization in background (non-blocking for app).

        每次启动都会执行数据拉取，由 Flowhub 根据数据库状态决定增量拉取范围。
        不再使用 completed_at 作为跳过标记，而是记录最后执行时间用于监控。
        """
        logger.info("DataInitializationCoordinator started")

        # 记录本次执行的开始时间（不再检查 completed_at）
        current_run_start = _now_iso()
        logger.info(f"Starting data initialization run at {current_run_start}")

        # 如果有上次执行记录，记录日志
        if self.state.completed_at:
            logger.info(f"Last initialization completed at {self.state.completed_at}")

        # 重置状态为新的执行周期
        self.state.started_at = current_run_start
        self.state.completed_at = None  # 清除完成标记，表示正在执行
        self.state.save(self.state_path)

        try:
            await self._wait_dependencies()
            # phases in order
            await self._run_phase("stock_meta", self._phase_stock_meta)
            await self._run_phase("macro_core", self._phase_macro_core)
            await self._run_phase("equities", self._phase_equities)
            await self._run_phase("macro_rest", self._phase_macro_rest)

            # 记录本次执行完成时间（用于监控，不作为跳过标记）
            self.state.completed_at = _now_iso()
            self.state.save(self.state_path)
            logger.info(f"Data initialization completed at {self.state.completed_at}")
            logger.info("Next startup will trigger data fetch again (incremental by Flowhub)")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            # 保存失败状态，但不设置 completed_at
            self.state.save(self.state_path)
            logger.info("Initialization failed, will retry on next startup")

    async def _wait_dependencies(self) -> None:
        deps = getattr(self.config.service, "init_wait_dependencies", ["flowhub"]) or ["flowhub"]
        if "flowhub" not in deps:
            return
        retry_cfg = getattr(self.config.service, "init_retry", {}) or {}
        max_retries = int(retry_cfg.get("max_retries", 10))
        backoff = retry_cfg.get("backoff", [1, 2, 3, 5, 8, 13])
        timeout_total = int(retry_cfg.get("timeout", 300))

        attempt = 0
        start_ts = datetime.utcnow()
        while True:
            attempt += 1
            try:
                adapter = FlowhubAdapter(self.config)
                connected = await adapter.connect_to_system()
                await adapter.disconnect_from_system()
                if connected:
                    logger.info("Flowhub dependency is healthy")
                    return
            except Exception as e:
                logger.warning(f"Flowhub health not ready (attempt {attempt}): {e}")

            if attempt >= max_retries:
                elapsed = (datetime.utcnow() - start_ts).total_seconds()
                if elapsed >= timeout_total:
                    logger.error("Dependency wait timed out, proceeding in degraded mode")
                    return
                # else continue with backoff cycles
                attempt = 0

            delay = backoff[min(len(backoff) - 1, attempt - 1)]
            await asyncio.sleep(delay)

    async def _run_phase(self, name: str, fn) -> None:
        """执行初始化阶段

        每次启动都会重新执行所有阶段，不再跳过已完成的阶段。
        Flowhub 会根据数据库状态自动决定增量拉取范围。
        """
        phase = self.state.phases[name]

        # 记录上次执行状态（用于监控）
        if phase.status in ("completed", "skipped"):
            logger.info(f"Phase {name} was previously {phase.status}, re-running for incremental update")

        # 重置阶段状态为运行中
        phase.status = "running"
        phase.started_at = _now_iso()
        phase.job_ids = []  # 清空旧的 job_ids
        phase.errors = []   # 清空旧的错误记录
        self.state.save(self.state_path)

        try:
            jobs = await fn()
            if jobs:
                phase.job_ids.extend(jobs)
            phase.status = "completed"
            phase.completed_at = _now_iso()
            self.state.save(self.state_path)
            logger.info(f"Phase {name} completed with {len(jobs or [])} jobs")
        except Exception as e:
            phase.status = "failed"
            phase.errors.append(str(e))
            self.state.save(self.state_path)
            logger.error(f"Phase {name} failed: {e}")
            # do not raise to keep app running

    async def _wait_job_started(
        self,
        adapter: FlowhubAdapter,
        job_id: str,
        *,
        timeout_seconds: int = 60,
        poll_interval_seconds: int = 2,
    ) -> str:
        if not job_id:
            return "unknown"
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        last_status = "unknown"
        while asyncio.get_running_loop().time() < deadline:
            try:
                status_resp = await adapter.get_job_status(job_id)
                last_status = str(status_resp.get("status") or "unknown").lower()
                if last_status in {"running", "succeeded", "failed", "cancelled", "canceled"}:
                    return last_status
            except Exception as e:
                logger.warning(f"Wait job started failed for {job_id}: {e}")
            await asyncio.sleep(poll_interval_seconds)
        return last_status

    # ===== Phases =====
    async def _phase_stock_meta(self) -> List[str]:
        # 空库首启必须直接提交基础元数据任务，不能依赖后续 schedule bootstrap。
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            # 1. 股票基础信息
            req_stock_basic = {"update_mode": "incremental"}
            resp_stock_basic = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_stock_basic, "data_type": "stock_basic_data"},
            })
            jobs.append(resp_stock_basic.get("job_id", ""))

            # 2. 交易日历
            req_trade_calendar = {"exchange": "SSE", "update_mode": "incremental"}
            resp_trade_calendar = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_trade_calendar, "data_type": "trade_calendar_data"},
            })
            jobs.append(resp_trade_calendar.get("job_id", ""))

            # 3. 申万行业分类及成分映射
            req_sw_industry = {"src": "SW2021", "update_mode": "incremental", "include_members": True}
            resp_sw_industry = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_sw_industry, "data_type": "sw_industry_data"},
            })
            jobs.append(resp_sw_industry.get("job_id", ""))

            # 4. 指数基础信息
            req_index_info = {"update_mode": "incremental"}
            resp_index_info = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_index_info, "data_type": "index_info"},
            })
            jobs.append(resp_index_info.get("job_id", ""))

            # 5. 指数日线
            req_index_daily = {"update_mode": "incremental"}
            resp_index_daily = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_index_daily, "data_type": "index_daily_data"},
            })
            jobs.append(resp_index_daily.get("job_id", ""))

            # 6. 指数成分股
            req_index_components = {
                "update_mode": "snapshot",
                "index_codes": ["000300.SH", "000905.SH", "000852.SH", "000001.SH", "399001.SZ", "399006.SZ"],
            }
            resp_index_components = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_index_components, "data_type": "index_components"},
            })
            jobs.append(resp_index_components.get("job_id", ""))

            # 7. 行业板块数据
            req_industry_board = {"source": "ths", "update_mode": "full_update"}
            resp_industry_board = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_industry_board, "data_type": "industry_board"},
            })
            jobs.append(resp_industry_board.get("job_id", ""))

            # 8. 概念板块数据
            req_concept_board = {"source": "ths", "update_mode": "full_update"}
            resp_concept_board = await adapter.send_request({
                "method": "POST",
                "endpoint": "/api/v1/jobs",
                "payload": {**req_concept_board, "data_type": "concept_board"},
            })
            jobs.append(resp_concept_board.get("job_id", ""))

            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_macro_core(self) -> List[str]:
        """核心宏观数据初始化阶段

        注意：不再传递 max_history 或任何日期范围参数。
        Flowhub 会根据数据库状态自动确定抓取范围。
        """
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            stock_index_resp = await adapter.create_macro_data_job("stock_index_data", incremental=True)
            stock_index_job_id = stock_index_resp.get("job_id", "")
            if stock_index_job_id:
                jobs.append(stock_index_job_id)
                stock_index_status = await self._wait_job_started(adapter, stock_index_job_id, timeout_seconds=90)
                logger.info(
                    "stock_index_data bootstrap job status after warmup: %s (job_id=%s)",
                    stock_index_status,
                    stock_index_job_id,
                )

            concurrency = int(getattr(self.config.service, "init_concurrency", 2) or 2)
            sem = asyncio.Semaphore(concurrency)

            async def _create(dt: str, **kwargs):
                async with sem:
                    # 只传递 incremental=True，不传递 max_history 或日期范围参数
                    resp = await adapter.create_macro_data_job(dt, incremental=True, **kwargs)
                    jobs.append(resp.get("job_id", ""))

            # 核心宏观（月度+日度）：价格指数、货币供应、利率、股指、资金流、商品价格
            # 注意：exchange-rate-data 目前未在 flowhub 路由中实现，先移除避免 405
            tasks = [
                _create("price_index_data"),
                _create("money_supply_data"),
                _create("interest_rate_data"),
                # 市场资金流数据：明确指定所有类型（MARGIN, NORTHBOUND, TURNOVER）
                _create("market_flow_data", flow_types=["MARGIN", "NORTHBOUND", "TURNOVER"]),
                _create("commodity_price_data"),
            ]
            await asyncio.gather(*tasks)
            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_equities(self) -> List[str]:
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            basic_req = {"incremental": True}
            basic_resp = await adapter.create_batch_basic_data_job(basic_req)
            jobs.append(basic_resp.get("job_id", ""))

            # 股票日K（增量/全量由 flowhub 决定）
            # 统一任务接口：POST /api/v1/jobs + data_type=batch_daily_ohlc
            req = {"incremental": True}
            resp = await adapter.create_batch_stock_data_job(req)
            jobs.append(resp.get("job_id", ""))
            return jobs
        finally:
            await adapter.disconnect_from_system()

    async def _phase_macro_rest(self) -> List[str]:
        """其他宏观数据初始化阶段

        注意：不再传递 max_history 或任何日期范围参数。
        Flowhub 会根据数据库状态自动确定抓取范围。
        """
        jobs: List[str] = []
        adapter = FlowhubAdapter(self.config)
        await adapter.connect_to_system()
        try:
            concurrency = int(getattr(self.config.service, "init_concurrency", 2) or 2)
            sem = asyncio.Semaphore(concurrency)

            async def _create(dt: str, **kwargs):
                async with sem:
                    # 只传递 incremental=True，不传递 max_history 或日期范围参数
                    resp = await adapter.create_macro_data_job(dt, incremental=True, **kwargs)
                    jobs.append(resp.get("job_id", ""))
            # 其他宏观：社融、投资、工业、情绪、库存周期、GDP、创新、人口
            tasks = [
                _create("social_financing_data"),
                _create("investment_data"),
                _create("industrial_data"),
                _create("sentiment_index_data"),
                _create("inventory_cycle_data"),
                _create("gdp_data"),
                _create("innovation_data"),
                _create("demographic_data"),
            ]
            await asyncio.gather(*tasks)
            return jobs
        finally:
            await adapter.disconnect_from_system()
