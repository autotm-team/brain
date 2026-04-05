import asyncio
import threading
from types import SimpleNamespace

import pytest

from coordinators.system_coordinator import SystemCoordinator
from routers.signal_router import SignalRouter


def _job_payload(task_job_id: str, status: str, result=None, message: str | None = None):
    return {
        "id": task_job_id,
        "service": "execution",
        "service_job_id": f"execution-{task_job_id}",
        "job_type": "batch_analyze" if task_job_id == "task-1" else "run_backtest",
        "status": status,
        "result": result or {},
        "message": message,
    }


class _FakeTaskOrchestrator:
    def __init__(self, *, sequences=None):
        self.calls = []
        self.get_calls = []
        self._sequences = list(sequences or [])
        self._jobs = {}

    async def create_task_job(self, service, job_type, params=None, metadata=None, service_payload=None, lineage_context=None):
        task_job_id = f"task-{len(self.calls) + 1}"
        self.calls.append(
            {
                "service": service,
                "job_type": job_type,
                "params": params or {},
                "metadata": metadata or {},
                "task_job_id": task_job_id,
            }
        )
        sequence = self._sequences[len(self.calls) - 1] if len(self._sequences) >= len(self.calls) else [
            _job_payload(task_job_id, "succeeded", {})
        ]
        self._jobs[task_job_id] = {"sequence": list(sequence), "last": _job_payload(task_job_id, "queued", {})}
        return {
            "id": task_job_id,
            "service": service,
            "service_job_id": f"{service}-job-{len(self.calls)}",
            "job_type": job_type,
            "status": "queued",
        }

    async def get_task_job(self, task_job_id):
        self.get_calls.append(task_job_id)
        state = self._jobs[task_job_id]
        if state["sequence"]:
            item = state["sequence"].pop(0)
            payload = await item() if callable(item) else item
            state["last"] = payload
            return payload
        return state["last"]


class _FakeStrategyAdapter:
    def build_strategy_analysis_params(self, portfolio_instruction, symbols, callback_url=None):
        payload = {
            "symbols": list(symbols),
            "portfolio_instruction": dict(portfolio_instruction or {}),
        }
        if callback_url:
            payload["callback_url"] = callback_url
        return payload

    def build_backtest_validation_params(self, symbols, strategy_config, callback_url=None):
        payload = {
            "symbols": list(symbols),
            "strategy_config": dict(strategy_config or {}),
        }
        if callback_url:
            payload["callback_url"] = callback_url
        return payload

    async def normalize_analysis_job_result(self, job_result):
        return list(job_result.get("analysis_results") or [])

    def normalize_backtest_job_result(self, job_result):
        return {
            "status": "success",
            "data": dict(job_result or {}),
        }


class _TimeBasedTaskOrchestrator:
    def __init__(self, *, succeed_after_seconds: float):
        self.started_at = None
        self.succeed_after_seconds = float(succeed_after_seconds)

    async def get_task_job(self, task_job_id):
        loop = asyncio.get_running_loop()
        if self.started_at is None:
            self.started_at = loop.time()
        elapsed = loop.time() - self.started_at
        if elapsed >= self.succeed_after_seconds:
            return _job_payload(task_job_id, "succeeded", {"analysis_results": []})
        return _job_payload(task_job_id, "running", {})


def _coordinator():
    coordinator = SystemCoordinator.__new__(SystemCoordinator)
    coordinator._strategy_adapter = _FakeStrategyAdapter()
    coordinator.config = SimpleNamespace(
        system_coordinator=SimpleNamespace(
            cycle_timeout=5,
            max_concurrent_cycles=3,
            strategy_analysis_timeout_seconds=1800,
            strategy_validation_timeout_seconds=1800,
        )
    )
    coordinator._active_cycles = {}
    coordinator._cycle_lock = threading.RLock()
    coordinator._is_running = True
    return coordinator


@pytest.mark.asyncio
async def test_system_coordinator_waits_for_strategy_analysis_and_returns_materialized_results():
    orchestrator = _FakeTaskOrchestrator(
        sequences=[
            [
                _job_payload("task-1", "running", {}),
                _job_payload(
                    "task-1",
                    "succeeded",
                    {
                        "analysis_results": [
                            {"symbol": "000001.SZ", "analyzer_scores": {"livermore": 0.8}},
                        ]
                    },
                ),
            ]
        ]
    )
    coordinator = _coordinator()
    coordinator._task_orchestrator = orchestrator

    result = await coordinator._execute_strategy_analysis("cycle-1", {"symbols": ["000001.SZ"]})

    assert len(orchestrator.calls) == 1
    assert orchestrator.calls[0]["job_type"] == "batch_analyze"
    assert orchestrator.get_calls == ["task-1", "task-1"]
    assert result == [{"symbol": "000001.SZ", "analyzer_scores": {"livermore": 0.8}}]


@pytest.mark.asyncio
async def test_system_coordinator_waits_for_strategy_validation_and_returns_materialized_result():
    orchestrator = _FakeTaskOrchestrator(
        sequences=[
            [
                _job_payload(
                    "task-1",
                    "succeeded",
                    {
                        "performance": {"total_return": 0.12, "max_drawdown": 0.08},
                        "config": {"start_date": "2023-01-01", "end_date": "2023-12-31"},
                    },
                )
            ]
        ]
    )
    coordinator = _coordinator()
    coordinator._task_orchestrator = orchestrator

    result = await coordinator._execute_strategy_validation(
        "cycle-2",
        [{"symbol": "000001.SZ", "analyzer_scores": {"livermore": 0.8, "multi_indicator": 0.7}}],
    )

    assert len(orchestrator.calls) == 1
    assert orchestrator.calls[0]["job_type"] == "run_backtest"
    assert result["status"] == "success"
    assert result["data"]["performance"]["total_return"] == 0.12


@pytest.mark.asyncio
async def test_coordinate_full_analysis_cycle_stays_running_until_analysis_task_finishes_and_validation_completes():
    analysis_release = asyncio.Event()

    async def _analysis_completion():
        await analysis_release.wait()
        return _job_payload(
            "task-1",
            "succeeded",
            {
                "analysis_results": [
                    {
                        "symbol": "000001.SZ",
                        "analyzer_scores": {"livermore": 0.8, "multi_indicator": 0.7},
                    }
                ]
            },
        )

    orchestrator = _FakeTaskOrchestrator(
        sequences=[
            [_analysis_completion],
            [
                _job_payload(
                    "task-2",
                    "succeeded",
                    {
                        "performance": {"total_return": 0.15, "max_drawdown": 0.05},
                        "config": {"start_date": "2023-01-01", "end_date": "2023-12-31"},
                    },
                )
            ],
        ]
    )
    coordinator = _coordinator()
    coordinator._task_orchestrator = orchestrator

    async def _macro(*args, **kwargs):
        return {"macro": "ok"}

    async def _portfolio(*args, **kwargs):
        return {"symbols": ["000001.SZ"]}

    coordinator._execute_macro_analysis = _macro
    coordinator._execute_portfolio_management = _portfolio

    cycle_task = asyncio.create_task(coordinator.coordinate_full_analysis_cycle())
    await asyncio.sleep(0.05)

    assert len(orchestrator.calls) == 1
    assert orchestrator.calls[0]["job_type"] == "batch_analyze"
    assert len(coordinator._active_cycles) == 1
    active_cycle = next(iter(coordinator._active_cycles.values()))
    assert active_cycle.status == "running"
    assert active_cycle.end_time is None

    analysis_release.set()
    cycle_result = await asyncio.wait_for(cycle_task, timeout=1.0)

    assert [call["job_type"] for call in orchestrator.calls] == ["batch_analyze", "run_backtest"]
    assert cycle_result.status == "completed"
    assert cycle_result.analysis_results == [
        {
            "symbol": "000001.SZ",
            "analyzer_scores": {"livermore": 0.8, "multi_indicator": 0.7},
        }
    ]
    assert cycle_result.validation_result["status"] == "success"
    assert cycle_result.validation_result["data"]["performance"]["total_return"] == 0.15


@pytest.mark.asyncio
async def test_signal_router_routes_strategy_work_via_task_orchestrator():
    router = SignalRouter.__new__(SignalRouter)
    router._strategy_adapter = _FakeStrategyAdapter()
    router._task_orchestrator = _FakeTaskOrchestrator()

    result = await router._route_to_strategy_layer(
        SimpleNamespace(signal_id="sig-1", data={"symbols": ["000001.SZ"]})
    )

    assert router._task_orchestrator.calls == [
        {
            "service": "execution",
            "job_type": "batch_analyze",
            "params": {
                "symbols": ["000001.SZ"],
                "portfolio_instruction": {"symbols": ["000001.SZ"]},
            },
            "metadata": {
                "control_plane_source": "signal_router",
                "signal_id": "sig-1",
                "stage": "strategy_analysis",
            },
            "task_job_id": "task-1",
        }
    ]
    assert result["mode"] == "control_plane_async"
    assert result["task_job_id"] == "task-1"


@pytest.mark.asyncio
async def test_system_coordinator_wait_uses_stage_timeout_not_cycle_timeout():
    coordinator = _coordinator()
    coordinator.config = SimpleNamespace(
        system_coordinator=SimpleNamespace(
            cycle_timeout=1,
            max_concurrent_cycles=3,
            strategy_analysis_timeout_seconds=2,
            strategy_validation_timeout_seconds=2,
        )
    )
    coordinator._task_orchestrator = _TimeBasedTaskOrchestrator(succeed_after_seconds=1.2)

    payload = await coordinator._wait_for_task_job_terminal(
        "cycle-timeout",
        "strategy_analysis",
        "task-timeout",
        poll_interval_seconds=0.1,
    )

    assert payload["status"] == "succeeded"
