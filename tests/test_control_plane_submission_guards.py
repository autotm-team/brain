import pytest

from adapters.execution_adapter import ExecutionAdapter
from adapters.flowhub_adapter import FlowhubAdapter
from adapters.strategy_adapter import StrategyAdapter
from exceptions import AdapterException


@pytest.mark.asyncio
async def test_execution_adapter_rejects_direct_job_submission():
    adapter = ExecutionAdapter.__new__(ExecutionAdapter)

    with pytest.raises(RuntimeError, match="TaskOrchestrator.create_task_job"):
        await adapter.trigger_batch_analysis({"symbols": ["000001.SZ"]})

    with pytest.raises(RuntimeError, match="TaskOrchestrator.create_task_job"):
        await adapter.submit_ui_job("ui_candidates_sync_from_analysis", {"as_of_date": "2026-04-01"})


@pytest.mark.asyncio
async def test_strategy_adapter_rejects_direct_job_submission():
    adapter = StrategyAdapter.__new__(StrategyAdapter)

    with pytest.raises(AdapterException, match="TaskOrchestrator.create_task_job"):
        await adapter.send_request({"type": "strategy_analysis"})

    with pytest.raises(AdapterException, match="TaskOrchestrator.create_task_job"):
        await adapter.request_backtest_validation(["000001.SZ"], {"name": "demo"})


@pytest.mark.asyncio
async def test_flowhub_adapter_rejects_direct_job_submission():
    adapter = FlowhubAdapter.__new__(FlowhubAdapter)

    with pytest.raises(AdapterException, match="TaskOrchestrator.create_task_job"):
        await adapter.create_batch_stock_data_job({"symbols": ["000001.SZ"]})

    with pytest.raises(AdapterException, match="TaskOrchestrator.create_task_job"):
        await adapter.create_macro_data_job("gdp_data", incremental=True)
