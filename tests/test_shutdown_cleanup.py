import asyncio

from coordinators.system_coordinator import SystemCoordinator
from routers.signal_router import SignalRouter


class _DummyAdapter:
    def __init__(self):
        self.disconnected = False

    async def disconnect_from_system(self):
        self.disconnected = True
        return True


class _DummyComponent:
    async def stop(self):
        return True


class _DummyExecutor:
    def __init__(self):
        self.shutdown_called = False

    def shutdown(self, wait=True):
        self.shutdown_called = True


def test_signal_router_stop_disconnects_adapters():
    asyncio.run(_test_signal_router_stop_disconnects_adapters())


async def _test_signal_router_stop_disconnects_adapters():
    router = SignalRouter.__new__(SignalRouter)
    router._is_running = True
    router._monitoring_task = None
    router._cleanup_task = None
    router._conflict_resolver = _DummyComponent()
    router._signal_processor = _DummyComponent()
    router._strategy_adapter = _DummyAdapter()
    router._portfolio_adapter = _DummyAdapter()

    strategy = router._strategy_adapter
    portfolio = router._portfolio_adapter
    result = await router.stop()

    assert result is True
    assert strategy.disconnected is True
    assert portfolio.disconnected is True
    assert router._strategy_adapter is None
    assert router._portfolio_adapter is None


def test_system_coordinator_stop_disconnects_adapters_after_shutdown_failure():
    asyncio.run(_test_system_coordinator_stop_disconnects_adapters_after_shutdown_failure())


async def _test_system_coordinator_stop_disconnects_adapters_after_shutdown_failure():
    coordinator = SystemCoordinator.__new__(SystemCoordinator)
    coordinator._is_running = True
    coordinator._monitoring_task = None
    coordinator._executor = _DummyExecutor()
    coordinator._data_flow_manager = None
    coordinator._macro_adapter = _DummyAdapter()
    coordinator._portfolio_adapter = _DummyAdapter()
    coordinator._strategy_adapter = _DummyAdapter()

    async def wait_cycles():
        return None

    async def fail_shutdown():
        raise RuntimeError("shutdown failed")

    coordinator._wait_for_active_cycles = wait_cycles
    coordinator.coordinate_system_shutdown = fail_shutdown
    strategy = coordinator._strategy_adapter
    macro = coordinator._macro_adapter
    portfolio = coordinator._portfolio_adapter

    result = await coordinator.stop()

    assert result is False
    assert strategy.disconnected is True
    assert macro.disconnected is True
    assert portfolio.disconnected is True
    assert coordinator._strategy_adapter is None
    assert coordinator._macro_adapter is None
    assert coordinator._portfolio_adapter is None
