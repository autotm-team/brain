import asyncio
import inspect
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_ASYNCRON = PROJECT_ROOT / "external" / "asyncron"
EXTERNAL_ECONDB = PROJECT_ROOT / "external" / "econdb"

for path in (EXTERNAL_ASYNCRON, EXTERNAL_ECONDB, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def _ensure_stub(name: str) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.pro_api = lambda *args, **kwargs: None
    sys.modules[name] = module


_ensure_stub("tushare")
_ensure_stub("akshare")


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark async tests")


def pytest_pyfunc_call(pyfuncitem):
    if pyfuncitem.config.pluginmanager.hasplugin("asyncio"):
        return None
    if not inspect.iscoroutinefunction(pyfuncitem.obj):
        return None
    funcargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
    }
    asyncio.run(pyfuncitem.obj(**funcargs))
    return True
