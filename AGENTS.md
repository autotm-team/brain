# Repository Guidelines

## Project Structure & Module Organization
- Entry points: `main.py` (runtime) and `app.py` (service app wiring).
- Core modules live at the repo root: `adapters/`, `coordinators/`, `handlers/`, `managers/`, `monitors/`, `routers/`, `routes/`, `utils/`, `validators/`.
- Dependency injection and configuration: `container.py`, `config.py`, `interfaces.py`, `models.py`.
- External dependencies or submodules: `external/` (notably `external/econdb/`).
- Tests: `tests/` with `tests/unit/` and integration-style tests like `tests/test_*`.

## Build, Test, and Development Commands
- Install deps: `pip install -r requirements.txt`.
- Run locally: `python main.py` (starts the Brain service on port 8088 by default).
- Docker: `docker-compose up -d brain` (preferred for full dependency stack).
- Run tests: `pytest` (uses `pytest-asyncio` for async tests).
- Optional quality checks: `black .`, `flake8 .`, `mypy .`.

## Coding Style & Naming Conventions
- Language: Python 3, use 4-space indentation and PEP 8 naming.
- Formatters/linters: Black, Flake8, and mypy are listed in `requirements.txt`.
- Tests follow `test_*.py` naming; fixtures live in `tests/conftest.py`.
- Do not add placeholder success handlers. If an endpoint is unsupported, return an explicit `501/NOT_IMPLEMENTED`.
- Keep handlers thin: parse request, call one service/mixin/application layer entry, return response.
- Do not use runtime `sys.path` patching to reach local dependencies; rely on installed packages or explicit test bootstrapping.
- Do not add single files above roughly 2000 lines to active request paths.

## Testing Guidelines
- Frameworks: pytest + pytest-asyncio.
- Unit tests in `tests/unit/`; integration tests at `tests/test_*`.
- Prefer descriptive test names that reflect behavior, e.g., `test_strategy_integration.py`.
- Run a focused test: `pytest tests/unit/test_<name>.py`.
- Keep async tests runnable even in lightweight local environments by using the shared `tests/conftest.py` bootstrap instead of per-file import hacks.
- Add smoke coverage for route registration or app creation when changing startup wiring.

## Commit & Pull Request Guidelines
- Commit messages in history use scoped prefixes like `feat:`, `test:`, `docker:`, `external:`, `readme:`, `init:`, `ignore:`.
- Follow the same pattern with a short, imperative summary after the prefix.
- PRs should include a clear description, linked issues (if any), and test evidence (command run + result).

## Configuration & Service Dependencies
- This service integrates with macro/portfolio/execution/flowhub and depends on Redis and TimescaleDB.
- Runtime settings are environment-variable driven via `config.py`.
- Preferred downstream URL variables are `BRAIN_MACRO_SERVICE_URL`, `BRAIN_PORTFOLIO_SERVICE_URL`, `BRAIN_EXECUTION_SERVICE_URL`, `BRAIN_FLOWHUB_SERVICE_URL`.
