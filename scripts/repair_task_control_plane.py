"""Repair Brain task projections from durable runtime task truth.

Usage:
    python scripts/repair_task_control_plane.py --apply

The script is intentionally narrow: it only updates Brain projection fields from
runtime_task_jobs and initializes the auto-chain continuation cursor to replay
recent terminal events.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from econdb import TaskRuntimeDataAPI
from econdb.database import DatabaseManager


CONTINUATION_NAME = "brain_auto_chain_v1"


def _build_api(database_url: str) -> TaskRuntimeDataAPI:
    return TaskRuntimeDataAPI(DatabaseManager(database_url=database_url))


def repair_projection(api: TaskRuntimeDataAPI, *, apply: bool) -> int:
    session = api.db_manager.Session()
    try:
        sql = """
        UPDATE brain_task_jobs b
        SET
          status = r.status,
          progress = r.progress,
          message = COALESCE(r.metadata->>'message', b.message),
          error = CASE WHEN r.error IS NULL THEN NULL ELSE jsonb_build_object('message', r.error, 'code', r.error_code) END,
          result = r.result,
          updated_at = r.updated_at,
          started_at = r.started_at,
          completed_at = r.completed_at,
          last_seen_at = NOW(),
          cancellable = r.status IN ('queued', 'running')
        FROM runtime_task_jobs r
        WHERE b.service = r.service_name
          AND b.service_job_id = r.job_id
          AND (
            b.status IS DISTINCT FROM r.status
            OR b.progress IS DISTINCT FROM r.progress
            OR b.completed_at IS DISTINCT FROM r.completed_at
          )
        """
        if not apply:
            count_sql = """
            SELECT COUNT(*)
            FROM brain_task_jobs b
            JOIN runtime_task_jobs r
              ON b.service = r.service_name
             AND b.service_job_id = r.job_id
            WHERE b.status IS DISTINCT FROM r.status
               OR b.progress IS DISTINCT FROM r.progress
               OR b.completed_at IS DISTINCT FROM r.completed_at
            """
            result = session.execute(text(count_sql))
            session.rollback()
            return int(result.scalar() or 0)
        result = session.execute(text(sql))
        session.commit()
        return int(result.rowcount or 0)
    finally:
        session.close()


def initialize_cursor(api: TaskRuntimeDataAPI, *, apply: bool, replay_days: int) -> int:
    session = api.db_manager.Session()
    try:
        since = datetime.utcnow() - timedelta(days=max(1, replay_days))
        stmt = """
        SELECT COALESCE(MIN(event_id), 0) - 1 AS cursor
        FROM runtime_task_job_events
        WHERE event_type IN ('succeeded', 'failed', 'cancelled')
          AND created_at >= :since
        """
        cursor = int(session.execute(text(stmt), {"since": since}).scalar() or 0)
        cursor = max(cursor, 0)
    finally:
        session.close()
    if apply:
        api.upsert_brain_task_continuation_cursor(CONTINUATION_NAME, last_event_id=cursor)
    return cursor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Persist repairs")
    parser.add_argument(
        "--database-url",
        default="postgresql://postgres:postgres@localhost:5432/stock_data",
        help="PostgreSQL database URL",
    )
    parser.add_argument("--replay-days", type=int, default=7)
    args = parser.parse_args()

    api = _build_api(args.database_url)
    changed = repair_projection(api, apply=args.apply)
    cursor = initialize_cursor(api, apply=args.apply, replay_days=args.replay_days)
    mode = "applied" if args.apply else "dry-run"
    print({"mode": mode, "projection_rows": changed, "continuation_cursor": cursor})


if __name__ == "__main__":
    main()
