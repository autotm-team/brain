#!/usr/bin/env python3
"""Cleanup legacy Redis task-control-plane keys after PostgreSQL cutover."""

from __future__ import annotations

import asyncio
import json
import os
try:
    import redis.asyncio as redis
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"redis.asyncio import failed: {exc}")


async def main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    dry_run = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes", "on"}
    client = redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
    try:
        await client.ping()
        legacy_patterns = [
            "brain:task_job:*",
            "asyncron:v2:brain:schedule:*",
            "asyncron:v2:brain:schedules",
        ]
        deleted = []
        for pattern in legacy_patterns:
            async for key in client.scan_iter(match=pattern):
                if key == "asyncron:v2:brain:schedule:dispatch":
                    continue
                deleted.append(key)
                if not dry_run:
                    await client.delete(key)
        payload = {
            "redis_url": redis_url,
            "dry_run": dry_run,
            "matched_keys": len(deleted),
            "keys": deleted[:500],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
