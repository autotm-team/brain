#!/usr/bin/env python3
"""Cleanup legacy Redis task-control-plane keys after PostgreSQL cutover."""

from __future__ import annotations

import asyncio
import argparse
import json
try:
    import redis.asyncio as redis
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"redis.asyncio import failed: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup legacy Redis task-control-plane keys after PostgreSQL cutover")
    parser.add_argument("--redis-url", required=True, help="Redis URL")
    parser.add_argument("--dry-run", action="store_true", help="Only print matched keys")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    redis_url = args.redis_url
    dry_run = bool(args.dry_run)
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
