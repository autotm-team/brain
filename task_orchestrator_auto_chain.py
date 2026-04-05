"""Auto-chain and recovery helpers extracted from the task orchestrator."""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class TaskOrchestratorAutoChainMixin:
    async def _maybe_trigger_followups(self, task_job_id: str, normalized: Dict[str, Any]) -> None:
        status = str(normalized.get("status") or "")
        service = str(normalized.get("service") or "").strip().lower()
        job_type = str(normalized.get("job_type") or "").strip()
        metadata = normalized.get("metadata") if isinstance(normalized.get("metadata"), dict) else {}

        if bool(metadata.get("task_probe_disable_auto_chain")):
            return

        if status in {"failed", "cancelled"}:
            try:
                await self._maybe_release_failed_chain_locks(task_job_id, normalized)
            except Exception as exc:
                logger.warning("Failed to release chain locks for %s: %s", task_job_id, exc)
            return

        if status != "succeeded":
            return
        try:
            if service == "flowhub" and job_type == "stock_basic_data":
                await self._maybe_trigger_stock_daily_fetches(task_job_id, normalized)
            elif service == "flowhub" and job_type in {"batch_daily_ohlc", "batch_daily_basic"}:
                await self._maybe_trigger_daily_analysis(task_job_id, normalized)
            elif service == "execution" and job_type == "batch_analyze":
                await self._maybe_trigger_candidate_sync_incremental(
                    task_job_id,
                    normalized,
                    source_job_type="batch_analyze",
                    repair_window_days=7,
                )
                await self._try_converge_snapshot_prereq("batch_analyze", task_job_id, normalized)
            elif service == "execution" and job_type == "analysis_backfill":
                await self._maybe_trigger_candidate_sync_incremental(
                    task_job_id,
                    normalized,
                    source_job_type="analysis_backfill",
                    repair_window_days=30,
                )
            elif service == "flowhub" and job_type == "index_daily_data":
                await self._try_converge_snapshot_prereq("index_daily", task_job_id, normalized)
            elif service == "flowhub" and job_type == "industry_board":
                await self._try_converge_snapshot_prereq("industry_board", task_job_id, normalized)
        except Exception as exc:
            await self._append_history(task_job_id, "auto_chain_error", normalized)
            normalized_metadata = normalized.get("metadata") if isinstance(normalized.get("metadata"), dict) else {}
            normalized_metadata["auto_chain_error"] = str(exc)
            await self._update_task_record_metadata(task_job_id, normalized_metadata)

    async def _maybe_release_failed_chain_locks(self, task_job_id: str, normalized: Dict[str, Any]) -> None:
        service = str(normalized.get("service") or "").strip().lower()
        job_type = str(normalized.get("job_type") or "").strip()
        metadata = normalized.get("metadata") if isinstance(normalized.get("metadata"), dict) else {}
        trade_date = str(metadata.get("auto_chain_trade_date") or "").strip()

        if service == "flowhub" and job_type in {"batch_daily_ohlc", "batch_daily_basic"}:
            parent_task_job_id = str(metadata.get("auto_chain_parent_task_job_id") or "").strip()
            if parent_task_job_id:
                claim_key = self._auto_chain_claim_key("stock_daily_fetch", parent_task_job_id)
                released = await self._release_auto_chain(claim_key)
                if released:
                    await self._append_history(task_job_id, "auto_chain_lock_released", normalized)
                    logger.info(
                        "Released stock_daily_fetch chain lock for parent task %s after %s failure (task_job_id=%s)",
                        parent_task_job_id, job_type, task_job_id,
                    )
                    parent_view = await self._build_local_task_view(parent_task_job_id)
                    if isinstance(parent_view, dict):
                        await self._maybe_trigger_stock_daily_fetches(parent_task_job_id, parent_view)
            return

        if service == "macro" and job_type == "ui_snapshot_refresh" and trade_date:
            released = await self._release_auto_chain(f"snapshot_refresh:{trade_date}")
            if released:
                await self._append_history(task_job_id, "auto_chain_lock_released", normalized)
                logger.info(
                    "Released snapshot_refresh chain lock for trade_date=%s after failure (task_job_id=%s)",
                    trade_date, task_job_id,
                )
            return

        if service == "execution" and job_type == "batch_analyze" and trade_date:
            record = await self._load_task_record(task_job_id)
            source_params = record.get("params") if isinstance(record, dict) and isinstance(record.get("params"), dict) else {}
            scope_signature = self._scope_signature(source_params)
            claim_key = self._auto_chain_claim_key("daily_analysis", trade_date, scope_signature)
            released = await self._release_auto_chain(claim_key)
            if released:
                await self._append_history(task_job_id, "auto_chain_lock_released", normalized)
                logger.info(
                    "Released daily_analysis chain lock for trade_date=%s after batch_analyze failure (task_job_id=%s)",
                    trade_date, task_job_id,
                )
                await self._retry_failed_chain_task(task_job_id, record, normalized, "batch_analyze")

        elif service == "execution" and job_type == "ui_candidates_sync_from_analysis" and trade_date:
            record = await self._load_task_record(task_job_id)
            released = await self._release_auto_chain(f"candidate_sync:{trade_date}")
            if released:
                await self._append_history(task_job_id, "auto_chain_lock_released", normalized)
                logger.info(
                    "Released candidate_sync chain lock for trade_date=%s after failure (task_job_id=%s)",
                    trade_date, task_job_id,
                )
                await self._retry_failed_chain_task(task_job_id, record, normalized, "ui_candidates_sync_from_analysis")
        elif service == "execution" and job_type == "ui_candidates_sync_incremental":
            record = await self._load_task_record(task_job_id)
            claim_key = str(metadata.get("auto_chain_claim_key") or "").strip()
            if claim_key:
                released = await self._release_auto_chain(claim_key)
                if released:
                    await self._append_history(task_job_id, "auto_chain_lock_released", normalized)
                    logger.info(
                        "Released candidate_sync_incremental chain lock '%s' after failure (task_job_id=%s)",
                        claim_key, task_job_id,
                    )
                    await self._retry_failed_chain_task(task_job_id, record, normalized, "ui_candidates_sync_incremental")

    async def _retry_failed_chain_task(
        self,
        failed_task_job_id: str,
        record: Optional[Dict[str, Any]],
        normalized: Dict[str, Any],
        job_type: str,
    ) -> None:
        if not isinstance(record, dict):
            logger.warning("Cannot retry chain task %s: no record found", failed_task_job_id)
            return

        service = str(record.get("service") or "").strip()
        params = record.get("params") if isinstance(record.get("params"), dict) else {}
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}

        retry_count = int(metadata.get("auto_chain_retry_count", 0)) + 1
        max_retries = 3
        if retry_count > max_retries:
            logger.warning(
                "Skipping auto-retry for %s: exceeded max retries (%d/%d), task_job_id=%s",
                job_type, retry_count, max_retries, failed_task_job_id,
            )
            return

        retry_metadata = dict(metadata)
        retry_metadata["auto_chain_retry_count"] = retry_count
        retry_metadata["auto_chain_retry_from"] = failed_task_job_id
        retry_metadata["auto_chain_retry_reason"] = str(normalized.get("status", "failed"))

        try:
            created = await self.create_task_job(
                service=service,
                job_type=job_type,
                params=params,
                metadata=retry_metadata,
            )
            new_task_job_id = created.get("id", "")
            logger.info(
                "Auto-retried failed chain task %s → new task %s (retry %d/%d)",
                failed_task_job_id, new_task_job_id, retry_count, max_retries,
            )
            await self._append_history(
                failed_task_job_id,
                "auto_chain_retried",
                normalized,
                new_task_job_id=new_task_job_id,
                retry_count=retry_count,
            )
        except Exception as exc:
            logger.error("Failed to auto-retry chain task %s: %s", failed_task_job_id, exc)

    @staticmethod
    def _extract_daily_fetch_trade_date(normalized: Dict[str, Any]) -> Optional[str]:
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else {}
        for key in ("latest_trade_date", "trade_date", "target_trade_date", "end_date"):
            value = str(result.get(key) or "").strip()
            if value:
                return value
        data_summary = result.get("data_summary") if isinstance(result.get("data_summary"), dict) else {}
        value = str(data_summary.get("trade_date") or "").strip()
        return value or None

    async def _maybe_trigger_stock_daily_fetches(self, task_job_id: str, normalized: Dict[str, Any]) -> None:
        metadata = normalized.get("metadata") if isinstance(normalized.get("metadata"), dict) else {}
        if str(metadata.get("bootstrap_source") or "").strip().lower() == "brain_initializer":
            return
        if not self._job_result_ready_for_auto_chain(normalized):
            return
        claim_key = self._auto_chain_claim_key("stock_daily_fetch", task_job_id)
        if not await self._claim_auto_chain(claim_key):
            logger.warning(
                "Auto-chain claim '%s' already taken, skipping batch_daily trigger for task_job_id=%s",
                claim_key, task_job_id,
            )
            return
        ohlc_created = await self.create_task_job(
            service="flowhub",
            job_type="batch_daily_ohlc",
            params={"incremental": True},
            metadata={
                "auto_chain_source": "stock_basic_data",
                "auto_chain_parent_task_job_id": task_job_id,
            },
            lineage_context=await self._build_child_lineage_context(task_job_id),
        )
        basic_created = await self.create_task_job(
            service="flowhub",
            job_type="batch_daily_basic",
            params={"incremental": True},
            metadata={
                "auto_chain_source": "stock_basic_data",
                "auto_chain_parent_task_job_id": task_job_id,
            },
            lineage_context=await self._build_child_lineage_context(task_job_id),
        )
        await self._update_task_record_metadata(
            task_job_id,
            {
                "followup_flowhub_ohlc_task_job_id": ohlc_created.get("id"),
                "followup_flowhub_basic_task_job_id": basic_created.get("id"),
            },
        )
        await self._append_history(task_job_id, "auto_chain_created", normalized)

    async def _maybe_trigger_daily_analysis(self, task_job_id: str, normalized: Dict[str, Any]) -> None:
        ready, blocked_reason, latest_ohlc, latest_basic = await self._evaluate_daily_analysis_prereqs(
            source_task_job_id=task_job_id,
        )
        if not ready:
            await self._update_task_record_metadata(
                task_job_id,
                {
                    "auto_chain_blocked_reason": blocked_reason,
                    "auto_chain_latest_ohlc_trade_date": latest_ohlc,
                    "auto_chain_latest_basic_trade_date": latest_basic,
                },
            )
            await self._append_history(task_job_id, "auto_chain_blocked", normalized)
            return
        source_record = await self._load_task_record(task_job_id) or {}
        source_params = source_record.get("params") if isinstance(source_record.get("params"), dict) else {}
        scope_signature = self._scope_signature(source_params)
        claim_key = self._auto_chain_claim_key("daily_analysis", latest_ohlc, scope_signature)
        if not await self._claim_auto_chain(claim_key):
            logger.warning(
                "Auto-chain claim '%s' already taken, skipping batch_analyze trigger for trade_date=%s",
                claim_key, latest_ohlc,
            )
            return
        try:
            redis = self._redis()
            if redis:
                running_holder = await redis.get("execution:batch_analyze:running_lock")
                if running_holder:
                    logger.warning(
                        "batch_analyze already running on execution (holder=%s), skipping trigger for trade_date=%s",
                        running_holder, latest_ohlc,
                    )
                    await self._release_auto_chain(claim_key)
                    return
        except Exception as exc:
            logger.debug("Failed to check execution batch_analyze lock: %s", exc)
        source_symbols = source_params.get("symbols") if isinstance(source_params.get("symbols"), list) else None
        created = await self.create_task_job(
            service="execution",
            job_type="batch_analyze",
            params={
                **({"symbols": list(source_symbols)} if source_symbols else {}),
                "analyzers": ["livermore", "multi_indicator", "chanlun"],
                "config": {
                    "mode": "incremental",
                    "save_all_dates": True,
                    "save_daily_states": True,
                    "emit_events": True,
                    "cache_enabled": False,
                    "use_incremental": True,
                },
            },
            metadata={
                "auto_chain_source": "flowhub_daily",
                "auto_chain_trade_date": latest_ohlc,
                **({"auto_chain_symbol_scope": list(source_symbols)} if source_symbols else {}),
            },
            lineage_context=await self._build_child_lineage_context(task_job_id),
        )
        await self._update_task_record_metadata(
            task_job_id,
            {
                "auto_chain_trade_date": latest_ohlc,
                "followup_execution_task_job_id": created.get("id"),
            },
        )
        await self._append_history(task_job_id, "auto_chain_created", normalized)

    @staticmethod
    def _scope_signature(params: Dict[str, Any]) -> str:
        if not isinstance(params, dict):
            return ""
        normalized = {}
        for key in ("symbols", "start_date", "end_date", "incremental"):
            value = params.get(key)
            if value is None:
                continue
            normalized[key] = value
        if not normalized:
            return ""
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _auto_chain_claim_key(prefix: str, trade_date: Optional[str], scope_signature: str = "") -> str:
        base = f"{prefix}:{trade_date or 'unknown'}"
        if not scope_signature:
            return base
        digest = hashlib.sha1(scope_signature.encode("utf-8")).hexdigest()[:12]
        return f"{base}:{digest}"

    @staticmethod
    def _job_result_ready_for_auto_chain(normalized: Dict[str, Any]) -> bool:
        if str(normalized.get("status") or "") != "succeeded":
            return False
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else {}
        failed_count = result.get("failed_count")
        if failed_count is not None:
            try:
                return int(failed_count) == 0
            except Exception:
                return False
        success_flag = result.get("success")
        if success_flag is not None:
            return bool(success_flag)
        return True

    async def _evaluate_daily_analysis_prereqs(
        self,
        source_task_job_id: Optional[str] = None,
    ) -> tuple[bool, str, Optional[str], Optional[str]]:
        source_record = await self._load_task_record(source_task_job_id) if source_task_job_id else None
        source_params = source_record.get("params") if isinstance(source_record, dict) and isinstance(source_record.get("params"), dict) else {}
        scope_signature = self._scope_signature(source_params)
        if scope_signature:
            scoped_jobs: Dict[str, Dict[str, Any]] = {}
            for record in self._brain_jobs.values():
                if not isinstance(record, dict):
                    continue
                if record.get("service") != "flowhub":
                    continue
                job_type = str(record.get("job_type") or "").strip()
                if job_type not in {"batch_daily_ohlc", "batch_daily_basic"}:
                    continue
                params = record.get("params") if isinstance(record.get("params"), dict) else {}
                if self._scope_signature(params) != scope_signature:
                    continue
                existing = scoped_jobs.get(job_type)
                if existing is None or self._as_timestamp(record.get("created_at")) >= self._as_timestamp(existing.get("created_at")):
                    scoped_jobs[job_type] = record
            trade_dates: Dict[str, Optional[str]] = {}
            for job_type in ("batch_daily_ohlc", "batch_daily_basic"):
                record = scoped_jobs.get(job_type)
                if not record:
                    return False, f"paired_scope_missing:{job_type}", trade_dates.get("batch_daily_ohlc"), trade_dates.get("batch_daily_basic")
                payload = await self._safe_get_service_job("flowhub", str(record.get("service_job_id") or ""))
                normalized = self._normalize_job(
                    "flowhub",
                    payload or {"job_id": record.get("service_job_id"), "status": "queued"},
                    task_job_id=str(record.get("task_job_id") or f"flowhub:{record.get('service_job_id')}"),
                )
                trade_dates[job_type] = self._extract_daily_fetch_trade_date(normalized)
                if not self._job_result_ready_for_auto_chain(normalized):
                    return False, f"paired_scope_not_ready:{job_type}:{normalized.get('status')}", trade_dates.get("batch_daily_ohlc"), trade_dates.get("batch_daily_basic")
                if not trade_dates[job_type]:
                    return False, f"missing_trade_date:{job_type}", trade_dates.get("batch_daily_ohlc"), trade_dates.get("batch_daily_basic")
            latest_ohlc = trade_dates.get("batch_daily_ohlc")
            latest_basic = trade_dates.get("batch_daily_basic")
            if latest_ohlc != latest_basic:
                return False, "trade_date_not_aligned", latest_ohlc, latest_basic
            return True, "", latest_ohlc, latest_basic
        try:
            recent_jobs = await self._list_service_jobs("flowhub", status=None, max_items=100)
        except Exception:
            return False, "flowhub_jobs_unavailable", None, None
        latest_jobs: Dict[str, Dict[str, Any]] = {}
        for raw_job in recent_jobs:
            job_type = str(raw_job.get("job_type") or "").strip()
            if job_type not in {"batch_daily_ohlc", "batch_daily_basic"}:
                continue
            normalized = self._normalize_job(
                "flowhub",
                raw_job,
                task_job_id=f"flowhub:{self._get_service_job_id(raw_job) or job_type}",
            )
            ts = self._as_timestamp(
                normalized.get("updated_at")
                or normalized.get("completed_at")
                or normalized.get("created_at")
            )
            current = latest_jobs.get(job_type)
            current_ts = self._as_timestamp(
                current.get("updated_at") or current.get("completed_at") or current.get("created_at")
            ) if isinstance(current, dict) else 0.0
            if current is None or ts >= current_ts:
                latest_jobs[job_type] = normalized
        ohlc_job = latest_jobs.get("batch_daily_ohlc")
        basic_job = latest_jobs.get("batch_daily_basic")
        latest_ohlc = self._extract_daily_fetch_trade_date(ohlc_job or {})
        latest_basic = self._extract_daily_fetch_trade_date(basic_job or {})
        ohlc_status = str((ohlc_job or {}).get("status") or "")
        basic_status = str((basic_job or {}).get("status") or "")
        if ohlc_status != "succeeded" or basic_status != "succeeded":
            return False, f"upstream_status_not_ready:ohlc={ohlc_status or 'missing'},basic={basic_status or 'missing'}", latest_ohlc, latest_basic
        if not self._job_result_ready_for_auto_chain(ohlc_job or {}) or not self._job_result_ready_for_auto_chain(basic_job or {}):
            return False, "upstream_result_not_ready", latest_ohlc, latest_basic
        if not latest_ohlc or not latest_basic:
            return False, "missing_trade_date", latest_ohlc, latest_basic
        if latest_ohlc != latest_basic:
            return False, "trade_date_not_aligned", latest_ohlc, latest_basic
        return True, "", latest_ohlc, latest_basic

    async def _maybe_trigger_candidate_sync(self, task_job_id: str, normalized: Dict[str, Any]) -> None:
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else {}
        trade_date = str(result.get("last_trade_date") or "").strip() or self._get_table_latest_date("analysis_results")
        if not trade_date:
            return
        if not await self._claim_auto_chain(f"candidate_sync:{trade_date}"):
            logger.warning(
                "Auto-chain claim 'candidate_sync:%s' already taken, skipping candidate_sync trigger",
                trade_date,
            )
            return
        created = await self.create_task_job(
            service="execution",
            job_type="ui_candidates_sync_from_analysis",
            params={"as_of_date": trade_date},
            metadata={
                "auto_chain_source": "batch_analyze",
                "auto_chain_trade_date": trade_date,
            },
            lineage_context=await self._build_child_lineage_context(task_job_id),
        )
        await self._update_task_record_metadata(
            task_job_id,
            {
                "auto_chain_trade_date": trade_date,
                "followup_candidate_sync_task_job_id": created.get("id"),
            },
        )
        await self._append_history(task_job_id, "auto_chain_created", normalized)

    async def _maybe_trigger_candidate_sync_incremental(
        self,
        task_job_id: str,
        normalized: Dict[str, Any],
        *,
        source_job_type: str,
        repair_window_days: int,
    ) -> None:
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else {}
        latest_trade_date = str(result.get("last_trade_date") or "").strip() or self._get_table_latest_date("analysis_results")
        if not latest_trade_date:
            return
        coverage_start_date = str(result.get("coverage_start_date") or "").strip() or latest_trade_date
        analyzer_signature = ",".join(sorted(["livermore", "chanlun", "multi_indicator"]))
        lookback_days = 30
        formula_version = "candidate_sync_v4_per_analyzer"
        claim_key = f"candidate_sync_incremental:{latest_trade_date}:{analyzer_signature}:{lookback_days}:{formula_version}"
        if not await self._claim_auto_chain(claim_key):
            logger.warning(
                "Auto-chain claim '%s' already taken, skipping candidate_sync_incremental trigger",
                claim_key,
            )
            return
        params: Dict[str, Any] = {
            "mode": "resume_repair",
            "to_trade_date": latest_trade_date,
            "repair_window_days": int(repair_window_days),
            "lookback_days": lookback_days,
            "analyzers": ["livermore", "chanlun", "multi_indicator"],
        }
        if source_job_type == "batch_analyze":
            params["from_trade_date"] = latest_trade_date
        else:
            params["from_trade_date"] = coverage_start_date
        created = await self.create_task_job(
            service="execution",
            job_type="ui_candidates_sync_incremental",
            params=params,
            metadata={
                "auto_chain_source": source_job_type,
                "auto_chain_trade_date": latest_trade_date,
                "auto_chain_claim_key": claim_key,
                "auto_chain_profile": {
                    "analyzer_signature": analyzer_signature,
                    "lookback_days": lookback_days,
                    "formula_version": formula_version,
                },
            },
            lineage_context=await self._build_child_lineage_context(task_job_id),
        )
        await self._update_task_record_metadata(
            task_job_id,
            {
                "auto_chain_trade_date": latest_trade_date,
                "followup_candidate_sync_incremental_task_job_id": created.get("id"),
            },
        )
        await self._append_history(task_job_id, "auto_chain_created", normalized)

    SNAPSHOT_PREREQ_FIELDS = frozenset({"batch_analyze", "index_daily", "industry_board"})

    def _snapshot_prereqs_key(self, trade_date: str) -> str:
        return f"brain:snapshot_prereqs:{trade_date}"

    def _resolve_snapshot_trade_date(self, normalized: Dict[str, Any]) -> Optional[str]:
        service = str(normalized.get("service") or "").strip().lower()
        job_type = str(normalized.get("job_type") or "").strip()
        result = normalized.get("result") if isinstance(normalized.get("result"), dict) else {}
        if service == "execution" and job_type == "batch_analyze":
            td = str(result.get("last_trade_date") or "").strip()
            return td or None
        if service == "flowhub" and job_type in {"index_daily_data", "industry_board"}:
            for key in ("latest_trade_date", "trade_date"):
                td = str(result.get(key) or "").strip()
                if td:
                    return td
        return None

    async def _try_converge_snapshot_prereq(self, field: str, task_job_id: str, normalized: Dict[str, Any]) -> None:
        trade_date = self._resolve_snapshot_trade_date(normalized)
        if not trade_date:
            logger.warning("Cannot resolve trade_date for snapshot prereq '%s'", field)
            return

        prereq_key = self._snapshot_prereqs_key(trade_date)
        redis = self._redis()

        if redis:
            try:
                await redis.hset(prereq_key, field, "1")
                await redis.expire(prereq_key, 86400)
                raw = await redis.hgetall(prereq_key)
                completed_fields = set()
                for k, v in raw.items():
                    key_str = k.decode("utf-8") if isinstance(k, bytes) else str(k)
                    val_str = v.decode("utf-8") if isinstance(v, bytes) else str(v)
                    if val_str == "1":
                        completed_fields.add(key_str)
            except Exception as exc:
                logger.warning("Redis snapshot prereq tracking failed for '%s': %s", field, exc)
                completed_fields = {field}
        else:
            local_key = f"_snapshot_prereqs_{trade_date}"
            if not hasattr(self, local_key):
                setattr(self, local_key, set())
            local_set: set = getattr(self, local_key)
            local_set.add(field)
            completed_fields = set(local_set)

        missing = self.SNAPSHOT_PREREQ_FIELDS - completed_fields
        if missing:
            logger.info(
                "Snapshot prereq '%s' ready for trade_date=%s, still waiting: %s (completed: %s)",
                field, trade_date, sorted(missing), sorted(completed_fields),
            )
            await self._update_task_record_metadata(
                task_job_id,
                {
                    "snapshot_prereq_field": field,
                    "snapshot_prereq_trade_date": trade_date,
                    "snapshot_prereq_completed": sorted(completed_fields),
                    "snapshot_prereq_missing": sorted(missing),
                },
            )
            return

        logger.info(
            "All snapshot prereqs met for trade_date=%s (fields: %s), firing snapshot refresh",
            trade_date, sorted(completed_fields),
        )
        await self._fire_snapshot_refresh(trade_date, task_job_id, normalized)

    async def _fire_snapshot_refresh(self, trade_date: str, task_job_id: str, normalized: Dict[str, Any]) -> None:
        claim_key = f"snapshot_refresh:{trade_date}"
        if not await self._claim_auto_chain(claim_key):
            logger.warning(
                "Auto-chain claim '%s' already taken, skipping snapshot_refresh trigger",
                claim_key,
            )
            return
        try:
            created = await self.create_task_job(
                service="macro",
                job_type="ui_snapshot_refresh",
                params={
                    "benchmark": "CSI300",
                    "scale": "D1",
                    "expected_trade_date": trade_date,
                },
                metadata={
                    "auto_chain_source": "snapshot_prereq_convergence",
                    "auto_chain_trade_date": trade_date,
                    "auto_chain_prereqs": sorted(self.SNAPSHOT_PREREQ_FIELDS),
                },
                lineage_context=await self._build_child_lineage_context(task_job_id),
            )
            await self._update_task_record_metadata(
                task_job_id,
                {
                    "auto_chain_trade_date": trade_date,
                    "followup_snapshot_refresh_task_job_id": created.get("id"),
                },
            )
            await self._append_history(task_job_id, "auto_chain_created", normalized)
            logger.info(
                "Convergence triggered macro:ui_snapshot_refresh for trade_date=%s (task_job_id=%s)",
                trade_date, created.get("id"),
            )
        except Exception as exc:
            logger.warning("Failed to trigger snapshot_refresh for trade_date=%s: %s", trade_date, exc)
            await self._release_auto_chain(claim_key)
