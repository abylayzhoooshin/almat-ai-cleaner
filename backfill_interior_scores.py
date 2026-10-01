"""Возобновляемый локальный backfill оценки интерьера frozen baseline.

Команда намеренно не подключена к service.py/pipeline.py. Платные вызовы
разрешены только с явным ``--execute`` и отдельным
``RENOVATION_OPENAI_API_KEY``.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import random
import sqlite3
import statistics
import tempfile
import threading
import time
from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import renovation_scorer
import renovation_worker
from renovation_store import RenovationStore


INTERIOR_FIELDS = [
    "interior_status",
    "interior_score",
    "interior_coverage",
    "interior_evaluator_version",
    "interior_evaluation_key",
    "interior_content_set_key",
    "interior_evaluated_at",
]
TERMINAL_STATUSES = {
    "ready", "ready_with_download_gaps", "no_photos", "photos_unavailable",
}
MAX_CONCURRENCY = 4
MAX_ROW_ATTEMPTS = 3
OPENAI_TIMEOUT_SECONDS = 180.0


class BackfillError(RuntimeError):
    pass


class InputValidationError(BackfillError):
    pass


class FatalAuthorizationError(BackfillError):
    pass


@contextmanager
def _exclusive_run_lock(db_path: Path):
    """Prevent two CLI backfills from paying for the same rows concurrently."""
    lock_path = Path(f"{db_path}.run.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    try:
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise BackfillError(
                f"уже запущен другой backfill для базы {db_path}"
            ) from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


@dataclass(frozen=True)
class BackfillConfig:
    input_path: Path
    output_path: Path
    db_path: Path
    report_path: Path
    concurrency: int = 1
    limit: int | None = None
    ids: tuple[str, ...] = ()
    dry_run: bool = False
    execute: bool = False
    input_cost_per_million: float | None = None
    output_cost_per_million: float | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolved(path: Path) -> Path:
    return path.expanduser().resolve()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise InputValidationError("input CSV не содержит заголовок")
        fields = list(reader.fieldnames)
        if any(not str(field or "").strip() for field in fields):
            raise InputValidationError("input CSV содержит пустое имя колонки")
        if len(fields) != len(set(fields)):
            raise InputValidationError("input CSV содержит повторяющиеся имена колонок")
        if "id" not in fields or "photo_urls" not in fields:
            raise InputValidationError("input CSV должен содержать поля id и photo_urls")
        rows = []
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise InputValidationError(
                    f"строка CSV {line_number} содержит больше значений, чем заголовок"
                )
            rows.append(dict(row))
    return fields, rows


def _parse_price(row: dict[str, str]) -> float | None:
    raw = row.get("price") or row.get("price_kzt")
    if raw is None or not str(raw).strip():
        return None
    try:
        return float(str(raw).replace(" ", "").replace(",", "."))
    except ValueError:
        return None


def _selected_rows(
    rows: list[dict[str, str]], ids: Iterable[str], limit: int | None,
) -> list[dict[str, str]]:
    requested = tuple(dict.fromkeys(str(value).strip() for value in ids if str(value).strip()))
    if requested:
        wanted = set(requested)
        present = {str(row.get("id") or "").strip() for row in rows}
        missing = [value for value in requested if value not in present]
        if missing:
            raise InputValidationError(f"ID отсутствуют во входном CSV: {', '.join(missing[:20])}")
        selected = [row for row in rows if str(row.get("id") or "").strip() in wanted]
    else:
        selected = list(rows)
    return selected if limit is None else selected[:limit]


def _input_analysis(rows: list[dict[str, str]]) -> dict[str, Any]:
    ids = [str(row.get("id") or "").strip() for row in rows]
    counts = Counter(value for value in ids if value)
    duplicate_ids = sorted(value for value, count in counts.items() if count > 1)
    source_status = Counter((row.get("status") or "").strip() or "<empty>" for row in rows)
    with_photos = 0
    invalid: list[dict[str, str]] = []
    for row in rows:
        try:
            urls = renovation_worker.parse_photo_urls(row.get("photo_urls"))
            with_photos += bool(urls)
        except Exception as exc:
            invalid.append({
                "id": str(row.get("id") or ""),
                "error": f"{type(exc).__name__}: {str(exc)[:200]}",
            })
    return {
        "row_count": len(rows),
        "empty_id_count": sum(not value for value in ids),
        "duplicate_id_count": len(duplicate_ids),
        "duplicate_ids": duplicate_ids[:100],
        "source_status_counts": dict(sorted(source_status.items())),
        "rows_with_photos": with_photos,
        "rows_without_photos": len(rows) - with_photos - len(invalid),
        "invalid_photo_urls_count": len(invalid),
        "invalid_photo_urls": invalid[:100],
    }


def _readonly_states(path: Path, listing_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not path.exists() or not listing_ids:
        return {}
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        tables = {
            row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "listing_states" not in tables:
            return {}
        states: dict[str, dict[str, Any]] = {}
        values = sorted(listing_ids)
        for offset in range(0, len(values), 500):
            chunk = values[offset:offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT s.*, r.evaluator_version FROM listing_states AS s "
                "LEFT JOIN results AS r ON r.evaluation_key = s.evaluation_key "
                f"WHERE s.listing_id IN ({placeholders})"
            )
            states.update((row["listing_id"], dict(row)) for row in connection.execute(query, chunk))
        return states
    finally:
        connection.close()


def _resume_counts(rows: list[dict[str, str]], db_path: Path) -> dict[str, int]:
    ids = {str(row.get("id") or "").strip() for row in rows}
    states = _readonly_states(db_path, ids)
    counts = Counter()
    for row in rows:
        listing_id = str(row.get("id") or "").strip()
        if not listing_id:
            counts["invalid_input"] += 1
            continue
        state = states.get(listing_id)
        if state is None:
            counts["new"] += 1
        elif state["status"] in {"pending", "error"}:
            counts["resume"] += 1
        elif state["status"] in TERMINAL_STATUSES:
            try:
                current_source_key = renovation_worker.source_set_key(
                    renovation_worker.parse_photo_urls(row.get("photo_urls"))
                )
            except Exception:
                counts["invalid_input"] += 1
                continue
            version_ok = (
                state["status"] not in {"ready", "ready_with_download_gaps"}
                or state.get("evaluator_version") == renovation_scorer.EVALUATOR_VERSION
            )
            if state["source_set_key"] == current_source_key and version_ok:
                counts["terminal"] += 1
            else:
                counts["stale"] += 1
        else:
            counts["other"] += 1
    return dict(sorted(counts.items()))


def _validate_config(config: BackfillConfig) -> None:
    if config.dry_run == config.execute:
        raise InputValidationError("укажите ровно один флаг: --dry-run или --execute")
    if not 1 <= config.concurrency <= MAX_CONCURRENCY:
        raise InputValidationError("--concurrency должен быть от 1 до 4")
    if config.limit is not None and config.limit < 1:
        raise InputValidationError("--limit должен быть положительным")
    artifact_paths = {
        "--input": _resolved(config.input_path),
        "--output": _resolved(config.output_path),
        "--db": _resolved(config.db_path),
        "--report": _resolved(config.report_path),
    }
    if len(set(artifact_paths.values())) != len(artifact_paths):
        collisions = [
            name for name, path in artifact_paths.items()
            if list(artifact_paths.values()).count(path) > 1
        ]
        raise InputValidationError(
            "пути input/output/db/report должны различаться; конфликт: "
            + ", ".join(collisions)
        )
    if not config.input_path.is_file():
        raise InputValidationError(f"input CSV не найден: {config.input_path}")
    prices = (config.input_cost_per_million, config.output_cost_per_million)
    if (prices[0] is None) != (prices[1] is None):
        raise InputValidationError("для оценки стоимости задайте оба тарифа")
    if any(value is not None and value < 0 for value in prices):
        raise InputValidationError("тарифы не могут быть отрицательными")


def _status_code(exc: BaseException) -> int | None:
    direct = getattr(exc, "status_code", None)
    if isinstance(direct, int):
        return direct
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _is_auth_error(exc: BaseException) -> bool:
    return _status_code(exc) in {401, 403} or type(exc).__name__ in {
        "AuthenticationError", "PermissionDeniedError",
    }


def _is_retryable(exc: BaseException) -> bool:
    code = _status_code(exc)
    if code in {408, 409, 425, 429} or (code is not None and code >= 500):
        return True
    if isinstance(exc, renovation_worker.PhotoSetUnavailable):
        return True
    if isinstance(exc, renovation_scorer.InvalidModelResponse):
        return True
    return isinstance(exc, (TimeoutError, ConnectionError, OSError)) or type(exc).__name__ in {
        "APITimeoutError", "APIConnectionError", "RateLimitError", "InternalServerError",
    }


def _clean_error(exc: BaseException, secret: str = "") -> str:
    message = f"{type(exc).__name__}: {str(exc)}"
    if secret:
        message = message.replace(secret, "[REDACTED]")
    if "data:image/" in message:
        message = message.split("data:image/", 1)[0] + "[DATA_URL_REDACTED]"
    return message.replace("\r", " ").replace("\n", " ")[:300]


def _terminal_for_current_input(store: RenovationStore, row: dict[str, str]) -> bool:
    listing_id = str(row.get("id") or "").strip()
    state = store.get_listing_state(listing_id)
    if state is None or state["status"] not in TERMINAL_STATUSES:
        return False
    try:
        current_source_key = renovation_worker.source_set_key(
            renovation_worker.parse_photo_urls(row.get("photo_urls"))
        )
    except Exception:
        return False
    if state["source_set_key"] != current_source_key:
        return False
    if state["status"] in {"ready", "ready_with_download_gaps"}:
        if not state["evaluation_key"]:
            return False
        payload = store.get_result(state["evaluation_key"])
        return bool(payload and payload.get("evaluator_version") == renovation_scorer.EVALUATOR_VERSION)
    return True


def _pin_gap_state_to_frozen_input(
    store: RenovationStore,
    row: dict[str, str],
    result: dict[str, Any],
) -> None:
    """Make a partial result resumable for this exact frozen photo URL set.

    The reusable worker deliberately leaves ``source_set_key`` empty when one
    or more downloads failed: outside a frozen backfill, a later call should
    try the missing photos again. A backfill can run for hours and be resumed,
    so repeatedly paying for the same partial result on every restart is the
    wrong tradeoff here. Pin only the listing state to the current frozen
    input. A later baseline with a changed URL set still gets evaluated.
    """
    if result.get("status") != "ready_with_download_gaps":
        return
    payload = result.get("payload") or {}
    failures = payload.get("download_failures") or []
    # A timeout/429/5xx may succeed after a restart, so do not freeze a
    # degraded result caused by a transient CDN failure. Permanent missing
    # photos (normally 404/410) are safe to checkpoint for this exact input.
    if not failures or any(bool(item.get("retryable")) for item in failures):
        return
    listing_id = str(row.get("id") or "").strip()
    source_key = renovation_worker.source_set_key(
        renovation_worker.parse_photo_urls(row.get("photo_urls"))
    )
    state = store.get_listing_state(listing_id)
    if state is None or state["status"] != "ready_with_download_gaps":
        return
    store.save_listing_state(
        listing_id=listing_id,
        status=state["status"],
        evaluation_key=state["evaluation_key"],
        source_set_key=source_key,
        photo_url_count=state["photo_url_count"],
        downloaded_photo_count=state["downloaded_photo_count"],
        failed_photo_count=state["failed_photo_count"],
        price=state["last_price"],
        event_reason=state["event_reason"],
        error=state["error"],
    )


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return round(ordered[index], 4)


def _attempt_metrics(connection: sqlite3.Connection, after_attempt_id: int) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT * FROM attempts WHERE attempt_id > ? ORDER BY attempt_id", (after_attempt_id,)
    ).fetchall()
    input_tokens = output_tokens = 0
    models: set[str] = set()
    for row in rows:
        if row["model"]:
            models.add(row["model"])
        if not row["usage_json"]:
            continue
        try:
            usage = json.loads(row["usage_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        input_tokens += int(usage.get("input_tokens") or 0)
        output_tokens += int(usage.get("output_tokens") or 0)
    statuses = Counter(row["status"] for row in rows)
    return {
        "attempts": len(rows),
        "succeeded": statuses.get("succeeded", 0),
        "failed": statuses.get("failed", 0),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "models": sorted(models),
    }


def _state_summary(store: RenovationStore, listing_ids: set[str]) -> dict[str, Any]:
    status_counts = Counter()
    coverage_counts = Counter()
    downloaded = unavailable = 0
    errors: list[dict[str, str]] = []
    statuses: dict[str, str] = {}
    rows = []
    values = sorted(listing_ids)
    for offset in range(0, len(values), 500):
        chunk = values[offset:offset + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(store.connection.execute(
            "SELECT s.*, r.coverage FROM listing_states AS s "
            "LEFT JOIN results AS r ON r.evaluation_key = s.evaluation_key "
            f"WHERE s.listing_id IN ({placeholders})",
            chunk,
        ).fetchall())
    for state in rows:
        listing_id = state["listing_id"]
        statuses[listing_id] = state["status"]
        status_counts[state["status"]] += 1
        downloaded += int(state["downloaded_photo_count"] or 0)
        unavailable += int(state["failed_photo_count"] or 0)
        coverage = state["coverage"]
        if coverage:
            coverage_counts[coverage] += 1
        if state["status"] == "error":
            errors.append({"id": listing_id, "reason": str(state["error"] or "")[:300]})
    return {
        "status_counts": dict(sorted(status_counts.items())),
        "coverage_counts": dict(sorted(coverage_counts.items())),
        "downloaded_photos": downloaded,
        "unavailable_photos": unavailable,
        "errors": sorted(errors, key=lambda item: item["id"]),
        "statuses": statuses,
    }


def _write_enriched_csv(
    input_fields: list[str], rows: list[dict[str, str]], store: RenovationStore,
    output_path: Path,
) -> None:
    exports = {item["listing_id"]: item for item in store.iter_baseline_exports()}
    fields = [field for field in input_fields if field not in INTERIOR_FIELDS] + INTERIOR_FIELDS
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                enriched = dict(row)
                export = exports.get(str(row.get("id") or "").strip(), {})
                for field in INTERIOR_FIELDS:
                    value = export.get(field)
                    enriched[field] = "" if value is None else value
                writer.writerow(enriched)
            handle.flush()
            os.fsync(handle.fileno())

        check_fields, check_rows = _read_csv(Path(temporary))
        if len(check_rows) != len(rows):
            raise BackfillError("валидация output: изменилось число строк")
        if [row.get("id") for row in check_rows] != [row.get("id") for row in rows]:
            raise BackfillError("валидация output: изменился порядок ID")
        if check_fields != fields:
            raise BackfillError("валидация output: неожиданный заголовок")
        os.replace(temporary, output_path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


async def run_backfill(
    config: BackfillConfig,
    *,
    client: Any = None,
    fetcher: Callable[[str], renovation_worker.DownloadedPhoto] = renovation_worker.download_photo,
    sleep: Callable[[float], Any] = asyncio.sleep,
    random_value: Callable[[], float] = random.random,
) -> dict[str, Any]:
    _validate_config(config)
    input_fields, all_rows = _read_csv(config.input_path)
    analysis = _input_analysis(all_rows)
    selected = _selected_rows(all_rows, config.ids, config.limit)
    selected_ids = {str(row.get("id") or "").strip() for row in selected}
    started_at = _utc_now()
    started_clock = time.perf_counter()

    base_report: dict[str, Any] = {
        "input": str(_resolved(config.input_path)),
        "output": str(_resolved(config.output_path)),
        "database": str(_resolved(config.db_path)),
        "started_at": started_at,
        "finished_at": None,
        "evaluator_version": renovation_scorer.EVALUATOR_VERSION,
        "configured_model": renovation_scorer.MODEL,
        "returned_models": [],
        "input_rows": len(all_rows),
        "selected_rows": len(selected),
        "complete_input_selected": len(selected) == len(all_rows),
        "output_rows": None,
        "input_analysis": analysis,
        "resume_plan": _resume_counts(selected, config.db_path),
        "status_counts": {},
        "coverage_counts": {},
        "api": {"attempts": 0, "succeeded": 0, "failed": 0, "cache_hits": 0},
        "photos": {
            "downloaded": 0,
            "unavailable": 0,
            "selected_state_downloaded": 0,
            "selected_state_unavailable": 0,
            "rows_over_set_limit": 0,
        },
        "tokens": {"input": 0, "output": 0},
        "estimated_cost": None,
        "latency_seconds": {"p50": None, "p95": None},
        "elapsed_seconds": 0,
        "errors": [],
        "clean": False,
        "aborted": False,
        "dry_run": config.dry_run,
    }

    if config.dry_run:
        base_report["finished_at"] = _utc_now()
        base_report["elapsed_seconds"] = round(time.perf_counter() - started_clock, 4)
        base_report["clean"] = not (
            analysis["empty_id_count"] or analysis["duplicate_id_count"]
            or analysis["invalid_photo_urls_count"]
        )
        return base_report

    if analysis["empty_id_count"] or analysis["duplicate_id_count"]:
        raise InputValidationError("пустые или дублирующиеся ID запрещают платный запуск")
    secret = os.environ.get("RENOVATION_OPENAI_API_KEY", "").strip()
    owns_client = False
    if client is None:
        if not secret:
            raise InputValidationError("для --execute задайте RENOVATION_OPENAI_API_KEY")
        from openai import AsyncOpenAI
        # The batch owns its retry policy. SDK retries here would multiply the
        # three row attempts below and make spend/progress hard to reason about.
        client = AsyncOpenAI(
            api_key=secret,
            max_retries=0,
            timeout=OPENAI_TIMEOUT_SECONDS,
        )
        owns_client = True

    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    latencies: list[float] = []
    cache_hits = 0
    rows_over_limit = 0
    downloaded_photos = 0
    unavailable_photos = 0
    photo_counter_lock = threading.Lock()
    aborted = False
    fatal_error: str | None = None
    errors: list[dict[str, str]] = []

    def counted_fetcher(url: str) -> renovation_worker.DownloadedPhoto:
        nonlocal downloaded_photos, unavailable_photos
        try:
            photo = fetcher(url)
        except BaseException:
            with photo_counter_lock:
                unavailable_photos += 1
            raise
        with photo_counter_lock:
            downloaded_photos += 1
        return photo

    with RenovationStore(config.db_path) as store:
        # Процесс мог завершиться после start_attempt(), но до finish_attempt().
        # Такая запись не означает активный запрос после рестарта и не должна
        # навсегда блокировать финальный экспорт.
        store.connection.execute(
            "UPDATE attempts SET status = 'interrupted', finished_at = ?, "
            "error = COALESCE(error, 'previous process interrupted') "
            "WHERE status = 'running'",
            (_utc_now(),),
        )
        store.connection.commit()
        before_row = store.connection.execute(
            "SELECT COALESCE(MAX(attempt_id), 0) AS n FROM attempts"
        ).fetchone()
        before_attempt_id = int(before_row["n"])
        report_lock = asyncio.Lock()
        completed_rows = 0
        queue: asyncio.Queue[dict[str, str]] = asyncio.Queue()
        for row in selected:
            queue.put_nowait(row)

        async def update_progress() -> None:
            summary = _state_summary(store, selected_ids)
            attempt = _attempt_metrics(store.connection, before_attempt_id)
            progress = dict(base_report)
            progress["status_counts"] = summary["status_counts"]
            progress["coverage_counts"] = summary["coverage_counts"]
            progress["api"] = {
                "attempts": attempt["attempts"], "succeeded": attempt["succeeded"],
                "failed": attempt["failed"], "cache_hits": cache_hits,
            }
            progress["photos"] = {
                "downloaded": downloaded_photos,
                "unavailable": unavailable_photos,
                "selected_state_downloaded": summary["downloaded_photos"],
                "selected_state_unavailable": summary["unavailable_photos"],
                "rows_over_set_limit": rows_over_limit,
            }
            progress["tokens"] = {
                "input": attempt["input_tokens"], "output": attempt["output_tokens"],
            }
            progress["returned_models"] = attempt["models"]
            progress["errors"] = errors[-100:]
            progress["elapsed_seconds"] = round(time.perf_counter() - started_clock, 4)
            progress["aborted"] = aborted
            async with report_lock:
                _atomic_json(config.report_path, progress)
                if owns_client:
                    print(json.dumps({
                        "event": "backfill_progress",
                        "processed": completed_rows,
                        "selected": len(selected),
                        "remaining": max(0, len(selected) - completed_rows),
                        "status_counts": summary["status_counts"],
                        "api_attempts": attempt["attempts"],
                        "api_failed": attempt["failed"],
                        "photos_downloaded": downloaded_photos,
                        "photos_unavailable": unavailable_photos,
                        "error_rows": summary["status_counts"].get("error", 0),
                        "elapsed_seconds": progress["elapsed_seconds"],
                    }, ensure_ascii=False), flush=True)

        async def process_row(row: dict[str, str]) -> None:
            nonlocal cache_hits, rows_over_limit, aborted, fatal_error
            listing_id = str(row.get("id") or "").strip()
            if _terminal_for_current_input(store, row):
                cache_hits += 1
                return
            row_started = time.perf_counter()
            for attempt_number in range(1, MAX_ROW_ATTEMPTS + 1):
                try:
                    result = await renovation_worker.process_listing(
                        store=store,
                        client=client,
                        listing_id=listing_id,
                        price=_parse_price(row),
                        photo_urls=row.get("photo_urls"),
                        event_reason="frozen_baseline",
                        fetcher=counted_fetcher,
                    )
                    _pin_gap_state_to_frozen_input(store, row, result)
                    cache_hits += int(bool(result.get("cache_hit")))
                    return
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    cleaned = _clean_error(exc, secret)
                    if secret:
                        store.connection.execute(
                            "UPDATE listing_states SET error = REPLACE(error, ?, '[REDACTED]') "
                            "WHERE error LIKE ?",
                            (secret, f"%{secret}%"),
                        )
                        store.connection.execute(
                            "UPDATE attempts SET error = REPLACE(error, ?, '[REDACTED]') "
                            "WHERE error LIKE ?",
                            (secret, f"%{secret}%"),
                        )
                        store.connection.commit()
                    if _is_auth_error(exc):
                        aborted = True
                        fatal_error = cleaned
                        raise FatalAuthorizationError(cleaned) from exc
                    if "набор фотографий превышает лимит" in str(exc):
                        rows_over_limit += 1
                    if not _is_retryable(exc) or attempt_number >= MAX_ROW_ATTEMPTS:
                        errors.append({"id": listing_id, "reason": cleaned})
                        return
                    delay = (2 ** (attempt_number - 1)) + random_value()
                    await sleep(delay)
        async def worker() -> None:
            nonlocal aborted, completed_rows
            while not aborted:
                try:
                    row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                started = time.perf_counter()
                try:
                    await process_row(row)
                    latencies.append(time.perf_counter() - started)
                    completed_rows += 1
                    if completed_rows % 25 == 0 or queue.empty():
                        await update_progress()
                finally:
                    queue.task_done()

        _atomic_json(config.report_path, base_report)
        if owns_client:
            print(json.dumps({
                "event": "backfill_started",
                "selected": len(selected),
                "concurrency": config.concurrency,
                "database": str(config.db_path),
            }, ensure_ascii=False), flush=True)
        tasks = [asyncio.create_task(worker()) for _ in range(config.concurrency)]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, FatalAuthorizationError):
                aborted = True
            elif isinstance(outcome, BaseException):
                aborted = True
                fatal_error = _clean_error(outcome, secret)

        summary = _state_summary(store, selected_ids)
        attempt = _attempt_metrics(store.connection, before_attempt_id)
        final_statuses = summary["statuses"]
        incomplete = [
            listing_id for listing_id in selected_ids
            if final_statuses.get(listing_id) not in TERMINAL_STATUSES | {"error"}
        ]
        running = store.connection.execute(
            "SELECT COUNT(*) AS n FROM attempts WHERE status = 'running'"
        ).fetchone()["n"]
        if incomplete or running:
            aborted = True
            fatal_error = fatal_error or (
                "нет финального состояния у ID: " + ", ".join(sorted(incomplete)[:20])
                if incomplete else "остались running attempts"
            )

        if not aborted:
            _write_enriched_csv(input_fields, all_rows, store, config.output_path)

        final = dict(base_report)
        final["finished_at"] = _utc_now()
        final["elapsed_seconds"] = round(time.perf_counter() - started_clock, 4)
        final["output_rows"] = len(all_rows) if not aborted else None
        final["status_counts"] = summary["status_counts"]
        final["coverage_counts"] = summary["coverage_counts"]
        final["api"] = {
            "attempts": attempt["attempts"], "succeeded": attempt["succeeded"],
            "failed": attempt["failed"], "cache_hits": cache_hits,
        }
        final["photos"] = {
            "downloaded": downloaded_photos,
            "unavailable": unavailable_photos,
            "selected_state_downloaded": summary["downloaded_photos"],
            "selected_state_unavailable": summary["unavailable_photos"],
            "rows_over_set_limit": rows_over_limit,
        }
        final["tokens"] = {"input": attempt["input_tokens"], "output": attempt["output_tokens"]}
        final["returned_models"] = attempt["models"]
        final["latency_seconds"] = {
            "p50": round(statistics.median(latencies), 4) if latencies else None,
            "p95": _percentile(latencies, 0.95),
        }
        combined_errors = {item["id"]: item for item in summary["errors"]}
        combined_errors.update({item["id"]: item for item in errors})
        final["errors"] = sorted(combined_errors.values(), key=lambda item: item["id"])
        if fatal_error:
            final["fatal_error"] = fatal_error
        if (
            config.input_cost_per_million is not None
            and config.output_cost_per_million is not None
        ):
            final["estimated_cost"] = round(
                attempt["input_tokens"] / 1_000_000 * config.input_cost_per_million
                + attempt["output_tokens"] / 1_000_000 * config.output_cost_per_million,
                8,
            )
        final["aborted"] = aborted
        final["clean"] = not aborted and not final["errors"]
        _atomic_json(config.report_path, final)
        if owns_client:
            await client.close()
        if aborted:
            raise FatalAuthorizationError(fatal_error or "batch аварийно остановлен")
        return final


def _parse_ids(raw: str | None, ids_file: Path | None) -> tuple[str, ...]:
    values: list[str] = []
    if raw:
        values.extend(part.strip() for part in raw.split(","))
    if ids_file:
        values.extend(line.strip() for line in ids_file.read_text(encoding="utf-8-sig").splitlines())
    return tuple(value for value in values if value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--limit", type=int)
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument("--ids", help="ID через запятую")
    choice.add_argument("--ids-file", type=Path, help="один ID на строку")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--input-cost-per-million", type=float)
    parser.add_argument("--output-cost-per-million", type=float)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = BackfillConfig(
        input_path=args.input,
        output_path=args.output,
        db_path=args.db,
        report_path=args.report,
        concurrency=args.concurrency,
        limit=args.limit,
        ids=_parse_ids(args.ids, args.ids_file),
        dry_run=args.dry_run,
        execute=args.execute,
        input_cost_per_million=args.input_cost_per_million,
        output_cost_per_million=args.output_cost_per_million,
    )
    lock = _exclusive_run_lock(config.db_path) if config.execute else nullcontext()
    try:
        with lock:
            report = asyncio.run(run_backfill(config))
    except (BackfillError, OSError, csv.Error) as exc:
        print(f"ERROR: {_clean_error(exc)}")
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("clean") else 2


if __name__ == "__main__":
    raise SystemExit(main())
