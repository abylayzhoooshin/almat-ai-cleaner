"""SQLite-кэш результатов оценки интерьера.

Хранилище не вызывает сеть и не зависит от orchestrator. Результат по одному
evaluation_key неизменяем: повторная запись возвращает уже сохранённую версию.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RenovationStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS results (
                evaluation_key TEXT PRIMARY KEY,
                content_set_key TEXT NOT NULL,
                evaluator_version TEXT NOT NULL,
                score REAL,
                coverage TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS listing_results (
                listing_id TEXT PRIMARY KEY,
                evaluation_key TEXT NOT NULL REFERENCES results(evaluation_key),
                source_set_key TEXT,
                last_price REAL,
                event_reason TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS listing_states (
                listing_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                evaluation_key TEXT REFERENCES results(evaluation_key),
                source_set_key TEXT,
                photo_url_count INTEGER NOT NULL,
                downloaded_photo_count INTEGER NOT NULL,
                failed_photo_count INTEGER NOT NULL,
                last_price REAL,
                event_reason TEXT,
                error TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                evaluation_key TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                request_id TEXT,
                model TEXT,
                usage_json TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS attempts_evaluation_key
                ON attempts(evaluation_key);
            """
        )
        # Локальная БД v1 уже существует у разработчика и позже будет жить на
        # persistent disk Render. Добавление nullable-поля безопасно и не
        # требует удаления оплаченного кэша.
        columns = {
            row["name"]
            for row in self.connection.execute("PRAGMA table_info(listing_results)")
        }
        if "source_set_key" not in columns:
            self.connection.execute(
                "ALTER TABLE listing_results ADD COLUMN source_set_key TEXT"
            )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "RenovationStore":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    def get_result(self, evaluation_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT payload_json FROM results WHERE evaluation_key = ?",
            (evaluation_key,),
        ).fetchone()
        return json.loads(row["payload_json"]) if row else None

    def save_result(
        self,
        *,
        evaluation_key: str,
        content_set_key: str,
        evaluator_version: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        result = payload["result"]
        with self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO results (
                    evaluation_key, content_set_key, evaluator_version,
                    score, coverage, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evaluation_key,
                    content_set_key,
                    evaluator_version,
                    result["score"],
                    result["coverage"],
                    encoded,
                    _utc_now(),
                ),
            )
        stored = self.get_result(evaluation_key)
        if stored is None:  # pragma: no cover - SQLite invariant
            raise RuntimeError("результат не сохранился")
        return stored

    def bind_listing(
        self,
        *,
        listing_id: str,
        evaluation_key: str,
        source_set_key: str | None,
        price: float | int | None,
        event_reason: str | None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO listing_results (
                    listing_id, evaluation_key, source_set_key,
                    last_price, event_reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id) DO UPDATE SET
                    evaluation_key = excluded.evaluation_key,
                    source_set_key = excluded.source_set_key,
                    last_price = excluded.last_price,
                    event_reason = excluded.event_reason,
                    updated_at = excluded.updated_at
                """,
                (
                    str(listing_id), evaluation_key, source_set_key,
                    price, event_reason, _utc_now(),
                ),
            )

    def get_listing_result_by_source(
        self,
        *,
        listing_id: str,
        source_set_key: str,
        evaluator_version: str,
    ) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT lr.evaluation_key, r.payload_json
            FROM listing_results AS lr
            JOIN results AS r ON r.evaluation_key = lr.evaluation_key
            WHERE lr.listing_id = ?
              AND lr.source_set_key = ?
              AND r.evaluator_version = ?
            """,
            (str(listing_id), source_set_key, evaluator_version),
        ).fetchone()
        if row is None:
            return None
        return {
            "evaluation_key": row["evaluation_key"],
            "payload": json.loads(row["payload_json"]),
        }

    def get_listing_binding(self, listing_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM listing_results WHERE listing_id = ?",
            (str(listing_id),),
        ).fetchone()
        return dict(row) if row else None

    def save_listing_state(
        self,
        *,
        listing_id: str,
        status: str,
        evaluation_key: str | None,
        source_set_key: str | None,
        photo_url_count: int,
        downloaded_photo_count: int,
        failed_photo_count: int,
        price: float | int | None,
        event_reason: str | None,
        error: str | None = None,
    ) -> None:
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO listing_states (
                    listing_id, status, evaluation_key, source_set_key,
                    photo_url_count, downloaded_photo_count, failed_photo_count,
                    last_price, event_reason, error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id) DO UPDATE SET
                    status = excluded.status,
                    evaluation_key = excluded.evaluation_key,
                    source_set_key = excluded.source_set_key,
                    photo_url_count = excluded.photo_url_count,
                    downloaded_photo_count = excluded.downloaded_photo_count,
                    failed_photo_count = excluded.failed_photo_count,
                    last_price = excluded.last_price,
                    event_reason = excluded.event_reason,
                    error = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (
                    str(listing_id), status, evaluation_key, source_set_key,
                    photo_url_count, downloaded_photo_count, failed_photo_count,
                    price, event_reason, error, _utc_now(),
                ),
            )

    def get_listing_state(self, listing_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM listing_states WHERE listing_id = ?",
            (str(listing_id),),
        ).fetchone()
        return dict(row) if row else None

    def iter_baseline_exports(self) -> list[dict[str, Any]]:
        """Возвращает компактные поля для присоединения к baseline по ID.

        Cleaner не должен разбирать внутренний payload_json или самостоятельно
        вычислять ключи. Для неоценённых состояний result-поля остаются NULL, а
        время берётся из listing_states, чтобы no_photos/error тоже имели метку.
        """
        rows = self.connection.execute(
            """
            SELECT
                s.listing_id,
                s.status,
                r.score,
                r.coverage,
                r.evaluator_version,
                s.evaluation_key,
                r.content_set_key,
                COALESCE(r.created_at, s.updated_at) AS evaluated_at
            FROM listing_states AS s
            LEFT JOIN results AS r ON r.evaluation_key = s.evaluation_key
            ORDER BY s.listing_id
            """
        ).fetchall()
        return [
            {
                "listing_id": row["listing_id"],
                "interior_status": row["status"],
                "interior_score": row["score"],
                "interior_coverage": row["coverage"],
                "interior_evaluator_version": row["evaluator_version"],
                "interior_evaluation_key": row["evaluation_key"],
                "interior_content_set_key": row["content_set_key"],
                "interior_evaluated_at": row["evaluated_at"],
            }
            for row in rows
        ]

    def start_attempt(self, evaluation_key: str) -> int:
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO attempts (evaluation_key, started_at, status)
                VALUES (?, ?, 'running')
                """,
                (evaluation_key, _utc_now()),
            )
        return int(cursor.lastrowid)

    def finish_attempt(
        self,
        attempt_id: int,
        *,
        status: str,
        request_id: str | None = None,
        model: str | None = None,
        usage: Any = None,
        error: str | None = None,
    ) -> None:
        usage_json = None if usage is None else json.dumps(usage, ensure_ascii=False, sort_keys=True)
        with self.connection:
            self.connection.execute(
                """
                UPDATE attempts
                SET finished_at = ?, status = ?, request_id = ?, model = ?,
                    usage_json = ?, error = ?
                WHERE attempt_id = ?
                """,
                (_utc_now(), status, request_id, model, usage_json, error, attempt_id),
            )

    def api_attempt_count(self, evaluation_key: str | None = None) -> int:
        if evaluation_key is None:
            row = self.connection.execute("SELECT COUNT(*) AS n FROM attempts").fetchone()
        else:
            row = self.connection.execute(
                "SELECT COUNT(*) AS n FROM attempts WHERE evaluation_key = ?",
                (evaluation_key,),
            ).fetchone()
        return int(row["n"])
