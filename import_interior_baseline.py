"""One-time import of a completed photo backfill into clean_baseline."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any

import cleaner_db
import interior_baseline
import renovation_scorer


class ImportError(RuntimeError):
    pass


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _optional(value: Any) -> Any:
    return None if value is None or str(value).strip() == "" else value


def _import_fields(source: dict[str, str], current_hash: str) -> dict[str, Any]:
    status = _optional(source.get("interior_status"))
    if status not in interior_baseline.TERMINAL_STATUSES:
        raise ImportError(f"неожиданный interior_status: {status!r}")

    score_text = _optional(source.get("interior_score"))
    score = None if score_text is None else float(score_text)
    coverage = _optional(source.get("interior_coverage"))
    version = _optional(source.get("interior_evaluator_version"))
    if version is None and status in {"no_photos", "photos_unavailable"}:
        version = renovation_scorer.EVALUATOR_VERSION

    if score is not None and not (1.0 <= score <= 10.0):
        raise ImportError(f"interior_score вне диапазона: {score}")
    if coverage == "insufficient" and score is not None:
        raise ImportError("coverage=insufficient требует пустой score")
    if status in {"no_photos", "photos_unavailable"} and score is not None:
        raise ImportError(f"{status} требует пустой score")
    if status in {"ready", "ready_with_download_gaps"} and not version:
        raise ImportError(f"{status} требует evaluator_version")

    return {
        "photo_set_hash": current_hash,
        "interior_status": status,
        "interior_score": score,
        "interior_coverage": coverage,
        "interior_evaluator_version": version,
        "interior_evaluation_key": _optional(source.get("interior_evaluation_key")),
        "interior_content_set_key": _optional(source.get("interior_content_set_key")),
        "interior_evaluated_at": _optional(source.get("interior_evaluated_at")),
    }


def import_completed_backfill(csv_path: Path, *, execute: bool) -> dict[str, Any]:
    imports = _read_csv(csv_path)
    import_ids = [str(row.get("id") or "").strip() for row in imports]
    if not import_ids or any(not listing_id for listing_id in import_ids):
        raise ImportError("CSV пуст или содержит пустой id")
    if len(set(import_ids)) != len(import_ids):
        raise ImportError("CSV содержит повторяющиеся id")

    with cleaner_db.connect() as conn:
        info = cleaner_db.clean_baseline_info(conn)
        if info is None:
            raise ImportError("clean baseline ещё не опубликован")
        current = list(cleaner_db.iter_clean_baseline(conn))
        current_ids = [str(row.get("id") or "") for row in current]
        if current_ids != import_ids:
            raise ImportError("ID или порядок CSV не совпадает с текущим clean baseline")

        enriched = []
        counts: dict[str, int] = {}
        for current_row, source_row in zip(current, imports):
            current_hash = interior_baseline.photo_set_hash(
                current_row.get("photo_urls"), current_row.get("photo_set_hash")
            )
            source_hash = interior_baseline.photo_set_hash(
                source_row.get("photo_urls"), source_row.get("photo_set_hash")
            )
            if current_hash != source_hash:
                raise ImportError(f"набор фото изменился у id={current_row['id']}")
            fields = _import_fields(source_row, current_hash)
            row = dict(current_row)
            row.update(fields)
            enriched.append(row)
            status = str(fields["interior_status"])
            counts[status] = counts.get(status, 0) + 1

        if execute:
            cleaner_db.replace_clean_baseline(conn, enriched, info[1])
            conn.commit()

    return {
        "rows": len(enriched),
        "status_counts": dict(sorted(counts.items())),
        "executed": execute,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    result = import_completed_backfill(args.input, execute=args.execute)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
