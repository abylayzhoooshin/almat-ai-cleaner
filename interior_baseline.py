"""Compact interior-score fields carried by the published clean baseline."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

import renovation_scorer


INTERIOR_FIELDS = (
    "photo_set_hash",
    "interior_status",
    "interior_score",
    "interior_coverage",
    "interior_evaluator_version",
    "interior_evaluation_key",
    "interior_content_set_key",
    "interior_evaluated_at",
)

TERMINAL_STATUSES = {
    "ready",
    "ready_with_download_gaps",
    "no_photos",
    "photos_unavailable",
}


def _raw_photo_urls(value: Any) -> list[str]:
    if isinstance(value, str):
        value = json.loads(value)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("photo_urls должен быть массивом строк")
    return value


def photo_set_hash(photo_urls: Any, supplied: Any = None) -> str | None:
    """Validate and return Collector's public photo-set hash.

    This deliberately differs from renovation_worker's private source cache
    key. Collector keeps exact duplicates, ignores order and uses NULL for an
    empty list.
    """
    urls = _raw_photo_urls(photo_urls)
    expected = None
    if urls:
        expected = hashlib.sha256("|".join(sorted(urls)).encode("utf-8")).hexdigest()
    normalized = str(supplied or "").strip().lower() or None
    if normalized is not None and normalized != expected:
        raise ValueError("photo_set_hash не совпадает с photo_urls Collector")
    return expected


def empty_interior(photo_hash: str | None) -> dict[str, Any]:
    result = {field: None for field in INTERIOR_FIELDS}
    result["photo_set_hash"] = photo_hash
    return result


def reusable_interior(
    previous: Mapping[str, Any] | None,
    current_photo_hash: str | None,
) -> dict[str, Any]:
    """Keep an old score only when it describes the current photo set."""
    if not previous or previous.get("photo_set_hash") != current_photo_hash:
        return empty_interior(current_photo_hash)

    status = previous.get("interior_status")
    if status not in TERMINAL_STATUSES:
        return empty_interior(current_photo_hash)

    if previous.get("interior_evaluator_version") != renovation_scorer.EVALUATOR_VERSION:
        return empty_interior(current_photo_hash)

    result = empty_interior(current_photo_hash)
    for field in INTERIOR_FIELDS:
        if field in previous:
            result[field] = previous[field]
    result["photo_set_hash"] = current_photo_hash
    return result


def enrich_rows(
    rows: list[dict[str, Any]],
    previous_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Attach current hashes and any still-valid prior interior results."""
    enriched = []
    for source in rows:
        row = dict(source)
        listing_id = str(row.get("id") or "")
        try:
            current_hash = photo_set_hash(
                row.get("photo_urls"), row.get("photo_set_hash")
            )
        except (TypeError, ValueError):
            # A malformed Collector row must never inherit an old no-photos
            # result merely because both hashes happen to be NULL.
            row.update(empty_interior(None))
            enriched.append(row)
            continue
        row.update(reusable_interior(previous_by_id.get(listing_id), current_hash))
        enriched.append(row)
    return enriched
