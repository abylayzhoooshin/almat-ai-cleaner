"""Подготовка фотографий, content-cache и вызов renovation_scorer."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
import weakref
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import renovation_scorer
from renovation_store import RenovationStore


PHOTO_TIMEOUT_SEC = 15
MAX_PHOTO_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 15 * 1024 * 1024
DOWNLOAD_CONCURRENCY = 2
ALLOWED_PHOTO_HOSTS = {"krisha-photos.kcdn.online"}
KRISHA_PLACEHOLDER_SUFFIX = "/static/frontend/images/photo-moderation-big.png"


@dataclass(frozen=True)
class DownloadedPhoto:
    url: str
    body: bytes
    mime_type: str
    sha256: str


@dataclass(frozen=True)
class PhotoDownloadFailure:
    source_index: int
    error: str
    retryable: bool
    http_status: int | None


class PhotoSetUnavailable(RuntimeError):
    pass


def _download_failure(exc: BaseException, source_index: int) -> PhotoDownloadFailure:
    http_status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
    if http_status is not None:
        # 401/403 от CDN могут означать временную блокировку/смену политики,
        # поэтому не помечаем тысячи карточек окончательно недоступными.
        retryable = http_status in {401, 403, 408, 425, 429} or http_status >= 500
    else:
        retryable = isinstance(
            exc,
            (TimeoutError, urllib.error.URLError, ConnectionError, OSError),
        )
    return PhotoDownloadFailure(
        source_index=source_index,
        error=f"{type(exc).__name__}: {str(exc)[:300]}",
        retryable=retryable,
        http_status=http_status,
    )


_LOCK_POOLS: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()


def _evaluation_lock(evaluation_key: str) -> asyncio.Lock:
    """Ограниченный пул lock'ов не даёт дважды оплатить одинаковую оценку."""
    loop = asyncio.get_running_loop()
    pool = _LOCK_POOLS.get(loop)
    if pool is None:
        pool = tuple(asyncio.Lock() for _ in range(64))
        _LOCK_POOLS[loop] = pool
    return pool[int(evaluation_key[:8], 16) % len(pool)]


def parse_photo_urls(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("photo_urls не является JSON-массивом") from exc
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("photo_urls должен быть массивом")
    urls: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("каждый photo URL должен использовать HTTPS")
        parsed = urllib.parse.urlsplit(item)
        if (
            parsed.path.endswith(KRISHA_PLACEHOLDER_SUFFIX)
            and parsed.hostname in {None, "krisha.kz"}
        ):
            continue
        if parsed.scheme != "https" or parsed.hostname not in ALLOWED_PHOTO_HOSTS:
            raise ValueError("photo URL использует неизвестный хост или не HTTPS")
        if item not in seen:
            seen.add(item)
            urls.append(item)
    return urls


def _detect_mime(body: bytes, header: str | None) -> str:
    declared = (header or "").split(";", 1)[0].strip().lower()
    if body.startswith(b"\xff\xd8\xff"):
        detected = "image/jpeg"
    elif body.startswith(b"\x89PNG\r\n\x1a\n"):
        detected = "image/png"
    elif body.startswith(b"RIFF") and body[8:12] == b"WEBP":
        detected = "image/webp"
    elif body.startswith((b"GIF87a", b"GIF89a")):
        detected = "image/gif"
    else:
        raise ValueError("ответ CDN не похож на поддерживаемое изображение")
    if declared.startswith("image/") and declared != detected:
        raise ValueError(
            f"Content-Type {declared} не совпадает с содержимым {detected}"
        )
    return detected


def download_photo(url: str) -> DownloadedPhoto:
    request = urllib.request.Request(url, headers={"User-Agent": "rieltor-almaty/renovation-scorer"})
    with urllib.request.urlopen(request, timeout=PHOTO_TIMEOUT_SEC) as response:
        body = response.read(MAX_PHOTO_BYTES + 1)
        header = response.headers.get("Content-Type")
        final_url = response.geturl()
    if urllib.parse.urlsplit(final_url).hostname not in ALLOWED_PHOTO_HOSTS:
        raise ValueError("CDN перенаправил запрос на неизвестный хост")
    if not body:
        raise ValueError("CDN вернул пустую фотографию")
    if len(body) > MAX_PHOTO_BYTES:
        raise ValueError("фотография превышает лимит 10 MiB")
    return DownloadedPhoto(
        url=url,
        body=body,
        mime_type=_detect_mime(body, header),
        sha256=hashlib.sha256(body).hexdigest(),
    )


async def _download_all(
    urls: Iterable[str],
    fetcher: Callable[[str], DownloadedPhoto],
) -> tuple[list[DownloadedPhoto], list[PhotoDownloadFailure]]:
    semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    async def one(url: str) -> DownloadedPhoto:
        async with semaphore:
            return await asyncio.to_thread(fetcher, url)

    source_urls = list(urls)
    downloaded: list[DownloadedPhoto] = []
    failures: list[PhotoDownloadFailure] = []
    total_bytes = 0
    # Загружаем небольшими пачками: gather всего списка удерживал бы все тела
    # до проверки общего лимита и делал сам лимит бесполезным для защиты RAM.
    for offset in range(0, len(source_urls), DOWNLOAD_CONCURRENCY):
        chunk = source_urls[offset:offset + DOWNLOAD_CONCURRENCY]
        outcomes = await asyncio.gather(
            *(one(url) for url in chunk),
            return_exceptions=True,
        )
        for chunk_index, outcome in enumerate(outcomes):
            source_index = offset + chunk_index + 1
            if isinstance(outcome, asyncio.CancelledError):
                raise outcome
            if isinstance(outcome, BaseException):
                failures.append(_download_failure(outcome, source_index))
            else:
                total_bytes += len(outcome.body)
                if total_bytes > MAX_TOTAL_BYTES:
                    raise ValueError("набор фотографий превышает лимит 15 MiB")
                downloaded.append(outcome)
    return downloaded, failures


def source_set_key(urls: Iterable[str]) -> str:
    """Быстрый ключ источников; основной content-cache остаётся по байтам."""
    canonical = json.dumps(
        {"source_urls": sorted(set(urls))},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_photos(downloaded: Iterable[DownloadedPhoto]) -> list[DownloadedPhoto]:
    """Удаляет точные дубли, сохраняя порядок фотографий в объявлении.

    Сортировка нужна только внутри content_set_key. Для модели порядок важен:
    соседние кадры часто относятся к одной комнате, а начало карточки может
    содержать фасад и общие зоны ЖК.
    """
    unique: list[DownloadedPhoto] = []
    seen: set[str] = set()
    for photo in downloaded:
        if photo.sha256 not in seen:
            seen.add(photo.sha256)
            unique.append(photo)
    return unique


def _model_photos(downloaded: Iterable[DownloadedPhoto]) -> list[dict[str, str]]:
    photos = []
    for index, photo in enumerate(downloaded, 1):
        encoded = base64.b64encode(photo.body).decode("ascii")
        photos.append({
            "photo_id": f"p{index:02d}",
            "image_url": f"data:{photo.mime_type};base64,{encoded}",
        })
    return photos


async def process_listing(
    *,
    store: RenovationStore,
    client: Any,
    listing_id: str,
    price: float | int | None,
    photo_urls: Any,
    event_reason: str | None,
    fetcher: Callable[[str], DownloadedPhoto] = download_photo,
) -> dict[str, Any]:
    try:
        urls = parse_photo_urls(photo_urls)
    except Exception as exc:
        raw_count = len(photo_urls) if isinstance(photo_urls, list) else 0
        store.save_listing_state(
            listing_id=str(listing_id), status="error",
            evaluation_key=None, source_set_key=None,
            photo_url_count=raw_count, downloaded_photo_count=0,
            failed_photo_count=raw_count, price=price,
            event_reason=event_reason,
            error=f"{type(exc).__name__}: {str(exc)[:500]}",
        )
        raise
    source_key = source_set_key(urls)
    if not urls:
        store.save_listing_state(
            listing_id=str(listing_id), status="no_photos",
            evaluation_key=None, source_set_key=source_key,
            photo_url_count=0, downloaded_photo_count=0,
            failed_photo_count=0, price=price, event_reason=event_reason,
        )
        return {
            "listing_id": str(listing_id),
            "status": "no_photos",
            "cache_hit": False,
            "api_called": False,
        }

    store.save_listing_state(
        listing_id=str(listing_id), status="pending",
        evaluation_key=None, source_set_key=source_key,
        photo_url_count=len(urls), downloaded_photo_count=0,
        failed_photo_count=0, price=price, event_reason=event_reason,
    )

    fast_cached = store.get_listing_result_by_source(
        listing_id=str(listing_id),
        source_set_key=source_key,
        evaluator_version=renovation_scorer.EVALUATOR_VERSION,
    )
    if fast_cached is not None:
        payload = fast_cached["payload"]
        photo_count = len(payload.get("photo_manifest") or [])
        store.bind_listing(
            listing_id=str(listing_id),
            evaluation_key=fast_cached["evaluation_key"],
            source_set_key=source_key,
            price=price,
            event_reason=event_reason,
        )
        store.save_listing_state(
            listing_id=str(listing_id), status="ready",
            evaluation_key=fast_cached["evaluation_key"],
            source_set_key=source_key, photo_url_count=len(urls),
            downloaded_photo_count=photo_count, failed_photo_count=0,
            price=price, event_reason=event_reason,
        )
        return {
            "listing_id": str(listing_id), "status": "ready",
            "cache_hit": True, "cache_basis": "source_urls",
            "api_called": False,
            "evaluation_key": fast_cached["evaluation_key"],
            "photo_count": photo_count, "download_failed_count": 0,
            "payload": payload,
        }

    try:
        downloaded_raw, download_failures = await _download_all(urls, fetcher)
    except Exception as exc:
        store.save_listing_state(
            listing_id=str(listing_id), status="error",
            evaluation_key=None, source_set_key=source_key,
            photo_url_count=len(urls), downloaded_photo_count=0,
            failed_photo_count=len(urls), price=price,
            event_reason=event_reason,
            error=f"{type(exc).__name__}: {str(exc)[:500]}",
        )
        raise
    downloaded = _canonical_photos(downloaded_raw)
    if not downloaded:
        terminal = bool(download_failures) and not any(
            failure.retryable for failure in download_failures
        )
        status = "photos_unavailable" if terminal else "error"
        message = f"не удалось скачать ни одной из {len(urls)} фотографий"
        store.save_listing_state(
            listing_id=str(listing_id), status=status,
            evaluation_key=None, source_set_key=source_key,
            photo_url_count=len(urls), downloaded_photo_count=0,
            failed_photo_count=len(download_failures), price=price,
            event_reason=event_reason, error=message,
        )
        if terminal:
            return {
                "listing_id": str(listing_id),
                "status": status,
                "cache_hit": False,
                "api_called": False,
                "download_failed_count": len(download_failures),
            }
        raise PhotoSetUnavailable(message)
    digests = [photo.sha256 for photo in downloaded]
    content_key = renovation_scorer.content_set_key(digests)
    result_key = renovation_scorer.evaluation_key(digests)
    cached = store.get_result(result_key)
    if cached is not None:
        binding_source_key = None if download_failures else source_key
        store.bind_listing(
            listing_id=str(listing_id),
            evaluation_key=result_key,
            source_set_key=binding_source_key,
            price=price,
            event_reason=event_reason,
        )
        status = "ready_with_download_gaps" if download_failures else "ready"
        store.save_listing_state(
            listing_id=str(listing_id), status=status,
            evaluation_key=result_key, source_set_key=binding_source_key,
            photo_url_count=len(urls), downloaded_photo_count=len(downloaded),
            failed_photo_count=len(download_failures), price=price,
            event_reason=event_reason,
        )
        return {
            "listing_id": str(listing_id),
            "status": status,
            "cache_hit": True,
            "cache_basis": "content",
            "api_called": False,
            "evaluation_key": result_key,
            "photo_count": len(downloaded),
            "download_failed_count": len(download_failures),
            "payload": cached,
        }

    api_called = False
    async with _evaluation_lock(result_key):
        stored = store.get_result(result_key)
        if stored is None:
            api_called = True
            attempt_id = store.start_attempt(result_key)
            try:
                evaluation = await renovation_scorer.evaluate(client, _model_photos(downloaded))
                payload = {
                    **evaluation,
                    "content_set_key": content_key,
                    "evaluation_key": result_key,
                    "requested_photo_count": len(urls),
                    "downloaded_photo_count": len(downloaded),
                    "photo_bytes": sum(len(photo.body) for photo in downloaded),
                    "download_failures": [
                        {
                            "source_index": failure.source_index,
                            "error": failure.error,
                            "retryable": failure.retryable,
                            "http_status": failure.http_status,
                        }
                        for failure in download_failures
                    ],
                    "photo_manifest": [
                        {"sha256": photo.sha256, "source_url": photo.url}
                        for photo in downloaded
                    ],
                }
                stored = store.save_result(
                    evaluation_key=result_key,
                    content_set_key=content_key,
                    evaluator_version=renovation_scorer.EVALUATOR_VERSION,
                    payload=payload,
                )
                store.finish_attempt(
                    attempt_id,
                    status="succeeded",
                    request_id=evaluation.get("request_id"),
                    model=evaluation.get("model"),
                    usage=evaluation.get("usage"),
                )
            except Exception as exc:
                store.finish_attempt(
                    attempt_id,
                    status="failed",
                    error=f"{type(exc).__name__}: {str(exc)[:500]}",
                )
                store.save_listing_state(
                    listing_id=str(listing_id), status="error",
                    evaluation_key=None, source_set_key=source_key,
                    photo_url_count=len(urls),
                    downloaded_photo_count=len(downloaded),
                    failed_photo_count=len(download_failures), price=price,
                    event_reason=event_reason,
                    error=f"{type(exc).__name__}: {str(exc)[:500]}",
                )
                raise

    binding_source_key = None if download_failures else source_key
    store.bind_listing(
        listing_id=str(listing_id),
        evaluation_key=result_key,
        source_set_key=binding_source_key,
        price=price,
        event_reason=event_reason,
    )
    status = "ready_with_download_gaps" if download_failures else "ready"
    store.save_listing_state(
        listing_id=str(listing_id), status=status,
        evaluation_key=result_key, source_set_key=binding_source_key,
        photo_url_count=len(urls), downloaded_photo_count=len(downloaded),
        failed_photo_count=len(download_failures), price=price,
        event_reason=event_reason,
    )
    return {
        "listing_id": str(listing_id),
        "status": status,
        "cache_hit": not api_called,
        "cache_basis": None if api_called else "content_after_wait",
        "api_called": api_called,
        "evaluation_key": result_key,
        "photo_count": len(downloaded),
        "download_failed_count": len(download_failures),
        "payload": stored,
    }
