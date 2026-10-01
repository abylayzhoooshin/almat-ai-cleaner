import hashlib
import json
import asyncio
import tempfile
import unittest
import urllib.error
from unittest import mock
from pathlib import Path
from types import SimpleNamespace

from renovation_store import RenovationStore
from renovation_worker import (
    DownloadedPhoto,
    _canonical_photos,
    _download_all,
    parse_photo_urls,
    process_listing,
)


class FakeResponses:
    def __init__(self, delay=0):
        self.calls = 0
        self.delay = delay

    async def create(self, **kwargs):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        photo_ids = [
            item["text"]
            for item in kwargs["input"][0]["content"]
            if item["type"] == "input_text" and item["text"].startswith("p")
        ]
        result = {
            "score": 6.2,
            "coverage": "sufficient",
            "evidence": [
                {"photo_ids": [photo_ids[0]], "observation": "Цельная отделка."},
                {"photo_ids": [photo_ids[-1]], "observation": "Продуманная обстановка."},
            ],
            "limitations": [],
            "summary": "Уверенный современный уровень.",
        }
        return SimpleNamespace(
            status="completed",
            output_text=json.dumps(result, ensure_ascii=False),
            id="resp_test",
            model="gpt-6-luna",
            usage={"input_tokens": 10, "output_tokens": 5},
        )


def fake_download(url):
    body = b"\xff\xd8\xff" + url.encode("ascii")
    return DownloadedPhoto(
        url=url,
        body=body,
        mime_type="image/jpeg",
        sha256=hashlib.sha256(body).hexdigest(),
    )


class WorkerCacheTests(unittest.IsolatedAsyncioTestCase):
    def test_placeholder_is_not_treated_as_photo(self):
        self.assertEqual(parse_photo_urls([
            "//krisha.kz/static/frontend/images/photo-moderation-big.png"
        ]), [])

    def test_unknown_photo_host_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_photo_urls(["https://example.test/not-a-krisha-photo.jpg"])

    async def test_total_limit_stops_before_remaining_chunks(self):
        calls = []

        def large_fake(url):
            calls.append(url)
            body = b"x" * 8
            return DownloadedPhoto(
                url=url, body=body, mime_type="image/jpeg",
                sha256=hashlib.sha256(body + url.encode()).hexdigest(),
            )

        urls = [f"https://krisha-photos.kcdn.online/{n}.jpg" for n in range(5)]
        with mock.patch("renovation_worker.MAX_TOTAL_BYTES", 10):
            with self.assertRaises(ValueError):
                await _download_all(urls, large_fake)
        self.assertEqual(len(calls), 2)

    async def test_invalid_photo_input_is_persisted_as_error(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)
            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                with self.assertRaises(ValueError):
                    await process_listing(
                        store=store, client=client, listing_id="bad-url",
                        price=300_000,
                        photo_urls=["https://example.test/not-a-photo.jpg"],
                        event_reason="new", fetcher=fake_download,
                    )
                state = store.get_listing_state("bad-url")
                self.assertEqual(state["status"], "error")
                self.assertEqual(state["photo_url_count"], 1)
                self.assertEqual(client_responses.calls, 0)

    def test_exact_duplicates_are_removed_without_reordering(self):
        first = fake_download("https://krisha-photos.kcdn.online/first.jpg")
        second = fake_download("https://krisha-photos.kcdn.online/second.jpg")
        selected = _canonical_photos([second, first, second])
        self.assertEqual([photo.url for photo in selected], [second.url, first.url])

    async def test_price_drop_reuses_result_without_api_call(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)
            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                downloads = []

                def counted_download(url):
                    downloads.append(url)
                    return fake_download(url)

                first = await process_listing(
                    store=store,
                    client=client,
                    listing_id="123",
                    price=300_000,
                    photo_urls=["https://krisha-photos.kcdn.online/1.jpg", "https://krisha-photos.kcdn.online/2.jpg"],
                    event_reason="new",
                    fetcher=counted_download,
                )
                second = await process_listing(
                    store=store,
                    client=client,
                    listing_id="123",
                    price=280_000,
                    photo_urls=["https://krisha-photos.kcdn.online/2.jpg", "https://krisha-photos.kcdn.online/1.jpg"],
                    event_reason="price_drop",
                    fetcher=counted_download,
                )
                self.assertFalse(first["cache_hit"])
                self.assertTrue(second["cache_hit"])
                self.assertFalse(second["api_called"])
                self.assertEqual(client_responses.calls, 1)
                self.assertEqual(store.api_attempt_count(), 1)
                self.assertEqual(store.get_listing_binding("123")["last_price"], 280_000)
                self.assertEqual(first["evaluation_key"], second["evaluation_key"])
                self.assertEqual(second["cache_basis"], "source_urls")
                self.assertEqual(len(downloads), 2)
                self.assertEqual(store.get_listing_state("123")["status"], "ready")
                exported = store.iter_baseline_exports()
                self.assertEqual(len(exported), 1)
                self.assertEqual(exported[0]["listing_id"], "123")
                self.assertEqual(exported[0]["interior_status"], "ready")
                self.assertEqual(exported[0]["interior_score"], 6.2)
                self.assertEqual(exported[0]["interior_coverage"], "sufficient")
                self.assertEqual(
                    exported[0]["interior_evaluator_version"],
                    "simple-decimal-v4-original-gpt6-luna",
                )
                self.assertIsNotNone(exported[0]["interior_evaluated_at"])

    async def test_one_failed_download_keeps_available_photos(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)

            def sometimes_fails(url):
                if url.endswith("missing.jpg"):
                    raise OSError("404")
                return fake_download(url)

            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                result = await process_listing(
                    store=store, client=client, listing_id="partial-download",
                    price=300_000,
                    photo_urls=[
                        "https://krisha-photos.kcdn.online/1.jpg",
                        "https://krisha-photos.kcdn.online/missing.jpg",
                        "https://krisha-photos.kcdn.online/2.jpg",
                    ],
                    event_reason="new", fetcher=sometimes_fails,
                )
                self.assertEqual(result["status"], "ready_with_download_gaps")
                self.assertEqual(result["photo_count"], 2)
                self.assertEqual(result["download_failed_count"], 1)
                self.assertEqual(client_responses.calls, 1)
                state = store.get_listing_state("partial-download")
                self.assertEqual(state["status"], "ready_with_download_gaps")
                self.assertEqual(state["failed_photo_count"], 1)
                # Не закрепляем быстрый URL-кэш: пропавшее фото может ожить.
                self.assertIsNone(store.get_listing_binding("partial-download")["source_set_key"])

    async def test_all_failed_downloads_are_persisted_as_error(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)

            def always_fails(url):
                raise OSError("CDN unavailable")

            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                with self.assertRaises(Exception):
                    await process_listing(
                        store=store, client=client, listing_id="all-failed",
                        price=300_000,
                        photo_urls=["https://krisha-photos.kcdn.online/1.jpg"],
                        event_reason="new", fetcher=always_fails,
                    )
                self.assertEqual(client_responses.calls, 0)
                self.assertEqual(store.get_listing_state("all-failed")["status"], "error")

    async def test_all_permanent_404s_are_terminal_without_api_call(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)

            def gone(url):
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                result = await process_listing(
                    store=store, client=client, listing_id="deleted-card",
                    price=300_000,
                    photo_urls=[
                        "https://krisha-photos.kcdn.online/old-1.jpg",
                        "https://krisha-photos.kcdn.online/old-2.jpg",
                    ],
                    event_reason="baseline", fetcher=gone,
                )
                self.assertEqual(result["status"], "photos_unavailable")
                self.assertFalse(result["api_called"])
                self.assertEqual(client_responses.calls, 0)
                state = store.get_listing_state("deleted-card")
                self.assertEqual(state["status"], "photos_unavailable")
                exported = store.iter_baseline_exports()[0]
                self.assertEqual(exported["interior_status"], "photos_unavailable")
                self.assertIsNone(exported["interior_score"])

    async def test_concurrent_equal_content_calls_api_once(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses(delay=0.03)
            client = SimpleNamespace(responses=client_responses)
            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                common = dict(
                    store=store, client=client, price=300_000,
                    photo_urls=[
                        "https://krisha-photos.kcdn.online/1.jpg",
                        "https://krisha-photos.kcdn.online/2.jpg",
                    ],
                    event_reason="new", fetcher=fake_download,
                )
                first, second = await asyncio.gather(
                    process_listing(listing_id="a", **common),
                    process_listing(listing_id="b", **common),
                )
                self.assertEqual(client_responses.calls, 1)
                self.assertEqual(sum((first["api_called"], second["api_called"])), 1)
                self.assertEqual(store.api_attempt_count(), 1)

    async def test_no_photos_has_durable_state(self):
        with tempfile.TemporaryDirectory() as directory:
            client_responses = FakeResponses()
            client = SimpleNamespace(responses=client_responses)
            with RenovationStore(Path(directory) / "renovation.sqlite3") as store:
                result = await process_listing(
                    store=store, client=client, listing_id="empty",
                    price=300_000, photo_urls=[], event_reason="new",
                    fetcher=fake_download,
                )
                self.assertEqual(result["status"], "no_photos")
                self.assertEqual(store.get_listing_state("empty")["status"], "no_photos")
                self.assertEqual(client_responses.calls, 0)
                exported = store.iter_baseline_exports()[0]
                self.assertEqual(exported["interior_status"], "no_photos")
                self.assertIsNone(exported["interior_score"])
                self.assertIsNone(exported["interior_evaluator_version"])
                self.assertIsNotNone(exported["interior_evaluated_at"])


if __name__ == "__main__":
    unittest.main()
