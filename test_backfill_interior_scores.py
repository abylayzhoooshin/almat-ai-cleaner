import asyncio
import csv
import hashlib
import json
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from backfill_interior_scores import (
    BackfillConfig,
    FatalAuthorizationError,
    InputValidationError,
    run_backfill,
)
from renovation_store import RenovationStore
from renovation_worker import DownloadedPhoto


PHOTO_ROOT = "https://krisha-photos.kcdn.online/"


def csv_row(listing_id, photos=None, status="active"):
    return {
        "id": listing_id,
        "status": status,
        "price": "300000",
        "photo_urls": json.dumps(photos or []),
        "title": f"Listing {listing_id}",
    }


def write_csv(path, rows):
    fields = ["id", "status", "price", "photo_urls", "title"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fake_download(url):
    body = b"\xff\xd8\xff" + url.encode("ascii")
    return DownloadedPhoto(
        url=url,
        body=body,
        mime_type="image/jpeg",
        sha256=hashlib.sha256(body).hexdigest(),
    )


class FakeResponses:
    def __init__(self, exception=None):
        self.calls = 0
        self.exception = exception

    async def create(self, **kwargs):
        self.calls += 1
        if self.exception is not None:
            raise self.exception
        photo_ids = [
            item["text"] for item in kwargs["input"][0]["content"]
            if item["type"] == "input_text" and item["text"].startswith("p")
        ]
        result = {
            "score": 6.2,
            "coverage": "partial",
            "evidence": [
                {"photo_ids": [photo_ids[0]], "observation": "Видна отделка."},
                {"photo_ids": [photo_ids[-1]], "observation": "Видна мебель."},
            ],
            "limitations": ["Показана часть квартиры."],
            "summary": "Хороший обычный интерьер.",
        }
        return SimpleNamespace(
            status="completed",
            output_text=json.dumps(result, ensure_ascii=False),
            id=f"response-{self.calls}",
            model="gpt-6-luna",
            usage={"input_tokens": 100, "output_tokens": 20},
        )


class FakeClient:
    def __init__(self, exception=None):
        self.responses = FakeResponses(exception)


class StatusError(RuntimeError):
    def __init__(self, status_code, message="provider error"):
        super().__init__(message)
        self.status_code = status_code


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.input = self.root / "frozen.csv"
        self.output = self.root / "enriched.csv"
        self.db = self.root / "state.sqlite3"
        self.report = self.root / "report.json"

    def tearDown(self):
        self.temp.cleanup()

    def config(self, *, dry=False, **changes):
        values = dict(
            input_path=self.input,
            output_path=self.output,
            db_path=self.db,
            report_path=self.report,
            concurrency=1,
            dry_run=dry,
            execute=not dry,
        )
        values.update(changes)
        return BackfillConfig(**values)

    async def test_dry_run_makes_no_network_or_api_calls(self):
        write_csv(self.input, [csv_row("1", [PHOTO_ROOT + "1.jpg"])])

        def forbidden_fetch(_url):
            self.fail("dry-run tried to download")

        client = FakeClient()
        result = await run_backfill(self.config(dry=True), client=client, fetcher=forbidden_fetch)
        self.assertEqual(result["input_analysis"]["rows_with_photos"], 1)
        self.assertEqual(client.responses.calls, 0)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.output.exists())

    async def test_input_can_never_be_overwritten(self):
        write_csv(self.input, [csv_row("1")])
        with self.assertRaises(InputValidationError):
            await run_backfill(self.config(output_path=self.input), client=FakeClient())
        with self.input.open(encoding="utf-8") as handle:
            self.assertEqual(len(list(csv.DictReader(handle))), 1)

    async def test_all_artifact_paths_must_be_distinct(self):
        write_csv(self.input, [csv_row("1")])
        for change in (
            {"report_path": self.input},
            {"report_path": self.output},
            {"db_path": self.output},
            {"db_path": self.report},
        ):
            with self.subTest(change=change):
                with self.assertRaises(InputValidationError):
                    await run_backfill(self.config(**change), client=FakeClient())

    async def test_join_preserves_row_count_and_order(self):
        rows = [csv_row("3"), csv_row("1"), csv_row("2")]
        write_csv(self.input, rows)
        await run_backfill(self.config(), client=FakeClient(), fetcher=fake_download)
        with self.output.open(encoding="utf-8", newline="") as handle:
            output = list(csv.DictReader(handle))
        self.assertEqual([row["id"] for row in output], ["3", "1", "2"])
        self.assertTrue(all(row["interior_status"] == "no_photos" for row in output))

    async def test_duplicate_and_empty_ids_stop_before_paid_run(self):
        for rows in (
            [csv_row("1"), csv_row("1")],
            [csv_row("")],
        ):
            write_csv(self.input, rows)
            client = FakeClient()
            with self.assertRaises(InputValidationError):
                await run_backfill(self.config(), client=client, fetcher=fake_download)
            self.assertEqual(client.responses.calls, 0)
            self.assertFalse(self.db.exists())

    async def test_missing_status_with_photos_is_processed(self):
        write_csv(self.input, [csv_row("missing", [PHOTO_ROOT + "1.jpg"], status="missing")])
        client = FakeClient()
        report = await run_backfill(self.config(), client=client, fetcher=fake_download)
        self.assertEqual(client.responses.calls, 1)
        self.assertEqual(report["status_counts"], {"ready": 1})

    async def test_no_photos_is_terminal_without_api(self):
        write_csv(self.input, [csv_row("empty")])
        client = FakeClient()
        report = await run_backfill(self.config(), client=client, fetcher=fake_download)
        self.assertEqual(client.responses.calls, 0)
        self.assertEqual(report["status_counts"], {"no_photos": 1})
        self.assertEqual(report["api"]["attempts"], 0)

    async def test_partial_404_is_ready_with_download_gaps(self):
        write_csv(self.input, [csv_row("partial", [PHOTO_ROOT + "ok.jpg", PHOTO_ROOT + "gone.jpg"])])

        def partial_fetch(url):
            if url.endswith("gone.jpg"):
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            return fake_download(url)

        report = await run_backfill(self.config(), client=FakeClient(), fetcher=partial_fetch)
        self.assertEqual(report["status_counts"], {"ready_with_download_gaps": 1})
        self.assertEqual(report["photos"]["unavailable"], 1)

    async def test_invalid_photo_urls_marks_only_that_row_as_error(self):
        rows = [
            csv_row("bad", ["https://example.test/not-allowed.jpg"]),
            csv_row("good", [PHOTO_ROOT + "good.jpg"]),
        ]
        write_csv(self.input, rows)
        client = FakeClient()
        report = await run_backfill(self.config(), client=client, fetcher=fake_download)
        self.assertEqual(report["status_counts"], {"error": 1, "ready": 1})
        self.assertEqual(client.responses.calls, 1)
        self.assertTrue(self.output.exists())

    async def test_all_404_or_410_is_photos_unavailable_without_api(self):
        write_csv(self.input, [csv_row("gone", [PHOTO_ROOT + "a.jpg", PHOTO_ROOT + "b.jpg"])])
        client = FakeClient()

        def gone(url):
            code = 404 if url.endswith("a.jpg") else 410
            raise urllib.error.HTTPError(url, code, "Gone", {}, None)

        report = await run_backfill(self.config(), client=client, fetcher=gone)
        self.assertEqual(report["status_counts"], {"photos_unavailable": 1})
        self.assertEqual(client.responses.calls, 0)

    async def test_temporary_download_error_retries_with_limit(self):
        write_csv(self.input, [csv_row("retry", [PHOTO_ROOT + "retry.jpg"])])
        calls = 0
        sleeps = []

        def flaky(url):
            nonlocal calls
            calls += 1
            if calls < 3:
                raise OSError("temporary CDN failure")
            return fake_download(url)

        async def no_wait(delay):
            sleeps.append(delay)

        report = await run_backfill(
            self.config(), client=FakeClient(), fetcher=flaky,
            sleep=no_wait, random_value=lambda: 0,
        )
        self.assertEqual(calls, 3)
        self.assertEqual(sleeps, [1, 2])
        self.assertEqual(report["status_counts"], {"ready": 1})

    async def test_openai_401_or_403_aborts_batch_and_preserves_output(self):
        for code in (401, 403):
            with self.subTest(code=code):
                secret = f"rk-auth-secret-{code}"
                database = self.root / f"state-{code}.sqlite3"
                report = self.root / f"report-{code}.json"
                output = self.root / f"output-{code}.csv"
                output.write_text("previous output", encoding="utf-8")
                write_csv(self.input, [csv_row("1", [PHOTO_ROOT + "1.jpg"])])
                config = self.config(db_path=database, report_path=report, output_path=output)
                with mock.patch.dict(os.environ, {"RENOVATION_OPENAI_API_KEY": secret}):
                    with self.assertRaises(FatalAuthorizationError):
                        await run_backfill(
                            config,
                            client=FakeClient(StatusError(code, f"denied {secret}")),
                            fetcher=fake_download,
                        )
                self.assertEqual(output.read_text(encoding="utf-8"), "previous output")
                self.assertTrue(json.loads(report.read_text(encoding="utf-8"))["aborted"])
                self.assertNotIn(secret.encode(), database.read_bytes())
                self.assertNotIn(secret, report.read_text(encoding="utf-8"))

    async def test_restart_resumes_pending_and_error_but_skips_ready(self):
        rows = [
            csv_row("ready", [PHOTO_ROOT + "ready.jpg"]),
            csv_row("pending", [PHOTO_ROOT + "pending.jpg"]),
            csv_row("error", [PHOTO_ROOT + "error.jpg"]),
        ]
        write_csv(self.input, rows)
        first_client = FakeClient()
        await run_backfill(self.config(), client=first_client, fetcher=fake_download)
        with RenovationStore(self.db) as store:
            store.start_attempt("stale-running-attempt")
            for listing_id, status in (("pending", "pending"), ("error", "error")):
                state = store.get_listing_state(listing_id)
                store.save_listing_state(
                    listing_id=listing_id, status=status,
                    evaluation_key=state["evaluation_key"], source_set_key=state["source_set_key"],
                    photo_url_count=1, downloaded_photo_count=0, failed_photo_count=0,
                    price=300000, event_reason="restart", error="interrupted",
                )
                store.connection.execute(
                    "DELETE FROM listing_results WHERE listing_id = ?", (listing_id,)
                )
            store.connection.commit()
        downloads = []

        def counted(url):
            downloads.append(url)
            return fake_download(url)

        second = await run_backfill(self.config(), client=FakeClient(), fetcher=counted)
        self.assertEqual(len(downloads), 2)
        self.assertEqual(second["api"]["attempts"], 0)
        self.assertEqual(second["status_counts"], {"ready": 3})
        with RenovationStore(self.db) as store:
            statuses = [row[0] for row in store.connection.execute(
                "SELECT status FROM attempts WHERE evaluation_key = 'stale-running-attempt'"
            )]
        self.assertEqual(statuses, ["interrupted"])

    async def test_repeated_input_adds_zero_attempts_and_downloads(self):
        write_csv(self.input, [csv_row("same", [PHOTO_ROOT + "same.jpg"])])
        await run_backfill(self.config(), client=FakeClient(), fetcher=fake_download)

        def forbidden(_url):
            self.fail("terminal row was downloaded again")

        report = await run_backfill(self.config(), client=FakeClient(), fetcher=forbidden)
        self.assertEqual(report["api"]["attempts"], 0)
        self.assertEqual(report["api"]["cache_hits"], 1)
        self.assertEqual(report["photos"]["downloaded"], 0)
        self.assertEqual(report["photos"]["unavailable"], 0)

    async def test_missing_final_state_aborts_without_replacing_output(self):
        write_csv(self.input, [csv_row("lost", [PHOTO_ROOT + "lost.jpg"])])
        self.output.write_text("previous", encoding="utf-8")
        with mock.patch(
            "backfill_interior_scores.renovation_worker.process_listing",
            side_effect=RuntimeError("failed before state persistence"),
        ):
            with self.assertRaises(FatalAuthorizationError):
                await run_backfill(self.config(), client=FakeClient(), fetcher=fake_download)
        self.assertEqual(self.output.read_text(encoding="utf-8"), "previous")

    async def test_output_is_replaced_only_after_successful_validation(self):
        write_csv(self.input, [csv_row("1")])
        self.output.write_text("old", encoding="utf-8")
        with mock.patch("backfill_interior_scores._read_csv", side_effect=[
            (["id", "status", "price", "photo_urls", "title"], [csv_row("1")]),
            InputValidationError("broken temporary output"),
        ]):
            with self.assertRaises(InputValidationError):
                await run_backfill(self.config(), client=FakeClient(), fetcher=fake_download)
        self.assertEqual(self.output.read_text(encoding="utf-8"), "old")

    async def test_secret_is_absent_from_output_report_database_and_stdout_data(self):
        secret = "rk-test-secret-must-never-be-written"
        write_csv(self.input, [csv_row("1", [PHOTO_ROOT + "1.jpg"])])
        with mock.patch.dict(os.environ, {"RENOVATION_OPENAI_API_KEY": secret}):
            await run_backfill(self.config(), client=FakeClient(), fetcher=fake_download)
        for path in (self.output, self.report, self.db):
            self.assertNotIn(secret.encode(), path.read_bytes())
        self.assertNotIn("data:image/", self.report.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
