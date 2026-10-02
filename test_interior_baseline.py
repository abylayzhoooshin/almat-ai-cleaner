import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import cleaner_db
import interior_baseline
import renovation_scorer
from import_interior_baseline import ImportError, import_completed_backfill


PHOTO_A = "https://krisha-photos.kcdn.online/a.jpg"
PHOTO_B = "https://krisha-photos.kcdn.online/b.jpg"


class InteriorBaselineTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.old_db_path = cleaner_db.DB_PATH
        cleaner_db.DB_PATH = str(self.root / "cleaner.db")

    def tearDown(self):
        cleaner_db.DB_PATH = self.old_db_path
        self.temporary.cleanup()

    def _seed_snapshot(self):
        rows = [
            {
                "id": "1", "price": 100, "photo_urls": [PHOTO_A],
                "photo_set_hash": interior_baseline.photo_set_hash([PHOTO_A]),
            },
            {"id": "2", "price": 200, "photo_urls": [], "photo_set_hash": None},
        ]
        with cleaner_db.connect() as conn:
            cleaner_db.replace_clean_baseline(conn, rows, "v1")
            conn.commit()
        return rows

    def _write_import(self, ids=("1", "2")):
        path = self.root / "enriched.csv"
        fields = [
            "id", "photo_urls", "photo_set_hash", "interior_status", "interior_score",
            "interior_coverage", "interior_evaluator_version",
            "interior_evaluation_key", "interior_content_set_key",
            "interior_evaluated_at",
        ]
        source = {
            "1": {
                "id": "1", "photo_urls": json.dumps([PHOTO_A]),
                "photo_set_hash": interior_baseline.photo_set_hash([PHOTO_A]),
                "interior_status": "ready", "interior_score": "6.4",
                "interior_coverage": "sufficient",
                "interior_evaluator_version": renovation_scorer.EVALUATOR_VERSION,
                "interior_evaluation_key": "evaluation-1",
                "interior_content_set_key": "content-1",
                "interior_evaluated_at": "2026-10-02T00:00:00+00:00",
            },
            "2": {
                "id": "2", "photo_urls": "[]", "photo_set_hash": "",
                "interior_status": "no_photos", "interior_score": "",
                "interior_coverage": "", "interior_evaluator_version": "",
                "interior_evaluation_key": "", "interior_content_set_key": "",
                "interior_evaluated_at": "2026-10-02T00:00:00+00:00",
            },
        }
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(source[listing_id] for listing_id in ids)
        return path

    def test_photo_hash_matches_collector_contract(self):
        urls = [PHOTO_B, PHOTO_A, PHOTO_A]
        expected = hashlib.sha256(
            "|".join(sorted(urls)).encode("utf-8")
        ).hexdigest()
        self.assertEqual(expected, interior_baseline.photo_set_hash(urls))
        self.assertEqual(expected, interior_baseline.photo_set_hash(list(reversed(urls))))
        self.assertIsNone(interior_baseline.photo_set_hash([]))

    def test_import_and_next_rebuild_preserve_matching_score(self):
        rows = self._seed_snapshot()
        path = self._write_import()

        dry = import_completed_backfill(path, execute=False)
        self.assertFalse(dry["executed"])
        with cleaner_db.connect() as conn:
            self.assertNotIn("interior_status", next(cleaner_db.iter_clean_baseline(conn)))

        result = import_completed_backfill(path, execute=True)
        self.assertEqual(2, result["rows"])
        with cleaner_db.connect() as conn:
            imported = list(cleaner_db.iter_clean_baseline(conn))
            previous = cleaner_db.clean_baseline_interior_map(conn)

        self.assertEqual(6.4, imported[0]["interior_score"])
        self.assertEqual(
            renovation_scorer.EVALUATOR_VERSION,
            imported[1]["interior_evaluator_version"],
        )
        rebuilt = interior_baseline.enrich_rows(
            [{**rows[0], "price": 90}, rows[1]], previous
        )
        self.assertEqual(6.4, rebuilt[0]["interior_score"])
        self.assertEqual("no_photos", rebuilt[1]["interior_status"])

        changed = interior_baseline.enrich_rows(
            [{**rows[0], "photo_urls": [PHOTO_B]}], previous
        )
        self.assertIsNone(changed[0]["interior_status"])
        self.assertIsNone(changed[0]["interior_score"])

    def test_import_rejects_different_id_order(self):
        self._seed_snapshot()
        path = self._write_import(ids=("2", "1"))
        with self.assertRaises(ImportError):
            import_completed_backfill(path, execute=True)


if __name__ == "__main__":
    unittest.main()
