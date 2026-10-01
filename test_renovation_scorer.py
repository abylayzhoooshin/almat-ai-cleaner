import hashlib
import json
import unittest
from types import SimpleNamespace

import renovation_scorer as scorer


PHOTOS = [
    {"photo_id": "p1", "image_url": "https://example.test/1.jpg"},
    {"photo_id": "p2", "image_url": "data:image/jpeg;base64,AA=="},
]


def valid_result(score=6.2, coverage="sufficient"):
    return {
        "score": score,
        "coverage": coverage,
        "evidence": [
            {"photo_ids": ["p1"], "observation": "Цельная спокойная отделка."},
            {"photo_ids": ["p2"], "observation": "Обстановка выглядит продуманной."},
        ],
        "limitations": [],
        "summary": "Хороший современный интерьер без уровня следующего якоря.",
    }


class RenovationScorerTests(unittest.TestCase):
    def test_request_freezes_selected_configuration(self):
        request = scorer.build_request(PHOTOS)
        self.assertEqual(request["model"], "gpt-6-luna")
        self.assertEqual(request["temperature"], 0)
        self.assertEqual(request["reasoning"], {"effort": "none"})
        self.assertEqual(request["max_output_tokens"], 1400)
        self.assertFalse(request["store"])
        self.assertEqual(request["input"][0]["content"][2]["detail"], "low")
        self.assertIn("одной цифрой после запятой", request["instructions"])
        self.assertIn("Фасад и", request["instructions"])
        self.assertIn("не повышают и не снижают score", request["instructions"])

    def test_decimal_result_is_valid(self):
        self.assertEqual(scorer.validate_result(valid_result(), ["p1", "p2"])["score"], 6.2)

    def test_more_than_one_decimal_is_rejected(self):
        with self.assertRaises(scorer.InvalidModelResponse):
            scorer.validate_result(valid_result(6.25), ["p1", "p2"])

    def test_insufficient_has_no_score(self):
        value = valid_result(None, "insufficient")
        value["evidence"] = []
        value["limitations"] = ["Нет общего вида жилой зоны."]
        self.assertIsNone(scorer.validate_result(value, ["p1", "p2"])["score"])

    def test_unknown_evidence_photo_is_rejected(self):
        value = valid_result()
        value["evidence"][0]["photo_ids"] = ["missing"]
        with self.assertRaises(scorer.InvalidModelResponse):
            scorer.validate_result(value, ["p1", "p2"])

    def test_duplicate_photo_id_is_rejected(self):
        photos = [PHOTOS[0], dict(PHOTOS[0], image_url="https://example.test/2.jpg")]
        with self.assertRaises(scorer.InvalidPhotoInput):
            scorer.build_request(photos)

    def test_content_key_ignores_order_and_exact_duplicates(self):
        first = hashlib.sha256(b"one").hexdigest()
        second = hashlib.sha256(b"two").hexdigest()
        self.assertEqual(
            scorer.content_set_key([first, second]),
            scorer.content_set_key([second, first, first]),
        )
        self.assertEqual(
            scorer.evaluation_key([first, second]),
            scorer.evaluation_key([second, first]),
        )


class FakeResponses:
    def __init__(self, response):
        self.response = response
        self.kwargs = None

    async def create(self, **kwargs):
        self.kwargs = kwargs
        return self.response


class EvaluateTests(unittest.IsolatedAsyncioTestCase):
    async def test_evaluate_returns_result_and_metadata(self):
        response = SimpleNamespace(
            status="completed",
            output_text=json.dumps(valid_result(), ensure_ascii=False),
            id="resp_1",
            model="gpt-6-luna",
            usage={"input_tokens": 10, "output_tokens": 5},
        )
        responses = FakeResponses(response)
        client = SimpleNamespace(responses=responses)
        output = await scorer.evaluate(client, PHOTOS)
        self.assertEqual(output["result"]["score"], 6.2)
        self.assertEqual(output["request_id"], "resp_1")
        self.assertEqual(output["evaluator_version"], scorer.EVALUATOR_VERSION)
        self.assertEqual(responses.kwargs["text"]["format"]["schema"], scorer.OUTPUT_SCHEMA)


if __name__ == "__main__":
    unittest.main()
