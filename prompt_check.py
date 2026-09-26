"""Регрессионная проверка системного промпта на размеченных случаях (prompt_cases.json).

Запуск:  python prompt_check.py
Нужны OPENAI_API_KEY и OPENAI_MODEL (та же модель, что в проде, иначе проверка
бессмысленна). Один прогон — несколько запросов, копейки. Код возврата 1, если
не сошёлся хотя бы один не-"soft" случай.

Каждая правка SYSTEM_PROMPT должна проходить эту проверку до заливки.
"""
import json
import os
import sys
import time

import requests

import openai_batch

CASES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompt_cases.json")

BASE_ROW = {
    "title": "1-комнатная квартира · 40 м² · 5/9 этаж",
    "rooms": 1, "square_m2": 40.0, "floor": 5, "floor_total": 9, "price": 200000,
    "rent_renovation": None, "furniture": None, "priv_dorm": None,
}


def build_row(i, case):
    row = dict(BASE_ROW)
    row.update(case.get("fields") or {})
    if case.get("title"):
        row["title"] = case["title"]
    row["id"] = "C%03d" % i
    row["full_description"] = case["text"]
    return row


def call(group):
    body = openai_batch.build_request_line([(r["id"], r) for r in group])["body"]
    for _ in range(4):
        try:
            r = requests.post(openai_batch.API_BASE + "/chat/completions",
                              headers=openai_batch._headers(), json=body, timeout=180)
            r.raise_for_status()
            break
        except requests.RequestException:
            time.sleep(3)
    else:
        sys.exit("OpenAI недоступен")
    data = r.json()
    items = json.loads(data["choices"][0]["message"]["content"])["verdicts"]
    return {str(v.get("id")): v for v in items}, data["usage"]


def main():
    if not openai_batch.API_KEY:
        sys.exit("OPENAI_API_KEY не задан")
    cases = json.load(open(CASES_PATH, encoding="utf-8"))
    rows = [build_row(i, c) for i, c in enumerate(cases)]
    print("модель: %s | промпт: %d символов | случаев: %d" % (
        openai_batch.MODEL, len(openai_batch.SYSTEM_PROMPT), len(cases)))

    got, tokens_in, tokens_out = {}, 0, 0
    for i in range(0, len(rows), openai_batch.GROUP_SIZE):
        res, usage = call(rows[i:i + openai_batch.GROUP_SIZE])
        got.update(res)
        tokens_in += usage["prompt_tokens"]
        tokens_out += usage["completion_tokens"]

    hard_fail = soft_fail = 0
    for row, case in zip(rows, cases):
        raw = got.get(row["id"])
        v = openai_batch._validate_verdict(raw) if raw is not None else None
        expect = case["expect"]
        if v is None or v["source"] != "llm":
            ok, seen = False, "нет ответа/невалидно: %s" % (raw,)
        elif expect == "ok":
            ok, seen = v["usable"] is True, "usable=%s %s" % (v["usable"], v["reason_code"])
        else:
            ok, seen = v["usable"] is False and v["reason_code"] in expect, \
                "usable=%s %s" % (v["usable"], v["reason_code"])
        if not ok:
            if case.get("soft"):
                soft_fail += 1
            else:
                hard_fail += 1
        print("%-5s %-58s %s" % ("OK" if ok else ("soft" if case.get("soft") else "FAIL"),
                                 case["name"][:58], "" if ok else "ждали %s, получили %s" % (expect, seen)))

    print("\nтокенов: %d вход / %d выход" % (tokens_in, tokens_out))
    print("провалов: %d, спорных (soft): %d из %d" % (hard_fail, soft_fail, len(cases)))
    sys.exit(1 if hard_fail else 0)


if __name__ == "__main__":
    main()
