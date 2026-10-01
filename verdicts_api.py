"""
verdicts_api.py — HTTP-слой поверх cleaner_db, тем же паттерном, что
baseline_api.py в коллекторе (токен, пагинация, health с возрастом).

ДВА РАЗНЫХ ПОТРЕБИТЕЛЯ — ДВА РАЗНЫХ ЭНДПОИНТА.

/baseline/clean — основной. Отдаёт ГОТОВЫЙ чистый baseline, который
    хранится у нас и пересобирается в конце каждого цикла
    (pipeline.publish_clean_baseline). В нём только объявления, прошедшие
    разметку по текущему тексту; новые и изменённые ждут следующего цикла.

    Коллектор на запрос потребителя не дёргается: снимок согласован
    (все страницы из одной выгрузки) и доступен, даже когда коллектор
    лежит. Плата — цены на момент последнего цикла, до 12 часов давности.

/verdicts/* — служебный. Сами вердикты без данных объявлений: для
    ручной проверки качества разметки и для потребителей, которые
    предпочитают свести данные сами.
"""
import json
import os
import secrets
import time

import csv
import io

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse, Response

import cleaner_db

API_KEY = os.environ.get("CLEANER_API_KEY", "").strip()
API_ONLY = os.environ.get("CLEANER_API_ONLY", "").strip().lower() in {
    "1", "true", "yes", "on",
}
MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 200

# Порог "давно не было ни одного успешного цикла". При 12-часовом цикле
# трое суток — это шесть пропущенных подряд, то есть точно поломка, а не
# разовая неудача из-за недоступного коллектора.
STALE_AFTER_SECONDS = int(os.environ.get("CLEANER_STALE_AFTER_S", str(3 * 24 * 3600)))

LAST_CYCLE_KEY = "last_cycle"

app = FastAPI(title="rieltor-cleaner")

_started_at = time.monotonic()


def record_cycle_result(result):
    """Вызывается из service.py после каждого run_cycle().

    Пишем в БД, а не только в память: иначе после каждого редеплоя
    /health на несколько суток отвечал бы "starting" и переставал
    отличать свежий перезапуск от сломанного цикла.
    """
    with cleaner_db.connect() as conn:
        cleaner_db.set_meta(conn, LAST_CYCLE_KEY, json.dumps(result, ensure_ascii=False))
        conn.commit()


def require_api_key(x_api_key: str = Header(default="")):
    if not API_KEY:
        return
    if not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="неверный или отсутствующий X-API-Key")


# ========================= чистый baseline =========================

@app.get("/baseline/clean", dependencies=[Depends(require_api_key)])
def baseline_clean(limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
                   offset: int = Query(0, ge=0)):
    """Опубликованный чистый baseline, постранично.

    `total` — число строк в снимке, страницы полные вплоть до последней.
    `built_at` меняется при каждой пересборке: если он сменился посреди
    обхода, страницы из разных снимков — начните обход заново.
    """
    with cleaner_db.connect() as conn:
        info = cleaner_db.clean_baseline_info(conn)
        if info is None:
            # 503, а не пустой список: пустой ответ потребитель принял бы
            # за «объявлений нет» и затёр бы свои данные.
            raise HTTPException(status_code=503,
                                detail="чистый baseline ещё не собран — дождитесь первого цикла")
        total, version, built_at = info
        rows = cleaner_db.clean_baseline_page(conn, limit, offset)
    return {
        "version": version,
        "built_at": built_at,
        "total": total,
        "limit": limit,
        "offset": offset,
        "returned": len(rows),
        "rows": rows,
    }


@app.get("/baseline/clean.csv", dependencies=[Depends(require_api_key)])
def baseline_clean_csv():
    """Весь чистый baseline одним CSV-файлом (UTF-8, без BOM, разделитель — запятая).

    Для потребителя, которому нужен просто файл: никакой пагинации и
    проверки built_at, весь снимок читается одним согласованным чтением.
    Значения — текст: None приходит пустой ячейкой, списки (photo_urls) —
    JSON-строкой. Файл собирается в памяти (~30 МБ на ~13 тыс. строк).
    """
    with cleaner_db.connect() as conn:
        info = cleaner_db.clean_baseline_info(conn)
        if info is None:
            raise HTTPException(status_code=503,
                                detail="чистый baseline ещё не собран — дождитесь первого цикла")
        total, version, built_at = info

        buf = io.StringIO()
        writer = None
        for row in cleaner_db.iter_clean_baseline(conn):
            if writer is None:
                writer = csv.DictWriter(buf, fieldnames=list(row), extrasaction="ignore")
                writer.writeheader()
            writer.writerow({
                k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
                for k, v in row.items()
            })

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="clean_baseline.csv"',
            "X-Built-At": built_at,
            "X-Total-Rows": str(total),
        },
    )


# ============================= вердикты =============================

# Колонки вердикта без снимков текста: снимки нужны только для ручной
# проверки через /verdicts/review, а в постраничной выгрузке они раздували
# бы ответ на порядок (до 1000 символов описания на строку).
_VERDICT_COLUMNS = (
    "id, usable, reason_code, confidence, reason, source, model, "
    "processed_at, baseline_version"
)


@app.get("/verdicts/table", dependencies=[Depends(require_api_key)])
def table(limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
          offset: int = Query(0, ge=0),
          usable: bool = Query(None, description="только годные (true) или только отсеянные (false)"),
          reason_code: str = Query(None, description="фильтр по причине, напр. hotel_hostel")):
    with cleaner_db.connect() as conn:
        clauses, params = [], []
        if usable is not None:
            clauses.append("usable = ?")
            params.append(int(usable))
        if reason_code:
            clauses.append("reason_code = ?")
            params.append(reason_code)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        total = conn.execute(f"SELECT COUNT(*) FROM verdicts {where}", params).fetchone()[0]
        # ORDER BY по (processed_at, id), а не по одному processed_at.
        # Метка времени имеет точность до секунды, и целый batch пишется
        # одной и той же секундой: на реальных данных 1000 из 1014 строк
        # имели идентичный processed_at. Без уникального tie-break порядок
        # строк между запросами не определён, и потребитель, идущий по
        # страницам, мог бы получить дубли и пропуски.
        rows = [dict(r) for r in conn.execute(
            f"SELECT {_VERDICT_COLUMNS} FROM verdicts {where} "
            "ORDER BY processed_at, id LIMIT ? OFFSET ?",
            params + [limit, offset],
        )]
    return {"total": total, "limit": limit, "offset": offset, "returned": len(rows), "rows": rows}


@app.get("/verdicts/review", dependencies=[Depends(require_api_key)])
def review(limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
           offset: int = Query(0, ge=0),
           reason_code: str = Query(None)):
    """Отбракованные объявления вместе с текстом — для проверки глазами.

    Главный вопрос при приёмке модели: не выкидывает ли она нормальные
    квартиры. Отвечать на него, глядя на голые id и reason_code,
    невозможно, поэтому здесь отдаётся сохранённый снимок заголовка и
    описания (см. snapshot_* в cleaner_db).

    Сортировка по confidence: сначала low — там, где модель сама
    сомневалась, ложные срабатывания вероятнее всего.
    """
    with cleaner_db.connect() as conn:
        clauses = ["(usable = 0 OR usable IS NULL)"]
        params = []
        if reason_code:
            clauses.append("reason_code = ?")
            params.append(reason_code)
        where = "WHERE " + " AND ".join(clauses)
        total = conn.execute(f"SELECT COUNT(*) FROM verdicts {where}", params).fetchone()[0]
        rows = [dict(r) for r in conn.execute(
            f"""SELECT id, usable, reason_code, confidence, reason, source, model,
                       snapshot_title, snapshot_desc, snapshot_url, processed_at
                FROM verdicts {where}
                ORDER BY CASE confidence WHEN 'low' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                         processed_at, id
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        )]
    return {"total": total, "limit": limit, "offset": offset,
            "returned": len(rows), "rows": rows}


@app.get("/verdicts/meta", dependencies=[Depends(require_api_key)])
def meta():
    with cleaner_db.connect() as conn:
        stats = cleaner_db.stats(conn)
        raw, updated_at = cleaner_db.get_meta(conn, LAST_CYCLE_KEY)
    stats["last_cycle_at"] = updated_at
    stats["last_cycle_result"] = json.loads(raw) if raw else None
    return stats


@app.get("/health")
def health():
    """Как и у коллектора: не 503 на "ещё не было ни одного цикла" (иначе
    платформа перезапускала бы сервис, который просто ждёт первого
    тика) — 503 только если последний УСПЕШНЫЙ цикл был давно."""
    if API_ONLY:
        return JSONResponse({"status": "maintenance", "api_only": True}, status_code=200)

    uptime = time.monotonic() - _started_at
    with cleaner_db.connect() as conn:
        raw, updated_at = cleaner_db.get_meta(conn, LAST_CYCLE_KEY)

    if not updated_at:
        if uptime > STALE_AFTER_SECONDS:
            return JSONResponse(
                {"status": "broken", "detail": "ни одного цикла за отведённое время"},
                status_code=503,
            )
        return JSONResponse(
            {"status": "starting", "uptime_seconds": int(uptime)}, status_code=200
        )

    age = time.time() - cleaner_db.parse_iso(updated_at)
    stale = age > STALE_AFTER_SECONDS
    body = {
        "status": "stale" if stale else "ok",
        "last_cycle_age_seconds": int(age),
        "last_cycle_result": json.loads(raw) if raw else None,
    }
    return JSONResponse(body, status_code=503 if stale else 200)
