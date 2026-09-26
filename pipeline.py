"""
pipeline.py — один цикл очистки: забрать готовые batch-результаты, найти
новое/изменённое у коллектора, прогнать через бесплатный слой, остаток
отправить новыми batch-заданиями.

ПОРЯДОК ДЕЙСТВИЙ В КАЖДОМ ЦИКЛЕ — И ПОЧЕМУ ИМЕННО ТАКОЙ.

1. Сначала проверяем ЗАВИСШИЕ batch-задания с прошлого цикла.
   Batch асинхронный (до 24ч), поэтому предыдущий запуск почти
   наверняка что-то отправил и ушёл, не дождавшись ответа. Если не
   собирать результаты в начале следующего цикла — они бы накапливались
   и никогда не попадали в verdicts.

2. Потом — новый снимок у коллектора + дифф по content_hash.
   Намеренно после сбора старых результатов, не до: если сбор упадёт
   (сеть, квота), мы всё равно узнаем актуальное состояние очереди, а
   не отправим второй batch поверх ещё не собранного первого.

3. Бесплатный слой (heuristics) — сразу пишем вердикт, ИИ не трогаем.

4. Остаток — крупными пакетами по MAX_BATCH_SIZE, до MAX_BATCHES_PER_CYCLE
   заданий за цикл. Не по одному объявлению на запрос и не одним
   гигантским файлом на всю базу: первое дорого (системный промпт
   отправлялся бы на каждое объявление), второе складывает все яйца в
   одну корзину — отказ провайдера заблокировал бы разом всю базу.
   Подробнее — в docstring submit_new_batches.

ЧТО, ЕСЛИ ОСТАТОК ПУСТ ИЛИ ОЧЕНЬ МАЛ.
Не отправляем batch на 1-2 объявления — не потому что дорого (копейки),
а потому что каждое задание — это отдельный файл и отдельный объект
отслеживания в batches, а вертикально система рассчитана на пакеты.
Порог настраиваемый (MIN_BATCH_SIZE); если объявлений мало, они просто
подождут следующего цикла и наберут пакет побольше.
"""
import logging
import os

import requests

import cleaner_db
import collector_client
import heuristics
import openai_batch

log = logging.getLogger("pipeline")

MIN_BATCH_SIZE = int(os.environ.get("MIN_BATCH_SIZE", "5"))
MAX_BATCH_SIZE = int(os.environ.get("MAX_BATCH_SIZE", "3000"))

# Сколько batch-заданий разрешено отправить за ОДИН цикл.
#
# MAX_BATCH_SIZE * MAX_BATCHES_PER_CYCLE = потолок объявлений за цикл.
# По умолчанию 3000*6 = 18000, то есть стартовая база (~13 тыс.) уходит в
# разметку целиком на первом же запуске, а не растягивается на дни при
# 12-часовом интервале. Резать на куски всё равно нужно: см. комментарий
# к submit_new_batches про радиус поражения при отказе провайдера.
MAX_BATCHES_PER_CYCLE = int(os.environ.get("MAX_BATCHES_PER_CYCLE", "6"))

# ПИЛОТНЫЙ ЛИМИТ: сколько ВСЕГО объявлений разрешено отдать в ИИ за всё
# время, пока не убедимся, что модель размечает адекватно.
#
# MAX_BATCH_SIZE для этого не годится — он ограничивает ОДНУ отправку,
# а каждый следующий цикл берёт новую порцию: при MAX_BATCH_SIZE=200 и
# часовом интервале вся база разметится за сутки, пока человек ещё
# смотрит результаты первой сотни. Проверено: три цикла подряд дали
# 200+200+200, а не 200 и стоп.
#
# 0 = без лимита (боевой режим). Ставить число имеет смысл ровно на
# время приёмки: разметили N, посмотрели /verdicts/review, и только
# потом снимаем ограничение.
PILOT_LIMIT = int(os.environ.get("PILOT_LIMIT", "0"))

# Сколько раз пробовать объявление, на котором ИИ споткнулся, прежде
# чем перестать (см. ingest_completed_batches).
MAX_LLM_ATTEMPTS = int(os.environ.get("MAX_LLM_ATTEMPTS", "3"))


def _rebuild_group_map(listing_ids):
    """{custom_id: [listing_id, ...]} — восстанавливает разбиение на
    группы ровно так же, как его делал build_jsonl при отправке.

    Порядок id в batches.listing_ids сохраняется с момента отправки
    (JSON-объект в Python 3.7+ сохраняет порядок вставки), а размер
    группы берётся из той же константы, поэтому разбиение совпадает.
    Если GROUP_SIZE изменят между отправкой и получением ответа,
    восстановление разъедется — тогда часть объявлений получит
    llm_failed и уйдёт на переотправку. Неприятно, но не теряется.
    """
    gmap = {}
    gs = openai_batch.GROUP_SIZE
    for i in range(0, len(listing_ids), gs):
        chunk = listing_ids[i:i + gs]
        gmap[f"grp_{chunk[0]}"] = chunk
    return gmap


def ingest_completed_batches(conn, fresh_rows=None):
    """Проверяет все pending batch-задания, забирает готовые результаты."""
    pending = cleaner_db.pending_batches(conn)
    if not pending:
        return 0
    log.info("проверяю %s batch-заданий в ожидании", len(pending))

    ingested = 0
    for b in pending:
        try:
            status, output_file_id, error_file_id, error = openai_batch.check_batch(
                b["batch_id"])
        except Exception:
            log.exception("не удалось проверить статус batch %s", b["batch_id"])
            continue

        # Часть запросов внутри завершённого batch могла провалиться —
        # у OpenAI это НЕ делает batch неуспешным. Причина лежит только
        # в error-файле, вытаскиваем её в лог.
        if error_file_id:
            log.error("batch %s: часть запросов отклонена OpenAI — %s",
                      b["batch_id"], openai_batch.describe_errors(error_file_id))

        if status == "completed" and not output_file_id:
            # Ни одного успешного запроса. Раньше код всё равно шёл
            # скачивать результат по output_file_id=None, получал 404,
            # ловил исключение и оставлял batch в статусе pending —
            # НАВСЕГДА. А его объявления навсегда оставались в
            # in_flight_ids, то есть исключались из обработки: сервис
            # выглядел здоровым и при этом не делал ничего.
            log.error(
                "batch %s: завершён без единого успешного ответа. Помечаю failed, "
                "его объявления вернутся в очередь следующим циклом.",
                b["batch_id"])
            cleaner_db.mark_batch(conn, b["batch_id"], "failed",
                                  error=openai_batch.describe_errors(error_file_id)
                                  or "completed без output_file_id")
            conn.commit()
            continue

        if status == "completed":
            import json as _json
            try:
                parsed = _json.loads(b["listing_ids"])
            except (ValueError, TypeError):
                # Та же защита, что в cleaner_db.in_flight_ids: битая
                # запись не должна ронять разбор ОСТАЛЬНЫХ batch-заданий
                # в этом же цикле.
                log.error("batch %s: listing_ids не читается — пропускаю запись",
                          b["batch_id"])
                cleaner_db.mark_batch(conn, b["batch_id"], "failed",
                                      error="listing_ids повреждён")
                conn.commit()
                continue

            if isinstance(parsed, dict):
                listing_hashes = parsed
            else:
                # Старый формат (список id без хэшей). Хэша нет — пишем
                # пустую строку, и следующий дифф-цикл переотправит
                # объявление. Дороже, но честнее, чем сохранить
                # неправильный хэш и считать объявление обработанным.
                listing_hashes = {lid: "" for lid in parsed}

            ingested_before = ingested
            group_map = _rebuild_group_map(list(listing_hashes.keys()))

            try:
                results = openai_batch.download_results(output_file_id,
                                                        group_map=group_map)
            except requests.HTTPError as exc:
                # 4xx — файла нет и не будет (удалён, чужой, истёк).
                # Держать batch в pending бессмысленно: его объявления
                # так и останутся заблокированными в in_flight.
                code = exc.response.status_code if exc.response is not None else None
                if code and 400 <= code < 500:
                    log.error("batch %s: результат недоступен (HTTP %s) — помечаю "
                              "failed, объявления вернутся в очередь", b["batch_id"], code)
                    cleaner_db.mark_batch(conn, b["batch_id"], "failed",
                                          error=f"download {code}")
                    conn.commit()
                else:
                    log.exception("не удалось скачать результат batch %s (попробую "
                                  "в следующем цикле)", b["batch_id"])
                continue
            except Exception:
                # Сеть моргнула — это лечится повтором, batch оставляем pending.
                log.exception("не удалось скачать результат batch %s (попробую "
                              "в следующем цикле)", b["batch_id"])
                continue

            # Карта id -> строка объявления для снимков отбракованных.
            # Берём из того же снимка коллектора, что и в этом цикле:
            # если объявление уже пропало с сайта, снимка не будет —
            # это честнее, чем подставлять чужие данные.
            rows_by_id = {r["id"]: r for r in (fresh_rows or [])}

            for lid, saved_hash in listing_hashes.items():
                verdict = results.get(lid)
                if verdict is None:
                    # Ни вердикта, ни отметки об ошибке — объявление
                    # просто не записываем, и следующий дифф-цикл
                    # отправит его заново (хэш в verdicts не появился).
                    log.warning("batch %s: нет ответа для %s", b["batch_id"], lid)
                    continue
                # llm_failed — это "не знаю", а не вердикт. Записываем
                # результат (чтобы было видно в /verdicts/review, что
                # модель споткнулась), но НЕ сохраняем настоящий хэш:
                # иначе дифф следующего цикла увидит совпадение, сочтёт
                # объявление обработанным и больше никогда его не
                # переотправит. В симуляции на 20 объявлениях так
                # застряли бы 2 (10%) — навсегда, без вердикта.
                #
                # Пустой хэш гарантированно не совпадёт ни с одним
                # реальным, поэтому объявление вернётся в очередь.
                failed = verdict["source"] == "llm_failed"
                attempts = 0
                if failed:
                    attempts = cleaner_db.llm_attempts_for(conn, lid) + 1
                    attempts_so_far = attempts
                    if attempts_so_far >= MAX_LLM_ATTEMPTS:
                        # Сдаёмся: фиксируем настоящий хэш, объявление
                        # перестаёт крутиться в очереди. Оно останется
                        # с usable=None ("неизвестно"), потребитель его
                        # не выбросит, а человек увидит в /verdicts/review.
                        log.warning(
                            "объявление %s: ИИ не смог разобрать %s раз подряд — "
                            "прекращаю попытки", lid, attempts_so_far)
                        hash_to_store = saved_hash
                    else:
                        hash_to_store = ""
                else:
                    hash_to_store = saved_hash
                cleaner_db.upsert_verdict(
                    conn, lid, hash_to_store, verdict,
                    baseline_version=None,
                    model=openai_batch.MODEL if verdict["source"] == "llm" else None,
                    batch_id=b["batch_id"],
                    row=rows_by_id.get(lid),
                    attempts=attempts,
                )
                ingested += 1
            cleaner_db.mark_batch(conn, b["batch_id"], "completed", output_file_id)
            conn.commit()
            log.info("batch %s: записано %s вердиктов из %s отправленных",
                     b["batch_id"], ingested - ingested_before, len(listing_hashes))

        elif status in ("failed", "expired", "cancelled"):
            cleaner_db.mark_batch(conn, b["batch_id"], status, error=error)
            conn.commit()
            log.warning("batch %s завершился как %s: %s", b["batch_id"], status, error)
            # Объявления из проваленного batch НЕ помечены как обработанные
            # (upsert_verdict для них не вызывался) — следующий дифф-цикл
            # снова увидит их как "нужен ИИ" и переотправит автоматически.

        else:
            log.info("batch %s: всё ещё %s", b["batch_id"], status)

    return ingested


def apply_heuristics(conn, to_process, baseline_version):
    """to_process — [(row, content_hash), ...]. Возвращает остаток,
    который бесплатный слой не смог решить: [(row, content_hash), ...].
    """
    remainder = []
    resolved = 0
    for row, h in to_process:
        v = heuristics.classify(row)
        if v is not None:
            cleaner_db.upsert_verdict(conn, row["id"], h, v, baseline_version, row=row)
            resolved += 1
        else:
            remainder.append((row, h))
    conn.commit()
    log.info("бесплатный слой: решено %s, осталось для ИИ %s", resolved, len(remainder))
    return remainder


def apply_rules_to_known(conn, rows):
    """Применяет бесплатные правила ко ВСЕМ строкам, а не только к новым.

    Зачем. Дифф по content_hash пересматривает только новые и изменённые
    объявления. Появилось новое правило (или поменялось старое) — уже
    размеченные объявления остались бы со старым вердиктом навсегда: их
    текст не менялся, значит хэш тот же. Правила бесплатны и работают в
    памяти, поэтому прогоняем их по всей выгрузке каждый цикл, и любая
    правка правил применяется сама, без ручной чистки базы.

    Два случая:
      - правило срабатывает, а вердикт другой (например, старый ИИ-вердикт
        «ok» на объявление «без ремонта») — переписываем вердиктом правила;
      - вердикт выставило правило, а теперь оно молчит (хозяин поменял
        поле, или правило убрали) — сбрасываем хэш, и объявление вернётся
        в очередь к модели. Без этого оно осталось бы отбракованным навсегда.
    Новые объявления (вердикта ещё нет) не трогаем: их обработает обычный
    путь через apply_heuristics.
    """
    known = cleaner_db.verdict_source_map(conn)
    overridden = reopened = 0
    for row in rows:
        st = known.get(row["id"])
        if st is None:
            continue
        v = heuristics.classify(row)
        if v is not None:
            if st == (v["source"], int(v["usable"]), v["reason_code"]):
                continue
            cleaner_db.upsert_verdict(conn, row["id"], collector_client.content_hash(row),
                                      v, None, row=row)
            overridden += 1
        elif st[0] == "rule":
            cleaner_db.invalidate_hash(conn, row["id"])
            reopened += 1
    conn.commit()
    if overridden or reopened:
        log.info("правила по уже размеченным: переписано вердиктов %s, возвращено в очередь %s",
                 overridden, reopened)
    return overridden, reopened


def submit_new_batches(conn, remainder):
    """remainder — [(row, content_hash), ...], уже прошедшие heuristics.

    Отправляет НЕСКОЛЬКО batch-заданий за цикл: остаток режется на куски по
    MAX_BATCH_SIZE, за один цикл уходит не больше MAX_BATCHES_PER_CYCLE штук.

    ПОЧЕМУ НЕ ОДНО ГИГАНТСКОЕ ЗАДАНИЕ НА ВСЮ БАЗУ.
    Первый прогон — это ~13 тыс. объявлений. Одним заданием это ~650
    запросов и ~3 млн входных токенов в одной корзине: если провайдер
    отклонит его целиком (превышен лимит очереди токенов на модель, кончилась
    квота), заблокированной окажется вся база сразу. Куски по MAX_BATCH_SIZE
    ограничивают радиус поражения — упавший кусок вернётся в очередь
    следующим циклом, остальные к тому времени уже посчитаются.

    ПОЧЕМУ НЕ ПО ОДНОМУ ЗАДАНИЮ ЗА ЦИКЛ (как было раньше).
    При цикле раз в 12 часов и потолке в 3000 полная разметка стартовой базы
    растянулась бы на двое суток. Несколько заданий за цикл дают полный
    прогон сразу на первом запуске — ровно то поведение, которое нужно при
    холодном старте.

    Возвращает список batch_id (пустой, если ничего не отправили).
    """
    if len(remainder) < MIN_BATCH_SIZE:
        log.info("объявлений для ИИ меньше порога (%s < %s) — жду следующего цикла",
                 len(remainder), MIN_BATCH_SIZE)
        return [], 0

    budget = min(len(remainder), MAX_BATCH_SIZE * MAX_BATCHES_PER_CYCLE)
    batch_items = remainder[:budget]

    if PILOT_LIMIT:
        # Считаем и уже размеченные ИИ, и те, что прямо сейчас в работе:
        # без второго слагаемого лимит переполнился бы на несколько
        # циклов вперёд, пока первые batch ещё не вернулись.
        spent = conn.execute(
            "SELECT COUNT(*) FROM verdicts WHERE source IN ('llm','llm_failed')"
        ).fetchone()[0]
        in_flight = len(cleaner_db.in_flight_ids(conn))
        available = PILOT_LIMIT - spent - in_flight
        if available <= 0:
            log.warning(
                "ПИЛОТНЫЙ ЛИМИТ исчерпан: размечено %s, в работе %s, лимит %s. "
                "Новые отправки остановлены. Проверьте /verdicts/review и снимите "
                "PILOT_LIMIT (или увеличьте), чтобы продолжить.",
                spent, in_flight, PILOT_LIMIT,
            )
            return [], 0
        if available < len(batch_items):
            log.info("пилотный лимит: отправляю %s вместо %s (осталось до лимита %s)",
                     available, len(batch_items), PILOT_LIMIT)
            batch_items = batch_items[:available]
        if len(batch_items) < MIN_BATCH_SIZE:
            log.info("остаток до пилотного лимита (%s) меньше порога отправки", available)
            return [], 0

    if len(remainder) > len(batch_items):
        log.info("остаток %s больше отправляемого за цикл %s — остальное в след. цикл",
                 len(remainder), len(batch_items))

    # Защита от дублирующей отправки — не здесь, а в run_cycle(): туда
    # remainder попадает уже БЕЗ id, которые сидят в незавершённых
    # batch-заданиях (см. cleaner_db.in_flight_ids). Если процесс упадёт
    # между отправкой и коммитом ниже — риск не дубль-отправки, а
    # потери записи о том, что batch вообще был отправлен; тогда
    # provider всё равно посчитает и обработает batch, просто мы не
    # будем знать его batch_id и не заберём результат. Это на практике
    # означает: потраченные деньги без вердикта. Смягчается тем, что
    # окно между submit_batch() и create_batch()+commit() — миллисекунды.

    submitted = []
    sent_count = 0
    for start in range(0, len(batch_items), MAX_BATCH_SIZE):
        chunk = batch_items[start:start + MAX_BATCH_SIZE]

        # Хвост меньше порога отправки не гоним: он дешевле подождёт
        # следующего цикла и уедет вместе с новым приростом.
        if len(chunk) < MIN_BATCH_SIZE:
            log.info("хвост из %s объявлений меньше порога — оставляю на след. цикл",
                     len(chunk))
            break

        items = [(row["id"], row) for row, _h in chunk]
        jsonl = openai_batch.build_jsonl(items)
        try:
            batch_id = openai_batch.submit_batch(jsonl)
        except Exception:
            # Скорее всего упёрлись в лимит очереди токенов у провайдера
            # или в квоту. Дальше в этом цикле долбиться бессмысленно —
            # остальные куски вернутся в очередь следующим циклом.
            log.exception("не удалось отправить batch (отправлено до этого: %s) — "
                          "остальное перенесено на следующий цикл", len(submitted))
            break

        listing_hashes = {row["id"]: h for row, h in chunk}
        try:
            cleaner_db.create_batch(conn, batch_id, listing_hashes)
            conn.commit()
        except Exception:
            # Batch УЖЕ отправлен и оплачен. Если не смогли записать его в
            # свою таблицу — результат потом некому будет забрать, деньги
            # уйдут впустую. Поэтому не роняем цикл, а кричим в лог с
            # batch_id: по нему задание можно найти и разобрать вручную.
            log.exception(
                "КРИТИЧНО: batch %s отправлен провайдеру, но не записан в БД. "
                "Результат нужно забрать вручную, иначе оплата пропадёт. "
                "Объявлений в задании: %s", batch_id, len(listing_hashes),
            )
            break

        submitted.append(batch_id)
        sent_count += len(chunk)
        log.info("отправлен batch %s: %s объявлений в %s группах по %s",
                 batch_id, len(listing_hashes),
                 -(-len(listing_hashes) // openai_batch.GROUP_SIZE),
                 openai_batch.GROUP_SIZE)

    if submitted:
        log.info("за цикл отправлено заданий: %s, объявлений в них: %s",
                 len(submitted), sent_count)
    return submitted, sent_count


def publish_clean_baseline(conn, rows, version):
    """Пересобирает опубликованный чистый baseline из свежей выгрузки коллектора.

    Объявление попадает в снимок, только если:
      - у него есть вердикт, вынесенный по ТЕКУЩЕМУ тексту (content_hash
        совпадает). Новые и изменённые объявления ждут разметки — иначе
        мусор жил бы в «чистой» выдаче до следующего цикла, а у изменённого
        объявления висел бы вердикт по старому тексту;
      - вердикт не usable=0. usable=None (модель так и не смогла разобрать
        после MAX_LLM_ATTEMPTS) остаётся: сбой модели — «не знаю», а не «плохое».

    Пересборка целиком, а не точечное добавление: так из снимка сами уходят
    объявления, пропавшие с сайта, у опубликованных обновляется цена, а
    недоделанное обновление не может оставить снимок в промежуточном виде.
    """
    state = cleaner_db.verdict_state_map(conn)
    clean, rejected, waiting = [], 0, 0
    for row in rows:
        st = state.get(row["id"])
        if st is None or st[1] != collector_client.content_hash(row):
            waiting += 1
        elif st[0] == 0:
            rejected += 1
        else:
            clean.append(row)

    # ПУСТОЙ СНИМОК НЕ ПУБЛИКУЕМ. На холодном старте первый цикл только
    # отправляет объявления в batch, вердиктов по текущему тексту ещё нет
    # ни у кого — clean пуст. Записав его, мы бы отдавали потребителю
    # 200 с "total: 0" вместо 503, а пустой ответ он принял бы за
    # «объявлений нет» и затёр свои данные (ровно то, от чего защищается
    # 503 в verdicts_api). Лучше остаться без снимка (503) или сохранить
    # прежний, чем объявить чистой базой пустоту.
    if not clean:
        log.warning("чистый baseline НЕ опубликован: подходящих объявлений 0 "
                    "(отбраковано %s, ждут разметки %s) — прежний снимок оставлен как есть",
                    rejected, waiting)
        return {"published": 0, "rejected": rejected, "waiting": waiting,
                "publish_skipped": "empty_result"}

    cleaner_db.replace_clean_baseline(conn, clean, version)
    conn.commit()
    log.info("чистый baseline опубликован: %s объявлений (отбраковано %s, ждут разметки %s)",
             len(clean), rejected, waiting)
    return {"published": len(clean), "rejected": rejected, "waiting": waiting}


def run_cycle():
    """Один полный цикл: собрать готовое, найти новое, разложить по
    слоям, отправить остаток, опубликовать чистый baseline.
    Вызывается раз в CLEANER_CYCLE_INTERVAL_H часов из service.py."""
    with cleaner_db.connect() as conn:
        # Снимок коллектора берём ДО разбора batch: он нужен и для
        # диффа, и для снимков текста отбракованных объявлений.
        try:
            version, rows = collector_client.fetch_all_rows()
        except (collector_client.CollectorError, requests.RequestException):
            log.exception("не удалось получить baseline у коллектора — цикл прерван")
            # Собрать готовые batch всё равно пытаемся: результаты уже
            # оплачены, терять их из-за недоступности коллектора нельзя.
            ingested = ingest_completed_batches(conn)
            # Снимок не трогаем: без свежих строк коллектора пересобрать его
            # нечем, а прежний остаётся корректным — просто на цикл старее.
            return {"ingested": ingested, "error": "collector_unavailable"}

        ingested = ingest_completed_batches(conn, fresh_rows=rows)

        # После разбора batch и до диффа: правила перебивают ИИ-вердикты
        # (включая только что забранные), а сброшенные хэши сразу попадают
        # в текущий дифф.
        overridden, reopened = apply_rules_to_known(conn, rows)

        # Разовый перепрогон всей базы новым промптом: включается сменой
        # переменной CLEANER_RELABEL_GEN (любое новое значение).
        relabel_gen = os.environ.get("CLEANER_RELABEL_GEN", "")
        if relabel_gen and cleaner_db.get_meta(conn, "relabel_gen")[0] != relabel_gen:
            queued = cleaner_db.start_relabel(conn)
            cleaner_db.set_meta(conn, "relabel_gen", relabel_gen)
            conn.commit()
            log.info("перепрогон базы (CLEANER_RELABEL_GEN=%s): в очередь поставлено %s", relabel_gen, queued)
        cleaner_db.prune_relabel(conn, [r["id"] for r in rows])
        conn.commit()

        known = cleaner_db.get_known_hashes(conn, [r["id"] for r in rows])
        to_process, unchanged = collector_client.diff_against_known(rows, known)

        # Исключаем то, что уже в необработанном batch-задании — иначе
        # см. cleaner_db.in_flight_ids: повторный запуск раньше, чем
        # ответит предыдущий batch, отправил бы дубль тех же объявлений.
        in_flight = cleaner_db.in_flight_ids(conn)
        before = len(to_process)
        to_process = [(row, h) for row, h in to_process if row["id"] not in in_flight]
        skipped_in_flight = before - len(to_process)

        log.info(
            "коллектор версия=%s: всего %s, без изменений %s, уже в работе %s, к обработке %s",
            version, len(rows), unchanged, skipped_in_flight, len(to_process),
        )

        remainder = apply_heuristics(conn, to_process, version)
        submitted, sent_count = submit_new_batches(conn, remainder)

        # Последним шагом: к этому моменту в базе уже и результаты batch,
        # забранные в начале цикла, и вердикты бесплатного слоя.
        # Пока идёт перепрогон, прежний снимок остаётся: иначе объявления с
        # сброшенным хэшем выпали бы из выдачи до конца разметки.
        left = cleaner_db.relabel_remaining(conn)
        if left:
            log.info("перепрогон не закончен (осталось %s) — снимок не пересобираем", left)
            published = {"published": None, "rejected": None, "waiting": left,
                         "publish_skipped": "relabel_in_progress"}
        else:
            published = publish_clean_baseline(conn, rows, version)

        return {
            **published,
            "rules_overridden": overridden,
            "rules_reopened": reopened,
            "ingested": ingested,
            "collector_version": version,
            "total": len(rows),
            "unchanged": unchanged,
            "to_process": len(to_process),
            "resolved_free": len(to_process) - len(remainder),
            "submitted_batches": len(submitted),
            "submitted_listings": sent_count,
            # Сколько объявлений НЕ уехало в этом цикле и ждёт следующего.
            # Раньше здесь было "len(remainder) если ничего не отправили,
            # иначе 0" — при частичной отправке цифра врала.
            "pending_ai": len(remainder) - sent_count,
        }
