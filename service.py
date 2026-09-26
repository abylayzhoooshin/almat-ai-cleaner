"""
service.py — точка входа. Цикл очистки (pipeline.run_cycle) + FastAPI
(verdicts_api:app) в одном процессе, тем же паттерном, что у коллектора.

ПОЧЕМУ ОДИН ПРОЦЕСС, А НЕ CRON JOB НА ПЛАТФОРМЕ.
Cron job у Render не умеет двух вещей, без которых этот сервис не
работает: у него нет постоянного диска и он не отдаёт HTTP. Без диска
между запусками терялись бы cleaner.db (а значит content_hash — и вся
база переплачивалась бы заново каждый запуск) и таблица batches — то
есть отправленные и оплаченные batch-задания никто бы уже не забрал.
Без HTTP до нас не достучался бы сервис-потребитель. Поэтому
расписание живёт внутри процесса, а сам процесс — обычный web service.

ПОЧЕМУ РАЗ В 12 ЧАСОВ.
Цикл сам по себе дешёвый (дифф по хэшу), но batch-задание у OpenAI
асинхронное с окном до 24ч. Интервал в 12 часов даёт две попытки забрать
результат в сутки и при этом не плодит кучу мелких pending-batch, как
было бы при часовом интервале.

ПОЧЕМУ ПЕРВЫЙ ЦИКЛ — СРАЗУ ПРИ СТАРТЕ, А НЕ ЖДЁТ 12 ЧАСОВ.
Иначе после каждого редеплоя пришлось бы полсуток ждать первого
результата. На холодном старте (пустая база) это ещё и единственный
способ разметить всю базу сразу, а не через 12 часов. Дальше — по
расписанию.
"""
import asyncio
import logging
import os
import signal
import sys
import time

import uvicorn

import pipeline
import verdicts_api

CYCLE_INTERVAL_S = float(os.environ.get("CLEANER_CYCLE_INTERVAL_H", "12")) * 3600

# Между полными циклами дополнительно проверяем, не досчитал ли уже
# OpenAI batch — см. pipeline.run_ingest_tick. Полный цикл (дифф по
# всей базе коллектора + отправка новых batch) дорог и нужен редко;
# проверка готовности уже отправленного — бесплатна, и незачем
# заставлять потребителя ждать до 12 часов результат, который может
# быть готов через 10 минут.
INGEST_TICK_INTERVAL_S = float(os.environ.get("CLEANER_INGEST_TICK_S", "300"))


def _port():
    """Render задаёт порт через PORT и ожидает, что сервис слушает именно его.

    CLEANER_API_PORT оставлен для локального запуска и обратной
    совместимости, но PORT имеет приоритет: если платформа назначит
    другой порт, а мы будем слушать свой, healthcheck не пройдёт и
    деплой будет отбит.
    """
    return int(os.environ.get("PORT") or os.environ.get("CLEANER_API_PORT", "8002"))


def setup_logging():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


async def cycle_loop(stop_event):
    """Один и тот же цикл событий по очереди делает полный проход
    (run_cycle, раз в CLEANER_CYCLE_INTERVAL_H) и лёгкие проверки
    готовности batch между ними (run_ingest_tick, каждые
    CLEANER_INGEST_TICK_S). Последовательный await на to_thread —
    полный цикл и тик никогда не выполняются одновременно, поэтому
    pipeline._last_full_fetch можно безопасно читать/писать без блокировок.
    """
    log = logging.getLogger("service")
    last_full = 0.0
    while not stop_event.is_set():
        if time.time() - last_full >= CYCLE_INTERVAL_S:
            log.info("▶️  цикл очистки — старт")
            started = time.time()
            try:
                result = await asyncio.to_thread(pipeline.run_cycle)
                verdicts_api.record_cycle_result(result)
                log.info("⏹  цикл завершён за %.0fс: %s", time.time() - started, result)
            except Exception:
                log.exception("💥 цикл упал с ошибкой")
                verdicts_api.record_cycle_result({"error": "cycle_exception"})
            last_full = time.time()
        else:
            try:
                result = await asyncio.to_thread(pipeline.run_ingest_tick)
                if result:
                    verdicts_api.record_cycle_result(result)
                    log.info("⚡ промежуточная проверка batch: %s", result)
            except Exception:
                log.exception("💥 промежуточная проверка batch упала с ошибкой")

        try:
            timeout = INGEST_TICK_INTERVAL_S if time.time() - last_full < CYCLE_INTERVAL_S \
                else CYCLE_INTERVAL_S
            await asyncio.wait_for(stop_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass


async def main():
    setup_logging()
    log = logging.getLogger("service")

    stop_event = asyncio.Event()

    config = uvicorn.Config(
        verdicts_api.app,
        host=os.environ.get("CLEANER_API_HOST", "0.0.0.0"),
        port=_port(),
        log_level="info",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None

    loop = asyncio.get_running_loop()

    def _stop(*_):
        stop_event.set()
        server.should_exit = True

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, _stop)

    log.info(
        "🚀 rieltor-cleaner запущен. API на :%s, цикл очистки каждые %.0fч.",
        config.port, CYCLE_INTERVAL_S / 3600,
    )
    if pipeline.PILOT_LIMIT:
        log.warning(
            "ВНИМАНИЕ: включён PILOT_LIMIT=%s — в ИИ уйдёт не больше этого числа "
            "объявлений за всё время. Для боевого режима задайте PILOT_LIMIT=0.",
            pipeline.PILOT_LIMIT,
        )

    await asyncio.gather(cycle_loop(stop_event), server.serve())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
