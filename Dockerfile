# ИИ-слой очистки объявлений. Отдельный сервис от коллектора: если он
# упадёт или будет тормозить (недоступен OpenAI, кончилась квота), это
# не должно останавливать сбор данных или отдачу baseline.
FROM python:3.12-slim

WORKDIR /app

# Зависимости отдельным слоем от кода: правка service.py не должна
# заставлять пересобирать pip install.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

# Своё хранилище вердиктов — отдельно от диска коллектора. На Render
# сюда монтируется постоянный диск (см. render.yaml), локально — просто
# каталог внутри контейнера.
ENV DATA_DIR=/app/data

ENV CLEANER_API_HOST=0.0.0.0
ENV PORT=8002
ENV PYTHONUNBUFFERED=1
EXPOSE 8002

CMD ["python", "service.py"]
