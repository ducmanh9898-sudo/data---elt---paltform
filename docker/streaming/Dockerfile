FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements/streaming.txt /tmp/streaming-requirements.txt

RUN python -m pip install \
    --no-cache-dir \
    --upgrade pip \
    && python -m pip install \
    --no-cache-dir \
    -r /tmp/streaming-requirements.txt

COPY streaming/ ./streaming/

CMD ["python", "-u", "-m", "streaming.producer.iot_simulator", "--count", "0", "--interval-seconds", "0.5", "--bootstrap-servers", "kafka:9092"]
