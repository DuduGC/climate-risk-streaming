FROM python:3.11.14-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN groupadd --gid 10001 climate \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin climate

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --no-compile -c /app/requirements.txt requests confluent-kafka

COPY src/ /app/src/

USER 10001:10001
CMD ["python", "-m", "climate_logistics.producer"]
