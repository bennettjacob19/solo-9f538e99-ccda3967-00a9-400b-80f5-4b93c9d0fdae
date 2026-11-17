FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py upstream.py ./
COPY static ./static
COPY artifacts ./artifacts

# The SQLite database lives on a named volume shared by both workers (and kept
# across container restarts).
ENV GATEWAY_DB=/data/breaker.db
RUN mkdir -p /data

EXPOSE 8000 9000
