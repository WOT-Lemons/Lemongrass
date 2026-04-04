FROM python:3.12-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY pi-requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r pi-requirements.txt

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /install /usr/local
COPY telem.py pisugar-monitor.py ./
