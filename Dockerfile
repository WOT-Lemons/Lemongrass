FROM python:3.12-slim
WORKDIR /app
COPY pi-requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r pi-requirements.txt
COPY telem.py pisugar-monitor.py ./
