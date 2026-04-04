FROM python:3.12-slim
WORKDIR /app
COPY pi-requirements.txt .
RUN pip install --no-cache-dir -r pi-requirements.txt
COPY telem.py pisugar-monitor.py ./
