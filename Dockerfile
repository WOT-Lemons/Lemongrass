FROM ghcr.io/astral-sh/uv:0.11.21-python3.14-trixie@sha256:05abd865132ddbe8b607b7063514f8debacbc98e60a01823a8abdacbdd61e0d7 AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
COPY src/ src/
RUN uv sync --frozen --no-dev --group race --group pi

FROM python:3.14-slim-trixie@sha256:44dd04494ee8f3b538294360e7c4b3acb87c8268e4d0a4828a6500b1eff50061
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
# No ENTRYPOINT/CMD — the runtime command is supplied by docker-compose (external repo).
# Use the installed console script names: `laps`, `race-backfill`, `telem`, or `pisugar-monitor`
# (not the old `python <script>.py` style).
