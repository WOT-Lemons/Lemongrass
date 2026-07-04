FROM ghcr.io/astral-sh/uv:0.11.21-python3.14-trixie@sha256:05abd865132ddbe8b607b7063514f8debacbc98e60a01823a8abdacbdd61e0d7 AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim-trixie@sha256:b877e50bd90de10af8d82c57a022fc2e0dc731c5320d762a27986facfc3355c1
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
# No ENTRYPOINT/CMD — the runtime command is supplied by docker-compose (external repo).
# Use `lemongrass <command>` — e.g. `lemongrass laps`, `lemongrass telem`, `lemongrass race-backfill`.
