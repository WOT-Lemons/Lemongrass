FROM ghcr.io/astral-sh/uv:0.13.5-python3.14-trixie@sha256:00c6beb41f4ceb136a7e28a48f9d7ded75f85f4e99bce30a6c89dffbd5d10d7c AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim-trixie@sha256:cd89c30eda26ee911a4611b18680086e0ea42e6f5f8cbe653d07fa7958c9cec8
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
# Drop to a non-root user. The venv stays root-owned (execute-only for this
# user), so the app can't modify itself. /data is the one writable dir: the
# legacy `lemongrass laps -o` CSV lands there — mount a writable dir at /data
# to persist it. All other commands are network-only and write nothing.
RUN useradd --uid 10001 --no-create-home lemongrass \
    && mkdir /data \
    && chown lemongrass:lemongrass /data
WORKDIR /data
USER lemongrass
# No ENTRYPOINT/CMD — the runtime command is supplied by docker-compose (external repo).
# Use `lemongrass <command>` — e.g. `lemongrass laps`, `lemongrass telem`, `lemongrass race-backfill`.
