FROM ghcr.io/astral-sh/uv:0.12.10-python3.14-trixie@sha256:a3e6118ca6b41a5e5c457fd70eb0fc71f11aea67408fb68e5888ba955ad545c0 AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim-trixie@sha256:cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6
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
