FROM ghcr.io/astral-sh/uv:0.11.29-python3.14-trixie@sha256:cd22b8ef1b9a27e285a0e8ee3416db1c955d7d14c33bb39ec2a41306c68a5500 AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim-trixie@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6
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
