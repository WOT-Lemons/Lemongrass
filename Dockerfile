FROM ghcr.io/astral-sh/uv:0.12.1-python3.14-trixie@sha256:ac3d9fe21a46ce45caefc31d4208b167b4568d11e919c9485b02b4e3730dd2f3 AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim-trixie@sha256:a7fb1e634c4a578f9e0bd6327f11a3cde11b7a9395f48e24360c0988bcc5c2bc
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
