# Virtual ELM327 OBD-II adapter for the local test stack. Reuses the repo's
# pinned uv base image. Two sequential installs guarantee setuptools<81 (which
# still ships pkg_resources) is present before the sdist builds.
FROM ghcr.io/astral-sh/uv:0.11.21-python3.14-trixie@sha256:05abd865132ddbe8b607b7063514f8debacbc98e60a01823a8abdacbdd61e0d7
RUN uv pip install --system 'setuptools<81'
RUN uv pip install --system --no-build-isolation 'ELM327-emulator==3.0.5'
# Log to stdout (visible via `docker logs`) instead of the stock rotating
# elm.log, which a non-root, read-only-cwd container can't create.
COPY logging.yaml /etc/elm327/logging.yaml
ENV ELM_LOG_CFG=/etc/elm327/logging.yaml
# Drop to a non-root user; port 35000 is unprivileged so no cap needed.
RUN useradd --uid 10001 --no-create-home elm
USER elm
# `car` scenario = Toyota Auris Hybrid (realistic PID/DTC set). The emulator
# runs a cmd console on the main thread and needs stdin held open (compose
# sets stdin_open: true) or it prints "Terminating..." and exits.
# Compose overrides this command to inject the TCP port from OBD_EMULATOR_PORT;
# the -n here is only the default for a bare `docker run`.
CMD ["python", "-m", "elm", "-s", "car", "-n", "35000"]
