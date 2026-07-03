# Virtual ELM327 OBD-II adapter for the local test stack. Reuses the repo's
# pinned uv base image. Two sequential installs guarantee setuptools<81 (which
# still ships pkg_resources) is present before the sdist builds.
FROM ghcr.io/astral-sh/uv:0.11.21-python3.14-trixie@sha256:05abd865132ddbe8b607b7063514f8debacbc98e60a01823a8abdacbdd61e0d7
RUN uv pip install --system 'setuptools<81'
RUN uv pip install --system --no-build-isolation 'ELM327-emulator==3.0.5'
# `car` scenario = Toyota Auris Hybrid (realistic PID/DTC set). The emulator
# runs a cmd console on the main thread and needs stdin held open (compose
# sets stdin_open: true) or it prints "Terminating..." and exits.
CMD ["python", "-m", "elm", "-s", "car", "-n", "35000"]
