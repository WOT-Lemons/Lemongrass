#!/usr/bin/env bash
# Install the ELM327 emulator into the active uv venv for integration tests.
#
# Kept OUT of pyproject/uv.lock on purpose: the sdist imports pkg_resources at
# build time (setuptools>=81 removed it) and pulls POSIX-only python-daemon,
# which would pollute the universal lock for a tool used in one CI job only.
set -euo pipefail
uv pip install 'setuptools<81'
uv pip install --no-build-isolation 'ELM327-emulator==3.0.5'
