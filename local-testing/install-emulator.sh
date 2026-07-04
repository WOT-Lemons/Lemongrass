#!/usr/bin/env bash
# Install the ELM327 emulator into the active uv venv for integration tests.
#
# Kept OUT of pyproject/uv.lock on purpose: the sdist imports pkg_resources at
# build time (setuptools>=81 removed it) and pulls POSIX-only python-daemon,
# which would pollute the universal lock for a tool used in one CI job only.
set -euo pipefail
uv pip install 'setuptools<81'
# The emulator's setup.py appends `-$GITHUB_RUN_NUMBER` to its own version when
# that env var is present (always set in GitHub Actions), producing e.g.
# `3.0.5.post244`, which fails uv's `==3.0.5` metadata check. Unset it for the
# build so the version stays exactly 3.0.5. No-op outside GitHub Actions.
env -u GITHUB_RUN_NUMBER uv pip install --no-build-isolation 'ELM327-emulator==3.0.5'
