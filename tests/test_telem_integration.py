"""Integration tests driving telem's real OBD path against the ELM327 emulator.

Marked `integration` (excluded from the default pytest run). Requires the
emulator installed via local-testing/install-emulator.sh. The emulator is
spawned as a subprocess and never imported, so this module imports cleanly even
when the emulator is absent (the tests are simply deselected by default).
"""
import os
import select
import socket
import subprocess
import sys
import time
from collections import namedtuple

import pytest

import lemongrass.telem as telem

Emu = namedtuple("Emu", "url obd")


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_for_ready(proc, needle, timeout):
    """Block until the emulator prints its 'listening' line, or fail loudly.

    Reads the raw stdout fd via ``select`` so the deadline is enforced *during*
    the wait — a plain ``readline()`` blocks until a newline arrives and would
    ignore the timeout if the emulator stalled mid-line.
    """
    deadline = time.monotonic() + timeout
    fd = proc.stdout.fileno()
    captured = []
    buf = ""
    while time.monotonic() < deadline:
        ready, _, _ = select.select([fd], [], [], deadline - time.monotonic())
        if not ready:
            continue
        chunk = os.read(fd, 4096).decode(errors="replace")
        if not chunk:  # EOF
            if proc.poll() is not None:
                raise RuntimeError("emulator exited early:\n" + "".join(captured))
            continue
        captured.append(chunk)
        buf += chunk
        if needle in buf:
            return
    raise RuntimeError(f"emulator not ready in {timeout}s:\n" + "".join(captured))


@pytest.fixture(scope="module")
def emulator():
    # conftest.py installs a MagicMock for `obd`; swap in the real library and
    # rebind it onto telem for the duration of this module.
    saved = sys.modules.pop("obd", None)
    import obd as real_obd

    orig_telem_obd = telem.obd
    telem.obd = real_obd

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "elm", "-s", "car", "-n", str(port)],
        stdin=subprocess.PIPE,          # keep stdin open — cmd console exits on EOF
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    try:
        _wait_for_ready(proc, "running on TCP network port", timeout=30)
        yield Emu(url=f"socket://127.0.0.1:{port}", obd=real_obd)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        telem.obd = orig_telem_obd
        if saved is not None:
            sys.modules["obd"] = saved
        else:
            sys.modules.pop("obd", None)


@pytest.fixture
def connection(emulator, monkeypatch):
    monkeypatch.setenv("OBD_PORT", emulator.url)
    monkeypatch.setenv("OBD_BAUDRATE", "38400")
    conn = telem.connect()
    yield conn
    conn.close()


@pytest.mark.integration
def test_connect_reports_car_connected(emulator, connection):
    assert connection.status() == emulator.obd.OBDStatus.CAR_CONNECTED
    assert connection.protocol_name()  # non-empty once a protocol is negotiated


@pytest.mark.integration
def test_supported_commands_include_core_pids(emulator, connection):
    names = {c.name for c in connection.supported_commands}
    assert {"RPM", "SPEED"} <= names


@pytest.mark.integration
def test_new_value_queues_point_from_real_rpm(emulator, connection):
    telem.pending_points.clear()
    r = emulator.obd.OBD.query(connection, emulator.obd.commands.RPM, force=True)
    assert not r.is_null()
    telem.new_value(r)
    assert len(telem.pending_points) == 1


@pytest.mark.integration
def test_query_fuel_type_once_matches_support(emulator, connection):
    telem.pending_points.clear()
    telem._query_fuel_type_once(connection)
    supported = connection.supports(emulator.obd.commands.FUEL_TYPE)
    assert len(telem.pending_points) == (1 if supported else 0)
