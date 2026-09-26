# One-off verification, not a test: a product run of the confined server on macOS.
#
# Spawns `tinio server <root> --port 0 --sandbox` with a short root under `/tmp` (the control
# socket path has to fit `sun_path`'s 104 bytes), waits for the `event: listening` frame, answers
# one request, sends `SIGTERM` to the launcher, and asserts the forwarded exit status is `0`.
#
# ```text
# cargo build -p tinio
# /usr/bin/python3 crates/tinio-sandbox-macos/scripts/verify-product-run.py
# ```
#
# It prints one line per observation and exits non-zero on the first disagreement, so it can be
# read as a gate or as evidence.
#
# **The frame is read off the descriptor, not through a buffered reader.** `select` and a
# `BufferedReader` do not compose: the reader fills its buffer on the first `readline`, the bytes
# sit there while `select` on the same descriptor truthfully reports nothing to read, and the frame
# that did arrive is never seen. This script reads chunks with `os.read` and cuts its own lines,
# which is why `listening_frame` owns a byte buffer rather than a file object.

import os
import re
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

# The readiness budget: the same 60 s the Rust script allowed a confined launch.
READY_BUDGET = 60.0

# Relative to the repository root the script is run from, which is where `cargo build -p tinio` put
# it.
BINARY = os.path.join("target", "debug", "tinio")

REQUEST = b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"


def listening_frame(fd, deadline):
    """The `data:` payload of the `listening` event, or `None` before the deadline."""
    name = ""
    data = ""
    buffer = b""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            return None
        chunk = os.read(fd, 65536)
        if not chunk:
            return None
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            line = line.rstrip(b"\r")
            if line.startswith(b"event: "):
                name = line[len(b"event: ") :].decode()
            elif line.startswith(b"data: "):
                data = line[len(b"data: ") :].decode()
            elif not line.strip():
                if name == "listening":
                    return data
                name, data = "", ""
        # A frame whose name arrived with no payload never becomes the readiness event, and a
        # trailing partial line waits for the next chunk.


def answer(addr):
    """Connect to the data plane the frame named and answer one request."""
    # The payload is `{"addr":"127.0.0.1:<port>"}`, so the port is what follows the last colon --
    # the frame shape is owned by the Windows leg (decision 19) and this script reads it rather
    # than re-defining it.
    port = re.match(r'\d+', addr.rsplit(":", 1)[-1].strip('"}'))
    assert port is not None, "the payload names no port: {}".format(addr)
    with socket.create_connection(("127.0.0.1", int(port.group(0))), timeout=READY_BUDGET) as plane:
        plane.sendall(REQUEST)
        chunks = []
        while True:
            chunk = plane.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    response = b"".join(chunks).decode(errors="replace")
    answer = response.splitlines()[0] if response.splitlines() else ""
    assert answer.startswith("HTTP/1.1 "), "the confined server's answer is not HTTP: {!r}".format(
        response
    )
    return answer


def reap(launcher):
    """Kill and wait a launcher that has nowhere left to go, so nothing is left running."""
    if launcher.poll() is None:
        launcher.kill()
    launcher.wait()


def main():
    root = tempfile.mkdtemp(prefix="tinio-verify-", dir="/tmp")
    assert os.path.isfile(BINARY), "build the binary first: {}".format(BINARY)
    launcher = None
    try:
        launcher = subprocess.Popen(
            [BINARY, "server", root, "--port", "0", "--sandbox"],
            env=dict(os.environ, NO_COLOR="1"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=None,
            # The launcher must not have its own descriptors closed by this spawn: the confined
            # child is exec'd from a process in this tree, and a closed stream or pipe would change
            # what that child starts with.
            close_fds=False,
        )

        addr = listening_frame(launcher.stdout.fileno(), time.monotonic() + READY_BUDGET)
        if addr is None:
            reap(launcher)
            print("no `listening` frame within {} s".format(READY_BUDGET), file=sys.stderr)
            return 1
        print("1. readiness: event=listening data={} -- ok".format(addr))

        status = answer(addr)
        print("2. a request was answered: {} -- ok".format(status))

        # SIGTERM to the launcher, which forwards it to the confined child; the launcher outlives
        # the signal and carries the child's own status back, which for a clean drain is 0.
        if launcher.poll() is not None:
            print(
                "the launcher had already exited ({}) before the signal".format(launcher.returncode),
                file=sys.stderr,
            )
            return 1
        launcher.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        deadline = time.monotonic() + READY_BUDGET
        while launcher.poll() is None:
            if time.monotonic() >= deadline:
                reap(launcher)
                print(
                    "the launcher did not exit within {} s".format(READY_BUDGET), file=sys.stderr
                )
                return 1
            time.sleep(0.05)
        exit_code = launcher.returncode
        print("3. SIGTERM forwarded, launcher exit {} -- ok".format(exit_code))
        assert exit_code == 0, "a clean stop forwards the child's 0"
    finally:
        if launcher is not None:
            reap(launcher)
        shutil.rmtree(root, ignore_errors=True)

    print("all observations agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
