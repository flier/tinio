# One-off verification, not a test: SEC-04's handover, measured on a spawned process.
#
# **What this asks, and why it is not a unit test.** The claim is about *inheritance* -- the
# pre-flight's process must not hold the control socket, and the child's must -- so the observation
# has to come from a process this one spawns, reporting its own descriptor table. That is a
# program, not a unit: the unit leg (`tinio-sandbox-macos`'s
# `the_clear_reaches_the_plans_handles_and_not_the_launchers`) reads the `FD_CLOEXEC` bit off the
# kernel for the same decision, and `tinio-server`'s `sandbox_macos_e2e` proves the confined child
# really receives both descriptors end to end.
#
# **How to run it** (macOS only; the flag writes are `os.set_inheritable`, which is `fcntl(F_SETFD)`
# with that one bit):
#
# ```text
# /usr/bin/python3 crates/tinio-sandbox-macos/scripts/verify-descriptor-handover.py
# ```
#
# It prints one line per observation and exits non-zero on the first disagreement, so it can be
# read as a gate or as evidence.
#
# **Every descriptor it asserts on is high, and that is load-bearing.** The numbers a process
# starts with -- the runtime's own, libtest's, a shell's -- are inherited whatever a launch does, so
# a membership test on a low number can be satisfied by something that has nothing to do with this
# handoff. Opening throwaway descriptors first pushes the socket and its duplicate past `BASELINE`,
# where their numbers cannot collide with an inherited one.
#
# **The probe is spawned with `close_fds=False`, and the flag is the measurement.** `subprocess`'s
# default closes every descriptor above the standard three in the child -- exactly the set under
# test -- so without it the observation would be vacuous. The Rust version compiled a probe of its
# own for the other half of the same trap: a `sh -c 'ls /dev/fd'` was measured to report
# descriptors the kernel had already closed on exec (macOS 15.8), which is why the probe is a
# program rather than a shell listing.

import os
import socket
import subprocess
import sys
import tempfile

# How many descriptors to hold open so the socket lands past every number a process inherits.
BASELINE = 100

# The probe: a process that lists its own descriptors and nothing else.
#
# `/dev/fd` lists every descriptor this process holds, and the kernel has already closed the
# close-on-exec ones on the `exec` that started it -- so the listing **is** the inherited set. The
# child opens nothing before it reports: one directory read, one write to its stdout, no shell in
# between. `-I` keeps the parent's `sys.path` and `PYTHONPATH` from putting an import ahead of `os`,
# and `-S` keeps `site` out of the startup altogether.
PROBE = (
    "import os,sys;"
    "sys.stdout.write(' '.join(str(fd) for fd in sorted(int(n) for n in os.listdir('/dev/fd'))))"
)


def probe_argv():
    """The interpreter's path and the probe, ready for `subprocess.run`."""
    return [sys.executable, "-I", "-S", "-c", PROBE]


def inherited():
    """The descriptors a spawn of the probe carries, as the probe reports them."""
    completed = subprocess.run(
        probe_argv(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        # Without this every descriptor under test is closed in the child before it reports, and
        # the observation says nothing about inheritance.
        close_fds=False,
        check=False,
    )
    assert completed.returncode == 0, "the probe did not run (status {}, stderr {!r})".format(
        completed.returncode, completed.stderr.decode(errors="replace")
    )
    reported = [int(field) for field in completed.stdout.decode().split()]
    assert reported[:3] == [0, 1, 2], (
        "the probe did not report its standard three (status {}, stderr {!r}): {}".format(
            completed.returncode,
            completed.stderr.decode(errors="replace"),
            reported,
        )
    )
    return reported


def raise_the_table():
    """Hold `BASELINE` descriptors open so a bind lands above every number a process inherits."""
    held = [open(os.devnull, "rb") for _ in range(BASELINE)]
    # The caller has to hold the list open for the whole run, so nothing here may drop one: a
    # descriptor freed while the socket is being placed is a number the socket could take.
    assert len(held) == BASELINE, "the table is not BASELINE descriptors"
    return held


def bind(root, name):
    """A unix listener bound under `root`, at whatever number the table gives it."""
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(os.path.join(root, name))
    return listener


def report(nth, what, ok):
    verdict = "ok" if ok else "DISAGREES"
    print("{}. {} -- {}".format(nth, what, verdict))


def main():
    held = []
    root = tempfile.mkdtemp(prefix="tinio-handover-")
    failures = 0
    try:
        held = raise_the_table()

        # 1. **The state the plan's duplicate starts in.** A fresh duplicate of the socket is
        #    close-on-exec, like the socket it copies, so a spawn sees neither. This is what
        #    `ControlHandle::duplicate` answers and what `crates/tinio-cli/src/sandbox/macos.rs`
        #    puts on the plan.
        listener = bind(root, "one.sock")
        socket_fd = listener.fileno()
        copy = os.dup(socket_fd)
        seen = inherited()
        ok = (
            socket_fd > BASELINE
            and copy > BASELINE
            and socket_fd not in seen
            and copy not in seen
        )
        report(
            1,
            "a plan's duplicate (fd {}) and the launcher's socket (fd {}) before any "
            "clear: probe reports {}".format(copy, socket_fd, seen),
            ok,
        )
        failures += int(not ok)

        # 2. **The pre-fix ordering, reproduced.** The plan carried a bare integer, so the only
        #    process that could clear the flag was the launcher that still had the socket -- ahead
        #    of the pre-flight. The probe accordingly holds the socket.
        listener = bind(root, "two.sock")
        socket_fd = listener.fileno()
        before = inherited()
        os.set_inheritable(socket_fd, True)
        after = inherited()
        ok = socket_fd > BASELINE and socket_fd not in before and socket_fd in after
        report(
            2,
            "the pre-fix ordering -- the clear ahead of the spawn, on the launcher's socket "
            "(fd {}): probe reports {} before and {} after".format(socket_fd, before, after),
            ok,
        )
        failures += int(not ok)

        # 3. **The fix's ordering.** The clear runs on the plan's duplicate and the launcher's
        #    socket keeps its flag, so a spawn carries the duplicate and not the socket.
        listener = bind(root, "three.sock")
        socket_fd = listener.fileno()
        copy = os.dup(socket_fd)
        assert not os.get_inheritable(copy), "a duplicate starts close-on-exec"
        before = inherited()
        os.set_inheritable(copy, True)
        after = inherited()
        ok = (
            socket_fd > BASELINE
            and copy > BASELINE
            and socket_fd not in after
            and copy in after
            and copy not in before
        )
        report(
            3,
            "the fixed ordering -- the clear on the plan's duplicate (fd {}), the "
            "launcher's socket (fd {}) untouched: probe reports {} before and {} "
            "after".format(copy, socket_fd, before, after),
            ok,
        )
        failures += int(not ok)
    finally:
        for name in sorted(os.listdir(root)):
            os.unlink(os.path.join(root, name))
        os.rmdir(root)

    if failures == 0:
        print("all observations agree")
        return 0
    print("{} observation(s) disagree".format(failures), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
