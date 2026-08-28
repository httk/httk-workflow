"""Internal exec gate used to close the running-to-process crash window."""

import os
import sys


def main() -> int:
    """Wait for the parent commit token, then replace this process with the runner."""

    if len(sys.argv) < 4 or sys.argv[2] != "--":
        return 125
    gate_fd: int | None = None
    try:
        gate_fd = int(sys.argv[1])
        token = os.read(gate_fd, 1)
    except (OSError, ValueError):
        return 125
    finally:
        if gate_fd is not None:
            try:
                os.close(gate_fd)
            except OSError:
                pass
    if token != b"R":
        return 125
    command = sys.argv[3:]
    if not command:
        return 125
    try:
        os.execvpe(command[0], command, os.environ)
    except OSError:
        return 127


if __name__ == "__main__":
    raise SystemExit(main())
