"""Workflow-side client for one confined daemon MPI application step."""

import logging
import os
import secrets
import socket
import sys
from collections.abc import Sequence
from pathlib import Path

from ._daemon_mailbox import MailboxDirectory
from ._daemon_mpi_protocol import (
    Manifest,
    _reserved_environment,
    decode_terminal,
    encode_manifest,
    encode_request,
    manifest_path,
    recv_frame,
    send_frame,
)
from ._daemon_policy import load_policy

_LOGGER = logging.getLogger(__name__)
_POLICY_PATH = Path("/daemon-policy.json")
_WORKSPACE = Path("/workspace")
_CONTROL_SOCKET = Path("/run/httk-mpi/control.sock")
_CONNECT_TIMEOUT = 5.0
_HANDLE_VARIABLE = "HTTK_DAEMON_MPI_HANDLE"
_PROFILE_VARIABLE = "HTTK_DAEMON_MPI_PROFILE"


def _manager_context() -> tuple[str, str]:
    """Return validated manager handle and workspace identity."""

    handle = os.environ.get(_HANDLE_VARIABLE)
    profile_name = os.environ.get(_PROFILE_VARIABLE)
    if handle is None or profile_name is None:
        raise RuntimeError("MPI execution requires a protected daemon MPI manager")
    policy = load_policy(_POLICY_PATH)
    profile = policy.profile(profile_name)
    if profile.mpi is None or policy.mpi is None:
        raise RuntimeError("daemon manager profile is not configured for MPI")
    # Path construction checks the handle shape before any workspace mutation.
    manifest_path(_WORKSPACE, handle, "0" * 32)
    return handle, policy.workspace_id


def _relative_cwd() -> str:
    """Return the current directory as one canonical path below the workspace."""

    current = Path.cwd()
    try:
        relative = current.relative_to(_WORKSPACE)
    except ValueError as exc:
        raise RuntimeError("MPI application cwd must be inside /workspace") from exc
    return "." if relative == Path(".") else relative.as_posix()


def _application_environment() -> tuple[tuple[str, str], ...]:
    """Capture nonreserved application environment entries."""

    return tuple(
        sorted(
            (name, value)
            for name, value in os.environ.items()
            if name.isascii() and name.isidentifier() and not _reserved_environment(name)
        )
    )


def _create_directory(parent: Path, name: str) -> Path:
    """Create one private workspace directory and refuse symlink substitution."""

    path = parent / name
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    with MailboxDirectory(path):
        pass
    return path


def _manifest_mailbox(handle: str) -> MailboxDirectory:
    """Create and open the manager's descriptor-anchored manifest directory."""

    control = _WORKSPACE / ".httk-workspace"
    with MailboxDirectory(control):
        pass
    mpi = _create_directory(control, "mpi")
    directory = _create_directory(mpi, handle)
    return MailboxDirectory(directory)


def _cleanup_manifest(mailbox: MailboxDirectory, name: str) -> None:
    """Best-effort remove a manifest after its terminal result is known."""

    try:
        mailbox.remove(name)
    except OSError as exc:
        _LOGGER.warning(
            "MPI manifest cleanup failed (%s)",
            type(exc).__name__,
            extra={"context": "daemon_mpi_manifest_cleanup"},
        )


def _stream_response(sock: socket.socket) -> int:
    """Forward application output until one terminal frame arrives."""

    while True:
        kind, payload = recv_frame(sock)
        if kind == b"O":
            sys.stdout.buffer.write(payload)
            sys.stdout.buffer.flush()
        elif kind == b"E":
            sys.stderr.buffer.write(payload)
            sys.stderr.buffer.flush()
        elif kind == b"X":
            code, error = decode_terminal(payload)
            if error is not None:
                sys.stderr.write(error + "\n")
                sys.stderr.flush()
            return code
        else:
            raise ValueError("daemon MPI service returned an invalid frame kind")


def run(argv: Sequence[str]) -> int:
    """Run one application through the active protected MPI allocation.

    :param argv: Nonempty application argument vector.
    :return: Confirmed application exit status.
    :raises OSError: If manifest publication or local socket transport fails.
    :raises RuntimeError: If this is not an active matching MPI manager.
    :raises ValueError: If application data or the response violates the protocol.
    """

    if not isinstance(argv, Sequence) or isinstance(argv, (str, bytes)):
        raise ValueError("MPI application argv must be a sequence of strings")
    handle, workspace_id = _manager_context()
    request_id = secrets.token_hex(16)
    manifest = Manifest(
        request_id=request_id,
        manager_handle=handle,
        workspace_id=workspace_id,
        argv=tuple(argv),
        cwd=_relative_cwd(),
        environment=_application_environment(),
    )
    path = manifest_path(_WORKSPACE, handle, request_id)
    confirmed = False
    with _manifest_mailbox(handle) as mailbox:
        mailbox.replace(path.name, encode_manifest(manifest))
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(_CONNECT_TIMEOUT)
                client.connect(str(_CONTROL_SOCKET))
                send_frame(client, b"Q", encode_request(request_id))
                # The application may run for the full allocation time. The service
                # independently bounds request reads and each output write.
                client.settimeout(None)
                result = _stream_response(client)
                confirmed = True
        finally:
            if confirmed:
                _cleanup_manifest(mailbox, path.name)
    return result


__all__ = ["run"]
