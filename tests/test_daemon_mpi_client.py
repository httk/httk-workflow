"""Manifest publication, streaming and uncertainty tests for the MPI client."""

import socket
import threading
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import httk.workflow._daemon_mpi_client as client_module
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_mpi_protocol import (
    Manifest,
    decode_request,
    encode_terminal,
    read_manifest,
    recv_frame,
    send_frame,
)

REQUEST_ID = "1" * 32
HANDLE = "2" * 32
WORKSPACE_ID = "12345678-1234-4234-8234-123456789abc"


def _policy(*, mpi_profile: bool = True, mpi_settings: bool = True):
    profile = SimpleNamespace(name="parallel", mpi=SimpleNamespace(nodes=2, ranks=4) if mpi_profile else None)
    return SimpleNamespace(
        workspace_id=WORKSPACE_ID,
        mpi=SimpleNamespace(environment=()) if mpi_settings else None,
        profile=lambda name: profile if name == "parallel" else (_ for _ in ()).throw(ValueError("unknown profile")),
    )


@pytest.fixture
def client_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    workspace = tmp_path / "workspace"
    (workspace / ".httk-workspace").mkdir(parents=True)
    workdir = workspace / "jobs" / "example" / "run"
    workdir.mkdir(parents=True)
    socket_path = tmp_path / "control.sock"
    monkeypatch.setattr(client_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(client_module, "_CONTROL_SOCKET", socket_path)
    monkeypatch.setattr(client_module, "_POLICY_PATH", tmp_path / "policy.json")
    monkeypatch.setattr(client_module, "load_policy", lambda _path: _policy())
    monkeypatch.setattr(client_module.secrets, "token_hex", lambda _size: REQUEST_ID)
    monkeypatch.setattr(
        client_module.os,
        "environ",
        {
            "HTTK_DAEMON_MPI_HANDLE": HANDLE,
            "HTTK_DAEMON_MPI_PROFILE": "parallel",
            "PATH": "/usr/bin",
            "LD_LIBRARY_PATH": "/site/mpi/lib",
            "MODULEPATH": "/site/modules",
            "USER_VALUE": "visible",
            "BASH_FUNC_module%%": "() {  :\n}",
            "PMIX_SERVER_URI": "reserved",
            "SLURM_JWT": "reserved",
            "OMPI_COMM_WORLD_RANK": "reserved",
            "OPAL_PREFIX": "reserved",
        },
    )
    monkeypatch.chdir(workdir)
    return workspace, socket_path


def _server(socket_path: Path, action) -> tuple[threading.Thread, list[BaseException]]:
    ready = threading.Event()
    errors: list[BaseException] = []

    def serve() -> None:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(socket_path))
                listener.listen(1)
                ready.set()
                connection, _ = listener.accept()
                with connection:
                    action(connection)
        except BaseException as exc:  # pragma: no cover - asserted by caller
            errors.append(exc)
            ready.set()

    thread = threading.Thread(target=serve)
    thread.start()
    assert ready.wait(5)
    assert errors == []
    return thread, errors


def test_client_publishes_only_id_streams_output_and_removes_on_terminal(
    client_environment, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    workspace, socket_path = client_environment
    observed: dict[str, object] = {}

    def action(connection: socket.socket) -> None:
        kind, payload = recv_frame(connection)
        observed["kind"] = kind
        observed["request_id"] = decode_request(payload)
        observed["manifest"] = read_manifest(workspace, HANDLE, REQUEST_ID)
        send_frame(connection, b"O", b"normal output\n")
        send_frame(connection, b"E", b"diagnostic\xff\n")
        send_frame(connection, b"X", encode_terminal(7, "application failed"))

    thread, errors = _server(socket_path, action)
    try:
        assert client_module.run(["solver", "--flag", "value with spaces"]) == 7
    finally:
        thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert observed["kind"] == b"Q"
    assert observed["request_id"] == REQUEST_ID
    manifest = cast(Manifest, observed["manifest"])
    assert manifest.argv == ("solver", "--flag", "value with spaces")
    assert manifest.cwd == "jobs/example/run"
    environment = cast(Mapping[str, str], dict(manifest.environment))
    assert environment["PATH"] == "/usr/bin"
    assert environment["LD_LIBRARY_PATH"] == "/site/mpi/lib"
    assert environment["MODULEPATH"] == "/site/modules"
    assert environment["USER_VALUE"] == "visible"
    assert "BASH_FUNC_module%%" not in environment
    assert not any(
        name.startswith(("PMI_", "PMIX_", "OMPI_", "OPAL_", "SLURM_", "HTTK_DAEMON_MPI_")) for name in environment
    )
    captured = capsysbinary.readouterr()
    assert captured.out == b"normal output\n"
    assert captured.err == b"diagnostic\xff\napplication failed\n"
    assert not (workspace / ".httk-workspace" / "mpi" / HANDLE / f"{REQUEST_ID}.json").exists()


def test_client_transport_eof_leaves_manifest_for_diagnosis(client_environment) -> None:
    workspace, socket_path = client_environment

    def action(connection: socket.socket) -> None:
        assert recv_frame(connection)[0] == b"Q"

    thread, errors = _server(socket_path, action)
    try:
        with pytest.raises(EOFError):
            client_module.run(["solver"])
    finally:
        thread.join(5)
    assert errors == []
    assert (workspace / ".httk-workspace" / "mpi" / HANDLE / f"{REQUEST_ID}.json").exists()


def test_client_invalid_terminal_leaves_manifest(client_environment) -> None:
    workspace, socket_path = client_environment

    def action(connection: socket.socket) -> None:
        assert recv_frame(connection)[0] == b"Q"
        send_frame(connection, b"X", b"{}")

    thread, errors = _server(socket_path, action)
    try:
        with pytest.raises(ValueError):
            client_module.run(["solver"])
    finally:
        thread.join(5)
    assert errors == []
    assert (workspace / ".httk-workspace" / "mpi" / HANDLE / f"{REQUEST_ID}.json").exists()


def test_connect_failure_leaves_published_manifest(client_environment) -> None:
    workspace, _socket_path = client_environment

    with pytest.raises(OSError):
        client_module.run(["solver"])

    assert (workspace / ".httk-workspace" / "mpi" / HANDLE / f"{REQUEST_ID}.json").exists()


@pytest.mark.parametrize(
    ("environment", "policy", "message"),
    [
        ({}, _policy(), "protected daemon MPI manager"),
        (
            {"HTTK_DAEMON_MPI_HANDLE": HANDLE, "HTTK_DAEMON_MPI_PROFILE": "parallel"},
            _policy(mpi_profile=False),
            "not configured for MPI",
        ),
        (
            {"HTTK_DAEMON_MPI_HANDLE": HANDLE, "HTTK_DAEMON_MPI_PROFILE": "parallel"},
            _policy(mpi_settings=False),
            "not configured for MPI",
        ),
    ],
)
def test_client_requires_matching_explicit_mpi_manager_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    policy,
    message: str,
) -> None:
    workspace = tmp_path / "workspace"
    (workspace / ".httk-workspace").mkdir(parents=True)
    monkeypatch.setattr(client_module, "_WORKSPACE", workspace)
    monkeypatch.setattr(client_module.os, "environ", environment)
    monkeypatch.setattr(client_module, "load_policy", lambda _path: policy)

    with pytest.raises(RuntimeError, match=message):
        client_module.run(["solver"])

    assert not (workspace / ".httk-workspace" / "mpi").exists()


def test_client_refuses_cwd_outside_workspace_before_publication(
    client_environment, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _socket_path = client_environment
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)

    with pytest.raises(RuntimeError, match="cwd"):
        client_module.run(["solver"])

    assert not (workspace / ".httk-workspace" / "mpi").exists()


def test_client_refuses_symlink_manifest_directory(client_environment) -> None:
    workspace, _socket_path = client_environment
    outside = workspace.parent / "outside"
    outside.mkdir()
    (workspace / ".httk-workspace" / "mpi").symlink_to(outside)

    with pytest.raises(OSError):
        client_module.run(["solver"])

    assert not list(outside.iterdir())


def test_cleanup_error_preserves_confirmed_status(
    client_environment, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _workspace, socket_path = client_environment

    def action(connection: socket.socket) -> None:
        assert recv_frame(connection)[0] == b"Q"
        send_frame(connection, b"X", encode_terminal(0))

    thread, errors = _server(socket_path, action)
    monkeypatch.setattr(
        MailboxDirectory,
        "remove",
        lambda *_args: (_ for _ in ()).throw(OSError("injected cleanup failure")),
    )
    try:
        assert client_module.run(["solver"]) == 0
    finally:
        thread.join(5)
    assert errors == []
    assert "cleanup failed" in caplog.text
