"""Enabling the exchange extension, and the confinement rule keyed on it."""

import errno
import json
import logging
import os
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity, register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow._exchange import (
    EXCHANGE_SUBDIRECTORIES,
    ExchangeUnavailableError,
    enable_exchange,
    exchange_directory,
)
from httk.workflow.errors import ConfinementUnavailableError, FormatError, SealedError
from httk.workflow.seals import seal_workspace
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload


def _events(caplog: pytest.LogCaptureFixture, name: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if getattr(record, "event", None) == name]


def _extensions(workspace: Workspace) -> list[str]:
    return list(json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))["extensions"])


def test_enable_creates_the_layout_and_records_the_extension(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert "exchange" not in workspace.extensions and _extensions(workspace) == []
    assert enable_exchange(workspace) is True
    exchange = exchange_directory(workspace)
    assert exchange == workspace.root / "exchange"
    for relative in EXCHANGE_SUBDIRECTORIES:
        assert (exchange / relative).is_dir() and not (exchange / relative).is_symlink()
        assert not os.listdir(exchange / relative) or relative == "outbox"
    assert os.listdir(exchange / "outbox") == ["rejected"]
    assert json.loads((exchange / "exchange.json").read_bytes()) == {
        "format": "httk-workspace-exchange",
        "format_version": 1,
        "workspace_id": workspace.workspace_id,
    }
    assert sorted(os.listdir(exchange)) == sorted(
        ["exchange.json", "inbox", "outbox", "requests", "responses", "managers"]
    )
    assert _extensions(workspace) == ["exchange"] and "exchange" in workspace.extensions
    assert "exchange" in Workspace(workspace.root).extensions
    # Nothing is left behind: no birth directory, no rename probe.
    assert os.listdir(workspace.control / "tmp") == []


def test_enable_is_idempotent(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert enable_exchange(workspace)
    (exchange_directory(workspace) / "inbox" / "a-bundle").mkdir()
    before = (workspace.control / "format.json").read_bytes()
    identity = os.stat(exchange_directory(workspace)).st_ino
    assert enable_exchange(workspace) is False
    assert enable_exchange(Workspace(workspace.root)) is False
    assert (workspace.control / "format.json").read_bytes() == before
    assert os.stat(exchange_directory(workspace)).st_ino == identity
    assert os.listdir(exchange_directory(workspace) / "inbox") == ["a-bundle"]


def test_enable_completes_a_half_made_exchange(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    (workspace.root / "exchange" / "inbox").mkdir(parents=True)
    assert enable_exchange(workspace)
    for relative in EXCHANGE_SUBDIRECTORIES:
        assert (workspace.root / "exchange" / relative).is_dir()
    assert (workspace.root / "exchange" / "exchange.json").is_file()
    # The extension recorded but a directory lost: enabling again restores it.
    os.rmdir(workspace.root / "exchange" / "outbox" / "rejected")
    assert enable_exchange(workspace)
    assert (workspace.root / "exchange" / "outbox" / "rejected").is_dir()


@pytest.mark.parametrize("hostile", ["exchange", "inbox", "outbox", "rejected", "file"])
def test_enable_refuses_a_symlinked_or_foreign_exchange(tmp_path: Path, hostile: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    exchange = workspace.root / "exchange"
    if hostile == "exchange":
        exchange.symlink_to(elsewhere)
    elif hostile == "file":
        exchange.write_text("not a directory", encoding="utf-8")
    else:
        relative = {"inbox": "inbox", "outbox": "outbox", "rejected": "outbox/rejected"}[hostile]
        (exchange / relative).parent.mkdir(parents=True, exist_ok=True)
        (exchange / relative).symlink_to(elsewhere)
    with pytest.raises(ExchangeUnavailableError, match="not a real directory"):
        enable_exchange(workspace)
    assert _extensions(workspace) == [] and "exchange" not in workspace.extensions
    assert os.listdir(elsewhere) == []


def test_enable_refuses_when_the_rename_probe_crosses_filesystems(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    rename = os.rename

    def cross_device(src: object, dst: object, **arguments: object) -> None:
        if str(src).startswith(".httk-probe-"):
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        rename(src, dst, **arguments)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "rename", cross_device)
    with pytest.raises(ExchangeUnavailableError, match="different filesystems"):
        enable_exchange(workspace)
    monkeypatch.undo()
    assert _extensions(workspace) == [] and "exchange" not in Workspace(workspace.root).extensions
    assert os.listdir(workspace.control / "tmp") == []
    # Once the filesystems agree, enabling completes.
    assert enable_exchange(workspace) and _extensions(workspace) == ["exchange"]


def test_enable_refuses_a_sealed_workspace(tmp_path: Path) -> None:
    configure_identity()
    workspace = Workspace.initialize(tmp_path / "workspace")
    seal_workspace(workspace)
    with pytest.raises(SealedError):
        enable_exchange(workspace)
    assert not os.path.lexists(workspace.root / "exchange")


def test_initialize_with_the_extension_creates_the_exchange(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", extensions=["exchange"])
    assert _extensions(workspace) == ["exchange"] and "exchange" in workspace.extensions
    assert (workspace.root / "exchange" / "outbox" / "rejected").is_dir()


def test_the_enable_command_reports_and_is_idempotent(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root)
    assert command(["workspace", "exchange", "enable", name], context) == 0
    captured = capsys.readouterr()
    assert f"{workspace.root}\texchange enabled\t{workspace.root / 'exchange'}" in captured.out
    assert "manager.confine=bwrap" in captured.err
    assert command(["workspace", "exchange", "enable", "--by-path", str(workspace.root)], context) == 0
    assert "exchange already enabled" in capsys.readouterr().out
    assert _extensions(workspace) == ["exchange"]


# -- the confinement rule ----------------------------------------------------------------


def test_an_unconfined_manager_refuses_a_workspace_with_the_extension(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    enable_exchange(workspace)
    with pytest.raises(ConfinementUnavailableError, match="exchange extension"):
        TaskManager(Workspace(workspace.root))
    assert not list((workspace.control / "managers").iterdir())


def test_an_unconfined_manager_stops_claiming_when_the_extension_appears(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    first = workspace.submit(_payload(tmp_path / "payloads", "first"), "jobs")
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(Workspace(workspace.root), heartbeat_interval=0.01) as manager,
    ):
        manager.run_until_idle(timeout=60.0)
        done = workspace.find_marker_by_id(first.job_id)
        assert done is not None and done.kind == "succeeded"
        # Another process enables the extension while the manager runs.
        assert enable_exchange(Workspace(workspace.root))
        second = workspace.submit(_payload(tmp_path / "payloads", "second"), "jobs")
        manager.run_until_idle(timeout=60.0)
        held = workspace.find_marker_by_id(second.job_id)
        assert held is not None and held.kind == "ready"
        (event,) = _events(caplog, "confinement_unavailable")
        assert "exchange extension" in str(getattr(event, "reason", ""))
        census = manager._work_census()
        assert census.ready_blocked.get("confinement") == {"manager.confine": 1}


def test_an_unreadable_format_document_stops_claiming(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    path = workspace.control / "format.json"
    good = path.read_bytes()
    with (
        caplog.at_level("INFO", logger="httk.workflow.manager"),
        TaskManager(Workspace(workspace.root), heartbeat_interval=0.01) as manager,
    ):
        marker = workspace.submit(_payload(tmp_path / "payloads", "job"), "jobs")
        manager._register_submissions()
        for damaged in (b"not json", json.dumps({**json.loads(good), "extensions": ["teleport"]}).encode()):
            path.write_bytes(damaged)
            for _ in range(3):
                manager.tick()
            held = workspace.find_marker_by_id(marker.job_id)
            assert held is not None and held.kind == "ready"
            assert _events(caplog, "confinement_unavailable")
            caplog.clear()
            manager._reported.pop("confinement", None)
        path.write_bytes(good)
        manager.run_until_idle(timeout=60.0)
    done = workspace.find_marker_by_id(marker.job_id)
    assert done is not None and done.kind == "succeeded"


def test_refresh_format_refuses_another_workspace_identity(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    other = Workspace.initialize(tmp_path / "other")
    (workspace.control / "format.json").write_bytes((other.control / "format.json").read_bytes())
    with pytest.raises(FormatError, match="now names workspace"):
        workspace.refresh_format()
