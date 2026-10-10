"""Monitor transfer tests split out of ``test_monitor.py``: they drive the removed transfer stack."""

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, transfers
from httk.workflow.registry import WorkspaceBinding
from httk.workflow.workflow_cli import _transfer as transfer_cli


def test_known_marker_detach_skips_full_marker_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Known-marker detaches inspect only waiting parents (joins) and transferring markers (fences)."""

    from test_eject_adopt import _payload

    workspace = Workspace.initialize(tmp_path / "known-marker")
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    scanned: list[tuple[str, ...]] = []
    real_scan = Workspace.scan_marker_entries

    def recording(self: Workspace, kinds: Any = None) -> Any:
        scanned.append(tuple(kinds or ()))
        return real_scan(self, kinds)

    monkeypatch.setattr(Workspace, "scan_marker_entries", recording)
    bundle = transfers.detach_job(
        workspace, marker.job_id, marker=marker, destination_workspace_id="00000000-0000-0000-0000-000000000002"
    )
    assert bundle.is_dir()
    assert scanned and all(set(kinds) <= {"waiting", "transferring"} and kinds for kinds in scanned)


def test_quiet_remote_relay_does_not_write_worker_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The returning remote-to-remote transfer helper honors quiet workers."""

    source = WorkspaceBinding("source:jobs", "source", None)
    destination = WorkspaceBinding("destination:jobs", "destination", None)
    context = CLIContext("httk", tmp_path)
    monkeypatch.setattr(
        transfer_cli,
        "resolve_workspace",
        lambda name, project: source if name == source.name else destination,
    )
    monkeypatch.setattr(transfer_cli, "resolve_remote", lambda *_args, **_kwargs: SimpleNamespace())
    monkeypatch.setattr(transfer_cli, "_remote_workspace_settings", lambda *_args, **_kwargs: None)
    seen: dict[str, bool] = {}

    def fake_relay(*_args: object, **kwargs: object) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        seen["quiet"] = bool(kwargs["quiet"])
        return [], []

    monkeypatch.setattr(transfer_cli, "_transfer_remote_to_remote", fake_relay)
    arguments = Namespace(
        source=source.name,
        destination=destination.name,
        jobs=[],
        state=None,
        placement=None,
        adapter_timeout=None,
        strict_environment=False,
    )
    assert transfer_cli.run_transfer_verb_result(arguments, context, quiet=True) == {"moved": [], "retired": []}
    assert seen == {"quiet": True}
    assert capsys.readouterr().out == ""
