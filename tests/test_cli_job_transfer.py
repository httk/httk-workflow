"""``httk job transfer`` accepting a workspace name or a workspace directory.

The verb moved from the (now-removed) user-facing ``httk workflow transfer``
into ``httk job transfer``; the hidden protocol spellings
(``httk workflow transfer receive|offer|retire``) are unaffected and are
covered by their own tests elsewhere.
"""

from pathlib import Path

from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload
from httk.workflow.workflow_cli import command


def _payload(root: Path, name: str = "job") -> Path:
    """Prepare one minimal job payload."""

    payload = root / name
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "run"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    runner.chmod(0o755)
    prepare_job_payload(
        payload,
        JobSpec(name=name, workflow="tests.job_transfer", runner_path="files/run"),
    )
    return payload


def test_unregistered_workspaces_are_addressed_by_directory(tmp_path: Path) -> None:
    """Neither endpoint needs to be registered: a workspace directory suffices."""

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_payload(tmp_path), "jobs")
    context = CLIContext("httk", tmp_path)

    assert (
        command(
            ["job", "transfer", "--job", marker.job_id, str(source.root), str(destination.root)],
            context,
        )
        == 0
    )

    arrived = destination.find_marker_by_id(marker.job_id)
    assert arrived is not None and arrived.kind == marker.kind
    assert source.find_marker_by_id(marker.job_id) is None


def test_relative_directories_resolve_against_the_context_cwd(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    marker = source.submit(_payload(tmp_path), "jobs")
    context = CLIContext("httk", tmp_path)

    assert command(["job", "transfer", "--job", marker.job_id, "source", "destination"], context) == 0

    assert destination.find_marker_by_id(marker.job_id) is not None
    assert source.find_marker_by_id(marker.job_id) is None


def test_a_registered_name_shadows_a_same_named_directory(tmp_path: Path) -> None:
    registered = Workspace.initialize(tmp_path / "registered-ws")
    shadowed_dir = Workspace.initialize(tmp_path / "ws")
    context = CLIContext("httk", tmp_path)
    register_ws(context, registered.root, "ws")

    other = Workspace.initialize(tmp_path / "other")
    marker_for_name = registered.submit(_payload(tmp_path, "for-name"), "jobs")
    marker_for_dir = shadowed_dir.submit(_payload(tmp_path, "for-dir"), "jobs")

    # "ws" resolves the registered name, not the directory of the same name.
    assert command(["job", "transfer", "--job", marker_for_name.job_id, "ws", str(other.root)], context) == 0
    assert other.find_marker_by_id(marker_for_name.job_id) is not None
    assert shadowed_dir.find_marker_by_id(marker_for_dir.job_id) is not None

    # "./ws" addresses the directory unambiguously.
    assert command(["job", "transfer", "--job", marker_for_dir.job_id, "./ws", str(other.root)], context) == 0
    assert other.find_marker_by_id(marker_for_dir.job_id) is not None
    assert shadowed_dir.find_marker_by_id(marker_for_dir.job_id) is None


def test_an_endpoint_that_is_neither_a_name_nor_a_directory_is_refused(tmp_path: Path, capsys) -> None:
    source = Workspace.initialize(tmp_path / "source")
    context = CLIContext("httk", tmp_path)

    assert command(["job", "transfer", "--job", "anything", str(source.root), "nowhere"], context) != 0
    assert "neither a registered workspace name nor a workspace directory" in capsys.readouterr().err


def test_the_old_workflow_spelling_is_gone_but_protocol_still_dispatches(tmp_path: Path) -> None:
    """`httk workflow transfer SRC DST` is removed; the protocol spellings are not.

    Full protocol-dispatch coverage (e.g. ``transfer offer``/``receive``/
    ``retire`` actually running) is exercised by ``tests/test_fetch.py`` and
    ``tests/test_renames.py::test_the_protocol_vectors_send_the_frozen_transfer_spellings``;
    here it is enough to show the old user-facing spelling no longer parses.
    """

    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    context = CLIContext("httk", tmp_path)

    assert command(["transfer", "--job", "anything", str(source.root), str(destination.root)], context) != 0
