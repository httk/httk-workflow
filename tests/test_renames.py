"""What the renames of the project promise, held to by test rather than by note.

Several things were renamed as the package settled, and each has a way of going
wrong no other test would notice:

* *computer* became *remote*, git's word for the same idea, everywhere — the CLI
  group, the Python API, the bundle metadata file, and the directory the
  definitions live in;
* the group that *was* called ``remote`` — send, fetch, offer, retire — became
  ``transfer``;
* the per-user remote definitions and identity keys moved from the data home to
  the configuration home, with a one-time migration of whatever is still there;
* ``WorkflowWorkspace`` became ``Workspace``, and the packaged VASP runners moved
  into :mod:`httk.workflow.vasp.runners`.

The superseded spellings are now **removed**, not merely hidden. This module
asserts both halves: the canonical spellings work, and the old ones are gone —
so restoring a superseded name by accident becomes a failing test.
"""

import json
import logging
import os
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from httk.core import CLIContext

import httk.workflow
from httk.workflow import TaskManager, Workspace
from httk.workflow import workflow_cli as cli
from httk.workflow.adapters import (
    METADATA_FILE,
    add_remote,
    list_remotes,
    metadata_path,
    resolve_remote,
)
from httk.workflow.configuration import config_home, data_home, keys_home, remotes_home
from httk.workflow.projects import initialize_project
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.runners import RUNNERS, runner_package, runner_path, runner_reference
from httk.workflow.scaffold import PACKAGED_TEMPLATES, new_job
from httk.workflow.workflow_cli import command


@pytest.fixture
def isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point both XDG homes inside *tmp_path* and return a fresh project."""

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    project = tmp_path / "project"
    initialize_project(project, name="renames")
    return project


# ---------------------------------------------------------------------------
# computer -> remote
# ---------------------------------------------------------------------------


def test_the_computer_group_is_gone(isolated: Path) -> None:
    """``remote`` is the only spelling; the old ``computer`` group no longer parses."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", isolated))
    assert parser.parse_args(["remote", "list"]).handler is cli.handle_remote_list
    with pytest.raises(SystemExit):
        parser.parse_args(["computer", "list"])


def test_adding_a_remote_writes_remote_json_below_remotes(isolated: Path) -> None:
    context = CLIContext("httk", isolated)
    assert command(["remote", "add", "cluster", "--template", "local"], context) == 0

    bundle = isolated / ".httk-project" / "remotes" / "cluster"
    assert (bundle / METADATA_FILE).is_file()
    assert not (bundle / "computer.json").exists()
    assert metadata_path(bundle).name == METADATA_FILE
    assert resolve_remote("cluster", project=isolated).bundle == bundle
    assert [row["name"] for row in list_remotes(isolated)] == ["cluster"]


def test_a_global_remote_lands_in_the_configuration_home(isolated: Path) -> None:
    assert command(["remote", "add", "shared", "--template", "local", "--global"], CLIContext("httk", isolated)) == 0
    assert (config_home() / "remotes" / "shared" / METADATA_FILE).is_file()
    assert not (data_home() / "computers").exists()


def test_a_legacy_computer_json_bundle_is_no_longer_read(isolated: Path) -> None:
    """``remote.json`` below ``remotes/`` is the only spelling a bundle is read under."""

    bundle = add_remote("legacy", template="local", project=isolated)
    # Rename the metadata file to the superseded name it once fell back to.
    (bundle / METADATA_FILE).rename(bundle / "computer.json")

    assert metadata_path(bundle).name == METADATA_FILE
    with pytest.raises(ValueError):
        resolve_remote("legacy", project=isolated)


def test_the_wire_format_names_keep_their_historical_spelling() -> None:
    """The file was renamed; the protocol identifiers in it were not."""

    from httk.workflow import adapter_runtime, adapters

    assert adapters.ADAPTER_FORMAT == "httk-computer-adapter"
    assert adapters.REQUEST_FORMAT == "httk-computer-request"
    assert adapters.RESULT_FORMAT == "httk-computer-result"
    assert adapter_runtime.RESULT_FORMAT == adapters.RESULT_FORMAT
    for template in ("local", "local-slurm", "ssh-slurm"):
        packaged = Path(httk.workflow.__file__).with_name("adapter_templates") / template
        assert (packaged / METADATA_FILE).is_file()
        assert json.loads((packaged / METADATA_FILE).read_text(encoding="utf-8"))["format"] == adapters.ADAPTER_FORMAT


def test_the_deprecated_python_spellings_are_gone() -> None:
    from httk.workflow import adapters

    for name in ("add_computer", "list_computers", "resolve_computer", "split_computer", "import_v1_computer"):
        assert not hasattr(adapters, name), f"adapters still exposes the removed alias {name!r}"
    assert not hasattr(adapters, "ComputerTarget")
    # The canonical spellings are what remains.
    assert callable(adapters.add_remote) and callable(adapters.resolve_remote)


# ---------------------------------------------------------------------------
# the transfer group, and the frozen protocol spellings
# ---------------------------------------------------------------------------


def _sample(action: str) -> list[str]:
    """The least one transfer subcommand needs in order to parse."""

    if action == "fetch":
        return ["--remote", "cluster"]
    if action == "offer":
        return ["WS", "--destination-workspace-id", "UUID"]
    if action == "retire":
        return ["WS", "JOB"]
    if action == "receive":
        return ["--workspace", "/w", "--bundle", "/b"]
    if action == "send":
        return ["cluster", "JOB"]
    return ["cluster"]


def test_the_transfer_group_is_the_former_remote_group(tmp_path: Path) -> None:
    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    expected = {
        "send": cli.handle_transfer_send,
        "fetch": cli.handle_transfer_fetch,
        "offer": cli.handle_transfer_offer,
        "retire": cli.handle_transfer_retire,
        "start-manager": cli.handle_transfer_operation,
        "status": cli.handle_transfer_operation,
        "receive": cli.handle_transfer_receive,
    }
    for action, handler in expected.items():
        assert parser.parse_args(["transfer", action, *_sample(action)]).handler is handler
    # The superseded ``tasks`` group is gone entirely.
    with pytest.raises(SystemExit):
        parser.parse_args(["tasks", "offer", "WS", "--destination-workspace-id", "UUID"])


def test_the_transfer_commands_name_a_remote(tmp_path: Path) -> None:
    """``REMOTE`` is what the argument is, on both the positional and the option."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    assert parser.parse_args(["transfer", "send", "cluster", "JOB"]).remote == "cluster"
    assert parser.parse_args(["transfer", "status", "cluster"]).remote == "cluster"
    assert parser.parse_args(["transfer", "fetch", "--remote", "cluster"]).remote == "cluster"
    # The superseded ``--computer`` option is gone.
    with pytest.raises(SystemExit):
        parser.parse_args(["transfer", "fetch", "--computer", "cluster"])


def test_neither_removed_alias_is_advertised_or_parses(tmp_path: Path, capsys) -> None:
    assert command(["--help"], CLIContext("httk", tmp_path)) == 0
    printed = capsys.readouterr().out
    assert "remote" in printed and "transfer" in printed
    assert "computer" not in printed and "tasks" not in printed
    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    for group in ("computer", "tasks", "internal"):
        with pytest.raises(SystemExit):
            parser.parse_args([group, "--help"])


def test_the_protocol_vectors_send_the_frozen_transfer_spellings() -> None:
    """The frozen protocol spelling is the ``transfer`` group from now on."""

    assert cli.REMOTE_RECEIVE_COMMAND == ("httk", "workflow", "transfer", "receive")
    assert cli.REMOTE_OFFER_COMMAND == ("httk", "workflow", "transfer", "offer")
    assert cli.REMOTE_RETIRE_COMMAND == ("httk", "workflow", "transfer", "retire")
    # And the frozen spelling really resolves to its handler.
    parser = cli.build_parser("httk workflow", CLIContext("httk", Path.cwd()))
    assert parser.parse_args(["transfer", "receive", "--workspace", "/w", "--bundle", "/b"]).handler is (
        cli.handle_transfer_receive
    )


# ---------------------------------------------------------------------------
# the XDG move (retained one-time migration)
# ---------------------------------------------------------------------------


def _legacy_tree(tmp_path: Path) -> tuple[Path, Path]:
    """Write what an earlier release left in the data home, with its modes."""

    computers = data_home() / "computers" / "cluster"
    computers.mkdir(parents=True)
    (computers / METADATA_FILE).write_text('{"kind":"local"}\n', encoding="utf-8")
    (computers / "credentials.json").write_text('{"default":{"password":"hunter2"}}\n', encoding="utf-8")
    os.chmod(computers / "credentials.json", 0o600)
    os.chmod(computers.parent, 0o700)
    keys = data_home() / "keys"
    keys.mkdir(parents=True)
    (keys / "identity.seed").write_text("seed\n", encoding="utf-8")
    os.chmod(keys / "identity.seed", 0o600)
    os.chmod(keys, 0o700)
    return computers.parent, keys


def test_the_legacy_data_home_is_adopted_once_and_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    legacy_remotes, legacy_keys = _legacy_tree(tmp_path)

    current = remotes_home()
    assert current == config_home() / "remotes"
    notice = capsys.readouterr().err
    assert str(legacy_remotes) in notice and str(current) in notice

    # The content moved, and the modes moved with it.
    assert not legacy_remotes.exists() and not legacy_keys.exists()
    assert (current / "cluster" / METADATA_FILE).is_file()
    assert (current / "cluster" / "credentials.json").stat().st_mode & 0o777 == 0o600
    assert current.stat().st_mode & 0o777 == 0o700
    assert (keys_home() / "identity.seed").stat().st_mode & 0o777 == 0o600
    assert keys_home().stat().st_mode & 0o777 == 0o700

    # Asking again is a no-op that says nothing.
    assert remotes_home() == current and keys_home() == config_home() / "keys"
    assert capsys.readouterr().err == ""


def test_both_roots_present_prefers_the_new_one_and_reports_the_stale_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    legacy_remotes, _ = _legacy_tree(tmp_path)
    current = config_home() / "remotes" / "elsewhere"
    current.mkdir(parents=True)
    (current / METADATA_FILE).write_text('{"kind":"local"}\n', encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="httk.workflow.configuration"):
        assert remotes_home() == config_home() / "remotes"
    assert "stale legacy directory" in caplog.text
    # Nothing was merged: the new root is exactly what it was.
    assert sorted(path.name for path in (config_home() / "remotes").iterdir()) == ["elsewhere"]
    assert (legacy_remotes / "cluster" / METADATA_FILE).is_file()


# ---------------------------------------------------------------------------
# WorkflowWorkspace -> Workspace
# ---------------------------------------------------------------------------


def test_the_workflow_workspace_alias_is_gone() -> None:
    assert "WorkflowWorkspace" not in httk.workflow.__all__
    assert "Workspace" in httk.workflow.__all__
    with pytest.raises(AttributeError):
        httk.workflow.WorkflowWorkspace  # pyright: ignore[reportAttributeAccessIssue]
    from httk.workflow import workspace

    with pytest.raises(AttributeError):
        workspace.WorkflowWorkspace  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# the vasp package
# ---------------------------------------------------------------------------


def test_the_vasp_helpers_live_only_in_the_vasp_package() -> None:
    import httk.workflow.vasp as vasp

    for name in ("prepare_vasp_inputs", "read_poscar_header", "run_vasp"):
        assert callable(getattr(vasp, name))
        # They were subtracted from the package root.
        assert not hasattr(httk.workflow, name), f"httk.workflow still re-exports vasp.{name}"
    assert vasp.__doc__ is not None and vasp.__doc__.startswith("Small, dependency-free VASP runner helpers")


def test_the_packaged_runners_moved_with_the_science_they_implement() -> None:
    for name in RUNNERS:
        assert runner_package(name) == "httk.workflow.vasp.runners"
        installed = runner_path(name)
        assert installed.is_file()
        assert installed.parent.name == "runners" and installed.parent.parent.name == "vasp"
        assert runner_reference(name)["path"] == f"pkg:httk.workflow.vasp.runners/{name}"
    with pytest.raises(ValueError, match="unknown packaged runner"):
        runner_path("nothing.py")


def test_the_scaffold_template_names_are_unchanged(tmp_path: Path) -> None:
    assert PACKAGED_TEMPLATES == ("vasp-relax", "vasp-relax-bash", "vasp-static", "vasp-relax-static")
    workspace = Workspace.initialize(tmp_path / "workspace", extensions=["transactional-data-v1"])
    job = new_job(workspace, "vasp-relax", publish="installed", tag="silicon")
    assert job.runner["path"] == "pkg:httk.workflow.vasp.runners/vasp_relax.py"
    assert job.workflow == "httk.vasp.relax"


def test_a_job_pinning_the_new_package_path_runs(tmp_path: Path) -> None:
    """The manager resolves and executes the runner at its new package path.

    The payload deliberately has no structure, so the runner itself refuses the
    job by name. That failure is the proof: it can only be reported by a runner
    that was found, read, and run.
    """

    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = runner_reference("vasp_static.py")
    assert reference["path"] == "pkg:httk.workflow.vasp.runners/vasp_static.py"
    job = prepare_job_payload(
        tmp_path / "payload",
        JobSpec(
            name="packaged by its new path",
            workflow="httk.vasp.static",
            runner_path=str(reference["path"]),
            runner_source="installed",
            runner_sha256=str(reference["sha256"]),
            tag="packaged",
            initial_step="prepare",
            maximum_attempts_per_activation=1,
        ),
    )
    workspace.submit(tmp_path / "payload", "project/packaged")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    markers = workspace.find_markers(job.job_key)
    assert len(markers) == 1 and markers[0].kind == "failed"
    frame = workspace.read_state(markers[0])
    failure = frame.get("failure")
    assert isinstance(failure, dict) and failure["code"] == "vasp.input_missing"
