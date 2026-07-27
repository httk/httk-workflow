"""What the renames of this release promise, held to by test rather than by note.

Four things were renamed at once, and each one has a way of going wrong that no
other test in this suite would notice:

* *computer* became *remote*, git's word for the same idea, everywhere — the CLI
  group, the Python API, the bundle metadata file, and the directory the
  definitions live in;
* the group that *was* called ``remote`` — send, fetch, offer, retire — became
  ``transfer``, and the spellings that cross an ssh connection did **not** move
  with it;
* the per-user remote definitions and identity keys moved from the data home to
  the configuration home, with a one-time migration of whatever is still there;
* ``WorkflowWorkspace`` became ``Workspace``, and the packaged VASP runners moved
  into :mod:`httk.workflow.vasp.runners`.

Every superseded spelling that is documented as still working is exercised here,
so that removing one becomes a failing test rather than somebody's surprise.
"""

import json
import logging
import os
import shutil
import warnings
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from httk.core import CLIContext

import httk.workflow
from httk.workflow import JobSpec, TaskManager, Workspace, prepare_job_payload
from httk.workflow import workflow_cli as cli
from httk.workflow.adapters import (
    LEGACY_METADATA_FILE,
    METADATA_FILE,
    add_remote,
    list_remotes,
    metadata_path,
    queue_settings,
    resolve_remote,
)
from httk.workflow.configuration import config_home, data_home, keys_home, remotes_home
from httk.workflow.hygiene import describe_remote
from httk.workflow.projects import initialize_project
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


def test_the_remote_group_is_the_former_computer_group(isolated: Path) -> None:
    """Every leaf of the old group is a leaf of the new one, doing the same work."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", isolated))
    for action, arguments in (
        ("list", []),
        ("add", ["NAME"]),
        ("configure", ["NAME"]),
        ("install", ["NAME"]),
        ("import-v1", ["SOURCE"]),
        ("show", ["NAME"]),
        ("remove", ["NAME"]),
    ):
        assert (
            parser.parse_args(["remote", action, *arguments]).handler
            is parser.parse_args(["computer", action, *arguments]).handler
        )


def test_adding_a_remote_writes_remote_json_below_remotes(isolated: Path) -> None:
    context = CLIContext("httk", isolated)
    assert command(["remote", "add", "cluster", "--template", "local"], context) == 0

    bundle = isolated / ".httk-project" / "remotes" / "cluster"
    assert (bundle / METADATA_FILE).is_file()
    assert not (bundle / LEGACY_METADATA_FILE).exists()
    assert metadata_path(bundle).name == METADATA_FILE
    assert resolve_remote("cluster", project=isolated).bundle == bundle
    assert [row["name"] for row in list_remotes(isolated)] == ["cluster"]


def test_a_global_remote_lands_in_the_configuration_home(isolated: Path) -> None:
    assert command(["remote", "add", "shared", "--template", "local", "--global"], CLIContext("httk", isolated)) == 0
    assert (config_home() / "remotes" / "shared" / METADATA_FILE).is_file()
    assert not (data_home() / "computers").exists()


def test_a_legacy_computer_json_bundle_still_resolves(isolated: Path, caplog) -> None:
    """A definition written before the rename is read where it lies."""

    bundle = add_remote("legacy", template="local", project=isolated)
    legacy_root = isolated / ".httk-project" / "computers" / "legacy"
    legacy_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(bundle), str(legacy_root))
    metadata = json.loads((legacy_root / METADATA_FILE).read_text(encoding="utf-8"))
    metadata["queues"]["default"] = {"workspace": "/scratch/legacy"}
    (legacy_root / LEGACY_METADATA_FILE).write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
    (legacy_root / METADATA_FILE).unlink()

    with caplog.at_level(logging.DEBUG, logger="httk.workflow.adapters"):
        target = resolve_remote("legacy", project=isolated)
    assert target.bundle == legacy_root and target.project_local is True
    assert metadata_path(legacy_root).name == LEGACY_METADATA_FILE
    assert "legacy adapter metadata" in caplog.text
    assert queue_settings(legacy_root, "default") == {"workspace": "/scratch/legacy"}

    # It is listed, described, and reported under the file it really has.
    assert [row["name"] for row in list_remotes(isolated)] == ["legacy"]
    description = describe_remote("legacy", project=isolated)
    assert description["format"] == "httk-remote-description"
    queues = description["queues"]
    assert isinstance(queues, dict)
    assert queues["default"]["settings_source"] == {"workspace": LEGACY_METADATA_FILE}


def test_configuring_a_legacy_bundle_rewrites_the_file_it_has(isolated: Path) -> None:
    """Nothing leaves two metadata files behind in one bundle."""

    bundle = add_remote("legacy", template="local", project=isolated)
    shutil.move(str(bundle / METADATA_FILE), str(bundle / LEGACY_METADATA_FILE))

    context = CLIContext("httk", isolated)
    assert command(["remote", "configure", "legacy", "--set", "workspace=/scratch/x"], context) == 0
    assert not (bundle / METADATA_FILE).exists()
    stored = json.loads((bundle / LEGACY_METADATA_FILE).read_text(encoding="utf-8"))
    assert stored["queues"]["default"] == {"workspace": "/scratch/x"}


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


def test_the_deprecated_python_spellings_still_reach_the_same_functions() -> None:
    from httk.workflow import adapters

    assert adapters.add_computer is adapters.add_remote
    assert adapters.list_computers is adapters.list_remotes
    assert adapters.resolve_computer is adapters.resolve_remote
    assert adapters.split_computer is adapters.split_remote
    assert adapters.import_v1_computer is adapters.import_v1_remote
    assert adapters.ComputerTarget is adapters.RemoteTarget


# ---------------------------------------------------------------------------
# the transfer group, and the spellings that stayed
# ---------------------------------------------------------------------------


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
        # And the hidden spelling the protocol itself sends still parses.
        assert parser.parse_args(["tasks", action, *_sample(action)]).handler is handler


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


def test_the_transfer_commands_name_a_remote(tmp_path: Path) -> None:
    """``REMOTE`` is what the argument is, on both the positional and the option."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    assert parser.parse_args(["transfer", "send", "cluster", "JOB"]).remote == "cluster"
    assert parser.parse_args(["transfer", "status", "cluster"]).remote == "cluster"
    assert parser.parse_args(["transfer", "fetch", "--remote", "cluster"]).remote == "cluster"
    # The superseded option spelling still names the same value.
    assert parser.parse_args(["transfer", "fetch", "--computer", "cluster"]).remote == "cluster"


def test_neither_hidden_alias_is_advertised(tmp_path: Path, capsys) -> None:
    assert command(["--help"], CLIContext("httk", tmp_path)) == 0
    printed = capsys.readouterr().out
    assert "remote" in printed and "transfer" in printed
    assert "computer" not in printed and "tasks" not in printed


def test_the_protocol_vectors_still_send_the_frozen_spellings() -> None:
    """The rename must not reach an installation on the far side of an adapter."""

    assert cli.REMOTE_RECEIVE_COMMAND == ("httk", "workflow", "tasks", "receive")
    assert cli.REMOTE_OFFER_COMMAND == ("httk", "workflow", "tasks", "offer")
    assert cli.REMOTE_RETIRE_COMMAND == ("httk", "workflow", "tasks", "retire")


# ---------------------------------------------------------------------------
# the XDG move
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


def test_the_workflow_workspace_alias_warns_and_works(tmp_path: Path) -> None:
    with pytest.warns(DeprecationWarning, match="WorkflowWorkspace"):
        legacy = httk.workflow.WorkflowWorkspace
    assert legacy is Workspace

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from httk.workflow.workspace import WorkflowWorkspace as from_module
    assert from_module is Workspace

    # The alias is the class, so it really does the work under either name.
    workspace = legacy.initialize(tmp_path / "workspace")
    assert isinstance(workspace, Workspace)
    assert Workspace(workspace.root).workspace_id == workspace.workspace_id


def test_the_canonical_name_comes_first_and_both_are_exported() -> None:
    exported = httk.workflow.__all__
    assert exported.index("Workspace") < exported.index("WorkflowWorkspace")
    with pytest.raises(AttributeError):
        httk.workflow.NoSuchName  # pyright: ignore[reportAttributeAccessIssue]


# ---------------------------------------------------------------------------
# the vasp package
# ---------------------------------------------------------------------------


def test_the_vasp_package_keeps_the_module_api_it_had() -> None:
    from httk.workflow.vasp import prepare_vasp_inputs, read_poscar_header, run_vasp

    assert prepare_vasp_inputs is httk.workflow.prepare_vasp_inputs
    assert read_poscar_header is httk.workflow.read_poscar_header
    assert run_vasp is httk.workflow.run_vasp
    import httk.workflow.vasp as vasp

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
