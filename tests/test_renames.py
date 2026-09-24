"""What the renames of the project promise, held to by test rather than by note.

Several things were renamed as the package settled, and each has a way of going
wrong no other test would notice:

* *computer* became *remote*, git's word for the same idea, everywhere — the CLI
  group, the Python API, the bundle metadata file, and the directory the
  definitions live in;
* the group that *was* called ``remote`` — send, fetch, offer, retire — became
  ``transfer``;
* the per-user remote definitions and identity keys moved from the data home to
  the configuration home;
* ``WorkflowWorkspace`` became ``Workspace``, and the packaged VASP workflows
  left the distribution for https://github.com/httk/workflows-vasp.

The superseded spellings are now **removed**, not merely hidden. This module
asserts both halves: the canonical spellings work, and the old ones are gone —
so restoring a superseded name by accident becomes a failing test.
"""

import json
import re
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

import httk.workflow
from httk.workflow import Attempt
from httk.workflow import workflow_cli as cli
from httk.workflow.adapters import (
    METADATA_FILE,
    add_remote,
    list_remotes,
    metadata_path,
    resolve_remote,
)
from httk.workflow.configuration import config_home, data_home
from httk.workflow.projects import PROJECT_DIRECTORY, initialize_project
from httk.workflow.scaffold import registered_workflows
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
    assert command(["remote", "add", "--template", "local", "cluster"], context) == 0

    bundle = isolated / PROJECT_DIRECTORY / "remotes" / "cluster"
    assert (bundle / METADATA_FILE).is_file()
    assert not (bundle / "computer.json").exists()
    assert metadata_path(bundle).name == METADATA_FILE
    assert resolve_remote("cluster", project=isolated).bundle == bundle
    assert [row["name"] for row in list_remotes(isolated)] == ["cluster"]


def test_a_global_remote_lands_in_the_configuration_home(isolated: Path) -> None:
    assert command(["remote", "add", "--template", "local", "--global", "shared"], CLIContext("httk", isolated)) == 0
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
    for template in ("local", "ssh"):
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


def test_the_transfer_group_is_the_former_remote_group(tmp_path: Path) -> None:
    """The former send/fetch/offer/retire/status verbs are one ``transfer`` verb now."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    parsed = parser.parse_args(["transfer", "--job", "J", "source", "destination"])
    assert parsed.handler is cli.handle_transfer
    # The superseded per-verb handlers are removed, not merely hidden.
    for name in ("handle_transfer_send", "handle_transfer_fetch", "handle_transfer_operation"):
        assert not hasattr(cli, name), f"workflow_cli still exposes the removed handler {name!r}"
    # The superseded ``tasks`` group is gone entirely.
    with pytest.raises(SystemExit):
        parser.parse_args(["tasks", "offer", "WS", "--destination-workspace-id", "UUID"])


def test_the_transfer_commands_name_a_remote(tmp_path: Path) -> None:
    """The transfer verb captures its source and destination after its options."""

    parser = cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    parsed = parser.parse_args(["transfer", "--json", "src", "dst"])
    assert (parsed.source, parsed.destination, parsed.json) == ("src", "dst", True)
    # The superseded ``--computer`` option is gone.
    with pytest.raises(SystemExit):
        parser.parse_args(["transfer", "--computer", "cluster"])


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
    assert callable(cli.handle_transfer_receive)


# ---------------------------------------------------------------------------
# WorkflowWorkspace -> Workspace
# ---------------------------------------------------------------------------


def test_the_workflow_workspace_alias_is_gone() -> None:
    assert "WorkflowWorkspace" not in httk.workflow.__all__
    assert "Workspace" in httk.workflow.__all__
    with pytest.raises(AttributeError):
        getattr(httk.workflow, "WorkflowWorkspace")  # noqa: B009 - test probes a removed compatibility alias
    from httk.workflow import workspace

    with pytest.raises(AttributeError):
        getattr(workspace, "WorkflowWorkspace")  # noqa: B009 - test probes a removed compatibility alias


# ---------------------------------------------------------------------------
# the vasp package
# ---------------------------------------------------------------------------


def test_the_vasp_helpers_live_only_in_the_vasp_package() -> None:
    from httk.workflow import vasp

    for name in ("prepare_vasp_inputs", "read_poscar_header", "run_vasp"):
        assert callable(getattr(vasp, name))
        # They were subtracted from the package root.
        assert not hasattr(httk.workflow, name), f"httk.workflow still re-exports vasp.{name}"
    assert vasp.__doc__ is not None and vasp.__doc__.startswith("Small, dependency-free VASP runner helpers")


def test_the_packaged_vasp_workflows_are_gone() -> None:
    """They live in https://github.com/httk/workflows-vasp; only the helpers stay."""

    import importlib

    for module in ("httk.workflow.runners", "httk.workflow.vasp.runners", "httk.workflow.vasp.workflows"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module)
    assert not any(name.startswith("httk.vasp.") for name in registered_workflows())


@pytest.mark.usefixtures("relax_workflow")
def test_retired_lifecycle_spellings_are_gone() -> None:
    from httk.workflow import scaffold

    for name in (
        "TemplateProvider",
        "register_template",
        "resolve_template",
        "registered_templates",
        "template_provider",
        "packaged_template",
        "JobTemplate",
    ):
        assert not hasattr(scaffold, name)
    assert not hasattr(httk.workflow, "HarvestRecord")
    assert not hasattr(httk.workflow, "harvest")
    providers = [scaffold.workflow_provider("test-relax")]
    assert {provider.workflow_id for provider in providers if provider is not None} == {"tests.relax"}
    from httk.workflow.protocol import JobSpec

    assert not hasattr(JobSpec, "inputs")
    assert not hasattr(Attempt, "inputs")
    for provider in providers:
        assert provider is not None
        declaration = provider.declarations["workflow"]
        assert "parameters" not in declaration
        assert "output_types" not in declaration
    parser = cli.build_parser("httk workflow", CLIContext("httk", Path.cwd()))
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["job", "new", "--workspace", "WS", "--workflow", "test-relax", "--parameter-from", "structure", "x"]
        )
    root = Path(httk.workflow.__file__).parents[2]
    retired = re.compile(
        r"\b(?:TemplateProvider|register_template|resolve_template|registered_templates|template_provider|"
        r"packaged_template|JobTemplate|HarvestRecord|campaign_harvest)\b|--template|\bharvest\b"
    )
    for path in root.rglob("*.py"):
        if path.name in {"_transfer.py", "adapters.py"}:
            continue
        assert not retired.search(path.read_text(encoding="utf-8")), path
