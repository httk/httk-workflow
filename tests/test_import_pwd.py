"""Importing Python Workflow Definition documents, and running what was imported.

Nothing here fakes an execution. Every imported document is submitted to a real
workspace and driven to a terminal state by a real
:class:`httk.workflow.TaskManager`, so what is asserted is what the packaged
``pwd_runner.py`` really did with the graph.
"""

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from httk.core import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.compat._integration import runner_path, runner_reference
from httk.workflow.compat.pwd import (
    DEFAULT_MAXIMUM_EMBEDDED_BYTES,
    DOCUMENT_FILE,
    PACKAGE,
    RUNNER,
    PwdFormatError,
    import_pwd,
    load_pwd_document,
    validate_pwd_document,
)
from httk.workflow.models import JobDefinition
from httk.workflow.scaffold import describe_runner
from httk.workflow.workflow_cli import command

#: The helper module the imported documents name their functions in. It is
#: written to disk per test and staged into the payload, which is exactly how a
#: PWD document is meant to travel: one JSON graph plus one Python module.
_MODULE = '''"""Functions for the imported test workflows."""

import json
from pathlib import Path


def _record(log, name):
    path = Path(log)
    entries = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    entries.append(name)
    path.write_text(json.dumps(entries), encoding="utf-8")


def get_prod_and_div(x, y):
    return {"prod": x * y, "div": x / y}


def get_sum(x, y):
    return x + y


def get_square(x):
    return x * x


def step_one(x, log):
    _record(log, "step_one")
    return x + 1


def poison(x, log, marker):
    """Fail exactly once, so a restart has to resume from the checkpoint."""

    _record(log, "poison")
    path = Path(marker)
    if not path.exists():
        path.write_text("tripped", encoding="utf-8")
        raise RuntimeError("the poison node fails on its first run")
    return x * 2


def step_three(x, log):
    _record(log, "step_three")
    return x + 100
'''

#: The arithmetic example of the Python Workflow Definition repository.
_ARITHMETIC: dict[str, object] = {
    "version": "0.1.0",
    "nodes": [
        {"id": 0, "type": "function", "value": "workflow.get_prod_and_div"},
        {"id": 1, "type": "function", "value": "workflow.get_sum"},
        {"id": 2, "type": "function", "value": "workflow.get_square"},
        {"id": 3, "type": "input", "value": 1, "name": "x"},
        {"id": 4, "type": "input", "value": 2, "name": "y"},
        {"id": 5, "type": "output", "name": "result"},
    ],
    "edges": [
        {"target": 0, "targetPort": "x", "source": 3, "sourcePort": None},
        {"target": 0, "targetPort": "y", "source": 4, "sourcePort": None},
        {"target": 1, "targetPort": "x", "source": 0, "sourcePort": "prod"},
        {"target": 1, "targetPort": "y", "source": 0, "sourcePort": "div"},
        {"target": 2, "targetPort": "x", "source": 1, "sourcePort": None},
        {"target": 5, "targetPort": None, "source": 2, "sourcePort": None},
    ],
}


@pytest.fixture()
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    yield Workspace.initialize(tmp_path / "workspace", extensions=["transactional-data-v1"])


@pytest.fixture()
def module(tmp_path: Path) -> Path:
    path = tmp_path / "workflow.py"
    path.write_text(_MODULE, encoding="utf-8")
    return path


def _document(tmp_path: Path, content: dict[str, object], name: str = "workflow.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


def _drive(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)


def _poison_document(log: Path, marker: Path) -> dict[str, object]:
    """A three-function chain whose middle node fails on its first run."""

    return {
        "version": "0.1.0",
        "nodes": [
            {"id": 0, "type": "function", "value": "workflow.step_one"},
            {"id": 1, "type": "function", "value": "workflow.poison"},
            {"id": 2, "type": "function", "value": "workflow.step_three"},
            {"id": 3, "type": "input", "value": 1, "name": "x"},
            {"id": 4, "type": "input", "value": str(log), "name": "log"},
            {"id": 5, "type": "input", "value": str(marker), "name": "marker"},
            {"id": 6, "type": "output", "name": "result"},
        ],
        "edges": [
            {"target": 0, "targetPort": "x", "source": 3, "sourcePort": None},
            {"target": 0, "targetPort": "log", "source": 4, "sourcePort": None},
            {"target": 1, "targetPort": "x", "source": 0, "sourcePort": None},
            {"target": 1, "targetPort": "log", "source": 4, "sourcePort": None},
            {"target": 1, "targetPort": "marker", "source": 5, "sourcePort": None},
            {"target": 2, "targetPort": "x", "source": 1, "sourcePort": None},
            {"target": 2, "targetPort": "log", "source": 4, "sourcePort": None},
            {"target": 6, "targetPort": None, "source": 2, "sourcePort": None},
        ],
    }


# ---------------------------------------------------------------------------
# The document itself
# ---------------------------------------------------------------------------


def test_a_document_is_validated_and_ordered_before_anything_is_submitted(tmp_path: Path) -> None:
    document = load_pwd_document(_document(tmp_path, _ARITHMETIC))

    assert document.version == "0.1.0"
    assert document.functions == ("workflow.get_prod_and_div", "workflow.get_sum", "workflow.get_square")
    assert document.input_names == ("x", "y")
    assert document.output_names == ("result",)
    # Inputs first, then every function in dependency order, then the output.
    assert document.order == (3, 4, 0, 1, 2, 5)


def test_a_document_that_is_not_a_workflow_is_refused_with_a_reason() -> None:
    with pytest.raises(PwdFormatError, match="must have a nodes array"):
        validate_pwd_document({"version": "0.1.0", "edges": []})
    with pytest.raises(PwdFormatError, match="version '9.9.9'"):
        validate_pwd_document({"version": "9.9.9", "nodes": [], "edges": []})
    with pytest.raises(PwdFormatError, match="not the id of a declared node"):
        validate_pwd_document(
            {
                "version": "0.1.0",
                "nodes": [{"id": 0, "type": "function", "value": "m.f"}],
                "edges": [{"target": 0, "targetPort": "x", "source": 7, "sourcePort": None}],
            }
        )
    with pytest.raises(PwdFormatError, match="has a cycle through nodes 0, 1"):
        validate_pwd_document(
            {
                "version": "0.1.0",
                "nodes": [
                    {"id": 0, "type": "function", "value": "m.f"},
                    {"id": 1, "type": "function", "value": "m.g"},
                ],
                "edges": [
                    {"target": 0, "targetPort": "x", "source": 1, "sourcePort": None},
                    {"target": 1, "targetPort": "x", "source": 0, "sourcePort": None},
                ],
            }
        )
    with pytest.raises(PwdFormatError, match="module.function"):
        validate_pwd_document(
            {"version": "0.1.0", "nodes": [{"id": 0, "type": "function", "value": "nodots"}], "edges": []}
        )
    # An unknown version is a refusal that names the way past it.
    assert (
        validate_pwd_document(
            {"version": "9.9.9", "nodes": [{"id": 0, "type": "input", "name": "x"}], "edges": []},
            allow_unknown_version=True,
        ).version
        == "9.9.9"
    )


def test_unknown_members_survive_the_import(tmp_path: Path, workspace: Workspace, module: Path) -> None:
    annotated = {**_ARITHMETIC, "engineAnnotations": {"who": "some other engine"}}
    job = import_pwd(workspace, _document(tmp_path, annotated), modules=[module], tag="annotated")

    definition = JobDefinition.from_path(job.payload / "job.json")
    embedded = definition.inputs["pwd_document"]
    assert isinstance(embedded, dict)
    assert embedded["engineAnnotations"] == {"who": "some other engine"}


# ---------------------------------------------------------------------------
# The imported job
# ---------------------------------------------------------------------------


def test_the_packaged_runner_describes_itself_as_one_step() -> None:
    """One entry point, whatever document a job carries.

    The step set of a runner is registered at import time and pinned per job, so
    the PWD runner deliberately has exactly one step and keeps the position in
    the graph in the job state instead.
    """

    described = describe_runner(runner_path(PACKAGE, "pwd_runner.py"))
    assert described == {"workflow": "pwd.workflow", "steps": ["execute"]}
    assert (PACKAGE, RUNNER) == ("httk.workflow.compat.pwd", "pwd_runner.py")


def test_an_imported_job_references_the_packaged_runner_and_writes_no_runner_file(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    job = import_pwd(workspace, _document(tmp_path, _ARITHMETIC), modules=[module], tag="arithmetic")

    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.runner_source == "installed"
    assert definition.runner_path.as_posix() == f"pkg:{PACKAGE}/pwd_runner.py"
    assert definition.runner_sha256 == runner_reference(PACKAGE, "pwd_runner.py")["sha256"]
    assert definition.initial_step == "execute"
    # Nothing but the staged module and the job definition: no generated runner.
    staged = sorted(path.relative_to(job.payload).as_posix() for path in job.payload.rglob("*") if path.is_file())
    assert staged == ["files/workflow.py", "job.json"]
    assert not list(workspace.runners.rglob("*")) if workspace.runners.is_dir() else True


def test_an_imported_document_runs_to_success_through_the_manager(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    job = import_pwd(
        workspace,
        _document(tmp_path, _ARITHMETIC),
        modules=[module],
        tag="arithmetic",
        data_mode="transactional",
    )

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8"))
    # ((1*2) + (1/2)) squared.
    assert outputs == {"result": 6.25}
    published = job.payload / "data" / "pwd" / "pwd-outputs.json"
    assert json.loads(published.read_text(encoding="utf-8")) == outputs
    state = json.loads((job.payload / ".httk-job" / "state.json").read_text(encoding="utf-8"))
    assert state["pwd_completed"] == [3, 4, 0, 1, 2, 5]


def test_workflow_inputs_override_the_input_nodes_of_the_document(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    job = import_pwd(
        workspace,
        _document(tmp_path, _ARITHMETIC),
        modules=[module],
        tag="overridden",
        workflow_inputs={"x": 3, "y": 4},
    )

    _drive(workspace)

    assert (marker := workspace.find_marker_by_id(job.job_id)) is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8"))
    # ((3*4) + (3/4)) squared.
    assert outputs == {"result": 12.75**2}


def test_a_failed_node_is_retried_from_the_checkpoint_rather_than_from_the_start(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    """The whole point of checkpointing: node one does not run twice."""

    log = tmp_path / "calls.json"
    marker_file = tmp_path / "poison.marker"
    job = import_pwd(
        workspace,
        _document(tmp_path, _poison_document(log, marker_file)),
        modules=[module],
        tag="poisoned",
    )

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8"))
    assert outputs == {"result": 104}
    # step_one is checkpointed by the first attempt and skipped by the second;
    # the node that failed is the only one that runs twice.
    assert json.loads(log.read_text(encoding="utf-8")) == ["step_one", "poison", "poison", "step_three"]


def test_a_node_outside_the_allowlist_is_refused_at_run_time(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    job = import_pwd(
        workspace,
        _document(tmp_path, _ARITHMETIC),
        modules=[module],
        tag="restricted",
        allowed_modules=["httk"],
        maximum_attempts=1,
    )

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "pwd.node_failed"
    assert "pwd_allowed_modules" in failure["message"]


def test_a_document_too_large_to_embed_is_staged_in_the_payload(
    tmp_path: Path, workspace: Workspace, module: Path
) -> None:
    job = import_pwd(
        workspace,
        _document(tmp_path, _ARITHMETIC),
        modules=[module],
        tag="staged",
        maximum_embedded_bytes=64,
    )

    definition = JobDefinition.from_path(job.payload / "job.json")
    assert "pwd_document" not in definition.inputs
    assert definition.inputs["pwd_document_path"] == DOCUMENT_FILE
    staged = job.payload / "files" / "pwd.json"
    assert json.loads(staged.read_text(encoding="utf-8"))["nodes"] == _ARITHMETIC["nodes"]
    assert DEFAULT_MAXIMUM_EMBEDDED_BYTES > 64

    # And the staged spelling runs exactly like the embedded one.
    _drive(workspace)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8")) == {"result": 6.25}


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_import_group_is_a_group_of_the_canonical_tree(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)

    assert command(["--help"], context) == 0
    assert "import" in capsys.readouterr().out
    # A group invoked with no action is somebody exploring, not somebody wrong.
    assert command(["import"], context) == 0
    printed = capsys.readouterr().out
    assert "usage: httk workflow import" in printed
    assert "pwd" in printed and "cwl" in printed
    assert command(["import", "--help"], context) == 0
    assert "cwl" in capsys.readouterr().out


def test_the_import_command_submits_a_job_and_reports_it(
    tmp_path: Path, workspace: Workspace, module: Path, capsys
) -> None:
    document = _document(tmp_path, _ARITHMETIC)
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)

    assert (
        command(
            [
                "import",
                "pwd",
                ws,
                str(document),
                "--module",
                str(module),
                "--tag",
                "cli",
                "--input",
                "x=5",
                "--json",
            ],
            context,
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["template"] == "pwd" and report["workflow"] == "pwd.workflow"
    assert report["runner"]["path"] == f"pkg:{PACKAGE}/pwd_runner.py"

    _drive(workspace)
    payload = Path(report["payload_path"])
    outputs = json.loads((payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8"))
    assert outputs == {"result": (5 * 2 + 5 / 2) ** 2}
