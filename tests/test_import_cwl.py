"""Importing CWL documents, and running the subset natively on httk.

Every document here is parsed by the real *cwl-utils* and then executed by the
packaged ``cwl_runner.py`` under a real :class:`httk.workflow.TaskManager`.
Nothing calls cwltool, and the tools are the ones every POSIX machine has:
``echo`` and ``cat``.

*cwl-utils* is the optional ``cwl`` extra, so every test that parses a document
is skipped without it. The module itself imports without it — nothing in
*httk-workflow* imports a CWL library at module level — which is what lets the
tests that matter most in that situation still run: that the packaged runner
describes itself, and that a missing extra is reported as an instruction rather
than as a traceback.
"""

import json
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

import pytest
from httk.core import CLIContext

from httk.workflow import TaskManager, WorkflowWorkspace
from httk.workflow.integrations import PACKAGE, runner_path
from httk.workflow.integrations.cwl import (
    DOCKER_CAPABILITY,
    CwlImportError,
    UnsupportedCwlError,
    import_cwl,
    load_cwl_plan,
)
from httk.workflow.introspection import list_jobs
from httk.workflow.models import JobDefinition
from httk.workflow.scaffold import describe_runner
from httk.workflow.workflow_cli import command

_ECHO_TOOL = """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
inputs:
  message:
    type: string
    inputBinding: {position: 1}
outputs:
  spoken:
    type: stdout
stdout: spoken.txt
"""

_COUNT_TOOL = """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: [cat]
inputs:
  files:
    type: File[]
    inputBinding: {position: 1}
outputs:
  joined:
    type: File
    outputBinding: {glob: "joined.txt"}
stdout: joined.txt
"""

#: One scatter, one gather: three shards speak, and one step joins what they said.
_SCATTER_WORKFLOW = """
cwlVersion: v1.2
class: Workflow
requirements:
  ScatterFeatureRequirement: {}
inputs:
  messages: string[]
outputs:
  transcript:
    type: File
    outputSource: join/joined
steps:
  say:
    run: echo.cwl
    scatter: message
    in:
      message: messages
    out: [spoken]
  join:
    run: count.cwl
    in:
      files: say/spoken
    out: [joined]
"""

_SUBWORKFLOW = """
cwlVersion: v1.2
class: Workflow
requirements:
  SubworkflowFeatureRequirement: {}
inputs:
  message: string
outputs:
  transcript:
    type: File
    outputSource: inner/transcript
steps:
  inner:
    in:
      message: message
    out: [transcript]
    run:
      class: Workflow
      inputs:
        message: string
      outputs:
        transcript:
          type: File
          outputSource: say/spoken
      steps:
        say:
          run: echo.cwl
          in:
            message: message
          out: [spoken]
"""


@pytest.fixture()
def workspace(tmp_path: Path) -> Iterator[WorkflowWorkspace]:
    yield WorkflowWorkspace.initialize(tmp_path / "workspace", extensions=["transactional-data-v1"])


@pytest.fixture()
def flows(tmp_path: Path) -> Path:
    """The directory every CWL document of these tests is written into.

    Asking for this fixture is what says a test parses CWL, and therefore what
    skips it in an environment without the optional extra.
    """

    pytest.importorskip("cwl_utils", reason="importing CWL needs the optional cwl extra")
    root = tmp_path / "flows"
    root.mkdir()
    (root / "echo.cwl").write_text(_ECHO_TOOL, encoding="utf-8")
    (root / "count.cwl").write_text(_COUNT_TOOL, encoding="utf-8")
    return root


def _write(flows: Path, name: str, text: str) -> Path:
    path = flows / name
    path.write_text(text, encoding="utf-8")
    return path


def _inputs(flows: Path, values: dict[str, object], name: str = "job.yml") -> Path:
    path = flows / name
    lines = []
    for key, value in values.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            lines.extend(f"  - {item}" for item in value)
        else:
            lines.append(f"{key}: {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _drive(workspace: WorkflowWorkspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)


# ---------------------------------------------------------------------------
# The runner and the plan
# ---------------------------------------------------------------------------


def test_the_packaged_runner_describes_its_dispatch_vocabulary() -> None:
    """The runner needs no CWL library, so it describes itself without the extra."""

    described = describe_runner(runner_path("cwl_runner.py"))
    assert described == {"workflow": "cwl.workflow", "steps": ["advance", "collect", "enter", "start"]}


def test_importing_without_the_extra_says_exactly_what_to_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.integrations import cwl as importer

    monkeypatch.setattr(importer, "find_spec", lambda name: None)
    document = tmp_path / "flow.cwl"
    document.write_text(_ECHO_TOOL, encoding="utf-8")

    with pytest.raises(CwlImportError, match=r"pip install httk-workflow\[cwl\]"):
        load_cwl_plan(document)


def test_a_workflow_is_normalized_into_one_self_contained_plan(flows: Path) -> None:
    plan, context = load_cwl_plan(_write(flows, "flow.cwl", _SCATTER_WORKFLOW))

    assert plan["class"] == "Workflow" and plan["cwlVersion"] == "v1.2"
    steps = plan["steps"]
    assert isinstance(steps, dict) and sorted(steps) == ["join", "say"]
    # The referenced tool is inlined, so the plan needs nothing beside it.
    say = steps["say"]
    assert say["run"]["class"] == "CommandLineTool" and say["run"]["baseCommand"] == ["echo"]
    assert say["scatter"] == ["message"] and say["in"]["message"]["source"] == ["messages"]
    assert say["run"]["inputs"]["message"]["type"] == {"type": "string", "optional": False}
    outputs = plan["outputs"]
    assert isinstance(outputs, dict) and outputs["transcript"]["outputSource"] == ["join/joined"]
    assert context.warnings == [] and context.capabilities == set()


# ---------------------------------------------------------------------------
# Running the subset
# ---------------------------------------------------------------------------


def test_a_scattered_workflow_runs_to_success_with_labeled_children(flows: Path, workspace: WorkflowWorkspace) -> None:
    workflow = _write(flows, "flow.cwl", _SCATTER_WORKFLOW)
    inputs = _inputs(flows, {"messages": ["alpha", "beta", "gamma"]})

    imported = import_cwl(workspace, workflow, inputs, tag="scatter", data_mode="transactional")
    _drive(workspace)

    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "succeeded"

    # One labeled child per shard, and every one of them finished.
    rows = {row["job_key"]: row for row in list_jobs(workspace)}
    shards = sorted(key for key in rows if key.startswith("s000"))
    assert [key.split("--")[0] for key in shards] == ["s0000", "s0001", "s0002"]
    assert {rows[key]["state"] for key in shards} == {"succeeded"}

    outputs = json.loads((imported.job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    transcript = Path(str(outputs["transcript"]["path"]))
    assert transcript.read_text(encoding="utf-8").split() == ["alpha", "beta", "gamma"]
    # And the same file is published as transactional data of the job.
    published = sorted((imported.job.payload / "data" / "cwl" / "transcript").iterdir())
    assert [path.name for path in published] == ["0000-joined.txt"]


def test_a_subworkflow_runs_as_one_child_carrying_its_position_in_the_plan(
    flows: Path, workspace: WorkflowWorkspace
) -> None:
    workflow = _write(flows, "nested.cwl", _SUBWORKFLOW)
    inputs = _inputs(flows, {"message": "nested"})

    imported = import_cwl(workspace, workflow, inputs, tag="nested")
    _drive(workspace)

    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((imported.job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["transcript"]["path"])).read_text(encoding="utf-8").strip() == "nested"
    # The subworkflow child carries no document of its own: it points into the
    # plan of the job that spawned it.
    child = next(row for row in list_jobs(workspace) if row["job_key"].startswith("sub--"))
    assert child["state"] == "succeeded"
    definition = JobDefinition.from_path(
        workspace.payload_path(PurePosixPath(str(child["placement"])), str(child["job_key"])) / "job.json"
    )
    assert definition.inputs["cwl_target"] == ["inner"]
    assert definition.inputs["cwl_document"] == "files/workflow.cwl.json"


def test_a_single_command_line_tool_is_a_workflow_of_one(flows: Path, workspace: WorkflowWorkspace) -> None:
    inputs = _inputs(flows, {"message": "solo"})

    imported = import_cwl(workspace, flows / "echo.cwl", inputs, tag="solo")
    _drive(workspace)

    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((imported.job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == "solo"
    assert outputs["spoken"]["checksum"].startswith("sha1$")


def test_a_staged_file_input_travels_with_the_job(flows: Path, workspace: WorkflowWorkspace, tmp_path: Path) -> None:
    source = tmp_path / "letters.txt"
    source.write_text("one\ntwo\n", encoding="utf-8")
    document = _write(
        flows,
        "count.job.yml",
        f"files:\n  - {{class: File, path: {source}}}\n",
    )

    imported = import_cwl(workspace, flows / "count.cwl", document, tag="staged")
    staged = imported.job.payload / "files" / "inputs" / "files[0]" / "letters.txt"
    assert staged.read_text(encoding="utf-8") == "one\ntwo\n"

    _drive(workspace)
    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((imported.job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["joined"]["path"])).read_text(encoding="utf-8") == "one\ntwo\n"


def test_a_v1_0_document_is_read_and_normalized_to_one_shape(flows: Path, workspace: WorkflowWorkspace) -> None:
    """An older document runs without the operator having to convert it first."""

    document = _write(flows, "old.cwl", _ECHO_TOOL.replace("cwlVersion: v1.2", "cwlVersion: v1.0"))
    plan, _ = load_cwl_plan(document)
    assert plan["class"] == "CommandLineTool" and plan["baseCommand"] == ["echo"]

    imported = import_cwl(workspace, document, _inputs(flows, {"message": "vintage"}), tag="old")
    _drive(workspace)

    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((imported.job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == "vintage"


def test_a_tool_that_exits_nonzero_fails_the_job_with_what_it_said(flows: Path, workspace: WorkflowWorkspace) -> None:
    document = _write(
        flows,
        "broken.cwl",
        _ECHO_TOOL.replace("baseCommand: echo", "baseCommand: [cat]").replace("stdout: spoken.txt", "stdout: out.txt"),
    )

    imported = import_cwl(workspace, document, _inputs(flows, {"message": "no-such-file"}), tag="broken")
    _drive(workspace)

    marker = workspace.find_marker_by_id(imported.job.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "cwl.tool_failed"
    assert "no-such-file" in failure["message"]
    # Failing once is enough: the same tool says the same thing on every attempt,
    # so nothing declared the failure retryable and the job really is terminal.
    assert failure.get("retryable", False) is False
    assert workspace.read_state(marker)["attempt_ordinal"] == 1


# ---------------------------------------------------------------------------
# The job an import produces
# ---------------------------------------------------------------------------


def test_an_imported_job_references_the_packaged_runner_and_stages_the_plan(
    flows: Path, workspace: WorkflowWorkspace
) -> None:
    imported = import_cwl(
        workspace,
        _write(flows, "flow.cwl", _SCATTER_WORKFLOW),
        _inputs(flows, {"messages": ["only"]}),
        tag="referenced",
    )

    definition = JobDefinition.from_path(imported.job.payload / "job.json")
    assert definition.runner_source == "installed"
    assert definition.runner_path.as_posix() == f"pkg:{PACKAGE}/cwl_runner.py"
    assert definition.inputs == {"cwl_document": "files/workflow.cwl.json", "cwl_inputs": "files/inputs.json"}
    staged = sorted(
        path.relative_to(imported.job.payload).as_posix() for path in imported.job.payload.rglob("*") if path.is_file()
    )
    assert staged == ["files/inputs.json", "files/workflow.cwl.json", "job.json"]


def test_a_docker_requirement_becomes_a_capability_and_a_warning(flows: Path, workspace: WorkflowWorkspace) -> None:
    document = _write(
        flows,
        "docker.cwl",
        _ECHO_TOOL.replace(
            "baseCommand: echo",
            "requirements:\n  DockerRequirement:\n    dockerPull: debian:stable\nbaseCommand: echo",
        ),
    )

    imported = import_cwl(workspace, document, _inputs(flows, {"message": "contained"}), tag="docker")

    definition = JobDefinition.from_path(imported.job.payload / "job.json")
    assert definition.required_capabilities == frozenset({DOCKER_CAPABILITY})
    assert any(
        "DockerRequirement is recorded as the required capability 'docker'" in item for item in imported.warnings
    )
    assert any("never pulls or enters an image" in item for item in imported.warnings)


# ---------------------------------------------------------------------------
# The subset, as refusals
# ---------------------------------------------------------------------------

#: Every feature outside the subset, the document that uses it, and the words the
#: refusal has to contain. This table *is* the subset table of the documentation.
_REFUSALS: tuple[tuple[str, str, str], ...] = (
    (
        "javascript",
        _ECHO_TOOL.replace(
            "inputBinding: {position: 1}",
            'inputBinding: {position: 1, valueFrom: "${return 1;}"}',
        ),
        "JavaScript expression",
    ),
    (
        "inline javascript requirement",
        _ECHO_TOOL.replace("baseCommand: echo", "requirements:\n  InlineJavascriptRequirement: {}\nbaseCommand: echo"),
        "InlineJavascriptRequirement",
    ),
    (
        "computed expression",
        _ECHO_TOOL.replace("stdout: spoken.txt", 'stdout: $(inputs.message.split("/")[0])'),
        "expression at",
    ),
    (
        "shell command",
        _ECHO_TOOL.replace("baseCommand: echo", "requirements:\n  ShellCommandRequirement: {}\nbaseCommand: echo"),
        "ShellCommandRequirement",
    ),
    (
        "initial work dir",
        _ECHO_TOOL.replace(
            "baseCommand: echo",
            "requirements:\n  InitialWorkDirRequirement:\n    listing: []\nbaseCommand: echo",
        ),
        "InitialWorkDirRequirement",
    ),
    (
        "streamable",
        _ECHO_TOOL.replace("type: stdout", "type: stdout\n    streamable: true"),
        "streamable",
    ),
    (
        "output eval",
        _COUNT_TOOL.replace('{glob: "joined.txt"}', '{glob: "joined.txt", outputEval: "$(self[0])"}'),
        "outputEval",
    ),
    (
        "stdin",
        _COUNT_TOOL.replace("stdout: joined.txt", "stdout: joined.txt\nstdin: something.txt"),
        "stdin redirection",
    ),
    (
        "record type",
        _ECHO_TOOL.replace(
            "    type: string\n",
            "    type:\n      type: record\n      fields: []\n",
        ),
        "schema at",
    ),
)


@pytest.mark.parametrize(("feature", "document", "expected"), _REFUSALS, ids=[item[0] for item in _REFUSALS])
def test_a_feature_outside_the_subset_is_refused_by_name(
    feature: str, document: str, expected: str, flows: Path
) -> None:
    path = _write(flows, "refused.cwl", document)

    with pytest.raises((UnsupportedCwlError, CwlImportError)) as refusal:
        load_cwl_plan(path)
    message = str(refusal.value)
    assert expected in message, message
    assert "refused.cwl" in message or "unsupported CWL feature" in message


def test_a_crossproduct_scatter_is_refused_by_name(flows: Path) -> None:
    document = _SCATTER_WORKFLOW.replace(
        "    scatter: message\n",
        "    scatter: [message]\n    scatterMethod: flat_crossproduct\n",
    )
    with pytest.raises(UnsupportedCwlError, match="scatterMethod flat_crossproduct"):
        load_cwl_plan(_write(flows, "cross.cwl", document))


def test_an_expression_tool_is_refused_by_class(flows: Path) -> None:
    document = """
cwlVersion: v1.2
class: ExpressionTool
requirements:
  InlineJavascriptRequirement: {}
inputs:
  message: string
outputs:
  out: string
expression: "${return {out: inputs.message};}"
"""
    with pytest.raises((UnsupportedCwlError, CwlImportError), match="ExpressionTool"):
        load_cwl_plan(_write(flows, "expression.cwl", document))


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_import_command_submits_a_cwl_job(
    flows: Path, workspace: WorkflowWorkspace, tmp_path: Path, capsys
) -> None:
    workflow = _write(flows, "flow.cwl", _SCATTER_WORKFLOW)
    inputs = _inputs(flows, {"messages": ["one", "two"]})

    assert (
        command(
            ["import", "cwl", str(workspace.root), str(workflow), str(inputs), "--tag", "cli", "--json"],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["template"] == "cwl" and report["workflow"] == "cwl.workflow"

    _drive(workspace)
    payload = Path(report["payload_path"])
    outputs = json.loads((payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["transcript"]["path"])).read_text(encoding="utf-8").split() == ["one", "two"]
