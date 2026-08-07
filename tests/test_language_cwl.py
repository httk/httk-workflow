import hashlib
import json
import logging
import re
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest
from httk.core import DataRecord, FileEntry, FileRecord
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, collect
from httk.workflow import collecting as collecting_module
from httk.workflow import scaffold as scaffold_module
from httk.workflow.collecting import job_records
from httk.workflow.introspection import list_jobs
from httk.workflow.languages import _CUSTOM_DEFINITIONS, _data_record, runner_path
from httk.workflow.languages.cwl import (
    DOCKER_CAPABILITY,
    PACKAGE,
    CwlImportError,
    UnsupportedCwlError,
    load_cwl_plan,
)
from httk.workflow.models import JobDefinition
from httk.workflow.packages import load_workflow_package
from httk.workflow.scaffold import describe_runner, new_job, new_jobs, resolve_workflow
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

_FILE_TOOL = """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: cat
inputs:
  input_file:
    type: File
    inputBinding: {position: 1}
outputs:
  spoken:
    type: stdout
stdout: spoken.txt
"""

_ANY_TOOL = _ECHO_TOOL.replace("type: string", "type: Any")

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

_SCATTER_FILE_OUTPUT_WORKFLOW = """
cwlVersion: v1.2
class: Workflow
requirements:
  ScatterFeatureRequirement: {}
inputs:
  messages: string[]
outputs:
  transcripts:
    type: File[]
    outputSource: say/spoken
steps:
  say:
    run: echo.cwl
    scatter: message
    in:
      message: messages
    out: [spoken]
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


def _drive(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)


@pytest.fixture()
def package(tmp_path: Path) -> Path:
    pytest.importorskip("cwl_utils")
    return _package(tmp_path / "package", "echo.cwl", _ECHO_TOOL, "message", "spoken")


@pytest.fixture()
def flows(tmp_path: Path) -> Iterator[Path]:
    pytest.importorskip("cwl_utils", reason="importing CWL needs the optional cwl extra")
    root = tmp_path / "flows"
    root.mkdir()
    (root / "echo.cwl").write_text(_ECHO_TOOL, encoding="utf-8")
    (root / "count.cwl").write_text(_COUNT_TOOL, encoding="utf-8")
    yield root


@pytest.fixture()
def workspace(tmp_path: Path) -> Iterator[Workspace]:
    yield Workspace.initialize(tmp_path / "workspace")


def _package(
    root: Path,
    document_name: str,
    document: str,
    input_name: str,
    output_name: str,
    *,
    input_port: str | None = None,
    input_entry_type: str = "strings",
    data_mode: str = "none",
    extra_documents: dict[str, str] | None = None,
    runner_extra: str = "",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    port_line = "" if input_port is None else f'port = "{input_port}"\n'
    (root / "httk_workflow.toml").write_text(
        f'''[workflow]
id = "tests.language.{root.name}"

[workflow.runner]
language = "cwl"
document = "{document_name}"
data_mode = "{data_mode}"
{runner_extra}

[workflow.inputs.{input_name}]
entry_type = "{input_entry_type}"
{port_line}
[workflow.outputs.{output_name}]
entry_type = "strings"
''',
        encoding="utf-8",
    )
    (root / document_name).write_text(document, encoding="utf-8")
    for name, text in (extra_documents or {}).items():
        (root / name).write_text(text, encoding="utf-8")
    return root


def _write(flows: Path, name: str, text: str) -> Path:
    path = flows / name
    path.write_text(text, encoding="utf-8")
    return path


def test_cwl_language_job_runs_to_success(package: Path, workspace: Workspace) -> None:
    assert resolve_workflow(package).language == "cwl"
    job = new_job(workspace, package, inputs={"message": "hello"})

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == "hello"


def test_bare_cwl_document_runs_and_collects_without_a_provider(package: Path, workspace: Workspace) -> None:
    document = package / "echo.cwl"
    job = new_job(workspace, document, inputs={"message": "hello"})
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    item = next(collect(workspace))
    assert isinstance(item.outputs["spoken"], FileRecord)
    output = item.outputs["spoken"]
    assert (workspace.root / output.url).read_text(encoding="utf-8").strip() == "hello"


def test_bare_cwl_document_can_be_forced(package: Path, workspace: Workspace) -> None:
    assert resolve_workflow(package / "echo.cwl", format="cwl").language == "cwl"
    with pytest.raises(ValueError, match="available languages"):
        resolve_workflow(package / "echo.cwl", format="unknown")


def test_cli_job_new_accepts_a_bare_cwl_document(package: Path, workspace: Workspace, tmp_path: Path, capsys) -> None:
    name = register_ws(CLIContext("httk", tmp_path), workspace.root, "bare-cwl")
    assert (
        command(
            ["job", "new", name, "--workflow", str(package / "echo.cwl"), "--input", "message=hello"],
            CLIContext("httk", tmp_path),
        )
        == 0
    )
    payload = Path(capsys.readouterr().out.split("\t", 1)[1].strip())
    assert JobDefinition.from_path(payload / "job.json").workflow == "cwl.echo"
    _drive(workspace)
    assert any(record.outputs.get("spoken") is not None for record in collect(workspace))


def test_cwl_language_collects_records_and_product_links(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _package(tmp_path / "collect", "echo.cwl", _ECHO_TOOL, "message", "spoken")
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest.replace(
            '[workflow.outputs.spoken]\nentry_type = "strings"',
            '[workflow.outputs.spoken]\nentry_type = "strings"\nproduct_of = "message"',
        ),
        encoding="utf-8",
    )
    new_job(workspace, package, inputs={"message": "hello"})
    provider = load_workflow_package(package)
    _drive(workspace)

    record = next(job_records(workspace))
    monkeypatch.setattr(
        collecting_module,
        "job_records",
        lambda *_args, **_kwargs: iter(
            (
                replace(
                    record,
                    declarations={
                        **record.declarations,
                        "provenance": {
                            "declared": {"inputs": {"message": {"type": "strings", "id": "source-1"}}},
                            "observed": None,
                        },
                    },
                ),
            )
        ),
    )
    item = next(collect(workspace))
    scaffold_module._WORKFLOW_PROVIDERS.pop(provider.workflow_id, None)
    assert set(item.outputs) == {"spoken"}
    assert isinstance(item.outputs["spoken"], FileRecord)
    assert item.outputs["spoken"].type and item.outputs["spoken"].id
    assert item.unfulfilled == ()
    assert {edge.label for edge in item.run.artifacts} >= {"spoken"}
    assert {edge.label for edge in item.run.outputs} >= {"spoken"}
    assert len(item.products) == 1
    assert item.products[0].label == "spoken"


def test_cwl_file_output_collects_a_workspace_relative_descriptor(tmp_path: Path, workspace: Workspace) -> None:
    source = tmp_path / "input.txt"
    source.write_text("hello\n", encoding="utf-8")
    package = _package(tmp_path / "collect-file", "file.cwl", _FILE_TOOL, "input_file", "spoken")
    job = new_job(workspace, package, inputs={"input_file": {"class": "File", "path": str(source)}})
    _drive(workspace)

    item = next(collect(workspace))
    value = item.outputs["spoken"]
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    old_path = Path(str(outputs["spoken"]["path"])).resolve().relative_to(workspace.root).as_posix()
    assert isinstance(value, FileRecord)
    assert value.type == "files"
    assert re.fullmatch(r"[0-9a-f]{64}", value.id)
    assert value.url == old_path
    assert not PurePosixPath(value.url).is_absolute()
    assert value.name == "spoken.txt"
    expected = hashlib.sha256((workspace.root / value.url).read_bytes()).hexdigest()
    assert value.sha256 == expected
    assert value.size == len("hello\n")
    assert value.media_type is None
    assert job.payload.exists()


def test_cwl_file_output_rejects_outside_and_missing_paths(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "confined", "echo.cwl", _ECHO_TOOL, "message", "spoken")
    job = new_job(workspace, package, inputs={"message": "hello"})
    _drive(workspace)
    output_path = job.payload / "run" / "cwl-outputs.json"
    outputs = json.loads(output_path.read_text(encoding="utf-8"))

    outputs["spoken"]["path"] = "/etc/passwd"
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    outside = next(collect(workspace))
    assert outside.outputs == {} and outside.missing_collector is not None
    assert "outside" in outside.missing_collector

    outputs["spoken"]["path"] = "does-not-exist.txt"
    output_path.write_text(json.dumps(outputs), encoding="utf-8")
    missing = next(collect(workspace))
    assert missing.outputs == {} and missing.missing_collector is not None
    assert "missing" in missing.missing_collector


def test_cwl_bare_jobs_collect_with_or_without_allow_job_collector(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "allow", "echo.cwl", _ECHO_TOOL, "message", "spoken")
    new_job(workspace, package, inputs={"message": "hello"})
    _drive(workspace)

    without = next(collect(workspace))
    with_flag = next(collect(workspace, allow_job_collector=True))
    assert without.missing_collector is None
    assert with_flag.missing_collector is None
    without_output = without.outputs["spoken"]
    with_output = with_flag.outputs["spoken"]
    assert isinstance(without_output, FileRecord) and isinstance(with_output, FileRecord)
    assert without_output == with_output


def test_cwl_custom_package_collect_marker_degrades_without_provider(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(
        tmp_path / "custom-hook",
        "echo.cwl",
        _ECHO_TOOL,
        "message",
        "spoken",
        runner_extra='''

[workflow.collect]
file = "collect.py"
''',
    )
    (package / "collect.py").write_text(
        "def collect(record):\n    return {}\n",
        encoding="utf-8",
    )
    job = new_job(workspace, package, inputs={"message": "hello"})
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.parameters["workflow_collect"] == "package"
    assert definition.parameters["workflow_realization"] == "language"
    _drive(workspace)

    item = next(collect(workspace))
    assert item.outputs == {}
    assert item.missing_collector is not None
    assert "package collect hook" in item.missing_collector


def test_generated_definitions_follow_a_role_when_its_kind_changes() -> None:
    role = "test_shape_variance"
    _CUSTOM_DEFINITIONS.clear()
    forward = (_data_record(role, "text"), _data_record(role, [1, 2]))
    _CUSTOM_DEFINITIONS.clear()
    reverse = (_data_record(role, [1, 2]), _data_record(role, "text"))
    assert forward[0].value == "text" and forward[1].value == [1, 2]
    assert (forward[0].name, forward[1].name) == (reverse[1].name, reverse[0].name)
    assert (role, "list of float") in _CUSTOM_DEFINITIONS

    hyphenated = _data_record("a-b", "text")
    underscored = _data_record("a_b", "text")
    assert hyphenated.definition_id != underscored.definition_id


def test_cwl_collect_into_round_trips_a_file_record(tmp_path: Path, workspace: Workspace, capsys) -> None:
    pytest.importorskip("httk.data")
    pytest.importorskip("httk.atomistic")
    from httk.data.db import Database, SqlStore

    package = _package(tmp_path / "collect-into", "echo.cwl", _ECHO_TOOL, "message", "spoken")
    new_job(workspace, package, inputs={"message": "hello"})
    _drive(workspace)

    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "language-collect-into")
    store_path = tmp_path / "results.sqlite"
    assert command(["collect", workspace_name, "--into", str(store_path)], context) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["outputs"]["spoken"]["type"] == "files"
    assert report["stored"]["entries"]
    assert report["stored"]["run"]
    with Database.sqlite(store_path) as database:
        store = SqlStore(database)
        entry = store.fetch_entry(FileEntry, report["stored"]["entries"][0])
    assert isinstance(entry, FileRecord)
    assert entry.id == report["stored"]["entries"][0]


def test_cwl_file_list_output_stays_a_data_record_descriptor_list(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(
        tmp_path / "collect-file-list",
        "flow.cwl",
        _SCATTER_FILE_OUTPUT_WORKFLOW,
        "messages",
        "transcripts",
        input_entry_type="strings",
        data_mode="transactional",
        extra_documents={"echo.cwl": _ECHO_TOOL},
    )
    new_job(workspace, package, inputs={"messages": ["one", "two"]})
    _drive(workspace)

    value = next(collect(workspace)).outputs["transcripts"]
    assert isinstance(value, DataRecord)
    assert isinstance(value.value, list) and len(value.value) == 2
    assert all(set(descriptor) == {"kind", "path", "basename", "sha256", "size"} for descriptor in value.value)
    assert all(descriptor["kind"] == "File" for descriptor in value.value)
    assert all(not PurePosixPath(str(descriptor["path"])).is_absolute() for descriptor in value.value)


def test_cwl_language_reserved_parameter_and_file_collisions_fail(
    package: Path, tmp_path: Path, workspace: Workspace
) -> None:
    with pytest.raises(ValueError, match="cwl_document"):
        new_job(workspace, package, parameters={"cwl_document": "other.json"})

    user_file = tmp_path / "workflow.json"
    user_file.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="workflow.cwl.json"):
        new_job(workspace, package, files={"workflow.cwl.json": user_file})
    with pytest.raises(ValueError, match="cwl_inputs"):
        new_job(workspace, package, parameters={"cwl_inputs": "other.json"})


def test_cwl_language_rejects_literal_file_path_traversal(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "literal", "file.cwl", _FILE_TOOL, "input_file", "spoken")
    with pytest.raises(ValueError, match=r"\.\./\.\./"):
        new_job(
            workspace,
            package,
            inputs={
                "input_file": {
                    "class": "File",
                    "contents": "unsafe",
                    "basename": "../../../../job.json",
                }
            },
        )
    assert not (workspace.control / "tmp" / "job.json").exists()


def test_cwl_language_rejects_generated_member_collision(tmp_path: Path, workspace: Workspace) -> None:
    source = tmp_path / "letters.txt"
    source.write_text("one\n", encoding="utf-8")
    user_file = tmp_path / "existing.txt"
    user_file.write_text("user\n", encoding="utf-8")
    package = _package(
        tmp_path / "generated-collision",
        "count.cwl",
        _COUNT_TOOL,
        "source",
        "joined",
        input_port="files",
        input_entry_type="files",
    )
    with pytest.raises(ValueError, match=r"files/inputs/files\[0\]/letters.txt"):
        new_job(
            workspace,
            package,
            inputs={"source": [{"class": "File", "path": str(source)}]},
            files={"files/inputs/files[0]/letters.txt": user_file},
        )


def test_cwl_language_rejects_duplicate_generated_members(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "duplicate", "any.cwl", _ANY_TOOL, "value", "spoken", input_port="message")
    with pytest.raises(ValueError, match=r"duplicates.*message\.a\.b"):
        new_job(
            workspace,
            package,
            inputs={
                "value": {
                    "a": {"b": {"class": "File", "contents": "first", "basename": "same.txt"}},
                    "a.b": {"class": "File", "contents": "second", "basename": "same.txt"},
                }
            },
        )
    assert not list((workspace.control / "tmp").iterdir())


def test_cwl_language_scatter_runs_labeled_children_and_publishes_data(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(
        tmp_path / "scatter",
        "flow.cwl",
        _SCATTER_WORKFLOW,
        "messages",
        "transcript",
        input_entry_type="strings",
        data_mode="transactional",
        extra_documents={"echo.cwl": _ECHO_TOOL, "count.cwl": _COUNT_TOOL},
    )
    job = new_job(workspace, package, inputs={"messages": ["alpha", "beta", "gamma"]}, tag="scatter")

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    rows = {row["job_key"]: row for row in list_jobs(workspace)}
    shards = sorted(key for key in rows if key.startswith("s000"))
    assert [key.split("--")[0] for key in shards] == ["s0000", "s0001", "s0002"]
    assert {rows[key]["state"] for key in shards} == {"succeeded"}
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    transcript = Path(str(outputs["transcript"]["path"]))
    assert transcript.read_text(encoding="utf-8").split() == ["alpha", "beta", "gamma"]
    published = sorted((job.payload / "data" / "cwl" / "transcript").iterdir())
    assert [path.name for path in published] == ["0000-joined.txt"]


def test_cwl_language_subworkflow_carries_its_target(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(
        tmp_path / "nested",
        "nested.cwl",
        _SUBWORKFLOW,
        "message",
        "transcript",
        extra_documents={"echo.cwl": _ECHO_TOOL},
    )
    job = new_job(workspace, package, inputs={"message": "nested"}, tag="nested")

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    child = next(row for row in list_jobs(workspace) if row["job_key"].startswith("sub--"))
    assert child["state"] == "succeeded"
    child_definition = JobDefinition.from_path(
        workspace.payload_path(PurePosixPath(str(child["placement"])), str(child["job_key"])) / "job.json"
    )
    assert child_definition.parameters["cwl_target"] == ["inner"]
    assert child_definition.parameters["cwl_document"] == "files/workflow.cwl.json"


def test_cwl_language_command_line_tool_is_a_workflow_of_one(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "solo", "echo.cwl", _ECHO_TOOL, "message", "spoken")
    job = new_job(workspace, package, inputs={"message": "solo"}, tag="solo")

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == "solo"
    assert outputs["spoken"]["checksum"].startswith("sha1$")


def test_cwl_language_stages_file_inputs_by_effective_port(tmp_path: Path, workspace: Workspace) -> None:
    source = tmp_path / "letters.txt"
    source.write_text("one\ntwo\n", encoding="utf-8")
    package = _package(
        tmp_path / "files",
        "count.cwl",
        _COUNT_TOOL,
        "source",
        "joined",
        input_port="files",
        input_entry_type="files",
    )
    job = new_job(
        workspace,
        package,
        inputs={"source": [{"class": "File", "path": str(source)}]},
        tag="staged",
    )
    staged = job.payload / "files" / "inputs" / "files[0]" / "letters.txt"
    assert staged.read_text(encoding="utf-8") == "one\ntwo\n"

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["joined"]["path"])).read_text(encoding="utf-8") == "one\ntwo\n"


def test_cwl_language_v1_document_is_upgraded_and_runs(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "old", "old.cwl", _ECHO_TOOL.replace("v1.2", "v1.0"), "message", "spoken")
    job = new_job(workspace, package, inputs={"message": "vintage"}, tag="old")

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
    assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == "vintage"


def test_cwl_language_tool_failure_is_terminal_with_stderr(tmp_path: Path, workspace: Workspace) -> None:
    document = _ECHO_TOOL.replace("baseCommand: echo", "baseCommand: [cat]").replace(
        "stdout: spoken.txt", "stdout: out.txt"
    )
    package = _package(tmp_path / "broken", "broken.cwl", document, "message", "spoken")
    job = new_job(workspace, package, inputs={"message": "no-such-file"}, tag="broken")

    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "cwl.tool_failed"
    assert "no-such-file" in failure["message"]
    assert failure.get("retryable", False) is False
    assert workspace.read_state(marker)["attempt_ordinal"] == 1


def test_cwl_language_docker_requirement_is_recorded_and_warned(
    tmp_path: Path, workspace: Workspace, caplog: pytest.LogCaptureFixture
) -> None:
    document = _ECHO_TOOL.replace(
        "baseCommand: echo",
        "requirements:\n  DockerRequirement:\n    dockerPull: debian:stable\nbaseCommand: echo",
    )
    package = _package(tmp_path / "docker", "docker.cwl", document, "message", "spoken")
    caplog.set_level(logging.WARNING, logger="httk.workflow.languages.cwl")
    job = new_job(workspace, package, inputs={"message": "contained"}, tag="docker")

    definition = JobDefinition.from_path(job.payload / "job.json")
    assert json.loads((job.payload / "job.json").read_text(encoding="utf-8"))["claim"]["required_capabilities"] == [
        DOCKER_CAPABILITY
    ]
    assert definition.required_capabilities == frozenset({DOCKER_CAPABILITY})
    messages = [record.getMessage() for record in caplog.records]
    assert any("DockerRequirement is recorded as the required capability 'docker'" in item for item in messages)
    assert any("never pulls or enters an image" in item for item in messages)


def test_cwl_language_campaign_prepares_once_and_runs_each_job(
    package: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.languages import cwl as cwl_language

    calls = 0
    original = cwl_language.LANGUAGE.prepare

    def prepare(request):
        nonlocal calls
        calls += 1
        return original(request)

    monkeypatch.setattr(cwl_language, "LANGUAGE", replace(cwl_language.LANGUAGE, prepare=prepare))
    jobs = list(
        new_jobs(
            workspace,
            package,
            [
                {"inputs": {"message": "one"}, "tag": "one"},
                {"inputs": {"message": "two"}, "tag": "two"},
                {"inputs": {"message": "three"}, "tag": "three"},
            ],
        )
    )
    assert [job.tag for job in jobs] == ["one", "two", "three"]
    assert calls == 1

    _drive(workspace)

    for job, expected in zip(jobs, ("one", "two", "three")):
        marker = workspace.find_marker_by_id(job.job_id)
        assert marker is not None and marker.kind == "succeeded"
        outputs = json.loads((job.payload / "run" / "cwl-outputs.json").read_text(encoding="utf-8"))
        assert Path(str(outputs["spoken"]["path"])).read_text(encoding="utf-8").strip() == expected


def test_cwl_language_payload_shape_and_runner(package: Path, workspace: Workspace) -> None:
    job = new_job(workspace, package, inputs={"message": "shape"})
    definition = JobDefinition.from_path(job.payload / "job.json")
    staged = sorted(path.relative_to(job.payload).as_posix() for path in job.payload.rglob("*") if path.is_file())
    assert staged == ["files/inputs.json", "files/workflow.cwl.json", "job.json"]
    assert definition.runner_source == "installed"
    assert definition.runner_path.as_posix() == f"pkg:{PACKAGE}/cwl_runner.py"


def test_the_packaged_runner_describes_its_dispatch_vocabulary() -> None:
    described = describe_runner(runner_path(PACKAGE, "cwl_runner.py"))
    assert described == {"workflow": "cwl.workflow", "steps": ["advance", "collect", "enter", "start"]}


def test_importing_without_the_extra_says_exactly_what_to_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.languages import cwl as importer

    monkeypatch.setattr(importer, "find_spec", lambda name: None)
    document = tmp_path / "flow.cwl"
    document.write_text(_ECHO_TOOL, encoding="utf-8")

    with pytest.raises(CwlImportError, match=r"pip install httk-workflow\[cwl\]"):
        load_cwl_plan(document)


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


def test_cwl_crossproduct_scatter_is_refused_by_name(flows: Path) -> None:
    document = _SCATTER_WORKFLOW.replace(
        "    scatter: message\n",
        "    scatter: [message]\n    scatterMethod: flat_crossproduct\n",
    )
    with pytest.raises(UnsupportedCwlError, match="scatterMethod flat_crossproduct"):
        load_cwl_plan(_write(flows, "cross.cwl", document))


def test_cwl_expression_tool_is_refused_by_class(flows: Path) -> None:
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
