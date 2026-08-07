"""Directory-package realization and execution for the PWD language."""

import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from httk.core import DataRecord

from httk.workflow import TaskManager, Workspace, collect
from httk.workflow import scaffold as scaffold_module
from httk.workflow.languages import runner_path, runner_reference
from httk.workflow.languages.pwd import (
    DEFAULT_MAXIMUM_EMBEDDED_BYTES,
    DOCUMENT_FILE,
    PACKAGE,
    PwdFormatError,
    load_pwd_document,
    validate_pwd_document,
)
from httk.workflow.models import JobDefinition
from httk.workflow.packages import load_workflow_package
from httk.workflow.scaffold import describe_runner, new_job, new_jobs, resolve_workflow

_MODULE = '''"""Functions for the packaged test workflows."""

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
    yield Workspace.initialize(tmp_path / "workspace")


def _drive(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)


def _poison_document(log: Path, marker: Path) -> dict[str, object]:
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


def _package(root: Path, document: dict[str, object], *, runner_extra: str = "") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "httk_workflow.toml").write_text(
        f'''[workflow]
id = "tests.language.{root.name}"

[workflow.runner]
language = "pwd"
document = "workflow.json"
modules = ["workflow.py"]
{runner_extra}

[workflow.inputs.x]
entry_type = "strings"

[workflow.outputs.result]
entry_type = "strings"
''',
        encoding="utf-8",
    )
    (root / "workflow.json").write_text(json.dumps(document), encoding="utf-8")
    (root / "workflow.py").write_text(_MODULE, encoding="utf-8")
    return root


def _document(tmp_path: Path, content: dict[str, object]) -> Path:
    path = tmp_path / "workflow.json"
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


def test_a_document_is_validated_and_ordered_before_anything_is_submitted(tmp_path: Path) -> None:
    document = load_pwd_document(_document(tmp_path, _ARITHMETIC))

    assert document.version == "0.1.0"
    assert document.functions == ("workflow.get_prod_and_div", "workflow.get_sum", "workflow.get_square")
    assert document.input_names == ("x", "y")
    assert document.output_names == ("result",)
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
    assert (
        validate_pwd_document(
            {"version": "9.9.9", "nodes": [{"id": 0, "type": "input", "name": "x"}], "edges": []},
            allow_unknown_version=True,
        ).version
        == "9.9.9"
    )


def test_pwd_language_job_runs_to_success_and_carries_embedded_document(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "package", _ARITHMETIC)
    assert resolve_workflow(package).language == "pwd"
    job = new_job(workspace, package)

    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.parameters["workflow_language"] == "pwd"
    assert definition.parameters["workflow_realization"] == "language"
    assert definition.parameters["pwd_document"] == _ARITHMETIC
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8")) == {"result": 6.25}


def test_bare_pwd_document_runs_with_a_module_root_parameter(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "bare", _ARITHMETIC)
    job = new_job(workspace, package / "workflow.json", parameters={"pwd_module_path": [str(package)]})
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    item = next(collect(workspace))
    assert isinstance(item.outputs["result"], DataRecord)
    assert item.outputs["result"].value == 6.25


def test_pwd_language_preserves_unknown_document_members(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "annotated", {**_ARITHMETIC, "engineAnnotations": {"who": "other"}})
    job = new_job(workspace, package)

    definition = JobDefinition.from_path(job.payload / "job.json")
    embedded = definition.parameters["pwd_document"]
    assert isinstance(embedded, dict)
    assert embedded["engineAnnotations"] == {"who": "other"}


def test_pwd_language_collects_scalar_records_and_degrades_when_unregistered(
    tmp_path: Path, workspace: Workspace
) -> None:
    package = _package(tmp_path / "collect", _ARITHMETIC)
    job = new_job(workspace, package)
    _drive(workspace)

    item = next(collect(workspace))
    assert isinstance(item.outputs["result"], DataRecord)
    assert item.outputs["result"].value == 6.25
    assert item.unfulfilled == ()

    scaffold_module._WORKFLOW_PROVIDERS.pop(resolve_workflow(package).workflow_id, None)
    (job.payload / "run" / "pwd-outputs.json").unlink()
    degraded = next(collect(workspace))
    assert degraded.outputs == {}
    assert degraded.missing_collector is not None
    assert "pwd-outputs.json" in degraded.missing_collector


def test_pwd_manifest_collect_override_wins(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _package(
        tmp_path / "override-hook",
        _ARITHMETIC,
        runner_extra='''

[workflow.collect]
file = "collect.py"
''',
    )
    (package / "collect.py").write_text(
        '''from httk.core import DataRecord


def collect(record):
    return {"result": DataRecord.from_value("https://example.test/custom", "custom", 9)}
''',
        encoding="utf-8",
    )
    new_job(workspace, package)
    _drive(workspace)
    provider = load_workflow_package(package)

    item = next(collect(workspace, allow_job_collector=True))
    scaffold_module._WORKFLOW_PROVIDERS.pop(provider.workflow_id, None)
    result = item.outputs["result"]
    assert isinstance(result, DataRecord)
    assert result.value == 9


def test_pwd_language_input_override_matches_import_behavior(tmp_path: Path, workspace: Workspace) -> None:
    job = new_job(workspace, _package(tmp_path / "override", _ARITHMETIC), inputs={"x": 3})
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8")) == {"result": 56.25}


def test_pwd_language_resumes_from_a_checkpoint(tmp_path: Path, workspace: Workspace) -> None:
    log = tmp_path / "calls.json"
    package = _package(tmp_path / "poison", _poison_document(log, tmp_path / "poison.marker"))
    job = new_job(workspace, package)
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8")) == {"result": 104}
    assert json.loads(log.read_text(encoding="utf-8")) == ["step_one", "poison", "poison", "step_three"]


def test_pwd_language_allowlist_failure_names_refused_module(tmp_path: Path, workspace: Workspace) -> None:
    package = _package(tmp_path / "restricted", _ARITHMETIC, runner_extra='allowed_modules = ["some.other"]')
    job = new_job(workspace, package, parameters={"pwd_retry_failed_nodes": False})
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "pwd.node_failed"
    assert "workflow" in failure["message"]


def test_pwd_language_stages_an_oversized_document(tmp_path: Path, workspace: Workspace) -> None:
    document = {**_ARITHMETIC, "padding": "x" * DEFAULT_MAXIMUM_EMBEDDED_BYTES}
    package = _package(tmp_path / "staged", document)
    job = new_job(workspace, package)

    definition = JobDefinition.from_path(job.payload / "job.json")
    assert "pwd_document" not in definition.parameters
    assert definition.parameters["pwd_document_path"] == DOCUMENT_FILE
    assert (
        json.loads((job.payload / "files" / "pwd.json").read_text(encoding="utf-8"))["padding"] == document["padding"]
    )
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_pwd_language_payload_stages_module_and_job_definition(tmp_path: Path, workspace: Workspace) -> None:
    job = new_job(workspace, _package(tmp_path / "shape", _ARITHMETIC))
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.runner_source == "installed"
    assert definition.runner_path.as_posix() == f"pkg:{PACKAGE}/pwd_runner.py"
    assert definition.runner_sha256 == runner_reference(PACKAGE, "pwd_runner.py")["sha256"]
    staged = sorted(path.relative_to(job.payload).as_posix() for path in job.payload.rglob("*") if path.is_file())
    assert staged == ["files/workflow.py", "job.json"]


def test_pwd_language_snapshots_non_utf8_module_bytes(tmp_path: Path, workspace: Workspace) -> None:
    document: dict[str, object] = {
        "version": "0.1.0",
        "nodes": [
            {"id": 0, "type": "function", "value": "workflow.echo"},
            {"id": 1, "type": "input", "value": 1, "name": "x"},
            {"id": 2, "type": "output", "name": "result"},
        ],
        "edges": [
            {"target": 0, "targetPort": "x", "source": 1, "sourcePort": None},
            {"target": 2, "targetPort": None, "source": 0, "sourcePort": None},
        ],
    }
    package = _package(tmp_path / "latin1", document)
    source = b'# -*- coding: latin-1 -*-\n\ndef echo(x):\n    return "caf\xe9"\n'
    (package / "workflow.py").write_bytes(source)

    job = new_job(workspace, package)
    assert (job.payload / "files" / "workflow.py").read_bytes() == source
    _drive(workspace)

    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_pwd_language_campaign_prepares_once_and_snapshots_modules(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.languages import pwd as pwd_language

    package = _package(tmp_path / "campaign", _ARITHMETIC)
    calls = 0
    original_prepare = pwd_language.LANGUAGE.prepare

    def prepare(request):
        nonlocal calls
        calls += 1
        return original_prepare(request)

    monkeypatch.setattr(pwd_language, "LANGUAGE", replace(pwd_language.LANGUAGE, prepare=prepare))
    campaign = new_jobs(
        workspace,
        package,
        [
            {"inputs": {"x": 1}, "tag": "one"},
            {"inputs": {"x": 2}, "tag": "two"},
            {"inputs": {"x": 3}, "tag": "three"},
        ],
    )
    jobs = [next(campaign)]
    module = package / "workflow.py"
    module.write_text(
        module.read_text(encoding="utf-8").replace(
            'return {"prod": x * y, "div": x / y}', 'return {"prod": 999, "div": 999}'
        ),
        encoding="utf-8",
    )
    jobs.extend(campaign)
    assert [job.tag for job in jobs] == ["one", "two", "three"]
    assert calls == 1
    _drive(workspace)

    results = []
    for job in jobs:
        marker = workspace.find_marker_by_id(job.job_id)
        assert marker is not None and marker.kind == "succeeded"
        results.append(json.loads((job.payload / "run" / "pwd-outputs.json").read_text(encoding="utf-8"))["result"])
    assert results == [6.25, 25.0, 56.25]


def test_pwd_language_rejects_non_json_input_at_submit(tmp_path: Path, workspace: Workspace) -> None:
    with pytest.raises(ValueError, match="x"):
        new_job(workspace, _package(tmp_path / "non-json", _ARITHMETIC), inputs={"x": object()})


def test_pwd_language_reserved_parameter_collision_fails(tmp_path: Path, workspace: Workspace) -> None:
    with pytest.raises(ValueError, match="pwd_document"):
        new_job(workspace, _package(tmp_path / "collision", _ARITHMETIC), parameters={"pwd_document": "other.json"})
    with pytest.raises(ValueError, match="pwd_inputs"):
        new_job(workspace, _package(tmp_path / "input-collision", _ARITHMETIC), parameters={"pwd_inputs": {}})


def test_the_packaged_pwd_runner_describes_one_step() -> None:
    described = describe_runner(runner_path(PACKAGE, "pwd_runner.py"))
    assert described == {"workflow": "pwd.workflow", "steps": ["execute"]}
