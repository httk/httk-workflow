"""Executable instantiate hooks and their v1 boundary contract."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from httk.core.register import register_format_serializer, register_writer

from httk.workflow import TaskManager, Workspace
from httk.workflow.models import JobDefinition
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli._describe import _workflow_description


class _TinyValue:
    def __init__(self, value: str) -> None:
        self.value = value


class _NoWriter:
    def __eq__(self, other: object) -> bool:
        return other == {"raise": True}


def _tiny_serializer(value: object) -> dict[str, object]:
    if not isinstance(value, _TinyValue):
        raise TypeError("not a tiny value")
    return {"value": value.value}


def _tiny_writer(destination: Path, value: dict[str, object]) -> None:
    destination.write_text(str(value["value"]), encoding="utf-8")


def _tiny_writer_second(destination: Path, value: dict[str, object]) -> None:
    destination.write_text(str(value["value"]), encoding="utf-8")


def _tuple_serializer(value: object) -> dict[str, object]:
    if not isinstance(value, tuple):
        raise TypeError("not a tuple")
    return {"value": "tuple"}


def _dict_serializer(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not any(isinstance(key, int) for key in value):
        raise TypeError("not an int-keyed dict")
    return {"value": "dict"}


class _BasenameValue:
    def __eq__(self, other: object) -> bool:
        return other == {"raise": True}


class _TupleInput(tuple[object, ...]):
    def __eq__(self, other: object) -> bool:
        return other == {"raise": True}


def _basename_serializer(value: object) -> dict[str, object]:
    if not isinstance(value, _BasenameValue):
        raise TypeError("not a basename value")
    return {"value": "basename"}


register_writer(
    name="instantiate-exec-test-first",
    writer=_tiny_writer,
    format="instantiate-exec-test-first",
    extensions=(".aainst",),
)
register_writer(
    name="instantiate-exec-test-second",
    writer=_tiny_writer_second,
    format="instantiate-exec-test-second",
    extensions=(".zzinst",),
)
register_format_serializer(format="instantiate-exec-test-first", serializer=_tiny_serializer)
register_format_serializer(format="instantiate-exec-test-second", serializer=_tiny_serializer)
register_writer(
    name="instantiate-exec-test-tuple",
    writer=_tiny_writer,
    format="instantiate-exec-test-tuple",
    extensions=(".tupleinst",),
)
register_format_serializer(format="instantiate-exec-test-tuple", serializer=_tuple_serializer)
register_writer(
    name="instantiate-exec-test-dict",
    writer=_tiny_writer,
    format="instantiate-exec-test-dict",
    extensions=(".dictinst",),
)
register_format_serializer(format="instantiate-exec-test-dict", serializer=_dict_serializer)
register_writer(
    name="instantiate-exec-test-basename",
    writer=_tiny_writer,
    format="instantiate-exec-test-basename",
    filenames=("SPECIAL",),
)
register_format_serializer(format="instantiate-exec-test-basename", serializer=_basename_serializer)


_RUNNER = """#!/usr/bin/env python3
from httk.workflow import Runner
run = Runner({workflow!r})
@run.step
def start(attempt):
    assert (attempt.payload / 'derived.txt').is_file()
    attempt.succeed()
if __name__ == '__main__':
    raise SystemExit(run.main())
"""


def _package(root: Path, hook: str, *, workflow: str = "tests.instantiate.exec", member: str = "hook") -> Path:
    root.mkdir()
    (root / "httk_workflow.toml").write_text(
        f"""[workflow]
id = {workflow!r}

[workflow.runner]
steps = ["start"]

[workflow.instantiate]
file = "{member}"

[workflow.inputs.file]
role = "file"

[workflow.inputs.value]
role = "value"

[workflow.inputs.object]
role = "object"
""",
        encoding="utf-8",
    )
    (root / "run").write_text(_RUNNER.format(workflow=workflow), encoding="utf-8")
    (root / "run").chmod(0o755)
    hook_path = root / member
    hook_path.write_text(hook, encoding="utf-8")
    hook_path.chmod(0o755)
    return root


def _hook_source(body: str) -> str:
    source = Path(__file__).parents[1] / "src"
    return f"#!/usr/bin/env python3\nimport sys; sys.path.insert(0, {str(source)!r})\n{body}\n"


_EXEC_HOOK = _hook_source(
    """from pathlib import Path
from httk.workflow.hookapi import instantiate_main
def instantiate(request):
    assert request.inputs['file']['kind'] == 'file'
    assert request.inputs['value'] == {'kind': 'value', 'value': 'literal'}
    text = Path(request.inputs['file']['path']).read_text(encoding='utf-8')
    Path('derived.txt').write_text(text + '-derived', encoding='utf-8')
    return {'parameters': {'derived': text, 'hook': True}, 'tag': 'suggested'}
instantiate_main(instantiate)
"""
)


def test_executable_instantiate_stages_inputs_and_runs(tmp_path: Path) -> None:
    package = _package(tmp_path / "package", _EXEC_HOOK)
    source = tmp_path / "input.txt"
    source.write_text("source", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")

    job = new_job(workspace, package, inputs={"file": source, "value": "literal"}, parameters={"base": 1})

    assert JobDefinition.from_path(job.payload / "job.json").parameters == {
        "base": 1,
        "derived": "source",
        "hook": True,
    }
    assert (job.payload / "files/inputs/file/input.txt").read_text(encoding="utf-8") == "source"
    assert (job.payload / "derived.txt").read_text(encoding="utf-8") == "source-derived"
    assert job.tag == "suggested"
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_executable_input_serialization_uses_value_and_registered_writer(tmp_path: Path) -> None:
    hook = _hook_source(
        """from pathlib import Path
from httk.workflow.hookapi import instantiate_main
def instantiate(request):
    assert request.inputs['value'] == {'kind': 'value', 'value': {'nested': [{'ok': True}], 'list': [1, 2]}}
    assert request.inputs['object']['kind'] == 'file'
    assert Path(request.inputs['object']['path']).read_text(encoding='utf-8') == 'written'
    return {'parameters': {}}
instantiate_main(instantiate)
"""
    )
    package = _package(tmp_path / "package", hook)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(
        workspace,
        package,
        inputs={"value": {"nested": [{"ok": True}], "list": [1, 2]}, "object": _TinyValue("written")},
    )
    assert (job.payload / "files/inputs/object/object.aainst").is_file()
    assert not (job.payload / "files/inputs/object/object.zzinst").exists()


def test_non_native_json_shapes_and_basename_writers_are_serialized(tmp_path: Path) -> None:
    hook = _hook_source(
        """from httk.workflow.hookapi import instantiate_main
def instantiate(request):
    return {'parameters': {}}
instantiate_main(instantiate)
"""
    )
    tuple_job = new_job(
        Workspace.initialize(tmp_path / "tuple-workspace"),
        _package(tmp_path / "tuple-package", hook),
        inputs={"object": _TupleInput((1, 2))},
    )
    dict_job = new_job(
        Workspace.initialize(tmp_path / "dict-workspace"),
        _package(tmp_path / "dict-package", hook),
        inputs={"object": {1: "one"}},
    )
    basename_job = new_job(
        Workspace.initialize(tmp_path / "basename-workspace"),
        _package(tmp_path / "basename-package", hook),
        inputs={"object": _BasenameValue()},
    )
    assert (tuple_job.payload / "files/inputs/object/object.tupleinst").is_file()
    assert (dict_job.payload / "files/inputs/object/object.dictinst").is_file()
    assert (basename_job.payload / "files/inputs/object/special").is_file()


def test_executable_input_without_writer_cleans_staging(tmp_path: Path) -> None:
    package = _package(tmp_path / "package", _EXEC_HOOK)
    source = tmp_path / "input.txt"
    source.write_text("source", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match=r"input 'object'.*serializ.*(\.py hook|register)"):
        new_job(workspace, package, inputs={"file": source, "value": "literal", "object": _NoWriter()})
    assert list((workspace.control / "tmp").iterdir()) == []
    assert not list(workspace.scan_markers())


def test_executable_input_path_typo_is_refused(tmp_path: Path) -> None:
    package = _package(tmp_path / "package", _EXEC_HOOK)
    workspace = Workspace.initialize(tmp_path / "workspace")
    # A separator-bearing value that resolves to no file is a mistyped path, so
    # the submission is refused before the hook ever runs.
    with pytest.raises(ValueError, match="input 'file' looks like a file path but nothing exists at no/such/path.txt"):
        new_job(workspace, package, inputs={"file": "no/such/path.txt", "value": "literal"})
    assert list((workspace.control / "tmp").iterdir()) == []


@pytest.mark.parametrize("output", ["exit", "malformed"])
def test_executable_instantiate_failure_cleans_staging(tmp_path: Path, output: str) -> None:
    body = "import sys; print('hook stderr', file=sys.stderr); sys.exit(1)" if output == "exit" else "print('{')"
    hook = _hook_source(
        f"""from httk.workflow.hookapi import instantiate_main
import sys
{body}
"""
    )
    package = _package(tmp_path / "package", hook)
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="executable instantiate hook"):
        new_job(workspace, package, inputs={"value": "value"})
    assert list((workspace.control / "tmp").iterdir()) == []
    assert not list(workspace.scan_markers())


def test_executable_and_python_hooks_have_equivalent_payloads(tmp_path: Path) -> None:
    python_hook = _hook_source(
        """from pathlib import Path
from shutil import copyfile
def instantiate(ctx):
    source = Path(ctx.inputs['file'])
    destination = ctx.payload / 'files/inputs/file' / source.name
    destination.parent.mkdir(parents=True)
    copyfile(source, destination)
    (ctx.payload / 'derived.txt').write_text('same', encoding='utf-8')
    ctx.parameters['same'] = True
    ctx.suggest_tag('same-tag')
"""
    )
    executable_hook = _hook_source(
        """from pathlib import Path
from httk.workflow.hookapi import instantiate_main
def instantiate(request):
    source = Path(request.inputs['file']['path'])
    Path('derived.txt').write_text('same', encoding='utf-8')
    return {'parameters': {'same': True}, 'tag': 'same-tag'}
instantiate_main(instantiate)
"""
    )
    source = tmp_path / "source.txt"
    source.write_text("same-source", encoding="utf-8")
    first = Workspace.initialize(tmp_path / "workspace-py")
    second = Workspace.initialize(tmp_path / "workspace-exec")
    py_job = new_job(
        first,
        _package(tmp_path / "python", python_hook, workflow="tests.conformance", member="hook.py"),
        inputs={"file": source},
    )
    exec_job = new_job(
        second,
        _package(tmp_path / "exec", executable_hook, workflow="tests.conformance"),
        inputs={"file": source},
    )
    py_definition = JobDefinition.from_path(py_job.payload / "job.json")
    exec_definition = JobDefinition.from_path(exec_job.payload / "job.json")
    assert py_definition.parameters == exec_definition.parameters
    assert py_definition.workflow == exec_definition.workflow == "tests.conformance"
    assert py_definition.tag == exec_definition.tag == "same-tag"
    assert py_job.tag == exec_job.tag
    assert _payload_file_hashes(py_job.payload) == _payload_file_hashes(exec_job.payload)


def _payload_file_hashes(payload: Path) -> dict[str, str]:
    """Hash staged payload files, excluding only root ``job.json``.

    The exclusion list is exactly ``job.json``: its job UUID and runner-store
    path/digest legitimately differ between the two independently published
    package trees. Every other payload member must have identical bytes.
    """

    result: dict[str, str] = {}
    for path in payload.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(payload)
        if relative.as_posix() == "job.json":
            continue
        result[relative.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def test_executable_manifest_and_description_report_kind(tmp_path: Path) -> None:
    package = _package(tmp_path / "package", _EXEC_HOOK)
    provider = parse_workflow_manifest(package)
    assert provider.instantiate_exec == "hook"
    description = _workflow_description(str(package))
    hooks = cast(Mapping[str, object], description["hooks"])
    assert hooks["instantiate"] == {
        "present": True,
        "file": "hook",
        "kind": "executable",
        "packaged": False,
    }
