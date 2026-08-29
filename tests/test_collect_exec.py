"""Executable collector stream and manifest checks."""

from __future__ import annotations

import json
import subprocess
import time
from builtins import __import__ as real_import
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, replace
from pathlib import Path, PurePosixPath
from types import ModuleType, SimpleNamespace
from typing import cast

import pytest
from httk.core import DataRecord, FileRecord
from httk.core.storage import content_id

from httk.workflow import Workspace
from httk.workflow import collecting as collecting_module
from httk.workflow import scaffold as scaffold_module
from httk.workflow.collecting import (
    MAX_COLLECT_RESPONSE_LINE_BYTES,
    MAX_COLLECT_STDERR_LINE_BYTES,
    JobRecord,
    _resolve_executable_output,
    _run_executable_collector,
)
from httk.workflow.languages import _data_record
from httk.workflow.languages.cwl import _file_record
from httk.workflow.packages import parse_workflow_manifest
from httk.workflow.scaffold import WorkflowProvider
from httk.workflow.workflow_cli._describe import _workflow_description
from test_collect import campaign as _collect_campaign


@pytest.fixture(name="campaign")
def _campaign_fixture(tmp_path_factory: pytest.TempPathFactory) -> tuple[Workspace, dict[str, str]]:
    factory = cast(
        Callable[[pytest.TempPathFactory], tuple[Workspace, dict[str, str]]],
        vars(_collect_campaign)["__wrapped__"],
    )
    return factory(tmp_path_factory)


def _record(root: Path, job_id: str = "job-1") -> JobRecord:
    return JobRecord(
        workspace_root=root,
        workspace_id="workspace",
        job_id=job_id,
        job_key=f"{job_id}--{job_id}",
        job={},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=PurePosixPath("."),
        payload_path=PurePosixPath("."),
        workdir_path=PurePosixPath("."),
        data_path=None,
        data_generation=None,
        provenance={},
        runner_steps=None,
        children={},
        declarations={},
    )


def _hook(root: Path, body: str) -> Path:
    path = root / "collect-hook"
    path.write_text(f"#!/usr/bin/env python3\n{body}\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def test_collect_main_emits_v1_jsonl(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "src"
    hook = _hook(
        tmp_path,
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
        "from httk.workflow.hookapi import collect_main\n"
        "collect_main(lambda record: {'answer': {'value': record['job_id']}})",
    )
    completed = subprocess.run(
        [str(hook)],
        input='{"format":"httk-workflow-collect-stream","format_version":2}\n{"record":{"job_id":"job-1"}}\n',
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0
    assert json.loads(completed.stdout.splitlines()[0]) == {
        "job_id": "job-1",
        "outputs": {"answer": {"value": "job-1"}},
    }


def test_executable_outputs_resolve_core_records(tmp_path: Path) -> None:
    (tmp_path / "result.txt").write_text("result", encoding="utf-8")
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' not in request: continue\n"
        "    job_id = request['record']['job_id']\n"
        "    print(json.dumps({'job_id': job_id, 'outputs': {\n"
        "        'value': {'value': 3},\n"
        "        'file': {'file': 'result.txt'},\n"
        "        'entry': {'entry': {'type': 'files', 'url': 'result.txt', 'name': 'result.txt'}}\n"
        "    }}), flush=True)",
    )
    provider = SimpleNamespace(
        collector_exec=hook.name,
        directory=tmp_path,
        outputs={"value": {"role": "value"}, "file": {"role": "file"}, "entry": {"role": "entry"}},
    )
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert failures == {}
    assert isinstance(resolved[0]["value"], DataRecord)
    assert isinstance(resolved[0]["file"], FileRecord)
    assert isinstance(resolved[0]["entry"], FileRecord)


def test_non_utf8_response_degrades_only_that_job(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "seen = 0\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' not in request: continue\n"
        "    if seen == 0: sys.stdout.buffer.write(b'\\xff\\n'); sys.stdout.buffer.flush()\n"
        "    else: print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 2}}}), flush=True)\n"
        "    seen += 1",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector(
        [_record(tmp_path, "job-1"), _record(tmp_path, "job-2")], provider, tmp_path
    )
    assert 0 in failures and 1 in resolved


def test_oversized_response_line_degrades_that_job(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        "        sys.stdout.buffer.write(b'{' + b'x' * (8 * 1024 * 1024) + b'\\n'); sys.stdout.buffer.flush()\n",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert resolved == {}
    assert "exceeded" in failures[0]


def test_response_line_boundary_excludes_newline(tmp_path: Path) -> None:
    exact = _hook(
        tmp_path,
        f"import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        "        response = {'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': ''}}}\n"
        f"        response['outputs']['answer']['value'] = 'x' * ({MAX_COLLECT_RESPONSE_LINE_BYTES} - len(json.dumps(response, separators=(',', ':'))))\n"
        "        encoded = json.dumps(response, separators=(',', ':')).encode(); assert len(encoded) == "
        f"{MAX_COLLECT_RESPONSE_LINE_BYTES}\n"
        "        sys.stdout.buffer.write(encoded + b'\\n'); sys.stdout.buffer.flush()\n",
    )
    provider = SimpleNamespace(collector_exec=exact.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert failures == {} and 0 in resolved

    oversized = _hook(
        tmp_path,
        f"import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        "        response = {'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': ''}}}\n"
        f"        response['outputs']['answer']['value'] = 'x' * ({MAX_COLLECT_RESPONSE_LINE_BYTES} - len(json.dumps(response, separators=(',', ':'))) + 1)\n"
        "        encoded = json.dumps(response, separators=(',', ':')).encode(); assert len(encoded) == "
        f"{MAX_COLLECT_RESPONSE_LINE_BYTES + 1}\n"
        "        sys.stdout.buffer.write(encoded + b'\\n'); sys.stdout.buffer.flush()\n",
    )
    provider.collector_exec = oversized.name
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert resolved == {} and "exceeded" in failures[0]


def test_oversized_stderr_degrades_the_group(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        "        sys.stderr.buffer.write((b'x' * 65535 + b'\\n') * 17); sys.stderr.buffer.flush()\n"
        "        print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 1}}}), flush=True)\n",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector(
        [_record(tmp_path, "job-1"), _record(tmp_path, "job-2")], provider, tmp_path
    )
    assert resolved == {} and set(failures) == {0, 1}
    assert "stderr exceeded" in failures[0]


def test_stderr_line_boundary_excludes_newline(tmp_path: Path) -> None:
    exact = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        f"        sys.stderr.buffer.write(b'x' * {MAX_COLLECT_STDERR_LINE_BYTES} + b'\\n'); sys.stderr.buffer.flush()\n"
        "        print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 1}}}), flush=True)\n",
    )
    provider = SimpleNamespace(collector_exec=exact.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert failures == {} and 0 in resolved

    oversized = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        f"        sys.stderr.buffer.write(b'x' * {MAX_COLLECT_STDERR_LINE_BYTES + 1} + b'\\n'); sys.stderr.buffer.flush()\n"
        "        print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 1}}}), flush=True)\n",
    )
    provider.collector_exec = oversized.name
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert resolved == {} and "stderr line exceeded" in failures[0]


@pytest.mark.timing
def test_descendant_holding_pipes_does_not_hang_collection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(collecting_module, "DEFAULT_COLLECT_TIMEOUT", 0.2)
    hook = _hook(
        tmp_path,
        "import subprocess, sys\nsubprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    started = time.monotonic()
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert time.monotonic() - started < 2
    assert resolved == {} and "timed out" in failures[0]


def test_unlaunchable_executable_degrades_the_group(tmp_path: Path) -> None:
    hook = tmp_path / "unlaunchable"
    hook.write_text("#!/definitely/missing/interpreter\n", encoding="utf-8")
    hook.chmod(0o755)
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector(
        [_record(tmp_path, "job-1"), _record(tmp_path, "job-2")], provider, tmp_path
    )
    assert resolved == {} and set(failures) == {0, 1}


def test_non_executable_registered_collector_degrades_the_group(tmp_path: Path) -> None:
    hook = _hook(tmp_path, "raise SystemExit(0)")
    hook.chmod(0o644)
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector(
        [_record(tmp_path, "job-1"), _record(tmp_path, "job-2")], provider, tmp_path
    )
    assert resolved == {} and set(failures) == {0, 1}
    assert "not executable" in failures[0]


def test_unicode_line_separator_inside_json_survives(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request: print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 'left\\u2028right'}}}, ensure_ascii=False), flush=True)",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert failures == {}
    answer = resolved[0]["answer"]
    assert isinstance(answer, DataRecord)
    assert answer.value == "left\u2028right"


def test_surplus_response_degrades_the_group(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' in request:\n"
        "        response = {'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 1}}}\n"
        "        print(json.dumps(response), flush=True); print(json.dumps(response), flush=True)",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert resolved == {}
    assert "surplus" in failures[0]


def test_collect_main_serialization_error_does_not_drop_the_tail(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "src"
    hook = _hook(
        tmp_path,
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
        "from httk.workflow.hookapi import collect_main\n"
        "collect_main(lambda record: {'answer': {'value': set()} if record['job_id'] == 'job-1' else {'value': 2}})",
    )
    completed = subprocess.run(
        [str(hook)],
        input='{"format":"httk-workflow-collect-stream","format_version":2}\n'
        '{"record":{"job_id":"job-1"}}\n{"record":{"job_id":"job-2"}}\n',
        text=True,
        capture_output=True,
        check=False,
    )
    lines = [json.loads(line) for line in completed.stdout.splitlines()]
    assert lines[0]["job_id"] == "job-1" and "error" in lines[0]
    assert lines[1] == {"job_id": "job-2", "outputs": {"answer": {"value": 2}}}


def test_cwl_empty_basename_is_preserved(tmp_path: Path) -> None:
    (tmp_path / "result.txt").write_text("result", encoding="utf-8")
    result = _file_record(_record(tmp_path), {"path": "result.txt", "basename": ""}, "prefix", "port", 0)
    assert result.name == ""


def test_in_process_and_executable_collectors_have_identical_results(
    campaign: tuple[Workspace, dict[str, str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, _ = campaign
    source_record = next(iter(collecting_module.job_records(workspace)))
    package = tmp_path / "collector-package"
    package.mkdir()
    source = Path(__file__).parents[1] / "src"
    hook = _hook(
        package,
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
        "from httk.workflow.hookapi import collect_main\n"
        "collect_main(lambda record: {'first': {'value': 'first-' + record['job_id']}, "
        "'second': {'value': 'second-' + record['job_id']}})",
    )
    workflow_id = "tests.collect.exec-conformance"
    workflow_uri = "https://example.test/workflows/exec-conformance"
    document = {
        "$id": workflow_uri,
        "inputs": [{"name": "source", "entry_type": "_httk_records"}],
        "outputs": [
            {"name": "first", "entry_type": "_httk_records"},
            {"name": "second", "entry_type": "_httk_records"},
        ],
    }
    provenance = {
        "workflow_declaration_uri": workflow_uri,
        "inputs": {"source": {"type": "_httk_records", "id": "input-id"}},
        "artifacts": {"artifact": {"type": "_httk_records", "id": "artifact-id"}},
        "outputs": {"first": {"type": "_httk_records", "id": "old-output-id"}},
    }
    record = replace(
        source_record,
        job={**source_record.job, "workflow": workflow_id},
        declarations={
            "workflow": {"declared": document, "observed": None},
            "provenance": {"declared": provenance, "observed": None},
        },
    )

    def in_process(record: JobRecord) -> Mapping[str, object]:
        return {
            "first": _data_record("first", f"first-{record.job_id}"),
            "second": _data_record("second", f"second-{record.job_id}"),
        }

    def prepared_records(
        _workspace: Workspace,
        *,
        states: Iterable[str],
        placement: str | PurePosixPath | None,
        on_skipped: Callable[[str], None] | None = None,
    ) -> Iterator[JobRecord]:
        del states, placement, on_skipped
        yield record

    provider = WorkflowProvider(
        workflow_id=workflow_id,
        declarations={"workflow": document},
        directory=package,
        outputs={
            "first": {"entry_type": "_httk_records", "role": "first", "product_of": "source"},
            "second": {"entry_type": "_httk_records", "role": "second", "product_of": "first"},
        },
        collector=in_process,
    )
    monkeypatch.setitem(scaffold_module._WORKFLOW_PROVIDERS, provider.workflow_id, provider)
    monkeypatch.setattr(collecting_module, "job_records", prepared_records)
    in_process_results = list(collecting_module.collect(workspace))

    executable_provider = provider.__class__(
        workflow_id=provider.workflow_id,
        declarations=provider.declarations,
        directory=provider.directory,
        outputs=provider.outputs,
        collector_exec=hook.name,
    )
    monkeypatch.setitem(scaffold_module._WORKFLOW_PROVIDERS, provider.workflow_id, executable_provider)
    executable_results = list(collecting_module.collect(workspace))

    assert len(in_process_results) == len(executable_results) == 1
    expected, actual = in_process_results[0], executable_results[0]
    assert expected.unfulfilled == actual.unfulfilled == ()
    assert set(expected.outputs) == set(actual.outputs) == {"first", "second"}
    for role in expected.outputs:
        expected_value = expected.outputs[role]
        actual_value = actual.outputs[role]
        assert type(expected_value) is type(actual_value)
        assert isinstance(expected_value, DataRecord) and isinstance(actual_value, DataRecord)
        assert (expected_value.type, expected_value.id, expected_value.value) == (
            actual_value.type,
            actual_value.id,
            actual_value.value,
        )
        assert asdict(expected_value) == asdict(actual_value)
    assert expected.run.workflow_declaration_uri == actual.run.workflow_declaration_uri == workflow_uri
    assert expected.run.source_id == actual.run.source_id
    assert expected.run.immutable_id == actual.run.immutable_id
    assert expected.run.immutable_id is None
    assert expected.run.id == actual.run.id
    assert asdict(expected.run) == asdict(actual.run)
    for side in ("inputs", "artifacts", "outputs"):
        expected_edges = tuple((edge.label, edge.entry_type, edge.entry_id) for edge in getattr(expected.run, side))
        actual_edges = tuple((edge.label, edge.entry_type, edge.entry_id) for edge in getattr(actual.run, side))
        assert expected_edges == actual_edges
        for edge in getattr(expected.run, side):
            if edge.label in expected.outputs:
                assert edge.entry_id == content_id(expected.outputs[edge.label])
    assert expected.products == actual.products
    assert len(expected.products) == 2
    assert expected.products[0].label == "first"
    assert expected.products[1].label == "second"


@pytest.mark.parametrize("mode", ["malformed", "wrong-job", "error", "unknown-entry", "bad-wrapper"])
def test_executable_response_failures_degrade_the_job(tmp_path: Path, mode: str) -> None:
    hook = _hook(
        tmp_path,
        f"import json, sys\nmode = {mode!r}\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' not in request: continue\n"
        "    job_id = request['record']['job_id']\n"
        "    if mode == 'malformed': print('{{', flush=True); continue\n"
        "    if mode == 'wrong-job': job_id = 'other'\n"
        "    if mode == 'error': print(json.dumps({'job_id': job_id, 'error': 'hook failed'}), flush=True); continue\n"
        "    if mode == 'unknown-entry': value = {'entry': {'type': 'not-registered'}}\n"
        "    elif mode == 'bad-wrapper': value = {'nope': 1}\n"
        "    else: value = {'value': 1}\n"
        "    print(json.dumps({'job_id': job_id, 'outputs': {'answer': value}}), flush=True)",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert resolved == {}
    assert 0 in failures


def test_executable_midstream_exit_degrades_only_unanswered_jobs(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' not in request: continue\n"
        "    print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {'answer': {'value': 1}}}), flush=True)\n"
        "    raise SystemExit(1)",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={"answer": {"role": "answer"}})
    resolved, failures, _ = _run_executable_collector(
        [_record(tmp_path, "job-1"), _record(tmp_path, "job-2")], provider, tmp_path
    )
    assert 0 in resolved and 1 in failures


def test_declared_ref_validates_and_uses_definition(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from httk.core.register import known_property_definitions, load_property_definition

    reference = known_property_definitions()[0]
    definition = load_property_definition(reference)
    provider = SimpleNamespace(outputs={"answer": {"role": "answer", "ref": reference}})
    record = _record(tmp_path)
    value = _resolve_executable_output(record, provider, "answer", {"value": []})
    assert isinstance(value, DataRecord) and value.definition_id == definition.definition_id
    with pytest.raises(ValueError, match=definition.name):
        _resolve_executable_output(record, provider, "answer", {"value": "invalid"})

    def blocked_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = None,
        level: int = 0,
    ) -> ModuleType:
        if name.startswith("httk.store"):
            raise ModuleNotFoundError("httk-store is absent", name="httk.store")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", blocked_import)
    with pytest.raises(RuntimeError, match="pip install httk-store"):
        _resolve_executable_output(record, provider, "answer", {"value": []})


def test_collect_manifest_accepts_executable_and_describes_kind(tmp_path: Path) -> None:
    (tmp_path / "httk_workflow.toml").write_text(
        '[workflow]\nid = "tests.exec"\n'
        '[workflow.runner]\nsteps = ["start"]\n'
        '[workflow.collect]\nfile = "collect-hook"\n'
        '[workflow.outputs.answer]\nentry_type = "records"\n',
        encoding="utf-8",
    )
    (tmp_path / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (tmp_path / "run").chmod(0o755)
    hook = _hook(tmp_path, "raise SystemExit(0)")
    provider = parse_workflow_manifest(tmp_path)
    assert provider.collector is None
    assert provider.collector_exec == hook.name
    description = _workflow_description(str(tmp_path))
    hooks = description["hooks"]
    assert isinstance(hooks, Mapping)
    collect = hooks["collect"]
    assert isinstance(collect, Mapping)
    assert collect["kind"] == "executable"


def test_collect_manifest_rejects_non_executable_non_python(tmp_path: Path) -> None:
    (tmp_path / "httk_workflow.toml").write_text(
        '[workflow]\nid = "tests.exec"\n'
        '[workflow.runner]\nsteps = ["start"]\n'
        '[workflow.collect]\nfile = "collect-hook"\n',
        encoding="utf-8",
    )
    (tmp_path / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (tmp_path / "run").chmod(0o755)
    (tmp_path / "collect-hook").write_text("exit 0\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\.py member or an executable member"):
        parse_workflow_manifest(tmp_path)


def test_truncated_outputs_document_degrades_the_job(tmp_path: Path) -> None:
    from httk.workflow.languages import LanguageOutputsMissingError, _load_outputs

    # A published-but-truncated JSON document must degrade this one job, not
    # abort the whole sweep with a bare JSONDecodeError.
    (tmp_path / "outputs.json").write_text('{"a":', encoding="utf-8")
    with pytest.raises(LanguageOutputsMissingError, match="missing or unreadable"):
        _load_outputs(_record(tmp_path), "outputs.json", "prefix")


def test_executable_exit_status_is_surfaced_after_complete_responses(tmp_path: Path) -> None:
    hook = _hook(
        tmp_path,
        "import json, sys\n"
        "for line in sys.stdin:\n"
        "    request = json.loads(line)\n"
        "    if 'record' not in request: continue\n"
        "    print(json.dumps({'job_id': request['record']['job_id'], 'outputs': {}}), flush=True)\n"
        "sys.exit(3)",
    )
    provider = SimpleNamespace(collector_exec=hook.name, directory=tmp_path, outputs={})
    resolved, failures, exit_status = _run_executable_collector([_record(tmp_path)], provider, tmp_path)
    assert failures == {} and resolved[0] == {}
    assert exit_status == 3
