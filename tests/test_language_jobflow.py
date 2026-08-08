import ast
import importlib
import importlib.util
import json
import os
import sys
import time
from pathlib import Path, PurePosixPath
from typing import cast

import pytest
from httk.core import DataRecord

from httk.workflow import TaskManager, Workspace, collect, job_records
from httk.workflow.collecting import JobRecord
from httk.workflow.languages import LanguageRequest, available_languages, jobflow, match_document
from httk.workflow.scaffold import InstantiateContext, describe_runner, new_job, resolve_workflow


def _request(
    tmp_path: Path,
    *,
    document: Path | None = None,
    runner_options: dict[str, object] | None = None,
    inputs: dict[str, dict[str, object]] | None = None,
    parameters: dict[str, dict[str, object]] | None = None,
) -> LanguageRequest:
    return LanguageRequest(
        workflow_id="tests.jobflow",
        directory=tmp_path,
        document=document,
        runner_options={} if runner_options is None else runner_options,
        inputs={} if inputs is None else inputs,
        outputs={"result": {"role": "result_role"}},
        parameters={} if parameters is None else parameters,
    )


def _jobflow_package(
    root: Path,
    source: str,
    *,
    maker: str | None = "toy:Maker",
    document: str | None = None,
    parameters: str = "",
    inputs: str = "",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    runner_source = f'maker = "{maker}"' if maker is not None else f'document = "{document}"'
    (root / "httk_workflow.toml").write_text(
        f'''[workflow]
id = "tests.jobflow.{root.name}"

[workflow.runner]
language = "jobflow"
{runner_source}
{parameters}
{inputs}
[workflow.outputs.output]
entry_type = "records"
role = "result"
''',
        encoding="utf-8",
    )
    (root / "toy.py").write_text(source, encoding="utf-8")
    return root


def _drive(workspace: Workspace, *, maximum_workers: int = 4, timeout: float = 300.0) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01, maximum_workers=maximum_workers) as manager:
        manager.run_until_idle(timeout=timeout)


def _set_pythonpath(monkeypatch: pytest.MonkeyPatch, package: Path) -> None:
    monkeypatch.setenv("PYTHONPATH", f"{package}{os.pathsep}{os.environ.get('PYTHONPATH', '')}")


def _parent_record(workspace: Workspace, job_id: str, *, states: tuple[str, ...] = ("succeeded",)) -> JobRecord:
    return next(record for record in job_records(workspace, states=states) if record.job_id == job_id)


def _collected_value(workspace: Workspace, job_id: str, *, role: str = "result") -> object:
    item = next(item for item in collect(workspace) if item.record.job_id == job_id)
    value = item.outputs[role]
    assert isinstance(value, DataRecord)
    return value.value


def test_jobflow_registers_and_matches_only_maker_documents(tmp_path: Path) -> None:
    maker = tmp_path / "maker.json"
    maker.write_text(json.dumps({"@module": "atomate2.vasp", "@class": "Maker"}), encoding="utf-8")
    pwd = tmp_path / "pwd.json"
    pwd.write_text(json.dumps({"nodes": [], "edges": []}), encoding="utf-8")
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"hello": "world"}), encoding="utf-8")

    assert "jobflow" in available_languages()
    assert match_document(maker) is jobflow.LANGUAGE
    assert match_document(pwd) is not jobflow.LANGUAGE
    assert match_document(other) is None


@pytest.mark.parametrize("path", [Path("missing.json"), Path("not-json.txt")])
def test_matches_rejects_missing_and_non_json_files(tmp_path: Path, path: Path) -> None:
    if path.name != "missing.json":
        (tmp_path / path).write_text("{}", encoding="utf-8")
    assert not jobflow._matches(tmp_path / path)


def test_matches_rejects_bad_json_and_non_dict(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("not json", encoding="utf-8")
    array = tmp_path / "array.json"
    array.write_text("[]", encoding="utf-8")
    assert not jobflow._matches(bad)
    assert not jobflow._matches(array)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}, "unknown"),
        ({"maker": "atomate2:bad-name"}, "dotted Maker"),
    ],
)
def test_validate_runner_rejects_bad_options(options: dict[str, object], message: str, tmp_path: Path) -> None:
    if "unknown" in message:
        options = {"unknown": True}
    with pytest.raises(ValueError, match=message):
        jobflow._validate_runner(options, tmp_path)


def test_validate_runner_accepts_maker(tmp_path: Path) -> None:
    jobflow._validate_runner({"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}, tmp_path)


@pytest.mark.parametrize(
    ("document", "options"),
    [
        (None, {}),
        (Path("maker.json"), {"maker": "atomate2:Maker"}),
    ],
)
def test_prepare_requires_exactly_one_source(tmp_path: Path, document: Path | None, options: dict[str, object]) -> None:
    if document is not None:
        document = tmp_path / document
        document.write_text(json.dumps({"@module": "atomate2", "@class": "Maker"}), encoding="utf-8")
    with pytest.raises(jobflow.JobflowFormatError, match="either maker=.*document=.*not both/neither"):
        jobflow._prepare(_request(tmp_path, document=document, runner_options=options))


def test_prepare_document_stages_maker_and_sets_parameters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    document = tmp_path / "maker.json"
    document.write_text(json.dumps({"@class": "Maker", "@module": "atomate2", "z": 1}), encoding="utf-8")
    monkeypatch.setattr(jobflow, "runner_reference", lambda package, name: {"path": name})

    prepared = jobflow._prepare(_request(tmp_path, document=document))

    assert json.loads(prepared.documents[jobflow.DOCUMENT_FILE]) == {
        "@class": "Maker",
        "@module": "atomate2",
        "z": 1,
    }
    assert prepared.parameters["jobflow_document"] == jobflow.DOCUMENT_FILE
    assert prepared.parameters["workflow_language"] == "jobflow"
    assert prepared.parameters["jobflow_output_roles"] == {"result": "result_role"}


def test_prepare_maker_sets_spec_without_declared_parameters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobflow, "runner_reference", lambda package, name: {"path": name})
    prepared = jobflow._prepare(_request(tmp_path, runner_options={"maker": "atomate2:Maker"}))
    assert prepared.documents == {}
    assert prepared.parameters["jobflow_maker"] == "atomate2:Maker"
    assert "jobflow_maker_parameters" not in prepared.parameters


def test_prepare_records_declared_maker_parameters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(jobflow, "runner_reference", lambda package, name: {"path": name})
    request = _request(
        tmp_path,
        runner_options={"maker": "atomate2:Maker"},
        parameters={"relax_steps": {"type": "integer", "default": 300}},
    )

    prepared = jobflow._prepare(request)

    assert prepared.parameters["jobflow_maker_parameters"] == ("relax_steps",)


def test_prepare_uses_the_packaged_runner_reference(tmp_path: Path) -> None:
    jobflow._prepare(_request(tmp_path, runner_options={"maker": "atomate2:Maker"}))


def test_jobflow_runner_describes_without_importing_jobflow() -> None:
    from httk.workflow.scaffold import describe_runner

    runner_path = Path(jobflow.__file__).with_name(jobflow.RUNNER)
    description = describe_runner(runner_path)
    assert description["workflow"] == "jobflow.workflow"
    steps = cast(list[str], description["steps"])
    assert set(steps) == {"start", "advance", "enter"}
    tree = ast.parse(runner_path.read_text(encoding="utf-8"))
    top_level = tree.body
    assert not any(
        isinstance(node, ast.ImportFrom)
        and node.module == "jobflow"
        or isinstance(node, ast.Import)
        and any(alias.name == "jobflow" for alias in node.names)
        for node in top_level
    )


def test_jobflow_runner_dependency_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.languages.jobflow import jobflow_runner

    original = importlib.util.find_spec

    def missing_jobflow(name: str, package: str | None = None) -> object:
        return None if name == "jobflow" else original(name, package)

    monkeypatch.setattr(jobflow_runner.importlib.util, "find_spec", missing_jobflow)
    with pytest.raises(ValueError, match=r"httk-workflow\[jobflow\]"):
        jobflow_runner._require_dependencies(None)

    def missing_atomate2(name: str, package: str | None = None) -> object:
        return None if name == "atomate2" else original(name, package)

    monkeypatch.setattr(jobflow_runner.importlib.util, "find_spec", missing_atomate2)
    with pytest.raises(ValueError, match=r"httk-workflow\[atomate2\]"):
        jobflow_runner._require_dependencies("atomate2.toy")


def test_jobflow_master_store_snapshot_is_atomic_and_recovers_from_stray_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.languages.jobflow import jobflow_runner
    from httk.workflow.languages.jobflow._driver import merge_documents

    store = jobflow_runner._open_store()
    merge_documents(store, [{"uuid": "u", "index": 1, "output": 2}])
    snapshot = tmp_path / "store.json"
    replaced: list[tuple[Path, Path]] = []
    real_replace = jobflow_runner.os.replace

    def replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        replaced.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr(jobflow_runner.os, "replace", replace)
    jobflow_runner._persist_store(store, snapshot)
    assert replaced == [(snapshot.with_name(".store.json.tmp"), snapshot)]

    snapshot.with_name(".store.json.tmp").write_text("{partial", encoding="utf-8")
    recovered = jobflow_runner._open_store(snapshot)
    document = recovered.query_one({"uuid": "u", "index": 1})
    assert document is not None and document["output"] == 2


def test_jobflow_phantom_spawn_is_re_pended_and_can_complete() -> None:
    from jobflow.core.flow import Flow
    from jobflow.core.job import job

    from httk.workflow.languages.jobflow import jobflow_runner
    from httk.workflow.languages.jobflow._driver import DriverState

    @job
    def one() -> int:
        return 1

    root = one()
    state = DriverState.from_flow(Flow([root], output=root.output))
    key = f"{root.uuid}:{root.index}"
    state.mark_running(key)
    children = {"j00000": key}
    processed: set[str] = set()

    jobflow_runner._recover_unmaterialized(state, children, {"j00000"}, processed)
    assert key in state.running and children == {"j00000": key}
    jobflow_runner._recover_unmaterialized(state, children, set(), processed)
    assert key not in state.running and children == {}

    store = jobflow_runner._open_store()
    state.mark_running(key)
    state.apply_success(key, root.run(store))
    assert state.succeeded


def test_jobflow_linear_maker_runs_through_task_manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("jobflow")
    module = tmp_path / "toy.py"
    module.write_text(
        """from jobflow import Flow, job\n\n@job\ndef first():\n    return 1\n\n@job\ndef second(value):\n    return value + 1\n\nclass Maker:\n    def make(self):\n        one = first()\n        two = second(one.output)\n        return Flow([one, two], output=two.output)\n""",
        encoding="utf-8",
    )
    package = tmp_path / "package"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        """[workflow]\nid = \"tests.jobflow.smoke\"\n\n[workflow.runner]\nlanguage = \"jobflow\"\nmaker = \"toy:Maker\"\n\n[workflow.outputs.output]\nentry_type = \"records\"\nrole = \"result\"\n""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", f"{tmp_path}{os.pathsep}{os.environ.get('PYTHONPATH', '')}")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert json.loads((job.payload / "run" / jobflow.OUTPUTS_FILE).read_text(encoding="utf-8"))["output"] == 2
    assert _collected_value(workspace, job.job_id) == 2
    collected = next(item for item in collect(workspace) if item.record.job_id == job.job_id)
    assert collected.run.inputs == ()
    assert collected.record.provenance["activations"]


def test_jobflow_parallel_diamond_runs_branches_concurrently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    marker_dir = tmp_path / "parallel"
    monkeypatch.setenv("JOBFLOW_PARALLEL_MARKER", str(marker_dir))
    package = _jobflow_package(
        tmp_path / "parallel-package",
        '''import os
import time
from pathlib import Path
from jobflow import Flow, job

MARKER = Path(os.environ["JOBFLOW_PARALLEL_MARKER"])

@job
def root():
    return 1

@job
def branch(value, name):
    MARKER.mkdir(parents=True, exist_ok=True)
    (MARKER / name).write_text("started", encoding="utf-8")
    other = "right" if name == "left" else "left"
    deadline = time.monotonic() + 30
    while not (MARKER / other).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("the sibling branch did not start")
        time.sleep(0.01)
    return value + (1 if name == "left" else 2)

@job
def join(left, right):
    return left + right

class Maker:
    def make(self):
        one = root()
        left = branch(one.output, "left")
        right = branch(one.output, "right")
        final = join(left.output, right.output)
        return Flow([one, left, right, final], output=final.output)
        ''',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package)
    _drive(workspace, maximum_workers=4)

    parent = _parent_record(workspace, root_job.job_id)
    assert parent.state == "succeeded"
    assert _collected_value(workspace, root_job.job_id) == 5
    assert len(parent.children) >= 4
    assert (marker_dir / "left").is_file() and (marker_dir / "right").is_file()
    assert parent.provenance["activations"]


def test_jobflow_dynamic_replace_adds_replacement_children(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _jobflow_package(
        tmp_path / "replace-package",
        '''from jobflow import Flow, Response, job

@job
def seed():
    return 1

@job
def replacement(value):
    return value + 5

@job
def middle(value):
    replaced = replacement(value)
    return Response(replace=Flow([replaced], output=replaced.output))

@job
def finish(value):
    return value * 2

class Maker:
    def make(self):
        one = seed()
        two = middle(one.output)
        three = finish(two.output)
        return Flow([one, two, three], output=three.output)
        ''',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package)
    _drive(workspace)

    assert _collected_value(workspace, root_job.job_id) == 12
    assert len(_parent_record(workspace, root_job.job_id).children) >= 4


def test_jobflow_dynamic_addition_runs_without_changing_flow_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ran = tmp_path / "addition-ran"
    monkeypatch.setenv("JOBFLOW_ADDITION_MARKER", str(ran))
    package = _jobflow_package(
        tmp_path / "addition-package",
        '''import os
from pathlib import Path
from jobflow import Flow, Response, job

@job
def seed():
    return 1

@job
def extra(value):
    Path(os.environ["JOBFLOW_ADDITION_MARKER"]).write_text(str(value), encoding="utf-8")
    return value + 100

@job
def middle(value):
    return Response(output=value + 1, addition=extra(value))

@job
def finish(value):
    return value * 2

class Maker:
    def make(self):
        one = seed()
        two = middle(one.output)
        three = finish(two.output)
        return Flow([one, two, three], output=three.output)
        ''',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package)
    _drive(workspace)

    assert _collected_value(workspace, root_job.job_id) == 4
    assert ran.read_text(encoding="utf-8") == "1"
    assert len(_parent_record(workspace, root_job.job_id).children) >= 4


def test_jobflow_failure_poisoning_preserves_independent_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    good_marker = tmp_path / "good-ran"
    monkeypatch.setenv("JOBFLOW_GOOD_MARKER", str(good_marker))
    failed_uuid = "00000000-0000-0000-0000-000000000001"
    poisoned_uuid = "00000000-0000-0000-0000-000000000002"
    package = _jobflow_package(
        tmp_path / "failure-package",
        f'''import os
from pathlib import Path
from jobflow import Flow, job

@job(uuid="{failed_uuid}")
def fail():
    raise RuntimeError("the failed branch")

@job(uuid="{poisoned_uuid}")
def poisoned(value):
    return value + 1

@job
def good():
    Path(os.environ["JOBFLOW_GOOD_MARKER"]).write_text("good", encoding="utf-8")
    return 7

@job
def independent(value):
    return value + 1

class Maker:
    def make(self):
        bad = fail()
        descendant = poisoned(bad.output)
        healthy = good()
        result = independent(healthy.output)
        return Flow([bad, descendant, healthy, result], output=result.output)
        ''',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package)
    _drive(workspace)

    parent = _parent_record(workspace, root_job.job_id, states=("failed", "succeeded"))
    assert parent.state == "failed" and parent.failure is not None
    assert parent.failure.code == "jobflow.flow_failed"
    assert parent.failure.details is not None
    assert failed_uuid in json.dumps(parent.failure.details)
    state = json.loads((parent.payload / "run" / "jobflow" / "state.json").read_text(encoding="utf-8"))
    assert any(failed_uuid in key for key in state["errored"])
    assert any(poisoned_uuid in key for key in state["skipped"])
    assert good_marker.read_text(encoding="utf-8") == "good"
    assert len(parent.children) == 3

    degraded = next(item for item in collect(workspace, states=("failed",)) if item.record.job_id == root_job.job_id)
    assert degraded.outputs == {}
    assert degraded.missing_collector is not None


def test_jobflow_resumes_after_a_manager_session_ends(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    flag = tmp_path / "resume.flag"
    started = tmp_path / "resume.started"
    monkeypatch.setenv("JOBFLOW_RESUME_FLAG", str(flag))
    monkeypatch.setenv("JOBFLOW_RESUME_STARTED", str(started))
    package = _jobflow_package(
        tmp_path / "resume-package",
        '''import os
import time
from pathlib import Path
from jobflow import Flow, job

@job
def first():
    return 1

@job
def gated(value):
    started = Path(os.environ["JOBFLOW_RESUME_STARTED"])
    started.write_text("started", encoding="utf-8")
    deadline = time.monotonic() + 30
    while not Path(os.environ["JOBFLOW_RESUME_FLAG"]).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("resume gate was not released")
        time.sleep(0.01)
    return value + 1

@job
def last(value):
    return value + 1

class Maker:
    def make(self):
        one = first()
        two = gated(one.output)
        three = last(two.output)
        return Flow([one, two, three], output=three.output)
        ''',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package)
    first_manager = TaskManager(
        workspace,
        heartbeat_interval=0.01,
        lease_seconds=0.1,
        takeover_grace_factor=1.0,
    )
    try:
        deadline = time.monotonic() + 30
        while not started.exists() and time.monotonic() < deadline:
            first_manager.tick()
            time.sleep(0.01)
        assert started.is_file()
    finally:
        first_manager.close()
    flag.write_text("release", encoding="utf-8")

    with TaskManager(
        workspace,
        heartbeat_interval=0.01,
        lease_seconds=0.1,
        takeover_grace_factor=1.0,
    ) as second_manager:
        second_manager.run_until_idle(timeout=300.0)
    marker = workspace.find_marker_by_id(root_job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert _collected_value(workspace, root_job.job_id) == 3


def test_jobflow_maker_parameters_and_document_configuration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = '''from dataclasses import dataclass
from jobflow import Flow, job

@job
def add(value, increment):
    return value + increment

@dataclass
class ToyMaker:
    increment: int = 0
    jobflow_child_outputs: str = ""

    def as_dict(self):
        return {"@module": __name__, "@class": "ToyMaker", "increment": self.increment}

    def make(self):
        if self.jobflow_child_outputs != "declared":
            raise ValueError("Maker configuration did not receive jobflow_child_outputs")
        step = add(1, self.increment)
        return Flow([step], output=step.output)
'''
    parameters = '''[workflow.parameters.increment]
type = "integer"
default = 10

[workflow.parameters.jobflow_child_outputs]
type = "string"
default = "declared"
'''
    package = _jobflow_package(tmp_path / "parameters-package", source, maker="toy:ToyMaker", parameters=parameters)
    _set_pythonpath(monkeypatch, package)
    monkeypatch.syspath_prepend(str(package))
    monkeypatch.delitem(sys.modules, "toy", raising=False)
    workspace = Workspace.initialize(tmp_path / "workspace")
    default_job = new_job(workspace, package)
    override_job = new_job(workspace, package, parameters={"increment": 4})
    importlib.invalidate_caches()
    maker_class = importlib.import_module("toy").ToyMaker
    document = tmp_path / "maker.json"
    document.write_text(jobflow.document_from_maker(maker_class(increment=2)), encoding="utf-8")
    document_package = _jobflow_package(
        tmp_path / "document-parameters-package",
        source,
        maker=None,
        document="maker.json",
        parameters=parameters,
    )
    (document_package / "maker.json").write_text(document.read_text(encoding="utf-8"), encoding="utf-8")
    document_job = new_job(workspace, document_package, parameters={"increment": 7})
    _drive(workspace)

    assert _collected_value(workspace, default_job.job_id) == 11
    assert _collected_value(workspace, override_job.job_id) == 5
    assert _collected_value(workspace, document_job.job_id) == 8


def test_jobflow_structure_input_is_decoded_in_the_real_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _jobflow_package(
        tmp_path / "structure-package",
        '''from jobflow import Flow, job

@job
def formula(structure):
    return structure.composition.reduced_formula

class Maker:
    def make(self, structure):
        result = formula(structure)
        return Flow([result], output=result.output)
''',
        inputs='''[workflow.inputs.structure]
entry_type = "structures"
''',
    )
    _set_pythonpath(monkeypatch, package)
    cif = tmp_path / "silicon.cif"
    cif.write_text(
        """data_Si
_symmetry_space_group_name_H-M 'P 1'
_cell_length_a 5.43
_cell_length_b 5.43
_cell_length_c 5.43
_cell_angle_alpha 90
_cell_angle_beta 90
_cell_angle_gamma 90
loop_
_symmetry_equiv_pos_as_xyz
 'x, y, z'
loop_
_atom_site_type_symbol
_atom_site_label
_atom_site_fract_x
_atom_site_fract_y
_atom_site_fract_z
Si Si1 0 0 0
""",
        encoding="utf-8",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, package, inputs={"structure": cif})
    _drive(workspace)
    assert _collected_value(workspace, root_job.job_id) == "Si"


def test_jobflow_bare_document_runs_and_collects_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = '''from dataclasses import dataclass
from jobflow import Flow, job

@job
def value(increment):
    return increment + 1

@dataclass
class ToyMaker:
    increment: int = 4

    def as_dict(self):
        return {"@module": __name__, "@class": "ToyMaker", "increment": self.increment}

    def make(self):
        result = value(self.increment)
        return Flow([result], output=result.output)
'''
    package = _jobflow_package(tmp_path / "bare-source", source)
    _set_pythonpath(monkeypatch, package)
    monkeypatch.syspath_prepend(str(package))
    monkeypatch.delitem(sys.modules, "toy", raising=False)
    importlib.invalidate_caches()
    maker_class = importlib.import_module("toy").ToyMaker
    document = tmp_path / "bare-maker.json"
    document.write_text(jobflow.document_from_maker(maker_class()), encoding="utf-8")
    assert jobflow._matches(document)
    workspace = Workspace.initialize(tmp_path / "workspace")
    root_job = new_job(workspace, document)
    _drive(workspace)

    assert _collected_value(workspace, root_job.job_id, role="output") == 5


def test_jobflow_atomate2_document_round_trips_without_execution(tmp_path: Path) -> None:
    pytest.importorskip("atomate2")
    from atomate2.vasp.flows.core import DoubleRelaxMaker

    document = tmp_path / "double-relax.json"
    document.write_text(jobflow.document_from_maker(DoubleRelaxMaker()), encoding="utf-8")
    assert jobflow._matches(document)
    resolved = resolve_workflow(document)
    assert resolved.language == "jobflow"
    assert resolved.document_path == document.resolve()
    assert resolved.inputs == {}
    assert resolved.outputs["output"]["role"] == "output"
    description = describe_runner(Path(jobflow.__file__).with_name(jobflow.RUNNER))
    assert description["workflow"] == "jobflow.workflow"
    assert set(cast(list[str], description["steps"])) == {"start", "advance", "enter"}


def test_instantiate_stages_paths_and_preserves_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "POSCAR"
    source.write_text("structure", encoding="utf-8")
    monkeypatch.setattr(jobflow, "runner_reference", lambda package, name: {"path": name})
    prepared = jobflow._prepare(
        _request(
            tmp_path,
            runner_options={"maker": "atomate2:Maker"},
            inputs={"structure": {"port": "maker_structure"}, "settings": {}},
        )
    )
    payload = tmp_path / "payload"
    payload.mkdir()
    context = InstantiateContext(
        payload=payload,
        inputs={"structure": "POSCAR", "settings": {"a": 1}},
        parameters={},
        tag=None,
    )

    assert prepared.instantiate is not None
    prepared.instantiate(context)

    assert (payload / "files/inputs/maker_structure/POSCAR").read_text(encoding="utf-8") == "structure"
    assert context.parameters["jobflow_inputs"] == {
        "maker_structure": {"kind": "path", "value": "files/inputs/maker_structure/POSCAR"},
        "settings": {"kind": "value", "value": {"a": 1}},
    }


def test_jobflow_input_path_typo_is_refused_but_plain_literal_passes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(jobflow, "runner_reference", lambda package, name: {"path": name})
    root = tmp_path / "pkg"
    root.mkdir()
    prepared = jobflow._prepare(
        _request(root, runner_options={"maker": "atomate2:Maker"}, inputs={"structure": {}, "label": {}})
    )
    payload = tmp_path / "payload"
    payload.mkdir()
    assert prepared.instantiate is not None

    # A file that exists in the current directory but not under the package root
    # is a real file the run cannot reach — the refusal names the root location
    # jobflow actually resolved to, not the current-directory path.
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    (cwd / "POSCAR").write_text("structure", encoding="utf-8")
    monkeypatch.chdir(cwd)
    typo = InstantiateContext(payload=payload, inputs={"structure": "POSCAR"}, parameters={}, tag=None)
    with pytest.raises(ValueError, match=r"no file exists under the workflow root at .*pkg.*POSCAR"):
        prepared.instantiate(typo)

    # A bare identifier that names nothing — even an entry-typed id — is a literal.
    literal = InstantiateContext(payload=payload, inputs={"label": "mp-149"}, parameters={}, tag=None)
    prepared.instantiate(literal)
    assert literal.parameters["jobflow_inputs"] == {"label": {"kind": "value", "value": "mp-149"}}


def test_document_from_maker() -> None:
    pytest.importorskip("monty")

    class Maker:
        def as_dict(self) -> dict[str, object]:
            return {"@module": "atomate2", "@class": "Maker", "value": 1}

    assert json.loads(jobflow.document_from_maker(Maker()))["@class"] == "Maker"
    with pytest.raises(jobflow.JobflowFormatError, match="as_dict"):
        jobflow.document_from_maker(object())


def test_collect_uses_jobflow_output_names(tmp_path: Path) -> None:
    (tmp_path / jobflow.OUTPUTS_FILE).write_text(json.dumps({"output": 2}), encoding="utf-8")
    record = JobRecord(
        workspace_root=tmp_path,
        workspace_id="workspace",
        job_id="job",
        job_key="job--job",
        job={"parameters": {"jobflow_output_roles": {"output": "role"}}},
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
    assert set(jobflow.collect(record).keys()) == {"role"}
