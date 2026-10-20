import ast
import contextlib
import importlib
import importlib.util
import json
import os
import re
import shlex
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import pytest
from httk.core import DataRecord

from attempt_fixtures import every_job, fabricate, find, new_job
from httk.workflow import Attempt, TaskManager, Workspace, collect, job_records
from httk.workflow.collecting import JobRecord
from httk.workflow.compat import LanguageRequest, available_languages, jobflow, match_document
from httk.workflow.compat.jobflow import jobflow_runner
from httk.workflow.scaffold import InstantiateContext, describe_runner, resolve_workflow

# The optional language integration is in the [ci] extra; these tests skip without it.
NEEDS_JOBFLOW = pytest.mark.skipif(
    importlib.util.find_spec("jobflow") is None, reason="needs the httk-workflow[jobflow] extra"
)


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
    outputs: str = "",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    runner_source = f'maker = "{maker}"' if maker is not None else f'document = "{document}"'
    (root / "httk_workflow.toml").write_text(
        f'''[workflow]
name = "tests.jobflow.{root.name}"

[workflow.runner]
format = "jobflow"
{runner_source}
{parameters}
{inputs}
[workflow.outputs.output]
entry_type = "records"
role = "result"
{outputs}
''',
        encoding="utf-8",
    )
    (root / "toy.py").write_text(source, encoding="utf-8")
    return root


def _drive(
    workspace: Workspace,
    *,
    maximum_workers: int = 4,
    timeout: float = 300.0,
    resources: dict[str, int] | None = None,
) -> None:
    with TaskManager(
        workspace, heartbeat_interval=0.01, maximum_workers=maximum_workers, resources=resources
    ) as manager:
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


def test_validate_runner_accepts_maker_without_importing_it(tmp_path: Path) -> None:
    # validate_runner runs on every manifest load (describe/resolve of an untrusted
    # package), so it only checks the spec syntax and never imports the module — a
    # bogus class name is accepted here and caught later, at submission.
    jobflow._validate_runner({"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}, tmp_path)
    jobflow._validate_runner({"maker": "json:NoSuchMaker"}, tmp_path)


def test_prepare_rejects_a_missing_maker_class(tmp_path: Path) -> None:
    # ``json`` is importable on the preparing machine, but defines no such class,
    # so the typo is caught at submission (in _prepare) rather than at run time.
    with pytest.raises(ValueError, match="names class 'NoSuchMaker'.*module 'json' does not define"):
        jobflow._prepare(_request(tmp_path, runner_options={"maker": "json:NoSuchMaker"}))


def test_prepare_accepts_a_run_machine_only_maker(tmp_path: Path) -> None:
    # A Maker module not installed on the preparing machine (staged onto PYTHONPATH
    # only at run time) stays accepted; precheck and the runner report a truly
    # missing module.
    prepared = jobflow._prepare(_request(tmp_path, runner_options={"maker": "not_installed_anywhere_xyz:Maker"}))
    assert prepared.parameters["jobflow_maker"] == "not_installed_anywhere_xyz:Maker"


def test_bare_jobflow_document_input_teaches_to_use_a_package(tmp_path: Path) -> None:
    document = tmp_path / "maker.json"
    document.write_text(json.dumps({"@module": "atomate2", "@class": "Maker"}), encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="this jobflow workflow declares no ports"):
        new_job(workspace, document, inputs={"structure": 1})


@pytest.mark.parametrize(
    ("document", "options"),
    [
        (None, {}),
        (Path("maker.json"), {"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}),
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
    prepared = jobflow._prepare(
        _request(tmp_path, runner_options={"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"})
    )
    assert prepared.documents == {}
    assert prepared.parameters["jobflow_maker"] == "atomate2.vasp.flows.core:DoubleRelaxMaker"
    assert "jobflow_maker_parameters" not in prepared.parameters


def test_prepare_records_declared_maker_parameters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request(
        tmp_path,
        runner_options={"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"},
        parameters={"relax_steps": {"type": "integer", "default": 300}},
    )

    prepared = jobflow._prepare(request)

    assert prepared.parameters["jobflow_maker_parameters"] == ("relax_steps",)


def test_prepare_needs_no_runner_reference(tmp_path: Path) -> None:
    jobflow._prepare(_request(tmp_path, runner_options={"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}))


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
    from httk.workflow.compat.jobflow import jobflow_runner

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


def _settings_attempt(settings: dict[str, object], parameters: dict[str, object] | None = None) -> Attempt:
    """Return a stub carrying the real ``Attempt.setting`` resolution over plain layers."""
    stub = SimpleNamespace(parameters=parameters or {}, context=SimpleNamespace(settings=settings), warnings=[])
    stub.setting = lambda name, default=None: Attempt.setting(cast(Attempt, stub), name, default)
    stub.log = SimpleNamespace(append=lambda kind, message: stub.warnings.append(message))
    return cast(Attempt, stub)


@pytest.fixture
def _no_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [name for name in os.environ if name.startswith("HTTK_")]:
        monkeypatch.delenv(name)


@pytest.mark.usefixtures("_no_launch")
def test_atomate2_environment_prepends_the_launch_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.compat.jobflow.jobflow_runner import _atomate2_environment

    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    # Without httk-workflow-vasp's launch wrapper, commands map directly behind the prefix.
    monkeypatch.setattr(jobflow_runner.importlib.util, "find_spec", lambda name, package=None: None)
    mapped = _atomate2_environment(_settings_attempt({"vasp.command": "vasp_std -x"}))
    assert mapped == {
        "ATOMATE2_VASP_CMD": "srun -n 4 vasp_std -x",
        "ATOMATE2_VASP_GAMMA_CMD": "srun -n 4 vasp_std -x",
        "ATOMATE2_VASP_NCL_CMD": "srun -n 4 vasp_std -x",
    }
    # Variant commands then map directly too, after one warning.
    attempt = _settings_attempt({"vasp.command": "vasp_std", "vasp.gamma_command": "vasp_gam"})
    mapped = _atomate2_environment(attempt)
    assert mapped["ATOMATE2_VASP_GAMMA_CMD"] == "srun -n 4 vasp_gam"
    assert mapped["ATOMATE2_VASP_NCL_CMD"] == "srun -n 4 vasp_std"
    assert len(cast(Any, attempt).warnings) == 1 and "httk-workflow-vasp" in cast(Any, attempt).warnings[0]


_DISPATCH = [sys.executable, "-P", "-m", "httk.codes.vasp.launch"]


def _vasp_argv(value: object, command: str) -> bool:
    """Return whether a child's ``ATOMATE2_VASP_CMD`` maps ``vasp.command`` without a launch prefix.

    The child's interpreter path may differ in spelling from this process's ``sys.executable``.
    """
    argv = shlex.split(str(value))
    if jobflow_runner._dispatcher_installed():
        return argv[1:] == [*_DISPATCH[1:], "--standard", command, "--ncl", command]
    return argv == [command]


@pytest.mark.usefixtures("_no_launch")
@pytest.mark.parametrize(
    ("variants", "flags"),
    [
        ({}, ["--ncl", "vasp_std -x"]),
        ({"vasp.ncl_command": "vasp_ncl"}, ["--ncl", "vasp_ncl"]),
        ({"vasp.gamma_command": "vasp_gam -q"}, ["--ncl", "vasp_std -x", "--gamma", "vasp_gam -q"]),
        (
            {"vasp.ncl_command": "vasp_ncl", "vasp.gamma_command": "vasp_gam"},
            ["--ncl", "vasp_ncl", "--gamma", "vasp_gam"],
        ),
    ],
)
def test_atomate2_environment_dispatches_vasp_variants(
    monkeypatch: pytest.MonkeyPatch, variants: dict[str, object], flags: list[str]
) -> None:
    from httk.workflow.compat.jobflow.jobflow_runner import _atomate2_environment

    monkeypatch.setattr(jobflow_runner, "_dispatcher_installed", lambda: True)
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    mapped = _atomate2_environment(_settings_attempt({"vasp.command": "vasp_std -x", **variants}))
    # The wrapper runs in front of the prefix and applies it itself: the mapping prepends nothing,
    # and the inner commands stay program-only.
    wrapper = shlex.join([*_DISPATCH, "--standard", "vasp_std -x", *flags])
    assert mapped == {
        name: wrapper for name in ("ATOMATE2_VASP_CMD", "ATOMATE2_VASP_GAMMA_CMD", "ATOMATE2_VASP_NCL_CMD")
    }
    assert "srun" not in mapped["ATOMATE2_VASP_CMD"]


@pytest.mark.usefixtures("_no_launch")
def test_atomate2_environment_refuses_a_launcher_in_a_variant_command(monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.compat.jobflow.jobflow_runner import _atomate2_environment

    monkeypatch.setattr(jobflow_runner, "_dispatcher_installed", lambda: True)
    attempt = _settings_attempt({"vasp.command": "vasp_std", "vasp.ncl_command": "mpirun vasp_ncl"})
    assert "mpirun vasp_ncl" in _atomate2_environment(attempt)["ATOMATE2_VASP_CMD"]
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    with pytest.raises(ValueError, match=r"vasp\.ncl_command setting.*bare program"):
        _atomate2_environment(attempt)


@pytest.mark.usefixtures("_no_launch")
def test_atomate2_environment_refuses_a_launcher_only_under_a_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.compat.jobflow import jobflow_runner
    from httk.workflow.compat.jobflow.jobflow_runner import _atomate2_environment

    monkeypatch.setattr(jobflow_runner, "_dispatcher_installed", lambda: False)
    attempt = _settings_attempt({"vasp.command": "srun vasp_std"})
    assert _atomate2_environment(attempt)["ATOMATE2_VASP_CMD"] == "srun vasp_std"
    monkeypatch.setenv("HTTK_WORKFLOW_LAUNCH", "srun -n 4")
    with pytest.raises(ValueError, match=r"vasp\.command setting.*bare program"):
        _atomate2_environment(attempt)
    events: list[tuple[str, str]] = []
    stub = cast(SimpleNamespace, attempt)
    stub.fail = lambda code, message: events.append((code, message))
    stub.log = SimpleNamespace(append=lambda kind, message: events.append((kind, message)))
    atomate2_job = {"function": {"@module": "atomate2.vasp.jobs.base", "@callable": "BaseVaspMaker.make"}}
    assert not jobflow_runner._apply_atomate2_environment(attempt, atomate2_job)
    assert [code for code, _message in events] == ["jobflow.launch_conflict"]
    # Any other job runs on, without the VASP variables, after one warning.
    events.clear()
    monkeypatch.setattr(os, "environ", {"HTTK_WORKFLOW_LAUNCH": "srun -n 4"})
    assert jobflow_runner._apply_atomate2_environment(attempt, {"function": {"@module": "toy"}})
    assert [kind for kind, _message in events] == ["warning"]
    assert not any(name.startswith("ATOMATE2_VASP") for name in os.environ)


@pytest.mark.usefixtures("_no_launch")
def test_atomate2_environment_maps_library_settings_and_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.compat.jobflow.jobflow_runner import _atomate2_environment

    monkeypatch.setattr(jobflow_runner, "_dispatcher_installed", lambda: False)

    settings: dict[str, object] = {
        "pymatgen.psp_dir": "/potcars",
        "atomate2.vasp_custodian_max_errors": 3,
        "atomate2.flag": True,
        "vasp.pseudo_library": "/httk-potcars",
    }
    assert _atomate2_environment(_settings_attempt(settings)) == {
        "PMG_VASP_PSP_DIR": "/potcars",
        "ATOMATE2_VASP_CUSTODIAN_MAX_ERRORS": "3",
    }
    settings["vasp.command"] = "vasp_std"
    mapped = _atomate2_environment(
        _settings_attempt(settings, {"vasp.command": "vasp_job", "atomate2.vasp_custodian_max_errors": 5})
    )
    assert mapped["ATOMATE2_VASP_CMD"] == "vasp_job"
    assert mapped["ATOMATE2_VASP_CUSTODIAN_MAX_ERRORS"] == "5"
    assert "ATOMATE2_VASP_CMD" not in _atomate2_environment(_settings_attempt(settings), vasp=False)


@NEEDS_JOBFLOW
def test_jobflow_runner_exports_atomate2_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    names = ("ATOMATE2_VASP_CMD", "PMG_VASP_PSP_DIR", "ATOMATE2_VASP_INCAR_UPDATES")
    for name in (*names, "HTTK_WORKFLOW_LAUNCH", "HTTK_VASP_COMMAND"):
        monkeypatch.delenv(name, raising=False)
    package = _jobflow_package(
        tmp_path / "settings-package",
        f"""import os
from jobflow import job

@job
def report():
    return {{name: os.environ.get(name) for name in {names!r}}}

class Maker:
    def make(self):
        return report()
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.configure_settings(
        {"vasp.command": "vasp_std", "pymatgen.psp_dir": "/potcars", "atomate2.vasp_incar_updates": "{}"}
    )
    job = new_job(workspace, package)
    _drive(workspace)
    assert find(workspace, job.job_id).state == "succeeded"
    collected = cast(dict[str, object], _collected_value(workspace, job.job_id))
    assert _vasp_argv(collected.pop("ATOMATE2_VASP_CMD"), "vasp_std")
    assert collected == {"PMG_VASP_PSP_DIR": "/potcars", "ATOMATE2_VASP_INCAR_UPDATES": "{}"}


@NEEDS_JOBFLOW
def test_jobflow_master_store_snapshot_is_atomic_and_recovers_from_stray_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.compat.jobflow import jobflow_runner
    from httk.workflow.compat.jobflow._driver import merge_documents

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


@NEEDS_JOBFLOW
def test_jobflow_phantom_spawn_is_re_pended_and_can_complete() -> None:
    from jobflow.core.flow import Flow
    from jobflow.core.job import job

    from httk.workflow.compat.jobflow import jobflow_runner
    from httk.workflow.compat.jobflow._driver import DriverState

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
        """[workflow]\nname = \"tests.jobflow.smoke\"\n\n[workflow.runner]\nformat = \"jobflow\"\nmaker = \"toy:Maker\"\n\n[workflow.outputs.output]\nentry_type = \"records\"\nrole = \"result\"\n""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONPATH", f"{tmp_path}{os.pathsep}{os.environ.get('PYTHONPATH', '')}")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    marker = find(workspace, job.job_id)
    assert marker.state == "succeeded"
    assert (
        json.loads((find(workspace, job.job_id).path / "run" / jobflow.OUTPUTS_FILE).read_text(encoding="utf-8"))[
            "output"
        ]
        == 2
    )
    assert _collected_value(workspace, job.job_id) == 2
    items = list(collect(workspace))
    collected = next(item for item in items if item.record.job_id == job.job_id)
    assert collected.run.inputs == ()
    assert collected.record.provenance["activations"]
    # Each sub-job is named after its jobflow job, in job.json and on its stored run.
    jobs = [(item, cast(dict[str, Any], item.record.job)) for item in items]
    children = [(item, job) for item, job in jobs if "jobflow_child_job" in job.get("parameters", {})]
    assert sorted(cast(str, job["name"]) for _, job in children) == ["first", "second"]
    assert sorted(cast(str, item.run.name) for item, _ in children) == ["first", "second"]


@NEEDS_JOBFLOW
@pytest.mark.timing
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


@NEEDS_JOBFLOW
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


def _snapshot_outputs(workspace: Workspace, job_id: str) -> list[list[object]]:
    """Return the sorted job outputs held by each spooled snapshot of a finished parent."""
    spool = find(workspace, job_id).path / "run" / "jobflow" / "spool"
    snapshots = [json.loads(path.read_text(encoding="utf-8")) for path in spool.glob("*-store.json")]
    return sorted(sorted((doc["output"] for doc in snapshot), key=repr) for snapshot in snapshots)


@NEEDS_JOBFLOW
def test_jobflow_snapshots_carry_only_referenced_documents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _jobflow_package(
        tmp_path / "chain-package",
        """from jobflow import Flow, job

@job
def first():
    return "one"

@job
def second(value):
    return value + "-two"

@job
def third(value):
    return value + "-three"

class Maker:
    def make(self):
        one = first()
        two = second(one.output)
        three = third(two.output)
        return Flow([one, two, three], output=three.output)
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    assert _collected_value(workspace, job.job_id) == "one-two-three"
    # The third job's snapshot holds the second job's document only, not the first's.
    assert _snapshot_outputs(workspace, job.job_id) == [[], ["one"], ["one-two"]]


@NEEDS_JOBFLOW
def test_jobflow_replacement_referencing_an_outside_job_runs_on_filtered_snapshots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _jobflow_package(
        tmp_path / "outside-package",
        """from jobflow import Flow, OutputReference, Response, job

@job
def seed():
    return 3

@job
def replacement(value):
    return value * 10

@job
def middle(seed_uuid):
    replaced = replacement(OutputReference(seed_uuid))
    return Response(replace=Flow([replaced], output=replaced.output))

@job
def finish(value):
    return value + 1

class Maker:
    def make(self):
        one = seed()
        two = middle(one.uuid)
        three = finish(two.output)
        return Flow([one, two, three], output=three.output)
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    # finish references the replaced middle job, whose stored output refers to the replacement.
    assert _collected_value(workspace, job.job_id) == 31


@NEEDS_JOBFLOW
def test_jobflow_replaced_data_job_consumed_downstream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # jobflow copies a data=True job's config to the replacement's store_inputs job, whose
    # OutputReference output then lives in the data store as a blob.
    package = _jobflow_package(
        tmp_path / "data-package",
        """from jobflow import Flow, Response, job

@job
def inner(x):
    return x + 1

@job(data=True)
def outer(x):
    replaced = inner(x)
    return Response(replace=Flow([replaced], output=replaced.output))

@job
def finish(value):
    return value * 2

class Maker:
    def make(self):
        first = outer(1)
        last = finish(first.output)
        return Flow([first, last], output=last.output)
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    assert _collected_value(workspace, job.job_id) == 4


@NEEDS_JOBFLOW
def test_jobflow_expose_store_job_gets_the_full_snapshot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _jobflow_package(
        tmp_path / "expose-package",
        """from jobflow import CURRENT_JOB, Flow, JobConfig, job

@job
def first():
    return 1

@job
def second(value):
    return value + 1

@job(config=JobConfig(expose_store=True))
def count(value):
    return len(list(CURRENT_JOB.store.query()))

class Maker:
    def make(self):
        one = first()
        two = second(one.output)
        three = count(two.output)
        return Flow([one, two, three], output=three.output)
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    assert _collected_value(workspace, job.job_id) == 2


_RESOURCE_FLOW = """from jobflow import Flow, JobConfig, job

def _config(resources):
    return JobConfig(manager_config={"httk": {"resources": resources}})

@job
def relax_a():
    return 1

@job(config=_config({"procs": 5}))
def relax_b():
    return 2

@job(config=_config({"procs": 3}))
def static():
    return 3

@job
def other():
    return 4

class Maker:
    def make(self):
        jobs = [relax_a(), relax_b(), static(), other()]
        return Flow(jobs, output=[item.output for item in jobs])
"""

_RESOURCE_TABLES = """[workflow.resources]
procs = 1
mem = 1000
maxtime = "02:00:00"

[workflow.runner.resources."relax*"]
procs = 2
maxtime = "01:00:00"
"""


def _child_resources_by_name(workspace: Workspace, parent_id: str) -> dict[str, dict[str, object]]:
    """Return each spawned child's resources, keyed by the name of the jobflow job it ran."""
    run = find(workspace, parent_id).path / "run"
    result: dict[str, dict[str, object]] = {}
    for record in job_records(workspace):
        job = cast(dict[str, Any], record.job)
        pointer = job.get("parameters", {}).get("jobflow_child_job")
        if pointer is not None:
            name = json.loads((run / pointer).read_text(encoding="utf-8"))["name"]
            result[name] = dict(job["resources"])
    return result


@NEEDS_JOBFLOW
def test_jobflow_job_resources_overlay_by_glob_then_manager_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _jobflow_package(tmp_path / "resources-package", _RESOURCE_FLOW, parameters=_RESOURCE_TABLES)
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace, resources={"procs": 8, "mem": 10000})
    assert _collected_value(workspace, job.job_id) == [1, 2, 3, 4]
    assert _child_resources_by_name(workspace, job.job_id) == {
        # The glob table overlays the inherited resources (mem kept) and wins over manager_config.
        "relax_a": {"procs": 2, "mem": 1000, "maxtime": 3600},
        "relax_b": {"procs": 2, "mem": 1000, "maxtime": 3600},
        "static": {"procs": 3, "mem": 1000, "maxtime": 7200},
        "other": {"procs": 1, "mem": 1000, "maxtime": 7200},
    }


def test_jobflow_invalid_resource_table_is_refused_at_job_creation(tmp_path: Path) -> None:
    package = _jobflow_package(
        tmp_path / "bad-resources-package",
        "",
        parameters='[workflow.runner.resources."relax*"]\nmaxtime = 3600\n',
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match=r"workflow\.runner\.resources.*maxtime"):
        new_job(workspace, package)


@NEEDS_JOBFLOW
def test_jobflow_invalid_manager_config_resources_fail_the_flow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _jobflow_package(
        tmp_path / "bad-config-package",
        """from jobflow import JobConfig, job

@job(config=JobConfig(manager_config={"httk": {"resources": {"procs": "lots"}}}))
def greedy():
    return 1

class Maker:
    def make(self):
        return greedy()
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    parent = _parent_record(workspace, job.job_id, states=("failed",))
    assert parent.failure is not None and parent.failure.code == "jobflow.resources_invalid"
    assert "greedy" in parent.failure.message


_OVERRIDE_FLOW = """from dataclasses import dataclass, field
from jobflow import Flow, Maker as JobflowMaker, job

@dataclass
class Settings(JobflowMaker):
    name: str = "settings"
    value: int = 1

    def make(self):
        raise NotImplementedError

@dataclass
class Step(JobflowMaker):
    name: str = "relax step"
    settings: Settings = field(default_factory=Settings)

    @job
    def make(self, offset):
        return self.settings.value + offset

@job
def constant(value):
    return value

def replace_with_constant(flow, value):
    return constant(value)

def keep(flow):
    return None

@dataclass
class Maker:
    offset: int = 10
    step: Step = field(default_factory=Step)

    def make(self):
        step = self.step.make(self.offset)
        return Flow([step], output=step.output)
"""


def _override_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrides: list[Any], *, document: bool = False
) -> tuple[Workspace, str]:
    if document:
        package = _jobflow_package(tmp_path / "override-package", _OVERRIDE_FLOW, maker=None, document="maker.json")
        (package / "maker.json").write_text(json.dumps({"@module": "toy", "@class": "Maker"}), encoding="utf-8")
    else:
        package = _jobflow_package(tmp_path / "override-package", _OVERRIDE_FLOW)
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, parameters={"jobflow_overrides": overrides})
    _drive(workspace, resources={"procs": 4})
    return workspace, job.job_id


@NEEDS_JOBFLOW
def test_jobflow_overrides_update_nested_makers_and_job_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, job_id = _override_job(
        tmp_path,
        monkeypatch,
        [
            {"kind": "maker_kwargs", "update": {"value": 5}, "class_filter": "toy:Settings"},
            {
                "kind": "config",
                "config": {"manager_config": {"httk": {"resources": {"procs": 2}}}},
                "name_filter": "relax",
                "attributes": ["manager_config"],
            },
            {"kind": "function", "function": "toy:keep"},
        ],
    )
    # The nested Settings Maker took the update; the config override set the job's resources.
    assert _collected_value(workspace, job_id) == 15
    assert _child_resources_by_name(workspace, job_id) == {"relax step": {"procs": 2}}


@NEEDS_JOBFLOW
def test_jobflow_function_override_replaces_the_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    overrides = [{"kind": "function", "function": "toy:replace_with_constant", "kwargs": {"value": 42}}]
    workspace, job_id = _override_job(tmp_path, monkeypatch, overrides)
    assert _collected_value(workspace, job_id) == 42


@NEEDS_JOBFLOW
@pytest.mark.parametrize(
    ("overrides", "expected", "document"),
    [
        # Nested and top-level fields that make() reads, on a Maker spec and on a staged Maker document.
        ([{"kind": "maker", "update": {"offset": 100, "step.settings.value": 3}}], 103, False),
        ([{"kind": "maker", "update": {"offset": 100, "step.settings.value": 3}}], 103, True),
        # Listed after maker_kwargs, the maker override still precedes make(); both take effect.
        (
            [
                {"kind": "maker_kwargs", "update": {"value": 5}, "class_filter": "toy:Settings"},
                {"kind": "maker", "update": {"offset": 100}},
            ],
            105,
            False,
        ),
    ],
)
def test_jobflow_maker_override_precedes_make(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrides: list[Any], expected: int, document: bool
) -> None:
    workspace, job_id = _override_job(tmp_path, monkeypatch, overrides, document=document)
    assert _collected_value(workspace, job_id) == expected


@dataclass
class _Inner:
    settings: dict[str, object] = field(default_factory=lambda: {"keep": 1})
    config: object = None


@dataclass
class _Outer:
    inner: _Inner | None = field(default_factory=_Inner)
    plain: int = 0


@NEEDS_JOBFLOW
def test_maker_override_rebuilds_immutably_and_decodes_values() -> None:
    from jobflow.core.job import JobConfig

    original = _Outer()
    config = JobConfig(resolve_references=False)
    updated = cast(
        _Outer,
        jobflow_runner._apply_maker_override(
            original, {"inner.settings.ENCUT": 600, "inner.config": config.as_dict(), "plain": None}
        ),
    )
    assert updated.inner is not None and updated.inner.settings == {"keep": 1, "ENCUT": 600}
    assert updated.inner.config == config and updated.plain is None
    # The original Maker and its shared mapping are untouched.
    assert original.inner is not None and original.inner.settings == {"keep": 1} and original.inner.config is None
    assert cast(_Outer, jobflow_runner._apply_maker_override(original, {"inner": None})).inner is None


@NEEDS_JOBFLOW
@pytest.mark.parametrize(
    ("update", "message"),
    [
        ({"nothing": 1}, "path 'nothing': 'nothing' is not a field of _Outer"),
        ({"plain.x": 1}, "path 'plain.x': 'x' is not a field of int"),
        ({"inner.settings.missing.x": 1}, "path 'inner.settings.missing.x': no key 'missing'"),
        ({"inner": None, "inner.settings": {}}, "path 'inner.settings': 'settings' is reached through None"),
        (
            {"inner.config": [{"@module": "nonexistent.module", "@class": "X"}]},
            "path 'inner.config': the value cannot be decoded",
        ),
    ],
)
def test_maker_override_refuses_unknown_paths(update: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        jobflow_runner._apply_maker_override(_Outer(), update)


@NEEDS_JOBFLOW
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            [{"kind": "function", "function": "toy:keep"}, {"kind": "function", "function": "toy:missing"}],
            "jobflow_overrides[1] (function) failed: AttributeError",
        ),
        (
            [{"kind": "function", "function": "toy:keep"}, {"kind": "maker", "update": {"step.nothing": 1}}],
            "jobflow_overrides[1] (maker) failed: ValueError: path 'step.nothing'",
        ),
    ],
)
def test_jobflow_failing_override_is_named_by_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overrides: list[Any], message: str
) -> None:
    workspace, job_id = _override_job(tmp_path, monkeypatch, overrides)
    parent = _parent_record(workspace, job_id, states=("failed",))
    assert parent.failure is not None and parent.failure.code == "jobflow.override_failed"
    assert parent.failure.message.startswith(message)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"kind": "config"}, "must be a list"),
        ([{"kind": "powerup"}], r"jobflow_overrides\[0\] must be an object whose kind"),
        ([{"kind": "config"}], r"jobflow_overrides\[0\] lacks 'config'"),
        ([{"kind": "function", "function": "toy:keep", "extra": 1}], "unknown member 'extra'"),
        ([{"kind": "maker_kwargs", "update": {}, "class_filter": "Settings"}], "class_filter must be"),
        ([{"kind": "config", "config": {}, "dynamic": "yes"}], "dynamic must be a boolean"),
        ([{"kind": "maker", "update": []}], r"jobflow_overrides\[0\]\.update must be an object"),
        ([{"kind": "maker", "update": {}}], r"jobflow_overrides\[0\]\.update must be a non-empty object"),
        ([{"kind": "maker", "update": {"step..name": 1}}], r"\.update must be a non-empty object keyed by dotted"),
        ([{"kind": "maker", "update": {"offset": 1}, "name_filter": "x"}], "unknown member 'name_filter'"),
    ],
)
def test_jobflow_malformed_overrides_are_refused_at_job_creation(
    tmp_path: Path, overrides: object, message: str
) -> None:
    package = _jobflow_package(tmp_path / "override-package", _OVERRIDE_FLOW)
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match=message):
        new_job(workspace, package, parameters={"jobflow_overrides": overrides})


def test_jobflow_overrides_need_no_declaration(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    package = _jobflow_package(
        tmp_path / "declared-package",
        _OVERRIDE_FLOW,
        parameters='[workflow.parameters.offset]\ntype = "integer"\n',
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    with caplog.at_level("WARNING"):
        new_job(workspace, package, parameters={"jobflow_overrides": [], "stray": 1})
    warned = [record.getMessage() for record in caplog.records if "is not declared" in record.getMessage()]
    assert len(warned) == 1 and "'stray'" in warned[0]


def test_atomate2_powerup_override_updates_the_input_set_generator() -> None:
    pytest.importorskip("atomate2")
    from atomate2.vasp.jobs.core import StaticMaker
    from pymatgen.core import Lattice, Structure

    flow = cast(Any, StaticMaker()).make(Structure(Lattice.cubic(3.0), ["Si"], [[0, 0, 0]]))
    override = {
        "kind": "function",
        "function": "atomate2.vasp.powerups:update_user_incar_settings",
        "kwargs": {"incar_updates": {"ENCUT": 600}},
    }
    updated = jobflow_runner._apply_override(flow, override)
    assert updated.function.__self__.input_set_generator.user_incar_settings["ENCUT"] == 600


@NEEDS_JOBFLOW
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


@NEEDS_JOBFLOW
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
    state = json.loads(
        (find(workspace, parent.job_id).path / "run" / "jobflow" / "state.json").read_text(encoding="utf-8")
    )
    assert any(failed_uuid in key for key in state["errored"])
    assert any(poisoned_uuid in key for key in state["skipped"])
    assert good_marker.read_text(encoding="utf-8") == "good"
    assert len(parent.children) == 3

    degraded = next(item for item in collect(workspace, states=("failed",)) if item.record.job_id == root_job.job_id)
    assert degraded.outputs == {}
    assert degraded.missing_collector is not None


@NEEDS_JOBFLOW
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


@NEEDS_JOBFLOW
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


@NEEDS_JOBFLOW
def test_jobflow_datetime_output_and_object_stored_data_are_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _jobflow_package(
        tmp_path / "datetime-package",
        """from datetime import datetime
from jobflow import Response, job
from monty.json import MSONable
from pydantic import BaseModel

class Doc(BaseModel):
    completed_at: datetime

class Note(MSONable):
    def __init__(self, text):
        self.text = text

@job
def report():
    return Response(output=Doc(completed_at=datetime(2026, 1, 2, 3, 4, 5)), stored_data={"note": Note("kept")})

class Maker:
    def make(self):
        return report()
""",
        outputs='[workflow.outputs.stored_data]\nentry_type = "records"\nrole = "stored"\n',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    ref = find(workspace, job.job_id)
    assert ref.state == "succeeded"
    written = json.loads((ref.path / "run" / jobflow.OUTPUTS_FILE).read_text(encoding="utf-8"))
    assert written["output"]["completed_at"].startswith("2026-01-02")
    [stored] = written["stored_data"].values()
    assert stored["note"]["@class"] == "Note" and stored["note"]["text"] == "kept"
    collected = cast(dict[str, object], _collected_value(workspace, job.job_id))
    assert collected["completed_at"] == written["output"]["completed_at"]
    assert _collected_value(workspace, job.job_id, role="stored") == written["stored_data"]


@NEEDS_JOBFLOW
def test_jobflow_directory_references_follow_a_moved_sibling(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _jobflow_package(
        tmp_path / "directory-package",
        """import os
import socket
from pathlib import Path
from jobflow import Flow, job

@job
def first():
    Path("marker.txt").write_text(os.getcwd(), encoding="utf-8")
    return {"dir_name": f"{socket.gethostname()}:{os.getcwd()}"}

@job
def second(prev_dir):
    # As atomate2.utils.path.strip_hostname: the resolved path has no host prefix.
    if ":" in prev_dir:
        prev_dir = prev_dir.split(":", 1)[1]
    return (Path(prev_dir) / "marker.txt").read_text(encoding="utf-8")

class Maker:
    def make(self):
        one = first()
        two = second(one.output["dir_name"])
        return Flow([one, two], output={"dir_name": one.output["dir_name"], "content": two.output})
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package)
    _drive(workspace)
    assert find(workspace, job.job_id).state == "succeeded"
    output = cast(dict[str, str], _collected_value(workspace, job.job_id))
    # The path the first job ran in is gone (its payload moved to a terminal state),
    # yet the second job read the marker through the resolved token.
    assert not Path(output["content"]).exists()
    assert output["dir_name"].startswith(jobflow_runner.EXEC_TOKEN)
    producer = output["dir_name"].removeprefix(jobflow_runner.EXEC_TOKEN)
    assert (find(workspace, producer).path / "run" / "exec" / "marker.txt").read_text(encoding="utf-8") == output[
        "content"
    ]


def test_exec_directory_tokens(tmp_path: Path) -> None:
    exec_dir = tmp_path / "run" / "exec"
    exec_dir.mkdir(parents=True)
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path)
    job_id = "00000000-0000-4000-8000-000000000001"
    sibling = "00000000-0000-4000-8000-000000000002"
    directories = {
        str(alias / "run" / "exec"): jobflow_runner.EXEC_TOKEN + job_id,
        os.path.realpath(alias / "run" / "exec"): jobflow_runner.EXEC_TOKEN + job_id,
        str(tmp_path / "sibling" / "run" / "exec"): jobflow_runner.EXEC_TOKEN + sibling,
    }
    real = str(exec_dir.resolve())
    token = jobflow_runner.EXEC_TOKEN + job_id
    shipped = {
        "exact": str(alias / "run" / "exec"),
        "host": [f"node1:{alias}/run/exec", f"node1:{real}/OUTCAR"],
        "subpath": f"{alias}/run/exec/CONTCAR",
        "real": real,
        "sibling": f"{tmp_path}/sibling/run/exec/CHGCAR",
        "untouched": [f"{real}-other", f"a/b:{real}", "INCAR", 3, None, f"x {real}"],
    }
    tokenized = jobflow_runner._map_strings(shipped, lambda value: jobflow_runner._tokenize_exec(value, directories))
    assert tokenized == {
        "exact": token,
        "host": [token, f"{token}/OUTCAR"],
        "subpath": f"{token}/CONTCAR",
        "real": token,
        "sibling": f"{jobflow_runner.EXEC_TOKEN}{sibling}/CHGCAR",
        "untouched": shipped["untouched"],
    }
    assert jobflow_runner._map_strings(
        tokenized, lambda value: jobflow_runner._resolve_exec(value, {job_id: "/now/run/exec"})
    ) == {**tokenized, "exact": "/now/run/exec", "host": ["/now/run/exec", "/now/run/exec/OUTCAR"]} | {
        "subpath": "/now/run/exec/CONTCAR",
        "real": "/now/run/exec",
    }


@NEEDS_JOBFLOW
def test_enter_fails_on_an_unresolved_input_directory_and_warns_for_the_store(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_policy({"visibility_deadline_seconds": 0.05})
    absent = "00000000-0000-4000-8000-000000000001"
    other = "00000000-0000-4000-8000-000000000002"
    token = jobflow_runner.EXEC_TOKEN + absent
    parent_workdir = tmp_path / "parent" / "run"
    parent_workdir.mkdir(parents=True)
    (parent_workdir / "job.json").write_text(json.dumps({"function": {"@module": "toy"}, "args": [token]}))
    producer = "11111111-1111-4111-8111-111111111111"
    store = [{"uuid": producer, "index": 1, "output": {"dir_name": f"{jobflow_runner.EXEC_TOKEN}{other}/x"}}]
    (parent_workdir / "store.json").write_text(json.dumps(store))
    events: list[tuple[str, str]] = []
    stub = SimpleNamespace(
        workspace=workspace.root,
        context=SimpleNamespace(durable=False, settings={}, job_id="00000000-0000-4000-8000-000000000003"),
        parameters={},
        setting=lambda name, default=None: default,
        parameter=lambda name, default=None: {
            "jobflow_child_job": "job.json",
            "jobflow_child_snapshot": "store.json",
        }[name],
        parent=SimpleNamespace(workdir=parent_workdir, payload=parent_workdir.parent, placement=PurePosixPath(".")),
        workdir=tmp_path / "child",
        fail=lambda code, message: events.append((code, message)),
        succeed=lambda: events.append(("succeeded", "")),
        log=SimpleNamespace(append=lambda kind, message: events.append((kind, message))),
    )
    assert jobflow_runner._locate_exec_directories(cast(Attempt, stub), PurePosixPath("."), {absent, "bad"}) == {}
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [
        (
            "jobflow.reference_unresolved",
            f"this job's inputs name the directory of jobflow job {absent}, which is not in this workspace",
        )
    ]
    # A real job whose input reference resolves to the output document holding the token.
    from jobflow.core.job import store_inputs
    from jobflow.core.reference import OutputReference

    from httk.workflow.compat.jobflow._driver import spool_job

    events.clear()
    (parent_workdir / "job.json").write_text(json.dumps(spool_job(store_inputs(OutputReference(producer)))))
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [
        (
            "jobflow.reference_unresolved",
            f"this job's input references name the directory of jobflow job {other}, which is not in this workspace",
        )
    ]
    # The same token in a document the job does not reference only warns.
    events.clear()
    (parent_workdir / "job.json").write_text(json.dumps(spool_job(store_inputs(5))))
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [("warning", f"the jobflow store names directories of absent jobs {other}"), ("succeeded", "")]


_DIRECTORY_FLOW = """import os
from jobflow import Flow, job

@job
def listing(prev_dir):
    assert os.path.isdir(prev_dir) and "/run/inputs/prev_dir/" in prev_dir
    files = sorted(os.path.relpath(os.path.join(root, name), prev_dir)
                   for root, _dirs, names in os.walk(prev_dir) for name in names)
    return {"files": files, "dir": prev_dir}

@job
def again(previous, prev_dir):
    return previous["files"] == listing.original(prev_dir)["files"]

class Maker:
    def make(self, prev_dir):
        first = listing(prev_dir)
        second = again(first.output, prev_dir)
        return Flow([first, second], output={"first": first.output, "same": second.output})
"""


@NEEDS_JOBFLOW
def test_jobflow_directory_input_reaches_each_job_as_a_local_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package = _jobflow_package(
        tmp_path / "directory-input-package",
        _DIRECTORY_FLOW,
        inputs='[workflow.inputs.prev_dir]\ndirectory = true\n',
    )
    _set_pythonpath(monkeypatch, package)
    calculation = tmp_path / "calc"
    (calculation / "sub").mkdir(parents=True)
    (calculation / "INCAR").write_text("ENCUT = 500\n", encoding="utf-8")
    (calculation / "sub" / "OUTCAR").write_text("done\n", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"prev_dir": str(calculation)})
    _drive(workspace)
    output = cast(dict[str, Any], _collected_value(workspace, job.job_id))
    # Both jobs ran after the parent had moved between activations, each on its own copy;
    # the first job's local path ships back as the stable input token.
    assert output["first"] == {"files": ["INCAR", "sub/OUTCAR"], "dir": f"{jobflow_runner.INPUT_TOKEN}prev_dir/calc"}
    assert output["same"] is True


def test_jobflow_directory_inputs_are_checked_at_job_creation(tmp_path: Path) -> None:
    package = _jobflow_package(
        tmp_path / "directory-input-package",
        _DIRECTORY_FLOW,
        inputs='[workflow.inputs.prev_dir]\ndirectory = true\n',
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    a_file = tmp_path / "INCAR"
    a_file.write_text("ENCUT = 500\n", encoding="utf-8")
    with pytest.raises(ValueError, match="declared a directory"):
        new_job(workspace, package, inputs={"prev_dir": str(a_file)})
    plain = _jobflow_package(tmp_path / "plain-package", _DIRECTORY_FLOW, inputs="[workflow.inputs.prev_dir]\n")
    with pytest.raises(ValueError, match="declare the input with directory = true"):
        new_job(workspace, plain, inputs={"prev_dir": str(tmp_path)})


def test_directory_inputs_are_refused_outside_jobflow(tmp_path: Path) -> None:
    package = tmp_path / "plain"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(
        '[workflow]\nname = "tests.plain"\n\n[workflow.runner]\nsteps = ["start"]\n\n[workflow.inputs.prev_dir]\ndirectory = true\n',
        encoding="utf-8",
    )
    run = package / "run"
    run.write_text("#!/bin/sh\n", encoding="utf-8")
    run.chmod(0o755)
    with pytest.raises(ValueError, match=r"directory is only supported by \[workflow.runner\] format"):
        resolve_workflow(package)


@NEEDS_JOBFLOW
def test_jobflow_httk_structure_input_reaches_make_as_pymatgen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pymatgen")
    atomistic = pytest.importorskip("httk.atomistic")
    import httk.core

    poscar = tmp_path / "POSCAR"
    poscar.write_text("Si\n1.0\n5.43 0 0\n0 5.43 0\n0 0 5.43\nSi\n2\nDirect\n0 0 0\n0.25 0.25 0.25\n", encoding="utf-8")
    structure = atomistic.UnitcellStructureView(httk.core.load(str(poscar), format="vasp-poscar", precision=1e-4))
    package = _jobflow_package(
        tmp_path / "httk-structure-package",
        """from jobflow import job

@job
def formula(structure):
    return [type(structure).__name__, structure.composition.formula]

class Maker:
    def make(self, structure):
        return formula(structure)
""",
        inputs='[workflow.inputs.structure]\nentry_type = "structures"\n',
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"structure": structure})
    _drive(workspace)
    assert _collected_value(workspace, job.job_id) == ["Structure", "Si2"]


@NEEDS_JOBFLOW
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
    prepared = jobflow._prepare(
        _request(
            tmp_path,
            runner_options={"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"},
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
    root = tmp_path / "pkg"
    root.mkdir()
    prepared = jobflow._prepare(
        _request(
            root,
            runner_options={"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"},
            inputs={"structure": {}, "label": {}},
        )
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
        provenance={},
        runner_steps=None,
        children={},
        declarations={},
    )
    assert set(jobflow.collect(record).keys()) == {"role"}


@NEEDS_JOBFLOW
def test_jobflow_job_parameters_reach_children(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("HTTK_WORKFLOW_LAUNCH", "HTTK_VASP_COMMAND", "ATOMATE2_VASP_CMD"):
        monkeypatch.delenv(name, raising=False)
    package = _jobflow_package(
        tmp_path / "parameters-package",
        """import os
from jobflow import job

@job
def report():
    return os.environ["ATOMATE2_VASP_CMD"]

class Maker:
    def make(self):
        return report()
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("vasp.command", "vasp_workspace")
    job = new_job(workspace, package, parameters={"vasp.command": "x"})
    _drive(workspace)
    assert find(workspace, job.job_id).state == "succeeded"
    assert _vasp_argv(_collected_value(workspace, job.job_id), "x")


@NEEDS_JOBFLOW
def test_jobflow_stop_signal_reaches_a_new_session_descendant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    psutil = pytest.importorskip("psutil")
    marker = tmp_path / "pids.json"
    monkeypatch.setenv("JOBFLOW_SIGNAL_MARKER", str(marker))
    package = _jobflow_package(
        tmp_path / "signal-package",
        """import json
import os
import subprocess
import time
from jobflow import job

@job
def hold():
    sleeper = subprocess.Popen(["sleep", "600"], start_new_session=True)
    marker = os.environ["JOBFLOW_SIGNAL_MARKER"]
    # A group whose leader exits on SIGTERM while its member ignores it.
    leader = subprocess.Popen(
        ["sh", "-c", '(trap "" TERM; exec sleep 601) & echo $! > "$0.member"; wait', marker],
        start_new_session=True,
    )
    while not os.path.exists(marker + ".member") or not open(marker + ".member").read().strip():
        time.sleep(0.01)
    member = int(open(marker + ".member").read())
    with open(marker + ".tmp", "w", encoding="utf-8") as stream:
        json.dump({"runner": os.getpid(), "sleeper": sleeper.pid, "leader": leader.pid, "member": member}, stream)
    os.replace(marker + ".tmp", marker)
    time.sleep(600)

class Maker:
    def make(self):
        return hold()
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    new_job(workspace, package)
    with TaskManager(workspace, heartbeat_interval=0.01, maximum_workers=2, cancel_grace_seconds=1.0) as manager:
        deadline = time.monotonic() + 120
        while not marker.exists():
            assert time.monotonic() < deadline, "the jobflow job did not start"
            assert not any((workspace.root / "jobs" / "failed").iterdir()), "the jobflow job failed"
            manager.tick()
            time.sleep(0.05)
        pids = json.loads(marker.read_text(encoding="utf-8"))
        processes = [psutil.Process(pids["sleeper"]), psutil.Process(pids["member"])]
        os.kill(pids["runner"], signal.SIGTERM)
        deadline = time.monotonic() + 30

        def alive(process: Any) -> bool:
            with contextlib.suppress(psutil.NoSuchProcess):
                return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
            return False

        while any(alive(process) for process in processes):
            if time.monotonic() > deadline:
                for group in (pids["sleeper"], pids["leader"]):
                    with contextlib.suppress(OSError):
                        os.killpg(group, signal.SIGKILL)
                pytest.fail("a new-session descendant survived the runner's SIGTERM")
            manager.tick()
            time.sleep(0.05)


@NEEDS_JOBFLOW
def test_jobflow_vasp_variant_dispatch_runs_the_ncl_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    if not jobflow_runner._dispatcher_installed():
        pytest.skip("needs httk-workflow-vasp's httk.codes.vasp.launch")
    for name in ("HTTK_WORKFLOW_LAUNCH", "HTTK_VASP_COMMAND", "HTTK_VASP_NCL_COMMAND", "HTTK_VASP_GAMMA_COMMAND"):
        monkeypatch.delenv(name, raising=False)
    programs = {}
    for variant in ("standard", "ncl"):
        script = tmp_path / f"vasp_{variant}.py"
        script.write_text(f"open('{variant}.marker', 'w').close()\n", encoding="utf-8")
        programs[variant] = shlex.join([sys.executable, str(script)])
    package = _jobflow_package(
        tmp_path / "dispatch-package",
        """import os
import shlex
import subprocess
from jobflow import job

@job
def run_vasp():
    with open("INCAR", "w") as stream:
        stream.write("LSORBIT = .TRUE.\\n")
    subprocess.run(shlex.split(os.environ["ATOMATE2_VASP_CMD"]), check=True)
    return sorted(name for name in os.listdir(".") if name.endswith(".marker"))

class Maker:
    def make(self):
        return run_vasp()
""",
    )
    _set_pythonpath(monkeypatch, package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.configure_settings({"vasp.command": programs["standard"], "vasp.ncl_command": programs["ncl"]})
    job = new_job(workspace, package)
    _drive(workspace)
    assert _collected_value(workspace, job.job_id) == ["ncl.marker"]


def test_directory_input_copy_survives_the_parent_being_renamed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import _fs

    payload = tmp_path / "parent"
    calculation = payload / "files" / "inputs" / "prev" / "calc"
    (calculation / "sub").mkdir(parents=True)
    for name in ("INCAR", "OUTCAR", "sub/CHGCAR"):
        (calculation / name).write_text(name, encoding="utf-8")
    anchor = _fs.open_dir(payload)
    real_copy = _fs._copy_file

    def copy_while_moving(*args: Any, **kwargs: Any) -> None:
        # The parent is re-claimed (renamed) in the middle of the copy.
        if payload.exists():
            payload.rename(tmp_path / "moved")
        real_copy(*args, **kwargs)

    monkeypatch.setattr(_fs, "_copy_file", copy_while_moving)
    root = tmp_path / "child" / "inputs"
    try:
        jobflow_runner._stage_directory_inputs(anchor, {"prev/calc"}, root)
        with pytest.raises(ValueError, match="'prev/other' is not in the parent job"):
            jobflow_runner._stage_directory_inputs(anchor, {"prev/other"}, root)
    finally:
        os.close(anchor)
    copied = root / "prev" / "calc"
    assert not payload.exists()
    assert sorted(path.relative_to(copied).as_posix() for path in copied.rglob("*") if path.is_file()) == [
        "INCAR",
        "OUTCAR",
        "sub/CHGCAR",
    ]


def _spooled_child(tmp_path: Path, payload: Path, relocate: Any) -> tuple[SimpleNamespace, list[tuple[str, str]]]:
    """Return an ``enter`` stub whose parent ``payload`` holds a spooled job and its store snapshot."""
    from jobflow.core.job import store_inputs

    from httk.workflow.compat.jobflow._driver import spool_job

    spool = payload / "run" / "jobflow" / "spool"
    spool.mkdir(parents=True)
    (spool / "key.json").write_text(json.dumps(spool_job(store_inputs(5))), encoding="utf-8")
    (spool / "key-store.json").write_text("[]", encoding="utf-8")
    events: list[tuple[str, str]] = []
    stub = SimpleNamespace(
        context=SimpleNamespace(durable=False, settings={}, job_id="00000000-0000-4000-8000-000000000003"),
        parameters={},
        setting=lambda name, default=None: default,
        parameter=lambda name, default=None: {
            "jobflow_child_job": "jobflow/spool/key.json",
            "jobflow_child_snapshot": "jobflow/spool/key-store.json",
        }[name],
        parent=_parent_at(payload),
        _locate_parent=relocate,
        workdir=tmp_path / "child",
        fail=lambda code, message: events.append((code, message)),
        succeed=lambda: events.append(("succeeded", "")),
        log=SimpleNamespace(append=lambda kind, message: events.append((kind, message))),
    )
    return stub, events


def _parent_at(payload: Path) -> SimpleNamespace:
    return SimpleNamespace(workdir=payload / "run", payload=payload, placement=PurePosixPath("."))


@NEEDS_JOBFLOW
def test_enter_relocates_a_parent_renamed_before_it_is_anchored(tmp_path: Path) -> None:
    ready, owned = tmp_path / "ready" / "parent", tmp_path / "owned" / "parent"
    located: list[Path] = []

    def relocate(attempts: int) -> SimpleNamespace:
        located.append(owned)
        return _parent_at(owned)

    stub, events = _spooled_child(tmp_path, ready, relocate)
    # Re-claimed between the cached lookup and the open.
    owned.parent.mkdir()
    ready.rename(owned)
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [("succeeded", "")]
    assert located == [owned]


@NEEDS_JOBFLOW
def test_enter_reads_through_the_anchor_after_the_parent_is_renamed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import _fs

    ready, owned = tmp_path / "ready" / "parent", tmp_path / "owned" / "parent"

    def relocate(attempts: int) -> None:
        raise AssertionError("an anchored parent is never located again")

    stub, events = _spooled_child(tmp_path, ready, relocate)
    owned.parent.mkdir()
    real_open_dir = _fs.open_dir

    def open_then_rename(path: Path) -> int:
        descriptor = real_open_dir(path)
        # Re-claimed right after the anchor is open, before the spool is read.
        ready.rename(owned)
        return descriptor

    monkeypatch.setattr(_fs, "open_dir", open_then_rename)
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [("succeeded", "")]
    assert owned.is_dir() and not ready.exists()


@NEEDS_JOBFLOW
def test_enter_fails_a_parent_that_vanishes_every_time_it_is_located(tmp_path: Path) -> None:
    gone = tmp_path / "ready" / "parent"
    located: list[Path] = []

    def relocate(attempts: int) -> SimpleNamespace:
        located.append(gone)
        return _parent_at(gone)

    stub, events = _spooled_child(tmp_path, tmp_path / "staged", relocate)
    stub.parent = _parent_at(gone)
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [
        (
            "jobflow.job_unloadable",
            f"the parent job directory moved away each of {jobflow_runner._PARENT_ATTEMPTS} times it was located",
        )
    ]
    assert len(located) == jobflow_runner._PARENT_ATTEMPTS - 1
    # A spool file missing from an anchored parent stays unloadable, without relocating.
    stub.parent, stub.parameter = _parent_at(tmp_path / "staged"), lambda name, default=None: "jobflow/none.json"
    events.clear()
    jobflow_runner.enter(cast(Attempt, stub))
    assert events == [("jobflow.job_unloadable", "run/jobflow/none.json is not in the parent job")]
    assert len(located) == jobflow_runner._PARENT_ATTEMPTS - 1


@NEEDS_JOBFLOW
def test_enter_relocates_a_parent_renamed_before_its_job_json_is_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from jobflow.core.job import store_inputs

    from httk.workflow import sdk
    from httk.workflow.compat.jobflow._driver import spool_job

    fabricated = fabricate(
        tmp_path,
        step="enter",
        parent=True,
        parameters={
            "jobflow_child_job": "jobflow/spool/key.json",
            "jobflow_child_snapshot": "jobflow/spool/key-store.json",
        },
    )
    (parent,) = every_job(fabricated.workspace)
    spool = parent.path / "run" / "jobflow" / "spool"
    spool.mkdir(parents=True)
    (spool / "key.json").write_text(json.dumps(spool_job(store_inputs(5))), encoding="utf-8")
    (spool / "key-store.json").write_text("[]", encoding="utf-8")
    waiting = parent.path.relative_to(fabricated.workspace.root / "jobs" / "waiting")
    ready = fabricated.workspace.root / "jobs" / "ready" / waiting
    real_from_path = sdk.JobDefinition.from_path
    reads: list[Path] = []

    def read_after_a_move(path: Path) -> Any:
        if path == fabricated.payload / "job.json":
            return real_from_path(path)
        reads.append(path)
        # Re-claimed between the listing that found it and the read of its job.json.
        if parent.path.exists():
            ready.parent.mkdir(parents=True)
            parent.path.rename(ready)
        return real_from_path(path)

    monkeypatch.setattr(sdk.JobDefinition, "from_path", read_after_a_move)
    jobflow_runner.enter(Attempt.initialize(fabricated.environment))
    outcome = json.loads((fabricated.control / "outcome.ready" / "outcome.json").read_text(encoding="utf-8"))
    assert outcome["action"] == "succeed", outcome
    assert reads == [parent.path / "job.json", ready / "job.json"]


def test_directory_tokens_never_name_the_inputs_root_or_climb(tmp_path: Path) -> None:
    root = str(tmp_path / "inputs")
    directories = {root: jobflow_runner.INPUT_TOKEN.removesuffix("/")}
    assert jobflow_runner._tokenize_exec(root, directories) == root
    assert jobflow_runner._tokenize_exec(f"{root}/prev/calc", directories) == f"{jobflow_runner.INPUT_TOKEN}prev/calc"
    copied = {"prev/calc"}
    token = f"{jobflow_runner.INPUT_TOKEN}prev/calc/INCAR"
    assert jobflow_runner._resolve_exec(token, {}, root, copied) == f"{root}/prev/calc/INCAR"
    # Not copied for this job: the placeholder stays.
    assert jobflow_runner._resolve_exec(f"{jobflow_runner.INPUT_TOKEN}other/x", {}, root, copied).startswith("httk-")
    for climbing in (f"{jobflow_runner.INPUT_TOKEN}prev/calc/../../x", f"{jobflow_runner.EXEC_TOKEN}id/../x"):
        with pytest.raises(ValueError, match="climbs out"):
            jobflow_runner._resolve_exec(climbing, {"id": "/exec"}, root, copied)


def test_directory_inputs_follow_links_and_refuse_dangling_ones(tmp_path: Path) -> None:
    package = _jobflow_package(
        tmp_path / "directory-input-package",
        _DIRECTORY_FLOW,
        inputs="[workflow.inputs.prev_dir]\ndirectory = true\n",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "POTCAR"
    outside.write_text("potcar\n", encoding="utf-8")
    calculation = tmp_path / "calc"
    calculation.mkdir()
    (calculation / "POTCAR").symlink_to(outside)
    job = new_job(workspace, package, inputs={"prev_dir": str(calculation)})
    staged = find(workspace, job.job_id).path / "files" / "inputs" / "prev_dir" / "calc" / "POTCAR"
    assert staged.is_file() and not staged.is_symlink() and staged.read_text(encoding="utf-8") == "potcar\n"
    (calculation / "WAVECAR").symlink_to(tmp_path / "missing")
    with pytest.raises(ValueError, match="cannot stage directory"):
        new_job(workspace, package, inputs={"prev_dir": str(calculation)})
    with pytest.raises(ValueError, match="relative paths resolve against"):
        new_job(workspace, package, inputs={"prev_dir": "no-such-calc"})


def test_jobflow_httk_structure_with_site_moments_is_refused(tmp_path: Path) -> None:
    atomistic = pytest.importorskip("httk.atomistic")
    structure = atomistic.UnitcellStructure(
        [[3, 0, 0], [0, 3, 0], [0, 0, 3]],
        [[0, 0, 0]],
        species_at_sites=["Fe"],
        site_moments=atomistic.CollinearSiteMoments([2]),
    )
    package = _jobflow_package(tmp_path / "moments-package", "", inputs='[workflow.inputs.structure]\n')
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match="site moments.*magmom"):
        new_job(workspace, package, inputs={"structure": structure})


def test_child_resources_warn_when_a_table_exceeds_the_flow_maxtime() -> None:
    warnings: list[str] = []
    attempt = SimpleNamespace(
        parameter=lambda name, default=None: [["relax*", {"maxtime": 7200, "procs": 4}]],
        context=SimpleNamespace(resources={"maxtime": 3600}),
        job=SimpleNamespace(resources={"procs": 1, "maxtime": 3600}),
        log=SimpleNamespace(append=lambda kind, message: warnings.append(message)),
    )
    job = SimpleNamespace(name="relax 1", config=SimpleNamespace(manager_config={}))
    resources = jobflow_runner._child_resources(cast(Attempt, attempt), job)
    assert resources == {"procs": 4, "maxtime": "02:00:00"}
    assert warnings == [
        "jobflow job 'relax 1' asks for maxtime 02:00:00, but this flow's attempt has 01:00:00; the job gets 01:00:00"
    ]


def test_jobflow_caller_parameters_are_narrow(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    package = _jobflow_package(
        tmp_path / "declared-package",
        _OVERRIDE_FLOW,
        parameters='[workflow.parameters.offset]\ntype = "integer"\n',
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    with caplog.at_level("WARNING"):
        new_job(workspace, package, parameters={"jobflow_override": []})
    assert any("'jobflow_override' is not declared" in record.getMessage() for record in caplog.records)
    with pytest.raises(ValueError, match="reserved language parameter 'jobflow_job_resources'"):
        new_job(workspace, package, parameters={"jobflow_job_resources": [["*", {"procs": 64}]]})
    with pytest.raises(ValueError, match=r"jobflow_overrides\[0\] must be an object whose kind"):
        new_job(workspace, package, parameters={"jobflow_overrides": [{"kind": []}]})


def test_dispatcher_detection_does_not_import_the_vasp_package() -> None:
    probe = (
        "import sys; from httk.workflow.compat.jobflow import jobflow_runner as r; "
        "print(r._dispatcher_installed(), 'httk.codes.vasp' in sys.modules)"
    )
    import subprocess

    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)
    installed, imported = result.stdout.split()
    if installed != "True":
        pytest.skip("needs httk-workflow-vasp's httk.codes.vasp.launch")
    assert imported == "False"
