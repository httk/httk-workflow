"""The packaged VASP runners, driven end to end through the real manager.

A fake ``vasp`` writes plausible OSZICAR, OUTCAR, CONTCAR, and ``vasprun.xml``
files, and one variant of it fails once with a diagnosable ``ZPOTRF`` error before
succeeding, which is what exercises the remedy ladder. Nothing here simulates the
workflow protocol: every test submits a real job to a real workspace and lets a
real :class:`httk.workflow.TaskManager` run it, so what is tested is the runner as
a deployment would use it — resolved from the installed package or from a
published workspace runner, and in Bash as well as in Python.
"""

import json
import os
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.runners import RUNNERS, runner_path, runner_reference
from httk.workflow.vasp.runners import PACKAGE

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""
# The semicolon line is deliberate: preparation has to update ISYM without leaving
# the inherited assignment behind.
_INCAR = "ENCUT = 300\nISPIN = 2 ; ISYM = 2\n"
_POTCAR = "  TITEL  = PAW_PBE Si 05Jan2001\n   ZVAL   =    4.000    mass and valenz\n"

_FAKE_VASP = '''#!/usr/bin/env python3
"""A fake VASP that writes plausible outputs, optionally failing once first."""

import sys
import re
from pathlib import Path

FAIL_ONCE = {fail_once}

attempts = Path("fake-vasp-attempts")
count = int(attempts.read_text()) if attempts.is_file() else 0
attempts.write_text(str(count + 1))
structure = Path("POSCAR").read_text().splitlines()

if FAIL_ONCE and count == 0:
    print("LAPACK: Routine ZPOTRF failed! " + str(count))
    Path("OUTCAR").write_text(" fake vasp 6.4.1\\n   NELM   =     60\\n   NSW    =     99\\n")
    Path("OSZICAR").write_text("DAV:   1    -0.100000000000E+02\\n")
    raise SystemExit(1)

Path("OUTCAR").write_text(
    " fake vasp 6.4.1\\n"
    "   NELM   =     60;   NELMIN=  2; NELMDL= -5\\n"
    "   NSW    =     99    number of steps for IOM\\n"
    "   maximum number of plane-waves:    1234\\n"
    " General timing and accounting information for this job:\\n"
    "   FREE ENERGIE OF THE ION-ELECTRON SYSTEM (eV)\\n"
    "   free  energy   TOTEN  =       -10.50000000 eV\\n"
    "   energy  without entropy=      -10.50000000  energy(sigma->0) =      -10.50000000\\n"
)
Path("OSZICAR").write_text(
    "       N       E                     dE             d eps       ncg     rms\\n"
    "DAV:   1    -0.100000000000E+02   -0.10000E+02   -0.30000E+01   128   0.500E+01\\n"
    "DAV:   2    -0.105000000000E+02   -0.50000E+00   -0.10000E+00   128   0.100E+00\\n"
    "   1 F= -.10500000E+02 E0= -.10500000E+02  d E =-.105000E+02\\n"
)
relaxed = list(structure)
relaxed[0] = "relaxed by the fake vasp"
relaxed[-1] = "0.5100000000 0.5100000000 0.5100000000"
Path("CONTCAR").write_text("\\n".join(relaxed) + "\\n")
Path("vasprun.xml").write_text(
    '<modeling><structure name="finalpos"><crystal>'
    '<i name="volume">      8.00000000 </i></crystal></structure></modeling>\\n'
)
if re.search(r"^NSW\\s*=\\s*0\\s*$", Path("INCAR").read_text(), re.MULTILINE):
    outcar = Path("OUTCAR")
    outcar.write_text(outcar.read_text().replace("-10.50000000", "{static_energy:.8f}"))
'''

_COLLECTED = ("INCAR", "KPOINTS", "OUTCAR", "CONTCAR", "OSZICAR", "vasprun.xml", "vasp-run-report.json")


def _fake_vasp(root: Path, *, fail_once: bool, static_energy: float = -10.5) -> Path:
    """Install the fake VASP executable and return its path."""

    path = root / ("fail-once-vasp" if fail_once else "fake-vasp")
    path.write_text(_FAKE_VASP.format(fail_once=fail_once, static_energy=static_energy), encoding="utf-8")
    path.chmod(0o755)
    return path


def _reference(workspace: Workspace, runner: str, source: str) -> dict[str, object]:
    """Return the job runner reference for one packaged runner."""

    if source == "workspace":
        return dict(workspace.publish_runner(runner_path(runner), name=runner))
    return runner_reference(runner)


def _campaign(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    runner: str,
    *,
    source: str = "workspace",
    parameters: dict[str, object] | None = None,
    fail_once: bool = False,
    files: tuple[str, ...] = ("POSCAR", "INCAR", "POTCAR"),
    data_mode: str = "transactional",
    initial_step: str = "prepare",
    command: str | None = None,
    workspace_settings: dict[str, object] | None = None,
    set_command_environment: bool = True,
) -> tuple[Workspace, str]:
    """Submit and run one job of one packaged runner, and return where it landed."""

    root.mkdir(parents=True)
    executable = _fake_vasp(root, fail_once=fail_once)
    if set_command_environment:
        monkeypatch.setenv("HTTK_VASP_COMMAND", str(executable) if command is None else command)
    workspace = Workspace.initialize(root / "workspace")
    for key, value in (workspace_settings or {}).items():
        workspace.set_setting(key, value)
    reference = _reference(workspace, runner, source)
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    for name, content in (("POSCAR", _POSCAR), ("INCAR", _INCAR), ("POTCAR", _POTCAR)):
        if name in files:
            (payload / "files" / name).write_text(content, encoding="utf-8")
    job = prepare_job_payload(
        payload,
        JobSpec(
            name=f"packaged {runner}",
            workflow=_workflow_of(runner),
            runner_path=str(reference["path"]),
            runner_source="workspace" if source == "workspace" else "installed",
            runner_sha256=str(reference["sha256"]),
            tag="calculation",
            initial_step=initial_step,
            data_mode="transactional" if data_mode == "transactional" else "none",
            maximum_total_attempts=8,
            parameters=parameters or {},
        ),
    )
    workspace.submit(payload, "project/vasp")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    return workspace, job.id


def _workflow_of(runner: str) -> str:
    """Return the workflow name one packaged runner declares."""

    return {
        "vasp_relax.py": "httk.vasp.relax",
        "vasp_relax.sh": "httk.vasp.relax-bash",
        "vasp_static.py": "httk.vasp.static",
        "vasp_relax_static.py": "httk.vasp.relax-static",
    }[runner]


def _payload_of(workspace: Workspace, job_id: str) -> tuple[str, Path]:
    """Return the terminal marker kind and the payload directory of one job."""

    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None, "the job vanished from the workspace"
    return marker.kind, workspace.payload_path(marker.placement, marker.job_key)


def _failure(workspace: Workspace, job_id: str) -> dict[str, Any]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    failure = workspace.read_state(marker).get("failure")
    assert isinstance(failure, dict)
    return failure


def _job_state(payload: Path) -> dict[str, Any]:
    path = payload / ".httk-job" / "state.json"
    return {} if not path.is_file() else json.loads(path.read_text(encoding="utf-8"))


def _files(root: Path) -> list[str]:
    """Every regular file below *root*, excluding runner-private bookkeeping."""

    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and ".httk-runner" not in path.parts
    )


def test_the_packaged_relax_runner_prepares_runs_and_collects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, job_id = _campaign(
        tmp_path / "relax",
        monkeypatch,
        "vasp_relax.py",
        parameters={"incar_tags": {"ISYM": 0}},
    )

    kind, payload = _payload_of(workspace, job_id)
    assert kind == "succeeded"
    workdir = payload / "run"

    # Preparation derived what was missing and rewrote what the job overrode: the
    # inherited ``ISPIN = 2 ; ISYM = 2`` line kept its ISPIN and lost its ISYM, so
    # the new value is the only one in the file.
    incar = (workdir / "INCAR").read_text(encoding="utf-8")
    assert incar.count("ISYM") == 1
    assert "ISYM = 0" in incar
    assert "ISPIN = 2" in incar
    assert "ENCUT = 300" in incar
    for tag in ("EDIFF", "EDIFFG", "MAGMOM", "NBANDS"):
        assert f"{tag} = " in incar
    assert (workdir / "KPOINTS").read_text(encoding="utf-8").splitlines()[2] == "Monkhorst-Pack"

    # The run happened once, was classified, and its energy is job state.
    state = _job_state(payload)
    assert state["classification"] == "completed"
    assert float(str(state["energy"])) == pytest.approx(-10.5)
    assert "remedies" not in state
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "1"

    # And the finished calculation was published as transactional data.
    published = _files(payload / "data")
    assert published == [f"vasp/{name}" for name in sorted(_COLLECTED)]
    assert (payload / "data" / "vasp" / "CONTCAR").read_text(encoding="utf-8").splitlines()[-1].startswith("0.51")


def test_the_installed_package_form_resolves_the_packaged_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No publication at all: the job names the runner inside the installed package,
    # which the manager resolves within its own module allowlist.
    workspace, job_id = _campaign(tmp_path / "installed", monkeypatch, "vasp_relax.py", source="installed")

    kind, payload = _payload_of(workspace, job_id)
    assert kind == "succeeded"
    assert not (payload / "attempts").exists()
    assert json.loads((payload / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()[0])[
        "runner_path"
    ] == str(runner_path("vasp_relax.py"))
    assert json.loads((payload / "job.json").read_text(encoding="utf-8"))["runner"]["path"] == (
        f"pkg:{PACKAGE}/vasp_relax.py"
    )


def test_a_diagnosed_failure_is_remedied_and_the_rerun_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id = _campaign(tmp_path / "remedy", monkeypatch, "vasp_relax.py", fail_once=True)

    kind, payload = _payload_of(workspace, job_id)
    assert kind == "succeeded"
    workdir = payload / "run"
    state = _job_state(payload)

    # One remedy was applied, and it was the first rung of the reviewed zpotrf
    # ladder: the lattice was scaled by five percent.
    assert state["remedies"] == 1
    assert state["classification"] == "completed"
    assert (workdir / "POSCAR").read_text(encoding="utf-8").splitlines()[1] == "1.05"
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "2"

    # The ladder itself is job state, outside every workdir, so an isolated workdir
    # would find it too.
    history = json.loads((payload / ".httk-job" / "vasp-remedies.json").read_text(encoding="utf-8"))
    assert history["attempts"] == {"zpotrf": 1}
    assert history["events"][0]["problem"] == "zpotrf"
    assert history["events"][0]["files"][0]["path"] == "POSCAR"
    assert not (workdir / ".httk-vasp").exists()


@pytest.mark.parametrize("recovery", ("decomposition", "bands", "never"))
def test_zhegv_manager_retries_each_rung_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recovery: str
) -> None:
    # Simulate acceptance of the edited inputs, not VASP's eigensolver physics.
    executable = tmp_path / "zhegv-vasp"
    source = _FAKE_VASP.format(fail_once=False, static_energy=-10.5)
    condition = (
        f"{recovery == 'never'!r} or tags.get('NPAR') != '1' or ({recovery == 'bands'!r} and int(tags['NBANDS']) < 8)"
    )
    source = source.replace(
        "if FAIL_ONCE and count == 0:",
        "from httk.workflow.vasp import read_incar\n"
        "tags = read_incar('INCAR')\n"
        "import json\n"
        "with Path('fake-inputs.jsonl').open('a') as stream:\n"
        "    stream.write(json.dumps(tags) + '\\n')\n"
        f"if {condition}:",
    ).replace(
        'print("LAPACK: Routine ZPOTRF failed! " + str(count))',
        'print("| EDDAV: Call to ZHEGV failed. Returncode = 42 2 64 |")\n'
        '    print("| I REFUSE TO CONTINUE WITH THIS SICK JOB ... BYE!!! |")',
    )
    executable.write_text(source, encoding="utf-8")
    executable.chmod(0o755)
    workspace, job_id = _campaign(
        tmp_path / "zhegv",
        monkeypatch,
        "vasp_relax.py",
        command=str(executable),
        parameters={"incar_tags": {"NPAR": 32, "NCORE": 1, "NBANDS": 6}},
    )
    kind, payload = _payload_of(workspace, job_id)
    assert kind == ("failed" if recovery == "never" else "succeeded")
    inputs = [json.loads(line) for line in (payload / "run" / "fake-inputs.jsonl").read_text().splitlines()]
    expected = [("32", "6"), ("1", "6")]
    if recovery != "decomposition":
        expected.append(("1", "8"))
    assert [(tags["NPAR"], tags["NBANDS"]) for tags in inputs] == expected
    history = json.loads((payload / ".httk-job" / "vasp-remedies.json").read_text())
    assert history["attempts"] == {"edddav_zhegv": len(expected) - 1}
    assert [event["step"] for event in history["events"]] == list(range(len(expected) - 1))
    assert all(event["files"][0]["path"] == "INCAR" for event in history["events"])
    if recovery == "never":
        failure = _failure(workspace, job_id)
        assert failure["code"] == "vasp.failed"
        assert failure["details"]["problem"] == "edddav_zhegv"
        assert failure["details"]["give_up"]
        assert failure["details"]["step"] == 2


@pytest.mark.parametrize(
    ("label", "fail_once"),
    (
        ("plain", False),
        ("remedied", True),
    ),
)
def test_the_bash_runner_and_the_python_runner_publish_the_same_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, label: str, fail_once: bool
) -> None:
    """Parity: one workflow, two languages, the same job state and the same files."""

    observed: dict[str, dict[str, Any]] = {}
    for runner in ("vasp_relax.py", "vasp_relax.sh"):
        workspace, job_id = _campaign(
            tmp_path / f"{label}-{runner}",
            monkeypatch,
            runner,
            fail_once=fail_once,
            parameters={"kpoint_density": 15.0, "incar_tags": {"NELM": 40}},
        )
        kind, payload = _payload_of(workspace, job_id)
        state = _job_state(payload)
        state["energy"] = round(float(str(state["energy"])), 9)
        history = payload / ".httk-job" / "vasp-remedies.json"
        ladder = {} if not history.is_file() else json.loads(history.read_text(encoding="utf-8"))["attempts"]
        observed[runner] = {
            "kind": kind,
            "state": state,
            "ladder": ladder,
            "workdir": _files(payload / "run"),
            "data": _files(payload / "data"),
            "inputs": [(payload / "run" / name).read_text(encoding="utf-8") for name in ("INCAR", "KPOINTS", "POSCAR")],
            "outputs": [
                (payload / "data" / "vasp" / name).read_text(encoding="utf-8")
                for name in ("CONTCAR", "OUTCAR", "OSZICAR")
            ],
        }

    assert observed["vasp_relax.py"]["kind"] == "succeeded"
    if fail_once:
        assert observed["vasp_relax.py"]["state"]["remedies"] == 1
    assert "NELM = 40" in observed["vasp_relax.py"]["inputs"][0]
    assert observed["vasp_relax.py"] == observed["vasp_relax.sh"]


@pytest.mark.parametrize("workflow", ("vasp-relax", "vasp-relax-bash", "vasp-static", "vasp-relax-static"))
@pytest.mark.parametrize("data_mode", (None, "transactional"), ids=("default-none", "transactional-opt-in"))
def test_vasp_cli_runs_and_collects_default_workdir_or_transactional_results(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    workflow: str,
    data_mode: str | None,
) -> None:
    pytest.importorskip("httk.atomistic")
    pytest.importorskip("httk.store")
    from httk.atomistic import UnitcellStructureView
    from httk.atomistic.entries.structures import StructureEntry
    from httk.core import DataRecord
    from httk.core.cli import CLIContext
    from httk.core.storage import content_id
    from httk.store import Backend, SqlStore

    from conftest import register_ws
    from httk.workflow import collect
    from httk.workflow.models import JobDefinition
    from httk.workflow.postprocessing import run_postprocess_script
    from httk.workflow.scaffold import registered_workflow
    from httk.workflow.workflow_cli import command

    executable = _fake_vasp(tmp_path, fail_once=False, static_energy=-11.5)
    monkeypatch.setenv("HTTK_VASP_COMMAND", str(executable))
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root)
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")
    args = ["job", "new", "--workspace", name, "--workflow", workflow, "--input", f"structure={structure}"]
    if data_mode is not None:
        args += ["--data-mode", data_mode]
    assert command(args, context) == 0
    key, path = capsys.readouterr().out.strip().split("\t")
    payload = Path(path)
    definition = JobDefinition.from_path(payload / "job.json")
    assert definition.data_mode == (data_mode or "none")
    assert definition.workdir_mode == "persistent"
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)

    kind, actual_payload = _payload_of(workspace, definition.id)
    assert kind == "succeeded"
    assert actual_payload == payload
    assert (payload / "data").exists() == (data_mode == "transactional")
    assert set(_COLLECTED) <= set(_files(payload / "run"))
    assert _job_state(payload)["classification"] == "completed"
    if data_mode is None and workflow in ("vasp-relax", "vasp-relax-bash", "vasp-static"):
        # The result files occur exactly once in the whole payload.
        for filename in _COLLECTED:
            assert list(payload.rglob(filename)) == [payload / "run" / filename]
    if data_mode == "transactional":
        prefixes = ("relax", "static") if workflow == "vasp-relax-static" else ("vasp",)
        assert _files(payload / "data") == sorted(f"{prefix}/{file}" for prefix in prefixes for file in _COLLECTED)

    (item,) = collect(workspace, fail_fast=True)
    roles = {"total_energy"} if workflow == "vasp-static" else {"relaxed_structure", "total_energy"}
    energy = -11.5 if workflow in ("vasp-static", "vasp-relax-static") else -10.5
    assert set(item.outputs) == roles
    assert not item.unfulfilled
    collected_energy = item.outputs["total_energy"]
    assert isinstance(collected_energy, DataRecord)
    assert collected_energy.value == pytest.approx(energy)
    assert item.record.workdir == payload / "run"
    assert (item.record.data is None) == (data_mode is None)
    if "relaxed_structure" in roles:
        relaxed = item.outputs["relaxed_structure"]
        assert isinstance(relaxed, UnitcellStructureView)
        assert float(relaxed.sites.reduced_coords[1][0]) == pytest.approx(0.51)

    for into in (False, True):
        args = ["collect", "--workspace", name]
        if into:
            args += ["--into", str(tmp_path / "results.sqlite"), "--id-base", "httk.test", "--no-id-ledger"]
        assert command(args, context) == 0
        report, summary = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert report["job_key"] == key
        assert set(report["outputs"]) == roles
        assert report["unfulfilled"] == []
        assert report["missing_collector"] is None
        if into:
            assert len(report["stored"]["entries"]) == len(roles)
            assert report["stored"]["run"]
        assert summary["format"] == "httk-workflow-collect-summary"

    with Backend.sqlite(tmp_path / "results.sqlite") as database:
        store = SqlStore(database)
        # Storing rewrites product_of links to public structure IDs, which also
        # changes the energy's content ID. Query the stored records instead.
        searcher = store.searcher()
        variable = searcher.variable(DataRecord)
        energies = [row.energy for row in searcher.results(energy=variable)]
        assert len(energies) == 1
        assert energies[0].value == pytest.approx(energy)
        assert energies[0].id in report["stored"]["entries"]
        if "relaxed_structure" in roles:
            assert (
                store.fetch_entry(StructureEntry, content_id(item.outputs["relaxed_structure"]), eager=True) is not None
            )

    if "relaxed_structure" in roles:
        provider = registered_workflow(workflow)
        assert provider is not None
        for script in ("relaxation-report", "relaxation-plot"):
            result = run_postprocess_script(provider, script, item.record)
            assert result.returncode == 0, result.stderr
            if script == "relaxation-report":
                report = json.loads((result.output_dir / "relaxation_report.json").read_text())
                assert report["final_energy"] == pytest.approx(energy)
            else:
                assert f"{energy:.8f} eV" in (result.output_dir / "relaxation_energies.svg").read_text()


def test_the_bash_vasp_runner_uses_the_workspace_command_setting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HTTK_VASP_COMMAND", raising=False)
    workspace, job_id = _campaign(
        tmp_path / "settings-command",
        monkeypatch,
        "vasp_relax.sh",
        workspace_settings={"vasp.command": str(tmp_path / "settings-command" / "fake-vasp")},
        set_command_environment=False,
    )

    assert _payload_of(workspace, job_id)[0] == "succeeded"


def test_a_job_with_no_configured_vasp_command_fails_by_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, job_id = _campaign(tmp_path / "nocommand", monkeypatch, "vasp_relax.py", command="")

    kind, _ = _payload_of(workspace, job_id)
    assert kind == "failed"
    failure = _failure(workspace, job_id)
    assert failure["code"] == "vasp.command_missing"
    assert "HTTK_VASP_COMMAND" in failure["message"]


def test_a_job_whose_structure_is_not_in_its_payload_fails_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, job_id = _campaign(tmp_path / "nostructure", monkeypatch, "vasp_relax.py", files=("INCAR",))

    assert _payload_of(workspace, job_id)[0] == "failed"
    assert _failure(workspace, job_id)["code"] == "vasp.input_missing"


def test_the_static_runner_switches_off_the_ionic_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, job_id = _campaign(
        tmp_path / "static",
        monkeypatch,
        "vasp_static.py",
        parameters={"poscar": "files/POSCAR", "data_prefix": "single-point"},
    )

    kind, payload = _payload_of(workspace, job_id)
    assert kind == "succeeded"
    tags = (payload / "run" / "INCAR").read_text(encoding="utf-8")
    assert "NSW = 0" in tags and "IBRION = -1" in tags
    assert _files(payload / "data") == [f"single-point/{name}" for name in sorted(_COLLECTED)]


def test_the_chain_runner_relaxes_promotes_and_runs_statically(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, job_id = _campaign(tmp_path / "chain", monkeypatch, "vasp_relax_static.py")

    kind, payload = _payload_of(workspace, job_id)
    assert kind == "succeeded"
    workdir = payload / "run"

    # The relaxation was archived before the single point overwrote the workdir,
    # and the relaxed structure became the structure of the single point.
    assert (workdir / "relax" / "OUTCAR").is_file()
    poscar = (workdir / "POSCAR").read_text(encoding="utf-8").splitlines()
    assert poscar[0] == "silicon"
    assert poscar[-1].startswith("0.51")
    assert "NSW = 0" in (workdir / "INCAR").read_text(encoding="utf-8")

    state = _job_state(payload)
    assert state["relax_classification"] == "completed"
    assert float(str(state["relax_energy"])) == pytest.approx(-10.5)
    assert float(str(state["static_energy"])) == pytest.approx(-10.5)

    published = _files(payload / "data")
    assert published == sorted(
        [f"relax/{name}" for name in _COLLECTED] + [f"static/{name}" for name in _COLLECTED],
    )
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "2"


def test_every_packaged_runner_describes_itself_and_is_referenceable(tmp_path: Path) -> None:
    import subprocess
    import sys

    shell = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell"
    environment = dict(os.environ)
    environment.update(
        {
            "HTTK_WORKFLOW_DESCRIBE": "1",
            "HTTK_WORKFLOW_BASH_API": str(shell / "httk-workflow.sh"),
            "HTTK_WORKFLOW_VASP_BASH_API": str(shell / "httk-vasp.sh"),
        }
    )
    described: dict[str, str] = {}
    for name in RUNNERS:
        path = runner_path(name)
        command = ["bash", str(path)] if name.endswith(".sh") else [sys.executable, str(path)]
        completed = subprocess.run(command, cwd=tmp_path, env=environment, text=True, capture_output=True, check=False)
        assert completed.returncode == 0, completed.stderr
        described[name] = completed.stdout
        assert json.loads(completed.stdout)["workflow"] == _workflow_of(name)
        # Every packaged runner is referenceable both ways, pinned by its bytes.
        assert runner_reference(name)["path"] == f"pkg:{PACKAGE}/{name}"
        assert runner_reference(name, source="workspace")["path"] == name

    # The Bash relaxation runner and the Python one describe themselves with the
    # same bytes, which is what makes them one workflow rather than two.
    assert json.loads(described["vasp_relax.sh"])["steps"] == json.loads(described["vasp_relax.py"])["steps"]
    with pytest.raises(ValueError, match="unknown packaged runner"):
        runner_path("vasp_nonexistent.py")
