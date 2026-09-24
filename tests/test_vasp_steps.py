"""The Python VASP step API, driven end to end through the real manager.

The three fixture runners in ``workflow_fixtures`` (relax, static, and
relax-static) are nothing but step declarations over the
:mod:`httk.workflow.vasp` step API, mirroring the thin workflows-vasp runners. A fake ``vasp`` writes plausible OSZICAR, OUTCAR,
CONTCAR, and ``vasprun.xml`` files, and variants of it fail with diagnosable
errors, which is what exercises the remedy ladder. Every test submits a real job
to a real workspace and lets a real :class:`httk.workflow.TaskManager` run it.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload

FIXTURES = Path(__file__).with_name("workflow_fixtures")

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
energy = "-.11500000E+02" if re.search(r"^NSW\\s*=\\s*0\\s*$", Path("INCAR").read_text(), re.MULTILINE) else "-.10500000E+02"
Path("OSZICAR").write_text(
    "       N       E                     dE             d eps       ncg     rms\\n"
    "DAV:   1    -0.100000000000E+02   -0.10000E+02   -0.30000E+01   128   0.500E+01\\n"
    "   1 F= " + energy + " E0= " + energy + "  d E =-.105000E+02\\n"
)
relaxed = list(structure)
relaxed[0] = "relaxed by the fake vasp"
relaxed[-1] = "0.5100000000 0.5100000000 0.5100000000"
Path("CONTCAR").write_text({contcar})
Path("vasprun.xml").write_text(
    '<modeling><structure name="finalpos"><crystal>'
    '<i name="volume">      8.00000000 </i></crystal></structure></modeling>\\n'
)
'''

_COLLECTED = ("INCAR", "KPOINTS", "OUTCAR", "CONTCAR", "OSZICAR", "vasprun.xml", "vasp-run-report.json")


def _fake_vasp(root: Path, *, fail_once: bool = False, empty_contcar: bool = False) -> Path:
    """Install the fake VASP executable and return its path."""

    path = root / "fake-vasp"
    contcar = '""' if empty_contcar else '"\\n".join(relaxed) + "\\n"'
    path.write_text(_FAKE_VASP.format(fail_once=fail_once, contcar=contcar), encoding="utf-8")
    path.chmod(0o755)
    return path


def _campaign(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    runner: str = "vasp_relax.py",
    executable: Path | None = None,
    command: str | None = None,
    parameters: dict[str, object] | None = None,
    files: tuple[str, ...] = ("POSCAR", "INCAR", "POTCAR"),
    data_mode: str = "transactional",
    **fake: bool,
) -> tuple[Workspace, Path, dict[str, Any]]:
    """Submit and run one job of one fixture runner; return its workspace, payload and terminal state."""

    root.mkdir(parents=True, exist_ok=True)
    if executable is None:
        executable = _fake_vasp(root, **fake)
    monkeypatch.setenv("HTTK_VASP_COMMAND", str(executable) if command is None else command)
    workspace = Workspace.initialize(root / "workspace")
    reference = workspace.publish_runner(FIXTURES / runner, name=runner)
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    for name, content in (("POSCAR", _POSCAR), ("INCAR", _INCAR), ("POTCAR", _POTCAR)):
        if name in files:
            (payload / "files" / name).write_text(content, encoding="utf-8")
    job = prepare_job_payload(
        payload,
        JobSpec(
            name=f"vasp steps {runner}",
            workflow="tests." + runner.removesuffix(".py").replace("_", "-"),
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="calculation",
            initial_step="prepare",
            data_mode="transactional" if data_mode == "transactional" else "none",
            maximum_total_attempts=8,
            parameters=parameters or {},
        ),
    )
    workspace.submit(payload, "project/vasp")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)
    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None, "the job vanished from the workspace"
    state = dict(workspace.read_state(marker))
    state["kind"] = marker.kind
    return workspace, workspace.payload_path(marker.placement, marker.job_key), state


def _job_state(payload: Path) -> dict[str, Any]:
    path = payload / ".httk-job" / "state.json"
    return {} if not path.is_file() else json.loads(path.read_text(encoding="utf-8"))


def _notes(payload: Path) -> list[str]:
    lines = (payload / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
    return [event["message"] for event in map(json.loads, lines) if event.get("kind") == "note"]


def _files(root: Path) -> list[str]:
    """Every regular file below *root*, excluding runner-private bookkeeping."""

    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and ".httk-runner" not in path.parts
    )


@pytest.mark.parametrize("data_mode", ("transactional", "none"))
def test_a_clean_relaxation_prepares_runs_and_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data_mode: str
) -> None:
    _, payload, terminal = _campaign(tmp_path, monkeypatch, parameters={"incar_tags": {"ISYM": 0}}, data_mode=data_mode)

    assert terminal["kind"] == "succeeded"
    workdir = payload / "run"
    # The inherited ``ISPIN = 2 ; ISYM = 2`` line kept its ISPIN and lost its ISYM.
    incar = (workdir / "INCAR").read_text(encoding="utf-8")
    assert incar.count("ISYM") == 1
    assert "ISYM = 0" in incar and "ISPIN = 2" in incar and "ENCUT = 300" in incar
    for tag in ("EDIFF", "EDIFFG", "MAGMOM", "NBANDS"):
        assert f"{tag} = " in incar
    assert (workdir / "KPOINTS").read_text(encoding="utf-8").splitlines()[2] == "Monkhorst-Pack"

    state = _job_state(payload)
    assert state["classification"] == "completed"
    assert float(str(state["energy"])) == pytest.approx(-10.5)
    assert "remedies" not in state
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "1"
    notes = _notes(payload)
    assert "prepared a tests.vasp-relax calculation" in notes
    assert "VASP completed" in notes

    if data_mode == "transactional":
        assert _files(payload / "data") == [f"vasp/{name}" for name in sorted(_COLLECTED)]
        assert (payload / "data" / "vasp" / "CONTCAR").read_text(encoding="utf-8").splitlines()[-1].startswith("0.51")
        assert f"published to data/vasp: {', '.join(_COLLECTED)}" in notes
    else:
        # Without transactional data the persistent workdir is the result.
        assert not (payload / "data").exists()
        assert set(_COLLECTED) <= set(_files(workdir))
        assert f"kept in the workdir: {', '.join(_COLLECTED)}" in notes


def test_a_diagnosed_failure_is_remedied_and_the_rerun_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, payload, terminal = _campaign(tmp_path, monkeypatch, fail_once=True, parameters={"rattle_amplitude": 0.01})

    assert terminal["kind"] == "succeeded"
    workdir = payload / "run"
    state = _job_state(payload)
    # One remedy was applied, the first rung of the reviewed zpotrf ladder: the
    # lattice was scaled by five percent, and the rattle moved the atoms.
    assert state["remedies"] == 1
    assert state["classification"] == "completed"
    poscar = (workdir / "POSCAR").read_text(encoding="utf-8").splitlines()
    assert poscar[1] == "1.05"
    assert poscar[-1] != _POSCAR.splitlines()[-1]
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "2"
    assert "applied a remedy for zpotrf" in _notes(payload)

    history = json.loads((payload / ".httk-job" / "vasp-remedies.json").read_text(encoding="utf-8"))
    assert history["attempts"] == {"zpotrf": 1}
    assert history["events"][0]["files"][0]["path"] == "POSCAR"


@pytest.mark.parametrize(
    "recovery",
    (
        pytest.param("decomposition", marks=pytest.mark.extended),
        pytest.param("bands", marks=pytest.mark.extended),
        "never",
    ),
)
def test_the_zhegv_ladder_retries_each_rung_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recovery: str
) -> None:
    # Simulate acceptance of the edited inputs, not VASP's eigensolver physics.
    executable = _fake_vasp(tmp_path)
    condition = (
        f"{recovery == 'never'!r} or tags.get('NPAR') != '1' or ({recovery == 'bands'!r} and int(tags['NBANDS']) < 8)"
    )
    source = (
        executable.read_text(encoding="utf-8")
        .replace(
            "if FAIL_ONCE and count == 0:",
            "from httk.workflow.vasp import read_incar\n"
            "tags = read_incar('INCAR')\n"
            "import json\n"
            "with Path('fake-inputs.jsonl').open('a') as stream:\n"
            "    stream.write(json.dumps(tags) + '\\n')\n"
            f"if {condition}:",
        )
        .replace(
            'print("LAPACK: Routine ZPOTRF failed! " + str(count))',
            'print("| EDDAV: Call to ZHEGV failed. Returncode = 42 2 64 |")\n'
            '    print("| I REFUSE TO CONTINUE WITH THIS SICK JOB ... BYE!!! |")',
        )
    )
    executable.write_text(source, encoding="utf-8")
    _, payload, terminal = _campaign(
        tmp_path,
        monkeypatch,
        executable=executable,
        parameters={"incar_tags": {"NPAR": 32, "NCORE": 1, "NBANDS": 6}},
    )

    assert terminal["kind"] == ("failed" if recovery == "never" else "succeeded")
    inputs = [json.loads(line) for line in (payload / "run" / "fake-inputs.jsonl").read_text().splitlines()]
    expected = [("32", "6"), ("1", "6")]
    if recovery != "decomposition":
        expected.append(("1", "8"))
    assert [(tags["NPAR"], tags["NBANDS"]) for tags in inputs] == expected
    history = json.loads((payload / ".httk-job" / "vasp-remedies.json").read_text())
    assert history["attempts"] == {"edddav_zhegv": len(expected) - 1}
    assert [event["step"] for event in history["events"]] == list(range(len(expected) - 1))
    if recovery == "never":
        failure = terminal["failure"]
        assert failure["code"] == "vasp.failed"
        assert failure["message"] == "VASP process_failure with no remaining remedy"
        assert failure["details"]["problem"] == "edddav_zhegv"
        assert failure["details"]["give_up"]
        assert failure["details"]["step"] == 2
        assert _job_state(payload)["remedies"] == 2


def test_an_exhausted_remedy_budget_fails_after_planning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, payload, terminal = _campaign(tmp_path, monkeypatch, fail_once=True, parameters={"maximum_remedies": 0})

    assert terminal["kind"] == "failed"
    failure = terminal["failure"]
    assert failure["code"] == "vasp.failed"
    assert failure["message"] == "VASP diagnosed_stop after 0 remedies"
    # The remedy it would have applied is still named.
    assert failure["details"]["problem"] == "zpotrf"
    assert _job_state(payload)["classification"] == "diagnosed_stop"


@pytest.mark.parametrize(
    ("case", "code"),
    (
        ("no command", "vasp.command_missing"),
        ("no structure", "vasp.input_missing"),
        ("bad parameter", None),
    ),
)
def test_input_problems_fail_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str, code: str | None
) -> None:
    cases: dict[str, dict[str, Any]] = {
        "no command": {"command": ""},
        "no structure": {"files": ("INCAR",)},
        "bad parameter": {"parameters": {"kpoint_density": "dense"}},
    }
    arguments = cases[case]
    _, _, terminal = _campaign(tmp_path, monkeypatch, **arguments)

    assert terminal["kind"] == "failed"
    if code == "vasp.command_missing":
        assert terminal["failure"]["code"] == code
        assert "HTTK_VASP_COMMAND" in terminal["failure"]["message"]
    elif code is not None:
        assert terminal["failure"]["code"] == code
        assert terminal["failure"]["details"]["expected"].endswith("files/POSCAR")
    else:
        # The runner died of the type check, and its crash breadcrumb says so.
        errors = [json.loads(path.read_text(encoding="utf-8")) for path in tmp_path.rglob("error.json")]
        assert errors
        assert {(error["exception"], error["message"]) for error in errors} == {
            ("ValueError", "job parameter 'kpoint_density' must be a number, not str")
        }


@pytest.mark.parametrize("case", ("plain", "prefixed", "empty contcar"))
def test_the_relax_static_chain_promotes_the_relaxed_structure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    # The prefixed case also overrides the static tags: the override replaces the
    # defaults wholesale, so NSW = 0 lands only because it is in the override.
    parameters: dict[str, object] = (
        {"data_prefix": "chain", "static_incar_tags": {"NSW": 0, "ISMEAR": -5}} if case == "prefixed" else {}
    )
    _, payload, terminal = _campaign(
        tmp_path,
        monkeypatch,
        runner="vasp_relax_static.py",
        parameters=parameters,
        empty_contcar=case == "empty contcar",
    )
    workdir = payload / "run"

    if case == "empty contcar":
        assert terminal["kind"] == "failed"
        assert terminal["failure"]["code"] == "vasp.no_relaxed_structure"
        return
    assert terminal["kind"] == "succeeded"
    # The relaxation was archived before the single point overwrote the workdir,
    # and the relaxed structure, under the reference comment line, became the
    # structure of the single point.
    assert (workdir / "relax" / "OUTCAR").is_file()
    poscar = (workdir / "POSCAR").read_text(encoding="utf-8").splitlines()
    assert poscar[0] == "silicon"
    assert poscar[-1].startswith("0.51")
    incar = (workdir / "INCAR").read_text(encoding="utf-8")
    assert "NSW = 0" in incar
    if case == "prefixed":
        assert "ISMEAR = -5" in incar
    else:
        assert "IBRION = -1" in incar

    state = _job_state(payload)
    assert state["relax_classification"] == "completed"
    assert float(str(state["relax_energy"])) == pytest.approx(-10.5)
    assert float(str(state["energy"])) == pytest.approx(-11.5)
    assert float(str(state["static_energy"])) == pytest.approx(-11.5)
    assert "promoted the relaxed structure to a static calculation" in _notes(payload)
    root = "chain/" if case == "prefixed" else ""
    assert _files(payload / "data") == sorted(
        [f"{root}relax/{name}" for name in _COLLECTED] + [f"{root}static/{name}" for name in _COLLECTED],
    )
    assert (workdir / "fake-vasp-attempts").read_text(encoding="utf-8") == "2"


@pytest.mark.parametrize(("prefix", "published"), ((None, "vasp"), ("single-point", "single-point")))
def test_the_static_stage_switches_off_the_ionic_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix: str | None, published: str
) -> None:
    parameters: dict[str, object] = {} if prefix is None else {"data_prefix": prefix}
    _, payload, terminal = _campaign(tmp_path, monkeypatch, runner="vasp_static.py", parameters=parameters)

    assert terminal["kind"] == "succeeded"
    incar = (payload / "run" / "INCAR").read_text(encoding="utf-8")
    assert "NSW = 0" in incar and "IBRION = -1" in incar
    assert _files(payload / "data") == [f"{published}/{name}" for name in sorted(_COLLECTED)]
