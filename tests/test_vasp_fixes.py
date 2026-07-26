"""The reviewed corrections to the VASP helpers, one test per correction.

Every test here pins a behavior a design review found wrong: an update that left a
duplicate tag behind, a rattle that repeated itself, three different k-point
defaults, a cleanup that deleted its own evidence, a remedy that could not be
applied, a history that lived in the wrong place, a POTCAR choice nobody recorded,
and a policy that could not be extended without editing the module.
"""

import json
from pathlib import Path

import pytest

from httk.workflow import (
    DEFAULT_KPOINT_CENTERING,
    VASP_RESTART_ARTIFACTS,
    Diagnostic,
    VaspPreparationOptions,
    VaspRemedyDecision,
    apply_vasp_remedy,
    assemble_potcar,
    clean_vasp_outputs,
    derive_seed,
    job_remedy_history_path,
    plan_vasp_remedy,
    prepare_vasp_inputs,
    rattle_poscar,
    read_incar,
    register_remedy_policy,
    remedy_policy,
    remedy_policy_names,
    update_incar,
    write_automatic_kpoints,
)
from httk.workflow.vasp import DEFAULT_REMEDY_HISTORY

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.00 0.00 0.00
0.50 0.50 0.50
"""


def _poscar(root: Path) -> Path:
    path = root / "POSCAR"
    path.write_text(_POSCAR, encoding="utf-8")
    return path


def _kpoints(root: Path, grid: str = "3 3 3") -> Path:
    path = root / "KPOINTS"
    path.write_text(f"mesh\n0\nMonkhorst-Pack\n{grid}\n0 0 0\n", encoding="utf-8")
    return path


def test_updating_one_tag_of_a_semicolon_line_leaves_no_second_assignment(tmp_path: Path) -> None:
    incar = tmp_path / "INCAR"
    incar.write_text("ISPIN = 2 ; ISYM = 2\nENCUT = 400 ; PREC = Accurate # inherited\n", encoding="utf-8")

    update_incar({"ISYM": 0}, incar)

    text = incar.read_text(encoding="utf-8")
    # The old assignment is gone, not shadowed: read_incar reads the last value of
    # every statement, so a surviving `ISYM = 2` would be a coin toss.
    assert text.count("ISYM") == 1
    assert read_incar(incar) == {"ISPIN": "2", "ENCUT": "400", "PREC": "Accurate", "ISYM": "0"}

    # What the line still assigns survives, and so does its comment.
    update_incar({"ENCUT": 520}, incar)
    assert "PREC = Accurate # inherited" in incar.read_text(encoding="utf-8")
    assert read_incar(incar)["ENCUT"] == "520"


def test_a_line_that_becomes_empty_disappears_and_untouched_lines_are_verbatim(tmp_path: Path) -> None:
    incar = tmp_path / "INCAR"
    incar.write_text("  ISYM=2\n\tLREAL = Auto\t\n# a bare comment\n", encoding="utf-8")

    update_incar({"ISYM": 0}, incar)

    lines = incar.read_text(encoding="utf-8").splitlines()
    assert lines == ["\tLREAL = Auto\t", "# a bare comment", "ISYM = 0"]


def test_a_rattle_needs_caller_supplied_entropy_and_two_attempts_differ(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="explicit seed or an entropy string"):
        rattle_poscar(_poscar(tmp_path))

    first = tmp_path / "first"
    second = tmp_path / "second"
    third = tmp_path / "third"
    for directory in (first, second, third):
        directory.mkdir()
        _poscar(directory)
    rattle_poscar(first / "POSCAR", entropy="job-key:1")
    rattle_poscar(second / "POSCAR", entropy="job-key:2")
    rattle_poscar(third / "POSCAR", entropy="job-key:1")

    # Reproducible from the same entropy, different from different entropy: two
    # retries of one job are two different structures, and a replayed attempt is
    # the same structure.
    assert (first / "POSCAR").read_text() == (third / "POSCAR").read_text()
    assert (first / "POSCAR").read_text() != (second / "POSCAR").read_text()
    assert (first / "POSCAR").read_text() != _POSCAR
    assert derive_seed("job-key:1") != derive_seed("job-key:2")


def test_every_entry_point_starts_from_the_same_kpoint_centering(tmp_path: Path) -> None:
    from httk.workflow._shell_bridge import _parser

    assert DEFAULT_KPOINT_CENTERING == "Monkhorst-Pack"
    assert VaspPreparationOptions().centering == DEFAULT_KPOINT_CENTERING
    written = write_automatic_kpoints((2, 2, 2), tmp_path / "KPOINTS")
    assert written.read_text(encoding="utf-8").splitlines()[2] == DEFAULT_KPOINT_CENTERING
    arguments = _parser().parse_args(["vasp-kpoints", "20"])
    assert arguments.centering == DEFAULT_KPOINT_CENTERING


def test_a_preclean_keeps_the_evidence_the_remedy_machinery_reads(tmp_path: Path) -> None:
    for name in ("OUTCAR", "OSZICAR", "CONTCAR", "vasp-run-report.json", "WAVECAR"):
        (tmp_path / name).write_text(name, encoding="utf-8")

    removed = clean_vasp_outputs(tmp_path, keep=("WAVECAR",))

    assert {path.name for path in removed} == {"OUTCAR", "OSZICAR"}
    for name in VASP_RESTART_ARTIFACTS:
        assert (tmp_path / name).is_file()

    # And they are removable, for the caller who really means it.
    removed = clean_vasp_outputs(tmp_path, keep=("WAVECAR",), also_remove=VASP_RESTART_ARTIFACTS)
    assert {path.name for path in removed} == set(VASP_RESTART_ARTIFACTS)
    assert (tmp_path / "WAVECAR").is_file()


def test_planning_skips_a_remedy_the_workdir_cannot_execute(tmp_path: Path) -> None:
    # A zpotrf ladder whose first two rungs need what this directory does not
    # have: no POSCAR scale to change and no KPOINTS to bump, but an NBANDS to
    # raise. The planned step is the one that can run.
    (tmp_path / "INCAR").write_text("NBANDS = 10\n", encoding="utf-8")
    history = tmp_path / "history.json"

    decision = plan_vasp_remedy(
        (Diagnostic("zpotrf", "fatal", "factorization failed", "stdout"),),
        directory=tmp_path,
        history_path=history,
    )

    assert decision.changes == (("bump_bands", 2),)
    assert decision.step == 2
    # Applying it advances the ladder past everything that was skipped.
    apply_vasp_remedy(decision, directory=tmp_path, history_path=history)
    assert json.loads(history.read_text(encoding="utf-8"))["attempts"]["zpotrf"] == 3
    assert "NBANDS = 12" in (tmp_path / "INCAR").read_text(encoding="utf-8")


def test_planning_gives_up_when_no_rung_is_executable(tmp_path: Path) -> None:
    # Nothing is staged at all, so every zpotrf rung is inapplicable.
    decision = plan_vasp_remedy(
        (Diagnostic("zpotrf", "fatal", "factorization failed", "stdout"),),
        directory=tmp_path,
        history_path=tmp_path / "history.json",
    )

    assert decision.give_up
    assert "scale_lattice needs a POSCAR" in decision.reason
    with pytest.raises(ValueError, match="cannot apply give-up decision"):
        apply_vasp_remedy(decision, directory=tmp_path)


def test_an_inapplicable_remedy_is_refused_before_anything_is_touched(tmp_path: Path) -> None:
    _poscar(tmp_path)
    (tmp_path / "INCAR").write_text("ENCUT = 400\n", encoding="utf-8")
    decision = VaspRemedyDecision("reviewed-v1", "too_few_bands", 0, (("bump_bands", 2),), False, "manual")

    with pytest.raises(ValueError, match="needs NBANDS in INCAR"):
        apply_vasp_remedy(decision, directory=tmp_path)

    # Refused, not half-applied: the inputs and the history are as they were.
    assert (tmp_path / "INCAR").read_text(encoding="utf-8") == "ENCUT = 400\n"
    assert not (tmp_path / DEFAULT_REMEDY_HISTORY).exists()


def test_a_planned_remedy_is_always_applicable_for_every_reviewed_problem(tmp_path: Path) -> None:
    # The contract item 5 states: plan proposes only what apply can execute. A
    # fully staged calculation therefore never has a planned remedy refused.
    _poscar(tmp_path)
    _kpoints(tmp_path)
    (tmp_path / "INCAR").write_text("NBANDS = 10\nEDIFF = 1e-5\n", encoding="utf-8")
    (tmp_path / "CONTCAR").write_text(_POSCAR.replace("silicon", "relaxed"), encoding="utf-8")
    policy = remedy_policy("reviewed-v1")
    history = tmp_path / "history.json"

    for problem in policy.sequences:
        for _ in policy.sequences[problem]:
            decision = plan_vasp_remedy(
                (Diagnostic(problem, "error", problem, "stdout"),),
                directory=tmp_path,
                history_path=history,
            )
            if decision.give_up:
                break
            apply_vasp_remedy(decision, directory=tmp_path, history_path=history)


def test_the_remedy_history_is_job_scoped_and_read_from_the_old_place(tmp_path: Path) -> None:
    payload = tmp_path / "payload"
    workdir = payload / "run"
    workdir.mkdir(parents=True)
    _poscar(workdir)
    _kpoints(workdir)
    history = job_remedy_history_path(payload)
    assert history == payload / ".httk-job" / "vasp-remedies.json"

    # An escalation recorded in the pre-0.2 workdir location is still honored, so
    # a job that was already climbing the ladder does not restart it.
    legacy = workdir / DEFAULT_REMEDY_HISTORY
    legacy.parent.mkdir(parents=True)
    legacy.write_text(json.dumps({"attempts": {"kpoints_class": 1}, "events": []}), encoding="utf-8")
    decision = plan_vasp_remedy(
        (Diagnostic("kpoints_class", "error", "mismatch", "stdout"),),
        directory=workdir,
        history_path=history,
    )
    assert decision.step == 1
    assert decision.changes == (("centering", "Gamma"),)

    apply_vasp_remedy(decision, directory=workdir, history_path=history)

    # From now on the ladder lives with the job state, outside every workdir.
    assert json.loads(history.read_text(encoding="utf-8"))["attempts"]["kpoints_class"] == 2
    assert (workdir / "KPOINTS").read_text(encoding="utf-8").splitlines()[2] == "Gamma"
    assert not (workdir / "vasp-remedies.json").exists()


def test_planning_and_applying_resolve_one_relative_history_against_the_workdir(tmp_path: Path) -> None:
    workdir = tmp_path / "run"
    workdir.mkdir()
    _poscar(workdir)
    _kpoints(workdir)
    diagnostics = (Diagnostic("kpoints_class", "error", "mismatch", "stdout"),)

    first = plan_vasp_remedy(diagnostics, directory=workdir)
    apply_vasp_remedy(first, directory=workdir)
    second = plan_vasp_remedy(diagnostics, directory=workdir)

    # One relative name, one file, one ladder — whatever the process working
    # directory happens to be.
    assert (workdir / DEFAULT_REMEDY_HISTORY).is_file()
    assert first.step == 0 and second.step == 1


def test_the_chosen_pseudopotential_variant_is_returned_and_recorded(tmp_path: Path) -> None:
    library = tmp_path / "potentials"
    (library / "Si_sv").mkdir(parents=True)
    (library / "Si").mkdir()
    (library / "Si_sv" / "POTCAR").write_text("TITEL  = PAW_PBE Si_sv 23Jan2001\nZVAL   =   12.000\n", encoding="utf-8")
    (library / "Si" / "POTCAR").write_text("TITEL  = PAW_PBE Si 05Jan2001\nZVAL   =    4.000\n", encoding="utf-8")
    _poscar(tmp_path)

    assembly = assemble_potcar(library, poscar=tmp_path / "POSCAR", output=tmp_path / "POTCAR")

    assert [item.variant for item in assembly.choices] == ["Si_sv"]
    choice = assembly.choices[0]
    assert choice.suffix == "_sv"
    assert choice.source == library / "Si_sv" / "POTCAR"
    assert choice.titel == "PAW_PBE Si_sv 23Jan2001"
    recorded = json.loads(assembly.provenance.read_text(encoding="utf-8"))
    assert assembly.provenance == tmp_path / "POTCAR.provenance.json"
    assert recorded["format"] == "httk-vasp-potcar-provenance"
    assert recorded["potentials"][0]["source"] == str(choice.source)
    assert recorded["potentials"][0]["sha256"] == choice.sha256

    # Preparation carries the same record, so a prepared calculation says which
    # potentials it was given.
    (tmp_path / "INCAR").write_text("", encoding="utf-8")
    prepared = prepare_vasp_inputs(
        VaspPreparationOptions(pseudopotential_library=library, incar_tags={"ENCUT": 520}),
        directory=tmp_path,
    )
    potcar = prepared["potcar"]
    assert isinstance(potcar, dict)
    assert potcar["potentials"][0]["variant"] == "Si_sv"
    assert read_incar(tmp_path / "INCAR")["ENCUT"] == "520"
    assert read_incar(tmp_path / "INCAR")["NBANDS"]


def test_explicit_incar_tags_win_over_every_derived_value(tmp_path: Path) -> None:
    _poscar(tmp_path)
    (tmp_path / "INCAR").write_text("", encoding="utf-8")

    prepare_vasp_inputs(
        VaspPreparationOptions(incar_tags={"EDIFF": 1e-9, "MAGMOM": "2*1"}),
        directory=tmp_path,
    )

    tags = read_incar(tmp_path / "INCAR")
    assert tags["EDIFF"] == "1e-09"
    assert tags["MAGMOM"] == "2*1"
    assert tags["EDIFFG"]


def test_a_group_registers_its_own_policy_without_editing_the_module(tmp_path: Path) -> None:
    _poscar(tmp_path)
    (tmp_path / "INCAR").write_text("ISYM = 2\n", encoding="utf-8")
    register_remedy_policy(
        "tests-group",
        {"zpotrf": ((("incar.ISYM", 0),), (("scale_lattice", 1.1),))},
        ("zpotrf", "dentet"),
        refusals={"dentet": "this group inspects a k-point precision problem by hand"},
        replace=True,
    )
    assert "tests-group" in remedy_policy_names()

    decision = plan_vasp_remedy(
        (Diagnostic("zpotrf", "fatal", "factorization failed", "stdout"),),
        directory=tmp_path,
        history_path=tmp_path / "history.json",
        policy="tests-group",
    )
    assert decision.policy == "tests-group"
    assert decision.changes == (("incar.ISYM", 0),)
    apply_vasp_remedy(decision, directory=tmp_path, history_path=tmp_path / "history.json")
    assert read_incar(tmp_path / "INCAR")["ISYM"] == "0"

    refused = plan_vasp_remedy(
        (Diagnostic("dentet", "warning", "precision", "stdout"),),
        directory=tmp_path,
        history_path=tmp_path / "history.json",
        policy="tests-group",
    )
    assert refused.give_up and "by hand" in refused.reason

    with pytest.raises(ValueError, match="registered policies: reviewed-v1, tests-group"):
        plan_vasp_remedy((), directory=tmp_path, policy="no-such-policy")


def test_an_unexecutable_policy_is_refused_at_registration() -> None:
    with pytest.raises(ValueError, match="unsupported remedy operation"):
        register_remedy_policy("tests-bad-operation", {"zpotrf": ((("teleport", 1),),)}, ("zpotrf",))
    with pytest.raises(ValueError, match="which its precedence omits"):
        register_remedy_policy("tests-bad-precedence", {"zpotrf": ((("bump_bands", 2),),)}, ("dentet",))
    with pytest.raises(ValueError, match="already registered"):
        register_remedy_policy("reviewed-v1", {}, ())
    assert "tests-bad-operation" not in remedy_policy_names()
