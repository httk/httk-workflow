#!/usr/bin/env python3
"""One single-point VASP calculation of a structure that is already chosen.

The workflow is the relaxation workflow with the ionic loop switched off:
``prepare`` stages the structure and the INCAR of the job payload and adds the
static tags — ``IBRION = -1`` and ``NSW = 0`` unless the job says otherwise —
``run`` executes VASP with the reviewed remedy ladder, and ``collect`` publishes
the result. The structure may be a POSCAR or the CONTCAR of an earlier
relaxation, staged as an input file of this job and named by the ``poscar`` input.

The job inputs are documented in :mod:`httk.workflow.vasp.runners`. Nothing here
imports anything but an installed *httk-workflow*, so this one file is the whole
runner: reference it as ``pkg:httk.workflow.vasp.runners/vasp_static.py``, publish it to
a workspace runner store, or copy it and edit it.
"""

import os
import shlex
import shutil
import sys
from collections.abc import Mapping
from dataclasses import replace

try:
    from httk.workflow import Attempt, Runner
    from httk.workflow.vasp import (
        VaspPreparationOptions,
        apply_vasp_remedy,
        clean_vasp_outputs,
        job_remedy_history_path,
        last_oszicar_energy,
        plan_vasp_remedy,
        prepare_vasp_inputs,
        rattle_poscar,
        run_vasp,
        validate_vasp_workdir,
    )
except ModuleNotFoundError:  # pragma: no cover - interpreter bootstrap
    # The manager launches this file directly, so the interpreter is whatever the
    # shebang found on PATH, which on a cluster is not necessarily the one httk is
    # installed in. HTTK_WORKFLOW_PYTHON is the interpreter the manager itself runs,
    # so re-exec under it once and let a second failure be reported honestly.
    _python = os.environ.get("HTTK_WORKFLOW_PYTHON")
    if _python is None or os.environ.get("HTTK_WORKFLOW_RUNNER_BOOTSTRAP") == "1":
        raise
    os.environ["HTTK_WORKFLOW_RUNNER_BOOTSTRAP"] = "1"
    os.execv(_python, [_python, os.path.abspath(__file__), *sys.argv[1:]])

WORKFLOW = "httk.vasp.static"
#: What makes a calculation a single point: no ionic step, and no ionic loop.
DEFAULT_STATIC_TAGS: dict[str, object] = {"IBRION": -1, "NSW": 0}
DEFAULT_COLLECT = "INCAR KPOINTS OUTCAR CONTCAR OSZICAR vasprun.xml vasp-run-report.json POTCAR.provenance.json"
DEFAULT_TIMEOUT = 86400.0
DEFAULT_MAXIMUM_REMEDIES = 8
# Kept across a remedied rerun because they are what makes the rerun cheaper, and
# because VASP overwrites them itself when it reuses them.
KEEP_BETWEEN_RUNS = ("WAVECAR", "CHGCAR", "CHG")

run = Runner(WORKFLOW)


def text_input(a: Attempt, name: str, default: str) -> str:
    """Return one string input, refusing a value of another type."""

    value = a.input(name, default)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"job input {name!r} must be a string, not {type(value).__name__}")
    return value


def number_input(a: Attempt, name: str, default: float | None) -> float | None:
    """Return one numeric input, refusing a value of another type."""

    value = a.input(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"job input {name!r} must be a number, not {type(value).__name__}")
    return float(value)


def tags_input(a: Attempt, name: str) -> dict[str, object]:
    """Return one INCAR tag object input."""

    value = a.input(name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"job input {name!r} must be an object of INCAR tags")
    return {str(tag): item for tag, item in value.items()}


def names_input(a: Attempt, name: str, default: str) -> tuple[str, ...]:
    """Return one space-separated list input as a tuple of file names."""

    return tuple(text_input(a, name, default).split())


def state_int(a: Attempt, name: str) -> int:
    """Return one nonnegative integer job-state counter, absent meaning zero."""

    value = a.state.get(name, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def preparation_options(a: Attempt, *, library: str | None) -> VaspPreparationOptions:
    """Build the preparation options this job's inputs describe."""

    parallel_tag = text_input(a, "parallel_tag", "") or None
    parallel_value = number_input(a, "parallel_value", None)
    return VaspPreparationOptions(
        kpoint_density=number_input(a, "kpoint_density", 20.0) or 20.0,
        centering=text_input(a, "centering", VaspPreparationOptions.centering),
        accuracy_per_atom=number_input(a, "accuracy_per_atom", 0.001),
        pseudopotential_library=library,
        parallel_tag=parallel_tag,
        parallel_value=None if parallel_value is None else int(parallel_value),
        incar_tags=tags_input(a, "incar_tags"),
    )


def stage_inputs(a: Attempt, *, extra_tags: Mapping[str, object] | None = None) -> dict[str, object] | None:
    """Stage the payload inputs into the workdir and derive the rest.

    Returns the preparation record, or ``None`` after publishing the failure of a
    job whose starting structure is not where its inputs say it is.
    """

    validate_vasp_workdir(a.workdir)
    poscar = a.payload / text_input(a, "poscar", "files/POSCAR")
    if not poscar.is_file():
        a.fail(
            "vasp.input_missing",
            f"the starting structure {poscar.name} is not in this payload",
            details={"expected": str(poscar)},
        )
        return None
    shutil.copyfile(poscar, a.workdir / "POSCAR")
    incar = a.payload / text_input(a, "incar", "files/INCAR")
    if incar.is_file():
        shutil.copyfile(incar, a.workdir / "INCAR")
    else:
        # Everything an INCAR needs is derived below, so an absent one is a valid
        # starting point rather than a reason to refuse the job.
        (a.workdir / "INCAR").write_text("", encoding="utf-8")
    potcar = a.payload / text_input(a, "potcar", "files/POTCAR")
    library: str | None = str(a.setting("vasp.pseudo_library", text_input(a, "pseudopotential_library", ""))) or None
    if potcar.is_file():
        shutil.copyfile(potcar, a.workdir / "POTCAR")
        library = None
    options = preparation_options(a, library=library)
    if extra_tags:
        options = replace(options, incar_tags={**extra_tags, **options.incar_tags})
    record = prepare_vasp_inputs(options, directory=a.workdir)
    a.log.append("note", f"prepared a {WORKFLOW} calculation")
    return record


def vasp_argv(a: Attempt) -> tuple[str, ...]:
    """Return the VASP command as an argv array, resolved through its layers.

    The command is the ``vasp.command`` application setting, so
    :meth:`~httk.workflow.sdk.Attempt.setting` resolves it most-specific first: the job's own
    ``vasp.command`` input, then ``HTTK_VASP_COMMAND`` in the environment, then the
    workspace's configured command, and finally the legacy ``vasp_command`` input.
    That keeps a machine's ``srun -n 32 vasp_std`` — deployment state a job
    submitted elsewhere cannot know — winning over the workspace default, while
    letting an operator configure the command once per workspace instead of
    exporting it for every job.
    """

    text = a.setting("vasp.command", text_input(a, "vasp_command", ""))
    return tuple(shlex.split(str(text)))


def execute(a: Attempt, *, next_step: str) -> None:
    """Run VASP once and publish what its classified result means.

    Exactly one outcome is published: the next step when the calculation
    completed, another attempt when a remedy was applied, and ``vasp.failed`` when
    the ladder has nothing left to try.
    """

    argv = vasp_argv(a)
    if not argv:
        a.fail(
            "vasp.command_missing",
            "no VASP command is configured: set HTTK_VASP_COMMAND on the machine that runs this job, "
            "configure the workspace's vasp.command setting, or give the job a vasp_command input",
        )
        return
    history = job_remedy_history_path(a.payload)
    # A rerun must not read the previous run's outputs. CONTCAR and the run report
    # survive on purpose: they are what a remedy and a restart are derived from.
    clean_vasp_outputs(a.workdir, keep=KEEP_BETWEEN_RUNS)
    report = run_vasp(
        argv,
        directory=a.workdir,
        timeout=number_input(a, "timeout", DEFAULT_TIMEOUT),
    )
    a.log.append("note", f"VASP {report.classification}")
    energy = last_oszicar_energy(a.workdir / "OSZICAR") if (a.workdir / "OSZICAR").is_file() else None
    state: dict[str, object] = {"classification": report.classification}
    if energy is not None:
        state["energy"] = energy
    if report.classification == "completed":
        a.advance(next_step, state=state)
        return
    applied = state_int(a, "remedies")
    maximum = int(number_input(a, "maximum_remedies", DEFAULT_MAXIMUM_REMEDIES) or 0)
    # The decision is planned before the budget is consulted so that a job which
    # stops here says which remedy it would have applied, and why the ladder ended.
    try:
        decision = plan_vasp_remedy(
            report.diagnostics,
            directory=a.workdir,
            history_path=history,
            policy=text_input(a, "remedy_policy", "reviewed-v1"),
        )
    except ValueError as exception:
        # An unregistered policy or an unusable history is a job that cannot be
        # remedied, not a runner that should die of an exception.
        a.state.merge(state)
        a.fail("vasp.failed", f"planning a VASP remedy failed: {exception}")
        return
    if decision.give_up or applied >= maximum:
        message = (
            f"VASP {report.classification} after {applied} remedies"
            if applied >= maximum
            else f"VASP {report.classification} with no remaining remedy"
        )
        a.state.merge(state)
        a.fail("vasp.failed", message, details=decision.as_mapping())
        return
    apply_vasp_remedy(decision, directory=a.workdir, history_path=history)
    amplitude = number_input(a, "rattle_amplitude", 0.0) or 0.0
    if amplitude > 0:
        # The entropy is the attempt itself, so the perturbation is reproducible
        # and no two attempts of this job rattle the same way.
        rattle_poscar(
            a.workdir / "POSCAR",
            amplitude=amplitude,
            entropy=f"{a.context.job_key}:{a.context.attempt_ordinal}",
        )
    state["remedies"] = applied + 1
    a.state.merge(state)
    a.log.append("note", f"applied a remedy for {decision.problem}")
    a.retry(f"applied the {decision.policy} remedy for {decision.problem}")


def publish(a: Attempt, *, prefix: str) -> None:
    """Publish the collected files of a finished calculation."""

    names = names_input(a, "collect", DEFAULT_COLLECT)
    published: list[str] = []
    for name in names:
        source = a.workdir / name
        if not source.is_file():
            continue
        if a.context.data_generation is not None:
            a.put(source, f"{prefix}/{name}")
        published.append(name)
    if a.context.data_generation is None:
        # Without transactional data the persistent workdir *is* the result, so
        # nothing is copied and nothing is deleted.
        a.log.append("note", f"kept in the workdir: {', '.join(published) or 'nothing'}")
    else:
        a.log.append("note", f"published to data/{prefix}: {', '.join(published) or 'nothing'}")


@run.step
def prepare(a: Attempt) -> None:
    """Stage the payload inputs, switch off relaxation, and go on to run VASP."""

    record = stage_inputs(a, extra_tags=tags_input(a, "static_incar_tags") or DEFAULT_STATIC_TAGS)
    if record is not None:
        a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    """Run VASP, remedy a recognized failure, or fail with what was diagnosed."""

    execute(a, next_step="collect")


@run.step
def collect(a: Attempt) -> None:
    """Publish the finished calculation and complete the job."""

    publish(a, prefix=text_input(a, "data_prefix", "vasp"))
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
