#!/usr/bin/env python3
"""One VASP relaxation: prepare inputs, run with remedies, publish the result.

The three steps are the whole workflow. ``prepare`` stages the structure and the
INCAR of the job payload into the workdir and derives everything else;  ``run``
executes VASP under supervision and, when the run fails in a way the reviewed
remedy ladder recognizes, applies exactly one remedy and asks for another attempt;
``publish`` publishes the files that describe the finished calculation.

The job inputs and parameters are documented in :mod:`httk.workflow.vasp.runners`. Nothing here
imports anything but an installed *httk-workflow*, so this one file is the whole
runner: reference it as ``pkg:httk.workflow.vasp.runners/vasp_relax.py``, publish it to
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

WORKFLOW = "httk.vasp.relax"
DEFAULT_COLLECT = "INCAR KPOINTS OUTCAR CONTCAR OSZICAR vasprun.xml vasp-run-report.json POTCAR.provenance.json"
DEFAULT_TIMEOUT = 86400.0
DEFAULT_MAXIMUM_REMEDIES = 8
# Kept across a remedied rerun because they are what makes the rerun cheaper, and
# because VASP overwrites them itself when it reuses them.
KEEP_BETWEEN_RUNS = ("WAVECAR", "CHGCAR", "CHG")

run = Runner(WORKFLOW)


def text_parameter(a: Attempt, name: str, default: str) -> str:
    """Return one string parameter, refusing a value of another type.

    :param a: Read the job parameters from this attempt.
    :param name: Read this parameter name.
    :param default: Use this value when the parameter is absent.
    :return: The validated string value.
    :raises ValueError: If the parameter is present with another type.
    """

    value = a.parameter(name, default)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"job parameter {name!r} must be a string, not {type(value).__name__}")
    return value


def number_parameter(a: Attempt, name: str, default: float | None) -> float | None:
    """Return one numeric input, refusing a value of another type.

    :param a: Read the job parameters from this attempt.
    :param name: Read this parameter name.
    :param default: Use this value when the parameter is absent.
    :return: The validated numeric value, or the default.
    :raises ValueError: If the parameter is present with another type.
    """

    value = a.parameter(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"job parameter {name!r} must be a number, not {type(value).__name__}")
    return float(value)


def tags_parameter(a: Attempt, name: str) -> dict[str, object]:
    """Return one INCAR tag object input.

    :param a: Read the job parameters from this attempt.
    :param name: Read this parameter name.
    :return: The validated INCAR tag mapping.
    :raises ValueError: If the parameter is not a mapping.
    """

    value = a.parameter(name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"job parameter {name!r} must be an object of INCAR tags")
    return {str(tag): item for tag, item in value.items()}


def names_parameter(a: Attempt, name: str, default: str) -> tuple[str, ...]:
    """Return one space-separated list input as a tuple of file names.

    :param a: Read the job parameters from this attempt.
    :param name: Read this parameter name.
    :param default: Use this space-separated list when absent.
    :return: The selected file names.
    """

    return tuple(text_parameter(a, name, default).split())


def state_int(a: Attempt, name: str) -> int:
    """Return one nonnegative integer job-state counter, with invalid or absent values producing zero.

    :param a: Read the job state from this attempt.
    :param name: Read this state name.
    :return: The nonnegative counter, or zero for an invalid or absent value.
    """

    value = a.state.get(name, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def preparation_options(a: Attempt, *, library: str | None) -> VaspPreparationOptions:
    """Build the preparation options this job's parameters describe.

    :param a: Read preparation parameters from this attempt.
    :param library: Use this pseudopotential library when set.
    :return: The preparation options for the job.
    """

    parallel_tag = text_parameter(a, "parallel_tag", "") or None
    parallel_value = number_parameter(a, "parallel_value", None)
    return VaspPreparationOptions(
        kpoint_density=number_parameter(a, "kpoint_density", 20.0) or 20.0,
        centering=text_parameter(a, "centering", VaspPreparationOptions.centering),
        accuracy_per_atom=number_parameter(a, "accuracy_per_atom", 0.001),
        pseudopotential_library=library,
        parallel_tag=parallel_tag,
        parallel_value=None if parallel_value is None else int(parallel_value),
        incar_tags=tags_parameter(a, "incar_tags"),
    )


def stage_inputs(a: Attempt, *, extra_tags: Mapping[str, object] | None = None) -> dict[str, object] | None:
    """Stage the payload inputs into the workdir and derive the rest.

    Returns the preparation record, or ``None`` after publishing the failure of a
    job whose starting structure is not where its inputs say it is.

    :param a: Stage files and publish failures through this attempt.
    :param extra_tags: Add these INCAR tags to the preparation defaults.
    :return: The preparation record, or ``None`` after an input failure.
    """

    validate_vasp_workdir(a.workdir)
    poscar = a.payload / text_parameter(a, "poscar", "files/POSCAR")
    if not poscar.is_file():
        a.fail(
            "vasp.input_missing",
            f"the starting structure {poscar.name} is not in this payload",
            details={"expected": str(poscar)},
        )
        return None
    shutil.copyfile(poscar, a.workdir / "POSCAR")
    incar = a.payload / text_parameter(a, "incar", "files/INCAR")
    if incar.is_file():
        shutil.copyfile(incar, a.workdir / "INCAR")
    else:
        # Everything an INCAR needs is derived below, so an absent one is a valid
        # starting point rather than a reason to refuse the job.
        (a.workdir / "INCAR").write_text("", encoding="utf-8")
    potcar = a.payload / text_parameter(a, "potcar", "files/POTCAR")
    library: str | None = (
        str(a.setting("vasp.pseudo_library", text_parameter(a, "pseudopotential_library", ""))) or None
    )
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
    workspace's configured command, and finally the legacy ``vasp_command`` parameter.
    That keeps a machine's ``srun -n 32 vasp_std`` — deployment state a job
    submitted elsewhere cannot know — winning over the workspace default, while
    letting an operator configure the command once per workspace instead of
    exporting it for every job.

    :param a: Read command settings and parameters from this attempt.
    :return: The VASP command argument vector.
    """

    text = a.setting("vasp.command", text_parameter(a, "vasp_command", ""))
    return tuple(shlex.split(str(text)))


def execute(a: Attempt, *, next_step: str) -> None:
    """Run VASP once and publish what its classified result means.

    Exactly one outcome is published: the next step when the calculation
    completed, another attempt when a remedy was applied, and ``vasp.failed`` when
    the ladder has nothing left to try.

    :param a: Run and update this VASP attempt.
    :param next_step: Advance here after a completed calculation.
    """

    argv = vasp_argv(a)
    if not argv:
        a.fail(
            "vasp.command_missing",
            "no VASP command is configured: set it with `httk workflow workspace settings set vasp.command '...'`, "
            "or set HTTK_VASP_COMMAND on the machine that runs this job, or give the job a vasp_command parameter",
        )
        return
    history = job_remedy_history_path(a.payload)
    # A rerun must not read the previous run's outputs. CONTCAR and the run report
    # survive on purpose: they are what a remedy and a restart are derived from.
    clean_vasp_outputs(a.workdir, keep=KEEP_BETWEEN_RUNS)
    report = run_vasp(
        argv,
        directory=a.workdir,
        timeout=number_parameter(a, "timeout", DEFAULT_TIMEOUT),
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
    maximum = int(number_parameter(a, "maximum_remedies", DEFAULT_MAXIMUM_REMEDIES) or 0)
    # The decision is planned before the budget is consulted so that a job which
    # stops here says which remedy it would have applied, and why the ladder ended.
    try:
        decision = plan_vasp_remedy(
            report.diagnostics,
            directory=a.workdir,
            history_path=history,
            policy=text_parameter(a, "remedy_policy", "reviewed-v1"),
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
    amplitude = number_parameter(a, "rattle_amplitude", 0.0) or 0.0
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


def publish_files(a: Attempt, *, prefix: str) -> None:
    """Publish the collected files of a finished calculation.

    :param a: Read files and publish them through this attempt.
    :param prefix: Publish files below this data prefix.
    """

    names = names_parameter(a, "collect", DEFAULT_COLLECT)
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
    """Stage the payload inputs, derive the rest, and go on to run VASP.

    :param a: Prepare and advance this VASP attempt.
    """

    record = stage_inputs(a)
    if record is not None:
        a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    """Run VASP, remedy a recognized failure, or fail with what was diagnosed.

    :param a: Execute this VASP workflow attempt.
    """

    execute(a, next_step="publish")


@run.step
def publish(a: Attempt) -> None:
    """Publish the finished calculation and complete the job.

    :param a: Publish results and complete this VASP attempt.
    """

    publish_files(a, prefix=text_parameter(a, "data_prefix", "vasp"))
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
