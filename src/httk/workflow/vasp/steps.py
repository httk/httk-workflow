"""The attempt-level VASP steps every VASP workflow runner repeats.

A Python VASP runner is a handful of step declarations, each delegating to one
function here: :func:`stage_vasp_inputs` stages the payload inputs and derives
the rest, :func:`run_vasp_step` runs VASP once under supervision and publishes
what its classified result means — including one reviewed remedy and a retry —
:func:`promote_vasp_relaxation` turns a finished relaxation into the inputs of a
single point, and :func:`publish_vasp_files` publishes a finished stage. The
functions read their job parameters (``poscar``, ``incar``, ``potcar``,
``kpoint_density``, ``timeout``, ``maximum_remedies``, ``remedy_policy``,
``rattle_amplitude``, ``collect``, ...) from the attempt, and keep the job state
keys ``classification``, ``energy`` and ``remedies``, plus ``relax_energy`` and
``relax_classification`` after a promotion.
"""

import shlex
import shutil
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from ..sdk import Attempt
from .diagnostics import clean_vasp_outputs, last_oszicar_energy, validate_vasp_workdir
from .inputs import VaspPreparationOptions, contcar_to_poscar, prepare_vasp_inputs, rattle_poscar
from .remedies import apply_vasp_remedy, job_remedy_history_path, plan_vasp_remedy
from .reports import run_vasp

__all__ = [
    "DEFAULT_STATIC_TAGS",
    "DEFAULT_VASP_COLLECT",
    "promote_vasp_relaxation",
    "publish_vasp_files",
    "run_vasp_step",
    "stage_vasp_inputs",
    "vasp_command",
    "vasp_data_prefix",
    "vasp_preparation_options",
    "vasp_static_tags",
]

#: The files a finished stage publishes unless the ``collect`` parameter says otherwise.
DEFAULT_VASP_COLLECT: tuple[str, ...] = (
    "INCAR",
    "KPOINTS",
    "OUTCAR",
    "CONTCAR",
    "OSZICAR",
    "vasprun.xml",
    "vasp-run-report.json",
    "POTCAR.provenance.json",
)
#: Kept across a remedied rerun because they are what makes the rerun cheaper, and
#: because VASP overwrites them itself when it reuses them.
_VASP_KEEP_BETWEEN_RUNS: tuple[str, ...] = ("WAVECAR", "CHGCAR", "CHG")
#: The wall-clock limit of one VASP run in seconds, unless ``timeout`` is set.
_DEFAULT_VASP_TIMEOUT = 86400.0
#: The remedy budget of one job, unless ``maximum_remedies`` is set.
_DEFAULT_VASP_MAXIMUM_REMEDIES = 8
#: What makes a stage a single point: no ionic step, and no ionic loop.
DEFAULT_STATIC_TAGS: Mapping[str, object] = MappingProxyType({"IBRION": -1, "NSW": 0})


def _text_parameter(a: Attempt, name: str, default: str) -> str:
    """Return one string parameter, refusing a value of another type."""

    value = a.parameter(name, default)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"job parameter {name!r} must be a string, not {type(value).__name__}")
    return value


def _number_parameter(a: Attempt, name: str, default: float | None) -> float | None:
    """Return one numeric parameter, refusing a value of another type."""

    value = a.parameter(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"job parameter {name!r} must be a number, not {type(value).__name__}")
    return float(value)


def _tags_parameter(a: Attempt, name: str) -> dict[str, object]:
    """Return one INCAR tag object parameter."""

    value = a.parameter(name, {})
    if not isinstance(value, Mapping):
        raise ValueError(f"job parameter {name!r} must be an object of INCAR tags")
    return {str(tag): item for tag, item in value.items()}


def _collect_names(a: Attempt) -> tuple[str, ...]:
    """Return the space-separated ``collect`` parameter as file names."""

    return tuple(_text_parameter(a, "collect", " ".join(DEFAULT_VASP_COLLECT)).split())


def _state_int(a: Attempt, name: str) -> int:
    """Return one nonnegative integer job-state counter, zero when invalid or absent."""

    value = a.state.get(name, 0)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def vasp_data_prefix(a: Attempt, default: str = "vasp") -> str:
    """Return the ``data_prefix`` job parameter a stage publishes under.

    :param a: Read the job parameters from this attempt.
    :param default: Use this prefix when the parameter is absent.
    :return: The prefix, empty when the parameter is an explicit null.
    :raises ValueError: If the parameter is not a string.
    """

    return _text_parameter(a, "data_prefix", default)


def vasp_static_tags(a: Attempt) -> dict[str, object]:
    """Return the INCAR tags that make a stage a single point.

    :param a: Read the job parameters from this attempt.
    :return: The ``static_incar_tags`` parameter, or a copy of :data:`DEFAULT_STATIC_TAGS` when it is empty or absent.
    :raises ValueError: If the parameter is not an object of INCAR tags.
    """

    return _tags_parameter(a, "static_incar_tags") or dict(DEFAULT_STATIC_TAGS)


def vasp_preparation_options(a: Attempt, *, library: str | None) -> VaspPreparationOptions:
    """Build the preparation options this job's parameters describe.

    :param a: Read preparation parameters from this attempt.
    :param library: Use this pseudopotential library when set.
    :return: The preparation options for the job.
    :raises ValueError: If a preparation parameter has the wrong type.
    """

    parallel_tag = _text_parameter(a, "parallel_tag", "") or None
    parallel_value = _number_parameter(a, "parallel_value", None)
    return VaspPreparationOptions(
        kpoint_density=_number_parameter(a, "kpoint_density", 20.0) or 20.0,
        centering=_text_parameter(a, "centering", VaspPreparationOptions.centering),
        accuracy_per_atom=_number_parameter(a, "accuracy_per_atom", 0.001),
        pseudopotential_library=library,
        parallel_tag=parallel_tag,
        parallel_value=None if parallel_value is None else int(parallel_value),
        incar_tags=_tags_parameter(a, "incar_tags"),
    )


def stage_vasp_inputs(a: Attempt, *, extra_tags: Mapping[str, object] | None = None) -> dict[str, object] | None:
    """Stage the payload inputs into the workdir and derive the rest.

    Returns the preparation record, or ``None`` after publishing the
    ``vasp.input_missing`` failure of a job whose starting structure is not where
    its inputs say it is.

    :param a: Stage files and publish failures through this attempt.
    :param extra_tags: Add these INCAR tags to the preparation defaults; job ``incar_tags`` win.
    :return: The preparation record, or ``None`` after an input failure.
    :raises ValueError: If a preparation parameter has the wrong type.
    """

    validate_vasp_workdir(a.workdir)
    poscar = a.payload / _text_parameter(a, "poscar", "files/POSCAR")
    if not poscar.is_file():
        a.fail(
            "vasp.input_missing",
            f"the starting structure {poscar.name} is not in this payload",
            details={"expected": str(poscar)},
        )
        return None
    shutil.copyfile(poscar, a.workdir / "POSCAR")
    incar = a.payload / _text_parameter(a, "incar", "files/INCAR")
    if incar.is_file():
        shutil.copyfile(incar, a.workdir / "INCAR")
    else:
        # Everything an INCAR needs is derived below, so an absent one is a valid
        # starting point rather than a reason to refuse the job.
        (a.workdir / "INCAR").write_text("", encoding="utf-8")
    potcar = a.payload / _text_parameter(a, "potcar", "files/POTCAR")
    library: str | None = (
        str(a.setting("vasp.pseudo_library", _text_parameter(a, "pseudopotential_library", ""))) or None
    )
    if potcar.is_file():
        shutil.copyfile(potcar, a.workdir / "POTCAR")
        library = None
    options = vasp_preparation_options(a, library=library)
    if extra_tags:
        options = replace(options, incar_tags={**extra_tags, **options.incar_tags})
    record = prepare_vasp_inputs(options, directory=a.workdir)
    a.log.append("note", f"prepared a {a.job.workflow} calculation")
    return record


def vasp_command(a: Attempt) -> tuple[str, ...]:
    """Return the VASP command as an argv array, resolved through its layers.

    The command is the ``vasp.command`` application setting, so
    :meth:`~httk.workflow.sdk.Attempt.setting` resolves it most-specific first: the
    job's own ``vasp.command`` parameter, then ``HTTK_VASP_COMMAND`` in the
    environment, then the workspace's configured command, and finally the legacy
    ``vasp_command`` parameter.

    :param a: Read command settings and parameters from this attempt.
    :return: The VASP command argument vector, empty when none is configured.
    :raises ValueError: If the legacy ``vasp_command`` parameter is not a string.
    """

    text = a.setting("vasp.command", _text_parameter(a, "vasp_command", ""))
    return tuple(shlex.split(str(text)))


def run_vasp_step(a: Attempt, *, next_step: str) -> None:
    """Run VASP once and publish what its classified result means.

    Exactly one outcome is published: an advance to *next_step* when the
    calculation completed, a retry when a remedy was applied, ``vasp.failed``
    when the ladder or the remedy budget has nothing left to try, and
    ``vasp.command_missing`` when no VASP command is configured.

    :param a: Run and update this VASP attempt.
    :param next_step: Advance here after a completed calculation.
    :raises ValueError: If a job parameter has the wrong type.
    """

    argv = vasp_command(a)
    if not argv:
        a.fail(
            "vasp.command_missing",
            "no VASP command is configured: set it with `httk workspace settings set --key vasp.command --value '...' WORKSPACE`, "
            "or set HTTK_VASP_COMMAND on the machine that runs this job, or give the job a vasp_command parameter",
        )
        return
    history = job_remedy_history_path(a.payload)
    # A rerun must not read the previous run's outputs. CONTCAR and the run report
    # survive on purpose: they are what a remedy and a restart are derived from.
    clean_vasp_outputs(a.workdir, keep=_VASP_KEEP_BETWEEN_RUNS)
    report = run_vasp(
        argv,
        directory=a.workdir,
        timeout=_number_parameter(a, "timeout", _DEFAULT_VASP_TIMEOUT),
    )
    a.log.append("note", f"VASP {report.classification}")
    energy = last_oszicar_energy(a.workdir / "OSZICAR") if (a.workdir / "OSZICAR").is_file() else None
    state: dict[str, object] = {"classification": report.classification}
    if energy is not None:
        state["energy"] = energy
    if report.classification == "completed":
        a.advance(next_step, state=state)
        return
    applied = _state_int(a, "remedies")
    maximum = int(_number_parameter(a, "maximum_remedies", _DEFAULT_VASP_MAXIMUM_REMEDIES) or 0)
    # The decision is planned before the budget is consulted so that a job which
    # stops here says which remedy it would have applied, and why the ladder ended.
    try:
        decision = plan_vasp_remedy(
            report.diagnostics,
            directory=a.workdir,
            history_path=history,
            policy=_text_parameter(a, "remedy_policy", "reviewed-v1"),
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
    amplitude = _number_parameter(a, "rattle_amplitude", 0.0) or 0.0
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


def promote_vasp_relaxation(a: Attempt, *, next_step: str, archive: str = "relax") -> None:
    """Archive a finished relaxation, adopt its CONTCAR, and derive static inputs.

    The collected files of the relaxation are copied into *archive* inside the
    workdir, the CONTCAR becomes the POSCAR (keeping the reference POSCAR's
    comment line), and the inputs are re-derived with the ``static_incar_tags``
    parameter (or :data:`DEFAULT_STATIC_TAGS`) under the job's ``incar_tags``.
    The job then advances to *next_step* with ``relax_energy`` and
    ``relax_classification`` state, or fails ``vasp.no_relaxed_structure`` when
    the CONTCAR is missing or empty.

    :param a: Promote the completed relaxation in this attempt.
    :param next_step: Advance here after promoting.
    :param archive: Archive the relaxation into this workdir subdirectory.
    :raises ValueError: If a job parameter has the wrong type.
    """

    contcar = a.workdir / "CONTCAR"
    if not contcar.is_file() or not contcar.read_text(encoding="utf-8", errors="replace").strip():
        a.fail(
            "vasp.no_relaxed_structure",
            "the relaxation completed without leaving a CONTCAR to run statically",
        )
        return
    target = a.workdir / archive
    target.mkdir(exist_ok=True)
    for name in _collect_names(a):
        source = a.workdir / name
        if source.is_file():
            shutil.copyfile(source, target / name)
    # The relaxed cell is the structure of the single point, and the reference
    # POSCAR only lends it its comment line, which carries the MAGMOM override.
    contcar_to_poscar(contcar, reference=a.workdir / "POSCAR", output=a.workdir / "POSCAR")
    options = replace(
        vasp_preparation_options(a, library=None),
        incar_tags={
            **vasp_static_tags(a),
            **_tags_parameter(a, "incar_tags"),
        },
    )
    # Re-derived rather than reused: the relaxed cell has its own k-point grid and
    # its own convergence budget. Tags the relaxation already fixed — NBANDS,
    # MAGMOM — are left exactly as they were, because they are already explicit.
    prepare_vasp_inputs(options, directory=a.workdir)
    relaxed = a.state.get("energy")
    a.log.append("note", "promoted the relaxed structure to a static calculation")
    a.advance(next_step, state={"relax_energy": relaxed, "relax_classification": a.state.get("classification")})


def publish_vasp_files(a: Attempt, *, prefix: str, directory: Path | None = None) -> None:
    """Publish the collected files of one finished calculation stage.

    The ``collect`` parameter (space separated, default
    :data:`DEFAULT_VASP_COLLECT`) names the files. With transactional data they
    are put under *prefix*; without, the persistent workdir is the result and
    only a log note records what it holds. Either way no outcome is published.

    :param a: Read files and publish them through this attempt.
    :param prefix: Publish files below this data prefix.
    :param directory: Read files from this stage directory, or the workdir when unset.
    :raises ValueError: If the ``collect`` parameter is not a string.
    """

    root = a.workdir if directory is None else directory
    published: list[str] = []
    for name in _collect_names(a):
        source = root / name
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
