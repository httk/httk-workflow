"""Bounded VASP remedy policies, planning, application, and history.

This is the remediation side of the dependency-free VASP interface: a policy is
data (an ordered ladder of bounded input edits per diagnosed problem), and
:func:`plan_vasp_remedy` / :func:`apply_vasp_remedy` advance one job through that
ladder, recording an escalation history so a retried job keeps climbing rather
than repeating one remedy for ever. Historical authorship is documented in
``v1_runtime/NOTICE``.
"""

import logging
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .._util import read_json, sha256_file, utc_now, write_json_atomic
from ..models import JOB_STATE_DIRECTORY
from ..runtime_builders import ReplayableWorkdirBatch
from ..supervision import Diagnostic
from .inputs import (
    _write_text_atomic,
    contcar_to_poscar,
    read_incar,
    scale_poscar_lattice,
    update_incar,
)

_LOGGER = logging.getLogger(__name__)

#: Where a remedy history lives when a caller names no other place. The
#: job-scoped location of :func:`job_remedy_history_path` is what a runner
#: should use; this workdir-relative name is the pre-0.2 location, still read for
#: compatibility.
DEFAULT_REMEDY_HISTORY = ".httk-vasp/remedies.json"
REMEDY_HISTORY_NAME = "vasp-remedies.json"


type RemedyChange = tuple[str, object]
type RemedySequence = tuple[tuple[RemedyChange, ...], ...]

_REVIEWED_SEQUENCES: dict[str, RemedySequence] = {
    "kpoints_class": (
        (("bump_kpoints", 1),),
        (("centering", "Gamma"),),
        (("bump_kpoints", 1), ("centering", "Gamma")),
        (("equal_kpoints", True),),
        (("bump_kpoints", 1), ("equal_kpoints", True)),
        (("incar.ISYM", 0),),
    ),
    "dentet": (
        (("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),
        (("bump_kpoints", 1),),
        (("bump_kpoints", 1), ("centering", "Gamma")),
        (("incar.NEDOS", 1000),),
    ),
    "kpoint_shifts": ((("centering", "Gamma"),),),
    "lreal_false": ((("incar.LREAL", ".FALSE."),),),
    "inverse_rotation": ((("incar.ISYM", 0),),),
    "rotation_matrix": ((("incar.ISYM", 0),),),
    "fexcf": (
        (("incar.ICHARG", 2), ("incar.AMIX", 0.10), ("incar.BMIX", 0.01)),
        (("incar.ICHARG", 2), ("incar.BMIX", 3.0), ("incar.AMIN", 0.01)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.4)),
        (("incar.ALGO", "Damped"), ("incar.TIME", 0.05)),
    ),
    "electronic_nonconvergence": (
        (("incar.ICHARG", 2), ("incar.AMIX", 0.10), ("incar.BMIX", 0.01)),
        (("incar.ICHARG", 2), ("incar.BMIX", 3.0), ("incar.AMIN", 0.01)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.4)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.05)),
        (("incar.ALGO", "Damped"), ("incar.TIME", 0.05)),
    ),
    "pricell": (
        (("incar.SYMPREC", 1e-4),),
        (("incar.SYMPREC", 1e-6),),
        (("incar.ISYM", 0),),
    ),
    "tetrahedron_kpoints": ((("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),),
    "zbrent_bracketing": ((("scale_ediff", 0.1),), (("scale_ediff", 0.1),)),
    "real_optlay": ((("incar.LREAL", ".FALSE."),),),
    "too_few_bands": ((("bump_bands", 2),),),
    "ionic_nonconvergence": ((("contcar_to_poscar", True),),),
    "zpotrf": (
        (("scale_lattice", 1.05),),
        (("bump_kpoints", 1),),
        (("bump_bands", 2),),
        (("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),
        (("incar.ISYM", 0),),
    ),
    "ions_too_close": ((("scale_lattice", 1.05),),),
}
_REVIEWED_PRECEDENCE = (
    "pricell",
    "zpotrf",
    "tetrahedron_kpoints",
    "zbrent_bracketing",
    "real_optlay",
    "ions_too_close",
    "nonlr_alloc",
    "kpoints_class",
    "dentet",
    "kpoint_shifts",
    "lreal_false",
    "inverse_rotation",
    "rotation_matrix",
    "fexcf",
    "electronic_nonconvergence",
    "ionic_nonconvergence",
    "too_few_bands",
)
_REVIEWED_REFUSALS = {"nonlr_alloc": "memory allocation failure has no safe input remedy"}


@dataclass(frozen=True)
class RemedyPolicy:
    """One named, ordered ladder of bounded remedies.

    A policy is data, not code: *sequences* maps a diagnosed problem to the
    escalating steps tried for it, *precedence* orders the problems so one
    diagnosis is remedied at a time, and *refusals* names the problems this policy
    deliberately has no input remedy for. A group with its own reviewed practice
    registers its own policy with :func:`register_remedy_policy` instead of
    editing this module.

    :param name: Identify the registered policy.
    :param sequences: Map diagnostic codes to ordered remedy changes.
    :param precedence: Order diagnostic codes for remedy selection.
    :param refusals: Explain diagnostic codes this policy will not remedy.
    """

    name: str
    sequences: Mapping[str, RemedySequence]
    precedence: tuple[str, ...]
    refusals: Mapping[str, str] = field(default_factory=dict)


_REMEDY_POLICIES: dict[str, RemedyPolicy] = {}

#: Every remedy operation :func:`apply_vasp_remedy` implements, besides the
#: ``incar.<TAG>`` form that assigns one INCAR tag.
REMEDY_OPERATIONS = (
    "bump_bands",
    "bump_kpoints",
    "centering",
    "contcar_to_poscar",
    "equal_kpoints",
    "scale_ediff",
    "scale_lattice",
)


def register_remedy_policy(
    name: str,
    sequences: Mapping[str, Sequence[Sequence[tuple[str, object]]]],
    precedence: Sequence[str],
    *,
    refusals: Mapping[str, str] | None = None,
    replace: bool = False,
) -> RemedyPolicy:
    """Register one named remedy policy and return the normalized result.

    Every problem named in *sequences* must also appear in *precedence*, which is
    what decides which of several simultaneous diagnostics is acted on, and every
    change must spell one supported remedy operation, so a policy that cannot be
    executed is refused when it is registered rather than when a run needs it.

    :param name: Register the policy under this nonempty name.
    :param sequences: Define ordered changes for each diagnostic code.
    :param precedence: Order diagnostic codes for selection.
    :param refusals: Explain diagnostic codes deliberately left without a remedy.
    :param replace: Replace an existing policy with this definition.
    :return: The normalized registered policy.
    :raises ValueError: If the policy is duplicate, incomplete, or unsupported.
    """

    if not isinstance(name, str) or not name:
        raise ValueError("a remedy policy name must be a nonempty string")
    if name in _REMEDY_POLICIES and not replace:
        raise ValueError(f"remedy policy {name!r} is already registered; pass replace to redefine it")
    order = tuple(precedence)
    if len(set(order)) != len(order):
        raise ValueError(f"remedy policy {name!r} lists a problem twice in its precedence")
    normalized: dict[str, RemedySequence] = {}
    for problem, sequence in sequences.items():
        if problem not in order:
            raise ValueError(f"remedy policy {name!r} has a sequence for {problem!r}, which its precedence omits")
        steps: list[tuple[RemedyChange, ...]] = []
        for step in sequence:
            changes = tuple((str(operation), value) for operation, value in step)
            if not changes:
                raise ValueError(f"remedy policy {name!r} has an empty remedy step for {problem!r}")
            for operation, _ in changes:
                if operation not in REMEDY_OPERATIONS and not operation.startswith("incar."):
                    raise ValueError(
                        f"remedy policy {name!r} uses unsupported remedy operation {operation!r}; "
                        f"supported operations: {', '.join(REMEDY_OPERATIONS)}, or incar.<TAG>"
                    )
            steps.append(changes)
        normalized[problem] = tuple(steps)
    for problem in refusals or {}:
        if problem not in order:
            raise ValueError(f"remedy policy {name!r} refuses {problem!r}, which its precedence omits")
        if problem in normalized:
            raise ValueError(f"remedy policy {name!r} both refuses {problem!r} and has a remedy sequence for it")
    policy = RemedyPolicy(name, normalized, order, dict(refusals or {}))
    _REMEDY_POLICIES[name] = policy
    return policy


def remedy_policy_names() -> tuple[str, ...]:
    """Return the names of every registered remedy policy, in registration order.

    :return: Registered policy names in registration order.
    """

    return tuple(_REMEDY_POLICIES)


def remedy_policy(name: str) -> RemedyPolicy:
    """Return one registered remedy policy, naming the alternatives if absent.

    :param name: Select this registered policy.
    :return: The selected remedy policy.
    :raises ValueError: If no policy has this name.
    """

    policy = _REMEDY_POLICIES.get(name)
    if policy is None:
        raise ValueError(
            f"unknown VASP remedy policy {name!r}; registered policies: {', '.join(_REMEDY_POLICIES) or 'none'}"
        )
    return policy


register_remedy_policy(
    "reviewed-v1",
    _REVIEWED_SEQUENCES,
    _REVIEWED_PRECEDENCE,
    refusals=_REVIEWED_REFUSALS,
)


@dataclass(frozen=True)
class VaspRemedyDecision:
    """One explicit bounded remedy proposal.

    :param policy: Identify the policy that produced the decision.
    :param problem: Identify the selected diagnostic problem.
    :param step: Record the ladder step considered.
    :param changes: Record the input changes to apply.
    :param give_up: Mark that no remedy should be applied.
    :param reason: Explain the decision.
    """

    policy: str
    problem: str
    step: int
    changes: tuple[tuple[str, object], ...]
    give_up: bool
    reason: str

    def as_mapping(self) -> dict[str, object]:
        """Serialize the remedy decision.

        :return: The JSON-compatible decision mapping.
        """
        return {
            "format": "httk-vasp-remedy-decision",
            "format_version": 1,
            "policy": self.policy,
            "problem": self.problem,
            "step": self.step,
            "changes": [{"operation": name, "value": value} for name, value in self.changes],
            "give_up": self.give_up,
            "reason": self.reason,
        }


def job_remedy_history_path(payload: str | os.PathLike[str]) -> Path:
    """Return the job-scoped remedy history file of one job payload.

    The escalation ladder is a property of the *job*, not of the directory one
    attempt happened to run in, so it lives beside the job state in
    ``<payload>/.httk-job/`` rather than in the workdir. A job with an isolated
    workdir therefore keeps escalating instead of silently starting the ladder
    from the beginning on every attempt.

    :param payload: Locate the job payload.
    :return: The job-scoped remedy history path.
    """

    return Path(payload).resolve() / JOB_STATE_DIRECTORY / REMEDY_HISTORY_NAME


def _remedy_history_file(root: Path, history_path: str | os.PathLike[str]) -> Path:
    """Resolve one history location the same way in planning and in application.

    A relative path is relative to the VASP directory in both, never to the
    process working directory: a plan and the application of that plan must read
    and write one file.
    """

    candidate = Path(history_path)
    return candidate if candidate.is_absolute() else root / candidate


def _empty_remedy_history() -> dict[str, Any]:
    return {"format": "httk-vasp-remedy-history", "format_version": 1, "attempts": {}, "events": []}


def _read_remedy_history(root: Path, history_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read the remedy history, falling back to the pre-0.2 workdir location."""

    history_file = _remedy_history_file(root, history_path)
    if history_file.exists():
        return read_json(history_file)
    legacy = root / DEFAULT_REMEDY_HISTORY
    if legacy != history_file and legacy.exists():
        return read_json(legacy)
    return _empty_remedy_history()


def _remedy_obstacle(root: Path, changes: Sequence[RemedyChange]) -> str | None:
    """Return why *changes* cannot be applied in *root*, or ``None`` when they can.

    Planning and application ask exactly this question, so a planned remedy is by
    construction one that can be executed: proposing ``bump_bands`` for a
    calculation whose INCAR never set ``NBANDS`` would otherwise reach
    :func:`apply_vasp_remedy` and kill the runner with an uncaught error.
    """

    tags: dict[str, str] | None = read_incar(root / "INCAR") if (root / "INCAR").is_file() else None
    for operation, _ in changes:
        if operation.startswith("incar."):
            if tags is None:
                return f"{operation} needs an INCAR in {root}"
            continue
        if operation in {"scale_ediff", "bump_bands"}:
            name = "EDIFF" if operation == "scale_ediff" else "NBANDS"
            if tags is None:
                return f"{operation} needs an INCAR in {root}"
            if name not in tags:
                return f"{operation} needs {name} in INCAR, which does not set it"
            try:
                float(tags[name])
            except ValueError:
                return f"{operation} needs a numeric {name} in INCAR, which reads {tags[name]!r}"
            continue
        if operation in {"bump_kpoints", "equal_kpoints", "centering"}:
            kpoints = root / "KPOINTS"
            if not kpoints.is_file():
                return f"{operation} needs a staged KPOINTS in {root}"
            lines = kpoints.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(lines) < 4:
                return f"{operation} needs a KPOINTS with at least four lines"
            if operation != "centering":
                grid = lines[3].split()[:3]
                if len(grid) != 3 or any(re.fullmatch(r"[-+]?[0-9]+", item) is None for item in grid):
                    return f"{operation} needs an explicit three-integer KPOINTS grid line"
            continue
        if operation == "scale_lattice":
            poscar = root / "POSCAR"
            if not poscar.is_file():
                return f"{operation} needs a POSCAR in {root}"
            lines = poscar.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(lines) < 2 or not lines[1].split():
                return f"{operation} needs a POSCAR scale line"
            try:
                float(lines[1].split()[0])
            except ValueError:
                return f"{operation} needs a numeric POSCAR scale line"
            continue
        if operation == "contcar_to_poscar":
            if not (root / "CONTCAR").is_file() or not (root / "CONTCAR").read_text(errors="replace").strip():
                return "contcar_to_poscar needs a nonempty CONTCAR from the interrupted run"
            if not (root / "POSCAR").is_file():
                return "contcar_to_poscar needs the reference POSCAR"
            continue
        return f"unsupported remedy operation: {operation}"
    return None


def plan_vasp_remedy(
    diagnostics: Sequence[Diagnostic],
    *,
    directory: str | os.PathLike[str] = ".",
    history_path: str | os.PathLike[str] = DEFAULT_REMEDY_HISTORY,
    policy: str = "reviewed-v1",
) -> VaspRemedyDecision:
    """Return, but do not apply, the next remedy of *policy*.

    The decision is validated against the real contents of *directory*: a ladder
    step whose changes cannot be executed there is skipped, and the first
    executable step wins. When nothing is left the decision gives up, so a runner
    only ever hands :func:`apply_vasp_remedy` a remedy it can perform.

    :param diagnostics: Supply diagnostics from the VASP run.
    :param directory: Inspect and plan changes in this VASP directory.
    :param history_path: Read the remedy history from this path.
    :param policy: Select this registered remedy policy.
    :return: The next applicable remedy decision.
    :raises ValueError: If the remedy history or policy is invalid.
    """

    resolved = remedy_policy(policy)
    root = Path(directory).resolve()
    history = _read_remedy_history(root, history_path)
    attempts = history.get("attempts")
    if not isinstance(attempts, Mapping):
        raise ValueError("VASP remedy history has invalid attempts")
    codes = {item.code for item in diagnostics}
    problem = next((item for item in resolved.precedence if item in codes), "unknown")
    step = int(attempts.get(problem, 0))
    refusal = resolved.refusals.get(problem)
    if refusal is not None:
        return VaspRemedyDecision(policy, problem, step, (), True, refusal)
    sequence = resolved.sequences.get(problem, ())
    skipped: list[str] = []
    while step < len(sequence):
        obstacle = _remedy_obstacle(root, sequence[step])
        if obstacle is None:
            return VaspRemedyDecision(policy, problem, step, sequence[step], False, "reviewed remedy available")
        _LOGGER.info("remedy step %d for %s is not applicable in %s: %s", step, problem, root, obstacle)
        skipped.append(f"step {step}: {obstacle}")
        step += 1
    reason = f"no further {policy} remedy is available"
    if skipped:
        reason = f"{reason} ({'; '.join(skipped)})"
    return VaspRemedyDecision(policy, problem, step, (), True, reason)


def apply_vasp_remedy(
    decision: VaspRemedyDecision,
    *,
    directory: str | os.PathLike[str] = ".",
    history_path: str | os.PathLike[str] = DEFAULT_REMEDY_HISTORY,
    durable: bool = False,
) -> Path:
    """Explicitly apply a proposed remedy through a replayable workdir batch.

    The history is recorded before the inputs change, so an application
    interrupted halfway advances the ladder rather than repeating one remedy for
    ever. A history file inside the VASP directory is written by the same
    replayable batch as the inputs; a job-scoped one, which the batch cannot
    reach, is written atomically just before the batch commits.

    *durable* synchronizes the batch and the job-scoped history on a durable
    workspace, so a power cut cannot lose a remedy the ladder has already
    recorded. It defaults to ``False`` for a caller applying a remedy outside an
    attempt; the Bash bridge sources it from the workspace durability contract.

    :param decision: Apply this previously planned remedy.
    :param directory: Apply changes in this VASP directory.
    :param history_path: Read and update the remedy history at this path.
    :param durable: Synchronize the published batch when true.
    :return: The committed remedy batch path.
    :raises ValueError: If the decision gives up or its changes are not applicable.
    """

    if decision.give_up:
        raise ValueError(f"cannot apply give-up decision: {decision.reason}")
    root = Path(directory).resolve()
    obstacle = _remedy_obstacle(root, decision.changes)
    if obstacle is not None:
        raise ValueError(f"cannot apply the {decision.policy} remedy for {decision.problem}: {obstacle}")
    staging = Path(tempfile.mkdtemp(prefix=".httk-vasp-remedy.", dir=root))
    try:
        changed: dict[str, Path] = {}
        before: dict[str, str] = {}
        incar_source = root / "INCAR"
        if incar_source.is_file():
            shutil.copy2(incar_source, staging / "INCAR")
            before["INCAR"] = sha256_file(incar_source)
        kpoints_source = root / "KPOINTS"
        if kpoints_source.is_file():
            shutil.copy2(kpoints_source, staging / "KPOINTS")
            before["KPOINTS"] = sha256_file(kpoints_source)
        poscar_source = root / "POSCAR"
        if poscar_source.is_file():
            shutil.copy2(poscar_source, staging / "POSCAR")
            before["POSCAR"] = sha256_file(poscar_source)
        incar_updates: dict[str, object] = {}
        for operation, value in decision.changes:
            if operation.startswith("incar."):
                incar_updates[operation.removeprefix("incar.")] = value
            elif operation == "scale_ediff":
                tags = read_incar(staging / "INCAR")
                if "EDIFF" not in tags:
                    raise ValueError("cannot scale absent EDIFF")
                incar_updates["EDIFF"] = float(tags["EDIFF"]) * float(str(value))
            elif operation == "bump_bands":
                tags = read_incar(staging / "INCAR")
                if "NBANDS" not in tags:
                    raise ValueError("cannot bump absent NBANDS")
                bands = int(tags["NBANDS"]) + int(str(value))
                if "NPAR" in tags:
                    divisor = int(tags["NPAR"])
                    if divisor < 1:
                        raise ValueError("NPAR must be positive")
                    remainder = bands % divisor
                    if remainder:
                        bands += divisor - remainder
                incar_updates["NBANDS"] = bands
            elif operation in {"bump_kpoints", "equal_kpoints", "centering"}:
                _modify_kpoints(staging / "KPOINTS", operation, value)
                changed["KPOINTS"] = staging / "KPOINTS"
            elif operation == "scale_lattice":
                scale_poscar_lattice(float(str(value)), staging / "POSCAR")
                changed["POSCAR"] = staging / "POSCAR"
            elif operation == "contcar_to_poscar":
                contcar_to_poscar(root / "CONTCAR", reference=root / "POSCAR", output=staging / "POSCAR")
                changed["POSCAR"] = staging / "POSCAR"
            else:
                raise ValueError(f"unsupported remedy operation: {operation}")
        if incar_updates:
            update_incar(incar_updates, staging / "INCAR")
            changed["INCAR"] = staging / "INCAR"
        history_file = _remedy_history_file(root, history_path)
        history = _read_remedy_history(root, history_path)
        attempts = dict(history.get("attempts", {}))
        attempts[decision.problem] = decision.step + 1
        events = list(history.get("events", []))
        evidence = [
            {
                "path": name,
                "before_sha256": before.get(name),
                "after_sha256": sha256_file(source),
            }
            for name, source in sorted(changed.items())
        ]
        events.append(
            {
                **decision.as_mapping(),
                "applied_at": utc_now(),
                "files": evidence,
            }
        )
        history.update({"attempts": attempts, "events": events})
        batch = ReplayableWorkdirBatch.create(root, durable=durable)
        for index, (name, source) in enumerate(sorted(changed.items())):
            batch.transaction.put_file(f"input-{index}", source, name)
        if history_file.is_relative_to(root):
            staged_history = staging / REMEDY_HISTORY_NAME
            # Staged into the batch, whose seal synchronizes the whole tree.
            write_json_atomic(staged_history, history, durable=False)
            batch.transaction.put_file("remedy-history", staged_history, history_file.relative_to(root).as_posix())
        else:
            write_json_atomic(history_file, history, durable=durable)
        return batch.commit()
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _modify_kpoints(path: Path, operation: str, value: object) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 4:
        raise ValueError("KPOINTS has fewer than four lines")
    if operation == "centering":
        lines[2] = str(value)
    else:
        grid = [int(item) for item in lines[3].split()[:3]]
        if len(grid) != 3:
            raise ValueError("KPOINTS grid line is invalid")
        if operation == "bump_kpoints":
            grid = [item + int(str(value)) for item in grid]
        elif operation == "equal_kpoints" and bool(value):
            grid = [max(grid)] * 3
        lines[3] = " ".join(str(item) for item in grid)
    _write_text_atomic(path, "\n".join(lines) + "\n")
