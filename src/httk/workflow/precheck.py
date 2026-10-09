"""Read-only readiness checks for jobs before an attempt starts."""

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import replace
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import PurePosixPath
from typing import Any, cast

from . import _store, compat
from ._job import JobDefinition
from ._kernel import JobRef
from ._manager_scheduling import unmet_job_requirements
from .errors import RunnerResolutionError, WorkflowError
from .introspection import JOB_STATES, iter_jobs, job_placement, read_job, read_state
from .introspection._diagnosis import ManagerRecord, claim_requirements, manager_refusals, read_managers
from .models import placement_text
from .scaffold import payload_relative
from .sdk import resolve_declared_environment
from .workspace import Workspace

ENVIRONMENT_VARIABLE_CAVEAT = (
    "HTTK_* environment variables are read from this process; compute-node environments may differ."
)
DEFAULT_PRECHECK_STATES = ("ready", "waiting", "paused")


def _find_module_spec_without_import(module: str) -> ModuleSpec | None:
    """Find a module by path without importing any parent package."""

    parent_locations: Sequence[str] | None = None
    qualified = ""
    parts = module.split(".")
    spec: ModuleSpec | None = None
    for index, part in enumerate(parts):
        qualified = part if not qualified else f"{qualified}.{part}"
        spec = PathFinder.find_spec(qualified, parent_locations)
        if spec is None:
            return None
        if index < len(parts) - 1:
            if spec.submodule_search_locations is None:
                return None
            parent_locations = spec.submodule_search_locations
    return spec


def _environment_entries(
    job: JobDefinition,
    settings: Mapping[str, object],
    *,
    include_process_environment: bool,
) -> tuple[list[dict[str, object]], list[str]]:
    """Return sanitized environment findings and resolution problems."""

    declared = job.environment.get("declared", {})
    if not isinstance(declared, Mapping):
        return [], ["declared environment is malformed"]
    overrides = job.environment.get("overrides", {})
    entries: list[dict[str, object]] = []
    for name in sorted(declared):
        metadata = declared[name]
        setting = metadata.get("setting", name) if isinstance(metadata, Mapping) else name
        entry = {"name": name, "setting": setting}
        single_overrides = {name: overrides[name]} if isinstance(overrides, Mapping) and name in overrides else {}
        single_job = replace(
            job,
            environment={"declared": {name: metadata}, "overrides": single_overrides},
        )
        try:
            # C3: the SDK's environment resolution reads only ``environment``; it moves to the v3 job model there.
            values, unresolved = resolve_declared_environment(
                cast(Any, single_job),
                settings,
                include_process_environment=include_process_environment,
            )
        except ValueError as exc:
            entry.update({"status": "unresolved", "source": None, "problem": str(exc)})
            entries.append(entry)
            continue
        item = values.get(name)
        source = item.get("source") if item is not None else None
        entry["source"] = source
        entry["status"] = "unresolved" if name in unresolved else "default" if source == "default" else "resolved"
        entries.append(entry)
    return entries, [str(entry["problem"]) for entry in entries if "problem" in entry]


def environment_findings(
    job: JobDefinition,
    settings: Mapping[str, object],
    *,
    include_process_environment: bool = True,
) -> dict[str, object]:
    """Return the destination-specific environment half of one precheck.

    :param job: The immutable job definition to inspect.
    :param settings: The destination workspace application settings.
    :param include_process_environment: Include this process's environment-variable layer.
    :return: Environment entries and any type/resolution problems.
    """

    entries, problems = _environment_entries(
        job,
        settings,
        include_process_environment=include_process_environment,
    )
    return {"entries": entries, "problems": problems}


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _workflow_problems(
    workspace: Workspace, job: JobDefinition
) -> tuple[tuple[str, str] | None, list[str], _store.Installed | None]:
    """Check the job's workflow and its call closure: installed here, and built for this platform.

    :param workspace: The workspace.
    :param job: The job.
    :return: The root workflow's ``(status, problem)`` or ``None``, one problem per callee, and the installation.
    """

    try:
        closure = _store.closure(workspace, job.workflow_id, check_builds=True)
        installed = _store.lookup(workspace, job.workflow_id)
    except (RunnerResolutionError, ValueError) as exc:
        return ("problem", f"workflow {job.workflow_id} cannot be checked: {exc}"), [], None
    root: tuple[str, str] | None = None
    calls: list[str] = []
    for missing in closure.missing:
        problem = f"workflow {missing} is not installed in this workspace; run 'httk workflow install'"
        if missing == job.workflow_id:
            root = ("problem", problem)
        else:
            calls.append(f"called {problem}")
    for unbuilt in closure.unbuilt:
        problem = f"workflow {unbuilt} is not built for this platform; run 'httk workflow build {unbuilt}'"
        if unbuilt == job.workflow_id:
            root = ("problem", problem)
        else:
            calls.append(f"called {problem}")
    return root, calls, installed


def _claim_finding(
    ref: JobRef,
    job: JobDefinition,
    managers: Sequence[ManagerRecord],
) -> dict[str, object] | None:
    """Return a claimability problem when no live manager could claim *job*.

    Every manager not proven dead is measured against the same refusal checks
    ``job why`` renders — pool, capabilities, placement, ownership and the
    allocation's end — and the closest manager's unmet requirements name what to
    fix. When no manager is live at all, claimability cannot be judged here, so
    this is left to the workspace-level notice.

    :param ref: The job.
    :param job: The parsed job definition.
    :param managers: Every manager registered in the workspace.
    :return: A claim finding, or ``None`` when a live manager could claim it.
    """

    live = [record for record in managers if record.alive()]
    if not live:
        return None
    requirements = claim_requirements(job)
    placement = placement_text(job.placement)
    try:
        owner_uid: int | None = ref.path.lstat().st_uid
    except OSError:
        owner_uid = None
    closest: list[str] | None = None
    for record in live:
        reasons = manager_refusals(record, requirements, placement=placement, owner_uid=owner_uid, job=job)
        if not reasons:
            return None
        if closest is None or len(reasons) < len(closest):
            closest = reasons
    return {"status": "problem", "problem": "no live manager can claim this job: " + "; ".join(closest or ())}


def _language_finding(
    installed: _store.Installed | None, managers: Sequence[ManagerRecord]
) -> dict[str, object] | None:
    """Return an engine-importability finding for a job of a built-in language realization, if any.

    The realization is the installed workflow's ``runner.builtin``. The engine is
    resolved without importing its runtime; only the non-importing spec finder
    checks that each module is present, and the pip extra is named. Because the
    extras belong on the machine that runs the job, a missing module is only a
    problem when no manager is live; otherwise its environment may differ from
    this process's, so it is reported as ``indeterminate``.

    :param installed: The job's installed workflow, when installed.
    :param managers: Every manager registered in the workspace.
    :return: A language finding, or ``None`` when nothing is missing.
    """

    runner = None if installed is None else installed.record.get("runner")
    name = runner.get("builtin") if isinstance(runner, Mapping) else None
    if not isinstance(name, str):
        return None
    try:
        language = compat.language(name)
    except ValueError:
        return {"status": "problem", "problem": f"workflow format {name!r} is not available in this installation"}
    missing = [module for module in language.required_modules if _find_module_spec_without_import(module) is None]
    if not missing:
        return None
    problem = (
        f"workflow format {language.name} needs Python module(s) {', '.join(missing)}; "
        f"install them with 'pip install httk-workflow[{language.name}]'"
    )
    if language.name == "jobflow":
        problem += " (pymatgen is additionally required when the workflow has structure inputs)"
    if any(record.alive() for record in managers):
        return {
            "status": "indeterminate",
            "problem": problem + "; the engine could not be found in this process, but a manager's environment may "
            "differ — this is verified only at run time",
        }
    return {"status": "problem", "problem": problem}


def _requirements_finding(
    installed: _store.Installed | None, managers: Sequence[ManagerRecord]
) -> dict[str, object] | None:
    """Return a finding for the installed manifest's ``requires`` this process's environment does not meet.

    A manager checks ``requires`` in its own environment and leaves an unmet job
    unclaimed, so, as for a language engine, a miss here is only a problem when no
    manager is live; otherwise it is ``indeterminate``.

    :param installed: The job's installed workflow, when installed.
    :param managers: Every manager registered in the workspace.
    :return: A requirements finding, or ``None`` when every requirement is met here.
    """

    unmet = unmet_job_requirements(() if installed is None else _strings(installed.record.get("requires")))
    if not unmet:
        return None
    problem = f"unmet requirement(s) {'; '.join(unmet)}"
    if any(record.alive() for record in managers):
        return {
            "status": "indeterminate",
            "problem": problem + " in this process; a manager claims this job only if its own environment meets them",
        }
    return {"status": "problem", "problem": problem}


def _input_problems(ref: JobRef, job: JobDefinition) -> list[str]:
    """Return one problem per required declared input missing from the payload.

    A declared required input with a staged ``destination`` must still be a
    member of the payload; an absent one is a tamper or relocation the runner
    would only discover mid-attempt.

    :param ref: The job.
    :param job: The parsed job definition.
    :return: Human-readable problems, one per missing required destination.
    """

    declared_inputs = job.declared.get("inputs", {})
    if not isinstance(declared_inputs, Mapping) or not declared_inputs:
        return []
    payload = ref.path
    problems: list[str] = []
    for name in sorted(declared_inputs):
        metadata = declared_inputs[name]
        if not (isinstance(metadata, Mapping) and metadata.get("required")):
            continue
        destination = metadata.get("destination")
        if not isinstance(destination, str):
            continue
        try:
            member = payload.joinpath(*payload_relative(destination).parts)
        except (WorkflowError, ValueError):
            problems.append(f"required input {name!r} declares an invalid destination {destination!r}")
            continue
        if not member.exists():
            problems.append(f"required input {name!r} is missing its staged destination {destination}")
    return problems


def _step_finding(ref: JobRef, job: JobDefinition) -> str | None:
    """Return a step problem when the job's next step is outside the runner's recorded step set.

    An outcome records the steps its runner implements as ``runner_steps`` in
    ``state.json``. When that list is present and the step this job would run
    next is not in it, the next attempt cannot succeed. The runner is never
    executed, so a job that has not recorded its steps yet is never faulted
    here; the finding is advisory.

    :param ref: The job.
    :param job: The job's immutable definition, for its initial step.
    :return: A step problem message, or ``None`` when nothing can be faulted.
    """

    doc, _ = read_state(ref)
    known = [] if doc is None else list(_strings(doc.runner_steps))
    if not known:
        return None
    step = None if doc is None or doc.activation is None else doc.activation.get("step")
    step = step if isinstance(step, str) and step else job.initial_step
    if step in known:
        return None
    return f"step {step!r} is not one of the runner's recorded steps: {', '.join(known)}"


def _finding(
    workspace: Workspace,
    ref: JobRef,
    settings: Mapping[str, object],
    managers: Sequence[ManagerRecord],
) -> dict[str, object]:
    """Build one finding from one job."""

    job, error = read_job(ref)
    if job is None:
        placement = job_placement(ref)
        return {
            "job_key": ref.job_key,
            "job_id": ref.job_id,
            "workflow": None,
            "state": ref.state,
            "placement": None if placement is None else placement_text(placement),
            "environment": [],
            "environment_problems": [str(error)],
            "runner": {"status": "problem", "problem": str(error)},
            "claim": None,
            "language": None,
            "requirements": None,
            "calls": [],
            "inputs": [],
            "step": None,
        }
    environment = environment_findings(job, settings)
    workflow_problem, call_problems, installed = _workflow_problems(workspace, job)
    runner: dict[str, object] = {"status": "ok", "ok": True}
    if workflow_problem is not None:
        status, problem = workflow_problem
        runner = {"status": status, "problem": problem}
    return {
        "job_key": job.job_key,
        "job_id": job.id,
        "workflow": job.workflow_id,
        "state": ref.state,
        "placement": placement_text(job.placement),
        "environment": environment["entries"],
        "environment_problems": environment["problems"],
        "runner": runner,
        "claim": _claim_finding(ref, job, managers),
        "language": _language_finding(installed, managers),
        "requirements": _requirements_finding(installed, managers),
        "calls": call_problems,
        "inputs": _input_problems(ref, job),
        "step": _step_finding(ref, job),
    }


def precheck_jobs(
    workspace: Workspace,
    *,
    states: Iterable[str] = DEFAULT_PRECHECK_STATES,
    placement: str | PurePosixPath | None = None,
    settings: Mapping[str, object] | None = None,
) -> Iterator[dict[str, object]]:
    """Yield read-only environment, workflow and claimability findings for pending jobs.

    :param workspace: Workspace to inspect.
    :param states: The job states to inspect.
    :param placement: Optional placement subtree.
    :param settings: Destination settings, or the workspace's current settings.
    :yields: Lazy per-job findings.
    :raises ValueError: For an unknown state.
    """

    selected = tuple(dict.fromkeys(states))
    if unknown := [state for state in selected if state not in JOB_STATES]:
        raise ValueError(f"unknown precheck state: {', '.join(unknown)}")
    current_settings = workspace.read_settings() if settings is None else settings
    managers = read_managers(workspace)
    for ref in iter_jobs(workspace, selected, placement_prefix=placement):
        yield _finding(workspace, ref, current_settings, managers)


def manager_availability_notice(workspace: Workspace) -> str | None:
    """Return one workspace-level manager-availability notice, or ``None``.

    Per-job claimability can only be judged against a manager that is not proven
    dead. When none is, one notice replaces per-job claim spam: whether no
    manager is registered, or every registered one is proven dead.

    :param workspace: The workspace to inspect.
    :return: The notice, or ``None`` when a live manager exists.
    """

    managers = read_managers(workspace)
    if any(record.alive() for record in managers):
        return None
    if not managers:
        return "no manager is registered in this workspace; claimability was not checked"
    return f"{len(managers)} manager(s) registered here, all proven dead; claimability was not checked"


def has_claim_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names a job no live manager can claim."""

    claim = finding.get("claim")
    return isinstance(claim, Mapping) and claim.get("status") == "problem"


def has_language_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names an unimportable language engine."""

    language = finding.get("language")
    return isinstance(language, Mapping) and language.get("status") == "problem"


def has_requirements_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names unmet ``requires`` with no live manager to judge them."""

    requirements = finding.get("requirements")
    return isinstance(requirements, Mapping) and requirements.get("status") == "problem"


def has_call_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names a declared called workflow not ready here."""

    return bool(finding.get("calls"))


def has_input_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names a missing required input destination."""

    return bool(finding.get("inputs"))


def has_step_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names a step outside the runner's recorded set."""

    return bool(finding.get("step"))


def has_environment_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding has an unresolved or invalid environment."""

    entries = finding.get("environment", [])
    unresolved = (
        any(isinstance(item, Mapping) and item.get("status") == "unresolved" for item in entries)
        if isinstance(entries, Iterable) and not isinstance(entries, (str, bytes))
        else False
    )
    return unresolved or bool(finding.get("environment_problems"))


def has_runner_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding's workflow is not installed or not built here."""

    runner = finding.get("runner")
    return isinstance(runner, Mapping) and runner.get("status", "problem") == "problem"
