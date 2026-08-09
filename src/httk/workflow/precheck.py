"""Read-only readiness checks for jobs before an attempt starts."""

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import replace
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import Path, PurePosixPath

from httk.core.digests import sha256_file, tree_digest

from . import languages
from ._manager_runners import check_runner_reference, contained, runner_module_allowed
from .errors import WorkflowError
from .introspection._diagnosis import ManagerRecord, claim_requirements, manager_refusals, read_managers
from .models import STATE_KINDS, JobDefinition, Marker, parse_package_runner
from .scaffold import payload_relative
from .sdk import resolve_declared_environment
from .workspace import Workspace

ENVIRONMENT_VARIABLE_CAVEAT = (
    "HTTK_* environment variables are read from this process; compute-node environments may differ."
)
DEFAULT_PRECHECK_STATES = ("submitted", "ready", "waiting", "paused")


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
            values, unresolved = resolve_declared_environment(
                single_job,
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


def _runner_problem(
    workspace: Workspace,
    marker: Marker,
    job: JobDefinition,
    runner_search_paths: Iterable[str | Path],
) -> tuple[str, str] | None:
    """Check a runner, using a non-importing path for packaged runners."""

    if job.runner_source == "installed":
        package = parse_package_runner(job.runner_path.as_posix())
        if package is None and not tuple(runner_search_paths):
            return (
                "indeterminate",
                f"installed runner {job.runner_path.as_posix()} needs --runner-search-path to be checked",
            )
        if package is not None:
            module, resource = package
            if not runner_module_allowed(module):
                return "problem", f"runner module {module} is not in this precheck's allowlist"
            try:
                spec = _find_module_spec_without_import(module)
            except (ImportError, ModuleNotFoundError, ValueError) as exc:
                return "problem", f"runner module {module} cannot be inspected: {exc}"
            if spec is None:
                return "problem", f"runner module {module} is not importable"
            locations = spec.submodule_search_locations
            root = Path(next(iter(locations))) if locations is not None else None
            if root is None and spec.origin is not None:
                root = Path(spec.origin).parent
            if root is None:
                return "problem", f"runner module {module} has no filesystem location"
            candidate = contained(root, resource.parts)
            if candidate is None or not candidate.exists():
                return "problem", f"installed runner pkg:{module}/{resource.as_posix()} does not exist"
            actual = tree_digest(candidate) if candidate.is_dir() else sha256_file(candidate)
            if actual != job.runner_sha256:
                return "problem", f"runner digest {actual} does not match pinned {job.runner_sha256}"
            return None

    problem = check_runner_reference(
        workspace,
        job,
        placement=marker.placement,
        runner_search_paths=runner_search_paths,
    )
    if problem is not None:
        return "problem", problem
    if job.runner_source == "workspace":
        candidate = workspace.runner_store_path(job.runner_path)
        try:
            from ._runner_builds import registered_artifacts, workspace_build_command
            from .packages import read_build_spec

            build_spec = read_build_spec(candidate) if candidate.is_dir() else None
        except ValueError as exc:
            return "problem", f"published runner manifest is malformed: {exc}"
        if build_spec is not None:
            if build_spec.platform is not None:
                return (
                    "indeterminate",
                    (
                        "declares platform-specific builds; registration is checked at manager start — run: "
                        f"{workspace_build_command(workspace, job.runner_path)}"
                    ),
                )
            if (
                registered_artifacts(
                    workspace,
                    job.runner_path,
                    "any",
                    expected_source_sha256=job.runner_sha256,
                )
                is None
            ):
                return (
                    "problem",
                    (
                        f"workflow package {job.runner_path.as_posix()} is not built on this machine for platform any; "
                        f"run: {workspace_build_command(workspace, job.runner_path)}"
                    ),
                )
    return None


def _claim_finding(
    marker: Marker,
    job: JobDefinition,
    managers: Sequence[ManagerRecord],
) -> dict[str, object] | None:
    """Return a claimability problem when no live manager could claim *job*.

    Every live manager is measured against the same refusal checks ``job why``
    renders — executor, pool, capabilities, placement, ownership, and runner
    reachability — and the closest manager's unmet requirements name what to
    fix. When no manager is live at all, claimability cannot be judged here, so
    this is left to the workspace-level notice.

    :param marker: The authoritative marker of the job.
    :param job: The parsed job definition.
    :param managers: Every manager registered in the workspace.
    :return: A claim finding, or ``None`` when a live manager could claim it.
    """

    live = [record for record in managers if record.alive()]
    if not live:
        return None
    requirements = claim_requirements(job)
    placement = marker.placement.as_posix()
    try:
        owner_uid: int | None = marker.path.lstat().st_uid
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


def _language_finding(job: JobDefinition, managers: Sequence[ManagerRecord]) -> dict[str, object] | None:
    """Return an engine-importability finding for a language job, if any.

    A language job is the one the collect gate recognizes: ``workflow_realization``
    is ``language`` and ``workflow_language`` names the engine. The engine is
    resolved without importing its runtime; only the non-importing spec finder
    checks that each module is present, and the pip extra is named. Because the
    extras belong on the machine that runs the job, a missing module is only a
    problem when no live manager serves this job's executor; when one does, its
    environment may differ from this process's, so it is reported as
    ``indeterminate`` and does not fail the run.

    :param job: The parsed job definition.
    :param managers: Every manager registered in the workspace.
    :return: A language finding, or ``None`` when nothing is missing.
    """

    if job.parameters.get("workflow_realization") != "language":
        return None
    name = job.parameters.get("workflow_language")
    if not isinstance(name, str):
        return None
    try:
        language = languages.language(name)
    except ValueError:
        problem = f"workflow language {name!r} is not available in this installation"
        return {"status": "problem", "problem": problem}
    missing = [module for module in language.required_modules if _find_module_spec_without_import(module) is None]
    if not missing:
        return None
    problem = (
        f"workflow language {language.name} needs Python module(s) {', '.join(missing)}; "
        f"install them with 'pip install httk-workflow[{language.name}]'"
    )
    if language.name == "jobflow":
        problem += " (pymatgen is additionally required when the workflow has structure inputs)"
    served = any(record.alive() and job.runner_executor in record.executors for record in managers)
    if served:
        return {
            "status": "indeterminate",
            "problem": problem
            + "; the engine could not be found in this process, but the serving manager's environment may "
            "differ — this is verified only at run time",
        }
    return {"status": "problem", "problem": problem}


def _input_problems(workspace: Workspace, marker: Marker, job: JobDefinition) -> list[str]:
    """Return one problem per required declared input missing from the payload.

    A declared required input with a staged ``destination`` must still be a
    member of the payload; an absent one is a tamper or relocation the runner
    would only discover mid-attempt.

    :param workspace: The workspace holding the payload.
    :param marker: The authoritative marker of the job.
    :param job: The parsed job definition.
    :return: Human-readable problems, one per missing required destination.
    """

    declared_inputs = job.declared.get("inputs", {})
    if not declared_inputs:
        return []
    payload = workspace.payload_path(marker.placement, marker.job_key)
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


def _step_finding(workspace: Workspace, marker: Marker, job: JobDefinition) -> str | None:
    """Return a step problem when the job's step is outside its recorded step set.

    A runner records the steps it actually implements in the job's state frame as
    ``runner_steps`` after its first attempt. When that list is present and the
    step this job would run next is not in it, the next attempt cannot succeed.
    This is frame-based only: the runner is never executed, so a job that has not
    recorded its steps yet is never faulted here. The frame reflects the last
    attempt's runner, so the finding is advisory — a mutated payload runner may
    implement a different set by the next attempt.

    :param workspace: The workspace holding the job's state frame.
    :param marker: Identify the job to check.
    :param job: The job's immutable definition, for its initial step.
    :return: A step problem message, or ``None`` when nothing can be faulted.
    """

    try:
        state = workspace.read_state(marker)
    except (WorkflowError, OSError):
        return None
    runner_steps = state.get("runner_steps")
    if not isinstance(runner_steps, list) or not runner_steps:
        return None
    known = [str(item) for item in runner_steps]
    step = state.get("step")
    step = str(step) if isinstance(step, str) and step else job.initial_step
    if step in known:
        return None
    return f"step {step!r} is not one of the runner's recorded steps: {', '.join(known)}"


def _finding(
    workspace: Workspace,
    marker: Marker,
    settings: Mapping[str, object],
    runner_search_paths: Iterable[str | Path],
    managers: Sequence[ManagerRecord],
) -> dict[str, object]:
    """Build one finding from one current marker."""

    try:
        job = workspace.load_job(marker)
    except (WorkflowError, OSError) as exc:
        return {
            "job_key": marker.job_key,
            "job_id": marker.job_id,
            "workflow": None,
            "state": marker.kind,
            "placement": marker.placement.as_posix(),
            "environment": [],
            "environment_problems": [str(exc)],
            "runner": {"problem": str(exc)},
            "claim": None,
            "language": None,
            "inputs": [],
            "step": None,
        }
    environment = environment_findings(job, settings)
    runner_problem = _runner_problem(workspace, marker, job, runner_search_paths)
    runner: dict[str, object] = {"status": "ok", "ok": True}
    if runner_problem is not None:
        status, problem = runner_problem
        runner = {"status": status, "problem": problem}
    return {
        "job_key": job.job_key,
        "job_id": job.id,
        "workflow": job.workflow,
        "state": marker.kind,
        "placement": marker.placement.as_posix(),
        "environment": environment["entries"],
        "environment_problems": environment["problems"],
        "runner": runner,
        "claim": _claim_finding(marker, job, managers),
        "language": _language_finding(job, managers),
        "inputs": _input_problems(workspace, marker, job),
        "step": _step_finding(workspace, marker, job),
    }


def precheck_jobs(
    workspace: Workspace,
    *,
    states: Iterable[str] = DEFAULT_PRECHECK_STATES,
    placement: str | PurePosixPath | None = None,
    settings: Mapping[str, object] | None = None,
    runner_search_paths: Iterable[str | Path] = (),
) -> Iterator[dict[str, object]]:
    """Yield read-only environment and runner findings for pending jobs.

    :param workspace: Workspace to inspect.
    :param states: Current marker kinds to inspect.
    :param placement: Optional placement subtree.
    :param settings: Destination settings, or the workspace's current settings.
    :param runner_search_paths: Roots for plain installed runner references.
    :yields: Lazy per-job findings.
    """

    selected = tuple(dict.fromkeys(states))
    unknown = [state for state in selected if state not in STATE_KINDS]
    if unknown:
        raise ValueError(f"unknown precheck state kind: {', '.join(unknown)}")
    prefix = None if placement is None else PurePosixPath(placement).parts
    current_settings = workspace.read_settings() if settings is None else settings
    search_paths = tuple(runner_search_paths)
    managers = read_managers(workspace)
    for entry in workspace.scan_marker_entries(selected):
        if not isinstance(entry, Marker):
            continue
        if prefix is not None and entry.placement.parts[: len(prefix)] != prefix:
            continue
        yield _finding(workspace, entry, current_settings, search_paths, managers)


def manager_availability_notice(workspace: Workspace) -> str | None:
    """Return one workspace-level manager-availability notice, or ``None``.

    Per-job claimability can only be judged against a live manager. When none is
    live, one notice replaces per-job claim spam: whether no manager ever
    registered, or every registered one has a stale heartbeat.

    :param workspace: The workspace to inspect.
    :return: The notice, or ``None`` when a live manager exists.
    """

    managers = read_managers(workspace)
    if any(record.alive() for record in managers):
        return None
    if not managers:
        return "no manager has ever registered in this workspace; claimability was not checked"
    return f"{len(managers)} manager(s) registered here, but none has a live heartbeat; claimability was not checked"


def has_claim_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names a job no live manager can claim."""

    claim = finding.get("claim")
    return isinstance(claim, Mapping) and claim.get("status") == "problem"


def has_language_problem(finding: Mapping[str, object]) -> bool:
    """Return whether a finding names an unimportable language engine."""

    language = finding.get("language")
    return isinstance(language, Mapping) and language.get("status") == "problem"


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
    """Return whether a finding has a broken runner reference."""

    runner = finding.get("runner")
    return isinstance(runner, Mapping) and runner.get("status", "problem") == "problem"
