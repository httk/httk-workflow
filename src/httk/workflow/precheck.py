"""Read-only readiness checks for jobs before an attempt starts."""

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import replace
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import Path, PurePosixPath

from ._manager_runners import check_runner_reference, contained, runner_module_allowed
from ._util import sha256_file, tree_digest
from .errors import WorkflowError
from .models import STATE_KINDS, JobDefinition, Marker, parse_package_runner
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
    return None if problem is None else ("problem", problem)


def _finding(
    workspace: Workspace,
    marker: Marker,
    settings: Mapping[str, object],
    runner_search_paths: Iterable[str | Path],
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
    for entry in workspace.scan_marker_entries(selected):
        if not isinstance(entry, Marker):
            continue
        if prefix is not None and entry.placement.parts[: len(prefix)] != prefix:
            continue
        yield _finding(workspace, entry, current_settings, search_paths)


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
