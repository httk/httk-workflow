"""Readiness of the workflows a job declared it calls.

A job whose workflow declares ``[workflow.calls]`` records each target in its
``job.json``. Before claiming such a job a manager checks, on its own machine,
that every target resolves and, when it is a compiled package, is built for
this workspace, so a job never starts only to have its first ``call`` fail. The
check only looks things up: a git URI resolves through the installed cache and
never fetches, and nothing is built.
"""

import time
from pathlib import PurePosixPath

from .errors import WorkflowError
from .models import JobDefinition
from .workspace import Workspace

# Answers are cached per workspace and reference so scheduling ticks stay
# cheap. An unready answer is looked at again after _RECHECK_SECONDS, with the
# plugin and installed-workflow discovery refreshed first, so installing or
# building a target while a manager runs is picked up; a ready answer is
# re-confirmed after _READY_SECONDS, so an upgraded target (a new source digest,
# and so a new build) is noticed too.
_RECHECK_SECONDS = 15.0
_READY_SECONDS = 60.0
_READY: dict[tuple[str, str], float] = {}
_UNREADY: dict[tuple[str, str], tuple[float, str]] = {}
_LAST_REFRESH = [0.0]


def unready_calls(workspace: Workspace, job: JobDefinition) -> tuple[str, ...]:
    """Return one problem per declared call target that is not ready on this machine.

    A target is ready when it resolves here, is built in this workspace when it is
    a compiled package, and every workflow it declares it calls is ready in turn.

    :param workspace: The workspace the job would run in.
    :param job: The job definition.
    :return: Human-readable problems, empty when every target is ready.
    """

    if not job.calls:
        return ()
    return tuple(
        f"call {alias} ({reference}): {problem}"
        for alias, reference in sorted(job.calls.items())
        if (problem := call_problem(workspace, reference)) is not None
    )


def call_problem(workspace: Workspace, reference: str) -> str | None:
    """Return why one call target is not ready on this machine, or ``None``.

    :param workspace: The workspace a job calling it would run in.
    :param reference: The workflow name or pinned git URI the job recorded.
    :return: The problem, or ``None`` when the target is ready.
    """

    key = (str(workspace.root), reference)
    now = time.monotonic()
    confirmed = _READY.get(key)
    if confirmed is not None and now - confirmed < _READY_SECONDS:
        return None
    cached = _UNREADY.get(key)
    if cached is not None and now - cached[0] < _RECHECK_SECONDS:
        return cached[1]
    if cached is not None and now - _LAST_REFRESH[0] >= _RECHECK_SECONDS:
        _refresh_discovery()
        _LAST_REFRESH[0] = now
    problem = _check(workspace, reference, {reference})
    if problem is None:
        _READY[key] = now
        _UNREADY.pop(key, None)
    else:
        _READY.pop(key, None)
        _UNREADY[key] = (now, problem)
    return problem


def _refresh_discovery() -> None:
    """Forget the process's plugin and installed-workflow discovery, so new installs are seen."""

    from .git_workflows import _reset_fetched_workflow_cache
    from .packages import _reset_plugin_workflow_cache

    _reset_plugin_workflow_cache()
    _reset_fetched_workflow_cache()


def _check(workspace: Workspace, reference: str, seen: set[str]) -> str | None:
    from ._runner_builds import platform_tag, registered_artifacts
    from .packages import read_build_spec, source_tree_digest
    from .scaffold import workflow_provider

    try:
        provider = workflow_provider(reference)
    except ValueError as exc:
        return str(exc)
    if provider is None:
        install = f"httk workflow install '{reference}'" if reference.startswith("git+") else "install or register it"
        return f"not known on this machine; {install}"
    if not provider.runnable:
        return f"{provider.workflow_id} recognizes calculations and cannot be run"
    directory = provider.directory
    # Only a directory package runs from the workspace runner store, where a
    # compiled one needs a build; a language document or a packaged runner file
    # never does.
    if directory is not None and provider.language is None and provider.runner_package is None:
        try:
            spec = read_build_spec(directory)
            if spec is not None:
                digest = source_tree_digest(directory)
                tag = platform_tag(spec)
                store = PurePosixPath(f"{directory.name}.{digest[:12]}")
                if registered_artifacts(workspace, store, tag, expected_source_sha256=digest) is None:
                    return (
                        f"not built for platform {tag} in this workspace; run: "
                        f"httk workflow build --workspace {workspace.root} '{reference}'"
                    )
        except (WorkflowError, ValueError, OSError) as exc:
            return f"cannot be checked: {exc}"
    for alias, called in sorted((provider.calls or {}).items()):
        if called in seen:
            continue
        seen.add(called)
        problem = _check(workspace, called, seen)
        if problem is not None:
            return f"its call {alias} ({called}): {problem}"
    return None


def reset_call_readiness() -> None:
    """Forget every cached readiness answer (for tests and long-lived tools)."""

    _READY.clear()
    _UNREADY.clear()
    _LAST_REFRESH[0] = 0.0
