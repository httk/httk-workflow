"""The foreground workflow runner build command."""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from httk.core.building import BuildSpec
from httk.core.cli import CLIContext

from .._runner_builds import register_build
from ..errors import FormatError
from ..introspection import JobSelectorResolver
from ..packages import MANIFEST_NAME, load_workflow_package, read_build_spec, source_tree_digest
from ..scaffold import WorkflowProvider, workflow_provider
from ..workspace import Workspace
from ._common import _ERRORS, _add_by_path_argument, _leaf, _local_root


def _workspace(arguments: argparse.Namespace, context: CLIContext) -> Workspace:
    return Workspace(_local_root(arguments, context, action="build workflow runners in it"))


def _registration_rows(workspace: Workspace) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not workspace.runner_builds.is_dir():
        return rows
    for pointer in sorted(workspace.runner_builds.rglob("current.json")):
        try:
            current = json.loads(pointer.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        generation_name = current.get("generation") if isinstance(current, Mapping) else None
        if not isinstance(generation_name, str) or not generation_name.startswith("gen-"):
            continue
        generation = pointer.parent / generation_name
        stamp = generation / "build.json"
        try:
            value = json.loads(stamp.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if (
            generation.parent != pointer.parent
            or not (generation / "artifacts").is_dir()
            or not isinstance(value, Mapping)
            or value.get("format") != "httk-workflow-runner-build"
            or value.get("format_version") != 2
        ):
            continue
        store = pointer.parent.parent.relative_to(workspace.runner_builds).as_posix()
        rows.append({"store": store, "tag": pointer.parent.name, "built_at": value.get("built_at")})
    return rows


def _list_builds(workspace: Workspace, *, as_json: bool) -> int:
    rows = _registration_rows(workspace)
    if as_json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for row in rows:
            print(f"{row['store']}\t{row['tag']}\t{row['built_at'] or '-'}")
    return 0


def _resolve_job_target(
    workspace: Workspace,
    target: str,
    *,
    base: Path,
    resolver: JobSelectorResolver | None = None,
) -> tuple[Path, PurePosixPath, BuildSpec, str]:
    """Resolve a job target and return its pinned workspace runner."""

    selected = resolver if resolver is not None else JobSelectorResolver(workspace, base)
    markers = selected.resolve_one(target)
    if len(markers) != 1:
        raise ValueError("workflow build job target must resolve to one job")
    marker = markers[0]
    job = workspace.load_job(marker)
    if job.runner_source != "workspace":
        raise ValueError(f"job {target} does not reference a workspace workflow package")
    store_path = workspace.runner_store_path(job.runner_path)
    spec = read_build_spec(store_path)
    if spec is None:
        raise ValueError(f"target {job.runner_path.as_posix()} has no [workflow.build] section")
    if job.runner_sha256 is None:
        raise ValueError(f"job {target} has no pinned workspace runner digest")
    return store_path, job.runner_path, spec, job.runner_sha256


def _publish_provider(
    workspace: Workspace, provider: WorkflowProvider, reference: str
) -> tuple[Path, PurePosixPath, BuildSpec, str] | None:
    """Publish a resolved directory package and return its build target, or ``None`` when it has no build.

    The package is published under the digest-pinned name a job selecting the
    same workflow publishes it under, so the registration is found by that job.
    """

    directory = provider.directory
    if directory is None:
        raise ValueError(
            f"workflow {reference!r} ({provider.workflow_id}) is not a directory workflow package; "
            "only a package with a [workflow.build] section can be built"
        )
    try:
        spec = read_build_spec(directory)
    except ValueError as exc:
        raise ValueError(f"workflow {reference!r} package {directory} is malformed: {exc}") from exc
    if spec is None:
        return None
    reference_mapping = workspace.publish_runner(
        directory, name=f"{directory.name}.{source_tree_digest(directory)[:12]}"
    )
    store_relative = PurePosixPath(str(reference_mapping["path"]))
    return workspace.runner_store_path(store_relative), store_relative, spec, str(reference_mapping["sha256"])


def _register(
    workspace: Workspace,
    target: tuple[Path, PurePosixPath, BuildSpec, str],
    *,
    stdout_to_stderr: bool,
) -> dict[str, object]:
    """Build one published runner and return its registration report fields."""

    store_path, store_relative, spec, source_sha256 = target
    artifacts = register_build(
        workspace,
        store_path,
        store_relative,
        spec,
        source_sha256=source_sha256,
        stdout_to_stderr=stdout_to_stderr,
    )
    stamp = json.loads((artifacts.parent / "build.json").read_text(encoding="utf-8"))
    return {
        "status": "registered",
        "store": store_relative.as_posix(),
        "platform": stamp["platform"],
        "platform_output": stamp["platform_output"],
        "platform_tag": stamp["platform_tag"],
        "artifacts": str(artifacts),
        "built_at": stamp["built_at"],
    }


def build_workflow_provider(
    workspace: Workspace,
    provider: WorkflowProvider,
    *,
    reference: str,
    stdout_to_stderr: bool = False,
) -> dict[str, object]:
    """Build one resolved workflow provider in the foreground and register its artifacts.

    This is the unit ``httk workflow build NAME`` applies to a workflow
    reference; it is meant to be applied once per workflow a build covers. A
    directory package without a ``[workflow.build]`` section has nothing to
    build and reports ``status`` ``"nothing-to-build"`` rather than failing.

    :param workspace: Build into this workspace's runner store and registrations.
    :param provider: Build this resolved workflow provider.
    :param reference: Name the provider in messages and in the report, as the user wrote it.
    :param stdout_to_stderr: Send build output to standard error (for JSON reports).
    :return: The report row: ``workflow``, ``package``, ``status`` and, when built, the registration fields.
    :raises ValueError: If the provider is not a directory package or its manifest is malformed.
    :raises RunnerResolutionError: If the build fails.
    """

    row: dict[str, object] = {"workflow": provider.workflow_id, "package": str(provider.directory)}
    published = _publish_provider(workspace, provider, reference)
    if published is None:
        row["status"] = "nothing-to-build"
        return row
    row.update(_register(workspace, published, stdout_to_stderr=stdout_to_stderr))
    return row


def _path_like(target: str) -> bool:
    """Return whether *target* is spelled as a filesystem path rather than a name."""

    return (
        Path(target).expanduser().is_absolute()
        or target in {".", ".."}
        or target.startswith(("./", "../"))
        or os.sep in target
        or (os.altsep is not None and os.altsep in target)
    )


def _workflow_reference(workspace: Workspace, target: str) -> WorkflowProvider | None:
    """Return the provider a workflow reference selects, as ``job new --workflow`` resolves it.

    A git URI is fetched and installed (an explicit URI is consent to fetch).
    Otherwise path spellings, job globs and existing workspace runner-store
    names keep their meaning, and a remaining bare name selects a registered id
    or alias, an installed plugin workflow, or an installed git workflow's short
    name; ``None`` means the target is not a workflow reference.
    """

    if target.startswith("git+"):
        from ..git_workflows import fetch_workflow

        return fetch_workflow(target)
    if _path_like(target) or any(character in target for character in "*?["):
        return None
    try:
        if workspace.runner_store_path(target).is_dir():
            return None
    except FormatError:
        pass
    return workflow_provider(target)


def _resolve_target(
    workspace: Workspace,
    target: str,
    *,
    base: Path,
    resolver: JobSelectorResolver | None = None,
) -> tuple[Path, PurePosixPath, BuildSpec, str]:
    raw_path = Path(target).expanduser()
    candidate = raw_path if raw_path.is_absolute() else base / raw_path
    if any(character in target for character in "*?["):
        return _resolve_job_target(workspace, target, base=base, resolver=resolver)
    if _path_like(target):
        path = raw_path if raw_path.is_absolute() else base / raw_path
        if not path.is_dir():
            raise ValueError(f"workflow package directory does not exist: {path}")
        if (path / MANIFEST_NAME).is_file():
            try:
                spec = read_build_spec(path)
            except ValueError as exc:
                raise ValueError(f"target package is malformed: {exc}") from exc
            if spec is None:
                raise ValueError(f"target {path} has no [workflow.build] section")
            published = _publish_provider(workspace, load_workflow_package(path, register=False), target)
            if published is None:
                raise ValueError(f"target {path} has no [workflow.build] section")
            return published
        if candidate.resolve().is_relative_to(workspace.root.resolve()):
            return _resolve_job_target(workspace, target, base=base, resolver=resolver)
        raise ValueError(f"target {path} has no [workflow.build] section")

    try:
        store_candidate = workspace.runner_store_path(target)
    except FormatError:
        store_candidate = None
    if store_candidate is not None and store_candidate.is_dir():
        spec = read_build_spec(store_candidate)
        if spec is None:
            raise ValueError(f"target {target} has no [workflow.build] section")
        relative = PurePosixPath(target)
        return store_candidate, relative, spec, source_tree_digest(store_candidate)

    try:
        selected = resolver if resolver is not None else JobSelectorResolver(workspace, base)
        markers = selected.resolve_one(target)
        if len(markers) != 1:
            raise ValueError("workflow build job target must resolve to one job")
        marker = markers[0]
    except ValueError:
        raise ValueError(
            f"no workflow package, workflow name, or workspace runner matches {target!r}; "
            "write ./NAME for a package directory in the current directory"
        ) from None
    job = workspace.load_job(marker)
    if job.runner_source != "workspace":
        raise ValueError(f"job {target} does not reference a workspace workflow package")
    store_path = workspace.runner_store_path(job.runner_path)
    spec = read_build_spec(store_path)
    if spec is None:
        raise ValueError(f"target {job.runner_path.as_posix()} has no [workflow.build] section")
    if job.runner_sha256 is None:
        raise ValueError(f"job {target} has no pinned workspace runner digest")
    return store_path, job.runner_path, spec, job.runner_sha256


def _resolve_store_target(workspace: Workspace, store: str) -> tuple[Path, PurePosixPath, BuildSpec, str]:
    """Resolve an explicit store-relative runner selector."""

    try:
        store_path = workspace.runner_store_path(store)
    except FormatError as exc:
        raise ValueError(f"invalid store runner path {store!r}: {exc}") from exc
    if not store_path.is_dir():
        raise ValueError(f"workspace runner {store!r} does not exist")
    spec = read_build_spec(store_path)
    if spec is None:
        raise ValueError(f"target {store} has no [workflow.build] section")
    relative = PurePosixPath(store)
    return store_path, relative, spec, source_tree_digest(store_path)


def _package_provider(target: str, base: Path) -> WorkflowProvider | None:
    """Return the provider of a package-directory target, for building what it calls."""

    if not _path_like(target):
        return None
    path = Path(target).expanduser()
    path = path if path.is_absolute() else base / path
    if not (path / MANIFEST_NAME).is_file():
        return None
    from ..packages import parse_workflow_manifest

    return parse_workflow_manifest(path)


def _called_rows(
    workspace: Workspace,
    provider: WorkflowProvider,
    seen: set[str],
    *,
    stdout_to_stderr: bool,
) -> list[dict[str, object]]:
    """Build every workflow *provider* declares in ``[workflow.calls]``, transitively.

    Each call resolves as ``httk workflow build NAME`` resolves a name, so a
    workflow and everything it calls are ready after one command. A call target
    that does not resolve is reported as a failed row rather than stopping the
    others.
    """

    rows: list[dict[str, object]] = []
    for alias, reference in sorted((provider.calls or {}).items()):
        if reference in seen:
            continue
        seen.add(reference)
        row: dict[str, object] = {"target": reference, "called_by": provider.workflow_id, "alias": alias}
        try:
            # A call resolves as Attempt.call resolves it, never as a store or job selector.
            called: WorkflowProvider | None
            if reference.startswith("git+"):
                from ..git_workflows import fetch_workflow

                called = fetch_workflow(reference)
            else:
                called = workflow_provider(reference)
            if called is None:
                raise ValueError(f"called workflow {reference!r} is not known on this machine")
            if (
                called.directory is None
                or called.language is not None
                or called.runner_package is not None
                or not called.runnable
            ):
                # Only a directory package runs from a build; anything else is ready as it is.
                rows.append({**row, "workflow": called.workflow_id, "package": None, "status": "nothing-to-build"})
                rows.extend(_called_rows(workspace, called, seen, stdout_to_stderr=stdout_to_stderr))
                continue
            row.update(
                build_workflow_provider(workspace, called, reference=reference, stdout_to_stderr=stdout_to_stderr)
            )
        except _ERRORS as exc:
            row.update({"workflow": reference, "package": None, "status": "failed", "error": str(exc)})
            rows.append(row)
            continue
        rows.append(row)
        rows.extend(_called_rows(workspace, called, seen, stdout_to_stderr=stdout_to_stderr))
    return rows


def _print_result(result: Mapping[str, object]) -> None:
    """Print one build report row as text."""

    called_by = f" (called by {result['called_by']})" if result.get("called_by") else ""
    print(f"{result['target']}:{called_by}")
    if result.get("status") == "failed":
        print(f"failed: {result.get('error')}")
        return
    if result["package"] is not None:
        print(f"workflow: {result['workflow']}")
        print(f"package: {result['package']}")
    if result["status"] == "nothing-to-build":
        print(
            "nothing to build: the package has no [workflow.build] section"
            if result["package"] is not None
            else "nothing to build: not a compiled workflow package"
        )
        return
    print(
        f"platform probe: command={result['platform'] or 'any'} "
        f"output={result['platform_output']!r} tag={result['platform_tag']}"
    )
    print(f"registered: {result['artifacts']}")


def handle_build(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Build a workflow package in the foreground and register its artifacts."""

    workspace = _workspace(arguments, context)
    if arguments.list:
        return _list_builds(workspace, as_json=arguments.json)
    targets = [arguments.store] if arguments.store is not None else arguments.targets
    if not targets:
        raise ValueError("workflow build requires at least one TARGET unless --list is given")
    results: list[dict[str, object]] = []
    failed = False
    resolver = JobSelectorResolver(workspace, Path(context.cwd))
    for target in targets:
        try:
            provider = None if arguments.store is not None else _workflow_reference(workspace, target)
            if provider is not None:
                result = {
                    "target": target,
                    **build_workflow_provider(workspace, provider, reference=target, stdout_to_stderr=arguments.json),
                }
            else:
                if arguments.store is not None:
                    resolved = _resolve_store_target(workspace, target)
                else:
                    resolved = _resolve_target(workspace, target, base=Path(context.cwd), resolver=resolver)
                result = {
                    "target": target,
                    "workflow": None,
                    "package": None,
                    **_register(workspace, resolved, stdout_to_stderr=arguments.json),
                }
            results.append(result)
            if not arguments.json:
                _print_result(result)
            root = provider if provider is not None else _package_provider(target, Path(context.cwd))
            if root is not None and root.calls:
                for row in _called_rows(workspace, root, {root.workflow_id, target}, stdout_to_stderr=arguments.json):
                    failed = failed or row.get("status") == "failed"
                    results.append(row)
                    if not arguments.json:
                        _print_result(row)
        except _ERRORS as exc:
            failed = True
            print(f"{target}: {exc}", file=sys.stderr)
    if arguments.json:
        print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


def build_build_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the top-level ``build`` workflow command."""

    parser = _leaf(
        subparsers,
        "build",
        summary="build and register a compiled workflow package",
        description=(
            "Build a workflow package, selected by path or by workflow name, in the foreground and register "
            "its artifacts"
        ),
        handler=handle_build,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help=(
            "a package path (absolute, ./, ../, or containing /), a workflow reference (a git URI, "
            "or a registered, plugin, or installed git workflow name), a store runner path, or a job reference"
        ),
    )
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace to build workflow runners in (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
    _add_by_path_argument(parser)
    parser.add_argument(
        "--store",
        metavar="STORE/PATH",
        help="build this explicit nested store-relative runner path instead of classifying TARGET",
    )
    parser.add_argument("--list", action="store_true", help="list the workspace's registered builds")
    parser.add_argument("--json", action="store_true", help="print build or list output as JSON")
