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
from ..introspection import resolve_job
from ..packages import read_build_spec, source_tree_digest
from ..scaffold import resolve_workflow
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


def _resolve_target(workspace: Workspace, target: str, *, base: Path) -> tuple[Path, PurePosixPath, BuildSpec, str]:
    raw_path = Path(target).expanduser()
    path_like = (
        raw_path.is_absolute()
        or target in {".", ".."}
        or target.startswith(("./", "../"))
        or os.sep in target
        or (os.altsep is not None and os.altsep in target)
    )
    if path_like:
        path = raw_path if raw_path.is_absolute() else base / raw_path
        if not path.is_dir():
            raise ValueError(f"workflow package directory does not exist: {path}")
        try:
            spec = read_build_spec(path)
        except ValueError as exc:
            raise ValueError(f"target package is malformed: {exc}") from exc
        if spec is None:
            raise ValueError(f"target {path} has no [workflow.build] section")
        resolved = resolve_workflow(path)
        if resolved.directory is None:
            raise ValueError(f"target {path} is not a workflow package directory")
        reference = workspace.publish_runner(path, name=resolved.store_name)
        store_relative = PurePosixPath(str(reference["path"]))
        return workspace.runner_store_path(store_relative), store_relative, spec, str(reference["sha256"])

    try:
        store_path = workspace.runner_store_path(target)
    except FormatError:
        store_path = None
    if store_path is not None and store_path.is_dir():
        spec = read_build_spec(store_path)
        if spec is None:
            raise ValueError(f"target {target} has no [workflow.build] section")
        relative = PurePosixPath(target)
        return store_path, relative, spec, source_tree_digest(store_path)

    try:
        marker = resolve_job(workspace, target)
    except ValueError:
        raise ValueError(f"no workflow package or workspace runner matches {target!r}") from None
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
    for target in targets:
        try:
            if arguments.store is not None:
                store_path, store_relative, spec, source_sha256 = _resolve_store_target(workspace, target)
            else:
                store_path, store_relative, spec, source_sha256 = _resolve_target(
                    workspace, target, base=Path(context.cwd)
                )
            artifacts = register_build(
                workspace,
                store_path,
                store_relative,
                spec,
                source_sha256=source_sha256,
                stdout_to_stderr=arguments.json,
            )
            stamp = json.loads((artifacts.parent / "build.json").read_text(encoding="utf-8"))
            result = {
                "target": target,
                "store": store_relative.as_posix(),
                "platform": stamp["platform"],
                "platform_output": stamp["platform_output"],
                "platform_tag": stamp["platform_tag"],
                "artifacts": str(artifacts),
                "built_at": stamp["built_at"],
            }
            results.append(result)
            if not arguments.json:
                print(f"{target}:")
                print(
                    f"platform probe: command={stamp['platform'] or 'any'} "
                    f"output={stamp['platform_output']!r} tag={stamp['platform_tag']}"
                )
                print(f"registered: {artifacts}")
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
        description="Build a published workflow package in the foreground and register its artifacts",
        handler=handle_build,
    )
    parser.add_argument(
        "targets",
        nargs="*",
        metavar="TARGET",
        help="a package path (absolute, ./, ../, or containing /), store runner path, or job reference",
    )
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace to build workflow runners in (default: this project's workspace, or the per-user default)",
    )
    _add_by_path_argument(parser)
    parser.add_argument(
        "--store",
        metavar="STORE/PATH",
        help="build this explicit nested store-relative runner path instead of classifying TARGET",
    )
    parser.add_argument("--list", action="store_true", help="list the workspace's registered builds")
    parser.add_argument("--json", action="store_true", help="print build or list output as JSON")
