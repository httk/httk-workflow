"""The ``workflow build`` command: build installed workflows for this platform in the foreground."""

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path

from httk.core.cli import CLIContext

from .. import _kernel, _store
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _add_by_path_argument,
    _leaf,
    _modifiable,
    _remote_workspace_read,
    _resolve_binding,
)
from ._describe import _stdout_to_stderr


def _stamp(build: Path) -> dict[str, object]:
    """Return a build's ``build.json``, or an empty mapping when it is unreadable."""

    try:
        value = json.loads((build / "build.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _row(workflow_id: str, build: Path) -> dict[str, object]:
    stamp = _stamp(build)
    return {
        "workflow": workflow_id,
        "platform_tag": build.name,
        "platform": stamp.get("platform"),
        "platform_output": stamp.get("platform_output"),
        "built_at": stamp.get("built_at"),
        "artifacts": str(build / "artifacts"),
    }


def _print_row(row: Mapping[str, object]) -> None:
    print(f"{row['workflow']}:")
    print(
        f"platform probe: command={row['platform'] or 'any'} "
        f"output={row['platform_output']!r} tag={row['platform_tag']}"
    )
    print(f"registered: {row['artifacts']}")


def handle_build(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Build installed workflows for this platform, or list the registered builds."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        flags = [flag for flag in ("--list", "--json") if getattr(arguments, flag[2:])]
        return _remote_workspace_read(
            binding,
            context,
            ("httk", "workflow", "build"),
            arguments,
            tail=(*flags, *arguments.targets, "--workspace"),
            unwrap_json_array=False,
        )
    if arguments.list:
        workspace = Workspace(root)
        rows = [
            _row(installed.id, build.parent)
            for installed in _store.list_installed(workspace)
            for build in sorted((installed.directory / "builds").glob("*/build.json"))
        ]
        if arguments.json:
            print(json.dumps(rows, indent=2, sort_keys=True))
            return 0
        for row in rows:
            print(f"{row['workflow']}\t{row['platform_tag']}\t{row['built_at'] or '-'}")
        return 0
    if not arguments.targets:
        raise ValueError("workflow build requires at least one installed WORKFLOW unless --list is given")
    workspace = _modifiable(arguments, context, action="build workflows in it")
    rows = []
    failed = False
    with _kernel.register_owner(workspace, kind="cli", label="workflow build", allocation=None, advertised={}) as owner:
        for target in arguments.targets:
            try:
                with _stdout_to_stderr(arguments.json):
                    build = _store.build(workspace, owner, target)
                installed = _store.lookup(workspace, target)
            except _ERRORS as exc:
                failed = True
                print(f"{target}: {exc}", file=sys.stderr)
                continue
            rows.append(_row(target if installed is None else installed.id, build))
            if not arguments.json:
                _print_row(rows[-1])
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    return 1 if failed else 0


def build_build_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the top-level ``build`` workflow command."""

    parser = _leaf(
        subparsers,
        "build",
        summary="build installed workflows for this platform",
        description=(
            "Build installed workflows (by id or short name) that declare [workflow.build], in the foreground, "
            "and register the artifacts for this platform; a REMOTE:WS workspace builds on the remote"
        ),
        handler=handle_build,
    )
    parser.add_argument("targets", nargs="*", metavar="WORKFLOW", help="an installed workflow id or short name")
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace whose installed workflows to build (default: the enclosing workspace, this "
        "project's workspace, or the per-user default)",
    )
    _add_by_path_argument(parser)
    parser.add_argument("--list", action="store_true", help="list the registered builds of the installed workflows")
    parser.add_argument("--json", action="store_true", help="print build or list output as JSON")
    _add_adapter_timeout(parser)
