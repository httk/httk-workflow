"""Manager launcher command group."""

import argparse
import json
import sys
from argparse import Namespace
from collections.abc import Callable
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from httk.core.cli import CLIContext

from .._confine import confine_settings, default_readonly_paths
from ..launchers import (
    add_launcher,
    check_launcher,
    configure_launcher,
    describe_launcher,
    list_launchers,
    remove_launcher,
    resolve_launcher,
)
from ._common import _group, _leaf, _required, _settings, confirm


def _launcher_batch(
    arguments: Namespace,
    context: CLIContext,
    handler: Callable[[Namespace, CLIContext], int],
    *,
    json_output: bool = False,
) -> int:
    """Run one launcher command for each NAME, retaining all results."""

    names = arguments.name
    assert isinstance(names, list)
    outputs: list[object] = []
    failed = False
    for name in names:
        item = Namespace(**vars(arguments))
        item.name = name
        try:
            if json_output:
                output = StringIO()
                with redirect_stdout(output):
                    result = handler(item, context)
                outputs.append(json.loads(output.getvalue()))
            else:
                result = handler(item, context)
            failed |= result != 0
        except (OSError, ValueError, RuntimeError, TimeoutError) as exc:
            print(f"launcher {name}: {exc}", file=sys.stderr)
            failed = True
            continue
    if json_output:
        print(json.dumps(outputs, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_launcher_list(arguments: Namespace, context: CLIContext) -> int:
    """List launchers visible to this project."""

    rows = list_launchers(context.cwd)
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for row in rows:
            print(f"{row['name']}\t{row['scope']}\t{row['path']}")
    return 0


def handle_launcher_add(arguments: Namespace, context: CLIContext) -> int:
    """Create launcher bundles from a maintained template."""

    if isinstance(arguments.name, list):
        return _launcher_batch(arguments, context, handle_launcher_add)
    template = _required(
        arguments.template,
        "launcher template",
        non_interactive=arguments.non_interactive,
        default="slurm",
    )
    print(
        add_launcher(
            arguments.name,
            template=template,
            settings=_settings(arguments.set),
            project=context.cwd,
            global_=arguments.global_scope,
        )
    )
    return 0


def handle_launcher_configure(arguments: Namespace, context: CLIContext) -> int:
    """Update or remove settings in one or more launcher bundles."""

    if isinstance(arguments.name, list):
        return _launcher_batch(arguments, context, handle_launcher_configure)
    settings = _settings(arguments.set)
    changed = _changed_paths(arguments.name, settings, arguments.unset, arguments.add, arguments.remove, context.cwd)
    print(configure_launcher(arguments.name, {**settings, **changed}, unset=arguments.unset, project=context.cwd))
    for key, value in changed.items():
        print(f"{key}={value}")
    return 0


_PATH_LISTS = ("confine.devices", "confine.pmix_roots", "confine.readonly_paths")


def _changed_paths(
    name: str,
    settings: dict[str, str],
    unset: list[str],
    additions: list[str],
    removals: list[str],
    project: Path,
) -> dict[str, str]:
    """Apply path additions and removals after unset and set operations."""

    if not additions and not removals:
        return {}
    description = describe_launcher(name, project=project)
    stored = description["settings"]
    current = {key: value for key, value in stored.items() if key not in unset} if isinstance(stored, dict) else {}
    current.update(settings)
    changed: dict[str, str] = {}
    for operation, items in (("--add", additions), ("--remove", removals)):
        for item in items:
            key, separator, paths = item.partition("=")
            if description["kind"] != "slurm" or key not in _PATH_LISTS or not separator or not paths:
                raise ValueError(
                    f"{operation} only applies to colon-separated path settings of slurm launchers "
                    f"(KEY=PATH[:PATH...]): {', '.join(_PATH_LISTS)}; got {item!r}"
                )
            confine_settings({key: paths})
            if current.get(key) is not None:
                entries = str(current[key]).split(":") if current[key] else []
            elif key == "confine.readonly_paths":
                entries = [str(path) for path in default_readonly_paths()]
            else:
                entries = []
            requested = paths.split(":")
            if operation == "--add":
                entries = list(dict.fromkeys([*entries, *requested]))
            else:
                entries = [path for path in entries if path not in requested]
            current[key] = changed[key] = ":".join(entries)
    return changed


def handle_launcher_show(arguments: Namespace, context: CLIContext) -> int:
    """Describe one or more launcher bundles."""

    if isinstance(arguments.name, list):
        return _launcher_batch(arguments, context, handle_launcher_show, json_output=arguments.json)
    description = describe_launcher(arguments.name, project=context.cwd)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_launcher(description))
    return 0


def _render_launcher(description: dict[str, object]) -> str:
    """Render one launcher description as readable lines."""

    required_binaries = description.get("required_binaries", [])
    required_text = ", ".join(str(item) for item in required_binaries) if isinstance(required_binaries, list) else "-"
    lines = [
        f"name: {description.get('name')}",
        f"scope: {description.get('scope')}",
        f"bundle: {description.get('bundle')}",
        f"kind: {description.get('kind') or '-'}",
        f"launcher_version: {description.get('launcher_version') or '-'}",
        f"valid: {'yes' if description.get('valid') else 'no: ' + str(description.get('problem'))}",
        f"timeout_seconds: {description.get('timeout_seconds')}",
        f"required_binaries: {required_text or '-'}",
    ]
    settings = description.get("settings", {})
    if isinstance(settings, dict):
        lines.extend(f"{key}={value}" for key, value in sorted(settings.items()))
    return "\n".join(lines)


def handle_launcher_check(arguments: Namespace, context: CLIContext) -> int:
    """Run a launcher's environment check operation."""

    if isinstance(arguments.name, list):
        return _launcher_batch(arguments, context, handle_launcher_check, json_output=True)
    target = resolve_launcher(arguments.name, project=context.cwd)
    result = check_launcher(target, timeout=arguments.launcher_timeout)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def handle_launcher_remove(arguments: Namespace, context: CLIContext) -> int:
    """Remove launcher bundles after an optional terminal confirmation."""

    if isinstance(arguments.name, list):
        return _launcher_batch(arguments, context, handle_launcher_remove, json_output=True)
    if not confirm(f"remove the launcher {arguments.name!r} and everything configured in it?", force=arguments.force):
        return 1
    print(json.dumps(remove_launcher(arguments.name, project=context.cwd), indent=2, sort_keys=True))
    return 0


def build_launcher_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``launcher`` group: bundles that start managers."""

    _, group = _group(
        subparsers,
        "launcher",
        summary="define, check, describe, and remove manager launchers",
        description="Define, check, describe, and remove the launcher bundles that start workflow managers",
    )

    listing = _leaf(
        group,
        "list",
        summary="list manager launchers",
        description="List the manager launchers this project and this user define",
        handler=handle_launcher_list,
    )
    listing.add_argument("--json", action="store_true", help="print the launchers as one JSON array")
    add = _leaf(
        group,
        "add",
        summary="create a launcher from a packaged template",
        description="Create one manager launcher bundle from a packaged template",
        handler=handle_launcher_add,
    )
    add.add_argument("name", metavar="NAME", nargs="+", help="the name this launcher is addressed by")
    template_option = "--" + "template"
    add.add_argument(template_option, metavar="TEMPLATE", help="slurm (default: slurm)")
    add.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the launcher for this user rather than for this project",
    )
    add.add_argument("--non-interactive", action="store_true", help="never prompt; refuse a missing value")
    add.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="one launcher setting (repeatable)",
    )

    configure = _leaf(
        group,
        "configure",
        summary="configure one launcher",
        description="Update launcher settings; apply --unset, --set, --add, then --remove",
        handler=handle_launcher_configure,
    )
    configure.add_argument("name", metavar="NAME", nargs="+", help="the launcher to configure")
    configure.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="one launcher setting (repeatable)",
    )
    configure.add_argument(
        "--unset", action="append", default=[], metavar="KEY", help="remove a setting override (repeatable)"
    )
    configure.add_argument(
        "--add",
        "--add-path",
        action="append",
        default=[],
        metavar="KEY=PATH[:PATH...]",
        help=(
            "append absolute paths to a colon-separated confine.* path setting of a slurm launcher, skipping paths "
            "already listed (repeatable, applied after --set); an unset confine.readonly_paths starts from the "
            "default computed by this interpreter"
        ),
    )
    configure.add_argument(
        "--remove",
        "--remove-path",
        action="append",
        default=[],
        metavar="KEY=PATH[:PATH...]",
        help="remove paths from a confine.* path setting (repeatable, applied after --add; --unset restores defaults)",
    )

    show = _leaf(
        group,
        "show",
        summary="describe one launcher",
        description="Describe one manager launcher: where it lives, what it is, and how it is configured",
        handler=handle_launcher_show,
    )
    show.add_argument("name", metavar="NAME", nargs="+", help="the launcher to describe")
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    check = _leaf(
        group,
        "check",
        summary="check a launcher",
        description="Run the check operation of one manager launcher",
        handler=handle_launcher_check,
    )
    check.add_argument("name", metavar="NAME", nargs="+", help="the launcher to check")
    check.add_argument(
        "--launcher-timeout",
        type=float,
        metavar="SECONDS",
        help="bound this launcher operation (default: the launcher's timeout_seconds)",
    )

    remove = _leaf(
        group,
        "remove",
        summary="remove one launcher bundle",
        description="Remove one manager launcher bundle",
        handler=handle_launcher_remove,
    )
    remove.add_argument("name", metavar="NAME", nargs="+", help="the launcher to remove")
    remove.add_argument("--force", action="store_true", help="skip the confirmation")
