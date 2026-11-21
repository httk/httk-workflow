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
from .._setting_catalog import annotate_setting, report_written_settings, use_color
from ..launchers import (
    add_launcher,
    check_launcher,
    configure_launcher,
    describe_launcher,
    list_launchers,
    remove_launcher,
    resolve_launcher,
)
from ._common import _group, _leaf, _required, _setting_pair, _settings, confirm


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
    """Create one launcher bundle from a maintained template."""

    kind = _required(
        arguments.kind,
        "launcher kind",
        non_interactive=arguments.non_interactive,
        default="slurm",
    )
    settings = _settings(arguments.pairs)
    print(
        add_launcher(
            arguments.name,
            template=kind,
            settings=settings,
            project=context.cwd,
            global_=arguments.global_scope,
        )
    )
    report_written_settings(settings, layer="launcher", kind=kind)
    return 0


def handle_launcher_set(arguments: Namespace, context: CLIContext) -> int:
    """Update one launcher's settings: sets, then ``--add``, then ``--remove``."""

    if not (arguments.pairs or arguments.add or arguments.remove):
        raise ValueError("launcher set requires at least one KEY=VALUE, --add or --remove")
    return _configure(arguments.name, _settings(arguments.pairs), [], arguments.add, arguments.remove, context)


def handle_launcher_unset(arguments: Namespace, context: CLIContext) -> int:
    """Remove setting overrides from one launcher."""

    return _configure(arguments.name, {}, arguments.keys, [], [], context)


def _configure(
    name: str,
    settings: dict[str, str],
    unset: list[str],
    additions: list[str],
    removals: list[str],
    context: CLIContext,
) -> int:
    """Validate and store one launcher's changed settings, reporting the changed path lists."""

    changed = _changed_paths(name, settings, unset, additions, removals, context.cwd)
    print(configure_launcher(name, {**settings, **changed}, unset=unset, project=context.cwd))
    for key, value in changed.items():
        print(f"{key}={value}")
    kind = describe_launcher(name, project=context.cwd)["kind"]
    report_written_settings(settings, layer="launcher", kind=kind if isinstance(kind, str) else None)
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
        color = use_color()
        kind = description.get("kind")
        for key, value in sorted(settings.items()):
            shown, annotation = annotate_setting(
                key, value, layer="launcher", kind=kind if isinstance(kind, str) else None, color=color
            )
            lines.append(f"{shown}={value}  # {annotation}")
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
    if not confirm(f"remove the launcher {arguments.name!r} and everything configured in it?", yes=arguments.yes):
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
    add.add_argument("name", metavar="NAME", help="the name this launcher is addressed by")
    add.add_argument("pairs", metavar="KEY=VALUE", nargs="*", type=_setting_pair, help="launcher settings")
    add.set_defaults(pairs_parser=add)
    add.add_argument("--kind", metavar="KIND", help="slurm (default: slurm)")
    add.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the launcher for this user rather than for this project",
    )
    add.add_argument("--non-interactive", action="store_true", help="never prompt; refuse a missing value")

    store = _leaf(
        group,
        "set",
        summary="change the settings of one launcher",
        description="Update launcher settings; apply KEY=VALUE sets, then --add, then --remove",
        handler=handle_launcher_set,
    )
    store.add_argument("name", metavar="NAME", help="the launcher to configure")
    store.add_argument("pairs", metavar="KEY=VALUE", nargs="*", type=_setting_pair, help="launcher settings")
    store.set_defaults(pairs_parser=store)
    store.add_argument(
        "--add",
        action="append",
        default=[],
        metavar="KEY=PATH[:PATH...]",
        help=(
            "append absolute paths to a colon-separated confine.* path setting of a slurm launcher, skipping paths "
            "already listed (repeatable, applied after the sets); an unset confine.readonly_paths starts from the "
            "default computed by this interpreter"
        ),
    )
    store.add_argument(
        "--remove",
        action="append",
        default=[],
        metavar="KEY=PATH[:PATH...]",
        help="remove paths from a confine.* path setting (repeatable, applied after --add; unset restores defaults)",
    )

    clear = _leaf(
        group,
        "unset",
        summary="remove settings of one launcher",
        description="Remove setting overrides from one launcher",
        handler=handle_launcher_unset,
    )
    clear.add_argument("name", metavar="NAME", help="the launcher to configure")
    clear.add_argument("keys", metavar="KEY", nargs="+", help="a setting override to remove")

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
        "--adapter-timeout",
        dest="launcher_timeout",
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
    remove.add_argument("--yes", action="store_true", help="skip the confirmation")
