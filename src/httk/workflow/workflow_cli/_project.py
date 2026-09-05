"""Configuration, project, and umbrella-project command groups."""

import argparse
import json
import sys
from contextlib import redirect_stdout
from copy import copy
from io import StringIO
from typing import Any

from httk.core.cli import CLIContext

from ..configuration import import_v1_configuration, read_config, set_config_key, unset_config_key
from ._common import (
    _ERRORS,
    _group,
    _leaf,
)

# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


def _batch(
    arguments: argparse.Namespace,
    context: CLIContext,
    handler: Any,
    attribute: str,
    noun: str,
    *,
    json_output: bool = False,
) -> int:
    """Run a leaf once per positional target without duplicating its operation."""

    targets = getattr(arguments, attribute)
    assert isinstance(targets, list)
    targets = targets or [None]
    multiple = len(targets) > 1
    json_output |= getattr(arguments, "json", False)
    results: list[object] = []
    failed = False
    for target in targets:
        item = copy(arguments)
        setattr(item, attribute, target)
        label = target or "default"
        try:
            if json_output:
                output = StringIO()
                with redirect_stdout(output):
                    code = handler(item, context)
                results.append(json.loads(output.getvalue()))
            else:
                if multiple:
                    print(f"== {noun} {label} ==")
                code = handler(item, context)
            failed |= code != 0
        except _ERRORS as exc:
            print(f"{noun} {label}: {exc}", file=sys.stderr)
            failed = True
    if json_output:
        print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_config_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print the whole user configuration, or one member of it."""

    if isinstance(arguments.key, list):
        return _batch(arguments, context, handle_config_show, "key", "configuration key")
    values = read_config()
    if arguments.key:
        if arguments.key not in values:
            raise ValueError(f"configuration key is not set: {arguments.key}")
        value = values[arguments.key]
        print(json.dumps(value) if not isinstance(value, str) else value)
    else:
        print(json.dumps(values, indent=2, sort_keys=True))
    return 0


def handle_config_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Store one member of the user configuration."""

    print(set_config_key(arguments.key, arguments.value))
    return 0


def handle_config_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one member of the user configuration."""

    if isinstance(arguments.key, list):
        return _batch(arguments, context, handle_config_unset, "key", "configuration key")
    print(unset_config_key(arguments.key))
    return 0


def handle_config_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Read a legacy ``~/.httk`` configuration into the XDG one."""

    print(json.dumps(import_v1_configuration(arguments.source), indent=2, sort_keys=True))
    return 0


def build_config_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``config`` group: the per-user configuration and identity."""

    _, group = _group(
        subparsers,
        "config",
        summary="read and write the per-user httk configuration",
        description="Read and write the per-user httk configuration below $XDG_CONFIG_HOME",
    )

    show = _leaf(
        group,
        "show",
        summary="print the configuration, or one member",
        description="Print the whole user configuration, or the value of one key",
        handler=handle_config_show,
    )
    show.add_argument(
        "key",
        metavar="KEY",
        nargs="*",
        help="one configuration key (default: print everything)",
    )

    store = _leaf(
        group,
        "set",
        summary="store one configuration member",
        description="Store one member of the user configuration",
        handler=handle_config_set,
    )
    store.add_argument("key", metavar="KEY", help="the configuration key to write")
    store.add_argument("value", metavar="VALUE", help="the value to store")

    remove = _leaf(
        group,
        "unset",
        summary="remove one configuration member",
        description="Remove one member of the user configuration",
        handler=handle_config_unset,
    )
    remove.add_argument("key", metavar="KEY", nargs="+", help="the configuration key to remove")

    imported = _leaf(
        group,
        "import-v1",
        summary="read a legacy ~/.httk configuration",
        description="Read a legacy httk v1 configuration into the XDG configuration",
        handler=handle_config_import_v1,
    )
    imported.add_argument(
        "source",
        metavar="SOURCE",
        nargs="?",
        help="the legacy directory (default: ~/.httk)",
    )


# ---------------------------------------------------------------------------
# project
# ---------------------------------------------------------------------------
