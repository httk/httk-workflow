"""Configuration, project, and umbrella-project command groups."""

import sys
from collections.abc import Mapping
from contextlib import redirect_stdout
from copy import copy
from io import StringIO

from httk.core.identity import (
    add_identity,
    identity_config_path,
    identity_key_paths,
    identity_public_key,
    read_identity_config,
    remove_identity,
    set_default_identity,
)

from ._common import *
from ._common import (
    _ERRORS,
    _group,
    _leaf,
    _required,
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


def handle_config_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the user configuration and ensure an operator identity key."""

    current = read_config()
    name = _required(
        arguments.name,
        "name",
        non_interactive=arguments.non_interactive,
        default=str(current.get("name", "")) or None,
    )
    email = _required(
        arguments.email,
        "email",
        non_interactive=arguments.non_interactive,
        default=str(current.get("email", "")) or None,
    )
    print(json.dumps(initialize_config(name=name, email=email), indent=2, sort_keys=True))
    return 0


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


def _identity_report(short: str, name: str, email: str, is_default: bool) -> dict[str, object]:
    seed_path, _ = identity_key_paths(short)
    return {
        "short": short,
        "name": name,
        "email": email,
        "public_key": identity_public_key(seed_path),
        "default": is_default,
    }


def handle_config_identity_add(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create and configure one named operator identity."""

    values = add_identity(arguments.short, arguments.name, arguments.email, make_default=arguments.default)
    is_default = values.get("default_identity") == arguments.short
    print(json.dumps(_identity_report(arguments.short, arguments.name, arguments.email, is_default)))
    return 0


def handle_config_identity_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List configured named operator identities."""

    values = read_identity_config()
    raw = values.get("identities")
    identities: dict[str, dict[str, str]] = {}
    if isinstance(raw, Mapping):
        for short, item in raw.items():
            if isinstance(item, Mapping):
                identities[str(short)] = {"name": str(item.get("name", "")), "email": str(item.get("email", ""))}
    default = values.get("default_identity")
    default_short = default if isinstance(default, str) else None
    if "default_identity" in values and (default_short is None or default_short not in identities):
        print(
            f"warning: config identity default {default!r} is not a configured identity",
            file=sys.stderr,
        )
    reports = [
        _identity_report(short, identities[short]["name"], identities[short]["email"], short == default_short)
        for short in sorted(identities)
    ]
    if arguments.json:
        print(json.dumps(reports, indent=2, sort_keys=True))
    else:
        for report in reports:
            print(
                f"{'*' if report['default'] else ' '} {report['short']}\t{report['name']} <{report['email']}>\t{report['public_key']}"
            )
    return 0


def handle_config_identity_default(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Select the default named operator identity."""

    set_default_identity(arguments.short)
    print(identity_config_path())
    return 0


def handle_config_identity_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one named identity while leaving its key files untouched."""

    if isinstance(arguments.short, list):
        return _batch(arguments, context, handle_config_identity_remove, "short", "identity")
    short = arguments.short
    remove_identity(short)
    seed_path, public_path = identity_key_paths(short)
    print(f"removed {short}; key files remain: {seed_path}, {public_path}")
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

    initialize = _leaf(
        group,
        "init",
        summary="write the configuration and the identity key",
        description="Write the per-user configuration and ensure an operator identity key",
        handler=handle_config_init,
    )
    initialize.add_argument("--name", metavar="NAME", help="the operator's name")
    initialize.add_argument("--email", metavar="EMAIL", help="the operator's email address")
    initialize.add_argument(
        "--non-interactive",
        action="store_true",
        help="never prompt; refuse a missing value",
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

    _, identities = _group(
        group,
        "identity",
        summary="manage named operator identities",
        description="Create, list, select, and remove named operator identities",
    )
    add = _leaf(
        identities,
        "add",
        summary="add a named operator identity",
        description="Create a named operator identity and its Ed25519 signing key",
        handler=handle_config_identity_add,
    )
    add.add_argument("short", metavar="SHORT", help="the short identity name ([a-z0-9][a-z0-9_-]*)")
    add.add_argument("--name", required=True, metavar="NAME", help="the operator's full name")
    add.add_argument("--email", required=True, metavar="EMAIL", help="the operator's email address")
    add.add_argument("--default", action="store_true", help="make this identity the default")

    listed = _leaf(
        identities,
        "list",
        summary="list named operator identities",
        description="List named operator identities and their public keys",
        handler=handle_config_identity_list,
    )
    listed.add_argument("--json", action="store_true", help="print the identities as JSON")

    selected = _leaf(
        identities,
        "default",
        summary="select the default identity",
        description="Select the default named operator identity",
        handler=handle_config_identity_default,
    )
    selected.add_argument("short", metavar="SHORT", help="the configured identity short name")

    removed = _leaf(
        identities,
        "remove",
        summary="remove a named operator identity",
        description="Remove an identity from configuration and leave its key files on disk",
        handler=handle_config_identity_remove,
    )
    removed.add_argument("short", metavar="SHORT", nargs="+", help="the configured identity short name")


# ---------------------------------------------------------------------------
# project
# ---------------------------------------------------------------------------
