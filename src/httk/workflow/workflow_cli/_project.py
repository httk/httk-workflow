"""Configuration, project, and umbrella-project command groups."""

import sys
from contextlib import nullcontext, redirect_stdout
from copy import copy
from io import StringIO

from ..configuration import (
    _configured_identities,
    _validate_operator_identity_fields,
    ensure_identity_key,
    identity_key_paths,
    identity_public_key,
    write_config,
)
from ..manifests import workspace_maintenance_guard
from ..models import WORKSPACE_DIRECTORY
from ..projects import PROJECT_DIRECTORY, require_project
from ..seals import default_project_keys, project_seal_path, read_seal, seal_project, unseal_project
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

    values = read_config()
    identities = _configured_identities(values)
    if arguments.short in identities:
        raise ValueError(f"identity already exists: {arguments.short}")
    identity_key_paths(arguments.short)
    _validate_operator_identity_fields(arguments.name, arguments.email)
    had_default = "default_identity" in values
    previous_default = values.get("default_identity")
    ensure_identity_key(arguments.short)
    identities[arguments.short] = (arguments.name, arguments.email)
    values["identities"] = {short: {"name": name, "email": email} for short, (name, email) in identities.items()}
    selected_default = arguments.short if len(identities) == 1 or arguments.default else previous_default
    if len(identities) == 1 or arguments.default or had_default:
        values["default_identity"] = selected_default
    write_config(values)
    print(
        json.dumps(
            _identity_report(arguments.short, arguments.name, arguments.email, selected_default == arguments.short)
        )
    )
    return 0


def handle_config_identity_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List configured named operator identities."""

    values = read_config()
    identities = _configured_identities(values)
    default = values.get("default_identity")
    if "default_identity" in values and (not isinstance(default, str) or default not in identities):
        print(
            f"warning: config identity default {default!r} is not a configured identity",
            file=sys.stderr,
        )
    reports = [_identity_report(short, *identities[short], short == default) for short in sorted(identities)]
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

    values = read_config()
    identities = _configured_identities(values)
    if arguments.short not in identities:
        shorts = ", ".join(sorted(identities)) or "(none)"
        raise ValueError(f"unknown identity {arguments.short!r}; configured identities: {shorts}")
    values["default_identity"] = arguments.short
    print(write_config(values))
    return 0


def handle_config_identity_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one named identity while leaving its key files untouched."""

    if isinstance(arguments.short, list):
        return _batch(arguments, context, handle_config_identity_remove, "short", "identity")
    values = read_config()
    identities = _configured_identities(values)
    short = arguments.short
    if short not in identities:
        shorts = ", ".join(sorted(identities)) or "(none)"
        raise ValueError(f"unknown identity {short!r}; configured identities: {shorts}")
    default = values.get("default_identity")
    if default == short and len(identities) > 2:
        raise ValueError(f"cannot remove default identity {short!r}; choose another default first")
    identities.pop(short)
    values["identities"] = {name: {"name": item[0], "email": item[1]} for name, item in identities.items()}
    if default == short:
        if len(identities) == 1:
            values["default_identity"] = next(iter(identities))
        else:
            values.pop("default_identity", None)
    if not identities:
        values.pop("default_identity", None)
    write_config(values)
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


def handle_project_doctor(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check one project for the conditions that quietly break it later."""

    if isinstance(arguments.path, list):
        if arguments.repair and not arguments.path:
            raise ValueError("project doctor --repair requires at least one PATH")
        return _batch(arguments, context, handle_project_doctor, "path", "project")
    report = project_doctor(arguments.path or context.cwd, repair=arguments.repair)
    reported = report["findings"]
    findings: list[dict[str, Any]] = [
        finding for finding in (reported if isinstance(reported, list) else []) if isinstance(finding, dict)
    ]
    if arguments.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for finding in findings:
            repaired = " (repaired)" if finding.get("repaired") else ""
            print(f"{finding['status']}\t{finding['check']}\t{finding['message']}{repaired}")
        print(f"{report['problems']} problem(s), {report['repaired']} repaired")
    # A warning is a thing to know about, not a thing to fail a script on; only
    # a check that is actually broken makes the command itself fail.
    return 1 if any(finding.get("status") == "error" for finding in findings) else 0


def handle_project_manifest_create(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Write the signed manifest of one project."""

    if isinstance(arguments.project, list):
        if arguments.manifest and len(arguments.project) != 1:
            raise ValueError("project manifest create --manifest requires exactly one PROJECT")
        return _batch(arguments, context, handle_project_manifest_create, "project", "project")
    print(create_manifest(arguments.project, output=arguments.manifest))
    return 0


def handle_project_manifest_verify(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Verify one project manifest against the tree and its trust anchors."""

    if isinstance(arguments.project, list):
        if arguments.manifest and len(arguments.project) != 1:
            raise ValueError("project manifest verify --manifest requires exactly one PROJECT")
        return _batch(arguments, context, handle_project_manifest_verify, "project", "project")
    verification = verify_manifest(
        arguments.project or context.cwd,
        manifest=arguments.manifest,
        trusted_keys=arguments.trusted_key,
    )
    # The first line keeps the shape every existing caller reads; the verdict
    # line is what distinguishes a manifest that is merely self-consistent
    # from one signed by a key this project actually pins.
    print("valid" if verification.valid else "invalid")
    print(f"{verification.verdict}: {verification.reason}")
    return verification.exit_code


def handle_project_seal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal a project: its loose files, and the seal digest of every workspace.

    Every workspace at or below the project must already be sealed;
    :func:`seal_project` refuses otherwise. When the project root is itself a
    workspace, the maintenance guard fences it exactly as ``manifest create`` does.
    """

    if isinstance(arguments.project, list):
        return _batch(arguments, context, handle_project_seal, "project", "project")
    root = require_project(arguments.project or context.cwd)
    refs = [ref.strip() for ref in arguments.keys.split(",") if ref.strip()] if arguments.keys else None
    resolved = default_project_keys(root, refs) if refs is not None else None
    workspace = (
        Workspace(root)
        if (root / PROJECT_DIRECTORY).is_dir() and (root / WORKSPACE_DIRECTORY / "format.json").is_file()
        else None
    )
    guard = workspace_maintenance_guard(workspace) if workspace is not None else nullcontext()
    with guard:
        seal_project(root, keys=resolved)
    seal = read_seal(project_seal_path(root))
    roles = ",".join(str(signature.get("role")) for signature in seal.signatures)
    print(f"{root}\tsealed\t{roles}")
    return 0


def handle_project_unseal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove a project's seal, after confirmation.

    Unsealing runs top down: the project seal is removed here, which then frees
    each workspace to be unsealed, which frees each job.
    """

    if isinstance(arguments.project, list):
        return _batch(arguments, context, handle_project_unseal, "project", "project")
    root = require_project(arguments.project or context.cwd)
    if not confirm(f"Unseal the project at {root}?", force=arguments.force):
        return 1
    unseal_project(root)
    print(f"{root}\tunsealed")
    return 0


def add_project_doctor_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project doctor`, shared with ``httk project doctor``."""

    parser.add_argument(
        "path",
        metavar="PATH",
        nargs="*",
        help="the project to check (default: the nearest project of the working directory)",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="also fix every finding that can be fixed automatically",
    )
    parser.add_argument("--json", action="store_true", help="print the report as one JSON document")


def add_project_manifest_create_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project manifest create`, shared with the umbrella command."""

    parser.add_argument(
        "project",
        metavar="PROJECT",
        nargs="+",
        help="one or more projects",
    )
    parser.add_argument(
        "--manifest",
        metavar="PATH",
        help="write the manifest here rather than in the project",
    )


def add_project_manifest_verify_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project manifest verify`, shared with the umbrella command."""

    parser.add_argument(
        "project",
        metavar="PROJECT",
        nargs="*",
        help="the project (default: the nearest one)",
    )
    parser.add_argument(
        "--manifest",
        metavar="PATH",
        help="verify this manifest rather than the project's",
    )
    parser.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="PATH_OR_VALUE",
        help=(
            "trust this Ed25519 public key as well: an ed25519:BASE64 value or the path of a "
            "*.pub file (repeatable). The project's pinned key is always trusted"
        ),
    )


def project_extension(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Mount httk-workflow's project verbs onto the core ``httk project`` command.

    Registered through ``register_cli_extension("project", ...)``, this adds
    ``doctor``, ``manifest create|verify``, ``seal``, and ``unseal`` beside the
    core-owned ``init | show | import-v1 | export | verify-export`` leaves. The
    handlers honour core's ``(argparse.Namespace, CLIContext) -> int`` contract
    (workflow shares :class:`httk.core.cli.CLIContext`, so ``context.cwd`` and
    ``context.program`` mean the same thing) and route every operator error
    through :func:`_batch`, so a :class:`~httk.workflow.errors.WorkflowError`
    (``SealError`` and ``SealedError`` among them, which core's error dispatch
    does not catch) surfaces as a clean message rather than a traceback.
    """

    add_project_doctor_arguments(
        _leaf(
            subparsers,
            "doctor",
            summary="check, and optionally repair, this project",
            description="Check one project for the conditions that quietly break it later",
            handler=handle_project_doctor,
        )
    )

    _, manifest_actions = _group(
        subparsers,
        "manifest",
        summary="create and verify the signed project manifest",
        description="Create and verify the deterministic signed manifest of one project",
    )
    add_project_manifest_create_arguments(
        _leaf(
            manifest_actions,
            "create",
            summary="write the signed manifest",
            description="Write the deterministic signed manifest of one project",
            handler=handle_project_manifest_create,
        )
    )
    add_project_manifest_verify_arguments(
        _leaf(
            manifest_actions,
            "verify",
            summary="verify the manifest against the tree",
            description="Verify one project manifest against the tree and this project's trust anchors",
            handler=handle_project_manifest_verify,
        )
    )

    seal = _leaf(
        subparsers,
        "seal",
        summary="seal the project and every workspace it holds",
        description="Seal a project's loose files and the seal digest of every nested workspace under one signed seal",
        handler=handle_project_seal,
    )
    seal.add_argument(
        "project",
        metavar="PROJECT",
        nargs="*",
        help="the project to seal (default: the nearest one)",
    )
    seal.add_argument(
        "--keys",
        metavar="REFS",
        help="comma-separated seal-key refs to sign with (default: the project's seal_keys member)",
    )

    unseal = _leaf(
        subparsers,
        "unseal",
        summary="remove the project's seal",
        description="Remove the project's seal, which frees its workspaces and jobs to be unsealed in turn",
        handler=handle_project_unseal,
    )
    unseal.add_argument(
        "project",
        metavar="PROJECT",
        nargs="*",
        help="the project to unseal (default: the nearest one)",
    )
    unseal.add_argument("--force", action="store_true", help="skip the confirmation prompt")
