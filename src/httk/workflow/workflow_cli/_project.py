"""Configuration, project, and umbrella-project command groups."""

from ._common import *
from ._common import (
    _field,
    _group,
    _leaf,
    _required,
)

# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


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
        nargs="?",
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
    remove.add_argument("key", metavar="KEY", help="the configuration key to remove")

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


def handle_project_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one project directory, its key, and its workspace."""

    default_name = Path(arguments.path).resolve().name
    name = _required(
        arguments.name,
        "project name",
        non_interactive=arguments.non_interactive,
        default=default_name,
    )
    result = initialize_project(
        arguments.path,
        name=name,
        description=arguments.description,
        default_queue=arguments.default_queue,
        manifest_exclusions=arguments.exclude,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def handle_project_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Read a legacy ``ht.project`` into an *httk₂* project."""

    print(
        json.dumps(
            import_v1_project(arguments.path, source=arguments.source, name=arguments.name),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _render_project(description: dict[str, Any]) -> str:
    """Render one project description as readable lines."""

    project = description.get("project", {})
    keys = description.get("keys", {})
    workspace = description.get("workspace", {})
    manifest = description.get("manifest", {})
    public = keys.get("public_key") or {}
    lines = [
        _field("root", description.get("root")),
        _field("name", project.get("name") or "-"),
        _field("project_id", project.get("project_id") or "-"),
        _field("default_queue", project.get("default_queue") or "-"),
        _field("key_pinned", "yes" if keys.get("pinned") else "no"),
        _field("key_fingerprint", public.get("fingerprint") or "-"),
        _field("trusted_keys", len(keys.get("trusted_keys", []))),
        _field(
            "workspace",
            workspace.get("workspace_id") or ("present" if workspace.get("present") else "-"),
        ),
        _field("jobs", workspace.get("jobs", 0)),
        _field(
            "manifest",
            f"{manifest.get('verdict') or 'none'}: {manifest.get('reason') or '-'}",
        ),
        _field("remotes", ", ".join(description.get("remotes", [])) or "-"),
    ]
    lock = workspace.get("maintenance_lock")
    if isinstance(lock, dict):
        lines.append(
            _field(
                "maintenance_lock",
                f"{lock.get('holder')} ({'stale' if lock.get('stale') else 'live'})",
            )
        )
    return "\n".join(lines)


def handle_project_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one project: its metadata, its keys, its workspace, its manifest."""

    description = describe_project(arguments.path or context.cwd, verify=not arguments.no_verify)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_project(description))
    return 0


def handle_project_doctor(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Check one project for the conditions that quietly break it later."""

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

    print(create_manifest(arguments.project or context.cwd, output=arguments.manifest))
    return 0


def handle_project_manifest_verify(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Verify one project manifest against the tree and its trust anchors."""

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


def add_project_doctor_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`project doctor`, shared with ``httk project doctor``."""

    parser.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
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
        nargs="?",
        help="the project (default: the nearest one)",
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
        nargs="?",
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


def build_umbrella_doctor_parser(parser: argparse.ArgumentParser) -> None:
    """Declare ``httk project doctor`` on the umbrella command's own parser.

    Registered into the core ``httk project`` command, so ``httk project doctor``
    and ``httk workflow project doctor`` drive the very same handler.
    """

    add_project_doctor_arguments(parser)


def build_umbrella_manifest_parser(parser: argparse.ArgumentParser) -> None:
    """Declare ``httk project manifest create|verify`` on the umbrella parser."""

    parser.set_defaults(handler=None, help_parser=parser)
    actions = parser.add_subparsers(metavar="COMMAND")
    create = actions.add_parser(
        "create",
        help="write the signed manifest",
        description="Write the deterministic signed manifest of one project",
        formatter_class=HelpFormatter,
    )
    create.set_defaults(handler=handle_project_manifest_create, help_parser=create)
    add_project_manifest_create_arguments(create)
    verify = actions.add_parser(
        "verify",
        help="verify the manifest against the tree",
        description="Verify one project manifest against the tree and this project's trust anchors",
        formatter_class=HelpFormatter,
    )
    verify.set_defaults(handler=handle_project_manifest_verify, help_parser=verify)
    add_project_manifest_verify_arguments(verify)


def build_project_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    context: CLIContext,
) -> None:
    """Declare the ``project`` group: the directory a campaign lives in."""

    _, group = _group(
        subparsers,
        "project",
        summary="create, describe, check, and sign a project directory",
        description="Create, describe, check, and sign one httk project directory",
    )

    initialize = _leaf(
        group,
        "init",
        summary="create a project, its key, and its workspace",
        description="Create one project directory with its identity key and workflow workspace",
        handler=handle_project_init,
    )
    initialize.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        default=str(context.cwd),
        help="the directory to make a project (default: the working directory)",
    )
    initialize.add_argument("--name", metavar="NAME", help="the project name (default: the directory name)")
    initialize.add_argument("--description", metavar="TEXT", default="", help="a one-line description")
    initialize.add_argument(
        "--default-queue",
        metavar="QUEUE",
        help="the remote queue commands use by default",
    )
    initialize.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="PATTERN",
        help="exclude paths matching this glob from signed manifests (repeatable)",
    )
    initialize.add_argument(
        "--non-interactive",
        action="store_true",
        help="never prompt; refuse a missing value",
    )

    imported = _leaf(
        group,
        "import-v1",
        summary="read a legacy ht.project into a project",
        description="Read a legacy httk v1 ht.project directory into an httk project",
        handler=handle_project_import_v1,
    )
    imported.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        default=str(context.cwd),
        help="the directory to make a project (default: the working directory)",
    )
    imported.add_argument("--source", metavar="SOURCE", help="the legacy ht.project directory to read")
    imported.add_argument("--name", metavar="NAME", help="the project name (default: the legacy one)")

    show = _leaf(
        group,
        "show",
        summary="describe this project, its keys, and its workspace",
        description="Describe one project: its metadata, its keys, its workspace, and its manifest",
        handler=handle_project_show,
    )
    show.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        help="the project to describe (default: the nearest project of the working directory)",
    )
    show.add_argument(
        "--no-verify",
        action="store_true",
        help="do not walk the tree to classify the manifest, which is much cheaper on a large project",
    )
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    add_project_doctor_arguments(
        _leaf(
            group,
            "doctor",
            summary="check, and optionally repair, this project",
            description="Check one project for the conditions that quietly break it later",
            handler=handle_project_doctor,
        )
    )

    _, manifest_actions = _group(
        group,
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
