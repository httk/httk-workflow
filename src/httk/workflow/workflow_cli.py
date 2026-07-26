"""Nested :command:`httk workflow` command tree."""

import argparse
import json
import sys
import uuid
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

from httk.core import CLIContext

from . import cli as native_cli
from . import v1_cli
from ._logging import LOG_LEVELS, configure_logging
from ._util import read_json, write_json_atomic
from .adapters import (
    add_computer,
    import_v1_computer,
    list_computers,
    queue_settings,
    resolve_computer,
    run_adapter,
    split_settings,
    store_credentials,
)
from .configuration import (
    import_v1_configuration,
    initialize_config,
    read_config,
    write_config,
)
from .errors import WorkflowError
from .harvest import DEFAULT_HARVEST_STATES, HARVESTABLE_KINDS, harvest
from .introspection import (
    JOB_HISTORY_FORMAT,
    JOB_LIST_FORMAT,
    debug_job,
    describe_job,
    explain_job,
    job_frames,
    list_jobs,
    render_frames,
    render_job,
    render_rows,
    resolve_job,
)
from .manifests import create_manifest, release_maintenance_lock, verify_manifest
from .models import CORE_PROFILE, STATE_KINDS
from .projects import import_v1_project, initialize_project, require_project
from .scaffold import (
    DEFAULT_PLACEMENT,
    PACKAGED_TEMPLATES,
    STRUCTURE_PATTERNS,
    JobItem,
    ScaffoldedJob,
    new_job,
    new_jobs,
    structure_files,
    structure_tag,
)
from .workspace import WorkflowWorkspace

_HELP = """usage: {program} GROUP COMMAND [ARG ...]

Filesystem-native workflow execution and project management.

command groups:
  workspace   init, status, upgrade, unlock
  runner      publish
  job         new, submit, request, list, show, log, why, debug
  harvest     stream the finished jobs of a workspace as records
  manager     run
  v1          prepare, submit, run
  config      init, show, set, import-v1
  project     init, import-v1, manifest create, manifest verify
  computer    list, add, configure, install, import-v1
  tasks       send, receive, start-manager, status
"""


def _parser(program: str, description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog=program, description=description)


def _parse(parser: argparse.ArgumentParser, argv: Sequence[str]) -> argparse.Namespace | int:
    try:
        return parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1


def _delegate(function: Callable[..., int], argv: Sequence[str], program: str) -> int:
    try:
        return function(argv, program=program)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 1


def _required(
    value: str | None,
    label: str,
    *,
    non_interactive: bool,
    default: str | None = None,
) -> str:
    if value:
        return value
    if non_interactive or not sys.stdin.isatty():
        raise ValueError(f"missing required value {label!r} in non-interactive operation")
    suffix = f" [{default}]" if default else ""
    entered = input(f"{label}{suffix}: ").strip()
    result = entered or default
    if not result:
        raise ValueError(f"{label} cannot be empty")
    return result


def _workspace_upgrade(argv: Sequence[str], program: str) -> int:
    parser = _parser(program, "Enable an implemented workflow workspace extension")
    parser.add_argument("workspace")
    parser.add_argument("--extension", action="append", required=True)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace)
    print("\n".join(sorted(workspace.upgrade(parsed.extension))))
    return 0


def _workspace_unlock(argv: Sequence[str], program: str) -> int:
    parser = _parser(program, "Release a stale workspace maintenance lock")
    parser.add_argument("workspace")
    parser.add_argument("--force", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    print(release_maintenance_lock(WorkflowWorkspace(parsed.workspace), force=parsed.force))
    return 0


def _runner(argv: Sequence[str], program: str) -> int:
    """Manage the shared runners one workspace publishes for its jobs."""

    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} publish [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    if action != "publish":
        raise ValueError(f"unknown runner command: {action}")
    parser = _parser(f"{program} publish", "Publish one runner into a workspace runner store")
    parser.add_argument("file", help="the runner file to publish")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--name", help="store name, including any subdirectory (default: the file name)")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="overwrite a stored runner of the same name whose content differs",
    )
    parsed = _parse(parser, rest)
    if isinstance(parsed, int):
        return parsed
    reference = WorkflowWorkspace(parsed.workspace).publish_runner(
        parsed.file,
        name=parsed.name,
        replace=parsed.replace,
    )
    print(json.dumps(reference, indent=2, sort_keys=True))
    return 0


def _json_value(text: str, label: str) -> object:
    """Return the JSON value one command-line argument denotes.

    The spelling is the one the Bash SDK's ``--input`` uses: ``@path`` is the JSON
    content of a file, which is how a value too large or too quoted for a command
    line is passed, and anything else is a JSON value when it parses as one and the
    literal string when it does not, so ``k=42`` is a number and ``k=Si`` a string
    without the author quoting either.
    """

    if text.startswith("@"):
        path = Path(text[1:]).expanduser()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"{label} cannot be read as JSON from {path}: {exc}") from exc
    try:
        return json.loads(text)
    except ValueError:
        return text


def _pairs(values: Sequence[str], label: str) -> list[tuple[str, str]]:
    """Split ``NAME=VALUE`` arguments, keeping the order they were given in."""

    result: list[tuple[str, str]] = []
    for item in values:
        name, separator, text = item.partition("=")
        if not separator or not name:
            raise ValueError(f"{label} must be spelled NAME=VALUE, not {item!r}")
        result.append((name, text))
    return result


def _job_new(argv: Sequence[str], program: str) -> int:
    """Scaffold and submit one job per template, structure, or both."""

    parser = _parser(program, "Scaffold and submit jobs from a runner template")
    parser.add_argument("workspace")
    parser.add_argument(
        "--template",
        required=True,
        help=f"a packaged template ({', '.join(PACKAGED_TEMPLATES)}) or the path of a runner file",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one job input; VALUE is JSON when it parses as JSON and a string otherwise, "
        "and NAME=@FILE reads a JSON file (repeatable)",
    )
    parser.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="stage PATH in the payload as NAME; a bare NAME lands in files/ (repeatable)",
    )
    parser.add_argument(
        "--from",
        dest="structures",
        metavar="PATH",
        help="a structure file staged as files/POSCAR, or a directory of "
        f"{' / '.join(STRUCTURE_PATTERNS)} files, one job each",
    )
    parser.add_argument("--tag", help="the readable half of the job key (default: derived from --from)")
    parser.add_argument("--name", help="the human-readable job name")
    parser.add_argument(
        "--placement", default=DEFAULT_PLACEMENT, help=f"placement subtree (default: {DEFAULT_PLACEMENT})"
    )
    parser.add_argument("--priority", type=int)
    parser.add_argument("--step", help="the step the job starts at (default: the template's own)")
    parser.add_argument("--data-mode", choices=("none", "transactional"), help="default: what the template needs")
    parser.add_argument("--workdir-mode", choices=("persistent", "isolated"), default="persistent")
    parser.add_argument(
        "--publish",
        choices=("workspace", "installed"),
        default="workspace",
        help="publish the runner into the workspace store (default), or reference a packaged one where it is installed",
    )
    parser.add_argument("--json", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace)
    inputs = {name: _json_value(text, f"job input {name!r}") for name, text in _pairs(parsed.inputs, "a job input")}
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(parsed.files, "a staged file")}
    shared: dict[str, Any] = {
        "inputs": inputs,
        "placement": parsed.placement,
        "priority": parsed.priority,
        "workdir_mode": parsed.workdir_mode,
        "data_mode": parsed.data_mode,
        "publish": parsed.publish,
        "step": parsed.step,
        "name": parsed.name,
    }
    structures = Path(parsed.structures).expanduser() if parsed.structures else None
    if structures is not None and structures.is_dir():
        found = structure_files(structures)
        if not found:
            raise ValueError(f"no {' or '.join(STRUCTURE_PATTERNS)} file in {structures}")
        items: list[JobItem] = [
            {"files": {**files, "POSCAR": path}, "tag": parsed.tag or structure_tag(path) or f"structure-{index:04d}"}
            for index, path in enumerate(found)
        ]
        results: Iterator[ScaffoldedJob] = new_jobs(workspace, parsed.template, items, **shared)
    else:
        tag = parsed.tag
        if structures is not None:
            files["POSCAR"] = structures
            tag = tag or structure_tag(structures)
        results = iter([new_job(workspace, parsed.template, files=files, tag=tag, **shared)])
    if parsed.json:
        # One self-describing report per job, as an array, exactly as `harvest
        # --json` prints one array of records.
        print(json.dumps([job.as_mapping() for job in results], indent=2))
        return 0
    for job in results:
        # One tab-separated line per job, so a shell reads the key of one job with
        # cut and a campaign streams as it is submitted.
        print(f"{job.job_key}\t{job.payload}")
    return 0


def _job_list(argv: Sequence[str], program: str) -> int:
    """List the jobs of one workspace as a cheap table."""

    parser = _parser(program, "List the jobs of one workflow workspace")
    parser.add_argument("workspace")
    parser.add_argument("--kind", action="append", choices=STATE_KINDS, help="state kind to list (repeatable)")
    parser.add_argument("--placement", help="list only jobs at or below this placement")
    parser.add_argument("--json", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace, mutable=False)
    rows = list_jobs(workspace, kinds=parsed.kind, placement=parsed.placement)
    if parsed.json:
        print(json.dumps({"format": JOB_LIST_FORMAT, "format_version": 1, "jobs": rows}, indent=2))
        return 0
    print(render_rows(rows))
    return 0


def _job_show(argv: Sequence[str], program: str) -> int:
    """Describe one job completely from its authoritative state."""

    parser = _parser(program, "Describe one job from its authoritative state")
    parser.add_argument("workspace")
    parser.add_argument("job", help="job UUID, job key, or any unique prefix of either")
    parser.add_argument("--json", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace, mutable=False)
    report = describe_job(workspace, resolve_job(workspace, parsed.job))
    print(json.dumps(report, indent=2, sort_keys=True) if parsed.json else render_job(report))
    return 0


def _job_log(argv: Sequence[str], program: str) -> int:
    """Print the recorded transition history of one job, oldest first."""

    parser = _parser(program, "Print the transition history of one job")
    parser.add_argument("workspace")
    parser.add_argument("job", help="job UUID, job key, or any unique prefix of either")
    parser.add_argument("--limit", type=int, help="read at most this many frames, newest first")
    parser.add_argument("--json", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    if parsed.limit is not None and parsed.limit < 1:
        raise ValueError("--limit must be positive")
    workspace = WorkflowWorkspace(parsed.workspace, mutable=False)
    frames = job_frames(workspace, resolve_job(workspace, parsed.job), limit=parsed.limit)
    if parsed.json:
        print(json.dumps({"format": JOB_HISTORY_FORMAT, "format_version": 1, "frames": frames}, indent=2))
        return 0
    print(render_frames(frames))
    return 0


def _job_why(argv: Sequence[str], program: str) -> int:
    """Explain why one job is, or is not, making progress."""

    parser = _parser(program, "Explain why one job is not running")
    parser.add_argument("workspace")
    parser.add_argument("job", help="job UUID, job key, or any unique prefix of either")
    parser.add_argument("--json", action="store_true")
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace, mutable=False)
    diagnosis = explain_job(workspace, resolve_job(workspace, parsed.job))
    print(json.dumps(diagnosis.as_mapping(), indent=2, sort_keys=True) if parsed.json else diagnosis.render())
    return 0


def _job_debug(argv: Sequence[str], program: str) -> int:
    """Drive one job to a terminal state in the foreground."""

    parser = _parser(program, "Drive one job to a terminal state in the foreground")
    parser.add_argument("workspace")
    parser.add_argument("job", help="a payload directory to submit, or a selector of an existing job")
    parser.add_argument("--step", help="initial step of a freshly submitted payload")
    parser.add_argument("--placement", default="debug", help="placement of a freshly submitted payload")
    parser.add_argument("--follow-children", action="store_true", help="drive spawned children depth first")
    parser.add_argument("--timeout", type=float, default=3600.0)
    parser.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default="error",
        help="log level of the private manager on the console (default: error)",
    )
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    # The transitions of the debugged job are reported by the debug runner
    # itself, so the private manager's own log stays quiet unless asked for.
    configure_logging(level=parsed.log_level)
    workspace = WorkflowWorkspace(parsed.workspace)
    outcome = debug_job(
        workspace,
        parsed.job,
        placement=parsed.placement,
        step=parsed.step,
        follow_children=parsed.follow_children,
        timeout=parsed.timeout,
    )
    return outcome.exit_code


def _harvest(argv: Sequence[str], program: str) -> int:
    """Stream the finished jobs of one workspace as harvest records."""

    parser = _parser(program, "Harvest the finished jobs of one workflow workspace")
    parser.add_argument("workspace")
    parser.add_argument(
        "--state",
        action="append",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to harvest (repeatable, default: {', '.join(DEFAULT_HARVEST_STATES)})",
    )
    parser.add_argument("--placement", help="harvest only jobs at or below this placement")
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--jsonl",
        action="store_true",
        help="print one record object per line, streaming (the default)",
    )
    output.add_argument(
        "--json",
        action="store_true",
        help="print one JSON array of every record, which materializes the whole harvest",
    )
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    workspace = WorkflowWorkspace(parsed.workspace, mutable=False)
    records = harvest(
        workspace,
        states=parsed.state or DEFAULT_HARVEST_STATES,
        placement=parsed.placement,
    )
    if parsed.json:
        print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
        return 0
    for record in records:
        print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
    return 0


_JOB_INSPECTION: dict[str, Callable[[Sequence[str], str], int]] = {
    "list": _job_list,
    "show": _job_show,
    "log": _job_log,
    "why": _job_why,
    "debug": _job_debug,
}


def _config(argv: Sequence[str], program: str) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} init|show|set|import-v1 [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    parser = _parser(f"{program} {action}", f"Workflow configuration: {action}")
    if action == "init":
        parser.add_argument("--name")
        parser.add_argument("--email")
        parser.add_argument("--non-interactive", action="store_true")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        current = read_config()
        name = _required(
            parsed.name,
            "name",
            non_interactive=parsed.non_interactive,
            default=str(current.get("name", "")) or None,
        )
        email = _required(
            parsed.email,
            "email",
            non_interactive=parsed.non_interactive,
            default=str(current.get("email", "")) or None,
        )
        print(json.dumps(initialize_config(name=name, email=email), indent=2, sort_keys=True))
        return 0
    if action == "show":
        parser.add_argument("key", nargs="?")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        values = read_config()
        if parsed.key:
            if parsed.key not in values:
                raise ValueError(f"configuration key is not set: {parsed.key}")
            value = values[parsed.key]
            print(json.dumps(value) if not isinstance(value, str) else value)
        else:
            print(json.dumps(values, indent=2, sort_keys=True))
        return 0
    if action == "set":
        parser.add_argument("key")
        parser.add_argument("value")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        values = read_config()
        values[parsed.key] = parsed.value
        print(write_config(values))
        return 0
    if action == "import-v1":
        parser.add_argument("source", nargs="?")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        print(json.dumps(import_v1_configuration(parsed.source), indent=2, sort_keys=True))
        return 0
    raise ValueError(f"unknown config command: {action}")


def _project_manifest(argv: Sequence[str], program: str) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} create|verify [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    parser = _parser(f"{program} {action}", f"Project manifest: {action}")
    parser.add_argument("project", nargs="?")
    parser.add_argument("--manifest")
    parsed = _parse(parser, rest)
    if isinstance(parsed, int):
        return parsed
    if action == "create":
        print(create_manifest(parsed.project, output=parsed.manifest))
        return 0
    if action == "verify":
        valid = verify_manifest(parsed.project, manifest=parsed.manifest)
        print("valid" if valid else "invalid")
        return 0 if valid else 1
    raise ValueError(f"unknown project manifest command: {action}")


def _project(argv: Sequence[str], program: str, context: CLIContext) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} init|import-v1|manifest [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    if action == "manifest":
        return _project_manifest(rest, f"{program} manifest")
    parser = _parser(f"{program} {action}", f"Workflow project: {action}")
    if action == "init":
        parser.add_argument("path", nargs="?", default=str(context.cwd))
        parser.add_argument("--name")
        parser.add_argument("--description", default="")
        parser.add_argument("--default-queue")
        parser.add_argument("--exclude", action="append", default=[])
        parser.add_argument("--non-interactive", action="store_true")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        default_name = Path(parsed.path).resolve().name
        name = _required(
            parsed.name,
            "project name",
            non_interactive=parsed.non_interactive,
            default=default_name,
        )
        result = initialize_project(
            parsed.path,
            name=name,
            description=parsed.description,
            default_queue=parsed.default_queue,
            manifest_exclusions=parsed.exclude,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if action == "import-v1":
        parser.add_argument("path", nargs="?", default=str(context.cwd))
        parser.add_argument("--source")
        parser.add_argument("--name")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        print(
            json.dumps(
                import_v1_project(parsed.path, source=parsed.source, name=parsed.name),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise ValueError(f"unknown project command: {action}")


def _settings(values: Sequence[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise ValueError(f"adapter setting must use KEY=VALUE: {value!r}")
        result[key] = item
    return result


def _computer(argv: Sequence[str], program: str, context: CLIContext) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} list|add|configure|install|import-v1 [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    parser = _parser(f"{program} {action}", f"Workflow computer adapter: {action}")
    if action == "list":
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        print(json.dumps(list_computers(context.cwd), indent=2, sort_keys=True))
        return 0
    if action == "add":
        parser.add_argument("name")
        parser.add_argument("--template")
        parser.add_argument("--global", dest="global_scope", action="store_true")
        parser.add_argument("--non-interactive", action="store_true")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        template = _required(
            parsed.template,
            "adapter template",
            non_interactive=parsed.non_interactive,
            default="local",
        )
        print(
            add_computer(
                parsed.name,
                template=template,
                global_scope=parsed.global_scope,
                project=context.cwd,
            )
        )
        return 0
    if action in {"configure", "install"}:
        parser.add_argument("computer")
        parser.add_argument("--set", action="append", default=[])
        parser.add_argument("--timeout", type=float)
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        target = resolve_computer(parsed.computer, project=context.cwd)
        settings = _settings(parsed.set)
        result = run_adapter(
            target.bundle,
            action,
            {"queue": target.queue, "settings": settings},
            timeout=parsed.timeout,
        )
        if action == "configure" and settings:
            persistable, credentials = split_settings(settings)
            if persistable:
                metadata = read_json(target.bundle / "computer.json")
                queues = metadata.setdefault("queues", {})
                if not isinstance(queues, dict):
                    raise ValueError("adapter queue configuration is not mutable JSON")
                queue = queues.setdefault(target.queue, {})
                if not isinstance(queue, dict):
                    raise ValueError("adapter queue configuration is not an object")
                queue.update(persistable)
                write_json_atomic(target.bundle / "computer.json", metadata)
            if credentials:
                path = store_credentials(target.bundle, target.queue, credentials)
                names = ", ".join(sorted(credentials))
                print(
                    f"stored {names} for queue {target.queue} in {path}; "
                    "values there are excluded from signed project manifests",
                    file=sys.stderr,
                )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if action == "import-v1":
        parser.add_argument("source")
        parser.add_argument("--name")
        parser.add_argument("--global", dest="global_scope", action="store_true")
        parsed = _parse(parser, rest)
        if isinstance(parsed, int):
            return parsed
        print(
            import_v1_computer(
                parsed.source,
                name=parsed.name,
                global_scope=parsed.global_scope,
                project=context.cwd,
            )
        )
        return 0
    raise ValueError(f"unknown computer command: {action}")


def _destination_from_adapter(target: Any, supplied: str | None) -> str:
    if supplied:
        return supplied
    workspace = queue_settings(target.bundle, target.queue).get("workspace")
    if isinstance(workspace, str) and workspace:
        return workspace
    raise ValueError("destination workspace is missing; use --destination-workspace or configure queue workspace=PATH")


def _tasks_send(argv: Sequence[str], program: str, context: CLIContext) -> int:
    parser = _parser(program, "Detach and send explicit workflow jobs")
    parser.add_argument("computer")
    parser.add_argument("jobs", nargs="+")
    parser.add_argument("--workspace")
    parser.add_argument("--destination-workspace")
    parser.add_argument("--destination-placement")
    parser.add_argument("--timeout", type=float)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    source_root = Path(parsed.workspace).resolve() if parsed.workspace else require_project(context.cwd)
    source = WorkflowWorkspace(source_root)
    target = resolve_computer(parsed.computer, project=context.cwd)
    destination_root = _destination_from_adapter(target, parsed.destination_workspace)
    status = run_adapter(
        target.bundle,
        "status",
        {
            "queue": target.queue,
            "argv": ["httk", "workflow", "workspace", "status", destination_root, "--json"],
        },
        timeout=parsed.timeout,
    )
    if status.get("returncode") != 0:
        raise RuntimeError(f"destination workspace compatibility check failed: {status.get('stderr', '')}")
    try:
        status_data = json.loads(str(status.get("stdout", "")))
        if (
            status_data.get("format") != "httk-workflow-status"
            or status_data.get("format_version") != 1
            or status_data.get("core_profile") != CORE_PROFILE
            or "detached-transfer-v1" not in status_data.get("extensions", [])
        ):
            raise ValueError
        destination_workspace_id = str(status_data["workspace_id"])
        uuid.UUID(destination_workspace_id)
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError("destination did not return a compatible workflow workspace status") from exc
    acknowledgements: list[dict[str, object]] = []
    for job_id in parsed.jobs:
        source.recover_transfers()
        candidates: list[dict[str, object]] = []
        for ledger_path in (source.control / "transfers").glob("*.json"):
            ledger = read_json(ledger_path)
            if (
                ledger.get("job_id") == job_id
                and ledger.get("destination_workspace_id") == destination_workspace_id
                and ledger.get("status") == "sealed"
            ):
                candidates.append(ledger)
        if len(candidates) > 1:
            raise ValueError(f"multiple resumable transfers exist for job: {job_id}")
        transfer_id = str(candidates[0]["transfer_id"]) if candidates else str(uuid.uuid4())
        if candidates and parsed.destination_placement:
            requested = str(parsed.destination_placement).strip("/")
            if candidates[0].get("destination_placement") != requested:
                raise ValueError("resumed transfer destination placement disagrees with the request")
        bundle = source.detach(
            job_id,
            destination_workspace_id=destination_workspace_id,
            destination_placement=parsed.destination_placement,
            transfer_id=transfer_id,
        )
        incoming = f"{destination_root.rstrip('/')}/.httk-workflow/transfers/incoming/{transfer_id}"
        push = run_adapter(
            target.bundle,
            "push",
            {"queue": target.queue, "source": str(bundle), "destination": incoming},
            timeout=parsed.timeout,
        )
        remote_bundle = str(push.get("path", incoming))
        invoked = run_adapter(
            target.bundle,
            "invoke",
            {
                "queue": target.queue,
                "argv": [
                    "httk",
                    "workflow",
                    "tasks",
                    "receive",
                    "--workspace",
                    destination_root,
                    "--bundle",
                    remote_bundle,
                ],
            },
            timeout=parsed.timeout,
        )
        if invoked.get("returncode") != 0:
            raise RuntimeError(f"destination import failed: {invoked.get('stderr', '')}")
        try:
            acknowledgement = json.loads(str(invoked.get("stdout", "")))
        except json.JSONDecodeError as exc:
            raise ValueError("destination import did not return an acknowledgement") from exc
        if not isinstance(acknowledgement, dict):
            raise ValueError("destination acknowledgement is not an object")
        source.acknowledge_transfer(acknowledgement)
        acknowledgements.append(acknowledgement)
    print(json.dumps(acknowledgements, indent=2, sort_keys=True))
    return 0


def _tasks_receive(argv: Sequence[str], program: str) -> int:
    parser = _parser(program, "Import one sealed detached transfer bundle")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--bundle", required=True)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    acknowledgement = WorkflowWorkspace(parsed.workspace).import_bundle(parsed.bundle)
    print(json.dumps(acknowledgement, sort_keys=True, separators=(",", ":")))
    return 0


def _tasks_remote(argv: Sequence[str], program: str, context: CLIContext, operation: str) -> int:
    parser = _parser(program, f"Remote workflow {operation}")
    parser.add_argument("computer")
    parser.add_argument("--workspace")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--workers", type=int, default=1)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    target = resolve_computer(parsed.computer, project=context.cwd)
    workspace = _destination_from_adapter(target, parsed.workspace)
    if operation == "start-manager":
        request = {
            "queue": target.queue,
            "argv": [
                "httk",
                "workflow",
                "manager",
                "run",
                workspace,
                "--workers",
                str(parsed.workers),
            ],
        }
    else:
        request = {
            "queue": target.queue,
            "argv": ["httk", "workflow", "workspace", "status", workspace, "--json"],
        }
    print(json.dumps(run_adapter(target.bundle, operation, request, timeout=parsed.timeout), indent=2))
    return 0


def _tasks(argv: Sequence[str], program: str, context: CLIContext) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(f"usage: {program} send|receive|start-manager|status [ARG ...]")
        return 0
    action, rest = argv[0], argv[1:]
    if action == "send":
        return _tasks_send(rest, f"{program} send", context)
    if action == "receive":
        return _tasks_receive(rest, f"{program} receive")
    if action == "start-manager":
        return _tasks_remote(rest, f"{program} start-manager", context, "start-manager")
    if action == "status":
        return _tasks_remote(rest, f"{program} status", context, "status")
    raise ValueError(f"unknown tasks command: {action}")


def command(argv: Sequence[str], context: CLIContext) -> int:
    """Handle the registered top-level ``workflow`` command."""

    program = f"{context.program} workflow"
    arguments = list(argv)
    if not arguments or arguments[0] in {"-h", "--help"}:
        print(_HELP.format(program=program))
        return 0
    group, rest = arguments[0], arguments[1:]
    try:
        if group == "workspace":
            if not rest or rest[0] in {"-h", "--help"}:
                print(f"usage: {program} workspace init|status|upgrade|unlock [ARG ...]")
                return 0
            action, tail = rest[0], rest[1:]
            if action in {"init", "status"}:
                return _delegate(native_cli.main, [action, *tail], f"{program} workspace")
            if action == "upgrade":
                return _workspace_upgrade(tail, f"{program} workspace upgrade")
            if action == "unlock":
                return _workspace_unlock(tail, f"{program} workspace unlock")
        elif group == "runner":
            return _runner(rest, f"{program} runner")
        elif group == "job":
            if not rest or rest[0] in {"-h", "--help"}:
                print(f"usage: {program} job new|submit|request|list|show|log|why|debug [ARG ...]")
                return 0
            if rest[0] == "new":
                return _job_new(rest[1:], f"{program} job new")
            if rest[0] in {"submit", "request"}:
                return _delegate(native_cli.main, rest, f"{program} job")
            inspection = _JOB_INSPECTION.get(rest[0])
            if inspection is not None:
                return inspection(rest[1:], f"{program} job {rest[0]}")
        elif group == "harvest":
            return _harvest(rest, f"{program} harvest")
        elif group == "manager":
            if not rest or rest[0] in {"-h", "--help"}:
                return _delegate(native_cli.main, ["run", "--help"], f"{program} manager")
            if rest[0] == "run":
                return _delegate(native_cli.main, rest, f"{program} manager")
        elif group == "v1":
            return _delegate(v1_cli.main, rest, f"{program} v1")
        elif group == "config":
            return _config(rest, f"{program} config")
        elif group == "project":
            return _project(rest, f"{program} project", context)
        elif group == "computer":
            return _computer(rest, f"{program} computer", context)
        elif group == "tasks":
            return _tasks(rest, f"{program} tasks", context)
        raise ValueError(f"unknown workflow command group: {group}")
    except (WorkflowError, OSError, ValueError, RuntimeError, TimeoutError) as exc:
        print(f"{program}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(command(sys.argv[1:], CLIContext("httk", Path.cwd())))
