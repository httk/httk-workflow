"""Nested :command:`httk workflow` command tree."""

import argparse
import json
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from httk.core import CLIContext

from . import cli as native_cli
from . import v1_cli
from ._util import read_json, write_json_atomic
from .adapters import (
    add_computer,
    import_v1_computer,
    list_computers,
    resolve_computer,
    run_adapter,
)
from .configuration import (
    import_v1_configuration,
    initialize_config,
    read_config,
    write_config,
)
from .errors import WorkflowError
from .manifests import create_manifest, verify_manifest
from .projects import import_v1_project, initialize_project, require_project
from .store import WorkflowStore

_HELP = """usage: {program} GROUP COMMAND [ARG ...]

Filesystem-native workflow execution and project management.

command groups:
  store       init, status, upgrade
  job         submit, request
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


def _store_upgrade(argv: Sequence[str], program: str) -> int:
    parser = _parser(program, "Enable an implemented workflow-store extension")
    parser.add_argument("store")
    parser.add_argument("--extension", action="append", required=True)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    store = WorkflowStore(parsed.store)
    print("\n".join(sorted(store.upgrade(parsed.extension))))
    return 0


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
        result = run_adapter(
            target.bundle,
            action,
            {"queue": target.queue, "settings": _settings(parsed.set)},
            timeout=parsed.timeout,
        )
        if action == "configure" and parsed.set:
            metadata = read_json(target.bundle / "computer.json")
            queues = metadata.setdefault("queues", {})
            if not isinstance(queues, dict):
                raise ValueError("adapter queue configuration is not mutable JSON")
            queue = queues.setdefault(target.queue, {})
            if not isinstance(queue, dict):
                raise ValueError("adapter queue configuration is not an object")
            queue.update(_settings(parsed.set))
            write_json_atomic(target.bundle / "computer.json", metadata)
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
    metadata = read_json(target.bundle / "computer.json")
    queues = metadata.get("queues", {})
    if isinstance(queues, Mapping):
        queue = queues.get(target.queue)
        if isinstance(queue, Mapping) and isinstance(queue.get("store"), str):
            return str(queue["store"])
    raise ValueError("destination store is missing; use --destination-store or configure queue store=PATH")


def _tasks_send(argv: Sequence[str], program: str, context: CLIContext) -> int:
    parser = _parser(program, "Detach and send explicit workflow jobs")
    parser.add_argument("computer")
    parser.add_argument("jobs", nargs="+")
    parser.add_argument("--store")
    parser.add_argument("--destination-store")
    parser.add_argument("--destination-placement")
    parser.add_argument("--timeout", type=float)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    source_root = Path(parsed.store).resolve() if parsed.store else require_project(context.cwd)
    source = WorkflowStore(source_root)
    target = resolve_computer(parsed.computer, project=context.cwd)
    destination_root = _destination_from_adapter(target, parsed.destination_store)
    status = run_adapter(
        target.bundle,
        "status",
        {
            "queue": target.queue,
            "argv": ["httk", "workflow", "store", "status", destination_root, "--json"],
        },
        timeout=parsed.timeout,
    )
    if status.get("returncode") != 0:
        raise RuntimeError(f"destination store compatibility check failed: {status.get('stderr', '')}")
    try:
        status_data = json.loads(str(status.get("stdout", "")))
        if (
            status_data.get("format") != "httk-workflow-status"
            or status_data.get("format_version") != 1
            or status_data.get("core_profile") != "core-v1"
            or "detached-transfer-v1" not in status_data.get("extensions", [])
        ):
            raise ValueError
        destination_store_id = str(status_data["store_id"])
        uuid.UUID(destination_store_id)
    except (AttributeError, json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
        raise ValueError("destination did not return a compatible workflow store status") from exc
    acknowledgements: list[dict[str, object]] = []
    for job_id in parsed.jobs:
        source.recover_transfers()
        candidates: list[dict[str, object]] = []
        for ledger_path in (source.control / "transfers").glob("*.json"):
            ledger = read_json(ledger_path)
            if (
                ledger.get("job_id") == job_id
                and ledger.get("destination_store_id") == destination_store_id
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
            destination_store_id=destination_store_id,
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
                    "--store",
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
    parser.add_argument("--store", required=True)
    parser.add_argument("--bundle", required=True)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    acknowledgement = WorkflowStore(parsed.store).import_bundle(parsed.bundle)
    print(json.dumps(acknowledgement, sort_keys=True, separators=(",", ":")))
    return 0


def _tasks_remote(argv: Sequence[str], program: str, context: CLIContext, operation: str) -> int:
    parser = _parser(program, f"Remote workflow {operation}")
    parser.add_argument("computer")
    parser.add_argument("--store")
    parser.add_argument("--timeout", type=float)
    parser.add_argument("--workers", type=int, default=1)
    parsed = _parse(parser, argv)
    if isinstance(parsed, int):
        return parsed
    target = resolve_computer(parsed.computer, project=context.cwd)
    store = _destination_from_adapter(target, parsed.store)
    if operation == "start-manager":
        request = {
            "queue": target.queue,
            "argv": [
                "httk",
                "workflow",
                "manager",
                "run",
                store,
                "--workers",
                str(parsed.workers),
            ],
        }
    else:
        request = {
            "queue": target.queue,
            "argv": ["httk", "workflow", "store", "status", store, "--json"],
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
        if group == "store":
            if not rest or rest[0] in {"-h", "--help"}:
                print(f"usage: {program} store init|status|upgrade [ARG ...]")
                return 0
            action, tail = rest[0], rest[1:]
            if action in {"init", "status"}:
                return _delegate(native_cli.main, [action, *tail], f"{program} store")
            if action == "upgrade":
                return _store_upgrade(tail, f"{program} store upgrade")
        elif group == "job":
            if not rest or rest[0] in {"-h", "--help"}:
                print(f"usage: {program} job submit|request [ARG ...]")
                return 0
            if rest[0] in {"submit", "request"}:
                return _delegate(native_cli.main, rest, f"{program} job")
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
