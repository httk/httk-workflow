"""Remote and transfer command groups."""

import argparse
import json
import os
import sys
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from copy import copy
from io import StringIO
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.cli import CLIContext

from .._runner_builds import workspace_build_command
from .._util import read_json, write_json_atomic
from ..adapters import (
    REMOTE_OFFER_COMMAND,
    REMOTE_RECEIVE_COMMAND,
    REMOTE_RETIRE_COMMAND,
    REMOTE_WORKSPACE_SETTINGS_COMMAND,
    add_remote,
    import_v1_remote,
    list_remotes,
    metadata_path,
    probe_remote_workspace,
    read_metadata,
    resolve_remote,
    run_adapter,
    split_settings,
    store_credentials,
)
from ..adapters import (
    run_adapter as read_adapter,
)
from ..collecting import COLLECTABLE_KINDS
from ..errors import ResolutionMiss, WorkflowError
from ..hygiene import describe_remote, remove_remote
from ..introspection import JobSelectorResolver
from ..models import QUIESCENT_KINDS, WORKSPACE_DIRECTORY, JobDefinition, Marker, canonical_uuid, parse_job_key
from ..packages import read_build_spec
from ..precheck import environment_findings
from ..registry import LOCAL_REMOTE, WorkspaceBinding, list_workspaces, resolve_workspace
from ..transfers import (
    DEFAULT_OFFER_STATES,
    TRANSFER_OFFER_FORMAT,
    TRANSFER_RETIREMENT_FORMAT,
    TransferCandidate,
    _waiting_parent_map,
    discard_staged_bundle,
    offer_transfers,
    retire_transfers,
    select_transfer_jobs,
)
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _field,
    _group,
    _leaf,
    _required,
    _settings,
    confirm,
)


def _print_build_reminder(
    workspace: Workspace,
    acknowledgement: Mapping[str, object],
) -> None:
    """Remind users to register artifacts for an imported compiled runner."""

    try:
        payload = workspace.payload_path(
            PurePosixPath(str(acknowledgement["placement"])), str(acknowledgement["job_key"])
        )
        job = JobDefinition.from_path(payload / "job.json")
        if job.runner_source != "workspace":
            return
        runner = workspace.runner_store_path(job.runner_path)
        if runner.is_dir() and read_build_spec(runner) is not None:
            print(
                f"workflow {job.workflow} declares a build; run: {workspace_build_command(workspace, job.runner_path)} before starting managers here",
                file=sys.stderr,
            )
    except (OSError, ValueError, KeyError):
        pass


def _relay_success_stderr(result: Mapping[str, object]) -> None:
    """Relay diagnostics emitted by a successful remote protocol command."""

    stderr = result.get("stderr")
    if isinstance(stderr, str) and stderr:
        print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")


# ---------------------------------------------------------------------------
# remote
# ---------------------------------------------------------------------------


def _remote_batch(
    arguments: argparse.Namespace,
    context: CLIContext,
    handler: Any,
    attribute: str,
    *,
    json_output: bool = False,
) -> int:
    """Run a remote leaf sequentially, preserving the existing one-remote code."""

    targets = getattr(arguments, attribute)
    assert isinstance(targets, list)
    json_output |= getattr(arguments, "json", False)
    results: list[object] = []
    failed = False
    multiple = len(targets) > 1
    for target in targets:
        item = copy(arguments)
        setattr(item, attribute, target)
        try:
            if json_output:
                output = StringIO()
                with redirect_stdout(output):
                    code = handler(item, context)
                results.append(json.loads(output.getvalue()))
            else:
                if multiple:
                    print(f"== remote {target} ==")
                code = handler(item, context)
            failed |= code != 0
        except _ERRORS as exc:
            print(f"remote {target}: {exc}", file=sys.stderr)
            failed = True
    if json_output:
        print(json.dumps(results, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_remote_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the remotes this project and this user define."""

    print(json.dumps(list_remotes(context.cwd), indent=2, sort_keys=True))
    return 0


def handle_remote_add(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Create one remote bundle from a packaged adapter template."""

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_add, "name")
    template = _required(
        arguments.template,
        "adapter template",
        non_interactive=arguments.non_interactive,
        default="local",
    )
    print(
        add_remote(
            arguments.name,
            template=template,
            global_scope=arguments.global_scope,
            project=context.cwd,
        )
    )
    return 0


def handle_remote_adapter_operation(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Run the ``configure`` or ``check`` verb of one remote adapter.

    ``check`` runs the adapter operation that keeps its historical protocol
    spelling ``install``: it verifies the target has a working httk and never
    installs anything.
    """

    if isinstance(arguments.remote, list):
        return _remote_batch(arguments, context, handle_remote_adapter_operation, "remote", json_output=True)
    operation = arguments.operation
    target = resolve_remote(arguments.remote, project=context.cwd)
    settings = _settings(arguments.set)
    if operation == "configure":
        split_settings(settings)
    result = run_adapter(
        target.bundle,
        operation,
        {"settings": settings},
        timeout=arguments.adapter_timeout,
    )
    if operation == "configure" and settings:
        persistable, credentials = split_settings(settings)
        if persistable:
            metadata = read_metadata(target.bundle)
            remote_settings = metadata.setdefault("settings", {})
            if not isinstance(remote_settings, dict):
                raise ValueError("adapter settings are not mutable JSON")
            remote_settings.update(persistable)
            # A bundle that still carries the pre-rename file name keeps it, so
            # configuring an old definition rewrites what is there rather than
            # leaving two metadata files behind.
            write_json_atomic(metadata_path(target.bundle), metadata)
        if credentials:
            path = store_credentials(target.bundle, credentials)
            names = ", ".join(sorted(credentials))
            print(
                f"stored {names} for remote {target.name} in {path}; "
                "values there are excluded from signed project manifests",
                file=sys.stderr,
            )
    print(json.dumps(result, indent=2, sort_keys=True))
    if operation == "configure":
        print(
            f"configured; verify httk is available there with: httk workflow remote check {arguments.remote}",
            file=sys.stderr,
        )
    return 1 if result.get("returncode") not in (None, 0) else 0


def handle_remote_import_v1(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Map one recognized legacy httk v1 computer bundle into a remote."""

    if isinstance(arguments.source, list):
        if arguments.name and len(arguments.source) != 1:
            raise ValueError("remote import-v1 --name requires exactly one SOURCE")
        return _remote_batch(arguments, context, handle_remote_import_v1, "source")
    print(
        import_v1_remote(
            arguments.source,
            name=arguments.name,
            global_scope=arguments.global_scope,
            project=context.cwd,
        )
    )
    return 0


def _render_remote(description: dict[str, Any]) -> str:
    """Render one remote description as readable lines."""

    lines = [
        _field("name", description.get("name")),
        _field("scope", description.get("scope")),
        _field("bundle", description.get("bundle")),
        _field("kind", description.get("kind") or "-"),
        _field("adapter_version", description.get("adapter_version") or "-"),
        _field(
            "valid",
            "yes" if description.get("valid") else f"no: {description.get('problem')}",
        ),
        _field("timeout_seconds", description.get("timeout_seconds")),
        _field(
            "required_binaries",
            ", ".join(description.get("required_binaries", [])) or "-",
        ),
        _field("credentials_file", description.get("credentials_file") or "-"),
    ]
    settings = description.get("settings", {})
    if isinstance(settings, dict):
        for key, value in sorted(settings.items()):
            lines.append(f"{key}={value}")
    for key in description.get("credential_keys", []):
        lines.append(f"{key}=<credential>")
    return "\n".join(lines)


def handle_remote_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one remote: where it lives, what it is, how it is configured."""

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_show, "name")
    description = describe_remote(arguments.name, project=context.cwd)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_remote(description))
    return 0


def handle_remote_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove one remote bundle, after asking unless told not to.

    ``--force`` skips the confirmation, and nothing else: a remote a sealed
    transfer still depends on is refused either way, because removing it would
    leave that transfer with no way home.
    """

    if isinstance(arguments.name, list):
        return _remote_batch(arguments, context, handle_remote_remove, "name", json_output=True)
    if not confirm(f"remove the remote {arguments.name!r} and everything configured in it?", force=arguments.force):
        return 1
    print(json.dumps(remove_remote(arguments.name, project=context.cwd), indent=2, sort_keys=True))
    return 0


def build_remote_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``remote`` group: the adapters that reach other machines."""

    _, group = _group(
        subparsers,
        "remote",
        summary="define, configure, describe, and remove remotes",
        description="Define, configure, describe, and remove the remote adapters of this project",
    )

    _leaf(
        group,
        "list",
        summary="list the remotes this project can reach",
        description="List the remotes this project and this user define",
        handler=handle_remote_list,
    )

    add = _leaf(
        group,
        "add",
        summary="create a remote from a packaged template",
        description="Create one remote bundle from a packaged adapter template",
        handler=handle_remote_add,
    )
    add.add_argument("name", metavar="NAME", nargs="+", help="the name this remote is addressed by")
    add.add_argument(
        "--template",
        metavar="TEMPLATE",
        help="local or ssh (default: local)",
    )
    add.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )
    add.add_argument(
        "--non-interactive",
        action="store_true",
        help="never prompt; refuse a missing value",
    )

    # The check verb runs the adapter operation whose frozen protocol spelling
    # is "install"; the maintained adapters only ever verify, never install.
    for verb, operation, summary in (
        ("configure", "configure", "run the adapter's configure operation"),
        ("check", "install", "check that httk answers on the remote"),
    ):
        parser = _leaf(
            group,
            verb,
            summary=summary,
            description=f"Run the {verb} operation of one remote adapter",
            handler=handle_remote_adapter_operation,
        )
        parser.set_defaults(operation=operation)
        parser.add_argument(
            "remote", metavar="NAME", nargs="+", help="the remote name; ':' addresses a workspace on a remote"
        )
        parser.add_argument(
            "--set",
            action="append",
            default=[],
            metavar="KEY=VALUE",
            help=(
                "one adapter setting, e.g. host=, username=, port=, httk_command=, or "
                "prelude= (shell run before each remote command, e.g. module loads and a "
                "venv activation); a secret one is stored in credentials.json (repeatable)"
            ),
        )
        parser.add_argument(
            "--adapter-timeout",
            type=float,
            metavar="SECONDS",
            help="bound this adapter operation (default: the remote's timeout_seconds)",
        )

    imported = _leaf(
        group,
        "import-v1",
        summary="map a legacy computer bundle",
        description="Map recognized legacy httk v1 computer bundles into remote adapter bundles",
        handler=handle_remote_import_v1,
    )
    imported.add_argument("source", metavar="SOURCE", nargs="+", help="legacy computer directories to read")
    imported.add_argument("--name", metavar="NAME", help="the name to define for one source (default: the legacy one)")
    imported.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="define the remote for this user rather than for this project",
    )

    show = _leaf(
        group,
        "show",
        summary="describe one remote",
        description="Describe one remote: where it lives, what it is, and how it is configured",
        handler=handle_remote_show,
    )
    show.add_argument("name", metavar="NAME", nargs="+", help="the remote to describe")
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    remove = _leaf(
        group,
        "remove",
        summary="remove one remote bundle",
        description="Remove one remote bundle, refusing while a sealed transfer still needs it",
        handler=handle_remote_remove,
    )
    remove.add_argument("name", metavar="NAME", nargs="+", help="the remote to remove")
    remove.add_argument(
        "--force",
        action="store_true",
        help="skip the confirmation; a remote an unretired transfer still needs is refused either way",
    )


# ---------------------------------------------------------------------------
# transfer (formerly tasks, then remote)
# ---------------------------------------------------------------------------


def _remote_workspace_probe(
    target: Any,
    name: str,
    *,
    timeout: float | None,
    noun: str = "destination",
) -> tuple[str, str]:
    """Probe a remote workspace through the CLI adapter seam."""

    return probe_remote_workspace(target, name, timeout=timeout, noun=noun, adapter=run_adapter)


def _environment_advisory(
    source: Workspace,
    jobs: Sequence[str],
    settings: Mapping[str, object] | None,
    *,
    strict: bool,
    candidates: Sequence[TransferCandidate] | None = None,
    quiet: bool = False,
) -> None:
    """Warn about destination environment gaps before transfer state moves."""

    if settings is None:
        message = (
            "warning: destination environment could not be prechecked remotely; "
            "transfer continues (use --strict-environment to block)"
        )
        if strict:
            if not quiet:
                print(message, file=sys.stderr)
            raise ValueError("strict environment mode blocked an unreachable destination precheck")
        if not quiet:
            print(message, file=sys.stderr)
        return
    problems: list[str] = []
    selected_candidates = candidates
    if selected_candidates is None:
        loaded: list[TransferCandidate] = []
        for job_id in jobs:
            marker = source.find_marker_by_id(job_id)
            if marker is not None:
                try:
                    job = source.load_job(marker)
                    problem = None
                except (WorkflowError, OSError) as exc:
                    job = None
                    problem = str(exc)
                loaded.append(
                    TransferCandidate(
                        marker.job_id,
                        marker.job_key,
                        marker.kind,
                        marker.placement,
                        None,
                        marker,
                        job,
                        None,
                        problem,
                    )
                )
        selected_candidates = loaded
    for candidate in selected_candidates:
        if candidate.problem is not None:
            problems.append(f"{candidate.job_id}: {candidate.problem}")
            continue
        job = candidate.job
        if job is None:
            problems.append(f"{candidate.job_id}: job definition is unreadable")
            continue
        finding = environment_findings(job, settings, include_process_environment=False)
        entries = finding["entries"]
        assert isinstance(entries, list)
        names = [
            str(entry["name"])
            for entry in entries
            if isinstance(entry, Mapping) and entry.get("status") == "unresolved"
        ]
        problems_found = finding["problems"]
        assert isinstance(problems_found, list)
        detail = [str(item) for item in problems_found]
        if names or detail:
            problems.append(f"{job.id}: {', '.join(names + detail)}")
    if not problems:
        return
    message = "destination environment unresolved: " + "; ".join(problems)
    if strict:
        raise ValueError(f"strict environment precheck blocked transfer: {message}")
    if not quiet:
        print(f"warning: {message}", file=sys.stderr)


def _resolve_transfer_jobs(workspace: Workspace, cwd: Path, selectors: Sequence[str]) -> list[str]:
    """Resolve public transfer selectors, retaining exact IDs for resumptions."""

    resolver = JobSelectorResolver(workspace, cwd)
    job_ids: list[str] = []
    seen: set[str] = set()
    for selector in selectors:
        try:
            markers = resolver.resolve_one(selector)
        except ValueError as exc:
            if str(exc) != f"no job in {workspace.root} matches {selector!r}":
                raise
            try:
                _, job_id = parse_job_key(selector)
            except (WorkflowError, ValueError, TypeError):
                raise exc from None
            resolved_ids = [job_id]
        else:
            resolved_ids = [marker.job_id for marker in markers]
        for job_id in resolved_ids:
            if job_id not in seen:
                seen.add(job_id)
                job_ids.append(job_id)
    return job_ids


def _remote_workspace_settings(target: Any, name: str, *, timeout: float | None) -> dict[str, object] | None:
    """Read destination settings through a remote adapter, or report unavailable."""

    result = read_adapter(
        target.bundle,
        "invoke",
        {"argv": [*REMOTE_WORKSPACE_SETTINGS_COMMAND, "show", "--json", name]},
        timeout=timeout,
    )
    if result.get("returncode") != 0:
        raise RuntimeError(f"remote destination settings read failed: {result.get('stderr', '')}")
    try:
        values = json.loads(str(result.get("stdout", "")))
    except json.JSONDecodeError as exc:
        raise ValueError("remote destination settings were not a JSON object") from exc
    if not isinstance(values, list) or len(values) != 1:
        raise ValueError("remote destination settings were not a JSON object")
    value = values[0]
    if not isinstance(value, dict):
        raise ValueError("remote destination settings were not a JSON object")
    return value


def _send_jobs_to_remote(
    source: Workspace,
    target: Any,
    destination_name: str,
    jobs: Sequence[str],
    *,
    destination_placement: str | None,
    timeout: float | None,
    destination_settings: Mapping[str, object] | None = None,
    strict_environment: bool = False,
    quiet: bool = False,
    known_markers: Sequence[Marker] | None = None,
) -> list[dict[str, object]]:
    """Detach the named jobs from *source* and import them on a remote.

    This is the local→remote leg of the ``transfer`` verb: probe the destination
    workspace, then for each job seal a detached bundle, push it, and ask the far
    side to import it. Every step is idempotent, so an interrupted transfer is
    finished by running the same command again.
    """

    destination_workspace_id, destination_root = _remote_workspace_probe(target, destination_name, timeout=timeout)
    waiting_parent_map = _waiting_parent_map(source)
    precheck_candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination_workspace_id,
        states=(*QUIESCENT_KINDS, "transferring"),
        job_ids=jobs,
        destination_remote=target.name,
        include_transferring=True,
        known_markers=known_markers,
        waiting_parent_map=waiting_parent_map,
    )
    if destination_settings is not None:
        _environment_advisory(
            source,
            jobs,
            destination_settings,
            strict=strict_environment,
            candidates=precheck_candidates,
            quiet=quiet,
        )
    source.recover_transfers()
    acknowledgements: list[dict[str, object]] = []
    known_by_id = {} if known_markers is None else {marker.job_id: marker for marker in known_markers}
    for job_id in jobs:
        source.recover_transfers()
        candidates: list[tuple[Path, dict[str, object]]] = []
        for ledger_path in (source.control / "transfers").glob("*.json"):
            ledger = read_json(ledger_path)
            if ledger.get("job_id") != job_id or ledger.get("status") != "sealed":
                continue
            if ledger.get("destination_workspace_id") != destination_workspace_id:
                continue
            if "destination_remote" not in ledger:
                raise ValueError(
                    f"cannot resume job {job_id}: sealed transfer ledger {ledger_path} has no destination_remote"
                )
            if ledger.get("destination_remote") == target.name:
                candidates.append((ledger_path, ledger))
        if len(candidates) > 1:
            ledger_path = candidates[0][0]
            raise ValueError(
                f"cannot resume job {job_id}: sealed transfer ledger {ledger_path} is ambiguous; "
                "retire it or fetch the job from the destination"
            )
        transfer_id = str(candidates[0][1]["transfer_id"]) if candidates else str(uuid.uuid4())
        if candidates and destination_placement:
            requested = str(destination_placement).strip("/")
            if candidates[0][1].get("destination_placement") != requested:
                raise ValueError("resumed transfer destination placement disagrees with the request")
        bundle = source.detach(
            job_id,
            marker=known_by_id.get(job_id),
            waiting_parent_map=waiting_parent_map,
            destination_workspace_id=destination_workspace_id,
            destination_remote=target.name,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
        )
        incoming = f"{destination_root.rstrip('/')}/{WORKSPACE_DIRECTORY}/transfers/incoming/{transfer_id}"
        push = run_adapter(
            target.bundle,
            "push",
            {"source": str(bundle), "destination": incoming},
            timeout=timeout,
        )
        remote_bundle = str(push.get("path", incoming))
        invoked = run_adapter(
            target.bundle,
            "invoke",
            {
                "argv": [
                    *REMOTE_RECEIVE_COMMAND,
                    "--workspace",
                    destination_name,
                    "--bundle",
                    remote_bundle,
                ],
            },
            timeout=timeout,
        )
        if invoked.get("returncode") != 0:
            raise RuntimeError(f"destination import failed: {invoked.get('stderr', '')}")
        if not quiet:
            _relay_success_stderr(invoked)
        try:
            acknowledgement = json.loads(str(invoked.get("stdout", "")))
        except json.JSONDecodeError as exc:
            raise ValueError("destination import did not return an acknowledgement") from exc
        if not isinstance(acknowledgement, dict):
            raise ValueError("destination acknowledgement is not an object")
        source.acknowledge_transfer(acknowledgement)
        acknowledgements.append(acknowledgement)
    return acknowledgements


def _protocol_workspace(value: str, context: CLIContext) -> Workspace:
    """Resolve a protocol workspace name, with narrow legacy path support."""

    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = Path(context.cwd) / candidate
    path_like = (
        value in {".", ".."} or ":" in value or os.sep in value or (os.altsep is not None and os.altsep in value)
    )
    # A path-like value never reads the registry through resolve_workspace (it
    # fails name validation first), so read it explicitly: a corrupt registry
    # must surface rather than be masked by the path fallback.
    list_workspaces()
    try:
        binding = resolve_workspace(value, project=context.cwd)
    except ResolutionMiss:
        if not path_like and not candidate.is_dir():
            raise
        return Workspace(candidate)
    if binding.remote != LOCAL_REMOTE or binding.path is None:
        raise ValueError(f"protocol workspace must be local on this machine: {value}")
    return Workspace(binding.path)


def handle_transfer_receive(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Import by registry name, with an explicit-path compatibility fallback.

    Names are tried first. A path is accepted only when it contains a path
    separator or already names an existing directory; receive has no hidden
    ``--by-path`` spelling.
    """

    workspace = _protocol_workspace(arguments.workspace, context)
    acknowledgement = workspace.import_bundle(arguments.bundle)
    print(json.dumps(acknowledgement, sort_keys=True, separators=(",", ":")))
    _print_build_reminder(workspace, acknowledgement)
    return 0


def handle_transfer_offer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal the finished jobs of this workspace for one that will fetch them."""

    workspace = _protocol_workspace(arguments.workspace, context)
    offer_states = tuple(
        arguments.state
        if arguments.state is not None
        else (QUIESCENT_KINDS if arguments.jobs else DEFAULT_OFFER_STATES)
    )
    environment_settings = None
    if arguments.environment_settings:
        try:
            environment_settings = json.loads(arguments.environment_settings)
        except json.JSONDecodeError as exc:
            raise ValueError("remote offer environment settings are not valid JSON") from exc
        if not isinstance(environment_settings, dict):
            raise ValueError("remote offer environment settings must be an object")
        candidates = select_transfer_jobs(
            workspace,
            destination_workspace_id=arguments.destination_workspace_id,
            states=(*offer_states, "transferring"),
            placement=arguments.placement,
            job_ids=arguments.jobs or None,
            include_transferring=True,
        )
        _environment_advisory(
            workspace,
            [candidate.job_id for candidate in candidates],
            environment_settings,
            strict=arguments.strict_environment,
            candidates=candidates,
        )
    offers = offer_transfers(
        workspace,
        destination_workspace_id=arguments.destination_workspace_id,
        states=offer_states,
        placement=arguments.placement,
        job_ids=arguments.jobs or None,
    )
    if arguments.json:
        document = {
            "format": TRANSFER_OFFER_FORMAT,
            "format_version": 2,
            "workspace_id": workspace.workspace_id,
            "destination_workspace_id": arguments.destination_workspace_id,
            "offers": offers,
        }
        print(json.dumps(document, sort_keys=True, separators=(",", ":")))
        return 0
    for offer in offers:
        print(f"{offer['job_key']}\t{offer['state']}\t{offer['bundle_path']}")
    return 0


def handle_transfer_retire(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Retire the sealed source bundles of jobs another workspace has imported."""

    retired = retire_transfers(
        _protocol_workspace(arguments.workspace, context),
        arguments.jobs,
        destination_workspace_id=arguments.destination_workspace_id,
    )
    if arguments.json:
        document = {
            "format": TRANSFER_RETIREMENT_FORMAT,
            "format_version": 2,
            "retired": retired,
        }
        print(json.dumps(document, sort_keys=True, separators=(",", ":")))
        return 0
    for entry in retired:
        print(f"{entry['job_key']}\t{entry['status']}\t{entry['retired_bundle']}")
    return 0


def _remote_offer(
    target: Any,
    remote_name: str,
    destination_workspace_id: str,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
    job_ids: Sequence[str] | None = None,
    environment_settings: Mapping[str, object] | None = None,
    strict_environment: bool = False,
    quiet: bool = False,
) -> list[dict[str, object]]:
    """Ask a remote to seal its finished jobs and return the offers it made."""

    argv = [
        *REMOTE_OFFER_COMMAND,
        "--destination-workspace-id",
        destination_workspace_id,
        "--json",
    ]
    if states is not None:
        for state in states:
            argv += ["--state", state]
    elif not job_ids:
        for state in DEFAULT_OFFER_STATES:
            argv += ["--state", state]
    for job_id in job_ids or ():
        argv += ["--job", job_id]
    if placement:
        argv += ["--placement", placement]
    if environment_settings is not None:
        argv += ["--environment-settings", json.dumps(environment_settings, sort_keys=True, separators=(",", ":"))]
    if strict_environment:
        argv += ["--strict-environment"]
    argv.append(remote_name)
    offered = run_adapter(target.bundle, "invoke", {"argv": argv}, timeout=timeout)
    if offered.get("returncode") != 0:
        raise RuntimeError(f"remote offer failed: {offered.get('stderr', '')}")
    printed: set[str] = set()
    for field in ("stderr", "diagnostics"):
        diagnostics = offered.get(field)
        if diagnostics and str(diagnostics) not in printed:
            rendered = str(diagnostics)
            if not quiet:
                print(rendered, file=sys.stderr, end="" if rendered.endswith("\n") else "\n")
            printed.add(rendered)
    try:
        document = json.loads(str(offered.get("stdout", "")))
        if document.get("format") != TRANSFER_OFFER_FORMAT or document.get("format_version") != 2:
            raise ValueError
        offers = document["offers"]
        if not isinstance(offers, list):
            raise ValueError
    except (
        AttributeError,
        json.JSONDecodeError,
        KeyError,
        ValueError,
        TypeError,
    ) as exc:
        raise ValueError("remote offer did not return a transfer offer document") from exc
    return [offer for offer in offers if isinstance(offer, dict)]


def _require_offers_for_jobs(offers: Sequence[Mapping[str, object]], jobs: Sequence[str]) -> None:
    if not jobs:
        return
    requested = set(jobs)
    offered = {str(offer.get("job_id")) for offer in offers}
    missing = sorted(requested - offered)
    unexpected = sorted(offered - requested)
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected: {', '.join(unexpected)}")
        raise ValueError(f"remote offer did not exactly match requested jobs ({'; '.join(details)})")


def _remote_retire(
    target: Any,
    remote_name: str,
    job_ids: Sequence[str],
    destination_workspace_id: str,
    *,
    timeout: float | None,
) -> list[object]:
    """Tell a remote the sources of imported jobs are no longer needed there."""

    if not job_ids:
        return []
    argv = [
        *REMOTE_RETIRE_COMMAND,
        "--destination-workspace-id",
        destination_workspace_id,
        "--json",
        remote_name,
        *job_ids,
    ]
    response = run_adapter(target.bundle, "invoke", {"argv": argv}, timeout=timeout)
    if response.get("returncode") != 0:
        raise RuntimeError(f"remote retirement failed: {response.get('stderr', '')}")
    try:
        report = json.loads(str(response.get("stdout", "")))
        return list(report["retired"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise ValueError("remote retirement did not return a retirement report") from exc


def _fetch_jobs_from_remote(
    local: Workspace,
    target: Any,
    remote_name: str,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
    jobs: Sequence[str] = (),
    destination_settings: Mapping[str, object] | None = None,
    strict_environment: bool = False,
    quiet: bool = False,
) -> tuple[list[dict[str, object]], list[object]]:
    """Bring the jobs that finished on one remote back into *local*.

    Probe the remote workspace, ask it to offer what stopped there, pull each
    offered bundle into local staging, import it, and only then tell the remote
    to retire the sources it still holds. Every step is idempotent, so an
    interrupted fetch is finished by running the same command again.
    """

    _remote_workspace_probe(target, remote_name, timeout=timeout, noun="remote")
    offers = _remote_offer(
        target,
        remote_name,
        local.workspace_id,
        states=states,
        job_ids=jobs,
        placement=placement,
        timeout=timeout,
        environment_settings=destination_settings,
        strict_environment=strict_environment,
        quiet=quiet,
    )
    _require_offers_for_jobs(offers, jobs)
    staging_root = local.control / "transfers" / "incoming"
    acknowledgements: list[dict[str, object]] = []
    for offer in offers:
        transfer_id = canonical_uuid(offer.get("transfer_id"), "transfer_id")
        staging = staging_root / transfer_id
        pulled = run_adapter(
            target.bundle,
            "pull",
            {
                "source": str(offer["bundle_path"]),
                "destination": str(staging),
            },
            timeout=timeout,
        )
        acknowledgement = local.import_bundle(str(pulled.get("path", staging)))
        if not quiet:
            _print_build_reminder(local, acknowledgement)
        # The payload now lives at its placement in this workspace, so the
        # staged copy is dropped through a rename rather than left to be
        # re-imported by the next fetch.
        discard_staged_bundle(local, staging)
        acknowledgements.append(acknowledgement)
    retired = _remote_retire(
        target,
        remote_name,
        [str(acknowledgement["job_id"]) for acknowledgement in acknowledgements],
        local.workspace_id,
        timeout=timeout,
    )
    return acknowledgements, retired


def _transfer_local_to_local(
    source: Workspace,
    destination: Workspace,
    jobs: Sequence[str],
    *,
    strict_environment: bool = False,
    quiet: bool = False,
    known_markers: Sequence[Marker] | None = None,
) -> list[dict[str, object]]:
    """Move explicit jobs from one local workspace into another, directly."""

    if not jobs:
        raise ValueError("a local-to-local transfer needs at least one --job JOB_ID")
    waiting_parent_map = _waiting_parent_map(source)
    candidates = select_transfer_jobs(
        source,
        destination_workspace_id=destination.workspace_id,
        states=(*QUIESCENT_KINDS, "transferring"),
        job_ids=jobs,
        include_transferring=True,
        known_markers=known_markers,
        waiting_parent_map=waiting_parent_map,
    )
    _environment_advisory(
        source,
        jobs,
        destination.read_settings(),
        strict=strict_environment,
        candidates=candidates,
        quiet=quiet,
    )
    acknowledgements: list[dict[str, object]] = []
    known_by_id = {} if known_markers is None else {marker.job_id: marker for marker in known_markers}
    for job_id in jobs:
        source.recover_transfers()
        bundle = source.detach(
            job_id,
            marker=known_by_id.get(job_id),
            waiting_parent_map=waiting_parent_map,
            destination_workspace_id=destination.workspace_id,
            transfer_id=str(uuid.uuid4()),
        )
        acknowledgement = destination.import_bundle(str(bundle))
        source.acknowledge_transfer(acknowledgement)
        if not quiet:
            _print_build_reminder(destination, acknowledgement)
        acknowledgements.append(acknowledgement)
    return acknowledgements


def _transfer_remote_to_remote(
    source_binding: WorkspaceBinding,
    destination_binding: WorkspaceBinding,
    context: CLIContext,
    *,
    states: Sequence[str] | None,
    placement: str | None,
    timeout: float | None,
    jobs: Sequence[str] = (),
    destination_settings: Mapping[str, object] | None = None,
    strict_environment: bool = False,
    quiet: bool = False,
) -> tuple[list[dict[str, object]], list[object]]:
    """Relay jobs between two remotes through this client (v1 semantics).

    A direct remote-to-remote copy is deferred: this pulls each offered bundle
    from the source into local staging and pushes it to the destination, then
    asks the destination to import it and the source to retire the sources it
    still holds. Every leg reuses the same offer, pull, push, receive and retire
    the single-hop transfers use.
    """

    source_target = resolve_remote(source_binding.remote, project=context.cwd)
    destination_target = resolve_remote(destination_binding.remote, project=context.cwd)
    destination_name = destination_binding.name.split(":", 1)[1]
    source_name = source_binding.name.split(":", 1)[1]
    destination_workspace_id, destination_root = _remote_workspace_probe(
        destination_target, destination_name, timeout=timeout
    )
    _source_workspace_id, _source_root = _remote_workspace_probe(
        source_target, source_name, timeout=timeout, noun="source"
    )
    offers = _remote_offer(
        source_target,
        source_name,
        destination_workspace_id,
        states=states,
        job_ids=jobs,
        placement=placement,
        timeout=timeout,
        environment_settings=destination_settings,
        strict_environment=strict_environment,
        quiet=quiet,
    )
    _require_offers_for_jobs(offers, jobs)
    acknowledgements: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="httk-relay-") as relay:
        for offer in offers:
            transfer_id = canonical_uuid(offer.get("transfer_id"), "transfer_id")
            staging = Path(relay) / transfer_id
            pulled = run_adapter(
                source_target.bundle,
                "pull",
                {
                    "source": str(offer["bundle_path"]),
                    "destination": str(staging),
                },
                timeout=timeout,
            )
            local_bundle = str(pulled.get("path", staging))
            incoming = f"{destination_root.rstrip('/')}/{WORKSPACE_DIRECTORY}/transfers/incoming/{transfer_id}"
            pushed = run_adapter(
                destination_target.bundle,
                "push",
                {
                    "source": local_bundle,
                    "destination": incoming,
                },
                timeout=timeout,
            )
            remote_bundle = str(pushed.get("path", incoming))
            imported = run_adapter(
                destination_target.bundle,
                "invoke",
                {
                    "argv": [
                        *REMOTE_RECEIVE_COMMAND,
                        "--workspace",
                        destination_name,
                        "--bundle",
                        remote_bundle,
                    ],
                },
                timeout=timeout,
            )
            if imported.get("returncode") != 0:
                raise RuntimeError(f"destination import failed: {imported.get('stderr', '')}")
            if not quiet:
                _relay_success_stderr(imported)
            try:
                acknowledgement = json.loads(str(imported.get("stdout", "")))
            except json.JSONDecodeError as exc:
                raise ValueError("destination import did not return an acknowledgement") from exc
            if not isinstance(acknowledgement, dict):
                raise ValueError("destination acknowledgement is not an object")
            acknowledgements.append(acknowledgement)
    retired = _remote_retire(
        source_target,
        source_name,
        [str(acknowledgement["job_id"]) for acknowledgement in acknowledgements],
        destination_workspace_id,
        timeout=timeout,
    )
    return acknowledgements, retired


def handle_transfer(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move jobs between two registered workspaces, or run a protocol command.

    ``transfer SRC DST`` is the canonical verb. It resolves both names and moves
    work whichever way they point: local→remote seals and imports on the remote,
    remote→local fetches finished jobs home, local→local imports directly, and
    remote→remote relays through this client. The hidden ``receive``, ``offer``,
    and ``retire`` spellings are the frozen protocol one machine runs on another.
    """

    return _run_transfer_verb(arguments, context)


def _run_transfer_verb(
    arguments: argparse.Namespace,
    context: CLIContext,
) -> int:
    """Run the transfer operation and print its report."""

    return _report_transfer(arguments, run_transfer_verb_result(arguments, context))


def run_transfer_verb_result(
    arguments: argparse.Namespace,
    context: CLIContext,
    quiet: bool = False,
    known_markers: Sequence[Marker] | None = None,
) -> Mapping[str, object]:
    """Run the parsed transfer verb and return its report without formatting."""

    source_binding = resolve_workspace(arguments.source, project=context.cwd)
    destination_binding = resolve_workspace(arguments.destination, project=context.cwd)
    source_local = source_binding.remote == LOCAL_REMOTE
    destination_local = destination_binding.remote == LOCAL_REMOTE
    timeout = arguments.adapter_timeout

    if source_local and known_markers is None:
        assert source_binding.path is not None
        arguments.jobs = _resolve_transfer_jobs(Workspace(source_binding.path), context.cwd, arguments.jobs)
    elif not source_local:
        for selector in arguments.jobs:
            try:
                canonical_uuid(selector)
            except (WorkflowError, ValueError, TypeError):
                raise ValueError("remote source requires canonical job ids; path and prefix selectors are not allowed")

    if source_local and not destination_local:
        assert source_binding.path is not None
        target = resolve_remote(destination_binding.remote, project=context.cwd)
        if not arguments.jobs:
            raise ValueError("a local-to-remote transfer needs at least one --job JOB_ID")
        try:
            destination_settings = _remote_workspace_settings(
                target, destination_binding.name.split(":", 1)[1], timeout=timeout
            )
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            notice = (
                f"warning: destination environment could not be prechecked remotely: {exc}; "
                "transfer continues (use --strict-environment to block)"
            )
            if arguments.strict_environment:
                if not quiet:
                    print(notice, file=sys.stderr)
                raise ValueError("strict environment mode blocked an unreachable destination precheck") from exc
            if not quiet:
                print(notice, file=sys.stderr)
            destination_settings = None
        acknowledgements = _send_jobs_to_remote(
            Workspace(source_binding.path),
            target,
            destination_binding.name.split(":", 1)[1],
            arguments.jobs,
            destination_placement=arguments.destination_placement,
            timeout=timeout,
            destination_settings=destination_settings,
            strict_environment=arguments.strict_environment,
            quiet=quiet,
            known_markers=known_markers,
        )
        return {"moved": acknowledgements}
    if destination_local and not source_local:
        assert destination_binding.path is not None
        target = resolve_remote(source_binding.remote, project=context.cwd)
        acknowledgements, retired = _fetch_jobs_from_remote(
            Workspace(destination_binding.path),
            target,
            source_binding.name.split(":", 1)[1],
            states=arguments.state,
            jobs=arguments.jobs,
            placement=arguments.placement,
            timeout=timeout,
            destination_settings=Workspace(destination_binding.path, mutable=False).read_settings(),
            strict_environment=arguments.strict_environment,
            quiet=quiet,
        )
        return {"moved": acknowledgements, "retired": retired}
    if source_local and destination_local:
        assert source_binding.path is not None and destination_binding.path is not None
        acknowledgements = _transfer_local_to_local(
            Workspace(source_binding.path),
            Workspace(destination_binding.path),
            arguments.jobs,
            strict_environment=arguments.strict_environment,
            quiet=quiet,
            known_markers=known_markers,
        )
        return {"moved": acknowledgements}
    destination_target = resolve_remote(destination_binding.remote, project=context.cwd)
    try:
        destination_settings = _remote_workspace_settings(
            destination_target, destination_binding.name.split(":", 1)[1], timeout=timeout
        )
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        notice = (
            f"warning: destination environment could not be prechecked remotely: {exc}; "
            "transfer continues (use --strict-environment to block)"
        )
        if arguments.strict_environment:
            if not quiet:
                print(notice, file=sys.stderr)
            raise ValueError("strict environment mode blocked an unreachable destination precheck") from exc
        if not quiet:
            print(notice, file=sys.stderr)
        destination_settings = None
    acknowledgements, retired = _transfer_remote_to_remote(
        source_binding,
        destination_binding,
        context,
        states=arguments.state,
        jobs=arguments.jobs,
        placement=arguments.placement,
        timeout=timeout,
        destination_settings=destination_settings,
        strict_environment=arguments.strict_environment,
        quiet=quiet,
    )
    return {"moved": acknowledgements, "retired": retired}


def _report_transfer(arguments: argparse.Namespace, report: Mapping[str, object]) -> int:
    """Print the result of one ``transfer`` verb run."""

    if arguments.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    moved = report.get("moved", [])
    assert isinstance(moved, list)
    for acknowledgement in moved:
        assert isinstance(acknowledgement, Mapping)
        print(f"{acknowledgement['job_key']}\t{acknowledgement['state']}\t{acknowledgement['placement']}")
    return 0


def _dispatch_transfer_protocol(tokens: Sequence[str], context: CLIContext) -> int:
    """Parse and run one hidden ``receive``/``offer``/``retire`` protocol command."""

    parser = argparse.ArgumentParser(prog="httk workflow transfer", add_help=True)
    protocol = parser.add_subparsers(dest="_which", required=True)

    receive = protocol.add_parser("receive")
    receive.add_argument("--workspace", metavar="WORKSPACE", required=True)
    receive.add_argument("--bundle", metavar="BUNDLE", required=True)
    receive.set_defaults(handler=handle_transfer_receive)

    offer = protocol.add_parser("offer")
    offer.add_argument("workspace", metavar="WORKSPACE")
    offer.add_argument("--destination-workspace-id", metavar="UUID", required=True)
    offer.add_argument("--job", action="append", default=[], dest="jobs", metavar="JOB_ID")
    offer.add_argument("--state", action="append", metavar="STATE", choices=COLLECTABLE_KINDS)
    offer.add_argument("--placement", metavar="PLACEMENT")
    offer.add_argument("--json", action="store_true")
    offer.add_argument("--environment-settings", help=argparse.SUPPRESS)
    offer.add_argument("--strict-environment", action="store_true", help=argparse.SUPPRESS)
    offer.set_defaults(handler=handle_transfer_offer)

    retire = protocol.add_parser("retire")
    retire.add_argument("workspace", metavar="WORKSPACE")
    retire.add_argument("jobs", metavar="JOB_ID", nargs="+")
    retire.add_argument("--destination-workspace-id", metavar="UUID")
    retire.add_argument("--json", action="store_true")
    retire.set_defaults(handler=handle_transfer_retire)

    try:
        arguments = parser.parse_args(list(tokens))
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    return arguments.handler(arguments, context)


def build_transfer_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``transfer`` verb: move jobs between two registered workspaces."""

    transfer = _leaf(
        subparsers,
        "transfer",
        summary="move jobs between two registered workspaces",
        description=(
            "Move jobs between two registered workspaces: `transfer [OPTIONS] SRC DST`. It works whichever way the "
            "workspaces point — local to remote, remote to local, local to local, or remote to remote "
            "(relayed through this client). The hidden receive/offer/retire spellings are protocol."
        ),
        handler=handle_transfer,
    )
    transfer.add_argument("source", metavar="SRC", help="the registered workspace the jobs leave")
    transfer.add_argument("destination", metavar="DST", help="the registered workspace the jobs arrive in")
    transfer.add_argument(
        "--job",
        action="append",
        default=[],
        dest="jobs",
        metavar="JOB_ID",
        help="a local-source job UUID, unique prefix, or path; remote sources require canonical job IDs (repeatable)",
    )
    transfer.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help=f"state kind to move when fetching (repeatable, default: {', '.join(DEFAULT_OFFER_STATES)})",
    )
    transfer.add_argument("--placement", metavar="PLACEMENT", help="move only jobs at or below this placement")
    transfer.add_argument(
        "--destination-placement",
        metavar="PLACEMENT",
        help="where the jobs land (default: their placement)",
    )
    _add_adapter_timeout(transfer)
    transfer.add_argument(
        "--strict-environment",
        action="store_true",
        help="block before moving state when destination environment precheck is unavailable or unresolved",
    )
    transfer.add_argument("--json", action="store_true", help="print what moved as one JSON document")
