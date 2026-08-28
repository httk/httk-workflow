"""Runner and job command groups."""

import os
import time
import uuid

from ..adapters import (
    REMOTE_JOB_PUBLISH_REQUESTS_COMMAND,
    REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
    resolve_remote,
)
from ..configuration import OperatorIdentity, identity_seed, resolve_operator_identity, sign_document, verify_document
from ..introspection import read_managers
from ..models import TERMINAL_KINDS, Marker, ensure_step_known, parse_job_key
from ._common import *
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _durable,
    _group,
    _json_value,
    _leaf,
    _load_inputs,
    _local_root,
    _pairs,
    _resolve_binding,
    _run_adapter,
    _sanitize_tag,
)
from ._transfer import _protocol_workspace

# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


def _add_workspace_option(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help=f"{help_text} (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )


def handle_runner_publish(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish runner files or directories into a workspace runner store."""

    if arguments.name is not None and len(arguments.files) != 1:
        raise ValueError("--name can be used only when publishing one FILE_OR_DIRECTORY")
    workspace = Workspace(_local_root(arguments, context, action="publish a runner into it"))
    references: list[dict[str, object]] = []
    failed = False
    for source in arguments.files:
        try:
            reference = workspace.publish_runner(source, name=arguments.name, replace=arguments.replace)
        except _ERRORS as exc:
            failed = True
            print(f"{source}: {exc}", file=sys.stderr)
            continue
        references.append(reference)
        if not arguments.json:
            print(f"{source}: {reference['path']}")
    if arguments.json:
        print(json.dumps(references, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_runner_describe(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Report the runners a workspace has published, with their digests."""

    workspace = Workspace(_local_root(arguments, context, action="read its runners"), mutable=False)
    store = workspace.runners

    def published_entries(directory: Path) -> Iterator[Path]:
        for path in sorted(directory.iterdir()):
            if path.is_file():
                yield path
            elif path.is_dir():
                if (path / RUNNER_TREE_ENTRY).is_file():
                    yield path
                else:
                    yield from published_entries(path)

    names = arguments.names
    references: list[dict[str, object]] = []
    failed = False
    for name in names or [None]:
        try:
            if name is None:
                found = list(published_entries(store)) if store.is_dir() else []
            else:
                target = workspace.runner_store_path(name)
                if not target.is_file() and not target.is_dir():
                    raise ValueError(f"no such workspace runner: {name}")
                found = [target]
            references.extend(
                {
                    "source": "workspace",
                    "path": path.relative_to(store).as_posix(),
                    "sha256": tree_digest(path) if path.is_dir() else sha256_file(path),
                    "kind": "tree" if path.is_dir() else "file",
                    "inferred": path.is_dir(),
                }
                for path in found
            )
        except _ERRORS as exc:
            failed = True
            print(f"{name}: {exc}", file=sys.stderr)
    if arguments.json:
        print(json.dumps(references, indent=2, sort_keys=True))
        return 1 if failed else 0
    for reference in references:
        path = workspace.runner_store_path(str(reference["path"]))
        inferred = "\ttree (inferred)" if path.is_dir() else ""
        print(f"{reference['path']}\t{reference['sha256']}{inferred}")
    return 1 if failed else 0


def build_runner_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``runner`` group: the shared runners a workspace publishes."""

    _, group = _group(
        subparsers,
        "runner",
        summary="publish and describe the shared runners of a workspace",
        description="Manage the shared runners one workspace publishes for its jobs",
    )

    publish = _leaf(
        group,
        "publish",
        summary="publish one runner into a workspace runner store",
        description="Publish one runner file or directory into a workspace runner store, pinned by digest",
        handler=handle_runner_publish,
    )
    publish.add_argument("files", metavar="FILE_OR_DIRECTORY", nargs="+", help="runner files or directories to publish")
    _add_workspace_option(publish, help_text="the workspace to publish into")
    publish.add_argument(
        "--name",
        metavar="NAME",
        help="store name, including any subdirectory (default: the source name)",
    )
    publish.add_argument("--json", action="store_true", help="print published references as one JSON array")
    publish.add_argument(
        "--replace",
        action="store_true",
        help="overwrite a stored runner of the same name whose content differs",
    )

    describe = _leaf(
        group,
        "describe",
        summary="report the published runners and their digests",
        description="Report the runners a workspace has published, as the references a job pins",
        handler=handle_runner_describe,
    )
    describe.add_argument("names", metavar="NAME", nargs="*", help="store names (default: every published runner)")
    _add_workspace_option(describe, help_text="the workspace to read")
    describe.add_argument("--json", action="store_true", help="print the references as one JSON array")


# ---------------------------------------------------------------------------
# job
# ---------------------------------------------------------------------------


def handle_job_new(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Scaffold and submit one job or an input-source batch."""

    workspace = Workspace(_local_root(arguments, context, action="submit into it"))
    if arguments.workflow_dir is not None:
        workflow_dir = Path(arguments.workflow_dir).expanduser()
        if not workflow_dir.is_dir() or not (workflow_dir / "httk_workflow.toml").is_file():
            raise ValueError(f"--workflow-dir must name a directory containing httk_workflow.toml: {workflow_dir}")
        workflow_target: str | os.PathLike[str] = workflow_dir.resolve()
    else:
        workflow_target = arguments.workflow
    environment = {
        name: _json_value(text, f"workflow environment {name!r}")
        for name, text in _pairs(arguments.environment, "a workflow environment override")
    }
    parameters = {
        name: _json_value(text, f"job parameter {name!r}")
        for name, text in _pairs(arguments.parameters, "a job parameter")
    }
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    inputs, items, input_tag = _load_inputs(arguments.inputs, arguments.input_from)
    shared: dict[str, Any] = {
        "inputs": inputs,
        "files": files,
        "parameters": parameters,
        "environment": environment,
        "placement": arguments.placement,
        "priority": arguments.priority,
        "workdir_mode": arguments.workdir_mode,
        "data_mode": arguments.data_mode,
        "publish": arguments.publish,
        "step": arguments.step,
        "format": arguments.format,
        "name": arguments.name,
    }
    is_batch = bool(items)
    total = len(items)
    if items:
        for item in items:
            # In a batch, --tag prefixes each item's derived tag (run7-si2o), so
            # one flag names the whole sweep without erasing per-item identity; a
            # single-item submission keeps --tag as the whole tag. Re-sanitize the
            # composed tag so a 48-char derived tag or a prefix ending in '-' can
            # never emit an over-long tag or a forbidden '--'.
            derived = item.get("tag")
            if arguments.tag and derived:
                item["tag"] = _sanitize_tag(f"{arguments.tag}-{derived}")
            else:
                item["tag"] = arguments.tag or derived
        results: Iterator[ScaffoldedJob] = new_jobs(workspace, workflow_target, items, **shared)
    else:
        results = iter([new_job(workspace, workflow_target, tag=arguments.tag or input_tag, **shared)])

    program = f"{context.program} workflow"
    seen_warnings: set[str] = set()

    def _emit_warnings(job: ScaffoldedJob) -> None:
        for warning in job.warnings:
            if warning not in seen_warnings:
                seen_warnings.add(warning)
                print(f"{program}: warning: {warning}", file=sys.stderr)

    submitted = 0
    collected: list[ScaffoldedJob] = []
    try:
        for job in results:
            submitted += 1
            _emit_warnings(job)
            if arguments.json:
                collected.append(job)
            else:
                # One tab-separated line per job, so a shell reads the key of one
                # job with cut and a campaign streams as it is submitted.
                print(f"{job.job_key}\t{job.payload}")
    except _ERRORS:
        if is_batch:
            # A partial batch reports how far it got before failing, so an operator
            # knows how many jobs already landed; the exit stays 2 via dispatch.
            print(f"submitted {submitted} of {total} jobs before failing", file=sys.stderr)
        raise
    if arguments.json:
        # One self-describing report per job, as an array, exactly as `job_records
        # --json` prints one array of records.
        print(json.dumps([job.as_mapping() for job in collected], indent=2))
    if is_batch:
        # A batch submission ends with one count on stderr, so a scripted
        # submission of a directory can confirm how many jobs it created.
        print(f"submitted {submitted} jobs", file=sys.stderr)
    return 0


def add_job_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job submit`."""

    _add_workspace_option(parser, help_text="the workspace to submit into")
    parser.add_argument("sources", metavar="SOURCE", nargs="+", help="complete payload directories to submit")
    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        required=True,
        help="where the job lands in the tree",
    )
    parser.add_argument("--move", action="store_true", help="rename rather than copy the source")
    parser.add_argument("--json", action="store_true", help="print submitted markers as one JSON array")
    add_durability_arguments(parser)


def handle_job_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit prepared payload directories."""

    workspace = Workspace(
        _local_root(arguments, context, action="submit into it"),
        durable=_durable(arguments),
    )
    markers: list[str] = []
    failed = False
    for source in arguments.sources:
        try:
            marker = workspace.submit(source, arguments.placement, move=arguments.move)
        except _ERRORS as exc:
            failed = True
            print(f"{source}: {exc}", file=sys.stderr)
            continue
        markers.append(str(marker.path))
        if not arguments.json:
            print(f"{source}: {marker.path}")
    if arguments.json:
        print(json.dumps(markers, indent=2))
    return 1 if failed else 0


def add_job_request_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job request`."""

    parser.add_argument(
        "action",
        metavar="ACTION",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
        help="continue, override_step, cancel, set_priority, or pause",
    )
    _add_workspace_option(parser, help_text="the workspace holding the job")
    parser.add_argument(
        "job_id",
        metavar="JOB_ID",
        nargs="+",
        help="one or more job UUIDs the request is about",
    )
    parser.add_argument(
        "--operator",
        metavar="IDENTITY",
        required=False,
        help='configured identity short name or a literal "Name <email>"',
    )
    parser.add_argument(
        "--reason",
        metavar="TEXT",
        required=True,
        help="why, recorded in the state frame",
    )
    parser.add_argument(
        "--priority",
        type=int,
        metavar="PRIORITY",
        help="the new priority, for set_priority",
    )
    parser.add_argument("--step", metavar="STEP", help="the step to resume at, for override_step")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "accept the hazard of reviving a job a decided join already consumed; "
            "the hazard is journalled in the resulting state frame"
        ),
    )
    parser.add_argument("--wait", action="store_true", help="wait until every pause request reaches paused")
    parser.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        help="stop waiting after SECONDS (requires --wait)",
    )
    _add_adapter_timeout(parser)
    add_durability_arguments(parser)


def add_job_request_envelopes_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the hidden remote envelope-building protocol command."""

    parser.add_argument(
        "action",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
        help="the request action",
    )
    parser.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    parser.add_argument("job_id", metavar="JOB_ID", nargs="+", help="one or more job UUIDs")
    parser.add_argument("--operator", required=True, help="the operator attribution label")
    parser.add_argument("--reason", required=True, help="why the request is being made")
    parser.add_argument("--priority", type=int, help="the new priority")
    parser.add_argument("--step", help="the step to resume at")
    parser.add_argument("--force", action="store_true", help="accept an override hazard")
    parser.add_argument("--json", action="store_true", required=True, help=argparse.SUPPRESS)


def add_job_publish_requests_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare the hidden remote request-publication protocol command."""

    parser.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    parser.add_argument(
        "--document",
        action="append",
        dest="documents",
        required=True,
        metavar="JSON",
        help="one complete request document",
    )
    parser.add_argument("--wait", action="store_true", help="wait for pause requests to reach paused")
    parser.add_argument("--timeout", type=float, metavar="SECONDS", help="stop waiting after SECONDS")
    add_durability_arguments(parser)


def handle_job_request(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish operator requests against jobs and optionally wait for pauses."""

    if arguments.timeout is not None and not arguments.wait:
        raise ValueError("--timeout requires --wait")
    if arguments.wait and arguments.action != "pause":
        raise ValueError("--wait is only valid with the pause action")
    if arguments.timeout is not None and arguments.timeout < 0:
        raise ValueError("--timeout must not be negative")
    identity = resolve_operator_identity(arguments.operator)
    _ensure_identity_key(identity)

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None and ":" in binding.name
        return _request_remote_job(binding, context, arguments, identity)

    workspace = Workspace(root, durable=_durable(arguments))
    envelopes = _build_request_envelopes(workspace, arguments, identity.label)
    published: list[tuple[str, Marker, Path]] = []
    for job_id, marker, request in envelopes:
        document = dict(request) if identity.seed_path is None else sign_document(request, seed_path=identity.seed_path)
        if identity.seed_path is not None and not verify_document(document).valid:
            raise ValueError(f"local signature verification failed for job {job_id}")
        path = workspace.publish_request(document)
        published.append((job_id, marker, path))
    return _complete_job_requests(workspace, published, wait=arguments.wait, timeout=arguments.timeout)


def _build_request_envelopes(
    workspace: Workspace,
    arguments: argparse.Namespace,
    operator: str,
) -> list[tuple[str, Marker, dict[str, object]]]:
    """Resolve jobs and build unsigned operator request envelopes."""

    resolved: list[tuple[str, Marker]] = []
    for selector in arguments.job_id:
        marker = resolve_job(workspace, selector)
        if arguments.action == "override_step" and arguments.step is not None:
            _prevalidate_override_step(workspace, marker, arguments.step, force=bool(arguments.force))
        resolved.append((selector, marker))

    envelopes: list[tuple[str, Marker, dict[str, object]]] = []
    for job_id, marker in resolved:
        request: dict[str, object] = {
            "format": "httk-workflow-request",
            "format_version": 2,
            "request_id": str(uuid.uuid4()),
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "expected_generation": marker.generation,
            "expected_record_ref": marker.record_ref,
            "action": arguments.action,
            "operator": operator,
            "reason": arguments.reason,
            "created_at": utc_now(),
        }
        if arguments.priority is not None:
            request["priority"] = arguments.priority
        if arguments.step is not None:
            request["step"] = arguments.step
        if arguments.force:
            request["force"] = True
        envelopes.append((job_id, marker, request))
    return envelopes


_REQUEST_ACTIONS = frozenset(("continue", "override_step", "cancel", "set_priority", "pause"))
_REQUEST_MEMBERS = frozenset(
    {
        "format",
        "format_version",
        "request_id",
        "job_id",
        "job_key",
        "placement",
        "expected_generation",
        "expected_record_ref",
        "action",
        "operator",
        "reason",
        "created_at",
    }
)
_REQUEST_OPTIONAL_MEMBERS = frozenset(("priority", "step", "force"))
_REQUEST_SIGNATURE_MEMBERS = frozenset(("operator_key", "signature"))


def _validate_request_document(
    document: Mapping[str, object],
    *,
    index: int,
    expected_action: str | None = None,
    expected_operator: str | None = None,
    expected_reason: str | None = None,
    priority: int | None = None,
    step: str | None = None,
    force: bool = False,
    constrain_options: bool = False,
    allow_signature: bool = False,
) -> None:
    """Validate one operator request's exact schema and constrained values.

    :param document: Request document to validate.
    :param index: Zero-based document index for diagnostics.
    :param expected_action: Required action when validating a client response.
    :param expected_operator: Required operator label when validating a client response.
    :param expected_reason: Required reason when validating a client response.
    :param priority: Requested priority when client options are constrained.
    :param step: Requested step when client options are constrained.
    :param force: Whether the client requested the force member.
    :param constrain_options: Require optional members to match the client request exactly.
    :param allow_signature: Permit the optional operator_key/signature pair.
    :raises ValueError: If a member, type, or constrained value is invalid.
    """

    label = f"request envelope {index}"
    if not isinstance(document, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    members = set(document)
    if allow_signature and bool(members & _REQUEST_SIGNATURE_MEMBERS) and not _REQUEST_SIGNATURE_MEMBERS <= members:
        raise ValueError(f"{label} must contain both operator_key and signature")
    expected = set(_REQUEST_MEMBERS)
    if constrain_options:
        if priority is not None:
            expected.add("priority")
        if step is not None:
            expected.add("step")
        if force:
            expected.add("force")
    else:
        expected.update(members & _REQUEST_OPTIONAL_MEMBERS)
    if allow_signature:
        expected.update(members & _REQUEST_SIGNATURE_MEMBERS)
    missing = sorted(expected - members)
    extra = sorted(members - expected)
    if missing:
        raise ValueError(f"{label} is missing member {missing[0]!r}")
    if extra:
        raise ValueError(f"{label} has unexpected member {extra[0]!r}")

    exact_types: dict[str, type | tuple[type, ...]] = {
        "request_id": str,
        "job_id": str,
        "job_key": str,
        "placement": str,
        "expected_generation": int,
        "expected_record_ref": (str, type(None)),
        "operator": str,
        "reason": str,
        "created_at": str,
    }
    for member, expected_type in exact_types.items():
        value = document[member]
        if (
            (type(value) is not expected_type)
            if isinstance(expected_type, type)
            else not isinstance(value, expected_type)
        ):
            raise ValueError(f"{label} member {member!r} has the wrong type")
    if type(document["format_version"]) is not int or document["format_version"] != 2:
        raise ValueError(f"{label} member 'format_version' must be integer 2")
    if document["format"] != "httk-workflow-request":
        raise ValueError(f"{label} member 'format' must be 'httk-workflow-request'")
    action = document["action"]
    if not isinstance(action, str) or action not in _REQUEST_ACTIONS:
        raise ValueError(f"{label} member 'action' has an invalid value")
    if expected_action is not None and action != expected_action:
        raise ValueError(f"{label} member 'action' disagrees with the requested action")
    if expected_operator is not None and document["operator"] != expected_operator:
        raise ValueError(f"{label} member 'operator' disagrees with the requested identity")
    if expected_reason is not None and document["reason"] != expected_reason:
        raise ValueError(f"{label} member 'reason' disagrees with the requested reason")
    if "priority" in document and type(document["priority"]) is not int:
        raise ValueError(f"{label} member 'priority' has the wrong type")
    if "step" in document and not isinstance(document["step"], str):
        raise ValueError(f"{label} member 'step' has the wrong type")
    if "force" in document and document["force"] is not True:
        raise ValueError(f"{label} member 'force' must be true")
    if constrain_options:
        for member, expected_value in (("priority", priority), ("step", step)):
            if expected_value is not None and document[member] != expected_value:
                raise ValueError(f"{label} member {member!r} disagrees with the requested value")
        if force and document.get("force") is not True:
            raise ValueError(f"{label} member 'force' disagrees with the requested value")
    if allow_signature:
        for member in _REQUEST_SIGNATURE_MEMBERS:
            if member in document and not isinstance(document[member], str):
                raise ValueError(f"{label} member {member!r} has the wrong type")


def _validate_remote_envelopes(
    envelopes: list[dict[str, object]],
    arguments: argparse.Namespace,
    operator: str,
) -> None:
    """Validate leg-one envelopes against the exact local request."""

    if len(envelopes) != len(arguments.job_id):
        raise ValueError(
            f"remote returned {len(envelopes)} request envelopes for {len(arguments.job_id)} requested jobs"
        )
    for index, (envelope, selector) in enumerate(zip(envelopes, arguments.job_id, strict=True)):
        _validate_request_document(
            envelope,
            index=index,
            expected_action=arguments.action,
            expected_operator=operator,
            expected_reason=arguments.reason,
            priority=arguments.priority,
            step=arguments.step,
            force=bool(arguments.force),
            constrain_options=True,
        )
        job_id = envelope["job_id"]
        job_key = envelope["job_key"]
        assert isinstance(job_id, str) and isinstance(job_key, str)
        try:
            _, key_job_id = parse_job_key(job_key)
        except WorkflowError as exc:
            raise ValueError(f"request envelope {index} member 'job_key' is invalid: {exc}") from exc
        if key_job_id != job_id:
            raise ValueError(f"request envelope {index} member 'job_key' disagrees with 'job_id'")
        try:
            canonical_selector = str(uuid.UUID(selector)) if len(selector) == 36 else None
        except ValueError:
            canonical_selector = None
        if canonical_selector == selector:
            if job_id != selector:
                raise ValueError(f"request envelope {index} does not match UUID selector {selector!r}")
        elif not (job_id.startswith(selector) or job_key.startswith(selector)):
            raise ValueError(f"request envelope {index} does not match requested selector {selector!r}")


def _ensure_identity_key(identity: OperatorIdentity) -> None:
    """Refuse a configured identity whose seed file is absent or unreadable.

    :param identity: The identity selected for signing.
    :raises ValueError: If a configured identity has no usable seed file.
    """

    if identity.seed_path is not None and identity_seed(identity.seed_path) is None:
        if identity.short is None:
            raise ValueError(
                f"the default identity has no key file at {identity.seed_path}; "
                f"create one with `httk workflow config init`, "
                f"or restore the key file at {identity.seed_path}"
            )
        short = identity.short
        raise ValueError(
            f"identity {short!r} has no key file at {identity.seed_path}; "
            f"remove it with `httk workflow config identity remove {short}` then re-add it with "
            f"`httk workflow config identity add {short} ...`, or restore the key file at {identity.seed_path}"
        )


def _complete_job_requests(
    workspace: Workspace,
    published: list[tuple[str, Marker, Path]],
    *,
    wait: bool,
    timeout: float | None,
) -> int:
    """Print published requests and optionally wait for pause outcomes."""

    for _, _, path in published:
        print(path)

    if not wait:
        for _, marker, _ in published:
            _warn_if_no_live_manager(workspace, marker)
        return 0

    available = True
    for _, marker, _ in published:
        available &= _warn_if_no_live_manager(workspace, marker)
    if not available:
        print("waiting is pointless until a manager starts", file=sys.stderr)
        return 1
    return _wait_for_pauses(workspace, published, timeout=timeout)


def _request_remote_job(
    binding: WorkspaceBinding,
    context: CLIContext,
    arguments: argparse.Namespace,
    identity: OperatorIdentity,
) -> int:
    """Build remotely, sign locally, and publish requests remotely."""

    target = resolve_remote(binding.remote, project=context.cwd)
    remote_name = binding.name.split(":", 1)[1]
    envelope_argv = [
        *REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
        arguments.action,
        f"--workspace={remote_name}",
        f"--operator={identity.label}",
        f"--reason={arguments.reason}",
        "--json",
    ]
    for option in ("priority", "step"):
        value = getattr(arguments, option)
        if value is not None:
            envelope_argv.append(f"--{option}={value}")
    if arguments.force:
        envelope_argv.append("--force")
    envelope_argv.extend(arguments.job_id)
    result = _run_adapter(
        target.bundle,
        "invoke",
        {"argv": envelope_argv},
        timeout=arguments.adapter_timeout,
    )
    stderr = str(result.get("stderr", ""))
    if stderr:
        sys.stderr.write(stderr)
    if result.get("returncode") != 0:
        raise RuntimeError(
            f"remote request envelope build failed (exit {result.get('returncode')}); "
            "see the relayed remote error above"
        )
    try:
        document = json.loads(str(result.get("stdout", "")))
        if document.get("format") != "httk-workflow-request-envelopes" or document.get("format_version") != 1:
            raise ValueError
        envelopes = document["envelopes"]
        if not isinstance(envelopes, list) or not all(isinstance(item, dict) for item in envelopes):
            raise ValueError
    except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("remote did not return a valid request-envelopes document") from exc
    _validate_remote_envelopes(envelopes, arguments, identity.label)

    signed: list[str] = []
    for envelope in envelopes:
        signed_document = (
            dict(envelope) if identity.seed_path is None else sign_document(envelope, seed_path=identity.seed_path)
        )
        if identity.seed_path is not None and not verify_document(signed_document).valid:
            raise ValueError(f"local signature verification failed for job {envelope['job_id']}")
        signed.append(json.dumps(signed_document, separators=(",", ":")))

    publish_argv = [*REMOTE_JOB_PUBLISH_REQUESTS_COMMAND, f"--workspace={remote_name}"]
    for document_text in signed:
        publish_argv.append(f"--document={document_text}")
    if arguments.wait:
        publish_argv.append("--wait")
    if arguments.timeout is not None:
        publish_argv.append(f"--timeout={arguments.timeout}")
    if getattr(arguments, "durable", False):
        publish_argv.append("--durable")
    if getattr(arguments, "no_durable", False):
        publish_argv.append("--no-durable")
    published = _run_adapter(
        target.bundle,
        "invoke",
        {"argv": publish_argv},
        timeout=arguments.adapter_timeout,
    )
    stdout = str(published.get("stdout", ""))
    if stdout:
        sys.stdout.write(stdout)
    stderr = str(published.get("stderr", ""))
    if stderr:
        sys.stderr.write(stderr)
    return int(published.get("returncode", 0) or 0)


def handle_job_request_envelopes(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Build unsigned operator request envelopes for the remote protocol."""

    workspace = _protocol_workspace(arguments.workspace, context)
    envelopes = _build_request_envelopes(workspace, arguments, arguments.operator)
    print(
        json.dumps(
            {
                "format": "httk-workflow-request-envelopes",
                "format_version": 1,
                "envelopes": [request for _, _, request in envelopes],
            },
            separators=(",", ":"),
        )
    )
    return 0


def handle_job_publish_requests(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish request documents received from the local signing client."""

    workspace = _protocol_workspace(arguments.workspace, context)
    documents: list[dict[str, object]] = []
    for text in arguments.documents:
        try:
            document = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"request document is not valid JSON: {exc}") from exc
        if not isinstance(document, dict):
            raise ValueError("request document must be a JSON object")
        _validate_request_document(document, index=len(documents), allow_signature=True)
        documents.append(document)
    if arguments.timeout is not None and not arguments.wait:
        raise ValueError("--timeout requires --wait")
    if arguments.wait and any(document["action"] != "pause" for document in documents):
        raise ValueError("--wait is only valid with the pause action")
    if arguments.timeout is not None and arguments.timeout < 0:
        raise ValueError("--timeout must not be negative")

    resolved: list[tuple[str, Marker, dict[str, object]]] = []
    for document in documents:
        job_id = document["job_id"]
        assert isinstance(job_id, str)
        marker = workspace.find_marker_by_id(job_id)
        if marker is None:
            raise ValueError(f"job does not exist: {job_id}")
        resolved.append((job_id, marker, document))
    published = [(job_id, marker, workspace.publish_request(document)) for job_id, marker, document in resolved]
    return _complete_job_requests(workspace, published, wait=arguments.wait, timeout=arguments.timeout)


def _prevalidate_override_step(workspace: Workspace, marker: Marker, step: str, *, force: bool) -> None:
    """Refuse an override_step whose target is outside the job's recorded steps.

    The runner's real step set is recorded in the job's state frame as
    ``runner_steps`` after its first attempt, so the request can be refused here
    against that list — unless ``--force`` is given, since a payload runner is
    mutable and an operator may have edited it to add the step. Before the first
    attempt nothing is recorded, so the request is allowed with a note.

    :param workspace: The workspace holding the job's state frame.
    :param marker: Identify the job the request targets.
    :param step: The step the override_step request names.
    :param force: Whether ``--force`` downgrades the refusal to a note.
    :raises httk.workflow.errors.FormatError: If the recorded steps exclude *step* and *force* is not set.
    """

    try:
        state = workspace.read_state(marker)
    except (WorkflowError, OSError):
        state = {}
    runner_steps = state.get("runner_steps")
    if isinstance(runner_steps, list) and runner_steps:
        known = [str(item) for item in runner_steps]
        if step in known:
            return
        if not force:
            ensure_step_known(step, known, f"job {marker.job_key}")
        print(
            f"the step {step!r} is not one of this job's recorded runner steps ({', '.join(known)}), "
            "but --force was given: publishing anyway; the runner will refuse it at the next attempt "
            "if it does not implement it",
            file=sys.stderr,
        )
        return
    print(
        f"the step {step!r} could not be pre-validated: this job has not recorded its runner steps yet, "
        "so the runner will refuse it at the next attempt if it does not implement it",
        file=sys.stderr,
    )


def _warn_if_no_live_manager(workspace: Workspace, marker: Marker) -> bool:
    """Warn when no live manager serves the executor a published request needs.

    A request only takes effect when a manager applies it, so a request against
    a job whose executor nothing serves waits indefinitely with no error. The
    warning is advisory: the request is already published and stays valid until
    a manager starts.
    """

    try:
        executor = workspace.load_job(marker).runner_executor
    except (WorkflowError, OSError):
        return True
    if any(executor in record.executors for record in read_managers(workspace) if record.alive()):
        return True
    print(
        f"no live manager currently serves executor {executor!r}; the request will wait until one starts",
        file=sys.stderr,
    )
    return False


def _wait_for_pauses(
    workspace: Workspace,
    published: list[tuple[str, Marker, Path]],
    *,
    timeout: float | None,
) -> int:
    """Wait for published pause requests and report each final outcome."""

    pending = {path.name: (job_id, marker, path) for job_id, marker, path in published}
    outcomes: dict[str, str] = {}
    successful: set[str] = set()
    deadline = None if timeout is None else time.monotonic() + timeout
    while pending:
        for name, (job_id, original, path) in list(pending.items()):
            marker = workspace.find_marker_by_id(original.job_id)
            if marker is not None:
                if marker.kind == "paused":
                    outcomes[name] = f"{marker.job_key}: paused"
                    successful.add(name)
                elif marker.kind in TERMINAL_KINDS:
                    outcomes[name] = f"{marker.job_key}: {marker.kind} (pause superseded)"
            if name not in outcomes:
                retirement = workspace.control / "requests" / "retired" / f"{name}.retirement"
                if retirement.is_file():
                    reason = read_json(retirement).get("reason", "unknown reason")
                    outcomes[name] = f"{job_id}: request retired: {reason}"
            if name not in outcomes:
                quarantine = _find_quarantined_request(workspace, name)
                if quarantine is not None:
                    outcomes[name] = f"{job_id}: request quarantined ({quarantine})"
            if name in outcomes:
                pending.pop(name)
        if not pending:
            break
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))
        else:
            time.sleep(1.0)

    for name, (job_id, _, _) in pending.items():
        outcomes[name] = f"{job_id}: timeout; still pending (request remains published)"
    for _, _, path in published:
        print(outcomes[path.name])
    return int(len(successful) != len(published))


def _find_quarantined_request(workspace: Workspace, request_name: str) -> str | None:
    """Return a quarantine reason for *request_name*, if one is recorded."""

    quarantine = workspace.control / "quarantine"
    if not quarantine.is_dir():
        return None
    for entry in quarantine.iterdir():
        report_path = entry / "report.json"
        if not report_path.is_file():
            continue
        try:
            report = read_json(report_path)
        except WorkflowError:
            continue
        if Path(str(report.get("original_path", ""))).name == request_name:
            return str(report.get("reason", "invalid request"))
    return None


def handle_job_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the jobs of one workspace as a cheap table."""

    workspace = Workspace(_local_root(arguments, context, action="list its jobs"), mutable=False)
    rows = list_jobs(workspace, kinds=arguments.kind, placement=arguments.placement)
    if arguments.json:
        print(json.dumps({"format": JOB_LIST_FORMAT, "format_version": 2, "jobs": rows}, indent=2))
        return 0
    print(render_rows(rows))
    return 0


def handle_job_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe jobs completely from their authoritative state."""

    workspace = Workspace(_local_root(arguments, context, action="show its jobs"), mutable=False)
    reports: list[dict[str, object]] = []
    failed = False
    for job in arguments.jobs:
        try:
            report = describe_job(workspace, resolve_job(workspace, job))
        except _ERRORS as exc:
            failed = True
            print(f"{job}: {exc}", file=sys.stderr)
            continue
        reports.append(report)
        if not arguments.json:
            print(f"{job}:")
            print(render_job(report))
    if arguments.json:
        print(json.dumps(reports, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_job_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print recorded transition histories, oldest first."""

    if arguments.limit is not None and arguments.limit < 1:
        raise ValueError("--limit must be positive")
    workspace = Workspace(_local_root(arguments, context, action="read its job log"), mutable=False)
    reports: list[dict[str, object]] = []
    failed = False
    for job in arguments.jobs:
        try:
            frames = job_frames(workspace, resolve_job(workspace, job), limit=arguments.limit)
        except _ERRORS as exc:
            failed = True
            print(f"{job}: {exc}", file=sys.stderr)
            continue
        reports.append({"format": JOB_HISTORY_FORMAT, "format_version": 2, "frames": frames})
        if not arguments.json:
            print(f"{job}:")
            print(render_frames(frames))
    if arguments.json:
        print(json.dumps(reports, indent=2))
    return 1 if failed else 0


def handle_job_why(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Explain why jobs are, or are not, making progress."""

    workspace = Workspace(_local_root(arguments, context, action="explain its jobs"), mutable=False)
    diagnoses: list[dict[str, object]] = []
    failed = False
    for job in arguments.jobs:
        try:
            diagnosis = explain_job(workspace, resolve_job(workspace, job))
        except _ERRORS as exc:
            failed = True
            print(f"{job}: {exc}", file=sys.stderr)
            continue
        diagnoses.append(diagnosis.as_mapping())
        if not arguments.json:
            print(f"{job}:")
            print(diagnosis.render())
    if arguments.json:
        print(json.dumps(diagnoses, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_job_debug(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Drive one job to a terminal state in the foreground."""

    # The transitions of the debugged job are reported by the debug runner
    # itself, so the private manager's own log stays quiet unless asked for.
    configure_logging(level=arguments.log_level)
    workspace = Workspace(_local_root(arguments, context, action="debug in it"))
    outcome = debug_job(
        workspace,
        arguments.job,
        placement=arguments.placement,
        step=arguments.step,
        follow_children=arguments.follow_children,
        timeout=arguments.timeout,
    )
    return outcome.exit_code


def _add_job_selector(parser: argparse.ArgumentParser) -> None:
    """Add the workspace and job selectors every inspection command shares."""

    _add_workspace_option(parser, help_text="the workspace holding the job")
    parser.add_argument("jobs", metavar="JOB", nargs="+", help="job UUID, job key, or any unique prefix of either")


def build_job_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    *,
    program: str | None = None,
) -> None:
    """Declare the ``job`` group: making jobs, and finding out about them."""

    _, group = _group(
        subparsers,
        "job",
        summary="create, submit, inspect, and debug individual jobs",
        description="Create, submit, inspect, and debug the jobs of one execution workspace",
        prog=program,
    )

    new = _leaf(
        group,
        "new",
        summary="scaffold and submit jobs from a runner workflow",
        description="Scaffold and submit jobs from a runner workflow",
        handler=handle_job_new,
    )
    _add_workspace_option(new, help_text="the workspace to submit into")
    workflow_names = list(registered_workflow_labels())
    workflow_group = new.add_mutually_exclusive_group(required=True)
    workflow_group.add_argument(
        "--workflow",
        metavar="WORKFLOW",
        help="a registered workflow ("
        + ", ".join(workflow_names)
        + ") or the path of a runner file or package directory",
    )
    workflow_group.add_argument(
        "--workflow-dir",
        metavar="PATH",
        help="a workflow package directory containing httk_workflow.toml (path-only; no registry lookup)",
    )
    new.add_argument(
        "--parameter",
        action="append",
        default=[],
        dest="parameters",
        metavar="NAME=VALUE",
        help="one implementation parameter; VALUE is JSON when it parses as JSON and a string otherwise, "
        "and NAME=@FILE reads a JSON file (repeatable)",
    )
    new.add_argument(
        "--environment",
        action="append",
        default=[],
        dest="environment",
        metavar="NAME=VALUE",
        help="override one declared workflow environment value; VALUE is JSON when it parses as JSON (repeatable)",
    )
    new.add_argument(
        "--format",
        metavar="LANG",
        help="force LANG for a bare workflow document or directory (not a registered or manifest workflow)",
    )
    new.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="stage PATH in the payload as NAME; a bare NAME lands in files/ (repeatable)",
    )
    new.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one declared input value to stage (repeatable)",
    )
    new.add_argument(
        "--input-from",
        action="append",
        nargs="+",
        default=[],
        metavar=("NAME", "SOURCE"),
        help="load a declared input from one or more files, or readable files in a directory (repeatable)",
    )
    new.add_argument(
        "--tag",
        metavar="TAG",
        help="the readable half of the job key (default: derived from an input source)",
    )
    new.add_argument("--name", metavar="NAME", help="the human-readable job name")
    new.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default=DEFAULT_PLACEMENT,
        help=f"placement subtree (default: {DEFAULT_PLACEMENT})",
    )
    new.add_argument(
        "--priority",
        type=int,
        metavar="PRIORITY",
        help="scheduling priority (default: the workflow's)",
    )
    new.add_argument(
        "--step",
        metavar="STEP",
        help="the step the job starts at (default: the workflow's own)",
    )
    new.add_argument(
        "--data-mode",
        choices=("none", "transactional"),
        help="the job's data mode (default: what the workflow needs)",
    )
    new.add_argument(
        "--workdir-mode",
        choices=("persistent", "isolated"),
        default="persistent",
        help="the job's working-directory mode (default: persistent)",
    )
    new.add_argument(
        "--publish",
        choices=("workspace", "installed"),
        default="workspace",
        help="publish the runner into the workspace store (default), or reference a packaged one where it is installed",
    )
    new.add_argument("--json", action="store_true", help="print one JSON report per job, as an array")

    add_job_submit_arguments(
        _leaf(
            group,
            "submit",
            summary="submit a complete payload directory",
            description="Submit one complete payload directory into a workspace",
            handler=handle_job_submit,
        )
    )
    add_job_request_arguments(
        _leaf(
            group,
            "request",
            summary="publish an operator request",
            description="Publish one operator request against a job of a workspace",
            handler=handle_job_request,
        )
    )
    add_job_request_envelopes_arguments(
        _leaf(
            group,
            "request-envelopes",
            description="Build unsigned operator request envelopes for a signing client",
            summary="build unsigned request envelopes",
            handler=handle_job_request_envelopes,
            hidden=True,
        )
    )
    add_job_publish_requests_arguments(
        _leaf(
            group,
            "publish-requests",
            description="Publish signed operator request documents from a signing client",
            summary="publish signed request documents",
            handler=handle_job_publish_requests,
            hidden=True,
        )
    )

    listing = _leaf(
        group,
        "list",
        summary="list the jobs of a workspace",
        description="List the jobs of one execution workspace",
        handler=handle_job_list,
    )
    _add_workspace_option(listing, help_text="the workspace to list")
    listing.add_argument(
        "--kind",
        action="append",
        metavar="KIND",
        choices=STATE_KINDS,
        help="state kind to list (repeatable, default: every kind)",
    )
    listing.add_argument(
        "--placement",
        metavar="PLACEMENT",
        help="list only jobs at or below this placement",
    )
    listing.add_argument("--json", action="store_true", help="print the rows as one JSON document")

    show = _leaf(
        group,
        "show",
        summary="describe jobs from their authoritative state",
        description="Describe jobs from their authoritative state",
        handler=handle_job_show,
    )
    _add_job_selector(show)
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    log = _leaf(
        group,
        "log",
        summary="print transition histories",
        description="Print recorded transition histories, oldest first",
        handler=handle_job_log,
    )
    _add_job_selector(log)
    log.add_argument(
        "--limit",
        type=int,
        metavar="COUNT",
        help="read at most this many frames, newest first",
    )
    log.add_argument("--json", action="store_true", help="print the frames as one JSON document")

    why = _leaf(
        group,
        "why",
        summary="explain why jobs are not running",
        description="Explain why jobs are, or are not, making progress",
        handler=handle_job_why,
    )
    _add_job_selector(why)
    why.add_argument("--json", action="store_true", help="print the diagnosis as one JSON document")

    debug = _leaf(
        group,
        "debug",
        summary="drive one job to a terminal state in the foreground",
        description="Drive one job to a terminal state in the foreground, reporting every transition",
        handler=handle_job_debug,
    )
    _add_workspace_option(debug, help_text="the workspace to debug in")
    debug.add_argument(
        "job",
        metavar="JOB",
        help="a payload directory to submit, or a selector of an existing job",
    )
    debug.add_argument("--step", metavar="STEP", help="initial step of a freshly submitted payload")
    debug.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default="debug",
        help="placement of a freshly submitted payload (default: debug)",
    )
    debug.add_argument(
        "--follow-children",
        action="store_true",
        help="drive spawned children depth first",
    )
    debug.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        default=3600.0,
        help="give up driving the job after this long (default: 3600)",
    )
    debug.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default="error",
        help="log level of the private manager on the console (default: error)",
    )
