"""The ``job`` command group: creating, submitting, inspecting and steering jobs on the filesystem kernel."""

import argparse
import json
import re
import shlex
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.cli import CLIContext
from httk.core.identity import (
    OperatorIdentity,
    configured_operator_identity,
    identity_seed,
    resolve_operator_identity,
    sign_document,
)

from .. import _fs, _kernel, _moving, _requests
from .._kernel import JobRef
from .._logging import LOG_LEVELS, configure_logging
from .._state import TERMINAL_STATES
from .._util import utc_now
from ..adapters import (
    REMOTE_JOB_DELETE_COMMAND,
    REMOTE_JOB_LIST_COMMAND,
    REMOTE_JOB_LOG_COMMAND,
    REMOTE_JOB_PUBLISH_REQUESTS_COMMAND,
    REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
    REMOTE_JOB_SHOW_COMMAND,
    REMOTE_JOB_WHY_COMMAND,
    resolve_remote,
    run_adapter,
)
from ..errors import FormatError, WorkflowError
from ..introspection import (
    JOB_HISTORY_FORMAT,
    JOB_LIST_FORMAT,
    JOB_STATES,
    JobSelectorResolver,
    claim_requirements,
    count_jobs,
    debug_job,
    describe_job,
    explain_held,
    explain_job,
    job_events,
    job_placement,
    list_jobs,
    manager_refusals,
    read_job,
    read_managers,
    read_state,
    render_events,
    render_job,
    render_rows,
    resolve_job,
    resolve_job_selectors,
    selector_uses_remote_path,
)
from ..introspection._reading import _jsonl
from ..models import JOB_STATE_DIRECTORY, canonical_uuid, ensure_step_known, parse_job_key, placement_text
from ..registry import WorkspaceBinding
from ..removal import RemovalReport, remove_jobs, request_now
from ..scaffold import (
    DEFAULT_PLACEMENT,
    ScaffoldedJob,
    _sanitize_tag,
    new_job,
    new_jobs,
    payload_relative,
    registered_workflow_labels,
    submit_payload,
)
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _group,
    _json_value,
    _leaf,
    _load_inputs,
    _modifiable,
    _pairs,
    _remote_workspace_read,
    _resolve_binding,
    add_durability_arguments,
    confirm,
    remote_workspace_output,
)
from ._transfer import _protocol_workspace, adopt_document, build_transfer_parser, eject_once

_ENVELOPES_FORMAT = "httk-workflow-request-envelopes"
#: The option each action requires (and that no other action takes).
_ACTION_OPTIONS = {"priority": "set_priority", "step": "override_step", "destination": "eject"}
_FORCE_ACTIONS = ("continue", "override_step")


def _add_workspace_option(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help=f"{help_text} (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )


# ---------------------------------------------------------------------------
# job new / job submit
# ---------------------------------------------------------------------------


_COMMAND_PARAMETER_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*\Z")


def _command_word_parts(word: str) -> list[tuple[str, str]]:
    """Split one argv word into literal and parameter-placeholder parts."""

    parts: list[tuple[str, str]] = []
    literal: list[str] = []
    index = 0

    def add_literal() -> None:
        if literal:
            parts.append(("literal", "".join(literal)))
            literal.clear()

    while index < len(word):
        if word.startswith("{{", index):
            literal.append("{")
            index += 2
            continue
        if word.startswith("}}", index):
            literal.append("}")
            index += 2
            continue
        if word[index] == "{":
            close = word.find("}", index + 1)
            if close >= 0:
                name = word[index + 1 : close]
                if _COMMAND_PARAMETER_NAME.fullmatch(name):
                    add_literal()
                    parts.append(("parameter", name))
                    index = close + 1
                    continue
                if "/" in name:
                    add_literal()
                    parts.append(("invalid", name))
                    index = close + 1
                    continue
        literal.append(word[index])
        index += 1
    add_literal()
    return parts or [("literal", "")]


def _command_runner_text(
    template: str,
    parameters: Mapping[str, object],
    files: Mapping[str, object] | None = None,
) -> str:
    """Render a minimal Bash runner for a command template."""

    try:
        argv = shlex.split(template)
    except ValueError as exc:
        raise ValueError(f"--from-command is not a valid shell word list: {exc}") from exc
    if not argv:
        raise ValueError("--from-command must contain at least one command word")

    words = [_command_word_parts(word) for word in argv]
    file_paths = {name: payload_relative(name).as_posix() for name in (files or {})}
    invalid_file_names = sorted(
        {value for parts in words for kind, value in parts if kind == "invalid" and value in file_paths}
    )
    if invalid_file_names:
        name = invalid_file_names[0]
        raise ValueError(f"`{{{name}}}` is not a valid placeholder name; stage it as a bare name to reference it")
    names = {value for parts in words for kind, value in parts if kind == "parameter"}
    conflicts = sorted(names & set(parameters) & set(file_paths))
    if conflicts:
        joined = ", ".join(f"{{{name}}}" for name in conflicts)
        raise ValueError(f"--from-command placeholder {joined} is both a --file and a --parameter")
    missing = sorted(names - set(parameters) - set(file_paths))
    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            "--from-command has placeholder(s) without --parameter or --file "
            f"(supply with --parameter NAME=VALUE or --file NAME=PATH): {joined}"
        )

    staged_files = sorted(
        (
            staged_path,
            name,
            PurePosixPath(staged_path).name,
        )
        for name, staged_path in file_paths.items()
    )
    staged_basenames: dict[str, str] = {}
    for staged_path, name, basename in staged_files:
        previous = staged_basenames.get(basename)
        if previous is not None:
            raise ValueError(f"--file inputs {previous} and {name} would both stage as {basename} in the workdir")
        staged_basenames[basename] = name

    staging_lines = "".join(
        f'    [ -e {shlex.quote(basename)} ] || [ -L {shlex.quote(basename)} ] || '
        f'cp -p -- "$HTTK_WORKFLOW_JOB_DIR"/{shlex.quote(staged_path)} {shlex.quote(basename)}\n'
        for staged_path, _, basename in staged_files
    )

    def render_word(parts: list[tuple[str, str]]) -> str:
        if not any(kind == "parameter" for kind, _ in parts):
            return shlex.quote("".join(value for _, value in parts))
        pieces: list[str] = ['"']
        for kind, value in parts:
            if kind == "parameter":
                if value in parameters:
                    pieces.append(f"$(httk_workflow_parameter {value})")
                else:
                    pieces.append(f"$HTTK_WORKFLOW_JOB_DIR/{file_paths[value]}")
            else:
                pieces.append(value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`"))
        pieces.append('"')
        return "".join(pieces)

    command_line = " ".join(render_word(parts) for parts in words)
    return (
        "#!/usr/bin/env bash\n"
        "# Generated by `httk job new --from-command`; edit and pass with --from-runner to customize.\n"
        "set -euo pipefail\n"
        'source "$HTTK_WORKFLOW_BASH_API"\n'
        "httk_workflow_runner command run\n"
        "\n"
        "step_run() {\n"
        f"{staging_lines}"
        f"    httk_workflow_run -- {command_line}\n"
        "    httk_workflow_succeed\n"
        "}\n"
        "\n"
        "httk_workflow_main\n"
    )


def _command_default_tag(parameters: Mapping[str, object]) -> str | None:
    """Suggest a compact tag for a command with one parameter."""

    if len(parameters) != 1:
        return None
    name, value = next(iter(parameters.items()))
    return _sanitize_tag(f"{name}{value}")


def _expand_file_directories(
    file_arguments: Sequence[str], directories: Sequence[str], context: CLIContext
) -> list[tuple[str, str]]:
    """Expand ``--files`` directories into ``--file``-style arguments."""

    expanded = _pairs(file_arguments, "a staged file")
    for directory_text in directories:
        directory = Path(directory_text).expanduser()
        if not directory.is_dir():
            raise ValueError(f"--files directory does not exist or is not a directory: {directory_text}")
        children = sorted(directory.iterdir(), key=lambda path: path.name)
        regular_files: list[Path] = []
        skipped: list[str] = []
        for child in children:
            if child.is_file():
                if child.name.strip() != child.name or not child.name.strip():
                    raise ValueError(
                        f"--files {directory_text} entry {child.name!r} changes under staging normalization"
                    )
                regular_files.append(child)
            else:
                skipped.append(child.name)
        if skipped:
            shown = ", ".join(skipped[:5])
            if len(skipped) > 5:
                shown += ", …"
            print(
                f"{context.program} workflow: warning: skipped {len(skipped)} non-file entries "
                f"in {directory_text}: {shown}",
                file=sys.stderr,
            )
        if not regular_files:
            raise ValueError(f"no regular files in {directory_text}")
        expanded.extend((child.name, str(child)) for child in regular_files)
    return expanded


def _workflow_target(
    arguments: argparse.Namespace, directory: Path, parameters: Mapping[str, object], files: Mapping[str, object]
) -> str | Path:
    """Return what ``job new`` creates jobs of; ``--from-command`` writes its runner into *directory*."""

    if arguments.from_command is not None:
        # The ad hoc id is adhoc:command@<sha12>: the stem names it, the digest pins the rendered text.
        runner = directory / "command.sh"
        runner.write_text(_command_runner_text(arguments.from_command, parameters, files), encoding="utf-8")
        return runner
    if arguments.from_runner is not None:
        path = Path(arguments.from_runner).expanduser()
        if not path.is_file():
            raise ValueError(f"--from-runner must name a runner file: {path}")
        return path.resolve()
    if arguments.workflow_dir is not None:
        path = Path(arguments.workflow_dir).expanduser()
        if not path.is_dir() or not (path / "httk_workflow.toml").is_file():
            raise ValueError(f"--workflow-dir must name a directory containing httk_workflow.toml: {path}")
        return path.resolve()
    path = Path(arguments.workflow).expanduser()
    if path.is_file():
        raise ValueError(
            "--workflow accepts a workflow name, not a runner file; "
            "use --from-runner FILE (or --workflow-dir DIR for a package)"
        )
    if path.is_dir():
        raise ValueError(
            "--workflow accepts a workflow name, not a package directory; "
            "use --workflow-dir DIR (or --from-runner FILE for a runner)"
        )
    if not arguments.workflow.startswith("git+") and (
        "/" in arguments.workflow or path.suffix.lower() in {".py", ".sh", ".bash", ".cwl", ".json", ".yaml", ".yml"}
    ):
        raise ValueError(
            "--workflow accepts workflow names only; use --from-runner FILE for a runner "
            "or --workflow-dir DIR for a package directory"
        )
    return arguments.workflow


def _staged_files(arguments: argparse.Namespace, context: CLIContext) -> dict[str, str | Path]:
    """Return the ``--file``/``--files`` payload files, refusing two that land on one destination."""

    files: dict[str, str | Path] = {}
    destinations: dict[str, str] = {}
    sources: dict[str, str] = {}
    for name, text in _expand_file_directories(arguments.files, arguments.file_directories, context):
        if name in files:
            raise ValueError(
                f"--file entries {name!r} and {name!r} use the same name (sources: {sources[name]!r} and {text!r})"
            )
        destination = payload_relative(name).as_posix()
        previous = destinations.get(destination)
        if previous is not None:
            raise ValueError(
                f"--file entries {previous!r} and {name!r} have the same normalized destination {destination!r} "
                f"(sources: {sources[previous]!r} and {text!r})"
            )
        files[name] = Path(text)
        sources[name] = text
        destinations[destination] = name
    return files


def handle_job_new(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Scaffold and submit one job or an input-source batch of an installed workflow."""

    selections = (arguments.workflow, arguments.workflow_dir, arguments.from_runner, arguments.from_command)
    if sum(selection is not None for selection in selections) != 1:
        raise ValueError("choose exactly one of --workflow, --workflow-dir, --from-runner, or --from-command")
    workspace = _modifiable(arguments, context, action="submit into it")
    environment = {
        name: _json_value(text, f"workflow environment {name!r}")
        for name, text in _pairs(arguments.environment, "a workflow environment override")
    }
    parameters = {
        name: _json_value(text, f"job parameter {name!r}")
        for name, text in _pairs(arguments.parameters, "a job parameter")
    }
    files = _staged_files(arguments, context)
    inputs, items, input_tag = _load_inputs(arguments.inputs, arguments.input_from)
    command_tag = _command_default_tag(parameters) if arguments.from_command is not None else None
    with tempfile.TemporaryDirectory(prefix="httk-command-") as directory:
        target = _workflow_target(arguments, Path(directory), parameters, files)
        shared: dict[str, Any] = {
            "inputs": inputs,
            "files": files,
            "parameters": parameters,
            "environment": environment,
            "placement": arguments.placement,
            "priority": arguments.priority,
            "step": arguments.step,
            "format": arguments.format,
            "name": arguments.name,
            "install": arguments.install,
        }
        results: Iterator[ScaffoldedJob]
        if items:
            for item in items:
                # In a batch, --tag prefixes each item's derived tag (run7-si2o), so one flag names the whole
                # sweep without erasing per-item identity; the composed tag is re-sanitized to stay a valid tag.
                derived = item.get("tag")
                if arguments.tag and derived:
                    item["tag"] = _sanitize_tag(f"{arguments.tag}-{derived}")
                else:
                    item["tag"] = arguments.tag or derived or command_tag
            results = new_jobs(workspace, target, items, **shared)
        else:
            results = iter([new_job(workspace, target, tag=arguments.tag or command_tag or input_tag, **shared)])
        return _report_jobs(results, arguments, context, batch=len(items) if items else None)


def _report_jobs(
    results: Iterator[ScaffoldedJob], arguments: argparse.Namespace, context: CLIContext, *, batch: int | None
) -> int:
    """Print each submitted job as it lands (or one JSON array), and a batch's count on stderr."""

    seen_warnings: set[str] = set()
    submitted = 0
    collected: list[ScaffoldedJob] = []
    try:
        for job in results:
            submitted += 1
            for warning in job.warnings:
                if warning not in seen_warnings:
                    seen_warnings.add(warning)
                    print(f"{context.program} workflow: warning: {warning}", file=sys.stderr)
            if arguments.json:
                collected.append(job)
            else:
                # One tab-separated line per job, so a shell reads the key with cut and a campaign streams.
                print(f"{job.job_key}\t{job.payload}")
    except _ERRORS:
        if batch is not None:
            print(f"submitted {submitted} of {batch} jobs before failing", file=sys.stderr)
        raise
    if arguments.json:
        print(json.dumps([job.as_mapping() for job in collected], indent=2))
    if batch is not None:
        print(f"submitted {submitted} jobs", file=sys.stderr)
    return 0


def handle_job_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit prepared payload directories (a ``job.json`` v3 and its files) into ``jobs/ready/``."""

    workspace = _modifiable(arguments, context, action="submit into it")
    submitted: list[str] = []
    failed = False
    with _kernel.register_owner(workspace, kind="cli", label="job submit", allocation=None, advertised={}) as owner:
        for source in arguments.sources:
            try:
                ref = submit_payload(workspace, owner, Path(source).expanduser(), move=arguments.move)
            except _ERRORS as exc:
                failed = True
                print(f"{source}: {exc}", file=sys.stderr)
                continue
            submitted.append(str(ref.path))
            if not arguments.json:
                print(f"{source}: {ref.path}")
    if arguments.json:
        print(json.dumps(submitted, indent=2))
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# job request and the remote signing protocol
# ---------------------------------------------------------------------------


def _request_options(
    action: str, *, priority: int | None, step: str | None, force: bool, destination: str | None
) -> dict[str, object]:
    """Return the optional request members of *action*, refusing an option it does not take or lacks."""

    given = {"priority": priority, "step": step, "destination": destination}
    for name, owner in _ACTION_OPTIONS.items():
        if (given[name] is None) == (action == owner):
            raise ValueError(f"--{name} is required by, and only valid with, the {owner} action")
    if force and action not in _FORCE_ACTIONS:
        raise ValueError("--force applies only to the continue and override_step actions")
    options: dict[str, object] = {name: value for name, value in given.items() if value is not None}
    if force:
        options["force"] = True
    return options


def _ref_placement(ref: JobRef) -> PurePosixPath:
    placement = job_placement(ref)
    if placement is None:
        raise ValueError(f"the job.json of {ref.job_key} is unreadable, so its placement is unknown")
    return placement


def _prevalidate(refs: Sequence[JobRef], action: str, step: str | None, *, force: bool) -> None:
    """Refuse an override_step whose target is outside a job's recorded runner steps, before anything is posted.

    The runner records its real step set in ``state.json`` (``runner_steps``) with
    its outcomes, so the request is refused against that list unless ``--force``
    is given. Before the first outcome nothing is recorded, so the request is
    allowed with a note.
    """

    if action != "override_step" or step is None:
        return
    for ref in refs:
        doc, _damage = read_state(ref)
        known = [str(item) for item in (doc.runner_steps if doc is not None and doc.runner_steps else ())]
        if not known:
            print(
                f"the step {step!r} could not be pre-validated: this job has not recorded its runner steps yet, "
                "so the runner will refuse it at the next attempt if it does not implement it",
                file=sys.stderr,
            )
        elif step not in known:
            if not force:
                ensure_step_known(step, known, f"job {ref.job_key}")
            print(
                f"the step {step!r} is not one of this job's recorded runner steps ({', '.join(known)}), "
                "but --force was given: publishing anyway; the runner will refuse it at the next attempt "
                "if it does not implement it",
                file=sys.stderr,
            )


def publish_job_requests(
    workspace: Workspace,
    refs: Sequence[JobRef],
    *,
    action: str,
    reason: str,
    operator: str | None = None,
    priority: int | None = None,
    step: str | None = None,
    force: bool = False,
    destination: str | None = None,
    identity: OperatorIdentity | None = None,
) -> list[tuple[JobRef, Path]]:
    """Post one request per already resolved job, signed with the operator's identity when it has a key.

    Every job is checked before the first request is posted.

    :param workspace: Workspace receiving the requests.
    :param refs: The jobs, as last observed; no selector scan is performed.
    :param action: One of :data:`httk.workflow._requests.ACTIONS`.
    :param reason: Operator explanation.
    :param operator: Operator label, defaulting to the identity's.
    :param priority: The new priority, for ``set_priority``.
    :param step: The step, for ``override_step``.
    :param force: Accept the revival hazard, for ``continue`` and ``override_step``.
    :param destination: Where an ``eject`` sends the job.
    :param identity: Resolved identity used for signing, defaulting to the configured one.
    :return: The posted request files paired with their jobs.
    :raises ValueError: For an unknown action, a misplaced option, or an unknown runner step.
    """

    if action not in _requests.ACTIONS:
        raise ValueError(f"unknown job request action: {action}")
    selected = identity or _resolve_request_identity(None)
    ensure_identity_key(selected)
    options = _request_options(action, priority=priority, step=step, force=force, destination=destination)
    _prevalidate(refs, action, step, force=force)
    placements = [_ref_placement(ref) for ref in refs]
    return [
        (
            ref,
            _requests.post(
                workspace,
                action=action,
                job_id=ref.job_id,
                placement=placement,
                operator=operator or selected.label,
                reason=reason,
                seed_path=selected.seed_path,
                **options,
            ),
        )
        for ref, placement in zip(refs, placements, strict=True)
    ]


def _check_wait(wait: bool, timeout: float | None, actions: Sequence[str]) -> None:
    if timeout is not None and not wait:
        raise ValueError("--timeout requires --wait")
    if wait and any(action != "pause" for action in actions):
        raise ValueError("--wait is only valid with the pause action")
    if timeout is not None and timeout < 0:
        raise ValueError("--timeout must not be negative")


def handle_job_request(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Post operator requests against jobs, and optionally wait for pauses to land."""

    _check_wait(arguments.wait, arguments.timeout, (arguments.action,))
    identity = _resolve_request_identity(arguments.operator)
    ensure_identity_key(identity)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None and ":" in binding.name
        for selector in arguments.job_id:
            if selector_uses_remote_path(context.cwd, selector):
                raise ValueError("path selectors are resolved on this machine; give job ids for a remote workspace")
        status, stdout, stderr = request_remote_job_result(binding, context, arguments, identity)
        if stdout:
            sys.stdout.write(stdout)
        if stderr:
            sys.stderr.write(stderr)
        if status and not stdout:
            raise RuntimeError(
                f"remote request envelope build failed (exit {status}); see the relayed remote error above"
            )
        return status
    workspace = _modifiable(arguments, context, action="post requests in it")
    published = publish_job_requests(
        workspace,
        resolve_job_selectors(workspace, context.cwd, arguments.job_id),
        action=arguments.action,
        reason=arguments.reason,
        operator=identity.label,
        priority=arguments.priority,
        step=arguments.step,
        force=bool(arguments.force),
        destination=arguments.destination,
        identity=identity,
    )
    return _complete_job_requests(workspace, published, wait=arguments.wait, timeout=arguments.timeout)


def _request_document(
    ref: JobRef, action: str, operator: str, reason: str, options: Mapping[str, object]
) -> dict[str, object]:
    """Build one unsigned v3 request document (the remote signing protocol's first leg)."""

    document: dict[str, object] = {
        "format": _requests.REQUEST_FORMAT,
        "format_version": _requests.REQUEST_FORMAT_VERSION,
        "request_id": str(uuid.uuid4()),
        "job_id": ref.job_id,
        "placement": placement_text(_ref_placement(ref)),
        "action": action,
        "operator": operator,
        "reason": reason,
        "created_at": utc_now(),
        **options,
    }
    _requests.validate_envelope(document)
    return document


def handle_job_request_envelopes(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Build unsigned request documents for a signing client (the hidden protocol's first leg)."""

    workspace = _protocol_workspace(arguments.workspace, context)
    options = _request_options(
        arguments.action,
        priority=arguments.priority,
        step=arguments.step,
        force=bool(arguments.force),
        destination=arguments.destination,
    )
    refs = [resolve_job(workspace, selector) for selector in arguments.job_id]
    _prevalidate(refs, arguments.action, arguments.step, force=bool(arguments.force))
    document = {
        "format": _ENVELOPES_FORMAT,
        "format_version": 2,
        "envelopes": [
            _request_document(ref, arguments.action, arguments.operator, arguments.reason, options) for ref in refs
        ],
        "job_keys": [ref.job_key for ref in refs],
    }
    print(json.dumps(document, separators=(",", ":")))
    return 0


def _validate_remote_envelopes(
    document: Mapping[str, object], arguments: argparse.Namespace, operator: str
) -> list[dict[str, object]]:
    """Check the far side's envelopes against exactly what was asked, so it cannot get anything else signed.

    :param document: The envelopes document the far side returned.
    :param arguments: The local request arguments.
    :param operator: The operator label the envelopes must carry.
    :return: The envelopes, in selector order.
    :raises ValueError: If the document is malformed or any envelope differs from the request.
    """

    envelopes, keys = document.get("envelopes"), document.get("job_keys")
    if (
        document.get("format") != _ENVELOPES_FORMAT
        or document.get("format_version") != 2
        or not isinstance(envelopes, list)
        or not isinstance(keys, list)
        or not all(isinstance(item, dict) for item in envelopes)
        or not all(isinstance(item, str) for item in keys)
    ):
        raise ValueError("remote did not return a valid request-envelopes document")
    if len(envelopes) != len(arguments.job_id) or len(keys) != len(envelopes):
        raise ValueError(
            f"remote returned {len(envelopes)} request envelopes for {len(arguments.job_id)} requested jobs"
        )
    options = _request_options(
        arguments.action,
        priority=arguments.priority,
        step=arguments.step,
        force=bool(arguments.force),
        destination=getattr(arguments, "destination", None),
    )
    expected = {"priority": None, "step": None, "destination": None, "force": None, **options}
    expected.update(action=arguments.action, operator=operator, reason=arguments.reason)
    for index, (envelope, key, selector) in enumerate(zip(envelopes, keys, arguments.job_id, strict=True)):
        try:
            _requests.validate_envelope(envelope)
            _, key_job_id = parse_job_key(key)
        except (FormatError, WorkflowError) as exc:
            raise ValueError(f"request envelope {index} is invalid: {exc}") from exc
        if "signature" in envelope:
            raise ValueError(f"request envelope {index} is already signed")
        for member, value in expected.items():
            if envelope.get(member) != value:
                raise ValueError(f"request envelope {index} member {member!r} disagrees with the request")
        job_id = envelope["job_id"]
        if key_job_id != job_id:
            raise ValueError(f"request envelope {index} job key disagrees with its job id")
        try:
            uuid_selector = canonical_uuid(selector, "JOB") == selector
        except (WorkflowError, ValueError):
            uuid_selector = False
        if uuid_selector and job_id != selector:
            raise ValueError(f"request envelope {index} does not match UUID selector {selector!r}")
        if not (job_id.startswith(selector) or key.startswith(selector)):
            raise ValueError(f"request envelope {index} does not match requested selector {selector!r}")
    return envelopes


def request_remote_job_result(
    binding: WorkspaceBinding,
    context: CLIContext,
    arguments: argparse.Namespace,
    identity: OperatorIdentity,
) -> tuple[int, str, str]:
    """Build request documents remotely, sign them here, and post them remotely.

    :param binding: The remote workspace binding.
    :param context: The invocation context.
    :param arguments: The request arguments (``action``, ``job_id``, ``reason`` and the options).
    :param identity: The signing identity.
    :return: The far side's exit status, standard output and standard error.
    """

    target = resolve_remote(binding.remote, project=context.cwd)
    remote_name = binding.name.split(":", 1)[1]
    argv = [
        *REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
        arguments.action,
        f"--workspace={remote_name}",
        f"--operator={identity.label}",
        f"--reason={arguments.reason}",
        "--json",
    ]
    for option in ("priority", "step", "destination"):
        value = getattr(arguments, option, None)
        if value is not None:
            argv.append(f"--{option}={value}")
    if arguments.force:
        argv.append("--force")
    argv.extend(arguments.job_id)
    result = run_adapter(target.bundle, "invoke", {"argv": argv}, timeout=arguments.adapter_timeout)
    stderr = str(result.get("stderr", ""))
    if result.get("returncode") != 0:
        return int(result.get("returncode", 1) or 1), "", stderr
    try:
        document = json.loads(str(result.get("stdout", "")))
    except json.JSONDecodeError as exc:
        raise ValueError("remote did not return a valid request-envelopes document") from exc
    if not isinstance(document, dict):
        raise ValueError("remote did not return a valid request-envelopes document")
    envelopes = _validate_remote_envelopes(document, arguments, identity.label)
    publish = [*REMOTE_JOB_PUBLISH_REQUESTS_COMMAND, f"--workspace={remote_name}"]
    for envelope in envelopes:
        signed = envelope if identity.seed_path is None else sign_document(envelope, seed_path=identity.seed_path)
        publish.append(f"--document={json.dumps(signed, separators=(',', ':'))}")
    if arguments.wait:
        publish.append("--wait")
    if arguments.timeout is not None:
        publish.append(f"--timeout={arguments.timeout}")
    if getattr(arguments, "no_durable", False):
        publish.append("--no-durable")
    published = run_adapter(target.bundle, "invoke", {"argv": publish}, timeout=arguments.adapter_timeout)
    return (
        int(published.get("returncode", 0) or 0),
        str(published.get("stdout", "")),
        stderr + str(published.get("stderr", "")),
    )


def handle_job_publish_requests(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Post request documents a signing client sent (the hidden protocol's second leg)."""

    workspace = _protocol_workspace(arguments.workspace, context)
    documents: list[dict[str, object]] = []
    for index, text in enumerate(arguments.documents):
        try:
            document = json.loads(text)
            if not isinstance(document, dict):
                raise FormatError("not a JSON object")
            _requests.validate_envelope(document)
            _requests.operator_key(document)
        except (ValueError, FormatError) as exc:
            raise ValueError(f"request document {index} is invalid: {exc}") from exc
        documents.append(document)
    _check_wait(arguments.wait, arguments.timeout, [str(document["action"]) for document in documents])
    # Every job is located before anything is posted.
    refs: list[JobRef] = []
    for document in documents:
        ref = _kernel.locate(
            workspace,
            str(document["job_id"]),
            placement_hint=PurePosixPath(str(document["placement"])),
            exhaustive=True,
        )
        if ref is None:
            raise ValueError(f"job does not exist: {document['job_id']}")
        refs.append(ref)
    published = [
        (ref, _kernel.post_request(workspace, document)) for ref, document in zip(refs, documents, strict=True)
    ]
    return _complete_job_requests(workspace, published, wait=arguments.wait, timeout=arguments.timeout)


def ensure_identity_key(identity: OperatorIdentity) -> None:
    """Refuse a configured identity whose seed file is absent or unreadable.

    :param identity: The identity selected for signing.
    :raises ValueError: If a configured identity has no usable seed file.
    """

    if identity.seed_path is not None and identity_seed(identity.seed_path) is None:
        if identity.short is None:
            raise ValueError(
                f"the selected default identity has no key file at {identity.seed_path}; "
                "check `httk identity list` and re-add the default identity"
            )
        short = identity.short
        raise ValueError(
            f"identity {short!r} has no key file at {identity.seed_path}; "
            f"remove it with `httk identity remove {short}` then re-add it with "
            f"`httk identity add {short} ...`, or restore the key file at {identity.seed_path}"
        )


def _resolve_request_identity(selector: str | None) -> OperatorIdentity:
    """Resolve a request identity and guide an unconfigured user to ``httk init``."""

    if selector is None:
        identity = configured_operator_identity()
        if identity is None:
            raise ValueError("no operator identity is configured; run `httk init` to establish one")
        return identity
    return resolve_operator_identity(selector)


def _complete_job_requests(
    workspace: Workspace, published: list[tuple[JobRef, Path]], *, wait: bool, timeout: float | None
) -> int:
    """Print the posted request files, warn about unserved jobs, and optionally wait for pauses."""

    for _, path in published:
        print(path)
    managers = read_managers(workspace)
    served = [_warn_if_unserved(managers, ref) for ref, _ in published]
    if not wait:
        return 0
    if not all(served):
        print("waiting is pointless until a manager starts", file=sys.stderr)
        return 1
    return _wait_for_pauses(workspace, published, timeout=timeout)


def _warn_if_unserved(managers: Sequence[Any], ref: JobRef) -> bool:
    """Warn when no live manager could claim the job, so its request would wait with no error.

    The warning is advisory: the request stays valid until a manager starts.
    """

    job, _error = read_job(ref)
    if job is None:
        return True
    requirements = claim_requirements(job)
    if any(record.alive() and not manager_refusals(record, requirements) for record in managers):
        return True
    capabilities = ",".join(sorted(requirements.capabilities))
    print(
        f"no live manager currently serves claim pool {requirements.pool!r}"
        f"{f' with capabilities {capabilities}' if capabilities else ''}; the request will wait until one starts",
        file=sys.stderr,
    )
    return False


def _pause_outcome(workspace: Workspace, ref: JobRef, request_id: str) -> tuple[bool, str] | None:
    """Return whether a pause landed and how, or ``None`` while it is still pending."""

    current = _kernel.locate(workspace, ref.job_id, placement_hint=job_placement(ref), exhaustive=True)
    if current is None:
        return False, f"{ref.job_id}: the job is gone"
    if current.state == "paused":
        return True, f"{current.job_key}: paused"
    if current.state in TERMINAL_STATES:
        return False, f"{current.job_key}: {current.state} (pause superseded)"
    doc, _damage = read_state(current)
    for entry in doc.history_tail if doc is not None else ():
        if entry.get("request_id") == request_id and entry.get("event") == "request_dropped":
            return False, f"{current.job_key}: request dropped: {entry.get('note')}"
    return None


def _wait_for_pauses(workspace: Workspace, published: list[tuple[JobRef, Path]], *, timeout: float | None) -> int:
    """Wait for posted pause requests and report each final outcome."""

    pending = {path.name.split(".")[1]: ref for ref, path in published}
    outcomes: dict[str, tuple[bool, str]] = {}
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        for request_id, ref in list(pending.items()):
            outcome = _pause_outcome(workspace, ref, request_id)
            if outcome is not None:
                outcomes[request_id] = outcome
                del pending[request_id]
        remaining = None if deadline is None else deadline - time.monotonic()
        if not pending or (remaining is not None and remaining <= 0):
            break
        time.sleep(1.0 if remaining is None else min(1.0, remaining))
    for request_id, ref in pending.items():
        outcomes[request_id] = (False, f"{ref.job_id}: timeout; still pending (request remains published)")
    for _, path in published:
        print(outcomes[path.name.split(".")[1]][1])
    return 0 if all(paused for paused, _ in outcomes.values()) else 1


# ---------------------------------------------------------------------------
# job list / delete / seal / unseal / detach / eject / adopt
# ---------------------------------------------------------------------------


def _validate_remote_job_ids(jobs: list[str], action: str) -> None:
    """Require canonical job UUIDs for a remote detail read."""

    for job in jobs:
        try:
            canonical_uuid(job, "JOB")
        except (WorkflowError, ValueError) as exc:
            raise ValueError(
                f"remote job {action} requires canonical job ids; keys and prefixes are not accepted: {job!r}"
            ) from exc


def handle_job_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the jobs of one workspace as a cheap table, one page at a time."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        tail: list[str] = []
        for state in arguments.kind or []:
            tail.extend(("--kind", state))
        for option in ("placement", "after", "limit", "tag_contains"):
            value = getattr(arguments, option)
            if value is not None:
                tail.extend((f"--{option.replace('_', '-')}", str(value)))
        tail.append("--workspace")
        return _remote_workspace_read(
            binding,
            context,
            REMOTE_JOB_LIST_COMMAND,
            arguments,
            flags=("--json", "--counts"),
            tail=tail,
            unwrap_json_array=False,
        )
    workspace = Workspace(root)
    page = list_jobs(
        workspace,
        kinds=arguments.kind,
        placement_prefix=arguments.placement,
        after=arguments.after,
        limit=arguments.limit,
        tag_contains=arguments.tag_contains,
    )
    if not arguments.json:
        print(render_rows(page.jobs))
        return 0
    document: dict[str, object] = {
        "format": JOB_LIST_FORMAT,
        "format_version": 3,
        "jobs": page.jobs,
        "next_after": page.next_after,
    }
    if arguments.counts:
        document["counts"] = {
            state: count_jobs(workspace, state, arguments.placement) for state in arguments.kind or JOB_STATES
        }
    print(json.dumps(document, indent=2))
    return 0


def _queued(workspace: Workspace, ref: JobRef) -> bool:
    """Whether a request that did not take effect waits for the owner now holding the job (``queued``)."""

    current = _kernel.locate(workspace, ref.job_id, placement_hint=None)
    return current is not None and current.state == _kernel.OWNED


def _print_removal_report(workspace: Workspace, refs: Sequence[JobRef], report: RemovalReport) -> int:
    """Print job removal outcomes and return 1 when one was refused (a queued delete is not refused)."""

    refused = False
    for ref, outcome in zip(refs, report.outcomes, strict=True):
        if outcome.removed:
            print(f"{outcome.job_key}\t{outcome.kind}\tremoved")
        elif _queued(workspace, ref):
            print(f"{outcome.job_key}\t{outcome.kind}\tqueued\t{outcome.reason}")
        else:
            refused = True
            print(f"{outcome.job_key}\t{outcome.kind}\trefused\t{outcome.reason}")
    print(f"removed {report.removed_count} of {len(report.outcomes)} job(s)")
    return 1 if refused else 0


def _confirm_job_delete(rows: Sequence[tuple[str, str]]) -> bool:
    """List the jobs to delete, then confirm on the terminal or refuse without one."""

    for job_key, state in rows:
        print(f"{job_key}\t{state}")
    return confirm(f"Delete {len(rows)} jobs?", force=False)


def _remote_delete_rows(
    binding: WorkspaceBinding, context: CLIContext, arguments: argparse.Namespace
) -> tuple[int, list[tuple[str, str]]]:
    """Read the remote job keys and states to confirm before deleting."""

    remote_name = binding.name.split(":", 1)[1]
    argv = [*REMOTE_JOB_SHOW_COMMAND, "--json", "--no-children", *arguments.jobs, "--workspace", remote_name]
    status, stdout, stderr = remote_workspace_output(binding, context, argv, timeout=arguments.adapter_timeout)
    if status:
        sys.stderr.write(stderr)
        return 1, []
    try:
        reports = json.loads(stdout)
        if not isinstance(reports, list):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("remote job show returned an invalid JSON document") from exc
    by_id = {str(report.get("job_id")): report for report in reports if isinstance(report, Mapping)}
    missing = [job_id for job_id in arguments.jobs if job_id not in by_id]
    for job_id in missing:
        print(f"remote job {job_id}: not found", file=sys.stderr)
    if missing:
        return 1, []
    rows = [(by_id[job_id].get("job_key"), by_id[job_id].get("state")) for job_id in arguments.jobs]
    if not all(isinstance(key, str) and isinstance(state, str) for key, state in rows):
        raise ValueError("remote job show returned an incomplete report")
    return 0, [(str(key), str(state)) for key, state in rows]


def handle_job_delete(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Delete selected terminal or paused jobs: a ``delete`` request, applied now for an unowned job."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        _validate_remote_job_ids(arguments.jobs, "delete")
        if not arguments.force and not sys.stdin.isatty():
            print("job delete without a terminal requires --force", file=sys.stderr)
            return 1
        status, rows = _remote_delete_rows(binding, context, arguments)
        if status:
            return status
        if not arguments.force and not _confirm_job_delete(rows):
            return 1
        remote_name = binding.name.split(":", 1)[1]
        option = "--force" if arguments.force else "--confirmed"
        argv = [*REMOTE_JOB_DELETE_COMMAND, option, *arguments.jobs, "--workspace", remote_name]
        status, stdout, stderr = remote_workspace_output(binding, context, argv, timeout=arguments.adapter_timeout)
        sys.stdout.write(stdout)
        sys.stderr.write(stderr)
        return status
    workspace = _modifiable(arguments, context, action="delete jobs in it")
    refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
    if not (arguments.force or arguments.confirmed or _confirm_job_delete([(ref.job_key, ref.state) for ref in refs])):
        return 1
    return _print_removal_report(workspace, refs, remove_jobs(workspace, refs))


def _request_now(arguments: argparse.Namespace, context: CLIContext, action: str, done: str) -> int:
    """Post one *action* request per selected job and apply it now where the job is unowned.

    A request for a job an owner holds is ``queued`` for that owner (exit status 0, as ``job request``);
    one that did not take effect otherwise is ``refused`` (exit status 1).
    """

    workspace = _modifiable(arguments, context, action=f"{action} jobs in it")
    refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
    if action == "unseal" and not confirm(f"Unseal {len(refs)} job(s)?", force=arguments.force):
        return 1
    failed = False
    with _kernel.register_owner(workspace, kind="cli", label=f"job {action}", allocation=None, advertised={}) as owner:
        for ref in refs:
            reason = request_now(workspace, owner, ref, action, f"job {action}")
            if reason is None:
                print(f"{ref.job_id}\t{done}")
            elif _queued(workspace, ref):
                print(f"{ref.job_id}\tqueued\t{reason}")
            else:
                failed = True
                print(f"{ref.job_id}\trefused\t{reason}")
    return 1 if failed else 0


def handle_job_seal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal succeeded jobs that carry no seal, with the workspace's seal keys (a repair verb)."""

    return _request_now(arguments, context, "seal", "sealed")


def handle_job_unseal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Release succeeded jobs from their seal protection, after confirmation, so they may be deleted."""

    return _request_now(arguments, context, "unseal", "unsealed")


def handle_job_detach(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Make spawned jobs independent of their parents, permanently."""

    return _request_now(arguments, context, "detach", "detached")


def handle_job_eject(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move a quiescent job (with ``--tree``, its terminal or paused descendants too) out into a bundle.

    With ``--hold`` the bundle goes to the workspace's own ``transfers/outgoing/<transfer id>`` (the first leg of
    ``job transfer``), and DEST, when given, is only recorded as the transfer's destination.
    """

    if arguments.timeout < 0:
        raise ValueError("--timeout must not be negative")
    if arguments.destination is None and not arguments.hold:
        raise ValueError("job eject needs DEST, or --hold")
    if arguments.destination_id is not None and not arguments.hold:
        raise ValueError("--destination-id is recorded by a hold; it needs --hold")
    destination_id = (
        None if arguments.destination_id is None else canonical_uuid(arguments.destination_id, "--destination-id")
    )
    workspace = _modifiable(arguments, context, action="eject jobs from it")
    refs = resolve_job_selectors(workspace, context.cwd, [arguments.job])
    if len(refs) != 1:
        raise ValueError(f"{arguments.job} names {len(refs)} jobs; eject one root at a time")
    job_id, placement = refs[0].job_id, _ref_placement(refs[0])
    destination = None if arguments.hold else (context.cwd / Path(str(arguments.destination)).expanduser()).resolve()
    deadline = time.monotonic() + arguments.timeout
    paused = False
    with _kernel.register_owner(workspace, kind="cli", label="job eject", allocation=None, advertised={}) as owner:
        while True:
            ref = _kernel.locate(workspace, job_id, placement_hint=placement)
            if ref is None:
                print(f"{job_id}: the job was not found", file=sys.stderr)
                return 1
            if ref.state == _kernel.OWNED:
                outcome: _moving.EjectReport | _moving.Hold | str = f"{ref.job_key} is running or held by an owner"
                if arguments.wait and not paused:
                    _requests.post(
                        workspace,
                        action="pause",
                        job_id=job_id,
                        placement=placement,
                        operator="cli",
                        reason="job eject --wait",
                    )
                    paused = True
            else:
                try:
                    outcome = eject_once(
                        workspace,
                        owner,
                        ref,
                        destination,
                        tree=arguments.tree,
                        locator=arguments.destination,
                        destination_workspace_id=destination_id,
                    )
                except _ERRORS as exc:
                    print(f"{ref.job_key}: {exc}", file=sys.stderr)
                    return 1
            if not isinstance(outcome, str):
                break
            remaining = deadline - time.monotonic()
            if not arguments.wait or remaining <= 0:
                print(f"{outcome}{'; timed out waiting' if arguments.wait else ''}", file=sys.stderr)
                return 1
            time.sleep(min(1.0, remaining))
    if isinstance(outcome, _moving.Hold):
        print(
            json.dumps(outcome.as_mapping())
            if arguments.json
            else f"held {len(outcome.members)} job(s) in {outcome.path}"
        )
    elif arguments.json:
        document = {
            "destination": str(outcome.destination),
            "members": list(outcome.members),
            "transfer_id": outcome.transfer_id,
        }
        print(json.dumps(document, indent=2))
    else:
        print(f"ejected {len(outcome.members)} job(s) to {outcome.destination}")
    return 0


def handle_job_adopt(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move an ejected bundle into the workspace, publishing its jobs into the states they left.

    One filesystem moves the bundle in; across filesystems it is copied, and with ``--move`` the source is
    removed once adopted (for a bundle that is the caller's own to remove, such as a fetched exchange return).
    """

    workspace = _modifiable(arguments, context, action="adopt jobs into it")
    source = context.cwd / Path(arguments.bundle).expanduser()
    with _kernel.register_owner(workspace, kind="cli", label="job adopt", allocation=None, advertised={}) as owner:
        try:
            report = _moving.adopt(workspace, owner, source, untrusted=False)
        except _ERRORS as exc:
            print(f"{arguments.bundle}: {exc}", file=sys.stderr)
            return 1
    if report is None:
        print(f"{arguments.bundle}: another actor took the bundle first", file=sys.stderr)
        return 1
    if arguments.move and report.copied:
        _fs.discard(_fs.loc(source), trash_dir=source.parent, durable=workspace.durable)
    if report.missing_workflows:
        print(
            f"warning: these workflows are not installed, so their jobs wait: {', '.join(report.missing_workflows)}",
            file=sys.stderr,
        )
    if arguments.json:
        print(json.dumps(adopt_document(report), indent=2))
    elif report.already_adopted:
        print(f"{arguments.bundle}: already adopted")
    else:
        for ref in report.published:
            print(f"{ref.job_key}\t{ref.state}\t{ref.path}")
    return 0


# ---------------------------------------------------------------------------
# job show / log / why / debug
# ---------------------------------------------------------------------------


def _remote_detail(arguments: argparse.Namespace, context: CLIContext, binding: WorkspaceBinding, action: str) -> int:
    """Relay one detail read to a remote workspace by canonical job ids."""

    _validate_remote_job_ids(arguments.jobs, action)
    command = {"show": REMOTE_JOB_SHOW_COMMAND, "log": REMOTE_JOB_LOG_COMMAND, "why": REMOTE_JOB_WHY_COMMAND}[action]
    tail: list[str] = []
    if getattr(arguments, "limit", None) is not None:
        tail.extend(("--limit", str(arguments.limit)))
    tail.extend(arguments.jobs)
    if getattr(arguments, "no_children", False):
        tail.append("--no-children")
    tail.append("--workspace")
    return _remote_workspace_read(
        binding, context, command, arguments, flags=("--json",), tail=tail, unwrap_json_array=False
    )


def _for_each_job(
    arguments: argparse.Namespace,
    context: CLIContext,
    action: str,
    report: Callable[[Workspace, JobRef], tuple[dict[str, object], str]],
    missing: Callable[[Workspace, str], list[tuple[dict[str, object], str]]] | None = None,
) -> int:
    """Run *report* for every job each selector names; print JSON or the text it returns, per job.

    A selector naming no job falls back to *missing*, when given and it finds something.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_detail(arguments, context, binding, action)
    workspace = Workspace(root)
    resolver = JobSelectorResolver(workspace, context.cwd)
    documents: list[dict[str, object]] = []
    failed = False
    for selector in arguments.jobs:
        refs: list[JobRef] | None = None
        held: list[tuple[dict[str, object], str]] = []
        try:
            try:
                refs = resolver.resolve_one(selector)
            except ValueError:
                held = [] if missing is None else missing(workspace, selector)
                if not held:
                    raise
            results = held if refs is None else (report(workspace, ref) for ref in refs)
            for document, text in results:
                documents.append(document)
                if not arguments.json:
                    print(f"{selector}:")
                    print(text)
        except _ERRORS as exc:
            failed = True
            print(f"{selector}: {exc}", file=sys.stderr)
    if arguments.json:
        print(json.dumps(documents, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_job_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe jobs from ``job.json``, ``state.json`` and their directory names."""

    def report(workspace: Workspace, ref: JobRef) -> tuple[dict[str, object], str]:
        document = describe_job(workspace, ref, include_children=not arguments.no_children)
        return document, render_job(document)

    return _for_each_job(arguments, context, "show", report)


def _render_annotations(annotations: Sequence[Mapping[str, Any]]) -> str:
    return "\n".join(
        f"{event.get('timestamp') or '-'!s:32s} runner:{event.get('kind') or '?'!s:<9s} {event.get('message') or ''}"
        for event in annotations
    )


def handle_job_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print each job's owner run log, oldest first, then the runner's own run-log annotations."""

    if arguments.limit is not None and arguments.limit < 1:
        raise ValueError("--limit must be positive")

    def report(_workspace: Workspace, ref: JobRef) -> tuple[dict[str, object], str]:
        events = job_events(ref, limit=arguments.limit)
        annotations = _jsonl(ref.path, f"{JOB_STATE_DIRECTORY}/runlog.jsonl")
        if arguments.limit is not None:
            annotations = annotations[-arguments.limit :]
        document: dict[str, object] = {
            "format": JOB_HISTORY_FORMAT,
            "format_version": 3,
            "job_id": ref.job_id,
            "job_key": ref.job_key,
            "events": events,
            "annotations": annotations,
        }
        text = render_events(events)
        if annotations:
            text += "\n" + _render_annotations(annotations)
        return document, text

    return _for_each_job(arguments, context, "log", report)


def handle_job_why(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Explain why jobs are, or are not, making progress."""

    def report(workspace: Workspace, ref: JobRef) -> tuple[dict[str, object], str]:
        diagnosis = explain_job(workspace, ref)
        return diagnosis.as_mapping(), diagnosis.render()

    def held(workspace: Workspace, selector: str) -> list[tuple[dict[str, object], str]]:
        # A held job is in neither state tree: look for it in the held bundles.
        return [(diagnosis.as_mapping(), diagnosis.render()) for diagnosis in explain_held(workspace, selector)]

    return _for_each_job(arguments, context, "why", report, held)


def handle_job_debug(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Drive one job to a terminal state in the foreground, as a CLI owner."""

    # The debugged job's transitions are reported by the debug driver itself; the log stays quiet unless asked.
    configure_logging(level=arguments.log_level)
    outcome = debug_job(
        _modifiable(arguments, context, action="debug in it"),
        arguments.job,
        placement=arguments.placement,
        step=arguments.step,
        follow_children=arguments.follow_children,
        timeout=arguments.timeout,
        cwd=context.cwd,
    )
    return outcome.exit_code


# ---------------------------------------------------------------------------
# the parser
# ---------------------------------------------------------------------------


def _add_job_selector(parser: argparse.ArgumentParser) -> None:
    """Add the workspace and job selectors every inspection command shares."""

    _add_workspace_option(parser, help_text="the workspace holding the job")
    parser.add_argument(
        "jobs",
        metavar="JOB",
        nargs="+",
        help="job UUID, job key, unique prefix, or a path inside the workspace",
    )


def _add_request_options(parser: argparse.ArgumentParser) -> None:
    """Add the per-action request options shared by ``request`` and ``request-envelopes``."""

    parser.add_argument("--priority", type=int, metavar="PRIORITY", help="the new priority, for set_priority")
    parser.add_argument("--step", metavar="STEP", help="the step to resume at, for override_step")
    parser.add_argument("--destination", metavar="DEST", help="where the job goes, for eject")
    parser.add_argument(
        "--force",
        action="store_true",
        help="accept reviving a job a decided join already consumed (continue, override_step)",
    )


def _build_new_parser(group: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    new = _leaf(
        group,
        "new",
        summary="scaffold and submit jobs of an installed workflow",
        description=(
            "Scaffold and submit jobs of a workflow installed in the workspace (httk workflow install); "
            "--install installs it first, and a runner file, command or bare document is installed ad hoc"
        ),
        handler=handle_job_new,
    )
    _add_workspace_option(new, help_text="the workspace to submit into")
    workflow_group = new.add_mutually_exclusive_group(required=True)
    workflow_group.add_argument(
        "--workflow",
        metavar="WORKFLOW",
        help="an installed workflow id or short name, a registered or packaged workflow name ("
        + ", ".join(registered_workflow_labels())
        + ") or a git URI git+https://HOST/PATH[@REF][#SUBDIR] (not a path; use --from-runner or --workflow-dir)",
    )
    workflow_group.add_argument(
        "--workflow-dir", metavar="PATH", help="a workflow package directory containing httk_workflow.toml"
    )
    workflow_group.add_argument(
        "--from-runner",
        metavar="FILE",
        help="a single-file runner or workflow document (cwl, pwd, jobflow), installed as an adhoc: workflow",
    )
    workflow_group.add_argument(
        "--from-command",
        metavar="TEMPLATE",
        help=(
            "generate a one-step Bash runner from an argv-only TEMPLATE, installed as adhoc:command@<digest>; "
            "{name} substitutes a --parameter or staged --file path, {{ and }} are literal braces"
        ),
    )
    new.add_argument(
        "--install",
        action="store_true",
        help="install the workflow (and its calls) into the workspace first when it is not installed",
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
        metavar="FORMAT",
        help="force FORMAT (cwl, pwd, jobflow, httk-v1) for a bare workflow document or directory",
    )
    new.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="stage PATH in the payload as NAME; a NAME=PATH with no / in NAME lands in files/ (repeatable)",
    )
    new.add_argument(
        "--files",
        action="append",
        default=[],
        dest="file_directories",
        metavar="DIR",
        help="stage every regular file directly in DIR under its own name (repeatable; subdirectories are skipped)",
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
    new.add_argument("--tag", metavar="TAG", help="the readable half of the job key (default: derived from an input)")
    new.add_argument("--name", metavar="NAME", help="the human-readable job name")
    new.add_argument(
        "--placement", metavar="PLACEMENT", default=DEFAULT_PLACEMENT, help="placement subtree (default: the jobs root)"
    )
    new.add_argument("--priority", type=int, metavar="PRIORITY", help="scheduling priority (default: the workflow's)")
    new.add_argument("--step", metavar="STEP", help="the step the job starts at (default: the workflow's own)")
    new.add_argument("--json", action="store_true", help="print one JSON report per job, as an array")


def _build_request_parsers(group: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    request = _leaf(
        group,
        "request",
        summary="post an operator request",
        description=(
            "Post one operator request per job; the job's owner applies it at its next boundary "
            "(a manager claims an unowned job to apply it)"
        ),
        handler=handle_job_request,
    )
    request.add_argument("action", metavar="ACTION", choices=_requests.ACTIONS, help=", ".join(_requests.ACTIONS))
    _add_workspace_option(request, help_text="the workspace holding the job")
    request.add_argument(
        "job_id", metavar="JOB_ID", nargs="+", help="job UUIDs, unique prefixes, or paths inside the workspace"
    )
    request.add_argument(
        "--operator", metavar="IDENTITY", help='configured identity short name or a literal "Name <email>"'
    )
    request.add_argument("--reason", metavar="TEXT", required=True, help="why, recorded with the request")
    _add_request_options(request)
    request.add_argument("--wait", action="store_true", help="wait until every pause request reaches paused")
    request.add_argument("--timeout", type=float, metavar="SECONDS", help="stop waiting after SECONDS (needs --wait)")
    _add_adapter_timeout(request)
    add_durability_arguments(request)

    envelopes = _leaf(
        group,
        "request-envelopes",
        description="Build unsigned operator request documents for a signing client",
        summary="build unsigned request documents",
        handler=handle_job_request_envelopes,
        hidden=True,
    )
    envelopes.add_argument("action", choices=_requests.ACTIONS, help="the request action")
    envelopes.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    envelopes.add_argument("job_id", metavar="JOB_ID", nargs="+", help="one or more job ids or unique prefixes")
    envelopes.add_argument("--operator", required=True, help="the operator attribution label")
    envelopes.add_argument("--reason", required=True, help="why the request is being made")
    _add_request_options(envelopes)
    envelopes.add_argument("--json", action="store_true", required=True, help=argparse.SUPPRESS)

    publish = _leaf(
        group,
        "publish-requests",
        description="Post signed operator request documents from a signing client",
        summary="post signed request documents",
        handler=handle_job_publish_requests,
        hidden=True,
    )
    publish.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    publish.add_argument(
        "--document", action="append", dest="documents", required=True, metavar="JSON", help="one request document"
    )
    publish.add_argument("--wait", action="store_true", help="wait for pause requests to reach paused")
    publish.add_argument("--timeout", type=float, metavar="SECONDS", help="stop waiting after SECONDS")
    add_durability_arguments(publish)


def build_job_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    *,
    program: str | None = None,
) -> None:
    """Declare the ``job`` group: making jobs, steering them, and finding out about them."""

    _, group = _group(
        subparsers,
        "job",
        summary="create, submit, inspect, and debug individual jobs",
        description="Create, submit, inspect, and debug the jobs of one execution workspace",
        prog=program,
    )
    _build_new_parser(group)

    submit = _leaf(
        group,
        "submit",
        summary="submit prepared payload directories",
        description="Submit prepared payload directories (job.json v3 and its files) into the workspace's ready jobs",
        handler=handle_job_submit,
    )
    _add_workspace_option(submit, help_text="the workspace to submit into")
    submit.add_argument("sources", metavar="SOURCE", nargs="+", help="complete payload directories to submit")
    submit.add_argument(
        "--move", action="store_true", help="move the directory in (same filesystem) rather than copy it"
    )
    submit.add_argument("--json", action="store_true", help="print the submitted job directories as one JSON array")
    add_durability_arguments(submit)

    _build_request_parsers(group)

    listing = _leaf(
        group,
        "list",
        summary="list the jobs of a workspace",
        description="List the jobs of one execution workspace, one page at a time",
        handler=handle_job_list,
    )
    _add_workspace_option(listing, help_text="the workspace to list")
    listing.add_argument(
        "--kind",
        action="append",
        metavar="STATE",
        choices=JOB_STATES,
        help=f"state to list (repeatable, default: every state: {', '.join(JOB_STATES)})",
    )
    listing.add_argument("--placement", metavar="PLACEMENT", help="prune the listing to this placement prefix")
    listing.add_argument("--limit", type=int, metavar="COUNT", help="return at most this many jobs")
    listing.add_argument("--after", metavar="CURSOR", help="resume after a <state>:<cursor> from next_after")
    listing.add_argument("--tag-contains", metavar="TEXT", help="list only jobs whose tag contains TEXT")
    listing.add_argument("--counts", action="store_true", help="include the per-state job counts in JSON output")
    listing.add_argument("--json", action="store_true", help="print the rows as one JSON document")
    _add_adapter_timeout(listing)

    delete = _leaf(
        group,
        "delete",
        summary="delete terminal or paused jobs",
        description=(
            "Delete terminal or paused jobs (a succeeded job only after job unseal): a delete request, "
            "applied now to an unowned job and by its owner otherwise"
        ),
        handler=handle_job_delete,
    )
    _add_job_selector(delete)
    delete.add_argument("--force", action="store_true", help="skip the confirmation")
    delete.add_argument("--confirmed", action="store_true", help=argparse.SUPPRESS)
    _add_adapter_timeout(delete)

    for name, handler, summary in (
        ("seal", handle_job_seal, "seal succeeded jobs that carry no seal"),
        ("unseal", handle_job_unseal, "release succeeded jobs from their seal, so they may be deleted"),
        ("detach", handle_job_detach, "make spawned jobs independent of their parents, permanently"),
    ):
        leaf = _leaf(
            group,
            name,
            summary=summary,
            description=f"{summary[0].upper()}{summary[1:]}: a {name} request, applied now to an unowned job",
            handler=handler,
        )
        _add_job_selector(leaf)
        if name == "unseal":
            leaf.add_argument("--force", action="store_true", help="skip the confirmation prompt")

    eject = _leaf(
        group,
        "eject",
        summary="move a job (and with --tree its descendants) out of a workspace",
        description=(
            "Move a quiescent job out of the workspace into DEST/<job key>, a bundle that job adopt takes in; "
            "with --tree its terminal or paused descendants go along. With --hold the bundle is held in the "
            "workspace's own .httk-workspace/transfers/outgoing/<transfer id> (the first leg of job transfer) and "
            "DEST, when given, is only recorded as the transfer's destination"
        ),
        handler=handle_job_eject,
    )
    _add_workspace_option(eject, help_text="the workspace holding the job")
    eject.add_argument("job", metavar="JOB", help="job UUID, job key, unique prefix, or a path inside the workspace")
    eject.add_argument(
        "destination", metavar="DEST", nargs="?", help="the directory to put the bundle in (with --hold: recorded only)"
    )
    eject.add_argument("--hold", action="store_true", help="hold the bundle in the workspace for a transfer")
    eject.add_argument(
        "--destination-id",
        metavar="WORKSPACE_ID",
        help="with --hold: the destination workspace's id, recorded so that the transfer adopts only there",
    )
    eject.add_argument("--tree", action="store_true", help="eject the job's descendants too")
    eject.add_argument("--wait", action="store_true", help="pause a running job and wait until it can move")
    eject.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        default=600.0,
        help="give up waiting after this long (default: 600)",
    )
    eject.add_argument("--json", action="store_true", help="print the result as one JSON document")
    add_durability_arguments(eject)
    adopt = _leaf(
        group,
        "adopt",
        summary="move an ejected bundle into a workspace",
        description="Move an ejected job bundle into the workspace, publishing its jobs into the states they left",
        handler=handle_job_adopt,
    )
    _add_workspace_option(adopt, help_text="the workspace to adopt into")
    adopt.add_argument("bundle", metavar="BUNDLE", help="an ejected job bundle directory")
    adopt.add_argument(
        "--move",
        action="store_true",
        help="remove the bundle once adopted when it had to be copied from another filesystem",
    )
    adopt.add_argument("--json", action="store_true", help="print the result as one JSON document")
    add_durability_arguments(adopt)

    build_transfer_parser(group)

    show = _leaf(
        group,
        "show",
        summary="describe jobs from their state",
        description="Describe jobs from job.json, state.json and their directory names",
        handler=handle_job_show,
    )
    _add_job_selector(show)
    show.add_argument("--no-children", action="store_true", help="omit per-child observations for waiting jobs")
    show.add_argument("--json", action="store_true", help="print the descriptions as one JSON array")
    _add_adapter_timeout(show)

    log = _leaf(
        group,
        "log",
        summary="print job run logs",
        description="Print each job's owner run log, oldest first, then the runner's run-log annotations",
        handler=handle_job_log,
    )
    _add_job_selector(log)
    log.add_argument("--limit", type=int, metavar="COUNT", help="print only the newest COUNT events of each log")
    log.add_argument("--json", action="store_true", help="print the logs as one JSON array")
    _add_adapter_timeout(log)

    why = _leaf(
        group,
        "why",
        summary="explain why jobs are not running",
        description="Explain why jobs are, or are not, making progress",
        handler=handle_job_why,
    )
    _add_job_selector(why)
    why.add_argument("--json", action="store_true", help="print the diagnoses as one JSON array")
    _add_adapter_timeout(why)

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
        help="a payload directory to submit, or a job UUID, unique prefix, or path inside the workspace",
    )
    debug.add_argument("--step", metavar="STEP", help="initial step of a freshly submitted payload")
    debug.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default="debug",
        help="placement of a freshly submitted payload (default: debug)",
    )
    debug.add_argument("--follow-children", action="store_true", help="drive spawned children depth first")
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
