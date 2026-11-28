"""The ``job`` command group: creating, submitting, inspecting and steering jobs on the filesystem kernel."""

import argparse
import json
import math
import os
import re
import shlex
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack
from functools import partial
from itertools import chain
from pathlib import Path, PurePosixPath
from typing import IO, Any

from httk.core.cli import CLIContext
from httk.core.identity import (
    OperatorIdentity,
    configured_operator_identity,
    identity_public_key,
    identity_seed,
    resolve_operator_identity,
    sign_document,
)

from .. import _fs, _kernel, _landing, _moving, _pool, _requests, _store, scaffold
from .._bundles import BundleError, read_manifest
from .._children import newest_labeled
from .._job import JobDefinition
from .._kernel import JobRef, LocationIndex
from .._landing import CommitFailed as _CommitFailed
from .._landing import StreamRefused
from .._logging import LOG_LEVELS, configure_logging
from .._manager_scheduling import current_step
from .._state import TERMINAL_STATES, read_state_unowned
from .._util import utc_now
from ..adapters import (
    REMOTE_JOB_DELETE_COMMAND,
    REMOTE_JOB_LIST_COMMAND,
    REMOTE_JOB_LOG_COMMAND,
    REMOTE_JOB_PUBLISH_REQUESTS_COMMAND,
    REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
    REMOTE_JOB_SHOW_COMMAND,
    REMOTE_JOB_WHY_COMMAND,
    read_metadata,
    resolve_remote,
    run_adapter,
)
from ..errors import DuplicateTagError, FormatError, ResolutionMiss, WorkflowError
from ..introspection import (
    JOB_HISTORY_FORMAT,
    JOB_LIST_FORMAT,
    JOB_STATES,
    JobSelectorResolver,
    count_jobs,
    debug_job,
    describe_job,
    explain_held,
    explain_job,
    iter_jobs,
    job_events,
    job_placement,
    list_jobs,
    read_job,
    read_state,
    render_events,
    render_job,
    render_rows,
    resolve_job,
    resolve_job_selectors,
    selector_uses_remote_path,
)
from ..introspection._reading import _jsonl, _location_index, _resolve_selectors
from ..models import (
    JOB_STATE_DIRECTORY,
    canonical_uuid,
    ensure_step_known,
    is_workflow_id,
    normalize_placement,
    parse_job_key,
    placement_text,
)
from ..registry import WorkspaceBinding
from ..removal import (
    RemovalOutcome,
    RemovalReport,
    RequestOutcome,
    _post_and_serve,
    _remove_jobs,
    _request_jobs,
    serve_requests,
    workflow_switch_problem,
)
from ..scaffold import (
    DEFAULT_PLACEMENT,
    JobItem,
    ScaffoldedJob,
    WorkflowProvider,
    _command_barrier,
    _sanitize_tag,
    _submit_copies,
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
    _add_by_path_argument,
    _add_step_option,
    _group,
    _json_value,
    _leaf,
    _load_inputs,
    _local_root,
    _modifiable,
    _pairs,
    _remote_workspace_read,
    _request_budget,
    _resolve_binding,
    _step_name,
    add_durability_arguments,
    confirm,
    remote_workspace_output,
)
from ._daemon_jobs import request_remote_jobs
from ._transfer import (
    _MOVABLE_STATES,
    _protocol_workspace,
    adopt_document,
    build_transfer_parser,
    eject_once,
    run_batch,
)

_ENVELOPES_FORMAT = "httk-workflow-request-envelopes"
#: The option each action requires (and that no other action takes), by request member.
_ACTION_OPTIONS = {
    "priority": "set_priority",
    "step": "override_step",
    "budget": "set_budget",
    "workflow": "set_workflow",
    "destination": "eject",
}
#: The ``set_budget`` options, by the retry-policy limit each sets (also their argparse destinations).
_BUDGET_OPTIONS = {
    "maximum_attempts_per_activation": "--max-attempts-per-activation",
    "maximum_total_attempts": "--max-total-attempts",
    "maximum_activations": "--max-activations",
}
_FORCE_ACTIONS = ("continue", "restart", "override_step")


def _add_workspace_option(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    parser.add_argument(
        "-w",
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
        raise ValueError(f"--command is not a valid shell word list: {exc}") from exc
    if not argv:
        raise ValueError("--command must contain at least one command word")

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
        raise ValueError(f"--command placeholder {joined} is both a --file and a --parameter")
    missing = sorted(names - set(parameters) - set(file_paths))
    if missing:
        joined = ", ".join(missing)
        raise ValueError(
            "--command has placeholder(s) without --parameter or --file "
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
        if PurePosixPath(staged_path).parts[0] == "run":
            # The payload file would itself be the workdir entry the command runs on.
            raise ValueError(f"--file input {name} stages into the workdir run/; stage it elsewhere, such as files/")
        previous = staged_basenames.get(basename)
        if previous is not None:
            raise ValueError(f"--file inputs {previous} and {name} would both stage as {basename} in the workdir")
        staged_basenames[basename] = name

    staging_lines = "".join(
        f'    [ -e {shlex.quote(basename)} ] || [ -L {shlex.quote(basename)} ] || '
        f'cp -p -- "$HTTK_WORKFLOW_JOB_DIR"/{shlex.quote(staged_path)} {shlex.quote(basename)}\n'
        for staged_path, _, basename in staged_files
    )

    def quoted(text: str) -> str:
        return text.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`")

    def render_word(parts: list[tuple[str, str]]) -> str:
        if not any(kind == "parameter" for kind, _ in parts):
            return shlex.quote("".join(value for _, value in parts))
        pieces: list[str] = ['"']
        for kind, value in parts:
            if kind == "parameter":
                if value in parameters:
                    pieces.append(f"$(httk_workflow_parameter {value})")
                else:
                    # The workdir copy (the command runs in the workdir), never the payload input itself.
                    pieces.append(f"$PWD/{quoted(PurePosixPath(file_paths[value]).name)}")
            else:
                pieces.append(quoted(value))
        pieces.append('"')
        return "".join(pieces)

    command_line = " ".join(render_word(parts) for parts in words)
    return (
        "#!/usr/bin/env bash\n"
        "# Generated by `httk job new --command`; edit it and pass it as `httk job new FILE` to customize.\n"
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


#: The suffixes of runner files and workflow documents: a SOURCE with one names a path, never a workflow.
_SOURCE_SUFFIXES = frozenset({".py", ".sh", ".bash", ".cwl", ".json", ".yaml", ".yml"})


def _workflow_target(
    arguments: argparse.Namespace, directory: Path, parameters: Mapping[str, object], files: Mapping[str, object]
) -> str | Path:
    """Return what ``job new`` creates jobs of; ``--command`` writes its runner into *directory*.

    A SOURCE is passed on as written: :func:`~httk.workflow.scaffold.new_job` tells its kinds apart the way
    ``httk workflow install`` does (a runner file, document or package directory before a git URI before a name).
    """

    if arguments.command_template is None:
        source = str(arguments.source)
        # A path that does not exist is not taken for a name (whose miss would suggest installing the path).
        head = source.split("@", 1)[0]
        pathlike = "/" in head or Path(head).suffix.lower() in _SOURCE_SUFFIXES
        if pathlike and not source.startswith("git+") and not Path(source).expanduser().exists():
            raise ValueError(f"no workflow package, git URI, runner file or known workflow name: {source!r}")
        return source
    # The ad hoc id is adhoc:command@<sha12>: the stem names it, the digest pins the rendered text.
    runner = directory / "command.sh"
    runner.write_text(_command_runner_text(arguments.command_template, parameters, files), encoding="utf-8")
    return runner


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


def _one_share(workspace: Workspace, batch: Sequence[JobItem]) -> list[tuple[Workspace, list[JobItem]]]:
    """Submit a whole batch into one workspace: ``job new`` without ``--campaign``."""

    return [(workspace, list(batch))]


def handle_job_new(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Scaffold and submit one job or an input-source batch of an installed workflow."""

    if arguments.source is None and arguments.command_template is None:
        if arguments.input_from:
            raise ValueError(
                f"no SOURCE: --input-from read {arguments.input_from[-1][-1]!r} as an input file; give SOURCE before "
                "--input-from (or after --)"
            )
        raise ValueError("give the workflow as SOURCE, or a command line with --command TEMPLATE")
    if arguments.source is not None and arguments.command_template is not None:
        raise ValueError("give either SOURCE or --command TEMPLATE, not both")
    shares: Callable[[Sequence[JobItem]], list[tuple[Workspace, list[JobItem]]]]
    if arguments.campaign:
        from ._campaign import campaign_shares

        shares = campaign_shares(arguments, context)
    else:
        if arguments.partition is not None or arguments.campaign_index is not None:
            raise ValueError("--partition and --campaign-index apply only with --campaign")
        workspace = _modifiable(arguments, context, action="submit into it")
        shares = partial(_one_share, workspace)
    environment = {
        name: _json_value(text, f"workflow environment {name!r}")
        for name, text in _pairs(arguments.environment, "a workflow environment override")
    }
    parameters = {
        name: _json_value(text, f"job parameter {name!r}")
        for name, text in _pairs(arguments.parameters, "a job parameter")
    }
    calls = dict(_pairs(arguments.calls, "a call slot binding"))
    if len(calls) != len(arguments.calls):
        raise ValueError("--call binds one call slot twice")
    call_parameters: dict[str, dict[str, object]] = {}
    for alias, pair in arguments.call_parameters:
        [(name, text)] = _pairs([pair], "a call slot parameter")
        call_parameters.setdefault(alias, {})[name] = _json_value(text, f"call slot {alias!r} parameter {name!r}")
    files = _staged_files(arguments, context)
    inputs, items, input_tag = _load_inputs(arguments.inputs, arguments.input_from)
    move_inputs = dict(_pairs(arguments.input_moves, "an input moved from a job"))
    if len(move_inputs) != len(arguments.input_moves):
        raise ValueError("--input-move-from-job names one input twice")
    if move_inputs and items:
        raise ValueError("--input-move-from-job moves one file into one job; it cannot be combined with --input-from")
    command_tag = _command_default_tag(parameters) if arguments.command_template is not None else None
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
            "install": arguments.install,
            "embed_workflow": arguments.embed_workflow,
            "calls": calls,
            "call_parameters": call_parameters,
            "duplicate_tag": arguments.duplicate_tag,
            "source_root": arguments.source_root,
        }
        results: Iterator[ScaffoldedJob]
        try:
            if items:
                for item in items:
                    # In a batch, --tag prefixes each item's derived tag (run7-si2o), so one flag names the whole
                    # sweep without erasing per-item identity; the composed tag is re-sanitized to stay a valid tag.
                    derived = item.get("tag")
                    if arguments.tag and derived:
                        item["tag"] = _sanitize_tag(f"{arguments.tag}-{derived}")
                    else:
                        item["tag"] = arguments.tag or derived or command_tag
                # Materialized items: synced and published in batches, one workspace (campaign partition) at a time.
                results = chain.from_iterable(
                    new_jobs(into, target, share, batch=scaffold._BATCH_JOBS, **shared) for into, share in shares(items)
                )
            else:
                tag = arguments.tag or command_tag or input_tag
                [(into, _)] = shares([{"tag": tag}])
                job = new_job(into, target, tag=tag, move_inputs=move_inputs, **shared)
                results = iter([] if job is None else [job])
            return _report_jobs(results, arguments, context, batch=len(items) if items else None)
        except DuplicateTagError as exc:
            # The library names its keyword; the operator needs the flag.
            existing = exc.existing
            raise WorkflowError(
                f"a job with tag {exc.tag!r} already exists in placement {exc.placement.as_posix()!r}: "
                f"{existing.job_key} ({existing.state}); "
                "pass --duplicate-tag allow to create another or --duplicate-tag skip to skip it"
            ) from exc


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
            # The error's notes name a job submitted after these but not reported (named job/ before it failed).
            print(f"submitted and reported {submitted} of {batch} jobs before failing", file=sys.stderr)
        raise
    if arguments.json:
        print(json.dumps([job.as_mapping() for job in collected], indent=2))
    if batch is not None:
        skipped = f" ({batch - submitted} skipped)" if submitted < batch else ""
        print(f"submitted {submitted} jobs{skipped}", file=sys.stderr)
    return 0


def handle_job_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit prepared payload directories (a ``job.json`` v3 and its files) into ``jobs/ready/``."""

    workspace = _modifiable(arguments, context, action="submit into it")
    submitted: list[str] = []
    failed = False

    def report(source: str, ref: JobRef | None, error: Exception | None) -> None:
        # A copy can have both: submitted all the same after an error once it was named job/.
        nonlocal failed
        if error is not None:
            failed = True
            print(f"{source}: {error}", file=sys.stderr)
            for note in getattr(error, "__notes__", ()):
                print(f"{source}: {note}", file=sys.stderr)
        if ref is not None:
            submitted.append(str(ref.path))
            if not arguments.json and error is None:
                print(f"{source}: {ref.path}")

    with (
        _kernel.register_owner(workspace, kind="cli", label="job submit", allocation=None, advertised={}) as owner,
        ExitStack() as stack,
    ):
        if arguments.move:
            # Payloads written before httk ran: one sync for all of them before any is moved; its error cannot be
            # told apart between them, so it fails every one.
            unsynced: str | None = None
            try:
                paths = [Path(source).expanduser().resolve() for source in arguments.sources]
                _moving.sync_operator_bundles(workspace, paths, what="payload")
            except _fs.SyncFailed as exc:
                unsynced = exc.strerror
            except _ERRORS as exc:
                unsynced = f"not submitted: the payloads could not be synced to disk: {exc}"
            for source in arguments.sources:
                if unsynced is not None:
                    failed = True
                    print(f"{source}: {unsynced}", file=sys.stderr)
                    continue
                try:
                    ref = submit_payload(workspace, owner, Path(source).expanduser(), move=True, synced=True)
                except _ERRORS as exc:
                    report(source, None, exc)
                    continue
                report(source, ref, None)
        else:
            # One barrier for the command, committed once per batch of copies before any of them is named job/.
            barrier = stack.enter_context(_command_barrier(workspace, inline=True))
            try:
                for source, copied, error in _submit_copies(
                    workspace, owner, arguments.sources, barrier, errors=_ERRORS
                ):
                    report(source, copied, error)
            except _fs.SyncFailed as exc:
                # As job new: a failed commit stops the command; the batches before it stay submitted.
                failed = True
                print(exc, file=sys.stderr)
    if arguments.json:
        print(json.dumps(submitted, indent=2))
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# job cancel/pause/resume/restart/set and the remote signing protocol
# ---------------------------------------------------------------------------


def _request_options(
    action: str,
    *,
    priority: int | None,
    step: str | None,
    budget: Mapping[str, int | None] | None,
    force: bool,
    destination: str | None,
    keep_budget: bool = False,
    workflow: str | None = None,
) -> dict[str, object]:
    """Return the optional request members of *action*, refusing an option it does not take or lacks."""

    given = {
        "priority": priority,
        "step": step,
        "budget": None if budget is None else dict(budget),
        "workflow": workflow,
        "destination": destination,
    }
    for name, owner in _ACTION_OPTIONS.items():
        if (given[name] is None) == (action == owner):
            if name == "budget":
                label = "one of " + ", ".join(_BUDGET_OPTIONS.values())
            else:
                label = "--override-step" if name == "step" else f"--{name}"
            raise ValueError(f"{label} is required by, and only valid with, the {owner} action")
    if force and action not in _FORCE_ACTIONS:
        raise ValueError("--force applies only to the continue, restart and override_step actions")
    if keep_budget and action not in _FORCE_ACTIONS:
        raise ValueError("--keep-budget applies only to the continue, restart and override_step actions")
    options: dict[str, object] = {name: value for name, value in given.items() if value is not None}
    if force:
        options["force"] = True
    if keep_budget:
        options["keep_budget"] = True
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


def _switch_target(workspace: Workspace, refs: Sequence[JobRef], workflow: str) -> str:
    """Resolve ``set_workflow``'s WORKFLOW once in the workspace and check every job against it, before anything
    is posted (the owner checks again when it applies a request).

    :param workspace: The workspace.
    :param refs: The selected jobs.
    :param workflow: The installed workflow: an id, a short name or ``NAME@REF``.
    :return: The installed id the requests name.
    :raises ValueError: If WORKFLOW is not installed, or a job cannot switch to it (naming the failed rule).
    """

    candidate = _store.lookup(workspace, workflow)
    if candidate is None:
        raise ValueError(f"rule 1 (installed): no workflow {workflow!r} is installed in the workspace")
    providers: dict[str, WorkflowProvider | None] = {}
    for ref in refs:
        job, problem = read_job(ref)
        if job is not None:
            doc, problem = read_state(ref)
            if (
                problem is None
                and job.embedded is None
                and doc is not None
                and doc.unlocked is not None
                and os.path.lexists(ref.path / _store.RUNNER_COPY)
            ):
                # As the owner refuses it: the copy, not an installation, is what the job runs.
                problem = f"the job runs its own workflow copy at {_store.RUNNER_COPY}/; edit or remove it instead of switching"
            problem = problem or workflow_switch_problem(workspace, job, doc, candidate, providers)
        if problem is not None:
            hint = "; narrow the selection with --placement or JOB ids" if len(refs) > 1 and "rule 2" in problem else ""
            raise ValueError(f"{ref.job_key} cannot switch to {candidate.id}: {problem}{hint}")
    provider = candidate.provider()
    if provider.instantiate or provider.instantiate_exec is not None:
        print(
            f"warning: {candidate.id} has an instantiate hook, which is not re-run: each job keeps the payload, "
            "parameters, declarations and resources it was created with",
            file=sys.stderr,
        )
    return candidate.id


def _wait_children(workspace: Workspace, ref: JobRef) -> list[tuple[str, JobRef | None]]:
    """Return each join child (its key, and its reference or ``None``) of a job that failed at its wait.

    A job that did not fail at its wait (with its join kept) has none.
    """

    if ref.state != "failed":
        return []
    doc, _damage = read_state(ref)
    children = None if doc is None or doc.join is None else doc.join.get("children")
    located: list[tuple[str, JobRef | None]] = []
    for entry in children if isinstance(children, tuple) else ():
        if isinstance(entry, Mapping):
            hint = PurePosixPath(str(entry.get("placement_hint")))
            child = _kernel.locate(workspace, str(entry.get("job_id")), placement_hint=hint, include_owned=True)
            located.append((str(entry.get("job_key")), child))
    return located


def _with_failed_descendants(workspace: Workspace, refs: Sequence[JobRef]) -> list[JobRef]:
    """Expand ``continue --tree``: each job's failed join descendants deepest first, then the job, once each."""

    expanded: list[JobRef] = []
    entered: set[str] = set()

    def visit(ref: JobRef) -> None:
        if ref.job_id in entered:
            return
        entered.add(ref.job_id)
        for _key, child in _wait_children(workspace, ref):
            if child is not None and child.state == "failed":
                visit(child)
        expanded.append(ref)

    for ref in refs:
        visit(ref)
    return expanded


def _continue_notes(workspace: Workspace, refs: Sequence[JobRef], action: str, *, tree: bool = False) -> None:
    """Note, before a ``continue`` reopens a wait, the join children that would fail it again.

    With *tree* the failed children are resumed, so only the others are noted.
    """

    if action != "continue":
        return
    for ref in refs:
        for key, child in _wait_children(workspace, ref):
            if child is None:
                print(
                    f"note: {key} was not found at its placement; the reopened wait of {ref.job_key} fails with "
                    "dependency_failure after the join grace unless it reappears",
                    file=sys.stderr,
                )
            elif child.state == _kernel.OWNED:
                print(
                    f"note: {key} is held by an owner right now; the reopened wait of {ref.job_key} fails again if "
                    "it is released still failed or cancelled",
                    file=sys.stderr,
                )
            elif child.state == "cancelled" or (child.state == "failed" and not tree):
                print(
                    f"note: {key} is still {child.state}; the reopened wait of {ref.job_key} fails again unless it "
                    "is resumed first (job resume --tree also resumes failed descendants)",
                    file=sys.stderr,
                )


def _stale_children(workspace: Workspace, ref: JobRef) -> list[tuple[str, JobRef]]:
    """Return the failed or cancelled children a rerun spawn of *ref* would reuse, with their labels.

    They are the newest recorded child per label that is found in the workspace (owned included), as the
    SDK reuses them.
    """

    doc, _damage = read_state(ref)
    newest = newest_labeled(() if doc is None else doc.children)
    if not newest:
        return []
    found = _kernel.locate_many(
        workspace,
        [str(entry["job_id"]) for entry in newest.values()],
        placements=[PurePosixPath(str(entry["placement"])) for entry in newest.values()],
        # ponytail: one unsettled listing, so a child caught mid-rename is missed and the restart proceeds
        # (the SDK still warns); settle the misses as the SDK does if that ever matters.
        settle=False,
    )
    stale = [(label, found.get(str(entry["job_id"]))) for label, entry in newest.items()]
    # An owned child counts by the state it was claimed from, as the SDK reuses it.
    return [
        (label, child)
        for label, child in stale
        if child is not None and (child.from_state or child.state) in {"failed", "cancelled"}
    ]


def _restart_refusal(workspace: Workspace, refs: Sequence[JobRef]) -> list[str]:
    """Return why a ``restart`` of *refs* is pointless (one line per stale child, then the remedy), or nothing.

    A restart reruns the spawning step, which reuses the existing children, failed and cancelled ones too.
    """

    lines: list[str] = []
    parents: list[str] = []
    for ref in refs:
        stale = _stale_children(workspace, ref)
        if stale:
            parents.append(ref.job_key)
        for label, child in stale:
            state = child.from_state or child.state
            code = ""
            if state == "failed":
                doc, _damage = read_state(child)
                failure = None if doc is None else doc.failure
                code = f" {failure.get('code')}" if failure is not None and failure.get("code") else ""
            held = " (held by an owner)" if child.from_state else ""
            lines.append(
                f"error: {label} {child.job_key} is {state}{code}{held}; "
                "a restart reruns the spawning step, which reuses it"
            )
    if lines:
        lines.append(
            "resume failed descendants and the parent with "
            f"'httk job resume --tree {' '.join(parents)}'; "
            "delete a child with 'httk job delete' to have the step create a fresh one; "
            "or pass --force to restart anyway"
        )
    return lines


def _serve_in_order(workspace: Workspace, posts: Sequence[tuple[JobRef, Callable[[], Path]]]) -> int:
    """Post and serve one request at a time, in order, stopping at the first that does not resume its job.

    Serving each before posting the next keeps a live manager from reopening an ancestor's wait while a
    descendant is still failed (``continue --tree`` posts deepest first). The far-side leg of a remote tree uses
    this; a local one is served by :func:`_continue_tree` in the same order.

    :param workspace: The workspace.
    :param posts: Each job with the callable that posts its request and returns the request file.
    :return: 0 when every job resumed, otherwise 1 (the rest are not posted).
    """

    for index, (ref, post) in enumerate(posts):
        (outcome,) = serve_requests(workspace, [(ref, post())])
        _print_outcome(outcome)
        if not _stops_tree(outcome, [rest for rest, _post in posts[index + 1 :]]):
            continue
        return 1
    return 0


def _stops_tree(outcome: RequestOutcome, rest: Sequence[JobRef]) -> bool:
    """Whether a tree stops at *outcome* (it did not resume its job); if so, say that *rest* are not posted."""

    if outcome.status == "applied" and outcome.state != "failed":
        return False
    status = "applied but left the job failed" if outcome.status == "applied" else outcome.status
    for ref in rest:
        print(
            f"not posted: {ref.job_key} (an earlier request in the tree was {status}; "
            "resolve it and rerun job resume --tree)",
            file=sys.stderr,
        )
    return True


def _continue_tree(
    workspace: Workspace,
    refs: Sequence[JobRef],
    *,
    reason: str,
    force: bool,
    keep_budget: bool,
    identity: OperatorIdentity,
    index: LocationIndex,
) -> int:
    """Post and serve ``continue --tree`` one job at a time, deepest first, stopping at one that does not resume.

    Each job is a chunk of its own (:func:`~httk.workflow.removal._post_and_serve`), so an uncertain outcome, an error
    or an interrupt withdraws its request as for any local request; one CLI owner, registered once the first request
    is posted, serves them all.
    """

    def prepare(ref: JobRef) -> tuple[JobRef, Mapping[str, object]]:
        ((_ref, document),) = _prepare_job_requests(
            workspace,
            [ref],
            action="continue",
            reason=reason,
            operator=identity.label,
            priority=None,
            step=None,
            force=force,
            destination=None,
            identity=identity,
            budget=None,
            keep_budget=keep_budget,
            workflow=None,
        )
        return ref, document

    with ExitStack() as stack:
        owner = _command_owner(workspace, stack, "immediate job requests")
        for position, ref in enumerate(refs):
            (outcome,) = _post_and_serve(
                workspace, [(ref.job_id, partial(prepare, ref))], owner=owner, index=index, report=_print_outcomes
            )
            if _stops_tree(outcome, refs[position + 1 :]):
                return 1
    return 0


def _command_owner(workspace: Workspace, stack: ExitStack, label: str) -> Callable[[], _kernel.Owner]:
    """Return a getter of the command's one CLI owner, registered in *stack* on the first call."""

    registered: list[_kernel.Owner] = []

    def owner() -> _kernel.Owner:
        if not registered:
            registered.append(
                stack.enter_context(
                    _kernel.register_owner(workspace, kind="cli", label=label, allocation=None, advertised={})
                )
            )
        return registered[0]

    return owner


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
    budget: Mapping[str, int | None] | None = None,
    keep_budget: bool = False,
    workflow: str | None = None,
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
    :param budget: The retry-policy limits (``None`` lifts one), for ``set_budget``.
    :param keep_budget: Keep the retry budget as it is, for ``continue``, ``restart`` and ``override_step``
        (otherwise the revival restores the job's declared allowance).
    :param workflow: The installed workflow to switch to, for ``set_workflow``: a short name, ``NAME@REF`` or
        an id, resolved once in *workspace*; the requests name its id.
    :return: The posted request files paired with their jobs.
    :raises ValueError: For an unknown action, a misplaced option, an unknown runner step, or a workflow a job
        cannot switch to.
    """

    prepared = _prepare_job_requests(
        workspace,
        refs,
        action=action,
        reason=reason,
        operator=operator,
        priority=priority,
        step=step,
        force=force,
        destination=destination,
        identity=identity,
        budget=budget,
        keep_budget=keep_budget,
        workflow=workflow,
    )
    return [(ref, _kernel.post_request(workspace, document)) for ref, document in prepared]


def _prepare_job_requests(
    workspace: Workspace,
    refs: Sequence[JobRef],
    *,
    action: str,
    reason: str,
    operator: str | None,
    priority: int | None,
    step: str | None,
    force: bool,
    destination: str | None,
    identity: OperatorIdentity | None,
    budget: Mapping[str, int | None] | None,
    keep_budget: bool,
    workflow: str | None,
) -> list[tuple[JobRef, dict[str, object]]]:
    """Check every job and build its signed and verified request document (:func:`publish_job_requests`), posting
    nothing; each document's bytes are remembered as verified (``_requests._remember_verified``)."""

    if action not in _requests.ACTIONS:
        raise ValueError(f"unknown job request action: {action}")
    selected = identity or _resolve_request_identity(None)
    ensure_identity_key(selected)
    options = _request_options(
        action,
        priority=priority,
        step=step,
        budget=budget,
        force=force,
        destination=destination,
        keep_budget=keep_budget,
        workflow=workflow,
    )
    _prevalidate(refs, action, step, force=force)
    if workflow is not None:
        options["workflow"] = _switch_target(workspace, refs, workflow)
    prepared = []
    for ref in refs:
        document = _request_document(ref, action, operator or selected.label, reason, options)
        if selected.seed_path is not None:
            document = sign_document(document, seed_path=selected.seed_path)
        # Verified here; serving exactly these bytes skips a second check.
        _requests._remember_verified(document)
        prepared.append((ref, document))
    return prepared


def _check_wait(wait: bool, timeout: float | None, actions: Sequence[str]) -> None:
    if timeout is not None and not wait:
        raise ValueError("--timeout requires --wait")
    if wait and any(action != "pause" for action in actions):
        raise ValueError("--wait is only valid with the pause action")
    if timeout is not None and (not math.isfinite(timeout) or timeout < 0):
        raise ValueError("--timeout must be finite and nonnegative")


def _at_steps(ref: JobRef, steps: frozenset[str], *, named: bool) -> bool:
    """Return whether *ref* is at one of *steps*; say why on standard error when a *named* job is left out."""

    job, error = read_job(ref)
    doc, state_error = (None, None) if job is None else read_state(ref)
    if job is None or state_error is not None:
        # Its step is unknown: a damaged state.json is not the initial step.
        print(f"{ref.job_key}: {error or state_error}; its step is unknown: left out", file=sys.stderr)
        return False
    step = current_step(job, doc)
    if step in steps:
        return True
    if named:
        print(f"{ref.job_key} is at step {step}, not --step {', '.join(sorted(steps))}: left out", file=sys.stderr)
    return False


def _selection_states(arguments: argparse.Namespace) -> Sequence[str] | None:
    """The states a ``--state`` (or a ``--step`` without JOB selectors) selection lists, or ``None`` without one."""

    if arguments.state:
        return list(arguments.state)
    if arguments.step is not None and not arguments.job_id:
        return _MOVABLE_STATES  # an away copy is selected only by name or --state away
    return None


def _selection_index(workspace: Workspace, arguments: argparse.Namespace) -> LocationIndex:
    """The command's location index, listing exactly what its ``--state`` selection traverses (all jobs without one)."""

    states = _selection_states(arguments)
    return _location_index(workspace) if states is None else _location_index(workspace, states, arguments.placement)


def _selected_jobs(
    workspace: Workspace, cwd: Path, arguments: argparse.Namespace, index: LocationIndex | None = None
) -> list[JobRef]:
    """Return the JOB selectors' jobs, then every other job in a ``--state`` (under ``--placement``), once each.

    ``--step`` narrows both; without JOB selectors or ``--state`` it selects from every unowned state but ``away``.
    The selection shares the listing of *index*, the command's location index (:func:`_selection_index`).
    """

    steps = None if arguments.step is None else frozenset(arguments.step)
    index = _selection_index(workspace, arguments) if index is None else index
    refs = _resolve_selectors(workspace, cwd, arguments.job_id, index)
    if steps is not None:
        refs = [ref for ref in refs if _at_steps(ref, steps, named=True)]
    if _selection_states(arguments) is not None:
        seen = {ref.job_id for ref in refs}
        for ref in index.refs():  # the --state listing itself
            if ref.job_id not in seen and (steps is None or _at_steps(ref, steps, named=False)):
                seen.add(ref.job_id)
                refs.append(ref)
    return refs


#: The request arguments a verb that does not take them leaves at these values.
_REQUEST_DEFAULTS: dict[str, object] = {
    "state": [],
    "step": None,
    "placement": None,
    "operator": None,
    "request_id": None,
    "priority": None,
    "override_step": None,
    "workflow": None,
    "destination": None,
    "force": False,
    "keep_budget": False,
    "tree": False,
    "wait": False,
    "timeout": None,
    "adapter_timeout": None,
}


def handle_job_control(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Post the request ``job cancel``, ``pause``, ``resume`` or ``restart`` names against jobs.

    A pause optionally waits for the pauses to land.
    """

    if arguments.reason is None:
        arguments.reason = f"httk job {arguments.verb}"
    return _post_requests(arguments, context)


def handle_job_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Post the requests the KEY=VALUE pairs of ``job set`` name, one per kind in the order of :data:`_SET_KEYS`.

    The budget keys form one ``set_budget`` request. The requests are posted one after another, each selecting
    its jobs anew, and the command stops at the first that does not exit zero.
    """

    pairs: dict[str, object] = {}
    for word in arguments.words:
        if isinstance(word, tuple):
            key, value = word
            if key in pairs:
                raise ValueError(f"{key} is given twice")
            pairs[key] = value
    if not pairs:
        raise ValueError(f"job set needs at least one KEY=VALUE pair: {', '.join(_SET_KEYS)}")
    if (arguments.force or arguments.keep_budget) and "step" not in pairs:
        raise ValueError("--force and --keep-budget apply to step=")
    if arguments.reason is None:
        arguments.reason = "httk job set"
    base = {name: value for name, value in vars(arguments).items() if name != "words"}
    base["job_id"] = [word for word in arguments.words if isinstance(word, str)]
    requests: list[dict[str, object]] = []
    if "workflow" in pairs:
        requests.append({"action": "set_workflow", "workflow": pairs["workflow"]})
    budget = {
        f"maximum_{key.removeprefix('budget.max_')}": value for key, value in pairs.items() if key.startswith("budget.")
    }
    if budget:
        requests.append({"action": "set_budget", **budget})
    if "priority" in pairs:
        requests.append({"action": "set_priority", "priority": pairs["priority"]})
    if "step" in pairs:
        requests.append({"action": "override_step", "override_step": pairs["step"]})
    if arguments.request_id is not None and len(requests) > 1:
        raise ValueError("--request-id names one daemon command: set one kind of key per call")
    for request in requests:
        revival = request["action"] == "override_step"
        options = {"force": base["force"] and revival, "keep_budget": base["keep_budget"] and revival}
        status = _post_requests(argparse.Namespace(**{**base, **options, **request}), context)
        if status:
            return status
    return 0


def _post_requests(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Post operator requests against jobs, and optionally wait for pauses to land.

    *arguments* name the request ``action`` and the selection; a request argument they leave out takes its
    :data:`_REQUEST_DEFAULTS` value.
    """

    arguments = argparse.Namespace(**{**_REQUEST_DEFAULTS, **vars(arguments)})
    _check_wait(arguments.wait, arguments.timeout, (arguments.action,))
    tree = bool(getattr(arguments, "tree", False))
    if tree and arguments.action != "continue":
        raise ValueError("--tree applies only to the continue action")
    if not (arguments.job_id or arguments.state or arguments.step):
        raise ValueError("select jobs: give JOB selectors, --state STATE, --step STEP, or a combination")
    if arguments.placement is not None and (
        not (arguments.state or (arguments.step and not arguments.job_id))
        or not normalize_placement(arguments.placement).parts
    ):
        raise ValueError(
            "--placement narrows --state, or --step without JOB, to a placement prefix; "
            "it needs one of them and a nonempty prefix"
        )
    identity = _resolve_request_identity(arguments.operator)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None and ":" in binding.name
        if arguments.state or arguments.step:
            raise ValueError(
                "--state, --step and --placement select jobs of a local workspace; give job ids for a remote one"
            )
        target = resolve_remote(binding.remote, project=context.cwd)
        if read_metadata(target.bundle).get("kind") == "mount-daemon":
            if arguments.action in ("set_workflow", "unlock"):
                raise ValueError(
                    f"{arguments.action} is not carried by the workspace daemon protocol; run it on the daemon host"
                )
            if tree:
                raise ValueError("--tree is not carried by the workspace daemon protocol; run it on the daemon host")
            status, stdout = request_remote_jobs(binding, context, arguments, identity)
            sys.stdout.write(stdout)
            return status
        _refuse_request_id(arguments)
        ensure_identity_key(identity)
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
    _refuse_request_id(arguments)
    ensure_identity_key(identity)
    workspace = _modifiable(arguments, context, action="post requests in it")
    index = _selection_index(workspace, arguments)
    refs = _selected_jobs(workspace, context.cwd, arguments, index)
    if not refs:
        print("no jobs selected", file=sys.stderr)
        return 1
    if arguments.action == "restart" and not arguments.force:
        refusal = _restart_refusal(workspace, refs)
        if refusal:
            for line in refusal:
                print(line, file=sys.stderr)
            return 1
    if tree:
        refs = _with_failed_descendants(workspace, refs)
    _continue_notes(workspace, refs, arguments.action, tree=tree)
    if tree:
        return _continue_tree(
            workspace,
            refs,
            reason=arguments.reason,
            force=bool(arguments.force),
            keep_budget=bool(arguments.keep_budget),
            identity=identity,
            index=index,
        )
    prepared = _prepare_job_requests(
        workspace,
        refs,
        action=arguments.action,
        reason=arguments.reason,
        operator=identity.label,
        priority=arguments.priority,
        step=arguments.override_step,
        force=bool(arguments.force),
        destination=arguments.destination,
        identity=identity,
        budget=_request_budget(arguments),
        keep_budget=bool(arguments.keep_budget),
        workflow=arguments.workflow,
    )
    with ExitStack() as stack:
        # One CLI owner for the command, registered once the first chunk is posted.
        owner = _command_owner(workspace, stack, "immediate job requests")
        outcomes = _post_and_serve(
            workspace,
            [(ref.job_id, partial(_prepared, ref, document)) for ref, document in prepared],
            owner=owner,
            index=index,
            report=_print_outcomes,
            action=arguments.action,
        )
    if any(outcome.status in {"refused", "uncertain"} for outcome in outcomes):
        return 1
    if not arguments.wait:
        return 0
    pending = [
        (ref, outcome.request_id)
        for ref, outcome in zip(refs, outcomes, strict=True)
        if outcome.status == "queued" or (outcome.status == "applied" and outcome.state != "paused")
    ]
    return 0 if not pending else _wait_for_pauses(workspace, pending, timeout=arguments.timeout)


def _refuse_request_id(arguments: argparse.Namespace) -> None:
    """Refuse ``--request-id`` where no workspace daemon carries the command (local and ssh workspaces)."""

    if getattr(arguments, "request_id", None) is not None:
        raise ValueError("--request-id is supported only for one mount-daemon job request")


def _prepared(ref: JobRef, document: Mapping[str, object]) -> tuple[JobRef, Mapping[str, object]]:
    """One job's request document, prepared already (:func:`_prepare_job_requests`)."""

    return ref, document


def _print_outcomes(outcomes: Sequence[RequestOutcome]) -> None:
    for outcome in outcomes:
        _print_outcome(outcome)


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
        step=arguments.override_step,
        budget=_request_budget(arguments),
        force=bool(arguments.force),
        destination=arguments.destination,
        keep_budget=bool(arguments.keep_budget),
        workflow=arguments.workflow,
    )
    tree = bool(arguments.tree)
    if tree and arguments.action != "continue":
        raise ValueError("--tree applies only to the continue action")
    refs = [resolve_job(workspace, selector) for selector in arguments.job_id]
    if arguments.action == "restart" and not arguments.force:
        refusal = _restart_refusal(workspace, refs)
        if refusal:
            raise ValueError("\n".join(refusal))
    if tree:
        refs = _with_failed_descendants(workspace, refs)
    _prevalidate(refs, arguments.action, arguments.override_step, force=bool(arguments.force))
    for ref in refs if arguments.action == "unlock" else ():
        # The envelope path posts exactly the signed requests: it cannot walk to the ancestors as `job unlock` does.
        job, _problem = read_job(ref)
        if job is not None and job.parent is not None:
            raise ValueError(
                f"{ref.job_key} is a spawned job: unlock spawned jobs on the workspace host with `httk job unlock`, "
                "which also unlocks their ancestors"
            )
    if arguments.workflow is not None:
        # Resolved and checked against this (the far) side's installations; the client accepts the id it names.
        options["workflow"] = _switch_target(workspace, refs, arguments.workflow)
        print(f"{arguments.workflow} is installed here as {options['workflow']}", file=sys.stderr)
    _continue_notes(workspace, refs, arguments.action, tree=tree)
    document: dict[str, object] = {
        "format": _ENVELOPES_FORMAT,
        "format_version": 2,
        "envelopes": [
            _request_document(ref, arguments.action, arguments.operator, arguments.reason, options) for ref in refs
        ],
        "job_keys": [ref.job_key for ref in refs],
    }
    if tree:
        # The envelopes are the expansion, deepest first: the client signs it and asks for it to be served in order.
        document["tree"] = True
    print(json.dumps(document, separators=(",", ":")))
    return 0


def _validate_remote_envelopes(
    document: Mapping[str, object], arguments: argparse.Namespace, operator: str
) -> list[dict[str, object]]:
    """Check the far side's envelopes against exactly what was asked, so it cannot get anything else signed.

    With ``--tree`` the far side expands the selection to failed descendants: every envelope must still be a
    ``continue`` with the requested options, and every selector must be matched by one; the others are noted.

    :param document: The envelopes document the far side returned.
    :param arguments: The local request arguments.
    :param operator: The operator label the envelopes must carry.
    :return: The envelopes, in selector order (in the far side's serving order with ``--tree``).
    :raises ValueError: If the document is malformed or any envelope differs from the request.
    """

    tree = bool(getattr(arguments, "tree", False))
    envelopes, keys = document.get("envelopes"), document.get("job_keys")
    if (
        document.get("format") != _ENVELOPES_FORMAT
        or document.get("format_version") != 2
        or not isinstance(envelopes, list)
        or not isinstance(keys, list)
        or not all(isinstance(item, dict) for item in envelopes)
        or not all(isinstance(item, str) for item in keys)
        or (tree and document.get("tree") is not True)
    ):
        raise ValueError("remote did not return a valid request-envelopes document")
    too_few = len(envelopes) < len(arguments.job_id)
    if too_few or (len(envelopes) > len(arguments.job_id) and not tree) or len(keys) != len(envelopes):
        raise ValueError(
            f"remote returned {len(envelopes)} request envelopes for {len(arguments.job_id)} requested jobs"
        )
    options = _request_options(
        arguments.action,
        priority=arguments.priority,
        step=arguments.override_step,
        budget=_request_budget(arguments),
        force=bool(arguments.force),
        destination=getattr(arguments, "destination", None),
        keep_budget=bool(getattr(arguments, "keep_budget", False)),
        workflow=getattr(arguments, "workflow", None),
    )
    # Every optional member the format knows (eject's tree and unlock's no_copy too, which a forwarded request never
    # asks for): the far side may add none the request did not carry.
    expected: dict[str, object] = {**dict.fromkeys(_requests._OPTIONS), **options}
    workflow = options.get("workflow")
    if isinstance(workflow, str) and not is_workflow_id(workflow) and envelopes:
        # A name (or NAME@REF) resolves on the far side: every envelope must name the one id it resolved to.
        expected["workflow"] = envelopes[0].get("workflow")
    expected.update(action=arguments.action, operator=operator, reason=arguments.reason)
    for index, (envelope, key) in enumerate(zip(envelopes, keys, strict=True)):
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
        if key_job_id != envelope["job_id"]:
            raise ValueError(f"request envelope {index} job key disagrees with its job id")
    if not tree:
        for index, (envelope, key, selector) in enumerate(zip(envelopes, keys, arguments.job_id, strict=True)):
            problem = _selector_mismatch(selector, envelope["job_id"], key)
            if problem is not None:
                raise ValueError(f"request envelope {index} {problem}")
        return envelopes
    if len({envelope["job_id"] for envelope in envelopes}) != len(envelopes):
        raise ValueError("remote returned more than one request envelope for a job")
    matched: set[int] = set()
    for selector in arguments.job_id:
        hits = [
            index
            for index, (envelope, key) in enumerate(zip(envelopes, keys, strict=True))
            if _selector_mismatch(selector, envelope["job_id"], key) is None
        ]
        if not hits:
            raise ValueError(f"no request envelope matches requested selector {selector!r}")
        matched.update(hits)
    for index, key in enumerate(keys):
        if index not in matched:
            print(f"note: job resume --tree also resumes {key}", file=sys.stderr)
    return envelopes


def _selector_mismatch(selector: str, job_id: object, key: str) -> str | None:
    """Return how a job (its id and key) fails to match a requested selector, or ``None`` when it matches."""

    try:
        uuid_selector = canonical_uuid(selector, "JOB") == selector
    except (WorkflowError, ValueError):
        uuid_selector = False
    if uuid_selector and job_id != selector:
        return f"does not match UUID selector {selector!r}"
    if not (str(job_id).startswith(selector) or key.startswith(selector)):
        return f"does not match requested selector {selector!r}"
    return None


def request_remote_job_result(
    binding: WorkspaceBinding,
    context: CLIContext,
    arguments: argparse.Namespace,
    identity: OperatorIdentity,
    *,
    interactive: bool = True,
    relay: Callable[[Sequence[str]], tuple[int, str, str]] | None = None,
) -> tuple[int, str, str]:
    """Build request documents remotely, sign them here, and post them remotely.

    :param binding: The remote workspace binding.
    :param context: The invocation context.
    :param arguments: The request arguments (``action``, ``job_id``, ``reason`` and the options).
    :param identity: The signing identity.
    :param interactive: Whether the adapter may prompt on this terminal; unused with ``relay``.
    :param relay: Runs each remote command vector instead of one adapter ``invoke`` per command, such as
        the monitor's session; the adapter timeout is then unused too.
    :return: The far side's exit status, standard output and standard error.
    """

    target = resolve_remote(binding.remote, project=context.cwd)

    def invoke(vector: list[str]) -> dict[str, Any]:
        if relay is not None:
            returncode, stdout, stderr = relay(vector)
            return {"returncode": returncode, "stdout": stdout, "stderr": stderr}
        return run_adapter(
            target.bundle, "invoke", {"argv": vector}, timeout=arguments.adapter_timeout, interactive=interactive
        )

    remote_name = binding.name.split(":", 1)[1]
    argv = [
        *REMOTE_JOB_REQUEST_ENVELOPES_COMMAND,
        arguments.action,
        f"--workspace={remote_name}",
        f"--operator={identity.label}",
        f"--reason={arguments.reason}",
        "--json",
    ]
    for option in ("priority", "override_step", "workflow", "destination"):
        value = getattr(arguments, option, None)
        if value is not None:
            argv.append(f"--{option.replace('_', '-')}={value}")
    for name, limit in (_request_budget(arguments) or {}).items():
        argv.append(f"{_BUDGET_OPTIONS[name]}={'unlimited' if limit is None else limit}")
    if arguments.force:
        argv.append("--force")
    if getattr(arguments, "keep_budget", False):
        argv.append("--keep-budget")
    tree = bool(getattr(arguments, "tree", False))
    if tree:
        argv.append("--tree")
    argv.extend(arguments.job_id)
    result = invoke(argv)
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
    if tree:
        publish.append("--sequential")
    published = invoke(publish)
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
    if arguments.sequential and any(document["action"] != "continue" for document in documents):
        raise ValueError("--sequential is only valid with continue requests")
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
    if arguments.sequential:
        return _serve_in_order(
            workspace,
            [
                (ref, partial(_kernel.post_request, workspace, document))
                for ref, document in zip(refs, documents, strict=True)
            ],
        )
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
    """Resolve a request identity and guide an unconfigured user to ``httk setup``."""

    if selector is None:
        identity = configured_operator_identity()
        if identity is None:
            raise ValueError("no operator identity is configured; run `httk setup` to establish one")
        return identity
    return resolve_operator_identity(selector)


def _print_outcome(outcome: RequestOutcome) -> None:
    state = f"; state={outcome.state}" if outcome.state is not None else ""
    print(f"{outcome.job_id}\t{outcome.request_id}\t{outcome.status}\t{outcome.explanation}{state}")


def _complete_job_requests(
    workspace: Workspace, published: list[tuple[JobRef, Path]], *, wait: bool, timeout: float | None
) -> int:
    """Serve a bounded snapshot immediately, then optionally wait for queued pauses."""

    outcomes = serve_requests(workspace, published)
    for outcome in outcomes:
        _print_outcome(outcome)
    if any(outcome.status in {"refused", "uncertain"} for outcome in outcomes):
        return 1
    if not wait:
        return 0
    pending = [
        (ref, outcome.request_id)
        for (ref, _path), outcome in zip(published, outcomes, strict=True)
        if outcome.status == "queued" or (outcome.status == "applied" and outcome.state != "paused")
    ]
    return 0 if not pending else _wait_for_pauses(workspace, pending, timeout=timeout)


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


def _wait_for_pauses(workspace: Workspace, published: list[tuple[JobRef, str]], *, timeout: float | None) -> int:
    """Wait for posted pause requests (jobs and request ids) and report each final outcome."""

    pending = {request_id: ref for ref, request_id in published}
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
    for _ref, request_id in published:
        print(outcomes[request_id][1])
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
        for state in arguments.state or []:
            tail.extend(("--state", state))
        for step in arguments.step or []:
            tail.extend(("--step", step))
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
        states=arguments.state,
        placement_prefix=arguments.placement,
        after=arguments.after,
        limit=arguments.limit,
        tag_contains=arguments.tag_contains,
        steps=arguments.step,
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
        states = list(arguments.state or JOB_STATES)
        counted = _pool.run_ordered(
            lambda state: count_jobs(workspace, state, arguments.placement), states, workspace.io_workers
        )
        document["counts"] = dict(zip(states, counted, strict=True))
    print(json.dumps(document, indent=2))
    return 0


def _print_removal_outcomes(outcomes: Sequence[RemovalOutcome]) -> None:
    """Print one line per job removal outcome, as the outcomes become known."""

    for outcome in outcomes:
        if outcome.removed:
            # A deleted job's reason says what is left of it (its tree left for workspace gc), if anything.
            print(
                f"{outcome.job_key}\t{outcome.kind}\tremoved"
                + ("" if outcome.reason is None else f"\t{outcome.reason}")
            )
        elif outcome.request is not None and outcome.request.status == "queued":
            print(f"{outcome.job_key}\t{outcome.kind}\tqueued\t{outcome.reason}")
        elif outcome.request is not None and outcome.request.status == "uncertain":
            print(f"{outcome.job_key}\t{outcome.kind}\tuncertain\t{outcome.reason}")
        else:
            print(f"{outcome.job_key}\t{outcome.kind}\trefused\t{outcome.reason}")


def _removal_status(report: RemovalReport) -> int:
    """Print the removal summary and return 1 when a job was refused (a queued delete is not refused)."""

    print(f"removed {report.removed_count} of {len(report.outcomes)} job(s)")
    refused = any(
        not outcome.removed and (outcome.request is None or outcome.request.status != "queued")
        for outcome in report.outcomes
    )
    return 1 if refused else 0


def _confirm_job_delete(rows: Sequence[tuple[str, str]]) -> bool:
    """List the jobs to delete, then confirm on the terminal or refuse without one."""

    for job_key, state in rows:
        print(f"{job_key}\t{state}")
    return confirm(f"Delete {len(rows)} jobs?", yes=False)


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
        if _daemon_binding(binding, context):
            # A workspace daemon runs no commands but its signed job requests.
            if not confirm(f"Delete {len(arguments.jobs)} job(s)?", yes=arguments.yes):
                return 1
            return _forward(arguments, context, "delete")
        _refuse_request_id(arguments)
        _validate_remote_job_ids(arguments.jobs, "delete")
        if not arguments.yes and not sys.stdin.isatty():
            print("job delete without a terminal requires --yes", file=sys.stderr)
            return 1
        status, rows = _remote_delete_rows(binding, context, arguments)
        if status:
            return status
        if not arguments.yes and not _confirm_job_delete(rows):
            return 1
        remote_name = binding.name.split(":", 1)[1]
        option = "--yes" if arguments.yes else "--confirmed"
        argv = [*REMOTE_JOB_DELETE_COMMAND, option, *arguments.jobs, "--workspace", remote_name]
        status, stdout, stderr = remote_workspace_output(binding, context, argv, timeout=arguments.adapter_timeout)
        sys.stdout.write(stdout)
        sys.stderr.write(stderr)
        return status
    _refuse_request_id(arguments)
    workspace = _modifiable(arguments, context, action="delete jobs in it")
    index = _location_index(workspace)
    refs = _resolve_selectors(workspace, context.cwd, arguments.jobs, index)
    if not (arguments.yes or arguments.confirmed or _confirm_job_delete([(ref.job_key, ref.state) for ref in refs])):
        return 1
    # Each chunk's outcomes are printed once served, and those known when the command stops before it propagates.
    return _removal_status(_remove_jobs(workspace, refs, index, report=_print_removal_outcomes))


def handle_job_recover(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Return away copies, each root with its away tree, to the states they left, so that they can be pushed again."""

    if bool(arguments.jobs) == arguments.all:
        raise ValueError(
            "give JOB selectors or --all, not both" if arguments.all else "name away jobs: JOB... or --all"
        )
    if arguments.placement is not None and (not arguments.all or not normalize_placement(arguments.placement).parts):
        raise ValueError("--placement narrows --all to a nonempty placement prefix")
    workspace = _modifiable(arguments, context, action="recover away copies in it")
    if arguments.all:
        listed = iter_jobs(workspace, ("away",), placement_prefix=arguments.placement)
        # A member goes with its root.
        refs = [ref for ref in listed if _moving.away_root(workspace, ref).job_id == ref.job_id]
        if not refs:
            print("no away jobs to recover")
    else:
        refs = resolve_job_selectors(workspace, context.cwd, arguments.jobs)
    failed = False
    with _kernel.register_owner(workspace, kind="cli", label="job recover", allocation=None, advertised={}) as owner:
        for ref in refs:
            if ref.state == _kernel.OWNED:
                message = "it is running or held by an owner; recover it once it is back in away"
                print(f"{ref.job_key}: {message}", file=sys.stderr)
                failed = True
                continue
            root = _kernel.claim(workspace, owner, ref)
            if root is None:
                print(f"{ref.job_key}: another actor moved it first", file=sys.stderr)
                failed = True
                continue
            try:
                released = _moving.recover(workspace, owner, root)
            except _ERRORS as exc:
                print(f"{ref.job_key}: {exc}", file=sys.stderr)
                failed = True
                continue
            print(f"recovered {ref.job_key}: {len(released)} job(s) back to {released[-1].state}")
    return 1 if failed else 0


def _request_now(arguments: argparse.Namespace, context: CLIContext, action: str, done: str) -> int:
    """Post one *action* request per selected job and apply it now where the job is unowned.

    A request for a job an owner holds is ``queued`` for that owner (exit status 0, as ``job pause`` and the other
    request verbs);
    one that did not take effect otherwise is ``refused`` or ``uncertain`` (exit status 1). An ``unlock`` copies
    the runner unless ``--no-copy``, and is followed by marker-only ``unlock`` requests for the ancestors.
    """

    if _resolve_binding(arguments, context)[1] is None:
        if action == "unseal" and not confirm(f"Unseal {len(arguments.jobs)} job(s)?", yes=arguments.yes):
            return 1
        if getattr(arguments, "no_copy", False):
            raise ValueError("--no-copy is not carried to a remote workspace")
        return _forward(arguments, context, action)
    _refuse_request_id(arguments)
    workspace = _modifiable(arguments, context, action=f"{action} jobs in it")
    index = _location_index(workspace)
    refs = _resolve_selectors(workspace, context.cwd, arguments.jobs, index)
    if action == "unseal" and not confirm(f"Unseal {len(refs)} job(s)?", yes=arguments.yes):
        return 1
    copy = not getattr(arguments, "no_copy", False)
    reason = getattr(arguments, "reason", None) or f"httk job {action}"
    # Found before any request moves a job; a string stands in for an ancestor that cannot be followed.
    ancestors = _ancestors(workspace, refs, index) if action == "unlock" else []
    failed: list[bool] = []
    with _kernel.register_owner(workspace, kind="cli", label=f"job {action}", allocation=None, advertised={}) as owner:
        _request_jobs(
            workspace,
            owner,
            refs,
            action,
            reason,
            copy=copy,
            index=index,
            report=lambda outcomes: failed.extend(_print_now(outcome, done) for outcome in outcomes),
        )
        failed.append(_unlock_ancestors(workspace, owner, ancestors, reason, index))
    return 1 if any(failed) else 0


def _forward(arguments: argparse.Namespace, context: CLIContext, action: str, **options: object) -> int:
    """Send the jobs of a dedicated verb on a remote workspace as *action* requests, as the request verbs do.

    :param arguments: The verb's arguments: ``workspace`` and ``jobs``, and ``operator``, ``reason``,
        ``adapter_timeout``, ``request_id`` and ``no_durable`` when it takes them.
    :param context: The invocation context.
    :param action: The request action.
    :param **options: Request arguments of the action, such as an eject's ``destination``.
    :return: The exit status of :func:`_post_requests`.
    """

    names = ("workspace", "operator", "adapter_timeout", "request_id")
    request = {name: getattr(arguments, name, None) for name in names}
    request["no_durable"] = getattr(arguments, "no_durable", False)
    reason = getattr(arguments, "reason", None) or f"httk job {action}"
    return _post_requests(
        argparse.Namespace(**request, **options, action=action, job_id=list(arguments.jobs), reason=reason), context
    )


def _daemon_binding(binding: WorkspaceBinding, context: CLIContext) -> bool:
    """Whether a remote workspace binding is reached through a workspace daemon (a ``mount-daemon`` remote)."""

    try:
        target = resolve_remote(binding.remote, project=context.cwd)
    except ResolutionMiss:
        return False  # the path the binding then takes resolves it again and reports the miss
    return read_metadata(target.bundle).get("kind") == "mount-daemon"


def _print_now(outcome: RequestOutcome, done: str, mark: str = "") -> bool:
    """Print the line of one request applied now where the job is unowned; whether it failed."""

    if outcome.status == "applied":
        print(f"{outcome.job_id}\t{done}{mark}")
        return False
    print(f"{outcome.job_id}\t{outcome.status}\t{outcome.explanation}{mark}")
    return outcome.status != "queued"


def _unlock_ancestors(
    workspace: Workspace,
    owner: _kernel.Owner,
    ancestors: Sequence[JobRef | str],
    reason: str,
    index: LocationIndex,
) -> bool:
    """Post a marker-only ``unlock`` for each of :func:`_ancestors`, printing its lines; whether one failed.

    Each line is printed in order as soon as its chunk is served (on an interrupt, those served so far), with the
    lines of ancestors that cannot be followed in their places.
    """

    refs = [ancestor for ancestor in ancestors if not isinstance(ancestor, str)]
    pending = iter(ancestors)
    failed: list[bool] = []

    def report(outcomes: Sequence[RequestOutcome]) -> None:
        for outcome in outcomes:
            # Print the lines that come before this ancestor, then its own.
            for ancestor in pending:
                if isinstance(ancestor, str):
                    print(ancestor)
                elif ancestor.job_id == outcome.job_id:
                    failed.append(_print_now(outcome, "unlocked", " (ancestor)"))
                    break

    try:
        _request_jobs(workspace, owner, refs, "unlock", reason, copy=False, index=index, report=report)
    except BaseException:
        # The lines before the ancestor that was being served when the command stopped.
        for ancestor in pending:
            if not isinstance(ancestor, str):
                break
            print(ancestor)
        raise
    for ancestor in pending:
        if isinstance(ancestor, str):
            print(ancestor)
    return any(failed)


def _ancestors(workspace: Workspace, refs: Sequence[JobRef], index: LocationIndex | None = None) -> list[JobRef | str]:
    """Return every ancestor of *refs* in this workspace, nearest first, without repeats or selected jobs.

    A parent that cannot be followed (not in this workspace, or behind an unreadable ``job.json``) ends
    its chain with a warning line in its place, and an ancestor already unlocked is a line too. A detached job
    still names the parent its data came from.
    """

    seen = {ref.job_id for ref in refs}
    found: list[JobRef | str] = []
    for ref in refs:
        child: JobRef | None = ref
        while child is not None:
            try:
                parent = JobDefinition.from_path(child.path / "job.json").parent
            except (FormatError, OSError) as exc:
                found.append(f"{child.job_id}\tjob.json unreadable ({exc}); its ancestors are not unlocked")
                break
            if parent is None or (job_id := str(parent["job_id"])) in seen:
                break
            seen.add(job_id)
            # By job id alone, as Attempt.parent does: a tree transferred here keeps its origin's workspace_id.
            hint = PurePosixPath(str(parent["placement"]))
            if index is not None:
                ancestor = index.locate(job_id, placement_hint=hint)
            else:
                ancestor = _kernel.locate(workspace, job_id, placement_hint=hint)
            if ancestor is None:
                found.append(f"{child.job_id}\tparent {parent['job_key']} not found; not unlocked")
            elif (doc := read_state_unowned(ancestor.path / "state.json")[0]) is not None and doc.unlocked is not None:
                # Already what this asks for (an unlock request would be refused): walked past, not requested.
                found.append(f"{ancestor.job_id}\talready unlocked (ancestor)")
            else:
                found.append(ancestor)
            child = ancestor
    return found


def handle_job_seal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Seal succeeded jobs that carry no seal, with the workspace's seal keys (a repair verb)."""

    return _request_now(arguments, context, "seal", "sealed")


def handle_job_unseal(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Release succeeded jobs from their seal protection, after confirmation, so they may be deleted."""

    return _request_now(arguments, context, "unseal", "unsealed")


def handle_job_detach(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Make spawned jobs independent of their parents, permanently."""

    return _request_now(arguments, context, "detach", "detached")


def handle_job_unlock(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Put jobs, and marker-only every ancestor of them, in debug mode (one-way), copying their runners."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        if _daemon_binding(binding, context):
            raise ValueError("the workspace daemon does not carry unlock; run it on the daemon host")
    return _request_now(arguments, context, "unlock", "unlocked")


def handle_job_eject(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Move a quiescent job (with ``--tree``, its terminal or paused descendants too) out into a bundle.

    With ``--hold`` the bundle goes to the workspace's own ``transfers/outgoing/<transfer id>`` (the first leg of
    ``job move``), and DEST, when given, is only recorded as the transfer's destination. A job of a remote
    workspace is ejected by an eject request there (:func:`_forward`): through a workspace daemon into DEST under
    its exchange outbox (the outbox itself without DEST), over ssh into DEST on the far side.
    """

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        local = (arguments.hold, arguments.destination_id, arguments.tree, arguments.wait, arguments.json)
        if any(local) or arguments.timeout is not None:
            raise ValueError(
                "--tree, --wait, --timeout and --json apply to a local workspace (a workspace daemon ejects the "
                "whole tree)"
            )
        target = resolve_remote(binding.remote, project=context.cwd)  # an unknown remote is reported as such
        if arguments.destination is None and read_metadata(target.bundle).get("kind") != "mount-daemon":
            raise ValueError("job eject needs DEST, the directory on the remote to put the bundle in")
        arguments.jobs = [arguments.job]
        return _forward(arguments, context, "eject", destination=arguments.destination)
    _refuse_request_id(arguments)
    if arguments.timeout is None:
        arguments.timeout = 600.0
    if arguments.timeout < 0:
        raise ValueError("--timeout must not be negative")
    if arguments.hold and arguments.operator is not None:
        raise ValueError("--operator needs a destination, not --hold")
    if arguments.hold:
        owner_key = None
    elif arguments.operator is not None:
        identity = _resolve_request_identity(arguments.operator)
        if identity.seed_path is None:
            raise ValueError(f"identity {identity.label} has no signing key")
        ensure_identity_key(identity)
        owner_key = identity_public_key(identity.seed_path)
        if owner_key is None:
            raise ValueError(f"identity {identity.label} has no signing key")
    else:
        configured = configured_operator_identity()
        owner_key = (
            identity_public_key(configured.seed_path)
            if configured is not None and configured.seed_path is not None
            else None
        )
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
                        owner_key=owner_key,
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
    """Move ejected bundles into the workspace in turn, publishing their jobs into the states they left.

    One filesystem moves a bundle in; across filesystems it is copied, and with ``--move`` the source is
    removed once adopted (for a bundle that is the caller's own to remove, such as a fetched exchange return).
    A refused bundle does not stop the ones after it. Afterwards, a bundle's parent directory that is a direct
    child of this workspace's ``transfers/incoming/`` (an earlier version's per-batch landing) is removed when empty.
    """

    workspace = _modifiable(arguments, context, action="adopt jobs into it")
    sources = [context.cwd / Path(bundle).expanduser() for bundle in arguments.bundles]
    stack = ExitStack()
    try:
        owner = stack.enter_context(
            _kernel.register_owner(workspace, kind="cli", label="job adopt", allocation=None, advertised={})
        )
        # One wait of the visibility deadline for every bundle's absent jobs, not one a bundle.
        settled = _moving.settle_absent(workspace, sources)
    except _ERRORS:
        stack.close()
        raise
    unsynced: str | None = None
    with stack:
        # Operators' bundles, written before httk ran: one sync for the batch before anything is taken. Its error
        # cannot be told apart between them, so it fails every bundle of the batch (a bundle it cannot reach is
        # skipped: its own take reports it).
        try:
            _moving.sync_operator_bundles(workspace, sources)
        except _fs.SyncFailed as exc:
            unsynced = exc.strerror
        except _ERRORS as exc:
            unsynced = f"not taken: the bundles could not be synced to disk: {exc}"
        # Outside the try above: an unexpected error after bundles were adopted prints no "refused" document.
        entries = _adopt_sources(
            workspace,
            owner,
            sources,
            arguments.bundles,
            settled,
            delivered=False,
            move=arguments.move,
            json_output=arguments.json,
            unsynced=unsynced,
        )
    _prune_landings(workspace, {source.parent.resolve() for source in sources})
    if arguments.json:
        report: dict[str, object] = {"bundles": entries, "workspace_id": workspace.workspace_id}
        print(json.dumps(report, indent=2))
    return _adoption_status(entries)


def _refusal(exc: Exception, workspace: Workspace | None) -> dict[str, object]:
    """The refusal document of a stream refused whole by the transfer session's ``stage`` op (nothing of it
    adopted), its ``job adopt:`` line printed to standard error.

    :param exc: Why.
    :param workspace: The adopting workspace, when it was resolved.
    :return: ``{"bundles": [], "workspace_id", "refused"}``, with ``bundle``, ``stream_aborted`` and ``sync_failed``
        when they apply: the member's bundle the extraction refused; the sender's stream incomplete (aborted by
        its sender, or cut); the sync barrier's commit failed.
    """

    print(f"job adopt: {exc}", file=sys.stderr)
    workspace_id = None if workspace is None else workspace.workspace_id
    refusal: dict[str, object] = {"bundles": [], "workspace_id": workspace_id, "refused": str(exc)}
    if isinstance(exc, StreamRefused) and exc.bundle is not None:
        refusal["bundle"] = exc.bundle
    if isinstance(exc, StreamRefused) and exc.stream_aborted:
        # The sender's own stream was incomplete (it aborted it, or the input ended): no verdict on the bundles.
        refusal["stream_aborted"] = True
    if isinstance(exc, _CommitFailed):
        # Only for the commit's writeback error: the sender delivers once more with --sync fsync.
        refusal["sync_failed"] = True
    return refusal


def stage_stream(
    resolve: Callable[[], Workspace],
    stream: IO[bytes],
    *,
    force_fsync: bool = False,
    finish: Callable[[], object] | None = None,
    progress: _landing.Progress | None = None,
    primed_devices: set[int] | None = None,
    defer_commit: bool = False,
) -> tuple[dict[str, object], _moving.Stage | None]:
    """Stage a tar stream of bundles for a later adoption (plan WORKFLOW_TRANSFER_SESSION §7.10, 1a): the
    transfer session's ``stage`` op.

    The stage's own owner is registered first; the stream is received into its ``landing`` scratch as
    ``outgoing/<name>`` (:func:`httk.workflow._landing.receive_tar`: the terminal validated, then the sync
    commit), and the first scan of the settle is taken (:meth:`~httk.workflow._moving.Stage.commit`). Until the
    commit any failure refuses the whole stream (after the landing is discarded and the owner closed), with the
    refusal document of :func:`_refusal`; an error after it escapes (the far side fails, its owner left for
    recovery, which discards the landing). With *defer_commit* (plan WORKFLOW_TRANSFER_SESSION §7.11, T6c-lite)
    the landing's sync barrier is left open, owned by the stage, and committed only by :func:`sync_stage` before
    its adoption: the stage is a refusal until its writes have quiesced and its terminal is validated, and its
    first scan, taken before the commit, is fatal as without it.

    :param resolve: Returns the adopting workspace (checked to be modifiable); its error refuses the stream.
    :param stream: The binary tar stream.
    :param force_fsync: ``--sync fsync``: fsync every landed file as it is written.
    :param finish: Called once the stream is extracted, before its commit (see
        :func:`~httk.workflow._landing.receive_tar`).
    :param progress: Told the phases of :func:`~httk.workflow._landing.receive_tar`, then ``settle``.
    :param primed_devices: The transfer session's filesystems primed already (see
        :func:`~httk.workflow._landing.receive_tar`).
    :param defer_commit: Leave the landing's sync commit to :func:`sync_stage` (nothing deferred in a non-durable
        workspace, or under ``--sync fsync``, whose commit has nothing left to do).
    :return: ``({"bundles": names, "workspace_id"}, stage)``, the names the stream carried sorted; or the refusal
        document and ``None``.
    """

    workspace: Workspace | None = None
    stage: _moving.Stage | None = None
    try:
        workspace = resolve()
        stage = _moving.Stage(
            workspace, _kernel.register_owner(workspace, kind="cli", label="job adopt", allocation=None, advertised={})
        )
        stage.landing = stage.owner.scratch("landing")
        landing = stage.landing / "outgoing"
        owned = stage

        def hand(deferred: _landing.DeferredCommit) -> None:
            owned.deferred = deferred  # the stage owns the open barrier: abandoning it closes the descriptor

        bundles = _landing.receive_tar(
            workspace,
            landing,
            stream,
            barrier=True,
            force_fsync=force_fsync,
            finish=finish,
            progress=progress,
            primed_devices=primed_devices,
            defer_commit=hand if defer_commit and not force_fsync else None,
        )
    except _ERRORS as exc:
        if stage is not None:
            stage.abandon()  # its landing discarded, its owner closed: nothing of the stream is left
        return _refusal(exc, workspace), None
    if progress is not None:
        progress("settle", None)
    stage.commit([landing / name for name in bundles])
    return {"bundles": bundles, "workspace_id": workspace.workspace_id}, stage


def sync_stage(stage: _moving.Stage, *, progress: _landing.Progress | None = None) -> dict[str, object] | None:
    """Commit a staged landing's deferred sync barrier (:meth:`~httk.workflow._moving.Stage.sync`) before its
    adoption (plan WORKFLOW_TRANSFER_SESSION §7.11, T6c-lite): the transfer session's ``adopt_staged`` op, first.

    A failed ``syncfs`` commit under ``io.sync_errors`` ``fail`` (:class:`~httk.workflow._landing.CommitFailed`,
    the expected durability failure) abandons the stage (its landing discarded, its descriptor and owner closed)
    and answers the refusal document of :func:`_refusal`, with ``sync_failed``: nothing of the stage was adopted,
    and the sender delivers it once more with ``--sync fsync``. A stage without a deferred commit has nothing to do.

    :param stage: The committed stage.
    :param progress: Told ``sync`` before the commit (a phase without a stall bound).
    :return: ``None`` once the landing is durable (or was already); else the refusal document.
    :raises httk.workflow._fs.SyncFailed: For any other failure of the commit (under ``ignore``, an unsupported
        ``syncfs``), the stage left as it is (its owner for recovery, which discards the landing).
    :raises Exception: Any error abandoning the stage after a failed commit (nothing of it adopted; its owner left for
        recovery).
    """

    if stage.synced:
        return None
    if progress is not None:
        progress("sync", None)
    try:
        stage.sync()
    except _CommitFailed as exc:
        stage.abandon()  # disposed of before the refusal is answered; an error here escapes
        return _refusal(exc, stage.workspace)
    return None


def adopt_stage(
    stage: _moving.Stage, *, progress: _landing.Progress | None = None
) -> tuple[dict[str, object], int, Exception | None]:
    """Adopt a committed stage (:func:`stage_stream`) once its visibility deadline has run: the transfer session's
    ``adopt_staged`` op.

    Its landing must be durable already (:func:`sync_stage` first, after a deferred commit). It waits out what is
    left of the deadline (``settle``), takes the final scan and adopts every bundle as
    ``job adopt`` does, except that a bundle bound for another workspace is refused and discarded, then closes the stage.
    An error of the adoption escapes (the far side fails; its adoption is left to recovery). An error closing the
    stage after its adoption (its landing's discard, its owner's close) does not change the adoption: it is
    returned, with a ``warning:`` line on standard error, and the stage is left for one more attempt to close it
    (:meth:`~httk.workflow._moving.Stage.close`), then for recovery (plan WORKFLOW_TRANSFER_SESSION §7.11, T6a);
    except that a lost owner (:class:`~httk.workflow._kernel.OwnerLost`) stays fatal.

    :param stage: The committed stage.
    :param progress: Told ``settle``, then ``adopt`` with the bundles adopted.
    :return: The ``job adopt --json`` document (``bundles``, ``workspace_id``), its exit status, and the error that
        closing the stage met, if any.
    """

    workspace = stage.workspace
    if progress is not None:
        progress("settle", None)
    settled = stage.begin()
    sources = list(stage.bundles)
    entries = _adopt_sources(
        workspace,
        stage.owner,
        sources,
        [source.name for source in sources],
        settled,
        delivered=True,
        json_output=True,
        progress=progress,
    )
    failure: Exception | None = None
    try:
        stage.close()
    except _kernel.OwnerLost:  # OwnerDeclaredDead included
        raise  # fail-stop: this owner is no longer this process's
    except _ERRORS as exc:
        # The adoption is complete: what is left is cleanup, which recovery can do.
        print(f"warning: the stage could not be closed after its adoption (left for cleanup): {exc}", file=sys.stderr)
        failure = exc
    return {"bundles": entries, "workspace_id": workspace.workspace_id}, _adoption_status(entries), failure


def _adoption_status(entries: Sequence[Mapping[str, object]]) -> int:
    """The exit status of ``job adopt``: 0 when every bundle was adopted (or already was), else 1."""

    return 0 if all(entry["status"] in ("adopted", "already_adopted") for entry in entries) else 1


def _adopt_sources(
    workspace: Workspace,
    owner: _kernel.Owner,
    sources: Sequence[Path],
    bundles: Sequence[str],
    settled: _moving.Settled,
    *,
    delivered: bool,
    move: bool = False,
    json_output: bool,
    unsynced: str | None = None,
    progress: _landing.Progress | None = None,
) -> list[dict[str, object]]:
    """Adopt each source in turn (up to ``io.workers`` at once, related ones in order), printing each one's human
    lines; return the ``job adopt --json`` entries in order.

    :param workspace: The adopting workspace.
    :param owner: The command's owner there.
    :param sources: The bundle directories.
    :param bundles: Their names as the entries report them.
    :param settled: The batch's settled absences.
    :param delivered: The sources are a delivered stream's: one bound for another workspace is refused and
        discarded.
    :param move: ``--move``: discard a copied source once adopted.
    :param json_output: ``--json``: no human lines on standard output.
    :param unsynced: Why the batch could not be synced: every bundle then fails with it.
    :param progress: Told ``("adopt", n)`` with the number of bundles done so far.
    :return: The entries.
    """

    entries: list[dict[str, object]] = []
    if progress is not None:
        progress("adopt", 0)

    def adopt(index: int) -> tuple[dict[str, object], list[tuple[bool, str]]]:
        source, bundle = sources[index], bundles[index]
        if unsynced is not None:
            return {"bundle": bundle, "status": "failed", "message": unsynced}, [(True, f"{bundle}: {unsynced}")]
        foreign = _foreign_bundle(workspace, source) if delivered else None
        if foreign is None:
            return _adopt_one(workspace, owner, source, bundle, settled.absent, move=move, json_output=json_output)
        _fs.discard(_fs.loc(source), trash_dir=workspace.control / "tmp", durable=workspace.durable)
        return {"bundle": bundle, "status": "refused", "message": foreign}, [(True, f"{bundle}: {foreign}")]

    def show(_index: int, adopted: tuple[dict[str, object], list[tuple[bool, str]]]) -> None:
        entry, lines = adopted
        for to_stderr, line in lines:
            print(line, file=sys.stderr if to_stderr else sys.stdout)
        entries.append(entry)
        if progress is not None:
            progress("adopt", len(entries))

    serial = {index for index, source in enumerate(sources) if source in settled.serial}
    run_batch(owner, adopt, range(len(sources)), workspace.io_workers, serial=serial, emit=show)
    return entries


def _prune_landings(workspace: Workspace, parents: set[Path]) -> None:
    """Remove each of *parents* that is a direct child of ``transfers/incoming/`` (a landing) and now empty."""

    incoming = (workspace.control / "transfers" / "incoming").resolve()
    for parent in parents:
        if parent.parent == incoming:
            try:
                _fs.remove_empty_dir(_fs.loc(parent))  # kept while a bundle stayed in it
            except OSError:
                pass


def _foreign_bundle(workspace: Workspace, bundle: Path) -> str | None:
    """Why a delivered bundle may not be adopted here: it records another destination workspace; else ``None``."""

    try:
        read = read_manifest(bundle / "bundle.json")
    except BundleError:
        return None  # the adoption refuses it with the reason
    recorded = None if read is None else read[1].destination_workspace_id
    if recorded is None or recorded == workspace.workspace_id:
        return None
    return f"the bundle is bound for workspace {recorded}, not this workspace {workspace.workspace_id}"


def _built_for(workspace: Workspace, workflow_ids: Sequence[str]) -> list[dict[str, object]]:
    """Report each id as the transfer session's ``workflows`` op answers it: ``present``, ``unbuilt`` or
    ``missing``."""

    rows: list[dict[str, object]] = []
    for workflow_id in workflow_ids:
        found = _store.lookup(workspace, workflow_id)
        if found is None:
            rows.append({"id": workflow_id, "status": "missing", "tree_sha256": None})
            continue
        digest = found.record.get("tree_sha256")
        status = "unbuilt" if _store.built(found) is False else "present"
        rows.append({"id": workflow_id, "status": status, "tree_sha256": digest if isinstance(digest, str) else None})
    return rows


def _adopt_one(
    workspace: Workspace,
    owner: _kernel.Owner,
    source: Path,
    bundle: str,
    absent: frozenset[str],
    *,
    move: bool,
    json_output: bool,
) -> tuple[dict[str, object], list[tuple[bool, str]]]:
    """Adopt one bundle of ``job adopt`` (*absent* settled for the batch): its entry, and its human output lines
    (``True`` for standard error), which the caller prints; *move* and *json_output* are ``--move`` and ``--json``."""

    try:
        # Synced already: a committed stage's landing, or the batch's operator bundles (handle_job_adopt).
        report = _moving.adopt(workspace, owner, source, untrusted=False, absent=absent, synced=True)
    except _ERRORS as exc:
        status = "refused" if isinstance(exc, BundleError) else "failed"
        return {"bundle": bundle, "status": status, "message": str(exc)}, [(True, f"{bundle}: {exc}")]
    if report is None:
        message = "another actor took the bundle first"
        return {"bundle": bundle, "status": "taken", "message": message}, [(True, f"{bundle}: {message}")]
    if move and report.copied:
        _fs.discard(_fs.loc(source), trash_dir=source.parent, durable=workspace.durable)
    lines: list[tuple[bool, str]] = []
    if report.missing_workflows:
        missing = ", ".join(report.missing_workflows)
        lines.append((True, f"warning: these workflows are not installed, so their jobs wait: {missing}"))
    if report.already_adopted and not json_output:
        lines.append((False, f"{bundle}: already adopted"))
    elif not json_output:
        lines += [(False, f"{ref.job_key}\t{ref.state}\t{ref.path}") for ref in report.published]
    status = "already_adopted" if report.already_adopted else "adopted"
    return {"bundle": bundle, "status": status, **adopt_document(report)}, lines


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


def handle_job_verify(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Verify the seals of job payloads against their files and signatures, as ``workspace verify`` does a workspace.

    A JOB that is a directory holding ``job.json`` is verified where it is, inside a workspace or free-standing;
    any other JOB is a selector in the workspace. Prints one verdict line per job and ``ok``, ``UNTRUSTED`` or
    ``FAILED``; exits 0 when every seal is valid and trusted, 3 when one is valid but signed by an unknown key, 1
    otherwise (an unsealed job is invalid).
    """

    from ..seals import verify_tree
    from ._seal import _default_trusted_keys, render_seal_entries

    workspace: Workspace | None = None
    payloads: list[Path] = []
    for selector in arguments.jobs:
        directory = (context.cwd / Path(selector).expanduser()).resolve()
        if (directory / "job.json").is_file():
            payloads.append(directory)
            continue
        if workspace is None:
            workspace = Workspace(_local_root(arguments, context, action="verify job seals"))
        payloads.extend(ref.path for ref in resolve_job_selectors(workspace, context.cwd, [selector]))
    entries = [
        entry
        for payload in payloads
        for entry in verify_tree(
            payload, trusted_keys=_default_trusted_keys(payload, list(arguments.trusted_key)), content=arguments.content
        ).entries
    ]
    return render_seal_entries(entries, as_json=arguments.json)


def handle_job_precheck(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Report pending-job environment and runner readiness without mutation (:func:`._precheck.handle_precheck`)."""

    from ._precheck import handle_precheck  # imported on use: a job verb started remotely stays lean

    return handle_precheck(arguments, context)


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


def _add_operator_option(parser: argparse.ArgumentParser) -> None:
    """Add ``--operator``, the identity that signs a command's requests."""

    parser.add_argument(
        "--operator", metavar="IDENTITY", help='configured identity short name or a literal "Name <email>"'
    )


def _add_request_id_option(parser: argparse.ArgumentParser) -> None:
    """Add ``--request-id``, the daemon command id of one mount-daemon job request, for exact retries."""

    parser.add_argument(
        "--request-id",
        metavar="ID",
        help="32 lowercase hexadecimal daemon command ID for one mount-daemon job request and exact retries",
    )


def _budget_limit(text: str) -> int | None:
    """Parse one ``set_budget`` limit: an integer of at least 1, or ``unlimited`` (``None``)."""

    if text == "unlimited":
        return None
    if not text.isdecimal() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"expected an integer of at least 1 or 'unlimited', not {text!r}")
    return int(text)


#: The ``job set`` keys, in the order one call posts their requests, with the request action each belongs to.
_SET_KEYS = {
    "workflow": "set_workflow",
    "budget.max_attempts_per_activation": "set_budget",
    "budget.max_total_attempts": "set_budget",
    "budget.max_activations": "set_budget",
    "priority": "set_priority",
    "step": "override_step",
}


def _set_word(word: str) -> str | tuple[str, object]:
    """Parse one ``job set`` positional: a ``KEY=VALUE`` pair with its value, or a job selector.

    A word is a pair when it holds ``=`` and its key is a ``job set`` key (or starts with ``budget.``); a word with
    ``=`` and ``/`` but another key is a path selector; any other word with ``=`` is an unknown key.
    """

    key, separator, text = word.partition("=")
    if not separator or (key not in _SET_KEYS and not key.startswith("budget.") and "/" in word):
        return word  # a selector; a path may hold = in a placement
    if key not in _SET_KEYS:
        raise argparse.ArgumentTypeError(f"unknown key {key!r} in {word!r} (valid keys: {', '.join(_SET_KEYS)})")
    if key == "priority":
        try:
            return key, int(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"priority must be an integer, not {text!r}") from None
    if key.startswith("budget."):
        return key, _budget_limit(text)
    if key == "step":
        return key, _step_name(text)
    if not text:
        raise argparse.ArgumentTypeError(f"{key} needs a value")
    return key, text


def _add_request_options(parser: argparse.ArgumentParser) -> None:
    """Add the per-action request options of the hidden ``request-envelopes`` leaf."""

    parser.add_argument("--priority", type=int, metavar="PRIORITY", help="the new priority, for set_priority")
    parser.add_argument("--override-step", metavar="STEP", help="the step to resume at, for override_step")
    parser.add_argument(
        "--workflow",
        metavar="WORKFLOW",
        help=(
            "for set_workflow: the installed workflow (id, short name or NAME@REF) the jobs run from their next "
            "attempt; it must share the job's declaration and its inputs, outputs, parameters, environment and step"
        ),
    )
    parser.add_argument(
        "--destination",
        metavar="DEST",
        help="where the job goes, for eject; for a mount-daemon remote, a directory in its outbox (default: the outbox)",
    )
    for name, option in _BUDGET_OPTIONS.items():
        parser.add_argument(
            option,
            dest=name,
            type=_budget_limit,
            default=argparse.SUPPRESS,
            metavar="N|unlimited",
            help="for set_budget: the new limit, or unlimited",
        )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "accept reviving a job a decided join already consumed (continue, restart, override_step), "
            "and restart a job whose failed or cancelled children its rerun spawning step would reuse"
        ),
    )
    parser.add_argument(
        "--keep-budget",
        action="store_true",
        help=(
            "revive without restoring the declared retry allowance (continue, restart, override_step); "
            "by default a revival raises the job's retry limits so it again has its declared allowance"
        ),
    )


class _WorkflowLabels:
    """The registered workflows ``job new SOURCE`` lists, discovered when its help is rendered."""

    def __str__(self) -> str:
        labels = registered_workflow_labels()
        return f" ({', '.join(labels)})" if labels else ""


def _build_new_parser(group: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    new = _leaf(
        group,
        "new",
        summary="scaffold and submit jobs of a workflow",
        description=(
            "Scaffold and submit jobs of SOURCE, told apart as httk workflow install tells its kinds apart: a "
            "runner file, workflow document or package directory (httk_workflow.toml) where it exists, else a git "
            "URI, else an installed id, short name or NAME@REF, or a registered workflow name; a directory without "
            "httk_workflow.toml is a name unless --language names a directory format. A name must be installed in "
            "the workspace (httk workflow install) unless --install installs it first; a runner file, document or "
            "--command is installed ad hoc. --campaign assigns each job to a partition of this project's campaign "
            "and submits it into that partition's workspace"
        ),
        handler=handle_job_new,
    )
    _add_workspace_option(new, help_text="the workspace to submit into")
    source = new.add_argument(
        "source",
        nargs="?",
        metavar="SOURCE",
        help="the workflow: a runner file, workflow document, package directory, git+https://HOST/PATH[@REF][#SUBDIR]"
        " URI, installed id, short name or NAME@REF, or a registered or packaged workflow name",
    )
    # argparse expands ``%(workflow_labels)s`` from the action's attributes only when this help is printed, so
    # every other job verb skips discovering the installed workflows. It is appended after add_argument, whose
    # help-string check (Python 3.14) cannot see the attribute yet.
    vars(source)["workflow_labels"] = _WorkflowLabels()
    source.help = f"{source.help}%(workflow_labels)s"
    new.add_argument(
        "--command",
        dest="command_template",
        metavar="TEMPLATE",
        help=(
            "instead of SOURCE, generate a one-step Bash runner from an argv-only TEMPLATE, installed as "
            "adhoc:command@<digest>; {name} substitutes a --parameter or staged --file path, {{ and }} are literal "
            "braces"
        ),
    )
    new.add_argument(
        "--campaign",
        action="store_true",
        help="assign each job to a partition of this project's campaign and submit it into that partition's "
        "workspace (hash assignment hashes the job's --tag or input-derived tag, not one an instantiate hook "
        "derives: give such a workflow --tag or --partition; round-robin numbers the jobs of the batch)",
    )
    new.add_argument(
        "--partition",
        metavar="NAME",
        help="with --campaign: submit into this partition (required by explicit assignment, a pin otherwise)",
    )
    new.add_argument(
        "--campaign-index",
        type=int,
        metavar="N",
        help="with --campaign and round-robin assignment: the position of the batch's first job (default: 0)",
    )
    new.add_argument(
        "--install",
        action="store_true",
        help="install the workflow (and its calls and bound call slot targets) into the workspace first when it "
        "is not installed",
    )
    new.add_argument(
        "--embed-workflow",
        action="store_true",
        help="embed the workflow in the job's workflow/ instead of using a workspace installation; "
        "nothing is installed",
    )
    new.add_argument(
        "--call",
        action="append",
        default=[],
        dest="calls",
        metavar="ALIAS=WORKFLOW",
        help="bind the workflow's call slot ALIAS to WORKFLOW instead of its default (repeatable)",
    )
    new.add_argument(
        "--call-parameter",
        action="append",
        nargs=2,
        default=[],
        dest="call_parameters",
        metavar=("ALIAS", "NAME=VALUE"),
        help="one parameter of the workflow bound to call slot ALIAS, spelled as for --parameter (repeatable)",
    )
    new.add_argument(
        "-p",
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
        "--language",
        dest="format",
        metavar="LANGUAGE",
        help="force LANGUAGE (cwl, pwd, jobflow, httk-v1) for a bare workflow document or directory",
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
        "-i",
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one declared input value to stage (repeatable)",
    )
    new.add_argument(
        "--source-root",
        metavar="DIR",
        help=(
            "record the path of each file given with --input or --input-from for a declared structures input, "
            "relative to DIR, which must contain them all (a wide DIR such as / records most of the absolute path); "
            "without it only the file's name, size and SHA-256 are recorded, never a path of this machine "
            "(httk collect --into stores the file and its import record either way)"
        ),
    )
    new.add_argument(
        "--input-move-from-job",
        action="append",
        default=[],
        dest="input_moves",
        metavar="NAME=PATH",
        help=(
            "move (not copy) PATH, a file inside a succeeded job of this workspace, to the declared input NAME's "
            "destination; a sealed job's seal is amended by a release record (repeatable)"
        ),
    )
    new.add_argument(
        "--input-from",
        action="append",
        nargs="+",
        default=[],
        metavar=("NAME", "SOURCE"),
        help="load a declared input from one or more files, or readable files in a directory (repeatable); it takes "
        "every word up to the next option, so give SOURCE before it (or after --)",
    )
    new.add_argument("--tag", metavar="TAG", help="the readable half of the job key (default: derived from an input)")
    new.add_argument(
        "--placement", metavar="PLACEMENT", default=DEFAULT_PLACEMENT, help="placement subtree (default: the jobs root)"
    )
    new.add_argument("--priority", type=int, metavar="PRIORITY", help="scheduling priority (default: the workflow's)")
    new.add_argument(
        "--start-step", dest="step", metavar="STEP", help="the step the job starts at (default: the workflow's own)"
    )
    new.add_argument(
        "--duplicate-tag",
        choices=("refuse", "skip", "allow"),
        default="refuse",
        help="what to do when a job with the same tag exists in the placement: refuse (error), skip it, "
        "or allow another (default: refuse)",
    )
    new.add_argument("--json", action="store_true", help="print one JSON report per job, as an array")


class _IntermixedParser(argparse.ArgumentParser):
    """A leaf whose optional positionals may follow its options.

    argparse stops filling a ``*`` positional at the first option, so ``job pause JOB --reason TEXT JOB2`` would
    leave ``JOB2`` unrecognized; intermixed parsing reads the options first.
    """

    _inner = False

    def parse_known_args(  # type: ignore[override]
        self, args: Sequence[str] | None = None, namespace: Any = None
    ) -> tuple[Any, list[str]]:
        """Parse intermixed options and positionals (the inner passes run the plain parser).

        :param args: The arguments, ``sys.argv[1:]`` by default.
        :param namespace: The namespace to fill, a new one by default.
        :return: The namespace and the unrecognized arguments.
        """

        if self._inner:
            return super().parse_known_args(args, namespace)
        self._inner = True
        try:
            return self.parse_known_intermixed_args(args, namespace)
        finally:
            self._inner = False


def _add_selection(leaf: argparse.ArgumentParser, *, words: str | None = None) -> None:
    """Add the job selection, operator, reason, request and durability options every request verb shares.

    :param leaf: The verb's parser.
    :param words: The positional's metavar and help when it takes more than job selectors (``job set``).
    """

    _add_workspace_option(leaf, help_text="the workspace holding the jobs")
    if words is None:
        leaf.add_argument(
            "job_id", metavar="JOB", nargs="*", help="job UUIDs, unique prefixes, or paths inside the workspace"
        )
    else:
        leaf.add_argument("words", metavar="JOB|KEY=VALUE", nargs="*", type=_set_word, help=words)
    leaf.add_argument(
        "-s",
        "--state",
        action="append",
        default=[],
        metavar="STATE",
        choices=_kernel.UNOWNED_STATES,
        help="also select every job in this state (repeatable; local workspaces)",
    )
    _add_step_option(
        leaf,
        help_text=(
            "only jobs at this step (repeatable; local workspaces): narrows JOB and --state, "
            "and alone selects from every unowned state"
        ),
    )
    leaf.add_argument(
        "--placement",
        metavar="PREFIX",
        help="with --state, or --step without JOB: only jobs under this placement prefix",
    )
    # add_parser takes no per-leaf parser class, so the leaf is given its intermixed class here.
    leaf.__class__ = _IntermixedParser
    _add_operator_option(leaf)
    leaf.add_argument(
        "--reason", metavar="TEXT", help="why, recorded with each request (default: the command, e.g. httk job pause)"
    )
    _add_request_id_option(leaf)
    _add_adapter_timeout(leaf)
    add_durability_arguments(leaf)


def _add_revival_options(leaf: argparse.ArgumentParser, *, what: str) -> None:
    """Add ``--force`` and ``--keep-budget``, the options of a revival (*what* names where they apply)."""

    leaf.add_argument(
        "--force",
        action="store_true",
        help=(
            f"{what}accept reviving a job a decided join already consumed, publish a step outside the runner's "
            "recorded steps, and restart a job whose failed or cancelled children its rerun spawning step would reuse"
        ),
    )
    leaf.add_argument(
        "--keep-budget",
        action="store_true",
        help=(
            f"{what}revive without restoring the declared retry allowance; by default a revival raises the job's "
            "retry limits so it again has its declared allowance"
        ),
    )


#: The request verbs: the request action each posts, and its summary.
_CONTROL_VERBS = {
    "cancel": ("cancel", "cancel jobs; a running attempt is stopped first"),
    "pause": ("pause", "pause jobs, at their next boundary when running"),
    "resume": ("continue", "resume failed or paused jobs"),
    "restart": ("restart", "rerun failed or paused jobs from a new attempt, dropping a retained join"),
}


def _build_request_parsers(group: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    for verb, (action, summary) in _CONTROL_VERBS.items():
        leaf = _leaf(
            group,
            verb,
            summary=summary,
            description=(
                f"{summary[0].upper()}{summary[1:]}: one {action} request per job, applied at once when no owner "
                "holds the job, otherwise by the owner at its next boundary"
            ),
            handler=handle_job_control,
        )
        leaf.set_defaults(action=action, verb=verb)
        _add_selection(leaf)
        if verb == "pause":
            leaf.add_argument("--wait", action="store_true", help="wait until every job reaches paused")
            leaf.add_argument(
                "--timeout", type=float, metavar="SECONDS", help="stop waiting after SECONDS (needs --wait)"
            )
        if verb == "resume":
            leaf.add_argument(
                "--tree",
                action="store_true",
                help=(
                    "also resume the failed descendants of a job that failed waiting on its children, deepest first "
                    "(not over a mount-daemon binding)"
                ),
            )
        if verb in ("resume", "restart"):
            _add_revival_options(leaf, what="")

    setting = _leaf(
        group,
        "set",
        summary="change the workflow, retry budget, priority or step of jobs",
        description=(
            "Change jobs through requests: workflow=WORKFLOW (an installed id, short name or NAME@REF), "
            "budget.max_attempts_per_activation=N|unlimited, budget.max_total_attempts=N|unlimited, "
            "budget.max_activations=N|unlimited, priority=N (0 is highest) and step=STEP (resume at a new "
            "activation of STEP). One call posts one request per kind, in that order (the budget keys together "
            "form one request), stopping at the first that fails"
        ),
        handler=handle_job_set,
    )
    setting.set_defaults(verb="set")
    _add_selection(
        setting,
        words="job selectors and KEY=VALUE pairs (a word holding = is a pair): " + ", ".join(_SET_KEYS),
    )
    _add_revival_options(setting, what="with step=: ")

    envelopes = _leaf(
        group,
        "request-envelopes",
        description="Build unsigned operator request documents for a signing client",
        summary="build unsigned request documents",
        handler=handle_job_request_envelopes,
        hidden=True,
    )
    envelopes.add_argument("action", choices=_requests.ACTIONS, help="the request action")
    envelopes.add_argument("-w", "--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    envelopes.add_argument("job_id", metavar="JOB_ID", nargs="+", help="one or more job ids or unique prefixes")
    envelopes.add_argument("--operator", required=True, help="the operator attribution label")
    envelopes.add_argument("--reason", required=True, help="why the request is being made")
    _add_request_options(envelopes)
    envelopes.add_argument(
        "--tree", action="store_true", help="with continue: also the failed descendants, deepest first"
    )
    envelopes.add_argument("--json", action="store_true", required=True, help=argparse.SUPPRESS)

    publish = _leaf(
        group,
        "publish-requests",
        description="Post signed operator request documents from a signing client",
        summary="post signed request documents",
        handler=handle_job_publish_requests,
        hidden=True,
    )
    publish.add_argument("-w", "--workspace", metavar="WORKSPACE", required=True, help="the far-side workspace name")
    publish.add_argument(
        "--document", action="append", dest="documents", required=True, metavar="JSON", help="one request document"
    )
    publish.add_argument("--wait", action="store_true", help="wait for pause requests to reach paused")
    publish.add_argument("--timeout", type=float, metavar="SECONDS", help="stop waiting after SECONDS")
    publish.add_argument(
        "--sequential",
        action="store_true",
        help="post and serve continue requests one at a time, in order, stopping at one that does not resume",
    )
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
        "-s",
        "--state",
        action="append",
        metavar="STATE",
        choices=JOB_STATES,
        help=f"state to list (repeatable, default: every state: {', '.join(JOB_STATES)})",
    )
    listing.add_argument("--placement", metavar="PLACEMENT", help="prune the listing to this placement prefix")
    listing.add_argument("--limit", type=int, metavar="COUNT", help="return at most this many jobs")
    listing.add_argument("--after", metavar="CURSOR", help="resume after a <state>:<cursor> from next_after")
    listing.add_argument("--tag-contains", metavar="TEXT", help="list only jobs whose tag contains TEXT")
    _add_step_option(
        listing,
        help_text="list only jobs at this step: the current activation's, else the initial step (repeatable)",
    )
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
    _add_operator_option(delete)
    _add_request_id_option(delete)
    delete.add_argument("--yes", action="store_true", help="skip the confirmation")
    delete.add_argument("--confirmed", action="store_true", help=argparse.SUPPRESS)
    _add_adapter_timeout(delete)

    recover = _leaf(
        group,
        "recover",
        summary="return away copies to the states they left, to push them again",
        description=(
            "Return away jobs, the copies push (or job move --keep) kept here, to the states and priorities "
            "they left, each root with its away tree, so that a job lost where it went can be pushed again; "
            "nothing checks whether it still runs there, and a later return of it from there is refused"
        ),
        handler=handle_job_recover,
    )
    _add_workspace_option(recover, help_text="the workspace holding the away copies")
    recover.add_argument(
        "jobs",
        metavar="JOB",
        nargs="*",
        help="the root of an away tree: job UUID, job key, unique prefix, or a path inside the workspace",
    )
    recover.add_argument("--all", action="store_true", help="recover every away root")
    recover.add_argument(
        "--placement", metavar="PREFIX", help="with --all: only away roots under this placement prefix"
    )
    add_durability_arguments(recover)

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
        _add_operator_option(leaf)
        _add_request_id_option(leaf)
        _add_adapter_timeout(leaf)
        if name == "unseal":
            leaf.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    unlock = _leaf(
        group,
        "unlock",
        summary="lift a job's integrity policies for debugging (debug mode), permanently",
        description=(
            "Lift a job's integrity policies for debugging (debug mode), permanently: an unlock request, applied now "
            "to an unowned job, that copies the job's runner to its workflow/ directory; every ancestor of the job gets a "
            "marker-only unlock request too"
        ),
        handler=handle_job_unlock,
    )
    _add_job_selector(unlock)
    _add_operator_option(unlock)
    unlock.add_argument(
        "--no-copy", action="store_true", help="unlock without copying the runner into the job (local workspaces)"
    )
    unlock.add_argument(
        "--reason", metavar="TEXT", help="the reason recorded with the marker (default: httk job unlock)"
    )
    _add_adapter_timeout(unlock)

    eject = _leaf(
        group,
        "eject",
        summary="move a job (and with --tree its descendants) out of a workspace",
        description=(
            "Move a quiescent job out of the workspace into DEST/<job key>, a bundle that job adopt takes in; "
            "with --tree its terminal or paused descendants go along"
        ),
        handler=handle_job_eject,
    )
    _add_workspace_option(eject, help_text="the workspace holding the job")
    eject.add_argument("job", metavar="JOB", help="job UUID, job key, unique prefix, or a path inside the workspace")
    eject.add_argument(
        "destination",
        metavar="DEST",
        nargs="?",
        help=(
            "the directory to put the bundle in; for a workspace daemon, a directory under its exchange outbox "
            "(default: the outbox)"
        ),
    )
    eject.add_argument("--hold", action="store_true", help=argparse.SUPPRESS)
    _add_operator_option(eject)
    _add_request_id_option(eject)
    eject.add_argument("--destination-id", metavar="WORKSPACE_ID", help=argparse.SUPPRESS)
    eject.add_argument("--tree", action="store_true", help="eject the job's descendants too")
    eject.add_argument("--wait", action="store_true", help="pause a running job and wait until it can move")
    eject.add_argument(
        "--timeout", type=float, metavar="SECONDS", help="give up waiting after this long (default: 600)"
    )
    eject.add_argument("--json", action="store_true", help="print the result as one JSON document")
    _add_adapter_timeout(eject)
    add_durability_arguments(eject)
    adopt = _leaf(
        group,
        "adopt",
        summary="move an ejected bundle into a workspace",
        description="Move an ejected job bundle into the workspace, publishing its jobs into the states they left",
        handler=handle_job_adopt,
    )
    _add_workspace_option(adopt, help_text="the workspace to adopt into")
    _add_by_path_argument(adopt)
    adopt.add_argument(
        "bundles", metavar="BUNDLE", nargs="+", help="an ejected job bundle directory (several are adopted in turn)"
    )
    adopt.add_argument(
        "--move",
        action="store_true",
        help="remove the bundle once adopted when it had to be copied from another filesystem",
    )
    adopt.add_argument(
        "--json",
        action="store_true",
        help='print one JSON document, {"bundles": [...], "workspace_id": ...}, an entry per BUNDLE',
    )
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

    verify = _leaf(
        group,
        "verify",
        summary="verify job seals against their payloads",
        description=(
            "Verify each job's seal: its signatures and the digests of the files it seals; exits 0 when every seal "
            "is valid and trusted, 3 when one is signed by an unknown key, 1 when one is invalid or missing"
        ),
        handler=handle_job_verify,
    )
    _add_workspace_option(verify, help_text="the workspace holding the jobs")
    verify.add_argument(
        "jobs",
        metavar="JOB",
        nargs="+",
        help="job UUID, job key, unique prefix, path inside the workspace, or a job payload directory anywhere",
    )
    verify.add_argument(
        "--content",
        action="store_true",
        help="also decompress job files and check them against their sealed uncompressed.sha256 manifests",
    )
    verify.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="KEY_OR_FINGERPRINT",
        help=(
            "trust this key as well: an ed25519:BASE64 value, a sha256: fingerprint, or the path of a *.pub file "
            "(repeatable); the project's pinned keys and all local identities are always trusted"
        ),
    )
    verify.add_argument("--json", action="store_true", help="print the verdicts as one JSON document")

    precheck = _leaf(
        group,
        "precheck",
        summary="check pending jobs before they start",
        description=(
            "Report declared-environment resolution, workflow installation and build, and claimability "
            "of pending jobs without changing the workspace"
        ),
        handler=handle_job_precheck,
    )
    _add_workspace_option(precheck, help_text="the workspace to precheck")
    precheck.add_argument("--placement", metavar="PLACEMENT", help="check only jobs at or below this placement")
    precheck.add_argument("--json", action="store_true", help="print the complete report as JSON")

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
    debug.add_argument("--start-step", dest="step", metavar="STEP", help="initial step of a freshly submitted payload")
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
