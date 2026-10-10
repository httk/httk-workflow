"""Private command bridge used by the packaged Bash libraries.

The bridge is the language-agnostic half of the Bash authoring SDK: every Bash
function in ``languages/bash/httk-workflow.sh`` is one invocation of one subcommand here,
and every subcommand does its work through :class:`httk.workflow.Attempt` and
:class:`httk.workflow.OutcomeDraft`. A Bash runner and a Python runner therefore
publish the same bytes, because they publish through exactly one implementation.

A Bash runner is many short-lived processes, so the bridge holds no state of its
own between calls. The one implicit outcome draft of an attempt lives in the
attempt control directory as ``outcome.tmp.<uuid>``, and every bridge process
rediscovers and resumes it there: the spawned children and the implicit data
transaction ``put`` stages into are read back from the draft itself. An
explicit transaction (``transaction begin``) is named by its six-digit
sequence, which every later ``transaction put``/``commit`` passes back.

Exit codes are uniform across every subcommand:

``0``
    the call succeeded.
``1``
    the answer is legitimately absent — an unset state key, a missing parameter
    without a default, a null child field.
``2``
    the call is refused — bad usage, a protocol violation, or a corrupt attempt
    context.

The supervised-command subcommand additionally reports the classified outcome
of the program it ran: ``run`` returns ``124`` on timeout, ``125`` when a checker
or diagnostic stopped it, and ``22`` on any other nonzero exit. ``125`` is also
what ``httk.workflow._launcher`` reports for a runner it could not start at all.
The ``<code>-*`` subcommands of each installed code-support package (see
:mod:`httk.workflow.codes`) are mounted beside these and define their own
outcome codes.
"""

import argparse
import dataclasses
import json
import os
import shlex
import sys
from collections.abc import Mapping, Sequence
from functools import cache
from pathlib import Path
from types import ModuleType
from typing import cast

from . import _fs
from ._data import Transaction
from ._durations import TIME_RESOURCES
from ._state import _thaw
from ._util import json_bytes, read_json
from .codes import BRIDGE_ABSENT, installed_codes
from .errors import FormatError
from .models import normalize_resources, placement_text
from .runtime import _read_environment
from .runtime_builders import (
    JobSpec,
    JoinCondition,
    OutcomeDraft,
    ReplayableWorkdirBatch,
    prepare_job_payload,
)
from .runtime_utils import (
    compress_files,
    decompress_files,
    evaluate_expression,
    render_template,
)
from .sdk import _IMPLICIT_TRANSACTION, RUNNER_ERROR_FORMAT, Attempt, ChildSpec, Runner
from .supervision import CheckerSpec, ProcessSupervisor

ABSENT = BRIDGE_ABSENT
REFUSED = 2

RUNNER_WORKFLOW_VARIABLE = "HTTK_WORKFLOW_RUNNER_WORKFLOW"
RUNNER_STEPS_VARIABLE = "HTTK_WORKFLOW_RUNNER_STEPS"

_CHILD_FIELDS = (
    "label",
    "state",
    "job_id",
    "job_key",
    "failure_code",
    "failure_message",
    "payload",
    "workdir",
    "data",
)
_JOIN_CONDITIONS = ("all_succeeded", "all_terminal", "any_succeeded", "any_terminal", "at_least")


@cache
def _code_bridges() -> dict[str, ModuleType]:
    """Return the bridge module of every installed code, by subcommand prefix."""

    bridges: dict[str, ModuleType] = {}
    for code in installed_codes():
        try:
            bridges[f"{code.name}-"] = code.resolve_bridge()
        except Exception as exc:
            # One broken code package must not take every other bridge call down.
            print(f"httk-workflow: code {code.name!r} bridge {code.bridge!r} is unavailable: {exc}", file=sys.stderr)
    return bridges


# The fields `parent FIELD` reads; anything else is a usage error, not an absence.
_PARENT_FIELDS = ("workspace_id", "job_id", "job_key", "placement", "activation_id", "spawn_id", "payload", "workdir")


class _Absent(Exception):
    """A read whose answer is legitimately not there."""


class _Refused(Exception):
    """A call the bridge will not perform: bad usage or a protocol violation."""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workflow-shell-bridge")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("begin")
    commands.add_parser("batch")
    context = commands.add_parser("context")
    context.add_argument("field", nargs="?")
    parent = commands.add_parser("parent")
    parent.add_argument("field", nargs="?")
    job_input = commands.add_parser("parameter")
    job_input.add_argument("name")
    job_input.add_argument("--default")
    parameter_items = commands.add_parser("parameter-items")
    parameter_items.add_argument("name")
    parameter_items.add_argument("--default")
    parameter_items.add_argument("-0", "--null", action="store_true")
    stage_input = commands.add_parser("stage-input")
    stage_input.add_argument("name")
    stage_input.add_argument("destination")
    stage_input.add_argument("--default")
    setting = commands.add_parser("setting")
    setting.add_argument("name")
    setting.add_argument("--default")
    environment = commands.add_parser("environment")
    environment.add_argument("name")
    environment.add_argument("--default")
    state_get = commands.add_parser("state-get")
    state_get.add_argument("name")
    state_set = commands.add_parser("state-set")
    state_set.add_argument("name")
    state_set.add_argument("value")
    state_delete = commands.add_parser("state-delete")
    state_delete.add_argument("name")
    state_merge = commands.add_parser("state-merge")
    state_merge.add_argument("assignments", nargs="+")
    declare = commands.add_parser("declare")
    declare.add_argument("name")
    declare.add_argument("document")
    declaration = commands.add_parser("declaration")
    declaration.add_argument("name")
    runlog = commands.add_parser("runlog")
    runlog.add_argument("kind")
    runlog.add_argument("message")
    runlog.add_argument("files", nargs="*")
    commands.add_parser("environment-log")

    put = commands.add_parser("put")
    put.add_argument("source")
    put.add_argument("destination")
    transaction = commands.add_parser("transaction")
    transaction_verbs = transaction.add_subparsers(dest="verb", required=True)
    transaction_verbs.add_parser("begin")
    transaction_put = transaction_verbs.add_parser("put")
    transaction_put.add_argument("handle")
    transaction_put.add_argument("source")
    transaction_put.add_argument("destination")
    transaction_verbs.add_parser("commit").add_argument("handle")

    spawn = commands.add_parser("spawn")
    spawn.add_argument("label")
    spawn.add_argument("--step")
    spawn.add_argument("--payload")
    spawn.add_argument("--parameter", action="append", default=[], dest="parameters")
    spawn.add_argument("--placement")
    spawn.add_argument("--priority", type=int)
    spawn.add_argument("--tag")
    spawn.add_argument("--name")
    spawn.add_argument("--claim-pool")
    spawn.add_argument("--capability", action="append", default=[], dest="capabilities")
    spawn.add_argument("--retry-on", action="append", default=[], dest="retry_on")
    spawn.add_argument("--resources")
    spawn.add_argument("--step-resources")
    spawn.add_argument("--max-attempts-per-activation", type=int)
    spawn.add_argument("--max-total-attempts", type=int)
    spawn.add_argument("--max-activations", type=int)

    call = commands.add_parser("call")
    call.add_argument("label")
    call.add_argument("workflow")
    call.add_argument("--file", action="append", default=[], dest="files")
    call.add_argument("--input", action="append", default=[], dest="inputs")
    call.add_argument("--parameter", action="append", default=[], dest="parameters")
    call.add_argument("--environment", action="append", default=[], dest="environment")
    call.add_argument("--tag")
    call.add_argument("--name")
    call.add_argument("--placement")
    call.add_argument("--priority", type=int)
    call.add_argument("--step")

    children = commands.add_parser("children")
    selection = children.add_mutually_exclusive_group()
    selection.add_argument("--all", dest="selection", action="store_const", const="all")
    selection.add_argument("--succeeded", dest="selection", action="store_const", const="succeeded")
    selection.add_argument("--failed", dest="selection", action="store_const", const="failed")
    children.set_defaults(selection="all")
    child = commands.add_parser("child")
    child.add_argument("label")
    child.add_argument("field", choices=_CHILD_FIELDS)

    advance = commands.add_parser("advance")
    advance.add_argument("next_step")
    advance.add_argument("--state", action="append", default=[], dest="state")
    advance.add_argument("--priority", type=int)
    advance.add_argument("--resource", action="append", default=[], dest="resources")
    gather = commands.add_parser("gather")
    gather.add_argument("next_step")
    gather.add_argument("--when", choices=_JOIN_CONDITIONS, default="all_succeeded")
    gather.add_argument("--count", type=int)
    gather.add_argument("--on-impossible")
    gather.add_argument("--priority", type=int)
    gather.add_argument("--resource", action="append", default=[], dest="resources")
    commands.add_parser("succeed")
    fail = commands.add_parser("fail")
    fail.add_argument("code")
    fail.add_argument("message")
    fail.add_argument("--details")
    fail.add_argument("--retryable", action="store_true")
    fail.add_argument("--priority", type=int)
    retry = commands.add_parser("retry")
    retry.add_argument("reason")
    pause = commands.add_parser("pause")
    pause.add_argument("reason")
    commands.add_parser("fail-unknown-step")
    commands.add_parser("fail-no-outcome")
    abort = commands.add_parser("abort")
    abort.add_argument("--exception", default="ShellError")
    abort.add_argument("--message", default="")
    abort.add_argument("--traceback-file")

    job_prepare = commands.add_parser("job-prepare")
    job_prepare.add_argument("destination")
    job_prepare.add_argument("spec")
    workdir_apply = commands.add_parser("workdir-apply")
    workdir_apply.add_argument("spec")

    run = commands.add_parser("run")
    run.add_argument("--timeout", type=float)
    run.add_argument("--grace", type=float, default=10.0)
    run.add_argument("--report", default="process-report.json")
    run.add_argument("--stdout")
    run.add_argument("--stderr")
    run.add_argument("--checker", action="append", default=[])
    run.add_argument("argv", nargs=argparse.REMAINDER)

    calc = commands.add_parser("calc")
    calc.add_argument("expression")
    template = commands.add_parser("template")
    template.add_argument("template")
    template.add_argument("output")
    template.add_argument("values")
    for name in ("compress", "decompress"):
        item = commands.add_parser(name)
        if name == "compress":
            item.add_argument("--method", choices=("bz2", "gz", "xz"), default="bz2")
        item.add_argument("--remove-source", action="store_true")
        item.add_argument("paths", nargs="+")

    for prefix, bridge in _code_bridges().items():
        try:
            bridge.add_commands(commands)
        except Exception as exc:
            print(f"httk-workflow: code {prefix[:-1]!r} bridge commands are unavailable: {exc}", file=sys.stderr)
    return parser


def _declared(attempt: Attempt) -> None:
    """Stand in for a step whose handler lives in the Bash runner."""


def _runner() -> Runner | None:
    """Return the step registration the Bash runner exported, if it did."""

    workflow = os.environ.get(RUNNER_WORKFLOW_VARIABLE)
    if not workflow:
        return None
    runner = Runner(workflow)
    for step in os.environ.get(RUNNER_STEPS_VARIABLE, "").split("\n"):
        if step:
            runner.step(name=step)(_declared)
    return runner


def _draft_root(control: Path) -> Path | None:
    """Return the one unpublished outcome draft of this attempt, if any."""

    roots = sorted(item for item in control.glob("outcome.tmp.*") if item.is_dir())
    if len(roots) > 1:
        raise _Refused(f"this attempt has {len(roots)} outcome drafts, which cannot happen for one attempt")
    return roots[0] if roots else None


def _bind() -> Attempt:
    """Bind this process to its attempt and resume the draft it left behind."""

    bound = _read_environment()
    attempt = Attempt(
        bound.context,
        control=bound.control,
        payload=bound.payload,
        workdir=bound.workdir,
        workspace=bound.workspace,
        data=bound.data,
        step=bound.step,
        runner=_runner(),
    )
    published = bound.control / "outcome.ready"
    if published.is_dir():
        attempt._published = published
        attempt._action = str(read_json(published / "outcome.json").get("action", "published"))
    root = _draft_root(bound.control)
    if root is None:
        attempt._prepare_environment()
        return attempt
    draft = OutcomeDraft._resume(bound.context, bound.control, root, durable=bound.context.durable)
    attempt._draft = draft
    implicit = draft.root / _IMPLICIT_TRANSACTION
    if implicit.is_file():
        seq = implicit.read_text(encoding="utf-8").strip()
        attempt._implicit = Transaction.resume(bound.control, seq, durable=bound.context.durable)
    attempt._prepare_environment()
    return attempt


_ATTEMPT: Attempt | None = None


def _attempt() -> Attempt:
    """Return this process's attempt, binding it exactly once."""

    global _ATTEMPT
    if _ATTEMPT is None:
        _ATTEMPT = _bind()
    return _ATTEMPT


def _publishing() -> Attempt:
    """Return an attempt whose runner registration is known.

    Composing or publishing an outcome names steps and records the runner's step
    set, so it needs the registration ``httk_workflow_runner`` exports. Refusing
    without it is better than publishing an outcome that silently skips the step
    checks a Python runner always performs.
    """

    attempt = _attempt()
    if attempt._runner is None:
        raise _Refused(
            "composing an outcome needs the runner registration; "
            "call httk_workflow_runner WORKFLOW STEP... before any step function"
        )
    return attempt


def _print(value: object) -> None:
    """Print one JSON value the way a shell wants to read it."""

    print(value if isinstance(value, str) else json.dumps(value, sort_keys=True, separators=(",", ":")))


def _print_items(name: str, value: object, *, null: bool) -> None:
    """Print the elements of one array parameter, one per line or NUL-terminated.

    A string element is printed raw and any other element as compact JSON, as
    :func:`_print` prints a whole value. Only an array has elements: an object
    or a scalar is refused rather than guessed at. An element the chosen
    separator cannot carry — a newline in line mode, a NUL in either mode — is
    refused too, so a reader never silently sees more or fewer elements.
    """

    if not isinstance(value, list):
        raise _Refused(f"parameter {name!r} is not a JSON array, so it has no items; read it with parameter instead")
    separator = "\0" if null else "\n"
    texts: list[str] = []
    for index, item in enumerate(cast(list[object], value)):
        text = item if isinstance(item, str) else json.dumps(item, sort_keys=True, separators=(",", ":"))
        if "\0" in text or separator in text:
            spelled = "a NUL" if "\0" in text else "a newline; use --null for NUL-separated items"
            raise _Refused(f"item {index} of parameter {name!r} contains {spelled}")
        texts.append(text)
    sys.stdout.write("".join(text + separator for text in texts))


def _value(text: str) -> object:
    """Return the JSON value one command-line argument denotes.

    ``@path`` is the JSON content of a file, which is how a shell passes a value
    it cannot quote. Anything else is a JSON scalar when it parses as one and the
    literal string when it does not, so ``k=42`` is a number and ``k=Si`` is a
    string without the author quoting either.
    """

    if text.startswith("@"):
        return json.loads(Path(text[1:]).read_text(encoding="utf-8"))
    try:
        return json.loads(text)
    except ValueError:
        return text


def _assignments(values: Sequence[str], name: str) -> dict[str, object]:
    """Parse ``key=value`` arguments into one JSON object."""

    result: dict[str, object] = {}
    for item in values:
        key, separator, text = item.partition("=")
        if not separator or not key:
            raise _Refused(f"{name} must be spelled NAME=VALUE, not {item!r}")
        result[key] = _value(text)
    return result


def _resources(values: Sequence[str]) -> dict[str, int | str] | None:
    """Parse repeatable ``NAME=VALUE`` resource arguments.

    A time label keeps its Slurm duration text, so the outcome draft performs
    the one conversion to seconds; the mapping is validated here only to
    refuse a bad spelling early.
    """

    if not values:
        return None

    result: dict[str, int | str] = {}
    for item in values:
        key, separator, text = item.partition("=")
        refusal = (
            f"a resource must be spelled NAME=VALUE (an integer, or a Slurm duration for maxtime and mintime), "
            f"not {item!r}"
        )
        if not separator or not key or not text:
            raise _Refused(refusal)
        if key in TIME_RESOURCES:
            result[key] = text
            continue
        try:
            result[key] = int(text)
        except ValueError as exc:
            raise _Refused(refusal) from exc
    try:
        normalize_resources(result)
    except FormatError as exc:
        raise _Refused(str(exc)) from exc
    return result


def _child_spec(arguments: argparse.Namespace) -> ChildSpec:
    """Build the synthesized child one ``spawn`` call describes."""

    if not arguments.step:
        raise _Refused("a synthesized child needs --step, or --payload for a prepared payload directory")
    resources = None if not arguments.resources else read_json(Path(str(arguments.resources).removeprefix("@")))
    step_resources = (
        None if not arguments.step_resources else read_json(Path(str(arguments.step_resources).removeprefix("@")))
    )
    return ChildSpec(
        step=arguments.step,
        parameters=_assignments(arguments.parameters, "a child parameter"),
        name=arguments.name,
        tag=arguments.tag,
        priority=arguments.priority,
        claim_pool=arguments.claim_pool,
        required_capabilities=tuple(arguments.capabilities),
        resources=resources,
        step_resources=step_resources,
        maximum_attempts_per_activation=arguments.max_attempts_per_activation,
        maximum_total_attempts=arguments.max_total_attempts,
        maximum_activations=arguments.max_activations,
        retry_on=tuple(arguments.retry_on),
    )


def _file_map(values: Sequence[str], name: str) -> dict[str, str]:
    """Parse ``NAME=PATH`` file arguments into a name-to-path mapping.

    A staged file value is a filesystem path used verbatim, never a JSON value,
    so a path that happens to parse as JSON is still the path the author wrote.
    """

    result: dict[str, str] = {}
    for item in values:
        key, separator, text = item.partition("=")
        if not separator or not key:
            raise _Refused(f"{name} must be spelled NAME=PATH, not {item!r}")
        result[key] = text
    return result


def _call(arguments: argparse.Namespace) -> None:
    attempt = _publishing()
    reference = attempt.call(
        arguments.workflow,
        label=arguments.label,
        inputs=_assignments(arguments.inputs, "a workflow input"),
        files=_file_map(arguments.files, "a staged file"),
        parameters=_assignments(arguments.parameters, "a child parameter"),
        environment=_assignments(arguments.environment, "a workflow environment value"),
        tag=arguments.tag,
        placement=arguments.placement,
        priority=arguments.priority,
        step=arguments.step,
        name=arguments.name,
    )
    print(reference.job_key)


def _spawn(arguments: argparse.Namespace) -> None:
    attempt = _publishing()
    if arguments.payload:
        if arguments.step or arguments.parameters or arguments.resources or arguments.step_resources:
            raise _Refused(
                "a prepared payload directory carries its own job definition, "
                "so --step, --parameter, and the resources apply only to a synthesized child"
            )
        reference = attempt.spawn(arguments.payload, label=arguments.label, placement=arguments.placement)
    else:
        reference = attempt.spawn(_child_spec(arguments), label=arguments.label, placement=arguments.placement)
    print(reference.job_key)


def _children(arguments: argparse.Namespace) -> None:
    """Print one tab-separated row per observed child."""

    view = _attempt().children
    selected = {"all": view.all, "succeeded": view.succeeded, "failed": view.failed}[arguments.selection]
    for child in selected:
        print(
            "\t".join(
                (
                    child.label or "",
                    child.kind,
                    child.job_key,
                    "" if child.workdir is None else str(child.workdir),
                    "" if child.data is None else str(child.data),
                )
            )
        )


def _child(arguments: argparse.Namespace) -> None:
    view = _attempt().children
    child = view.get(arguments.label)
    if child is None:
        raise _Absent(
            f"no child was spawned under label {arguments.label!r}; observed labels: {', '.join(view.labels) or 'none'}"
        )
    fields: dict[str, object] = {
        "label": child.label,
        "state": child.kind,
        "job_id": child.job_id,
        "job_key": child.job_key,
        "failure_code": None if child.failure is None else child.failure.code,
        "failure_message": None if child.failure is None else child.failure.message,
        "payload": None if child.payload is None else str(child.payload),
        "workdir": None if child.workdir is None else str(child.workdir),
        "data": None if child.data is None else str(child.data),
    }
    value = fields[arguments.field]
    if value is None:
        raise _Absent()
    _print(value)


def _transaction(arguments: argparse.Namespace) -> None:
    """Begin, stage into or commit one explicit transaction, named by its six-digit sequence."""

    attempt = _attempt()
    attempt._reject_published()
    if arguments.verb == "begin":
        print(attempt.transaction().seq)
        return
    transaction = Transaction.resume(attempt.control, arguments.handle, durable=attempt.context.durable)
    if arguments.verb == "put":
        transaction.put(arguments.source, arguments.destination)
    else:
        transaction.commit()


def _abort(arguments: argparse.Namespace) -> None:
    """Discard the unpublished draft and record why the step ended abruptly."""

    attempt = _attempt()
    attempt._discard_draft()
    trace = ""
    if arguments.traceback_file:
        path = Path(arguments.traceback_file)
        if path.is_file():
            trace = path.read_text(encoding="utf-8", errors="replace")
    error = {
        "format": RUNNER_ERROR_FORMAT,
        "format_version": 2,
        "step": attempt.step,
        "exception": arguments.exception,
        "message": arguments.message,
        "traceback": trace or f"{arguments.exception}: {arguments.message}\n",
    }
    target = _fs.loc((attempt.control / "error.json").absolute())
    _fs.write_file(target, json_bytes(error) + b"\n", durable=attempt.context.durable)


def _job_prepare(arguments: argparse.Namespace) -> None:
    """Create ``job.json`` in a prepared payload from a specification file.

    The specification is exactly the member set of :class:`JobSpec`, including
    ``workflow_id``, ``workflow_name`` and ``parameters``, so a Bash runner can
    prepare a payload of an installed workflow without a Python program in between.
    """

    raw = read_json(Path(arguments.spec))
    known = {field.name for field in dataclasses.fields(JobSpec)}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise _Refused(f"unknown job specification members: {', '.join(unknown)}")
    values = dict(raw)
    for name in ("required_capabilities", "retry_on"):
        if name in values:
            values[name] = tuple(values[name])
    job = prepare_job_payload(arguments.destination, JobSpec(**values))
    _print(
        {
            "id": job.id,
            "job_key": job.job_key,
            "workflow": {"id": job.workflow_id, "name": job.workflow_name},
            "parameters": _thaw(job.parameters),
        }
    )


def _workdir_apply(arguments: argparse.Namespace) -> None:
    raw = read_json(Path(arguments.spec))
    operations = raw.get("operations")
    if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
        raise _Refused("a workdir operation spec requires an operations array")
    batch = ReplayableWorkdirBatch.initialize(_attempt().workdir, durable=_attempt().context.durable)
    for item in operations:
        if not isinstance(item, Mapping):
            raise _Refused("a workdir operation must be an object")
        operation = str(item.get("op", ""))
        identifier = str(item.get("id", ""))
        path = str(item.get("path", ""))
        if operation == "make-dir":
            batch.transaction.make_dir(identifier, path)
        elif operation == "put-file":
            batch.transaction.put_file(identifier, str(item.get("source", "")), path)
        elif operation in {"put-tree", "replace-tree"}:
            batch.transaction.put_tree(
                identifier,
                str(item.get("source", "")),
                path,
                replace=operation == "replace-tree",
            )
        elif operation == "remove":
            batch.transaction.remove(identifier, path, missing_ok=bool(item.get("missing_ok", False)))
        else:
            raise _Refused(f"unknown workdir operation: {operation}")
    print(batch.commit())


def _run(arguments: argparse.Namespace) -> int:
    argv = arguments.argv[1:] if arguments.argv[:1] == ["--"] else arguments.argv
    checkers = tuple(CheckerSpec.from_mapping(read_json(Path(path))) for path in arguments.checker)
    report = ProcessSupervisor(
        checkers=checkers,
        follow=tuple(source for checker in checkers for source in checker.sources),
    ).run(
        argv,
        timeout=arguments.timeout,
        termination_grace=arguments.grace,
        stdout_path=arguments.stdout,
        stderr_path=arguments.stderr,
        stdout_sink=None if arguments.stdout is not None else sys.stdout.buffer,
        stderr_sink=None if arguments.stderr is not None else sys.stderr.buffer,
    )
    report.write(arguments.report)
    if report.timed_out:
        return 124
    if report.termination.startswith("checker") or any(item.stop for item in report.diagnostics):
        return 125
    return 0 if report.returncode == 0 else 22


def _attempt_command(arguments: argparse.Namespace) -> int:
    """Run one subcommand that reads or composes the current attempt."""

    command = arguments.command
    if command == "begin":
        attempt = _attempt()
        ReplayableWorkdirBatch.recover(attempt.workdir, durable=attempt.context.durable)
        print(attempt.step)
    elif command == "context":
        raw = _attempt().context.raw
        if arguments.field is None:
            _print(raw)
        elif arguments.field not in raw:
            raise _Absent()
        elif raw[arguments.field] is not None:
            _print(raw[arguments.field])
    elif command == "parent":
        if arguments.field is not None and arguments.field not in _PARENT_FIELDS:
            raise _Refused(f"unknown parent field {arguments.field!r}; expected one of {', '.join(_PARENT_FIELDS)}")
        located = _attempt().parent
        if located is None:
            raise _Absent()
        fields = {
            **located.raw,
            "job_id": located.job_id,
            "job_key": located.job_key,
            "placement": placement_text(located.placement),
            "payload": str(located.payload),
            "workdir": None if located.workdir is None else str(located.workdir),
        }
        if arguments.field is None:
            _print(fields)
        elif fields.get(arguments.field) is None:
            raise _Absent()
        else:
            _print(fields[arguments.field])
    elif command == "parameter":
        attempt = _attempt()
        try:
            if arguments.default is None:
                _print(attempt.parameter(arguments.name))
            else:
                _print(attempt.parameter(arguments.name, _value(arguments.default)))
        except KeyError as exc:
            raise _Absent(str(exc.args[0])) from exc
    elif command == "parameter-items":
        attempt = _attempt()
        try:
            if arguments.default is None:
                value = attempt.parameter(arguments.name)
            else:
                value = attempt.parameter(arguments.name, _value(arguments.default))
        except KeyError as exc:
            raise _Absent(str(exc.args[0])) from exc
        _print_items(arguments.name, value, null=arguments.null)
    elif command == "stage-input":
        attempt = _attempt()
        try:
            if arguments.default is None:
                staged = attempt.stage_input(arguments.name, arguments.destination)
            else:
                staged = attempt.stage_input(arguments.name, arguments.destination, arguments.default)
        except KeyError as exc:
            raise _Absent(str(exc.args[0])) from exc
        if staged is None:
            raise _Absent()
    elif command == "setting":
        attempt = _attempt()
        absent = object()
        value = attempt.setting(arguments.name, absent)
        if value is absent:
            if arguments.default is None:
                raise _Absent()
            _print(_value(arguments.default))
        else:
            _print(value)
    elif command == "environment":
        attempt = _attempt()
        try:
            if arguments.default is None:
                _print(attempt.environment(arguments.name))
            else:
                _print(attempt.environment(arguments.name, _value(arguments.default)))
        except KeyError as exc:
            raise _Absent(str(exc.args[0])) from exc
    elif command == "state-get":
        state = _attempt().state.read()
        if arguments.name not in state:
            raise _Absent()
        _print(state[arguments.name])
    elif command == "state-set":
        _attempt().state[arguments.name] = _value(arguments.value)
    elif command == "state-delete":
        if not _attempt().state.delete(arguments.name):
            raise _Absent()
    elif command == "state-merge":
        _attempt().state.merge(_assignments(arguments.assignments, "a state assignment"))
    elif command == "declare":
        # The document is a file because a declaration is a whole JSON object,
        # which is exactly what a shell cannot quote on a command line.
        _attempt().declare(arguments.name, read_json(Path(str(arguments.document).removeprefix("@"))))
    elif command == "declaration":
        document = _attempt().declaration(arguments.name)
        if document is None:
            raise _Absent()
        _print(document)
    elif command == "runlog":
        _attempt().log.append(arguments.kind, arguments.message, files=arguments.files)
    elif command == "environment-log":
        _attempt()._finish_environment_log()
    elif command == "put":
        print(_attempt().put(arguments.source, arguments.destination))
    elif command == "transaction":
        _transaction(arguments)
    elif command == "spawn":
        _spawn(arguments)
    elif command == "call":
        _call(arguments)
    elif command == "children":
        _children(arguments)
    elif command == "child":
        _child(arguments)
    elif command == "advance":
        _publishing().advance(
            arguments.next_step,
            state=_assignments(arguments.state, "a state assignment"),
            priority=arguments.priority,
            resources=_resources(arguments.resources),
        )
    elif command == "gather":
        _publishing().gather(
            arguments.next_step,
            when=cast(JoinCondition, arguments.when),
            count=arguments.count,
            on_impossible=arguments.on_impossible,
            priority=arguments.priority,
            resources=_resources(arguments.resources),
        )
    elif command == "succeed":
        _publishing().succeed()
    elif command == "fail":
        details = None if not arguments.details else read_json(Path(str(arguments.details).removeprefix("@")))
        _publishing().fail(
            arguments.code,
            arguments.message,
            details=details,
            retryable=arguments.retryable,
            priority=arguments.priority,
        )
    elif command == "retry":
        _publishing().retry(arguments.reason)
    elif command == "pause":
        _publishing().pause(arguments.reason)
    elif command == "fail-unknown-step":
        attempt = _publishing()
        runner = attempt._runner
        assert runner is not None
        registered = ", ".join(sorted(runner.steps)) or "none"
        attempt.fail(
            "unknown_step",
            f"step {attempt.step!r} is not implemented by the {runner.workflow} runner; registered steps: {registered}",
        )
    elif command == "fail-no-outcome":
        attempt = _publishing()
        attempt.fail("no_outcome", f"step {attempt.step!r} finished without publishing an outcome")
    elif command == "abort":
        _abort(arguments)
    elif command == "job-prepare":
        _job_prepare(arguments)
    elif command == "workdir-apply":
        _workdir_apply(arguments)
    else:
        raise AssertionError(command)
    return 0


def _utility_command(arguments: argparse.Namespace) -> int:
    """Run one subcommand that needs no attempt at all."""

    command = arguments.command
    if command == "run":
        return _run(arguments)
    if command == "calc":
        value = evaluate_expression(arguments.expression)
        print(int(value) if isinstance(value, bool) else f"{value:.14g}" if isinstance(value, float) else value)
    elif command == "template":
        render_template(arguments.template, arguments.output, read_json(Path(arguments.values)))
    elif command == "compress":
        for output_path in compress_files(
            arguments.paths, method=arguments.method, remove_source=arguments.remove_source
        ):
            print(output_path)
    elif command == "decompress":
        for output_path in decompress_files(arguments.paths, remove_source=arguments.remove_source):
            print(output_path)
    else:
        raise AssertionError(command)
    return 0


_UTILITY_COMMANDS = frozenset({"run", "calc", "template", "compress", "decompress"})


def _command(arguments: argparse.Namespace) -> int:
    command = str(arguments.command)
    for prefix, bridge in _code_bridges().items():
        if command.startswith(prefix):
            return bridge.run_command(arguments)
    if command in _UTILITY_COMMANDS:
        return _utility_command(arguments)
    return _attempt_command(arguments)


def _report(exception: BaseException) -> int:
    """Print one refusal or absence the way every subcommand reports it."""

    if isinstance(exception, _Absent):
        if exception.args and exception.args[0]:
            print(f"httk-workflow: {exception}", file=sys.stderr)
        return ABSENT
    print(f"httk-workflow: {exception}", file=sys.stderr)
    return REFUSED


def _batch(parser: argparse.ArgumentParser, text: str) -> int:
    """Run several newline-separated commands in this one process.

    A Bash runner pays one Python interpreter start per bridge call, so a step
    that composes many operations sends them as one batch instead. The commands
    share this process's attempt and its draft, which is also what makes their
    operation identifiers one sequence.
    """

    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        argv = shlex.split(line, comments=True)
        if not argv:
            continue
        if argv[0] == "batch":
            raise _Refused("a batch cannot contain another batch")
        try:
            code = _command(parser.parse_args(argv))
        except SystemExit as exc:
            code = REFUSED if exc.code in {None, 0} else int(cast(int, exc.code))
        except Exception as exc:
            code = _report(exc)
        if code != 0:
            print(f"httk-workflow: batch line {number} failed: {stripped}", file=sys.stderr)
            return code
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = _parser()
        arguments = parser.parse_args(argv)
        if arguments.command == "batch":
            return _batch(parser, sys.stdin.read())
        return _command(arguments)
    except Exception as exc:
        return _report(exc)


if __name__ == "__main__":
    raise SystemExit(main())
