"""Private command bridge used by the packaged native Bash libraries.

The bridge is the language-agnostic half of the Bash authoring SDK: every Bash
function in ``shell/httk-workflow.sh`` is one invocation of one subcommand here,
and every subcommand does its work through :class:`httk.workflow.Attempt` and
:class:`httk.workflow.OutcomeDraft`. A Bash runner and a Python runner therefore
publish the same bytes, because they publish through exactly one implementation.

A Bash runner is many short-lived processes, so the bridge holds no state of its
own between calls. The one implicit outcome draft of an attempt lives in the
attempt control directory as ``outcome.tmp.<uuid>``, and every bridge process
rediscovers and resumes it there: the spawned children, the staged data
transaction, and therefore the operation counter are all read back from the
draft itself.

Exit codes are uniform across every subcommand:

``0``
    the call succeeded.
``1``
    the answer is legitimately absent — an unset state key, a missing input
    without a default, a null child field.
``2``
    the call is refused — bad usage, a protocol violation, or a corrupt attempt
    context.

The supervised-command and VASP subcommands additionally report the classified
outcome of the program they ran: ``run`` returns ``124`` on timeout, ``125`` when
a checker or diagnostic stopped it, and ``22`` on any other nonzero exit;
``vasp-run`` returns ``20``, ``21``, ``22``, or ``124``; ``vasp-diagnose``
returns ``20`` when it found something; and ``vasp-remedy-plan`` returns ``3``
when the reviewed policy has no remaining safe action. ``125`` is also what
``httk.workflow._launcher`` reports for a runner it could not start at all.
"""

import argparse
import dataclasses
import json
import os
import shlex
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, cast

from ._util import read_json, write_json_atomic
from .runtime import _read_environment
from .runtime_builders import (
    JobSpec,
    JoinCondition,
    OutcomeDraft,
    ReplayableWorkdirBatch,
    TransactionBuilder,
    prepare_job_payload,
)
from .runtime_utils import (
    compress_files,
    decompress_files,
    evaluate_expression,
    render_template,
)
from .sdk import RUNNER_ERROR_FORMAT, Attempt, ChildSpec, Runner, RunnerRef
from .supervision import CheckerSpec, ProcessSupervisor
from .vasp import (
    DEFAULT_KPOINT_CENTERING,
    DEFAULT_REMEDY_HISTORY,
    VaspPreparationOptions,
    VaspRemedyDecision,
    apply_vasp_remedy,
    assemble_potcar,
    automatic_kpoint_grid,
    calculate_nbands,
    clean_outcar,
    clean_vasp_outputs,
    contcar_to_poscar,
    diagnose_vasp_files,
    job_remedy_history_path,
    last_oszicar_energy,
    last_vasprun_volume,
    normalize_poscar_handedness,
    outcar_plane_wave_count,
    outcar_potim,
    plan_vasp_remedy,
    potcar_summary,
    prepare_vasp_inputs,
    rattle_poscar,
    read_incar,
    run_vasp,
    scale_poscar_lattice,
    update_incar,
    write_automatic_kpoints,
)

ABSENT = 1
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
    "data_generation",
)
_JOIN_CONDITIONS = ("all_succeeded", "all_terminal", "any_succeeded", "at_least")


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
    job_input = commands.add_parser("input")
    job_input.add_argument("name")
    job_input.add_argument("--default")
    state_get = commands.add_parser("state-get")
    state_get.add_argument("name")
    state_set = commands.add_parser("state-set")
    state_set.add_argument("name")
    state_set.add_argument("value")
    state_delete = commands.add_parser("state-delete")
    state_delete.add_argument("name")
    state_merge = commands.add_parser("state-merge")
    state_merge.add_argument("assignments", nargs="+")
    runlog = commands.add_parser("runlog")
    runlog.add_argument("kind")
    runlog.add_argument("message")
    runlog.add_argument("files", nargs="*")

    put = commands.add_parser("put")
    put.add_argument("source")
    put.add_argument("destination")
    remove = commands.add_parser("remove")
    remove.add_argument("destination")
    remove.add_argument("--missing-ok", action="store_true")

    spawn = commands.add_parser("spawn")
    spawn.add_argument("label")
    spawn.add_argument("--step")
    spawn.add_argument("--payload")
    spawn.add_argument("--input", action="append", default=[], dest="inputs")
    spawn.add_argument("--runner", default="inherit")
    spawn.add_argument("--placement")
    spawn.add_argument("--priority", type=int)
    spawn.add_argument("--tag")
    spawn.add_argument("--name")
    spawn.add_argument("--workflow")
    spawn.add_argument("--workdir-mode", choices=("persistent", "isolated"), default="persistent")
    spawn.add_argument("--workdir-path", default="run")
    spawn.add_argument("--data-mode", choices=("none", "transactional"), default="none")
    spawn.add_argument("--claim-pool")
    spawn.add_argument("--capability", action="append", default=[], dest="capabilities")
    spawn.add_argument("--retry-on", action="append", default=[], dest="retry_on")
    spawn.add_argument("--resources")
    spawn.add_argument("--max-attempts-per-activation", type=int)
    spawn.add_argument("--max-total-attempts", type=int)
    spawn.add_argument("--max-activations", type=int)

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
    gather = commands.add_parser("gather")
    gather.add_argument("next_step")
    gather.add_argument("--when", choices=_JOIN_CONDITIONS, default="all_succeeded")
    gather.add_argument("--count", type=int)
    gather.add_argument("--on-impossible")
    commands.add_parser("succeed")
    fail = commands.add_parser("fail")
    fail.add_argument("code")
    fail.add_argument("message")
    fail.add_argument("--details")
    fail.add_argument("--retryable", action="store_true")
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

    _add_vasp_commands(commands)
    return parser


def _remedy_history_default() -> str:
    """Return where a Bash runner records its remedy ladder by default.

    Inside an attempt the ladder belongs to the job, not to the directory one
    attempt ran in, so it defaults to the job-scoped file of
    :func:`httk.workflow.vasp.job_remedy_history_path`. Outside an attempt — a
    bridge call made by hand — the historic workdir-relative name is kept.
    """

    payload = os.environ.get("HTTK_WORKFLOW_JOB_DIR")
    return str(job_remedy_history_path(payload)) if payload else DEFAULT_REMEDY_HISTORY


def _add_vasp_commands(commands: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    vasp_prepare = commands.add_parser("vasp-prepare")
    vasp_prepare.add_argument("--directory", default=".")
    vasp_prepare.add_argument("--options")
    vasp_get = commands.add_parser("vasp-get-tag")
    vasp_get.add_argument("tag")
    vasp_get.add_argument("path", nargs="?", default="INCAR")
    vasp_set = commands.add_parser("vasp-set-tag")
    vasp_set.add_argument("tag")
    vasp_set.add_argument("value")
    vasp_set.add_argument("path", nargs="?", default="INCAR")
    grid = commands.add_parser("vasp-kpoints")
    grid.add_argument("density", type=float)
    grid.add_argument("--poscar", default="POSCAR")
    grid.add_argument("--output", default="KPOINTS")
    grid.add_argument("--centering", default=DEFAULT_KPOINT_CENTERING)
    grid.add_argument("--equal", action="store_true")
    grid.add_argument("--bump", type=int, default=0)
    potcar = commands.add_parser("vasp-potcar")
    potcar.add_argument("library")
    potcar.add_argument("--poscar", default="POSCAR")
    potcar.add_argument("--output", default="POTCAR")
    nbands = commands.add_parser("vasp-nbands")
    nbands.add_argument("--poscar", default="POSCAR")
    nbands.add_argument("--potcar", default="POTCAR")
    nbands.add_argument("--incar", default="INCAR")
    nbands.add_argument("--divisor", type=int)
    for name, default in (
        ("vasp-energy", "OSZICAR"),
        ("vasp-volume", "vasprun.xml"),
        ("vasp-potim", "OUTCAR"),
        ("vasp-plane-waves", "OUTCAR"),
    ):
        item = commands.add_parser(name)
        item.add_argument("path", nargs="?", default=default)
    promote = commands.add_parser("vasp-promote-contcar")
    promote.add_argument("--contcar", default="CONTCAR")
    promote.add_argument("--reference", default="POSCAR")
    promote.add_argument("--output", default="POSCAR")
    summary = commands.add_parser("vasp-potcar-summary")
    summary.add_argument("--path", default="POTCAR")
    summary.add_argument("--output", default="POTCAR.summary")
    clean = commands.add_parser("vasp-clean-outcar")
    clean.add_argument("--path", default="OUTCAR")
    clean.add_argument("--output", default="OUTCAR.cleaned")
    preclean = commands.add_parser("vasp-preclean")
    preclean.add_argument("--directory", default=".")
    preclean.add_argument("--keep", action="append", default=[])
    # CONTCAR and vasp-run-report.json survive a preclean unless named here.
    preclean.add_argument("--also-remove", action="append", default=[], dest="also_remove")
    normalize = commands.add_parser("vasp-normalize-poscar")
    normalize.add_argument("path", nargs="?", default="POSCAR")
    scale = commands.add_parser("vasp-scale-poscar")
    scale.add_argument("factor", type=float)
    scale.add_argument("path", nargs="?", default="POSCAR")
    rattle = commands.add_parser("vasp-rattle-poscar")
    rattle.add_argument("path", nargs="?", default="POSCAR")
    rattle.add_argument("--amplitude", type=float, default=0.01)
    # A rattle needs caller-supplied entropy: two retries that rattle identically
    # are two identical calculations, so neither default invents a stream.
    rattle.add_argument("--seed", type=int)
    rattle.add_argument("--entropy")
    vasp_run = commands.add_parser("vasp-run")
    vasp_run.add_argument("--directory", default=".")
    vasp_run.add_argument("--timeout", type=float)
    vasp_run.add_argument("--grace", type=float, default=10.0)
    vasp_run.add_argument("--report", default="vasp-run-report.json")
    vasp_run.add_argument("argv", nargs=argparse.REMAINDER)
    diagnose = commands.add_parser("vasp-diagnose")
    diagnose.add_argument("--directory", default=".")
    diagnose.add_argument("--output", default="vasp-diagnostics.json")
    remedy_plan = commands.add_parser("vasp-remedy-plan")
    remedy_plan.add_argument("report")
    remedy_plan.add_argument("--directory", default=".")
    remedy_plan.add_argument("--history", default=_remedy_history_default())
    remedy_plan.add_argument("--output", default="vasp-remedy-decision.json")
    remedy_plan.add_argument("--policy", default="reviewed-v1")
    remedy_apply = commands.add_parser("vasp-remedy-apply")
    remedy_apply.add_argument("decision")
    remedy_apply.add_argument("--directory", default=".")
    remedy_apply.add_argument("--history", default=_remedy_history_default())


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
        return attempt
    draft = OutcomeDraft._resume(bound.context, bound.control, root)
    attempt._draft = draft
    if (draft.root / "transaction" / "manifest.json").is_file():
        generation = attempt.context.data_generation
        if generation is None:
            raise _Refused("this draft stages a transaction but the job has data.mode none")
        # The sealed manifest is the operation counter of a Bash runner, so the
        # next identifier is the same whichever process of this attempt asks.
        try:
            transaction = TransactionBuilder.resume(draft.root / "transaction", expected_generation=generation)
        except ValueError as exception:
            raise _Refused(f"the staged transaction manifest of this draft is unusable: {exception}") from exception
        attempt._transaction = transaction
        attempt._operations = len(transaction)
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


def _runner_reference(value: str) -> RunnerRef:
    """Parse the ``inherit``/``ws:PATH@SHA``/``installed:PATH@SHA`` spelling."""

    if value == "inherit":
        return RunnerRef.inherit()
    source, separator, rest = value.partition(":")
    path, marker, digest = rest.rpartition("@")
    if not separator or not marker or not path:
        raise _Refused(
            f"runner reference {value!r} must be inherit, ws:PATH@SHA256, or installed:PATH@SHA256",
        )
    if source in {"ws", "workspace"}:
        return RunnerRef.workspace(path, digest)
    if source == "installed":
        return RunnerRef.installed(path, digest)
    raise _Refused(f"runner source {source!r} must be ws or installed")


def _child_spec(arguments: argparse.Namespace) -> ChildSpec:
    """Build the synthesized child one ``spawn`` call describes."""

    if not arguments.step:
        raise _Refused("a synthesized child needs --step, or --payload for a prepared payload directory")
    resources = None if not arguments.resources else read_json(Path(str(arguments.resources).removeprefix("@")))
    return ChildSpec(
        step=arguments.step,
        inputs=_assignments(arguments.inputs, "a child input"),
        runner=_runner_reference(arguments.runner),
        name=arguments.name,
        workflow=arguments.workflow,
        tag=arguments.tag,
        workdir_mode=cast(Literal["persistent", "isolated"], arguments.workdir_mode),
        workdir_path=arguments.workdir_path,
        data_mode=cast(Literal["none", "transactional"], arguments.data_mode),
        priority=arguments.priority,
        claim_pool=arguments.claim_pool,
        required_capabilities=tuple(arguments.capabilities),
        resources=resources,
        maximum_attempts_per_activation=arguments.max_attempts_per_activation,
        maximum_total_attempts=arguments.max_total_attempts,
        maximum_activations=arguments.max_activations,
        retry_on=tuple(arguments.retry_on),
    )


def _spawn(arguments: argparse.Namespace) -> None:
    attempt = _publishing()
    if arguments.payload:
        if arguments.step or arguments.inputs or arguments.runner != "inherit":
            raise _Refused(
                "a prepared payload directory carries its own job definition, "
                "so --step, --input, and --runner apply only to a synthesized child"
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
        "payload": str(child.payload),
        "workdir": None if child.workdir is None else str(child.workdir),
        "data": None if child.data is None else str(child.data),
        "data_generation": child.data_generation,
    }
    value = fields[arguments.field]
    if value is None:
        raise _Absent()
    _print(value)


def _seal(attempt: Attempt) -> None:
    """Persist the staged transaction so the next bridge process resumes it."""

    if attempt._transaction is not None:
        attempt._transaction.seal()


def _abort(arguments: argparse.Namespace) -> None:
    """Discard the unpublished draft and record why the step ended abruptly."""

    attempt = _attempt()
    attempt._discard_draft()
    trace = ""
    if arguments.traceback_file:
        path = Path(arguments.traceback_file)
        if path.is_file():
            trace = path.read_text(encoding="utf-8", errors="replace")
    write_json_atomic(
        attempt.control / "error.json",
        {
            "format": RUNNER_ERROR_FORMAT,
            "format_version": 1,
            "step": attempt.step,
            "exception": arguments.exception,
            "message": arguments.message,
            "traceback": trace or f"{arguments.exception}: {arguments.message}\n",
        },
    )


def _job_prepare(arguments: argparse.Namespace) -> None:
    """Create ``job.json`` in a prepared payload from a specification file.

    The specification is exactly the member set of :class:`JobSpec`, including
    ``runner_backend``, ``runner_source``, ``runner_sha256``, and ``inputs``, so
    a Bash runner can prepare a payload for a shared runner without a Python
    program in between.
    """

    raw = read_json(Path(arguments.spec))
    known = {field.name for field in dataclasses.fields(JobSpec)}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise _Refused(f"unknown job specification members: {', '.join(unknown)}")
    values = dict(raw)
    for name in ("runner_arguments", "required_capabilities", "retry_on"):
        if name in values:
            values[name] = tuple(values[name])
    job = prepare_job_payload(arguments.destination, JobSpec(**values))
    _print(
        {
            "id": job.id,
            "job_key": job.job_key,
            "runner_backend": job.runner_backend,
            "runner_source": job.runner_source,
            "runner_sha256": job.runner_sha256,
            "inputs": dict(job.inputs),
        }
    )


def _workdir_apply(arguments: argparse.Namespace) -> None:
    raw = read_json(Path(arguments.spec))
    operations = raw.get("operations")
    if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
        raise _Refused("a workdir operation spec requires an operations array")
    batch = ReplayableWorkdirBatch.create(_attempt().workdir)
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
    )
    report.write(arguments.report)
    if report.timed_out:
        return 124
    if report.termination.startswith("checker") or any(item.stop for item in report.diagnostics):
        return 125
    return 0 if report.returncode == 0 else 22


def _diagnostics_from_report(path: Path):
    from .supervision import Diagnostic

    value = read_json(path)
    raw_items = value.get("diagnostics", [])
    result = []
    for item in raw_items:
        if isinstance(item, Mapping):
            result.append(
                Diagnostic(
                    str(item.get("code", "")),
                    str(item.get("severity", "error")),  # type: ignore[arg-type]
                    str(item.get("summary", "")),
                    str(item.get("source", "")),
                    None if item.get("evidence") is None else str(item["evidence"]),
                    bool(item.get("stop", False)),
                )
            )
    return tuple(result)


def _decision(path: Path) -> VaspRemedyDecision:
    value = read_json(path)
    raw_changes = value.get("changes", [])
    changes = tuple(
        (str(item["operation"]), item.get("value"))
        for item in raw_changes
        if isinstance(item, Mapping) and "operation" in item
    )
    return VaspRemedyDecision(
        str(value.get("policy", "")),
        str(value.get("problem", "")),
        int(value.get("step", 0)),
        changes,
        bool(value.get("give_up", False)),
        str(value.get("reason", "")),
    )


def _attempt_command(arguments: argparse.Namespace) -> int:
    """Run one subcommand that reads or composes the current attempt."""

    command = arguments.command
    if command == "begin":
        attempt = _attempt()
        ReplayableWorkdirBatch.recover(attempt.workdir)
        print(attempt.step)
    elif command == "context":
        raw = _attempt().context.raw
        if arguments.field is None:
            _print(raw)
        elif arguments.field not in raw:
            raise _Absent()
        elif raw[arguments.field] is not None:
            _print(raw[arguments.field])
    elif command == "input":
        attempt = _attempt()
        try:
            if arguments.default is None:
                _print(attempt.input(arguments.name))
            else:
                _print(attempt.input(arguments.name, _value(arguments.default)))
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
    elif command == "runlog":
        _attempt().log.append(arguments.kind, arguments.message, files=arguments.files)
    elif command == "put":
        attempt = _publishing()
        print(attempt.put(arguments.source, arguments.destination))
        _seal(attempt)
    elif command == "remove":
        attempt = _publishing()
        print(attempt.remove(arguments.destination, missing_ok=arguments.missing_ok))
        _seal(attempt)
    elif command == "spawn":
        _spawn(arguments)
    elif command == "children":
        _children(arguments)
    elif command == "child":
        _child(arguments)
    elif command == "advance":
        _publishing().advance(
            arguments.next_step,
            state=_assignments(arguments.state, "a state assignment"),
            priority=arguments.priority,
        )
    elif command == "gather":
        _publishing().gather(
            arguments.next_step,
            when=cast(JoinCondition, arguments.when),
            count=arguments.count,
            on_impossible=arguments.on_impossible,
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
            f"step {attempt.step!r} is not implemented by the {runner.workflow} runner; "
            f"registered steps: {registered}",
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


def _vasp_command(arguments: argparse.Namespace) -> int:
    command = arguments.command
    if command == "vasp-prepare":
        options = VaspPreparationOptions()
        if arguments.options:
            options = VaspPreparationOptions(**read_json(Path(arguments.options)))
        _print(prepare_vasp_inputs(options, directory=arguments.directory))
    elif command == "vasp-get-tag":
        value = read_incar(arguments.path).get(arguments.tag.upper())
        if value is None:
            raise _Absent()
        print(value)
    elif command == "vasp-set-tag":
        update_incar({arguments.tag: arguments.value}, arguments.path)
    elif command == "vasp-kpoints":
        grid = automatic_kpoint_grid(
            arguments.density, poscar=arguments.poscar, equal=arguments.equal, bump=arguments.bump
        )
        write_automatic_kpoints(grid, arguments.output, centering=arguments.centering)
        print(" ".join(str(value) for value in grid))
    elif command == "vasp-potcar":
        assemble_potcar(arguments.library, poscar=arguments.poscar, output=arguments.output)
    elif command == "vasp-nbands":
        print(
            calculate_nbands(
                poscar=arguments.poscar, potcar=arguments.potcar, incar=arguments.incar, divisor=arguments.divisor
            )
        )
    elif command in {"vasp-energy", "vasp-volume", "vasp-potim"}:
        reader = {
            "vasp-energy": last_oszicar_energy,
            "vasp-volume": last_vasprun_volume,
            "vasp-potim": outcar_potim,
        }[command]
        number = reader(arguments.path)
        if number is None:
            raise _Absent()
        print(f"{number:.16g}")
    elif command == "vasp-plane-waves":
        count = outcar_plane_wave_count(arguments.path)
        if count is None:
            raise _Absent()
        print(count)
    elif command == "vasp-promote-contcar":
        contcar_to_poscar(arguments.contcar, reference=arguments.reference, output=arguments.output)
    elif command == "vasp-potcar-summary":
        potcar_summary(arguments.path, arguments.output)
    elif command == "vasp-clean-outcar":
        clean_outcar(arguments.path, arguments.output)
    elif command == "vasp-preclean":
        for removed in clean_vasp_outputs(arguments.directory, keep=arguments.keep, also_remove=arguments.also_remove):
            print(removed)
    elif command == "vasp-normalize-poscar":
        normalize_poscar_handedness(arguments.path)
    elif command == "vasp-scale-poscar":
        scale_poscar_lattice(arguments.factor, arguments.path)
    elif command == "vasp-rattle-poscar":
        if arguments.seed is None and arguments.entropy is None:
            raise _Refused("vasp-rattle-poscar needs --seed or --entropy: an unseeded retry rattles identically")
        rattle_poscar(
            arguments.path,
            amplitude=arguments.amplitude,
            seed=arguments.seed,
            entropy=arguments.entropy,
        )
    elif command == "vasp-run":
        argv = arguments.argv[1:] if arguments.argv[:1] == ["--"] else arguments.argv
        report = run_vasp(
            argv,
            directory=arguments.directory,
            timeout=arguments.timeout,
            termination_grace=arguments.grace,
            report_path=arguments.report,
        )
        return {
            "completed": 0,
            "diagnosed_stop": 20,
            "nonconverged": 21,
            "process_failure": 22,
            "timeout": 124,
        }[report.classification]
    elif command == "vasp-diagnose":
        diagnostics = diagnose_vasp_files(arguments.directory)
        write_json_atomic(
            Path(arguments.output),
            {
                "format": "httk-vasp-diagnostics",
                "format_version": 1,
                "diagnostics": [item.as_mapping() for item in diagnostics],
            },
        )
        return 0 if not diagnostics else 20
    elif command == "vasp-remedy-plan":
        decision = plan_vasp_remedy(
            _diagnostics_from_report(Path(arguments.report)),
            directory=arguments.directory,
            history_path=arguments.history,
            policy=arguments.policy,
        )
        write_json_atomic(Path(arguments.output), decision.as_mapping())
        # The diagnosed problem is printed so a Bash runner can name it in the
        # outcome it publishes without parsing the decision it just wrote.
        print(decision.problem)
        return 3 if decision.give_up else 0
    elif command == "vasp-remedy-apply":
        apply_vasp_remedy(
            _decision(Path(arguments.decision)),
            directory=arguments.directory,
            history_path=arguments.history,
        )
    else:
        raise AssertionError(command)
    return 0


_UTILITY_COMMANDS = frozenset({"run", "calc", "template", "compress", "decompress"})


def _command(arguments: argparse.Namespace) -> int:
    command = str(arguments.command)
    if command.startswith("vasp-"):
        return _vasp_command(arguments)
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
    parser = _parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "batch":
            return _batch(parser, sys.stdin.read())
        return _command(arguments)
    except Exception as exc:
        return _report(exc)


if __name__ == "__main__":
    raise SystemExit(main())
