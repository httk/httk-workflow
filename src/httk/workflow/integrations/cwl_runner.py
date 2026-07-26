#!/usr/bin/env python3
"""Execute one imported CWL workflow natively on *httk₂*.

The job this runner executes carries the normalized plan
:mod:`httk.workflow.integrations.cwl` produced: one self-contained JSON document
with every ``run:`` reference inlined, every type reduced to the supported set,
and every expression already refused or accepted at import time. Nothing here
imports a CWL library — the plan is plain JSON — and **cwltool is never
invoked**. The workflow runs on *httk₂*'s own machinery:

* a workflow step that runs a ``CommandLineTool`` is executed *in* the job, one
  step per activation, so every step boundary is a journalled state frame that a
  restart resumes from;
* a scattered step spawns one labeled child job per shard — ``s0000``, ``s0001``,
  … — through :class:`~httk.workflow.ChildSpec` with
  :meth:`~httk.workflow.RunnerRef.inherit`, and the parent joins them with
  :meth:`~httk.workflow.Attempt.gather`, so shards are claimed, leased, retried
  and scheduled independently like any other children;
* a subworkflow spawns exactly one child carrying the path to itself inside the
  same plan, which recurses to any depth without copying the document.

Steps
-----

The registered steps are a fixed dispatch vocabulary rather than the workflow's
own step names: which CWL step is next is *state*, not a step name, because a
runner's step set is registered at import time and pinned per job while a
document's step set is per document.

``start``
    Read the plan and the staged input object of the root job, seed the state,
    and go to ``advance``.
``enter``
    The entry point of a spawned child: seed the state from the child's inputs —
    which name a position in the plan and the bindings for it — and go to
    ``advance``.
``advance``
    Run exactly one thing: the current tool, or the next ready workflow step;
    then either advance again, gather the children it spawned, or finish.
``collect``
    The join target: read the outputs of the children of one scattered step or
    subworkflow, record them, and go back to ``advance``.

Restarts
--------

Every value a step produces is written to the job state before the outcome that
follows it is published, so a repeated attempt re-reads the same values and runs
only what is left. A tool that was interrupted mid-run is re-run from a cleaned
execution directory: staging is idempotent, and a half-written output of a dead
process is never collected.

Job inputs
----------

* ``cwl_document`` — where the normalized plan is, inside the payload.
* ``cwl_inputs`` — where the staged input object is (root job only).
* ``cwl_payload`` — the workspace-relative payload holding both, for a child job
  whose own payload holds only its ``job.json``.
* ``cwl_target`` — the path of step names to the process a child runs.
* ``cwl_bindings`` — the resolved inputs of that process.
* ``cwl_timeout`` — seconds one tool execution may take, overriding
  ``ToolTimeLimit``.
* ``cwl_data_prefix`` (default ``cwl``) — where the outputs are published when
  the job has transactional data.
"""

import hashlib
import json
import os
import re
import shutil
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

try:
    from httk.workflow import Attempt, ChildSpec, Runner, RunnerRef
except ModuleNotFoundError:  # pragma: no cover - interpreter bootstrap
    # The manager launches this file directly, so the interpreter is whatever the
    # shebang found on PATH, which on a cluster is not necessarily the one httk is
    # installed in. HTTK_WORKFLOW_PYTHON is the interpreter the manager itself runs,
    # so re-exec under it once and let a second failure be reported honestly.
    _python = os.environ.get("HTTK_WORKFLOW_PYTHON")
    if _python is None or os.environ.get("HTTK_WORKFLOW_RUNNER_BOOTSTRAP") == "1":
        raise
    os.environ["HTTK_WORKFLOW_RUNNER_BOOTSTRAP"] = "1"
    os.execv(_python, [_python, os.path.abspath(__file__), *sys.argv[1:]])

WORKFLOW = "cwl.workflow"
OUTPUTS_FILE = "cwl-outputs.json"
DEFAULT_DATA_PREFIX = "cwl"
DEFAULT_TIMEOUT = 86400.0
#: How much of a ``loadContents`` output is read, as CWL's own 64 KiB.
MAXIMUM_CONTENTS = 65536

STATE_TARGET = "cwl_target"
STATE_VALUES = "cwl_values"
STATE_DONE = "cwl_done"
STATE_BINDINGS = "cwl_bindings"
STATE_PENDING = "cwl_pending"

_REFERENCE = re.compile(r"\$\((inputs|runtime)\.([A-Za-z_][A-Za-z0-9_.]*)\)")

run = Runner(WORKFLOW)


class ToolError(Exception):
    """One tool, step or binding could not be prepared or could not be run."""

    def __init__(self, code: str, message: str, *, details: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


# ---------------------------------------------------------------------------
# The plan, and where a job is in it
# ---------------------------------------------------------------------------


def plan_root(a: Attempt) -> Path:
    """Return the payload holding the plan: this job's, or the root job's."""

    pointer = a.input("cwl_payload", None)
    if isinstance(pointer, str) and pointer:
        return a.workspace.joinpath(*PurePosixPath(pointer).parts)
    return a.payload


def plan_of(a: Attempt) -> dict[str, object]:
    """Return the normalized plan this job runs."""

    root = plan_root(a)
    pointer = a.input("cwl_document")
    path = root.joinpath(*PurePosixPath(str(pointer)).parts)
    if not path.is_file():
        raise ToolError("cwl.document_missing", f"the CWL plan {pointer} is not in {root}")
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ToolError("cwl.document_missing", f"the CWL plan {path} is not an object")
    return loaded


def process_at(plan: Mapping[str, object], target: Sequence[str]) -> Mapping[str, object]:
    """Return the process one path of step names names inside the plan."""

    process: Mapping[str, object] = plan
    for name in target:
        steps = process.get("steps")
        if not isinstance(steps, Mapping) or name not in steps:
            raise ToolError("cwl.target_missing", f"the CWL plan has no step {name!r} at {'/'.join(target)}")
        step = steps[name]
        process = step["run"] if isinstance(step, Mapping) else {}
    if not isinstance(process, Mapping):
        raise ToolError("cwl.target_missing", f"the CWL plan has no process at {'/'.join(target)}")
    return process


def payload_relative(a: Attempt) -> str:
    """Return the workspace-relative payload holding the plan, for a child."""

    root = plan_root(a)
    try:
        return PurePosixPath(root.relative_to(a.workspace)).as_posix()
    except ValueError:  # pragma: no cover - a payload outside its own workspace
        return root.as_posix()


# ---------------------------------------------------------------------------
# Values: references, types, and files
# ---------------------------------------------------------------------------


def reference_value(namespace: str, path: str, bindings: Mapping[str, object], runtime: Mapping[str, object]) -> object:
    """Return the value one plain parameter reference names."""

    source: object = bindings if namespace == "inputs" else runtime
    for part in path.split("."):
        if isinstance(source, Mapping):
            source = source.get(part)
        else:
            source = getattr(source, part, None)
    return source


def interpolate(text: object, bindings: Mapping[str, object], runtime: Mapping[str, object]) -> object:
    """Resolve every plain parameter reference in one value.

    A string that is exactly one reference becomes the referenced *value*, of
    whatever type it has; a string that merely contains references becomes a
    string with each one substituted, which is what CWL means by interpolation.
    """

    if not isinstance(text, str):
        return text
    whole = _REFERENCE.fullmatch(text)
    if whole is not None:
        return reference_value(whole.group(1), whole.group(2), bindings, runtime)
    return _REFERENCE.sub(
        lambda match: as_text(reference_value(match.group(1), match.group(2), bindings, runtime)), text
    )


def as_text(value: object) -> str:
    """Return the command-line spelling of one CWL value."""

    if isinstance(value, Mapping) and value.get("class") in {"File", "Directory"}:
        return str(value.get("path") or value.get("basename") or "")
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def as_list(value: object) -> list[object]:
    """Return one plan member that may be a scalar or an array as a list."""

    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return [value]


def is_file(value: object) -> bool:
    """Report whether one value is a File or Directory object."""

    return isinstance(value, Mapping) and value.get("class") in {"File", "Directory"}


def file_object(path: Path, *, contents: bool = False) -> dict[str, object]:
    """Describe one produced file or directory the way CWL does."""

    if path.is_dir():
        return {"class": "Directory", "path": str(path), "basename": path.name}
    data = path.read_bytes()
    described: dict[str, object] = {
        "class": "File",
        "path": str(path),
        "basename": path.name,
        "size": len(data),
        "checksum": f"sha1${hashlib.sha1(data).hexdigest()}",
    }
    if contents:
        described["contents"] = data[:MAXIMUM_CONTENTS].decode("utf-8", errors="replace")
    return described


def resolve_staged(value: object, root: Path) -> object:
    """Turn every payload-relative staged file of the input object into a path."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and not isinstance(value, Mapping):
        return [resolve_staged(item, root) for item in value]
    if not isinstance(value, Mapping):
        return value
    staged = value.get("payload")
    if is_file(value) and isinstance(staged, str):
        path = root.joinpath(*PurePosixPath(staged).parts)
        return {**{key: item for key, item in value.items() if key != "payload"}, "path": str(path)}
    return {key: resolve_staged(item, root) for key, item in value.items()}


def stage_value(value: object, directory: Path, counter: list[int]) -> object:
    """Copy every File and Directory of one binding into *directory*.

    Inputs are staged beside the execution directory rather than into it, so a
    tool's output globs can never match the files it was given. Each one lands in
    a numbered slot of its own, because two inputs of one tool routinely have the
    same basename — three scatter shards each producing ``spoken.txt`` is the
    normal case, not the odd one — and a shared destination would silently make
    them one file. The numbering follows the declaration order of the inputs, so
    a repeated attempt stages exactly the same paths.
    """

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and not isinstance(value, Mapping):
        return [stage_value(item, directory, counter) for item in value]
    if not is_file(value) or not isinstance(value, Mapping):
        return value
    source = value.get("path")
    if not isinstance(source, str) or not source:
        return value
    origin = Path(source)
    if not origin.exists():
        raise ToolError("cwl.input_missing", f"the input file {origin} does not exist")
    slot = directory / f"{counter[0]:04d}"
    counter[0] += 1
    destination = slot / (str(value.get("basename")) or origin.name)
    slot.mkdir(parents=True, exist_ok=True)
    if origin.is_dir():
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(origin, destination)
    else:
        shutil.copyfile(origin, destination)
    return {**value, "path": str(destination), "basename": destination.name}


# ---------------------------------------------------------------------------
# Running one CommandLineTool
# ---------------------------------------------------------------------------


def tool_bindings(tool: Mapping[str, object], bindings: Mapping[str, object], staging: Path) -> dict[str, object]:
    """Return every input of one tool, defaulted, checked and staged."""

    resolved: dict[str, object] = {}
    counter = [0]
    declared = tool.get("inputs")
    parameters = declared if isinstance(declared, Mapping) else {}
    for port, parameter in parameters.items():
        assert isinstance(parameter, Mapping)
        value = bindings.get(port)
        if value is None:
            value = parameter.get("default")
        kind = parameter.get("type")
        optional = bool(kind.get("optional")) if isinstance(kind, Mapping) else True
        if value is None and not optional:
            raise ToolError("cwl.input_missing", f"the tool input {port!r} is required and was not connected")
        resolved[port] = stage_value(value, staging, counter)
    for port, value in bindings.items():
        resolved.setdefault(port, value)
    return resolved


def binding_arguments(binding: Mapping[str, object], value: object, kind: Mapping[str, object] | None) -> list[str]:
    """Return the argv fragment one input binding contributes."""

    prefix = binding.get("prefix")
    separate = bool(binding.get("separate", True))
    base = kind.get("type") if isinstance(kind, Mapping) else None
    if base == "boolean" or isinstance(value, bool):
        return [str(prefix)] if value and prefix is not None else []
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and not isinstance(value, Mapping):
        items = [as_text(item) for item in value]
        if not items:
            return []
        separator = binding.get("itemSeparator")
        if separator is not None:
            return _joined(prefix, separator.join(items) if isinstance(separator, str) else "".join(items), separate)
        return ([str(prefix)] if prefix is not None else []) + items
    return _joined(prefix, as_text(value), separate)


def _joined(prefix: object, text: str, separate: bool) -> list[str]:
    """Return one prefixed argument, joined or separated as the binding asks."""

    if prefix is None:
        return [text]
    return [str(prefix), text] if separate else [f"{prefix}{text}"]


def build_argv(
    tool: Mapping[str, object],
    resolved: Mapping[str, object],
    runtime: Mapping[str, object],
) -> list[str]:
    """Build the argument vector one tool execution runs."""

    ordered: list[tuple[int, int, list[str]]] = []
    index = 0
    for entry in as_list(tool.get("arguments")):
        assert isinstance(entry, Mapping)
        value = interpolate(entry.get("valueFrom"), resolved, runtime)
        ordered.append((int(entry.get("position") or 0), index, binding_arguments(entry, value, None)))
        index += 1
    declared = tool.get("inputs")
    parameters = declared if isinstance(declared, Mapping) else {}
    for port, parameter in parameters.items():
        assert isinstance(parameter, Mapping)
        binding = parameter.get("inputBinding")
        if not isinstance(binding, Mapping):
            continue
        value = resolved.get(port)
        if binding.get("valueFrom") is not None:
            value = interpolate(binding["valueFrom"], resolved, runtime)
        kind = parameter.get("type")
        ordered.append(
            (
                int(binding.get("position") or 0),
                index,
                binding_arguments(binding, value, kind if isinstance(kind, Mapping) else None),
            )
        )
        index += 1
    argv = [str(item) for item in as_list(tool.get("baseCommand"))]
    for _, _, fragment in sorted(ordered, key=lambda item: (item[0], item[1])):
        argv.extend(fragment)
    if not argv:
        raise ToolError("cwl.tool_invalid", "the tool has an empty command line")
    return argv


def collect_outputs(
    tool: Mapping[str, object],
    resolved: Mapping[str, object],
    runtime: Mapping[str, object],
    directory: Path,
    captured: Mapping[str, Path],
) -> dict[str, object]:
    """Collect the declared outputs of one finished tool execution."""

    outputs: dict[str, object] = {}
    declared = tool.get("outputs")
    parameters = declared if isinstance(declared, Mapping) else {}
    for port, parameter in parameters.items():
        assert isinstance(parameter, Mapping)
        kind = parameter.get("type")
        base = kind.get("type") if isinstance(kind, Mapping) else None
        optional = bool(kind.get("optional")) if isinstance(kind, Mapping) else True
        if base in {"stdout", "stderr"}:
            path = captured.get(str(base))
            outputs[port] = None if path is None or not path.is_file() else file_object(path)
            continue
        matched: list[Path] = []
        for pattern in as_list(parameter.get("glob")):
            text = as_text(interpolate(pattern, resolved, runtime))
            matched.extend(sorted(directory.glob(text)) if _has_magic(text) else [directory / text])
        found = [path for path in matched if path.exists()]
        loads = bool(parameter.get("loadContents"))
        if base == "array":
            outputs[port] = [file_object(path, contents=loads) for path in found]
            continue
        if not found:
            if not optional:
                raise ToolError(
                    "cwl.output_missing",
                    f"the tool output {port!r} matched nothing in {directory}",
                    details={"glob": list(parameter.get("glob") or [])},
                )
            outputs[port] = None
            continue
        outputs[port] = file_object(found[0], contents=loads)
    return outputs


def _has_magic(text: str) -> bool:
    """Report whether one glob pattern really is a pattern."""

    return any(character in text for character in "*?[")


def run_tool(a: Attempt, tool: Mapping[str, object], bindings: Mapping[str, object], name: str) -> dict[str, object]:
    """Run one CommandLineTool in its own execution directory and collect it."""

    base = a.workdir / "cwl"
    directory = base / name
    staging = base / f"{name}.inputs"
    # A repeated attempt starts from a clean directory: a half-written output of
    # an interrupted process must never be collected as a result.
    shutil.rmtree(directory, ignore_errors=True)
    shutil.rmtree(staging, ignore_errors=True)
    directory.mkdir(parents=True, exist_ok=True)
    runtime = {"outdir": str(directory), "tmpdir": str(base / f"{name}.tmp"), "cores": 1, "ram": 1024}
    Path(str(runtime["tmpdir"])).mkdir(parents=True, exist_ok=True)
    resolved = tool_bindings(tool, bindings, staging)
    argv = build_argv(tool, resolved, runtime)
    environment = dict(os.environ)
    requirements = tool.get("requirements")
    if isinstance(requirements, Mapping):
        variables = requirements.get("EnvVarRequirement")
        if isinstance(variables, Mapping):
            for entry in as_list(variables.get("envDef")):
                if isinstance(entry, Mapping):
                    key = str(entry.get("envName"))
                    environment[key] = as_text(interpolate(entry.get("envValue"), resolved, runtime))
    a.log.append("note", f"cwl {name}: {' '.join(argv)}")
    result = a.run(argv, timeout=_timeout(a, tool), cwd=directory, environment=environment)
    captured: dict[str, Path] = {}
    for stream, data in (("stdout", result.stdout), ("stderr", result.stderr)):
        declared = tool.get(stream)
        if isinstance(declared, str) and declared:
            path = directory / as_text(interpolate(declared, resolved, runtime))
        else:
            # Nothing declared the stream, so it is kept beside the execution
            # directory rather than inside it, where an output glob would see it.
            path = base / f"{name}.{stream}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        captured[stream] = path
    accepted = [int(str(code)) for code in as_list(tool.get("successCodes"))] or [0]
    if result.returncode not in accepted:
        tail = result.stderr.decode("utf-8", errors="replace").strip().splitlines()[-5:]
        raise ToolError(
            "cwl.tool_failed",
            f"{name} exited {result.returncode}: {' / '.join(tail) or 'no diagnostics'}",
            details={"argv": argv, "returncode": result.returncode, "timed_out": result.timed_out},
        )
    return collect_outputs(tool, resolved, runtime, directory, captured)


def _timeout(a: Attempt, tool: Mapping[str, object]) -> float:
    """Return how long one tool execution may take."""

    override = a.input("cwl_timeout", None)
    if isinstance(override, (int, float)) and not isinstance(override, bool):
        return float(override)
    requirements = tool.get("requirements")
    if isinstance(requirements, Mapping):
        limit = requirements.get("ToolTimeLimit")
        if isinstance(limit, Mapping):
            value = limit.get("timelimit")
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
                return float(value)
    return DEFAULT_TIMEOUT


# ---------------------------------------------------------------------------
# Running one Workflow
# ---------------------------------------------------------------------------


def step_bindings(step: Mapping[str, object], values: Mapping[str, object]) -> dict[str, object]:
    """Resolve the inputs of one workflow step from the values it can see."""

    connections = step.get("in")
    entries = connections if isinstance(connections, Mapping) else {}
    resolved: dict[str, object] = {}
    for port, connection in entries.items():
        assert isinstance(connection, Mapping)
        sources = [str(item) for item in as_list(connection.get("source"))]
        merge = str(connection.get("linkMerge") or "merge_nested")
        if not sources:
            value: object = None
        elif len(sources) == 1 and merge != "merge_flattened":
            value = values.get(sources[0])
        else:
            collected = [values.get(source) for source in sources]
            if merge == "merge_flattened":
                flattened: list[object] = []
                for item in collected:
                    if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
                        flattened.extend(item)
                    elif item is not None:
                        flattened.append(item)
                value = flattened
            else:
                value = collected
        if value is None and "default" in connection:
            value = connection["default"]
        resolved[port] = value
    for port, connection in entries.items():
        assert isinstance(connection, Mapping)
        if connection.get("valueFrom") is not None:
            resolved[port] = interpolate(connection["valueFrom"], resolved, {})
    return resolved


def ready_steps(process: Mapping[str, object], done: Sequence[str]) -> list[str]:
    """Return every step whose sources are all produced, in a stable order."""

    steps = process.get("steps")
    entries = steps if isinstance(steps, Mapping) else {}
    ready: list[str] = []
    for name in sorted(entries):
        step = entries[name]
        if name in done or not isinstance(step, Mapping):
            continue
        connections = step.get("in")
        required: set[str] = set()
        for connection in (connections if isinstance(connections, Mapping) else {}).values():
            for source in as_list((connection if isinstance(connection, Mapping) else {}).get("source")):
                producer = str(source).split("/")
                if len(producer) > 1:
                    required.add(producer[0])
        if required <= set(done):
            ready.append(name)
    return ready


def shard_bindings(step: Mapping[str, object], bindings: Mapping[str, object]) -> list[dict[str, object]]:
    """Split one scattered step's inputs into one binding set per shard."""

    ports = [str(port) for port in as_list(step.get("scatter"))]
    lengths: list[int] = []
    for port in ports:
        value = bindings.get(port)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise ToolError("cwl.scatter_invalid", f"the scattered input {port!r} is not an array")
        lengths.append(len(value))
    if len(set(lengths)) > 1:
        raise ToolError(
            "cwl.scatter_invalid",
            f"dotproduct scatter needs inputs of one length, and got {dict(zip(ports, lengths))}",
        )
    shards: list[dict[str, object]] = []
    for index in range(lengths[0] if lengths else 0):
        shard = dict(bindings)
        for port in ports:
            value = bindings[port]
            assert isinstance(value, Sequence)
            shard[port] = value[index]
        shards.append(shard)
    return shards


def child_inputs(a: Attempt, target: Sequence[str], bindings: Mapping[str, object]) -> dict[str, object]:
    """Return the job inputs one child needs to find and run its part."""

    return {
        "cwl_payload": payload_relative(a),
        "cwl_document": str(a.input("cwl_document")),
        "cwl_target": list(target),
        "cwl_bindings": dict(bindings),
    }


def finish(a: Attempt, outputs: Mapping[str, object]) -> None:
    """Write the outputs of this job, publish them when asked to, and succeed."""

    path = a.workdir / OUTPUTS_FILE
    path.write_text(json.dumps(outputs, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if a.context.data_generation is not None:
        prefix = str(a.input("cwl_data_prefix", DEFAULT_DATA_PREFIX))
        for name, value in outputs.items():
            produced = [value] if is_file(value) else list(value) if isinstance(value, list) else []
            for index, item in enumerate(produced):
                if not is_file(item) or not isinstance(item, Mapping):
                    continue
                source = Path(str(item.get("path")))
                if source.exists():
                    a.put(source, f"{prefix}/{name}/{index:04d}-{source.name}")
        a.put(path, f"{prefix}/{OUTPUTS_FILE}")
    a.log.append("note", f"cwl outputs: {', '.join(outputs) or 'none'}")
    a.succeed()


def workflow_outputs(process: Mapping[str, object], values: Mapping[str, object]) -> dict[str, object]:
    """Return the declared outputs of one finished workflow."""

    outputs: dict[str, object] = {}
    declared = process.get("outputs")
    for name, parameter in (declared if isinstance(declared, Mapping) else {}).items():
        assert isinstance(parameter, Mapping)
        sources = [str(item) for item in as_list(parameter.get("outputSource"))]
        collected = [values.get(source) for source in sources]
        if len(sources) == 1 and str(parameter.get("linkMerge")) != "merge_flattened":
            outputs[name] = collected[0]
        elif str(parameter.get("linkMerge")) == "merge_flattened":
            flattened: list[object] = []
            for item in collected:
                if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
                    flattened.extend(item)
                elif item is not None:
                    flattened.append(item)
            outputs[name] = flattened
        else:
            outputs[name] = collected
    return outputs


# ---------------------------------------------------------------------------
# The steps
# ---------------------------------------------------------------------------


def _state_list(a: Attempt, name: str) -> list[str]:
    value = a.state.get(name, [])
    return [str(item) for item in value] if isinstance(value, list) else []


def _state_mapping(a: Attempt, name: str) -> dict[str, object]:
    value = a.state.get(name, {})
    return dict(value) if isinstance(value, Mapping) else {}


def _failed(a: Attempt, exception: ToolError) -> None:
    """Publish one structured failure of this job.

    None of these are retryable: a tool that exited nonzero, a glob that matched
    nothing and a plan that cannot be satisfied all say the same thing again on
    the next attempt. What a repeated attempt *is* for — a lost node, a killed
    process — is handled by the manager without any failure being published.
    """

    a.fail(exception.code, str(exception), details=exception.details)


@run.step
def start(a: Attempt) -> None:
    """Read the plan and the staged input object, and start the root process."""

    try:
        plan = plan_of(a)
        pointer = str(a.input("cwl_inputs"))
        path = a.payload.joinpath(*PurePosixPath(pointer).parts)
        if not path.is_file():
            raise ToolError("cwl.document_missing", f"the CWL input object {pointer} is not in this payload")
        loaded = json.loads(path.read_text(encoding="utf-8"))
        values = resolve_staged(loaded if isinstance(loaded, Mapping) else {}, a.payload)
    except ToolError as exception:
        _failed(a, exception)
        return
    assert isinstance(values, Mapping)
    bindings = _defaulted(plan, values)
    a.state.merge(
        {
            STATE_TARGET: [],
            STATE_BINDINGS: bindings,
            STATE_VALUES: dict(bindings),
            STATE_DONE: [],
        }
    )
    a.advance("advance")


def _defaulted(process: Mapping[str, object], values: Mapping[str, object]) -> dict[str, object]:
    """Return the input object with the process's own defaults filled in."""

    declared = process.get("inputs")
    bindings = dict(values)
    for name, parameter in (declared if isinstance(declared, Mapping) else {}).items():
        if bindings.get(name) is None and isinstance(parameter, Mapping) and parameter.get("default") is not None:
            bindings[name] = parameter["default"]
    return bindings


@run.step
def enter(a: Attempt) -> None:
    """Start a child at the position of the plan its inputs name."""

    target = [str(item) for item in as_list(a.input("cwl_target", []))]
    bindings = a.input("cwl_bindings", {})
    if not isinstance(bindings, Mapping):
        a.fail("cwl.child_invalid", "the child job input 'cwl_bindings' is not an object")
        return
    try:
        process = process_at(plan_of(a), target)
    except ToolError as exception:
        _failed(a, exception)
        return
    resolved = _defaulted(process, bindings)
    a.state.merge(
        {
            STATE_TARGET: target,
            STATE_BINDINGS: resolved,
            STATE_VALUES: dict(resolved),
            STATE_DONE: [],
        }
    )
    a.advance("advance")


@run.step
def advance(a: Attempt) -> None:
    """Run exactly one tool or one workflow step, and say what happens next."""

    try:
        _advance(a)
    except ToolError as exception:
        _failed(a, exception)


def _advance(a: Attempt) -> None:
    plan = plan_of(a)
    target = _state_list(a, STATE_TARGET)
    process = process_at(plan, target)
    if process.get("class") == "CommandLineTool":
        outputs = run_tool(a, process, _state_mapping(a, STATE_BINDINGS), name=str(process.get("id") or "tool"))
        finish(a, outputs)
        return
    values = _state_mapping(a, STATE_VALUES)
    done = _state_list(a, STATE_DONE)
    steps = process.get("steps")
    entries = steps if isinstance(steps, Mapping) else {}
    ready = ready_steps(process, done)
    if not ready:
        if len(done) < len(entries):
            waiting = ", ".join(sorted(set(entries) - set(done)))
            raise ToolError("cwl.unsatisfiable", f"no step of this workflow can run; waiting on {waiting}")
        finish(a, workflow_outputs(process, values))
        return
    name = ready[0]
    step = entries[name]
    assert isinstance(step, Mapping)
    bindings = step_bindings(step, values)
    if step.get("when") is not None and not interpolate(step["when"], bindings, {}):
        a.log.append("note", f"cwl step {name}: skipped by its when condition")
        _record(a, name, {str(port): None for port in as_list(step.get("out"))}, values, done)
        a.advance("advance")
        return
    child = step.get("run")
    assert isinstance(child, Mapping)
    if step.get("scatter"):
        shards = shard_bindings(step, bindings)
        if not shards:
            _record(a, name, {str(port): [] for port in as_list(step.get("out"))}, values, done)
            a.advance("advance")
            return
        for index, shard in enumerate(shards):
            a.spawn(
                ChildSpec(
                    step="enter",
                    inputs=child_inputs(a, [*target, name], shard),
                    runner=RunnerRef.inherit(),
                    name=f"{name} shard {index}",
                ),
                label=f"s{index:04d}",
            )
        a.state.merge({STATE_PENDING: {"step": name, "scattered": True, "shards": len(shards)}})
        a.log.append("note", f"cwl step {name}: scattered over {len(shards)} children")
        a.gather("collect")
        return
    if child.get("class") == "Workflow":
        a.spawn(
            ChildSpec(
                step="enter",
                inputs=child_inputs(a, [*target, name], bindings),
                runner=RunnerRef.inherit(),
                name=f"{name} subworkflow",
            ),
            label="sub",
        )
        a.state.merge({STATE_PENDING: {"step": name, "scattered": False, "shards": 1}})
        a.log.append("note", f"cwl step {name}: one subworkflow child")
        a.gather("collect")
        return
    outputs = run_tool(a, child, bindings, name=name)
    _record(a, name, outputs, values, done)
    a.advance("advance")


def _record(
    a: Attempt,
    name: str,
    outputs: Mapping[str, object],
    values: dict[str, object],
    done: list[str],
) -> None:
    """Commit the outputs of one finished step before its outcome is published."""

    for port, value in outputs.items():
        values[f"{name}/{port}"] = value
    if name not in done:
        done.append(name)
    a.state.merge({STATE_VALUES: values, STATE_DONE: done, STATE_PENDING: None})


@run.step
def collect(a: Attempt) -> None:
    """Read the outputs of the children of one step, and go on."""

    pending = _state_mapping(a, STATE_PENDING)
    name = str(pending.get("step") or "")
    plan = plan_of(a)
    try:
        process = process_at(plan, _state_list(a, STATE_TARGET))
        steps = process.get("steps")
        entries = steps if isinstance(steps, Mapping) else {}
        if name not in entries:
            raise ToolError("cwl.target_missing", f"this join names the step {name!r}, which this workflow has not")
        step = entries[name]
        assert isinstance(step, Mapping)
        ports = [str(port) for port in as_list(step.get("out"))]
        if pending.get("scattered"):
            # Shard order is the label order, not the order the children happened
            # to finish in: a scattered output is an array in scatter order.
            shards = int(str(pending.get("shards") or 0))
            collected = [_child_outputs(a.children[f"s{index:04d}"]) for index in range(shards)]
            outputs: dict[str, object] = {port: [item.get(port) for item in collected] for port in ports}
        else:
            collected = [_child_outputs(child) for child in a.children.all]
            outputs = {port: (collected[0].get(port) if collected else None) for port in ports}
    except (ToolError, KeyError) as exception:
        _failed(a, exception if isinstance(exception, ToolError) else ToolError("cwl.child_invalid", str(exception)))
        return
    _record(a, name, outputs, _state_mapping(a, STATE_VALUES), _state_list(a, STATE_DONE))
    a.advance("advance")


def _child_outputs(child: object) -> dict[str, object]:
    """Return what one finished child job published as its outputs."""

    workdir = getattr(child, "workdir", None)
    label = getattr(child, "label", None)
    if workdir is None:
        raise ToolError("cwl.child_invalid", f"the child {label} has no workdir to read its outputs from")
    path = Path(workdir) / OUTPUTS_FILE
    if not path.is_file():
        raise ToolError("cwl.child_invalid", f"the child {label} published no {OUTPUTS_FILE}")
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, Mapping):
        raise ToolError("cwl.child_invalid", f"the child {label} published a malformed {OUTPUTS_FILE}")
    return dict(loaded)


if __name__ == "__main__":
    raise SystemExit(run.main())
