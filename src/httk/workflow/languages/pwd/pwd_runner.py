#!/usr/bin/env python3
"""Run one imported Python Workflow Definition document, node by node.

The job this runner executes carries a whole PWD graph — either embedded in its
``parameters`` as ``pwd_document`` or staged in the payload and pointed at by
``pwd_document_path``. The graph runs *inside one job*, sequentially, in the
topological order the importer already validated.

Why one step and not one step per node
--------------------------------------

A :class:`~httk.workflow.Runner` registers its steps when the module is imported,
before any job is looked at, and that is not an accident: the step set is what
``--describe`` reports, what ``ChildSpec`` and ``advance`` are checked against,
and what one ``job.json`` pins for the life of a job. A PWD document's node set
is per document, so registering a step per node would make the step set of *this
program* depend on the payload of whichever job happens to be running — two jobs
of one runner would then describe themselves differently, and a document edited
between two attempts of one job would invalidate the steps its own state frames
name.

So this runner registers exactly one step, ``execute``, and the position in the
graph is *state* rather than a step name. That keeps ``--describe`` honest — the
program really does have one entry point — and it keeps restarts correct through
a checkpoint instead of through the step name:

* every node's result is written to the job state as it is produced, in one
  atomic replace per node (:class:`~httk.workflow.JobState`);
* a repeated attempt reads that checkpoint back and *skips* every node already in
  it, so a graph that failed at node seven resumes at node seven;
* a result that is not JSON cannot be checkpointed, so its node is simply
  recomputed by the next attempt. Every consumer of it comes later in the
  topological order and is therefore recomputed too, which is exactly what
  correctness requires. Nothing is silently reused across attempts.

A node that raises fails the job with ``pwd.node_failed``, retryable by default,
so the manager repeats the attempt within the job's own attempt budget and the
repeat starts from the checkpoint.

Job parameters
----------

* ``pwd_document`` — the whole PWD document, embedded.
* ``pwd_document_path`` — where the document is in the payload, when it was too
  large to embed. Exactly one of the two is required.
* ``pwd_inputs`` — values overriding input nodes by name.
* ``pwd_module_path`` — extra import roots, absolute on the machine that runs
  this job. The payload's ``files/`` directory is always the first import root.
* ``pwd_allowed_modules`` — a prefix allowlist the imports of function nodes must
  match. Absent, no allowlist applies.
* ``pwd_retry_failed_nodes`` (default true) — whether a node that raised makes
  the failure retryable.
* ``pwd_data_prefix`` (default ``pwd``) — where the outputs are published when
  the job has transactional data.

Security
--------

Executing a PWD document means importing and calling the Python functions it
names, with no sandbox: the format has no other content. The document is
therefore trusted exactly as far as the person who imported it is — which is the
same trust as running ``python -c 'import module; module.function()'`` by hand.
``pwd_allowed_modules`` narrows what may be imported when a document comes from
somewhere less trusted than the operator.
"""

import json
import os
import sys
import traceback
from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path

try:
    from httk.workflow import Attempt, Runner
    from httk.workflow.languages.pwd import PwdDocument, validate_pwd_document
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

WORKFLOW = "pwd.workflow"
#: The checkpoint: one JSON value per completed node, keyed by node id.
STATE_RESULTS = "pwd_results"
#: The node ids completed so far, in completion order, for an operator reading
#: ``httk job show``.
STATE_COMPLETED = "pwd_completed"
DEFAULT_DATA_PREFIX = "pwd"
OUTPUTS_FILE = "pwd-outputs.json"

run = Runner(WORKFLOW)


class NodeError(Exception):
    """One node of the graph could not be prepared or could not be run.

    :param node: Identify the failing node.
    :param message: Describe the failure.
    :param details: Preserve structured failure details.
    """

    def __init__(self, node: int, message: str, *, details: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.node = node
        self.details = dict(details or {})


def document_of(a: Attempt) -> PwdDocument:
    """Return the PWD document this job carries, embedded or staged.

    :param a: Read the document from this attempt's parameters or payload.
    :return: The validated PWD document.
    :raises ValueError: If the job has no readable or valid PWD document.
    """

    embedded = a.parameter("pwd_document", None)
    if embedded is None:
        pointer = a.parameter("pwd_document_path", None)
        if not isinstance(pointer, str) or not pointer:
            raise ValueError(
                "this job carries no PWD document: it needs either a pwd_document parameter or a "
                "pwd_document_path parameter naming one in its payload"
            )
        path = a.payload.joinpath(*Path(pointer).parts)
        if not path.is_file():
            raise ValueError(f"the PWD document {pointer} is not in this payload: {path}")
        embedded = json.loads(path.read_text(encoding="utf-8"))
        source = str(path)
    else:
        source = "the pwd_document parameter"
    # The importer validated this document before the job was submitted; a job
    # whose payload was edited afterwards is refused here rather than half-run.
    return validate_pwd_document(embedded, source=source, allow_unknown_version=True)


def import_roots(a: Attempt) -> list[str]:
    """Return the import roots this job adds, payload first.

    :param a: Read module paths from this attempt.
    :return: Import roots with the payload files directory first.
    :raises ValueError: If the module path parameter is not a sequence.
    """

    roots = [str(a.payload / "files")]
    extra = a.parameter("pwd_module_path", [])
    if isinstance(extra, str) or not isinstance(extra, Sequence):
        raise ValueError("job parameter 'pwd_module_path' must be an array of import roots")
    roots.extend(str(item) for item in extra)
    return roots


def allowed_modules(a: Attempt) -> tuple[str, ...]:
    """Return the module prefix allowlist of this job, empty meaning none.

    :param a: Read the allowlist from this attempt.
    :return: Allowed module prefixes.
    :raises ValueError: If the allowlist parameter is not a sequence.
    """

    value = a.parameter("pwd_allowed_modules", [])
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError("job parameter 'pwd_allowed_modules' must be an array of module prefixes")
    return tuple(str(item) for item in value)


def resolve_callable(reference: str, allowlist: Sequence[str], node: int) -> object:
    """Import ``module.function`` and return the callable it names.

    :param reference: Name the callable as a dotted module and attribute.
    :param allowlist: Restrict the module to these prefixes when nonempty.
    :param node: Identify the graph node requesting the callable.
    :return: The imported callable.
    :raises httk.workflow.languages.pwd.pwd_runner.NodeError: If the reference is malformed, disallowed, unavailable, or not callable.
    """

    module_name, _, attribute = reference.rpartition(".")
    if not module_name or not attribute:
        raise NodeError(node, f"function node {node} does not name a callable as 'module.function': {reference!r}")
    if allowlist and not any(module_name == item or module_name.startswith(f"{item}.") for item in allowlist):
        raise NodeError(
            node,
            f"function node {node} imports {module_name}, which is outside this job's "
            f"pwd_allowed_modules allowlist ({', '.join(allowlist)})",
            details={"module": module_name, "allowed": list(allowlist)},
        )
    try:
        module = import_module(module_name)
    except Exception as exc:
        raise NodeError(
            node,
            f"function node {node} cannot import {module_name}: {exc}",
            details={"module": module_name, "sys_path": sys.path[:8]},
        ) from exc
    function = getattr(module, attribute, None)
    if function is None or not callable(function):
        raise NodeError(node, f"function node {node}: {module_name} has no callable named {attribute!r}")
    return function


def port_value(value: object, port: str | None, node: int, source: int) -> object:
    """Return the value one edge carries out of a node's result.

    :param value: Read the source node result.
    :param port: Select this mapping key or attribute, or the whole result when unset.
    :param node: Identify the receiving node in errors.
    :param source: Identify the producing node in errors.
    :return: The value carried by the edge.
    :raises httk.workflow.languages.pwd.pwd_runner.NodeError: If the requested output port does not exist.
    """

    if port is None:
        return value
    if isinstance(value, Mapping):
        if port not in value:
            raise NodeError(
                node,
                f"node {source} produced no output port {port!r}; it produced {', '.join(map(str, value)) or 'nothing'}",
            )
        return value[port]
    attribute = getattr(value, port, None)
    if attribute is None:
        raise NodeError(
            node,
            f"node {source} produced a {type(value).__name__}, which has no output port {port!r}",
        )
    return attribute


def node_arguments(
    document: PwdDocument,
    node: int,
    results: Mapping[int, object],
) -> dict[str, object]:
    """Return the keyword arguments the edges into *node* carry.

    :param document: Read edges from this validated document.
    :param node: Collect edges targeting this node.
    :param results: Read source node results from this mapping.
    :return: Keyword arguments assembled from incoming edges.
    """

    arguments: dict[str, object] = {}
    for edge in document.edges:
        if int(str(edge["target"])) != node:
            continue
        source = int(str(edge["source"]))
        port = edge.get("sourcePort")
        target_port = edge.get("targetPort")
        value = port_value(results[source], port if isinstance(port, str) else None, node, source)
        if isinstance(target_port, str):
            arguments[target_port] = value
        else:
            arguments[""] = value
    return arguments


def jsonable(value: object) -> bool:
    """Report whether one result can go into the checkpoint.

    :param value: Test this result for JSON serializability.
    :return: Whether the value can be encoded as JSON.
    """

    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False
    return True


def execute_node(
    a: Attempt,
    document: PwdDocument,
    node: int,
    results: dict[int, object],
    *,
    overrides: Mapping[str, object],
    allowlist: Sequence[str],
) -> object:
    """Compute one node of the graph and return its result.

    :param a: Log and execute through this attempt.
    :param document: Read the node definition from this document.
    :param node: Execute this node identifier.
    :param results: Read completed upstream results.
    :param overrides: Override input node values by name.
    :param allowlist: Restrict imported function modules to these prefixes.
    :return: The node result.
    :raises httk.workflow.languages.pwd.pwd_runner.NodeError: If the node callable fails or cannot be resolved.
    """

    definition = document.nodes[node]
    kind = str(definition.get("type"))
    if kind == "input":
        name = str(definition["name"])
        return overrides[name] if name in overrides else definition.get("value")
    arguments = node_arguments(document, node, results)
    if kind == "output":
        return arguments.get("")
    function = resolve_callable(str(definition["value"]), allowlist, node)
    a.log.append("note", f"pwd node {node}: {definition['value']}")
    try:
        return function(**arguments)  # type: ignore[operator]
    except Exception as exc:
        raise NodeError(
            node,
            f"function node {node} ({definition['value']}) raised {type(exc).__name__}: {exc}",
            details={"function": definition["value"], "traceback": traceback.format_exc()},
        ) from exc


def publish_outputs(a: Attempt, document: PwdDocument, results: Mapping[int, object]) -> None:
    """Write the named outputs of the graph, and publish them when asked to.

    :param a: Write and publish outputs through this attempt.
    :param document: Read output node definitions from this document.
    :param results: Read completed node results.
    """

    outputs: dict[str, object] = {}
    for node in document.order:
        definition = document.nodes[node]
        if definition.get("type") != "output":
            continue
        value = results[node]
        outputs[str(definition["name"])] = value if jsonable(value) else repr(value)
    path = a.workdir / OUTPUTS_FILE
    path.write_text(json.dumps(outputs, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if a.context.data_generation is not None:
        prefix = a.parameter("pwd_data_prefix", DEFAULT_DATA_PREFIX)
        a.put(path, f"{prefix}/{OUTPUTS_FILE}")
    a.log.append("note", f"pwd outputs: {', '.join(outputs) or 'none'}")


@run.step
def execute(a: Attempt) -> None:
    """Run every node of the document once, resuming from the checkpoint.

    :param a: Execute and checkpoint this PWD job attempt.
    """

    document = document_of(a)
    for root in reversed(import_roots(a)):
        if root not in sys.path:
            sys.path.insert(0, root)
    overrides = a.parameter("pwd_inputs", {})
    if not isinstance(overrides, Mapping):
        raise ValueError("job parameter 'pwd_inputs' must be an object of input node values")
    allowlist = allowed_modules(a)
    stored = a.state.get(STATE_RESULTS, {})
    checkpoint = dict(stored) if isinstance(stored, Mapping) else {}
    completed_raw = a.state.get(STATE_COMPLETED, [])
    completed = [int(item) for item in completed_raw] if isinstance(completed_raw, list) else []
    results: dict[int, object] = {}
    for node in document.order:
        key = str(node)
        if key in checkpoint:
            results[node] = checkpoint[key]
            continue
        try:
            value = execute_node(a, document, node, results, overrides=overrides, allowlist=allowlist)
        except NodeError as exception:
            retryable = bool(a.parameter("pwd_retry_failed_nodes", True))
            a.fail(
                "pwd.node_failed",
                str(exception),
                details={"node": exception.node, "completed": completed, **exception.details},
                retryable=retryable,
            )
            return
        results[node] = value
        completed.append(node)
        if jsonable(value):
            # One atomic replace per node: a process killed between two nodes
            # leaves the graph exactly as far along as it really got.
            checkpoint[key] = value
            a.state.merge({STATE_RESULTS: checkpoint, STATE_COMPLETED: completed})
        else:
            a.state.merge({STATE_COMPLETED: completed})
    publish_outputs(a, document, results)
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
