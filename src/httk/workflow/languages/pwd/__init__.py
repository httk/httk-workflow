"""Running one Python Workflow Definition document as one *httk₂* job.

The `Python Workflow Definition <https://github.com/pythonworkflow/python-workflow-definition>`_
(PWD) is a small JSON exchange format: a list of ``nodes`` and a list of
``edges``. A node is a Python function named ``module.function``, a literal
input, or a named output; an edge connects one node's output port to another
node's input port. The format is deliberately machine-facing — several workflow
engines read and write it — and this module is *httk₂* reading it.

.. code-block:: python

    from httk.workflow import Workspace, new_job

    workspace = Workspace.initialize("workflow-workspace")
    job = new_job(workspace, "workflow.json", parameters={"pwd_module_path": ["."]})

The import is one way and produces exactly one job. The whole graph runs inside
that job, sequentially, in topological order, by the packaged
``pwd_runner.py`` — no runner file is written per workflow, and no per-node job
is created: a PWD node is one Python call, which is not worth a claim, a lease
and a process of its own.

The document travels in the job's ``parameters`` when it fits within
*maximum_embedded_bytes*, and in ``files/pwd.json`` with a document pointer when
it does not, because ``parameters`` is bounded by
:data:`~httk.workflow.models.MAXIMUM_PARAMETERS_BYTES` and a generated document can
be much larger than that.

.. warning::

   Running a PWD document **executes the Python functions it names**. There is
   no sandbox and there cannot be one: the format's whole content is
   ``module.function`` references. Import a document exactly as carefully as you
   would run the module it names. Passing *allowed_modules* records an allowlist
   of module prefixes in the job, which the runner refuses to import outside of.
"""

import json
import logging
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, cast

from httk.workflow.languages import (
    LanguagePorts,
    LanguageRequest,
    LanguageScaffold,
    WorkflowLanguage,
    _data_record,
    _load_outputs,
    _output_roles,
    _parameter,
    runner_reference,
)
from httk.workflow.models import MAXIMUM_PARAMETERS_BYTES
from httk.workflow.scaffold import FILES_DIRECTORY

_LOGGER = logging.getLogger(__name__)

#: The package the packaged runner is resolved and digest-pinned within.
PACKAGE = __name__
#: The packaged runner an imported PWD job runs.
RUNNER = "pwd_runner.py"
#: The document versions this importer was written against.
KNOWN_VERSIONS = ("0.1.0",)
#: Where an oversized document is staged inside the payload.
DOCUMENT_FILE = f"{FILES_DIRECTORY}/pwd.json"
#: How much of the ``parameters`` budget an embedded document may take. The rest of
#: the budget belongs to the members that describe how to run it.
DEFAULT_MAXIMUM_EMBEDDED_BYTES = MAXIMUM_PARAMETERS_BYTES // 2

_NODE_TYPES = ("function", "input", "output")

if TYPE_CHECKING:
    from httk.workflow.collecting import JobRecord


class PwdFormatError(ValueError):
    """A document that is not a Python Workflow Definition this importer accepts."""


@dataclass(frozen=True)
class PwdDocument:
    """One validated PWD document, and the order its function nodes run in.

    :param raw: Preserve the validated source document.
    :param nodes: Preserve validated nodes keyed by identifier.
    :param edges: Preserve validated graph edges.
    :param order: Record the topological node execution order.
    """

    #: The document exactly as it was read, unknown members and all.
    raw: Mapping[str, object]
    #: Every node by id, exactly as it was read.
    nodes: Mapping[int, Mapping[str, object]]
    #: Every edge, exactly as it was read.
    edges: tuple[Mapping[str, object], ...]
    #: The node ids in one topological order.
    order: tuple[int, ...]

    @property
    def version(self) -> str | None:
        """Return the document version when one is declared."""
        version = self.raw.get("version")
        return version if isinstance(version, str) else None

    @property
    def functions(self) -> tuple[str, ...]:
        """Every ``module.function`` this document would import, in node order."""

        return tuple(
            str(self.nodes[node]["value"]) for node in self.order if self.nodes[node].get("type") == "function"
        )

    @property
    def input_names(self) -> tuple[str, ...]:
        """The names of every input node, in node order."""

        return tuple(str(self.nodes[node]["name"]) for node in self.order if self.nodes[node].get("type") == "input")

    @property
    def output_names(self) -> tuple[str, ...]:
        """The names of every output node, in node order."""

        return tuple(str(self.nodes[node]["name"]) for node in self.order if self.nodes[node].get("type") == "output")


def load_pwd_document(path: str | os.PathLike[str], *, allow_unknown_version: bool = False) -> PwdDocument:
    """Read and validate one PWD document from *path*.

    :param path: Read the PWD JSON document at this path.
    :param allow_unknown_version: Try versions outside the supported set.
    :return: The validated PWD document.
    :raises httk.workflow.languages.pwd.PwdFormatError: If the file cannot be read, parsed, or validated.
    """

    document = Path(path).expanduser()
    try:
        raw = json.loads(document.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PwdFormatError(f"cannot read the workflow document {document}: {exc}") from exc
    except ValueError as exc:
        raise PwdFormatError(f"{document} is not JSON: {exc}") from exc
    return validate_pwd_document(raw, source=str(document), allow_unknown_version=allow_unknown_version)


def validate_pwd_document(
    raw: object,
    *,
    source: str = "the document",
    allow_unknown_version: bool = False,
) -> PwdDocument:
    """Validate the shape of one PWD document and order its nodes.

    Every member the format defines is checked; every member it does not define
    is preserved untouched, so a document carrying an engine's own annotations
    survives the round trip into the job payload. When the
    ``python-workflow-definition`` package happens to be installed it is asked
    for a second opinion — it is never a dependency of *httk-workflow*, only a
    stricter validator when it is there.

    :param raw: Validate this decoded PWD document.
    :param source: Identify the document in validation errors.
    :param allow_unknown_version: Try versions outside the supported set.
    :return: The validated PWD document.
    :raises httk.workflow.languages.pwd.PwdFormatError: If the document shape, graph, or version is invalid.
    """

    if not isinstance(raw, Mapping):
        raise PwdFormatError(f"{source} must be a JSON object with nodes and edges")
    version = raw.get("version")
    if version is not None and not isinstance(version, str):
        raise PwdFormatError(f"{source} has a version that is not a string")
    if isinstance(version, str) and version not in KNOWN_VERSIONS and not allow_unknown_version:
        raise PwdFormatError(
            f"{source} declares Python Workflow Definition version {version!r}, and this importer was "
            f"written against {', '.join(KNOWN_VERSIONS)}; import it with allow_unknown_version=True "
            "(--allow-unknown-version) to try anyway"
        )
    nodes_raw = raw.get("nodes")
    edges_raw = raw.get("edges")
    if not isinstance(nodes_raw, Sequence) or isinstance(nodes_raw, (str, bytes)):
        raise PwdFormatError(f"{source} must have a nodes array")
    if not isinstance(edges_raw, Sequence) or isinstance(edges_raw, (str, bytes)):
        raise PwdFormatError(f"{source} must have an edges array")
    nodes: dict[int, Mapping[str, object]] = {}
    for entry in nodes_raw:
        node = _validate_node(entry, source)
        identifier = int(str(node["id"]))
        if identifier in nodes:
            raise PwdFormatError(f"{source} declares node id {identifier} twice")
        nodes[identifier] = node
    if not nodes:
        raise PwdFormatError(f"{source} declares no nodes")
    edges = tuple(_validate_edge(entry, nodes, source) for entry in edges_raw)
    _reject_duplicate_ports(edges, nodes, source)
    order = _topological_order(nodes, edges, source)
    _validate_with_package(raw, source)
    return PwdDocument(raw=dict(raw), nodes=nodes, edges=edges, order=order)


def _matches(path: Path) -> bool:
    try:
        if not path.is_file() or path.suffix != ".json":
            return False
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return isinstance(raw, dict) and isinstance(raw.get("nodes"), list) and isinstance(raw.get("edges"), list)


def _ports(path: Path) -> LanguagePorts:
    document = load_pwd_document(path)
    return LanguagePorts(document.input_names, document.output_names)


def _member(root: Path, value: object) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError("module member must be a relative .py file")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError(f"module member must be relative and contain no '..': {value!r}")
    candidate = root.joinpath(*relative.parts)
    resolved_root = root.resolve()
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"module member does not exist: {value!r}") from exc
    if (
        relative.suffix != ".py"
        or not resolved.is_relative_to(resolved_root)
        or candidate.is_symlink()
        or not candidate.is_file()
    ):
        raise ValueError(f"module member must be a regular .py member below the package: {value!r}")
    for index in range(1, len(relative.parts)):
        if root.joinpath(*relative.parts[:index]).is_symlink():
            raise ValueError(f"module member must not traverse a symlink: {value!r}")


def _validate_runner(options: Mapping[str, object], root: Path) -> None:
    for key, value in options.items():
        if key not in {"modules", "module_path", "allowed_modules"}:
            raise ValueError(f"unknown runner option {key!r} for pwd")
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise ValueError(f"runner option {key!r} must be a list of strings")
        if key == "modules":
            for member in value:
                try:
                    _member(root, member)
                except ValueError as exc:
                    raise ValueError(f"modules: {exc}") from exc


def _prepare(request: LanguageRequest) -> LanguageScaffold:
    document = request.document
    if document is None:
        raise PwdFormatError("a pwd language workflow requires a document")
    loaded = load_pwd_document(document)
    serialized = json.dumps(loaded.raw, sort_keys=True)
    documents: dict[str, str | bytes] = {}
    parameters: dict[str, object] = {"workflow_language": "pwd"}
    if len(serialized.encode()) <= DEFAULT_MAXIMUM_EMBEDDED_BYTES:
        parameters["pwd_document"] = loaded.raw
    else:
        documents[DOCUMENT_FILE] = serialized
        parameters["pwd_document_path"] = DOCUMENT_FILE

    options = request.runner_options
    modules = cast(Sequence[str], options.get("modules", ()))
    root = (request.directory or document.parent).resolve()
    for member in modules:
        source = root.joinpath(*PurePosixPath(member).parts)
        documents[f"{FILES_DIRECTORY}/{Path(member).name}"] = source.read_bytes()
    if "module_path" in options:
        parameters["pwd_module_path"] = list(cast(Sequence[str], options["module_path"]))
    if "allowed_modules" in options:
        parameters["pwd_allowed_modules"] = list(cast(Sequence[str], options["allowed_modules"]))
    parameters["pwd_output_roles"] = {
        str(metadata.get("port", name)): str(metadata.get("role", name)) for name, metadata in request.outputs.items()
    }

    def instantiate(ctx: object) -> object:
        """Apply supplied inputs as literal JSON values for PWD input nodes."""

        from httk.workflow.scaffold import InstantiateContext

        assert isinstance(ctx, InstantiateContext)
        overrides: dict[str, object] = {}
        for name, value in ctx.inputs.items():
            try:
                json.dumps(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"workflow input {name!r} must be JSON-serializable") from exc
            metadata = request.inputs.get(name, {})
            overrides[str(metadata.get("port", name))] = value
        if overrides:
            ctx.parameters["pwd_inputs"] = overrides
        return None

    return LanguageScaffold(
        documents=documents,
        files={},
        parameters=parameters,
        runner=runner_reference(PACKAGE, RUNNER),
        reserved_parameters=("pwd_inputs",),
        instantiate=instantiate,
    )


def collect(record: "JobRecord") -> Mapping[str, object]:
    """Convert one PWD runner output document into provenance-capable records."""

    prefix = _parameter(record, "pwd_data_prefix", "pwd")
    raw_outputs = _load_outputs(record, "pwd-outputs.json", prefix)
    roles = _output_roles(record, "pwd_output_roles", raw_outputs)
    return {str(roles[port]): _data_record(str(roles[port]), value) for port, value in raw_outputs.items()}


LANGUAGE = WorkflowLanguage(
    name="pwd",
    steps=("execute",),
    initial_step="execute",
    requires_document=True,
    has_default_collector=True,
    matches=_matches,
    ports=_ports,
    validate_runner=_validate_runner,
    prepare=_prepare,
    collect=collect,
)


def _validate_node(entry: object, source: str) -> Mapping[str, object]:
    """Validate one node of a PWD document."""

    if not isinstance(entry, Mapping):
        raise PwdFormatError(f"{source} has a node that is not an object")
    identifier = entry.get("id")
    if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier < 0:
        raise PwdFormatError(f"{source} has a node whose id is not a nonnegative integer: {identifier!r}")
    kind = entry.get("type")
    if kind not in _NODE_TYPES:
        raise PwdFormatError(f"{source} node {identifier} has type {kind!r}; expected one of {', '.join(_NODE_TYPES)}")
    if kind == "function":
        value = entry.get("value")
        if not isinstance(value, str) or "." not in value.strip(".") or value != value.strip():
            raise PwdFormatError(
                f"{source} function node {identifier} must name a callable as 'module.function', not {value!r}"
            )
    else:
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            raise PwdFormatError(f"{source} {kind} node {identifier} needs a nonempty name")
    return entry


def _validate_edge(entry: object, nodes: Mapping[int, Mapping[str, object]], source: str) -> Mapping[str, object]:
    """Validate one edge of a PWD document against the nodes it connects."""

    if not isinstance(entry, Mapping):
        raise PwdFormatError(f"{source} has an edge that is not an object")
    ends: dict[str, int] = {}
    for side in ("source", "target"):
        value = entry.get(side)
        if isinstance(value, bool) or not isinstance(value, int) or value not in nodes:
            raise PwdFormatError(f"{source} has an edge whose {side} is not the id of a declared node: {value!r}")
        ends[side] = value
    for side in ("sourcePort", "targetPort"):
        port = entry.get(side)
        if port is not None and (not isinstance(port, str) or not port):
            raise PwdFormatError(f"{source} has an edge whose {side} is neither null nor a nonempty string")
    target_type = nodes[ends["target"]].get("type")
    source_type = nodes[ends["source"]].get("type")
    if target_type == "input":
        raise PwdFormatError(f"{source} has an edge into input node {ends['target']}, which takes no inputs")
    if source_type == "output":
        raise PwdFormatError(f"{source} has an edge out of output node {ends['source']}, which produces nothing")
    if target_type == "function" and not isinstance(entry.get("targetPort"), str):
        raise PwdFormatError(
            f"{source} has an edge into function node {ends['target']} without a targetPort naming its parameter"
        )
    if target_type == "output" and entry.get("targetPort") is not None:
        raise PwdFormatError(f"{source} has an edge into output node {ends['target']} with a targetPort")
    return entry


def _reject_duplicate_ports(
    edges: Iterable[Mapping[str, object]],
    nodes: Mapping[int, Mapping[str, object]],
    source: str,
) -> None:
    """Refuse two edges feeding one parameter, which no engine can order."""

    seen: set[tuple[int, str | None]] = set()
    for edge in edges:
        port = edge.get("targetPort")
        key = (int(str(edge["target"])), port if isinstance(port, str) else None)
        if key in seen:
            port = key[1] or "its result"
            raise PwdFormatError(f"{source} feeds {port} of node {key[0]} from two edges")
        seen.add(key)
    for identifier, node in nodes.items():
        if node.get("type") != "output":
            continue
        if (identifier, None) not in seen:
            raise PwdFormatError(f"{source} output node {identifier} is not connected to anything")


def _topological_order(
    nodes: Mapping[int, Mapping[str, object]],
    edges: Iterable[Mapping[str, object]],
    source: str,
) -> tuple[int, ...]:
    """Return the node ids in a deterministic topological order."""

    incoming: dict[int, set[int]] = {identifier: set() for identifier in nodes}
    for edge in edges:
        incoming[int(str(edge["target"]))].add(int(str(edge["source"])))
    ordered: list[int] = []
    remaining = dict(incoming)
    while remaining:
        # Smallest id first among the ready nodes: two imports of one document
        # must produce exactly the same order, whatever the JSON member order was.
        ready = sorted(identifier for identifier, sources in remaining.items() if not sources)
        if not ready:
            cycle = ", ".join(str(identifier) for identifier in sorted(remaining))
            raise PwdFormatError(f"{source} has a cycle through nodes {cycle}, so it is not a workflow")
        for identifier in ready:
            ordered.append(identifier)
            del remaining[identifier]
        for sources in remaining.values():
            sources.difference_update(ready)
    return tuple(ordered)


def _validate_with_package(raw: Mapping[str, object], source: str) -> None:
    """Ask the ``python-workflow-definition`` package, when it is installed.

    The package is never a dependency: it is a stricter second reading of the
    same bytes when the environment happens to have it, and its absence changes
    nothing about what is accepted.
    """

    if find_spec("python_workflow_definition") is None:
        return
    try:
        models = import_module("python_workflow_definition.models")
        workflow = models.PythonWorkflowDefinitionWorkflow
    except Exception as exc:  # pragma: no cover - depends on the installed package
        _LOGGER.debug("the installed python-workflow-definition cannot be used as a validator: %s", exc)
        return
    try:
        workflow.model_validate(dict(raw))
    except Exception as exc:
        raise PwdFormatError(f"the installed python-workflow-definition package refuses {source}: {exc}") from exc
