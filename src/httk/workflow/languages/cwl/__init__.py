"""Run one CWL document as one *httk₂* job.

The `Common Workflow Language <https://www.commonwl.org/>`_ is supported here as
a workflow **language**, not as an execution engine. A document is parsed and
validated with `cwl-utils <https://github.com/common-workflow-language/cwl-utils>`_,
normalized into one self-contained JSON plan, and then executed entirely by
*httk₂*'s own packaged ``cwl_runner.py`` on *httk₂*'s own manager: claims, leases,
attempts, checkpoints, labeled children and journalled state frames are the same
ones every other job of a workspace gets. **cwltool is not used, not bundled and
never invoked.**

.. code-block:: python

    from httk.workflow import Workspace, new_job

    workspace = Workspace.initialize("workflow-workspace")
    job = new_job(workspace, "flow.cwl", inputs={"message": "hello"})
    print(job.job_key)

Parsing needs the optional extra::

    pip install httk-workflow[cwl]

which brings *cwl-utils* and *cwl-upgrader*. Nothing else in *httk-workflow*
depends on either, and the packaged runner that executes the normalized plan
needs neither: the plan is plain JSON, so the machine that *runs* an imported CWL
job does not need a CWL library at all.

The supported subset
--------------------

Supported: ``Workflow`` and ``CommandLineTool`` of CWL v1.0, v1.1 and v1.2
(older versions are upgraded when *cwl-upgrader* is installed); ``baseCommand``
and ``arguments``; input bindings with ``position``, ``prefix``, ``separate``
and ``itemSeparator``; the types ``File``, ``Directory``, ``string``, ``int``,
``long``, ``float``, ``double``, ``boolean``, ``Any``, arrays of those, and
optional (``?``) forms of all of them; ``stdout``/``stderr`` shortcuts and
``stdout``/``stderr`` redirection; output collection through
``outputBinding.glob`` with ``loadContents``; ``EnvVarRequirement``,
``ResourceRequirement`` (recorded), ``ToolTimeLimit`` (honoured),
``successCodes``; workflow steps with ``source``, ``default``, ``linkMerge`` and
plain-reference ``valueFrom``; single-input ``scatter`` and ``dotproduct``
``scatter``; subworkflows; and ``when`` written as one plain parameter
reference.

Rejected, always with the feature name and where it was found: any JavaScript
(``InlineJavascriptRequirement``, ``${...}`` bodies, and anything inside
``$(...)`` beyond a plain ``inputs.x``/``runtime.x`` reference), ``ExpressionTool``
and ``Operation``, ``nested_crossproduct`` and ``flat_crossproduct``,
``streamable`` inputs and outputs, ``secondaryFiles``, ``outputEval``,
``ShellCommandRequirement``, ``SchemaDefRequirement``,
``InitialWorkDirRequirement``, ``stdin`` redirection, record and enum schemas,
and CWL v1.2 loops.

``DockerRequirement`` is neither rejected nor honoured: it is recorded as the
required capability ``docker`` on the job — so only a manager declaring that
capability will claim it — and reported as a warning, because *httk₂* runs the
command directly and does not pull, build or enter the image.
"""

import json
import logging
import os
import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib import import_module
from importlib.util import find_spec
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote, urlparse

from httk.core import FileRecord

from httk.workflow._util import sha256_file
from httk.workflow.languages import (
    LanguageOutputsMissingError,
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
from httk.workflow.scaffold import FILES_DIRECTORY, payload_relative

_LOGGER = logging.getLogger(__name__)

#: The package the packaged runner is resolved and digest-pinned within.
PACKAGE = __name__
#: The packaged runner an imported CWL job runs.
RUNNER = "cwl_runner.py"
#: Where the normalized plan and the input object are staged in the payload.
DOCUMENT_FILE = f"{FILES_DIRECTORY}/workflow.cwl.json"
INPUTS_FILE = f"{FILES_DIRECTORY}/inputs.json"
#: Where File and Directory inputs are staged in the payload.
STAGED_DIRECTORY = f"{FILES_DIRECTORY}/inputs"
#: The capability a document asking for a container is filed under.
DOCKER_CAPABILITY = "docker"

#: What ``pip install httk-workflow[cwl]`` provides, named in the error a missing
#: install produces.
_CWL_EXTRA = "pip install httk-workflow[cwl]"

#: The process classes this importer understands.
_PROCESS_CLASSES = ("Workflow", "CommandLineTool")
#: The scalar types this importer understands, CWL name to normalized name.
_SCALAR_TYPES = {
    "string": "string",
    "int": "int",
    "long": "int",
    "float": "float",
    "double": "float",
    "boolean": "boolean",
    "File": "File",
    "Directory": "Directory",
    "Any": "Any",
    "stdout": "stdout",
    "stderr": "stderr",
}
#: Requirements and hints that change nothing here and are carried anyway.
_RECORDED_REQUIREMENTS = (
    "ResourceRequirement",
    "SoftwareRequirement",
    "NetworkAccess",
    "LoadListingRequirement",
    "WorkReuse",
    "ScatterFeatureRequirement",
    "SubworkflowFeatureRequirement",
    "MultipleInputFeatureRequirement",
    "StepInputExpressionRequirement",
    "ToolTimeLimit",
    "EnvVarRequirement",
)
#: Requirements this importer refuses, and why each one is outside the subset.
_REFUSED_REQUIREMENTS = {
    "InlineJavascriptRequirement": "httk runs no JavaScript engine, so only plain parameter references are supported",
    "ShellCommandRequirement": "httk runs an argument vector, never a shell command line",
    "SchemaDefRequirement": "record and enum schemas are outside the supported type set",
    "InitialWorkDirRequirement": "httk stages File inputs itself and does not build a working directory from a listing",
    "InplaceUpdateRequirement": "httk never lets a tool write to its input files",
}

#: One plain parameter reference: a namespace, a name, and dotted field access.
_PLAIN_REFERENCE = re.compile(r"\$\((inputs|runtime)\.[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*\)")
#: Anything at all that CWL would evaluate.
_ANY_EXPRESSION = re.compile(r"\$[({]")

if TYPE_CHECKING:
    from httk.workflow.collecting import JobRecord


class UnsupportedCwlError(ValueError):
    """One CWL feature outside the subset *httk₂* executes."""


class CwlImportError(ValueError):
    """A CWL document that cannot be read, parsed or staged at all."""


@dataclass
class CwlNotes:
    """What one normalization pass accumulates besides the plan itself.

    A note is never a refusal: everything here was accepted, and every entry is
    something an operator should nevertheless be told — a container that will not
    be entered, a hint that was dropped — or something the job must carry, like
    the capabilities a document implies.

    :param warnings: Collect accepted caveats in discovery order.
    :param capabilities: Collect manager capabilities required by the document.
    """

    #: Everything accepted with a caveat, in the order it was found.
    warnings: list[str] = field(default_factory=list)
    #: The capabilities the imported job must require of a manager.
    capabilities: set[str] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Expressions, identifiers and types
# ---------------------------------------------------------------------------


def _name(identifier: object) -> str:
    """Return the short name of one absolute CWL identifier."""

    text = str(identifier)
    fragment = text.partition("#")[2] or text
    return fragment.rsplit("/", 1)[-1]


def _source_name(identifier: object, steps: Sequence[str] = ()) -> str:
    """Return one output source as ``step/port``, or as a workflow input name.

    Identifiers are absolute and nested — an inlined subworkflow's own input is
    spelled ``…#outer/run/message`` — so the last component alone is ambiguous
    and the last *two* would read a nesting level as a step. The step names of
    the workflow being normalized are what tells the two apart.
    """

    text = str(identifier)
    fragment = text.partition("#")[2] or PurePosixPath(text).name
    parts = fragment.split("/")
    if len(parts) > 1 and parts[-2] in set(steps):
        return f"{parts[-2]}/{parts[-1]}"
    return parts[-1]


def _check_expression(value: object, where: str) -> object:
    """Refuse anything CWL would evaluate beyond a plain parameter reference."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            _check_expression(item, where)
        return value
    if not isinstance(value, str) or not _ANY_EXPRESSION.search(value):
        return value
    if "${" in value:
        raise UnsupportedCwlError(
            f"unsupported CWL feature JavaScript expression at {where}: {value!r}; "
            "httk evaluates plain parameter references such as $(inputs.name) and nothing else"
        )
    remaining = _PLAIN_REFERENCE.sub("", value)
    if _ANY_EXPRESSION.search(remaining):
        raise UnsupportedCwlError(
            f"unsupported CWL feature expression at {where}: {value!r}; "
            "httk evaluates plain parameter references such as $(inputs.name) or $(runtime.outdir) only"
        )
    return value


def _normalize_type(value: object, where: str) -> dict[str, object]:
    """Normalize one CWL type into ``{type, items?, optional}``."""

    optional = False
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and not isinstance(value, Mapping):
        members = [item for item in value if item != "null"]
        optional = len(members) != len(list(value))
        if len(members) != 1:
            raise UnsupportedCwlError(
                f"unsupported CWL feature union type at {where}: {value!r}; httk supports one type, optionally null"
            )
        normalized = _normalize_type(members[0], where)
        normalized["optional"] = True
        return normalized
    if isinstance(value, Mapping):
        kind = value.get("type")
        if kind == "array":
            items = _normalize_type(value.get("items"), f"{where} items")
            return {"type": "array", "items": items, "optional": optional}
        raise UnsupportedCwlError(
            f"unsupported CWL feature {kind!r} schema at {where}; "
            "httk supports File, Directory, string, int, long, float, double, boolean, Any and arrays of them"
        )
    text = str(value)
    if text.endswith("?"):
        normalized = _normalize_type(text[:-1], where)
        normalized["optional"] = True
        return normalized
    if text.endswith("[]"):
        return {"type": "array", "items": _normalize_type(text[:-2], where), "optional": optional}
    if text not in _SCALAR_TYPES:
        raise UnsupportedCwlError(
            f"unsupported CWL feature type {text!r} at {where}; "
            "httk supports File, Directory, string, int, long, float, double, boolean, Any and arrays of them"
        )
    return {"type": _SCALAR_TYPES[text], "optional": optional}


def _binding(value: object, where: str) -> dict[str, object] | None:
    """Normalize one ``inputBinding``, refusing what cannot be built into argv."""

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise CwlImportError(f"the inputBinding at {where} is not an object")
    if value.get("shellQuote") is False:
        raise UnsupportedCwlError(
            f"unsupported CWL feature shellQuote at {where}; httk runs an argument vector, never a shell command line"
        )
    if value.get("loadContents"):
        raise UnsupportedCwlError(
            f"unsupported CWL feature loadContents on an inputBinding at {where}; "
            "httk loads contents on output bindings only"
        )
    binding: dict[str, object] = {}
    position = value.get("position")
    if position is not None:
        _check_expression(position, f"{where} position")
        if isinstance(position, bool) or not isinstance(position, int):
            raise UnsupportedCwlError(
                f"unsupported CWL feature computed position at {where}: {position!r}; httk sorts by literal positions"
            )
        binding["position"] = position
    for member in ("prefix", "itemSeparator", "valueFrom"):
        item = value.get(member)
        if item is not None:
            binding[member] = _check_expression(item, f"{where} {member}")
    if value.get("separate") is not None:
        binding["separate"] = bool(value["separate"])
    return binding


# ---------------------------------------------------------------------------
# Requirements
# ---------------------------------------------------------------------------


def _requirements(process: Mapping[str, object], where: str, context: CwlNotes) -> dict[str, object]:
    """Normalize the requirements and hints of one process, refusing the rest."""

    collected: dict[str, object] = {}
    for member, mandatory in (("hints", False), ("requirements", True)):
        entries = process.get(member) or []
        if isinstance(entries, Mapping):
            entries = [{"class": name, **(body if isinstance(body, Mapping) else {})} for name, body in entries.items()]
        if not isinstance(entries, Sequence):
            raise CwlImportError(f"the {member} of {where} are neither an array nor an object")
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise CwlImportError(f"one of the {member} of {where} is not an object")
            name = str(entry.get("class", "")).rsplit("#", 1)[-1]
            if name in _REFUSED_REQUIREMENTS:
                if not mandatory:
                    # A hint is advisory by definition, so an unsupported one is
                    # dropped with a warning rather than refused.
                    context.warnings.append(f"{where}: the {name} hint is ignored")
                    continue
                raise UnsupportedCwlError(f"unsupported CWL feature {name} at {where}: {_REFUSED_REQUIREMENTS[name]}")
            if name == "DockerRequirement":
                context.capabilities.add(DOCKER_CAPABILITY)
                context.warnings.append(
                    f"{where}: DockerRequirement is recorded as the required capability {DOCKER_CAPABILITY!r} "
                    "and is otherwise ignored; httk runs the command directly and never pulls or enters an image"
                )
                collected[name] = {key: item for key, item in entry.items() if key != "class"}
                continue
            if name not in _RECORDED_REQUIREMENTS:
                if not mandatory:
                    context.warnings.append(f"{where}: the {name} hint is ignored")
                    continue
                raise UnsupportedCwlError(
                    f"unsupported CWL feature {name} at {where}; it is not part of the CWL subset httk executes"
                )
            body = {key: _check_expression(item, f"{where} {name}") for key, item in entry.items() if key != "class"}
            collected[name] = body
    return collected


# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------


def _normalize_process(saved: Mapping[str, object], where: str, context: CwlNotes) -> dict[str, object]:
    """Normalize one saved CWL process into the plan the runner executes."""

    process_class = str(saved.get("class", "")).rsplit("#", 1)[-1]
    if process_class not in _PROCESS_CLASSES:
        raise UnsupportedCwlError(
            f"unsupported CWL feature class {process_class or '(none)'} at {where}; "
            f"httk executes {' and '.join(_PROCESS_CLASSES)} documents"
        )
    if saved.get("loop") is not None:
        raise UnsupportedCwlError(f"unsupported CWL feature loop at {where}; httk has no CWL loop support")
    plan: dict[str, object] = {
        "class": process_class,
        "id": _name(saved.get("id", where)),
        "requirements": _requirements(saved, where, context),
        "inputs": _normalize_inputs(saved, where, context),
    }
    if process_class == "CommandLineTool":
        plan.update(_normalize_tool(saved, where, context))
    else:
        plan.update(_normalize_workflow(saved, where, context))
    return plan


def _normalize_inputs(saved: Mapping[str, object], where: str, context: CwlNotes) -> dict[str, object]:
    """Normalize the input parameters of one process."""

    inputs: dict[str, object] = {}
    for entry in _entries(saved.get("inputs"), f"{where} inputs"):
        name = _name(entry.get("id", ""))
        location = f"{where}.{name}"
        _reject_parameter_extras(entry, location)
        parameter: dict[str, object] = {"type": _normalize_type(entry.get("type"), location)}
        if "default" in entry and entry["default"] is not None:
            parameter["default"] = _check_expression(entry["default"], f"{location} default")
        binding = _binding(entry.get("inputBinding"), f"{location} inputBinding")
        if binding is not None:
            parameter["inputBinding"] = binding
        inputs[name] = parameter
    return inputs


def _reject_parameter_extras(entry: Mapping[str, object], location: str) -> None:
    """Refuse the parameter members outside the subset."""

    if entry.get("secondaryFiles"):
        raise UnsupportedCwlError(
            f"unsupported CWL feature secondaryFiles at {location}; httk stages exactly the files an input names"
        )
    if entry.get("streamable"):
        raise UnsupportedCwlError(
            f"unsupported CWL feature streamable at {location}; httk stages whole files and runs no pipes between tools"
        )


def _normalize_tool(saved: Mapping[str, object], where: str, context: CwlNotes) -> dict[str, object]:
    """Normalize the parts that only a ``CommandLineTool`` has."""

    base = saved.get("baseCommand")
    if base is None:
        command: list[str] = []
    elif isinstance(base, str):
        command = [base]
    elif isinstance(base, Sequence):
        command = [str(item) for item in base]
    else:
        raise CwlImportError(f"the baseCommand of {where} is neither a string nor an array")
    arguments: list[dict[str, object]] = []
    for index, entry in enumerate(_as_list(saved.get("arguments"))):
        location = f"{where} argument {index}"
        if isinstance(entry, str):
            arguments.append({"valueFrom": _check_expression(entry, location), "position": 0})
            continue
        if not isinstance(entry, Mapping):
            raise CwlImportError(f"{location} is neither a string nor a binding")
        binding = _binding(entry, location) or {}
        binding.setdefault("position", 0)
        arguments.append(binding)
    tool: dict[str, object] = {
        "baseCommand": command,
        "arguments": arguments,
        "outputs": _normalize_tool_outputs(saved, where),
    }
    if saved.get("stdin") is not None:
        raise UnsupportedCwlError(
            f"unsupported CWL feature stdin redirection at {where}; httk runs an argument vector without a shell, "
            "so a tool reading a file must be given it as an argument"
        )
    for member in ("stdout", "stderr"):
        value = saved.get(member)
        if value is not None:
            tool[member] = _check_expression(value, f"{where} {member}")
    codes = saved.get("successCodes")
    if codes:
        tool["successCodes"] = [int(str(code)) for code in _as_list(codes)]
    if not command and not arguments:
        raise CwlImportError(f"{where} has neither a baseCommand nor arguments, so there is nothing to run")
    return tool


def _normalize_tool_outputs(saved: Mapping[str, object], where: str) -> dict[str, object]:
    """Normalize the outputs of one tool, which httk collects from the disk."""

    outputs: dict[str, object] = {}
    for entry in _entries(saved.get("outputs"), f"{where} outputs"):
        name = _name(entry.get("id", ""))
        location = f"{where}.{name}"
        _reject_parameter_extras(entry, location)
        declared = _normalize_type(entry.get("type"), location)
        binding = entry.get("outputBinding")
        output: dict[str, object] = {"type": declared}
        if declared["type"] in {"stdout", "stderr"}:
            outputs[name] = output
            continue
        if not isinstance(binding, Mapping):
            raise UnsupportedCwlError(
                f"unsupported CWL feature output without an outputBinding at {location}; "
                "httk collects an output from a glob, and has no expression engine to compute one"
            )
        if binding.get("outputEval") is not None:
            raise UnsupportedCwlError(
                f"unsupported CWL feature outputEval at {location}; httk collects the files a glob matched, unchanged"
            )
        glob = binding.get("glob")
        if glob is None:
            raise UnsupportedCwlError(f"unsupported CWL feature outputBinding without a glob at {location}")
        base = declared["items"] if declared["type"] == "array" else declared
        if not isinstance(base, Mapping) or base.get("type") not in {"File", "Directory", "Any"}:
            raise UnsupportedCwlError(
                f"unsupported CWL feature output type {base!r} at {location}; "
                "without an expression engine httk can collect File and Directory outputs only"
            )
        output["glob"] = [_check_expression(item, f"{location} glob") for item in _as_list(glob)]
        if binding.get("loadContents"):
            output["loadContents"] = True
        outputs[name] = output
    return outputs


def _normalize_workflow(saved: Mapping[str, object], where: str, context: CwlNotes) -> dict[str, object]:
    """Normalize the parts that only a ``Workflow`` has."""

    declared = _entries(saved.get("steps"), f"{where} steps")
    names = [_name(entry.get("id", "")) for entry in declared]
    outputs: dict[str, object] = {}
    for entry in _entries(saved.get("outputs"), f"{where} outputs"):
        name = _name(entry.get("id", ""))
        location = f"{where}.{name}"
        _reject_parameter_extras(entry, location)
        source = entry.get("outputSource")
        if source is None:
            raise UnsupportedCwlError(f"unsupported CWL feature workflow output without an outputSource at {location}")
        outputs[name] = {
            "type": _normalize_type(entry.get("type"), location),
            "outputSource": [_source_name(item, names) for item in _as_list(source)],
            "linkMerge": str(entry.get("linkMerge") or "merge_nested"),
        }
    steps: dict[str, object] = {}
    for entry, name in zip(declared, names):
        steps[name] = _normalize_step(entry, f"{where}.{name}", context, names)
    if not steps:
        raise CwlImportError(f"{where} has no steps, so there is nothing to run")
    return {"outputs": outputs, "steps": steps}


def _normalize_step(
    entry: Mapping[str, object],
    where: str,
    context: CwlNotes,
    names: Sequence[str] = (),
) -> dict[str, object]:
    """Normalize one workflow step, its scatter and the process it runs."""

    if entry.get("loop") is not None or str(entry.get("class", "")).endswith("LoopWorkflowStep"):
        raise UnsupportedCwlError(f"unsupported CWL feature loop at {where}; httk has no CWL loop support")
    method = entry.get("scatterMethod")
    scatter = [_name(item) for item in _as_list(entry.get("scatter"))]
    if method is not None and str(method) != "dotproduct":
        raise UnsupportedCwlError(
            f"unsupported CWL feature scatterMethod {method} at {where}; httk scatters one input, "
            "or several of equal length with dotproduct"
        )
    if len(scatter) > 1 and method is None:
        raise UnsupportedCwlError(
            f"unsupported CWL feature scatter over {len(scatter)} inputs without scatterMethod at {where}; "
            "httk supports dotproduct only"
        )
    when = entry.get("when")
    if when is not None:
        text = str(_check_expression(when, f"{where} when"))
        if not _PLAIN_REFERENCE.fullmatch(text):
            raise UnsupportedCwlError(
                f"unsupported CWL feature conditional when at {where}: {text!r}; "
                "httk evaluates one plain parameter reference, and nothing that has to be computed"
            )
    connections: dict[str, object] = {}
    for item in _entries(entry.get("in"), f"{where} in"):
        port = _name(item.get("id", ""))
        location = f"{where}.{port}"
        connection: dict[str, object] = {
            "source": [_source_name(source, names) for source in _as_list(item.get("source"))]
        }
        if "default" in item and item["default"] is not None:
            connection["default"] = _check_expression(item["default"], f"{location} default")
        if item.get("valueFrom") is not None:
            value = str(_check_expression(item["valueFrom"], f"{location} valueFrom"))
            if "$(self" in value or "$(runtime" in value:
                raise UnsupportedCwlError(
                    f"unsupported CWL feature valueFrom reference at {location}: {value!r}; "
                    "httk resolves $(inputs.name) references of a step's own inputs only"
                )
            connection["valueFrom"] = value
        connection["linkMerge"] = str(item.get("linkMerge") or "merge_nested")
        connections[port] = connection
    run = entry.get("run")
    if isinstance(run, str):
        run = _saved_document(run, f"{where} run")
    if not isinstance(run, Mapping):
        raise CwlImportError(f"the run of {where} is neither a document nor a reference to one")
    return {
        "run": _normalize_process(run, f"{where} run", context),
        "in": connections,
        "out": [_name(item) for item in _as_list(entry.get("out"))],
        "scatter": scatter,
        "scatterMethod": "dotproduct" if method is None else str(method),
        "when": None if when is None else str(when),
    }


def _entries(value: object, where: str) -> list[Mapping[str, object]]:
    """Return one CWL array-or-map field as a list of objects carrying ids."""

    if value is None:
        return []
    if isinstance(value, Mapping):
        result: list[Mapping[str, object]] = []
        for name, body in value.items():
            if isinstance(body, Mapping):
                result.append({"id": name, **body})
            else:
                result.append({"id": name, "type": body})
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        entries = []
        for item in value:
            if isinstance(item, Mapping):
                entries.append(item)
            elif isinstance(item, str):
                entries.append({"id": item})
            else:
                raise CwlImportError(f"one entry of {where} is neither an object nor a name")
        return entries
    raise CwlImportError(f"{where} is neither an array nor an object")


def _as_list(value: object) -> list[object]:
    """Return one CWL scalar-or-array member as a list."""

    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return [value]


# ---------------------------------------------------------------------------
# cwl-utils, and the documents it reads
# ---------------------------------------------------------------------------


def _parser() -> Any:
    """Return :mod:`cwl_utils.parser`, or say exactly what to install."""

    if find_spec("cwl_utils") is None:
        raise CwlImportError(
            "importing CWL needs the cwl-utils parser, which is an optional dependency of httk-workflow: "
            f"install it with `{_CWL_EXTRA}`"
        )
    return import_module("cwl_utils.parser")


def _saved_document(uri: str, where: str) -> Mapping[str, object]:
    """Load one referenced CWL document and return it as saved plain JSON."""

    parser = _parser()
    try:
        loaded = parser.load_document_by_uri(uri)
    except Exception as exc:
        raise CwlImportError(f"cannot parse the CWL document referenced at {where} ({uri}): {exc}") from exc
    return _saved(loaded, uri)


def _saved(loaded: object, uri: str) -> Mapping[str, object]:
    """Return one parsed cwl-utils object as the plain JSON document it saves to."""

    parser = _parser()
    if isinstance(loaded, Sequence) and not isinstance(loaded, (str, bytes)):
        # A ``$graph`` document: only a named entry point can be executed.
        candidates = {str(getattr(item, "id", "")): item for item in loaded}
        for identifier, item in candidates.items():
            if identifier.endswith("#main"):
                loaded = item
                break
        else:
            raise UnsupportedCwlError(
                f"unsupported CWL feature $graph without a #main entry point in {uri}; "
                f"httk runs one process per document (found {', '.join(sorted(candidates)) or 'nothing'})"
            )
    saved = parser.save(loaded, relative_uris=False)
    if not isinstance(saved, Mapping):
        raise CwlImportError(f"cwl-utils did not save {uri} as one document")
    return saved


def _upgraded(path: Path, scratch: Path) -> Path:
    """Return a v1.2 copy of the CWL tree around *path*, when one can be made.

    *cwl-upgrader* rewrites one document at a time, and a workflow's ``run``
    references point at its siblings, so the whole tree of ``.cwl`` files is
    mirrored into *scratch* — a temporary directory, never the author's own — and
    upgraded there, which keeps every relative reference resolving. Without the
    package installed, or when the upgrade fails, the original is parsed as it
    is: cwl-utils reads v1.0 and v1.1 too, and the subset this importer accepts
    means the same thing in all three versions.
    """

    if find_spec("cwlupgrader") is None:
        return path
    try:
        upgrader = import_module("cwlupgrader.main")
        root = path.parent.resolve()
        for source in sorted(root.rglob("*.cwl")):
            destination = scratch / source.relative_to(root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            document = upgrader.load_cwl_document(str(source))
            upgraded = upgrader.upgrade_document(document, str(destination.parent), "v1.2")
            upgrader.write_cwl_document(upgraded, str(destination))
        return scratch / path.resolve().relative_to(root)
    except Exception as exc:  # pragma: no cover - depends on the installed package
        _LOGGER.debug("cannot upgrade %s to CWL v1.2, parsing it as it is: %s", path, exc)
        return path


def load_cwl_plan(workflow_path: str | os.PathLike[str]) -> tuple[dict[str, object], CwlNotes]:
    """Parse, check and normalize one CWL document into the plan httk executes.

    :param workflow_path: Read the CWL document at this path.
    :return: The normalized plan and accepted import notes.
    :raises httk.workflow.languages.cwl.CwlImportError: If the document cannot be read or parsed.
    :raises OSError: If accessing the workflow path fails while reading its declared version.
    :raises UnicodeError: If the workflow path is not valid UTF-8 while reading its declared version.
    :raises httk.workflow.languages.cwl.UnsupportedCwlError: If the document uses an unsupported CWL feature.
    """

    path = Path(workflow_path).expanduser()
    if not path.is_file():
        raise CwlImportError(f"the CWL document {path} does not exist")
    parser = _parser()
    version = _declared_version(path)
    with TemporaryDirectory(prefix="httk-cwl-") as scratch:
        source = path if version in {None, "v1.2"} else _upgraded(path, Path(scratch))
        try:
            loaded = parser.load_document_by_uri(str(source.resolve()))
        except Exception as exc:
            raise CwlImportError(f"cannot parse the CWL document {path}: {exc}") from exc
        # Saving happens inside the scratch directory's life as well: a referenced
        # document is loaded from beside the one that referenced it.
        saved = _saved(loaded, str(path))
        context = CwlNotes()
        plan = _normalize_process(saved, path.name, context)
    plan["cwlVersion"] = str(saved.get("cwlVersion") or version or "v1.2")
    plan["source"] = path.name
    return plan, context


def _matches(path: Path) -> bool:
    return path.is_file() and path.suffix == ".cwl"


def _ports(path: Path) -> LanguagePorts:
    plan, _ = load_cwl_plan(path)
    inputs = cast(Mapping[str, object], plan["inputs"])
    outputs = cast(Mapping[str, object], plan["outputs"])
    return LanguagePorts(inputs=tuple(inputs), outputs=tuple(outputs))


def _validate_runner(options: Mapping[str, object], root: Path) -> None:
    del root
    if options:
        raise ValueError(f"unknown runner option {next(iter(options))!r} for cwl")


def _prepare(request: LanguageRequest) -> LanguageScaffold:
    document = request.document
    if document is None:
        raise CwlImportError("a cwl language workflow requires a document")
    plan, notes = load_cwl_plan(document)
    documents = {DOCUMENT_FILE: json.dumps(plan, indent=1, sort_keys=True)}
    plan_inputs = cast(Mapping[str, object], plan.get("inputs", {}))
    output_roles = {
        str(metadata.get("port", name)): str(metadata.get("role", name)) for name, metadata in request.outputs.items()
    }

    def wrap_paths(value: object, schema: object) -> object:
        if isinstance(value, Mapping):
            return value
        if isinstance(schema, Mapping) and schema.get("type") == "array":
            items = schema.get("items")
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return [wrap_paths(item, items) for item in value]
        if isinstance(value, (str, os.PathLike)) and isinstance(schema, Mapping):
            kind = schema.get("type")
            if kind in {"File", "Directory"}:
                return {"class": kind, "path": str(value)}
        return value

    def instantiate(ctx: object) -> object:
        from httk.workflow.scaffold import InstantiateContext

        assert isinstance(ctx, InstantiateContext)
        values: dict[str, object] = {}
        for name, value in ctx.inputs.items():
            metadata = request.inputs.get(name, {})
            port = str(metadata.get("port", name))
            declaration = plan_inputs.get(port)
            schema = declaration.get("type") if isinstance(declaration, Mapping) else declaration
            values[port] = wrap_paths(value, schema)
        staged_documents: dict[str, str] = {}
        staged_files: dict[str, str | os.PathLike[str]] = {}
        staged = stage_cwl_inputs(
            values,
            (request.directory or document.parent).resolve(),
            staged_documents,
            staged_files,
        )

        def destination_for(member: str) -> Path:
            relative = payload_relative(member)
            destination = ctx.payload.joinpath(*relative.parts)
            if destination.exists():
                raise ValueError(f"generated member {member!r} collides with an existing payload member")
            return destination

        for member, text in staged_documents.items():
            destination = destination_for(member)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(text, encoding="utf-8")
        for member, source in staged_files.items():
            destination = destination_for(member)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        destination = destination_for(INPUTS_FILE)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(staged, indent=1, sort_keys=True), encoding="utf-8")
        ctx.parameters["cwl_inputs"] = INPUTS_FILE
        return None

    for warning in notes.warnings:
        _LOGGER.warning("%s", warning)
    return LanguageScaffold(
        documents=documents,
        files={},
        parameters={
            "cwl_document": DOCUMENT_FILE,
            "workflow_language": "cwl",
            "cwl_output_roles": output_roles,
        },
        runner=runner_reference(PACKAGE, RUNNER),
        required_capabilities=tuple(sorted(notes.capabilities)),
        reserved_parameters=("cwl_inputs",),
        warnings=tuple(notes.warnings),
        instantiate=instantiate,
    )


def _file_record(record: "JobRecord", value: Mapping[str, object], prefix: str, port: str, index: int) -> FileRecord:
    recorded = value.get("path")
    if not isinstance(recorded, str):
        raise ValueError(f"{record.workspace_id}:{record.job_id}: CWL File output {port!r} has no string path")
    root = record.workspace_root.resolve()
    recorded_path = Path(recorded)
    actual: Path | None = None
    invalid_absolute = False
    try:
        if recorded_path.is_absolute():
            resolved = recorded_path.resolve()
            if not resolved.is_relative_to(root):
                invalid_absolute = True
            elif resolved.is_file():
                actual = resolved
        else:
            if record.workdir is not None:
                workdir = record.workdir.resolve()
                candidate = (workdir / recorded_path).resolve()
                if candidate.is_relative_to(workdir) and candidate.is_relative_to(root) and candidate.is_file():
                    actual = candidate
        if actual is None and not invalid_absolute and record.data is not None:
            data_root = record.data.resolve()
            published = (data_root / prefix / port).resolve()
            if published.is_relative_to(data_root) and published.is_relative_to(root) and published.is_dir():
                candidate = (published / f"{index:04d}-{recorded_path.name}").resolve()
                if candidate.is_relative_to(published) and candidate.is_relative_to(root) and candidate.is_file():
                    actual = candidate
    except (OSError, RuntimeError):
        actual = None
    if actual is None:
        raise LanguageOutputsMissingError(
            f"{record.workspace_id}:{record.job_id}: CWL File output {port!r} path {recorded!r} "
            "is missing or outside the workspace/workdir/data roots"
        )
    descriptor_path = actual.relative_to(root).as_posix()
    basename = value.get("basename")
    name = basename if isinstance(basename, str) else recorded_path.name
    size: int | None = None
    digest: str | None = None
    if actual is not None:
        digest = sha256_file(actual)
        size = actual.stat().st_size
    elif isinstance(value.get("size"), int) and not isinstance(value.get("size"), bool):
        size = value["size"]
    return FileRecord(
        url=descriptor_path,
        name=name,
        size=size,
        # Host MIME databases would make the identity host-dependent; guessing is banned.
        media_type=None,
        sha256=digest,
    )


def _file_value(record: "JobRecord", value: object, prefix: str, port: str, index: int = 0) -> object:
    if isinstance(value, Mapping) and value.get("class") == "File":
        return _file_record(record, value, prefix, port, index)
    if isinstance(value, list) and any(isinstance(item, Mapping) and item.get("class") == "File" for item in value):
        # Single files are first-class ``files`` entries; lists deliberately stay DataRecord descriptors.
        return [
            (
                {
                    "kind": "File",
                    "path": file.url,
                    "basename": file.name,
                    "sha256": file.sha256,
                    "size": file.size,
                }
                if isinstance(file := _file_value(record, item, prefix, port, offset), FileRecord)
                else file
            )
            for offset, item in enumerate(value)
        ]
    return value


def collect(record: "JobRecord") -> Mapping[str, object]:
    """Convert one CWL runner output document into provenance-capable records."""

    prefix = _parameter(record, "cwl_data_prefix", "cwl")
    raw_outputs = _load_outputs(record, "cwl-outputs.json", prefix)
    roles = _output_roles(record, "cwl_output_roles", raw_outputs)
    return {
        role: converted
        if isinstance(converted := _file_value(record, value, prefix, port), FileRecord)
        else _data_record(role, converted)
        for port, value in raw_outputs.items()
        for role in (str(roles[port]),)
    }


LANGUAGE = WorkflowLanguage(
    name="cwl",
    steps=("start", "enter", "advance", "collect"),
    initial_step="start",
    document_policy="required",
    has_default_collector=True,
    matches=_matches,
    ports=_ports,
    validate_runner=_validate_runner,
    prepare=_prepare,
    collect=collect,
)


def _declared_version(path: Path) -> str | None:
    """Return the ``cwlVersion`` one document declares, read as text."""

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("cwlVersion:"):
            return stripped.partition(":")[2].strip().strip("\"'")
        if stripped.startswith('"cwlVersion"'):
            return stripped.partition(":")[2].strip().strip(" ,\"'")
    return None


# ---------------------------------------------------------------------------
# The input object
# ---------------------------------------------------------------------------


def load_cwl_inputs(inputs_path: str | os.PathLike[str]) -> dict[str, object]:
    """Read one CWL input object, as YAML when a YAML reader is installed.

    :param inputs_path: Read the CWL input object at this path.
    :return: The input values keyed by CWL input name.
    :raises httk.workflow.languages.cwl.CwlImportError: If the input object cannot be read or is not a mapping.
    :raises OSError: If the input path cannot be read.
    :raises UnicodeError: If the input path is not valid UTF-8.
    :raises json.JSONDecodeError: If a JSON input object is malformed.
    """

    path = Path(inputs_path).expanduser()
    if not path.is_file():
        raise CwlImportError(f"the CWL input object {path} does not exist")
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        loaded: object = json.loads(text)
    else:
        loaded = _yaml_load(text, path)
    if not isinstance(loaded, Mapping):
        raise CwlImportError(f"the CWL input object {path} is not a mapping of input names to values")
    return dict(loaded)


def _yaml_load(text: str, path: Path) -> object:
    """Parse one YAML input object with the reader *cwl-utils* brings."""

    parser = _parser()
    try:
        yaml = parser.yaml_no_ts()
        return yaml.load(text)
    except Exception as exc:
        raise CwlImportError(f"cannot read the CWL input object {path} as YAML: {exc}") from exc


def stage_cwl_inputs(
    values: Mapping[str, object],
    base: Path,
    documents: dict[str, str],
    files: dict[str, str | os.PathLike[str]],
) -> dict[str, object]:
    """Return the input object with every File and Directory staged in the payload.

    A staged entry carries a payload-relative ``payload`` member instead of a
    ``path``, so the job is self-contained: it travels to another machine with
    the files its first tool reads.

    :param values: Stage these decoded CWL input values.
    :param base: Resolve relative input paths from this directory.
    :param documents: Add literal files and normalized documents to this mapping.
    :param files: Add source files to this mapping for payload staging.
    :return: The input values rewritten with payload-relative paths.
    :raises httk.workflow.languages.cwl.CwlImportError: If a local input path is missing or invalid.
    :raises httk.workflow.languages.cwl.UnsupportedCwlError: If an input uses an unsupported CWL feature.
    """

    generated_members: dict[str, tuple[str, str]] = {
        payload_relative(member).as_posix(): (member, "existing member") for member in (*documents, *files)
    }

    def reserve(member: str, where: str) -> None:
        normalized = payload_relative(member).as_posix()
        previous = generated_members.get(normalized)
        if previous is not None:
            raise ValueError(
                f"generated member {member!r} for input {where!r} duplicates {previous[0]!r} from {previous[1]!r}"
            )
        generated_members[normalized] = (member, where)

    def stage(value: object, where: str) -> object:
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) and not isinstance(value, Mapping):
            return [stage(item, f"{where}[{index}]") for index, item in enumerate(value)]
        if not isinstance(value, Mapping):
            return value
        kind = value.get("class")
        if kind not in {"File", "Directory"}:
            return {key: stage(item, f"{where}.{key}") for key, item in value.items()}
        if value.get("secondaryFiles"):
            raise UnsupportedCwlError(
                f"unsupported CWL feature secondaryFiles at {where}; httk stages exactly the files an input names"
            )
        if value.get("contents") is not None and value.get("path") is None and value.get("location") is None:
            # A literal file: it has no source on disk, so it is written out.
            name = str(value.get("basename") or f"{where}.txt")
            entry = f"{STAGED_DIRECTORY}/{where}/{name}"
            reserve(entry, where)
            documents[entry] = str(value.get("contents"))
            return {"class": "File", "payload": entry, "basename": name}
        source = _local_path(value, base, where)
        entry = f"{STAGED_DIRECTORY}/{where}/{source.name}"
        if kind == "Directory":
            for item in sorted(source.rglob("*")):
                if item.is_file():
                    member = f"{entry}/{item.relative_to(source).as_posix()}"
                    reserve(member, where)
                    files[member] = item
            return {"class": "Directory", "payload": entry, "basename": source.name}
        reserve(entry, where)
        files[entry] = source
        return {"class": "File", "payload": entry, "basename": source.name}

    return {name: stage(value, name) for name, value in values.items()}


def _local_path(value: Mapping[str, object], base: Path, where: str) -> Path:
    """Return the local file or directory one File or Directory value names."""

    location = value.get("path") or value.get("location")
    if not isinstance(location, str) or not location:
        raise CwlImportError(f"the {value.get('class')} input at {where} has neither a path nor a location")
    if location.startswith("file://"):
        location = unquote(urlparse(location).path)
    elif "://" in location:
        raise UnsupportedCwlError(
            f"unsupported CWL feature remote location at {where}: {location}; httk stages local files only"
        )
    path = Path(location).expanduser()
    if not path.is_absolute():
        path = base / path
    if not path.exists():
        raise CwlImportError(f"the {value.get('class')} input at {where} does not exist: {path}")
    return path


# ---------------------------------------------------------------------------
# The import itself
# ---------------------------------------------------------------------------
