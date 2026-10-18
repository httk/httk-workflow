"""Prepare atomate2 jobflow Maker workflows for httk jobs.

Declared workflow parameters are Maker constructor configuration: the runner builds ``Class(**params)`` for the
``maker=`` form and ``dataclasses.replace(maker, **params)`` for the document form, so declared parameters override
document Maker fields; language plumbing parameters are reserved-prefixed and never Maker-bound.
"""

import importlib
import json
import os
import re
import shutil
import stat
from collections.abc import Mapping, Sequence
from dataclasses import replace
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from httk.workflow.compat import (
    LanguagePorts,
    LanguageRequest,
    LanguageScaffold,
    WorkflowLanguage,
    _data_record,
    _load_outputs,
    _output_roles,
    _parameter,
)
from httk.workflow.models import normalize_resources
from httk.workflow.scaffold import FILES_DIRECTORY, payload_relative

if TYPE_CHECKING:
    from httk.workflow.collecting import JobRecord
    from httk.workflow.runtime_builders import JobSpec

PACKAGE = __name__
RUNNER = "jobflow_runner.py"
DOCUMENT_FILE = f"{FILES_DIRECTORY}/maker.json"
STAGED_DIRECTORY = f"{FILES_DIRECTORY}/inputs"
DEFAULT_DATA_PREFIX = "jobflow"
OUTPUTS_FILE = "jobflow-outputs.json"

_MAKER_SPEC = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$")


class JobflowFormatError(ValueError):
    """A jobflow Maker document or runner configuration is invalid."""


def _maker_document(raw: object, source: str) -> dict[str, object]:
    if not isinstance(raw, dict) or not isinstance(raw.get("@module"), str) or not isinstance(raw.get("@class"), str):
        raise JobflowFormatError(f"{source} must be a JSON object with string @module and @class members")
    return raw


def _read_document(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise JobflowFormatError(f"cannot read jobflow Maker document {path}: {exc}") from exc
    return _maker_document(raw, str(path))


def _matches(path: Path) -> bool:
    if not path.is_file() or path.suffix != ".json":
        return False
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return False
    return isinstance(raw, dict) and isinstance(raw.get("@module"), str) and isinstance(raw.get("@class"), str)


def _ports(path: Path) -> LanguagePorts:
    del path
    return LanguagePorts(inputs=(), outputs=("output",))


def _find_module_spec_without_import(module: str) -> ModuleSpec | None:
    """Find a module by path without importing any parent package.

    Mirrors :func:`httk.workflow.precheck._find_module_spec_without_import`: the
    module tree is walked with :class:`~importlib.machinery.PathFinder` so a
    missing dependency never triggers a parent package's import side effects.

    :param module: The dotted module name to resolve.
    :return: The resolved spec, or ``None`` when the module is not importable here.
    """

    parent_locations: Sequence[str] | None = None
    qualified = ""
    parts = module.split(".")
    spec: ModuleSpec | None = None
    for index, part in enumerate(parts):
        qualified = part if not qualified else f"{qualified}.{part}"
        spec = PathFinder.find_spec(qualified, parent_locations)
        if spec is None:
            return None
        if index < len(parts) - 1:
            if spec.submodule_search_locations is None:
                return None
            parent_locations = spec.submodule_search_locations
    return spec


def _job_resources(value: object) -> list[list[object]]:
    """Return the ``[glob, resources]`` rows of ``[workflow.runner.resources]`` in manifest order, in seconds."""
    if not isinstance(value, Mapping):
        raise ValueError("runner option 'resources' must be a table of jobflow job-name globs")
    return [
        [str(glob), normalize_resources(table, f"[workflow.runner.resources.{glob!r}]")]
        for glob, table in value.items()
    ]


#: The required and optional members of each ``jobflow_overrides`` kind.
_OVERRIDE_MEMBERS = {
    "maker_kwargs": ({"update"}, {"name_filter", "class_filter", "nested", "dict_mod"}),
    "config": ({"config"}, {"name_filter", "attributes", "dynamic", "dict_mod"}),
    "function": ({"function"}, {"kwargs"}),
}


def _validate_overrides(value: object) -> None:
    """Refuse a malformed ``jobflow_overrides`` job parameter at job creation.

    :param value: The supplied parameter value.
    :raises ValueError: If the value is not a list of well-formed, JSON-only override operations.
    """
    if not isinstance(value, list):
        raise ValueError("job parameter 'jobflow_overrides' must be a list of override operations")
    for index, item in enumerate(value):
        where = f"jobflow_overrides[{index}]"
        kind = item.get("kind") if isinstance(item, Mapping) else None
        if not isinstance(kind, str) or kind not in _OVERRIDE_MEMBERS:
            raise ValueError(f"{where} must be an object whose kind is one of {', '.join(_OVERRIDE_MEMBERS)}")
        assert isinstance(item, Mapping)
        required, optional = _OVERRIDE_MEMBERS[kind]
        if missing := sorted(required - item.keys()):
            raise ValueError(f"{where} lacks {missing[0]!r}")
        if unknown := sorted(item.keys() - required - optional - {"kind"}):
            raise ValueError(f"{where} has unknown member {unknown[0]!r}")
        for name in ("update", "config", "kwargs"):
            if name in item and not isinstance(item[name], Mapping):
                raise ValueError(f"{where}.{name} must be an object")
        for name in ("nested", "dynamic", "dict_mod"):
            if name in item and not isinstance(item[name], bool):
                raise ValueError(f"{where}.{name} must be a boolean")
        if item.get("name_filter") is not None and not isinstance(item["name_filter"], str):
            raise ValueError(f"{where}.name_filter must be a string or null")
        attributes = item.get("attributes")
        if attributes is not None and not (
            isinstance(attributes, str)
            or (isinstance(attributes, list) and all(isinstance(a, str) for a in attributes))
        ):
            raise ValueError(f"{where}.attributes must be a string, a list of strings or null")
        for name in ("class_filter", "function"):
            spec = item.get(name)
            if spec is not None and (not isinstance(spec, str) or _MAKER_SPEC.fullmatch(spec) is None):
                raise ValueError(f"{where}.{name} must be a 'module:name' string")
        try:
            json.dumps(item, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{where} must hold JSON values only: {exc}") from exc


def _validate_runner(options: Mapping[str, object], root: Path) -> None:
    del root
    for key, value in options.items():
        if key == "resources":
            _job_resources(value)
            continue
        if key != "maker":
            raise ValueError(f"unknown runner option {key!r} for jobflow")
        if not isinstance(value, str) or _MAKER_SPEC.fullmatch(value) is None:
            raise ValueError("runner option 'maker' must be a dotted Maker spec like 'module:Class'")


def _verify_maker_class(maker: str) -> None:
    """Verify a ``module:Class`` Maker spec names a real class, at submission time.

    When the Maker module is present on the preparing machine the class is verified
    now — importing is acceptable at submission, as CWL's parser runs here too, and
    ``_prepare`` is only reached when a job is created. A module that is not
    installed here (a compute-node dependency, or a Maker staged onto ``PYTHONPATH``
    only at run time) stays accepted; precheck and the runner report it if it is
    truly missing.

    :param maker: The ``module:Class`` Maker spec to verify.
    :raises JobflowFormatError: If the resolvable, importable module defines no such class.
    """

    module_name, _, class_name = maker.partition(":")
    if _find_module_spec_without_import(module_name) is None:
        return
    try:
        module = importlib.import_module(module_name)
    except Exception:
        # A spec-resolvable but unimportable module is left to run time.
        return
    if not hasattr(module, class_name):
        raise JobflowFormatError(
            f"runner option 'maker' names class {class_name!r}, which module {module_name!r} does not define"
        )


def _staged_member(payload: Path, label: str, name: str) -> Path:
    """Return a fresh ``files/inputs/<label>/<name>`` payload path, with its parent directory created."""
    member = PurePosixPath(STAGED_DIRECTORY) / label / name
    destination = payload.joinpath(*payload_relative(member.as_posix()).parts)
    if destination.exists():
        raise ValueError(f"generated member {member.as_posix()!r} collides with an existing payload member")
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination


def _copy_regular(source: str, destination: str) -> None:
    """Copy one regular file for :func:`shutil.copytree`, refusing special files and dangling links."""
    if not stat.S_ISREG(os.stat(source).st_mode):
        raise OSError(f"{source} is not a regular file")
    shutil.copy2(source, destination)


def _is_httk_structure(value: object) -> bool:
    """Report whether *value* is a live httk structure (only when httk-atomistic is installed)."""
    try:
        from httk.atomistic.models.structure.backend import StructureBackend
    except ImportError:
        return False
    return isinstance(value, StructureBackend)


def _prepare(request: LanguageRequest) -> LanguageScaffold:
    options = request.runner_options
    has_document = request.document is not None
    has_maker = "maker" in options
    if has_document == has_maker:
        raise JobflowFormatError("give [workflow.runner] either maker= or document=, not both/neither")

    documents: dict[str, str | bytes] = {}
    parameters: dict[str, object] = {"workflow_language": "jobflow"}
    if has_document:
        assert request.document is not None
        loaded = _read_document(request.document)
        documents[DOCUMENT_FILE] = json.dumps(loaded, indent=2, sort_keys=True)
        parameters["jobflow_document"] = DOCUMENT_FILE
    else:
        maker = options["maker"]
        _verify_maker_class(str(maker))
        parameters["jobflow_maker"] = maker
    if "resources" in options:
        parameters["jobflow_job_resources"] = _job_resources(options["resources"])
    declared_parameters = tuple(sorted(request.parameters))
    if declared_parameters:
        parameters["jobflow_maker_parameters"] = declared_parameters
        defaults = {name: metadata["default"] for name, metadata in request.parameters.items() if "default" in metadata}
        if defaults:
            parameters["jobflow_maker_defaults"] = defaults
    parameters["jobflow_output_roles"] = {
        str(metadata.get("port", name)): str(metadata.get("role", name)) for name, metadata in request.outputs.items()
    }
    root = (request.directory or (request.document.parent if request.document is not None else Path.cwd())).resolve()

    def instantiate(ctx: object) -> object:
        """Stage file inputs and preserve literal JSON inputs for jobflow."""

        from httk.workflow.scaffold import InstantiateContext, _has_path_separator

        assert isinstance(ctx, InstantiateContext)
        if "jobflow_overrides" in ctx.parameters:
            _validate_overrides(ctx.parameters["jobflow_overrides"])
        inputs: dict[str, dict[str, object]] = {}
        for name, value in ctx.inputs.items():
            metadata = request.inputs.get(name, {})
            label = str(metadata.get("port", name))
            if _is_httk_structure(value):
                # A live httk structure travels as a POSCAR that the runner reads with pymatgen.
                if getattr(value, "site_moments", None) is not None:
                    raise ValueError(
                        f"workflow input {name!r} is an httk structure with site moments, which this input "
                        "cannot carry; pass a pymatgen Structure with 'magmom' site properties as Monty JSON "
                        "(structure.as_dict()) instead"
                    )
                import httk.core

                destination = _staged_member(ctx.payload, label, "POSCAR")
                httk.core.save(value, destination)
                inputs[label] = {"kind": "path", "value": destination.relative_to(ctx.payload).as_posix()}
                continue
            source: Path | None = None
            resolved: Path | None = None
            text: str | None = None
            if isinstance(value, (str, os.PathLike)):
                text = os.fspath(value)
                candidate = Path(text)
                resolved = (candidate if candidate.is_absolute() else root / candidate).resolve()
                if metadata.get("directory"):
                    if not resolved.is_dir():
                        where = "" if candidate.is_absolute() else f" (relative paths resolve against {root})"
                        raise ValueError(
                            f"workflow input {name!r} is declared a directory, but {resolved} is not a directory{where}"
                        )
                    destination = _staged_member(ctx.payload, label, resolved.name)
                    try:
                        # Links are followed so the staged tree is self-contained.
                        shutil.copytree(resolved, destination, copy_function=_copy_regular)
                    except (shutil.Error, OSError) as exc:
                        raise ValueError(f"workflow input {name!r}: cannot stage directory {resolved}: {exc}") from exc
                    inputs[label] = {"kind": "directory", "value": destination.relative_to(ctx.payload).as_posix()}
                    continue
                if resolved.is_file():
                    source = resolved
            elif metadata.get("directory"):
                raise ValueError(f"workflow input {name!r} is declared a directory; supply a directory path")
            if source is None:
                # jobflow resolves file inputs against the package/document root,
                # so the mistyped-path decision is made from that resolved path —
                # not the current directory. A value that carries a separator, or
                # that exists as a file somewhere the run can see (but not under
                # the root), is a real file the run cannot reach, not a literal;
                # the message names where jobflow actually looked.
                if resolved is not None and text is not None:
                    probe = Path(text).expanduser()
                    if resolved.is_dir() or probe.is_dir():
                        raise ValueError(
                            f"workflow input {name!r} is a directory, not a file: {resolved}; supply a regular "
                            "file, or declare the input with directory = true"
                        )
                    if _has_path_separator(text) or probe.exists():
                        if Path(text).is_absolute():
                            raise ValueError(
                                f"workflow input {name!r} looks like a file path but no file exists at {resolved}; "
                                "supply an existing file, or a literal value without path separators"
                            )
                        raise ValueError(
                            f"workflow input {name!r} looks like a file path but no file exists under the workflow "
                            f"root at {resolved}; supply an existing file, or a literal value without path separators"
                        )
                try:
                    json.dumps(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"workflow input {name!r} must be JSON-serializable") from exc
                inputs[label] = {"kind": "value", "value": value}
                continue
            destination = _staged_member(ctx.payload, label, source.name)
            shutil.copyfile(source, destination)
            inputs[label] = {"kind": "path", "value": destination.relative_to(ctx.payload).as_posix()}
        ctx.parameters["jobflow_inputs"] = inputs
        return None

    def finalize(spec: "JobSpec") -> "JobSpec":
        """Give jobflow jobs unlimited activations."""
        return replace(spec, maximum_activations=None)

    return LanguageScaffold(
        documents=documents,
        files={},
        parameters=parameters,
        reserved_parameters=(
            "jobflow_inputs",
            "jobflow_maker",
            "jobflow_document",
            "jobflow_job_resources",
            "jobflow_maker_parameters",
            "jobflow_maker_defaults",
        ),
        caller_parameters=("jobflow_overrides",),
        required_capabilities=(),
        instantiate=instantiate,
        finalize=finalize,
    )


def collect(record: "JobRecord") -> Mapping[str, object]:
    """Convert one jobflow runner output document into provenance records.

    :param record: The completed job record containing runner outputs.
    :return: Output records keyed by their declared workflow roles.
    """

    prefix = _parameter(record, "jobflow_data_prefix", DEFAULT_DATA_PREFIX)
    raw_outputs = _load_outputs(record, OUTPUTS_FILE, prefix)
    roles = _output_roles(record, "jobflow_output_roles", raw_outputs)
    return {str(roles[port]): _data_record(str(roles[port]), value) for port, value in raw_outputs.items()}


def document_from_maker(maker: object) -> str:
    """Serialize one MSONable Maker using monty.

    :param maker: A Maker object exposing ``as_dict()``.
    :return: A sorted, indented JSON Maker document.
    :raises JobflowFormatError: If *maker* has no usable MSONable document.

    Monty must be installed when this helper is called.
    """

    as_dict = getattr(maker, "as_dict", None)
    if not callable(as_dict):
        raise JobflowFormatError("maker must provide as_dict(); monty is required to serialize it")
    from monty.json import MontyEncoder

    try:
        serialized = json.dumps(as_dict(), cls=MontyEncoder, indent=2, sort_keys=True)
        _maker_document(json.loads(serialized), "maker")
    except (TypeError, ValueError) as exc:
        raise JobflowFormatError(f"maker did not serialize to a valid MSONable document: {exc}") from exc
    return serialized


LANGUAGE = WorkflowLanguage(
    name="jobflow",
    steps=("start", "advance", "enter"),
    initial_step="start",
    document_policy="optional",
    open_ports=True,
    has_default_collector=True,
    matches=_matches,
    ports=_ports,
    validate_runner=_validate_runner,
    prepare=_prepare,
    collect=collect,
    # pymatgen is required only when a job supplies structure inputs, so it is
    # named in the precheck finding text rather than checked unconditionally.
    required_modules=("jobflow", "maggma", "monty"),
)
