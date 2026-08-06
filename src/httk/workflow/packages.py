"""Load directory-based workflow packages from ``workflow.toml`` manifests."""

from __future__ import annotations

import hashlib
import json
import sys
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, cast

from ._util import tree_digest, validate_parameters
from .errors import FormatError
from .models import validate_declarations
from .scaffold import WorkflowProvider, payload_relative, register_workflow

MANIFEST_NAME = "workflow.toml"

__all__ = [
    "MANIFEST_NAME",
    "load_workflow_package",
    "parse_workflow_manifest",
    "workflow_declaration_from_manifest",
]


class _PinnedTreeError(ValueError):
    """The pinned tree changed before a hook member could be executed."""


def _error(directory: Path, message: str) -> ValueError:
    return ValueError(f"{directory}: {message}")


def _table(value: object, path: str, directory: Path) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _error(directory, f"{path} must be a table")
    return value


def _unknown(table: Mapping[str, object], allowed: set[str], path: str, directory: Path) -> None:
    for key in table:
        if key not in allowed:
            dotted = path[1:-1] if path.startswith("[") and path.endswith("]") else path
            prefix = f"{dotted}." if dotted else ""
            raise _error(directory, f"unknown key [{prefix}{key}]")


def _string(table: Mapping[str, object], key: str, path: str, directory: Path, *, required: bool = False) -> str | None:
    if key not in table:
        if required:
            raise _error(directory, f"{path}.{key} is required")
        return None
    value = table[key]
    if not isinstance(value, str) or not value:
        raise _error(directory, f"{path}.{key} must be a nonempty string")
    return value


def _optional_string(table: Mapping[str, object], key: str, path: str, directory: Path) -> str | None:
    if key not in table:
        return None
    value = table[key]
    if not isinstance(value, str):
        raise _error(directory, f"{path}.{key} must be a string")
    return value


def _member(directory: Path, value: object, path: str, *, python: bool = False) -> str:
    if not isinstance(value, str) or not value:
        raise _error(directory, f"{path} must name a relative regular file")
    relative = PurePosixPath(value)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise _error(directory, f"{path} must name a relative member without '..': {value!r}")
    if python and relative.suffix != ".py":
        raise _error(directory, f"{path} must name a .py member: {value!r}")
    candidate = directory.joinpath(*relative.parts)
    root = directory.resolve()
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise _error(directory, f"{path} member does not exist: {value!r}") from exc
    if not resolved.is_relative_to(root) or candidate.is_symlink() or not candidate.is_file():
        raise _error(directory, f"{path} must name a regular member below the package: {value!r}")
    for index in range(1, len(relative.parts)):
        if directory.joinpath(*relative.parts[:index]).is_symlink():
            raise _error(directory, f"{path} must not traverse a symlink: {value!r}")
    return relative.as_posix()


def _validate_name(name: object, path: str, directory: Path) -> str:
    if not isinstance(name, str) or not name:
        raise _error(directory, f"{path} must be a nonempty string")
    return name


def _validate_parameter_table(
    raw: Mapping[str, object], name: str, directory: Path
) -> tuple[dict[str, object], str | None]:
    path = f"[workflow.parameters.{name}]"
    _unknown(raw, {"destination", "description", "entry_type", "ref", "role"}, path, directory)
    destination = raw.get("destination")
    if destination is not None:
        try:
            validate_parameters({name: destination})
            payload_relative(str(destination))
        except (FormatError, ValueError) as exc:
            raise _error(directory, f"{path}.destination is invalid: {exc}") from exc
    role = _optional_string(raw, "role", path, directory) or name
    result: dict[str, object] = {"role": role}
    if destination is not None:
        result["destination"] = destination
    for key in ("description", "entry_type", "ref"):
        value = _optional_string(raw, key, path, directory)
        if value is not None:
            result[key] = value
    return result, None if destination is None else str(destination)


def _validate_inputs(raw: Mapping[str, object], directory: Path) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for name, value in raw.items():
        parameter = _validate_name(name, "[workflow.inputs] name", directory)
        table = _table(value, f"[workflow.inputs.{parameter}]", directory)
        path = f"[workflow.inputs.{parameter}]"
        _unknown(table, {"type", "description", "default"}, path, directory)
        input_type = _optional_string(table, "type", path, directory)
        if input_type is not None and input_type not in {"string", "number", "integer", "boolean", "array", "object"}:
            raise _error(directory, f"{path}.type is not one of string, number, integer, boolean, array, object")
        description = _optional_string(table, "description", path, directory)
        if "default" in table and input_type is not None and not _matches_input_type(table["default"], input_type):
            raise _error(directory, f"{path}.default does not match type {input_type!r}")
        entry: dict[str, object] = {}
        if input_type is not None:
            entry["type"] = input_type
        if description is not None:
            entry["description"] = description
        if "default" in table:
            entry["default"] = table["default"]
        result[parameter] = entry
    return result


def _matches_input_type(value: object, input_type: str) -> bool:
    if input_type == "string":
        return isinstance(value, str)
    if input_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if input_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if input_type == "boolean":
        return isinstance(value, bool)
    if input_type == "array":
        return isinstance(value, list)
    return isinstance(value, dict)


def _validate_outputs(
    raw: Mapping[str, object], parameters: Mapping[str, Mapping[str, object]], directory: Path
) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    roles: set[str] = set()
    for name, value in raw.items():
        output = _validate_name(name, "[workflow.outputs] name", directory)
        table = _table(value, f"[workflow.outputs.{output}]", directory)
        path = f"[workflow.outputs.{output}]"
        _unknown(table, {"entry_type", "ref", "description", "product_of", "role"}, path, directory)
        entry_type = _string(table, "entry_type", path, directory, required=True)
        role = _optional_string(table, "role", path, directory) or output
        if role in roles:
            raise _error(directory, f"{path}.role is duplicated")
        roles.add(role)
        product_of = _optional_string(table, "product_of", path, directory)
        if product_of is not None and product_of not in parameters:
            raise _error(directory, f"{path}.product_of names missing parameter {product_of!r}")
        if product_of is not None and "entry_type" not in parameters[product_of]:
            raise _error(directory, f"{path}.product_of parameter {product_of!r} is not entry-typed")
        entry: dict[str, object] = {"entry_type": entry_type, "role": role}
        for key in ("ref", "description"):
            optional = _optional_string(table, key, path, directory)
            if optional is not None:
                entry[key] = optional
        if product_of is not None:
            entry["product_of"] = product_of
        result[output] = entry
    return result


def _matches_parameter_roles(document: Mapping[str, object], provider: WorkflowProvider, directory: Path) -> None:
    parameters = document.get("parameters", [])
    if not isinstance(parameters, list):
        raise _error(directory, "external declaration parameters must be an array")
    expected = {
        str(entry.get("role", name)): entry
        for name, entry in provider._parameter_metadata.items()
        if "entry_type" in entry
    }
    actual: dict[str, Mapping[str, object]] = {}
    for index, entry in enumerate(parameters):
        if not isinstance(entry, Mapping) or not isinstance(entry.get("name"), str):
            raise _error(directory, f"external declaration parameters[{index}] must name a role")
        name = str(entry["name"])
        if name in actual:
            raise _error(directory, f"external declaration parameter {name!r} is duplicated")
        actual[name] = entry
        if name not in expected:
            raise _error(
                directory,
                f"external declaration parameter {name!r} is not a manifest parameter/input role "
                "(or is not entry-typed)",
            )
        if entry.get("entry_type") != expected[name].get("entry_type"):
            raise _error(directory, f"external declaration parameter {name!r} has an incompatible entry_type")
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    if missing:
        raise _error(directory, f"external declaration is missing manifest parameter role {missing[0]!r}")
    if extra:
        raise _error(directory, f"external declaration has extra parameter role {extra[0]!r}")


def _validate_external_declaration(
    document: object, provider: WorkflowProvider, directory: Path, declaration_uri: str | None
) -> dict[str, object]:
    if not isinstance(document, dict):
        raise _error(directory, "external workflow declaration must be a JSON object")
    if declaration_uri is not None and document.get("$id") != declaration_uri:
        raise _error(directory, "external declaration $id does not match workflow.declaration_uri")
    _matches_parameter_roles(document, provider, directory)
    outputs = document.get("output_types", [])
    if not isinstance(outputs, list):
        raise _error(directory, "external declaration output_types must be an array")
    output_roles = {str(entry.get("role", name)): entry for name, entry in provider.outputs.items()}
    actual_outputs: dict[str, Mapping[str, object]] = {}
    for index, entry in enumerate(outputs):
        if not isinstance(entry, Mapping) or not isinstance(entry.get("name"), str):
            raise _error(directory, f"external declaration output_types[{index}] must name a role")
        name = str(entry["name"])
        if name in actual_outputs:
            raise _error(directory, f"external declaration output {name!r} is duplicated")
        actual_outputs[name] = entry
        expected = output_roles.get(name)
        if expected is None:
            raise _error(directory, f"external declaration output {name!r} is not a manifest output role")
        if entry.get("entry_type") != expected.get("entry_type"):
            raise _error(directory, f"external declaration output {name!r} has an incompatible entry_type")
        expected_product = None
        if "product_of" in expected:
            parameter = provider._parameter_metadata.get(str(expected["product_of"]))
            expected_product = parameter.get("role", expected["product_of"]) if parameter is not None else None
            if expected_product is None:
                raise _error(directory, f"manifest output {name!r} has a dangling product_of")
        if entry.get("product_of") != expected_product:
            raise _error(directory, f"external declaration output {name!r} has an incompatible product_of")
    missing = sorted(set(output_roles) - set(actual_outputs))
    extra = sorted(set(actual_outputs) - set(output_roles))
    if missing:
        raise _error(directory, f"external declaration is missing manifest output role {missing[0]!r}")
    if extra:
        raise _error(directory, f"external declaration has extra output role {extra[0]!r}")
    return document


def workflow_declaration_from_manifest(provider: WorkflowProvider) -> dict[str, object]:
    """Generate the OPTIMADE workflow declaration carried by a package."""

    document: dict[str, object] = {}
    if provider.declaration_uri is not None:
        document["$id"] = provider.declaration_uri
    if provider.summary:
        document["description"] = provider.summary
    parameters: list[dict[str, object]] = []
    for name, entry in provider._parameter_metadata.items():
        if "entry_type" not in entry:
            continue
        parameter = {"name": entry.get("role", name), "entry_type": entry["entry_type"]}
        for key in ("ref", "description"):
            if key in entry:
                parameter[key] = entry[key]
        parameters.append(parameter)
    outputs: list[dict[str, object]] = []
    for name, entry in provider.outputs.items():
        output: dict[str, object] = {"name": entry.get("role", name), "entry_type": entry["entry_type"]}
        for key in ("ref", "description"):
            if key in entry:
                output[key] = entry[key]
        if "product_of" in entry:
            parameter_metadata = provider._parameter_metadata[str(entry["product_of"])]
            output["product_of"] = parameter_metadata.get("role", entry["product_of"])
        outputs.append(output)
    document["parameters"] = parameters
    document["output_types"] = outputs
    return document


def parse_workflow_manifest(directory: str | Path) -> WorkflowProvider:
    """Parse and validate one directory workflow package."""

    root = Path(directory).expanduser()
    if not root.is_dir() or root.is_symlink():
        raise _error(root, "workflow package must be a directory")
    manifest = root / MANIFEST_NAME
    manifest_text = ""
    try:
        manifest_text = manifest.read_text(encoding="utf-8")
        raw = tomllib.loads(manifest_text)
    except (OSError, UnicodeError) as exc:
        raise _error(root, f"cannot read {MANIFEST_NAME}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        lineno = getattr(exc, "lineno", None) or len(manifest_text.splitlines())
        colno = getattr(exc, "colno", None) or len(manifest_text.rsplit("\n", 1)[-1]) + 1
        location = f" (line {lineno}, column {colno})"
        raise _error(root, f"invalid {MANIFEST_NAME}{location}: {exc}") from exc
    if not isinstance(raw, dict):  # pragma: no cover - tomllib always returns a dict
        raise _error(root, "manifest must be a table")
    _unknown(raw, {"workflow"}, "", root)
    workflow = _table(raw.get("workflow"), "[workflow]", root)
    _unknown(
        workflow,
        {
            "id",
            "alias",
            "description",
            "declaration_uri",
            "declaration_file",
            "runner",
            "instantiate",
            "postprocess",
            "parameters",
            "inputs",
            "outputs",
        },
        "[workflow]",
        root,
    )
    workflow_id = _string(workflow, "id", "[workflow]", root, required=True)
    assert workflow_id is not None
    if any(character.isspace() for character in workflow_id or ""):
        raise _error(root, "[workflow].id must not contain whitespace")
    alias = _optional_string(workflow, "alias", "[workflow]", root)
    if alias is not None:
        import re

        if re.fullmatch(r"[a-z0-9._-]+", alias) is None:
            raise _error(root, "[workflow].alias must match [a-z0-9._-]+")
    description = _optional_string(workflow, "description", "[workflow]", root) or ""
    declaration_uri = _optional_string(workflow, "declaration_uri", "[workflow]", root)

    runner = _table(workflow.get("runner"), "[workflow.runner]", root)
    _unknown(runner, {"entry", "initial_step", "steps", "data_mode", "workdir_mode"}, "[workflow.runner]", root)
    entry = _member(root, runner.get("entry", "run"), "[workflow.runner].entry")
    if entry != "run":
        raise _error(root, "custom entries are not yet supported; the tree entry point must be named run")
    steps_raw = runner.get("steps")
    if (
        not isinstance(steps_raw, list)
        or not steps_raw
        or not all(isinstance(step, str) and step for step in steps_raw)
    ):
        raise _error(root, "[workflow.runner].steps must be a nonempty list of strings")
    steps = tuple(str(step) for step in steps_raw)
    initial_step = _optional_string(runner, "initial_step", "[workflow.runner]", root)
    if initial_step is None:
        if "start" in steps:
            initial_step = "start"
        elif len(steps) == 1:
            initial_step = steps[0]
        else:
            raise _error(root, "[workflow.runner].initial_step is required when steps omit 'start'")
    if initial_step not in steps:
        raise _error(root, f"[workflow.runner].initial_step {initial_step!r} is not in steps")
    data_mode = runner.get("data_mode", "none")
    workdir_mode = runner.get("workdir_mode", "persistent")
    if data_mode not in {"none", "transactional"}:
        raise _error(root, "[workflow.runner].data_mode must be 'none' or 'transactional'")
    if workdir_mode not in {"persistent", "isolated"}:
        raise _error(root, "[workflow.runner].workdir_mode must be 'persistent' or 'isolated'")

    raw_parameters = _table(workflow.get("parameters", {}), "[workflow.parameters]", root)
    parameter_metadata: dict[str, dict[str, object]] = {}
    parameters: dict[str, str | None] = {}
    roles: set[str] = set()
    for name, value in raw_parameters.items():
        parameter = _validate_name(name, "[workflow.parameters] name", root)
        table = _table(value, f"[workflow.parameters.{parameter}]", root)
        metadata, destination = _validate_parameter_table(table, parameter, root)
        if str(metadata["role"]) in roles:
            raise _error(root, f"[workflow.parameters.{parameter}].role is duplicated")
        roles.add(str(metadata["role"]))
        parameter_metadata[parameter] = metadata
        parameters[parameter] = destination

    instantiate_file: str | None = None
    if "instantiate" in workflow:
        instantiate = _table(workflow["instantiate"], "[workflow.instantiate]", root)
        _unknown(instantiate, {"file"}, "[workflow.instantiate]", root)
        instantiate_file = _member(root, instantiate.get("file"), "[workflow.instantiate].file", python=True)
    if any(destination is None for destination in parameters.values()) and instantiate_file is None:
        raise _error(root, "hook-consumed workflow parameters require [workflow.instantiate]")

    postprocess_file: str | None = None
    if "postprocess" in workflow:
        postprocess = _table(workflow["postprocess"], "[workflow.postprocess]", root)
        _unknown(postprocess, {"file"}, "[workflow.postprocess]", root)
        postprocess_file = _member(root, postprocess.get("file"), "[workflow.postprocess].file", python=True)

    inputs = _validate_inputs(_table(workflow.get("inputs", {}), "[workflow.inputs]", root), root)
    outputs = _validate_outputs(
        _table(workflow.get("outputs", {}), "[workflow.outputs]", root), parameter_metadata, root
    )
    provider = WorkflowProvider(
        workflow_id=workflow_id,
        runner_package=None,
        runner_file=None,
        initial_step=initial_step,
        alias=alias,
        steps=steps,
        data_mode=data_mode,  # type: ignore[arg-type]
        workdir_mode=workdir_mode,  # type: ignore[arg-type]
        summary=description,
        parameters=parameters,
        instantiate=instantiate_file is not None,
        declarations={},
        directory=root.resolve(),
        entry=entry,
        instantiate_file=instantiate_file,
        postprocess_file=postprocess_file,
        inputs=inputs,
        outputs=outputs,
        declaration_uri=declaration_uri,
        declaration_file=None,
        _parameter_metadata=parameter_metadata,
    )
    declaration_member: str | None = None
    if "declaration_file" in workflow:
        declaration_member = _member(root, workflow["declaration_file"], "[workflow].declaration_file")
        try:
            declaration = json.loads((root / declaration_member).read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise _error(root, f"cannot read external declaration {declaration_member!r}: {exc}") from exc
        document = _validate_external_declaration(declaration, provider, root, declaration_uri)
    else:
        document = workflow_declaration_from_manifest(provider)
    try:
        validated = validate_declarations({"workflow": document})
    except (FormatError, ValueError, TypeError) as exc:
        raise _error(root, f"workflow declaration exceeds or violates the declaration limit: {exc}") from exc
    provider = replace(provider, declarations=validated)
    if declaration_member is not None:
        provider = replace(provider, declaration_file=declaration_member)
    if postprocess_file is not None:
        provider = replace(provider, postprocessor=cast(Any, _source_hook(root, postprocess_file, "postprocess")))
    return provider


def load_workflow_package(path: str | Path, *, register: bool = True) -> WorkflowProvider:
    """Parse one package and optionally add it to the workflow registry."""

    provider = parse_workflow_manifest(path)
    if register:
        register_workflow(provider)
    return provider


def _source_hook(directory: Path, member: str, function_name: str) -> Callable[..., object]:
    """Load a hook from an explicitly selected source directory on first use.

    This is the tier-two explicit-path trust path. Scaffold instantiate hooks use
    :func:`_tree_hook` after publication, where the store tree is verified first.
    """

    member_key = hashlib.sha256(PurePosixPath(member).as_posix().encode()).hexdigest()
    module_name = (
        f"httk_workflow_pkg_source_{hashlib.sha256(str(directory.resolve()).encode()).hexdigest()}_{member_key}"
    )

    def hook(*args: object, **kwargs: object) -> object:
        module = sys.modules.get(module_name)
        if module is None:
            source = directory.joinpath(*PurePosixPath(member).parts)
            module = _exec_hook(module_name, source)
        function = getattr(module, function_name, None)
        if not callable(function):
            raise ValueError(f"workflow package hook {member} does not define {function_name}()")
        return function(*args, **kwargs)

    return hook


def _exec_hook(module_name: str, source: Path) -> ModuleType:
    module = ModuleType(module_name)
    module.__file__ = str(source)
    sys.modules[module_name] = module
    try:
        exec(compile(source.read_bytes(), str(source), "exec"), module.__dict__)  # noqa: S102
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module


def _tree_hook(store_tree: Path, pinned_sha256: str, member: str, function_name: str) -> Callable[..., object]:
    """Return a hook that verifies and executes one member of a pinned tree."""

    relative = PurePosixPath(member)
    member_key = hashlib.sha256(relative.as_posix().encode()).hexdigest()
    module_name = f"httk_workflow_pkg_{pinned_sha256}_{member_key}"

    def hook(*args: object, **kwargs: object) -> object:
        try:
            actual = tree_digest(store_tree)
        except Exception as exc:
            raise _PinnedTreeError(f"published workflow tree {store_tree} could not be verified: {exc}") from exc
        if actual != pinned_sha256:
            raise _PinnedTreeError(
                f"published workflow tree {store_tree} changed: digest {actual} does not match pinned {pinned_sha256}"
            )
        module = sys.modules.get(module_name)
        if module is None:
            source = store_tree.joinpath(*relative.parts)
            if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(store_tree.resolve()):
                raise ValueError(
                    f"workflow package hook member is unavailable in published tree {store_tree}: {member}"
                )
            module = _exec_hook(module_name, source)
        function = getattr(module, function_name, None)
        if not callable(function):
            raise ValueError(f"workflow package hook {member} does not define {function_name}()")
        return function(*args, **kwargs)

    return hook
