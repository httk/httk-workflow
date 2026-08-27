"""Load directory-based workflow packages from ``httk_workflow.toml`` manifests."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType, ModuleType
from typing import Any, cast

from httk.core.building import BuildSpec, artifact_excluder, read_manifest_build_spec
from httk.core.digests import tree_digest

from . import languages
from ._util import validate_inputs
from .errors import FormatError
from .models import (
    RESERVED_WORKFLOW_ENVIRONMENT_PREFIX,
    environment_variable_name,
    validate_declarations,
    validate_resources,
)
from .scaffold import WorkflowProvider, payload_relative, register_workflow

MANIFEST_NAME = "httk_workflow.toml"
_LOGGER = logging.getLogger(__name__)

_PLUGIN_WORKFLOW_CACHE: (
    tuple[Mapping[str, WorkflowProvider], Mapping[str, str], Mapping[str, tuple[str, ...]]] | None
) = None

__all__ = [
    "MANIFEST_NAME",
    "artifact_excluder",
    "installed_plugin_workflow_owners",
    "installed_plugin_workflows",
    "load_workflow_package",
    "parse_workflow_manifest",
    "read_build_spec",
    "source_tree_digest",
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


def _collect_member(directory: Path, value: object, path: str) -> tuple[str, bool]:
    """Validate a Python or executable collect member and return its kind."""

    if isinstance(value, str) and PurePosixPath(value).suffix == ".py":
        return _member(directory, value, path, python=True), False
    member = _member(directory, value, path)
    candidate = directory.joinpath(*PurePosixPath(member).parts)
    if not os.access(candidate, os.X_OK):
        raise _error(directory, f"{path} must name a .py member or an executable member (chmod +x): {value!r}")
    return member, True


def read_build_spec(root: Path) -> BuildSpec | None:
    """Read and validate only the build section of one package manifest.

    :param root: Locate the workflow package directory.
    :return: The package build specification, or ``None`` when it is absent.
    :raises ValueError: If the manifest or its build section is malformed.
    """

    return read_manifest_build_spec(
        root,
        manifest_name=MANIFEST_NAME,
        table_name="workflow",
        protected_names=("run", MANIFEST_NAME),
    )


def source_tree_digest(root: Path) -> str:
    """Return the digest of package sources, excluding declared build artifacts.

    :param root: Locate the workflow package directory.
    :return: The source-only tree digest.
    :raises ValueError: If the package build section is malformed.
    """

    spec = read_build_spec(root)
    return tree_digest(root, exclude=artifact_excluder(spec)) if spec is not None else tree_digest(root)


def _validate_name(name: object, path: str, directory: Path) -> str:
    if not isinstance(name, str) or not name:
        raise _error(directory, f"{path} must be a nonempty string")
    return name


def _validate_input_table(
    raw: Mapping[str, object],
    name: str,
    directory: Path,
    *,
    language: bool = False,
    allow_port: bool = False,
) -> tuple[dict[str, object], str | None]:
    path = f"[workflow.inputs.{name}]"
    _unknown(raw, {"destination", "description", "entry_type", "ref", "role", "port", "required"}, path, directory)
    destination = raw.get("destination")
    if language and destination is not None:
        raise _error(directory, f"{path}.destination is implied by the language")
    if destination is not None:
        try:
            validate_inputs({name: destination})
            payload_relative(str(destination))
        except (FormatError, ValueError) as exc:
            raise _error(directory, f"{path}.destination is invalid: {exc}") from exc
    role = _optional_string(raw, "role", path, directory) or name
    result: dict[str, object] = {"role": role}
    if "port" in raw:
        port = _string(raw, "port", path, directory)
        assert port is not None
        if not allow_port:
            raise _error(directory, f"{path}.port is only valid for a language workflow with a document")
        result["port"] = port
    elif allow_port:
        result["port"] = name
    if destination is not None:
        result["destination"] = destination
    for key in ("description", "entry_type", "ref"):
        value = _optional_string(raw, key, path, directory)
        if value is not None:
            result[key] = value
    # A required input must be supplied at submission. It defaults to required
    # exactly when the input declares an entry_type — a typed object a job cannot
    # run without — and to optional otherwise; a boolean overrides either way.
    if "required" in raw:
        required = raw["required"]
        if not isinstance(required, bool):
            raise _error(directory, f"{path}.required must be a boolean")
    else:
        required = "entry_type" in result
    result["required"] = required
    return result, None if destination is None else str(destination)


def _validate_parameters(raw: Mapping[str, object], directory: Path) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for name, value in raw.items():
        parameter = _validate_name(name, "[workflow.parameters] name", directory)
        table = _table(value, f"[workflow.parameters.{parameter}]", directory)
        path = f"[workflow.parameters.{parameter}]"
        _unknown(table, {"type", "description", "default"}, path, directory)
        parameter_type = _optional_string(table, "type", path, directory)
        if parameter_type is not None and parameter_type not in {
            "string",
            "number",
            "integer",
            "boolean",
            "array",
            "object",
        }:
            raise _error(directory, f"{path}.type is not one of string, number, integer, boolean, array, object")
        description = _optional_string(table, "description", path, directory)
        if (
            "default" in table
            and parameter_type is not None
            and not _matches_input_type(table["default"], parameter_type)
        ):
            raise _error(directory, f"{path}.default does not match type {parameter_type!r}")
        entry: dict[str, object] = {}
        if parameter_type is not None:
            entry["type"] = parameter_type
        if description is not None:
            entry["description"] = description
        if "default" in table:
            entry["default"] = table["default"]
        result[parameter] = entry
    return result


def _validate_steps(
    raw: Mapping[str, object] | None,
    executable_steps: tuple[str, ...],
    directory: Path,
    *,
    language: str | None,
) -> dict[str, dict[str, int]]:
    """Validate per-step resource requirements from a workflow manifest."""

    if raw is None:
        return {}
    if language is not None:
        raise _error(directory, "[workflow.steps] requires [workflow.runner].steps")
    result: dict[str, dict[str, int]] = {}
    declared = ", ".join(executable_steps) or "none"
    for name, value in raw.items():
        path = f"[workflow.steps.{name}]"
        if name not in executable_steps:
            raise _error(directory, f"{path} is an unknown step; declared runner steps: {declared}")
        table = _table(value, path, directory)
        _unknown(table, {"resources"}, path, directory)
        try:
            result[name] = validate_resources(table.get("resources", {}), f"{path}.resources")
        except FormatError as exc:
            raise _error(directory, str(exc)) from exc
    return result


def _validate_environment(raw: Mapping[str, object], directory: Path) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for name, value in raw.items():
        environment = _validate_name(name, "[workflow.environment] name", directory)
        table = _table(value, f"[workflow.environment.{environment}]", directory)
        path = f"[workflow.environment.{environment}]"
        _unknown(table, {"type", "description", "default", "setting"}, path, directory)
        environment_type = _optional_string(table, "type", path, directory)
        if environment_type is not None and environment_type not in {
            "string",
            "number",
            "integer",
            "boolean",
            "array",
            "object",
        }:
            raise _error(directory, f"{path}.type is not one of string, number, integer, boolean, array, object")
        description = _optional_string(table, "description", path, directory)
        if (
            "default" in table
            and environment_type is not None
            and not _matches_input_type(table["default"], environment_type)
        ):
            raise _error(directory, f"{path}.default does not match type {environment_type!r}")
        setting = _optional_string(table, "setting", path, directory)
        if (
            setting is not None
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", setting) is None
        ):
            raise _error(directory, f"{path}.setting must be a nonempty dotted identifier")
        effective_setting = setting if setting is not None else environment
        variable = environment_variable_name(effective_setting)
        if variable.startswith(RESERVED_WORKFLOW_ENVIRONMENT_PREFIX):
            raise _error(
                directory,
                f"{path} derives reserved {RESERVED_WORKFLOW_ENVIRONMENT_PREFIX!r} variable {variable!r}; "
                "choose a workflow setting outside the manager-owned namespace",
            )
        entry: dict[str, object] = {}
        if environment_type is not None:
            entry["type"] = environment_type
        if description is not None:
            entry["description"] = description
        if "default" in table:
            entry["default"] = table["default"]
        if setting is not None:
            entry["setting"] = setting
        result[environment] = entry
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
    raw: Mapping[str, object],
    inputs: Mapping[str, Mapping[str, object]],
    directory: Path,
    *,
    language: bool = False,
    allow_port: bool = False,
) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    roles: set[str] = set()
    input_roles = {str(entry.get("role", name)): name for name, entry in inputs.items()}
    for name, value in raw.items():
        output = _validate_name(name, "[workflow.outputs] name", directory)
        table = _table(value, f"[workflow.outputs.{output}]", directory)
        path = f"[workflow.outputs.{output}]"
        _unknown(table, {"entry_type", "ref", "description", "product_of", "role", "port"}, path, directory)
        entry_type = _string(table, "entry_type", path, directory, required=True)
        role = _optional_string(table, "role", path, directory) or output
        if role in roles:
            raise _error(directory, f"{path}.role is duplicated")
        roles.add(role)
        product_of = _optional_string(table, "product_of", path, directory)
        entry: dict[str, object] = {"entry_type": entry_type, "role": role}
        if "port" in table:
            port = _string(table, "port", path, directory)
            assert port is not None
            if not allow_port:
                raise _error(directory, f"{path}.port is only valid for a language workflow with a document")
            entry["port"] = port
        elif allow_port:
            entry["port"] = output
        for key in ("ref", "description"):
            optional = _optional_string(table, key, path, directory)
            if optional is not None:
                entry[key] = optional
        if product_of is not None:
            entry["product_of"] = product_of
        result[output] = entry
    for name, entry in result.items():
        source_role = entry.get("product_of")
        if not isinstance(source_role, str):
            continue
        path = f"[workflow.outputs.{name}]"
        role = str(entry["role"])
        if source_role == role:
            raise _error(directory, f"{path}.product_of cannot reference its own output role {role!r}")
        input_match = source_role in input_roles
        output_match = source_role in roles
        if input_match and output_match:
            raise _error(
                directory,
                f"{path}.product_of role {source_role!r} is both an input and output role; rename one",
            )
        if not input_match and not output_match:
            raise _error(directory, f"{path}.product_of names unknown input or output role {source_role!r}")
    output_paths = {str(entry["role"]): f"[workflow.outputs.{name}]" for name, entry in result.items()}
    graph = {
        str(entry["role"]): str(entry["product_of"])
        for entry in result.values()
        if isinstance(entry.get("product_of"), str) and str(entry["product_of"]) in roles
    }
    visiting: list[str] = []
    visited: set[str] = set()

    def visit(role: str) -> None:
        if role in visiting:
            cycle = " -> ".join([*visiting[visiting.index(role) :], role])
            raise _error(directory, f"{output_paths[role]}.product_of forms an output cycle: {cycle}")
        if role in visited:
            return
        visiting.append(role)
        source = graph.get(role)
        if source is not None:
            visit(source)
        visiting.pop()
        visited.add(role)

    for role in graph:
        visit(role)
    return result


def _matches_input_roles(document: Mapping[str, object], provider: WorkflowProvider, directory: Path) -> None:
    inputs = document.get("inputs", [])
    if not isinstance(inputs, list):
        raise _error(directory, "external declaration inputs must be an array")
    expected = {
        str(entry.get("role", name)): entry for name, entry in provider._input_metadata.items() if "entry_type" in entry
    }
    actual: dict[str, Mapping[str, object]] = {}
    for index, entry in enumerate(inputs):
        if not isinstance(entry, Mapping) or not isinstance(entry.get("name"), str):
            raise _error(directory, f"external declaration inputs[{index}] must name a role")
        name = str(entry["name"])
        if name in actual:
            raise _error(directory, f"external declaration input {name!r} is duplicated")
        actual[name] = entry
        if name not in expected:
            raise _error(
                directory,
                f"external declaration input {name!r} is not a manifest input role (or is not entry-typed)",
            )
        if entry.get("entry_type") != expected[name].get("entry_type"):
            raise _error(directory, f"external declaration input {name!r} has an incompatible entry_type")
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    if missing:
        raise _error(directory, f"external declaration is missing manifest input role {missing[0]!r}")
    if extra:
        raise _error(directory, f"external declaration has extra input role {extra[0]!r}")


def _validate_external_declaration(
    document: object, provider: WorkflowProvider, directory: Path, declaration_uri: str | None
) -> dict[str, object]:
    if not isinstance(document, dict):
        raise _error(directory, "external workflow declaration must be a JSON object")
    if declaration_uri is not None and document.get("$id") != declaration_uri:
        raise _error(directory, "external declaration $id does not match workflow.declaration_uri")
    _matches_input_roles(document, provider, directory)
    outputs = document.get("outputs", [])
    if not isinstance(outputs, list):
        raise _error(directory, "external declaration outputs must be an array")
    output_roles = {str(entry.get("role", name)): entry for name, entry in provider.outputs.items()}
    actual_outputs: dict[str, Mapping[str, object]] = {}
    for index, entry in enumerate(outputs):
        if not isinstance(entry, Mapping) or not isinstance(entry.get("name"), str):
            raise _error(directory, f"external declaration outputs[{index}] must name a role")
        name = str(entry["name"])
        if name in actual_outputs:
            raise _error(directory, f"external declaration output {name!r} is duplicated")
        actual_outputs[name] = entry
        expected = output_roles.get(name)
        if expected is None:
            raise _error(directory, f"external declaration output {name!r} is not a manifest output role")
        if entry.get("entry_type") != expected.get("entry_type"):
            raise _error(directory, f"external declaration output {name!r} has an incompatible entry_type")
        if "product_of" in entry:
            raise _error(
                directory,
                f"external declaration output {name!r} must not carry product_of; curation belongs in the manifest",
            )
    missing = sorted(set(output_roles) - set(actual_outputs))
    extra = sorted(set(actual_outputs) - set(output_roles))
    if missing:
        raise _error(directory, f"external declaration is missing manifest output role {missing[0]!r}")
    if extra:
        raise _error(directory, f"external declaration has extra output role {extra[0]!r}")
    return document


def workflow_declaration_from_manifest(provider: WorkflowProvider) -> dict[str, object]:
    """Generate the workflow declaration carried by a package.

    :param provider: Supply the validated package provider.
    :return: The generated workflow declaration.
    """

    return _workflow_declaration(
        provider.summary,
        provider._input_metadata,
        provider.outputs,
        declaration_uri=provider.declaration_uri,
    )


def _workflow_declaration(
    description: str,
    input_metadata: Mapping[str, Mapping[str, object]],
    output_metadata: Mapping[str, Mapping[str, object]],
    *,
    declaration_uri: str | None = None,
) -> dict[str, object]:
    """Build the declaration shape shared by packages and anonymous workflows."""

    document: dict[str, object] = {}
    if declaration_uri is not None:
        document["$id"] = declaration_uri
    if description:
        document["description"] = description
    inputs: list[dict[str, object]] = []
    for name, entry in input_metadata.items():
        if "entry_type" not in entry:
            continue
        input_entry = {"name": entry.get("role", name), "entry_type": entry["entry_type"]}
        for key in ("ref", "description"):
            if key in entry:
                input_entry[key] = entry[key]
        inputs.append(input_entry)
    outputs: list[dict[str, object]] = []
    for name, entry in output_metadata.items():
        output: dict[str, object] = {"name": entry.get("role", name), "entry_type": entry["entry_type"]}
        for key in ("ref", "description"):
            if key in entry:
                output[key] = entry[key]
        outputs.append(output)
    document["inputs"] = inputs
    document["outputs"] = outputs
    return document


def parse_workflow_manifest(directory: str | Path) -> WorkflowProvider:
    """Parse and validate one directory workflow package.

    The ``httk_workflow.toml`` file is httk-owned glue: unknown keys are rejected
    at every supported table, and external declarations are checked against
    the manifest's roles. Declared inputs describe staged objects, while
    parameters remain opaque implementation knobs and are carried under the
    job's ``parameters`` member.

    :param directory: Locate the package directory to parse.
    :return: The validated workflow provider.
    :raises ValueError: If the package is missing, malformed, or violates the
        manifest contract.
    """

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
            "build",
            "instantiate",
            "collect",
            "postprocess",
            "inputs",
            "parameters",
            "environment",
            "outputs",
            "resources",
            "steps",
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
    language_name = _optional_string(runner, "language", "[workflow.runner]", root)
    lang = None
    document_member: str | None = None
    runner_options: dict[str, object] = {}
    if language_name is not None:
        try:
            lang = languages.language(language_name)
        except ValueError as exc:
            raise _error(root, f"[workflow.runner].language: {exc}") from exc
        for key in ("entry", "steps", "initial_step"):
            if key in runner:
                raise _error(root, f"[workflow.runner].{key} is implied by language {language_name!r}")
        if "instantiate" in workflow:
            raise _error(root, f"[workflow.instantiate] is implied by language {language_name!r}")
        if lang.document_policy == "required":
            if "document" not in runner:
                raise _error(root, f"workflow language {language_name!r} requires [workflow.runner].document")
            document_member = _member(root, runner.get("document"), "[workflow.runner].document")
        elif lang.document_policy == "forbidden" and "document" in runner:
            raise _error(root, f"[workflow.runner].document is not used by language {language_name!r}")
        elif lang.document_policy == "optional" and "document" in runner:
            document_member = _member(root, runner.get("document"), "[workflow.runner].document")
        if not lang.allows_modes:
            for mode in ("data_mode", "workdir_mode"):
                if mode in runner:
                    raise _error(root, f"[workflow.runner].{mode} is not supported by language {language_name!r}")
        runner_options = {
            key: value
            for key, value in runner.items()
            if key not in {"language", "document", "data_mode", "workdir_mode"}
        }
        try:
            lang.validate_runner(runner_options, root)
        except ValueError as exc:
            raise _error(root, f"[workflow.runner]: {exc}") from exc
        if lang.document_policy == "optional":
            has_maker = "maker" in runner_options
            if document_member is not None and has_maker:
                raise _error(root, "[workflow.runner] give either document= or maker=, not both")
            if document_member is None and not has_maker:
                raise _error(root, "[workflow.runner] give either document= or maker=, not neither")
        entry = "run"
        steps = lang.steps
        initial_step = lang.initial_step
    else:
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
        requested_initial_step = _optional_string(runner, "initial_step", "[workflow.runner]", root)
        if requested_initial_step is None:
            if "start" in steps:
                initial_step = "start"
            elif len(steps) == 1:
                initial_step = steps[0]
            else:
                raise _error(root, "[workflow.runner].initial_step is required when steps omit 'start'")
        else:
            initial_step = requested_initial_step
        if initial_step not in steps:
            raise _error(root, f"[workflow.runner].initial_step {initial_step!r} is not in steps")
    data_mode = runner.get("data_mode", "none")
    workdir_mode = runner.get("workdir_mode", "persistent")
    if data_mode not in {"none", "transactional"}:
        raise _error(root, "[workflow.runner].data_mode must be 'none' or 'transactional'")
    if workdir_mode not in {"persistent", "isolated"}:
        raise _error(root, "[workflow.runner].workdir_mode must be 'persistent' or 'isolated'")

    try:
        resources = validate_resources(workflow.get("resources", {}), "[workflow.resources]")
    except FormatError as exc:
        raise _error(root, str(exc)) from exc
    step_resources = _validate_steps(
        _table(workflow["steps"], "[workflow.steps]", root) if "steps" in workflow else None,
        steps,
        root,
        language=language_name,
    )

    raw_inputs = _table(workflow.get("inputs", {}), "[workflow.inputs]", root)
    input_metadata: dict[str, dict[str, object]] = {}
    inputs: dict[str, str | None] = {}
    roles: set[str] = set()
    for name, value in raw_inputs.items():
        input_name = _validate_name(name, "[workflow.inputs] name", root)
        table = _table(value, f"[workflow.inputs.{input_name}]", root)
        metadata, destination = _validate_input_table(
            table,
            input_name,
            root,
            language=lang is not None,
            allow_port=lang is not None and (document_member is not None or lang.open_ports),
        )
        if str(metadata["role"]) in roles:
            raise _error(root, f"[workflow.inputs.{input_name}].role is duplicated")
        roles.add(str(metadata["role"]))
        input_metadata[input_name] = metadata
        inputs[input_name] = destination

    instantiate_file: str | None = None
    instantiate_exec: str | None = None
    if "instantiate" in workflow:
        instantiate = _table(workflow["instantiate"], "[workflow.instantiate]", root)
        _unknown(instantiate, {"file"}, "[workflow.instantiate]", root)
        value = instantiate.get("file")
        if isinstance(value, str) and PurePosixPath(value).suffix == ".py":
            instantiate_file = _member(root, value, "[workflow.instantiate].file", python=True)
        else:
            instantiate_file = _member(root, value, "[workflow.instantiate].file")
            candidate = root.joinpath(*PurePosixPath(instantiate_file).parts)
            if not os.access(candidate, os.X_OK):
                raise _error(
                    root,
                    f"[workflow.instantiate].file must name a .py member or an executable member (chmod +x): {value!r}",
                )
            instantiate_exec = instantiate_file
    if lang is not None:
        instantiate_file = None
    if lang is None and any(destination is None for destination in inputs.values()) and instantiate_file is None:
        raise _error(root, "hook-consumed workflow inputs require [workflow.instantiate]")

    collect_file: str | None = None
    collector_exec: str | None = None
    build = read_build_spec(root)
    if "collect" in workflow:
        collect = _table(workflow["collect"], "[workflow.collect]", root)
        _unknown(collect, {"file"}, "[workflow.collect]", root)
        collect_file, executable = _collect_member(root, collect.get("file"), "[workflow.collect].file")
        collector_exec = collect_file if executable else None

    postprocess_scripts: dict[str, dict[str, object]] = {}
    if "postprocess" in workflow:
        raw_postprocess = workflow["postprocess"]
        teaching_error = (
            "the collect hook moved to [workflow.collect], and "
            "[workflow.postprocess.<name>] tables declare curated scripts"
        )
        if not isinstance(raw_postprocess, Mapping) or "file" in raw_postprocess:
            raise _error(root, teaching_error)
        for name, value in raw_postprocess.items():
            script_name = _validate_name(name, "[workflow.postprocess] name", root)
            table = _table(value, f"[workflow.postprocess.{script_name}]", root)
            path = f"[workflow.postprocess.{script_name}]"
            _unknown(table, {"file", "description"}, path, root)
            member = _member(root, table.get("file"), f"{path}.file", python=False)
            description_value = _string(table, "description", path, root)
            postprocess_scripts[script_name] = {"file": member, "description": description_value}

    parameters = _validate_parameters(_table(workflow.get("parameters", {}), "[workflow.parameters]", root), root)
    manifest_environment = _validate_environment(
        _table(workflow.get("environment", {}), "[workflow.environment]", root), root
    )
    environment = {**(lang.environment if lang is not None else {}), **manifest_environment}
    outputs = _validate_outputs(
        _table(workflow.get("outputs", {}), "[workflow.outputs]", root),
        input_metadata,
        root,
        language=lang is not None,
        allow_port=lang is not None and (document_member is not None or lang.open_ports),
    )
    if lang is not None and (document_member is not None or lang.open_ports):
        static_ports: languages.LanguagePorts | None = None
        if document_member is not None and not lang.open_ports:
            document_path = (root / document_member).resolve()
            try:
                static_ports = lang.ports(document_path)
            except Exception as exc:
                raise _error(root, f"[workflow.runner].document: {exc}") from exc
        seen_inputs: dict[str, str] = {}
        for name, metadata in input_metadata.items():
            port = str(metadata["port"])
            if static_ports is not None:
                known_inputs = ", ".join(static_ports.inputs) or "none"
                if port not in static_ports.inputs:
                    raise _error(
                        root,
                        f"[workflow.inputs.{name}].port {port!r} is not a document input port; "
                        f"known ports: {known_inputs}",
                    )
            previous = seen_inputs.get(port)
            if previous is not None:
                raise _error(
                    root,
                    f"[workflow.inputs.{name}].port duplicates "
                    f"[workflow.inputs.{previous}].port for document port {port!r}",
                )
            seen_inputs[port] = name
        seen_outputs: dict[str, str] = {}
        for name, metadata in outputs.items():
            port = str(metadata["port"])
            if static_ports is not None:
                known_outputs = ", ".join(static_ports.outputs) or "none"
                if port not in static_ports.outputs:
                    raise _error(
                        root,
                        f"[workflow.outputs.{name}].port {port!r} is not a document output port; "
                        f"known ports: {known_outputs}",
                    )
            previous = seen_outputs.get(port)
            if previous is not None:
                raise _error(
                    root,
                    f"[workflow.outputs.{name}].port duplicates "
                    f"[workflow.outputs.{previous}].port for document port {port!r}",
                )
            seen_outputs[port] = name
    provider = WorkflowProvider(
        workflow_id=workflow_id,
        runner_package=None,
        runner_file=None,
        language=language_name,
        document=document_member,
        runner_options=runner_options,
        initial_step=initial_step,
        alias=alias,
        steps=steps,
        data_mode=data_mode,  # type: ignore[arg-type]
        workdir_mode=workdir_mode,  # type: ignore[arg-type]
        summary=description,
        inputs=inputs,
        instantiate=lang is not None or instantiate_file is not None,
        declarations={},
        directory=root.resolve(),
        entry=entry,
        instantiate_file=instantiate_file,
        instantiate_exec=instantiate_exec,
        collect_file=collect_file,
        collector_exec=collector_exec,
        build=build,
        postprocess_scripts=postprocess_scripts,
        parameters=parameters,
        environment=environment,
        outputs=outputs,
        resources=resources,
        step_resources=step_resources,
        declaration_uri=declaration_uri,
        declaration_file=None,
        _input_metadata=input_metadata,
        collector=(
            f"httk.workflow.languages.{language_name.replace('-', '_')}:collect"
            if language_name is not None and lang is not None and lang.has_default_collector
            else None
        ),
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
    if collect_file is not None and collector_exec is None:
        provider = replace(provider, collector=cast(Any, _source_hook(root, collect_file, "collect")))
    return provider


def _plugin_workflow_data() -> tuple[Mapping[str, WorkflowProvider], Mapping[str, str], Mapping[str, tuple[str, ...]]]:
    """Load and cache installed-plugin workflow providers and their metadata."""

    global _PLUGIN_WORKFLOW_CACHE
    if _PLUGIN_WORKFLOW_CACHE is not None:
        return _PLUGIN_WORKFLOW_CACHE

    try:
        from httk.core import plugins
    except ImportError:
        _PLUGIN_WORKFLOW_CACHE = (MappingProxyType({}), MappingProxyType({}), MappingProxyType({}))
        return _PLUGIN_WORKFLOW_CACHE

    records: list[tuple[str, WorkflowProvider]] = []
    name_records: dict[str, list[int]] = {}
    installed = sorted(plugins.installed_plugins(), key=lambda plugin: (plugin.name, str(plugin.root)))
    for plugin in installed:
        for member in plugin.manifest.workflows:
            try:
                provider = load_workflow_package(plugin.root / member, register=False)
            except (OSError, ValueError) as exc:
                _LOGGER.warning(
                    "Skipping workflow package from plugin %r member %r: %s",
                    plugin.name,
                    member,
                    exc,
                )
                continue
            record = len(records)
            records.append((plugin.name, provider))
            for name in (provider.workflow_id, provider.alias):
                if name is not None:
                    name_records.setdefault(name, []).append(record)

    conflicts: dict[str, set[str]] = {}
    for indexes in name_records.values():
        if len(indexes) < 2:
            continue
        plugin_names = {records[index][0] for index in indexes}
        for index in indexes:
            provider = records[index][1]
            for name in (provider.workflow_id, provider.alias):
                if name is not None:
                    conflicts.setdefault(name, set()).update(plugin_names)

    poisoned = set(conflicts)
    providers: dict[str, WorkflowProvider] = {}
    owners: dict[str, str] = {}
    for plugin_name, provider in records:
        if any(name in poisoned for name in (provider.workflow_id, provider.alias) if name is not None):
            continue
        providers[provider.workflow_id] = provider
        owners[provider.workflow_id] = plugin_name
    _PLUGIN_WORKFLOW_CACHE = (
        MappingProxyType(providers),
        MappingProxyType(owners),
        MappingProxyType({name: tuple(sorted(plugin_names)) for name, plugin_names in conflicts.items()}),
    )
    return _PLUGIN_WORKFLOW_CACHE


def installed_plugin_workflows() -> Mapping[str, WorkflowProvider]:
    """Return workflow packages bundled by installed plugins, keyed by id.

    Discovery is lazy and cached for the process lifetime. Installing a plugin
    during that lifetime requires a new process or the private
    :func:`_reset_plugin_workflow_cache` test helper.

    :return: The installed plugin workflow providers by canonical workflow id.
    """

    return _plugin_workflow_data()[0]


def installed_plugin_workflow_owners() -> Mapping[str, str]:
    """Return the installed plugin name owning each bundled workflow id."""

    return _plugin_workflow_data()[1]


def _plugin_workflow_conflicts() -> Mapping[str, tuple[str, ...]]:
    """Return poisoned bundled workflow names and their owning plugins."""

    return _plugin_workflow_data()[2]


def _reset_plugin_workflow_cache() -> None:
    """Clear cached installed-plugin workflow discovery for tests."""

    global _PLUGIN_WORKFLOW_CACHE
    _PLUGIN_WORKFLOW_CACHE = None


def load_workflow_package(path: str | Path, *, register: bool = True) -> WorkflowProvider:
    """Parse one package and optionally add it to the workflow registry.

    :param path: Locate the package directory to load.
    :param register: Add the provider to the process registry when true.
    :return: The validated workflow provider.
    :raises ValueError: If parsing or registration rejects the package.
    """

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
