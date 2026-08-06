"""The read-only top-level workflow description command."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from typing import Any

from ..scaffold import ResolvedWorkflow, resolve_workflow, workflow_provider
from ._common import _leaf

WORKFLOW_DESCRIPTION_FORMAT = "httk-workflow-workflow-description"


def _source_kind(target: str, workflow: ResolvedWorkflow) -> str:
    if workflow_provider(target) is not None:
        return "registered/packaged"
    return "directory" if workflow.directory is not None else "file"


def _parameter_document(workflow: ResolvedWorkflow) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for name, destination in workflow.parameters.items():
        metadata = workflow._parameter_metadata.get(name, {})
        entry: dict[str, object] = {
            "destination": destination if destination is not None else "consumed by the instantiate hook"
        }
        for key in ("entry_type", "role", "description"):
            if key in metadata:
                entry[key] = metadata[key]
        result[name] = entry
    return result


def _output_document(workflow: ResolvedWorkflow) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for name, metadata in workflow.outputs.items():
        role = str(metadata.get("role", name))
        entry = {key: value for key, value in metadata.items() if key != "role"}
        entry["role"] = role
        result[role] = entry
    return result


def _declaration_document(workflow: ResolvedWorkflow) -> dict[str, object]:
    document = workflow.declarations.get("workflow")
    if document is None:
        return {"present": False, "id": None, "origin": "none"}
    if workflow.directory is not None:
        origin = "external declaration file" if workflow.declaration_file is not None else "generated from manifest"
    else:
        origin = "declared"
    return {"present": True, "id": document.get("$id"), "origin": origin}


def _workflow_description(target: str) -> dict[str, object]:
    workflow = resolve_workflow(target)
    source = workflow.directory if workflow.directory is not None else workflow.source
    return {
        "format": WORKFLOW_DESCRIPTION_FORMAT,
        "format_version": 1,
        "workflow": workflow.workflow_id,
        "alias": workflow.alias,
        "source": {"kind": _source_kind(target, workflow), "path": str(source)},
        "summary": workflow.summary,
        "description": workflow.summary,
        "steps": [{"name": step, "initial": step == workflow.initial_step} for step in workflow.steps],
        "initial_step": workflow.initial_step,
        "data_mode": workflow.data_mode,
        "workdir_mode": workflow.workdir_mode,
        "parameters": _parameter_document(workflow),
        "inputs": {name: dict(metadata) for name, metadata in workflow.inputs.items()},
        "outputs": _output_document(workflow),
        "declaration": _declaration_document(workflow),
        "hooks": {
            "instantiate": {
                "present": workflow.instantiate or workflow.instantiate_file is not None,
                "file": workflow.instantiate_file,
                "packaged": workflow.packaged is not None,
            },
            "postprocess": {
                "present": workflow.postprocess_file is not None or workflow.postprocessor is not None,
                "file": workflow.postprocess_file,
                "packaged": workflow.packaged is not None,
            },
        },
    }


def _value(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def _render_text(description: Mapping[str, object]) -> str:
    lines = [
        f"workflow: {description['workflow']}",
        f"alias: {description['alias'] or '-'}",
    ]
    source = description["source"]
    assert isinstance(source, Mapping)
    lines.append(f"source: {source['kind']} ({source['path']})")
    lines.extend(
        [
            f"summary: {description['summary'] or '-'}",
            f"data_mode: {description['data_mode']}",
            f"workdir_mode: {description['workdir_mode']}",
            "steps:",
        ]
    )
    steps = description["steps"]
    assert isinstance(steps, list)
    for step in steps:
        assert isinstance(step, Mapping)
        lines.append(f"  {'*' if step['initial'] else '-'} {step['name']}")
    for title, key in (("parameters", "parameters"), ("inputs", "inputs"), ("outputs", "outputs")):
        lines.append(f"{title}:")
        values = description[key]
        assert isinstance(values, Mapping)
        if not values:
            lines.append("  -")
            continue
        for name, value in values.items():
            assert isinstance(value, Mapping)
            details = ", ".join(f"{field}={_value(item)}" for field, item in value.items())
            lines.append(f"  {name}: {details}")
    declaration = description["declaration"]
    assert isinstance(declaration, Mapping)
    lines.append(f"declaration: {declaration['origin']} ({declaration['id'] or 'no $id'})")
    hooks = description["hooks"]
    assert isinstance(hooks, Mapping)
    for name in ("instantiate", "postprocess"):
        hook = hooks[name]
        assert isinstance(hook, Mapping)
        location = f" ({hook['file']})" if hook["file"] else ""
        lines.append(f"{name} hook: {'yes' if hook['present'] else 'no'}{location}")
    return "\n".join(lines)


def handle_workflow_describe(arguments: argparse.Namespace, context: Any) -> int:
    """Describe one workflow without publishing or touching a workspace."""

    description = _workflow_description(arguments.target)
    print(json.dumps(description, indent=2, sort_keys=True) if arguments.json else _render_text(description))
    return 0


def build_describe_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Declare the top-level ``describe`` workflow command."""

    describe = _leaf(
        subparsers,
        "describe",
        summary="describe a workflow without publishing it",
        description="Describe a registered workflow, runner file, or workflow package directory",
        handler=handle_workflow_describe,
    )
    describe.add_argument("target", metavar="TARGET", help="workflow id, alias, runner file, or package directory")
    describe.add_argument("--json", action="store_true", help="print the description as one JSON object")
