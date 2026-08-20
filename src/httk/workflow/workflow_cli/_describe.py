"""The read-only top-level workflow description command."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from typing import Any

from ..scaffold import ResolvedWorkflow, describe_runner, resolve_workflow, workflow_provider
from ._common import _leaf

WORKFLOW_DESCRIPTION_FORMAT = "httk-workflow-workflow-description"


def _manifest_step_drift(workflow: ResolvedWorkflow) -> str | None:
    """Return a warning when a directory package's runner describes other steps.

    Resolving a package trusts the manifest and never executes anything. Describe
    is a report, not a parser, so here — and only here — the directory package's
    runner entry is run with ``--describe`` (which strips any surrounding attempt
    context) and its actual steps are compared to the manifest's. A disagreement
    is surfaced but does not change describe's exit status.

    :param workflow: The resolved workflow to check.
    :return: A drift warning, or ``None`` when nothing can be compared or they agree.
    """

    if workflow.directory is None or workflow.language is not None:
        return None
    entry = workflow.directory / workflow.entry
    try:
        described = describe_runner(entry)
    except (ValueError, OSError):
        # A runner that will not describe itself is not a steps disagreement;
        # describe stays a manifest report and leaves that to precheck/run.
        return None
    steps = described.get("steps")
    described_steps = [str(item) for item in steps] if isinstance(steps, list) else []
    if list(workflow.steps) == described_steps:
        return None
    return (
        f"the manifest declares steps {list(workflow.steps)} but the runner entry "
        f"{workflow.entry!r} describes {described_steps}"
    )


def _source_kind(target: str, workflow: ResolvedWorkflow) -> str:
    provider = workflow_provider(target)
    if provider is not None:
        return "registered-directory" if provider.directory is not None else "installed-package"
    return "directory" if workflow.directory is not None else "file"


def _input_document(workflow: ResolvedWorkflow) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    declaration = workflow.declarations.get("workflow", {})
    declared = declaration.get("inputs", []) if isinstance(declaration, Mapping) else []
    declared_inputs = [entry for entry in declared if isinstance(entry, Mapping)] if isinstance(declared, list) else []
    for name, destination in workflow.inputs.items():
        metadata = workflow._input_metadata.get(name, {})
        entry: dict[str, object] = {
            "destination": destination if destination is not None else "consumed by the instantiate hook"
        }
        for key in ("entry_type", "role", "description"):
            if key in metadata:
                entry[key] = metadata[key]
        if not any(key in metadata for key in ("entry_type", "role", "description")) and len(declared_inputs) == len(
            workflow.inputs
        ):
            declared_entry = declared_inputs[list(workflow.inputs).index(name)]
            if isinstance(declared_entry.get("entry_type"), str):
                entry["entry_type"] = declared_entry["entry_type"]
            if isinstance(declared_entry.get("name"), str):
                entry["role"] = declared_entry["name"]
            if isinstance(declared_entry.get("description"), str):
                entry["description"] = declared_entry["description"]
        result[name] = entry
    return result


def _output_document(workflow: ResolvedWorkflow) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    metadata_items: list[tuple[str, Mapping[str, object]]] = list(workflow.outputs.items())
    if not workflow.outputs:
        declaration = workflow.declarations.get("workflow", {})
        raw = declaration.get("outputs", []) if isinstance(declaration, Mapping) else []
        if isinstance(raw, list):
            for entry in raw:
                if isinstance(entry, Mapping) and isinstance(entry.get("name"), str):
                    metadata_items.append(
                        (str(entry["name"]), {key: value for key, value in entry.items() if key != "name"})
                    )
    for name, metadata in metadata_items:
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


def _workflow_description(target: str, format: str | None = None) -> dict[str, object]:
    workflow = resolve_workflow(target, format=format)
    source = workflow.directory if workflow.directory is not None else workflow.source
    return {
        "format": WORKFLOW_DESCRIPTION_FORMAT,
        "format_version": 2,
        "workflow": workflow.workflow_id,
        "alias": workflow.alias,
        "source": {"kind": _source_kind(target, workflow), "path": str(source)},
        "summary": workflow.summary,
        "description": workflow.summary,
        "steps": [{"name": step, "initial": step == workflow.initial_step} for step in workflow.steps],
        "manifest_step_drift": _manifest_step_drift(workflow),
        "initial_step": workflow.initial_step,
        "data_mode": workflow.data_mode,
        "workdir_mode": workflow.workdir_mode,
        "build": (
            {
                "present": True,
                "command": workflow.build.command,
                "platform": workflow.build.platform,
                "artifacts": list(workflow.build.artifacts),
            }
            if workflow.build is not None
            else {"present": False}
        ),
        "inputs": _input_document(workflow),
        "parameters": {name: dict(metadata) for name, metadata in workflow.parameters.items()},
        "environment": {name: dict(metadata) for name, metadata in workflow.environment.items()},
        "outputs": _output_document(workflow),
        "postprocess": {name: dict(script) for name, script in workflow.postprocess_scripts.items()},
        "declaration": _declaration_document(workflow),
        "hooks": {
            "instantiate": {
                "present": workflow.instantiate or workflow.instantiate_file is not None,
                "file": workflow.instantiate_file,
                "kind": (
                    "executable"
                    if workflow.instantiate_exec is not None
                    else "python"
                    if workflow.instantiate_file is not None
                    else None
                ),
                "packaged": workflow.packaged is not None,
            },
            "collect": {
                "present": workflow.collect_file is not None or workflow.collector is not None,
                "file": workflow.collect_file,
                "kind": (
                    "executable"
                    if workflow.collector_exec is not None
                    else "python"
                    if workflow.collect_file is not None
                    else None
                ),
                "packaged": workflow.packaged is not None,
            },
        },
    }


def _value(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True)


def _render_text(description: Mapping[str, object]) -> str:
    build = description["build"]
    assert isinstance(build, Mapping)
    build_line = (
        f"build: yes (command={build['command']}, platform={build['platform'] or '-'}, "
        f"artifacts={_value(build['artifacts'])})"
        if build["present"]
        else "build: no"
    )
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
            build_line,
            "steps:",
        ]
    )
    steps = description["steps"]
    assert isinstance(steps, list)
    for step in steps:
        assert isinstance(step, Mapping)
        lines.append(f"  {'*' if step['initial'] else '-'} {step['name']}")
    drift = description.get("manifest_step_drift")
    if isinstance(drift, str) and drift:
        lines.append(f"WARNING: step drift: {drift}")
    for title, key in (
        ("inputs", "inputs"),
        ("parameters", "parameters"),
        ("environment", "environment"),
        ("outputs", "outputs"),
    ):
        lines.append(f"{title}:")
        values = description[key]
        assert isinstance(values, Mapping)
        if not values:
            lines.append("  -")
            continue
        for name, value in values.items():
            assert isinstance(value, Mapping)
            if key == "environment":
                value = {
                    field: value[field] for field in ("type", "setting", "default", "description") if field in value
                }
            details = ", ".join(f"{field}={_value(item)}" for field, item in value.items())
            lines.append(f"  {name}: {details}")
    lines.append("postprocess scripts:")
    scripts = description["postprocess"]
    assert isinstance(scripts, Mapping)
    if not scripts:
        lines.append("  -")
    for name, value in scripts.items():
        assert isinstance(value, Mapping)
        details = f"{name}: {value['file']}"
        if value["description"]:
            details += f" — {value['description']}"
        lines.append(f"  {details}")
    declaration = description["declaration"]
    assert isinstance(declaration, Mapping)
    lines.append(f"declaration: {declaration['origin']} ({declaration['id'] or 'no $id'})")
    hooks = description["hooks"]
    assert isinstance(hooks, Mapping)
    for name in ("instantiate", "collect"):
        hook = hooks[name]
        assert isinstance(hook, Mapping)
        location = f" ({hook['file']})" if hook["file"] else ""
        kind = f", kind={hook['kind']}" if hook.get("kind") else ""
        lines.append(f"{name} hook: {'yes' if hook['present'] else 'no'}{location}{kind}")
    return "\n".join(lines)


def handle_workflow_describe(arguments: argparse.Namespace, context: Any) -> int:
    """Describe one workflow without publishing or touching a workspace."""

    description = _workflow_description(arguments.target, arguments.format)
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
    describe.add_argument(
        "--format",
        metavar="LANG",
        help="force LANG for a bare workflow document or directory",
    )
    describe.add_argument("--json", action="store_true", help="print the description as one JSON object")
