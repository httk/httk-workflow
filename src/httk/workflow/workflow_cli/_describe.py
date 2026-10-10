"""The workflow describe, list, install and uninstall commands: this machine's workflows and a workspace's store.

Without ``--workspace`` these commands read (and, for git URIs, fetch into)
this machine's *httk* setup. With ``--workspace WS`` they act on the store of
installed workflows of that workspace (``workflows/<slug>--<h16>/``); a
``REMOTE:WS`` workspace is reached through the remote's adapter.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from httk.core.git_sources import parse_git_uri

from .. import _fs, _kernel, _store
from .._durations import TIME_RESOURCES, format_duration
from ..adapters import probe_remote_workspace, resolve_remote, run_adapter
from ..git_workflows import _uninstall_workflows, fetch_workflow
from ..introspection import iter_jobs, read_job
from ..models import WORKSPACE_DIRECTORY
from ..packages import installed_plugin_workflow_owners, installed_plugin_workflows
from ..registry import WorkspaceBinding
from ..scaffold import (
    ResolvedWorkflow,
    _provider_resolution,
    describe_package_runner,
    describe_runner,
    registered_workflows,
    resolve_workflow,
    workflow_provider,
)
from ..workspace import Workspace
from ._common import (
    _ERRORS,
    _add_adapter_timeout,
    _leaf,
    _modifiable,
    _remote_workspace_read,
    _resolve_binding,
)

WORKFLOW_DESCRIPTION_FORMAT = "httk-workflow-workflow-description"


def _manifest_step_drift(workflow: ResolvedWorkflow) -> str | None:
    """Return a warning when a directory package's runner describes other steps.

    Resolving a package trusts the manifest and never executes anything. Describe
    is a report, not a parser, so here — and only here — the directory package's
    runner entry, or its declared command, is run with ``--describe`` (which
    strips any surrounding attempt context) and its actual steps are compared to
    the manifest's. A disagreement is surfaced but does not change describe's
    exit status. A command that needs ``{artifacts}`` is not run: describe has no
    workspace build registration, just as a ``run`` bridge into artifacts could
    not describe itself.

    :param workflow: The resolved workflow to check.
    :return: A drift warning, or ``None`` when nothing can be compared or they agree.
    """

    if workflow.directory is None or workflow.language is not None or not workflow.runnable:
        return None
    try:
        if workflow.command is not None:
            described = describe_package_runner(workflow.directory)
        else:
            described = describe_runner(workflow.directory / workflow.entry)
    except (ValueError, OSError):
        # A runner that will not describe itself is not a steps disagreement;
        # describe stays a manifest report and leaves that to precheck/run.
        return None
    steps = described.get("steps")
    described_steps = [str(item) for item in steps] if isinstance(steps, list) else []
    if sorted(workflow.steps) == sorted(described_steps):  # order is not semantic; initial_step is separate
        return None
    return (
        f"the manifest declares steps {list(workflow.steps)} but the runner "
        f"{'command' if workflow.command is not None else 'entry'} "
        f"{list(workflow.command) if workflow.command is not None else repr(workflow.entry)} describes {described_steps}"
    )


def _source_kind(target: str, workflow: ResolvedWorkflow) -> str:
    if (workflow.registration_id or "").startswith("git+"):
        return "installed"
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
    source_document: dict[str, object] = {"kind": _source_kind(target, workflow), "path": str(source)}
    if source_document["kind"] == "installed":
        uri = parse_git_uri(workflow.workflow_id)
        source_document.update(uri=workflow.workflow_id, commit=uri.ref, name=workflow.name)
    return _description(workflow, source_document)


def _installed_description(workspace: Workspace, target: str) -> dict[str, object]:
    """Describe the installation of *target* in a workspace's store."""

    installed = _store.lookup(workspace, target)
    if installed is None:
        raise ValueError(f"workflow {target!r} is not installed in the workspace {workspace.root}")
    source = {"kind": "workspace", "path": str(installed.package), **_installed_row(installed)}
    return _description(_provider_resolution(installed.provider()), source)


def _description(workflow: ResolvedWorkflow, source_document: Mapping[str, object]) -> dict[str, object]:
    return {
        "format": WORKFLOW_DESCRIPTION_FORMAT,
        "format_version": 2,
        "workflow": workflow.workflow_id,
        "alias": workflow.alias,
        "source": source_document,
        "summary": workflow.summary,
        "description": workflow.summary,
        "steps": [{"name": step, "initial": step == workflow.initial_step} for step in workflow.steps],
        "manifest_step_drift": _manifest_step_drift(workflow),
        "initial_step": workflow.initial_step,
        "resources": dict(workflow.resources),
        "step_resources": {step: dict(resources) for step, resources in workflow.step_resources.items()},
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
        "recognize": (
            None
            if workflow.recognize is None
            else {
                "file": workflow.recognize.file,
                "priority": workflow.recognize.priority,
                "requires": list(workflow.recognize.requires),
            }
        ),
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


def _resources_text(resources: Any) -> dict[str, object]:
    """Return one resource mapping with its time labels as Slurm durations."""

    return {name: format_duration(value) if name in TIME_RESOURCES else value for name, value in resources.items()}


def _render_text(description: Mapping[str, object]) -> str:
    build = description["build"]
    assert isinstance(build, Mapping)
    build_line = (
        f"build: yes (command={build['command']}, platform={build['platform'] or '-'}, "
        f"artifacts={_value(build['artifacts'])})"
        if build["present"]
        else "build: no"
    )
    step_resources = description["step_resources"]
    assert isinstance(step_resources, Mapping)
    lines = [
        f"workflow: {description['workflow']}",
        f"alias: {description['alias'] or '-'}",
    ]
    source = description["source"]
    assert isinstance(source, Mapping)
    if source["kind"] == "installed":
        lines.append(f"source: installed {source['uri']} ({source['path']})")
    elif source["kind"] == "workspace":
        lines.append(f"source: workspace installation {source['id']} of {source['source']} ({source['path']})")
    else:
        lines.append(f"source: {source['kind']} ({source['path']})")
    lines.extend(
        [
            f"summary: {description['summary'] or '-'}",
            build_line,
            f"resources: {_value(_resources_text(description['resources']))}" if description["resources"] else "",
            (
                f"step_resources: {_value({step: _resources_text(value) for step, value in step_resources.items()})}"
                if step_resources
                else ""
            ),
            "steps:",
        ]
    )
    lines = [line for line in lines if line]
    recognize = description.get("recognize")
    if isinstance(recognize, Mapping):
        lines.append(
            f"recognize: collector only, never run (file={recognize['file']}, "
            f"priority={recognize['priority']}, requires={_value(recognize['requires'])})"
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


_WORKFLOW_COMMAND = ("httk", "workflow")


@contextlib.contextmanager
def _stdout_to_stderr(enabled: bool) -> Iterator[None]:
    """Send what a build prints (it inherits standard output) to standard error, keeping JSON output clean."""

    if not enabled:
        yield
        return
    sys.stdout.flush()
    saved = os.dup(1)
    os.dup2(2, 1)
    try:
        yield
    finally:
        sys.stdout.flush()
        os.dup2(saved, 1)
        os.close(saved)


def _installed_row(installed: _store.Installed) -> dict[str, object]:
    record = installed.record
    builds = installed.directory / "builds"
    return {
        "id": installed.id,
        "name": installed.name,
        "source": record.get("source"),
        "tree_sha256": record.get("tree_sha256"),
        "installed_at": record.get("installed_at"),
        "builds": sorted(path.parent.name for path in builds.glob("*/build.json")),
    }


def _remote_relay(binding: WorkspaceBinding, context: Any, arguments: argparse.Namespace, verb: str, *tail: str) -> int:
    """Run ``httk workflow VERB … --workspace NAME`` on the remote that holds the workspace."""

    flags = [flag for flag in ("--json", "--check") if getattr(arguments, flag.lstrip("-"), False)]
    return _remote_workspace_read(
        binding,
        context,
        (*_WORKFLOW_COMMAND, verb),
        arguments,
        tail=(*flags, *tail, "--workspace"),
        unwrap_json_array=False,
    )


def _print_rows(rows: Sequence[Mapping[str, object]], arguments: argparse.Namespace, text: Any) -> None:
    if arguments.json:
        print(json.dumps(list(rows), indent=2, sort_keys=True))
    else:
        for row in rows:
            print(text(row))


def handle_workflow_describe(arguments: argparse.Namespace, context: Any) -> int:
    """Describe workflows of this machine, or the installations in a workspace, without changing anything."""

    workspace: Workspace | None = None
    if arguments.workspace is not None:
        binding, root = _resolve_binding(arguments, context)
        if root is None:
            assert binding is not None
            format_flags = () if arguments.format is None else ("--format", arguments.format)
            return _remote_relay(binding, context, arguments, "describe", *format_flags, *arguments.targets)
        if arguments.format is not None:
            raise ValueError("--format describes a bare document on this machine; an installation names its format")
        workspace = Workspace(root)
    descriptions: list[dict[str, object]] = []
    failed = False
    for target in arguments.targets:
        try:
            description = (
                _workflow_description(target, arguments.format)
                if workspace is None
                else _installed_description(workspace, target)
            )
        except (OSError, ValueError, RuntimeError) as exc:
            failed = True
            print(f"{target}: {exc}", file=sys.stderr)
            continue
        descriptions.append(description)
        if not arguments.json:
            print(f"{target}:")
            print(_render_text(description))
    if arguments.json:
        print(json.dumps(descriptions, indent=2, sort_keys=True))
    return 1 if failed else 0


def handle_workflow_list(arguments: argparse.Namespace, context: Any) -> int:
    """List the workflows of this machine, or the workflows installed in a workspace.

    Without ``--workspace`` the listing is the union ``--workflow`` resolves
    names against: the workflows ``register_workflow`` added in this process,
    the workflows installed plugins bundle, then fetched git workflows by short
    name. With ``--workspace`` it is that workspace's store.

    :param arguments: The parsed ``list`` arguments.
    :param context: The invocation context.
    :return: The process exit status.
    """

    if arguments.workspace is None:
        return _list_this_machine(arguments)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_relay(binding, context, arguments, "list")
    rows = [_installed_row(installed) for installed in _store.list_installed(Workspace(root))]
    if not rows and not arguments.json:
        print(f"no workflows installed in {root}")
    _print_rows(rows, arguments, lambda row: f"{row['id']}\t{row['name']}\t{row['source']}")
    return 0


def _list_this_machine(arguments: argparse.Namespace) -> int:
    """List the workflows a name selects on this machine."""

    plugin_providers = installed_plugin_workflows()
    owners = installed_plugin_workflow_owners()
    rows: list[dict[str, object]] = []
    for workflow_id in registered_workflows():
        provider = workflow_provider(workflow_id)
        # A plugin-sourced id resolves to the very provider the plugin discovery
        # holds; an in-process registration of the same id shadows it, so an
        # identity check — not mere membership — decides the source.
        is_plugin = provider is not None and provider is plugin_providers.get(workflow_id)
        owner = owners.get(workflow_id) if is_plugin else None
        is_installed = provider is not None and provider.definition_uri is not None
        row_source: dict[str, object] = {
            "kind": "plugin" if is_plugin else "installed" if is_installed else "registered",
            "plugin": owner,
        }
        if provider is not None and is_installed:
            row_source["uri"] = provider.workflow_id
        rows.append(
            {
                "workflow": workflow_id,
                "alias": provider.alias if provider is not None else None,
                "source": row_source,
                "summary": (provider.summary or None) if provider is not None else None,
            }
        )
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if not rows:
        print("no workflows registered")
        return 0
    for row in rows:
        source = row["source"]
        assert isinstance(source, dict)
        source_text = (
            f"plugin {source['plugin']}"
            if source["kind"] == "plugin"
            else f"installed {source['uri']}"
            if source["kind"] == "installed"
            else "registered"
        )
        print(f"{row['workflow']}\t{row['alias'] or '-'}\t{source_text}\t{row['summary'] or '-'}")
    return 0


def _install_on_this_machine(arguments: argparse.Namespace) -> int:
    """Fetch git workflows into this machine's cache, without a workspace."""

    rows: list[dict[str, object]] = []
    failed = False
    for target in arguments.sources:
        try:
            if not target.startswith("git+"):
                raise ValueError(
                    "without --workspace, install takes a git+https://HOST/PATH[@REF][#SUBDIR] URI; "
                    "give --workspace WS to install a package, runner file or document into a workspace"
                )
            provider = fetch_workflow(target)
        except (OSError, ValueError) as exc:
            failed = True
            print(f"{target}: {exc}", file=sys.stderr)
            continue
        rows.append({"uri": provider.workflow_id, "name": provider.name, "alias": provider.alias})
        if not arguments.json:
            print(f"{provider.workflow_id}\t{provider.name}")
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    return 1 if failed else 0


def _consume(workspace: Workspace, source: str) -> None:
    """Remove a remote install's pushed copy, ``tmp/push.<hex>/<name>`` of this workspace, and nothing else."""

    parent = Path(source).resolve().parent
    tmp = (workspace.control / "tmp").resolve()
    if parent.parent != tmp or not parent.name.startswith("push."):
        raise ValueError(f"--consume removes only a pushed copy below {tmp}/push.*: {source}")
    _fs.discard(_fs.loc(parent), trash_dir=tmp, durable=workspace.durable)


def handle_workflow_install(arguments: argparse.Namespace, context: Any) -> int:
    """Install workflows into a workspace's store, or fetch git workflows on this machine without one."""

    if arguments.workspace is None:
        return _install_on_this_machine(arguments)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _install_remote(binding, context, arguments)
    workspace = _modifiable(arguments, context, action="install workflows into it")
    rows: list[dict[str, object]] = []
    failed = False
    with _kernel.register_owner(
        workspace, kind="cli", label="workflow install", allocation=None, advertised={}
    ) as owner:
        for source in arguments.sources:
            try:
                with _stdout_to_stderr(arguments.json):
                    installed = _store.install(
                        workspace,
                        owner,
                        source,
                        calls=not arguments.no_calls,
                        build=not arguments.no_build,
                        language=arguments.format,
                    )
            except _ERRORS as exc:
                failed = True
                print(f"{source}: {exc}", file=sys.stderr)
                continue
            finally:
                if arguments.consume:
                    _consume(workspace, source)
            rows.append(_installed_row(installed))
    _print_rows(rows, arguments, lambda row: f"{row['id']}\t{row['name']}")
    return 1 if failed else 0


def _push_source(source: str) -> str | Path:
    """Resolve an install source here: the pinned git URI the remote installs, or the local tree or file to push."""

    path = Path(source).expanduser()
    if path.exists():
        return path.resolve()
    if source.startswith("git+"):
        return fetch_workflow(source).workflow_id
    provider = workflow_provider(source)
    if provider is None:
        raise ValueError(f"no workflow package, git URI, runner file or known workflow name: {source!r}")
    if provider.definition_uri is not None:
        return provider.definition_uri
    if provider.directory is None:
        raise ValueError(f"workflow {source!r} is registered in-process only and cannot be installed")
    return provider.directory


def _install_remote(binding: WorkspaceBinding, context: Any, arguments: argparse.Namespace) -> int:
    """Resolve each source here, push a local tree or file with the adapter, and install it on the remote.

    A git source is installed by its commit-pinned URI, which the remote fetches.
    """

    target = resolve_remote(binding.remote, project=context.cwd)
    name = binding.name.split(":", 1)[1]
    _, root = probe_remote_workspace(target, name, timeout=arguments.adapter_timeout)
    flags = [flag for flag in ("--no-calls", "--no-build", "--json") if getattr(arguments, flag[2:].replace("-", "_"))]
    if arguments.format is not None:
        flags += ["--format", arguments.format]
    status = 0
    for source in arguments.sources:
        try:
            local = _push_source(source)
            if isinstance(local, str):
                tail = [local]
            else:
                destination = f"{root.rstrip('/')}/{WORKSPACE_DIRECTORY}/tmp/push.{uuid.uuid4().hex}/{local.name}"
                pushed = run_adapter(
                    target.bundle,
                    "push",
                    {"source": str(local), "destination": destination},
                    timeout=arguments.adapter_timeout,
                )
                tail = ["--consume", str(pushed.get("path", destination))]
            result = run_adapter(
                target.bundle,
                "invoke",
                {"argv": [*_WORKFLOW_COMMAND, "install", *flags, "--workspace", name, *tail]},
                timeout=arguments.adapter_timeout,
            )
        except _ERRORS as exc:
            status = 1
            print(f"{source}: {exc}", file=sys.stderr)
            continue
        sys.stdout.write(str(result.get("stdout", "")))
        sys.stderr.write(str(result.get("stderr", "")))
        status = max(status, int(result.get("returncode", 0) or 0))
    return status


def _not_installable(selector: str) -> str | None:
    """Explain why a selector names a workflow uninstall cannot remove."""

    from ..packages import installed_plugin_workflows
    from ..scaffold import _WORKFLOW_PROVIDERS

    def claims(providers: Mapping[str, Any]) -> bool:
        return selector in providers or any(provider.alias == selector for provider in providers.values())

    if claims(installed_plugin_workflows()):
        return f"{selector!r} is a plugin workflow; remove its plugin with httk plugin uninstall"
    if claims(_WORKFLOW_PROVIDERS):
        return f"{selector!r} is registered in-process and cannot be uninstalled"
    return None


def _users(workspace: Workspace, workflow_id: str) -> list[str]:
    """Return the keys of the unfinished jobs of one workflow."""

    states = ("ready", "waiting", "paused", "owned")
    return [
        ref.job_key
        for ref in iter_jobs(workspace, states)
        if (job := read_job(ref)[0]) is not None and job.workflow_id == workflow_id
    ]


def handle_workflow_uninstall(arguments: argparse.Namespace, context: Any) -> int:
    """Remove workflows from a workspace's store, or forget fetched git workflows on this machine."""

    if arguments.workspace is None:
        return _uninstall_on_this_machine(arguments)
    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        return _remote_relay(binding, context, arguments, "uninstall", *arguments.selectors)
    workspace = _modifiable(arguments, context, action="uninstall workflows from it")
    rows: list[dict[str, object]] = []
    failed = False
    with _kernel.register_owner(
        workspace, kind="cli", label="workflow uninstall", allocation=None, advertised={}
    ) as owner:
        for selector in arguments.selectors:
            try:
                installed = _store.lookup(workspace, selector)
                if arguments.check and installed is not None:
                    for job_key in _users(workspace, installed.id):
                        print(f"warning: unfinished job {job_key} uses {installed.id}", file=sys.stderr)
                removed = _store.uninstall(workspace, owner, selector)
            except _ERRORS as exc:
                failed = True
                print(f"{selector}: {exc}", file=sys.stderr)
                continue
            rows.append({"id": removed.id, "name": removed.name})
    _print_rows(rows, arguments, lambda row: f"{row['id']}\t{row['name']}")
    return 1 if failed else 0


def _uninstall_on_this_machine(arguments: argparse.Namespace) -> int:
    """Forget fetched git workflows on this machine by short name or URI."""

    rows: list[dict[str, object]] = []
    failed = False
    for selector in arguments.selectors:
        try:
            removed = _uninstall_workflows(selector)
        except (OSError, ValueError) as exc:
            failed = True
            print(f"{selector}: {_not_installable(selector) or exc}", file=sys.stderr)
            continue
        for member in removed:
            rows.append({"uri": member.uri, "names": list(member.names)})
            if not arguments.json:
                print(f"{member.uri}\t{', '.join(member.names)}")
    if arguments.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    return 1 if failed else 0


def _add_store_options(parser: argparse.ArgumentParser, help_text: str) -> None:
    parser.add_argument("--workspace", metavar="WORKSPACE", help=help_text)
    parser.add_argument("--json", action="store_true", help="print the result as one JSON array")
    _add_adapter_timeout(parser)


def build_list_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Declare the top-level ``list`` workflow command."""

    listing = _leaf(
        subparsers,
        "list",
        summary="list this machine's workflows, or a workspace's installed workflows",
        description=(
            "Without --workspace, list the workflows a name selects on this machine: those registered in this "
            "process, those installed plugins bundle, then fetched git workflows by short name. With --workspace, "
            "list the workflows installed in that workspace (REMOTE:WS through the remote's adapter)"
        ),
        handler=handle_workflow_list,
    )
    _add_store_options(listing, "list the workflows installed in this workspace")


def build_describe_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Declare the top-level ``describe`` workflow command."""

    describe = _leaf(
        subparsers,
        "describe",
        summary="describe a workflow, or an installed one",
        description=(
            "Describe a registered workflow, git workflow URI, runner file, or workflow package directory; with "
            "--workspace, describe the installation of an id or short name in that workspace"
        ),
        handler=handle_workflow_describe,
    )
    describe.add_argument(
        "targets",
        metavar="TARGET",
        nargs="+",
        help="workflow id, alias, git+https://…@ref#subdir URI, runner file, or package directory",
    )
    describe.add_argument(
        "--format",
        metavar="FORMAT",
        help="force FORMAT (cwl, pwd, jobflow, httk-v1) for a bare workflow document or directory",
    )
    _add_store_options(describe, "describe workflows installed in this workspace")


def build_install_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Declare the top-level ``install`` and ``uninstall`` workflow commands."""

    install = _leaf(
        subparsers,
        "install",
        summary="install workflows into a workspace (or fetch git workflows on this machine)",
        description=(
            "Install each SOURCE into the workspace's store: a package directory, a git+https://HOST/PATH[@REF]"
            "[#SUBDIR] URI, a runner file or workflow document (installed ad hoc), or a workflow name known on "
            "this machine. Its declared calls are installed too, and a [workflow.build] package is built for "
            "this platform. A REMOTE:WS workspace gets the source pushed through the remote's adapter (a git "
            "source is fetched there by its pinned URI). Without --workspace, a git URI is only fetched into "
            "this machine's cache"
        ),
        handler=handle_workflow_install,
    )
    install.add_argument("sources", metavar="SOURCE", nargs="+", help="what to install")
    _add_store_options(install, "the workspace to install into (without it: fetch git URIs on this machine)")
    install.add_argument("--no-calls", action="store_true", help="do not install the declared calls")
    install.add_argument("--no-build", action="store_true", help="do not build a [workflow.build] package")
    install.add_argument("--format", metavar="FORMAT", help="force FORMAT for a bare workflow document or directory")
    install.add_argument("--consume", action="store_true", help=argparse.SUPPRESS)
    uninstall = _leaf(
        subparsers,
        "uninstall",
        summary="remove workflows from a workspace (or forget fetched git workflows)",
        description=(
            "Remove installed workflows, by id or short name, from the workspace's store; jobs are not checked "
            "unless --check is given. Without --workspace, forget fetched git workflows on this machine: a "
            "pinned URI removes that commit, an unpinned URI or a short name its whole lineage"
        ),
        handler=handle_workflow_uninstall,
    )
    uninstall.add_argument("selectors", metavar="SELECTOR", nargs="+", help="an id, short name or git workflow URI")
    _add_store_options(uninstall, "the workspace to uninstall from (without it: this machine's git workflows)")
    uninstall.add_argument("--check", action="store_true", help="warn about unfinished jobs of the workflow")
