"""Scaffolding submitted jobs from a workflow, some files, and some inputs.

A job is a payload directory plus a ``job.json`` that names the runner to execute,
and building one by hand means knowing the runner's workflow name, its initial
step, its digest, and where its inputs live in the payload. This module is the
short way: :func:`new_job` takes a *workflow* — a packaged runner a domain
registered by name, or the path of a runner file of your own — stages the files
the runner reads, writes the ``job.json``, and submits the result, all in one
call.

Packaged workflows are not known to this module. A domain or compat engine
registers each one it ships with :func:`~httk.workflow.scaffold.register_workflow`, supplying only the
generic description of a starting point — its name, the runner it starts from,
the workflow and steps that runner declares, the modes a job of it defaults to,
and what it does — so the scaffold resolves and pins a workflow without ever
importing the science that owns it.

.. code-block:: python

    from httk.workflow import Workspace
    from httk.workflow.scaffold import new_job

    workspace = Workspace.initialize("workflow-workspace")
    job = new_job(workspace, "some-workflow", files={"input": "input"}, tag="example")
    print(job.job_key, job.payload)

By default the runner file is *published into the workspace runner store*, and the
job references it there by digest. That is what makes a scaffolded job durable:
the bytes that will run are pinned in the workspace, so upgrading the installed
*httk-workflow* underneath a queued campaign cannot change what its jobs execute.
Publication is content addressed — the store name carries the digest of the
bytes — so scaffolding the same workflow twice publishes nothing the second time,
and a later, different version of a packaged runner lands beside the old one
instead of replacing it. ``publish="installed"`` instead references a packaged
workflow through the reserved ``pkg:`` form, which copies nothing at all.

:func:`new_jobs` is the same operation for a campaign: one workflow resolution and
one publication amortized over every job, and a lazy iterator over the results. By
design, generating a partitioned campaign costs one payload and one marker per
job and never materializes a list of them.
"""

from __future__ import annotations

import difflib
import hashlib
import importlib
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING, Literal, TypedDict, cast

from httk.core.building import BuildSpec
from httk.core.digests import sha256_file, tree_digest
from httk.core.report import context_logger

if TYPE_CHECKING:
    from .collecting import JobRecord
    from .languages import LanguageRequest

from ._util import validate_inputs
from .errors import FormatError
from .models import (
    ATTEMPTS_DIRECTORY,
    JOB_STATE_DIRECTORY,
    LOGS_DIRECTORY,
    ensure_step_known,
    normalize_placement,
    validate_parameters,
    validate_resources,
    validate_step,
)
from .runtime_builders import JobSpec, prepare_job_payload
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_PLACEMENT",
    "FILES_DIRECTORY",
    "JOB_SCAFFOLD_FORMAT",
    "STRUCTURE_PATTERNS",
    "BuildSpec",
    "InstantiateContext",
    "JobItem",
    "ResolvedWorkflow",
    "ScaffoldedJob",
    "WorkflowProvider",
    "describe_runner",
    "new_job",
    "new_jobs",
    "payload_relative",
    "register_workflow",
    "registered_workflow",
    "registered_workflow_labels",
    "registered_workflows",
    "resolve_workflow",
    "structure_files",
    "structure_tag",
    "workflow_provider",
]

#: The format of the machine-readable report :meth:`ScaffoldedJob.as_mapping` returns.
JOB_SCAFFOLD_FORMAT = "httk-workflow-job-scaffold"
#: Where a scaffolded job is placed when nothing else is asked for.
DEFAULT_PLACEMENT = "jobs"
#: The payload directory a staged file lands in when its name has no directory of
#: its own. Every packaged runner reads its inputs from there.
FILES_DIRECTORY = "files"
#: The file names legacy callers may recognize as structures in a directory.
STRUCTURE_PATTERNS = ("POSCAR*", "*.vasp")
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
_DESCRIBE_TIMEOUT = 120.0
_INSTANTIATE_TIMEOUT = 3600.0
_TAG_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789._-"
_MAXIMUM_TAG_LENGTH = 48

type DataMode = Literal["none", "transactional"]
type WorkdirMode = Literal["persistent", "isolated"]
type PublishMode = Literal["workspace", "installed"]


@dataclass(frozen=True)
class WorkflowProvider:
    """One packaged workflow a domain or compat engine offers by name.

    A provider is the generic description of a starting point. It names the
    packaged runner it starts from by the package the runner file is a module of
    and the file beside that module — so the reserved ``pkg:`` form and the
    digest are resolved from the provider alone — and it declares the runner's
    workflow, its steps, the modes a job of it defaults to, and what it does.
    Declaring the steps here rather than running the runner to ask keeps
    scaffolding cheap; the owning domain's tests hold the declaration to what the
    runner really describes.

    :param workflow_id: Name the workflow in registrations and job definitions.
    :param runner_package: Name the package containing a packaged runner.
    :param runner_file: Name the runner file beside the package module.
    :param language: Name the language runner realization, when applicable.
    :param document: Name the language document package member.
    :param runner_options: Supply language-specific runner options.
    :param initial_step: Select the default starting step.
    :param alias: Provide an alternate registered name.
    :param steps: Declare the steps the runner provides.
    :param resources: Declare the default resource requirement.
    :param step_resources: Declare per-step resource requirements.
    :param data_mode: Declare the workflow's default data mode.
    :param workdir_mode: Declare the workflow's default workdir mode.
    :param summary: Describe the workflow for callers.
    :param inputs: Map input names to payload destinations or hook handling.
    :param instantiate: Indicate that the workflow has an instantiate hook.
    :param declarations: Declare workflow declaration documents.
    :param collector: Identify the optional result collector.
    :param directory: Locate a directory-sourced workflow package.
    :param build: Describe the package build command and generated artifacts.
    :param entry: Name the directory package's runner entry.
    :param instantiate_file: Name the directory package's instantiate hook.
    :param instantiate_exec: Name an executable directory package instantiate hook.
    :param collect_file: Name the directory package's collector.
    :param collector_exec: Name an executable directory package collector.
    :param postprocess_scripts: Map curated postprocess script names to package members and descriptions.
    :param parameters: Declare the workflow's parameter metadata.
    :param environment: Declare the workflow's environment metadata.
    :param outputs: Declare the workflow's output metadata.
    :param declaration_uri: Identify the source declaration URI.
    :param declaration_file: Name the source declaration file.
    """

    workflow_id: str
    runner_package: str | None = None
    runner_file: str | None = None
    language: str | None = None
    document: str | None = None
    runner_options: Mapping[str, object] = field(default_factory=dict)
    initial_step: str = "start"
    alias: str | None = None
    steps: tuple[str, ...] = ()
    resources: Mapping[str, int] = field(default_factory=dict)
    step_resources: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    data_mode: DataMode = "none"
    workdir_mode: WorkdirMode = "persistent"
    summary: str = ""
    inputs: Mapping[str, str | None] = field(default_factory=dict)
    instantiate: bool = False
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    collector: Callable[[JobRecord], Mapping[str, object]] | str | None = None
    directory: Path | None = None
    build: BuildSpec | None = None
    entry: str = "run"
    instantiate_file: str | None = None
    instantiate_exec: str | None = None
    collect_file: str | None = None
    collector_exec: str | None = None
    postprocess_scripts: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    parameters: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    outputs: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    declaration_uri: str | None = None
    declaration_file: str | None = None
    _input_metadata: Mapping[str, Mapping[str, object]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.language is not None:
            if self.runner_package is not None or self.runner_file is not None:
                raise ValueError("a language workflow cannot supply a runner package or file")
            if self.directory is None:
                raise ValueError("a language workflow provider must be directory-sourced")
            if self.entry != "run" or self.instantiate_file is not None:
                raise ValueError("language workflow runner fields are implied by the language")
            if not self.instantiate:
                raise ValueError("a language workflow provider must have instantiate enabled")
        else:
            packaged = self.runner_package is not None or self.runner_file is not None
            if packaged != (self.runner_package is not None and self.runner_file is not None):
                raise ValueError("a workflow provider must supply both runner_package and runner_file")
            if packaged == (self.directory is not None):
                raise ValueError("a workflow provider must be packaged or directory-sourced, exclusively")
            if packaged and (
                self.entry != "run"
                or self.instantiate_file is not None
                or self.instantiate_exec is not None
                or self.collect_file is not None
                or self.declaration_file is not None
                or self.build is not None
            ):
                raise ValueError("directory-only fields are not allowed on a packaged workflow provider")
            if self.directory is not None and (not self.entry or PurePosixPath(self.entry).is_absolute()):
                raise ValueError(f"workflow directory entry must be a relative member: {self.entry!r}")
        inputs = validate_inputs(self.inputs)
        if any(destination is None for destination in inputs.values()) and not self.instantiate:
            raise ValueError(f"workflow {self.workflow_id!r} has hook-consumed inputs and requires an instantiate hook")
        if self.alias is not None and (
            not isinstance(self.alias, str) or re.fullmatch(r"[a-z0-9._-]+", self.alias) is None
        ):
            raise ValueError(f"workflow alias must match [a-z0-9._-]+: {self.alias!r}")
        object.__setattr__(self, "inputs", MappingProxyType(inputs))
        object.__setattr__(self, "runner_options", MappingProxyType(dict(self.runner_options)))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        object.__setattr__(
            self, "resources", MappingProxyType(validate_resources(self.resources, "workflow.resources"))
        )
        object.__setattr__(
            self,
            "step_resources",
            MappingProxyType(
                {
                    validate_step(step, "workflow step"): validate_resources(requirement, f"workflow.steps.{step}")
                    for step, requirement in self.step_resources.items()
                }
            ),
        )
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(self, "outputs", MappingProxyType(dict(self.outputs)))
        object.__setattr__(self, "postprocess_scripts", MappingProxyType(dict(self.postprocess_scripts)))
        object.__setattr__(self, "_input_metadata", MappingProxyType(dict(self._input_metadata)))


#: Every registered workflow, keyed by name in registration order. The scaffold
#: ships this empty: a domain populates it as an import side effect, so the
#: generic layer resolves a domain workflow without importing the domain.
_WORKFLOW_PROVIDERS: dict[str, WorkflowProvider] = {}


def register_workflow(provider: WorkflowProvider) -> None:
    """Register one packaged workflow, replacing any registered under its name.

    A domain calls this once per workflow it ships when its package is imported,
    which is how ``httk workflow job new --workflow NAME`` resolves a packaged
    runner the generic scaffold never names.

    :param provider: Supply the workflow provider to register.
    :raises ValueError: If the provider's id or alias collides with another registration.
    """

    collisions = {
        candidate
        for key, existing in _WORKFLOW_PROVIDERS.items()
        if key != provider.workflow_id
        for candidate in (existing.workflow_id, existing.alias)
        if candidate is not None
    }
    if provider.workflow_id in collisions or (provider.alias is not None and provider.alias in collisions):
        raise ValueError(f"workflow id or alias collides with an existing registration: {provider.workflow_id!r}")
    if provider.alias == provider.workflow_id:
        raise ValueError(f"workflow alias must differ from workflow id: {provider.alias!r}")
    _WORKFLOW_PROVIDERS[provider.workflow_id] = provider


def registered_workflows() -> tuple[str, ...]:
    """Return registered ids, followed by sorted installed-plugin workflow ids.

    :return: The registered workflow ids.
    """

    result = list(_WORKFLOW_PROVIDERS)
    registered_names = set(result)
    registered_names.update(provider.alias for provider in _WORKFLOW_PROVIDERS.values() if provider.alias)
    from .packages import installed_plugin_workflows

    result.extend(
        workflow_id for workflow_id in sorted(installed_plugin_workflows()) if workflow_id not in registered_names
    )
    return tuple(result)


#: File suffixes that make an argument path-shaped: a runner file or a language
#: document, versus a dotted registered id like ``domain.family.step``.
_RUNNER_SUFFIXES = frozenset({".py", ".cwl", ".json", ".yaml", ".yml"})


def registered_workflow_labels() -> tuple[str, ...]:
    """Return display labels for registered and installed-plugin workflows.

    :return: The registered workflow labels, in registration order.
    """

    labels = [
        f"{workflow_id} ({provider.alias})" if provider.alias else workflow_id
        for workflow_id, provider in _WORKFLOW_PROVIDERS.items()
    ]
    registered_names = set(_WORKFLOW_PROVIDERS)
    registered_names.update(provider.alias for provider in _WORKFLOW_PROVIDERS.values() if provider.alias)
    from .packages import (
        _plugin_workflow_conflicts,
        installed_plugin_workflow_owners,
        installed_plugin_workflows,
    )

    conflicts = _plugin_workflow_conflicts()
    owners = installed_plugin_workflow_owners()
    providers = installed_plugin_workflows()
    for workflow_id in sorted(providers):
        provider = providers[workflow_id]
        if workflow_id in registered_names or workflow_id in conflicts:
            continue
        alias = provider.alias if provider.alias not in conflicts else None
        label = f"{workflow_id} ({alias})" if alias else workflow_id
        labels.append(f"{label} [plugin {owners[workflow_id]}]")
    return tuple(labels)


def _registered_names() -> list[str]:
    """Return every registered or installed-plugin id and alias."""

    names: list[str] = []
    for workflow_id, provider in _WORKFLOW_PROVIDERS.items():
        names.append(workflow_id)
        if provider.alias:
            names.append(provider.alias)
    from .packages import _plugin_workflow_conflicts, installed_plugin_workflows

    conflicts = _plugin_workflow_conflicts()
    providers = installed_plugin_workflows()
    for workflow_id in sorted(providers):
        provider = providers[workflow_id]
        if workflow_id not in conflicts:
            names.append(workflow_id)
        if provider.alias and provider.alias not in conflicts:
            names.append(provider.alias)
    return names


def workflow_provider(name: str) -> WorkflowProvider | None:
    """Return the provider selected by canonical id or alias.

    :param name: Select a workflow by id or alias.
    :return: The selected provider, or ``None`` when no provider matches.
    """

    provider = _WORKFLOW_PROVIDERS.get(name)
    if provider is not None:
        return provider
    for candidate in _WORKFLOW_PROVIDERS.values():
        if candidate.alias == name:
            return candidate

    from .packages import _plugin_workflow_conflicts, installed_plugin_workflows

    conflict = _plugin_workflow_conflicts().get(name)
    if conflict is not None:
        plugins = ", ".join(repr(plugin) for plugin in conflict)
        raise ValueError(f"workflow name {name!r} is bundled by multiple plugins: {plugins}")
    providers = installed_plugin_workflows()
    provider = providers.get(name)
    if provider is not None:
        return provider
    for candidate in providers.values():
        if candidate.alias == name:
            return candidate
    return None


class JobItem(TypedDict, total=False):
    """What one job of a :func:`new_jobs` campaign varies from the shared values.

    Every member is optional, and a member that is absent takes the value
    :func:`new_jobs` was called with. ``inputs`` and ``files`` are merged over the
    shared mappings key by key; everything else replaces the shared value.
    """

    inputs: Mapping[str, object]
    files: Mapping[str, str | os.PathLike[str]]
    parameters: Mapping[str, object]
    environment: Mapping[str, object]
    tag: str | None
    name: str
    placement: str | PurePosixPath
    priority: int | None


@dataclass(frozen=True)
class ResolvedWorkflow:
    """One resolved starting point for a job: a runner file and how to run it.

    A workflow is either one of the runners a domain registered by name — see
    :func:`~httk.workflow.scaffold.registered_workflows` — or a runner file of your own, which is
    described by running it — every native runner answers ``--describe`` with its
    workflow and its steps — so a scaffolded job never guesses either.

    :param source: Locate the runner file or workflow package directory.
    :param workflow_id: Name the resolved workflow.
    :param language: Name the language runner realization, when applicable.
    :param document: Name the language document package member.
    :param runner_options: Preserve language-specific runner options.
    :param document_path: Locate the absolute language document.
    :param initial_step: Select the step a job starts at.
    :param alias: Preserve the registered workflow alias.
    :param steps: Preserve the steps the runner provides.
    :param resources: Preserve the default resource requirement.
    :param step_resources: Preserve per-step resource requirements.
    :param data_mode: Preserve the workflow data mode.
    :param workdir_mode: Preserve the workflow workdir mode.
    :param packaged: Preserve the packaged runner file name when applicable.
    :param runner_package: Preserve the package containing packaged members.
    :param registration_id: Preserve the registration id when applicable.
    :param summary: Describe the resolved workflow.
    :param inputs: Map input names to payload destinations or hook handling.
    :param instantiate: Indicate that the workflow has an instantiate hook.
    :param declarations: Preserve workflow declaration documents.
    :param collector: Preserve the optional result collector.
    :param directory: Locate a directory-sourced workflow package.
    :param build: Describe the package build command and generated artifacts.
    :param entry: Name the directory package's runner entry.
    :param instantiate_file: Name the directory package's instantiate hook.
    :param instantiate_exec: Name an executable directory package instantiate hook.
    :param collect_file: Name the directory package's collector.
    :param collector_exec: Name an executable directory package collector.
    :param postprocess_scripts: Preserve curated postprocess script metadata.
    :param parameters: Preserve the workflow's parameter metadata.
    :param environment: Preserve the workflow's environment metadata.
    :param outputs: Preserve the workflow's output metadata.
    :param declaration_uri: Identify the source declaration URI.
    :param declaration_file: Name the source declaration file.
    """

    source: Path
    workflow_id: str
    initial_step: str
    language: str | None = None
    document: str | None = None
    runner_options: Mapping[str, object] = field(default_factory=dict)
    document_path: Path | None = None
    alias: str | None = None
    steps: tuple[str, ...] = ()
    resources: Mapping[str, int] = field(default_factory=dict)
    step_resources: Mapping[str, Mapping[str, int]] = field(default_factory=dict)
    data_mode: DataMode = "none"
    workdir_mode: WorkdirMode = "persistent"
    packaged: str | None = None
    runner_package: str | None = None
    registration_id: str | None = None
    summary: str = ""
    inputs: Mapping[str, str | None] = field(default_factory=dict)
    instantiate: bool = False
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    collector: Callable[[JobRecord], Mapping[str, object]] | str | None = None
    directory: Path | None = None
    build: BuildSpec | None = None
    entry: str = "run"
    instantiate_file: str | None = None
    instantiate_exec: str | None = None
    collect_file: str | None = None
    collector_exec: str | None = None
    postprocess_scripts: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    parameters: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    outputs: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    declaration_uri: str | None = None
    declaration_file: str | None = None
    _input_metadata: Mapping[str, Mapping[str, object]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.language is not None:
            if self.packaged is not None or (self.directory is None and self.document_path is None):
                raise ValueError("a language workflow must have a source document or directory")
            if self.entry != "run" or self.instantiate_file is not None:
                raise ValueError("language workflow runner fields are implied by the language")
            if not self.instantiate:
                raise ValueError("a language workflow must have instantiate enabled")
        else:
            if self.packaged is not None and self.directory is not None:
                raise ValueError("a resolved workflow cannot be packaged and directory-sourced")
            if self.packaged is not None and (
                self.entry != "run"
                or self.instantiate_file is not None
                or self.instantiate_exec is not None
                or self.collect_file is not None
                or self.declaration_file is not None
                or self.build is not None
            ):
                raise ValueError("directory-only fields are not allowed on a packaged resolved workflow")
        object.__setattr__(self, "runner_options", MappingProxyType(dict(self.runner_options)))
        object.__setattr__(self, "postprocess_scripts", MappingProxyType(dict(self.postprocess_scripts)))
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(
            self, "resources", MappingProxyType(validate_resources(self.resources, "workflow.resources"))
        )
        object.__setattr__(
            self,
            "step_resources",
            MappingProxyType(
                {
                    validate_step(step, "workflow step"): validate_resources(requirement, f"workflow.steps.{step}")
                    for step, requirement in self.step_resources.items()
                }
            ),
        )

    @property
    def store_name(self) -> str:
        """Return the content-addressed name this workflow takes in a runner store.

        The digest of the bytes is part of the name, so publishing is idempotent
        for identical bytes and never overwrites a name a submitted job pinned:
        an upgraded packaged runner is published beside the version its queued
        jobs still reference.

        :return: The digest-pinned runner-store name.
        """

        if self.language is not None:
            raise ValueError("a language workflow is never published to the runner store")
        if self.directory is not None:
            from .packages import source_tree_digest

            return f"{self.directory.name}.{source_tree_digest(self.directory)[:12]}"
        digest = sha256_file(self.source)
        stem = self.source.name
        suffix = ""
        for candidate in (".py", ".sh", ".bash"):
            if stem.endswith(candidate):
                stem, suffix = stem[: -len(candidate)], candidate
                break
        return f"{stem}.{digest[:12]}{suffix}"


@dataclass(frozen=True)
class ScaffoldedJob:
    """Describe one job this module submitted.

    :param job_id: Identify the submitted job.
    :param job_key: Identify the job payload and state markers.
    :param tag: Preserve the optional job tag.
    :param placement: Locate the job within the workspace.
    :param payload: Locate the submitted payload.
    :param marker: Locate the submitted state marker.
    :param workflow: Name the workflow the job runs.
    :param initial_step: Name the step the job starts at.
    :param runner: Describe the pinned runner.
    :param warnings: Preserve the preparation warnings raised for this workflow.
    """

    job_id: str
    job_key: str
    tag: str | None
    placement: PurePosixPath
    payload: Path
    marker: Path
    workflow: str
    initial_step: str
    runner: Mapping[str, object]
    warnings: tuple[str, ...] = ()

    def as_mapping(self) -> dict[str, object]:
        """Return the machine-readable report of this job.

        :return: The serialized job report.
        """

        return {
            "format": JOB_SCAFFOLD_FORMAT,
            "format_version": 2,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "tag": self.tag,
            "placement": self.placement.as_posix(),
            "payload_path": str(self.payload),
            "marker_path": str(self.marker),
            "workflow": self.workflow,
            "initial_step": self.initial_step,
            "runner": dict(self.runner),
        }


@dataclass(frozen=True)
class _Prepared:
    """A workflow whose runner is resolved once for every job that will use it."""

    workflow: ResolvedWorkflow
    runner_source: Literal["payload", "workspace", "installed"]
    runner_path: str
    runner_sha256: str | None
    data_mode: DataMode
    runner_executor: str = "path"
    payload_runner: str | None = None
    workdir_path: str | None = None
    required_capabilities: tuple[str, ...] = ()
    reserved_parameters: tuple[str, ...] = ()
    documents: Mapping[str, str | bytes] = field(default_factory=dict)
    files: Mapping[str, Path] = field(default_factory=dict)
    parameters: Mapping[str, object] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    instantiate: Callable[[InstantiateContext], object] | None = None
    instantiate_exec: tuple[Path, str, str] | None = None
    finalize: Callable[[JobSpec], JobSpec] | None = None


@dataclass
class InstantiateContext:
    """The mutable creation-time view passed to a runner's instantiate hook.

    ``payload`` is the staging root, ``inputs`` contains every supplied staged
    input, ``parameters`` is the merged opaque knob mapping, and ``tag`` is the
    caller's tag. The hook may write below ``payload`` and update
    ``parameters``; use :meth:`suggest_tag` to provide a tag without overriding one
    the caller supplied.

    :param payload: Locate the payload being staged.
    :param inputs: Provide the supplied workflow inputs.
    :param parameters: Provide the merged job parameters.
    :param tag: Preserve or suggest the job tag.
    """

    payload: Path
    inputs: Mapping[str, object]
    parameters: dict[str, object]
    tag: str | None

    def suggest_tag(self, tag: str) -> None:
        """Set a tag only when the caller did not supply one."""

        if self.tag is None:
            self.tag = tag


def describe_runner(runner: str | os.PathLike[str]) -> dict[str, object]:
    """Return the self-description one runner file prints, by running it.

    Every native runner — Python or Bash — answers ``HTTK_WORKFLOW_DESCRIBE=1``
    with its workflow name and its registered steps and exits without touching
    anything, which is how a runner nobody wrote a workflow for is still
    scaffolded without being told what it implements.

    :param runner: Locate the runner file to describe.
    :return: The validated runner description.
    :raises ValueError: If the runner is missing, cannot run, or emits an invalid description.
    """

    # Resolve before building the exec command: a relative or bare name like
    # ``./relax`` normalizes to ``relax``, and exec-ing a name with no slash does
    # a PATH lookup, not a cwd-relative run — a FileNotFoundError at best and a
    # hijack by a same-named program on PATH at worst.
    path = Path(runner).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"a runner workflow must be an existing file: {path}")
    environment = dict(os.environ)
    environment[_DESCRIBE_VARIABLE] = "1"
    shell = Path(__file__).with_name("shell")
    environment["HTTK_WORKFLOW_BASH_API"] = str(shell / "httk-workflow.sh")
    environment["HTTK_WORKFLOW_PERL_API"] = str(Path(__file__).with_name("native") / "perl")
    environment["HTTK_WORKFLOW_VASP_BASH_API"] = str(shell / "httk-vasp.sh")
    # Describing is a pure read of the program, so no attempt context of a
    # surrounding job may leak into it: a runner scaffolding jobs is itself running
    # inside one.
    for name in (
        "HTTK_WORKFLOW_CONTEXT",
        "HTTK_WORKFLOW_CONTROL_DIR",
        "HTTK_WORKFLOW_JOB_DIR",
        "HTTK_WORKFLOW_WORKDIR",
        "HTTK_WORKFLOW_WORKSPACE_DIR",
        "HTTK_WORKFLOW_DATA_DIR",
        "HTTK_WORKFLOW_STEP",
    ):
        environment.pop(name, None)
    try:
        completed = subprocess.run(
            _describe_command(path),
            capture_output=True,
            text=True,
            timeout=_DESCRIBE_TIMEOUT,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot describe the runner {path}: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        raise ValueError(
            f"the runner {path} refused to describe itself (exit {completed.returncode})"
            + (f": {detail[-1]}" if detail else "")
        )
    return _parse_description(completed.stdout, path)


def _describe_command(path: Path) -> list[str]:
    """Return how to execute one runner file for its description.

    An executable file is run the way the manager runs it, through its own
    shebang; anything else is handed to the interpreter its suffix names, so a
    runner that was copied without its executable bit is still describable.
    """

    if os.access(path, os.X_OK):
        return [str(path)]
    if path.suffix == ".py":
        return [sys.executable, str(path)]
    if path.suffix in {".sh", ".bash"}:
        return ["bash", str(path)]
    raise ValueError(
        f"the runner {path} is not executable and its suffix names no interpreter; "
        "make it executable, or give it a .py or .sh suffix"
    )


def _parse_description(text: str, path: Path) -> dict[str, object]:
    """Validate the description one runner printed."""

    try:
        described = json.loads(text)
    except ValueError as exc:
        raise ValueError(f"the runner {path} did not print a runner description: {exc}") from exc
    if not isinstance(described, Mapping) or not isinstance(described.get("workflow"), str):
        raise ValueError(f"the runner {path} did not print a runner description with a workflow name")
    steps = described.get("steps")
    if not isinstance(steps, list) or not all(isinstance(step, str) for step in steps):
        raise ValueError(f"the runner {path} described steps that are not an array of names")
    raw_inputs = described.get("inputs", {})
    try:
        inputs = validate_inputs(raw_inputs)
    except ValueError as exc:
        raise ValueError(f"the runner {path} described invalid inputs: {exc}") from exc
    instantiate = described.get("instantiate", False)
    if not isinstance(instantiate, bool):
        raise ValueError(f"the runner {path} described instantiate that is not a boolean")
    if any(destination is None for destination in inputs.values()) and not instantiate:
        raise ValueError(f"the runner {path} has hook-consumed inputs and requires an instantiate hook")
    result: dict[str, object] = {"workflow": described["workflow"], "steps": [str(step) for step in steps]}
    if "inputs" in described:
        result["inputs"] = inputs
    if "instantiate" in described:
        result["instantiate"] = instantiate
    return result


def _packaged_runner_path(provider: WorkflowProvider) -> Path:
    """Return the installed runner file one provider names, without importing it.

    The runner package is imported by name so resolving one workflow never drags
    another domain's helpers into the process, and the file is taken from beside
    that package's module.
    """

    if provider.runner_package is None or provider.runner_file is None:
        raise ValueError(f"workflow {provider.workflow_id!r} is not a packaged workflow")
    module = importlib.import_module(provider.runner_package)
    location = getattr(module, "__file__", None)
    if location is None:  # pragma: no cover - only a namespace package has none
        raise ValueError(f"runner package {provider.runner_package} has no installed location")
    return Path(location).with_name(provider.runner_file)


def _packaged_runner_reference(provider: WorkflowProvider) -> dict[str, object]:
    """Return the installed ``runner`` member a provider's ``pkg:`` form pins."""

    path = _packaged_runner_path(provider)
    if provider.runner_package is None or provider.runner_file is None:
        raise ValueError(f"workflow {provider.workflow_id!r} is not a packaged workflow")
    return {
        "executor": "path",
        "source": "installed",
        "path": f"pkg:{provider.runner_package}/{PurePosixPath(provider.runner_file)}",
        "sha256": sha256_file(path),
        "arguments": [],
    }


def registered_workflow(name: str) -> ResolvedWorkflow | None:
    """Return the registered workflow selected by *name*, or ``None``.

    :param name: Select a workflow by id or alias.
    :return: The resolved workflow, or ``None`` when no provider matches.
    """

    provider = workflow_provider(name)
    if provider is None:
        return None
    source = provider.directory if provider.directory is not None else _packaged_runner_path(provider)
    return ResolvedWorkflow(
        source=source,
        workflow_id=provider.workflow_id,
        runner_package=provider.runner_package,
        language=provider.language,
        document=provider.document,
        runner_options=provider.runner_options,
        document_path=(
            provider.directory.resolve() / provider.document
            if provider.directory is not None and provider.document
            else None
        ),
        alias=provider.alias,
        initial_step=provider.initial_step,
        steps=provider.steps,
        resources=provider.resources,
        step_resources=provider.step_resources,
        data_mode=provider.data_mode,
        workdir_mode=provider.workdir_mode,
        packaged=None if provider.directory is not None else provider.runner_file,
        registration_id=provider.workflow_id,
        summary=provider.summary,
        inputs=provider.inputs,
        instantiate=provider.instantiate,
        declarations=provider.declarations,
        collector=provider.collector,
        directory=provider.directory,
        build=provider.build,
        entry=provider.entry,
        instantiate_file=provider.instantiate_file,
        instantiate_exec=provider.instantiate_exec,
        collect_file=provider.collect_file,
        collector_exec=provider.collector_exec,
        postprocess_scripts=provider.postprocess_scripts,
        parameters=provider.parameters,
        environment=provider.environment,
        outputs=provider.outputs,
        declaration_uri=provider.declaration_uri,
        declaration_file=provider.declaration_file,
        _input_metadata=provider._input_metadata,
    )


def resolve_workflow(
    workflow: str | os.PathLike[str],
    *,
    workflow_id: str | None = None,
    step: str | None = None,
    data_mode: DataMode | None = None,
    format: str | None = None,
) -> ResolvedWorkflow:
    """Return the :class:`ResolvedWorkflow` *workflow* names.

    *workflow* is the name of a packaged workflow, the file name of a packaged
    runner, or the path of a runner file. A runner file is described by running
    it, so its workflow name and its steps come from the runner itself; *workflow*
    and *step* override what it said, and *step* is required when a runner
    registers several steps and none of them is ``start``.

    :param workflow: Select a registered workflow, package directory, or runner file.
    :param workflow_id: Override the resolved workflow id.
    :param step: Override the resolved initial step.
    :param data_mode: Override the resolved data mode.
    :param format: Force a language for a bare document or directory.
    :return: The resolved workflow description.
    :raises ValueError: If the workflow cannot be found or its description is invalid.
    """

    text = os.fspath(workflow)
    resolved = registered_workflow(text)
    if resolved is not None and format is not None:
        raise ValueError(
            "--format applies only to bare workflow documents or directories; registered workflows use their manifest language"
        )
    if resolved is None:
        path = Path(text).expanduser()
        if path.is_dir() and (path / "httk_workflow.toml").is_file():
            if format is not None:
                raise ValueError(
                    "--format cannot be used with a workflow package directory; its language comes from the manifest"
                )
            from .packages import load_workflow_package

            provider = load_workflow_package(path, register=False)
            resolved = ResolvedWorkflow(
                source=provider.directory or path.resolve(),
                workflow_id=provider.workflow_id,
                language=provider.language,
                document=provider.document,
                runner_options=provider.runner_options,
                document_path=(
                    (provider.directory or path.resolve()).resolve() / provider.document
                    if provider.document is not None
                    else None
                ),
                alias=provider.alias,
                initial_step=provider.initial_step,
                steps=provider.steps,
                data_mode=provider.data_mode,
                workdir_mode=provider.workdir_mode,
                summary=provider.summary,
                inputs=provider.inputs,
                resources=provider.resources,
                step_resources=provider.step_resources,
                instantiate=provider.instantiate,
                declarations=provider.declarations,
                collector=provider.collector,
                directory=provider.directory or path.resolve(),
                build=provider.build,
                entry=provider.entry,
                instantiate_file=provider.instantiate_file,
                instantiate_exec=provider.instantiate_exec,
                collect_file=provider.collect_file,
                collector_exec=provider.collector_exec,
                postprocess_scripts=provider.postprocess_scripts,
                parameters=provider.parameters,
                environment=provider.environment,
                outputs=provider.outputs,
                declaration_uri=provider.declaration_uri,
                declaration_file=provider.declaration_file,
                _input_metadata=provider._input_metadata,
            )
        elif path.exists():
            from . import languages

            resolved_path = path.resolve()
            if format is None:
                lang = languages.match_document(resolved_path)
            else:
                lang = languages.language(format)
                if lang.document_policy == "forbidden" and not resolved_path.is_dir():
                    raise ValueError(
                        f"workflow language {lang.name!r} forbids documents; --format {format!r} requires a directory target"
                    )
                if lang.document_policy != "forbidden" and not resolved_path.is_file():
                    raise ValueError(
                        f"workflow language {lang.name!r} expects a document file for --format {format!r}: {resolved_path}"
                    )
            if lang is not None:
                ports = lang.ports(resolved_path)
                directory = resolved_path if resolved_path.is_dir() else None
                document_path = resolved_path if resolved_path.is_file() else None
                name = resolved_path.name if resolved_path.is_dir() else resolved_path.stem
                summary = f"the {lang.name} document {resolved_path.name}"
                input_metadata = {port: {"role": port} for port in ports.inputs}
                outputs = {port: {"entry_type": "records", "role": port} for port in ports.outputs}
                from .packages import _workflow_declaration

                resolved = ResolvedWorkflow(
                    source=resolved_path,
                    workflow_id=f"{lang.name}.{_sanitize_tag(name) or 'document'}",
                    initial_step=lang.initial_step,
                    language=lang.name,
                    runner_options={},
                    document_path=document_path,
                    steps=lang.steps,
                    summary=summary,
                    inputs={port: None for port in ports.inputs},
                    instantiate=True,
                    declarations={"workflow": _workflow_declaration(summary, input_metadata, outputs)},
                    directory=directory,
                    outputs=outputs,
                    environment=lang.environment,
                    _input_metadata=input_metadata,
                )
            elif path.is_file():
                described = describe_runner(path)
                steps = tuple(cast(list[str], described["steps"]))
                resolved = ResolvedWorkflow(
                    source=path.resolve(),
                    workflow_id=str(described["workflow"]),
                    initial_step=_initial_step(path, steps, step),
                    steps=steps,
                    summary=f"the runner {path}",
                    inputs=cast(dict[str, str | None], described.get("inputs", {})),
                    instantiate=bool(described.get("instantiate", False)),
                )
            else:
                raise ValueError(f"unknown workflow path {text!r}: it is not a workflow package or language document")
        elif _has_path_separator(text) or Path(text).suffix in _RUNNER_SUFFIXES:
            # A path-shaped argument that does not exist is a mistyped file, not a
            # mistyped workflow name; the registry listing would only mislead.
            raise ValueError(f"no such file: {text}")
        else:
            labels = ", ".join(registered_workflow_labels()) or "none registered"
            match = difflib.get_close_matches(text, _registered_names(), n=1)
            hint = f"did you mean {match[0]!r}? " if match else ""
            raise ValueError(f"unknown workflow {text!r}; {hint}registered: {labels}")
    if workflow_id is not None:
        resolved = replace(resolved, workflow_id=workflow_id)
    if step is not None:
        if resolved.steps:
            ensure_step_known(step, resolved.steps, f"the runner {resolved.source}")
        resolved = replace(resolved, initial_step=step)
    if data_mode is not None:
        resolved = replace(resolved, data_mode=data_mode)
    return resolved


def _initial_step(path: Path, steps: tuple[str, ...], requested: str | None) -> str:
    """Return the step a job of this runner starts at."""

    if requested is not None:
        return ensure_step_known(requested, steps, f"the runner {path}")
    if "start" in steps:
        return "start"
    if len(steps) == 1:
        return steps[0]
    raise ValueError(
        f"the runner {path} registers {len(steps)} steps and none of them is 'start'; "
        f"name the one this job starts at with step=... ({', '.join(steps)})"
    )


def structure_files(directory: str | os.PathLike[str]) -> list[Path]:
    """Return every structure file of one directory, in a stable order.

    A structure is a file matching one of :data:`STRUCTURE_PATTERNS` — the VASP
    conventions ``POSCAR``, ``POSCAR.something``, and ``something.vasp`` — which is
    what makes a directory of structures one campaign.

    :param directory: Locate the directory to scan.
    :return: Matching regular structure files in stable order.
    :raises ValueError: If *directory* is not a directory.
    """

    root = Path(directory).expanduser()
    if not root.is_dir():
        raise ValueError(f"a structure directory must be a directory: {root}")
    found: dict[Path, None] = {}
    for pattern in STRUCTURE_PATTERNS:
        for path in sorted(root.glob(pattern)):
            if path.is_file() and not path.is_symlink():
                found[path] = None
    return list(found)


def structure_tag(path: str | os.PathLike[str]) -> str | None:
    """Return the job tag one structure file name suggests, or ``None``.

    The tag is the part of the name that identifies the structure — ``Si2O`` of
    ``POSCAR.Si2O``, ``fcc-al`` of ``fcc-al.vasp`` — reduced to the tag syntax the
    protocol allows. A name that says nothing beyond ``POSCAR`` suggests no tag.

    :param path: Name the structure file whose tag to derive.
    :return: The sanitized suggested tag, or ``None`` when no tag is present.
    """

    name = Path(path).name
    for prefix in ("POSCAR", "CONTCAR"):
        if name.startswith(prefix):
            name = name[len(prefix) :].lstrip("._-")
            break
    else:
        for suffix in (".vasp", ".poscar"):
            if name.lower().endswith(suffix):
                name = name[: -len(suffix)]
                break
        else:
            return None
    return _sanitize_tag(name)


def _sanitize_tag(name: str) -> str | None:
    """Reduce a filename fragment to the protocol's tag syntax."""

    reduced: list[str] = []
    for character in name.lower():
        if character in _TAG_CHARACTERS:
            reduced.append(character)
        elif reduced and reduced[-1] != "-":
            reduced.append("-")
    # A tag starts with a letter or a digit, is at most 48 characters long, and
    # cannot contain the double dash that separates it from the job UUID.
    tag = "".join(reduced).strip("-._")[:_MAXIMUM_TAG_LENGTH].strip("-._")
    while "--" in tag:
        tag = tag.replace("--", "-")
    return tag or None


def new_job(
    workspace: Workspace,
    workflow: str | os.PathLike[str],
    *,
    inputs: Mapping[str, object] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    parameters: Mapping[str, object] | None = None,
    environment: Mapping[str, object] | None = None,
    tag: str | None = None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    priority: int | None = None,
    workdir_mode: WorkdirMode = "persistent",
    data_mode: DataMode | None = None,
    publish: PublishMode = "workspace",
    step: str | None = None,
    format: str | None = None,
    workflow_id: str | None = None,
    name: str | None = None,
) -> ScaffoldedJob:
    """Scaffold, submit, and describe one job of *workflow*.

    *workflow* is a registered workflow name — see :func:`~httk.workflow.scaffold.registered_workflows` —
    or the path of a runner file. *files* maps payload names to the files to stage
    there: a bare name lands in the payload's :data:`~httk.workflow.scaffold.FILES_DIRECTORY`, which is
    where a packaged runner reads its inputs, and a name with a directory in
    it is used verbatim. *inputs* stages the workflow's declared objects into
    the payload; *parameters* is the job's opaque implementation mapping.

    *data_mode* defaults to what the workflow needs — ``transactional`` for a
    workflow whose runner publishes collected results, and ``none`` for a runner
    that said nothing. *publish* ``workspace`` publishes the runner
    file into the workspace runner store and pins its digest; ``installed``
    references a packaged runner through the reserved ``pkg:`` form instead and
    copies nothing. It is ignored for language workflows, whose realization
    chooses the runner itself.

    :param workspace: Provide the workspace receiving the job.
    :param workflow: Select the workflow or runner file.
    :param inputs: Supply declared workflow inputs.
    :param files: Map payload names to files to stage.
    :param parameters: Supply opaque job parameters.
    :param environment: Supply overrides for declared workflow environment values.
    :param tag: Set the job tag.
    :param placement: Place the job within the workspace.
    :param priority: Set the scheduling priority.
    :param workdir_mode: Select the job workdir mode.
    :param data_mode: Override the workflow data mode.
    :param publish: Select workspace publication or installed reference.
    :param step: Override the workflow's initial step.
    :param format: Force a language for a bare workflow document or directory.
    :param workflow_id: Override the workflow id in the job definition.
    :param name: Set the job's display name.
    :return: The submitted job description.
    :raises ValueError: If workflow, inputs, placement, or job settings are invalid.
    """

    prepared = _prepare(
        workspace, workflow, publish=publish, step=step, workflow_id=workflow_id, data_mode=data_mode, format=format
    )
    return _submit(
        workspace,
        prepared,
        inputs=inputs,
        files=files,
        parameters=parameters,
        environment=environment,
        tag=tag,
        placement=placement,
        priority=priority,
        workdir_mode=workdir_mode,
        name=name,
    )


def new_jobs(
    workspace: Workspace,
    workflow: str | os.PathLike[str],
    items: Iterable[JobItem],
    *,
    inputs: Mapping[str, object] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    parameters: Mapping[str, object] | None = None,
    environment: Mapping[str, object] | None = None,
    tag: str | None = None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    priority: int | None = None,
    workdir_mode: WorkdirMode = "persistent",
    data_mode: DataMode | None = None,
    publish: PublishMode = "workspace",
    step: str | None = None,
    format: str | None = None,
    workflow_id: str | None = None,
    name: str | None = None,
) -> Iterator[ScaffoldedJob]:
    """Scaffold and submit one job per member of *items*, lazily.

    Every keyword is the shared value of the whole campaign, and every member of
    one :class:`~httk.workflow.scaffold.JobItem` is what that job varies: ``inputs`` and ``files`` are
    merged over the shared mappings, and ``tag``, ``name``, ``placement``, and
    ``priority`` replace the shared value.

    This is the pattern for a campaign of any size. The workflow is resolved once
    and its runner published once, however many jobs follow, so every job costs
    exactly one payload directory and one state marker; *items* is consumed as an
    iterator and the results are yielded as they are submitted, so a structure
    generator can be turned into jobs without either side of the loop ever being
    materialized.

    :param workspace: Provide the workspace receiving the jobs.
    :param workflow: Select the workflow or runner file.
    :param items: Yield per-job overrides.
    :param inputs: Supply shared declared workflow inputs.
    :param files: Supply shared payload files.
    :param parameters: Supply shared opaque job parameters.
    :param environment: Supply shared declared environment overrides.
    :param tag: Set the shared job tag.
    :param placement: Set the shared workspace placement.
    :param priority: Set the shared scheduling priority.
    :param workdir_mode: Select the shared workdir mode.
    :param data_mode: Override the workflow data mode.
    :param publish: Select workspace publication or installed reference.
    :param step: Override the workflow's initial step.
    :param format: Force a language for a bare workflow document or directory.
    :param workflow_id: Override the workflow id in each job definition.
    :param name: Set the shared display name.
    :return: An iterator yielding each submitted job description.
    :yield: Each submitted job description.
    :raises ValueError: If workflow, inputs, placement, or job settings are invalid.

    .. code-block:: python

        def structures():
            for path in sorted(Path("structures").glob("POSCAR.*")):
                yield {"files": {"POSCAR": path}, "tag": structure_tag(path)}

        for job in new_jobs(workspace, "some-workflow", structures(), parameters={"kpoint_density": 30.0}):
            print(job.job_key)
    """

    prepared = _prepare(
        workspace, workflow, publish=publish, step=step, workflow_id=workflow_id, data_mode=data_mode, format=format
    )
    for item in items:
        yield _submit(
            workspace,
            prepared,
            inputs={**(inputs or {}), **item.get("inputs", {})},
            files={**(files or {}), **item.get("files", {})},
            parameters={**(parameters or {}), **item.get("parameters", {})},
            environment={**(environment or {}), **item.get("environment", {})},
            tag=item.get("tag", tag),
            placement=item.get("placement", placement),
            priority=item.get("priority", priority),
            workdir_mode=workdir_mode,
            name=item.get("name", name),
        )


def _prepare(
    workspace: Workspace,
    workflow: str | os.PathLike[str],
    *,
    publish: PublishMode,
    step: str | None,
    workflow_id: str | None,
    data_mode: DataMode | None,
    format: str | None,
) -> _Prepared:
    """Resolve one workflow and make its runner referenceable, exactly once.

    ``publish`` is ignored for language workflows because their realization
    supplies the runner reference and payload members.
    """

    resolved = resolve_workflow(workflow, workflow_id=workflow_id, step=step, data_mode=data_mode, format=format)
    if resolved.language is not None:
        from . import languages

        lang = languages.language(resolved.language)
        scaffolded = lang.prepare(_language_request(resolved))
        if scaffolded.runner is not None:
            runner = scaffolded.runner
            runner_source = cast(Literal["payload", "workspace", "installed"], str(runner["source"]))
            runner_path = str(runner["path"])
            runner_sha256 = None if runner_source == "payload" else str(runner["sha256"])
            runner_executor = str(runner.get("executor", scaffolded.runner_executor))
        else:
            if scaffolded.payload_runner is None:
                raise ValueError(f"language {resolved.language!r} did not provide a runner")
            runner_source = "payload"
            runner_path = scaffolded.payload_runner
            runner_sha256 = None
            runner_executor = scaffolded.runner_executor
        parameters = dict(scaffolded.parameters)
        parameters["workflow_realization"] = "language"
        reserved_parameters = (*scaffolded.reserved_parameters, "workflow_realization")
        if resolved.collect_file is not None:
            parameters["workflow_collect"] = "package"
            reserved_parameters = (*reserved_parameters, "workflow_collect")
        return _Prepared(
            workflow=resolved,
            runner_source=runner_source,
            runner_path=runner_path,
            runner_sha256=runner_sha256,
            data_mode=resolved.data_mode,
            runner_executor=runner_executor,
            payload_runner=scaffolded.payload_runner,
            workdir_path=scaffolded.workdir_path,
            required_capabilities=tuple(sorted(set(scaffolded.required_capabilities))),
            reserved_parameters=reserved_parameters,
            documents=scaffolded.documents,
            files=scaffolded.files,
            parameters=parameters,
            warnings=scaffolded.warnings,
            instantiate=scaffolded.instantiate,
            finalize=scaffolded.finalize,
        )
    if publish == "installed":
        if resolved.directory is not None:
            raise ValueError(
                "publish='installed' is not supported for a directory workflow; publish it into the workspace"
            )
        provider = workflow_provider(resolved.registration_id or resolved.workflow_id)
        if resolved.packaged is None or provider is None:
            raise ValueError(
                f"publish='installed' references a packaged workflow, but {resolved.source} is a runner "
                "file of your own; publish it into the workspace instead (the default), or install it on "
                "a runner search path and write its job.json yourself"
            )
        reference = _packaged_runner_reference(provider)
    else:
        try:
            reference = workspace.publish_runner(resolved.source, name=resolved.store_name)
        except FileExistsError as exc:
            if resolved.directory is None:
                raise
            target = workspace.runner_store_path(resolved.store_name)
            actual = tree_digest(target)
            from .packages import source_tree_digest

            expected = source_tree_digest(resolved.source)
            raise ValueError(
                f"published workflow tree {target} has digest {actual}, but the package resolves to {expected}"
            ) from exc
    runner_sha256 = str(reference["sha256"])
    instantiate: Callable[[InstantiateContext], object] | None
    instantiate_exec: tuple[Path, str, str] | None = None
    if resolved.directory is not None and resolved.instantiate_file is not None:
        from .packages import _tree_hook

        if resolved.instantiate_exec is not None:
            instantiate = None
            instantiate_exec = (
                workspace.runner_store_path(str(reference["path"])),
                runner_sha256,
                resolved.instantiate_exec,
            )
        else:
            instantiate = cast(
                Callable[[InstantiateContext], object],
                _tree_hook(
                    workspace.runner_store_path(str(reference["path"])),
                    runner_sha256,
                    resolved.instantiate_file,
                    "instantiate",
                ),
            )
    else:
        instantiate = _resolve_instantiate(resolved, runner_sha256) if resolved.instantiate else None
    return _Prepared(
        workflow=resolved,
        runner_source=cast(Literal["workspace", "installed"], str(reference["source"])),
        runner_path=str(reference["path"]),
        runner_sha256=runner_sha256,
        data_mode=resolved.data_mode,
        instantiate=instantiate,
        instantiate_exec=instantiate_exec,
    )


def _language_request(resolved: ResolvedWorkflow) -> LanguageRequest:
    """Build the language request, including package-only exclusions."""

    from . import languages

    excluded: tuple[str, ...] = ()
    if resolved.directory is not None:
        from .packages import MANIFEST_NAME

        members = [MANIFEST_NAME]
        members.extend(member for member in (resolved.collect_file, resolved.declaration_file) if member is not None)
        members.extend(str(script["file"]) for script in resolved.postprocess_scripts.values())
        if not (resolved.directory / MANIFEST_NAME).is_file():
            members = []
        excluded = tuple(dict.fromkeys(members))
    return languages.LanguageRequest(
        workflow_id=resolved.workflow_id,
        directory=resolved.directory,
        document=resolved.document_path,
        runner_options=resolved.runner_options,
        inputs=resolved._input_metadata,
        outputs=resolved.outputs,
        parameters=resolved.parameters,
        environment=resolved.environment,
        excluded_members=excluded,
    )


def _resolve_instantiate(workflow: ResolvedWorkflow, runner_sha256: str) -> Callable[[InstantiateContext], object]:
    """Import and resolve a workflow's instantiate hook once."""

    if workflow.source.suffix != ".py":
        raise ValueError(f"the instantiate hook is Python-SDK-only: {workflow.source}")
    source = workflow.source
    source_bytes = source.read_bytes()
    digest = hashlib.sha256(source_bytes).hexdigest()
    if digest != runner_sha256:
        raise ValueError(
            f"the runner {source} changed while resolving its instantiate hook: "
            f"import digest {digest} does not match pinned runner digest {runner_sha256}"
        )
    module_name = f"httk_workflow_runner_{digest}"
    module = sys.modules.get(module_name)
    if module is None:
        module = ModuleType(module_name)
        module.__file__ = str(source)
        sys.modules[module_name] = module
        try:
            exec(compile(source_bytes, str(source), "exec"), module.__dict__)  # noqa: S102
        except BaseException:
            sys.modules.pop(module_name, None)
            raise
    from .sdk import Runner

    runners = [value for value in vars(module).values() if isinstance(value, Runner)]
    if not runners:
        raise ValueError(f"the runner {source} defines no Runner")
    if len(runners) != 1:
        raise ValueError(f"the runner {source} defines {len(runners)} Runner instances")
    runner = runners[0]
    if not runner.has_instantiate or runner._instantiate is None:
        raise ValueError(f"the runner {source} declares instantiate but the found Runner has no hook")
    return runner._instantiate


def _input_required(metadata: Mapping[str, object]) -> bool:
    """Report whether one declared input must be supplied at submission.

    A boolean ``required`` in the metadata decides it outright; absent one, an
    input is required exactly when it declares an ``entry_type``.

    :param metadata: Supply the declared input metadata.
    :return: Whether the input is required.
    """

    required = metadata.get("required")
    if isinstance(required, bool):
        return required
    return "entry_type" in metadata


def _declared_inputs(workflow: ResolvedWorkflow) -> dict[str, dict[str, object]]:
    """Return the declared input metadata, each stamped with its required flag.

    :param workflow: Supply the resolved workflow whose inputs to describe.
    :return: The input metadata keyed by input name.
    """

    return {
        name: {**metadata, "required": _input_required(metadata)} for name, metadata in workflow._input_metadata.items()
    }


def _submit(
    workspace: Workspace,
    prepared: _Prepared,
    *,
    inputs: Mapping[str, object] | None,
    files: Mapping[str, str | os.PathLike[str]] | None,
    parameters: Mapping[str, object] | None,
    environment: Mapping[str, object] | None,
    tag: str | None,
    placement: str | PurePosixPath,
    priority: int | None,
    workdir_mode: WorkdirMode,
    name: str | None,
) -> ScaffoldedJob:
    """Build one payload below the workspace and publish it as a submitted job."""

    workflow = prepared.workflow
    normalized = normalize_placement(placement)
    # The payload is built inside the workspace's own scratch directory, so
    # submitting it is a rename on one filesystem rather than a copy, whatever
    # the size of the files it stages.
    staging = workspace.control / "tmp" / f"scaffold.{uuid.uuid4()}"
    caller_tag = tag
    try:
        staging.mkdir(parents=True, exist_ok=False)
        user_files = dict(files or {})
        prepared_members: dict[str, str] = {}
        for member in (*prepared.documents, *prepared.files):
            relative = payload_relative(member).as_posix()
            if relative in prepared_members:
                raise ValueError(f"prepared member {member!r} collides with {prepared_members[relative]!r}")
            prepared_members[relative] = member
        for member in user_files:
            relative = payload_relative(member).as_posix()
            if relative in prepared_members:
                raise ValueError(f"prepared member {prepared_members[relative]!r} collides with user file {member!r}")
        _stage_files(staging, user_files)
        for member, text in prepared.documents.items():
            destination = staging.joinpath(*payload_relative(member).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(text, bytes):
                destination.write_bytes(text)
            else:
                destination.write_text(text, encoding="utf-8")
        _stage_files(staging, prepared.files)
        supplied_inputs = dict(inputs or {})
        _stage_inputs(
            staging,
            workflow,
            supplied_inputs,
            instantiate=prepared.instantiate is not None or prepared.instantiate_exec is not None,
        )
        supplied_parameters = dict(parameters or {})
        supplied_environment = dict(environment or {})
        declared_environment = workflow.environment
        unknown_environment = sorted(set(supplied_environment) - set(declared_environment))
        if unknown_environment:
            declared_names = ", ".join(sorted(declared_environment)) or "none"
            raise ValueError(
                f"workflow environment name {unknown_environment[0]!r} is not declared; declared names: {declared_names}"
            )
        if supplied_environment:
            from .packages import _matches_input_type

            for environment_name, value in supplied_environment.items():
                environment_type = declared_environment[environment_name].get("type")
                if isinstance(environment_type, str) and not _matches_input_type(value, environment_type):
                    raise ValueError(
                        f"workflow environment {environment_name!r} does not match type {environment_type!r}"
                    )
        reserved_parameters = set(prepared.parameters) | set(prepared.reserved_parameters)
        collisions = sorted(set(supplied_parameters) & reserved_parameters)
        if collisions:
            raise ValueError(f"user parameter collides with reserved language parameter {collisions[0]!r}")
        job_parameters = {**supplied_parameters, **prepared.parameters}
        declared_parameters = workflow.parameters
        if declared_parameters:
            from .packages import _matches_input_type

            # A workflow that declares a name the language realization reserves
            # would smuggle its default straight into the reserved wiring, so the
            # collision is refused here — before any default is applied — exactly
            # as a supplied value colliding with a reserved name is refused above.
            reserved_declared = sorted(set(declared_parameters) & reserved_parameters)
            if reserved_declared:
                raise ValueError(
                    f"workflow declares parameter {reserved_declared[0]!r}, which collides with a reserved "
                    f"{workflow.language or 'runner'} parameter; rename the declared parameter"
                )
            # A declared type is enforced, exactly like the environment channel;
            # an undeclared name only warns, because parameters are deliberately
            # open, and a declared default fills in for a name nobody supplied.
            for parameter_name, value in supplied_parameters.items():
                if parameter_name not in declared_parameters:
                    continue
                parameter_type = declared_parameters[parameter_name].get("type")
                if isinstance(parameter_type, str) and not _matches_input_type(value, parameter_type):
                    raise ValueError(
                        f"workflow parameter {parameter_name!r} does not match type {parameter_type!r}; "
                        f"got {type(value).__name__}. Supply a matching value — note that a command-line "
                        f"NAME=VALUE parses VALUE as JSON when it can, so quote a literal string as "
                        f'NAME=\'"text"\''
                    )
            declared_names = ", ".join(sorted(declared_parameters))
            logger = context_logger(_LOGGER, workflow.workflow_id)
            for parameter_name in sorted(set(supplied_parameters) - set(declared_parameters)):
                logger.warning(
                    "job parameter %r is not declared by this workflow; declared: %s",
                    parameter_name,
                    declared_names,
                )
            for parameter_name, metadata in declared_parameters.items():
                if "default" in metadata and parameter_name not in job_parameters:
                    job_parameters[parameter_name] = metadata["default"]
        if prepared.instantiate_exec is not None:
            hook_inputs = _serialize_executable_inputs(staging, workflow, supplied_inputs)
            hook_parameters, hook_tag = _run_executable_instantiate(
                prepared.instantiate_exec,
                staging,
                workflow.workflow_id,
                tag,
                job_parameters,
                hook_inputs,
                workspace.root,
            )
            job_parameters = {**job_parameters, **hook_parameters}
            if caller_tag is None and hook_tag is not None:
                tag = hook_tag
        elif prepared.instantiate is not None:
            context = InstantiateContext(
                payload=staging,
                inputs=MappingProxyType(supplied_inputs),
                parameters=job_parameters,
                tag=tag,
            )
            prepared.instantiate(context)
            job_parameters = context.parameters
            tag = caller_tag if caller_tag is not None else context.tag
        declared_input_metadata = _declared_inputs(workflow)
        # A language workflow satisfies its own inputs — a document value, a
        # maker default, a supplied override — so the generic required check is
        # scoped to packaged and directory-hook workflows, whose destinations
        # this scaffold stages itself.
        if workflow.language is None:
            for input_name, metadata in declared_input_metadata.items():
                if not metadata["required"]:
                    continue
                input_destination = workflow.inputs.get(input_name)
                if input_destination is None:
                    satisfied = input_name in supplied_inputs
                else:
                    satisfied = staging.joinpath(*payload_relative(str(input_destination)).parts).exists()
                if not satisfied:
                    declared_names = ", ".join(workflow.inputs) or "none"
                    raise ValueError(
                        f"workflow input {input_name!r} is required and was not supplied; "
                        f"declared inputs: {declared_names}"
                    )
        declared_member: dict[str, object] = {}
        if declared_parameters:
            declared_member["parameters"] = {name: dict(metadata) for name, metadata in declared_parameters.items()}
        if declared_input_metadata:
            declared_member["inputs"] = declared_input_metadata
        spec = JobSpec(
            name=name or f"{workflow.workflow_id}: {tag or 'job'}",
            workflow=workflow.workflow_id,
            runner_executor=prepared.runner_executor,
            runner_path=prepared.runner_path,
            runner_source=prepared.runner_source,
            runner_sha256=prepared.runner_sha256,
            initial_step=workflow.initial_step,
            tag=tag,
            workdir_mode=workdir_mode,
            workdir_path=prepared.workdir_path if prepared.workdir_path is not None else "run",
            data_mode=prepared.data_mode,
            priority=500 if priority is None else priority,
            required_capabilities=tuple(sorted(set(prepared.required_capabilities))),
            resources=workflow.resources,
            step_resources=workflow.step_resources,
            parameters=validate_parameters(job_parameters),
            environment=(
                {
                    "declared": {name: dict(metadata) for name, metadata in declared_environment.items()},
                    "overrides": supplied_environment,
                }
                if declared_environment
                else {}
            ),
            declarations=workflow.declarations,
            declared=declared_member,
        )
        if prepared.finalize is not None:
            spec = prepared.finalize(spec)
            if not isinstance(spec, JobSpec):
                raise ValueError("language finalize hook must return a JobSpec")
        job = prepare_job_payload(staging, spec)
        marker = workspace.submit(staging, normalized, move=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return ScaffoldedJob(
        job_id=job.id,
        job_key=job.job_key,
        tag=job.tag,
        placement=marker.placement,
        payload=workspace.payload_path(marker.placement, marker.job_key),
        marker=marker.path,
        workflow=job.workflow,
        initial_step=job.initial_step,
        runner={
            "source": prepared.runner_source,
            "path": prepared.runner_path,
            "sha256": prepared.runner_sha256,
        },
        warnings=prepared.warnings,
    )


def _serialize_executable_inputs(
    payload: Path, workflow: ResolvedWorkflow, inputs: Mapping[str, object]
) -> dict[str, dict[str, object]]:
    """Copy or serialize executable-hook inputs into descriptor files."""

    descriptors: dict[str, dict[str, object]] = {}
    for name, value in inputs.items():
        if workflow.inputs[name] is not None:
            continue
        source: Path | None = None
        if isinstance(value, (str, os.PathLike)):
            candidate = Path(os.fspath(value)).expanduser()
            if candidate.is_file():
                source = candidate
        if source is not None:
            relative = payload_relative(f"files/inputs/{name}/{source.name}")
            destination = payload.joinpath(*relative.parts)
            if destination.exists():
                raise ValueError(f"generated member {relative.as_posix()!r} collides with an existing payload member")
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            descriptors[name] = {"kind": "file", "path": relative.as_posix()}
            continue
        message = _missing_file_input_message(name, value)
        if message is not None:
            raise ValueError(message)
        if _is_json_native(value):
            descriptors[name] = {"kind": "value", "value": value}
        else:
            descriptors[name] = _save_executable_input(payload, name, value)
    return descriptors


def _has_path_separator(text: str) -> bool:
    """Report whether one input value carries a filesystem path separator."""

    return "/" in text or os.sep in text or (os.altsep is not None and os.altsep in text)


def _path_input_message(name: str, text: str, candidate: Path) -> str | None:
    """Return a teaching message when *candidate* names no usable file, or ``None``.

    *candidate* is where the caller resolved *text* — the caller's own root, not
    necessarily the current directory. A directory says so; a value that carries
    a separator or that resolves to something that exists but is not a file is a
    mistyped path. A bare identifier that resolves to nothing is a literal and
    passes through — an entry-typed id like ``mp-149`` is not a path.

    :param name: Name the workflow input the value was supplied for.
    :param text: Supply the raw input text.
    :param candidate: Supply the path the value resolved to.
    :return: The teaching message, or ``None`` when the value passes through.
    """

    if candidate.is_file():
        return None
    if candidate.is_dir():
        return f"workflow input {name!r} is a directory, not a file: {candidate}; supply a regular file"
    if not _has_path_separator(text) and not candidate.exists():
        return None
    return (
        f"workflow input {name!r} looks like a file path but nothing exists at {candidate}; "
        "supply an existing file, or a literal value without path separators"
    )


def _missing_file_input_message(name: str, value: object) -> str | None:
    """Return a teaching message for a mistyped current-directory path input.

    This is the current-directory-consistent probe used by the executable-hook
    and PWD input paths, which resolve their string values against the current
    directory. A runner that resolves against a different root decides from its
    own resolved path with :func:`_path_input_message` instead.

    :param name: Name the workflow input the value was supplied for.
    :param value: Supply the raw input value.
    :return: The teaching message, or ``None`` when the value passes through.
    """

    if not isinstance(value, (str, os.PathLike)):
        return None
    text = os.fspath(value)
    if not text:
        return None
    return _path_input_message(name, text, Path(text).expanduser())


def _is_json_native(value: object, active: set[int] | None = None) -> bool:
    """Return whether *value* is JSON-native without applying JSON coercions."""

    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if not isinstance(value, (list, dict)):
        return False
    active = set() if active is None else active
    identity = id(value)
    if identity in active:
        return False
    active.add(identity)
    try:
        if isinstance(value, list):
            return all(_is_json_native(item, active) for item in value)
        return all(isinstance(key, str) and _is_json_native(item, active) for key, item in value.items())
    finally:
        active.remove(identity)


def _save_executable_input(payload: Path, name: str, value: object) -> dict[str, object]:
    """Save one live executable-hook input using a registered extension."""

    import httk.core
    from httk.core.register import known_writers

    dispatch_keys = sorted(known_writers())
    last_error: Exception | None = None
    for dispatch_key in dispatch_keys:
        filename = f"{name}{dispatch_key}" if dispatch_key.startswith(".") else dispatch_key
        relative = payload_relative(f"files/inputs/{name}/{filename}")
        destination = payload.joinpath(*relative.parts)
        if destination.exists():
            raise ValueError(f"generated member {relative.as_posix()!r} collides with an existing payload member")
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            httk.core.save(value, destination)
        except Exception as exc:
            last_error = exc
            destination.unlink(missing_ok=True)
            continue
        # ponytail: core has no object-to-writer query; sorted-first-success is
        # deterministic until core exposes explicit writer selection.
        return {"kind": "file", "path": relative.as_posix()}
    detail = f": {last_error}" if last_error is not None else ""
    raise ValueError(
        f"workflow input {name!r} value of type {type(value).__name__} cannot be serialized for an executable hook"
        f"{detail}; use a .py hook or register a httk.core writer"
    ) from last_error


def _run_executable_instantiate(
    hook: tuple[Path, str, str],
    payload: Path,
    workflow_id: str,
    tag: str | None,
    parameters: Mapping[str, object],
    inputs: Mapping[str, Mapping[str, object]],
    workspace_root: Path,
) -> tuple[dict[str, object], str | None]:
    """Run an instantiate executable from its digest-pinned runner tree."""

    tree, pinned_sha256, member = hook
    actual = tree_digest(tree)
    if actual != pinned_sha256:
        raise ValueError(
            f"published workflow tree {tree} changed: digest {actual} does not match pinned {pinned_sha256}"
        )
    source = tree.joinpath(*PurePosixPath(member).parts)
    if source.is_symlink() or not source.is_file() or not source.resolve().is_relative_to(tree.resolve()):
        raise ValueError(f"workflow package hook member is unavailable in published tree {tree}: {member}")
    if not os.access(source, os.X_OK):
        raise ValueError(f"instantiate hook {member!r} is not executable; chmod +x")
    request = {
        "format": "httk-workflow-instantiate",
        "format_version": 2,
        "workflow": workflow_id,
        "tag": tag,
        "parameters": dict(parameters),
        "inputs": {name: dict(descriptor) for name, descriptor in inputs.items()},
    }
    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment["HTTK_WORKFLOW_WORKSPACE_DIR"] = str(workspace_root)
    try:
        completed = subprocess.run(
            [str(source)],
            input=json.dumps(request, separators=(",", ":"), allow_nan=False),
            capture_output=True,
            text=True,
            cwd=payload,
            env=environment,
            timeout=_INSTANTIATE_TIMEOUT,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot run executable instantiate hook {member!r}: {exc}") from exc
    stderr = completed.stderr.strip()
    excerpt = stderr[-1000:] if stderr else ""
    if completed.returncode != 0:
        detail = f" (stderr: {excerpt})" if excerpt else ""
        raise ValueError(f"executable instantiate hook {member!r} failed with exit {completed.returncode}{detail}")
    try:
        response = json.loads(completed.stdout)
        if not isinstance(response, Mapping):
            raise ValueError("response is not a JSON object")
        response_parameters = response.get("parameters")
        response_tag = response.get("tag")
        if not isinstance(response_parameters, Mapping):
            raise ValueError("response has no parameters mapping")
        if response_tag is not None and not isinstance(response_tag, str):
            raise ValueError("response tag is not a string")
        return dict(response_parameters), response_tag
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        detail = f" (stderr: {excerpt})" if excerpt else ""
        raise ValueError(f"executable instantiate hook {member!r} returned invalid JSON: {exc}{detail}") from exc


def _stage_files(payload: Path, files: Mapping[str, str | os.PathLike[str]]) -> None:
    """Copy every named file into the payload it belongs to."""

    for name, source in files.items():
        path = Path(source).expanduser()
        if path.is_dir():
            raise ValueError(f"a staged payload file must be a regular file, not a directory: {path}")
        if not path.is_file():
            raise ValueError(f"the file staged as {name!r} does not exist: {path}")
        destination = payload.joinpath(*payload_relative(name).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)


def _stage_inputs(
    payload: Path,
    workflow: ResolvedWorkflow,
    inputs: Mapping[str, object],
    *,
    instantiate: bool = False,
) -> None:
    """Realize declared inputs into the payload declared by the workflow."""

    declared = workflow.inputs
    for name, value in inputs.items():
        if name not in declared:
            names = ", ".join(declared) or "none"
            hint = ""
            if not declared and workflow.language is not None and workflow.directory is None:
                from . import languages

                if languages.language(workflow.language).open_ports:
                    hint = (
                        f"; this bare {workflow.language} document declares no ports — "
                        "declare inputs in an httk_workflow.toml package to pass them"
                    )
            raise ValueError(f"unknown workflow input {name!r}; declared inputs: {names}{hint}")
        destination_name = declared[name]
        if destination_name is None:
            if not instantiate:
                raise ValueError(f"workflow input {name!r} requires an instantiate hook")
            continue
        destination = payload_relative(destination_name)
        if isinstance(value, (str, os.PathLike)):
            _stage_files(payload, {destination.as_posix(): value})
            continue
        import httk.core

        if not httk.core.has_writer_for(destination.name):
            raise ValueError(
                f"no writer is registered for {destination.name!r}; a writer must be registered "
                "(installing httk-atomistic provides the POSCAR/CIF writers)"
            )
        path = payload.joinpath(*destination.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        httk.core.save(value, path)


def payload_relative(name: str) -> PurePosixPath:
    """Return where one staged file lands inside a payload.

    A bare name lands in :data:`~httk.workflow.scaffold.FILES_DIRECTORY`, so ``POSCAR`` becomes
    ``files/POSCAR`` — where the packaged runners read it — and a name that
    carries a directory of its own is used exactly as it is written.

    :param name: Name the staged file within the payload.
    :return: The validated payload-relative destination.
    :raises httk.workflow.errors.FormatError: If the name uses a reserved payload member.
    :raises ValueError: If the name is empty or absolute.
    """

    text = str(name).strip()
    if not text:
        raise ValueError("a staged payload file needs a name")
    relative = PurePosixPath(text)
    if relative.is_absolute():
        raise ValueError(f"a staged payload file name must be relative: {name!r}")
    if len(relative.parts) == 1:
        relative = PurePosixPath(FILES_DIRECTORY) / relative
    for index, part in enumerate(relative.parts):
        if part in {"", ".", ".."} or (
            index == 0 and part in {"job.json", ATTEMPTS_DIRECTORY, LOGS_DIRECTORY, JOB_STATE_DIRECTORY}
        ):
            raise FormatError(f"a staged payload file name may not contain {part!r}: {name!r}")
    return relative
