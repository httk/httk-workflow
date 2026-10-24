"""Scaffolding submitted jobs from an installed workflow, some files, and some inputs.

A job is a payload directory plus a ``job.json`` that names the installed
workflow to run, and building one by hand means knowing that workflow's id, its
initial step, and where its inputs live in the payload. This module is the
short way: :func:`new_job` takes a *workflow* — the id or name of a workflow
installed in the workspace, or the path of a runner file of your own — stages
the files the runner reads, writes the ``job.json``, and submits the result, all
in one call.

A job runs only a workflow installed in its workspace (``httk workflow
install``): the manager executes the installed package, so upgrading the
*httk-workflow* underneath a queued campaign never changes what its jobs run.
A workflow that is not installed is refused, unless ``install=True`` installs it
first. A runner file is always installed, as an ``adhoc:`` workflow.

Packaged workflows are not known to this module. A domain or compat engine
registers each one it ships with :func:`~httk.workflow.scaffold.register_workflow`, supplying only the
generic description of a starting point — its name, the runner it starts from,
the workflow and steps that runner declares, and what it does — so the scaffold
resolves a workflow without ever importing the science that owns it.

.. code-block:: python

    from httk.workflow import Workspace
    from httk.workflow.scaffold import new_job

    workspace = Workspace.initialize("workflow-workspace")
    job = new_job(workspace, "some-workflow", files={"input": "input"}, tag="example", install=True)
    print(job.job_key, job.payload)

:func:`new_jobs` is the same operation for a campaign: one workflow resolution
amortized over every job, and a lazy iterator over the results. By design,
generating a partitioned campaign costs one payload per job and never
materializes a list of them.
"""

from __future__ import annotations

import copy
import difflib
import hashlib
import importlib
import json
import logging
import math
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import uuid
from collections.abc import Callable, Generator, Iterable, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING, TypedDict, cast

from httk.core.building import BuildSpec
from httk.core.digests import sha256_file, tree_digest
from httk.core.report import context_logger
from httk.core.requirements import check_requirements, parse_requirements

if TYPE_CHECKING:
    from httk.core.project.sealing import Seal, SealKey

    from .collecting import JobRecord
    from .compat import LanguageRequest
    from .seals import Release

from . import _fs, _kernel
from ._durations import cap_maxtime
from ._job import JobDefinition
from ._util import interpreter_first_path, validate_inputs
from .codes import code_environment
from .errors import FormatError, WorkflowError
from .models import (
    LOGS_DIRECTORY,
    _reserved_payload_member,
    ensure_step_known,
    expand_runner_command,
    make_job_key,
    normalize_placement,
    normalize_resources,
    placement_text,
    validate_parameters,
    validate_resources,
    validate_step,
)
from .runtime_builders import JobSpec, prepare_job_payload
from .workspace import Workspace

if TYPE_CHECKING:
    from ._store import Installed

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_PLACEMENT",
    "FILES_DIRECTORY",
    "JOB_SCAFFOLD_FORMAT",
    "STRUCTURE_PATTERNS",
    "BuildSpec",
    "CallSlot",
    "InstantiateContext",
    "JobItem",
    "RecognizeSpec",
    "ResolvedWorkflow",
    "ScaffoldedJob",
    "WorkflowProvider",
    "describe_package_runner",
    "describe_runner",
    "new_job",
    "new_jobs",
    "payload_relative",
    "register_workflow",
    "registered_workflow",
    "registered_workflow_labels",
    "registered_workflows",
    "resolve_workflow",
    "scaffold_job",
    "structure_files",
    "structure_tag",
    "submit_payload",
    "workflow_provider",
]

#: The format of the machine-readable report :meth:`ScaffoldedJob.as_mapping` returns.
JOB_SCAFFOLD_FORMAT = "httk-workflow-job-scaffold"
#: Where a scaffolded job is placed when nothing else is asked for: directly in the jobs root.
DEFAULT_PLACEMENT = ""
#: The payload directory a staged file lands in when its name has no directory of
#: its own. Every packaged runner reads its inputs from there.
FILES_DIRECTORY = "files"
#: The file names legacy callers may recognize as structures in a directory.
STRUCTURE_PATTERNS = ("POSCAR*", "*.vasp")
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
#: The scratch purpose of a payload on its way to ``jobs/ready/``; its reconciler publishes a staged ``job/``.
_SUBMIT = "submit"
_DESCRIBE_TIMEOUT = 120.0
_INSTANTIATE_TIMEOUT = 3600.0
_TAG_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789._-"
_MAXIMUM_TAG_LENGTH = 48


@dataclass(frozen=True)
class RecognizeSpec:
    """The ``[workflow.recognize]`` declaration of a recognized-calculation collector.

    :param file: Name the package's recognize hook, a ``.py`` member.
    :param priority: Order this collector among those whose markers match; higher is tried first.
    :param requires: Give the cheap markers a directory must contain before the hook is called:
        exact basenames or ``*.ext`` globs.
    """

    file: str
    priority: int
    requires: tuple[str, ...]


@dataclass(frozen=True)
class CallSlot:
    """One ``[workflow.calls.<ALIAS>]`` table: a call whose target the job creator chooses.

    The chosen workflow must fit the slot's interface declaration, a workflow-declaration-shaped
    document whose inputs and outputs name an entry type and a definition ``ref``.

    :param interface: The validated interface declaration document.
    :param declaration_file: Name the package member holding the interface declaration.
    :param description: Describe what the called workflow must do.
    :param default: Give the workflow reference bound when the job creator chooses none.
    :param default_parameters: Give the parameters the default is called with, which a job's creator may
        override per entry.
    """

    interface: Mapping[str, object]
    declaration_file: str
    description: str | None = None
    default: str | None = None
    default_parameters: Mapping[str, object] | None = None


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
    :param resources: Declare the default resource requirement; ``maxtime`` and ``mintime`` are Slurm
        duration strings such as ``"24:00:00"``.
    :param step_resources: Declare per-step resource requirements, time labels spelled as in *resources*.
    :param seal_succeeded: Seal a succeeded job of it (``[workflow.runner] seal_succeeded``); ``None``
        leaves it to the workspace setting.
    :param summary: Describe the workflow for callers.
    :param inputs: Map input names to payload destinations or hook handling.
    :param instantiate: Indicate that the workflow has an instantiate hook.
    :param declarations: Declare workflow declaration documents.
    :param collector: Identify the optional result collector.
    :param directory: Locate a directory-sourced workflow package.
    :param build: Describe the package build command and generated artifacts.
    :param entry: Name the directory package's runner entry.
    :param command: Give the directory package's unexpanded runner command, replacing *entry*.
    :param instantiate_file: Name the directory package's instantiate hook.
    :param instantiate_exec: Name an executable directory package instantiate hook.
    :param collect_file: Name the directory package's collector.
    :param collector_exec: Name an executable directory package collector.
    :param postprocess_scripts: Map curated postprocess script names to package members and descriptions.
    :param parameters: Declare the workflow's parameter metadata.
    :param environment: Declare the workflow's environment metadata.
    :param settings: Name the workspace settings the workflow declares reading; the manifest parser gives the
        union of ``[workflow].settings`` and the setting each environment entry resolves.
    :param outputs: Declare the workflow's output metadata.
    :param declaration_uri: Identify the source declaration URI.
    :param declaration_file: Name the source declaration file.
    :param name: Give the short name of a workflow whose id is a git URI.
    :param requires: Give the ``NAME>=VERSION`` distribution requirements a job of it must meet.
    :param calls: Declare the fixed workflows a job of it may call, alias to workflow reference.
    :param slots: Declare the call slots whose targets a job's creator binds, alias to slot.
    :param recognize: Declare a recognized-calculation collector, a package that is collected but never run.
    :param references: Declare the literature references stored results cite, label to OPTIMADE reference table.
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
    resources: Mapping[str, int | str] = field(default_factory=dict)
    step_resources: Mapping[str, Mapping[str, int | str]] = field(default_factory=dict)
    seal_succeeded: bool | None = None
    summary: str = ""
    inputs: Mapping[str, str | None] = field(default_factory=dict)
    instantiate: bool = False
    declarations: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    collector: Callable[[JobRecord], Mapping[str, object]] | str | None = None
    directory: Path | None = None
    build: BuildSpec | None = None
    entry: str = "run"
    command: tuple[str, ...] | None = None
    instantiate_file: str | None = None
    instantiate_exec: str | None = None
    collect_file: str | None = None
    collector_exec: str | None = None
    postprocess_scripts: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    parameters: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    settings: tuple[str, ...] = ()
    outputs: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    declaration_uri: str | None = None
    declaration_file: str | None = None
    name: str | None = None
    requires: tuple[str, ...] = ()
    calls: Mapping[str, str] | None = None
    slots: Mapping[str, CallSlot] | None = None
    recognize: RecognizeSpec | None = None
    references: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    _input_metadata: Mapping[str, Mapping[str, object]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.language is not None:
            if self.runner_package is not None or self.runner_file is not None:
                raise ValueError("a language workflow cannot supply a runner package or file")
            if self.directory is None:
                raise ValueError("a language workflow provider must be directory-sourced")
            if self.entry != "run" or self.command is not None or self.instantiate_file is not None:
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
                or self.command is not None
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
        if self.recognize is not None and (self.language is not None or self.directory is None):
            raise ValueError("a recognize workflow must be a directory package without a language")
        if (
            self.recognize is None
            and any(destination is None for destination in inputs.values())
            and not self.instantiate
        ):
            raise ValueError(f"workflow {self.workflow_id!r} has hook-consumed inputs and requires an instantiate hook")
        if self.alias is not None and (
            not isinstance(self.alias, str) or re.fullmatch(r"[a-z0-9._-]+", self.alias) is None
        ):
            raise ValueError(f"workflow alias must match [a-z0-9._-]+: {self.alias!r}")
        object.__setattr__(self, "inputs", MappingProxyType(inputs))
        object.__setattr__(self, "runner_options", MappingProxyType(dict(self.runner_options)))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        # The authored form is kept, so dataclasses.replace() never converts
        # seconds twice; _requirements() is the one conversion to seconds.
        self._requirements()
        object.__setattr__(self, "resources", MappingProxyType(dict(self.resources)))
        object.__setattr__(
            self,
            "step_resources",
            MappingProxyType({step: dict(requirement) for step, requirement in self.step_resources.items()}),
        )
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(self, "settings", tuple(self.settings))
        object.__setattr__(self, "outputs", MappingProxyType(dict(self.outputs)))
        object.__setattr__(self, "postprocess_scripts", MappingProxyType(dict(self.postprocess_scripts)))
        object.__setattr__(self, "references", MappingProxyType(dict(self.references)))
        object.__setattr__(self, "_input_metadata", MappingProxyType(dict(self._input_metadata)))

    def _requirements(self) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
        """Return the resource requirements in protocol form, time labels in seconds."""

        return normalize_resources(self.resources, "workflow.resources"), {
            validate_step(step, "workflow step"): normalize_resources(requirement, f"workflow.steps.{step}")
            for step, requirement in self.step_resources.items()
        }

    @property
    def definition_uri(self) -> str | None:
        """The git URI identifying the workflow definition, when the id is one."""

        return self.workflow_id if self.workflow_id.startswith("git+") else None

    @property
    def runnable(self) -> bool:
        """Whether jobs can be created from the package; a recognize collector is never run."""

        return self.recognize is None


#: Every registered workflow, keyed by name in registration order. The scaffold
#: ships this empty: a domain populates it as an import side effect, so the
#: generic layer resolves a domain workflow without importing the domain.
_WORKFLOW_PROVIDERS: dict[str, WorkflowProvider] = {}


def register_workflow(provider: WorkflowProvider) -> None:
    """Register one packaged workflow, replacing any registered under its name.

    A domain calls this once per workflow it ships when its package is imported,
    which is how ``httk job new --workflow NAME`` resolves a packaged
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
    """Return registered ids, then sorted plugin ids, pip-installed and fetched short names.

    :return: The registered workflow ids.
    """

    result = list(_WORKFLOW_PROVIDERS)
    registered_names = set(result)
    registered_names.update(provider.alias for provider in _WORKFLOW_PROVIDERS.values() if provider.alias)
    from .packages import installed_plugin_workflows

    result.extend(
        workflow_id for workflow_id in sorted(installed_plugin_workflows()) if workflow_id not in registered_names
    )
    result.extend(sorted(name for name, provider in _visible_pip().items() if name == provider.name))
    result.extend(sorted(name for name, provider in _visible_fetched().items() if name == provider.name))
    return tuple(result)


def _base_claimed_names() -> set[str]:
    """Return every in-process and installed-plugin id and alias, conflicted or not."""

    from .packages import _plugin_workflow_conflicts, installed_plugin_workflows

    names = set(_plugin_workflow_conflicts())
    for provider in (*_WORKFLOW_PROVIDERS.values(), *installed_plugin_workflows().values()):
        names.update(name for name in (provider.workflow_id, provider.alias) if name)
    return names


def _claimed_names() -> set[str]:
    """Return the names that shadow fetched short names: base names plus pip names, conflicted or not."""

    from ._pip_repositories import _pip_names

    winners, conflicts = _pip_names()
    return _base_claimed_names() | set(winners) | set(conflicts)


def _visible_pip() -> dict[str, WorkflowProvider]:
    """Return pip-installed workflows by short name or alias, minus names in-process or plugin workflows claim."""

    from ._pip_repositories import _pip_names

    claimed = _base_claimed_names()
    return {name: provider for name, provider in _pip_names()[0].items() if name not in claimed}


def _visible_fetched() -> dict[str, WorkflowProvider]:
    """Return fetched workflows by short name or alias, minus shadowed and conflicted names."""

    from .git_workflows import _fetched_names

    claimed = _claimed_names()
    return {name: provider for name, provider in _fetched_names()[0].items() if name not in claimed}


#: File suffixes that make an argument path-shaped: a runner file or a language
#: document, versus a dotted registered id like ``domain.family.step``.
_RUNNER_SUFFIXES = frozenset({".py", ".sh", ".bash", ".cwl", ".json", ".yaml", ".yml"})


def registered_workflow_labels() -> tuple[str, ...]:
    """Return display labels for registered, installed-plugin, pip-installed and fetched workflows.

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
    from ._pip_repositories import _pip_owner

    pip = _visible_pip()
    for name, provider in sorted(pip.items()):
        if name == provider.name:
            label = f"{name} ({provider.alias})" if pip.get(provider.alias or "") is provider else name
            labels.append(f"{label} [pip {_pip_owner(provider.workflow_id)}]")
    fetched = _visible_fetched()
    for name, provider in sorted(fetched.items()):
        if name == provider.name:
            label = f"{name} ({provider.alias})" if fetched.get(provider.alias or "") is provider else name
            labels.append(f"{label} [{provider.workflow_id}]")
    return tuple(labels)


def _registered_names() -> list[str]:
    """Return every registered, installed-plugin, pip-installed or fetched id and alias."""

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
    names.extend(sorted(_visible_pip()))
    names.extend(sorted(_visible_fetched()))
    return names


def workflow_provider(name: str) -> WorkflowProvider | None:
    """Return the provider selected by canonical id, alias, or pip-installed or fetched short name.

    A git URI selects only an already installed workflow pinned to a full
    commit hash: this lookup never fetches or raises for a URI, so a job payload
    naming one can never cause code acquisition.

    :param name: Select a workflow by id, alias, short name, or pinned git URI.
    :return: The selected provider, or ``None`` when no provider matches.
    :raises ValueError: If the name is claimed by several plugins, pip-installed workflows or fetched lineages.
    """

    if name.startswith("git+"):
        from .git_workflows import _installed_provider

        return _installed_provider(name)
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
    from ._pip_repositories import _pip_names

    winners, conflicts = _pip_names()
    if name in conflicts:
        raise ValueError(
            f"workflow name {name!r} is claimed by several pip-installed workflows ({', '.join(conflicts[name])}); "
            "reference one by its URI"
        )
    if name in winners:
        return winners[name]
    from .git_workflows import _fetched_provider

    return _fetched_provider(name)


class JobItem(TypedDict, total=False):
    """What one job of a :func:`new_jobs` campaign varies from the shared values.

    Every member is optional, and a member that is absent takes the value
    :func:`new_jobs` was called with. ``inputs`` and ``files`` are merged over the
    shared mappings key by key; everything else, including ``provenance``, replaces
    the shared value — see :func:`new_job` for what a supplied ``provenance``
    document does. ``move_inputs`` exists only per job (a file moves once):
    see :func:`new_job`.
    """

    inputs: Mapping[str, object]
    files: Mapping[str, str | os.PathLike[str]]
    parameters: Mapping[str, object]
    environment: Mapping[str, object]
    tag: str | None
    name: str
    placement: str | PurePosixPath
    priority: int | None
    provenance: Mapping[str, object] | None
    move_inputs: Mapping[str, str | os.PathLike[str]]


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
    :param resources: Preserve the default resource requirement, time labels in seconds.
    :param step_resources: Preserve per-step resource requirements, time labels in seconds.
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
    :param command: Give the directory package's unexpanded runner command, replacing *entry*.
    :param instantiate_file: Name the directory package's instantiate hook.
    :param instantiate_exec: Name an executable directory package instantiate hook.
    :param collect_file: Name the directory package's collector.
    :param collector_exec: Name an executable directory package collector.
    :param postprocess_scripts: Preserve curated postprocess script metadata.
    :param parameters: Preserve the workflow's parameter metadata.
    :param environment: Preserve the workflow's environment metadata.
    :param settings: Preserve the workspace settings the workflow declares reading.
    :param outputs: Preserve the workflow's output metadata.
    :param declaration_uri: Identify the source declaration URI.
    :param declaration_file: Name the source declaration file.
    :param name: Give the short name of a workflow whose id is a git URI.
    :param requires: Give the ``NAME>=VERSION`` distribution requirements a job of it must meet.
    :param calls: Declare the fixed workflows a job of it may call, alias to workflow reference.
    :param slots: Declare the call slots whose targets a job's creator binds, alias to slot.
    :param recognize: Preserve the recognize declaration of a collector-only package.
    :param references: Preserve the declared literature references, label to OPTIMADE reference table.
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
    command: tuple[str, ...] | None = None
    instantiate_file: str | None = None
    instantiate_exec: str | None = None
    collect_file: str | None = None
    collector_exec: str | None = None
    postprocess_scripts: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    parameters: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    environment: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    settings: tuple[str, ...] = ()
    outputs: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    declaration_uri: str | None = None
    declaration_file: str | None = None
    name: str | None = None
    requires: tuple[str, ...] = ()
    calls: Mapping[str, str] | None = None
    slots: Mapping[str, CallSlot] | None = None
    recognize: RecognizeSpec | None = None
    references: Mapping[str, Mapping[str, object]] = field(default_factory=dict)
    _input_metadata: Mapping[str, Mapping[str, object]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if self.language is not None:
            if self.packaged is not None or (self.directory is None and self.document_path is None):
                raise ValueError("a language workflow must have a source document or directory")
            if self.entry != "run" or self.command is not None or self.instantiate_file is not None:
                raise ValueError("language workflow runner fields are implied by the language")
            if not self.instantiate:
                raise ValueError("a language workflow must have instantiate enabled")
        else:
            if self.packaged is not None and self.directory is not None:
                raise ValueError("a resolved workflow cannot be packaged and directory-sourced")
            if self.packaged is not None and (
                self.entry != "run"
                or self.command is not None
                or self.instantiate_file is not None
                or self.instantiate_exec is not None
                or self.collect_file is not None
                or self.declaration_file is not None
                or self.build is not None
            ):
                raise ValueError("directory-only fields are not allowed on a packaged resolved workflow")
        object.__setattr__(self, "runner_options", MappingProxyType(dict(self.runner_options)))
        object.__setattr__(self, "postprocess_scripts", MappingProxyType(dict(self.postprocess_scripts)))
        object.__setattr__(self, "references", MappingProxyType(dict(self.references)))
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(self, "settings", tuple(self.settings))
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
    def definition_uri(self) -> str | None:
        """The git URI identifying the workflow definition, when the id is one."""

        return self.workflow_id if self.workflow_id.startswith("git+") else None

    @property
    def runnable(self) -> bool:
        """Whether jobs can be created from the workflow; a recognize collector is never run."""

        return self.recognize is None


@dataclass(frozen=True)
class ScaffoldedJob:
    """Describe one job this module submitted.

    :param job_id: Identify the submitted job.
    :param job_key: Identify the job directory.
    :param tag: Preserve the optional job tag.
    :param placement: Locate the job within each state directory.
    :param payload: Locate the job directory as submitted, in ``jobs/ready/``.
    :param ref: The kernel reference of the submitted job directory.
    :param workflow: Name the installed workflow's id.
    :param workflow_name: Name the installed workflow's short name.
    :param initial_step: Name the step the job starts at.
    :param warnings: Preserve the preparation warnings raised for this workflow.
    """

    job_id: str
    job_key: str
    tag: str | None
    placement: PurePosixPath
    payload: Path
    ref: _kernel.JobRef
    workflow: str
    workflow_name: str
    initial_step: str
    warnings: tuple[str, ...] = ()

    def as_mapping(self) -> dict[str, object]:
        """Return the machine-readable report of this job.

        :return: The serialized job report.
        """

        return {
            "format": JOB_SCAFFOLD_FORMAT,
            "format_version": 3,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "tag": self.tag,
            "placement": placement_text(self.placement),
            "payload_path": str(self.payload),
            "state": self.ref.state,
            "workflow": {"id": self.workflow, "name": self.workflow_name},
            "initial_step": self.initial_step,
        }


@dataclass(frozen=True)
class _Prepared:
    """An installed workflow resolved once for every job that will use it."""

    installed: Installed
    workflow: ResolvedWorkflow
    required_capabilities: tuple[str, ...] = ()
    reserved_parameters: tuple[str, ...] = ()
    caller_parameters: tuple[str, ...] = ()
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
    input, ``parameters`` holds only the caller-supplied knobs (plus any a
    language realization wires in), ``defaults`` is the read-only mapping of the
    manifest's declared parameter defaults, and ``tag`` is the caller's tag. The
    hook may write below ``payload`` and update ``parameters``; after it returns,
    each declared default fills in a parameter still absent. Use
    :meth:`suggest_tag` to provide a tag without overriding one the caller
    supplied.

    :param payload: Locate the payload being staged.
    :param inputs: Provide the supplied workflow inputs.
    :param parameters: Provide the caller-supplied job parameters.
    :param tag: Preserve or suggest the job tag.
    :param defaults: Provide the declared parameter defaults, read-only.
    """

    payload: Path
    inputs: Mapping[str, object]
    parameters: dict[str, object]
    tag: str | None
    defaults: Mapping[str, object] = field(default_factory=lambda: MappingProxyType({}))

    def suggest_tag(self, tag: str) -> None:
        """Set a tag only when the caller did not supply one."""

        if self.tag is None:
            self.tag = tag


def describe_runner(runner: str | os.PathLike[str], *, preserve_registration_order: bool = False) -> dict[str, object]:
    """Return the self-description one runner file prints, by running it.

    Every native runner — Python or Bash — answers ``HTTK_WORKFLOW_DESCRIBE=1``
    with its workflow name and its registered steps and exits without touching
    anything, which is how a runner nobody wrote a workflow for is still
    scaffolded without being told what it implements.

    :param runner: Locate the runner file to describe.
    :param preserve_registration_order: Retain the native Bash registration order.
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
    return _describe(_describe_command(path), path, preserve_registration_order=preserve_registration_order)


def describe_package_runner(
    directory: str | os.PathLike[str], *, artifacts: str | os.PathLike[str] | None = None
) -> dict[str, object]:
    """Return the self-description of a directory package's runner, by running it.

    A package with ``[workflow.runner].command`` is described by running that
    command with its placeholders expanded, exactly as a manager launches it;
    otherwise its ``run`` entry is described with :func:`describe_runner`.

    :param directory: Locate the workflow package directory.
    :param artifacts: Locate the package's built artifacts, substituted for ``{artifacts}``.
    :return: The validated runner description.
    :raises ValueError: If the package is malformed, its command needs artifacts that
        were not given, or the runner emits an invalid description.
    """

    from .packages import parse_workflow_manifest

    root = Path(directory).expanduser().resolve()
    provider = parse_workflow_manifest(root)
    if not provider.runnable:
        raise ValueError(f"{provider.workflow_id} recognizes calculations and has no runner to describe")
    if provider.command is None:
        return describe_runner(root / provider.entry)
    artifacts_path = None if artifacts is None else Path(artifacts).expanduser().resolve()
    command = expand_runner_command(provider.command, root, artifacts_path)
    extra = {"HTTK_WORKFLOW_RUNNER_ROOT": str(root)}
    if artifacts_path is not None:
        extra["HTTK_WORKFLOW_RUNNER_ARTIFACTS"] = str(artifacts_path)
    return _describe(list(command), root, extra=extra)


def _describe(
    command: list[str],
    path: Path,
    *,
    preserve_registration_order: bool = False,
    extra: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Run one runner command in describe mode and validate what it prints."""

    environment = dict(os.environ)
    environment[_DESCRIBE_VARIABLE] = "1"
    # Describe under this interpreter, exactly as the manager runs an attempt.
    environment["PATH"] = interpreter_first_path(environment.get("PATH"))
    if preserve_registration_order:
        environment["HTTK_WORKFLOW_PRESERVE_STEP_ORDER"] = "1"
    environment["HTTK_WORKFLOW_BASH_API"] = str(Path(__file__).with_name("languages") / "bash" / "httk-workflow.sh")
    environment["HTTK_WORKFLOW_LANGUAGES_DIR"] = str(Path(__file__).with_name("languages"))
    environment["HTTK_WORKFLOW_PERL_API"] = str(Path(__file__).with_name("languages") / "perl")
    environment.update(code_environment())
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
        "HTTK_WORKFLOW_RUNNER_WORKFLOW",
        "HTTK_WORKFLOW_RUNNER_STEPS",
        "HTTK_WORKFLOW_RUNNER_ROOT",
        "HTTK_WORKFLOW_RUNNER_ARTIFACTS",
    ):
        environment.pop(name, None)
    environment.update(extra or {})
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=_DESCRIBE_TIMEOUT,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot describe the runner {path}: {exc}") from exc
    detail = (completed.stderr or completed.stdout).strip().splitlines()
    missing_registration = next(
        (
            line
            for line in (completed.stderr + completed.stdout).splitlines()
            if "runner exited without calling httk_workflow_runner" in line
        ),
        None,
    )
    if missing_registration is not None:
        raise ValueError(
            f"the runner {path} exited without calling httk_workflow_runner WORKFLOW STEP...: {missing_registration}"
        )
    if completed.returncode != 0:
        if any("call httk_workflow_runner" in line for line in detail):
            raise ValueError(
                f"the runner {path} must call httk_workflow_runner WORKFLOW STEP... before any work: {detail[-1]}"
            )
        raise ValueError(
            f"the runner {path} refused to describe itself (exit {completed.returncode})"
            + (f": {detail[-1]}" if detail else "")
        )
    if not completed.stdout.strip():
        raise ValueError(f"the runner {path} must call httk_workflow_runner WORKFLOW STEP... before describe mode")
    return _parse_description(completed.stdout, path, preserve_registration_order=preserve_registration_order)


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
    if _has_bash_shebang(path):
        return ["bash", str(path)]
    raise ValueError(
        f"the runner {path} is not executable and its suffix names no interpreter; "
        "make it executable, or give it a .py or .sh suffix"
    )


def _has_bash_shebang(path: Path) -> bool:
    """Return whether *path* has a Bash interpreter shebang."""

    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except (OSError, UnicodeError, IndexError):
        return False
    if not first_line.startswith("#!"):
        return False
    try:
        interpreter = shlex.split(first_line[2:])
    except ValueError:
        return False
    if not interpreter:
        return False
    if Path(interpreter[0]).name == "bash":
        return True
    if Path(interpreter[0]).name != "env":
        return False
    command = interpreter[1:]
    if command[:1] == ["-S"]:
        command = command[1:]
    return bool(command) and Path(command[0]).name == "bash"


def _parse_description(text: str, path: Path, *, preserve_registration_order: bool = False) -> dict[str, object]:
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
    ordered_steps = [str(step) for step in steps]
    if not preserve_registration_order:
        ordered_steps.sort()
    result: dict[str, object] = {"workflow": described["workflow"], "steps": ordered_steps}
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


def registered_workflow(name: str) -> ResolvedWorkflow | None:
    """Return the registered workflow selected by *name*, or ``None``.

    :param name: Select a workflow by id or alias.
    :return: The resolved workflow, or ``None`` when no provider matches.
    """

    provider = workflow_provider(name)
    return None if provider is None else _provider_resolution(provider)


def _provider_resolution(provider: WorkflowProvider) -> ResolvedWorkflow:
    """Return the resolution of one registered, plugin, or fetched provider."""

    source = provider.directory if provider.directory is not None else _packaged_runner_path(provider)
    resources, step_resources = provider._requirements()
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
        resources=resources,
        step_resources=step_resources,
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
        command=provider.command,
        instantiate_file=provider.instantiate_file,
        instantiate_exec=provider.instantiate_exec,
        collect_file=provider.collect_file,
        collector_exec=provider.collector_exec,
        postprocess_scripts=provider.postprocess_scripts,
        parameters=provider.parameters,
        environment=provider.environment,
        settings=provider.settings,
        outputs=provider.outputs,
        declaration_uri=provider.declaration_uri,
        declaration_file=provider.declaration_file,
        name=provider.name,
        requires=provider.requires,
        calls=provider.calls,
        slots=provider.slots,
        recognize=provider.recognize,
        references=provider.references,
        _input_metadata=provider._input_metadata,
    )


def resolve_workflow(
    workflow: str | os.PathLike[str],
    *,
    workflow_id: str | None = None,
    step: str | None = None,
    format: str | None = None,
) -> ResolvedWorkflow:
    """Return the :class:`ResolvedWorkflow` *workflow* names.

    *workflow* is the name of a packaged workflow, a git workflow URI (which is
    fetched and installed, see ``httk.workflow.git_workflows``), the file name
    of a packaged runner, or the path of a runner file. A runner file is described by running
    it, so its workflow name and its steps come from the runner itself; *workflow*
    and *step* override what it said, and *step* is required when a runner
    registers several steps and none of them is ``start``.

    :param workflow: Select a registered workflow, package directory, or runner file.
    :param workflow_id: Override the resolved workflow id.
    :param step: Override the resolved initial step.
    :param format: Force a language for a bare document or directory.
    :return: The resolved workflow description.
    :raises ValueError: If the workflow cannot be found, its description is invalid, or its
        ``requires`` are unmet in this interpreter (a ``RequirementError``).
    """

    text = os.fspath(workflow)
    if text.startswith("git+"):
        # An explicit URI reference is consent to fetch; job payloads go through
        # the cache-only :func:`workflow_provider` instead.
        from .git_workflows import fetch_workflow

        resolved: ResolvedWorkflow | None = _provider_resolution(fetch_workflow(text))
    else:
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
            resources, step_resources = provider._requirements()
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
                summary=provider.summary,
                inputs=provider.inputs,
                resources=resources,
                step_resources=step_resources,
                instantiate=provider.instantiate,
                declarations=provider.declarations,
                collector=provider.collector,
                directory=provider.directory or path.resolve(),
                build=provider.build,
                entry=provider.entry,
                command=provider.command,
                instantiate_file=provider.instantiate_file,
                instantiate_exec=provider.instantiate_exec,
                collect_file=provider.collect_file,
                collector_exec=provider.collector_exec,
                postprocess_scripts=provider.postprocess_scripts,
                parameters=provider.parameters,
                environment=provider.environment,
                settings=provider.settings,
                outputs=provider.outputs,
                declaration_uri=provider.declaration_uri,
                declaration_file=provider.declaration_file,
                requires=provider.requires,
                calls=provider.calls,
                slots=provider.slots,
                recognize=provider.recognize,
                references=provider.references,
                _input_metadata=provider._input_metadata,
            )
        elif path.exists():
            from . import compat

            resolved_path = path.resolve()
            if format is None:
                lang = compat.match_document(resolved_path)
            else:
                lang = compat.language(format)
                if lang.document_policy == "forbidden" and not resolved_path.is_dir():
                    raise ValueError(
                        f"workflow format {lang.name!r} forbids documents; --format {format!r} requires a directory target"
                    )
                if lang.document_policy != "forbidden" and not resolved_path.is_file():
                    raise ValueError(
                        f"workflow format {lang.name!r} expects a document file for --format {format!r}: {resolved_path}"
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
                    settings=_declared_settings(lang.environment),
                    _input_metadata=input_metadata,
                )
            elif path.is_file():
                described = describe_runner(
                    path,
                    preserve_registration_order=path.suffix in {".sh", ".bash"} or _has_bash_shebang(path),
                )
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
    if resolved.requires:
        check_requirements(
            parse_requirements(list(resolved.requires), "requires"), f"workflow {resolved.workflow_id!r}"
        )
    return resolved


def _initial_step(path: Path, steps: tuple[str, ...], requested: str | None) -> str:
    """Return the step a job of this runner starts at."""

    if requested is not None:
        return ensure_step_known(requested, steps, f"the runner {path}")
    if (_has_bash_shebang(path) or path.suffix in {".sh", ".bash"}) and steps:
        return steps[0]
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


def _declared_settings(environment: Mapping[str, Mapping[str, object]], listed: Iterable[str] = ()) -> tuple[str, ...]:
    """Return the workspace settings a workflow declares reading, sorted and without repeats.

    :param environment: The workflow's environment entries; each reads its ``setting``, or its own name.
    :param listed: The ``[workflow].settings`` names.
    :return: The union.
    """

    return tuple(sorted({*listed, *(str(entry.get("setting", name)) for name, entry in environment.items())}))


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
    step: str | None = None,
    format: str | None = None,
    name: str | None = None,
    provenance: Mapping[str, object] | None = None,
    install: bool = False,
    move_inputs: Mapping[str, str | os.PathLike[str]] | None = None,
    calls: Mapping[str, str] | None = None,
    call_parameters: Mapping[str, Mapping[str, object]] | None = None,
) -> ScaffoldedJob:
    """Scaffold, submit, and describe one job of an installed *workflow*.

    *workflow* is the id or short name of a workflow installed in *workspace*
    (``httk workflow install``), a registered or alias name, git URI or package
    directory whose installation is looked up, or the path of a runner file of
    your own or of a bare workflow document (a CWL or PWD file), which is
    installed as an ``adhoc:`` workflow. *files* maps payload
    names to the files to stage there: a bare name lands in the payload's
    :data:`~httk.workflow.scaffold.FILES_DIRECTORY`, which is where a packaged
    runner reads its inputs, and a name with a directory in it is used
    verbatim. *inputs* stages the workflow's declared objects into the payload;
    *parameters* is the job's opaque implementation mapping.

    The payload is built in the scratch of a CLI owner registered for the call
    and submitted to ``jobs/ready/<placement>/``.

    :func:`scaffold_job` is the same operation stopped one step short of
    submission: it builds the payload into a directory you name and returns its
    ``job.json`` definition, which is how :meth:`httk.workflow.Attempt.call`
    builds a child job from inside a running step.

    :param workspace: Provide the workspace receiving the job.
    :param workflow: Select the installed workflow, or a runner file to install.
    :param inputs: Supply declared workflow inputs.
    :param files: Map payload names to files to stage.
    :param parameters: Supply opaque job parameters.
    :param environment: Supply overrides for declared workflow environment values.
    :param tag: Set the job tag.
    :param placement: Place the job within the workspace.
    :param priority: Set the scheduling priority.
    :param step: Override the workflow's initial step.
    :param format: Force a workflow format for a bare document or directory, which is then installed ad hoc.
    :param name: Set the job's display name.
    :param provenance: Merge one declared-side ``provenance`` document (see
        :mod:`httk.workflow.provenance`) into the job's declarations. When the
        workflow declares no ``provenance`` of its own, *provenance* is used as
        that declaration outright; otherwise each of its ``inputs``/``artifacts``/
        ``outputs`` sections is concatenated label by label with the workflow's,
        and a label declared by both raises ``ValueError``. Its primary use is a
        birth-time claim that this job is for a database entity: an ``inputs``
        edge labelled ``entity`` naming the entity by its stable ledger key, e.g.
        ``{"inputs": {"entity": {"type": "amdb_material", "id": "magndata:1.108"}}}``.
    :param install: Install *workflow* into the workspace first when it is not installed.
    :param move_inputs: Map declared inputs that have a ``destination`` to files that
        are moved there, not copied, out of succeeded jobs of *workspace*. Each
        must be a regular file in a job's payload (not ``job.json``, ``state.json``,
        ``attempts/``, ``logs/`` or ``.httk-job/``) on the filesystem of the
        workspace. A sealed origin keeps its seal: a signed release record in its
        ``.httk-job/releases/`` amends it, and the file's ``staged_files``
        digest is the seal's, so the file is never hashed. An unsealed origin's
        file is hashed. If the job is not submitted, every file moves back and its
        release record is withdrawn.
    :param calls: Bind call slots of the workflow, alias to a workflow reference resolved like *workflow*
        (installed first with *install*). A slot not named here is bound to its installed default; each
        bound workflow must fit the slot's interface. Fixed ``[workflow.calls]`` cannot be rebound.
    :param call_parameters: Give the parameters a slot's bound workflow is called with, alias to parameters,
        checked against that workflow's declared parameter types.
    :return: The submitted job description.
    :raises ValueError: If the workflow is not installed (and *install* is false), or workflow,
        inputs, placement, or job settings are invalid, or a file to move is not a regular file
        inside a succeeded job of *workspace* on its filesystem, or a call slot is unbound, bound to a
        workflow that does not fit it, or given parameters of the wrong type.
    """

    [job] = new_jobs(
        workspace,
        workflow,
        [{"move_inputs": move_inputs} if move_inputs else {}],
        inputs=inputs,
        files=files,
        parameters=parameters,
        environment=environment,
        tag=tag,
        placement=placement,
        priority=priority,
        step=step,
        format=format,
        name=name,
        provenance=provenance,
        install=install,
        calls=calls,
        call_parameters=call_parameters,
    )
    return job


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
    step: str | None = None,
    format: str | None = None,
    name: str | None = None,
    provenance: Mapping[str, object] | None = None,
    install: bool = False,
    calls: Mapping[str, str] | None = None,
    call_parameters: Mapping[str, Mapping[str, object]] | None = None,
) -> Generator[ScaffoldedJob, None, None]:
    """Scaffold and submit one job per member of *items*, lazily.

    Every keyword is the shared value of the whole campaign, and every member of
    one :class:`~httk.workflow.scaffold.JobItem` is what that job varies: ``inputs`` and ``files`` are
    merged over the shared mappings, and ``tag``, ``name``, ``placement``, and
    ``priority`` replace the shared value.

    This is the pattern for a campaign of any size. The workflow is resolved (and,
    when asked, installed) once, however many jobs follow, so every job costs
    exactly one payload directory; *items* is consumed as an iterator and the
    results are yielded as they are submitted, so a structure generator can be
    turned into jobs without either side of the loop ever being materialized.
    Nothing happens until the first job is requested, and the CLI owner the jobs
    are built by is closed when the iterator is exhausted or closed.

    :param workspace: Provide the workspace receiving the jobs.
    :param workflow: Select the installed workflow, or a runner file to install.
    :param items: Yield per-job overrides.
    :param inputs: Supply shared declared workflow inputs.
    :param files: Supply shared payload files.
    :param parameters: Supply shared opaque job parameters.
    :param environment: Supply shared declared environment overrides.
    :param tag: Set the shared job tag.
    :param placement: Set the shared workspace placement.
    :param priority: Set the shared scheduling priority.
    :param step: Override the workflow's initial step.
    :param format: Force a workflow format for a bare document or directory, which is then installed ad hoc.
    :param name: Set the shared display name.
    :param provenance: Set the shared ``provenance`` document; a per-item
        ``provenance`` in :class:`~httk.workflow.scaffold.JobItem` replaces it entirely rather than
        merging with it. See :func:`new_job` for the merge rule against a
        workflow-declared ``provenance`` and the entity-claim convention.
    :param install: Install *workflow* into the workspace first when it is not installed.
    :param calls: Bind call slots of the workflow for every job; see :func:`new_job`.
    :param call_parameters: Give the parameters of the bound workflows; see :func:`new_job`.
    :return: An iterator yielding each submitted job description.
    :yield: Each submitted job description.
    :raises ValueError: If the workflow is not installed (and *install* is false), or workflow,
        inputs, placement, job settings or call slot bindings are invalid.

    .. code-block:: python

        def structures():
            for path in sorted(Path("structures").glob("POSCAR.*")):
                yield {"files": {"POSCAR": path}, "tag": structure_tag(path)}

        for job in new_jobs(workspace, "some-workflow", structures(), parameters={"kpoint_density": 30.0}):
            print(job.job_key)
    """

    owner = _kernel.register_owner(workspace, kind="cli", label="job new", allocation=None, advertised={})
    try:
        installed = _installed(workspace, workflow, owner=owner, install=install, step=step, language=format)
        prepared = _prepare(installed, step=step)
        bindings = _bind_slots(workspace, installed, calls, call_parameters, owner=owner, install=install)
        for item in items:
            scratch = owner.scratch(_SUBMIT)
            staging = scratch / "build"
            staging.mkdir()
            moves: list[_InputMove] = []
            try:
                job = _build_payload(
                    workspace,
                    prepared,
                    staging,
                    inputs={**(inputs or {}), **item.get("inputs", {})},
                    files={**(files or {}), **item.get("files", {})},
                    parameters={**(parameters or {}), **item.get("parameters", {})},
                    environment={**(environment or {}), **item.get("environment", {})},
                    tag=item.get("tag", tag),
                    placement=item.get("placement", placement),
                    priority=item.get("priority", priority),
                    name=item.get("name", name),
                    provenance=item.get("provenance", provenance),
                    move_inputs=item.get("move_inputs"),
                    moves=moves,
                    calls=bindings,
                )
                ref = _submit_staged(workspace, owner, scratch, staging)
            except BaseException:
                # A file that cannot go back keeps the markers, so closing the owner keeps the scratch holding it.
                if _undo_moves(workspace, moves):
                    _drop_markers(workspace, moves)
                raise
            finally:
                for move in moves:
                    os.close(move.directory)
            if moves:
                _drop_markers(workspace, moves)
                _fs.remove_empty_dir(_fs.loc(scratch))
            yield ScaffoldedJob(
                job_id=job.id,
                job_key=job.job_key,
                tag=job.tag,
                placement=job.placement,
                payload=ref.path,
                ref=ref,
                workflow=job.workflow_id,
                workflow_name=job.workflow_name,
                initial_step=job.initial_step,
                warnings=prepared.warnings,
            )
    finally:
        # Discards the scratch of a job whose building failed (never named job/), then the owner record.
        owner.close()


def submit_payload(
    workspace: Workspace,
    owner: _kernel.Owner,
    source: Path,
    *,
    move: bool,
    changes: Mapping[str, object] | None = None,
) -> _kernel.JobRef:
    """Submit a prepared payload directory (a ``job.json`` version 3 and its files) into ``jobs/ready/``.

    Everything :func:`httk.workflow._kernel.submit` checks is checked before
    *source* moves. The payload then reaches the owner's ``submit`` scratch as
    ``job/`` complete, so the scratch reconciler publishes it if the owner dies
    before the submission: a moved payload, the operator's only copy, is never
    discarded. A refused submission moves it back.

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch stages the payload.
    :param source: The payload directory.
    :param move: Move *source* instead of copying it.
    :param changes: ``job.json`` members to replace in the copy; only without *move*.
    :return: The published reference.
    :raises httk.workflow.errors.FormatError: If ``job.json`` is missing or invalid.
    :raises httk.workflow._fs.UnsafePath: If the placement below ``jobs/ready/`` is a symlink or not a directory,
        or a copied payload holds a special file.
    :raises ValueError: If the payload carries ``state.json`` or ``logs``, or *changes* come with *move*.
    """

    if move and changes:
        raise ValueError("a moved payload cannot be changed; copy it instead")
    job = JobDefinition.from_path(source / "job.json")
    if changes:
        job = JobDefinition.from_mapping({**job.as_mapping(), **changes})
    for trusted in ("state.json", LOGS_DIRECTORY):
        if os.path.lexists(source / trusted):
            raise ValueError(f"a prepared payload may not carry {trusted}; only a job's owner writes it")
    _kernel.check_placement(workspace, "ready", job.placement)
    scratch = owner.scratch(_SUBMIT)
    if move:
        return _submit_staged(workspace, owner, scratch, source.resolve())
    copied = scratch / "copy"
    _fs.copy_tree(source, copied, durable=workspace.durable)
    if changes:
        _fs.write_file(_fs.loc(copied / "job.json"), job.encode(), durable=workspace.durable)
    return _submit_staged(workspace, owner, scratch, copied)


def _submit_staged(workspace: Workspace, owner: _kernel.Owner, scratch: Path, payload: Path) -> _kernel.JobRef:
    """Rename a complete *payload* to ``scratch/job`` and submit it; a refused one goes back to *payload*."""

    staging = scratch / "job"
    _fs.move_owned(_fs.loc(payload), _fs.loc(staging), durable=workspace.durable)
    try:
        ref = _kernel.submit(workspace, owner, staging)
    except BaseException:
        if _fs.exists(_fs.loc(staging)):
            _fs.move_owned(_fs.loc(staging), _fs.loc(payload), durable=workspace.durable)
        raise
    _fs.remove_empty_dir(_fs.loc(scratch))
    return ref


def _finish_submit(owner: _kernel.Owner, scratch: Path) -> bool:
    """The ``submit`` scratch reconciler: publish the staged ``job/``, or keep it when that fails.

    Only a complete payload is ever named ``job/`` (moved there in one rename,
    or copied or built beside it and renamed), so publishing it after a crash is
    what the dead owner was doing; the fresh name makes a repeat harmless. A
    scratch without ``job/`` held an unfinished copy (its source is intact) or
    build (rerun by resubmitting), and is discarded, unless the build holds
    files moved out of other jobs (``job new --input-move-from-job``): those
    are their only copy, so the scratch is kept with a warning while a
    ``moved-*.json`` marker names a file that landed. A marker whose file is
    still home and never landed (a crash before the rename) is dropped.
    """

    staging = scratch / "job"
    if not _fs.exists(_fs.loc(staging)):
        markers = [marker for marker in sorted(scratch.glob(f"{_MOVED_PREFIX}*.json")) if _moved(owner, marker)]
        if _fs.exists(_fs.loc(scratch / "build")) and markers:
            # ponytail: a build holding files moved out of other jobs is kept for the operator, never discarded;
            # moving them back automatically would need their release records' rollback here too.
            _LOGGER.warning(
                "kept the unfinished payload %s: it holds files moved out of other jobs, listed in %s",
                scratch / "build",
                ", ".join(marker.name for marker in markers),
            )
            return False
        return True
    try:
        ref = _kernel.submit(owner.workspace, owner, staging)
    except (WorkflowError, OSError) as exc:
        _LOGGER.warning("kept the staged payload %s: %s", staging, exc)
        return False
    _LOGGER.warning("submitted the payload a dead owner staged: %s", ref.path)
    return True


def _moved(owner: _kernel.Owner, marker: Path) -> bool:
    """Report whether a marker's move may have happened; one whose file is still home and never landed is dropped."""

    try:
        document = json.loads(_fs.read_bounded(_fs.loc(marker), 1 << 16) or b"{}")
        source, target = Path(document["from"]), Path(document["to"])
    except (WorkflowError, OSError, ValueError, KeyError, TypeError):
        return True  # unreadable: keep the scratch for the operator
    if _is_regular(_fs.loc(source)) and _fs.lstat(_fs.loc(target)) is None:
        _fs.remove_file(_fs.loc(marker), durable=owner.workspace.durable)
        return False
    return _fs.lstat(_fs.loc(target)) is not None


_kernel.register_reconciler(_SUBMIT, _finish_submit)


def _installed(
    workspace: Workspace,
    workflow: str | os.PathLike[str],
    *,
    owner: _kernel.Owner | None,
    install: bool,
    step: str | None,
    language: str | None = None,
) -> Installed:
    """Return the installation *workflow* names, installing a runner file or bare document first.

    Anything else is installed only with *install*; without an *owner* nothing is installed.
    """

    from . import _store
    from .packages import MANIFEST_NAME, parse_workflow_manifest

    text = os.fspath(workflow)
    path = Path(text).expanduser()
    package = path.is_dir() and (path / MANIFEST_NAME).is_file()
    if owner is not None and (path.is_file() or (language is not None and path.is_dir() and not package)):
        return _store.install(workspace, owner, path, initial_step=step, language=language)
    found = None
    if package:
        found = _store.lookup(workspace, f"local:{parse_workflow_manifest(path).workflow_id}")
        # An edited source tree is installed again; an unchanged one is reused.
        if (
            found is not None
            and owner is not None
            and install
            and _store.source_digest(path) != found.record.get("tree_sha256")
        ):
            return _store.install(workspace, owner, path)
    elif not path.exists():
        found = _store.lookup(workspace, text)
        # An alias or a git URI of an already fetched workflow names an installation by its canonical id.
        if found is None and (provider := workflow_provider(text)) is not None:
            found = _store.lookup(workspace, provider.definition_uri or f"local:{provider.workflow_id}")
    if found is not None:
        return found
    if owner is not None and install:
        return _store.install(workspace, owner, path if path.exists() else text)
    raise ValueError(
        f"workflow {text!r} is not installed in the workspace {workspace.root}; "
        f"install it first with `httk workflow install --workspace {shlex.quote(str(workspace.root))} "
        f"{shlex.quote(text)}`, or create the job with `httk job new --install` (install=True)"
    )


def _installed_by_name(
    workspace: Workspace, reference: str, *, owner: _kernel.Owner | None, install: bool
) -> Installed:
    """Return the installation a workflow name, id or git URI names, as :func:`_installed` does for one.

    Never a path: a call slot binding must not pick up a directory or file of that name where the job is created.
    """

    from . import _store

    found = _store.lookup(workspace, reference)
    if found is None and (provider := workflow_provider(reference)) is not None:
        found = _store.lookup(workspace, provider.definition_uri or f"local:{provider.workflow_id}")
    if found is not None:
        return found
    if owner is not None and install:
        return _store.install(workspace, owner, reference, by_name=True)
    raise ValueError(
        f"workflow {reference!r} is not installed in the workspace {workspace.root}; "
        f"install it first with `httk workflow install --workspace {shlex.quote(str(workspace.root))} "
        f"{shlex.quote(reference)}`, or create the job with `httk job new --install` (install=True)"
    )


def _prepare(installed: Installed, *, step: str | None) -> _Prepared:
    """Resolve one installed workflow for job creation, exactly once per campaign.

    An instantiate hook runs from the installed package, verified against the
    installed tree digest. A language workflow is prepared by its realization,
    whose runner is the manager's own built-in, so only its payload members,
    parameters and hooks are taken.
    """

    resolved = _provider_resolution(installed.provider())
    if not resolved.runnable:
        raise ValueError(f"{resolved.workflow_id} recognizes calculations and cannot be run")
    if step is not None:
        if resolved.steps:
            ensure_step_known(step, resolved.steps, f"the workflow {installed.id}")
        resolved = replace(resolved, initial_step=step)
    if resolved.language is not None:
        from . import compat

        scaffolded = compat.language(resolved.language).prepare(_language_request(resolved))
        parameters = dict(scaffolded.parameters)
        parameters["workflow_realization"] = "language"
        reserved_parameters = (*scaffolded.reserved_parameters, "workflow_realization")
        if resolved.collect_file is not None:
            parameters["workflow_collect"] = "package"
            reserved_parameters = (*reserved_parameters, "workflow_collect")
        return _Prepared(
            installed=installed,
            workflow=resolved,
            required_capabilities=tuple(sorted(set(scaffolded.required_capabilities))),
            reserved_parameters=reserved_parameters,
            caller_parameters=scaffolded.caller_parameters,
            documents=scaffolded.documents,
            files=scaffolded.files,
            parameters=parameters,
            warnings=scaffolded.warnings,
            instantiate=scaffolded.instantiate,
            finalize=scaffolded.finalize,
        )
    if resolved.instantiate_file is None:
        return _Prepared(installed=installed, workflow=resolved)
    from .packages import _tree_hook

    digest = str(installed.record.get("tree_sha256"))
    if resolved.instantiate_exec is not None:
        return _Prepared(
            installed=installed,
            workflow=resolved,
            instantiate_exec=(installed.package, digest, resolved.instantiate_exec),
        )
    hook = _tree_hook(installed.package, digest, resolved.instantiate_file, "instantiate")
    return _Prepared(
        installed=installed, workflow=resolved, instantiate=cast(Callable[[InstantiateContext], object], hook)
    )


def _language_request(resolved: ResolvedWorkflow) -> LanguageRequest:
    """Build the language request, including package-only exclusions."""

    from . import compat

    excluded: tuple[str, ...] = ()
    if resolved.directory is not None:
        from .packages import MANIFEST_NAME

        members = [MANIFEST_NAME]
        members.extend(member for member in (resolved.collect_file, resolved.declaration_file) if member is not None)
        members.extend(str(script["file"]) for script in resolved.postprocess_scripts.values())
        if not (resolved.directory / MANIFEST_NAME).is_file():
            members = []
        excluded = tuple(dict.fromkeys(members))
    return compat.LanguageRequest(
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


def _runner_instantiate(source: Path) -> Callable[[InstantiateContext], object]:
    """Import an SDK runner file once and return the instantiate hook of its one :class:`~httk.workflow.Runner`.

    The ad hoc package of a runner file that declares a hook calls this from its
    generated ``instantiate.py`` member.

    :param source: The runner file.
    :return: The hook.
    :raises ValueError: If the file defines no or several runners, or its runner has no hook.
    """

    source_bytes = source.read_bytes()
    module_name = f"httk_workflow_runner_{hashlib.sha256(source_bytes).hexdigest()}"
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


def _merge_provenance_declaration(
    declarations: Mapping[str, Mapping[str, object]], provenance: Mapping[str, object] | None
) -> Mapping[str, Mapping[str, object]]:
    """Merge a caller-supplied ``provenance`` document into a workflow's declarations.

    *provenance* is one declared-side document in the vocabulary of
    :mod:`httk.workflow.provenance` (its ``inputs``/``artifacts``/``outputs``
    sections). ``None`` leaves *declarations* untouched. When *declarations* has
    no ``provenance`` member of its own, *provenance* becomes that member
    outright; otherwise the two documents are merged by :func:`_merge_provenance_documents`.

    :param declarations: The workflow's own declarations, keyed by name.
    :param provenance: The caller-supplied ``provenance`` document, or ``None``.
    :return: *declarations*, with its ``"provenance"`` member merged in when *provenance* is supplied.
    :raises ValueError: If a label is declared by both documents in one merged section.
    """

    if provenance is None:
        return declarations
    existing = declarations.get("provenance")
    document: Mapping[str, object] = (
        provenance if existing is None else _merge_provenance_documents(existing, provenance)
    )
    return {**declarations, "provenance": document}


def _merge_provenance_documents(
    workflow_document: Mapping[str, object], caller_document: Mapping[str, object]
) -> dict[str, object]:
    """Merge one caller-supplied ``provenance`` document into the workflow's own.

    Each of ``inputs``/``artifacts``/``outputs`` present in either document is
    the concatenation of the two documents' labeled edges; a label declared by
    both in the same section is a :class:`ValueError` naming the label and both
    sources. A section only one side declares is carried through verbatim, so a
    malformed shape is left for the existing declarations/provenance validation
    to reject, exactly as a workflow-only declaration would be — but a section
    both sides declare must be concatenated, so a non-mapping value on either
    side there is a :class:`ValueError` naming the section and the malformed
    side, rather than one side silently replacing the other's edges. No other
    members are merged: any other top-level member comes from the workflow's
    document only, since the caller-supplied document exists to carry only this
    section vocabulary.

    :param workflow_document: The workflow's declared ``provenance`` document.
    :param caller_document: The caller-supplied ``provenance`` document.
    :return: The merged ``provenance`` document.
    :raises ValueError: If a label is declared by both documents in one merged
        section, or if a section declared by both is not a mapping on one side.
    """

    merged = dict(workflow_document)
    for section in ("inputs", "artifacts", "outputs"):
        in_workflow = section in workflow_document
        in_caller = section in caller_document
        if not in_workflow and not in_caller:
            continue
        workflow_section = workflow_document.get(section)
        caller_section = caller_document.get(section)
        if in_workflow and in_caller:
            if not isinstance(workflow_section, Mapping):
                raise ValueError(f"provenance {section}: the workflow's section must be a mapping")
            if not isinstance(caller_section, Mapping):
                raise ValueError(f"provenance {section}: the caller-supplied section must be a mapping")
            combined: dict[str, object] = dict(workflow_section)
            for label, edge in caller_section.items():
                if label in combined:
                    raise ValueError(
                        f"provenance {section} label {label!r} is declared by both the workflow "
                        "and the caller-supplied provenance"
                    )
                combined[label] = edge
            merged[section] = combined
        elif in_caller:
            merged[section] = caller_section
        else:
            merged[section] = workflow_section
    return merged


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


def scaffold_job(
    workspace: Workspace,
    workflow: str | os.PathLike[str],
    destination: str | os.PathLike[str],
    *,
    inputs: Mapping[str, object] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    parameters: Mapping[str, object] | None = None,
    environment: Mapping[str, object] | None = None,
    tag: str | None = None,
    priority: int | None = None,
    step: str | None = None,
    name: str | None = None,
    maxtime_cap: int | None = None,
    calls: Mapping[str, str] | None = None,
    call_parameters: Mapping[str, Mapping[str, object]] | None = None,
) -> JobDefinition:
    """Build one job payload of an installed *workflow* into *destination*, without submitting it.

    This is :func:`new_job` stopped one step short of submission: it looks the
    workflow up in the workspace (it never installs), stages *files* and
    *inputs*, validates *parameters* and *environment*, runs any instantiate
    hook, and writes ``job.json`` — but into *destination* rather than into the
    workspace. *destination* must already exist and be an empty directory. The
    result is a prepared payload directory, which is exactly what
    :meth:`httk.workflow.Attempt.spawn` accepts, so a running step can build a
    child job of an installed workflow and spawn it; :meth:`httk.workflow.Attempt.call`
    does exactly that.

    Every argument other than *destination* means what it does for
    :func:`new_job`, minus *placement*: the payload's ``job.json`` carries the
    root placement until something spawns it at its own.

    :param workspace: Provide the workspace the workflow is installed in.
    :param workflow: Select the installed workflow by id or name.
    :param destination: Locate the empty directory to build the payload into.
    :param inputs: Supply declared workflow inputs.
    :param files: Map payload names to files to stage.
    :param parameters: Supply opaque job parameters.
    :param environment: Supply overrides for declared workflow environment values.
    :param tag: Set the job tag.
    :param priority: Set the scheduling priority.
    :param step: Override the workflow's initial step.
    :param name: Set the job's display name.
    :param maxtime_cap: Cap every ``maxtime`` of the job at these seconds, setting the job-level one
        when the workflow declares none; :meth:`httk.workflow.Attempt.call` passes its own ``maxtime``.
    :param calls: Bind call slots of the workflow to installed workflows; see :func:`new_job`.
    :param call_parameters: Give the parameters of the bound workflows; see :func:`new_job`.
    :return: The written job definition.
    :raises ValueError: If the destination is not an empty directory, the workflow is not installed,
        or workflow, inputs, job settings or call slot bindings are invalid.
    """

    target = Path(destination)
    if not target.is_dir():
        raise ValueError(f"a scaffold_job destination must be an existing directory: {target}")
    if any(target.iterdir()):
        raise ValueError(f"a scaffold_job destination must be an empty directory: {target}")
    installed = _installed(workspace, workflow, owner=None, install=False, step=step)
    prepared = _prepare(installed, step=step)
    bindings = _bind_slots(workspace, installed, calls, call_parameters, owner=None, install=False)
    return _build_payload(
        workspace,
        prepared,
        target,
        inputs=inputs,
        files=files,
        parameters=parameters,
        environment=environment,
        tag=tag,
        priority=priority,
        name=name,
        maxtime_cap=maxtime_cap,
        calls=bindings,
    )


def _bind_slots(
    workspace: Workspace,
    installed: Installed,
    calls: Mapping[str, str] | None,
    call_parameters: Mapping[str, Mapping[str, object]] | None,
    *,
    owner: _kernel.Owner | None,
    install: bool,
) -> dict[str, dict[str, object]] | None:
    """Bind every call slot of *installed* for a new job: the ``job.json`` ``calls`` member, or ``None`` without slots.

    A slot is bound to the reference *calls* gives it, else to the default its installation recorded; the bound
    workflow is looked up (or, with an *owner* and *install*, installed) like the job's own, must fit the slot's
    interface, and is given *call_parameters* checked against its declared parameter types. Bound to its default,
    a slot's ``default_parameters`` come first and *call_parameters* override them per entry.
    """

    from ._slots import SlotMismatch, fit
    from .models import validate_calls

    provider = installed.provider()
    slots = provider.slots or {}
    chosen: dict[str, str] = dict(calls or {})
    given_parameters: dict[str, Mapping[str, object]] = dict(call_parameters or {})
    for member, aliases in (("calls", list(chosen)), ("call_parameters", list(given_parameters))):
        for alias in aliases:
            if alias in (provider.calls or {}):
                raise ValueError(
                    f"{member} names {alias!r}, a fixed call of {installed.id}; fixed calls cannot be rebound"
                )
            if alias not in slots:
                declared = ", ".join(slots) or "none"
                raise ValueError(
                    f"{member} names {alias!r}, which is not a call slot of {installed.id} (slots: {declared})"
                )
    if not slots:
        return None
    recorded = installed.record.get("slots")
    defaults = recorded if isinstance(recorded, dict) else {}
    bindings: dict[str, dict[str, object]] = {}
    for alias, slot in slots.items():
        default = defaults.get(alias)
        reference = chosen.get(alias, default)
        if not isinstance(reference, str):
            raise ValueError(
                f"call slot {alias!r} of {installed.id} has no default; bind it (httk job new --call {alias}=WORKFLOW)"
            )
        try:
            # A binding names a workflow like a [workflow.calls] reference: never a path, so nothing is installed
            # from one.
            validate_calls({alias: reference}, "calls")
            bound = _installed_by_name(workspace, reference, owner=owner, install=install)
        except ValueError as exc:
            if alias not in chosen and owner is not None and install:
                raise ValueError(
                    f"call slot {alias!r} of {installed.id}: installing its default failed: {exc}"
                ) from exc
            if alias not in chosen:
                # Installing a workflow never installs its slot defaults; only an explicit --install does.
                raise ValueError(
                    f"call slot {alias!r} of {installed.id} defaults to {reference}, which is not installed; "
                    f"install it (httk workflow install {reference}) or bind another with --call {alias}=WORKFLOW"
                ) from exc
            raise ValueError(f"call slot {alias!r}: {exc}") from exc
        try:
            fit(slot.interface, bound.provider())
        except SlotMismatch as exc:
            raise ValueError(f"call slot {alias!r} bound to {bound.id}: {exc}") from exc
        parameters = dict(given_parameters.get(alias, {}))
        if slot.default_parameters and isinstance(default, str):
            # The default's parameters go with the default, whether bound implicitly or named again.
            same = alias not in chosen
            if not same:
                try:
                    same = _installed_by_name(workspace, default, owner=None, install=False).id == bound.id
                except ValueError:
                    same = False
            if same:
                parameters = {**slot.default_parameters, **parameters}
        declared_parameters = bound.provider().parameters
        if declared_parameters:
            _check_parameter_types(parameters, declared_parameters)
            logger = context_logger(_LOGGER, bound.id)
            for name in sorted(set(parameters) - set(declared_parameters)):
                logger.warning(
                    "call slot %r parameter %r is not declared by %s; declared: %s",
                    alias,
                    name,
                    bound.id,
                    ", ".join(sorted(declared_parameters)),
                )
        bindings[alias] = {"workflow": bound.id, "parameters": parameters}
    return bindings


@dataclass
class _InputMove:
    """One sealed or unsealed file moved into a payload being built, kept to roll the move back.

    :param origin: The payload of the job the file came from.
    :param directory: A descriptor of the file's directory there, open until the job is submitted, so the
        file goes back to the same directory even when its job moves meanwhile.
    :param name: The file's name in that directory.
    :param moved_to: Where the file is in the payload being built.
    :param entry: The ``staged_files`` entry ``{path, size, sha256}``.
    :param release: The release record created for the origin's seal, or ``None`` for an unsealed origin.
    :param seal: The origin's seal the record amends, or ``None`` for an unsealed origin.
    :param marker: The scratch marker naming the move, which keeps the scratch from being discarded.
    """

    origin: Path
    directory: int
    name: str
    moved_to: Path
    entry: dict[str, object]
    release: Release | None
    seal: Seal | None = None
    marker: Path | None = None


_MOVE_REFUSAL = "{path} is not a regular file inside a succeeded job of this workspace"
#: Beside a submit scratch's ``build/``: ``moved-<sha256 of "<origin job id>/<path>">.json`` per file moved into
#: it, written before the release record, so the scratch is never discarded and other movers see the move in flight.
_MOVED_PREFIX = "moved-"


def _release_keys(workspace: Workspace) -> tuple[SealKey, ...]:
    """Return the workspace's seal keys, or none for unsigned release records, exactly as seals resolve them."""

    from .errors import SealError
    from .seals import default_workspace_keys

    try:
        return default_workspace_keys(workspace).keys
    except SealError:
        return ()


def _move_source(workspace: Workspace, path: str | os.PathLike[str]) -> tuple[_kernel.JobRef, PurePosixPath]:
    """Return the succeeded job of *workspace* whose payload holds *path*, and the path below that payload."""

    from .introspection._reading import iter_jobs

    candidate = Path(os.path.abspath(Path(path).expanduser()))
    jobs = {ref.path.resolve(): ref for ref in iter_jobs(workspace, ("succeeded",))}
    for ancestor in candidate.parents:
        ref = jobs.get(ancestor.resolve())
        if ref is not None:
            relative = PurePosixPath(candidate.relative_to(ancestor).as_posix())
            if _reserved_payload_member(relative.parts[0]) or relative.parts[0] == "state.json":
                raise ValueError(f"{path} is a reserved member of its job, which is never moved")
            return ref, relative
    raise ValueError(_MOVE_REFUSAL.format(path=path))


def _move_input(
    workspace: Workspace,
    payload: Path,
    relative: PurePosixPath,
    source: str | os.PathLike[str],
    *,
    job_key: str,
    keys: tuple[SealKey, ...],
) -> _InputMove:
    """Move one file out of a succeeded job into *payload* at *relative*, amending the origin's seal.

    The digest comes from the origin's seal record and the file is never
    hashed; an unsealed origin is hashed once, with a warning. The release
    record is created exclusively before the rename: it is the intent, and the
    claim that refuses a second mover of the same file. A crash in between
    leaves a record whose file is still in place, which changes nothing. The
    rename is verified by the file it landed.
    """

    from .introspection._reading import read_state
    from .seals import _job_seal_bytes, _parse_job_seal, _signed_body_problem, job_seal_path

    ref, member = _move_source(workspace, source)
    origin = ref.path
    if (unlocked := read_state(ref)[0]) is not None and unlocked.unlocked is not None:
        # Moving would launder a debug-mode product into a clean job.
        raise ValueError(f"{source}: the source job is unlocked; copy the file with --input-from instead")
    try:
        directory = _fs.open_dir_under(origin, member.parent) if member.parent.parts else _fs.open_dir(origin)
    except (FileNotFoundError, _fs.UnsafePath) as exc:
        raise ValueError(_MOVE_REFUSAL.format(path=source)) from exc
    try:
        try:
            status = os.stat(member.name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            status = None
        if status is None or not stat.S_ISREG(status.st_mode):
            raise ValueError(_MOVE_REFUSAL.format(path=source))
        moved_to = payload.joinpath(*relative.parts)
        if os.path.lexists(moved_to):
            raise ValueError(f"the moved input {relative.as_posix()!r} collides with an existing payload member")
        moved_to.parent.mkdir(parents=True, exist_ok=True)
        if os.stat(moved_to.parent).st_dev != status.st_dev:
            raise ValueError(
                f"{source} is on another filesystem than the workspace's staging directory; "
                "a move must be a rename, and a cross-filesystem move is refused"
            )
        data = _job_seal_bytes(origin)
        release: Release | None = None
        seal: Seal | None = None
        if data is None:
            _LOGGER.warning("%s: its job is not sealed, so the moved file is hashed", source)
            digested = _fs.digest_regular(_fs.anchored(directory, member.name), -1)
            if digested is None:
                raise ValueError(_MOVE_REFUSAL.format(path=source))
            size, sha256, _ = digested
        else:
            # The authority is state.json: a seal document the job planted itself vouches for nothing.
            doc, damage = read_state(ref)
            authority = None if doc is None or doc.seal is None else doc.seal.get("sha256")
            if authority != hashlib.sha256(data).hexdigest():
                raise ValueError(
                    f"{source}: the seal of its job is not the one its manager wrote ({damage or 'no match'})"
                )
            seal = _parse_job_seal(data, job_seal_path(origin))
            problem = _signed_body_problem(json.loads(seal.body_bytes), seal.body_sha256, seal.signatures)
            recorded = next(
                (item for item in seal.records if item.get("path") == member.as_posix() and item.get("type") == "file"),
                None,
            )
            if problem is not None or recorded is None or recorded.get("size") != status.st_size:
                why = problem or ("it does not record the file" if recorded is None else "the file changed since")
                raise ValueError(f"{source}: the seal of its job does not vouch for it ({why}); verify the job")
            if seal.signatures:
                # A release counts only when signed by a key that signed the seal (see valid_job_releases).
                from httk.core.crypto import ed25519_public_key
                from httk.core.project.sealing import verify_signed_body

                from .projects import format_public_key, key_fingerprint

                sealed_by = verify_signed_body(seal.body_bytes, seal.body_sha256, seal.signatures).signers
                ours = {key_fingerprint(format_public_key(ed25519_public_key(seed))) for _role, seed in keys}
                if not ours & set(sealed_by):
                    raise ValueError(
                        f"{source}: the seal of its job is signed by {', '.join(sealed_by) or 'no verifying key'}, "
                        "and the workspace's seal.keys resolves none of those keys to sign its release record; "
                        "configure seal.keys with a key that signed the seal"
                    )
            size, sha256 = int(cast(int, recorded["size"])), str(recorded["sha256"])
        # The marker goes first: the scratch is then never discarded, and a mover finding this file's record sees
        # the move in flight (see _claim_release).
        name = f"{_MOVED_PREFIX}{hashlib.sha256(f'{ref.job_id}/{member.as_posix()}'.encode()).hexdigest()}.json"
        marker = payload.parent / name
        document = json.dumps({"from": str(origin / member), "to": str(moved_to)}).encode() + b"\n"
        try:
            os.close(_fs.create_exclusive(_fs.loc(marker), document, durable=workspace.durable))
        except FileExistsError as exc:
            raise ValueError(f"{source} is moved twice into one job") from exc
        try:
            if seal is not None:
                release = _claim_release(
                    workspace,
                    ref,
                    member,
                    source,
                    seal,
                    marker,
                    size=size,
                    sha256=sha256,
                    released_to={"job_key": job_key, "path": relative.as_posix()},
                    keys=keys,
                )
        except BaseException:
            _fs.remove_file(_fs.loc(marker), durable=workspace.durable)
            raise
        move = _InputMove(
            origin,
            directory,
            member.name,
            moved_to,
            {"path": relative.as_posix(), "size": size, "sha256": sha256},
            release,
            seal,
            marker,
        )
        try:
            _fs.move_owned(_fs.anchored(directory, member.name), _fs.loc(moved_to), durable=workspace.durable)
            # move_owned reports success once the source is gone, which another mover could also cause.
            if not _is_regular(_fs.loc(moved_to)):
                raise ValueError(_MOVE_REFUSAL.format(path=source))
        except BaseException as exc:
            # A source that vanished under us is not back: its marker (and so the scratch) is kept for the operator.
            if _undo_moves(workspace, [move], moved=_is_regular(_fs.loc(moved_to))):
                _drop_markers(workspace, [move])
            if isinstance(exc, _fs.CrossDevice):
                raise ValueError(f"{source} is on another filesystem; a cross-filesystem move is refused") from exc
            raise
        return move
    except BaseException:
        os.close(directory)
        raise


def _is_regular(target: _fs.Loc) -> bool:
    """Report whether *target* is a regular file, without following a symlink."""

    status = _fs.lstat(target)
    return status is not None and stat.S_ISREG(status.st_mode)


def _undo_moves(workspace: Workspace, moves: Iterable[_InputMove], *, moved: bool = True) -> bool:
    """Roll moves back, newest first, and report whether every file is back.

    Each file is renamed home, then its release record is withdrawn: only once
    the file is seen back home as a regular file, so a file that cannot be
    returned stays released (and the failure is logged). With *moved* false
    nothing was moved by this call, and the records are withdrawn; a file that
    is not home then vanished under the move, which is reported as not back.
    """

    from .seals import _withdraw_job_release

    returned = True
    for move in reversed(list(moves)):
        if moved:
            try:
                _fs.move_owned(
                    _fs.loc(move.moved_to),
                    _fs.anchored(move.directory, move.name),
                    durable=workspace.durable,
                    create_parents=False,
                )
            except (OSError, WorkflowError) as exc:
                _LOGGER.error("could not move %s back into %s as %s: %s", move.moved_to, move.origin, move.name, exc)
                returned = False
                continue
            if not _is_regular(_fs.anchored(move.directory, move.name)):
                _LOGGER.warning(
                    "%s is not back in %s as %s; its release record is kept", move.moved_to, move.origin, move.name
                )
                returned = False
                continue
        if move.release is not None:
            _withdraw_job_release(workspace, move.origin, move.release)
        if not moved and not _is_regular(_fs.anchored(move.directory, move.name)):
            returned = False
    return returned


def _drop_markers(workspace: Workspace, moves: Iterable[_InputMove]) -> None:
    """Remove the scratch markers of moves that completed or were rolled back."""

    for move in moves:
        if move.marker is not None:
            _fs.remove_file(_fs.loc(move.marker), durable=workspace.durable)


def _claim_release(
    workspace: Workspace,
    ref: _kernel.JobRef,
    member: PurePosixPath,
    source: str | os.PathLike[str],
    seal: Seal,
    marker: Path,
    *,
    size: int,
    sha256: str,
    released_to: Mapping[str, str],
    keys: tuple[SealKey, ...],
) -> Release:
    """Create the file's release record exclusively, replacing only a stale one; refuse otherwise.

    A record left by a move that crashed before its rename is stale: its file
    is still here, no job of the workspace has its key (an unreadable record
    names none), and no other submit scratch holds a marker for the same file
    (a move in flight).
    """

    from .models import parse_job_key
    from .seals import _release_name, job_releases_path, read_job_release, record_job_release

    def record(replace: bool) -> Release:
        return record_job_release(
            workspace,
            ref.path,
            path=member.as_posix(),
            size=size,
            sha256=sha256,
            released_to=released_to,
            keys=keys,
            seal=seal,
            replace=replace,
        )

    try:
        return record(False)
    except FileExistsError:
        pass
    except FormatError as exc:
        raise ValueError(f"{source}: cannot record its release: {exc}") from exc
    existing = read_job_release(ref.path, member.as_posix())
    shown = job_releases_path(ref.path) / _release_name(member.as_posix())
    named = existing.to_job_key if existing is not None else "an unreadable job key"
    try:
        receiver = None if existing is None else parse_job_key(existing.to_job_key)[1]
        # An unreadable record names no job.
        known = receiver is not None and _kernel.locate(workspace, receiver, placement_hint=None, exhaustive=True)
    except WorkflowError:
        known = False
    # A kept scratch of a dead owner (a move that may have renamed the file) keeps the path refused here until the
    # operator removes it; _finish_submit discards those whose file never moved.
    in_flight = any(path.parent != marker.parent for path in (workspace.control / "tmp").glob(f"*/{marker.name}"))
    if not known and not in_flight and _is_regular(_fs.loc(ref.path / member)):
        _LOGGER.warning("%s: replacing the stale release record %s naming job %s", source, shown, named)
        return record(True)
    raise ValueError(
        f"{source} is already released, or being moved, to job {named} (record {shown}); that record "
        "is safe to delete only when the file is present and that job does not exist"
    )


def _build_payload(
    workspace: Workspace,
    prepared: _Prepared,
    destination: Path,
    *,
    inputs: Mapping[str, object] | None,
    files: Mapping[str, str | os.PathLike[str]] | None,
    parameters: Mapping[str, object] | None,
    environment: Mapping[str, object] | None,
    tag: str | None,
    priority: int | None,
    name: str | None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    provenance: Mapping[str, object] | None = None,
    maxtime_cap: int | None = None,
    move_inputs: Mapping[str, str | os.PathLike[str]] | None = None,
    moves: list[_InputMove] | None = None,
    calls: Mapping[str, Mapping[str, object]] | None = None,
    persist_outputs: Mapping[str, str] | None = None,
) -> JobDefinition:
    """Stage one job payload into *destination* and write its ``job.json``.

    This is the shared body of :func:`new_jobs` and :func:`scaffold_job`:
    everything between an installed workflow and a written ``job.json``, with
    no submission of its own. *destination* is an existing empty directory the
    caller owns. *move_inputs* moves files out of succeeded jobs (only
    :func:`new_jobs` passes it, with *destination* the ``build/`` of a submit
    scratch); each move is appended to *moves* as it happens, so the caller can
    roll them back when the job is not submitted. *calls* is the job's call slot bindings
    (:func:`_bind_slots`); without them, a workflow's slots are bound to their installed defaults.
    *persist_outputs* is the ``persist_outputs`` member of a job called through a call slot.
    """

    workflow = prepared.workflow
    if calls is None:
        calls = _bind_slots(workspace, prepared.installed, None, None, owner=None, install=False)
    caller_tag = tag
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
    for input_name in move_inputs or {}:
        if input_name not in workflow.inputs:
            raise ValueError(
                f"unknown workflow input {input_name!r}; declared inputs: {', '.join(workflow.inputs) or 'none'}"
            )
        if input_name in (inputs or {}):
            raise ValueError(f"workflow input {input_name!r} is both supplied and moved from a job; choose one")
        if workflow.inputs.get(input_name) is None:
            names = ", ".join(name for name, member in workflow.inputs.items() if member is not None) or "none"
            raise ValueError(
                f"workflow input {input_name!r} has no destination to move a file to; inputs with one: {names}"
            )
    _stage_files(destination, user_files)
    for member, text in prepared.documents.items():
        document_path = destination.joinpath(*payload_relative(member).parts)
        document_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(text, bytes):
            document_path.write_bytes(text)
        else:
            document_path.write_text(text, encoding="utf-8")
    _stage_files(destination, prepared.files)
    supplied_inputs = dict(inputs or {})
    _stage_inputs(
        destination,
        workflow,
        supplied_inputs,
        instantiate=prepared.instantiate is not None or prepared.instantiate_exec is not None,
    )
    # The job id is chosen now, so a release record can name the receiving job before its file moves.
    job_id = str(uuid.uuid4()) if move_inputs else None
    keys = _release_keys(workspace) if move_inputs else ()
    for input_name, source in (move_inputs or {}).items():
        assert job_id is not None and moves is not None
        moves.append(
            _move_input(
                workspace,
                destination,
                payload_relative(str(workflow.inputs[input_name])),
                source,
                job_key=make_job_key(job_id, tag),
                keys=keys,
            )
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
                raise ValueError(f"workflow environment {environment_name!r} does not match type {environment_type!r}")
    reserved_parameters = set(prepared.parameters) | set(prepared.reserved_parameters)
    collisions = sorted(set(supplied_parameters) & reserved_parameters)
    if collisions:
        raise ValueError(f"user parameter collides with reserved language parameter {collisions[0]!r}")
    job_parameters = {**supplied_parameters, **prepared.parameters}
    declared_parameters = workflow.parameters
    if declared_parameters:
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
        _check_parameter_types(supplied_parameters, declared_parameters)
        declared_names = ", ".join(sorted(declared_parameters))
        logger = context_logger(_LOGGER, workflow.workflow_id)
        # A realization's caller options (``jobflow_overrides``) need no declaration.
        undeclared = set(supplied_parameters) - set(declared_parameters) - set(prepared.caller_parameters)
        for parameter_name in sorted(undeclared):
            logger.warning(
                "job parameter %r is not declared by this workflow; declared: %s",
                parameter_name,
                declared_names,
            )
    # Declared defaults stay out of the hook's parameters, so a hook can tell
    # "not given" from "defaulted"; it sees them separately and they fill in
    # after it for every name still absent.
    defaults = {name: metadata["default"] for name, metadata in declared_parameters.items() if "default" in metadata}
    declared_input_metadata = _declared_inputs(workflow)
    # A language workflow satisfies its own inputs — a document value, a
    # maker default, a supplied override — so the generic required check is
    # scoped to packaged and directory-hook workflows, whose destinations
    # this scaffold stages itself. It runs before any hook, so a missing
    # required input is refused without running package code.
    if workflow.language is None:
        for input_name, metadata in declared_input_metadata.items():
            if not metadata["required"]:
                continue
            input_destination = workflow.inputs.get(input_name)
            if input_destination is None:
                satisfied = input_name in supplied_inputs
            else:
                satisfied = destination.joinpath(*payload_relative(str(input_destination)).parts).exists()
            if not satisfied:
                declared_names = ", ".join(workflow.inputs) or "none"
                raise ValueError(
                    f"workflow input {input_name!r} is required and was not supplied; declared inputs: {declared_names}"
                )
    if prepared.instantiate_exec is not None:
        hook_inputs = _serialize_executable_inputs(destination, workflow, supplied_inputs)
        hook_parameters, hook_tag = _run_executable_instantiate(
            prepared.instantiate_exec,
            destination,
            workflow.workflow_id,
            tag,
            job_parameters,
            defaults,
            hook_inputs,
            workspace.root,
        )
        job_parameters = {**job_parameters, **hook_parameters}
        if caller_tag is None and hook_tag is not None:
            tag = hook_tag
    elif prepared.instantiate is not None:
        context = InstantiateContext(
            payload=destination,
            inputs=MappingProxyType(supplied_inputs),
            parameters=job_parameters,
            tag=tag,
            defaults=MappingProxyType(copy.deepcopy(defaults)),
        )
        prepared.instantiate(context)
        job_parameters = context.parameters
        tag = caller_tag if caller_tag is not None else context.tag
    if prepared.instantiate_exec is not None or prepared.instantiate is not None:
        # A hook's parameters meet the same declared types as supplied ones.
        _check_parameter_types(job_parameters, declared_parameters)
    for parameter_name, value in defaults.items():
        job_parameters.setdefault(parameter_name, value)
    declared_member: dict[str, object] = {}
    if declared_parameters:
        declared_member["parameters"] = {name: dict(metadata) for name, metadata in declared_parameters.items()}
    # Always written, even empty: set_workflow verifies a switch against the inputs the job was made with.
    declared_member["inputs"] = declared_input_metadata
    installed = prepared.installed
    spec = JobSpec(
        name=name or f"{installed.name}: {tag or 'job'}",
        workflow_id=installed.id,
        workflow_name=installed.name,
        initial_step=workflow.initial_step,
        tag=tag,
        placement=placement_text(normalize_placement(placement)),
        priority=500 if priority is None else priority,
        required_capabilities=prepared.required_capabilities,
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
        declarations=_merge_provenance_declaration(workflow.declarations, provenance),
        declared=declared_member,
        seal_succeeded=installed.provider().seal_succeeded,
        staged_files=_staged_entries(
            destination,
            [*user_files, *(member for member in workflow.inputs.values() if member is not None)],
            known=[move.entry for move in moves or ()],
        ),
        job_id=job_id,
        calls=calls,
        persist_outputs=persist_outputs,
    )
    if prepared.finalize is not None:
        spec = prepared.finalize(spec)
        if not isinstance(spec, JobSpec):
            raise ValueError("language finalize hook must return a JobSpec")
    if maxtime_cap is not None:
        resources, step_resources = cap_maxtime(spec.resources, spec.step_resources, maxtime_cap)
        spec = replace(spec, resources=resources, step_resources=step_resources)
    definition = prepare_job_payload(destination, spec)
    for move in moves or ():
        if move.release is not None and move.release.to_job_key != definition.job_key:
            # A hook retagged the job after its files moved: the record is replaced, naming the final key.
            from .seals import record_job_release

            assert move.seal is not None
            stale = move.release
            move.release = record_job_release(
                workspace,
                move.origin,
                path=stale.path,
                size=stale.size,
                sha256=stale.sha256,
                released_to={"job_key": definition.job_key, "path": stale.to_path},
                keys=keys,
                seal=move.seal,
                replace=True,
            )
    return definition


def _check_parameter_types(values: Mapping[str, object], declared: Mapping[str, Mapping[str, object]]) -> None:
    """Refuse a value whose declared ``[workflow.parameters.*] type`` it does not match."""

    from .packages import _matches_input_type

    for parameter_name, value in values.items():
        if parameter_name not in declared:
            continue
        parameter_type = declared[parameter_name].get("type")
        if isinstance(parameter_type, str) and not _matches_input_type(value, parameter_type):
            raise ValueError(
                f"workflow parameter {parameter_name!r} does not match type {parameter_type!r}; "
                f"got {type(value).__name__}. Supply a matching value — note that a command-line "
                f"NAME=VALUE parses VALUE as JSON when it can, so quote a literal string as "
                f'NAME=\'"text"\''
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
            _fs.remove_file(_fs.loc(destination.absolute()), durable=False)
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
    defaults: Mapping[str, object],
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
        "format_version": 3,
        "workflow": workflow_id,
        "tag": tag,
        "parameters": dict(parameters),
        "defaults": dict(defaults),
        "inputs": {name: dict(descriptor) for name, descriptor in inputs.items()},
    }
    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment["HTTK_WORKFLOW_WORKSPACE_DIR"] = str(workspace_root)
    # The same interpreter-first PATH a runner and describe get.
    environment["PATH"] = interpreter_first_path(environment.get("PATH"))
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


def _staged_entries(
    payload: Path, members: Iterable[str], *, known: Iterable[dict[str, object]] = ()
) -> tuple[dict[str, object], ...]:
    """Describe every staged file present in the payload by its path, size and SHA-256.

    This runs in ``job new``, before the job does, so the just-written files are trusted.
    A *known* entry (a moved file, described by its origin's seal) is taken as it is, never re-hashed.
    """

    entries: dict[str, dict[str, object]] = {str(entry["path"]): entry for entry in known}
    for member in members:
        relative = payload_relative(member)
        path = payload.joinpath(*relative.parts)
        if relative.as_posix() not in entries and path.is_file() and not path.is_symlink():
            entries[relative.as_posix()] = {
                "path": relative.as_posix(),
                "size": path.stat().st_size,
                "sha256": sha256_file(path),
            }
    return tuple(entries.values())


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
            if not declared and workflow.language is not None:
                from . import compat

                if compat.language(workflow.language).open_ports:
                    hint = (
                        f"; this {workflow.language} workflow declares no ports — "
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
        if part in {"", ".", ".."} or (index == 0 and _reserved_payload_member(part)):
            raise FormatError(f"a staged payload file name may not contain {part!r}: {name!r}")
    return relative
