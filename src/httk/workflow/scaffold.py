"""Scaffolding submitted jobs from a template, some files, and some inputs.

A job is a payload directory plus a ``job.json`` that names the runner to execute,
and building one by hand means knowing the runner's workflow name, its initial
step, its digest, and where its inputs live in the payload. This module is the
short way: :func:`new_job` takes a *template* — a packaged runner a domain
registered by name, or the path of a runner file of your own — stages the files
the runner reads, writes the ``job.json``, and submits the result, all in one
call.

Packaged templates are not known to this module. A domain or compat engine
registers each one it ships with :func:`~httk.workflow.scaffold.register_template`, supplying only the
generic description of a starting point — its name, the runner it starts from,
the workflow and steps that runner declares, the modes a job of it defaults to,
and what it does — so the scaffold resolves and pins a template without ever
importing the science that owns it.

.. code-block:: python

    from httk.workflow import Workspace
    from httk.workflow.scaffold import new_job

    workspace = Workspace.initialize("workflow-workspace", extensions=["transactional-data-v1"])
    job = new_job(workspace, "some-template", files={"input": "input"}, tag="example")
    print(job.job_key, job.payload)

By default the runner file is *published into the workspace runner store*, and the
job references it there by digest. That is what makes a scaffolded job durable:
the bytes that will run are pinned in the workspace, so upgrading the installed
*httk-workflow* underneath a queued campaign cannot change what its jobs execute.
Publication is content addressed — the store name carries the digest of the
bytes — so scaffolding the same template twice publishes nothing the second time,
and a later, different version of a packaged runner lands beside the old one
instead of replacing it. ``publish="installed"`` instead references a packaged
template through the reserved ``pkg:`` form, which copies nothing at all.

:func:`new_jobs` is the same operation for a campaign: one template resolution and
one publication amortized over every job, and a lazy iterator over the results, so
generating a hundred million jobs costs one payload and one marker each and never
materializes a list of them.
"""

import importlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Literal, TypedDict, cast

from ._util import sha256_file
from .errors import FormatError, UnsupportedExtensionError
from .models import (
    ATTEMPT_CONTROL_PREFIX,
    JOB_STATE_DIRECTORY,
    normalize_placement,
    validate_inputs,
)
from .runtime_builders import JobSpec, prepare_job_payload
from .workspace import Workspace

__all__ = [
    "JOB_SCAFFOLD_FORMAT",
    "DEFAULT_PLACEMENT",
    "FILES_DIRECTORY",
    "STRUCTURE_PATTERNS",
    "JobItem",
    "JobTemplate",
    "TemplateProvider",
    "ScaffoldedJob",
    "describe_runner",
    "register_template",
    "registered_templates",
    "template_provider",
    "packaged_template",
    "resolve_template",
    "structure_files",
    "structure_tag",
    "new_job",
    "new_jobs",
    "payload_relative",
]

#: The format of the machine-readable report :meth:`ScaffoldedJob.as_mapping` returns.
JOB_SCAFFOLD_FORMAT = "httk-workflow-job-scaffold"
#: Where a scaffolded job is placed when nothing else is asked for.
DEFAULT_PLACEMENT = "jobs"
#: The payload directory a staged file lands in when its name has no directory of
#: its own. Every packaged runner reads its inputs from there.
FILES_DIRECTORY = "files"
#: The file names ``--from`` recognizes as structures when it is given a directory.
STRUCTURE_PATTERNS = ("POSCAR*", "*.vasp")
_DESCRIBE_VARIABLE = "HTTK_WORKFLOW_DESCRIBE"
_DESCRIBE_TIMEOUT = 120.0
_TAG_CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789._-"
_MAXIMUM_TAG_LENGTH = 48

type DataMode = Literal["none", "transactional"]
type WorkdirMode = Literal["persistent", "isolated"]
type PublishMode = Literal["workspace", "installed"]


@dataclass(frozen=True)
class TemplateProvider:
    """One packaged template a domain or compat engine offers by name.

    A provider is the generic description of a starting point. It names the
    packaged runner it starts from by the package the runner file is a module of
    and the file beside that module — so the reserved ``pkg:`` form and the
    digest are resolved from the provider alone — and it declares the runner's
    workflow, its steps, the modes a job of it defaults to, and what it does.
    Declaring the steps here rather than running the runner to ask keeps
    scaffolding cheap; the owning domain's tests hold the declaration to what the
    runner really describes.
    """

    name: str
    runner_package: str
    runner_file: str
    workflow: str
    initial_step: str
    steps: tuple[str, ...] = ()
    data_mode: DataMode = "none"
    workdir_mode: WorkdirMode = "persistent"
    summary: str = ""


#: Every registered template, keyed by name in registration order. The scaffold
#: ships this empty: a domain populates it as an import side effect, so the
#: generic layer resolves a domain template without importing the domain.
_TEMPLATE_PROVIDERS: dict[str, TemplateProvider] = {}


def register_template(provider: TemplateProvider) -> None:
    """Register one packaged template, replacing any registered under its name.

    A domain calls this once per template it ships when its package is imported,
    which is how ``httk workflow job new --template NAME`` resolves a packaged
    runner the generic scaffold never names.
    """

    _TEMPLATE_PROVIDERS[provider.name] = provider


def registered_templates() -> tuple[str, ...]:
    """Return the name of every registered template, in registration order."""

    return tuple(_TEMPLATE_PROVIDERS)


def template_provider(name: str) -> TemplateProvider | None:
    """Return the provider *name* names, by template name or runner file."""

    provider = _TEMPLATE_PROVIDERS.get(name)
    if provider is not None:
        return provider
    for candidate in _TEMPLATE_PROVIDERS.values():
        if candidate.runner_file == name:
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
    tag: str | None
    name: str
    placement: str | PurePosixPath
    priority: int | None


@dataclass(frozen=True)
class JobTemplate:
    """One resolved starting point for a job: a runner file and how to run it.

    A template is either one of the runners a domain registered by name — see
    :func:`~httk.workflow.scaffold.registered_templates` — or a runner file of your own, which is
    described by running it — every native runner answers ``--describe`` with its
    workflow and its steps — so a scaffolded job never guesses either.
    """

    name: str
    source: Path
    workflow: str
    initial_step: str
    steps: tuple[str, ...] = ()
    data_mode: DataMode = "none"
    workdir_mode: WorkdirMode = "persistent"
    packaged: str | None = None
    summary: str = ""

    @property
    def store_name(self) -> str:
        """The content-addressed name this template takes in a runner store.

        The digest of the bytes is part of the name, so publishing is idempotent
        for identical bytes and never overwrites a name a submitted job pinned:
        an upgraded packaged runner is published beside the version its queued
        jobs still reference.
        """

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
    """One job this module submitted, and everything needed to look at it again."""

    job_id: str
    job_key: str
    tag: str | None
    placement: PurePosixPath
    payload: Path
    marker: Path
    workflow: str
    initial_step: str
    template: str
    runner: Mapping[str, object]

    def as_mapping(self) -> dict[str, object]:
        """Return the machine-readable report of this job."""

        return {
            "format": JOB_SCAFFOLD_FORMAT,
            "format_version": 1,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "tag": self.tag,
            "placement": self.placement.as_posix(),
            "payload_path": str(self.payload),
            "marker_path": str(self.marker),
            "workflow": self.workflow,
            "initial_step": self.initial_step,
            "template": self.template,
            "runner": dict(self.runner),
        }


@dataclass(frozen=True)
class _Prepared:
    """A template whose runner is resolved once for every job that will use it."""

    template: JobTemplate
    runner_source: Literal["workspace", "installed"]
    runner_path: str
    runner_sha256: str
    data_mode: DataMode


def describe_runner(runner: str | os.PathLike[str]) -> dict[str, object]:
    """Return the self-description one runner file prints, by running it.

    Every native runner — Python or Bash — answers ``HTTK_WORKFLOW_DESCRIBE=1``
    with its workflow name and its registered steps and exits without touching
    anything, which is how a runner nobody wrote a template for is still
    scaffolded without being told what it implements.
    """

    path = Path(runner).expanduser()
    if not path.is_file():
        raise ValueError(f"a runner template must be an existing file: {path}")
    environment = dict(os.environ)
    environment[_DESCRIBE_VARIABLE] = "1"
    shell = Path(__file__).with_name("shell")
    environment["HTTK_WORKFLOW_BASH_API"] = str(shell / "httk-workflow.sh")
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
    return {"workflow": described["workflow"], "steps": [str(step) for step in steps]}


def _packaged_runner_path(provider: TemplateProvider) -> Path:
    """Return the installed runner file one provider names, without importing it.

    The runner package is imported by name so resolving one template never drags
    another domain's helpers into the process, and the file is taken from beside
    that package's module.
    """

    module = importlib.import_module(provider.runner_package)
    location = getattr(module, "__file__", None)
    if location is None:  # pragma: no cover - only a namespace package has none
        raise ValueError(f"runner package {provider.runner_package} has no installed location")
    return Path(location).with_name(provider.runner_file)


def _packaged_runner_reference(provider: TemplateProvider) -> dict[str, object]:
    """Return the installed ``runner`` member a provider's ``pkg:`` form pins."""

    path = _packaged_runner_path(provider)
    return {
        "backend": "path",
        "source": "installed",
        "path": f"pkg:{provider.runner_package}/{PurePosixPath(provider.runner_file)}",
        "sha256": sha256_file(path),
        "arguments": [],
    }


def packaged_template(name: str) -> JobTemplate | None:
    """Return the registered template *name* names, or ``None``."""

    provider = template_provider(name)
    if provider is None:
        return None
    return JobTemplate(
        name=provider.name,
        source=_packaged_runner_path(provider),
        workflow=provider.workflow,
        initial_step=provider.initial_step,
        steps=provider.steps,
        data_mode=provider.data_mode,
        workdir_mode=provider.workdir_mode,
        packaged=provider.runner_file,
        summary=provider.summary,
    )


def resolve_template(
    template: str | os.PathLike[str],
    *,
    workflow: str | None = None,
    step: str | None = None,
    data_mode: DataMode | None = None,
) -> JobTemplate:
    """Return the :class:`JobTemplate` *template* names.

    *template* is the name of a packaged template, the file name of a packaged
    runner, or the path of a runner file. A runner file is described by running
    it, so its workflow name and its steps come from the runner itself; *workflow*
    and *step* override what it said, and *step* is required when a runner
    registers several steps and none of them is ``start``.
    """

    text = os.fspath(template)
    resolved = packaged_template(text)
    if resolved is None:
        path = Path(text).expanduser()
        if not path.is_file():
            known = ", ".join(registered_templates()) or "none registered"
            raise ValueError(
                f"unknown template {text!r}: it is neither a registered template "
                f"({known}) nor an existing runner file"
            )
        described = describe_runner(path)
        steps = tuple(cast(list[str], described["steps"]))
        resolved = JobTemplate(
            name=path.name,
            source=path.resolve(),
            workflow=str(described["workflow"]),
            initial_step=_initial_step(path, steps, step),
            steps=steps,
            summary=f"the runner {path}",
        )
    if workflow is not None:
        resolved = replace(resolved, workflow=workflow)
    if step is not None:
        if resolved.steps and step not in resolved.steps:
            raise ValueError(
                f"the runner {resolved.source} does not implement the step {step!r}; "
                f"its steps: {', '.join(resolved.steps)}"
            )
        resolved = replace(resolved, initial_step=step)
    if data_mode is not None:
        resolved = replace(resolved, data_mode=data_mode)
    return resolved


def _initial_step(path: Path, steps: tuple[str, ...], requested: str | None) -> str:
    """Return the step a job of this runner starts at."""

    if requested is not None:
        return requested
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
    template: str | os.PathLike[str],
    *,
    inputs: Mapping[str, object] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    tag: str | None = None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    priority: int | None = None,
    workdir_mode: WorkdirMode = "persistent",
    data_mode: DataMode | None = None,
    publish: PublishMode = "workspace",
    step: str | None = None,
    workflow: str | None = None,
    name: str | None = None,
) -> ScaffoldedJob:
    """Scaffold, submit, and describe one job of *template*.

    *template* is a registered template name — see :func:`~httk.workflow.scaffold.registered_templates` —
    or the path of a runner file. *files* maps payload names to the files to stage
    there: a bare name lands in the payload's :data:`~httk.workflow.scaffold.FILES_DIRECTORY`, which is
    where a packaged runner reads its inputs, and a name with a directory in
    it is used verbatim. *inputs* becomes the job's ``inputs`` object, whose
    members are documented per runner.

    *data_mode* defaults to what the template needs — ``transactional`` for a
    template whose runner publishes collected results, and ``none`` for a runner
    that said nothing. *publish* ``workspace`` publishes the runner
    file into the workspace runner store and pins its digest; ``installed``
    references a packaged runner through the reserved ``pkg:`` form instead and
    copies nothing.
    """

    prepared = _prepare(workspace, template, publish=publish, step=step, workflow=workflow, data_mode=data_mode)
    return _submit(
        workspace,
        prepared,
        inputs=inputs,
        files=files,
        tag=tag,
        placement=placement,
        priority=priority,
        workdir_mode=workdir_mode,
        name=name,
    )


def new_jobs(
    workspace: Workspace,
    template: str | os.PathLike[str],
    items: Iterable[JobItem],
    *,
    inputs: Mapping[str, object] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    tag: str | None = None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    priority: int | None = None,
    workdir_mode: WorkdirMode = "persistent",
    data_mode: DataMode | None = None,
    publish: PublishMode = "workspace",
    step: str | None = None,
    workflow: str | None = None,
    name: str | None = None,
) -> Iterator[ScaffoldedJob]:
    """Scaffold and submit one job per member of *items*, lazily.

    Every keyword is the shared value of the whole campaign, and every member of
    one :class:`~httk.workflow.scaffold.JobItem` is what that job varies: ``inputs`` and ``files`` are
    merged over the shared mappings, and ``tag``, ``name``, ``placement``, and
    ``priority`` replace the shared value.

    This is the pattern for a campaign of any size. The template is resolved once
    and its runner published once, however many jobs follow, so every job costs
    exactly one payload directory and one state marker; *items* is consumed as an
    iterator and the results are yielded as they are submitted, so a generator of
    a hundred million structures is turned into a hundred million jobs without
    either side of the loop ever being materialized.

    .. code-block:: python

        def structures():
            for path in sorted(Path("structures").glob("POSCAR.*")):
                yield {"files": {"POSCAR": path}, "tag": structure_tag(path)}

        for job in new_jobs(workspace, "some-template", structures(), inputs={"kpoint_density": 30.0}):
            print(job.job_key)
    """

    prepared = _prepare(workspace, template, publish=publish, step=step, workflow=workflow, data_mode=data_mode)
    for item in items:
        yield _submit(
            workspace,
            prepared,
            inputs={**(inputs or {}), **item.get("inputs", {})},
            files={**(files or {}), **item.get("files", {})},
            tag=item.get("tag", tag),
            placement=item.get("placement", placement),
            priority=item.get("priority", priority),
            workdir_mode=workdir_mode,
            name=item.get("name", name),
        )


def _prepare(
    workspace: Workspace,
    template: str | os.PathLike[str],
    *,
    publish: PublishMode,
    step: str | None,
    workflow: str | None,
    data_mode: DataMode | None,
) -> _Prepared:
    """Resolve one template and make its runner referenceable, exactly once."""

    resolved = resolve_template(template, workflow=workflow, step=step, data_mode=data_mode)
    if resolved.data_mode == "transactional" and "transactional-data-v1" not in workspace.extensions:
        raise UnsupportedExtensionError(
            "publishing the results of a job as transactional data needs the transactional-data-v1 "
            f"extension, which {workspace.root} does not have; initialize a workspace with "
            "--extension transactional-data-v1, or scaffold with data_mode='none' to leave the "
            "results in the job's workdir"
        )
    if publish == "installed":
        provider = template_provider(resolved.name)
        if resolved.packaged is None or provider is None:
            raise ValueError(
                f"publish='installed' references a packaged template, but {resolved.source} is a runner "
                "file of your own; publish it into the workspace instead (the default), or install it on "
                "a runner search path and write its job.json yourself"
            )
        reference = _packaged_runner_reference(provider)
    else:
        reference = workspace.publish_runner(resolved.source, name=resolved.store_name)
    return _Prepared(
        template=resolved,
        runner_source=cast(Literal["workspace", "installed"], str(reference["source"])),
        runner_path=str(reference["path"]),
        runner_sha256=str(reference["sha256"]),
        data_mode=resolved.data_mode,
    )


def _submit(
    workspace: Workspace,
    prepared: _Prepared,
    *,
    inputs: Mapping[str, object] | None,
    files: Mapping[str, str | os.PathLike[str]] | None,
    tag: str | None,
    placement: str | PurePosixPath,
    priority: int | None,
    workdir_mode: WorkdirMode,
    name: str | None,
) -> ScaffoldedJob:
    """Build one payload below the workspace and publish it as a submitted job."""

    template = prepared.template
    normalized = normalize_placement(placement)
    # The payload is built inside the workspace's own scratch directory, so
    # submitting it is a rename on one filesystem rather than a copy, whatever
    # the size of the files it stages.
    staging = workspace.control / "tmp" / f"scaffold.{uuid.uuid4()}"
    try:
        staging.mkdir(parents=True, exist_ok=False)
        _stage_files(staging, files or {})
        job = prepare_job_payload(
            staging,
            JobSpec(
                name=name or f"{template.name}: {tag or 'job'}",
                workflow=template.workflow,
                runner_path=prepared.runner_path,
                runner_source=prepared.runner_source,
                runner_sha256=prepared.runner_sha256,
                initial_step=template.initial_step,
                tag=tag,
                workdir_mode=workdir_mode,
                data_mode=prepared.data_mode,
                priority=500 if priority is None else priority,
                inputs=validate_inputs(inputs or {}),
            ),
        )
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
        template=template.name,
        runner={
            "source": prepared.runner_source,
            "path": prepared.runner_path,
            "sha256": prepared.runner_sha256,
        },
    )


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


def payload_relative(name: str) -> PurePosixPath:
    """Return where one staged file lands inside a payload.

    A bare name lands in :data:`~httk.workflow.scaffold.FILES_DIRECTORY`, so ``POSCAR`` becomes
    ``files/POSCAR`` — where the packaged runners read it — and a name that
    carries a directory of its own is used exactly as it is written.
    """

    text = str(name).strip()
    if not text:
        raise ValueError("a staged payload file needs a name")
    relative = PurePosixPath(text)
    if relative.is_absolute():
        raise ValueError(f"a staged payload file name must be relative: {name!r}")
    if len(relative.parts) == 1:
        relative = PurePosixPath(FILES_DIRECTORY) / relative
    for part in relative.parts:
        if part in {"", ".", "..", "job.json", JOB_STATE_DIRECTORY} or part.startswith(ATTEMPT_CONTROL_PREFIX):
            raise FormatError(f"a staged payload file name may not contain {part!r}: {name!r}")
    return relative
