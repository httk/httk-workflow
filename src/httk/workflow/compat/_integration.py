"""Shared tail for importing workflows written in another language.

This module is the compat-internal home the language importers share. Two
languages are understood, each living in its own consumer package under
:mod:`httk.workflow.compat`:

``pwd``
    The `Python Workflow Definition
    <https://github.com/pythonworkflow/python-workflow-definition>`_ exchange
    format: a JSON graph of Python function nodes, literal input nodes and named
    output nodes. :func:`httk.workflow.compat.pwd.import_pwd` turns one
    document into one job that runs the packaged ``pwd_runner.py``.
``cwl``
    The `Common Workflow Language <https://www.commonwl.org/>`_, supported as a
    workflow *language* only. :func:`httk.workflow.compat.cwl.import_cwl`
    parses and normalizes a document with *cwl-utils* and turns it into one job
    that runs the packaged ``cwl_runner.py``. **cwltool is neither used, bundled
    nor invoked**: the normalized document is executed entirely on *httk₂*'s own
    runner and manager machinery, so a CWL workflow claims, retries, checkpoints,
    spawns children and journals exactly like every other job of a workspace.

Both importers are *one way*. An imported job is a normal job whose payload
happens to contain the document it came from; nothing translates a job back into
the language it was written in, and nothing keeps an imported job in sync with a
document that changes afterwards. Import again to pick up a new revision.

Neither importer writes a runner file per workflow. Each references the runner
it ships beside its own package through the reserved installed form —
``pkg:httk.workflow.compat.pwd/pwd_runner.py`` and
``pkg:httk.workflow.compat.cwl/cwl_runner.py`` — which the manager resolves
inside its own module allowlist, whose default ``httk.workflow`` covers those
packages. The document is data in the payload; the program that executes it is
the same installed bytes for every imported job.

:func:`runner_reference` builds the ``runner`` member of one ``job.json`` for
those packaged runners, and :func:`submit_integration_job` is the small shared
tail both importers end in: stage some files below one payload, write its
``job.json``, submit it, and describe the result as a
:class:`~httk.workflow.scaffold.ScaffoldedJob`.
"""

import os
import shutil
import uuid
from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Literal

from httk.workflow import Workspace
from httk.workflow._util import sha256_file
from httk.workflow.protocol import (
    JobSpec,
    normalize_placement,
    prepare_job_payload,
    validate_inputs,
)
from httk.workflow.scaffold import (
    DEFAULT_PLACEMENT,
    FILES_DIRECTORY,
    ScaffoldedJob,
    payload_relative,
)

__all__ = [
    "DEFAULT_PLACEMENT",
    "FILES_DIRECTORY",
    "ScaffoldedJob",
    "runner_path",
    "runner_reference",
    "submit_integration_job",
]


def runner_path(package: str, name: str) -> Path:
    """Return the installed file of one packaged compat runner.

    *package* is the importable package the runner file lives beside — its own
    consumer package, e.g. ``httk.workflow.compat.cwl`` — so a compat engine
    resolves and digest-pins the exact bytes it ships rather than a runner owned
    by some shared module.
    """

    return Path(str(files(package).joinpath(name)))


def runner_reference(package: str, name: str) -> dict[str, object]:
    """Return the ``runner`` member of a ``job.json`` running one packaged runner.

    The reserved ``pkg:`` form names the runner inside its own consumer package,
    and the digest is taken from the installed bytes, which is exactly what the
    manager verifies before it stages and executes them.
    """

    path = runner_path(package, name)
    return {
        "executor": "path",
        "source": "installed",
        "path": f"pkg:{package}/{PurePosixPath(name)}",
        "sha256": sha256_file(path),
        "arguments": [],
    }


def submit_integration_job(
    workspace: Workspace,
    *,
    runner_package: str,
    runner: str,
    workflow: str,
    initial_step: str,
    name: str,
    inputs: Mapping[str, object],
    documents: Mapping[str, str] | None = None,
    files: Mapping[str, str | os.PathLike[str]] | None = None,
    tag: str | None = None,
    placement: str | PurePosixPath = DEFAULT_PLACEMENT,
    priority: int | None = None,
    data_mode: Literal["none", "transactional"] = "none",
    workdir_mode: Literal["persistent", "isolated"] = "persistent",
    required_capabilities: tuple[str, ...] = (),
    maximum_attempts_per_activation: int | None = None,
) -> ScaffoldedJob:
    """Build one payload for a packaged integration runner and submit it.

    *documents* are written verbatim into the payload and *files* are copied
    into it, both under the naming rule of
    :func:`~httk.workflow.scaffold.payload_relative`: a bare name lands in
    ``files/`` and a name carrying a directory is used as written. The payload is
    built inside the workspace's own scratch directory, so submitting it is a
    rename rather than a copy however large the staged inputs are.
    """

    normalized = normalize_placement(placement)
    reference = runner_reference(runner_package, runner)
    staging = workspace.control / "tmp" / f"import.{uuid.uuid4()}"
    try:
        staging.mkdir(parents=True, exist_ok=False)
        for entry, text in (documents or {}).items():
            destination = staging.joinpath(*payload_relative(entry).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(text, encoding="utf-8")
        for entry, source in (files or {}).items():
            path = Path(source).expanduser()
            if not path.is_file():
                raise ValueError(f"the file staged as {entry!r} does not exist: {path}")
            destination = staging.joinpath(*payload_relative(entry).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, destination)
        job = prepare_job_payload(
            staging,
            JobSpec(
                name=name,
                workflow=workflow,
                runner_path=str(reference["path"]),
                runner_source="installed",
                runner_sha256=str(reference["sha256"]),
                initial_step=initial_step,
                tag=tag,
                workdir_mode=workdir_mode,
                data_mode=data_mode,
                priority=500 if priority is None else priority,
                required_capabilities=required_capabilities,
                maximum_attempts_per_activation=maximum_attempts_per_activation,
                inputs=validate_inputs(dict(inputs)),
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
        runner={
            "source": "installed",
            "path": str(reference["path"]),
            "sha256": str(reference["sha256"]),
        },
    )
