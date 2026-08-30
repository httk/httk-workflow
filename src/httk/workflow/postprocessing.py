"""Run one provider-owned postprocess script against one collected job.

Fields: a manifest script name, workspace/job identifiers, an output directory,
the process return code, and captured stdout/stderr.

Postprocess output never lands in the job payload: it is written under a
workspace-level ``postprocess/`` tree, so a finished job may be sealed
immediately and still be postprocessed later without disturbing the bytes the
seal covers. The output root is ``<workspace.root>/postprocess/`` by default,
the workspace setting ``postprocess.directory`` when set (a relative path
resolves against the workspace root, an absolute path is used as given), or a
per-invocation override; the per-job directory below it mirrors payload
placement as ``<root>/<placement>/<job_key>/<script name>/``.

The script runs with its cwd set to that per-job output directory and receives
``HTTK_WORKFLOW_WORKSPACE_DIR`` and ``HTTK_WORKFLOW_JOB_DIR`` for the workspace
and the immutable payload (read-only from the script's perspective),
``HTTK_WORKFLOW_POSTPROCESS_DIR`` for the output directory, and
``HTTK_WORKFLOW_WORKDIR`` / ``HTTK_WORKFLOW_DATA_DIR`` for the collected job's
result directories when those exist. Those are the only variables exported from
the reserved ``HTTK_WORKFLOW_`` namespace; all others in it are removed before
launch. Scripts resolve only from the registered provider or an explicit
package directory; job payloads and pinned workflow trees are never trusted as
script sources.
"""

from __future__ import annotations

import importlib.resources
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .collecting import JobRecord
    from .scaffold import ResolvedWorkflow, WorkflowProvider
    from .workspace import Workspace

DEFAULT_POSTPROCESS_TIMEOUT: float = 3600.0

#: The workspace-relative default output root, used when no setting or override
#: names one.
DEFAULT_POSTPROCESS_DIRNAME = "postprocess"


@dataclass(frozen=True)
class PostprocessResult:
    """The captured result of one postprocess invocation."""

    script: str
    workspace_id: str
    job_id: str
    output_dir: Path
    returncode: int
    stdout: str
    stderr: str


def postprocess_root(workspace: Workspace, override: str | None = None) -> Path:
    """Resolve the output root all this workspace's postprocess output lands in.

    Precedence is the per-invocation *override*, then the ``postprocess.directory``
    application setting, then ``<workspace.root>/postprocess``. A relative value
    resolves against the workspace root; an absolute value is used as given.

    :param workspace: The workspace whose postprocess output root to resolve.
    :param override: A per-invocation output root that wins over the setting.
    :return: The resolved absolute output root.
    """

    raw = override
    if raw is None:
        setting = workspace.read_settings().get("postprocess.directory")
        raw = str(setting) if setting else DEFAULT_POSTPROCESS_DIRNAME
    base = Path(raw)
    return base if base.is_absolute() else workspace.root / base


def _script_path(resolved: ResolvedWorkflow | WorkflowProvider, name: str) -> Path:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError(
            "postprocess script name must be a nonempty single path component "
            "without '/' or '\\' (the manifest table-name rule)"
        )
    scripts = resolved.postprocess_scripts
    if name not in scripts:
        declared = ", ".join(scripts) or "declares no postprocess scripts"
        raise ValueError(f"unknown postprocess script {name!r}; declared scripts: {declared}")
    file = scripts[name].get("file")
    if not isinstance(file, str):  # pragma: no cover - manifests validate this
        raise ValueError(f"postprocess script {name!r} has no file")
    member = PurePosixPath(file)
    if member.is_absolute() or any(part in {"", ".", ".."} for part in member.parts):
        raise ValueError(f"postprocess script {name!r} member must be relative and contain no '.' or '..': {file!r}")
    directory = getattr(resolved, "directory", None)
    if directory is not None:
        root = Path(directory).resolve()
    else:
        package = getattr(resolved, "runner_package", None)
        if package is None:
            raise ValueError(f"postprocess script {name!r} has no trusted package source")
        root = Path(str(importlib.resources.files(package))).resolve()
    script = root.joinpath(*member.parts)
    resolved_script = script.resolve()
    if not resolved_script.is_relative_to(root):
        raise ValueError(f"postprocess script {name!r} member resolves outside provider root: {file!r}")
    if script.is_symlink() or any(
        root.joinpath(*member.parts[:index]).is_symlink() for index in range(1, len(member.parts))
    ):
        raise ValueError(f"postprocess script {name!r} member must not be a symlink: {file!r}")
    return script


def _output_dir(record: JobRecord, name: str, output_root: Path | None) -> Path:
    """Resolve and create the per-job output directory, refusing a symlink escape."""

    root = Path(record.workspace_root) / DEFAULT_POSTPROCESS_DIRNAME if output_root is None else output_root
    output_dir = root.joinpath(*record.placement.parts, record.job_key, name)
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    if output_dir.is_symlink() or (output_dir.exists() and not output_dir.is_dir()):
        raise ValueError(f"postprocess output directory is unsafe: {output_dir}")
    output_dir.mkdir(exist_ok=True)
    if output_dir.is_symlink() or not output_dir.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"postprocess output directory is unsafe: {output_dir}")
    return output_dir


def run_postprocess_script(
    resolved: ResolvedWorkflow | WorkflowProvider,
    name: str,
    record: JobRecord,
    *,
    output_root: Path | None = None,
    timeout: float | None = DEFAULT_POSTPROCESS_TIMEOUT,
) -> PostprocessResult:
    """Run provider script *name* in the collected job's postprocess directory.

    :param resolved: Resolve the script from a registered provider or package.
    :param name: Select the manifest ``[workflow.postprocess.<name>]`` entry.
    :param record: Supply the collected job and its result paths.
    :param output_root: The output root; defaults to ``<workspace.root>/postprocess``.
    :param timeout: Limit the subprocess, or wait indefinitely when ``None``.
    :return: Captured process result, including nonzero return codes.
    :raises ValueError: If the script cannot be resolved or run.
    """

    script = _script_path(resolved, name)
    if not os.access(script, os.X_OK):
        raise ValueError(f"postprocess script {script} is not executable; chmod +x and give it a shebang line")
    output_dir = _output_dir(record, name, output_root)

    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment.update(
        {
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(record.workspace_root),
            "HTTK_WORKFLOW_JOB_DIR": str(record.payload),
            "HTTK_WORKFLOW_POSTPROCESS_DIR": str(output_dir),
        }
    )
    if record.workdir is not None:
        environment["HTTK_WORKFLOW_WORKDIR"] = str(record.workdir)
    if record.data is not None:
        environment["HTTK_WORKFLOW_DATA_DIR"] = str(record.data)
    try:
        completed = subprocess.run(
            [str(script)],
            cwd=output_dir,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError(f"cannot run postprocess script {script} for job {record.job_id}: {exc}") from exc
    return PostprocessResult(
        script=name,
        workspace_id=record.workspace_id,
        job_id=record.job_id,
        output_dir=output_dir,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


__all__ = [
    "DEFAULT_POSTPROCESS_DIRNAME",
    "DEFAULT_POSTPROCESS_TIMEOUT",
    "PostprocessResult",
    "postprocess_root",
    "run_postprocess_script",
]
