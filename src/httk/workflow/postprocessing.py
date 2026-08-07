"""Run one provider-owned postprocess script against one collected job.

Fields: a manifest script name, workspace/job identifiers, an output directory,
the process return code, and captured stdout/stderr.

The script receives ``HTTK_WORKFLOW_WORKSPACE_DIR`` and
``HTTK_WORKFLOW_JOB_DIR`` for the workspace and immutable payload, plus
``HTTK_WORKFLOW_WORKDIR`` and (when present) ``HTTK_WORKFLOW_DATA_DIR`` for the
collected job's result directories. Those are exactly the four variables
exported from the reserved ``HTTK_WORKFLOW_`` namespace; all other variables in
that namespace are removed before launch. Scripts resolve only from the
registered provider or an explicit package directory; job payloads and pinned
workflow trees are never trusted as script sources.
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

DEFAULT_POSTPROCESS_TIMEOUT: float = 3600.0


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


def run_postprocess_script(
    resolved: ResolvedWorkflow | WorkflowProvider,
    name: str,
    record: JobRecord,
    *,
    timeout: float | None = DEFAULT_POSTPROCESS_TIMEOUT,
) -> PostprocessResult:
    """Run provider script *name* in the collected job's postprocess directory.

    :param resolved: Resolve the script from a registered provider or package.
    :param name: Select the manifest ``[workflow.postprocess.<name>]`` entry.
    :param record: Supply the collected job and its result paths.
    :param timeout: Limit the subprocess, or wait indefinitely when ``None``.
    :return: Captured process result, including nonzero return codes.
    :raises ValueError: If the script cannot be resolved or run.
    """

    script = _script_path(resolved, name)
    if not os.access(script, os.X_OK):
        raise ValueError(f"postprocess script {script} is not executable; chmod +x and give it a shebang line")
    workdir = record.workdir
    if workdir is None:
        raise ValueError("job has no workdir; nothing to postprocess")
    output_dir = workdir / "postprocess" / name
    resolved_workdir = workdir.resolve()
    if output_dir.is_symlink() or not output_dir.parent.resolve().is_relative_to(resolved_workdir):
        raise ValueError(f"postprocess output directory is unsafe: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_dir.is_symlink() or not output_dir.resolve().is_relative_to(resolved_workdir):
        raise ValueError(f"postprocess output directory is unsafe: {output_dir}")

    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment.update(
        {
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(record.workspace_root),
            "HTTK_WORKFLOW_JOB_DIR": str(record.payload),
            "HTTK_WORKFLOW_WORKDIR": str(workdir),
        }
    )
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


__all__ = ["DEFAULT_POSTPROCESS_TIMEOUT", "PostprocessResult", "run_postprocess_script"]
