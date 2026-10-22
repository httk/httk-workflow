"""Run one provider-owned postprocess script against one collected job.

Fields: a manifest script name, workspace/job identifiers, an output directory,
the process return code, and captured stdout/stderr.

Postprocess output never lands in the job payload: it is written under a
workspace-level ``postprocess/`` tree, so a finished job may be sealed
immediately and still be postprocessed later without disturbing the bytes the
seal covers. The output root is ``<workspace.root>/postprocess/`` by default,
the workspace setting ``postprocess.directory`` when set (an absolute path
outside the workspace), or a per-invocation override (a relative path
resolves against the current directory; it must also land outside the
workspace); the per-job directory below it mirrors payload
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

Every completed run (any exit status) leaves a receipt
``<output_dir>/.httk-postprocess.json`` naming the script, the workflow, the
job, the run times, the exit status, and the script's declared ``outputs``
patterns as they were at run time. A stale receipt is removed before launch,
so a timeout or launch failure leaves none. :func:`collect_postprocess` reads
the receipts (it needs no provider) and describes each job's declared output
files as file records; it collects only from the workspace's own
``postprocess/`` tree, never from a ``postprocess.directory`` elsewhere or a
per-invocation override. The output directory is reused between runs, so a
glob pattern also matches stale files an earlier run left that a later run did
not rewrite.
"""

from __future__ import annotations

import importlib.resources
import json
import os
import subprocess
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from httk.core.digests import sha256_file

from . import _fs
from ._util import interpreter_first_path, json_bytes, utc_now
from .models import POSTPROCESS_DIRECTORY

if TYPE_CHECKING:
    from httk.core import FileRecord

    from .collecting import JobRecord
    from .scaffold import ResolvedWorkflow, WorkflowProvider
    from .workspace import Workspace

DEFAULT_POSTPROCESS_TIMEOUT: float = 3600.0

#: The name and format of the receipt every completed postprocess run leaves in its output directory.
RECEIPT_NAME = ".httk-postprocess.json"
RECEIPT_FORMAT = "httk-workflow-postprocess-receipt"
_GLOB_CHARACTERS = frozenset("*?[")


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
    receipt: Path


@dataclass(frozen=True)
class CollectedPostprocess:
    """The declared output files of one job's postprocess run, read from its receipt.

    At most one of *skipped* and *error* is set; when either is, *files* is empty.

    :param script: Name the postprocess script.
    :param record: Carry the job the script ran on.
    :param files: Pair each output-relative POSIX path with its file record, sorted by path.
    :param workflow_id: Identify the workflow the script came from, as the receipt records it.
    :param started_at: Record when the script started.
    :param finished_at: Record when the script exited.
    :param skipped: Explain why there is benignly nothing to collect.
    :param error: Explain why collecting this job's postprocess output failed.
    """

    script: str
    record: JobRecord
    files: tuple[tuple[str, FileRecord], ...] = ()
    workflow_id: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    skipped: str | None = None
    error: str | None = None


def _valid_output_pattern(pattern: str) -> bool:
    """Return whether *pattern* is a declarable ``outputs`` entry (the manifest and receipt rule)."""

    if any(ord(character) < 32 or character == "\x7f" for character in pattern):
        return False
    return "\\" not in pattern and all(
        part and not part.startswith(".") and ("**" not in part or part == "**") for part in pattern.split("/")
    )


def _default_root(workspace_root: Path) -> Path:
    """Return the built-in default postprocess root for one workspace root."""

    return workspace_root / POSTPROCESS_DIRECTORY


def _reject_reserved_root(workspace: Workspace, base: Path) -> None:
    """Refuse a postprocess root inside the control dir or the jobs directory (every job payload lives there)."""

    resolved = base.resolve()
    if resolved.is_relative_to(workspace.control.resolve()):
        raise ValueError(f"postprocess output root must not be inside the workspace control directory: {base}")
    if resolved.is_relative_to(workspace.jobs.resolve()):
        raise ValueError(f"postprocess output root must not be inside a job payload: {base}")


def postprocess_root(workspace: Workspace, override: str | None = None) -> Path:
    """Resolve the output root all this workspace's postprocess output lands in.

    Precedence is the per-invocation *override*, then the ``postprocess.directory``
    application setting, then ``<workspace.root>/postprocess``. The setting must be
    an absolute path outside the workspace; a relative *override* resolves against
    the current directory and must also land outside the workspace.

    :param workspace: The workspace whose postprocess output root to resolve.
    :param override: A per-invocation output root that wins over the setting.
    :return: The resolved absolute output root.
    :raises ValueError: If the root is inside the workspace (other than the default), or the setting is relative.
    """

    raw = override
    if raw is None:
        setting = workspace.read_settings().get("postprocess.directory")
        if not setting:
            return _default_root(workspace.root)
        raw = str(setting)
        if not Path(raw).is_absolute():
            raise ValueError(
                f"postprocess.directory must be an absolute path outside the workspace, got {raw!r} "
                f"(unset it to use the default {_default_root(workspace.root)})"
            )
    base = Path(raw).absolute()
    _reject_reserved_root(workspace, base)  # specific messages first; symlinked placements can lead outside
    if base.resolve().is_relative_to(workspace.root.resolve()):
        raise ValueError(
            f"postprocess output root must be outside the workspace: {base} "
            f"(the default {_default_root(workspace.root)} applies when none is set)"
        )
    return base


def _check_script_name(name: str) -> None:
    if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise ValueError(
            "postprocess script name must be a nonempty single path component "
            "without '/' or '\\' (the manifest table-name rule)"
        )


def _script_path(resolved: ResolvedWorkflow | WorkflowProvider, name: str) -> Path:
    _check_script_name(name)
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


def _output_dir(record: JobRecord, name: str, output_root: Path) -> Path:
    """Resolve and create the per-job output directory, refusing a symlink escape."""

    root = output_root
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
    :raises ValueError: If the script cannot be resolved or run, or times out;
        neither leaves a receipt.
    """

    script = _script_path(resolved, name)
    if not os.access(script, os.X_OK):
        raise ValueError(f"postprocess script {script} is not executable; chmod +x and give it a shebang line")
    root = _default_root(Path(record.workspace_root)) if output_root is None else output_root
    output_dir = _output_dir(record, name, root)

    environment = dict(os.environ)
    for variable in tuple(environment):
        if variable.startswith("HTTK_WORKFLOW_"):
            environment.pop(variable)
    environment.update(
        {
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(record.workspace_root),
            "HTTK_WORKFLOW_JOB_DIR": str(record.payload),
            "HTTK_WORKFLOW_POSTPROCESS_DIR": str(output_dir),
            # The same interpreter-first PATH a runner and describe get.
            "PATH": interpreter_first_path(environment.get("PATH")),
        }
    )
    if record.workdir is not None:
        environment["HTTK_WORKFLOW_WORKDIR"] = str(record.workdir)
    if record.data is not None:
        environment["HTTK_WORKFLOW_DATA_DIR"] = str(record.data)
    receipt = output_dir / RECEIPT_NAME
    try:
        _fs.remove_file(_fs.loc(receipt), durable=True)
    except OSError as exc:
        raise ValueError(f"cannot remove the stale postprocess receipt {receipt}: {exc}") from exc
    started_at = utc_now()
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
    outputs: Any = resolved.postprocess_scripts[name].get("outputs")
    document = {
        "format": RECEIPT_FORMAT,
        "format_version": 1,
        "script": name,
        "workflow_id": resolved.workflow_id,
        "workspace_id": record.workspace_id,
        "job_id": record.job_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "returncode": completed.returncode,
        "outputs": None if outputs is None else list(outputs),
    }
    try:
        _fs.write_file(_fs.loc(receipt), json_bytes(document) + b"\n", durable=True)
    except OSError as exc:
        raise ValueError(f"cannot write the postprocess receipt {receipt}: {exc}") from exc
    return PostprocessResult(
        script=name,
        workspace_id=record.workspace_id,
        job_id=record.job_id,
        output_dir=output_dir,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        receipt=receipt,
    )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"timestamp must be a string, got {value!r}")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no time zone: {value!r}")
    return parsed


def _read_receipt(path: Path) -> tuple[bytes, dict[str, Any]]:
    """Return the receipt's bytes and validated document; raise ``ValueError`` when it is unusable."""

    data = path.read_bytes()
    try:
        document = json.loads(data)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"postprocess receipt {path} is not JSON: {exc}") from exc
    if (
        not isinstance(document, dict)
        or document.get("format") != RECEIPT_FORMAT
        or document.get("format_version") != 1
    ):
        raise ValueError(f"postprocess receipt {path} is not a {RECEIPT_FORMAT} version 1 document")
    returncode = document.get("returncode")
    outputs = document.get("outputs")
    workflow_id = document.get("workflow_id")
    if (
        not isinstance(returncode, int)
        or isinstance(returncode, bool)
        or not (workflow_id is None or isinstance(workflow_id, str))
        or not (
            outputs is None
            or (
                isinstance(outputs, list)
                and outputs
                and all(isinstance(pattern, str) and _valid_output_pattern(pattern) for pattern in outputs)
            )
        )
    ):
        raise ValueError(f"postprocess receipt {path} has an invalid returncode, workflow_id or outputs")
    try:
        document["started_at"] = _timestamp(document.get("started_at"))
        document["finished_at"] = _timestamp(document.get("finished_at"))
    except ValueError as exc:
        raise ValueError(f"postprocess receipt {path} has an invalid time: {exc}") from exc
    return data, document


def _output_file(output_dir: Path, relative: PurePosixPath) -> Path | None:
    """Return the regular file at *relative* below *output_dir*, or ``None`` when it must not be collected.

    A dot-prefixed component, a symlink in any component, or a path resolving
    outside *output_dir* excludes it.
    """

    if any(part.startswith(".") for part in relative.parts):
        return None
    path = output_dir.joinpath(*relative.parts)
    if any(output_dir.joinpath(*relative.parts[:index]).is_symlink() for index in range(1, len(relative.parts) + 1)):
        return None
    if not path.is_file() or not path.resolve().is_relative_to(output_dir.resolve()):
        return None
    return path


def _collect_one(
    record: JobRecord, script: str, root: Path, workspace_root: Path, url_prefix: str
) -> CollectedPostprocess:
    from httk.core import FileRecord, reader_format_for

    output_dir = root.joinpath(*record.placement.parts, record.job_key, script)
    receipt = output_dir / RECEIPT_NAME
    if output_dir.is_symlink() or (output_dir.exists() and not output_dir.resolve().is_relative_to(root.resolve())):
        return CollectedPostprocess(script, record, error=f"postprocess output directory is unsafe: {output_dir}")
    if not receipt.exists():
        return CollectedPostprocess(script, record, skipped=f"not postprocessed with {script}")
    try:
        data, document = _read_receipt(receipt)
    except (OSError, ValueError) as exc:
        return CollectedPostprocess(script, record, error=str(exc))
    times = {
        "workflow_id": document["workflow_id"],
        "started_at": document["started_at"],
        "finished_at": document["finished_at"],
    }
    if document["returncode"] != 0:
        reason = f"postprocess {script} failed (exit {document['returncode']})"
        return CollectedPostprocess(script, record, skipped=reason, **times)
    if document["outputs"] is None:
        return CollectedPostprocess(script, record, skipped=f"{script} declares no outputs", **times)
    matched: dict[str, Path] = {}
    try:
        for pattern in document["outputs"]:
            if _GLOB_CHARACTERS.isdisjoint(pattern):
                path = _output_file(output_dir, PurePosixPath(pattern))
                if path is None:
                    error = (
                        f"required postprocess output {output_dir / pattern} is missing, "
                        "not a regular file, or reached through a symlink"
                    )
                    return CollectedPostprocess(script, record, error=error, **times)
                matched[pattern] = path
                continue
            for candidate in output_dir.glob(pattern):
                relative = PurePosixPath(candidate.relative_to(output_dir).as_posix())
                if (path := _output_file(output_dir, relative)) is not None:
                    matched[relative.as_posix()] = path
        if not matched:
            return CollectedPostprocess(script, record, skipped="no output files", **times)
        files = tuple(
            (
                relative,
                FileRecord(
                    url=f"{url_prefix}/{path.relative_to(workspace_root).as_posix()}",
                    name=path.name,
                    size=path.stat().st_size,
                    sha256=sha256_file(path),
                    format=reader_format_for(path.name),
                ),
            )
            for relative, path in sorted(matched.items())
        )
        unchanged = receipt.read_bytes() == data
    except (OSError, ValueError) as exc:
        return CollectedPostprocess(script, record, error=f"cannot read postprocess output: {exc}", **times)
    if not unchanged:
        return CollectedPostprocess(
            script, record, error="postprocess output changed during collection; rerun collect", **times
        )
    return CollectedPostprocess(script, record, files=files, **times)


def collect_postprocess(
    workspace: Workspace,
    script: str,
    *,
    states: Iterable[str] = ("succeeded",),
    placement: str | PurePosixPath | None = None,
    jobs: Iterable[str] | None = None,
    on_skipped: Callable[[str], None] | None = None,
) -> Iterator[CollectedPostprocess]:
    """Describe the declared output files of *script*'s last run on each selected job.

    Jobs are selected exactly as :func:`~httk.workflow.collecting.job_records`
    selects them. Each job's receipt decides what is collected: no receipt, a
    failed run, a script declaring no ``outputs``, or nothing matching skip the
    job; an unusable receipt, a missing literal (required) output, or a receipt
    rewritten while the files were hashed make it an error. Files reached
    through a symlink, or with a dot-prefixed component (the receipt itself
    among them), are never collected. File URLs have the form
    ``data/<workspace name>/<workspace-relative path>`` like job files; nothing
    is read from the provider and nothing is created.

    :param workspace: Read jobs and their postprocess output from this workspace.
    :param script: Name the postprocess script whose output to collect.
    :param states: Select the stopped state kinds to report.
    :param placement: Restrict results to this placement and its descendants.
    :param jobs: Collect only these jobs and their descendants.
    :param on_skipped: Receive the job key of every selected job dropped for an
        unreadable ``job.json``, forwarded to ``job_records``.
    :return: A lazy iterator with one item per selected readable job.
    :raises ValueError: If *script* is not a single path component, or the
        postprocess root is not inside the workspace (a ``postprocess.directory``
        setting elsewhere).
    """

    from .collecting import _workspace_name, job_records

    _check_script_name(script)
    root = postprocess_root(workspace, None)
    workspace_root = workspace.root
    if not root.resolve().is_relative_to(workspace_root.resolve()):
        raise ValueError(
            f"postprocess output can only be collected from the workspace's own {POSTPROCESS_DIRECTORY}/ "
            f"directory, not {root}: unset postprocess.directory and rerun the script"
        )
    url_prefix = f"data/{_workspace_name(workspace)}"
    records = job_records(workspace, states=states, placement=placement, on_skipped=on_skipped, jobs=jobs)
    return (_collect_one(record, script, root, workspace_root, url_prefix) for record in records)


__all__ = [
    "DEFAULT_POSTPROCESS_TIMEOUT",
    "CollectedPostprocess",
    "PostprocessResult",
    "collect_postprocess",
    "postprocess_root",
    "run_postprocess_script",
]
