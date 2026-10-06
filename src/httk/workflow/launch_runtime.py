"""Packaged dispatcher for manager launcher bundles."""

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._allocation import argv_allocation, parse_allocation_spec
from ._confine import is_override_key
from ._daemon_policy import DEFAULT_SLURM_EXPORT, SLURM_EXPORT_SETTING, validate_slurm_export
from .launchers import _manager_command

BATCH_DIRECTORY = "logs/batch"
BATCH_DIRECTIVES = (
    ("slurm.account", "--account"),
    ("slurm.partition", "--partition"),
    ("slurm.time_limit", "--time"),
    ("slurm.nodes", "--nodes"),
    ("slurm.cpus_per_task", "--cpus-per-task"),
    ("slurm.ntasks", "--ntasks"),
    ("slurm.ntasks_per_node", "--ntasks-per-node"),
    ("slurm.mem", "--mem"),
    ("slurm.gres", "--gres"),
    ("slurm.reservation", "--reservation"),
)
SUPPORTED_KINDS = ("slurm",)
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")
_SUBMITTED_JOB = re.compile(r"Submitted batch job (\d+)")


def _result(operation: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "format": "httk-manager-launcher-result",
                "format_version": 2,
                "operation": operation,
                "ok": True,
                **values,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _refusal(operation: str, message: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "error": message,
                "format": "httk-manager-launcher-result",
                "format_version": 2,
                "operation": operation,
                "ok": False,
                **values,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _metadata(request: Mapping[str, object]) -> dict[str, Any]:
    value = request.get("launcher_dir")
    if not isinstance(value, str) or not value:
        raise ValueError("launcher request must carry launcher_dir")
    path = Path(value).expanduser() / "launcher.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"launcher JSON must be an object: {path}")
    return document


def _text(settings: Mapping[str, object], key: str) -> str | None:
    value = settings.get(key)
    if value is None or isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    text = str(value).strip()
    return text or None


def _batch_value(key: str, value: str) -> str:
    if _CONTROL_CHARACTER.search(value):
        raise ValueError(f"launcher setting {key} must not contain control characters")
    return value


class _PartialSubmissionError(RuntimeError):
    """Identify a Slurm submission failure after earlier jobs were accepted."""

    def __init__(self, message: str, job_ids: Sequence[str]) -> None:
        self.job_ids = list(job_ids)
        super().__init__(f"{message}; submitted: {len(self.job_ids)}; job_ids: {self.job_ids}")


def _request_argv(request: Mapping[str, object]) -> list[str]:
    value = request.get("argv")
    if not isinstance(value, list) or not value or not all(isinstance(item, str) for item in value):
        raise ValueError("start argv must be a nonempty string array")
    return list(value)


def _workspace(request: Mapping[str, object]) -> str:
    value = request.get("workspace")
    if not isinstance(value, str) or not value:
        raise ValueError("start needs a workspace path in the request")
    return value


def _count(request: Mapping[str, object]) -> int:
    value = request.get("count", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("start count must be a positive integer")
    return value


def _settings(request: Mapping[str, object]) -> dict[str, object]:
    value = request.get("settings", {})
    if not isinstance(value, Mapping):
        raise ValueError("start settings must be an object")
    return dict(value)


def _launcher_settings(request: Mapping[str, object]) -> dict[str, object]:
    """Read settings packaged with the resolved launcher bundle."""

    value = request.get("launcher_settings", {})
    if not isinstance(value, Mapping):
        raise ValueError("start launcher_settings must be an object")
    return dict(value)


def _manager_argv(
    argv: Sequence[str], settings: Mapping[str, object], launcher_settings: Mapping[str, object] | None = None
) -> list[str]:
    """Add configured manager workers and allocation probe when the caller supplied none.

    The launcher bundle's confinement settings are pinned on the manager with ``--setting``, appended
    last so they win over any forwarded command-line value; workspace settings are read live instead.
    """

    result = list(argv)
    if "--workers" not in result and "manager.workers" in settings:
        value = _text(settings, "manager.workers")
        if value is None or not value.isdigit() or int(value) < 1:
            raise ValueError("launcher setting manager.workers must be a positive integer")
        result += ["--workers", value]
    if argv_allocation(result) is None:
        result += ["--allocation", parse_allocation_spec(_text(settings, "manager.allocation") or "slurm")]
    for key, pin in sorted((launcher_settings or {}).items()):
        if not is_override_key(key) or pin is None:
            continue
        if isinstance(pin, bool) or not isinstance(pin, (str, int, float)):
            raise ValueError(f"launcher setting {key} must be a string or number")
        result += ["--setting", f"{key}={pin}"]
    return result


def _prelude_argv(argv: Sequence[str], settings: Mapping[str, object]) -> list[str]:
    """Use the post-prelude ``httk`` command for a canonical manager argv."""

    command = _manager_command(settings)
    if _text(settings, "environment.prelude") is None:
        return list(argv)
    # An isolated interpreter (-I) is equally canonical: the prelude decides which httk runs instead.
    for prefix in (["-m", "httk.core.cli"], ["-I", "-m", "httk.core.cli"]):
        if list(argv[1 : 1 + len(prefix)]) == prefix:
            return [command, *argv[1 + len(prefix) :]]
    return list(argv)


def _batch_script(argv: Sequence[str], *, settings: Mapping[str, object], workspace: str, directory: str | None) -> str:
    """Compose the Slurm script submitted for one manager.

    Without a batch ``directory`` the script names neither the job nor its output: the submitter passes
    them on the ``sbatch`` command line.
    """

    lines = ["#!/bin/bash -l", "# Generated by the httk workflow slurm launcher."]
    if directory is not None:
        lines.append("#SBATCH --job-name=httk-manager")
    for key, directive in BATCH_DIRECTIVES:
        value = _text(settings, key)
        if value is not None:
            lines.append(f"#SBATCH {directive}={shlex.quote(_batch_value(key, value))}")
    lines.append(f"#SBATCH --chdir={shlex.quote(_batch_value('workspace', workspace))}")
    if directory is not None:
        lines += [
            f"#SBATCH --output={shlex.quote(_batch_value('directory', directory + '/manager-%j.out'))}",
            f"#SBATCH --error={shlex.quote(_batch_value('directory', directory + '/manager-%j.err'))}",
        ]
    lines += ["", "set -e"]
    prelude = _text(settings, "environment.prelude")
    if prelude:
        lines.append(prelude)
    lines += [f"exec {shlex.join(_prelude_argv(argv, settings))}", ""]
    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class SubmissionIdentity:
    """Trusted scheduler identity of one manager submitted for the workspace daemon.

    :param job_name: Slurm job name, which status, accounting and cancellation filter on.
    :param sbatch: The approved ``sbatch`` executable.
    :param cluster: The fixed Slurm cluster.
    :param output: Slurm output path pattern, outside the workspace; standard error goes there too.
    """

    job_name: str
    sbatch: Path
    cluster: str
    output: str


def slurm_submission(
    *, settings: Mapping[str, object], argv: Sequence[str], workspace: str, identity: SubmissionIdentity
) -> tuple[list[str], str]:
    """Return the ``sbatch`` command and the batch script that submit one manager under a trusted identity.

    The script is the one :func:`main` writes for an ordinary ``slurm`` launcher start, built from the same
    ``settings``, except that the job name and output come from ``identity`` on the command line. The caller
    passes the script on standard input, so nothing is written into the workspace.

    :param settings: The launcher settings, which are also pinned on the manager.
    :param argv: The manager command before the launcher's additions.
    :param workspace: The real workspace path, the job's working directory.
    :param identity: The trusted scheduler identity.
    :return: The ``sbatch`` argument vector and the script text.
    :raises ValueError: If a setting cannot be expressed in the script.
    """

    script = _batch_script(
        _manager_argv(argv, settings, settings), settings=settings, workspace=workspace, directory=None
    )
    # Defensive revalidation: never place an unvalidated string into the sbatch argv (argv-injection
    # surface). An unset slurm.export resolves to the default; a present one must be NONE or NIL.
    export = settings.get(SLURM_EXPORT_SETTING)
    export_value = DEFAULT_SLURM_EXPORT if export is None else validate_slurm_export(export)
    command = [
        str(identity.sbatch),
        "--parsable",
        f"--export={export_value}",
        "--no-requeue",
        "--input=/dev/null",
        f"--clusters={identity.cluster}",
        f"--job-name={identity.job_name}",
        f"--output={identity.output}",
    ]
    return command, script


def _start_slurm(request: Mapping[str, object]) -> None:
    workspace = _workspace(request)
    settings = {**_settings(request), **_launcher_settings(request)}
    count = _count(request)
    directory = Path(workspace) / BATCH_DIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    script_path = directory / f"manager-{uuid.uuid4().hex}.sbatch"
    script_path.write_text(
        _batch_script(
            _manager_argv(_request_argv(request), settings, _launcher_settings(request)),
            settings=settings,
            workspace=workspace,
            directory=str(directory),
        ),
        encoding="utf-8",
    )
    os.chmod(script_path, 0o700)
    job_ids: list[str] = []
    for _ in range(count):
        try:
            completed = subprocess.run(
                ["sbatch", str(script_path)], cwd=workspace, text=True, capture_output=True, check=False
            )
        except OSError as exc:
            raise _PartialSubmissionError(f"sbatch failed: {exc}", job_ids) from exc
        if completed.returncode != 0:
            raise _PartialSubmissionError(
                completed.stderr.strip() or f"sbatch exited with status {completed.returncode}", job_ids
            )
        match = _SUBMITTED_JOB.search(completed.stdout)
        if match is None:
            raise _PartialSubmissionError(
                completed.stderr.strip() or f"sbatch did not report a submitted job: {completed.stdout.strip()}",
                job_ids,
            )
        job_ids.append(match.group(1))
    _result("start", kind="slurm", count=count, job_ids=job_ids, script=str(script_path))


def main(argv: list[str] | None = None) -> int:
    """Dispatch one launcher request file.

    :param argv: Dispatcher arguments, or process arguments when omitted.
    :return: Zero after writing a launcher result document.
    """

    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("launcher dispatcher expects one REQUEST.json path", file=sys.stderr)
        return 2
    try:
        request = json.loads(Path(arguments[0]).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"launcher dispatcher: {exc}", file=sys.stderr)
        return 2
    operation = request.get("operation") if isinstance(request, dict) else None
    if not isinstance(operation, str) or operation not in {"check", "start"}:
        print("launcher request must carry operation check or start", file=sys.stderr)
        return 2
    try:
        document = _metadata(request)
        kind = document.get("kind")
        if kind not in SUPPORTED_KINDS:
            _refusal(
                operation,
                f"launcher kind {kind!r} is not implemented; the packaged launchers support {', '.join(SUPPORTED_KINDS)} - refusing",
            )
            return 0
        if operation == "check":
            binaries = document.get("required_binaries", [])
            if not isinstance(binaries, Sequence) or isinstance(binaries, (str, bytes)):
                raise ValueError("required_binaries must be an array")
            missing = [binary for binary in binaries if not isinstance(binary, str) or not shutil.which(binary)]
            if missing:
                raise RuntimeError(
                    f"required launcher binary is unavailable: {', '.join(str(item) for item in missing)}"
                )
            _result("check", kind=kind)
        else:
            _start_slurm(request)
    except _PartialSubmissionError as exc:
        _refusal(operation, str(exc), submitted=len(exc.job_ids), job_ids=exc.job_ids)
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        _refusal(operation, f"launcher {operation}: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
