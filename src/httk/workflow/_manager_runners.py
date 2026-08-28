"""Private runner resolution and digest verification helpers."""

import hashlib
import os
import stat
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.digests import sha256_file, tree_digest

from .errors import FormatError, RunnerResolutionError
from .models import JobDefinition, parse_package_runner

RUNNER_TREE_ENTRY = "run"


@dataclass(frozen=True)
class VerifiedRunner:
    """Describe a shared runner verified for one launch."""

    path: Path
    root: Path
    sha256: str
    fd: int | None
    artifacts: Path | None


def runner_module_allowed(module: str, runner_modules: Sequence[str] = ("httk.workflow",)) -> bool:
    """Return whether a packaged runner module is in the manager allowlist."""

    return any(module == allowed or module.startswith(f"{allowed}.") for allowed in runner_modules)


def contained(root: Path, parts: Sequence[str]) -> Path | None:
    candidate = root.joinpath(*parts)
    try:
        resolved_root = root.resolve()
        resolved = candidate.resolve()
    except OSError:
        return None
    return candidate if resolved.is_relative_to(resolved_root) else None


def resolve_package_runner(module: str, resource: PurePosixPath, runner_modules: Sequence[str]) -> Path:
    if not runner_module_allowed(module, runner_modules):
        raise RunnerResolutionError(
            "runner_unavailable",
            f"runner module {module} is not in this manager's runner module allowlist "
            f"({', '.join(runner_modules) or 'empty'})",
        )
    try:
        root = Path(str(files(module)))
    except (ImportError, TypeError, ValueError) as exc:
        raise RunnerResolutionError("runner_unavailable", f"runner module {module} is not importable: {exc}") from exc
    candidate = contained(root, resource.parts)
    if candidate is None or not candidate.exists():
        raise RunnerResolutionError(
            "runner_unavailable", f"installed runner pkg:{module}/{resource.as_posix()} does not exist"
        )
    return candidate


def resolve_shared_runner(manager: Any, job: JobDefinition) -> Path:
    return resolve_runner_reference(
        manager.workspace,
        job,
        runner_search_paths=manager.runner_search_paths,
        runner_modules=manager.runner_modules,
    )


def resolve_runner_reference(
    workspace: Any,
    job: JobDefinition,
    *,
    runner_search_paths: Iterable[str | Path] = (),
    runner_modules: Sequence[str] = ("httk.workflow",),
) -> Path:
    """Resolve one shared runner without changing workspace state.

    :param workspace: Workspace whose runner store is searched.
    :param job: Job definition carrying the runner reference.
    :param runner_search_paths: Roots for non-package installed runners.
    :param runner_modules: Allowed package prefixes.
    :return: The resolved runner file or tree.
    """

    if job.runner_source == "workspace":
        try:
            candidate = workspace.runner_store_path(job.runner_path)
        except FormatError as exc:
            raise RunnerResolutionError("runner_unavailable", str(exc)) from exc
        if not candidate.exists():
            raise RunnerResolutionError(
                "runner_unavailable",
                f"workspace runner {job.runner_path.as_posix()} is not published in {workspace.runners}",
            )
        return candidate
    package = parse_package_runner(job.runner_path.as_posix())
    if package is not None:
        return resolve_package_runner(*package, runner_modules)
    for root_value in runner_search_paths:
        root = Path(root_value)
        installed = contained(root, job.runner_path.parts)
        if installed is not None and installed.exists():
            return installed
    searched = ", ".join(str(path) for path in runner_search_paths) or "no configured search path"
    raise RunnerResolutionError(
        "runner_unavailable", f"installed runner {job.runner_path.as_posix()} was not found in {searched}"
    )


def check_runner_reference(
    workspace: Any,
    job: JobDefinition,
    *,
    runner_search_paths: Iterable[str | Path] = (),
    runner_modules: Sequence[str] = ("httk.workflow",),
    tree_entry: str = RUNNER_TREE_ENTRY,
    placement: PurePosixPath | None = None,
) -> str | None:
    """Return a runner-reference problem, or ``None`` when it is sound.

    :param workspace: Workspace whose runner store is searched.
    :param job: Job definition carrying the runner reference.
    :param runner_search_paths: Roots for non-package installed runners.
    :param runner_modules: Allowed package prefixes.
    :param tree_entry: Executable entry point for a runner tree.
    :param placement: Payload placement when checking a payload runner.
    :return: A human-readable problem, or ``None``.
    """

    try:
        if job.runner_source == "payload":
            if placement is None:
                return "payload runner check needs the job placement"
            candidate = workspace.payload_path(placement, job.job_key)
            candidate = candidate.joinpath(*job.runner_path.parts)
        else:
            candidate = resolve_runner_reference(
                workspace,
                job,
                runner_search_paths=runner_search_paths,
                runner_modules=runner_modules,
            )
        if candidate.is_dir():
            executable = candidate / tree_entry
            if not executable.is_file():
                return f"runner tree {job.runner_path.as_posix()} has no {tree_entry} entry point"
            if not os.access(executable, os.X_OK):
                return "runner is not executable"
            actual = tree_digest(candidate)
        elif candidate.is_file():
            if not os.access(candidate, os.X_OK):
                return "runner is not executable"
            actual = sha256_file(candidate)
        else:
            return f"runner is not a regular file or directory: {candidate}"
        if job.runner_sha256 is not None and actual != job.runner_sha256:
            return f"runner digest {actual} does not match pinned {job.runner_sha256}"
    except (FormatError, OSError, RunnerResolutionError, ValueError) as exc:
        return str(exc)
    return None


def _hash_fd(fd: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while chunk := os.pread(fd, 1024 * 1024, offset):
        digest.update(chunk)
        offset += len(chunk)
    return digest.hexdigest()


def _close_fd(fd: int | None) -> None:
    if fd is not None:
        try:
            os.close(fd)
        except OSError:
            pass


def _registered_runner_artifacts(manager: Any, job: JobDefinition, source: Path) -> Path | None:
    if job.runner_source != "workspace" or not source.is_dir():
        return None
    from ._runner_builds import platform_tag, registered_artifacts, workspace_build_command
    from .packages import read_build_spec

    try:
        spec = read_build_spec(source)
        if spec is None:
            return None
        tag = platform_tag(spec)
        artifacts = registered_artifacts(
            manager.workspace,
            job.runner_path,
            tag,
            expected_source_sha256=job.runner_sha256,
        )
        if artifacts is None:
            raise RunnerResolutionError(
                "runner_not_built",
                f"workflow package {job.runner_path.as_posix()} is not built on this machine for "
                f"platform {tag}; run: {workspace_build_command(manager.workspace, job.runner_path)}",
            )
        return artifacts
    except ValueError as exc:
        raise RunnerResolutionError("runner_unavailable", f"published runner manifest is malformed: {exc}") from exc
    except OSError as exc:
        raise RunnerResolutionError("runner_unavailable", f"cannot locate runner artifacts: {exc}") from exc


def verify_runner(manager: Any, job: JobDefinition) -> VerifiedRunner:
    """Resolve and verify a shared runner without modifying its source tree.

    File runners are hashed through an open descriptor retained until launch;
    tree runners are hashed in place and consequently retain a small accepted
    TOCTOU window between tree verification and execution.
    """

    source = resolve_shared_runner(manager, job)
    if source.is_dir():
        try:
            digest = tree_digest(source)
        except (OSError, ValueError) as exc:
            raise RunnerResolutionError("runner_unavailable", f"cannot pin runner {source}: {exc}") from exc
        if digest != job.runner_sha256:
            raise RunnerResolutionError(
                "runner_mismatch",
                f"{job.runner_source} runner {job.runner_path.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}",
            )
        executable = source / RUNNER_TREE_ENTRY
        if not executable.is_file():
            raise RunnerResolutionError(
                "runner_unavailable",
                f"runner tree {job.runner_path.as_posix()} has no {RUNNER_TREE_ENTRY} entry point",
            )
        if not os.access(executable, os.X_OK):
            raise RunnerResolutionError("runner_unavailable", "runner is not executable")
        return VerifiedRunner(
            source / RUNNER_TREE_ENTRY, source, digest, None, _registered_runner_artifacts(manager, job, source)
        )

    fd: int | None = None
    try:
        fd = os.open(source, os.O_RDONLY | os.O_CLOEXEC)
        descriptor_stat = os.fstat(fd)
        if not stat.S_ISREG(descriptor_stat.st_mode):
            raise RunnerResolutionError("runner_unavailable", f"runner is not a regular file: {source}")
        digest = _hash_fd(fd)
        path_stat = os.stat(source)
        if (descriptor_stat.st_ino, descriptor_stat.st_dev) != (path_stat.st_ino, path_stat.st_dev):
            raise RunnerResolutionError("runner_unavailable", "runner replaced during verification")
        if not os.access(source, os.X_OK):
            raise RunnerResolutionError("runner_unavailable", "runner is not executable")
        if digest != job.runner_sha256:
            raise RunnerResolutionError(
                "runner_mismatch",
                f"{job.runner_source} runner {job.runner_path.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}",
            )
        return VerifiedRunner(source, source, digest, fd, None)
    except RunnerResolutionError:
        _close_fd(fd)
        raise
    except (OSError, ValueError) as exc:
        _close_fd(fd)
        raise RunnerResolutionError("runner_unavailable", f"cannot verify runner {source}: {exc}") from exc
