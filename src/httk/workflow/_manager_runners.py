"""Private runner resolution, staging, and digest verification helpers."""

import shutil
from collections.abc import Sequence
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from .errors import FormatError, RunnerResolutionError
from .models import JobDefinition, parse_package_runner

RUNNER_TREE_ENTRY = "run"


def contained(root: Path, parts: Sequence[str]) -> Path | None:
    candidate = root.joinpath(*parts)
    try:
        resolved_root = root.resolve()
        resolved = candidate.resolve()
    except OSError:
        return None
    return candidate if resolved.is_relative_to(resolved_root) else None


def resolve_package_runner(module: str, resource: PurePosixPath, runner_modules: Sequence[str]) -> Path:
    if not any(module == allowed or module.startswith(f"{allowed}.") for allowed in runner_modules):
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
    if job.runner_source == "workspace":
        try:
            candidate = manager.workspace.runner_store_path(job.runner_path)
        except FormatError as exc:
            raise RunnerResolutionError("runner_unavailable", str(exc)) from exc
        if not candidate.exists():
            raise RunnerResolutionError(
                "runner_unavailable",
                f"workspace runner {job.runner_path.as_posix()} is not published in {manager.workspace.runners}",
            )
        return candidate
    package = parse_package_runner(job.runner_path.as_posix())
    if package is not None:
        return resolve_package_runner(*package, manager.runner_modules)
    for root in manager.runner_search_paths:
        installed = contained(root, job.runner_path.parts)
        if installed is not None and installed.exists():
            return installed
    searched = ", ".join(str(path) for path in manager.runner_search_paths) or "no configured search path"
    raise RunnerResolutionError(
        "runner_unavailable", f"installed runner {job.runner_path.as_posix()} was not found in {searched}"
    )


def stage_runner(
    manager: Any,
    job: JobDefinition,
    control: Path,
    *,
    tree_digest: Callable[..., str],
    sha256_file: Callable[[Path], str],
    log: Any,
) -> Path:
    source = resolve_shared_runner(manager, job)
    staged = control / "runner"
    try:
        if source.is_dir():
            shutil.copytree(source, staged, symlinks=False)
            for entry in sorted(staged.rglob("*")):
                entry.chmod(0o500)
            staged.chmod(0o500)
            digest = tree_digest(staged)
        else:
            shutil.copyfile(source, staged)
            staged.chmod(0o500)
            digest = sha256_file(staged)
    except OSError as exc:
        raise RunnerResolutionError(
            "runner_unavailable", f"cannot stage runner {source} for {job.job_key}: {exc}"
        ) from exc
    except FormatError as exc:
        raise RunnerResolutionError("runner_unavailable", f"cannot pin runner {source}: {exc}") from exc
    if digest != job.runner_sha256:
        raise RunnerResolutionError(
            "runner_mismatch",
            f"{job.runner_source} runner {job.runner_path.as_posix()} has digest {digest}, but the job pinned {job.runner_sha256}",
        )
    executable = staged / RUNNER_TREE_ENTRY if staged.is_dir() else staged
    if not executable.is_file():
        raise RunnerResolutionError(
            "runner_unavailable",
            f"staged runner tree {job.runner_path.as_posix()} has no {RUNNER_TREE_ENTRY} entry point",
        )
    log.info(
        "staged %s runner %s for %s as %s (digest %s)",
        job.runner_source,
        job.runner_path.as_posix(),
        job.job_key,
        executable,
        digest,
        extra=manager._event(
            "runner_staged", runner=job.runner_path.as_posix(), runner_source=job.runner_source, sha256=digest
        ),
    )
    return executable
