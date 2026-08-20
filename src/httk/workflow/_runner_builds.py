"""Build and resolve compiled workspace runner artifacts.

Re-registration leaves prior generations on disk. Reclaim a tag directory only
when no managers are running.
"""

import os
import shlex
import shutil
import stat
import uuid
from pathlib import Path, PurePosixPath

from httk.core.building import (
    BuildError,
    BuildSpec,
    execute_build,
    registered_generation,
    write_generation,
)
from httk.core.building import platform_tag as _core_platform_tag
from httk.core.digests import tree_digest

from .errors import RunnerResolutionError
from .packages import read_build_spec
from .registry import list_workspaces
from .workspace import Workspace

BUILD_DIRECTORY = "runner-builds"


def _environment() -> dict[str, str]:
    """Return the build environment without workflow runtime variables."""

    environment = dict(os.environ)
    for key in tuple(environment):
        if key.startswith("HTTK_WORKFLOW_"):
            environment.pop(key)
    return environment


def platform_tag(spec: BuildSpec) -> str:
    """Return the local platform tag declared by a build specification."""

    try:
        return _core_platform_tag(spec.platform, env=_environment())[0]
    except BuildError as exc:
        raise RunnerResolutionError(exc.code, exc.message or str(exc)) from exc


def workspace_build_command(workspace: Workspace, store_relative: PurePosixPath) -> str:
    """Return an executable build command for a workspace runner."""

    root = workspace.root.resolve()
    for binding in list_workspaces():
        if binding.path is not None and Path(binding.path).resolve() == root:
            return shlex.join(("httk", "workflow", "build", binding.name, "--store", store_relative.as_posix()))
    return shlex.join(("httk", "workflow", "build", "--by-path", str(root), "--store", store_relative.as_posix()))


def registered_artifacts(
    workspace: Workspace,
    store_relative: PurePosixPath,
    tag: str,
    *,
    expected_source_sha256: str | None,
) -> Path | None:
    """Return registered artifacts when the build stamp still matches."""

    if store_relative.is_absolute() or any(part in {"", ".", ".."} for part in store_relative.parts):
        return None
    if Path(tag).name != tag or tag in {"", ".", ".."}:
        return None
    return registered_generation(
        workspace.runner_builds,
        store_relative.as_posix(),
        tag,
        format_name="httk-workflow-runner-build",
        expected_source_sha256=expected_source_sha256,
    )


def _make_writable(root: Path) -> None:
    for entry in (root, *root.rglob("*")):
        entry.chmod(entry.stat().st_mode | stat.S_IWUSR)


def register_build(
    workspace: Workspace,
    store_path: Path,
    store_relative: PurePosixPath,
    spec: BuildSpec,
    *,
    source_sha256: str,
    stdout_to_stderr: bool = False,
) -> Path:
    """Build one published runner and register its artifacts."""

    if not store_path.is_dir():
        raise RunnerResolutionError("runner_build_failed", f"runner store tree does not exist: {store_path}")
    # ponytail: foreground builds have no lock or timeout; add coordination only if concurrent builds matter.
    scratch = workspace.control / "tmp" / f"runner-build.{uuid.uuid4()}"
    source = scratch / "src"
    try:
        scratch.mkdir(parents=True, exist_ok=True)
        shutil.copytree(store_path, source, symlinks=False)
        if tree_digest(source) != source_sha256:
            raise RunnerResolutionError(
                "runner_build_failed",
                f"published source for {store_relative} does not match claimed digest {source_sha256}",
            )
        try:
            verified_spec = read_build_spec(source)
        except ValueError as exc:
            raise RunnerResolutionError("runner_build_failed", f"verified runner manifest is malformed: {exc}") from exc
        if verified_spec is None:
            raise RunnerResolutionError(
                "runner_build_failed", f"verified runner {store_relative} has no [workflow.build] section"
            )
        tag = platform_tag(verified_spec)
        log_path = workspace.runner_builds.joinpath(*store_relative.parts, f"{tag}.log")
        _make_writable(source)
        try:
            result = execute_build(
                source,
                verified_spec,
                strip_env_prefixes=("HTTK_WORKFLOW_",),
                log_path=log_path,
                stdout_to_stderr=stdout_to_stderr,
            )
        except BuildError as exc:
            raise RunnerResolutionError(exc.code, exc.message or str(exc)) from exc
        generation = write_generation(
            workspace.runner_builds,
            store_relative.as_posix(),
            result.tag,
            source,
            result.artifact_files,
            {
                "format": "httk-workflow-runner-build",
                "format_version": 2,
                "source_sha256": source_sha256,
                "command": verified_spec.command,
                "platform": verified_spec.platform,
                "platform_output": result.platform_output,
                "platform_tag": result.tag,
            },
        )
        return generation / "artifacts"
    except RunnerResolutionError:
        raise
    except (OSError, ValueError) as exc:
        raise RunnerResolutionError(
            "runner_build_failed", f"could not register build for {store_relative}: {exc}"
        ) from exc
    finally:
        if scratch.exists():
            for entry in sorted(scratch.rglob("*"), key=lambda path: len(path.parts), reverse=True):
                try:
                    entry.chmod(0o755 if entry.is_dir() else 0o644)
                except OSError:
                    pass
            try:
                scratch.chmod(0o755)
            except OSError:
                pass
        shutil.rmtree(scratch, ignore_errors=True)
