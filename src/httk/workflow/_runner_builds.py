"""Build and resolve compiled workspace runner artifacts.

Re-registration leaves prior generations on disk. Reclaim a tag directory only
when no managers are running.
"""

import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path, PurePosixPath

from ._util import tree_digest, utc_now, write_json_atomic
from .errors import RunnerResolutionError
from .packages import BuildSpec, artifact_excluder, read_build_spec
from .registry import list_workspaces
from .workspace import Workspace

BUILD_DIRECTORY = "runner-builds"
_PLATFORM_CACHE: dict[str, tuple[str, str]] = {}
_STDERR_TAIL = 1024


def _environment() -> dict[str, str]:
    """Return the build environment without workflow runtime variables."""

    environment = dict(os.environ)
    for key in tuple(environment):
        if key.startswith("HTTK_WORKFLOW_"):
            environment.pop(key)
    return environment


def _platform_result(spec: BuildSpec) -> tuple[str, str]:
    if spec.platform is None:
        return "any", ""
    cached = _PLATFORM_CACHE.get(spec.platform)
    if cached is not None:
        return cached
    try:
        argv = shlex.split(spec.platform)
        if not argv:
            raise ValueError("empty command")
        completed = subprocess.run(
            argv,
            env=_environment(),
            capture_output=True,
            text=True,
            check=False,
        )
    except (OSError, ValueError) as exc:
        raise RunnerResolutionError("runner_build_failed", f"platform probe {spec.platform!r} failed: {exc}") from exc
    if completed.returncode != 0:
        stderr = completed.stderr[-_STDERR_TAIL:].strip()
        detail = f"; stderr: {stderr}" if stderr else ""
        raise RunnerResolutionError(
            "runner_build_failed",
            f"platform probe {spec.platform!r} failed with exit code {completed.returncode}{detail}",
        )
    raw = completed.stdout
    value = raw.strip()
    digest = hashlib.sha256(raw.encode()).hexdigest()
    tag = re.sub(r"[^A-Za-z0-9._-]", "-", value)
    if not tag or tag in {".", ".."} or len(tag) > 64:
        tag = "h" + digest[:16]
    else:
        tag = f"{tag}.{digest[:8]}"
    result = (tag, raw)
    _PLATFORM_CACHE[spec.platform] = result
    return result


def platform_tag(spec: BuildSpec) -> str:
    """Return the local platform tag declared by a build specification.

    :param spec: Supply the build specification whose platform probe to run.
    :return: The sanitized platform tag.
    :raises httk.workflow.errors.RunnerResolutionError: If the probe fails.
    """

    return _platform_result(spec)[0]


def workspace_build_command(workspace: Workspace, store_relative: PurePosixPath) -> str:
    """Return an executable build command for a workspace runner.

    :param workspace: Identify the workspace that owns the runner.
    :param store_relative: Identify the runner store entry.
    :return: A shell-ready command using a registered name or literal path.
    """

    root = workspace.root.resolve()
    for binding in list_workspaces():
        if binding.path is not None and Path(binding.path).resolve() == root:
            return shlex.join(("httk", "workflow", "build", binding.name, "--store", store_relative.as_posix()))
    return shlex.join(("httk", "workflow", "build", "--by-path", str(root), "--store", store_relative.as_posix()))


def _cache_root(workspace: Workspace, store_relative: PurePosixPath, tag: str) -> Path:
    return workspace.runner_builds.joinpath(*store_relative.parts, tag)


def registered_artifacts(
    workspace: Workspace,
    store_relative: PurePosixPath,
    tag: str,
    *,
    expected_source_sha256: str | None,
) -> Path | None:
    """Return registered artifacts when the build stamp still matches.

    :param workspace: Provide the workspace-local build cache.
    :param store_relative: Identify the source runner in the runner store.
    :param tag: Identify the platform registration.
    :param expected_source_sha256: Require this source digest when supplied.
    :return: The artifact directory, or ``None`` when it is unusable.
    """

    if store_relative.is_absolute() or any(part in {"", ".", ".."} for part in store_relative.parts):
        return None
    if Path(tag).name != tag or tag in {"", ".", ".."}:
        return None
    root = _cache_root(workspace, store_relative, tag)
    try:
        pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    generation_name = pointer.get("generation") if isinstance(pointer, dict) else None
    if not isinstance(generation_name, str) or not generation_name.startswith("gen-"):
        return None
    generation = root / generation_name
    if generation.parent != root or not generation.is_dir():
        return None
    artifacts = generation / "artifacts"
    stamp = generation / "build.json"
    if not artifacts.is_dir() or not stamp.is_file():
        return None
    try:
        value = json.loads(stamp.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    if value.get("format") != "httk-workflow-runner-build" or value.get("format_version") != 1:
        return None
    if expected_source_sha256 is not None and value.get("source_sha256") != expected_source_sha256:
        return None
    return artifacts


def _make_writable(root: Path) -> None:
    for entry in (root, *root.rglob("*")):
        entry.chmod(entry.stat().st_mode | stat.S_IWUSR)


def _write_log(path: Path, *, command: str, cwd: Path, exit_code: int, platform_output: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            (
                f"command: {command}",
                f"cwd: {cwd}",
                f"exit code: {exit_code}",
                f"platform output: {platform_output!r}",
                "build output was inherited by the terminal",
            )
        )
        + "\n",
        encoding="utf-8",
    )


def register_build(
    workspace: Workspace,
    store_path: Path,
    store_relative: PurePosixPath,
    spec: BuildSpec,
    *,
    source_sha256: str,
    stdout_to_stderr: bool = False,
) -> Path:
    """Build one published runner and register its artifacts.

    :param workspace: Provide the workspace-local build cache.
    :param store_path: Locate the published source tree to copy and build.
    :param store_relative: Identify the runner store entry.
    :param spec: Describe the foreground build command and artifacts.
    :param source_sha256: Pin and verify the source tree this build consumes.
    :param stdout_to_stderr: Route inherited build stdout to the caller's stderr.
    :return: The registered artifact directory.
    :raises httk.workflow.errors.RunnerResolutionError: If probing or building fails.
    """

    if not store_path.is_dir():
        raise RunnerResolutionError("runner_build_failed", f"runner store tree does not exist: {store_path}")
    # ponytail: foreground builds have no lock or timeout; add coordination only if concurrent builds matter.
    scratch = workspace.control / "tmp" / f"runner-build.{uuid.uuid4()}"
    source = scratch / "src"
    output = scratch / "out"
    command_argv: list[str]
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
        tag, platform_output = _platform_result(verified_spec)
        tag_dir = _cache_root(workspace, store_relative, tag)
        log_path = tag_dir.parent / f"{tag}.log"
        try:
            command_argv = shlex.split(verified_spec.command)
            if not command_argv:
                raise ValueError("empty command")
        except ValueError as exc:
            raise RunnerResolutionError(
                "runner_build_failed", f"build command {verified_spec.command!r} is invalid: {exc}"
            ) from exc
        _make_writable(source)
        try:
            completed = subprocess.run(
                command_argv,
                cwd=source,
                env=_environment(),
                stdout=sys.stderr.fileno() if stdout_to_stderr else None,
                check=False,
            )
        except OSError as exc:
            _write_log(
                log_path,
                command=verified_spec.command,
                cwd=source,
                exit_code=-1,
                platform_output=platform_output,
            )
            raise RunnerResolutionError(
                "runner_build_failed", f"build command {verified_spec.command!r} failed: {exc}"
            ) from exc
        _write_log(
            log_path,
            command=verified_spec.command,
            cwd=source,
            exit_code=completed.returncode,
            platform_output=platform_output,
        )
        if completed.returncode != 0:
            raise RunnerResolutionError(
                "runner_build_failed",
                f"build command {verified_spec.command!r} failed with exit code {completed.returncode}",
            )
        predicate = artifact_excluder(verified_spec)
        matches = [
            entry for entry in source.rglob("*") if entry.is_file() and predicate(entry.relative_to(source).as_posix())
        ]
        if not matches:
            raise RunnerResolutionError(
                "runner_build_failed",
                f"build command {verified_spec.command!r} produced no artifacts matching {verified_spec.artifacts!r}",
            )
        for entry in matches:
            relative = entry.relative_to(source)
            destination = output / "artifacts" / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, destination)
        write_json_atomic(
            output / "build.json",
            {
                "format": "httk-workflow-runner-build",
                "format_version": 1,
                "source_sha256": source_sha256,
                "command": verified_spec.command,
                "platform": verified_spec.platform,
                "platform_output": platform_output,
                "platform_tag": tag,
                "built_at": utc_now(),
            },
        )
        tag_dir.mkdir(parents=True, exist_ok=True)
        generation_name = f"gen-{uuid.uuid4()}"
        generation = tag_dir / generation_name
        os.replace(output, generation)
        write_json_atomic(tag_dir / "current.json", {"generation": generation_name})
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


def overlay_artifacts(artifacts_root: Path, staged: Path) -> None:
    """Copy registered artifact files over a staged runner tree.

    :param artifacts_root: Locate the registered artifact tree.
    :param staged: Locate the manager's writable staged runner copy.
    """

    for entry in artifacts_root.rglob("*"):
        if entry.is_file():
            destination = staged / entry.relative_to(artifacts_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, destination)
