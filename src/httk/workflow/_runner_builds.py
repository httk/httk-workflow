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
import signal
import stat
import subprocess
import uuid
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.building import (
    DEFAULT_TAG,
    BuildError,
    BuildSpec,
    execute_build,
    registered_generation,
    write_generation,
)
from httk.core.digests import tree_digest

from .errors import FormatError, RunnerResolutionError
from .models import _read_regular_file
from .packages import read_build_spec
from .registry import list_workspaces
from .workspace import Workspace

BUILD_DIRECTORY = "runner-builds"
# The stamp format :func:`register_build` writes and resolution accepts.
_STAMP_FORMAT = "httk-workflow-runner-build"
# How long a platform probe may run, in seconds, before resolution gives up on it.
_PLATFORM_PROBE_TIMEOUT = 30.0
# The largest registration pointer or build stamp a resolver reads.
_REGISTRATION_DOCUMENT_BYTES = 1024 * 1024
# The tail of a failed probe's standard error quoted in its failure.
_PROBE_STDERR_TAIL = 1024
_PLATFORM_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], str] = {}


# The one workflow variable a build sees: the installed language SDK directory.
_LANGUAGES_DIR = {"HTTK_WORKFLOW_LANGUAGES_DIR": str(Path(__file__).with_name("languages"))}


def _environment() -> dict[str, str]:
    """Return the build environment without workflow runtime variables, except the language SDK directory."""

    environment = dict(os.environ)
    for key in tuple(environment):
        if key.startswith("HTTK_WORKFLOW_"):
            environment.pop(key)
    environment.update(_LANGUAGES_DIR)
    return environment


def platform_tag(spec: BuildSpec) -> str:
    """Return the local platform tag declared by a build specification.

    The probe runs exactly as :func:`httk.core.building.platform_tag` runs it,
    with the same build environment and the same tag derivation, so it selects
    the registration a build made, but bounded: standard input is
    ``/dev/null``, the working directory is ``/``, and a probe still running
    after 30 seconds is killed with its process group. Tags are memoized per
    probe command and environment.

    :param spec: The build specification naming the probe.
    :return: The sanitized platform tag (``any`` without a probe).
    :raises httk.workflow.errors.RunnerResolutionError: ``runner_build_failed`` if the probe cannot
        run or fails, ``runner_not_built`` if it does not finish in time.
    """

    if spec.platform is None:
        return DEFAULT_TAG
    environment = _environment()
    cache_key = (spec.platform, tuple(sorted(environment.items())))
    cached = _PLATFORM_CACHE.get(cache_key)
    if cached is not None:
        return cached
    try:
        argv = shlex.split(spec.platform)
        if not argv:
            raise ValueError("empty command")
        with subprocess.Popen(
            argv,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/",
            text=True,
            start_new_session=True,
        ) as process:
            try:
                raw, stderr = process.communicate(timeout=_PLATFORM_PROBE_TIMEOUT)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except OSError:
                    pass
                process.wait()
                raise RunnerResolutionError(
                    "runner_not_built",
                    f"platform probe {spec.platform!r} did not finish within {_PLATFORM_PROBE_TIMEOUT:g} s "
                    "and was killed; no build registration can be selected without its tag",
                ) from None
    except (OSError, ValueError) as exc:
        raise RunnerResolutionError("runner_build_failed", f"platform probe {spec.platform!r} failed: {exc}") from exc
    if process.returncode != 0:
        tail = stderr[-_PROBE_STDERR_TAIL:].strip()
        detail = f"; stderr: {tail}" if tail else ""
        raise RunnerResolutionError(
            "runner_build_failed",
            f"platform probe {spec.platform!r} failed with exit code {process.returncode}{detail}",
        )
    # The tag derivation of httk.core.building.platform_tag, which named the
    # registration directory when the build was registered.
    value = raw.strip()
    digest = hashlib.sha256(raw.encode()).hexdigest()
    tag = re.sub(r"[^A-Za-z0-9._-]", "-", value)
    tag = "h" + digest[:16] if not tag or tag in {".", ".."} or len(tag) > 64 else f"{tag}.{digest[:8]}"
    _PLATFORM_CACHE[cache_key] = tag
    return tag


def registered_platforms(workspace: Workspace, store_relative: PurePosixPath) -> list[dict[str, Any]]:
    """Return the build stamps of the current registrations of one runner store entry.

    One stamp is returned per platform-tag directory whose ``current.json``
    names a generation with a ``build.json`` stamp of the runner-build format.
    Nothing is executed: the documents are read bounded, without following a
    symlink, and unreadable or malformed registrations are skipped.

    :param workspace: The workspace whose registrations to list.
    :param store_relative: The runner store entry, relative to the store.
    :return: The stamps, in tag-directory order.
    """

    if store_relative.is_absolute() or any(part in {"", ".", ".."} for part in store_relative.parts):
        return []
    root = workspace.runner_builds.joinpath(*store_relative.parts)
    stamps: list[dict[str, Any]] = []
    try:
        with os.scandir(root) as scan:
            tags = sorted(entry.name for entry in scan if entry.is_dir(follow_symlinks=False))
    except OSError:
        return []
    for tag in tags:
        try:
            pointer = json.loads(_read_regular_file(root / tag / "current.json", _REGISTRATION_DOCUMENT_BYTES))
            generation = pointer.get("generation") if isinstance(pointer, dict) else None
            if (
                not isinstance(generation, str)
                or not generation.startswith("gen-")
                or Path(generation).name != generation
            ):
                continue
            if not stat.S_ISDIR(os.lstat(root / tag / generation).st_mode):
                continue
            stamp = json.loads(_read_regular_file(root / tag / generation / "build.json", _REGISTRATION_DOCUMENT_BYTES))
        except (OSError, FormatError, UnicodeError, json.JSONDecodeError, RecursionError):
            continue
        if isinstance(stamp, dict) and stamp.get("format") == _STAMP_FORMAT and stamp.get("format_version") == 2:
            stamps.append(stamp)
    return stamps


def workspace_build_command(workspace: Workspace, store_relative: PurePosixPath) -> str:
    """Return an executable build command for a workspace runner."""

    root = workspace.root.resolve()
    for binding in list_workspaces():
        if binding.path is not None and Path(binding.path).resolve() == root:
            return shlex.join(
                ("httk", "workflow", "build", "--workspace", binding.name, "--store", store_relative.as_posix())
            )
    return shlex.join(
        ("httk", "workflow", "build", "--workspace", str(root), "--by-path", "--store", store_relative.as_posix())
    )


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
        format_name=_STAMP_FORMAT,
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
                env=_LANGUAGES_DIR,
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
