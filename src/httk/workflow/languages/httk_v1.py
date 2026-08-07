"""Realize legacy httk v1 template directories as workflow jobs."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, cast

from httk.workflow.compat.v1 import (
    V1_CAPABILITY,
    V1_EXECUTOR,
    V2_TO_V1_PRIORITY,
    _execute_instantiator,
    templates,
)
from httk.workflow.languages import LanguagePorts, LanguageRequest, LanguageScaffold, WorkflowLanguage
from httk.workflow.protocol import validate_label

if TYPE_CHECKING:
    from httk.workflow.collecting import JobRecord
    from httk.workflow.runtime_builders import JobSpec
    from httk.workflow.scaffold import InstantiateContext

PROGRAMS = ("ht_steps", "ht_run")


def _program(root: Path) -> str | None:
    for name in PROGRAMS:
        if (root / name).is_file() or (root / f"{name}.template").is_file():
            return name
    return None


def matches(path: Path) -> bool:
    """Return whether *path* is a bare v1 task directory."""

    return path.is_dir() and not (path / "workflow.toml").is_file() and _program(path) is not None


def ports(path: Path) -> LanguagePorts:
    """Return the port shape of a v1 task, which has no document ports."""

    del path
    return LanguagePorts((), ())


def validate_runner(options: Mapping[str, object], root: Path) -> None:
    """Validate v1 taskset and retry options."""

    del root
    for key in options:
        if key not in {"taskset", "attempts"}:
            raise ValueError(f"unknown runner option {key!r} for httk-v1")
    taskset = options.get("taskset", "default")
    if not isinstance(taskset, str):
        raise ValueError("runner option 'taskset' must be a label")
    validate_label(taskset, "taskset")
    attempts = options.get("attempts", 10)
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise ValueError("runner option 'attempts' must be an integer of zero or greater")


def _snapshot_package(root: Path, excluded: tuple[str, ...]) -> tuple[dict[str, int], dict[str, tuple[bytes, int]]]:
    directories: dict[str, int] = {}
    snapshot: dict[str, tuple[bytes, int]] = {}
    excluded_set = set(excluded)

    def visit(directory: Path, relative_root: PurePosixPath | None = None) -> None:
        if relative_root is None:
            relative_root = PurePosixPath()
        with os.scandir(directory) as entries:
            for entry in sorted(entries, key=lambda item: item.name):
                relative = relative_root / entry.name
                member = relative.as_posix()
                if entry.is_symlink():
                    raise ValueError(f"httk-v1 package member must not be a symlink: {member}")
                if entry.is_dir(follow_symlinks=False):
                    directories[member] = stat.S_IMODE(entry.stat(follow_symlinks=False).st_mode)
                    visit(Path(entry.path), relative)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise ValueError(f"httk-v1 package member must be a regular file: {member}")
                if member in excluded_set:
                    continue
                mode = stat.S_IMODE(entry.stat(follow_symlinks=False).st_mode)
                snapshot[member] = (Path(entry.path).read_bytes(), mode)

    visit(root)
    return directories, snapshot


def _write_snapshot(payload: Path, snapshot: tuple[Mapping[str, int], Mapping[str, tuple[bytes, int]]]) -> None:
    directories, files = snapshot
    for member in sorted(directories, key=lambda value: (len(PurePosixPath(value).parts), value)):
        payload.joinpath(*PurePosixPath(member).parts).mkdir(parents=True, exist_ok=True)
    for member, (content, mode) in files.items():
        destination = payload.joinpath(*PurePosixPath(member).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        destination.chmod(mode)
    for member in sorted(directories, key=lambda value: (-len(PurePosixPath(value).parts), value)):
        payload.joinpath(*PurePosixPath(member).parts).chmod(directories[member])


def prepare(request: LanguageRequest) -> LanguageScaffold:
    """Prepare one v1 package realization."""

    if request.directory is None:
        raise ValueError("httk-v1 requires a task directory")
    root = request.directory.resolve()
    program = _program(root)
    if program is None:
        raise ValueError(f"{root}: no ht_steps or ht_run program found")
    snapshot = _snapshot_package(root, request.excluded_members)
    options = request.runner_options
    taskset = str(options.get("taskset", "default"))
    attempts = cast(int, options.get("attempts", 10))

    def instantiate(ctx: InstantiateContext) -> None:
        """Copy, render, and execute the trusted v1 realization."""

        _write_snapshot(ctx.payload, snapshot)
        supplied: dict[str, object] = {}
        for name, value in ctx.inputs.items():
            metadata = request.inputs.get(name, {})
            if "entry_type" in metadata and isinstance(value, (str, os.PathLike)) and Path(value).is_file():
                import httk.core

                supplied[name] = httk.core.load(os.fspath(value))
            else:
                supplied[name] = value
        globals_ = {key: value for key, value in {**ctx.parameters, **supplied}.items() if key != "workflow_language"}
        templates.apply_templates_in_place(ctx.payload, globals_)
        script = ctx.payload / "ht.instantiate.py"
        if script.is_file():
            _execute_instantiator(ctx.payload, globals_)
        runner = ctx.payload / program
        if not runner.is_file() or not os.access(runner, os.X_OK):
            raise ValueError(f"legacy runner is missing or not executable: {runner}")

    def finalize(spec: JobSpec) -> JobSpec:
        """Apply the v1 executor, retry, claim, and persistent-workdir contract."""

        return replace(
            spec,
            compatibility={
                "profile": "httk-v1-task-v1",
                "program": program,
                "legacy_priority": V2_TO_V1_PRIORITY.get(spec.priority, 3),
                "attempts": attempts,
            },
            claim_pool=taskset,
            retry_on=("lease_lost", "process_failure"),
            maximum_attempts_per_activation=attempts + 1,
            workdir_mode="persistent",
            workdir_path="ht.run.current",
            data_mode="none",
        )

    return LanguageScaffold(
        documents={},
        files={},
        parameters={"workflow_language": "httk-v1"},
        runner=None,
        payload_runner=program,
        runner_executor=V1_EXECUTOR,
        workdir_path="ht.run.current",
        required_capabilities=(V1_CAPABILITY,),
        instantiate=instantiate,
        finalize=finalize,
        reserved_parameters=(),
    )


def postprocess(record: JobRecord) -> Mapping[str, object]:
    """Reject use as a default collector for v1 packages."""

    del record
    raise ValueError("an httk-v1 workflow package declares [workflow.postprocess]")


LANGUAGE = WorkflowLanguage(
    name="httk-v1",
    steps=("start",),
    initial_step="start",
    requires_document=False,
    has_default_postprocessor=False,
    allows_modes=False,
    matches=matches,
    ports=ports,
    validate_runner=validate_runner,
    prepare=prepare,
    postprocess=postprocess,
)
