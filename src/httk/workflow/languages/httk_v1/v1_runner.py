#!/usr/bin/env python3
"""Run one httk v1 task through the ordinary :mod:`httk.workflow` SDK."""

from __future__ import annotations

import bz2
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from importlib.resources import files
from pathlib import Path, PurePosixPath

try:
    from httk.workflow import Attempt, Runner
    from httk.workflow._util import write_json_atomic
except ModuleNotFoundError:  # pragma: no cover - cluster interpreter bootstrap
    python = os.environ.get("HTTK_WORKFLOW_PYTHON")
    if python is None or os.environ.get("HTTK_WORKFLOW_RUNNER_BOOTSTRAP") == "1":
        raise
    os.environ["HTTK_WORKFLOW_RUNNER_BOOTSTRAP"] = "1"
    os.execv(python, [python, os.path.abspath(__file__), *sys.argv[1:]])

V1_PRIORITY_MAP = {1: 100, 2: 300, 3: 500, 4: 700, 5: 900}
TASK_PATTERN = re.compile(
    r"^ht\.task\.(?P<taskset>[^.]+)\.(?P<task_id>[^.]+)\.(?P<step>[^.]+)\."
    r"(?P<restarts>[0-9]+)\.(?P<owner>[^.]+)\.(?P<priority>[1-5])\."
    r"(?P<status>waitstart|waitstep|waitsubtasks|running|finished|broken|stopped|timeout)$"
)
ACTIVE: subprocess.Popen[bytes] | None = None
run = Runner("httk-v1")


class _V1ConfigError(Exception):
    """A v1 defect :func:`start` publishes as a structured failure, not a crash.

    Raising this instead of a bare exception lets :func:`start` publish the
    carried ``code`` with the real message, rather than letting the process die
    and be reported as ``process_failure`` and eventually ``retry_exhausted``
    with the text erased. Deterministic configuration defects use the default
    non-retryable ``v1.configuration_invalid``; a transient site (a legacy
    runner not yet visible on a shared filesystem) carries its own code and
    ``retryable=True`` so the manager may try again.

    :param message: The human-readable defect message.
    :param code: The stable failure code to publish.
    :param retryable: Whether repeating the attempt could help.
    """

    def __init__(self, message: str, *, code: str = "v1.configuration_invalid", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def _remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def replay_v1_atomic(workdir: Path) -> None:
    """Idempotently finish legacy ``ht.atomic.*`` directories."""

    for temporary in workdir.glob("ht.tmp.atomic.*"):
        _remove(temporary)
    for atomic in sorted(workdir.glob("ht.atomic.*")):
        if not atomic.is_dir():
            continue
        for instruction in sorted(atomic.glob("ht.atommv.*")):
            source_name = instruction.name.removeprefix("ht.atommv.")
            destination_text = instruction.read_text(encoding="utf-8").strip()
            if not source_name or not destination_text:
                raise RuntimeError(f"invalid legacy atomic move instruction: {instruction}")
            source, destination = workdir / source_name, workdir / destination_text
            if source.exists() or source.is_symlink():
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() or destination.is_symlink():
                    _remove(destination)
                os.rename(source, destination)
            elif not destination.exists() and not destination.is_symlink():
                raise RuntimeError(f"legacy atomic move lost both source and destination: {source_name}")
            instruction.unlink(missing_ok=True)
        for entry in sorted(atomic.iterdir()):
            destination = workdir / entry.name
            if destination.exists() or destination.is_symlink():
                _remove(destination)
            os.rename(entry, destination)
        atomic.rmdir()
    for temporary in workdir.glob("ht.tmp.atomic.*"):
        _remove(temporary)


def _next_step(workdir: Path) -> str | None:
    path = workdir / "ht.nextstep"
    return path.read_text(encoding="utf-8").strip() or None if path.is_file() else None


def _priority(workdir: Path) -> int | None:
    path = workdir / "ht.priority"
    if not path.is_file():
        return None
    try:
        return V1_PRIORITY_MAP[int(path.read_text(encoding="utf-8").strip())]
    except (KeyError, ValueError) as exc:
        raise _V1ConfigError("ht.priority must contain an integer from 1 through 5") from exc


def _terminate(process: subprocess.Popen[bytes], *, grace: float = 10.0) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def _forward_signal(signum: int, frame: object) -> None:
    del frame
    if ACTIVE is not None:
        _terminate(ACTIVE)
    raise SystemExit(128 + signum)


def _run_program(
    command: Sequence[str], *, cwd: Path, environment: Mapping[str, str], log: Path, timeout: float
) -> tuple[int, bool]:
    global ACTIVE
    with log.open("ab") as stream:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=dict(environment),
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        ACTIVE = process
        try:
            return process.wait(timeout=timeout), False
        except subprocess.TimeoutExpired:
            _terminate(process)
            return 99, True
        finally:
            ACTIVE = None


def _note(message: str, log: Path | None = None) -> None:
    text = f"httk-workflow: {message}\n"
    print(text, end="", file=sys.stderr, flush=True)
    if log is not None:
        try:
            with log.open("ab") as stream:
                stream.write(text.encode())
        except OSError:
            pass


def _freeze(program: Path, *, cwd: Path, environment: Mapping[str, str], log: Path, wrapper: str | None) -> None:
    if program.name != "ht_steps":
        return
    (cwd / "ht.nextstep").unlink(missing_ok=True)
    command = [str(program), "freeze"]
    if wrapper:
        command.insert(0, wrapper)
    try:
        status, timed_out = _run_program(command, cwd=cwd, environment=environment, log=log, timeout=300.0)
        if timed_out:
            _note("the legacy freeze step exceeded its 300 second limit and was stopped", log)
        elif status != 0:
            _note(f"the legacy freeze step returned exit status {status}", log)
        replay_v1_atomic(cwd)
    except Exception as exc:
        _note(f"the legacy freeze step could not be completed: {exc!r}", log)


def _compatibility(a: Attempt) -> Mapping[str, object]:
    compatibility = a.job.raw.get("compatibility")
    if not isinstance(compatibility, Mapping) or compatibility.get("profile") != "httk-v1-task-v1":
        raise _V1ConfigError("v1 compatibility metadata is missing")
    return compatibility


def _root(a: Attempt) -> Path:
    value = a.environment("httk_v1.root")
    if not isinstance(value, str):
        raise ValueError("httk_v1.root must be a string")
    return Path(value).resolve() if value else Path(str(files("httk.workflow.compat.v1").joinpath("v1_runtime")))


def _settings(a: Attempt) -> tuple[float, str | None, str, int]:
    timeout, wrapper, compression = (
        a.environment("httk_v1.timeout"),
        a.environment("httk_v1.wrapper"),
        a.environment("httk_v1.log_compression"),
    )
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("httk_v1.timeout must be a positive integer")
    if not isinstance(wrapper, str) or not isinstance(compression, str):
        raise ValueError("httk-v1 environment values are malformed")
    if compression not in {"none", "bzip2", "zstd"}:
        raise ValueError("httk_v1.log_compression must be none, bzip2, or zstd")
    attempts = _compatibility(a).get("attempts")
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise ValueError("compatibility.attempts must be a nonnegative integer")
    return float(timeout), wrapper or None, compression, attempts


def _environment(
    a: Attempt, *, root: Path, current: Path, program: str, timeout: float, attempts: int, wrapper: str | None
) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_DIR": str(root.resolve()),
            "HT_TASK_TOP_DIR": str(a.payload),
            "HT_TASK_CURRENT_DIR": str(current),
            "HT_TASK_RUN_NAME": "ht.run.current" if program == "ht_steps" else ".",
            "HT_TASK_REL_TOP_DIR": os.path.relpath(a.payload, current),
            "HT_TASK_STEP": str(a.state.get("v1_step", "start")),
            "HT_TASKMGR_TIMEOUT": str(int(timeout)),
            "HT_TASKMGR_SET": a.job.claim_pool,
            "HT_TASKMGR_ROOTDIR": str(a.workspace),
            "HT_TASKMGR_ATTEMPTS": str(attempts),
            "HT_NBR_NODES": environment.get("HT_NBR_NODES", "1"),
            "TASKMGRPID": str(os.getpid()),
        }
    )
    if wrapper:
        environment["HT_TASKMGR_WRAP"] = wrapper
    return environment


def _task_directories(root: Path, spawned: set[str]) -> list[Path]:
    result: list[Path] = []
    for directory, directories, _ in os.walk(root, followlinks=False):
        root_path, kept = Path(directory), []
        for name in directories:
            path = root_path / name
            if name.startswith((".httk-attempt.", ".httk-v1-")) or path.is_symlink():
                continue
            fields = TASK_PATTERN.fullmatch(name)
            relative = path.relative_to(root).as_posix()
            if fields is not None and fields["status"] in {"waitstart", "waitstep"}:
                if relative not in spawned:
                    result.append(path)
                continue
            kept.append(name)
        directories[:] = kept
    return sorted(result)


def _label(task_id: str, used: set[str]) -> str:
    base = re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9._-]+", "-", task_id.lower()).strip("-."))[:48].strip("-.")
    base = base if base and base[0].isalnum() else f"task-{base}"[:48].rstrip("-.")
    label, index = base, 1
    while label in used:
        index += 1
        label = f"{base[:44]}-{index}"
    used.add(label)
    return label


def _safe_tag(value: str) -> str:
    tag = re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip("-._")
    tag = re.sub(r"-{2,}", "-", tag)[:48].rstrip("-._")
    return tag or "v1-task"


def _placement(a: Attempt, child_id: str) -> str:
    root = str(_compatibility(a).get("root_placement", a.context.placement))
    return str(PurePosixPath(root) / "v1-children" / a.job.id[:2] / a.job.id[2:4] / child_id[:2] / child_id[2:4])


def _spawn(a: Attempt, sources: Sequence[Path], attempts: int, legacy_root: Path) -> list[str]:
    labels: set[str] = set()
    spawned: list[str] = []
    with tempfile.TemporaryDirectory(prefix="v1-spawn.", dir=a.control) as temporary:
        staging = Path(temporary)
        for source in sources:
            fields = TASK_PATTERN.fullmatch(source.name)
            if fields is None:
                continue
            relative = source.relative_to(legacy_root).as_posix()
            child_id = str(uuid.uuid5(uuid.UUID(a.job.id), f"{relative}|{fields['taskset']}|{fields['task_id']}"))
            prepared = staging / child_id
            shutil.copytree(source, prepared)
            mapping = dict(a.job.raw)
            mapping.update(
                {
                    "id": child_id,
                    "tag": _safe_tag(f"{fields['taskset']}-{fields['task_id']}"),
                    "name": f"httk v1 subtask {fields['task_id']}",
                    "initial_step": "start",
                    "priority": V1_PRIORITY_MAP[int(fields['priority'])],
                }
            )
            mapping["claim"] = {
                "pool": "default" if fields["taskset"] == "any" else fields["taskset"],
                "required_capabilities": [],
            }
            compatibility = dict(_compatibility(a))
            compatibility["legacy_step"] = fields["step"]
            compatibility["root_placement"] = str(compatibility.get("root_placement", a.context.placement))
            mapping["compatibility"] = compatibility
            write_json_atomic(prepared / "job.json", mapping)
            a.spawn(prepared, label=_label(fields["task_id"], labels), placement=_placement(a, child_id))
            spawned.append(relative)
    return spawned


def _pending_sources(a: Attempt, pending: Mapping[str, object]) -> list[Path]:
    """Return the payload-relative legacy directories one pending outcome owns."""

    raw = pending.get("sources", [])
    if not isinstance(raw, list):
        return []
    sources: list[Path] = []
    for value in raw:
        if not isinstance(value, str):
            continue
        relative = PurePosixPath(value)
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            continue
        candidate = a.payload.joinpath(*relative.parts)
        if candidate.is_dir() and TASK_PATTERN.fullmatch(candidate.name) is not None:
            sources.append(candidate)
    return sources


def _publish_pending(a: Attempt, pending: Mapping[str, object], attempts: int) -> None:
    """Reconstruct the spawn outcome recorded before the previous attempt died."""

    mode, step = pending.get("mode"), pending.get("step")
    if mode not in {"advance", "gather"} or not isinstance(step, str) or not step:
        raise _V1ConfigError("v1 pending continuation is malformed")
    priority = pending.get("priority")
    if priority is not None and (isinstance(priority, bool) or not isinstance(priority, int)):
        raise _V1ConfigError("v1 pending continuation priority is malformed")
    spawned_raw = a.state.get("v1_spawned", [])
    spawned = {item for item in spawned_raw if isinstance(item, str)} if isinstance(spawned_raw, list) else set()
    sources = _pending_sources(a, pending)
    added = _spawn(a, sources, attempts, a.payload)
    a.state.merge(
        {
            "v1_step": step,
            "v1_spawned": sorted(spawned | set(added)),
            "v1_spawned_activation": a.context.activation_id,
        }
    )
    if mode == "advance":
        a.advance("start", priority=priority)
    else:
        a.gather("start", when="all_terminal", priority=priority)


def _continue_with_children(
    a: Attempt,
    *,
    mode: str,
    step: str,
    priority: int | None,
    sources: Sequence[Path],
    attempts: int,
) -> None:
    """Checkpoint and publish one continuation that owns legacy child directories."""

    pending = {
        "activation": a.context.activation_id,
        "mode": mode,
        "step": step,
        "priority": priority,
        "sources": [source.relative_to(a.payload).as_posix() for source in sources],
    }
    a.state.merge({"v1_pending": pending})
    _publish_pending(a, pending, attempts)


def _archive(payload: Path, workdir: Path, compression: str) -> None:
    source = payload / "ht.taskmgr.stdout"
    if not source.is_file():
        return
    destination = workdir / source.name
    if source.resolve() != destination.resolve():
        shutil.copyfile(source, destination)
    if compression == "bzip2":
        destination.with_suffix(destination.suffix + ".bz2").write_bytes(bz2.compress(destination.read_bytes()))
        destination.unlink()
    elif compression == "zstd":
        try:
            subprocess.run(["zstd", "--rm", str(destination)], check=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            _note(f"the run log could not be compressed with zstd: {exc!r}", destination)


@run.step
def start(a: Attempt) -> None:
    """Run one legacy step, retaining the legacy step name in durable state.

    A deterministic v1 configuration or protocol defect is published as a
    non-retryable ``v1.configuration_invalid`` failure so it bypasses
    ``retry_on`` and keeps its message, rather than dying as a ``process_failure``
    that the manager eventually reports as ``retry_exhausted``.
    """

    try:
        _start(a)
    except _V1ConfigError as exc:
        a.fail(exc.code, str(exc), retryable=exc.retryable)


def _start(a: Attempt) -> None:
    """Run one legacy step; deterministic config defects raise :class:`_V1ConfigError`."""

    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)
    compatibility = _compatibility(a)
    program = str(compatibility.get("program", ""))
    if program not in {"ht_steps", "ht_run"}:
        raise _V1ConfigError("compatibility.program must be ht_steps or ht_run")
    timeout, wrapper, compression, attempts = _settings(a)
    program_path, log = a.payload / program, a.payload / "ht.taskmgr.stdout"
    if not program_path.is_file() or not os.access(program_path, os.X_OK):
        # A shared filesystem can lag (NFS attribute cache, slow automount after
        # transfer), so a runner that is not yet visible is transient, not a
        # deterministic config defect: publish a retryable code, message intact.
        raise _V1ConfigError(
            f"legacy runner is missing or not executable: {program_path}",
            code="v1.runner_unavailable",
            retryable=True,
        )
    if program == "ht_steps":
        resume = a.payload / "ht.run.resume"
        if resume.is_dir() and not any(a.workdir.iterdir()):
            a.workdir.rmdir()
            os.rename(resume, a.workdir)
        current = a.workdir
    else:
        current = a.payload
    current_step = a.state.get("v1_step", compatibility.get("legacy_step", "start"))
    if not isinstance(current_step, str) or not current_step:
        raise _V1ConfigError("v1_step must be a nonempty string")
    environment = _environment(
        a, root=_root(a), current=current, program=program, timeout=timeout, attempts=attempts, wrapper=wrapper
    )
    environment["HT_TASK_STEP"] = current_step
    replay_v1_atomic(current)
    pending_continuation = a.state.get("v1_pending")
    if isinstance(pending_continuation, Mapping) and pending_continuation.get("activation") == a.context.activation_id:
        _publish_pending(a, pending_continuation, attempts)
        return
    terminal = a.state.get("v1_terminal")
    if terminal == "succeed":
        _archive(a.payload, current, compression)
        a.succeed()
        return
    pending, priority = _next_step(current), _priority(current)
    if pending == "ht_finished":
        a.advance("start", state={"v1_terminal": "succeed"}, priority=priority)
        return
    if pending == "ht_broken":
        _freeze(program_path, cwd=current, environment=environment, log=log, wrapper=wrapper)
        a.fail("declared_failure", "legacy task declared ht_broken", priority=priority)
        return
    spawned_raw = a.state.get("v1_spawned", [])
    spawned = {item for item in spawned_raw if isinstance(item, str)} if isinstance(spawned_raw, list) else set()
    sources = _task_directories(a.payload, spawned)
    if pending is not None and pending != current_step:
        if sources:
            _continue_with_children(
                a, mode="gather", step=pending, priority=priority, sources=sources, attempts=attempts
            )
        else:
            a.advance("start", state={"v1_step": pending}, priority=priority)
        return
    if a.context.attempt_ordinal is not None and a.context.attempt_ordinal > attempts + 1:
        _freeze(program_path, cwd=current, environment=environment, log=log, wrapper=wrapper)
        a.fail("retry_exhausted", "legacy manager attempt limit exceeded", priority=priority)
        return
    command = [str(program_path), current_step]
    if wrapper:
        command.insert(0, wrapper)
    result, timed_out = _run_program(command, cwd=current, environment=environment, log=log, timeout=timeout)
    replay_v1_atomic(current)
    next_step, priority = _next_step(current), _priority(current)
    if result == 10 or (program == "ht_run" and result == 0) or next_step == "ht_finished":
        a.advance("start", state={"v1_terminal": "succeed"}, priority=priority)
    elif result == 2:
        if next_step is None:
            raise _V1ConfigError("legacy exit 2 requires ht.nextstep")
        sources = _task_directories(a.payload, spawned)
        if sources:
            _continue_with_children(
                a, mode="advance", step=next_step, priority=priority, sources=sources, attempts=attempts
            )
        else:
            a.advance("start", state={"v1_step": next_step}, priority=priority)
    elif result == 3:
        if next_step is None:
            raise _V1ConfigError("legacy exit 3 requires ht.nextstep")
        _continue_with_children(
            a,
            mode="gather",
            step=next_step,
            priority=priority,
            sources=_task_directories(a.payload, spawned),
            attempts=attempts,
        )
    elif result == 4 or next_step == "ht_broken":
        _freeze(program_path, cwd=current, environment=environment, log=log, wrapper=wrapper)
        a.fail("declared_failure", "legacy task returned exit status 4", priority=priority)
    elif timed_out or result == 99:
        _freeze(program_path, cwd=current, environment=environment, log=log, wrapper=wrapper)
        a.fail("timeout", "legacy task exceeded its manager timeout", priority=priority)
    else:
        a.fail(
            "process_failure",
            f"legacy task returned undefined exit status {result}",
            details={"exit_status": result},
            priority=priority,
        )


if __name__ == "__main__":
    raise SystemExit(run.main())
