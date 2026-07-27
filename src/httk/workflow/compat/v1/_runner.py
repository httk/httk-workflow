"""Internal process adapter for an httk v1 ``ht_steps`` or ``ht_run``.

The module is executed as ``python -m httk.workflow.compat.v1._runner``, so it is
imported through the same package the manager itself was imported from and
needs no path manipulation of its own.
"""

import argparse
import bz2
import os
import re
import shutil
import signal
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from httk.workflow._util import read_json, write_json_atomic
from httk.workflow.compat.v1 import (
    V1_PRIORITY_MAP,
    _job_mapping,
    parse_v1_task_name,
)
from httk.workflow.protocol import (
    AttemptContext,
    ChildReference,
    JobDefinition,
    OutcomeAction,
    OutcomeDraft,
    normalize_placement,
)

_active_process: subprocess.Popen[bytes] | None = None


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("--log-compression", choices=("none", "bzip2", "zstd"), required=True)
    parser.add_argument("--attempts", type=int, required=True)
    parser.add_argument("--wrapper")
    return parser.parse_args()


def _terminate_child(process: subprocess.Popen[bytes], *, grace: float = 10.0) -> None:
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
    if _active_process is not None:
        _terminate_child(_active_process)
    raise SystemExit(128 + signum)


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
            source = workdir / source_name
            destination = workdir / destination_text
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
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def _legacy_priority(workdir: Path) -> int | None:
    path = workdir / "ht.priority"
    if not path.is_file():
        return None
    try:
        legacy = int(path.read_text(encoding="utf-8").strip())
        return V1_PRIORITY_MAP[legacy]
    except (KeyError, ValueError) as exc:
        raise RuntimeError("ht.priority must contain an integer from 1 through 5") from exc


def _run_program(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    log_path: Path,
    timeout: float,
) -> tuple[int, bool]:
    global _active_process
    with log_path.open("ab") as log:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=dict(environment),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _active_process = process
        try:
            return process.wait(timeout=timeout), False
        except subprocess.TimeoutExpired:
            _terminate_child(process)
            return 99, True
        finally:
            _active_process = None


def _note(message: str, *, log_path: Path | None = None) -> None:
    """Record one best-effort failure where an operator will actually see it.

    Best effort must not mean invisible. The note goes to standard error, which
    the manager captures in the attempt's ``stderr.log``, and into the legacy
    run log beside the step output that is missing because of it.
    """

    text = f"httk-workflow: {message}\n"
    print(text, end="", file=sys.stderr, flush=True)
    if log_path is None:
        return
    try:
        with log_path.open("ab") as stream:
            stream.write(text.encode("utf-8"))
    except OSError:
        pass


def _freeze(
    program_path: Path,
    *,
    cwd: Path,
    environment: Mapping[str, str],
    log_path: Path,
    wrapper: str | None,
) -> None:
    """Run the legacy ``freeze`` step, reporting rather than hiding a failure.

    A freeze is the last chance a legacy task gets to write down why it stopped,
    and it runs while the job is already failing. It therefore never turns into
    the outcome — a broken freeze must not replace the real failure — but it is
    not silent either: whatever went wrong is noted in the run log.
    """

    if program_path.name != "ht_steps":
        return
    (cwd / "ht.nextstep").unlink(missing_ok=True)
    command = [str(program_path), "freeze"]
    if wrapper is not None:
        command.insert(0, wrapper)
    try:
        status, timed_out = _run_program(command, cwd=cwd, environment=environment, log_path=log_path, timeout=300.0)
        if timed_out:
            _note("the legacy freeze step exceeded its 300 second limit and was stopped", log_path=log_path)
        elif status != 0:
            _note(f"the legacy freeze step returned exit status {status}", log_path=log_path)
        replay_v1_atomic(cwd)
    except Exception as exc:  # noqa: BLE001 - a failing freeze must not replace the outcome
        _note(f"the legacy freeze step could not be completed: {exc!r}", log_path=log_path)
        return


def _legacy_environment(
    arguments: argparse.Namespace,
    *,
    job: JobDefinition,
    context: AttemptContext,
    payload: Path,
    current: Path,
    program_name: str,
) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_DIR": str(Path(arguments.runtime_root).resolve()),
            "HT_TASK_TOP_DIR": str(payload),
            "HT_TASK_CURRENT_DIR": str(current),
            "HT_TASK_RUN_NAME": "ht.run.current" if program_name == "ht_steps" else ".",
            "HT_TASK_REL_TOP_DIR": os.path.relpath(payload, current),
            "HT_TASK_STEP": str(context.step),
            "HT_TASKMGR_TIMEOUT": str(int(arguments.timeout)),
            "HT_TASKMGR_SET": job.claim_pool,
            "HT_TASKMGR_ROOTDIR": str(Path(os.environ["HTTK_WORKFLOW_WORKSPACE_DIR"])),
            "HT_TASKMGR_ATTEMPTS": str(arguments.attempts),
            "HT_NBR_NODES": environment.get("HT_NBR_NODES", "1"),
            "TASKMGRPID": str(os.getpid()),
        }
    )
    if arguments.wrapper is not None:
        environment["HT_TASKMGR_WRAP"] = arguments.wrapper
    return environment


def _task_directories(payload: Path) -> list[Path]:
    result: list[Path] = []
    for root, directories, _files in os.walk(payload, followlinks=False):
        root_path = Path(root)
        kept: list[str] = []
        for name in directories:
            path = root_path / name
            if name.startswith(".httk-attempt.") or name.startswith(".httk-v1-"):
                continue
            if path.is_symlink():
                continue
            fields = parse_v1_task_name(name)
            if fields is not None and fields["status"] in {"waitstart", "waitstep"}:
                result.append(path)
                continue
            kept.append(name)
        directories[:] = kept
    return sorted(result)


def _child_placement(root: PurePosixPath, parent_id: str, child_id: str) -> PurePosixPath:
    return root / "v1-children" / parent_id[:2] / parent_id[2:4] / child_id[:2] / child_id[2:4]


def _child_label(task_id: str, used: set[str]) -> str:
    """Derive one unique spawn label from a legacy ``ht.task.*`` task id.

    The legacy task id is the name an operator recognizes, so it is kept
    verbatim wherever the label syntax allows. Legacy names are not required to
    be unique across the directories of one task, so a collision appends the
    ordinal of the child within this spawn set.
    """

    cleaned = re.sub(r"[^a-z0-9._-]+", "-", task_id.lower()).strip("-.")
    cleaned = re.sub(r"-{2,}", "-", cleaned)[:48].strip("-.")
    base = cleaned if cleaned and cleaned[0].isalnum() else f"task-{cleaned}"[:48].rstrip("-.")
    label = base
    index = 1
    while label in used:
        index += 1
        label = f"{base[:44]}-{index}"
    used.add(label)
    return label


def _spawn_children(
    draft: OutcomeDraft,
    *,
    payload: Path,
    job: JobDefinition,
    context: AttemptContext,
    attempts: int,
) -> list[tuple[ChildReference, str]]:
    """Register every abandoned legacy task directory as a child of ``draft``.

    Each ``ht.task.*.waitstart``/``waitstep`` directory becomes one prepared
    payload with a deterministic identity, staged beside the attempt and handed
    to :meth:`OutcomeDraft.add_child`, which copies it into the draft and writes
    the canonical spawn set. The legacy directory the parent still owns is left
    in place; :meth:`V1RunnerBackend.commit_outcome` replaces it with a symlink
    to the registered child once the spawn is committed.
    """

    sources = _task_directories(payload)
    if not sources:
        return []
    compatibility = job.raw.get("compatibility")
    if not isinstance(compatibility, Mapping):
        raise RuntimeError("v1 compatibility metadata is missing")
    root_text = compatibility.get("root_placement", context.placement)
    root = normalize_placement(str(root_text))
    staging = draft.control / f"v1-spawn.tmp.{uuid.uuid4()}"
    staging.mkdir()
    results: list[tuple[ChildReference, str]] = []
    labels: set[str] = set()
    try:
        for source in sources:
            fields = parse_v1_task_name(source.name)
            if fields is None:
                continue
            relative = source.relative_to(payload)
            identity = f"{relative.as_posix()}|{fields['taskset']}|{fields['task_id']}"
            child_id = str(uuid.uuid5(uuid.UUID(job.id), identity))
            legacy_link = {
                "parent_job_id": job.id,
                "parent_job_key": job.job_key,
                "parent_placement": context.placement,
                "directory": relative.parent.as_posix(),
                "fields": fields,
            }
            prepared = staging / child_id
            shutil.copytree(source, prepared)
            mapping = _job_mapping(
                prepared,
                job_id=child_id,
                tag=f"{fields['taskset']}-{fields['task_id']}",
                name=f"httk v1 subtask {fields['task_id']}",
                initial_step=fields["step"],
                pool="default" if fields["taskset"] == "any" else fields["taskset"],
                priority=int(fields["priority"]),
                attempts=attempts,
                root_placement=root.as_posix(),
                legacy_link=legacy_link,
            )
            write_json_atomic(prepared / "job.json", mapping)
            label = _child_label(str(fields["task_id"]), labels)
            placement = _child_placement(root, job.id, child_id)
            # The draft owns the copy into ``children/jobs`` and the spawn set,
            # and stamps the parent identity on the child; the deterministic
            # ``child_id`` and placement keep the spawn stable across replays.
            reference = draft.add_child(prepared, placement, label=label)
            results.append((reference, label))
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return results


def _publish_outcome(
    control: Path,
    context: AttemptContext,
    job: JobDefinition,
    *,
    action: OutcomeAction,
    next_step: str | None = None,
    failure: Mapping[str, object] | None = None,
    priority: int | None = None,
    wait_for_children: bool = False,
    attempts: int,
) -> None:
    """Publish one v2 outcome through the canonical :class:`OutcomeDraft`.

    The draft is the single implementation of the on-disk outcome shape and its
    atomic publication, so a v1 attempt writes exactly the bytes a native runner
    would. Only a job that continues can have children: a legacy task that
    succeeded or failed may still have unconsumed ``ht.task.*.waitstart``
    directories — a task set it prepared and then abandoned — and publishing
    those as spawned jobs would strand real jobs behind a parent nothing joins.
    """

    draft = OutcomeDraft(context, control)
    children = (
        _spawn_children(
            draft,
            payload=Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]),
            job=job,
            context=context,
            attempts=attempts,
        )
        if action in {"advance", "wait"} or wait_for_children
        else []
    )
    published_action: OutcomeAction = "wait" if wait_for_children else action
    join: dict[str, object] | None = None
    if wait_for_children:
        join = {
            # v1 waitsubtasks resumed after every descendant left an active
            # state, including children which ended broken or stopped.
            "condition": "all_terminal",
            "children": [
                {
                    "workspace_id": reference.workspace_id,
                    "job_id": reference.job_id,
                    "job_key": reference.job_key,
                    "label": label,
                    "placement_hint": reference.placement_hint,
                }
                for reference, label in children
            ],
            "on_impossible": {"action": "fail"},
        }
    draft.publish(
        published_action,
        next_step=next_step,
        priority=priority,
        failure=failure,
        join=join,
    )


def _archive_log(payload: Path, workdir: Path, compression: str) -> None:
    source = payload / "ht.taskmgr.stdout"
    if not source.is_file():
        return
    destination = workdir / "ht.taskmgr.stdout"
    if source.resolve() != destination.resolve():
        shutil.copyfile(source, destination)
    if compression == "bzip2":
        compressed = destination.with_suffix(destination.suffix + ".bz2")
        compressed.write_bytes(bz2.compress(destination.read_bytes()))
        destination.unlink()
    elif compression == "zstd":
        try:
            subprocess.run(["zstd", "--rm", str(destination)], check=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            # The uncompressed log is still there and still complete, so this is
            # a disk-space disappointment rather than a lost outcome — but an
            # operator looking for a .zst that is not there deserves the reason.
            _note(f"the run log could not be compressed with zstd: {exc!r}", log_path=destination)
            return


def main() -> int:
    arguments = _arguments()
    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)
    context_path = Path(os.environ["HTTK_WORKFLOW_CONTEXT"])
    control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
    payload = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
    workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
    context = AttemptContext.read(context_path)
    job = JobDefinition.from_mapping(read_json(payload / "job.json"))
    compatibility = job.raw.get("compatibility")
    if not isinstance(compatibility, Mapping):
        raise RuntimeError("v1 compatibility metadata is missing")
    program_name = str(compatibility["program"])
    program_path = payload / program_name
    if program_name == "ht_steps":
        resume = payload / "ht.run.resume"
        if resume.is_dir() and not any(workdir.iterdir()):
            workdir.rmdir()
            os.rename(resume, workdir)
        current = workdir
    else:
        current = payload
    environment = _legacy_environment(
        arguments,
        job=job,
        context=context,
        payload=payload,
        current=current,
        program_name=program_name,
    )
    replay_v1_atomic(current)
    pending_step = _next_step(current)
    priority = _legacy_priority(current)
    if pending_step == "ht_finished":
        _archive_log(payload, current, arguments.log_compression)
        _publish_outcome(
            control,
            context,
            job,
            action="succeed",
            priority=priority,
            attempts=arguments.attempts,
        )
        return 0
    if pending_step == "ht_broken":
        _freeze(
            program_path,
            cwd=current,
            environment=environment,
            log_path=payload / "ht.taskmgr.stdout",
            wrapper=arguments.wrapper,
        )
        _publish_outcome(
            control,
            context,
            job,
            action="fail",
            failure={"code": "declared_failure", "message": "legacy task declared ht_broken"},
            priority=priority,
            attempts=arguments.attempts,
        )
        return 0
    if pending_step is not None and pending_step != context.step:
        children = bool(_task_directories(payload))
        _publish_outcome(
            control,
            context,
            job,
            action="advance",
            next_step=pending_step,
            priority=priority,
            wait_for_children=children,
            attempts=arguments.attempts,
        )
        return 0
    if context.attempt_ordinal is not None and context.attempt_ordinal > arguments.attempts + 1:
        _freeze(
            program_path,
            cwd=current,
            environment=environment,
            log_path=payload / "ht.taskmgr.stdout",
            wrapper=arguments.wrapper,
        )
        _publish_outcome(
            control,
            context,
            job,
            action="fail",
            failure={
                "code": "retry_exhausted",
                "message": "legacy manager attempt limit exceeded",
            },
            priority=priority,
            attempts=arguments.attempts,
        )
        return 0

    command = [str(program_path), str(context.step)]
    if arguments.wrapper is not None:
        command.insert(0, arguments.wrapper)
    log_path = payload / "ht.taskmgr.stdout"
    result, timed_out = _run_program(
        command,
        cwd=current,
        environment=environment,
        log_path=log_path,
        timeout=arguments.timeout,
    )
    replay_v1_atomic(current)
    next_step = _next_step(current)
    priority = _legacy_priority(current)

    if result == 10 or (program_name == "ht_run" and result == 0) or next_step == "ht_finished":
        _archive_log(payload, current, arguments.log_compression)
        _publish_outcome(
            control,
            context,
            job,
            action="succeed",
            priority=priority,
            attempts=arguments.attempts,
        )
    elif result == 2:
        if next_step is None:
            raise RuntimeError("legacy exit 2 requires ht.nextstep")
        _publish_outcome(
            control,
            context,
            job,
            action="advance",
            next_step=next_step,
            priority=priority,
            attempts=arguments.attempts,
        )
    elif result == 3:
        if next_step is None:
            raise RuntimeError("legacy exit 3 requires ht.nextstep")
        _publish_outcome(
            control,
            context,
            job,
            action="wait",
            next_step=next_step,
            priority=priority,
            wait_for_children=True,
            attempts=arguments.attempts,
        )
    elif result == 4 or next_step == "ht_broken":
        _freeze(
            program_path,
            cwd=current,
            environment=environment,
            log_path=log_path,
            wrapper=arguments.wrapper,
        )
        _publish_outcome(
            control,
            context,
            job,
            action="fail",
            failure={"code": "declared_failure", "message": "legacy task returned exit status 4"},
            priority=priority,
            attempts=arguments.attempts,
        )
    elif timed_out or result == 99:
        _freeze(
            program_path,
            cwd=current,
            environment=environment,
            log_path=log_path,
            wrapper=arguments.wrapper,
        )
        _publish_outcome(
            control,
            context,
            job,
            action="fail",
            failure={"code": "timeout", "message": "legacy task exceeded its manager timeout"},
            priority=priority,
            attempts=arguments.attempts,
        )
    else:
        _publish_outcome(
            control,
            context,
            job,
            action="fail",
            failure={
                "code": "process_failure",
                "message": f"legacy task returned undefined exit status {result}",
                "details": {"exit_status": result},
            },
            priority=priority,
            attempts=arguments.attempts,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
