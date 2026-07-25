"""Internal process adapter for an httk v1 ``ht_steps`` or ``ht_run``."""

import argparse
import bz2
import os
import shutil
import signal
import subprocess
import sys
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

# This file is executed by absolute path. Make source-tree execution work even
# when a relative PYTHONPATH no longer resolves from the job workspace.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from httk.workflow._util import read_json, write_json_atomic
from httk.workflow.models import JobDefinition, normalize_placement
from httk.workflow.v1 import (
    V1_PRIORITY_MAP,
    _job_mapping,
    parse_v1_task_name,
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


def replay_v1_atomic(workspace: Path) -> None:
    """Idempotently finish legacy ``ht.atomic.*`` directories."""

    for temporary in workspace.glob("ht.tmp.atomic.*"):
        _remove(temporary)
    for atomic in sorted(workspace.glob("ht.atomic.*")):
        if not atomic.is_dir():
            continue
        for instruction in sorted(atomic.glob("ht.atommv.*")):
            source_name = instruction.name.removeprefix("ht.atommv.")
            destination_text = instruction.read_text(encoding="utf-8").strip()
            if not source_name or not destination_text:
                raise RuntimeError(f"invalid legacy atomic move instruction: {instruction}")
            source = workspace / source_name
            destination = workspace / destination_text
            if source.exists() or source.is_symlink():
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.exists() or destination.is_symlink():
                    _remove(destination)
                os.rename(source, destination)
            elif not destination.exists() and not destination.is_symlink():
                raise RuntimeError(f"legacy atomic move lost both source and destination: {source_name}")
            instruction.unlink(missing_ok=True)
        for entry in sorted(atomic.iterdir()):
            destination = workspace / entry.name
            if destination.exists() or destination.is_symlink():
                _remove(destination)
            os.rename(entry, destination)
        atomic.rmdir()
    for temporary in workspace.glob("ht.tmp.atomic.*"):
        _remove(temporary)


def _next_step(workspace: Path) -> str | None:
    path = workspace / "ht.nextstep"
    if not path.is_file():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def _legacy_priority(workspace: Path) -> int | None:
    path = workspace / "ht.priority"
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


def _freeze(
    program_path: Path,
    *,
    cwd: Path,
    environment: Mapping[str, str],
    log_path: Path,
    wrapper: str | None,
) -> None:
    if program_path.name != "ht_steps":
        return
    (cwd / "ht.nextstep").unlink(missing_ok=True)
    command = [str(program_path), "freeze"]
    if wrapper is not None:
        command.insert(0, wrapper)
    try:
        _run_program(command, cwd=cwd, environment=environment, log_path=log_path, timeout=300.0)
        replay_v1_atomic(cwd)
    except Exception:
        return


def _legacy_environment(
    arguments: argparse.Namespace,
    *,
    job: JobDefinition,
    context: Mapping[str, Any],
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
            "HT_TASK_STEP": str(context["step"]),
            "HT_TASKMGR_TIMEOUT": str(int(arguments.timeout)),
            "HT_TASKMGR_SET": job.claim_pool,
            "HT_TASKMGR_ROOTDIR": str(Path(os.environ["HTTK_WORKFLOW_STORE_DIR"])),
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


def _prepare_children(
    outcome_temporary: Path,
    *,
    payload: Path,
    job: JobDefinition,
    context: Mapping[str, Any],
    attempts: int,
) -> list[dict[str, object]]:
    sources = _task_directories(payload)
    if not sources:
        return []
    compatibility = job.raw.get("compatibility")
    if not isinstance(compatibility, Mapping):
        raise RuntimeError("v1 compatibility metadata is missing")
    root_text = compatibility.get("root_placement", context.get("placement"))
    root = normalize_placement(str(root_text))
    children_root = outcome_temporary / "children"
    jobs_root = children_root / "jobs"
    jobs_root.mkdir(parents=True)
    entries: list[dict[str, object]] = []
    for source in sources:
        fields = parse_v1_task_name(source.name)
        if fields is None:
            continue
        relative = source.relative_to(payload)
        identity = f"{relative.as_posix()}|{fields['taskset']}|{fields['task_id']}"
        child_id = str(uuid.uuid5(uuid.UUID(job.id), identity))
        tag = f"{fields['taskset']}-{fields['task_id']}"
        child_payload = jobs_root / child_id
        legacy_link = {
            "parent_job_id": job.id,
            "parent_job_key": job.job_key,
            "parent_placement": str(context["placement"]),
            "directory": relative.parent.as_posix(),
            "fields": fields,
        }
        parent = {
            "store_id": context["store_id"],
            "job_id": job.id,
            "job_key": job.job_key,
        }
        shutil.copytree(source, child_payload)
        mapping = _job_mapping(
            child_payload,
            job_id=child_id,
            tag=tag,
            name=f"httk v1 subtask {fields['task_id']}",
            initial_step=fields["step"],
            pool="default" if fields["taskset"] == "any" else fields["taskset"],
            priority=int(fields["priority"]),
            attempts=attempts,
            parent=parent,
            root_placement=root.as_posix(),
            legacy_link=legacy_link,
        )
        write_json_atomic(child_payload / "job.json", mapping)
        child_job = JobDefinition.from_mapping(mapping)
        placement = _child_placement(root, job.id, child_id)
        entries.append(
            {
                "store_id": context["store_id"],
                "job_id": child_id,
                "job_key": child_job.job_key,
                "placement": placement.as_posix(),
                "compatibility": {"legacy_path": relative.as_posix()},
            }
        )
        child_payload.rename(jobs_root / child_job.job_key)
    write_json_atomic(
        children_root / "spawn.json",
        {
            "format": "httk-workflow-spawn",
            "format_version": 1,
            "children": entries,
        },
    )
    return entries


def _publish_outcome(
    control: Path,
    context: Mapping[str, Any],
    job: JobDefinition,
    *,
    action: str,
    next_step: str | None = None,
    failure: Mapping[str, object] | None = None,
    priority: int | None = None,
    wait_for_children: bool = False,
    attempts: int,
) -> None:
    temporary = control / f"outcome.tmp.{uuid.uuid4()}"
    temporary.mkdir()
    children = _prepare_children(
        temporary,
        payload=Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]),
        job=job,
        context=context,
        attempts=attempts,
    )
    body: dict[str, object] = {
        "format": "httk-workflow-outcome",
        "format_version": 1,
        "job_id": context["job_id"],
        "activation_id": context["activation_id"],
        "attempt_id": context["attempt_id"],
        "action": action,
    }
    if next_step is not None:
        body["next_step"] = next_step
    if failure is not None:
        body["failure"] = dict(failure)
    if priority is not None:
        body["priority"] = priority
    if wait_for_children:
        body["action"] = "wait"
        body["join"] = {
            # v1 waitsubtasks resumed after every descendant left an active
            # state, including children which ended broken or stopped.
            "condition": "all_terminal",
            "children": [
                {
                    "store_id": entry["store_id"],
                    "job_id": entry["job_id"],
                    "job_key": entry["job_key"],
                    "placement_hint": entry["placement"],
                }
                for entry in children
            ],
            "on_impossible": {"action": "fail"},
        }
    write_json_atomic(temporary / "outcome.json", body)
    os.rename(temporary, control / "outcome.ready")


def _archive_log(payload: Path, workspace: Path, compression: str) -> None:
    source = payload / "ht.taskmgr.stdout"
    if not source.is_file():
        return
    destination = workspace / "ht.taskmgr.stdout"
    if source.resolve() != destination.resolve():
        shutil.copyfile(source, destination)
    if compression == "bzip2":
        compressed = destination.with_suffix(destination.suffix + ".bz2")
        compressed.write_bytes(bz2.compress(destination.read_bytes()))
        destination.unlink()
    elif compression == "zstd":
        try:
            subprocess.run(["zstd", "--rm", str(destination)], check=True)
        except (OSError, subprocess.CalledProcessError):
            return


def main() -> int:
    arguments = _arguments()
    signal.signal(signal.SIGTERM, _forward_signal)
    signal.signal(signal.SIGINT, _forward_signal)
    context_path = Path(os.environ["HTTK_WORKFLOW_CONTEXT"])
    control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
    payload = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
    workspace = Path(os.environ["HTTK_WORKFLOW_RUN_DIR"])
    context = read_json(context_path)
    job = JobDefinition.from_mapping(read_json(payload / "job.json"))
    compatibility = job.raw.get("compatibility")
    if not isinstance(compatibility, Mapping):
        raise RuntimeError("v1 compatibility metadata is missing")
    program_name = str(compatibility["program"])
    program_path = payload / program_name
    if program_name == "ht_steps":
        resume = payload / "ht.run.resume"
        if resume.is_dir() and not any(workspace.iterdir()):
            workspace.rmdir()
            os.rename(resume, workspace)
        current = workspace
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
            failure={"class": "declared_failure", "summary": "legacy task declared ht_broken"},
            priority=priority,
            attempts=arguments.attempts,
        )
        return 0
    if pending_step is not None and pending_step != context["step"]:
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
    if int(context["attempt_ordinal"]) > arguments.attempts + 1:
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
                "class": "retry_exhausted",
                "summary": "legacy manager attempt limit exceeded",
            },
            priority=priority,
            attempts=arguments.attempts,
        )
        return 0

    command = [str(program_path), str(context["step"])]
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
            failure={"class": "declared_failure", "summary": "legacy task returned exit status 4"},
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
            failure={"class": "timeout", "summary": "legacy task exceeded its manager timeout"},
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
                "class": "process_failure",
                "summary": f"legacy task returned undefined exit status {result}",
                "exit_status": result,
            },
            priority=priority,
            attempts=arguments.attempts,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
