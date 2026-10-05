"""Private outcome and commit decisions used by the task manager."""

import contextlib
import errno
import hashlib
import logging
import os
import shutil
import stat
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from ._job_tree import record_spawns
from ._jobdir import CONTROL_DOCUMENT_LIMIT, JobDirectory, JobDirectoryError
from ._util import require_int, require_string
from .errors import FormatError, TransactionError, UnsupportedExtensionError
from .models import (
    ATTEMPTS_DIRECTORY,
    Failure,
    JobDefinition,
    Marker,
    StateFrame,
    check_job_placement,
    is_payload_private,
    normalize_placement,
    parse_job_key,
    validate_failure,
    validate_label,
    validate_resources,
    validate_step,
)
from .workspace import _runner_content_digest

_LOGGER = logging.getLogger("httk.workflow.manager")


def _remove_attempt_control_tree(job_dir: JobDirectory, control_name: str, job_key: str) -> None:
    """Remove one validated attempt-control tree and its emptied container through descriptors.

    The tree is removed with the descriptor-based :meth:`JobDirectory.remove_tree`,
    so a symlink the job planted inside it, or in place of it, is unlinked and
    never followed.
    """

    job_dir.remove_tree(control_name)
    if job_dir.stat(control_name) is not None:
        raise OSError(f"attempt control tree remains: {job_dir.path / control_name}")
    try:
        os.rmdir(ATTEMPTS_DIRECTORY, dir_fd=job_dir.fd)
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno != errno.ENOTEMPTY:
            _LOGGER.warning("cannot remove empty attempt container for %s: %s", job_key, exc)


def _remove_committed_attempt_control(manager: Any, marker: Marker, state: StateFrame, destination: Marker) -> None:
    """Remove local committed control only after this manager has reaped it.

    A live local attempt is marked for cleanup and removed when its process is
    reaped. A committing marker recovered by a manager that never owned the
    process is deliberately left for ``attempt_control`` garbage collection.
    """

    if destination.kind in {"failed", "cancelled"}:
        return
    attempt_id = state.attempt_id or ""
    local = manager._running.get(attempt_id)
    if local is not None:
        local.cleanup_pending = True
        local.cleanup_request = (marker, state, destination)
        if not local.reaped:
            return
    elif attempt_id not in manager._reaped_attempts:
        # This is an inherited commit: no process exit was proven by this
        # manager, so the evidence remains for the collector.
        return
    try:
        with manager._job_directory(marker) as job_dir:
            _remove_attempt_control_tree(job_dir, manager._attempt_control_name(state), marker.job_key)
    except Exception as exc:
        _LOGGER.warning("cannot remove committed attempt control for %s: %s", marker.job_key, exc)
    finally:
        manager._reaped_attempts.discard(attempt_id)


def failure(code: str, message: str, *, exit_status: int | None = None) -> dict[str, object]:
    details: dict[str, object] = {"log_paths": ["logs/stdio.out"]}
    if exit_status is not None:
        details["exit_status"] = exit_status
    return Failure(code, message, details=details).as_mapping()


def nested_reason(outcome: Mapping[str, Any], key: str) -> str:
    value = outcome.get(key)
    if not isinstance(value, Mapping) or not isinstance(value.get("reason"), str):
        raise FormatError(f"{key} outcome requires a reason")
    return str(value["reason"])


def attempt_budget_failure(job: JobDefinition, attempt_ordinal: int, total_attempts: int) -> str | None:
    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and attempt_ordinal > per_activation:
        return "maximum_attempts_per_activation exceeded"
    total = job.retry_policy.maximum_total_attempts
    if total is not None and total_attempts > total:
        return "maximum_total_attempts exceeded"
    return None


def retry_budget_available(job: JobDefinition, state: StateFrame) -> bool:
    attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
    total = state.total_attempts if state.total_attempts is not None else attempts
    maximum = job.retry_policy.maximum_attempts_per_activation
    if maximum is not None and attempts >= maximum:
        return False
    maximum_total = job.retry_policy.maximum_total_attempts
    return maximum_total is None or total < maximum_total


def declared_runner_steps(marker: Marker, outcome: Mapping[str, Any], log: Any) -> list[str] | None:
    declared = outcome.get("runner_steps")
    if declared is None:
        return None
    if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
        log.warning("ignoring runner_steps of %s: not an array", marker.job_key)
        return None
    try:
        return [validate_step(item, "runner_steps item") for item in declared]
    except FormatError as exc:
        log.warning("ignoring runner_steps of %s: %s", marker.job_key, exc)
        return None


# A malformed or partial outcome is the signature of an outcome assembled in
# place instead of published by an atomic rename, so the remedy is attached to
# the parse and shape errors — not to the identity comparisons, where the
# outcome is well-formed but stale or foreign and the remedy would mislead.
_ASSEMBLY_REMEDY = (
    "; an outcome must be published by renaming a staged directory onto outcome.ready, never assembled in place"
)


def _draft_digest(directory: JobDirectory, name: str, digest: Callable[..., str]) -> str:
    """Digest one child bundle of a draft, anchored at its no-follow descriptor."""

    return directory.digest_tree(name, skip=is_payload_private, digest=digest)


def read_outcome(source: Path | JobDirectory, marker: Marker, state: StateFrame) -> dict[str, Any]:
    """Read and check one published outcome document.

    :param source: The pinned ``outcome.ready`` directory, or the path of an
        ``outcome.json`` (whose directory is then the trusted anchor).
    :param marker: The committing job.
    :param state: Its state frame, naming the attempt.
    :return: The outcome document.
    :raises httk.workflow.errors.FormatError: If the outcome is unreadable,
        malformed, foreign, a symlink, special file, or oversized.
    """

    try:
        if isinstance(source, JobDirectory):
            outcome = source.read_json("outcome.json", CONTROL_DOCUMENT_LIMIT)
        else:
            with JobDirectory.at(source.parent) as directory:
                outcome = directory.read_json(source.name, CONTROL_DOCUMENT_LIMIT)
    except JobDirectoryError:
        raise
    except FormatError as exc:
        raise FormatError(f"{exc}{_ASSEMBLY_REMEDY}") from exc
    except OSError as exc:
        raise FormatError(f"cannot read JSON object {source}: {exc}{_ASSEMBLY_REMEDY}") from exc
    if outcome.get("format") != "httk-workflow-outcome" or outcome.get("format_version") != 2:
        raise FormatError(f"outcome must use httk-workflow-outcome version 2{_ASSEMBLY_REMEDY}")
    for key, expected in (
        ("job_id", marker.job_id),
        ("activation_id", state.activation_id),
        ("attempt_id", state.attempt_id),
    ):
        if outcome.get(key) != expected:
            raise FormatError(f"outcome {key} is {outcome.get(key)!r} but this attempt is {expected!r}")
    if not isinstance(outcome.get("action"), str):
        raise FormatError(f"outcome action must be a string{_ASSEMBLY_REMEDY}")
    return outcome


def child_digests(outcome: JobDirectory, digest: Callable[..., str]) -> dict[str, str]:
    """Digest every child bundle a draft publishes, through no-follow descriptors.

    :param outcome: The pinned ``outcome.ready`` directory.
    :param digest: The tree-digest function.
    :return: Child job key to bundle digest.
    :raises JobDirectoryError: If a path to a bundle, or an entry in one, is a
        symlink or special file.
    """

    if not outcome.exists_dir("children/jobs"):
        return {}
    digests: dict[str, str] = {}
    with outcome.directory("children/jobs") as jobs:
        for name in sorted(os.listdir(jobs.fd)):
            information = jobs.stat(name)
            if information is None or stat.S_ISREG(information.st_mode):
                continue
            if not stat.S_ISDIR(information.st_mode):
                raise JobDirectoryError(f"child bundle {jobs.path / name} is a symlink or special file")
            digests[name] = _draft_digest(jobs, name, digest)
    return digests


def _spawn_document(outcome: JobDirectory) -> dict[str, Any] | None:
    """Return the draft's ``children/spawn.json``, or ``None`` when it publishes none."""

    information = outcome.stat("children/spawn.json")
    if information is None:
        return None
    if not stat.S_ISREG(information.st_mode):
        raise JobDirectoryError(f"{outcome.path / 'children' / 'spawn.json'} is not a regular file")
    return outcome.read_json("children/spawn.json", CONTROL_DOCUMENT_LIMIT)


def spawn_labels(outcome: JobDirectory) -> dict[str, str]:
    """Return the label of every spawned child, checking that the labels are unique.

    :param outcome: The pinned ``outcome.ready`` directory.
    :return: Child job key to label.
    :raises httk.workflow.errors.FormatError: If the spawn set is malformed, or
        ``spawn.json`` is a symlink, special file, or oversized.
    """

    spawn = _spawn_document(outcome)
    if spawn is None:
        return {}
    entries = spawn.get("children")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise FormatError("spawn children must be an array")
    labels: dict[str, str] = {}
    used: set[str] = set()
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise FormatError("spawn child must be an object")
        job_key = require_string(raw.get("job_key"), "spawn child job_key")
        label = validate_label(raw.get("label"), "spawn child label")
        if label in used:
            raise FormatError(f"spawn child label is not unique within the spawn set: {label}")
        used.add(label)
        labels[job_key] = label
    return labels


def labeled_join(join: Mapping[str, Any], outcome: JobDirectory) -> dict[str, object]:
    """Return a join whose child references carry the labels of the spawn set.

    :param join: The outcome's join object.
    :param outcome: The pinned ``outcome.ready`` directory.
    :return: The join with labels filled in.
    :raises httk.workflow.errors.FormatError: If the spawn set is malformed or tampered with.
    """

    labels = spawn_labels(outcome)
    children = join.get("children")
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return dict(join)
    referenced: list[object] = []
    for raw in children:
        if not isinstance(raw, Mapping):
            referenced.append(raw)
            continue
        reference = dict(raw)
        if reference.get("label") is None:
            label = labels.get(str(reference.get("job_key", "")))
            if label is not None:
                reference["label"] = label
        referenced.append(reference)
    return {**join, "children": referenced}


def context_children(join_summary: object) -> list[dict[str, object]]:
    if not isinstance(join_summary, Sequence) or isinstance(join_summary, (str, bytes)):
        return []
    return [dict(item) for item in join_summary if isinstance(item, Mapping)]


def _retain_deferred_pause_process(state: StateFrame, progress: StateFrame) -> StateFrame:
    """Retain the committing attempt identity when a pause is deferred."""

    if state.pause_requested is None:
        return progress
    if "process" not in state.members:
        raise FormatError("a deferred pause requires the committing process identity")
    return StateFrame.replace(progress, process=state.members["process"])


def _auto_seal_succeeded(manager: Any, destination: Marker) -> None:
    """Seal a just-succeeded job's payload, unless the workspace opts out.

    Sealing is a convenience, never part of the job's success: a missing signing
    key, an existing conflicting seal, or any filesystem error is logged and
    swallowed so a job that ran cleanly always stays succeeded.
    """

    workspace = manager.workspace
    raw = workspace.read_settings().get("seal.succeeded", True)
    if str(raw).strip().lower() in {"false", "0", "no", "off"}:
        return
    from . import seals
    from .errors import SealedError, SealError

    try:
        seals.seal_job(workspace, destination)
    except (SealError, SealedError, OSError, ValueError) as exc:
        _LOGGER.warning(
            "could not seal succeeded job %s: %s",
            destination.job_key,
            exc,
            extra={"event": "job_seal_failed", "job_key": destination.job_key, "reason": str(exc)},
        )


def _open_draft(control: JobDirectory) -> JobDirectory:
    """Open a committing attempt's published ``outcome.ready`` without following links."""

    try:
        return control.directory("outcome.ready")
    except FileNotFoundError as exc:
        raise FormatError(f"published outcome is missing: {control.path / 'outcome.ready'}") from exc


def process_committing(manager: Any, marker: Marker) -> None:
    """Apply a validated committing outcome through the manager's effects."""

    state = manager._read_frame(marker)
    job = manager.workspace.load_job(marker)
    # The draft is reached only through descriptors pinned without following
    # links, so whatever the job planted in it cannot redirect the commit.
    with (
        manager._open_attempt_control(marker, state) as control,
        manager._job_directory(marker) as job_dir,
        _open_draft(control) as draft,
    ):
        outcome_path = draft.path
        outcome = manager._read_outcome(draft, marker, state)
        data_generation = state.data_generation
        if draft.exists_dir("transaction"):
            if job.data_mode != "transactional" or data_generation is None:
                raise TransactionError("transaction published by a nontransactional job")
            if outcome.get("expected_data_generation") != data_generation:
                raise TransactionError("outcome expected_data_generation is stale")
            from .transactions import _replay_pinned

            with draft.directory("transaction") as transaction, job_dir.directory("data", create=True) as data:
                changed_data = _replay_pinned(
                    transaction,
                    data,
                    expected_generation=data_generation,
                    durable=manager.workspace.durable,
                )
            if changed_data:
                data_generation += 1
        manager._register_children(marker, state, draft)
        joined = outcome.get("join")
        labeled = (
            manager._labeled_join(joined, draft)
            if outcome["action"] == "wait" and isinstance(joined, Mapping)
            else None
        )
    executor = manager._executor_for(job)
    if executor is None:
        return
    from .executors import OutcomeCommit

    executor.commit_outcome(
        OutcomeCommit(
            job=job,
            marker=marker,
            payload=manager.workspace.payload_path(marker.placement, marker.job_key),
            outcome_path=outcome_path,
            outcome=outcome,
        )
    )
    action = outcome["action"]
    next_resources: dict[str, int] | None = None
    if "resources" in outcome:
        if action not in {"advance", "wait"}:
            raise FormatError("outcome resources are only valid for advance or wait")
        next_resources = validate_resources(outcome["resources"], "outcome.resources")
    progress = StateFrame.replace(state.carried(), data_generation=data_generation)
    declared_steps = manager._declared_runner_steps(marker, outcome)
    if declared_steps is not None:
        progress = StateFrame.replace(progress, runner_steps=declared_steps)
    progress = _retain_deferred_pause_process(state, progress)
    priority_raw = outcome.get("priority")
    next_priority = (
        marker.priority if priority_raw is None else require_int(priority_raw, "outcome.priority", maximum=999)
    )
    if action == "advance":
        destination = manager._advance(
            marker,
            job,
            state,
            validate_step(outcome.get("next_step"), "next_step"),
            progress,
            resources=next_resources,
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "retry":
        destination = manager._retry(
            marker, job, state, progress, nested_reason(outcome, "retry"), unclean=False, priority=next_priority
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "wait":
        next_step = validate_step(outcome.get("next_step"), "next_step")
        if labeled is None:
            raise FormatError("wait outcome requires a join object")
        destination = manager._transition(
            marker,
            "waiting",
            StateFrame.replace(
                progress,
                next_step=next_step,
                resources=next_resources,
                join=labeled,
                reason="waiting_for_children",
            ),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "succeed":
        destination = manager._transition(
            marker, "succeeded", StateFrame.replace(progress, reason="succeeded"), priority=next_priority
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
        _auto_seal_succeeded(manager, destination)
    elif action == "fail":
        try:
            failure_value = validate_failure(outcome.get("failure"))
        except FormatError as exc:
            failure_value = Failure(
                "protocol_error",
                f"runner published a malformed failure object: {exc}",
                details={"log_paths": ["logs/stdio.out"]},
            )
            reason = "protocol_error"
        else:
            reason = "declared_failure"
            if failure_value.retryable and manager._retry_budget_available(job, state):
                _LOGGER.info(
                    "retrying %s: the runner declared %s retryable",
                    marker.job_key,
                    failure_value.code,
                    extra=manager._event("declared_retry", marker, failure_code=failure_value.code),
                )
                destination = manager._retry(
                    marker, job, state, progress, failure_value.code, unclean=False, priority=next_priority
                )
                _remove_committed_attempt_control(manager, marker, state, destination)
                return
        destination = manager._transition(
            marker,
            "failed",
            StateFrame.replace(progress, failure=failure_value.as_mapping(), reason=reason),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "pause":
        paused = progress
        if "process" in state.members:
            paused = StateFrame.replace(paused, process=state.members["process"])
        destination = manager._transition(
            marker,
            "paused",
            StateFrame.replace(paused, pause=outcome.get("pause"), reason="step_paused"),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    else:
        raise FormatError(f"unsupported outcome action: {action!r}")


def advance(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    state: StateFrame,
    next_step: str,
    progress: StateFrame,
    *,
    reason: str = "advance",
    join_summary: Sequence[object] | None = None,
    resources: Mapping[str, int] | None = None,
    priority: int | None = None,
) -> Marker:
    progress = _retain_deferred_pause_process(state, progress)
    activation_ordinal = (state.activation_ordinal if state.activation_ordinal is not None else 1) + 1
    maximum = job.retry_policy.maximum_activations
    if maximum is not None and activation_ordinal > maximum:
        return manager._transition(
            marker,
            "failed",
            StateFrame.replace(
                progress, failure=failure("budget_exhausted", "maximum_activations exceeded"), reason="budget_exhausted"
            ),
        )
    return manager._transition(
        marker,
        "ready",
        StateFrame.replace(
            progress,
            step=next_step,
            activation_id=str(uuid.uuid4()),
            activation_ordinal=activation_ordinal,
            attempt_ordinal=0,
            reason=reason,
            join_summary=join_summary,
            resources=resources,
            previous_attempt_id=state.attempt_id,
        ),
        priority=priority,
    )


def retry(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    state: StateFrame,
    progress: StateFrame,
    reason: str,
    *,
    unclean: bool,
    takeover_evidence: Mapping[str, object] | None = None,
    priority: int | None = None,
) -> Marker:
    progress = _retain_deferred_pause_process(state, progress)
    current_attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
    maximum = job.retry_policy.maximum_attempts_per_activation
    if maximum is not None and current_attempts >= maximum:
        return manager._transition(
            marker,
            "failed",
            StateFrame.replace(progress, failure=failure("retry_exhausted", reason), reason="retry_exhausted"),
        )
    evidence_value = {} if takeover_evidence is None else dict(takeover_evidence)
    retried = StateFrame.replace(
        progress,
        reason=reason,
        unclean_restart=unclean,
        unsafe_persistent_takeover=evidence_value.get("evidence") == "unsafe_persistent_takeover",
        previous_attempt_id=state.attempt_id,
    )
    if takeover_evidence is not None:
        retried = StateFrame.replace(retried, takeover_evidence=evidence_value)
    return manager._transition(marker, "ready", retried, priority=priority)


def child_staging_name(attempt_id: str, job_key: str) -> str:
    """Return the deterministic staging name of one spawned child bundle.

    A child bundle is renamed out of the job-writable draft into the manager's
    ``.httk-workspace/tmp`` before it is verified and published. The name is
    derived from the attempt and the child key, never random, so a commit
    replayed after a crash between that rename and the publication finds the
    bundle again in staging rather than missing from the draft.

    :param attempt_id: The attempt whose outcome spawned the child.
    :param job_key: The child's job key.
    :return: The staging entry name.
    """

    return f"child.{attempt_id}.{job_key}"


def _validated_spawn_entries(manager: Any, entries: Sequence[Mapping[str, object]]) -> list[tuple[str, PurePosixPath]]:
    """Validate every spawn entry, before anything durable is written for any of them."""

    validated: list[tuple[str, PurePosixPath]] = []
    for raw in entries:
        job_key = require_string(raw.get("job_key"), "spawn child job_key")
        parse_job_key(job_key)
        placement = normalize_placement(str(raw.get("placement", "")))
        check_job_placement(placement)
        if raw.get("workspace_id", manager.workspace.workspace_id) != manager.workspace.workspace_id:
            raise UnsupportedExtensionError("cross-workspace spawn children are not supported")
        validated.append((job_key, placement))
    return validated


def _stage_child(jobs: JobDirectory | None, staging: JobDirectory, job_key: str, staged: str) -> bool:
    """Move a child bundle out of the draft into staging; report whether one is staged.

    A bundle already in staging (from a commit interrupted after this rename)
    takes precedence: whatever the job put back in the draft since is ignored.
    """

    present = staging.stat(staged)
    if present is not None:
        if not stat.S_ISDIR(present.st_mode):
            raise JobDirectoryError(f"staged child {staging.path / staged} is not a real directory")
        return True
    if jobs is None:
        return False
    information = jobs.stat(job_key)
    if information is None:
        return False
    if not stat.S_ISDIR(information.st_mode):
        raise JobDirectoryError(f"child bundle {jobs.path / job_key} is a symlink or not a directory")
    jobs.rename_out(job_key, staging.fd, staged)
    return True


def _verify_staged(
    staging: JobDirectory, staged: str, job_key: str, expected: str, digest: Callable[..., str]
) -> JobDefinition:
    """Verify a staged child bundle and return its job definition.

    A refused bundle is left in staging as evidence; the parent fails, so its
    commit is never replayed, and ``tmp_entries`` collection sweeps the entry
    once it has aged and the parent is no longer committing.
    """

    child = JobDefinition.from_path(staging.path / staged / "job.json")
    if child.job_key != job_key:
        raise FormatError("spawn job_key disagrees with child job.json")
    if staging.digest_tree(staged, skip=is_payload_private, digest=digest) != expected:
        raise FormatError("spawn child changed after outcome publication")
    return child


#: Where :meth:`httk.workflow.Attempt.call` stages the runners of called
#: workflows in an outcome draft, keyed by their workspace store path.
STAGED_RUNNERS = PurePosixPath("children/runners")

_COPY_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC


def runner_staging_name(attempt_id: str, path: PurePosixPath) -> str:
    """Return the deterministic staging name of one staged runner's copy.

    A runner staged in a draft is copied into the manager's
    ``.httk-workspace/tmp`` and verified and published from that copy. The name
    is derived from the attempt and the store path (hashed, so a nested or long
    store path still names a single bounded entry), never random, so ``gc``
    can tell a copy of an unfinished commit from an abandoned one. A commit
    replayed after a crash never trusts a copy left behind: it removes it and
    copies the draft's runner again, because a copy interrupted midway is
    incomplete. The draft holds the staged runner until the commit finishes, and
    publication of an equal digest is a no-op, so a replay publishes the same
    runner exactly once.

    :param attempt_id: The attempt whose outcome staged the runner.
    :param path: The runner's workspace store path.
    :return: The staging entry name.
    """

    return f"runner.{attempt_id}.{hashlib.sha256(path.as_posix().encode()).hexdigest()[:32]}"


def _copy_file(source: JobDirectory, relative: str | PurePosixPath, target: JobDirectory, name: str) -> None:
    """Copy one regular file through no-follow descriptors into a new entry of *target*."""

    with (
        open(source.open_read(relative), "rb") as reader,
        open(os.open(name, _COPY_FLAGS, 0o600, dir_fd=target.fd), "wb") as writer,
    ):
        shutil.copyfileobj(reader, writer)


def _copy_staged_runner(outcome: JobDirectory, relative: PurePosixPath, staging: JobDirectory, name: str) -> None:
    """Copy one staged runner file or tree out of the draft, refusing links and special files.

    Every entry is opened without following links below the pinned draft, so a
    symlink or special file anywhere in the staged runner is a
    :class:`~httk.workflow._jobdir.JobDirectoryError`, never followed or read.
    """

    information = outcome.stat(relative)
    if information is not None and stat.S_ISREG(information.st_mode):
        _copy_file(outcome, relative, staging, name)
        return
    if information is None or not stat.S_ISDIR(information.st_mode):
        raise JobDirectoryError(f"staged runner {outcome.path / relative} is a symlink or special file")
    with outcome.directory(relative) as tree, staging.directory(name, create=True, exclusive=True, mode=0o700) as copy:
        pending = [PurePosixPath()]
        while pending:
            current = pending.pop()
            with contextlib.ExitStack() as handles:
                source = handles.enter_context(tree.directory(current)) if current.parts else tree
                target = handles.enter_context(copy.directory(current)) if current.parts else copy
                for entry in sorted(os.listdir(source.fd)):
                    entry_information = source.stat(entry)
                    if entry_information is None:
                        continue
                    if stat.S_ISDIR(entry_information.st_mode):
                        os.mkdir(entry, 0o700, dir_fd=target.fd)
                        pending.append(current / entry)
                    elif stat.S_ISREG(entry_information.st_mode):
                        _copy_file(source, entry, target, entry)
                    else:
                        raise JobDirectoryError(f"staged runner {source.path / entry} is a symlink or special file")


def _publish_staged_runners(
    manager: Any, outcome: JobDirectory, staging: JobDirectory, attempt_id: str, children: Iterable[JobDefinition]
) -> None:
    """Publish the runners staged in a draft for the verified children that reference them.

    For each workspace runner a verified child pins whose store path is staged
    at ``children/runners/<path>`` in the draft, the staged entry is copied into
    manager-owned staging (:func:`runner_staging_name`), the copy is digested
    with the store's own digest rule and must equal the child's pinned
    ``sha256``, and only then is the copy published with
    :meth:`~httk.workflow.Workspace.publish_runner` (a no-op when the store
    already holds that digest) and removed. Staged runners no child references
    are ignored; a referenced runner that is not staged is left to the store,
    where a missing one fails the child as ``runner_unavailable`` when claimed.
    """

    pinned: dict[PurePosixPath, set[str]] = {}
    for child in children:
        if child.runner_source == "workspace" and child.runner_sha256 is not None:
            pinned.setdefault(child.runner_path, set()).add(child.runner_sha256)
    for path, digests in pinned.items():
        relative = STAGED_RUNNERS / path
        if outcome.stat(relative) is None:
            continue
        if len(digests) != 1:
            raise FormatError(f"spawned children pin staged workspace runner {path} to different digests")
        (expected,) = digests
        name = runner_staging_name(attempt_id, path)
        # A copy left by an interrupted commit may be incomplete: never reuse it.
        staging.remove_tree(name)
        try:
            _copy_staged_runner(outcome, relative, staging, name)
            copied = staging.path / name
            actual = _runner_content_digest(copied)[1]
            if actual != expected:
                raise FormatError(f"staged workspace runner {path} has digest {actual}, but its child pins {expected}")
            try:
                manager.workspace.publish_runner(copied, name=path)
            except FileExistsError as exc:
                raise FormatError(f"cannot publish staged workspace runner {path}: {exc}") from exc
        finally:
            staging.remove_tree(name)


def register_children(
    manager: Any, marker: Marker, state: StateFrame, outcome: JobDirectory, digest: Callable[..., str]
) -> None:
    """Register the children one committed outcome spawned.

    Every spawn entry is validated (job key, placement rule, workspace) before
    the parent's durable spawn record is written, so a refused spawn leaves no
    record. Each child bundle is then renamed out of the job-writable draft
    into manager-owned staging (:func:`child_staging_name`), verified there —
    a real directory whose ``job.json`` names the key and whose digest is the
    one recorded when the outcome was accepted. Runners the verified children
    pin and the draft stages under ``children/runners/`` are then copied into
    staging, verified against those pins and published into the workspace
    runner store (:func:`_publish_staged_runners`), and only then is each child
    published at ``<placement>/<job_key>``. A job that keeps writing its draft
    after publication therefore cannot change what was verified.

    :param manager: The committing task manager.
    :param marker: The committing parent.
    :param state: The parent's committing frame.
    :param outcome: The pinned ``outcome.ready`` directory.
    :param digest: The tree-digest function.
    :raises httk.workflow.errors.FormatError: If the spawn set or a child bundle
        is malformed, tampered with, or changed since the outcome was accepted.
    :raises httk.workflow.errors.UnsupportedExtensionError: If a child names another workspace.
    """

    if not outcome.exists_dir("children"):
        return
    spawn = _spawn_document(outcome)
    if spawn is None:
        raise FormatError(f"cannot read JSON object {outcome.path / 'children' / 'spawn.json'}: it is missing")
    entries = spawn.get("children")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise FormatError("spawn children must be an array")
    manager._spawn_labels(outcome)
    expected_digests = state.child_digests
    if expected_digests is None and state.has("child_digests"):
        raise FormatError("committing child_digests must be an object")
    expected_digests = {} if expected_digests is None else expected_digests
    if not all(isinstance(raw, Mapping) for raw in entries):
        raise FormatError("spawn child must be an object")
    if not state.attempt_id:
        raise FormatError("a committing frame that registers children must name its attempt")
    validated = _validated_spawn_entries(manager, entries)
    # The parent's durable record of its children precedes every child it names,
    # so a child can never exist without its parent knowing it (tree transfers
    # rely on this); a replay must find the record byte-identical.
    with manager._job_directory(marker) as job_dir:
        record_spawns(job_dir, state.attempt_id, entries, durable=manager.workspace.durable)
    staging_path = manager.workspace.control / "tmp"
    staging_path.mkdir(parents=True, exist_ok=True)
    with JobDirectory.at(staging_path) as staging, contextlib.ExitStack() as handles:
        jobs = (
            handles.enter_context(outcome.directory("children/jobs")) if outcome.exists_dir("children/jobs") else None
        )
        # Every child still to publish is moved out of the draft and verified
        # first, then the runners those verified children pin are published, and
        # only then does any child appear: a child never exists before its
        # staged runner, and a refused runner publishes no child.
        verified: dict[str, JobDefinition] = {}
        for job_key, _placement in validated:
            staged = child_staging_name(state.attempt_id, job_key)
            if _stage_child(jobs, staging, job_key, staged):
                expected_digest = str(expected_digests.get(job_key, ""))
                verified[job_key] = _verify_staged(staging, staged, job_key, expected_digest, digest)
        _publish_staged_runners(manager, outcome, staging, state.attempt_id, verified.values())
        for job_key, placement in validated:
            expected_digest = str(expected_digests.get(job_key, ""))
            target = manager.workspace.payload_path(placement, job_key)
            staged = child_staging_name(state.attempt_id, job_key)
            published_here = False
            if verified.pop(job_key, None) is not None:
                target.parent.mkdir(parents=True, exist_ok=True)
                manager.workspace._publish_path(staging.path / staged, target)
                published_here = True
            if not target.is_dir():
                raise FormatError(f"registered child bundle does not match: {job_key}")
            if manager.workspace.find_marker_at(job_key, placement) is not None:
                continue
            if not published_here:
                try:
                    with JobDirectory.open(manager.workspace.root, placement, job_key) as published:
                        published_digest = published.digest_tree(skip=is_payload_private, digest=digest)
                except FileNotFoundError as exc:
                    raise FormatError(f"registered child bundle does not match: {job_key}") from exc
                if published_digest != expected_digest:
                    raise FormatError(f"registered child bundle does not match: {job_key}")
            child = JobDefinition.from_path(target / "job.json")
            temporary = manager.workspace.control / "tmp" / f"child-marker.{uuid.uuid4()}"
            temporary.touch(exist_ok=False)
            destination = manager.workspace.marker_path("submitted", placement, job_key, child.priority, 0, "init")
            manager.workspace._publish_path(temporary, destination)


def handle_attempt_failure(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    code: str,
    message: str,
    *,
    exit_status: int | None = None,
    unclean: bool = True,
    takeover_evidence: Mapping[str, object] | None = None,
    logger: Any,
) -> None:
    logger.warning(
        "attempt of %s failed with %s: %s",
        marker.job_key,
        code,
        message,
        extra=manager._event("attempt_failure", marker, failure_code=code, exit_status=exit_status),
    )
    try:
        state = manager._read_frame(marker)
        progress = state.carried()
        if code in job.retry_policy.retry_on:
            manager._retry(marker, job, state, progress, code, unclean=unclean, takeover_evidence=takeover_evidence)
            return
        failed = StateFrame.replace(progress, failure=failure(code, message, exit_status=exit_status), reason=code)
        if "process" in state.members:
            failed = StateFrame.replace(failed, process=state.members["process"])
        if takeover_evidence is not None:
            failed = StateFrame.replace(failed, takeover_evidence=dict(takeover_evidence))
        manager._transition(marker, "failed", failed)
    except Exception as exc:
        from .errors import TransitionLostError, WorkflowError

        if isinstance(exc, TransitionLostError):
            logger.debug("failure record for %s was lost to another actor", marker.job_key)
        elif isinstance(exc, (WorkflowError, OSError)):
            manager._report_anomaly(
                f"failure:{marker.job_key}",
                f"cannot record the {code} failure of {marker.job_key}: {exc}",
                manager._event("failure_error", marker, failure_code=code),
            )
        else:
            raise


def resume(manager: Any, logger: Any) -> bool:
    from .errors import (
        TransitionLostError,
        WorkflowError,
    )

    changed = False
    for marker in manager._window("resume_committing", "committing"):
        manager._pace()
        loaded = manager._load_job_and_state(marker, "resume_committing")
        if loaded is None:
            continue
        job, state = loaded
        if manager._executor_for(job) is None:
            logger.debug(
                "skipping committing job %s: runner executor %s is not served here", marker.job_key, job.runner_executor
            )
            continue
        try:
            manager._process_committing(marker)
        except TransitionLostError:
            pass
        except (FormatError, TransactionError) as exc:
            # FormatError covers JobDirectoryError: a symlink or special file the
            # job planted in its draft or data, or draft content a digest cannot
            # describe, is the job's own protocol violation, never a reason to
            # stop the manager.
            # A replay that fails midway is transaction corruption; a manifest or
            # outcome the manager cannot parse is a protocol violation of the
            # runner. Both used to be reported as corruption, which lied about a
            # malformed outcome.
            code = "transaction_corruption" if isinstance(exc, TransactionError) else "protocol_error"
            logger.error("commit of %s failed: %s", marker.job_key, exc, extra=manager._event("commit_failed", marker))
            try:
                manager._transition(
                    marker,
                    "failed",
                    StateFrame.replace(
                        StateFrame.replace(state.carried(), process=state.members["process"])
                        if "process" in state.members
                        else state.carried(),
                        failure=manager._failure(code, str(exc)),
                        reason="commit_failed",
                    ),
                )
            except TransitionLostError:
                pass
        except (WorkflowError, OSError) as exc:
            manager._report_anomaly(
                f"resume_committing:{marker.job_key}",
                f"cannot resume the commit of {marker.job_key}: {exc}",
                manager._event("commit_error", marker),
            )
        changed = True
    return changed
