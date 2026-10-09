"""Private scheduling decisions used by :mod:`httk.workflow.manager`: eligibility, resources and the census."""

import functools
import logging
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from httk.core.requirements import parse_requirements, unmet_requirements

from . import _kernel
from ._durations import TIME_RESOURCES
from ._job import JobDefinition
from ._manager_binding import INVENTORY_LABELS, can_assign, fits
from ._state import StateDoc, read_state_unowned
from .errors import FormatError

if TYPE_CHECKING:
    from .manager import TaskManager, WorkCensus

_LOGGER = logging.getLogger("httk.workflow.manager")

#: Why a job is not this manager's work: ``(kind, name)``, the :class:`~httk.workflow.manager.WorkCensus` keys.
type Blocker = tuple[str, str]


@functools.cache
def unmet_job_requirements(requires: tuple[str, ...]) -> tuple[str, ...]:
    """Return which of a workflow's ``requires`` this process's environment does not meet.

    Memoized for the process lifetime: installed distributions do not change
    under a running manager, and one campaign repeats one requirement tuple.

    :param requires: The requirement strings.
    :return: The unmet ones.
    """

    return unmet_requirements(parse_requirements(list(requires), "requires")) if requires else ()


def current_step(job: JobDefinition, doc: StateDoc | None) -> str:
    """Return the step the job's next attempt runs: its activation's, or the initial step before any.

    :param job: The job.
    :param doc: Its ``state.json``, or ``None`` before the first claim.
    :return: The step name.
    """

    step = None if doc is None or doc.activation is None else doc.activation.get("step")
    return step if isinstance(step, str) else job.initial_step


def effective_requirement(
    job: JobDefinition,
    doc: StateDoc | None,
    capacity: Mapping[str, int],
    maximum_workers: int,
    *,
    whole_nodes: bool = False,
) -> dict[str, int]:
    """Return the resource requirement selected for the job's next attempt.

    Consumable resources resolve wholesale (the dynamic requirement an outcome
    declared, then the step's, then the job's) and gain this manager's fair
    share of ``procs`` and ``mem``; a mapping that names only time labels leaves
    that choice to the next level. The time labels resolve one label at a time
    through the same precedence. With *whole_nodes* (a manager placing on a node
    inventory), a requirement of ``nodes`` gets no fair share.

    :param job: The job.
    :param doc: Its ``state.json``, or ``None``.
    :param capacity: The manager's capacity.
    :param maximum_workers: The manager's worker count.
    :param whole_nodes: Whether nodes are given whole.
    :return: The requirement.
    """

    levels = _levels(job, doc)
    selected = next(mapping for mapping in (*levels, {}) if not mapping or mapping.keys() - TIME_RESOURCES)
    requirement = consumable(selected)
    for name in ("procs", "mem"):
        if whole_nodes and requirement.get("nodes", 0) >= 1:
            break
        if name in capacity and name not in requirement:
            share = capacity[name] // maximum_workers
            if share == 0 and capacity[name] > 0:
                share = capacity[name]
            requirement[name] = share
    times: dict[str, int] = {}
    for name in sorted(TIME_RESOURCES):
        found = next((mapping[name] for mapping in levels if name in mapping), None)
        if found is not None:
            times[name] = found
    if "maxtime" in times and times.get("mintime", 0) > times["maxtime"]:
        times["mintime"] = times["maxtime"]
    return requirement | times


def _levels(job: JobDefinition, doc: StateDoc | None) -> list[Mapping[str, int]]:
    # The frozen JSON values below are validated resource mappings: names to integers.
    step = job.step_resources.get(current_step(job, doc))
    candidates: list[object] = [None if doc is None else doc.resources, step, job.resources]
    return [mapping for mapping in candidates if isinstance(mapping, Mapping)]


def consumable(requirement: Mapping[str, int]) -> dict[str, int]:
    """Return the part of *requirement* counted against manager capacity.

    :param requirement: A requirement.
    :return: Its non-time labels.
    """

    return {name: value for name, value in requirement.items() if name not in TIME_RESOURCES}


def unfit_resource(requirement: Mapping[str, int], capacity: Mapping[str, int]) -> str | None:
    """Return the first sorted consumable resource key that cannot fit the capacity.

    :param requirement: A requirement.
    :param capacity: A capacity.
    :return: The resource, or ``None``.
    """

    for name in sorted(consumable(requirement)):
        value = requirement[name]
        if name not in capacity or capacity[name] <= 0 or value > capacity[name]:
            return name
    return None


def unplaceable_resource(manager: "TaskManager", requirement: Mapping[str, int]) -> str | None:
    """Return the first resource that keeps *requirement* from ever fitting this manager.

    Beyond :func:`unfit_resource`, a manager with a node inventory also needs the
    labels it places to fit its nodes; the first of ``nodes``, ``procs``, ``gpus``,
    ``mem`` whose addition makes them not fit is reported.

    :param manager: The manager.
    :param requirement: A requirement.
    :return: The resource, or ``None``.
    """

    missing = unfit_resource(requirement, manager.resources)
    inventory = manager._inventory
    if missing is not None or inventory is None:
        return missing
    labels = [name for name in INVENTORY_LABELS if name in requirement and name in inventory.labels]
    for index, label in enumerate(labels):
        if not fits(inventory, {name: requirement[name] for name in labels[: index + 1]}):
            return label
    return None


def available_resources(capacity: Mapping[str, int], running: Iterable[Any]) -> dict[str, int]:
    """Return capacity remaining after reservations of locally running attempts.

    :param capacity: The manager's capacity.
    :param running: The running attempts (each with ``resources``).
    :return: The remaining capacity.
    """

    available = dict(capacity)
    for attempt in running:
        for name, value in attempt.resources.items():
            if name in available:
                available[name] = max(0, available[name] - value)
    return available


def time_left(manager: "TaskManager") -> float | None:
    """Return the seconds until the manager's drain start, negative once past, or ``None`` when unknown.

    :param manager: The manager.
    :return: The seconds left.
    """

    return None if manager.drain_start is None else manager.drain_start - time.time()


def assess(
    manager: "TaskManager", job: JobDefinition, doc: StateDoc | None
) -> tuple[dict[str, int] | None, Blocker | None]:
    """Decide whether this manager may run the job's next attempt, before claiming it (plan §7.7).

    The checks run in a fixed order, so a job this manager cannot run is
    attributed to exactly one blocker: pool, capability, the installed workflow
    and its call closure (``calls``), the installed manifest's ``requires``,
    resources, and ``mintime`` against the drain point.

    :param manager: The manager.
    :param job: The job.
    :param doc: Its ``state.json``, or ``None``.
    :return: ``(requirement, None)`` when eligible, otherwise ``(None, blocker)``.
    """

    if not manager.accept_any_pool and job.claim_pool not in manager.pools:
        return None, ("pool", job.claim_pool)
    if missing := job.required_capabilities - manager.capabilities:
        return None, ("capability", min(missing))
    installed, problem = manager._workflow(job.workflow_id)
    if installed is None:
        return None, ("calls", problem or f"workflow {job.workflow_id} is not installed")
    requires = installed.record.get("requires")
    unmet = unmet_job_requirements(tuple(str(item) for item in requires) if isinstance(requires, list) else ())
    if unmet:
        return None, ("requirements", unmet[0])
    requirement = effective_requirement(
        job, doc, manager.resources, manager.maximum_workers, whole_nodes=manager._inventory is not None
    )
    if (resource := unplaceable_resource(manager, requirement)) is not None:
        return None, ("resources", resource)
    left = time_left(manager)
    if left is not None and requirement.get("mintime", 0) > left:
        return None, ("time", "drain_point" if left <= 0 else "mintime")
    return requirement, None


def read_ready(manager: "TaskManager", ref: _kernel.JobRef) -> tuple[JobDefinition, StateDoc | None] | None:
    """Read an unowned job's ``job.json`` and ``state.json`` before claiming it; the reads are hints.

    :param manager: The manager.
    :param ref: The job reference.
    :return: The job and its state, or ``None`` for a job this manager may not or cannot read.
    """

    if not manager._owns(ref.path):
        return None
    try:
        job = JobDefinition.from_path(ref.path / "job.json")
    except FormatError as exc:
        if ref.path.exists():
            manager._report_anomaly(
                f"ready:{ref.job_key}", f"skipping ready job {ref.job_key}: {exc}", {"event": "job_unusable"}
            )
        return None
    # A damaged state.json is claimed anyway: reconcile fails the job with protocol_error.
    doc, _damaged = read_state_unowned(ref.path / "state.json")
    return job, doc


def claim_pass(manager: "TaskManager") -> bool:
    """Claim and launch eligible ready jobs within the manager's workers and capacity (plan §7.7 step 8).

    One bounded window of ``ready`` (``discovery_budget`` jobs, resumed from a
    cursor on the next pass) is read and assessed before any claim; the
    eligible jobs are claimed in priority order.

    :param manager: The manager.
    :return: Whether a job was claimed.
    """

    if manager._draining or not _has_room(manager):
        return False
    refs = list(
        _kernel.list_jobs(
            manager.workspace,
            "ready",
            prefixes=manager.placement_prefixes,
            limit=manager.discovery_budget,
            start=manager._cursor,
        )
    )
    # A short window reached the end: the next pass starts over.
    manager._cursor = refs[-1].cursor if len(refs) == manager.discovery_budget else None
    # ponytail: every job of the window is read before claiming; cap the window if wide queues make ticks slow.
    candidates: list[_kernel.JobRef] = []
    for ref in refs:
        loaded = read_ready(manager, ref)
        if loaded is None:
            continue
        job, doc = loaded
        requirement, blocker = assess(manager, job, doc)
        if blocker is not None:
            _LOGGER.debug("skipping ready job %s: %s %s", ref.job_key, *blocker)
            continue
        assert requirement is not None
        if fits_now(manager, requirement):
            candidates.append(ref)
    changed = False
    for ref in sorted(candidates, key=lambda item: (item.priority, item.cursor)):
        if not _has_room(manager):
            break
        changed |= manager._claim_and_launch(ref)
    return changed


def _has_room(manager: "TaskManager") -> bool:
    if len(manager._running) >= manager.maximum_workers:
        return False
    available = manager._available_resources()
    return not any(name in manager.resources and available[name] == 0 for name in ("procs", "mem"))


def fits_now(manager: "TaskManager", requirement: Mapping[str, int]) -> bool:
    """Return whether *requirement* fits what the manager has free right now.

    :param manager: The manager.
    :param requirement: The requirement.
    :return: Whether it fits.
    """

    available = manager._available_resources()
    inventory = manager._inventory
    if any(
        value > available.get(name, 0)
        for name, value in consumable(requirement).items()
        if inventory is None or name not in inventory.labels
    ):
        return False
    return inventory is None or can_assign(inventory, requirement)


def work_census(manager: "TaskManager") -> "WorkCensus":
    """Scan the workspace once and tag every job by why it is or is not this manager's work.

    Actionability applies exactly the claim predicates of :func:`assess`: a
    ready job counts as actionable only if this manager could claim it.

    :param manager: The manager.
    :return: The census.
    """

    from .manager import WorkCensus

    workspace, prefixes = manager.workspace, manager.placement_prefixes
    # ponytail: the terminal and resting states are counted by an exhaustive listing; it runs only on a
    # settled tick or at idle exit, never in the hot claim path.
    counts = {
        state: sum(1 for _ in _kernel.list_jobs(workspace, state, prefixes=prefixes))
        for state in ("succeeded", "failed", "waiting", "paused")
    }
    blocked: dict[str, Counter[str]] = {}
    claimable = 0
    for ref in _kernel.list_jobs(workspace, "ready", prefixes=prefixes):
        loaded = read_ready(manager, ref)
        if loaded is None:
            continue
        job, doc = loaded
        _, blocker = assess(manager, job, doc)
        if blocker is None:
            claimable += 1
        else:
            blocked.setdefault(blocker[0], Counter())[blocker[1]] += 1
    return WorkCensus(
        succeeded=counts["succeeded"],
        failed=counts["failed"],
        ready_claimable=claimable,
        ready_blocked={kind: dict(counter) for kind, counter in blocked.items()},
        waiting=counts["waiting"],
        paused=counts["paused"],
        # Every job in owned/<self>/ (running, or left for self-healing) is still this manager's work.
        actionable_count=claimable + len(manager.owner.owned()),
    )
