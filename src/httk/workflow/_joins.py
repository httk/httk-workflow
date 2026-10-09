"""Read-only evaluation of waiting parents' joins and the revival guard (note §7.4).

:func:`evaluate` never writes and never claims: it reads the waiting parent's
``state.json`` and probes each child at its placement in the unowned states
only, through the pass's :class:`~httk.workflow._kernel.ListingCache`. A child
not found there is pending (owned, or moving). Only a child that stays
unresolved past a grace is searched for in every ``owned/<id>/`` as well, and
only a miss there too makes the join ``unresolvable``. The manager claims the
parent and applies the decision.
"""

from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Literal

from httk.workflow._job import JobDefinition
from httk.workflow._kernel import JobRef, KernelWorkspace, ListingCache, locate
from httk.workflow._state import TERMINAL_STATES, _thaw, read_state_unowned
from httk.workflow._util import require_string
from httk.workflow.errors import FormatError
from httk.workflow.models import canonical_uuid, normalize_placement, parse_job_key, placement_text, validate_label

__all__ = [
    "CONDITIONS",
    "JoinDecision",
    "Observation",
    "consumed_by_decided_join",
    "evaluate",
    "impossible",
    "satisfied",
]

CONDITIONS = ("all_succeeded", "all_terminal", "any_succeeded", "any_terminal", "at_least")
#: The observed state of a child not found in an unowned state at its placement.
PENDING = "pending"


@dataclass(frozen=True)
class Observation:
    """One child as a join evaluation saw it.

    :param label: The child's label, or ``None``.
    :param job_id: The child UUID.
    :param job_key: The child key.
    :param placement: The child's placement text.
    :param state: Its unowned state, or ``"pending"`` when it was not found in one.
    :param token: The observed directory name's token, or ``None`` when pending.
    :param failure: The failure from the child's ``state.json`` when it is failed or cancelled.
    """

    label: str | None
    job_id: str
    job_key: str
    placement: str
    state: str
    token: str | None
    failure: Mapping[str, object] | None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON-ready observation for ``state.json``.

        :return: A fresh mutable mapping.
        """

        return {
            "label": self.label,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "placement": self.placement,
            "state": self.state,
            "token": self.token,
            "failure": _thaw(self.failure),
        }


@dataclass(frozen=True)
class JoinDecision:
    """A decided join.

    :param kind: ``satisfied``, ``impossible``, or ``unresolvable`` (a child vanished: ``dependency_failure``).
    :param observations: Every child, in join order.
    :param unresolved: Children not found: confirmed missing for ``unresolvable``, else merely not located.
    """

    kind: Literal["satisfied", "impossible", "unresolvable"]
    observations: tuple[Observation, ...]
    unresolved: tuple[str, ...]


def satisfied(condition: str, count: int, kinds: Sequence[str]) -> bool:
    """Report whether the observed child states satisfy *condition*.

    :param condition: One of :data:`CONDITIONS`.
    :param count: The success count of ``at_least``.
    :param kinds: The observed child states.
    :return: Whether the join is satisfied.
    :raises httk.workflow.errors.FormatError: For an unknown condition.
    """

    if condition == "all_succeeded":
        return all(kind == "succeeded" for kind in kinds)
    if condition == "all_terminal":
        return all(kind in TERMINAL_STATES for kind in kinds)
    if condition == "any_succeeded":
        return any(kind == "succeeded" for kind in kinds)
    if condition == "any_terminal":
        return any(kind in TERMINAL_STATES for kind in kinds)
    if condition == "at_least":
        return sum(kind == "succeeded" for kind in kinds) >= count
    raise FormatError(f"unknown join condition: {condition!r}")


def impossible(condition: str, count: int, kinds: Sequence[str]) -> bool:
    """Report whether enough children ended without success to make *condition* false for good.

    :param condition: One of :data:`CONDITIONS`.
    :param count: The success count of ``at_least``.
    :param kinds: The observed child states.
    :return: Whether success is impossible.
    :raises httk.workflow.errors.FormatError: For an unknown condition.
    """

    if condition == "all_succeeded":
        return any(kind in {"failed", "cancelled"} for kind in kinds)
    if condition in {"all_terminal", "any_terminal"}:
        return False
    if condition == "any_succeeded":
        return all(kind in TERMINAL_STATES for kind in kinds) and "succeeded" not in kinds
    if condition == "at_least":
        successes = sum(kind == "succeeded" for kind in kinds)
        return successes + sum(kind not in TERMINAL_STATES for kind in kinds) < count
    raise FormatError(f"unknown join condition: {condition!r}")


def _join_children(join: Mapping[str, object]) -> list[tuple[str | None, str, str, PurePosixPath]]:
    children = join.get("children")
    if not isinstance(children, tuple) or not children:
        raise FormatError("state.join.children must be a nonempty array")
    references = []
    for raw in children:
        if not isinstance(raw, Mapping):
            raise FormatError("join child must be an object")
        job_id = canonical_uuid(raw.get("job_id"), "join child job_id")
        job_key = require_string(raw.get("job_key"), "join child job_key")
        if parse_job_key(job_key)[1] != job_id:
            raise FormatError("join child job_key does not carry its job_id")
        hint = raw.get("placement_hint")
        if not isinstance(hint, str):
            raise FormatError("join child must carry a placement_hint")
        label = raw.get("label")
        label = None if label is None else validate_label(label, "join child label")
        references.append((label, job_id, job_key, normalize_placement(hint)))
    return references


def evaluate(
    workspace: KernelWorkspace,
    parent_ref: JobRef,
    *,
    cache: ListingCache,
    unresolved_since: MutableMapping[str, float],
    grace: float,
    now: float,
) -> JoinDecision | None:
    """Decide a waiting parent's join, reading only.

    :param workspace: The workspace.
    :param parent_ref: The waiting parent.
    :param cache: The pass's listings.
    :param unresolved_since: When each child was first not found, by job id; kept across passes by the caller.
    :param grace: Seconds a child may stay unresolved before the vanished-child check.
    :param now: The caller's clock, in the units of *unresolved_since*.
    :return: The decision, or ``None`` while the join is pending (or the parent has no readable join).
    :raises httk.workflow.errors.FormatError: If the recorded join is malformed or a child's key disagrees.
    """

    doc, _damaged = read_state_unowned(parent_ref.path / "state.json")
    if doc is None or doc.job_id != parent_ref.job_id or doc.join is None:
        return None
    join = doc.join
    condition = join.get("condition")
    if not isinstance(condition, str) or condition not in CONDITIONS:
        raise FormatError(f"unknown join condition: {condition!r}")
    count = join.get("count", 0)
    if type(count) is not int or count < 0:
        raise FormatError("state.join.count must be a non-negative integer")
    references = _join_children(join)
    observations: list[Observation] = []
    missing: list[tuple[str, PurePosixPath]] = []
    for label, job_id, job_key, placement in references:
        ref = locate(workspace, job_id, placement_hint=placement, include_owned=False, settle=False, cache=cache)
        if ref is None:
            unresolved_since.setdefault(job_id, now)
            missing.append((job_id, placement))
            observations.append(Observation(label, job_id, job_key, placement_text(placement), PENDING, None, None))
            continue
        if ref.job_key != job_key:
            raise FormatError(f"join child {job_id} is {ref.job_key} at its placement, not {job_key}")
        unresolved_since.pop(job_id, None)
        failure = None
        if ref.state in {"failed", "cancelled"}:
            child_doc, _ = read_state_unowned(ref.path / "state.json")
            failure = None if child_doc is None else child_doc.failure
        observations.append(
            Observation(label, job_id, job_key, placement_text(placement), ref.state, ref.token, failure)
        )
    kinds = [observation.state for observation in observations]
    decided: Literal["satisfied", "impossible"] | None = None
    if satisfied(condition, count, kinds):
        decided = "satisfied"
    elif impossible(condition, count, kinds):
        decided = "impossible"
    if decided is not None:
        for _, job_id, _, _ in references:
            unresolved_since.pop(job_id, None)
        return JoinDecision(decided, tuple(observations), tuple(job_id for job_id, _ in missing))
    vanished = []
    for job_id, placement in missing:
        if now - unresolved_since[job_id] <= grace:
            continue
        # The rare vanished-child check: owned directories too, settled, so a child in motion is found.
        # ponytail: one settle per overdue child blocks this pass up to the visibility deadline each;
        # batch them through locate_many if many children of one tick ever go overdue together.
        if locate(workspace, job_id, placement_hint=placement, include_owned=True, settle=True) is None:
            vanished.append(job_id)
        else:
            unresolved_since[job_id] = now
    if vanished:
        return JoinDecision("unresolvable", tuple(observations), tuple(vanished))
    return None


def consumed_by_decided_join(
    workspace: KernelWorkspace, child: JobDefinition, *, cache: ListingCache | None = None
) -> str | None:
    """The revival guard: describe the parent whose current ``state.json`` records *child* in a decided join.

    Advisory and read-only: the parent is probed at its recorded placement and in
    ``owned/``, never searched for elsewhere.

    :param workspace: The workspace.
    :param child: The child's ``job.json``.
    :param cache: The pass's listings, if any.
    :return: ``"<parent key> (<state>)"``, or ``None`` when no decided join consumed the child.
    """

    parent = child.parent
    if parent is None:
        return None
    job_id, placement = parent["job_id"], parent["placement"]
    assert isinstance(job_id, str) and isinstance(placement, str)
    ref = locate(workspace, job_id, placement_hint=PurePosixPath(placement), include_owned=True, cache=cache)
    if ref is None:
        return None
    doc, _ = read_state_unowned(ref.path / "state.json")
    if doc is None or doc.job_id != job_id:
        return None
    if any(observation.get("job_id") == child.id for observation in doc.observations):
        return f"{ref.job_key} ({ref.state})"
    return None
