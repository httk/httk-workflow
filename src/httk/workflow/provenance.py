"""The ``provenance`` declaration vocabulary and its pure :class:`httk.core.Run` builder.

The declaration name is ``provenance``. Its document is an object whose members
are all optional:

.. code-block:: json

    {
      "workflow_declaration_uri": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax",
      "inputs":    {"initial_structure": {"type": "structures", "id": "<served id>"}},
      "artifacts": {"relaxed_structure": {"type": "structures", "id": "..."}},
      "outputs":   {"total_energy":      {"type": "_httk_records", "id": "..."}}
    }

The object keys are edge labels, so labels are unique per side. Targets are
loose served-entry references. A document may be written as ``declared`` in
``JobSpec.declarations`` at scaffold time, which suits externally known inputs,
and/or as ``observed`` with ``Attempt.declare("provenance", ...)`` at collect
time, when produced-entry ids exist. Consumers choose one complete document;
they never merge the two.
"""

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Final

from httk.core import Run, RunEdge

from .harvesting import HarvestRecord

__all__ = ["PROVENANCE_DECLARATION", "run_record"]

PROVENANCE_DECLARATION: Final = "provenance"


def _error(identity: str, member: str, label: object, detail: str) -> ValueError:
    return ValueError(f"{identity}: {member} label {label!r}: {detail}")


def _edges(identity: str, document: Mapping[str, object], member: str) -> tuple[RunEdge, ...]:
    if member not in document:
        return ()
    raw = document[member]
    if not isinstance(raw, Mapping):
        raise _error(identity, member, member, "must be a mapping")
    edges: list[RunEdge] = []
    for label, target in raw.items():
        if not isinstance(label, str):
            raise _error(identity, member, label, "must be a string")
        if not isinstance(target, Mapping):
            raise _error(identity, member, label, "edge must be a mapping")
        if set(target) != {"type", "id"}:
            raise _error(identity, member, label, "edge must contain only 'type' and 'id'")
        entry_type = target["type"]
        entry_id = target["id"]
        if not isinstance(entry_type, str) or not isinstance(entry_id, str):
            raise _error(identity, member, label, "edge 'type' and 'id' must be strings")
        edges.append(RunEdge(label, entry_type, entry_id))
    return tuple(edges)


def _uri(identity: str, value: object, source: str, *, allow_none: bool) -> str | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{identity}: {source} must be a non-empty string without surrounding whitespace")
    if not isinstance(value, str) or not value or value != value.strip():
        requirement = "None or a non-empty string" if allow_none else "a non-empty string"
        raise ValueError(f"{identity}: {source} must be {requirement} without surrounding whitespace")
    return value


def _workflow_uri(identity: str, declarations: Mapping[str, Mapping[str, object] | None]) -> str | None:
    entry = declarations.get("workflow")
    if not isinstance(entry, Mapping):
        return None
    for side in ("observed", "declared"):
        document = entry.get(side)
        if isinstance(document, Mapping) and "$id" in document:
            return _uri(identity, document["$id"], "workflow $id", allow_none=False)
    return None


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _last_modified(record: HarvestRecord) -> datetime | None:
    latest: datetime | None = None
    activations = record.provenance.get("activations")
    if not isinstance(activations, Sequence) or isinstance(activations, (str, bytes)):
        return None
    for activation in activations:
        if not isinstance(activation, Mapping):
            continue
        attempts = activation.get("attempts")
        if not isinstance(attempts, Sequence) or isinstance(attempts, (str, bytes)):
            continue
        for attempt in attempts:
            if not isinstance(attempt, Mapping):
                continue
            parsed = _timestamp(attempt.get("finished_at"))
            if parsed is not None and (latest is None or parsed > latest):
                latest = parsed
    return latest


def run_record(record: HarvestRecord) -> Run:
    """Build the one :class:`httk.core.Run` represented by *record*.

    The observed ``provenance`` document is selected wholesale when present;
    otherwise the declared document is selected. Missing or ``None`` means no
    edges. Its URI is used when that member is present, including explicit
    ``null``; otherwise the observed-then-declared ``workflow`` declaration's
    ``$id`` is used. Each edge member must map labels to exactly ``type`` and
    ``id`` string members, and insertion order is preserved.

    ``immutable_id`` is ``"<workspace_id>:<job_id>"``. ``last_modified`` is the
    latest parseable aware ``finished_at`` timestamp in the attempt timeline;
    absent or unparseable timestamps produce ``None``. Children are not folded
    in: each child harvests to its own ``Run``, while a parent can name child
    products explicitly in its observed declaration. Runner identity, timeline,
    and failure are deliberately not folded into ``Run``; the caller's
    ``HarvestRecord`` remains the extra-information channel.
    """

    identity = f"{record.workspace_id}:{record.job_id}"
    declarations = record.declarations
    entry = declarations.get(PROVENANCE_DECLARATION)
    document: Mapping[str, object] | None = None
    if isinstance(entry, Mapping):
        observed = entry.get("observed")
        declared = entry.get("declared")
        chosen = observed if observed is not None else declared
        if isinstance(chosen, Mapping):
            document = chosen

    if document is not None and "workflow_declaration_uri" in document:
        workflow_uri = _uri(
            identity,
            document["workflow_declaration_uri"],
            "provenance.workflow_declaration_uri",
            allow_none=True,
        )
    else:
        workflow_uri = _workflow_uri(identity, declarations)

    return Run(
        workflow_declaration_uri=workflow_uri,
        inputs=() if document is None else _edges(identity, document, "inputs"),
        artifacts=() if document is None else _edges(identity, document, "artifacts"),
        outputs=() if document is None else _edges(identity, document, "outputs"),
        immutable_id=identity,
        last_modified=_last_modified(record),
    )
