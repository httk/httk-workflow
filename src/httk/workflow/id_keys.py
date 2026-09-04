"""Stable ledger source keys from workflow collect coordinates.

The httk-store id ledger (``httk.store.IdLedger``) maps a stable, opaque
**source key** to a permanent entry id, so a database rebuilt from the same
sources keeps its ids. This module builds those keys in the httk₂-native
grammar

.. code-block:: text

    <workspace_id>:<job_id>[:<role>[:file:<relpath>]]

from the coordinates a collected job already carries: the workspace and job
identities pin the producing job, an optional declared output *role* pins one
output of that job, and an optional *relpath* pins one file, marked by a literal
``file:`` segment so a role and a file cannot be confused.

The one hard rule is anchoring: a key must derive from a *stable* identity, not
from a path that can silently move. A :class:`~httk.workflow.collecting.CollectedJob`
harvested from a v1 tree without a manifest carries ``identity_stable=False``;
:func:`ledger_key` refuses it (``force=True`` overrides), because a path-derived
key that goes stale would hand an old id to new content — the one unforgivable
ledger failure. Keys are opaque to the allocator, so embedded colons in a role
or relative path are harmless.
"""

import os
from pathlib import PurePosixPath

from .collecting import CollectedJob, JobRecord

__all__ = [
    "UnstableIdentityError",
    "ledger_key",
]


class UnstableIdentityError(ValueError):
    """A ledger key was refused because the job's identity is not stable.

    Raised by :func:`ledger_key` for a collected job whose ``identity_stable``
    is ``False`` — a v1-harvested job with no manifest, whose only identity is
    its absolute path. A key built from it would go stale the moment the tree
    moved, so it is refused unless ``force=True`` is passed.
    """


def _coordinates(item: CollectedJob | JobRecord) -> tuple[str, str, bool | None]:
    """Return the workspace id, job id, and identity-stability of a collected job.

    :param item: The collected job or its mechanical record.
    :return: The workspace id, job id, and the identity-stable flag (``None``
        for a live-collected job, which is always stable).
    """

    record = item.record if isinstance(item, CollectedJob) else item
    identity_stable = item.identity_stable if isinstance(item, CollectedJob) else None
    return record.workspace_id, record.job_id, identity_stable


def ledger_key(
    item: CollectedJob | JobRecord,
    role: str | None = None,
    path: str | os.PathLike[str] | None = None,
    *,
    force: bool = False,
) -> str:
    """Build the native ledger source key for a collected job coordinate.

    The key is ``<workspace_id>:<job_id>``, extended with ``:<role>`` when a
    declared output role is named and with ``:file:<relpath>`` when a file is
    named, so the four shapes are the job, one of its outputs, one of its files,
    and one file of one output.

    :param item: The collected job (or its record) the key anchors on.
    :param role: The declared output role to pin, if any.
    :param path: The output-relative file path to pin, if any; its POSIX form
        is used verbatim.
    :param force: Build the key even when the job's identity is not stable.
    :return: The ledger source key.
    :raises UnstableIdentityError: If the job's identity is not stable and
        ``force`` is not set.
    """

    workspace_id, job_id, identity_stable = _coordinates(item)
    if identity_stable is False and not force:
        raise UnstableIdentityError(
            f"refusing to build a ledger key for job {workspace_id}:{job_id}: its identity is not stable "
            "(a v1-harvested job with no manifest is identified only by its absolute path, so a key built "
            "from it would hand an old id to new content once the tree moves). Pass force=True to override."
        )
    key = f"{workspace_id}:{job_id}"
    if role is not None:
        key += f":{role}"
    if path is not None:
        key += f":file:{PurePosixPath(path).as_posix()}"
    return key
