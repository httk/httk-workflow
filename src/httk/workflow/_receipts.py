"""13.1 receipts: the per-transfer fact that an addressed transfer was received here.

``transfers/received/<T>`` records that this workspace imported the addressed
transfer *T*. It is created no-replace (:func:`~httk.workflow._txn.link_new`)
when an import finishes, and again from a job's carried provenance before that
job can leave (sealing step S0), so a copy of *T* that arrives while the record
exists is recognized as a replay. Garbage collection deletes it only once no
copy can be accepted any more: after ``sealed_at + FRESHNESS_WINDOW_NS +
CLOCK_SKEW_NS``.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import _txn
from ._daemon_auth import CLOCK_SKEW_SECONDS
from ._util import json_bytes
from .errors import FormatError, WorkspaceCorruptionError
from .models import canonical_uuid, validate_sha256

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

#: How long after sealing an addressed bundle may still be accepted (W).
FRESHNESS_WINDOW_NS = 7 * 24 * 3600 * 10**9
#: The largest clock difference between two hosts the protocol tolerates (S).
CLOCK_SKEW_NS = CLOCK_SKEW_SECONDS * 10**9
#: The largest receipt document a reader accepts.
_RECEIPT_BYTES = 4096
_RECEIPT_KEYS = frozenset({"sealed_at", "source_workspace_id", "job_id", "payload_sha256"})


def receipt_path(workspace: "Workspace", transfer_id: str) -> Path:
    """Return where the receipt of one addressed transfer lives.

    :param workspace: The receiving workspace.
    :param transfer_id: The transfer id.
    :return: ``transfers/received/<transfer_id>``.
    """

    return workspace.control / "transfers" / "received" / canonical_uuid(transfer_id, "transfer_id")


def receipt_document(
    *, sealed_at: int, source_workspace_id: str, job_id: str, payload_sha256: str
) -> dict[str, object]:
    """Return the canonical receipt document.

    :param sealed_at: When the transfer was sealed, in integer UTC nanoseconds.
    :param source_workspace_id: The sending workspace.
    :param job_id: The transferred job (the bundle's root).
    :param payload_sha256: The root payload digest.
    :return: The document :func:`record_receipt` writes.
    :raises httk.workflow.errors.FormatError: If a value is malformed.
    """

    if isinstance(sealed_at, bool) or not isinstance(sealed_at, int) or not 0 <= sealed_at <= _txn.MAXIMUM_NS:
        raise FormatError("receipt sealed_at must be integer nanoseconds")
    return {
        "sealed_at": sealed_at,
        "source_workspace_id": canonical_uuid(source_workspace_id, "source_workspace_id"),
        "job_id": canonical_uuid(job_id, "job_id"),
        "payload_sha256": validate_sha256(payload_sha256, "payload_sha256"),
    }


def read_receipt(path: Path) -> dict[str, Any]:
    """Read and validate one receipt strictly.

    :param path: The receipt file.
    :return: The receipt document.
    :raises OSError: If the file cannot be read.
    :raises httk.workflow.errors.FormatError: If it is not a valid receipt.
    """

    import json

    from .models import _read_regular_file

    try:
        value = json.loads(_read_regular_file(path, _RECEIPT_BYTES).decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise FormatError(f"receipt {path} is not JSON: {exc}") from exc
    if not isinstance(value, dict) or set(value) != _RECEIPT_KEYS:
        raise FormatError(f"receipt {path} is malformed")
    return receipt_document(
        sealed_at=value["sealed_at"],
        source_workspace_id=value["source_workspace_id"],
        job_id=value["job_id"],
        payload_sha256=value["payload_sha256"],
    )


def record_receipt(workspace: "Workspace", transfer_id: str, document: Mapping[str, object]) -> Path:
    """Create the receipt of one addressed transfer, or confirm the one already there.

    :param workspace: The receiving workspace.
    :param transfer_id: The transfer id.
    :param document: The :func:`receipt_document`.
    :return: The receipt path.
    :raises httk.workflow.errors.WorkspaceCorruptionError: If a different receipt already names the transfer.
    """

    path = receipt_path(workspace, transfer_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json_bytes(dict(document)) + b"\n"
    if not _txn.link_new(path.parent, path.name, data, durable=workspace.durable):
        try:
            existing = read_receipt(path)
        except (OSError, FormatError) as exc:
            raise WorkspaceCorruptionError(f"receipt {path} exists but cannot be read: {exc}") from exc
        if existing != dict(document):
            raise WorkspaceCorruptionError(f"receipt {path} disagrees with transfer {transfer_id}")
    return path


def receipt_expired(document: Mapping[str, object], now: int) -> bool:
    """Report whether no copy of a received transfer can be accepted any more.

    :param document: A valid receipt.
    :param now: The current time, in integer UTC nanoseconds.
    :return: Whether ``now > sealed_at + W + S``.
    """

    sealed_at = document["sealed_at"]
    assert isinstance(sealed_at, int)
    return now > sealed_at + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS


def record_carried_receipt(workspace: "Workspace", job_id: str, provenance: object, now: int) -> Path | None:
    """Create ``received/<Tin>`` from a job's carried provenance, before the job can leave (S0).

    Only an *addressed* arrival has a receipt: the provenance names this
    workspace as its ``destination_workspace_id`` (an ejected bundle adopted
    here has none). A provenance whose window has passed needs no receipt.

    :param workspace: The workspace the job is in.
    :param job_id: The job (the root of the bundle it arrived in, or a member of it).
    :param provenance: The job's carried ``transfer`` member.
    :param now: The current time, in integer UTC nanoseconds.
    :return: The receipt path, or ``None`` when no receipt is due.
    :raises httk.workflow.errors.FormatError: If an addressed provenance is malformed.
    :raises httk.workflow.errors.WorkspaceCorruptionError: If a different receipt already names the transfer.
    """

    if not isinstance(provenance, Mapping) or provenance.get("destination_workspace_id") != workspace.workspace_id:
        return None
    sealed_at = provenance.get("sealed_at")
    if isinstance(sealed_at, bool) or not isinstance(sealed_at, int):
        raise FormatError(f"job {job_id} carries an addressed transfer provenance without sealed_at")
    if now > sealed_at + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS:
        return None
    root_job_id = provenance.get("job_id", job_id)
    document = receipt_document(
        sealed_at=sealed_at,
        source_workspace_id=str(provenance.get("source_workspace_id")),
        job_id=str(root_job_id),
        payload_sha256=str(provenance.get("payload_sha256")),
    )
    return record_receipt(workspace, str(provenance.get("transfer_id")), document)
