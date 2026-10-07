"""Transfer provenance carried by every state frame, and marker lookups over every kind (plan decision 8, P12)."""

import time
import uuid
from pathlib import Path

import pytest

from conftest import configure_identity
from httk.workflow import TaskManager, Workspace
from httk.workflow.errors import TransitionLostError, WorkspaceCorruptionError
from httk.workflow.journal import parse_record_ref, segment_path
from httk.workflow.models import STATE_KINDS, Marker
from test_eject_adopt import _payload


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    configure_identity()
    return Workspace.initialize(tmp_path / "workspace")


def _provenance() -> dict[str, object]:
    return {
        "transfer_id": str(uuid.uuid4()),
        "source_workspace_id": str(uuid.uuid4()),
        "payload_sha256": "0" * 64,
        "sealed_at": time.time_ns(),
    }


def _imported(workspace: Workspace, tmp_path: Path, provenance: dict[str, object]) -> Marker:
    """Submit a job and give it an import frame carrying transfer provenance and exchange origin."""

    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    with workspace.open_journal_writer() as writer:
        return workspace.transition(writer, marker, "submitted", {"transfer": provenance, "origin": "exchange"})


def test_provenance_survives_every_manager_transition(workspace: Workspace, tmp_path: Path) -> None:
    provenance = _provenance()
    imported = _imported(workspace, tmp_path, provenance)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = workspace.find_marker_by_id(imported.job_id)
    assert finished is not None and finished.kind == "succeeded"
    assert finished.generation >= imported.generation + 4  # ready, claimed, running, committing, succeeded
    state = workspace.read_state(finished)
    assert state["transfer"] == provenance and state["origin"] == "exchange"


def test_an_update_overrides_carried_provenance(workspace: Workspace, tmp_path: Path) -> None:
    imported = _imported(workspace, tmp_path, _provenance())
    replacement = _provenance()
    with workspace.open_journal_writer() as writer:
        moved = workspace.transition(writer, imported, "ready", {"transfer": replacement})
        state = workspace.read_state(moved)
        assert state["transfer"] == replacement and state["origin"] == "exchange"
        moved = workspace.transition(writer, moved, "paused", {"origin": None})
    state = workspace.read_state(moved)
    assert state["transfer"] == replacement and state["origin"] is None


def test_a_job_without_provenance_gets_none(workspace: Workspace, tmp_path: Path) -> None:
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    with workspace.open_journal_writer() as writer:
        moved = workspace.transition(writer, marker, "ready", {"reason": "submitted"})
        moved = workspace.transition(writer, moved, "paused", {})
    assert not {"transfer", "origin"} & set(workspace.read_state(moved))


def test_an_unreadable_previous_frame_fails_the_transition(workspace: Workspace, tmp_path: Path) -> None:
    imported = _imported(workspace, tmp_path, _provenance())
    writer_id, segment, offset, length, _ = parse_record_ref(imported.record_ref)
    path = segment_path(workspace.control, writer_id, segment)
    data = bytearray(path.read_bytes())
    data[offset + 8 + length // 2] ^= 0xFF  # damage the frame payload: its checksum no longer matches
    path.write_bytes(bytes(data))
    with workspace.open_journal_writer() as writer, pytest.raises(WorkspaceCorruptionError):
        workspace.transition(writer, imported, "ready", {})
    # The marker did not move: nothing was decided without the provenance.
    assert workspace.find_marker_by_id(imported.job_id) == imported


# ---------------------------------------------------------------------------
# Lookups over every kind
# ---------------------------------------------------------------------------


def test_find_marker_by_id_can_include_transferring(workspace: Workspace, tmp_path: Path) -> None:
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    assert workspace.find_marker_by_id(marker.job_id, kinds=STATE_KINDS) == marker
    assert workspace.find_marker_by_id(marker.job_id, kinds=("transferring",)) is None
    with workspace.open_journal_writer() as writer:
        fenced = workspace.transition(writer, marker, "transferring", {"prior_kind": "submitted"})
    assert workspace.find_marker_by_id(marker.job_id) is None
    assert workspace.find_marker_by_id(marker.job_id, kinds=STATE_KINDS) == fenced
    assert workspace.find_marker_by_id(marker.job_id, kinds=("transferring",)) == fenced
    assert workspace.find_marker_by_id(marker.job_id, kinds=("submitted", "ready")) is None
    assert workspace.find_marker_by_id(str(uuid.uuid4()), kinds=STATE_KINDS) is None
    with pytest.raises(ValueError, match="unknown state kinds"):
        workspace.find_marker_by_id(marker.job_id, kinds=("bogus",))
    # A second current marker in another kind is the corruption it always was.
    duplicate = workspace.state_directory("submitted", marker.placement) / marker.path.name
    duplicate.parent.mkdir(parents=True, exist_ok=True)
    duplicate.touch()
    workspace.invalidate_marker_index()
    with pytest.raises(WorkspaceCorruptionError, match="more than one state marker"):
        workspace.find_marker_by_id(marker.job_id, kinds=STATE_KINDS)


def test_a_fence_loser_learns_it_lost_at_once(workspace: Workspace, tmp_path: Path) -> None:
    workspace.set_policy({"visibility_deadline_seconds": 30.0})
    marker = workspace.submit(_payload(tmp_path / "payloads"), "jobs")
    with workspace.open_journal_writer() as writer:
        ready = workspace.transition(writer, marker, "ready", {"reason": "submitted"})
        # The winner fences the job; the loser still holds the ready marker it read before.
        workspace.transition(writer, ready, "transferring", {"prior_kind": "ready"})
        started = time.monotonic()
        with pytest.raises(TransitionLostError):
            workspace.transition(writer, ready, "paused", {"reason": "operator_pause"})
    assert time.monotonic() - started < 10.0
