"""Workspace policy, the visibility deadline it configures, durability, and fsck."""

import json
import os
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel
from httk.workflow import _util as util_module
from httk.workflow._state import Release, StateDoc
from httk.workflow.errors import FormatError
from httk.workflow.models import RetentionPolicy, WorkspacePolicy
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, find, submit, workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


def test_policy_is_written_at_initialization_and_round_trips(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    assert stored["policy"] == {
        "visibility_deadline_seconds": 5.0,
        "retention": {"trash_days": 1.0, "owner_tombstone_days": 30.0},
    }
    assert workspace.policy == WorkspacePolicy()
    assert workspace.policy.retention.attempt_control_days is None
    assert workspace.policy.retention.trash_days == 1.0
    updated = workspace.set_policy({"visibility_deadline_seconds": 90, "retention": {"trash_days": 30}})
    assert updated.visibility_deadline_seconds == 90.0
    # Another implementation attaching the same workspace sees the same policy,
    # and the unrelated members of format.json survived the read-modify-write.
    attached = Workspace(tmp_path / "workspace")
    assert attached.policy == updated
    assert attached.visibility_deadline == 90.0
    assert attached.policy.retention.trash_days == 30.0
    assert attached.workspace_id == workspace.workspace_id
    assert attached.format["core_profile"] == "core-v3"


def test_initialize_creates_jobs_and_every_format_section(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    assert (workspace.root / "jobs").is_dir()
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    assert stored["format_version"] == 3 and stored["core_profile"] == "core-v3"
    assert stored["settings"] == {} and stored["workflow_preludes"] == {} and "policy" in stored


def test_a_version_2_workspace_is_refused_with_a_teaching_message(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    stored["format_version"] = 2
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    with pytest.raises(FormatError, match="httk system reset"):
        Workspace(workspace.root)


@pytest.mark.parametrize("section", ["policy", "settings", "workflow_preludes"])
def test_a_workspace_missing_a_format_section_is_refused(tmp_path: Path, section: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    del stored[section]
    (workspace.control / "format.json").write_text(json.dumps(stored), encoding="utf-8")
    with pytest.raises(FormatError, match=section):
        Workspace(workspace.root)


@pytest.mark.parametrize(
    "changes",
    [
        {"visibility_dealine_seconds": 60.0},
        {"unknown": 1},
        {"visibility_deadline_seconds": "soon"},
        {"visibility_deadline_seconds": -1.0},
        {"visibility_deadline_seconds": 999999.0},
        {"lease_seconds": 60.0},
        {"journal_segment_bytes": 65536},
        {"retention": 30},
        {"retention": {"journal_hours": 4}},
        {"retention": {"journal_days": 1.0}},
        {"retention": {"trash_days": "many"}},
    ],
)
def test_policy_refuses_unknown_keys_and_impossible_values(tmp_path: Path, changes: dict[str, object]) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(FormatError):
        workspace.set_policy(changes)
    # A refused change never reaches the workspace.
    assert Workspace(tmp_path / "workspace").policy == WorkspacePolicy()


def test_policy_command_shows_sets_and_refuses(tmp_path: Path, capsys) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, root)
    assert command(["workspace", "policy", "show", "--json", ws], context) == 0
    shown = json.loads(capsys.readouterr().out)[0]
    assert shown["visibility_deadline_seconds"] == 5.0
    assert shown["retention"] == {"trash_days": 1.0, "owner_tombstone_days": 30.0}
    assert (
        command(["workspace", "policy", "set", "--key", "visibility_deadline_seconds", "--value", "60", ws], context)
        == 0
    )
    assert command(["workspace", "policy", "set", "--key", "retention.trash_days", "--value", "14", ws], context) == 0
    capsys.readouterr()
    assert command(["workspace", "policy", "set", "--key", "lease_seconds", "--value", "60", ws], context) == 1
    assert command(["workspace", "policy", "set", "--key", "no_such_key", "--value", "1", ws], context) == 1
    assert (
        command(
            ["workspace", "policy", "set", "--key", "visibility_deadline_seconds", "--value", "not-json", ws], context
        )
        == 1
    )
    policy = Workspace(root).policy
    assert policy.visibility_deadline_seconds == 60.0
    assert policy.retention.trash_days == 14.0
    assert command(["workspace", "policy", "set", "--key", "retention.trash_days", "--value", "null", ws], context) == 0
    assert Workspace(root).policy.retention.trash_days is None
    assert (
        command(
            ["workspace", "policy", "set", "--key", "retention.owner_tombstone_days", "--value", "keep", ws], context
        )
        == 0
    )
    assert Workspace(root).policy.retention.owner_tombstone_days is None
    # The retired journal retention is refused on write.
    assert command(["workspace", "policy", "set", "--key", "retention.journal_days", "--value", "1", ws], context) == 1
    assert command(["workspace", "policy", "show", ws], context) == 0
    assert "visibility_deadline_seconds\t60.0" in capsys.readouterr().out


def test_retention_keep_is_persisted_and_disables_collection(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    updated = workspace.set_policy({"retention": {"owner_tombstone_days": "keep", "trash_days": None}})

    assert updated.retention.owner_tombstone_days is None
    assert updated.retention.trash_days is None
    stored = json.loads((workspace.control / "format.json").read_text(encoding="utf-8"))
    assert stored["policy"]["retention"] == {"trash_days": None, "owner_tombstone_days": None}
    attached = Workspace(workspace.root)
    assert attached.policy.retention.owner_tombstone_days is None
    assert attached.policy.retention.trash_days is None


def test_a_policy_with_retired_members_still_attaches(tmp_path: Path) -> None:
    # Older jobs-v3 workspaces store journal_segment_bytes, lease_seconds and retention.journal_days; reading
    # ignores them, setting one is refused, and the next write drops them.
    workspace = Workspace.initialize(tmp_path / "workspace", durable=False)
    path = workspace.control / "format.json"
    stored = json.loads(path.read_text(encoding="utf-8"))
    stored["policy"].update(journal_segment_bytes=65536, lease_seconds=900.0)
    stored["policy"]["retention"]["journal_days"] = 1.0
    path.write_text(json.dumps(stored), encoding="utf-8")
    assert Workspace(workspace.root).policy == WorkspacePolicy()
    with pytest.raises(FormatError, match="unsupported members: lease_seconds"):
        Workspace(workspace.root).set_policy({"lease_seconds": 60})
    Workspace(workspace.root).set_policy({"visibility_deadline_seconds": 60})
    rewritten = json.loads(path.read_text(encoding="utf-8"))["policy"]
    assert not {"journal_segment_bytes", "lease_seconds"} & set(rewritten)
    assert "journal_days" not in rewritten["retention"]


def test_public_retention_none_round_trips_as_keep() -> None:
    policy = RetentionPolicy(trash_days=None, owner_tombstone_days=None)

    assert policy.as_mapping() == {"trash_days": None, "owner_tombstone_days": None}
    assert RetentionPolicy.from_mapping(policy.as_mapping()) == policy


@pytest.mark.timing
def test_the_visibility_schedule_spends_exactly_the_configured_deadline() -> None:
    schedule = util_module.visibility_schedule(60.0)
    assert schedule[:3] == (0.01, 0.02, 0.04)
    assert sum(schedule) == pytest.approx(60.0)
    # Long deadlines keep probing rather than sleeping through one huge wait.
    assert max(schedule) <= util_module.MAXIMUM_RETRY_DELAY_SECONDS
    assert sum(util_module.visibility_schedule()) == pytest.approx(util_module.DEFAULT_VISIBILITY_DEADLINE_SECONDS)
    assert sum(util_module.visibility_schedule(0.0)) == 0.0


# ---------------------------------------------------------------------------
# Durability
# ---------------------------------------------------------------------------


def test_workspaces_are_durable_by_default_and_opt_out_explicitly(tmp_path: Path) -> None:
    ws = Workspace.initialize(tmp_path / "workspace")
    assert ws.durable is True
    assert Workspace(tmp_path / "workspace").durable is True
    assert Workspace(tmp_path / "workspace", durable=False).durable is False


def test_the_default_write_path_synchronizes_and_no_durable_does_not(tmp_path: Path, monkeypatch) -> None:
    counted: list[int] = []
    real_fsync = os.fsync

    def counting_fsync(descriptor: int) -> None:
        counted.append(descriptor)
        real_fsync(descriptor)

    durable = Workspace.initialize(tmp_path / "durable")
    relaxed = Workspace.initialize(tmp_path / "relaxed", durable=False)
    monkeypatch.setattr(os, "fsync", counting_fsync)
    submit(durable, _WORKFLOW, {"start": "succeed"})
    assert counted
    counted.clear()
    submit(relaxed, _WORKFLOW, {"start": "succeed"})
    assert not counted


# ---------------------------------------------------------------------------
# fsck
# ---------------------------------------------------------------------------


def test_fsck_reports_nothing_about_a_healthy_workspace(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    with cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, ref)
        assert owned is not None
        report = ws.check()  # an owned job is a job too
        owned.release(StateDoc.empty(ref.job_id), Release("ready", 500))
    assert report.ok and report.jobs_checked == 1 and report.unresolved == 0
    assert report.as_mapping()["findings"] == []


def test_fsck_reports_unparsable_names_and_quarantines_them_only_on_request(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    submit(ws, _WORKFLOW, {"start": "succeed"}, placement="p/0")
    stray = ws.jobs / "ready" / "p" / "stray.txt"
    stray.write_text("x", encoding="utf-8")
    malformed = ws.jobs / "ready" / "p" / "not~a~job~name~at~all"
    malformed.mkdir()
    unknown = ws.jobs / "running"
    unknown.mkdir()
    report = ws.check()
    assert {(finding.entry, finding.problem, finding.action) for finding in report.findings} == {
        (stray, "unparsable_name", "reported"),
        (malformed, "unparsable_name", "reported"),
        (unknown, "unparsable_name", "reported"),
    }
    assert report.counts == {"unparsable_name": 3} and report.unresolved == 3 and stray.exists()
    repaired = ws.check(repair=True)
    assert {finding.action for finding in repaired.findings} == {"quarantined"}
    assert not stray.exists() and not malformed.exists() and not unknown.exists()
    quarantined = sorted((ws.control / "quarantine").iterdir())
    assert len(quarantined) == 3 and all((entry / "reason.json").is_file() for entry in quarantined)
    assert ws.check().ok


def test_fsck_reports_a_job_in_two_places(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"}, placement="p/0")
    copy = ws.jobs / "failed" / "p" / "0" / ref.path.name
    copy.parent.mkdir(parents=True)
    copy.mkdir()
    (copy / "job.json").write_bytes((ref.path / "job.json").read_bytes())
    report = ws.check(repair=True)
    assert {(finding.entry, finding.problem, finding.action) for finding in report.findings} == {
        (ref.path, "duplicate_job", "reported"),
        (copy, "duplicate_job", "reported"),
    }
    assert all(finding.job_id == ref.job_id for finding in report.findings)


def test_fsck_reports_an_owned_directory_without_its_owner(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    orphan = ws.jobs / _kernel.OWNED / uuid.uuid4().hex
    orphan.mkdir(parents=True)
    report = ws.check()
    assert [(finding.entry, finding.problem) for finding in report.findings] == [(orphan, "orphan_owned")]


def test_fsck_reports_a_recovered_owner_that_holds_jobs_again(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    with cli_owner(ws) as recoverer:
        sleeper = cli_owner(ws)
        _kernel.attest_dead(ws, sleeper.owner_id, by="operator", evidence=[], operator="op")
        _kernel.recover(ws, recoverer, sleeper.owner_id)
        assert ws.check().ok
        assert _kernel.claim(ws, sleeper, ref) is not None
        report = ws.check()
        assert [(finding.entry, finding.problem) for finding in report.findings] == [
            (sleeper.path, "tombstoned_owner_with_jobs")
        ]
        _kernel.recover(ws, recoverer, sleeper.owner_id)
    assert ws.check().ok


def test_fsck_reports_an_unreadable_state_document(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    (ref.path / "state.json").write_text("{torn", encoding="utf-8")
    report = ws.check(repair=True)
    assert [(finding.entry, finding.problem, finding.action) for finding in report.findings] == [
        (ref.path / "state.json", "unreadable_state", "reported")
    ]
    assert find(ws, ref.job_id).path == ref.path


def test_fsck_reports_a_job_another_user_owns(tmp_path: Path, monkeypatch) -> None:
    ws = workspace(tmp_path / "workspace")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    monkeypatch.setattr(os, "geteuid", lambda: os.getuid() + 1)
    report = ws.check()
    assert [(finding.entry, finding.problem) for finding in report.findings] == [(ref.path, "foreign_owner")]


def test_the_fsck_command_prints_the_findings(tmp_path: Path, capsys) -> None:
    ws = workspace(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    assert command(["workspace", "fsck", "--json", register_ws(context, ws.root)], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["findings"] == []
