"""A push refused before adoption takes back only the holds its own run created (plan WORKFLOW_TRANSFER_SESSION §7.1).

A hold that predates the run may have been adopted by an earlier delivery whose answer was lost: taking it back
would leave its jobs in both workspaces. Such a hold stays held, reported ``failed``; the holds that predate a run
and that run's own are delivered separately, so that a refusal of the new ones can still take them back.
"""

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace
from httk.workflow.adapters import TransferSession
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command
from test_job_transfer import (
    adapter_calls,
    at_once,
    jobs,
    outcomes,
    outgoing,
    recorded_requests,
    registered,
    run,
    selected,
    starts,
)
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_sync_errors_retry import Far, refusal
from test_sync_landings import statuses, transfer

#: A far side's answer to a segment that this side cannot read: whether anything was adopted is unknown.
UNREADABLE = {"ok": False, "error": "garbled"}


@pytest.fixture
def ssh_pair(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster, a local SRC and a DST on the cluster."""

    project = tmp_path / "project"
    initialize_project(project, name="ssh-takeback")
    fake_remote(project)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


@pytest.fixture
def refusing() -> Iterator[list[Workspace]]:
    """Make the listed workspaces' ``transfers/incoming`` and ``tmp`` unwritable, so that a delivery there is refused
    before anything is adopted (no landing can be made: a staged push lands in an owner scratch of ``tmp``, plan
    §7.10); restored afterwards."""

    workspaces: list[Workspace] = []
    yield workspaces
    for workspace in workspaces:
        unrefuse(workspace)


def refuse(refusing: list[Workspace], workspace: Workspace) -> None:
    for landing in (workspace.control / "transfers" / "incoming", workspace.control / "tmp"):
        landing.mkdir(parents=True, exist_ok=True)
        landing.chmod(0o555)
    refusing.append(workspace)


def unrefuse(workspace: Workspace) -> None:
    for landing in (workspace.control / "transfers" / "incoming", workspace.control / "tmp"):
        landing.chmod(0o755)


def adopted_unreadably(
    project: Path, near_name: str, far_name: str, plan: Any, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> list[str]:
    """Push *plan*: the far side adopts it, but its answer is unreadable, so the holds stay."""

    real = TransferSession.request

    def garbled(session: TransferSession, op: str, header: Any = None, **options: Any) -> dict[str, Any]:
        response = real(session, op, header, **options)
        return {"ok": True, "garbled": True} if op == "adopt_staged" else response

    monkeypatch.setattr(TransferSession, "request", garbled)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("whether the jobs arrived is unknown" in outcome["message"] for outcome in transfers)
    monkeypatch.setattr(TransferSession, "request", real)
    return sorted(outcome["transfer_id"] for outcome in transfers)


def assert_kept_held(
    transfers: list[dict[str, Any]], near: Workspace, far: Workspace, far_name: str, *, keep: bool = False
) -> None:
    for outcome in transfers:
        message, transfer_id = outcome["message"], outcome["transfer_id"]
        assert outcome["status"] == "failed" and message.startswith("refused before anything was adopted: ")
        assert f"stays held on {near.root}, because an earlier delivery may already have adopted it" in message
        assert f"httk job show` on cluster:{far_name}" in message
        assert f"httk job transfer --resume {near.root} cluster:{far_name}" in message
        kept = f" --keep --destination-id {far.workspace_id}" if keep else ""
        assert f"if the jobs are there, release with `httk job transfer --release {transfer_id}{kept} {near.root}`" in (
            message
        )
        hold = near.control / "transfers" / "outgoing" / transfer_id
        assert f"if they are not there, take it back with `httk job adopt --workspace {near.root} {hold}`" in message


def take_back_by_hand(outcome: dict[str, Any], cwd: Path) -> int:
    """Run the hand take-back the kept hold's message names."""

    (remedy,) = re.findall(r"take it back with `httk (job adopt [^`]*)`", outcome["message"])
    return command(remedy.split(), CLIContext("httk", cwd))


@pytest.mark.parametrize("resume", [True, False], ids=["resume", "push"])
def test_a_refused_redelivery_never_takes_back_a_hold_an_earlier_run_may_have_delivered(
    ssh_pair: tuple[Path, Workspace, str, Workspace, str],
    refusing: list[Workspace],
    resume: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh_pair
    plan = jobs(near, 2)
    held = adopted_unreadably(project, near_name, far_name, plan, monkeypatch, capsys)
    assert_in(far, plan)
    # Run 2 re-drives the holds (a --resume, or any later push to DST), and DST refuses the stream before adopting.
    refuse(refusing, far)
    # The push variant also asks for away copies: the release remedy then keeps them, for DST's reported id.
    argv = ["--resume", near_name] if resume else ["--keep", "--state", "ready", near_name]
    assert run(project, "--json", "-q", *argv, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert sorted(outcome["transfer_id"] for outcome in transfers) == held
    assert_kept_held(transfers, near, far, far_name, keep=not resume)
    # Not back in SRC: the jobs are only in DST, and the holds stay for a later --resume or release.
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert sorted(outgoing(near)) == held
    assert_in(far, plan)
    # Once DST takes deliveries again, --resume finds them already adopted and releases the holds.
    unrefuse(refusing.pop())
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["already_adopted"] * 2
    assert outgoing(near) == [] and all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_refused_earlier_hold_stops_the_push_before_the_new_jobs_are_held(
    ssh_pair: tuple[Path, Workspace, str, Workspace, str],
    refusing: list[Workspace],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh_pair
    earlier = jobs(near, 2)
    held = adopted_unreadably(project, near_name, far_name, earlier, monkeypatch, capsys)
    new = jobs(near, 2)
    refuse(refusing, far)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    before = len(remote.commands())
    assert run(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    by_status: dict[str, list[dict[str, Any]]] = {}
    for outcome in transfers:
        by_status.setdefault(outcome["status"], []).append(outcome)
    assert sorted(outcome["transfer_id"] for outcome in by_status["failed"]) == held
    assert_kept_held(by_status["failed"], near, far, far_name)
    # The earlier holds go first (plan §7.8, step 1), and the refusal of their segment as a whole stops the push
    # before the selection is held: the two new jobs were never held, so nothing of them needs taking back.
    assert sorted(by_status) == ["failed"]
    assert any("2 selected job(s) were not attempted" in error for error in document["errors"])
    stages = [header for op, header in requests if op == "stage"]
    assert len(stages) == 1 and sorted(stages[0]["transfer_ids"]) == held and calls == []
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (1, 1)
    # The new jobs are still in SRC and not in DST; the earlier ones are only in DST and stay held.
    assert_in(near, new)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in new)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in earlier)
    assert_in(far, earlier)
    assert sorted(outgoing(near)) == held


def test_earlier_and_new_holds_both_adopted_go_in_separate_segments(
    ssh_pair: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh_pair
    earlier = jobs(near, 2)
    held = adopted_unreadably(project, near_name, far_name, earlier, monkeypatch, capsys)
    new = jobs(near, 1)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    before = len(remote.commands())
    assert run(project, "--json", "-q", "--keep", *selected(new), near_name, f"cluster:{far_name}") == 0
    transfers = outcomes(capsys)["transfers"]
    statuses_by_id = {outcome["transfer_id"]: outcome["status"] for outcome in transfers}
    assert {statuses_by_id[transfer_id] for transfer_id in held} == {"already_adopted"}
    assert sorted(statuses_by_id.values()) == ["adopted", "already_adopted", "already_adopted"]
    assert all(outcome["kept"] for outcome in transfers)
    # Two segments, the earlier holds first, over one connection and one remote start.
    stages = [sorted(header["transfer_ids"]) for op, header in requests if op == "stage"]
    assert stages == [held, sorted(set(statuses_by_id) - set(held))] and calls == []
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(far, [*earlier, *new])
    assert outgoing(near) == []
    for mapping, _, _ in [*earlier, *new]:
        ref = find(near, mapping["id"])
        assert ref is not None and ref.state == "away"


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project whose ``cluster`` remote is the ``local`` adapter (for the scripted far side :class:`Far`)."""

    root = tmp_path / "project"
    initialize_project(root, name="takeback-scripted")
    fake_remote(root, template="local", name="cluster")
    return root


@pytest.mark.parametrize("answer", ["refused", "adopted"])
def test_a_second_delivery_answered_by_another_workspace_keeps_its_new_holds(
    project: Path, tmp_path: Path, answer: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = at_once(registered)(tmp_path, "far")  # each staged segment adopted before the next
    other = "00000000-0000-4000-8000-000000000001"
    earlier = jobs(near, 1)
    # Run 1's answer is lost before the far side adopts: the hold stays, to be re-driven by run 2.
    Far(monkeypatch, [UNREADABLE])
    assert transfer(project, "--json", "-q", *selected(earlier), near_name, f"cluster:{far_name}") == 1
    (held,) = (outcome["transfer_id"] for outcome in statuses(capsys))
    new = jobs(near, 1)

    def elsewhere(real: dict[str, Any]) -> dict[str, Any]:
        return {**real, "workspace_id": other}  # staged, but by another workspace

    second = refusal(other, "refused the stream: injected") if answer == "refused" else elsewhere
    far_side = Far(monkeypatch, [None, second])
    assert transfer(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in statuses(capsys)}
    first = by_id.pop(held)
    (fresh,) = by_id.values()
    assert len(far_side.calls) == 2 and first["status"] == "adopted"
    # Never taken back: whether (or where) the new jobs arrived is unknown, so their hold stays.
    assert fresh["status"] == "failed" and outgoing(near) == [fresh["transfer_id"]]
    if answer == "refused":
        assert "whether the jobs arrived is unknown" in fresh["message"]
        expected = f"answered this delivery as workspace {other}, the batch's earlier delivery as {far.workspace_id}"
        assert expected in fresh["message"]
        assert all(find(far, mapping["id"]) is None for mapping, _, _ in new)
    else:
        assert (
            f"staged the segment in workspace {other}, not the expected workspace {far.workspace_id} that answered "
            "the batch's earlier delivery; nothing of it was adopted" in fresh["message"]
        )
        assert all(find(far, mapping["id"]) is None for mapping, _, _ in new)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in new)
    assert_in(far, earlier)


def held_before(project: Path, near_name: str, far_name: str, plan: Any, monkeypatch: Any, capsys: Any) -> str:
    """Push *plan* (one job) to a scripted far side whose answer is lost before it adopts: the hold stays."""

    Far(monkeypatch, [UNREADABLE])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    ((transfer_id, status),) = [(outcome["transfer_id"], outcome["status"]) for outcome in statuses(capsys)]
    assert status == "failed"
    return str(transfer_id)


def test_a_retry_with_no_workspace_id_is_held_to_the_earlier_delivery_s(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = at_once(registered)(tmp_path, "far")  # each staged segment adopted before the next
    other = "00000000-0000-4000-8000-000000000001"
    earlier = jobs(near, 1)
    held = held_before(project, near_name, far_name, earlier, monkeypatch, capsys)
    new = jobs(near, 1)
    # The new holds' first attempt fails its sync commit naming no workspace; the retry is refused by another.
    unnamed = refusal(None, "refused the stream: syncfs failed", sync_failed=True)
    elsewhere = refusal(other, "refused the stream: injected")
    far_side = Far(monkeypatch, [None, unnamed, elsewhere])
    assert transfer(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in statuses(capsys)}
    assert by_id.pop(held)["status"] == "adopted"
    (fresh,) = by_id.values()
    assert far_side.strengthened() == [False, False, True]
    # Not taken back: the refusal came from another workspace than the batch's earlier delivery.
    assert fresh["status"] == "failed" and "whether the jobs arrived is unknown" in fresh["message"]
    expected = f"answered the second delivery as workspace {other}, the batch's earlier delivery as {far.workspace_id}"
    assert expected in fresh["message"]
    assert outgoing(near) == [fresh["transfer_id"]] and find(near, new[0][0]["id"]) is None


def test_a_hold_made_by_hold_alone_is_never_taken_back_by_a_refused_resume(
    ssh_pair: tuple[Path, Workspace, str, Workspace, str],
    refusing: list[Workspace],
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh_pair
    plan = jobs(near, 1)
    assert run(project, "-q", "--hold", *selected(plan), near_name, f"cluster:{far_name}") == 0
    capsys.readouterr()
    refuse(refusing, far)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert_kept_held([outcome], near, far, far_name)
    assert outgoing(near) == [outcome["transfer_id"]]
    assert find(near, plan[0][0]["id"]) is None and find(far, plan[0][0]["id"]) is None
    # It never reached DST: the hand take-back the message names brings it back.
    assert take_back_by_hand(outcome, project) == 0
    assert_in(near, plan)
    assert outgoing(near) == []
