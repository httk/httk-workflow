"""A push refused before adoption takes back only the holds its own run created (plan WORKFLOW_TRANSFER_SESSION §7.1).

A hold that predates the run may have been adopted by an earlier delivery whose answer was lost: taking it back
would leave its jobs in both workspaces. Such a hold stays held, reported ``failed``; the holds that predate a run
and that run's own are delivered separately, so that a refusal of the new ones can still take them back.
"""

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _store
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command
from test_job_transfer import adapter_calls, jobs, outcomes, outgoing, registered, run, selected, starts
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_sync_errors_retry import Far
from test_sync_landings import statuses, transfer
from test_transfer_workflows import job as workflow_job
from test_transfer_workflows import package, shipped
from v3_helpers import cli_owner


@pytest.fixture
def ssh_pair(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster, a local SRC and a DST on the cluster."""

    project = tmp_path / "project"
    initialize_project(project, name="ssh-takeback")
    # The adapter path these tests count and script (a session's take-back: test_session_push).
    fake_remote(project, transfer_session="off")
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


@pytest.fixture
def refusing() -> Iterator[list[Workspace]]:
    """Make the listed workspaces' ``transfers/incoming`` unwritable, so that a delivery there is refused before
    anything is adopted (no landing can be made); restored afterwards."""

    workspaces: list[Workspace] = []
    yield workspaces
    for workspace in workspaces:
        (workspace.control / "transfers" / "incoming").chmod(0o755)


def refuse(refusing: list[Workspace], workspace: Workspace) -> None:
    landing = workspace.control / "transfers" / "incoming"
    landing.mkdir(parents=True, exist_ok=True)
    landing.chmod(0o555)
    refusing.append(workspace)


def adopted_unreadably(
    project: Path, near_name: str, far_name: str, plan: Any, monkeypatch: pytest.MonkeyPatch, capsys: Any
) -> list[str]:
    """Push *plan*: the far side adopts it, but a login banner makes the answer unreadable, so the holds stay."""

    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER", "*** Welcome to the fake cluster ***")
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER_WHEN", "job adopt")
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("whether the jobs arrived is unknown" in outcome["message"] for outcome in transfers)
    monkeypatch.delenv("HTTK_FAKE_SSH_BANNER")
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
    refusing.pop().control.joinpath("transfers", "incoming").chmod(0o755)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["already_adopted"] * 2
    assert outgoing(near) == [] and all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_earlier_and_new_holds_are_delivered_separately_and_only_the_new_are_taken_back(
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
    before = len(remote.commands())
    assert run(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    by_status: dict[str, list[dict[str, Any]]] = {}
    for outcome in transfers:
        by_status.setdefault(outcome["status"], []).append(outcome)
    assert sorted(outcome["transfer_id"] for outcome in by_status["failed"]) == held
    assert_kept_held(by_status["failed"], near, far, far_name)
    # The two new jobs, held as one transfer each, are taken back.
    assert sorted(by_status) == ["failed", "not_moved"] and len(by_status["not_moved"]) == 2
    assert all("the jobs are back in" in outcome["message"] for outcome in by_status["not_moved"])
    # Two deliveries, the earlier holds first, each one connection and one remote start.
    assert calls == ["deliver job adopt", "deliver job adopt"]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (2, 2)
    first, second = (command for command in commands if "job adopt" in command)
    assert first.split("--token ")[1].split()[0] != second.split("--token ")[1].split()[0]
    # The new jobs are back in SRC and not in DST; the earlier ones are only in DST and stay held.
    assert_in(near, new)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in new)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in earlier)
    assert_in(far, earlier)
    assert sorted(outgoing(near)) == held


def test_earlier_and_new_holds_both_adopted_cost_two_deliveries(
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
    before = len(remote.commands())
    assert run(project, "--json", "-q", "--keep", *selected(new), near_name, f"cluster:{far_name}") == 0
    transfers = outcomes(capsys)["transfers"]
    statuses_by_id = {outcome["transfer_id"]: outcome["status"] for outcome in transfers}
    assert {statuses_by_id[transfer_id] for transfer_id in held} == {"already_adopted"}
    assert sorted(statuses_by_id.values()) == ["adopted", "already_adopted", "already_adopted"]
    assert all(outcome["kept"] for outcome in transfers)
    assert calls == ["deliver job adopt", "deliver job adopt"]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (2, 2)
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
    fake_remote(root, template="local", name="cluster", transfer_session="off")  # Far scripts the adapter path
    return root


@pytest.mark.parametrize("answer", ["refused", "adopted"])
def test_a_second_delivery_answered_by_another_workspace_keeps_its_new_holds(
    project: Path, tmp_path: Path, answer: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    other = "00000000-0000-4000-8000-000000000001"
    earlier = jobs(near, 1)
    # Run 1's answer is lost before the far side adopts: the hold stays, to be re-driven by run 2.
    Far(monkeypatch, [(255, "", "ssh: connection reset")])
    assert transfer(project, "--json", "-q", *selected(earlier), near_name, f"cluster:{far_name}") == 1
    (held,) = (outcome["transfer_id"] for outcome in statuses(capsys))
    new = jobs(near, 1)

    def elsewhere(real: tuple[int, str, str]) -> tuple[int, str, str]:
        code, out, err = real
        return code, json.dumps({**json.loads(out), "workspace_id": other}), err

    refusal = {"bundles": [], "workspace_id": other, "refused": "refused the stream: injected"}
    second = (2, json.dumps(refusal), "") if answer == "refused" else elsewhere
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
            f"adopted the jobs into workspace {other}, not the expected workspace {far.workspace_id} that answered "
            "the batch's earlier delivery" in fresh["message"]
        )
        assert_in(far, new)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in new)
    assert_in(far, earlier)


def test_the_workflows_both_deliveries_report_are_shipped_once_after_them(
    ssh_pair: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh_pair
    installed: dict[str, _store.Installed] = {}
    for workspace in (near, far):
        with cli_owner(workspace) as owner:
            for name in ("first", "second"):
                # The same tree in both workspaces: DST reports each present.
                directory = tmp_path / f"{workspace.root.name}-{name}"
                installed[name] = _store.install(workspace, owner, package(directory, name))
    earlier, new = workflow_job(near, installed["first"]), workflow_job(near, installed["second"])
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER", "*** Welcome to the fake cluster ***")
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER_WHEN", "job adopt")
    assert run(project, "--json", "-q", "--job", earlier, near_name, f"cluster:{far_name}") == 1
    ((held, status),) = [(outcome["transfer_id"], outcome["status"]) for outcome in outcomes(capsys)["transfers"]]
    assert status == "failed"
    monkeypatch.delenv("HTTK_FAKE_SSH_BANNER")
    calls = adapter_calls(monkeypatch)
    assert run(project, "--json", "-q", "--job", new, near_name, f"cluster:{far_name}") == 0
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    assert by_id.pop(held)["status"] == "already_adopted"
    (fresh,) = by_id.values()
    assert fresh["status"] == "adopted" and shipped(fresh) == {installed["second"].id: "present"}
    # Each delivery asked about its own holds' workflows; together they report both present: nothing is installed.
    assert calls == ["deliver job adopt", "deliver job adopt"]
    assert find(far, earlier) is not None and find(far, new) is not None and outgoing(near) == []


def held_before(project: Path, near_name: str, far_name: str, plan: Any, monkeypatch: Any, capsys: Any) -> str:
    """Push *plan* (one job) to a scripted far side whose answer is lost before it adopts: the hold stays."""

    Far(monkeypatch, [(255, "", "ssh: connection reset")])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    ((transfer_id, status),) = [(outcome["transfer_id"], outcome["status"]) for outcome in statuses(capsys)]
    assert status == "failed"
    return str(transfer_id)


@pytest.mark.parametrize("adopted", ["earlier", "new"])
def test_only_the_holds_of_an_adopting_delivery_get_their_workflows_shipped(
    project: Path, tmp_path: Path, adopted: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    with cli_owner(near) as owner:
        present = _store.install(near, owner, package(tmp_path / "near-present", "present"))
        lacking = _store.install(near, owner, package(tmp_path / "near-lacking", "lacking"))
    with cli_owner(far) as owner:
        _store.install(far, owner, package(tmp_path / "far-present", "present"))
    # The delivery DST refuses carries the job whose workflow DST lacks.
    earlier_workflow, new_workflow = (present, lacking) if adopted == "earlier" else (lacking, present)
    earlier = workflow_job(near, earlier_workflow)
    Far(monkeypatch, [(255, "", "ssh: connection reset")])
    assert run(project, "--json", "-q", "--job", earlier, near_name, f"cluster:{far_name}") == 1
    ((held,),) = [[outcome["transfer_id"] for outcome in outcomes(capsys)["transfers"]]]
    new = workflow_job(near, new_workflow)
    refusal = (2, json.dumps({"bundles": [], "workspace_id": far.workspace_id, "refused": "refused: injected"}), "")
    far_side = Far(monkeypatch, [None, refusal] if adopted == "earlier" else [refusal, None])
    assert run(project, "--json", "-q", "--job", new, near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    earlier_outcome = by_id.pop(held)
    (new_outcome,) = by_id.values()
    shipped_outcome, refused_outcome = (
        (earlier_outcome, new_outcome) if adopted == "earlier" else (new_outcome, earlier_outcome)
    )
    assert shipped_outcome["status"] in ("adopted", "already_adopted")
    assert shipped(shipped_outcome) == {present.id: "present"}
    assert refused_outcome["status"] == ("not_moved" if adopted == "earlier" else "failed")
    assert "workflows" not in refused_outcome
    # Two deliveries and no workflow install: the refused job's workflow is not installed in DST.
    assert [argv[1:3] for argv in far_side.calls] == [["job", "adopt"]] * 2
    assert _store.lookup(far, lacking.id) is None


@pytest.mark.parametrize("second", ["refused", "adopted"])
def test_an_unreadable_earlier_delivery_still_lets_the_new_holds_be_delivered(
    project: Path, tmp_path: Path, second: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    earlier = jobs(near, 1)
    held = held_before(project, near_name, far_name, earlier, monkeypatch, capsys)
    new = jobs(near, 1)
    refusal = (2, json.dumps({"bundles": [], "workspace_id": far.workspace_id, "refused": "refused: injected"}), "")
    far_side = Far(monkeypatch, [(255, "", "ssh: connection reset"), refusal if second == "refused" else None])
    assert transfer(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in statuses(capsys)}
    earlier_outcome = by_id.pop(held)
    (new_outcome,) = by_id.values()
    assert len(far_side.calls) == 2
    assert earlier_outcome["status"] == "failed" and "whether the jobs arrived is unknown" in earlier_outcome["message"]
    assert outgoing(near) == [held]
    if second == "refused":
        assert new_outcome["status"] == "not_moved"
        assert_in(near, new)
    else:
        assert new_outcome["status"] == "adopted"
        assert_in(far, new)
        assert find(near, new[0][0]["id"]) is None


def test_a_retry_with_no_workspace_id_is_held_to_the_earlier_delivery_s(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    other = "00000000-0000-4000-8000-000000000001"
    earlier = jobs(near, 1)
    held = held_before(project, near_name, far_name, earlier, monkeypatch, capsys)
    new = jobs(near, 1)
    # The new holds' first attempt fails its sync commit naming no workspace; the retry is refused by another.
    unnamed: dict[str, Any] = {
        "bundles": [],
        "workspace_id": None,
        "refused": "refused the stream: syncfs failed",
        "sync_failed": True,
    }
    elsewhere = {"bundles": [], "workspace_id": other, "refused": "refused the stream: injected"}
    far_side = Far(monkeypatch, [None, (2, json.dumps(unnamed), ""), (2, json.dumps(elsewhere), "")])
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
