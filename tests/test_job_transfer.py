"""``httk job transfer``: hold + copy + adopt + release, locally and through remote adapters, and its recovery.

Ported from the pre-redesign ``test_cli_job_transfer.py``, ``test_cli_transfer_tree.py``,
``test_management_transfer.py``, ``test_transfer_status.py`` and ``test_fetch.py`` (their transfer halves).
Remote ends use the ``local`` adapter (a remote that runs ``httk`` on this machine) and, once, the stand-in ssh
cluster of ``conftest``; every adapter call is real.
"""

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _moving, hygiene
from httk.workflow.projects import initialize_project
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command, transfer_command
from test_moving import assert_in, find, place, single, tree


class Crash(BaseException):
    """A simulated process death: no ``except Exception`` cleanup runs."""


def registered(root: Path, label: str) -> tuple[Workspace, str]:
    name = f"{label}-{uuid.uuid4().hex[:8]}"
    create_workspace(name, root / label)
    workspace = Workspace(root / label, durable=False)
    workspace.set_policy({"visibility_deadline_seconds": 0.05})
    return workspace, name


def run(cwd: Path, *argv: str) -> int:
    return command(["job", "transfer", "--no-durable", *argv], CLIContext("httk", cwd))


def outcomes(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def outgoing(workspace: Workspace) -> list[str]:
    return [hold.transfer_id for hold in _moving.held(workspace)]


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project with two ``local``-adapter remotes, so ``cluster:NAME`` and ``other:NAME`` reach this machine."""

    root = tmp_path / "project"
    initialize_project(root, name="transfer")
    fake_remote(root, template="local", name="cluster")
    fake_remote(root, template="local", name="other")
    return root


# -- local to local ---------------------------------------------------------------------------------------------


def test_a_single_job_moves_keeping_state_and_priority(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, state, priority = single(source, "failed", 321)
    assert run(tmp_path, "--json", "--job", str(mapping["id"]), source_name, target_name) == 0
    document = outcomes(capsys)
    ((outcome,),) = [document["transfers"]]
    assert outcome["status"] == "adopted" and outcome["destination"] == str(target.root)
    assert outcome["missing_workflows"] == ["demo--0123456789abcdef"]
    assert_in(target, [(mapping, state, priority)])
    assert find(source, mapping["id"]) is None
    assert outgoing(source) == [] and outgoing(target) == []


def test_a_tree_moves_whole(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    root_id = str(plan[0][0]["id"])
    assert run(tmp_path, "--tree", "--job", root_id, source_name, target_name) == 0
    assert capsys.readouterr().out.split("\t")[1:3] == ["adopted", "4 job(s)"]
    assert_in(target, plan)
    assert all(find(source, mapping["id"]) is None for mapping, _, _ in plan)
    assert outgoing(source) == []


def test_unregistered_workspaces_are_addressed_by_directory(tmp_path: Path) -> None:
    source, _ = registered(tmp_path, "source")
    target, _ = registered(tmp_path, "destination")
    first, _, _ = single(source)
    assert run(tmp_path, "--job", str(first["id"]), str(source.root), str(target.root)) == 0
    assert find(target, first["id"]) is not None and find(source, first["id"]) is None
    # Relative directories resolve against the working directory.
    second = place(source, {**first, "id": str(uuid.uuid4())}, "succeeded")
    assert run(tmp_path, "--job", second.job_id, "source", "destination") == 0
    assert find(target, second.job_id) is not None


def test_a_registered_name_shadows_a_same_named_directory(tmp_path: Path) -> None:
    named, name = registered(tmp_path, "registered")
    shadowed, _ = registered(tmp_path, name)
    other, _ = registered(tmp_path, "other")
    for_name, _, _ = single(named)
    for_dir, _, _ = single(shadowed)
    assert run(tmp_path, "--job", str(for_name["id"]), name, str(other.root)) == 0
    assert find(other, for_name["id"]) is not None and find(shadowed, for_dir["id"]) is not None
    assert run(tmp_path, "--job", str(for_dir["id"]), f"./{name}", str(other.root)) == 0
    assert find(other, for_dir["id"]) is not None and find(shadowed, for_dir["id"]) is None


def test_bad_invocations_are_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _, source_name = registered(tmp_path, "a")
    assert run(tmp_path, "--job", "anything", source_name, "nowhere") == 2
    assert "nowhere" in capsys.readouterr().err
    assert run(tmp_path, source_name, source_name) == 2
    assert "--job" in capsys.readouterr().err
    assert run(tmp_path, "--job", "x", source_name, source_name) == 2
    assert "the same workspace" in capsys.readouterr().err
    assert run(tmp_path, "--release", "nothex") == 2
    assert run(tmp_path, "--resume", "--job", "x", source_name) == 2
    capsys.readouterr()


def test_a_busy_job_is_reported_and_stays(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from v3_helpers import cli_owner

    source, source_name = registered(tmp_path, "a")
    _, target_name = registered(tmp_path, "b")
    plan = tree(source)
    other = cli_owner(source)
    from test_moving import claim_root

    blocker = claim_root(source, other, plan[3][0]["id"])
    assert run(tmp_path, "--tree", "--job", str(plan[0][0]["id"]), source_name, target_name) == 1
    assert "blocks the tree" in capsys.readouterr().err
    blocker.give_back()
    other.close()
    assert_in(source, plan)


# -- refusal and recovery ---------------------------------------------------------------------------------------


def test_a_refused_bundle_stays_held_and_can_be_taken_back(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    (mapping, _, _), (present, _, _) = plan[0], plan[3]
    # One member of the tree is in the destination already: the bundle is refused.
    place(target, present, "failed", 100)
    assert run(tmp_path, "--json", "--tree", "--job", str(mapping["id"]), source_name, target_name) == 1
    document = outcomes(capsys)
    (outcome,) = document["transfers"]
    assert outcome["status"] == "refused" and "already in this workspace" in outcome["message"]
    (held,) = _moving.held(source)
    assert held.transfer_id == outcome["transfer_id"] and held.destination_locator == str(target.root)
    assert held.destination_workspace_id == target.workspace_id
    assert find(source, mapping["id"]) is None and find(target, mapping["id"]) is None
    assert (find(target, present["id"]).state, find(target, present["id"]).priority) == ("failed", 100)  # type: ignore[union-attr]

    # Inspection lists the hold: transfer status, and job why by the job's id.
    context = CLIContext("httk", tmp_path)
    assert transfer_command(["status", source_name, "--json"], context) == 0
    status = json.loads(capsys.readouterr().out)
    (listed,) = status["holds"]
    assert listed["transfer_id"] == held.transfer_id and listed["destination"] == str(target.root)
    assert listed["members"] == list(held.members) and listed["age_seconds"] >= 0
    assert transfer_command(["status", source_name], context) == 0
    assert held.transfer_id in capsys.readouterr().out
    assert command(["job", "why", "--workspace", source_name, str(mapping["id"])[:8]], context) == 0
    why = capsys.readouterr().out
    assert "is held" in why and str(held.path) in why and "job adopt" in why

    # Resuming meets the same refusal; nothing changes.
    assert run(tmp_path, "--resume", source_name, target_name) == 1
    assert outgoing(source) == [held.transfer_id]
    capsys.readouterr()
    # An abandoned transfer is taken back into the source with job adopt of the held path.
    assert command(["job", "adopt", "--workspace", source_name, str(held.path)], context) == 0
    assert_in(source, plan)
    assert outgoing(source) == []


def test_a_single_job_already_at_the_destination_is_already_adopted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # Deduplication by job UUID: the destination's copy wins and the hold is released (at-least-once delivery).
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source, "succeeded", 400)
    place(target, mapping, "failed", 100)
    assert run(tmp_path, "--json", "--job", str(mapping["id"]), source_name, target_name) == 0
    assert outcomes(capsys)["transfers"][0]["status"] == "already_adopted"
    assert find(source, mapping["id"]) is None and outgoing(source) == []
    assert find(target, mapping["id"]).state == "failed"  # type: ignore[union-attr]


def test_a_crash_after_the_hold_is_resumed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)

    def crash(*_arguments: object) -> None:
        raise Crash

    with monkeypatch.context() as patch, pytest.raises(Crash):
        patch.setattr(transfer_cli, "_deliver", crash)
        run(tmp_path, "--tree", "--job", str(plan[0][0]["id"]), source_name, target_name)
    assert len(outgoing(source)) == 1 and find(target, plan[0][0]["id"]) is None
    # Without DST, --resume drives every hold to the destination it records.
    assert run(tmp_path, "--json", "--resume", source_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted"
    assert_in(target, plan)
    assert outgoing(source) == []
    assert run(tmp_path, "--resume", source_name) == 0
    assert "no held transfers" in capsys.readouterr().out


def test_an_ordinary_run_re_drives_holds_for_its_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    first, _, _ = single(source)
    second = place(source, {**first, "id": str(uuid.uuid4())}, "paused")
    with monkeypatch.context() as patch, pytest.raises(Crash):
        patch.setattr(transfer_cli, "_deliver", lambda *_arguments: (_ for _ in ()).throw(Crash()))
        run(tmp_path, "--job", str(first["id"]), source_name, target_name)
    assert run(tmp_path, "--json", "--job", second.job_id, source_name, target_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted", "adopted"]
    assert find(target, first["id"]) is not None and find(target, second.job_id).state == "paused"  # type: ignore[union-attr]


def test_release_discards_one_hold(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    mapping, _, _ = single(source)
    context = CLIContext("httk", tmp_path)
    argv = ["job", "eject", "--no-durable", "--workspace", source_name, "--hold", "--json", str(mapping["id"])]
    assert command(argv, context) == 0
    held = json.loads(capsys.readouterr().out)
    assert Path(held["path"]).name == held["transfer_id"] and len(held["members"]) == 1
    (hold,) = _moving.held(source)
    assert hold.destination_locator is None
    # A hold that records no destination is not driven anywhere.
    assert run(tmp_path, "--resume", source_name) == 1
    assert "records no destination" in capsys.readouterr().err
    assert run(tmp_path, "--release", held["transfer_id"], source_name) == 0
    assert outgoing(source) == [] and find(source, mapping["id"]) is None
    assert run(tmp_path, "--release", held["transfer_id"], source_name) == 1


def test_a_hold_adopts_only_into_the_workspace_it_records(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, state, priority = single(source)
    elsewhere = str(uuid.uuid4())
    context = CLIContext("httk", tmp_path)
    argv = ["job", "eject", "--no-durable", "--workspace", source_name, "--hold", "--json"]
    argv += ["--destination-id", elsewhere, str(mapping["id"]), str(target.root)]
    assert command(argv, context) == 0
    held = json.loads(capsys.readouterr().out)
    assert held["destination_workspace_id"] == elsewhere and held["destination"] == str(target.root)
    assert transfer_command(["status", source_name], context) == 0
    assert f"({elsewhere})" in capsys.readouterr().out
    # The path now holds another workspace than the one the hold was made for: --resume refuses it.
    assert run(tmp_path, "--json", "--resume", source_name) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "refused" and "not this workspace" in outcome["message"]
    assert outgoing(source) == [held["transfer_id"]] and find(target, mapping["id"]) is None
    assert run(tmp_path, "--release", held["transfer_id"], source_name) == 0
    capsys.readouterr()
    # A transfer records the destination's own id, and its holds go there.
    second = place(source, {**mapping, "id": str(uuid.uuid4())}, state, priority)
    with pytest.MonkeyPatch.context() as patch, pytest.raises(Crash):
        patch.setattr(transfer_cli, "_deliver", lambda *_arguments: (_ for _ in ()).throw(Crash()))
        run(tmp_path, "--job", second.job_id, source_name, target_name)
    (hold,) = _moving.held(source)
    assert hold.destination_workspace_id == target.workspace_id
    assert run(tmp_path, "--json", "--resume", source_name) == 0
    assert outcomes(capsys)["transfers"][0]["status"] == "adopted"
    assert find(target, second.job_id) is not None


def test_a_sealed_destination_is_refused_before_anything_is_held(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from conftest import configure_identity
    from httk.workflow.seals import seal_workspace

    configure_identity()
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)
    seal_workspace(target)
    assert run(tmp_path, "--job", str(mapping["id"]), source_name, target_name) != 0
    assert "sealed" in capsys.readouterr().err
    assert outgoing(source) == [] and find(source, mapping["id"]) is not None


def test_remove_remote_checks_the_holds_of_registered_workspaces_only(tmp_path: Path, project: Path) -> None:
    from v3_helpers import cli_owner

    source, _ = registered(tmp_path, "a")
    unregistered = Workspace.initialize(tmp_path / "unregistered", durable=False)
    for workspace in (source, unregistered):
        mapping, _, _ = single(workspace)
        with cli_owner(workspace) as owner:
            root = _moving._kernel.claim(workspace, owner, find(workspace, mapping["id"]))  # type: ignore[arg-type]
            assert root is not None
            _moving.hold(
                workspace,
                owner,
                root,
                tree=False,
                destination_locator=f"{'cluster' if workspace is source else 'other'}:far",
            )
    # A registered workspace's hold bound for cluster refuses its removal, and says what was checked.
    with pytest.raises(ValueError, match="registered on this machine"):
        hygiene.remove_remote("cluster", project=project)
    # An unregistered workspace's hold bound for other is not seen.
    assert hygiene.remove_remote("other", project=project)["removed"] is True


def test_job_eject_needs_a_destination_or_hold(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    mapping, _, _ = single(source)
    assert command(["job", "eject", "--workspace", source_name, str(mapping["id"])], CLIContext("httk", tmp_path)) == 2
    assert "--hold" in capsys.readouterr().err


# -- hygiene ----------------------------------------------------------------------------------------------------


def test_stale_holds_are_reported_and_never_touched(tmp_path: Path, project: Path) -> None:
    source, _ = registered(tmp_path, "a")
    mapping, _, _ = single(source)
    (source.control / "transfers" / "incoming" / "leftover").mkdir(parents=True)
    assert hygiene._check_transfers(source.root).status == "ok"
    from v3_helpers import cli_owner

    with cli_owner(source) as owner:
        root = _moving._kernel.claim(source, owner, find(source, mapping["id"]))  # type: ignore[arg-type]
        assert root is not None
        path = Path(_moving.hold(source, owner, root, tree=False, destination_locator="cluster:far").path)
    finding = hygiene._check_transfers(source.root, days=0)
    assert finding.status == "warning" and finding.details["stale_holds"] == [str(path)]
    assert finding.details["stale_incoming"] == [str(source.control / "transfers" / "incoming" / "leftover")]
    assert path.is_dir()
    # The hold is bound for the remote: removing it is refused until the hold is gone.
    with pytest.raises(ValueError, match="held transfers"):
        hygiene.remove_remote("cluster", project=project)
    assert hygiene.remove_remote("other", project=project)["removed"] is True
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, path.name)
    assert hygiene.remove_remote("cluster", project=project)["removed"] is True


# -- across machines --------------------------------------------------------------------------------------------


def _remote_pair(tmp_path: Path) -> tuple[Workspace, str, Workspace, str]:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    return near, near_name, far, far_name


def test_local_to_remote_and_back(project: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    near, near_name, far, far_name = _remote_pair(tmp_path)
    plan = tree(near)
    root_id = str(plan[0][0]["id"])
    assert run(project, "--json", "--tree", "--job", root_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted" and outcome["destination"] == f"cluster:{far_name}"
    assert outcome["missing_workflows"] == ["demo--0123456789abcdef"]
    assert_in(far, plan)
    assert outgoing(near) == []
    # The pushed copy was taken in by adoption: nothing is left in incoming.
    assert list((far.control / "transfers" / "incoming").iterdir()) == []

    assert run(project, "--json", "--tree", "--job", root_id, f"cluster:{far_name}", near_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted"
    assert_in(near, plan)
    assert outgoing(far) == [] and find(far, root_id) is None
    assert not list((near.control / "tmp").iterdir())


def test_a_refused_push_leaves_no_copy_at_the_remote_destination(
    project: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name, far, far_name = _remote_pair(tmp_path)
    plan = tree(near)
    place(far, plan[1][0], "succeeded")
    argv = ["--json", "--tree", "--job", str(plan[0][0]["id"]), near_name, f"cluster:{far_name}"]
    assert run(project, *argv) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "refused" and "discarded" in outcome["message"]
    (hold,) = _moving.held(near)
    assert hold.destination_workspace_id == far.workspace_id  # learnt by probing the remote
    for _ in range(2):  # each --resume pushes a copy again, and each refused copy goes
        assert run(project, "--resume", near_name, f"cluster:{far_name}") == 1
        assert list((far.control / "transfers" / "incoming").iterdir()) == []
    assert outgoing(near) == [hold.transfer_id]


def test_remote_to_remote_is_relayed(project: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    near, near_name, far, far_name = _remote_pair(tmp_path)
    mapping, state, priority = single(near, "cancelled", 7)
    argv = ["--json", "--job", str(mapping["id"]), f"cluster:{near_name}", f"other:{far_name}"]
    assert run(project, *argv) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted"
    assert_in(far, [(mapping, state, priority)])
    assert outgoing(near) == [] and find(near, mapping["id"]) is None


def test_a_remote_source_requires_canonical_job_ids(project: Path, tmp_path: Path, capsys) -> None:
    near, near_name, _far, far_name = _remote_pair(tmp_path)
    mapping, _, _ = single(near)
    assert run(project, "--job", str(mapping["id"])[:8], f"cluster:{near_name}", far_name) == 2
    assert "canonical job ids" in capsys.readouterr().err
    assert find(near, mapping["id"]) is not None


def test_a_crash_between_adopt_and_release_is_finished_by_resume(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name, far, far_name = _remote_pair(tmp_path)
    mapping, state, priority = single(near)

    def crash(*_arguments: object) -> None:
        raise Crash

    with monkeypatch.context() as patch, pytest.raises(Crash):
        patch.setattr(transfer_cli, "_release", crash)
        run(project, "--job", str(mapping["id"]), near_name, f"cluster:{far_name}")
    # The copy was adopted; the hold is still there: the job is in both places until the resume.
    assert_in(far, [(mapping, state, priority)])
    assert len(outgoing(near)) == 1
    assert run(project, "--json", "--resume", near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "already_adopted"
    assert outgoing(near) == []


def test_remote_status_and_refusal_keep_the_remote_hold(
    project: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name, far, far_name = _remote_pair(tmp_path)
    plan = tree(near)
    place(far, plan[1][0], "succeeded")
    argv = ["--json", "--tree", "--job", str(plan[0][0]["id"]), f"cluster:{near_name}", far_name]
    assert run(project, *argv) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "refused"
    (held,) = outgoing(near)
    context = CLIContext("httk", project)
    assert transfer_command(["status", "--json", f"cluster:{near_name}"], context) == 0
    status = json.loads(capsys.readouterr().out)
    assert [hold["transfer_id"] for hold in status["holds"]] == [held]
    assert status["holds"][0]["destination"] == str(far.root)
    # The remote source recorded the id the client learnt from the destination.
    assert status["holds"][0]["destination_workspace_id"] == far.workspace_id
    assert not list((far.control / "tmp").iterdir())
    # The remote hold is released through the adapter.
    assert run(project, "--release", held, f"cluster:{near_name}") == 0
    assert outgoing(near) == []


def test_a_job_crosses_the_ssh_adapter_with_rsync(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="ssh-transfer")
    fake_remote(project)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    plan = tree(near)
    root_id = str(plan[0][0]["id"])
    assert run(project, "--json", "--tree", "--job", root_id, near_name, f"cluster:{far_name}") == 0
    assert outcomes(capsys)["transfers"][0]["status"] == "adopted"
    assert_in(far, plan)
    assert run(project, "--json", "--tree", "--job", root_id, f"cluster:{far_name}", near_name) == 0
    assert outcomes(capsys)["transfers"][0]["status"] == "adopted"
    assert_in(near, plan)
    commands = remote.commands()
    assert any("job eject --hold" in item for item in commands)
    assert any("job adopt --json" in item for item in commands)


def test_a_banner_on_the_remote_stdout_is_reported_and_resumed(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="ssh-banner")
    fake_remote(project)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    mapping, state, priority = single(far)
    # A talkative login shell greets the connection that holds the job, so its answer is no hold document.
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER", "*** Welcome to the fake cluster ***")
    monkeypatch.setenv("HTTK_FAKE_SSH_BANNER_WHEN", "job eject")
    assert run(project, "--json", "--job", str(mapping["id"]), f"cluster:{far_name}", near_name) == 1
    document = outcomes(capsys)
    assert "printed no hold document" in document["errors"][0]
    # The hold was made all the same, and the same run's pass over the source's holds delivered it.
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"]
    assert_in(near, [(mapping, state, priority)])
    assert outgoing(far) == []


def test_a_detached_child_transfers_alone(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    root, child = plan[0][0], plan[1][0]
    context = CLIContext("httk", tmp_path)
    assert command(["job", "detach", "--workspace", source_name, str(child["id"])], context) == 0
    assert command(["job", "detach", "--workspace", source_name, str(root["id"])], context) == 1
    capsys.readouterr()
    assert run(tmp_path, "--tree", "--job", str(child["id"]), source_name, target_name) == 0
    # The child took its own subtree (the grandchild) and left its former parent and sibling behind.
    assert find(target, child["id"]) is not None and find(target, plan[3][0]["id"]) is not None
    assert find(source, root["id"]) is not None and find(source, plan[2][0]["id"]) is not None


@pytest.mark.parametrize("component", [str(uuid.UUID(int=7)), f"parent--{uuid.UUID(int=7)}"], ids=["bare", "tagged"])
def test_adopt_refuses_a_nesting_member_placement_and_leaves_the_bundle(
    tmp_path: Path, component: str, capsys: pytest.CaptureFixture[str]
) -> None:
    # Ported from test_placement_rule_transfer.py: job directories never nest, not even through a bundle.
    source, source_name = registered(tmp_path, "a")
    _, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)
    context = CLIContext("httk", tmp_path)
    argv = ["job", "eject", "--workspace", source_name, "--json", str(mapping["id"]), str(tmp_path / "out")]
    assert command(argv, context) == 0
    bundle = Path(outcomes(capsys)["destination"])
    manifest = json.loads((bundle / "bundle.json").read_bytes())
    manifest["members"][0]["placement"] = f"project/{component}/children"
    (bundle / "bundle.json").write_text(json.dumps(manifest))
    assert command(["job", "adopt", "--workspace", target_name, str(bundle)], context) == 1
    assert "must not name a job directory" in capsys.readouterr().err
    assert json.loads((bundle / "bundle.json").read_bytes()) == manifest
