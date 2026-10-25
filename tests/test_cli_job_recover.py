"""``httk job recover``: away copies back to the states they left; and ``job why``/``job show`` of an away job."""

import argparse
import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, _kernel
from httk.workflow._state import Release, read_state_unowned
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from test_job_transfer import everyday, outgoing, registered, run
from test_moving import WORKFLOW, assert_in, claim_root, find, parent_link, place, single, tree
from v3_helpers import cli_owner, job_mapping

type Plan = list[tuple[dict[str, object], str, int]]


def recover(cwd: Path, name: str, *argv: str) -> int:
    return command(["job", "recover", "--no-durable", "--workspace", name, *argv], CLIContext("httk", cwd))


def key(ws: Workspace, mapping: dict[str, object]) -> str:
    ref = find(ws, mapping["id"])
    assert ref is not None
    return ref.job_key


def kept_tree(tmp_path: Path) -> tuple[Workspace, str, Workspace, str, Plan]:
    """A tree moved from A to B with ``job transfer --keep``: away copies in A."""

    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    assert run(tmp_path, "-q", "--keep", "--tree", "--job", str(plan[0][0]["id"]), source_name, target_name) == 0
    assert [find(source, mapping["id"]).state for mapping, _, _ in plan] == ["away"] * 4  # type: ignore[union-attr]
    return source, source_name, target, target_name, plan


def test_recover_returns_one_job_to_its_state_and_priority(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    job = single(source, "ready", 321)
    assert everyday("push", tmp_path, "-q", source_name, target_name) == 0
    capsys.readouterr()
    assert recover(tmp_path, source_name, str(job[0]["id"])) == 0
    assert capsys.readouterr().out == f"recovered {key(source, job[0])}: 1 job(s) back to ready\n"
    assert_in(source, [job])
    ref = find(source, job[0]["id"])
    assert ref is not None
    doc = read_state_unowned(ref.path / "state.json")[0]
    assert doc is not None and doc.away is None and doc.history_tail[-1]["event"] == "recovered"
    # Recovered, it is an ordinary ready job: it can be pushed again (here elsewhere), leaving a copy again.
    other, other_name = registered(tmp_path, "c")
    assert everyday("push", tmp_path, "-q", source_name, other_name) == 0
    assert_in(other, [job])
    assert find(source, job[0]["id"]).state == "away"  # type: ignore[union-attr]


def test_recover_returns_a_tree_and_refuses_a_member(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name, _target, _target_name, plan = kept_tree(tmp_path)
    root = key(source, plan[0][0])
    assert recover(tmp_path, source_name, str(plan[3][0]["id"])) == 1
    assert f"is a member of the away tree of {root}; recover {root}" in capsys.readouterr().err
    assert find(source, plan[3][0]["id"]).state == "away"  # type: ignore[union-attr]
    assert recover(tmp_path, source_name, root) == 0
    assert capsys.readouterr().out == f"recovered {root}: 4 job(s) back to paused\n"
    assert_in(source, plan)
    # Not away any more: refused, per job.
    assert recover(tmp_path, source_name, root) == 1
    assert "is paused, not away" in capsys.readouterr().err


def test_recover_all_takes_each_away_root_once_narrowed_by_placement(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name, _target, target_name, plan = kept_tree(tmp_path)
    loose = job_mapping(WORKFLOW, {"start": "succeed"}, tag="loose", placement="loose/x")
    place(source, loose, "ready", 77)
    assert everyday("push", tmp_path, "-q", source_name, target_name) == 0
    capsys.readouterr()
    assert recover(tmp_path, source_name, "--all", "--placement", "loose") == 0
    assert capsys.readouterr().out == f"recovered {key(source, loose)}: 1 job(s) back to ready\n"
    assert find(source, plan[0][0]["id"]).state == "away"  # type: ignore[union-attr]
    assert recover(tmp_path, source_name, "--all") == 0
    # The tree's members go with its root: one line.
    assert capsys.readouterr().out == f"recovered {key(source, plan[0][0])}: 4 job(s) back to paused\n"
    assert_in(source, [*plan, (loose, "ready", 77)])
    assert recover(tmp_path, source_name, "--all") == 0
    assert capsys.readouterr().out == "no away jobs to recover\n"


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        ((), "name away jobs: JOB... or --all"),
        (("--all", "x"), "give JOB selectors or --all, not both"),
        (("--placement", "proj", "x"), "--placement narrows --all"),
    ],
)
def test_recover_needs_one_selection(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], argv: tuple[str, ...], message: str
) -> None:
    _source, source_name = registered(tmp_path, "a")
    assert recover(tmp_path, source_name, *argv) == 2
    assert message in capsys.readouterr().err


def test_a_late_return_after_recover_is_refused_and_overwrites_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name, target, target_name, plan = kept_tree(tmp_path)
    assert recover(tmp_path, source_name, "--all") == 0
    # Meanwhile B finished the root, which spawned one more child.
    extra = job_mapping(WORKFLOW, {"start": "succeed"}, tag="late", placement="proj/r/late")
    extra["parent"] = parent_link(plan[0][0], target.workspace_id)
    place(target, extra, "succeeded", 450)
    with cli_owner(target) as owner:
        root = claim_root(target, owner, plan[0][0]["id"])
        doc = root.read_state()
        assert doc is not None
        child = {"job_id": extra["id"], "job_key": f"{extra['tag']}--{extra['id']}", "placement": "proj/r/late"}
        root.release(doc.with_children([*doc.children, child]), Release("succeeded", 411))
    capsys.readouterr()
    assert run(tmp_path, "--json", "--tree", "--job", str(plan[0][0]["id"]), target_name, source_name) == 1
    (outcome,) = json.loads(capsys.readouterr().out)["transfers"]
    assert outcome["status"] == "refused"
    assert "was recovered here after it was pushed to workspace" in outcome["message"]
    assert (
        outcome["message"].startswith("bundle refused: ") and "bundle refused: bundle refused" not in outcome["message"]
    )
    assert_in(source, plan)
    assert find(source, extra["id"]) is None
    assert len(outgoing(target)) == 1  # the result stays held in B


def test_why_and_show_explain_an_away_job(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name, target, _target_name, plan = kept_tree(tmp_path)
    capsys.readouterr()
    context = CLIContext("httk", tmp_path)
    root = key(source, plan[0][0])
    destination = str(target.root)
    assert command(["job", "why", "--workspace", source_name, str(plan[1][0]["id"])], context) == 0
    text = capsys.readouterr().out
    assert f"this job is away: a copy kept when it was pushed to {destination} on " in text
    assert "it does not run here" in text and "left succeeded at priority 420" in text
    assert f"bring the finished job home with 'httk job pull {destination}' (it replaces this copy)" in text
    assert f"if the job was lost there, run 'httk job recover {root}' and push it again" in text
    assert command(["job", "show", "--workspace", source_name, "--json", root], context) == 0
    (report,) = json.loads(capsys.readouterr().out)
    assert report["state"] == "away" and report["away"]["locator"] == destination
    assert report["away"]["state"] == "paused" and report["away"]["priority"] == 410
    assert command(["job", "show", "--workspace", source_name, root], context) == 0
    assert f"at {destination} since" in capsys.readouterr().out


def test_request_selections_by_step_leave_away_jobs_out(tmp_path: Path) -> None:
    source, _source_name, _target, _target_name, plan = kept_tree(tmp_path)
    ready = single(source, "ready")
    arguments = argparse.Namespace(step=["start"], job_id=[], state=[], placement=None)
    assert [ref.job_id for ref in job_cli._selected_jobs(source, tmp_path, arguments)] == [ready[0]["id"]]
    arguments.state = ["away"]  # named, it is selected (and each request but delete is refused per job)
    assert len(job_cli._selected_jobs(source, tmp_path, arguments)) == len(plan)
    assert list(_kernel.list_jobs(source, "away"))


def test_debug_and_recover_refuse_without_waiting(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name, _target, _target_name, plan = kept_tree(tmp_path)
    capsys.readouterr()
    context = CLIContext("httk", tmp_path)
    root = key(source, plan[0][0])
    argv = ["job", "debug", "--workspace", source_name, "--timeout", "30", root]
    assert command(argv, context) == 2
    assert f"{root} is away; recover it first" in capsys.readouterr().err
    with cli_owner(source) as owner:  # an owner holds it: refused per job, not raised
        held = claim_root(source, owner, plan[0][0]["id"])
        assert recover(tmp_path, source_name, str(plan[0][0]["id"])) == 1
        assert "it is running or held by an owner; recover it once it is back in away" in capsys.readouterr().err
        held.give_back()
    assert find(source, plan[0][0]["id"]).state == "away"  # type: ignore[union-attr]


def test_why_names_an_interrupted_recover(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name, _target, _target_name, plan = kept_tree(tmp_path)
    # What a recover that cleared the record and died before the job left away leaves (written in place here:
    # handing the claim back would apply the release).
    ref = find(source, plan[1][0]["id"])
    assert ref is not None
    document = json.loads((ref.path / "state.json").read_bytes())
    document.update(away=None, release_to={"state": "succeeded", "priority": 420})
    (ref.path / "state.json").write_text(json.dumps(document))
    capsys.readouterr()
    member = key(source, plan[1][0])
    assert command(["job", "why", "--workspace", source_name, member], CLIContext("httk", tmp_path)) == 0
    text = capsys.readouterr().out
    assert f"an interrupted recover left this job; run 'httk job recover {member}' to finish it" in text
    assert "delete the copy" not in text
