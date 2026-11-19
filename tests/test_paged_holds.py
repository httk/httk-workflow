"""Paged holds (plan WORKFLOW_TRANSFER_SESSION §7.8, T3).

A local SRC (push; local → local) and a remote SRC over its transfer session (pull) hold their selection page by
page and drive each page before holding the next. With nothing changing concurrently, every page size holds what
holding everything first (``job transfer --hold``) holds; the deliberate differences, the bounds of a page, the state
each pair keeps across pages, the stops, the workflow attempts and the counts are tested here.
"""

import json
import shutil
import sys
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _frames, _kernel, _moving, _store
from httk.workflow._state import Release
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_job_transfer import (
    adapter_calls,
    adopted_or_abandoned,
    agrees_with_state,
    at_once,
    jobs,
    outgoing,
    recorded_requests,
    registered,
    requests_in_order,
    run,
    same_requests,
    selected,
    starts,
)
from test_moving import WORKFLOW, assert_in, find, io_workers, parent_link, place, single, tree  # noqa: F401
from test_session_pull import PATCHED_PULL, opened
from test_session_push import adopts, local_project, ops
from test_transfer_workflows import GIT_ID, shipped, workflows
from test_transfer_workflows import job as workflow_job
from v3_helpers import cli_owner, job_mapping

#: Faults of a paged pull's far side, added to :data:`test_session_pull.PATCHED_PULL`: the n-th ``hold_page`` holding
#: its page and then ending the process before it answers.
PAGED_PATCHES = """
if argv[:2] == ["workflow", "transfer-session"]:
    from httk.workflow.workflow_cli import _transfer as far_transfer

    if os.environ.get("HTTK_TEST_FAR_PAGE_HOLDS"):
        far_transfer._PAGE_HOLDS = int(os.environ["HTTK_TEST_FAR_PAGE_HOLDS"])
    if os.environ.get("HTTK_TEST_FAR_PAGE_SCANNED"):
        far_transfer._PAGE_SCANNED = int(os.environ["HTTK_TEST_FAR_PAGE_SCANNED"])
    nth_op = os.environ.get("HTTK_TEST_EXIT_ON_NTH_OP")
    if nth_op:
        # "op:n": the n-th request of that op ends the process before it is answered.
        nth_name, nth_count = nth_op.split(":")
        real_nth = _transfer_session.OPS[nth_name]
        nth_seen = []

        def exit_on_nth(request):
            nth_seen.append(1)
            if len(nth_seen) == int(nth_count):
                os._exit(3)
            return real_nth(request)

        _transfer_session.OPS[nth_name] = exit_on_nth
    exit_workspace = os.environ.get("HTTK_TEST_EXIT_ADOPT_WORKSPACE")
    if exit_workspace:
        for delivery in ("stage",):

            def delivery_or_exit(request, real=_transfer_session.OPS[delivery]):
                if request.header.get("workspace") == exit_workspace:
                    os._exit(3)
                return real(request)

            _transfer_session.OPS[delivery] = delivery_or_exit
    if os.environ.get("HTTK_TEST_DECIDE_FAILS_ONCE"):
        real_decide = far_transfer._Selection._decide
        decided = []

        def decide_once(selection, *arguments, **options):
            if not decided:
                decided.append(1)
                raise RuntimeError("a scripted failure while deciding")
            return real_decide(selection, *arguments, **options)

        far_transfer._Selection._decide = decide_once
    listing_noise = int(os.environ.get("HTTK_TEST_LISTING_NOISE", "0"))
    if listing_noise:
        from httk.workflow import _moving as far_moving

        real_held_one = far_moving.held_one

        def noisy_held_one(*arguments, **options):
            print("l" * listing_noise, file=sys.stderr)  # diagnostics of a listing, captured for its answer
            return real_held_one(*arguments, **options)

        far_moving.held_one = noisy_held_one
    long_errors = int(os.environ.get("HTTK_TEST_LONG_ERRORS", "0"))
    if long_errors:

        def long_error(selection, ref, *arguments, **options):
            return f"{ref.job_key}: " + "e" * long_errors  # every candidate an error of that many characters

        far_transfer._Selection._decide = long_error
    if os.environ.get("HTTK_TEST_EMPTY_PAGES"):

        def empty_page(request):
            # Out of step: an answer that is not done, carries nothing and consumed nothing.
            return {"holds": [], "errors": [], "changed": 0, "remaining": 1, "consumed": 0, "carried": 0,
                    "done": False, "note": None, "stderr": ""}

        _transfer_session.OPS["hold_page"] = empty_page
    if os.environ.get("HTTK_TEST_ERROR_PAGES"):

        def error_page(request):
            # Out of step: the same error again and again, consumed standing still.
            return {"holds": [], "errors": ["a repeated error"], "changed": 0, "remaining": 1, "consumed": 5,
                    "carried": 0, "done": False, "note": None, "stderr": ""}

        _transfer_session.OPS["hold_page"] = error_page
    if os.environ.get("HTTK_TEST_SNAPSHOT_FAILS"):

        def failing_snapshot(selection, tell=None):
            raise OSError(5, "the listing could not be read")

        far_transfer._Selection.snapshot = failing_snapshot
    noise = int(os.environ.get("HTTK_TEST_PAGE_NOISE", "0"))
    if noise:
        real_next_page = far_transfer._Selection.next_page

        def noisy_next_page(selection, **options):
            print("n" * noise, file=sys.stderr)  # diagnostics of the page, captured for its answer
            return real_next_page(selection, **options)

        far_transfer._Selection.next_page = noisy_next_page
    lose = int(os.environ.get("HTTK_TEST_LOSE_HOLD_PAGE", "0"))
    if lose:
        real_hold_page = _transfer_session.OPS["hold_page"]
        pages = []

        def hold_page(request):
            pages.append(1)
            response = real_hold_page(request)
            if len(pages) == lose:
                os._exit(3)  # held, but the answer is lost
            return response

        _transfer_session.OPS["hold_page"] = hold_page
"""
PATCHED_PAGED = PATCHED_PULL.replace('sys.argv = ["httk", *argv]\n', PAGED_PATCHES + 'sys.argv = ["httk", *argv]\n')
assert PATCHED_PAGED != PATCHED_PULL


#: The tests skipped with a positive visibility deadline, and why (by design; see each).
AT_ONCE_ONLY: dict[str, str] = {
    "test_keep_between_pages_refuses_a_parent_whose_child_left_an_away_copy": (
        "with a positive deadline the child's page may still be staged (a hold, not an away copy) when the parent's "
        "page is held, so whether the parent is refused depends on the schedule (documented in workflow_cli.md)"
    ),
}
#: The visibility deadline of the workspaces the running test registers (see :func:`_visibility_deadline`).
DEADLINE = 0.0


@pytest.fixture(autouse=True, params=[0, 0.05], ids=["D0", "D0.05"])
def _visibility_deadline(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> float:
    """Every test of this module twice: with a zero visibility deadline (each staged segment is adopted before the
    next is staged or the next page held, as before the pipelined settle of plan §7.10), and with a positive one
    (staged segments wait while later ones are copied). A test whose assertions are a request order of the first
    kind only is named in :data:`AT_ONCE_ONLY` and skipped with the positive deadline. Request orders are compared
    with :func:`same_requests`, and what a stop gives up with :func:`adopted_or_abandoned`, both by :data:`DEADLINE`."""

    deadline = float(request.param)
    if deadline and request.node.originalname in AT_ONCE_ONLY:
        pytest.skip(AT_ONCE_ONLY[request.node.originalname])
    monkeypatch.setattr(sys.modules[__name__], "registered", at_once(registered, deadline))
    monkeypatch.setattr(sys.modules[__name__], "DEADLINE", deadline)
    return deadline


def patched_paged(tmp_path: Path, remote: Remote) -> None:
    """Make the stand-in cluster's ``httk`` (the ``local`` adapter's too) :data:`PATCHED_PAGED`."""

    wrapper = tmp_path / "patched_paged.py"
    wrapper.write_text(PATCHED_PAGED, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')


#: The page sizes the equivalence runs: a page of 1, 2 and 3 candidates, and one page for everything.
SIZES = [1, 2, 3, None]


def paged(monkeypatch: pytest.MonkeyPatch, size: int | None, *, members: int | None = None) -> None:
    """Close every page at *size* candidates attempted, errors and candidates scanned (``None``: never)."""

    limit = 10**6 if size is None else size
    for name in ("_PAGE_HOLDS", "_PAGE_ERRORS", "_PAGE_SCANNED"):
        monkeypatch.setattr(transfer_cli, name, limit)
    if members is not None:
        monkeypatch.setattr(transfer_cli, "_PAGE_MEMBERS", members)


def pages(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int, bool]]:
    """Record each page this process holds: its holds, its errors and whether it was the last."""

    seen: list[tuple[int, int, bool]] = []
    real = transfer_cli._Selection.next_page

    def next_page(selection: Any, **options: Any) -> Any:
        page = real(selection, **options)
        seen.append((len(page.holds), len(page.errors), page.done))
        return page

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    return seen


def after_page(monkeypatch: pytest.MonkeyPatch, page: int, action: Callable[[], object]) -> None:
    """Call *action* after the *page*-th page this process drives (local SRC or pull)."""

    real = transfer_cli._drive
    driven: list[int] = []

    def drive(*arguments: Any, **options: Any) -> Any:
        outcome = real(*arguments, **options)
        driven.append(1)
        if len(driven) == page:
            action()
        return outcome

    monkeypatch.setattr(transfer_cli, "_drive", drive)


def reprioritize(workspace: Workspace, job_id: object, priority: int) -> None:
    """Change a job's priority (its directory name), as a ``set_priority`` request does."""

    ref = find(workspace, job_id)
    assert ref is not None
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = owned.read_state()
        assert doc is not None
        owned.release(doc, Release(ref.state, priority))


def family(workspace: Workspace, parent_at: str, children: dict[str, tuple[str, str]]) -> dict[str, Any]:
    """A succeeded parent at *parent_at* and its children by name, ``(state, placement)``: the mappings by name."""

    parent = job_mapping(WORKFLOW, {"start": "succeed"}, tag="parent", placement=parent_at)
    made: dict[str, Any] = {"parent": parent}
    for name, (state, placement) in children.items():
        child = job_mapping(WORKFLOW, {"start": "succeed"}, tag=name, placement=placement)
        child["parent"] = parent_link(parent, workspace.workspace_id)
        place(workspace, child, state, 300)
        made[name] = child
    place(workspace, parent, "succeeded", 300, tuple(made[name] for name in children))
    return made


def ids(*mappings: Any) -> list[str]:
    return [item for mapping in mappings for item in ("--job", str(mapping["id"]))]


def notes(err: str, *roots: Path) -> list[str]:
    """The notes of a selection (jobs left out by ``--step``, jobs that move only with their root), roots masked."""

    lines = [line for line in err.splitlines() if "left out" in line or "move only with their root" in line]
    return [masked(line, *roots) for line in lines]


def masked(text: str, *roots: Path) -> str:
    for root in roots:
        text = text.replace(str(root), "<SRC>")
    return text


# -- what every page size holds -------------------------------------------------------------------------------------


def _trees(workspace: Workspace, name: str) -> list[str]:
    tree(workspace)
    tree(workspace)
    single(workspace)
    return ["--state", "paused", "--state", "succeeded", "--tree"]


def _detached(workspace: Workspace, name: str) -> list[str]:
    made = family(workspace, "camp/p", {"c1": ("succeeded", "camp/p/c1"), "c2": ("succeeded", "camp/p/c2")})
    assert command(["job", "detach", "--workspace", name, str(made["c1"]["id"])], CLIContext("httk", Path.cwd())) == 0
    single(workspace)
    return ["--state", "succeeded"]


def _job_and_state(workspace: Workspace, name: str) -> list[str]:
    plan = jobs(workspace, 3)
    ready = single(workspace, "ready")
    return [*ids(ready[0], plan[1][0]), "--state", "succeeded"]


def _duplicates(workspace: Workspace, name: str) -> list[str]:
    plan = jobs(workspace, 3)
    return [*ids(plan[2][0], plan[0][0], plan[2][0]), "--state", "succeeded"]


def _placements(workspace: Workspace, name: str) -> list[str]:
    single(workspace)
    family(workspace, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    family(workspace, "camp/q", {"c1": ("succeeded", "other/c1")})
    return ["--state", "succeeded", "--placement", "camp", "--tree"]


def _steps(workspace: Workspace, name: str) -> list[str]:
    plan = jobs(workspace, 3)
    # Every job is at step start: the named ones are left out with a line each, the listed ones silently.
    return [*ids(plan[0][0], plan[1][0]), "--step", "other", "--state", "succeeded"]


def _child_before_root(workspace: Workspace, name: str) -> list[str]:
    family(workspace, "camp/p", {"c1": ("succeeded", "aaa/c1")})
    single(workspace)
    return ["--state", "succeeded", "--tree"]


def _child_before_root_without_tree(workspace: Workspace, name: str) -> list[str]:
    family(workspace, "camp/p", {"c1": ("succeeded", "aaa/c1")})
    single(workspace)
    return ["--state", "succeeded"]


def _step_child_after_root(workspace: Workspace, name: str) -> list[str]:
    family(workspace, "aaa/p", {"c1": ("succeeded", "camp/c1"), "c2": ("succeeded", "camp/c2")})
    return ["--state", "succeeded", "--step", "start", "--tree"]


def _refused_tree(workspace: Workspace, name: str) -> list[str]:
    family(workspace, "camp/p", {"c1": ("succeeded", "camp/p/c1"), "c2": ("ready", "camp/p/c2")})
    jobs(workspace, 2)
    return ["--state", "succeeded", "--tree"]


SCENARIOS = {
    "trees": _trees,
    "detached": _detached,
    "job-and-state": _job_and_state,
    "duplicates": _duplicates,
    "placements": _placements,
    "steps": _steps,
    "child-before-root": _child_before_root,
    "child-before-root-no-tree": _child_before_root_without_tree,
    "step-child-after-root": _step_child_after_root,
    "refused-tree": _refused_tree,
}


def everything_first(
    tmp_path: Path, workspace: Workspace, argv: list[str], capsys: pytest.CaptureFixture[str]
) -> tuple[list[list[str]], list[str], list[str]]:
    """What ``job transfer --hold`` holds of a copy of *workspace*: each hold's members, the errors, the notes."""

    copy = tmp_path / f"copy-{uuid.uuid4().hex[:8]}"
    shutil.copytree(workspace.root, copy, symlinks=True)
    capsys.readouterr()
    code = run(tmp_path, "--hold", "--json", *argv, str(copy), "elsewhere:x")
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert code == (1 if document["errors"] else 0)
    errors = [masked(error, copy) for error in document["errors"]]
    return [hold["members"] for hold in document["holds"]], errors, notes(captured.err, copy)


@pytest.mark.parametrize("size", SIZES, ids=lambda size: f"page={size}")
@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_every_page_size_holds_what_holding_everything_first_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], scenario: str, size: int | None
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    argv = SCENARIOS[scenario](source, source_name)
    holds, errors, expected = everything_first(tmp_path, source, argv, capsys)
    assert holds or scenario == "steps"  # every other scenario holds something
    assert bool(errors) == (scenario in ("detached", "child-before-root-no-tree", "refused-tree"))
    assert bool(expected) == (scenario in ("steps", "detached", "child-before-root-no-tree", "refused-tree"))
    paged(monkeypatch, size)
    seen = pages(monkeypatch)
    code = run(tmp_path, "--json", "-q", *argv, source_name, target_name)
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert [outcome["members"] for outcome in document["transfers"]] == holds
    assert all(outcome["status"] == "adopted" for outcome in document["transfers"])
    assert [masked(error, source.root) for error in document["errors"]] == errors
    assert notes(captured.err, source.root) == expected
    assert document["changed"] == 0 and code == (1 if errors else 0)
    if size is None:
        assert len(seen) == 1
    assert outgoing(source) == [] and outgoing(target) == []


@pytest.mark.parametrize("size", [1, 3, None], ids=lambda size: f"page={size}")
@pytest.mark.parametrize("scenario", ["trees", "detached", "job-and-state", "child-before-root", "steps"])
def test_every_page_size_of_a_pull_holds_what_holding_everything_first_holds(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    scenario: str,
    size: int | None,
) -> None:
    project, _near, near_name, far, far_name = paged_ssh
    argv = SCENARIOS[scenario](far, far_name)
    holds, errors, expected = everything_first(tmp_path, far, argv, capsys)
    paged(monkeypatch, size)
    requests = recorded_requests(monkeypatch)
    code = run(project, "--json", "-q", *argv, f"cluster:{far_name}", near_name)
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert [outcome["members"] for outcome in document["transfers"]] == holds
    assert all(outcome["status"] == "adopted" for outcome in document["transfers"])
    assert [masked(error, far.root) for error in document["errors"]] == errors
    assert notes(captured.err, far.root) == expected
    assert document["changed"] == 0 and code == (1 if errors else 0)
    assert ops(requests)[:2] == ["select", "holds_page"] and ops(requests).count("select") == 1
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert outgoing(far) == []


# -- the deliberate differences -------------------------------------------------------------------------------------


def test_keep_between_pages_refuses_a_parent_whose_child_left_an_away_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    argv = ["--json", "-q", "--keep", *ids(made["c1"], made["parent"]), source_name, target_name]
    # Holding everything first holds both: the child is in transfers/outgoing/ when the parent's tree is looked at.
    copy_source, copy_name = registered(tmp_path, "copy")
    copied = family(copy_source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    _other, other_name = registered(tmp_path, "other")
    paged(monkeypatch, None)
    assert run(tmp_path, "--json", "-q", "--keep", *ids(copied["c1"], copied["parent"]), copy_name, other_name) == 0
    assert [outcome["status"] for outcome in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"] * 2
    # A page each: the child's release keeps an away copy, which the parent's tree check refuses (tree safety).
    paged(monkeypatch, 1)
    assert run(tmp_path, *argv) == 1
    document = json.loads(capsys.readouterr().out)
    (outcome,) = document["transfers"]
    assert outcome["status"] == "adopted" and outcome["kept"] is True
    (error,) = document["errors"]
    assert "blocks the tree" in error and "away" in error
    assert find(target, made["c1"]["id"]) is not None and find(target, made["parent"]["id"]) is None
    assert find(source, made["parent"]["id"]) is not None


def test_a_listed_job_that_changes_between_pages_is_counted_and_the_command_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 3)
    paged(monkeypatch, 1)
    changed: list[object] = []

    def change_one() -> None:
        # A job still in SRC after the first page (listed in an order of their random ids).
        left = [mapping["id"] for mapping, _, _ in plan if find(source, mapping["id"]) is not None]
        changed.append(left[-1])
        reprioritize(source, left[-1], 777)

    after_page(monkeypatch, 1, change_one)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert document["changed"] == 1 and document["errors"] == []
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"] * 2
    assert "1 selected job(s) changed since they were listed: left out; run again to select them" in captured.err
    (job_id,) = changed
    assert find(source, job_id).priority == 777  # type: ignore[union-attr]
    # Run again: it is selected.
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 0
    assert json.loads(capsys.readouterr().out)["changed"] == 0
    assert find(target, job_id) is not None


def test_a_listed_job_whose_claim_misses_is_counted_as_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Read before the hold (its parent is here, but it is detached), renamed right after the read: the claim misses.
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    assert (
        command(["job", "detach", "--workspace", source_name, str(made["c1"]["id"])], CLIContext("httk", tmp_path)) == 0
    )
    capsys.readouterr()
    real = transfer_cli._parent_here

    def read_then_moved(ref: Any, present: Any, *, step: bool) -> Any:
        found = real(ref, present, step=step)
        if ref.job_id == str(made["c1"]["id"]):
            reprioritize(source, ref.job_id, 555)
        return found

    monkeypatch.setattr(transfer_cli, "_parent_here", read_then_moved)
    assert run(tmp_path, "--json", "-q", *ids(made["parent"]), "--state", "succeeded", source_name, target_name) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["changed"] == 1 and document["errors"] == []
    assert find(target, made["parent"]["id"]) is not None and find(source, made["c1"]["id"]) is not None


def test_a_named_job_that_changes_between_pages_is_re_located_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    after_page(monkeypatch, 1, lambda: reprioritize(source, plan[1][0]["id"], 777))
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["changed"] == 0 and document["errors"] == []
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"] * 2
    assert find(target, plan[1][0]["id"]).priority == 777  # type: ignore[union-attr]


@pytest.mark.parametrize("size", [1, None], ids=lambda size: f"page={size}")
def test_a_named_parent_and_child_with_tree_are_one_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], size: int | None
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    paged(monkeypatch, size)
    assert run(tmp_path, "--json", "-q", "--tree", *ids(made["parent"], made["c1"]), source_name, target_name) == 0
    (outcome,) = json.loads(capsys.readouterr().out)["transfers"]
    assert len(outcome["members"]) == 2 and outcome["status"] == "adopted"


# -- the bounds of a page -------------------------------------------------------------------------------------------


def test_an_error_only_page_and_a_zero_hold_page_go_on_to_the_next(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    # Listed: the succeeded child (camp/p/c1 sorts before the parent's own name in camp/p: it moves only with its
    # root), the parent (its tree refused: a ready child), a loose job.
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1"), "c2": ("ready", "camp/p/c2")})
    loose = single(source)
    paged(monkeypatch, 1)
    seen = pages(monkeypatch)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", "--tree", source_name, target_name) == 1
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert seen == [(0, 0, False), (0, 1, False), (1, 0, True)]
    (error,) = document["errors"]
    assert "tree members must be terminal or paused" in error
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"]
    assert find(target, loose[0]["id"]) is not None and find(source, made["parent"]["id"]) is not None
    assert f"1 selected job(s) have a parent in {source.root} and move only with their root" in captured.err


def test_a_page_closes_at_the_candidates_it_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    jobs(source, 3)
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 1)  # every other bound left as it is
    seen = pages(monkeypatch)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 0
    assert seen == [(1, 0, False), (1, 0, False), (1, 0, True)]
    assert len(json.loads(capsys.readouterr().out)["transfers"]) == 3


def test_a_named_child_moves_alone_although_its_parent_is_here(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A --job job is exempt from the parent rule: held alone (its parent then alone too, its child gone already).
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    paged(monkeypatch, 1)
    argv = ["--json", "-q", *ids(made["c1"]), "--step", "start", "--state", "succeeded", source_name, target_name]
    assert run(tmp_path, *argv) == 0
    document = json.loads(capsys.readouterr().out)
    assert [len(outcome["members"]) for outcome in document["transfers"]] == [1, 1] and document["errors"] == []
    assert all(find(target, mapping["id"]) is not None for mapping in made.values())


def test_a_page_closes_at_its_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    for _ in range(2):  # listed first (aaa/), each with a state.json whose step cannot be read: an error
        damaged = job_mapping(WORKFLOW, {"start": "succeed"}, tag="damaged", placement="aaa/damaged")
        (place(source, damaged, "succeeded").path / "state.json").write_text("{damaged", encoding="utf-8")
    (good, _, _) = single(source)
    monkeypatch.setattr(transfer_cli, "_PAGE_ERRORS", 1)
    seen = pages(monkeypatch)
    assert run(tmp_path, "--json", "-q", "--step", "start", source_name, target_name) == 1
    document = json.loads(capsys.readouterr().out)
    assert seen == [(0, 1, False), (0, 1, False), (1, 0, True)]
    assert len(document["errors"]) == 2 and all("its step is unknown" in error for error in document["errors"])
    assert find(target, good["id"]) is not None


def test_a_page_closes_at_the_candidates_it_scanned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    children = {f"c{index}": ("succeeded", f"aaa/c{index}") for index in range(3)}
    family(source, "zzz/p", children)  # listed last, after its three children and a loose job
    single(source)
    monkeypatch.setattr(transfer_cli, "_PAGE_SCANNED", 2)
    seen = pages(monkeypatch)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", "--tree", source_name, target_name) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert seen == [(0, 0, False), (1, 0, False), (1, 0, True)]
    assert sorted(len(outcome["members"]) for outcome in document["transfers"]) == [1, 4]
    assert "move only with their root" not in captured.err  # they moved with it


def test_the_job_key_limit_overshoots_by_one_sub_batch_at_most(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], request: pytest.FixtureRequest
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    tree(source)  # a tree of four is never split by the limit
    jobs(source, 9)  # with its root, more roots than io.workers
    monkeypatch.setattr(transfer_cli, "_PAGE_MEMBERS", 1)
    seen = pages(monkeypatch)
    argv = ["--json", "-q", "--state", "paused", "--state", "succeeded", "--tree", source_name, target_name]
    assert run(tmp_path, *argv) == 0
    document = json.loads(capsys.readouterr().out)
    assert sorted(len(outcome["members"]) for outcome in document["transfers"]) == [*[1] * 9, 4]
    # One root a page when held one at a time; pooled, a page stops after its first sub-batch of io.workers roots.
    held = [count for count, _, _ in seen]
    assert held == ([1] * 10 if request.getfixturevalue("io_workers") == 1 else [8, 2])


# -- the pairs: per-pair state across pages, stops ------------------------------------------------------------------


@pytest.fixture
def push_ssh(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster (its ``httk`` patched), a local SRC and a DST there."""

    project = tmp_path / "project"
    initialize_project(project, name="paged-push")
    fake_remote(project)
    patched_paged(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


@pytest.fixture
def paged_ssh(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster (its ``httk`` :data:`PATCHED_PAGED`), a SRC there and a local DST."""

    project = tmp_path / "project"
    initialize_project(project, name="paged-pull")
    fake_remote(project)
    patched_paged(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def test_a_push_keeps_one_session_and_one_pin_across_pages(
    push_ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, far_name = push_ssh
    paged(monkeypatch, 1)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"] * 4
    # The workflows asked about once (the jobs' workflow is installed nowhere), then one adoption a page.
    assert same_requests(ops(requests), ["workflows", *["stage", "adopt_staged"] * 4], DEADLINE)
    assert requests_in_order(requests)
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    # Another workspace answering the second page's adoption stops the pair: no new page starts (plan §7.8(b), as
    # amended for T6b in §7.11), and the third, held ahead while the second was driven, is cut before its claim or
    # was held already, and then stays held, reported as not delivered; the fourth is never held.
    requests.clear()
    monkeypatch.setenv("HTTK_TEST_OTHER_ID_FROM_ADOPT", "2")
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = json.loads(capsys.readouterr().out)
    statuses = [outcome["status"] for outcome in document["transfers"]]
    (error,) = document["errors"]
    assert "stopped" in error and f"--resume {near.root} cluster:{far_name}" in error
    if DEADLINE:
        # The third page may be held and staged before the second's adoption answers (given up, held), and the
        # fourth then held ahead.
        assert statuses[0] == "adopted" and statuses[1:] in (["failed"] * 1, ["failed"] * 2, ["failed"] * 3)
        assert len(outgoing(near)) == len(statuses) - 1
        return
    ahead = len(statuses) == 3
    assert statuses == ["adopted", "failed", *["failed"] * ahead]
    assert f"{2 - ahead} selected job(s) were not attempted" in error
    assert len(adopts(requests)) == 2 and len(outgoing(near)) == 1 + ahead
    if ahead:
        message = document["transfers"][2]["message"]
        assert message.startswith(f"not delivered, since the transfer to cluster:{far_name} stopped (")
    else:
        assert find(near, plan[2][0]["id"]).state == "succeeded"  # type: ignore[union-attr]
    assert find(near, plan[3][0]["id"]).state == "succeeded"  # type: ignore[union-attr]  # never held


def test_a_push_s_fsync_after_a_failed_commit_sticks_across_pages(
    push_ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = push_ssh
    far.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first barrier's commit, after its prime
    paged(monkeypatch, 1)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"] * 3
    assert [header.get("sync") for header in adopts(requests)] == [None, "fsync", "fsync", "fsync"]
    assert_in(far, plan)


def test_a_push_whose_session_cannot_open_holds_nothing(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="paged-open")
    fake_remote(project)
    near, near_name = registered(tmp_path, "near")
    _far, far_name = registered(remote.root, "far")
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", "--state", "succeeded", near_name, f"cluster:{far_name}") == 1
    document = json.loads(capsys.readouterr().out)
    assert document["transfers"] == [] and "did not open" in document["errors"][0]
    assert outgoing(near) == [] and len(remote.commands()) == 1
    assert_in(near, plan)


def test_a_local_to_local_stop_holds_nothing_more(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    # Listed first, a child that moves only with its root (a page of its own, holding nothing), then the jobs, then
    # its root.
    family(source, "zzz/p", {"c1": ("succeeded", "aaa/c1")})
    plan = jobs(source, 3)
    paged(monkeypatch, 1)
    real = transfer_cli._other_filesystem

    def failing(*arguments: Any, **options: Any) -> Any:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(transfer_cli, "_other_filesystem", failing)  # the chunk's stage fails (plan §7.10)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 1
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    (outcome,) = document["transfers"]
    assert outcome["status"] == "failed" and "No space left on device" in outcome["message"]
    (error,) = document["errors"]
    assert "stopped" in error and "3 selected job(s) were not attempted" in error
    assert "move only with their root" not in captured.err  # no note after a stop
    assert f"--resume {source.root} {target.root}" in error
    # The first page's hold stays for --resume; no hold exists for the pages after the stop.
    assert outgoing(source) == [outcome["transfer_id"]]
    assert sum(find(source, mapping["id"]) is not None for mapping, _, _ in plan) == 2
    monkeypatch.setattr(transfer_cli, "_other_filesystem", real)
    assert run(tmp_path, "--json", "-q", "--resume", source_name, target_name) == 0
    assert [o["status"] for o in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"]


def test_a_stop_of_one_resume_destination_leaves_the_other_driven(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    first, first_name = registered(tmp_path, "b")
    second, second_name = registered(tmp_path, "c")
    plan = jobs(source, 6)
    for index, (mapping, _, _) in enumerate(plan):
        destination = first_name if index % 2 else second_name
        assert run(tmp_path, "--hold", "--job", str(mapping["id"]), source_name, destination) == 0
    capsys.readouterr()
    paged(monkeypatch, 1)
    real = transfer_cli._other_filesystem

    def failing(holds: Any, dst: Workspace) -> Any:
        if dst.root == first.root:
            raise OSError(28, "No space left on device")  # the chunk's stage fails (plan §7.10)
        return real(holds, dst)

    monkeypatch.setattr(transfer_cli, "_other_filesystem", failing)
    assert run(tmp_path, "--json", "-q", "--resume", source_name) == 1
    document = json.loads(capsys.readouterr().out)
    by_destination: dict[str, list[str]] = {}
    for outcome in document["transfers"]:
        by_destination.setdefault(outcome["destination"], []).append(outcome["status"])
    assert by_destination[str(second.root)] == ["adopted"] * 3
    assert by_destination[str(first.root)] == ["failed"] * 3  # one failed delivery, the others not delivered
    (error,) = document["errors"]
    assert f"the transfer to {first.root} stopped" in error and f"--resume {source.root} {first.root}" in error
    assert len(outgoing(source)) == 3


def test_a_pull_keeps_one_session_and_its_counts_across_pages(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = paged_ssh
    paged(monkeypatch, 1)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"] * 4
    assert calls == []
    assert same_requests(ops(requests), ["select", "holds_page", *["hold_page", "send", "release"] * 4], DEADLINE)
    assert requests_in_order(requests)
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(near, plan)
    assert outgoing(far) == []


def test_a_lost_hold_page_answer_stops_with_resume_and_resume_adopts(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = paged_ssh
    paged(monkeypatch, 1)
    monkeypatch.setenv("HTTK_TEST_LOSE_HOLD_PAGE", "2")  # the second page is held, its answer lost
    plan = jobs(far, 3)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    document = json.loads(capsys.readouterr().out)
    # The first page adopted, or, still staged when the session failed (a positive deadline), given up and held.
    (first,) = document["transfers"]
    assert adopted_or_abandoned(first, DEADLINE) and agrees_with_state(first, far, near)
    (error,) = document["errors"]
    assert "may have held stay held" in error and "nothing was held" not in error
    # The count is the last answered page's: the lost page's job may be held.
    assert (
        f"--resume cluster:{far_name} {near.root}" in error and "at most 2 selected job(s) were not attempted" in error
    )
    left = 1 if first["status"] == "adopted" else 2
    assert len(outgoing(far)) == left  # the lost page's hold (and the given-up stage's)
    monkeypatch.delenv("HTTK_TEST_LOSE_HOLD_PAGE")
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [o["status"] for o in json.loads(capsys.readouterr().out)["transfers"]] == ["adopted"] * left
    assert outgoing(far) == [] and sum(find(near, mapping["id"]) is not None for mapping, _, _ in plan) == 2


def test_a_source_session_failure_stops_every_pair_of_that_source(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, _near_name, far, far_name = paged_ssh
    other, _other_name = registered(tmp_path, "other")
    plan = jobs(far, 4)
    for index, (mapping, _, _) in enumerate(plan):
        destination = str(near.root) if index % 2 else str(other.root)
        assert run(tmp_path, "--hold", "--job", str(mapping["id"]), str(far.root), destination) == 0
    capsys.readouterr()
    # One page of the four holds: the first destination's are pulled, the far side dies in the second's.
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "2")
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 1
    document = json.loads(capsys.readouterr().out)
    # The first destination's adopted (or, staged when SRC died with a positive deadline, given up, held).
    done = sorted(adopted_or_abandoned(outcome, DEADLINE) for outcome in document["transfers"])
    assert done == [False, False, True, True]
    stopped = sorted(error for error in document["errors"] if "stopped" in error)
    assert len(stopped) == 2 and all("--resume" in error for error in stopped)
    assert {root for root in (str(near.root), str(other.root)) if any(root in error for error in stopped)} == {
        str(near.root),
        str(other.root),
    }
    adopted = sum(outcome["status"] == "adopted" for outcome in document["transfers"])
    expected = ["holds_page", "send", "release", "send"] if adopted else ["holds_page", "send", "send"]
    assert same_requests(ops(requests), expected, DEADLINE) and len(outgoing(far)) == 4 - adopted


# -- oversized answers ----------------------------------------------------------------------------------------------


def test_holds_page_closes_a_page_at_its_byte_budget(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 4)
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(size + size // 2))  # one hold fits, two reach it
    listed: list[list[str]] = []
    with opened(bundle) as session:
        after = None
        while True:
            page = session.request("holds_page", {"workspace": far_name, "after": after})
            assert page["ok"] is True, page
            listed.append([hold["transfer_id"] for hold in page["holds"]])
            if (after := page["next"]) is None:
                break
    assert all(0 < len(names) < 4 for names in listed) and len(listed) > 1
    assert [name for names in listed for name in names] == sorted(hold.transfer_id for hold in holds)


def test_an_oversized_hold_is_answered_by_id_and_fails_alone_with_the_manual_remedy(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = paged_ssh
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", "300")  # smaller than any hold's document
    mapping, _, _ = single(far)
    assert run(project, "--json", "-q", *ids(mapping), f"cluster:{far_name}", near_name) == 1
    document = json.loads(capsys.readouterr().out)
    (outcome,) = document["transfers"]
    assert outcome["status"] == "failed" and outcome["members"] == []
    message = outcome["message"]
    assert "too many to describe in one answer of the transfer session" in message
    (transfer_id,) = outgoing(far)
    assert f".httk-workspace/transfers/outgoing/{transfer_id}/ of cluster:{far_name}" in message
    assert "`httk job adopt BUNDLE` in the destination workspace" in message
    assert f"`httk job transfer --release {transfer_id} cluster:{far_name}`" in message
    assert "transfer_session" not in message and find(near, mapping["id"]) is None
    # The remedy, by hand: copy the hold, adopt the copy, release the hold.
    copy = project.parent / "carried" / transfer_id
    shutil.copytree(far.control / "transfers" / "outgoing" / transfer_id, copy, symlinks=True)
    context = CLIContext("httk", project)
    assert command(["job", "adopt", "--workspace", near_name, str(copy)], context) == 0
    assert command(["job", "transfer", "--release", transfer_id, f"cluster:{far_name}"], context) == 0
    capsys.readouterr()
    assert find(near, mapping["id"]) is not None and outgoing(far) == []


def test_an_oversized_hold_without_a_destination_id_keeps_away_copies_by_the_named_remedy(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # A hold that records no destination workspace id (as a remote-to-remote one): a release with --keep alone
    # would keep nothing, so the remedy names --destination-id.
    project, near, near_name, far, far_name = paged_ssh
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", "300")  # smaller than any hold's document
    mapping, _, _ = single(far)
    assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(near.root)) == 0  # no --destination-id
    (transfer_id,) = outgoing(far)
    assert _moving.Hold.from_mapping(transfer_cli.status_document(far)["holds"][0]).destination_workspace_id is None
    capsys.readouterr()
    assert run(project, "--json", "-q", "--keep", "--resume", f"cluster:{far_name}") == 1
    (outcome,) = json.loads(capsys.readouterr().out)["transfers"]
    message = outcome["message"]
    release = f"httk job transfer --release {transfer_id} --keep --destination-id ID cluster:{far_name}"
    assert f"`{release}`, ID being the workspace_id that `httk workspace status --json DST` reports" in message
    # The remedy, by hand, as the message names it.
    copy = tmp_path / "carried" / transfer_id
    shutil.copytree(far.control / "transfers" / "outgoing" / transfer_id, copy, symlinks=True)
    context = CLIContext("httk", project)
    assert command(["job", "adopt", "--workspace", near_name, str(copy)], context) == 0
    argv = release.replace(" ID ", f" {near.workspace_id} ").split()[1:]
    assert command(argv, context) == 0
    capsys.readouterr()
    assert outgoing(far) == [] and find(near, mapping["id"]) is not None
    kept = find(far, mapping["id"])
    assert kept is not None and kept.state == "away"


def test_hold_page_clamps_its_limits_to_the_far_side_s_maxima(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    plan = jobs(far, 3)
    monkeypatch.setenv("HTTK_TEST_FAR_PAGE_HOLDS", "2")  # the far side's maximum
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        first = session.request("hold_page", {"limit_holds": 10**9})
        second = session.request("hold_page", {"limit_holds": 10**9})
    assert [len(first["holds"]), first["done"], len(second["holds"]), second["done"]] == [2, False, 1, True]


def test_hold_page_closes_a_page_at_its_answer_budget(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 1)  # one hold to measure what a hold of the same shape takes in an answer
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    plan = jobs(far, 3)
    budget = size + size // 2  # one hold fits an answer, two do not
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(budget))
    answers: list[dict[str, Any]] = []
    held = []
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        while not (answers and answers[-1]["done"]):
            answers.append(session.request("hold_page", {}))
            held.append(len(outgoing(far)) - 1)
    # The first page closes at the budget after two holds; the second does not fit its answer and goes, alone, in
    # the next one, which holds nothing new; the third page holds the last job.
    assert [len(answer["holds"]) for answer in answers] == [1, 1, 1] and held == [2, 2, 3]
    assert not any(hold.get("oversized") for answer in answers for hold in answer["holds"])
    assert all(len(_frames.json_payload(answer)) <= budget for answer in answers)


def test_hold_page_answers_a_hold_beyond_its_budget_by_id(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    plan = tree(far)
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", "300")
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(plan[0][0]["id"])], "tree": True, "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        page = session.request("hold_page", {})
    assert page["ok"] is True and page["done"] is True
    ((hold,),) = [page["holds"]]
    assert (
        hold["oversized"] is True
        and hold["member_count"] == 4
        and set(hold) == {"transfer_id", "member_count", "oversized"}
    )
    assert outgoing(far) == [hold["transfer_id"]]


# -- workflows: one attempt per id per pair, before the first adoption of the page that needs it ------------------


def test_local_workflows_are_installed_and_built_once_before_the_first_page_s_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    made = [workflow_job(source, caller) for _ in range(3)]
    events: list[str] = []
    real_install, real_build, real_adopt = _store.install_copy, _store.build, transfer_cli._adopt_local_stage

    def install_copy(*arguments: Any, **options: Any) -> Any:
        events.append("install")
        return real_install(*arguments, **options)

    def build(*arguments: Any, **options: Any) -> Any:
        events.append("build")
        return real_build(*arguments, **options)

    def adopt(*arguments: Any, **options: Any) -> Any:
        events.append("adopt")  # a page's stage adopted (plan §7.10)
        return real_adopt(*arguments, **options)

    monkeypatch.setattr(_store, "install_copy", install_copy)
    monkeypatch.setattr(_store, "build", build)
    monkeypatch.setattr(transfer_cli, "_adopt_local_stage", adopt)
    paged(monkeypatch, 1)
    assert (
        run(
            tmp_path, "--json", "-q", *[item for job_id in made for item in ("--job", job_id)], source_name, target_name
        )
        == 0
    )
    transfers = json.loads(capsys.readouterr().out)["transfers"]
    assert events == ["install", "install", "build", "adopt", "adopt", "adopt"]
    assert all(shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"} for outcome in transfers)


def test_a_failed_build_is_attempted_once_for_the_pair_and_reported_on_every_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    caller = workflows(source, tmp_path)
    made = [workflow_job(source, caller) for _ in range(2)]
    builds: list[str] = []

    def failing(*arguments: Any, **options: Any) -> Any:
        builds.append("build")
        raise RuntimeError("the build broke")

    monkeypatch.setattr(_store, "build", failing)
    paged(monkeypatch, 1)
    assert (
        run(
            tmp_path, "--json", "-q", *[item for job_id in made for item in ("--job", job_id)], source_name, target_name
        )
        == 0
    )
    transfers = json.loads(capsys.readouterr().out)["transfers"]
    assert builds == ["build"]
    assert all(shipped(outcome)[GIT_ID] == "unbuilt" for outcome in transfers)


def test_a_push_installs_its_workflows_once_before_the_first_page_s_adoption(
    push_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, far_name = push_ssh
    caller = workflows(near, tmp_path / "packages")
    made = [workflow_job(near, caller) for _ in range(3)]
    paged(monkeypatch, 1)
    requests = recorded_requests(monkeypatch)
    argv = [item for job_id in made for item in ("--job", job_id)]
    assert run(project, "--json", "-q", *argv, near_name, f"cluster:{far_name}") == 0
    transfers = json.loads(capsys.readouterr().out)["transfers"]
    assert same_requests(ops(requests), ["workflows", "install", *["stage", "adopt_staged"] * 3], DEADLINE)
    assert requests_in_order(requests)
    assert all(shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"} for outcome in transfers)
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)


def test_a_pull_installs_and_builds_its_workflows_once_before_the_first_page_s_adoption(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, _near, near_name, far, far_name = paged_ssh
    caller = workflows(far, tmp_path / "packages")
    made = [workflow_job(far, caller) for _ in range(2)]
    events: list[str] = []
    real_build, real_adopt = _store.build, transfer_cli._adopt_local

    def build(*arguments: Any, **options: Any) -> Any:
        events.append("build")
        return real_build(*arguments, **options)

    def adopt(*arguments: Any, **options: Any) -> Any:
        events.append("adopt")
        return real_adopt(*arguments, **options)

    monkeypatch.setattr(_store, "build", build)
    monkeypatch.setattr(transfer_cli, "_adopt_local", adopt)
    paged(monkeypatch, 1)
    requests = recorded_requests(monkeypatch)
    argv = [item for job_id in made for item in ("--job", job_id)]
    assert run(project, "--json", "-q", *argv, f"cluster:{far_name}", near_name) == 0
    transfers = json.loads(capsys.readouterr().out)["transfers"]
    assert events == ["build", "adopt", "adopt"]
    assert [header["kind"] for op, header in requests if op == "send"] == ["store", "holds", "holds"]
    assert all(shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"} for outcome in transfers)
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)


# -- correction round 1 (the Fable and Codex reviews) ---------------------------------------------------------------


def restep(workspace: Workspace, job_id: object, step: str) -> None:
    """Move a job to a new activation at *step* (and so to a new directory name)."""

    ref = find(workspace, job_id)
    assert ref is not None
    with cli_owner(workspace) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = owned.read_state()
        assert doc is not None
        owned.release(doc.next_activation(step, "initial"), Release(ref.state, ref.priority))


def test_a_hold_whose_workflows_cannot_be_resolved_is_reported_and_driven(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # E2: the workflows are resolved after the hold is published; a failure there must not lose the hold.
    from httk.workflow import _moving

    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)

    def failing(*arguments: Any, **options: Any) -> Any:
        raise OSError(5, "the store could not be read")

    monkeypatch.setattr(_moving, "_needed_workflows", failing)
    assert run(tmp_path, "--json", "-q", *ids(mapping), source_name, target_name) == 0
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    assert outcome["status"] == "adopted" and "workflows" not in outcome
    assert "reported no usable workflows for its holds" in captured.err
    assert find(target, mapping["id"]) is not None and outgoing(source) == []


def test_a_pull_selector_naming_no_job_drives_nothing(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # E3: the selection is checked first (step 0): the earlier holds are not driven when a selector names no job.
    project, near, near_name, far, far_name = paged_ssh
    earlier, _, _ = single(far)
    assert run(tmp_path, "--hold", *ids(earlier), str(far.root), str(near.root)) == 0
    capsys.readouterr()
    held = outgoing(far)
    requests = recorded_requests(monkeypatch)
    missing = str(uuid.UUID(int=12345))
    assert run(project, "--json", "-q", "--job", missing, f"cluster:{far_name}", near_name) == 1
    document = json.loads(capsys.readouterr().out)
    (error,) = document["errors"]
    assert document["transfers"] == [] and "was refused" in error and missing in error
    assert ops(requests) == ["select"]
    assert outgoing(far) == held and len(held) == 1 and find(near, earlier["id"]) is None


def test_hold_page_answers_carry_their_diagnostics_within_the_budget(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # E4: the whole answer (its diagnostics, cut to their end, included) is measured against the budget.
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 1)
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    plan = jobs(far, 3)
    budget = (64 << 10) + 4 * size  # the diagnostics' tail and every hold
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(budget))
    monkeypatch.setenv("HTTK_TEST_PAGE_NOISE", str(100_000))  # more diagnostics than an answer keeps
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        while not (answers and answers[-1]["done"]):
            answers.append(session.request("hold_page", {}))
    assert sum(len(answer["holds"]) for answer in answers) == 3
    assert not any(hold.get("oversized") for answer in answers for hold in answer["holds"])
    assert all(len(_frames.json_payload(answer)) <= budget for answer in answers)
    noisy = [answer["stderr"] for answer in answers if answer["stderr"]]  # their end, marked
    assert noisy and all(text.startswith("[") and "characters of diagnostics left out" in text for text in noisy)
    assert all(len(text) <= (64 << 10) + 100 for text in noisy)


def test_the_whole_answer_is_measured_again_after_its_holds_are_chosen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # E4: the frame-fit pass: when the items' sizes misjudge the answer, the whole answer is still held to the budget.
    from httk.workflow.workflow_cli import _transfer_session
    from test_deliver import hold_jobs

    workspace, _ = registered(tmp_path, "far")
    holds, _ = hold_jobs(workspace, 4)
    monkeypatch.setattr(_transfer_session, "_ANSWER_BUDGET", 1200)
    monkeypatch.setattr(_transfer_session, "_doc_size", lambda document: 1)  # every hold looks tiny
    answer: dict[str, Any] = {"errors": [], "done": False, "stderr": ""}
    rest = _transfer_session._fit(answer, [("hold", hold) for hold in holds])
    assert len(_frames.json_payload({"ok": True, **answer})) <= 1200
    assert 0 < len(answer["holds"]) < 4 and len(rest) == 4 - len(answer["holds"])


def test_a_named_job_moved_to_another_step_before_its_claim_is_left_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # E6: a --job ref left for the next page by the job-key limit, re-located at its claim, is checked again.
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    monkeypatch.setattr(transfer_cli, "_PAGE_MEMBERS", 1)
    after_page(monkeypatch, 1, lambda: restep(source, plan[1][0]["id"], "other"))
    argv = ["--json", "-q", *selected(plan), "--step", "start", source_name, target_name]
    assert run(tmp_path, *argv) == 0
    captured = capsys.readouterr()
    document = json.loads(captured.out)
    assert len(document["transfers"]) == 1 and document["errors"] == []
    assert "is at step other, not --step start: left out" in captured.err
    assert find(source, plan[1][0]["id"]) is not None and find(target, plan[1][0]["id"]) is None


def test_a_source_failure_in_one_destination_group_stops_the_others_at_once(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # E7: the source dies in the first destination's group: the second does no work at all.
    project, near, _near_name, far, far_name = paged_ssh
    other, _ = registered(tmp_path, "other")
    plan = jobs(far, 2)
    for (mapping, _, _), destination in zip(plan, (near, other), strict=True):
        assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(destination.root)) == 0
    capsys.readouterr()
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "1")
    driven: list[str] = []
    shipped_calls: list[str] = []
    real_drive, real_ship = transfer_cli._drive, transfer_cli._ship_workflows

    def drive(src: Any, dst: Any, *arguments: Any, **options: Any) -> Any:
        driven.append(dst.locator)
        return real_drive(src, dst, *arguments, **options)

    def ship(*arguments: Any, **options: Any) -> Any:
        shipped_calls.append("ship")
        return real_ship(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_drive", drive)
    monkeypatch.setattr(transfer_cli, "_ship_workflows", ship)
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 1
    document = json.loads(capsys.readouterr().out)
    assert len(driven) == 1 and shipped_calls == ["ship"]
    by_destination = {outcome["destination"]: outcome for outcome in document["transfers"]}
    (later,) = [outcome for locator, outcome in by_destination.items() if locator != driven[0]]
    assert later["status"] == "failed" and "not delivered, since the transfer to" in later["message"]
    assert len(outgoing(far)) == 2


def test_a_resume_from_an_unknown_remote_workspace_fails(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str], capsys: pytest.CaptureFixture[str]
) -> None:
    # E8: a refused listing of the holds, with no destination to carry it, is still reported.
    project, _near, _near_name, _far, _far_name = paged_ssh
    assert run(project, "--json", "-q", "--resume", "cluster:no-such-workspace") == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "no-such-workspace" in error and "--resume cluster:no-such-workspace" in error


def test_a_source_dying_in_its_first_listing_reports_the_holds_kept(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, _near_name, far, far_name = paged_ssh
    for mapping, _, _ in jobs(far, 2):
        assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(near.root)) == 0
    capsys.readouterr()
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "holds_page")
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 1
    document = json.loads(capsys.readouterr().out)
    (error,) = document["errors"]
    assert document["transfers"] == [] and "failed while listing its holds" in error
    assert f"the holds on cluster:{far_name} stay held there" in error and f"--resume cluster:{far_name}" in error
    assert len(outgoing(far)) == 2


def test_a_pull_whose_listing_of_earlier_holds_fails_says_the_selection_was_not_attempted(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = paged_ssh
    mapping, _, _ = single(far)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "holds_page")
    assert run(project, "--json", "-q", *ids(mapping), f"cluster:{far_name}", near_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "the selection was not attempted" in error and f"--resume cluster:{far_name} {near.root}" in error
    assert outgoing(far) == [] and find(far, mapping["id"]) is not None


def test_a_pull_stopped_in_its_earlier_holds_lists_no_more_of_them(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = paged_ssh
    for mapping, _, _ in jobs(far, 3):
        assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(near.root)) == 0
    capsys.readouterr()
    paged(monkeypatch, 1)
    real_scratch = _kernel.Owner.scratch

    def scratch(owner: Any, purpose: str) -> Path:
        if purpose == "landing":
            raise OSError(28, "No space left on device")
        return real_scratch(owner, purpose)

    monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 1
    document = json.loads(capsys.readouterr().out)
    assert ops(requests) == ["holds_page"]  # the first page's landing fails: nothing more is listed
    assert [outcome["status"] for outcome in document["transfers"]] == ["failed"]
    (error,) = document["errors"]
    assert "the holds not yet listed stay held" in error and "--resume" in error
    assert len(outgoing(far)) == 3


def test_a_session_failure_in_a_later_page_s_workflows_keeps_the_report(
    push_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # E9: page 1 needs a workflow installed nowhere, page 2 two others: the far side dies on the second workflows
    # request. Page 2 is not recorded as attempted, its hold is kept and named; page 3, held ahead while page 2 is
    # driven (plan §7.11, T6b), is cut before its claim or was held already, and then stays held, named too; page 4 is
    # never held.
    project, near, near_name, _far, far_name = push_ssh
    first, _, _ = single(near)
    caller = workflows(near, tmp_path / "packages")
    second, third, fourth = (workflow_job(near, caller) for _ in range(3))
    paged(monkeypatch, 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_NTH_OP", "workflows:2")
    selectors = ["--job", second, "--job", third, "--job", fourth]
    argv = ["--json", "-q", *ids(first), *selectors, near_name, f"cluster:{far_name}"]
    assert run(project, *argv) == 1
    document = json.loads(capsys.readouterr().out)
    page_one, page_two, *page_three = document["transfers"]
    assert adopted_or_abandoned(page_one, DEADLINE) and page_one["workflows"][0]["status"] == "missing"
    assert page_two["status"] == "failed" and "--resume" in page_two["message"] and "workflows" not in page_two
    ahead = len(page_three)
    assert ahead in (0, 1) and all(outcome["status"] == "failed" for outcome in page_three)
    assert all(f"stays held on {near.root}" in outcome["message"] for outcome in page_three)
    (error,) = document["errors"]
    assert "stopped" in error and f"{2 - ahead} selected job(s) were not attempted" in error
    assert len(outgoing(near)) == (1 if page_one["status"] == "adopted" else 2) + ahead
    assert (find(near, third) is not None) == (not ahead)
    assert find(near, fourth) is not None  # never held


def test_an_error_between_pages_keeps_the_report_of_the_pages_driven(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # E10: the owner of page 2 cannot be registered (ENOSPC): page 1 is reported, the error named, exit 1.
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    real_owner, real_next_page = transfer_cli._owner, transfer_cli._Selection.next_page
    armed: list[int] = []
    calls: list[int] = []

    def owner(workspace: Any, label: str) -> Any:
        if armed:
            raise OSError(28, "No space left on device")
        return real_owner(workspace, label)

    def next_page(selection: Any, **options: Any) -> Any:
        calls.append(1)
        if len(calls) == 2:
            armed.append(1)
        try:
            return real_next_page(selection, **options)
        finally:
            armed.clear()

    monkeypatch.setattr(transfer_cli, "_owner", owner)
    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    document = json.loads(capsys.readouterr().out)
    (first,) = document["transfers"]
    assert adopted_or_abandoned(first, DEADLINE)
    (error,) = document["errors"]
    assert "No space left on device" in error and "1 selected job(s) were not attempted" in error
    assert find(source, plan[1][0]["id"]) is not None
    assert outgoing(source) == ([] if first["status"] == "adopted" else [first["transfer_id"]])


def test_errors_of_holding_count_against_a_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # E11: two named parents refused without --tree (their children are here), then a loose job; one error a page.
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    refused = [family(source, f"camp/p{index}", {"c1": ("succeeded", f"camp/p{index}/c1")}) for index in range(2)]
    loose, _, _ = single(source)
    monkeypatch.setattr(transfer_cli, "_PAGE_ERRORS", 1)
    seen = pages(monkeypatch)
    argv = ["--json", "-q", *ids(refused[0]["parent"], refused[1]["parent"], loose), source_name, target_name]
    assert run(tmp_path, *argv) == 1
    document = json.loads(capsys.readouterr().out)
    assert seen == [(0, 1, False), (0, 1, False), (1, 0, True)]
    assert len(document["errors"]) == 2 and find(target, loose["id"]) is not None


def test_a_later_page_s_smaller_hold_limit_bounds_the_attempts_left_for_it(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # E11: attempts a page left (here by the job-key limit) count against the next page's limits.
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    plan = jobs(far, 3)
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        counts = [len(session.request("hold_page", {"limit_members": 1})["holds"])]
        counts += [len(session.request("hold_page", {"limit_holds": 1})["holds"]) for _ in range(2)]
    assert counts == [1, 1, 1]


# -- tests that kill the mutations the review found surviving --------------------------------------------------------


def test_a_named_job_gone_before_its_step_is_read_is_re_located(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    after_page(monkeypatch, 1, lambda: reprioritize(source, plan[1][0]["id"], 777))
    assert run(tmp_path, "--json", "-q", *selected(plan), "--step", "start", source_name, target_name) == 0
    document = json.loads(capsys.readouterr().out)
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"] * 2 and document["errors"] == []


def test_a_named_job_is_re_located_once_at_most(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Moved before its page (re-located when its step is read), then again before its claim: an error.
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    moving_id = str(plan[1][0]["id"])
    after_page(monkeypatch, 1, lambda: reprioritize(source, moving_id, 777))
    real = transfer_cli._parent_here
    reads: list[int] = []

    def read_then_moved(ref: Any, present: Any, *, step: bool) -> Any:
        found = real(ref, present, step=step)
        if ref.job_id == moving_id and not found.unreadable:
            reads.append(1)
            if len(reads) == 1:
                reprioritize(source, moving_id, 778)
        return found

    monkeypatch.setattr(transfer_cli, "_parent_here", read_then_moved)
    assert run(tmp_path, "--json", "-q", *selected(plan), "--step", "start", source_name, target_name) == 1
    document = json.loads(capsys.readouterr().out)
    moved = find(source, moving_id)
    assert moved is not None and document["errors"] == [f"{moved.job_key} moved before it could be claimed"]
    assert len(document["transfers"]) == 1


def test_a_named_job_claimed_by_another_owner_is_not_re_located_there(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    other = cli_owner(source)
    taken: list[Any] = []

    def claim_second() -> None:
        ref = find(source, plan[1][0]["id"])
        assert ref is not None
        taken.append(_kernel.claim(source, other, ref))

    after_page(monkeypatch, 1, claim_second)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    document = json.loads(capsys.readouterr().out)
    assert document["errors"] == [f"{taken[0].job_key} moved before it could be claimed"]
    taken[0].give_back()
    other.close()


def test_holds_page_closes_at_its_scan_bound_and_its_job_keys(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 2)  # bound elsewhere
    with cli_owner(far) as owner:
        for _ in range(2):
            ref = find(far, single(far)[0]["id"])
            assert ref is not None
            claimed = _kernel.claim(far, owner, ref)
            assert claimed is not None
            from httk.workflow import _moving

            _moving.hold(far, owner, claimed, tree=True, destination_locator="x:y")

    def listing(session: Any, **header: Any) -> list[list[str]]:
        pages_: list[list[str]] = []
        after = None
        while True:
            page = session.request("holds_page", {"workspace": far_name, "after": after, **header})
            assert page["ok"] is True, page
            pages_.append([hold["transfer_id"] for hold in page["holds"]])
            if (after := page["next"]) is None:
                return pages_

    with opened(bundle) as session:
        by_keys = listing(session, limit_members=1)
    monkeypatch.setenv("HTTK_TEST_FAR_PAGE_SCANNED", "1")
    with opened(bundle) as session:
        scanned = listing(session, destination="x:y")
    assert [len(names) for names in by_keys] == [1, 1, 1, 1]
    assert len(scanned) == 4 and sum(len(names) for names in scanned) == 2


def test_no_pull_note_after_a_stop(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, _near, near_name, far, far_name = paged_ssh
    family(far, "camp/p", {"c1": ("succeeded", "aaa/c1")})  # without --tree the child moves only with its root
    real_scratch = _kernel.Owner.scratch

    def scratch(owner: Any, purpose: str) -> Path:
        if purpose == "landing":
            raise OSError(28, "No space left on device")
        return real_scratch(owner, purpose)

    monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
    jobs(far, 1)
    assert run(project, "--json", "-q", "--state", "succeeded", f"cluster:{far_name}", near_name) == 1
    captured = capsys.readouterr()
    assert "stopped" in " ".join(json.loads(captured.out)["errors"])
    assert "move only with their root" not in captured.err


def test_a_listed_child_of_a_running_parent_is_deferred(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # The present set holds owned/ (and away/) jobs: a child whose parent runs moves only with it.
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    made = family(source, "camp/p", {"c1": ("succeeded", "aaa/c1")})
    with cli_owner(source) as other:
        parent = _kernel.claim(source, other, find(source, made["parent"]["id"]))  # type: ignore[arg-type]
        assert parent is not None
        assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 0
        parent.give_back()
    captured = capsys.readouterr()
    assert json.loads(captured.out)["transfers"] == []
    assert "1 selected job(s) have a parent" in captured.err


def test_the_count_not_attempted_leaves_out_what_moved_with_an_earlier_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    family(source, "aaa/p", {"c1": ("succeeded", "camp/c1")})  # the child is listed after its root
    single(source)
    paged(monkeypatch, 1)

    def failing(*arguments: Any, **options: Any) -> Any:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(transfer_cli, "_other_filesystem", failing)  # a local chunk fails to stage (plan §7.10)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", "--tree", source_name, target_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "1 selected job(s) were not attempted" in error  # the loose job; the child went with its root


def test_a_page_s_budget_counts_the_errors_found_deciding_it(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    damaged = []
    for _ in range(2):
        mapping, _, _ = single(far)
        ref = find(far, mapping["id"])
        assert ref is not None
        (ref.path / "state.json").write_text("{damaged", encoding="utf-8")
        damaged.append(mapping)
    good = jobs(far, 2)
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", "400")  # less than the two errors take
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        names = [str(mapping["id"]) for mapping in damaged] + [str(mapping["id"]) for mapping, _, _ in good]
        header = {"workspace": far_name, "jobs": names, "step": ["start"], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        answers.append(session.request("hold_page", {}))
        held_after_first = len(outgoing(far))
        while not answers[-1]["done"] and len(answers) < 10:
            answers.append(session.request("hold_page", {}))
    # The page's two errors (one carried to the next answer) closed it before any hold.
    assert answers[0]["holds"] == [] and held_after_first == 0
    assert sum(len(answer["errors"]) for answer in answers) == 2 and answers[-1]["done"] is True


def test_a_push_session_failure_stops_only_its_destination_on_resume(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="paged-resume")
    fake_remote(project)
    patched_paged(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    first, first_name = registered(remote.root, "first")
    second, second_name = registered(remote.root, "second")
    plan = jobs(near, 2)
    held: dict[str, Workspace] = {}
    for (mapping, _, _), (workspace, name) in zip(plan, ((first, first_name), (second, second_name)), strict=True):
        before = set(outgoing(near))
        assert run(project, "--hold", *ids(mapping), near_name, f"cluster:{name}") == 0
        (transfer_id,) = set(outgoing(near)) - before
        held[transfer_id] = workspace
    capsys.readouterr()
    # Pages of one hold, in transfer id order: the earlier hold's destination fails, the later one's is driven.
    failing, other = (held[transfer_id] for transfer_id in sorted(held))
    failing_name = first_name if failing is first else second_name
    other_name = second_name if failing is first else first_name
    paged(monkeypatch, 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ADOPT_WORKSPACE", failing_name)
    assert run(project, "--json", "-q", "--resume", near_name) == 1
    document = json.loads(capsys.readouterr().out)
    statuses = {outcome["destination"]: outcome["status"] for outcome in document["transfers"]}
    assert statuses == {f"cluster:{failing_name}": "failed", f"cluster:{other_name}": "adopted"}
    (error,) = document["errors"]
    assert f"the transfer to cluster:{failing_name} stopped" in error
    assert sum(find(other, mapping["id"]) is not None for mapping, _, _ in plan) == 1


def test_a_hold_page_refused_before_its_owner_leaves_the_selection_where_it_was(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    plan = jobs(far, 2)
    monkeypatch.setenv("HTTK_TEST_DECIDE_FAILS_ONCE", "1")
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        refused = session.request("hold_page", {})
        assert refused["ok"] is False and "fatal" not in refused and session.is_open
        assert outgoing(far) == []
        page = session.request("hold_page", {})
    assert len(page["holds"]) == 2 and page["done"] is True


def test_holds_page_measures_its_whole_answer_with_its_diagnostics(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 4)
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    budget = (64 << 10) + 2 * size + size // 2  # the diagnostics' tail and two holds
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(budget))
    monkeypatch.setenv("HTTK_TEST_LISTING_NOISE", str(100_000))
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        after = None
        while True:
            answers.append(session.request("holds_page", {"workspace": far_name, "after": after}))
            assert answers[-1]["ok"] is True, answers[-1]
            if (after := answers[-1]["next"]) is None:
                break
    assert all(len(_frames.json_payload(answer)) <= budget for answer in answers)
    listed = [hold["transfer_id"] for answer in answers for hold in answer["holds"]]
    assert listed == sorted(hold.transfer_id for hold in holds)


# -- correction round 2 ----------------------------------------------------------------------------------------------


def test_a_first_hold_in_the_gap_of_the_cursor_is_answered_by_id_and_nothing_is_skipped(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # G1: a hold's document fits an answer with an empty cursor but not with the real one: it goes by id.
    project, near, _near_name, far, far_name = paged_ssh
    plan = jobs(far, 3)
    for mapping, _, _ in plan:
        assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(near.root)) == 0
    capsys.readouterr()
    size = max(len(_frames.json_payload(hold)) for hold in transfer_cli.status_document(far)["holds"])
    empty = len(_frames.json_payload({"ok": True, "next": "", "stderr": "", "holds": []}))
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(empty + size + 10))  # the cursor takes 32 + 2 more
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 1
    transfers = json.loads(capsys.readouterr().out)["transfers"]
    assert sorted(outcome["transfer_id"] for outcome in transfers) == sorted(outgoing(far))
    assert len(transfers) == 3 and all(
        "too many to describe in one answer" in outcome["message"] for outcome in transfers
    )
    assert ops(requests) == ["holds_page"] * 3


def test_a_carried_hold_beside_a_note_is_answered_and_the_note_follows(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # G1: the note gives way to the hold carried from the answer before; the note comes with the last answer.
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 1)  # one hold to measure what a hold of the same shape takes in an answer
    size = max(len(_frames.json_payload(hold)) for hold in transfer_cli.status_document(far)["holds"])
    parent = job_mapping(WORKFLOW, {"start": "succeed"}, tag="parent", placement="camp/p")
    child = job_mapping(WORKFLOW, {"start": "succeed"}, tag="c1", placement="aaa/c1")
    child["parent"] = parent_link(parent, far.workspace_id)
    place(far, child, "succeeded", 300)
    place(far, parent, "paused", 300, (child,))  # not selected: its child is listed and moves only with it
    jobs(far, 2)
    note = f"1 selected job(s) have a parent in {far.root} and move only with their root (use --tree, or detach them)"
    envelope = {"ok": True, "changed": 0, "remaining": 0, "consumed": 99, "carried": 9, "done": False}
    without = len(_frames.json_payload({**envelope, "note": None, "stderr": "", "holds": [], "errors": []}))
    budget = without + size + 20  # a hold fits, a hold and the note do not
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(budget))
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        header = {"workspace": far_name, "state": ["succeeded"], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        while not (answers and answers[-1]["done"]) and len(answers) < 6:
            answers.append(session.request("hold_page", {}))
    assert [len(answer["holds"]) for answer in answers] == [1, 1, 0] and answers[-1]["done"] is True
    assert [answer["note"] for answer in answers] == [None, None, note]
    assert not any(hold.get("oversized") for answer in answers for hold in answer["holds"])
    assert all(len(_frames.json_payload(answer)) <= budget for answer in answers)
    assert [answer["carried"] for answer in answers] == [1, 0, 0]
    consumed = [answer["consumed"] for answer in answers]
    assert consumed == sorted(consumed) and consumed[0] < consumed[1]


def test_long_errors_are_cut_and_carried_within_the_budget(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # G2: 256 errors of 70,000 characters: each cut to 4 KiB, carried over answers, every answer within the budget.
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    jobs(far, 256)
    budget = 200_000
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(budget))
    monkeypatch.setenv("HTTK_TEST_LONG_ERRORS", "70000")
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        assert session.request("select", {"workspace": far_name, "state": ["succeeded"], "destination": "x:y"})["ok"]
        while not (answers and answers[-1]["done"]) and len(answers) < 40:
            answers.append(session.request("hold_page", {}))
    errors = [error for answer in answers for error in answer["errors"]]
    assert len(errors) == 256 and len(answers) > 2 and answers[-1]["done"] is True
    assert all(len(error) <= (4 << 10) + 40 and error.endswith("characters left out]") for error in errors)
    assert all(len(_frames.json_payload(answer)) <= budget for answer in answers)
    assert outgoing(far) == []


def test_a_far_side_answering_pages_without_progress_is_stopped(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # G3: an answer that is not done, carries nothing and consumed nothing more stops the pair, never a loop.
    project, near, near_name, far, far_name = paged_ssh
    mapping, _, _ = single(far)
    monkeypatch.setenv("HTTK_TEST_EMPTY_PAGES", "1")
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *ids(mapping), f"cluster:{far_name}", near_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "made no progress" in error and f"--resume cluster:{far_name} {near.root}" in error
    assert ops(requests) == ["select", "holds_page", "hold_page"]


def test_pooled_holding_takes_no_more_attempts_than_errors_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # G4: four listed parents, each refused without --tree (its child is here); two errors a page, pooled or not.
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    for index in range(4):
        family(source, f"camp/p{index}", {"c1": ("succeeded", f"aaa/c{index}")})
    monkeypatch.setattr(transfer_cli, "_PAGE_ERRORS", 2)
    seen = pages(monkeypatch)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 1
    assert len(json.loads(capsys.readouterr().out)["errors"]) == 4
    assert [errors for _, errors, _ in seen if errors] == [2, 2]


def test_a_lost_first_hold_page_answer_says_the_selection_may_be_partly_held(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # G5: the first page held, its answer lost: how much was attempted is not known.
    project, _near, near_name, far, far_name = paged_ssh
    plan = jobs(far, 2)
    paged(monkeypatch, 1)
    monkeypatch.setenv("HTTK_TEST_LOSE_HOLD_PAGE", "1")
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "the selection may have been partly held; how much was attempted is not known" in error
    assert "not attempted" not in error and len(outgoing(far)) == 1


def test_a_selection_whose_listing_was_never_taken_was_not_attempted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # G5: a stop in step 1, before the listing: no count.
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    earlier, _, _ = single(source)
    assert run(tmp_path, "--hold", *ids(earlier), source_name, str(target.root)) == 0
    jobs(source, 2)
    capsys.readouterr()

    def failing(*arguments: Any, **options: Any) -> Any:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(transfer_cli, "_other_filesystem", failing)  # a local chunk fails to stage (plan §7.10)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "the selection was not attempted" in error and "selected job(s)" not in error


def test_a_stop_names_the_held_jobs_not_yet_reported(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # G5: a page made two holds, its answer carried one; the pair stops: the other is named as held, not reported.
    project, _near, near_name, far, far_name = paged_ssh
    from test_deliver import hold_jobs

    measured, _ = hold_jobs(far, 1)
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    transfer_cli.release_holds(far, [measured[0].transfer_id], keep=False, destination_id=None)
    plan = jobs(far, 2)
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(size + size // 2))
    real_scratch = _kernel.Owner.scratch

    def scratch(owner: Any, purpose: str) -> Path:
        if purpose == "landing":
            raise OSError(28, "No space left on device")
        return real_scratch(owner, purpose)

    monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert f"1 held job(s) not yet reported stay held on cluster:{far_name}" in error
    assert len(outgoing(far)) == 2


def test_a_second_select_while_holds_are_carried_is_refused(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # G5: holds made but not reported are never dropped by a new selection.
    from test_deliver import hold_jobs

    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 1)
    size = max(len(_frames.json_payload({"item": hold})) for hold in transfer_cli.status_document(far)["holds"])
    plan = jobs(far, 2)
    monkeypatch.setenv("HTTK_TEST_ANSWER_BUDGET", str(size + size // 2))
    with opened(bundle) as session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        assert session.request("select", header)["ok"] is True
        first = session.request("hold_page", {})
        again = session.request("select", header)
        second = session.request("hold_page", {})
    assert len(first["holds"]) == 1 and first["carried"] == 1
    assert again["ok"] is False and "not reported yet" in again["error"]
    assert len(second["holds"]) == 1 and second["done"] is True


def test_the_diagnostics_give_way_to_a_first_hold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import _transfer_session
    from test_deliver import hold_jobs

    workspace, _ = registered(tmp_path, "far")
    holds, _ = hold_jobs(workspace, 1)
    size = len(_frames.json_payload(holds[0].as_mapping()))
    budget = size + 1000  # the hold and its envelope, not the diagnostics beside them
    monkeypatch.setattr(_transfer_session, "_ANSWER_BUDGET", budget)
    answer: dict[str, Any] = {"next": "0" * 32, "stderr": "d" * 5000}
    rest = _transfer_session._fit(answer, [("hold", holds[0])], optional=("stderr",))
    assert rest == [] and answer["holds"][0].get("oversized") is None
    assert answer["stderr"].startswith("[diagnostics left out") and len(_frames.json_payload(answer)) <= budget


def test_an_error_escaping_a_page_s_drive_reports_its_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The drive of page 2 raises: page 1 is reported, page 2's hold too (held unless adopted), exit 1.
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    paged(monkeypatch, 1)
    real = transfer_cli._drive
    calls: list[int] = []

    def drive(*arguments: Any, **options: Any) -> Any:
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("the drive broke")
        return real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_drive", drive)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    document = json.loads(capsys.readouterr().out)
    first, second = document["transfers"]
    assert adopted_or_abandoned(first, DEADLINE) and second["status"] == "failed"
    assert agrees_with_state(first, source, target) and agrees_with_state(second, source, target)
    assert "the drive broke" in second["message"] and "unless it was adopted" in second["message"]
    (error,) = document["errors"]
    assert "stopped: the drive broke" in error


def test_an_unreadable_earlier_hold_stops_a_local_transfer_with_its_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)
    assert run(tmp_path, "--hold", *ids(mapping), source_name, str(target.root)) == 0
    capsys.readouterr()
    (transfer_id,) = outgoing(source)
    (source.control / "transfers" / "outgoing" / transfer_id / "bundle.json").write_text("{damaged", encoding="utf-8")
    assert run(tmp_path, "--json", "-q", "--resume", source_name, target_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert f"reading the holds on {source.root} failed" in error and "the holds not yet listed stay held" in error


def test_an_error_while_a_pull_lists_its_earlier_holds_stops_with_its_report(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, _near_name, far, far_name = paged_ssh
    mapping, _, _ = single(far)
    assert run(tmp_path, "--hold", *ids(mapping), str(far.root), str(near.root)) == 0
    capsys.readouterr()

    def broken(*arguments: Any, **options: Any) -> Any:
        raise OSError(5, "the listing broke")

    monkeypatch.setattr(transfer_cli, "_oversized", broken)
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert f"listing the holds on cluster:{far_name} failed" in error and "the listing broke" in error


def test_a_local_stop_in_the_earlier_holds_names_those_not_yet_listed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    for mapping, _, _ in jobs(source, 3):
        assert run(tmp_path, "--hold", *ids(mapping), source_name, str(target.root)) == 0
    capsys.readouterr()
    paged(monkeypatch, 1)

    def failing(*arguments: Any, **options: Any) -> Any:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(transfer_cli, "_other_filesystem", failing)  # a local chunk fails to stage (plan §7.10)
    assert run(tmp_path, "--json", "-q", "--resume", source_name, target_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "the holds not yet listed stay held" in error and len(outgoing(source)) == 3


# -- correction round 3 ----------------------------------------------------------------------------------------------


def test_a_far_side_repeating_an_error_without_progress_is_stopped(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # H1: every answer that is not done must consume more, items or not.
    project, near, near_name, far, far_name = paged_ssh
    mapping, _, _ = single(far)
    monkeypatch.setenv("HTTK_TEST_ERROR_PAGES", "1")
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *ids(mapping), f"cluster:{far_name}", near_name) == 1
    errors = json.loads(capsys.readouterr().out)["errors"]
    assert errors.count("a repeated error") == 1
    (stopped,) = [error for error in errors if error != "a repeated error"]
    assert "made no progress" in stopped and f"--resume cluster:{far_name} {near.root}" in stopped
    assert ops(requests) == ["select", "holds_page", "hold_page", "hold_page"]


def test_retired_attempts_left_for_later_pages_advance_consumed(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # H2: a root and its three children named; the job-key limit leaves the children pending after the root's tree
    # took them; pages of one attempt then retire them one by one, each answer consuming more.
    _, bundle = local_project(tmp_path)
    patched_paged(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    plan = tree(far)
    answers: list[dict[str, Any]] = []
    with opened(bundle) as session:
        names = [str(mapping["id"]) for mapping, _, _ in plan]
        assert session.request("select", {"workspace": far_name, "jobs": names, "tree": True, "destination": "x:y"})[
            "ok"
        ]
        answers.append(session.request("hold_page", {"limit_members": 1}))
        while not answers[-1]["done"] and len(answers) < 10:
            answers.append(session.request("hold_page", {"limit_holds": 1}))
    assert [len(answer["holds"]) for answer in answers] == [1, 0, 0, 0] and answers[-1]["done"] is True
    consumed = [answer["consumed"] for answer in answers]
    # Every answer that is not done consumed more than the one before.
    assert consumed[:-1] == sorted(set(consumed[:-1])) and len(outgoing(far)) == 1, consumed


def test_errors_are_cut_by_their_encoded_size(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # H3: backslashes and emoji take more bytes encoded than characters: the cut and the budget count the bytes.
    from httk.workflow.workflow_cli import _transfer_session

    assert len(json.dumps(_transfer_session._cut("\U0001f600" * 70_000))) <= 4 << 10
    assert len(json.dumps(_transfer_session._cut("\\" * 70_000))) <= 4 << 10
    monkeypatch.setattr(_transfer_session, "_ANSWER_BUDGET", 1000)
    items: list[tuple[str, Any]] = [
        ("error", text) for text in ("\\" * 5000, "\U0001f600" * 5000, "a" * 100, "\\" * 300)
    ]
    answers = []
    while items:
        answer: dict[str, Any] = {"errors": [], "done": False, "note": None, "stderr": "diagnostics"}
        left = _transfer_session._fit(answer, items)
        assert len(left) < len(items)
        answers.append(answer)
        items = left
    assert all(_transfer_session._encoded(answer) <= 1000 for answer in answers)
    assert sum(len(answer["errors"]) for answer in answers) == 4
    assert answers[0]["errors"][0].endswith("characters left out]")


def test_a_first_page_refused_by_the_source_leaves_the_selection_not_attempted(
    paged_ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # H4: SRC refuses the first page (its listing fails before anything is held): no lost-answer wording.
    project, _near, near_name, far, far_name = paged_ssh
    mapping, _, _ = single(far)
    monkeypatch.setenv("HTTK_TEST_SNAPSHOT_FAILS", "1")
    assert run(project, "--json", "-q", "--state", "succeeded", f"cluster:{far_name}", near_name) == 1
    (error,) = json.loads(capsys.readouterr().out)["errors"]
    assert "the selection was not attempted" in error and "the listing could not be read" in error
    assert "may have" not in error and "partly held" not in error
    assert outgoing(far) == [] and find(far, mapping["id"]) is not None
