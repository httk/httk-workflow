"""Remote → remote over transfer sessions (plan WORKFLOW_TRANSFER_SESSION §7.9, T4).

Both ends are workspaces of the stand-in ssh cluster, reached through two remotes, ``cluster`` for SRC and
``other`` for DST, each under its own host name so that the counts are per end; the cluster's ``httk`` is
:data:`test_session_pull.PATCHED_PULL`, whose faults the environment selects (a fault applies to whichever end runs
the op it patches: ``send`` and ``release`` run on SRC, ``stage``, ``adopt_staged``, ``install`` and ``workflows`` on
DST). Each end takes a transfer session, which a transfer requires (plan §7.12): an end whose ``httk`` cannot serve
one stops the transfer before anything is selected or held on either end. Every copy goes through a local temporary
relay, which the tests keep in a directory of their own.
"""

import errno
import io
import json
import os
import shlex
import sys
import tarfile
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _fs, _landing, _moving, _store
from httk.workflow.adapters import TRANSFER_PROTOCOL, resolve_remote
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from test_job_transfer import (
    adapter_calls,
    adopted_or_abandoned,
    at_once,
    away_of,
    jobs,
    outcomes,
    outgoing,
    recorded_requests,
    registered,
    requests_in_order,
    run,
    same_requests,
    selected,
    starts,
)
from test_moving import assert_in, claim_root, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_session_pull import PATCHED_PULL
from test_transfer_workflows import GIT_ID, shipped, workflows
from test_transfer_workflows import job as workflow_job
from v3_helpers import cli_owner

#: The stand-in cluster's ``httk`` for both ends.
PATCHED_R2R = PATCHED_PULL

SRC_HOST = "src.example.test"
DST_HOST = "dst.example.test"
#: The ops of each end's session.
SRC_OPS = {"select", "holds_page", "hold_page", "send", "release"}
DST_OPS = {"workflows", "install", "stage", "adopt_staged"}


@dataclass
class Ends:
    project: Path
    near: Workspace
    src: str
    far: Workspace
    dst: str
    relays: Path
    remote: Remote


#: The tests skipped with a positive visibility deadline, and why (by design; see each).
AT_ONCE_ONLY: dict[str, str] = {}
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


@pytest.fixture
def ends(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> Ends:
    """A project whose ``cluster`` (SRC's host) and ``other`` (DST's host) reach the stand-in ssh cluster, a SRC and a
    DST workspace there, and this process's temporary directory (the relays') kept apart."""

    project = tmp_path / "project"
    initialize_project(project, name="remote-to-remote")
    fake_remote(project, host=SRC_HOST)
    fake_remote(project, name="other", host=DST_HOST)
    wrapper = tmp_path / "patched_r2r.py"
    wrapper.write_text(PATCHED_R2R, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    near, near_name = registered(remote.root, "near")
    far, far_name = registered(remote.root, "far")
    relays = tmp_path / "relays"
    relays.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(relays))
    return Ends(project, near, f"cluster:{near_name}", far, f"other:{far_name}", relays, remote)


def log(remote: Remote) -> list[dict[str, str]]:
    return (
        [json.loads(line) for line in remote.log.read_text(encoding="utf-8").splitlines()]
        if remote.log.exists()
        else []
    )


def counts(remote: Remote, before: int) -> dict[str, tuple[int, int]]:
    """The connections and remote ``httk`` starts per end (by host) since *before* log lines."""

    lines = log(remote)[before:]
    per = {}
    for end, host in (("src", SRC_HOST), ("dst", DST_HOST)):
        commands = [line["command"] for line in lines if host in line["destination"]]
        per[end] = (len(commands), starts(commands))
    assert sum(count for count, _ in per.values()) == len(lines)
    return per


def ops(requests: list[tuple[str, dict[str, Any]]], which: set[str] | None = None) -> list[str]:
    return [op for op, _ in requests if which is None or op in which]


def headers(requests: list[tuple[str, dict[str, Any]]], op: str) -> list[dict[str, Any]]:
    return [header for name, header in requests if name == op]


def statuses(capsys: pytest.CaptureFixture[str]) -> tuple[list[dict[str, Any]], list[str]]:
    document = outcomes(capsys)
    return document["transfers"], document["errors"]


def held(workspace: Workspace, locator: str, count: int) -> list[tuple[_moving.Hold, Any]]:
    """Hold *count* single jobs of *workspace* bound for *locator*, as an earlier run would have left them."""

    plan = jobs(workspace, count)
    with cli_owner(workspace) as owner:
        return [
            (
                _moving.hold(
                    workspace,
                    owner,
                    claim_root(workspace, owner, mapping["id"]),
                    tree=True,
                    destination_locator=locator,
                ),
                (mapping, state, priority),
            )
            for mapping, state, priority in plan
        ]


def relays_in(directory: Path) -> list[str]:
    return sorted(name for name in os.listdir(directory) if name.startswith("httk-transfer-"))


# -- counts ---------------------------------------------------------------------------------------------------------


def test_one_session_an_end_moves_the_jobs_at_its_counts(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Pages of two holds (asked of SRC's session) and segments of two bundles: several of each.
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 2)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 5)
    before = len(log(ends.remote))
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 5 and errors == []
    assert_in(ends.far, plan)
    assert outgoing(ends.near) == [] and relays_in(ends.relays) == []
    measured = counts(ends.remote, before)
    adopts = len(headers(requests, "stage"))
    # One session a end, whatever the pages (three) and segments: every request on it.
    assert measured == {"src": (1, 1), "dst": (1, 1)} and calls == []
    assert ops(requests, SRC_OPS)[:3] == ["select", "holds_page", "hold_page"]
    assert ops(requests, SRC_OPS).count("hold_page") >= 3 and adopts == 3
    assert ops(requests, SRC_OPS).count("send") == 3 and ops(requests, SRC_OPS).count("release") == 3


# -- the relay ------------------------------------------------------------------------------------------------------


def test_each_relay_is_removed_after_its_segment(ends: Ends, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    seen: list[list[str]] = []
    real = transfer_cli._push_segment

    def push_segment(*arguments: Any, **options: Any) -> None:
        seen.append(relays_in(ends.relays))
        real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_push_segment", push_segment)
    before: list[list[str]] = []
    make = transfer_cli._make_relay

    def make_relay() -> Any:
        before.append(relays_in(ends.relays))  # the earlier segment's relay is gone already
        return make()

    monkeypatch.setattr(transfer_cli, "_make_relay", make_relay)
    plan = jobs(ends.near, 5)
    assert run(ends.project, "-q", *selected(plan), ends.src, ends.dst) == 0
    # One relay at a time, each a new one, removed before the next is made, none left.
    assert [len(names) for names in seen] == [1, 1, 1] and len({names[0] for names in seen}) == 3
    assert before == [[], [], []]
    assert relays_in(ends.relays) == []
    assert_in(ends.far, plan)


def test_the_terminal_is_validated_before_the_relay_is_used(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # SRC aborts the first send's download after its whole archive: nothing of that relay is delivered, SRC (out of
    # step) is asked nothing more, and every hold stays on SRC.
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_ABORT_DOWNLOAD", "2")  # the first send's (the echo's is the first)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert any("aborted the download" in outcome["message"] for outcome in transfers)
    assert any("--resume" in error for error in errors)
    assert "stage" not in ops(requests) and ops(requests, SRC_OPS)[-1] == "send"
    assert len(outgoing(ends.near)) == 4 and all(find(ends.far, mapping["id"]) is None for mapping, _, _ in plan)
    monkeypatch.delenv("HTTK_TEST_ABORT_DOWNLOAD")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 4
    assert_in(ends.far, plan)


def test_a_full_relay_stops_the_pair_with_its_holds_kept_on_src(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    real = _fs.extract_tar
    extractions: list[int] = []

    def full(*arguments: Any, **options: Any) -> int:
        extractions.append(1)
        if len(extractions) == 1:
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        return real(*arguments, **options)

    monkeypatch.setattr(_fs, "extract_tar", full)
    requests = recorded_requests(monkeypatch)
    calls = adapter_calls(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert any("this machine's relay" in outcome["message"] for outcome in transfers)
    assert any("stopped" in error and "--resume" in error for error in errors)
    # Nothing reached DST; the one fetch was the last request; nothing was released.
    assert "stage" not in ops(requests) and calls == [] and ops(requests, SRC_OPS)[-1] == "send"
    assert len(outgoing(ends.near)) == 4 and relays_in(ends.relays) == []
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 4
    assert_in(ends.far, plan)


def test_a_member_the_relay_refuses_fails_its_hold_and_the_rest_is_fetched_again(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    # A FIFO among the second job's files: SRC sends it, the relay's untrusted-tar rules refuse it.
    ref = find(ends.near, plan[1][0]["id"])
    assert ref is not None
    os.mkfifo(ref.path / "pipe")
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, _ = statuses(capsys)
    by_member = {tuple(outcome["members"]): outcome for outcome in transfers}
    assert sorted(outcome["status"] for outcome in transfers) == ["adopted", "adopted", "failed"]
    (failed,) = [outcome for outcome in transfers if outcome["status"] == "failed"]
    assert "this machine's relay" in failed["message"] and "stays held" in failed["message"]
    assert outgoing(ends.near) == [failed["transfer_id"]] and len(by_member) == 3
    # Fetched once more without it; adopted once.
    assert [len(header["names"]) for header in headers(requests, "send")] == [3, 2] and len(
        headers(requests, "stage")
    ) == 1
    assert_in(ends.far, [plan[0], plan[2]])


# -- outcomes at DST ------------------------------------------------------------------------------------------------


def test_a_failed_commit_is_delivered_again_from_the_same_relay_with_fsync(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"1": {"refuse": {"sync_failed": True}}}))
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    sources: list[tuple[str, bool]] = []
    real = transfer_cli._session_stage  # DST stages each segment (plan §7.10)

    def session_stage(*arguments: Any) -> Any:
        path = Path(arguments[4][0].path)
        sources.append((str(path.parent), path.is_dir()))
        return real(*arguments)

    monkeypatch.setattr(transfer_cli, "_session_stage", session_stage)
    plan = jobs(ends.near, 3)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 3
    # The retry and the later segment both with fsync (sticky); the retry from the same relay, not fetched again.
    assert [header.get("sync") for header in headers(requests, "stage")] == [None, "fsync", "fsync"]
    assert sources[0] == sources[1] and sources[0][1] and sources[2][0] != sources[0][0]
    assert len(headers(requests, "send")) == 2 and calls == []
    assert_in(ends.far, plan)


def test_a_bundle_dst_refuses_fails_alone_and_the_rest_goes_again_from_the_relay(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Holds an earlier run left: the refused one gets the conditional remedy (its earlier delivery may have been
    # adopted), stays held on SRC; the others go once more from the same relay.
    left = held(ends.near, ends.dst, 3)
    refused = left[1][0].transfer_id
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"1": {"refuse": {"bundle": refused}}}))
    requests = recorded_requests(monkeypatch)
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 1
    transfers, _ = statuses(capsys)
    by_id = {outcome["transfer_id"]: outcome for outcome in transfers}
    assert by_id[refused]["status"] == "failed"
    message = by_id[refused]["message"]
    assert "an earlier delivery may already have adopted it" in message
    # Taken back by hand on SRC's own host, naming the workspace as that host knows it.
    plain = ends.src.split(":", 1)[1]
    assert f"take it back on the host of {ends.src} with `httk job adopt --workspace {plain} " in message
    assert sorted(outcome["status"] for outcome in transfers) == ["adopted", "adopted", "failed"]
    assert len(headers(requests, "send")) == 1 and len(headers(requests, "stage")) == 2
    assert outgoing(ends.near) == [refused]
    assert_in(ends.far, [plan for hold, plan in left if hold.transfer_id != refused])


def test_a_whole_segment_refusal_stops_the_pair_and_keeps_every_hold_on_src(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"1": {"refuse": {}}}))
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert all("stays held" in outcome["message"] or "not delivered" in outcome["message"] for outcome in transfers)
    assert any("a scripted refusal" in error and "--resume" in error for error in errors)
    # Nothing taken back, nothing more fetched or sent after the refusal, nothing released.
    assert len(outgoing(ends.near)) == 4 and all(find(ends.near, mapping["id"]) is None for mapping, _, _ in plan)
    assert len(headers(requests, "stage")) == 1 and "release" not in ops(requests)
    assert len(headers(requests, "send")) == 1 and calls == []
    monkeypatch.delenv("HTTK_TEST_SCRIPT")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    assert_in(ends.far, plan)


def test_dst_dying_in_a_segment_keeps_it_and_the_later_ones_held_and_releases_the_earlier(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 6)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 4
    assert all("whether the jobs arrived is unknown" in outcome["message"] for outcome in transfers[2:4])
    assert all("not delivered" in outcome["message"] for outcome in transfers[4:])
    assert len(headers(requests, "stage")) == 2 and any("--resume" in error for error in errors)
    assert sorted(outgoing(ends.near)) == sorted(outcome["transfer_id"] for outcome in transfers[2:])
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_ADOPT")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    found = {outcome["transfer_id"]: outcome["status"] for outcome in statuses(capsys)[0]}
    assert [found[outcome["transfer_id"]] for outcome in transfers[2:]] == ["already_adopted"] * 2 + ["adopted"] * 2
    assert outgoing(ends.near) == []
    assert_in(ends.far, plan)


def test_another_workspace_answering_a_later_segment_stops_the_pair(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_OTHER_ID_FROM_ADOPT", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 6)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, _ = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 4
    assert "not the expected workspace" in transfers[2]["message"]
    assert len(headers(requests, "stage")) == 2 and len(headers(requests, "send")) == 2
    assert len(outgoing(ends.near)) == 4


# -- outcomes at SRC ------------------------------------------------------------------------------------------------


def test_src_dying_stops_every_pair(
    ends: Ends, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Holds left bound for two DSTs, driven by --resume without DST: SRC's session ends at its second send, in the
    # first pair's second segment; the other pair is not driven, nor its session opened.
    other, other_name = registered(remote.root, "other-far")
    left = [*held(ends.near, ends.dst, 2), *held(ends.near, f"other:{other_name}", 2)]
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "2")
    requests = recorded_requests(monkeypatch)
    before = len(log(ends.remote))
    assert run(ends.project, "--json", "-q", "--resume", ends.src) == 1
    transfers, errors = statuses(capsys)
    # The first segment adopted (or, still staged when SRC died with a positive deadline, given up and held).
    (adopted,) = [outcome for outcome in transfers if adopted_or_abandoned(outcome, DEADLINE)]
    assert sorted(outcome["status"] for outcome in transfers if outcome is not adopted) == ["failed"] * 3
    stopped = [outcome for outcome in transfers if outcome["destination"] != adopted["destination"]]
    assert len(stopped) == 2 and all("not delivered" in outcome["message"] for outcome in stopped)
    assert len(errors) == 2 and all("--resume" in error for error in errors)
    # One stage only; SRC's one session and the first pair's DST session, none for the second pair.
    assert len(headers(requests, "stage")) == 1 and counts(ends.remote, before) == {"src": (1, 1), "dst": (1, 1)}
    assert len(outgoing(ends.near)) == (3 if adopted["status"] == "adopted" else 4)
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    assert run(ends.project, "--json", "-q", "--resume", ends.src) == 0
    assert outgoing(ends.near) == []
    assert_in(ends.far, [plan for hold, plan in left if hold.destination_locator == ends.dst])
    assert_in(other, [plan for hold, plan in left if hold.destination_locator != ends.dst])


def test_src_dying_while_the_workflows_are_fetched_adopts_nothing(
    ends: Ends, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "1")  # the store entries' send
    requests = recorded_requests(monkeypatch)
    job_id = workflow_job(ends.near, workflows(ends.near, tmp_path), state="ready")
    assert run(ends.project, "--json", "-q", "--job", job_id, ends.src, ends.dst) == 1
    (outcome,), errors = statuses(capsys)
    assert outcome["status"] == "failed"
    assert f"{ends.src}: the transfer session failed before anything was adopted" in outcome["message"]
    assert "stage" not in ops(requests) and "install" not in ops(requests)
    assert len(outgoing(ends.near)) == 1 and find(ends.far, job_id) is None and errors


def test_a_release_src_refuses_leaves_the_jobs_adopted_and_held(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("HTTK_TEST_REFUSE_RELEASE", "1")
    plan = jobs(ends.near, 2)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, _ = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2
    assert all("the hold was not released" in outcome["message"] for outcome in transfers)
    assert len(outgoing(ends.near)) == 2
    assert_in(ends.far, plan)
    monkeypatch.delenv("HTTK_TEST_REFUSE_RELEASE")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["already_adopted"] * 2
    assert outgoing(ends.near) == []


# -- workflows ------------------------------------------------------------------------------------------------------


def test_workflows_go_before_the_first_segment(
    ends: Ends, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    job_id = workflow_job(ends.near, workflows(ends.near, tmp_path), state="ready")
    before = len(log(ends.remote))
    assert run(ends.project, "--json", "-q", "--job", job_id, ends.src, ends.dst) == 0
    (outcome,), _ = statuses(capsys)
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert {found.id for found in _store.list_installed(ends.far)} == {GIT_ID, "local:caller"}
    assert _store.closure(ends.far, "local:caller", check_builds=True).ok
    assert find(ends.far, job_id) is not None and relays_in(ends.relays) == []
    measured = counts(ends.remote, before)
    # Before the first segment: DST's workflows, SRC's store entries into a relay, DST's install; then the job.
    assert ops(requests) == [
        "select", "holds_page", "hold_page", "workflows", "send", "install", "send", "stage", "adopt_staged", "release"
    ]  # fmt: skip
    assert requests_in_order(requests)
    assert [header["kind"] for header in headers(requests, "send")] == ["store", "holds"]
    assert calls == [] and measured == {"src": (1, 1), "dst": (1, 1)}


# -- keep, resume ---------------------------------------------------------------------------------------------------


def test_keep_records_dst_s_id_in_the_away_copies(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 2)
    assert run(ends.project, "--json", "-q", "--keep", *selected(plan), ends.src, ends.dst) == 0
    transfers, _ = statuses(capsys)
    assert all(outcome["status"] == "adopted" and outcome["kept"] for outcome in transfers)
    for mapping, _, _ in plan:
        assert away_of(ends.near, mapping["id"])["workspace_id"] == ends.far.workspace_id
    assert {header.get("destination_id") for header in headers(requests, "release")} == {ends.far.workspace_id}
    assert_in(ends.far, plan)


@pytest.mark.parametrize("named", [True, False], ids=["dst", "every-dst"])
def test_resume_drives_the_holds_left(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], named: bool
) -> None:
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    left = held(ends.near, ends.dst, 3)
    assert run(ends.project, "--json", "-q", "--resume", ends.src, *([ends.dst] if named else [])) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 3
    assert outgoing(ends.near) == [] and relays_in(ends.relays) == []
    assert_in(ends.far, [plan for _, plan in left])
    # SRC's holds listed by holds_page, delivered over DST's session.
    assert "holds_page" in ops(requests) and "stage" in ops(requests) and calls == []


def test_a_dst_session_that_cannot_open_selects_and_holds_nothing(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    real = transfer_cli._open_session

    def open_session(end: Any, *arguments: Any) -> Any:
        if end.locator == ends.dst:
            raise RuntimeError("a scripted connection failure")
        return real(end, *arguments)

    monkeypatch.setattr(transfer_cli, "_open_session", open_session)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 2)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert transfers == [] and any("a scripted connection failure" in error for error in errors)
    assert outgoing(ends.near) == [] and all(find(ends.near, mapping["id"]) is not None for mapping, _, _ in plan)
    # Both ends opened before anything is selected or held (plan §7.12): SRC was asked nothing.
    assert ops(requests) == [] and calls == []


def test_src_dying_in_a_release_stops_the_pair(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "release")
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    # The first segment is adopted, not released; nothing more is fetched or adopted.
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 2
    assert all("the hold was not released" in outcome["message"] for outcome in transfers[:2])
    # Not sent, or (a positive deadline) staged and given up: nothing more is adopted after the failed release.
    later = ("not delivered", "not adopted, since the transfer stopped") if DEADLINE else ("not delivered",)
    assert all(any(said in outcome["message"] for said in later) for outcome in transfers[2:])
    assert len(headers(requests, "adopt_staged")) == 1 and ops(requests, SRC_OPS)[-1] == "release"
    assert any("--resume" in error for error in errors) and len(outgoing(ends.near)) == 4
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_OP")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 0
    assert sorted(outcome["status"] for outcome in statuses(capsys)[0]) == ["adopted"] * 2 + ["already_adopted"] * 2
    assert outgoing(ends.near) == []
    assert_in(ends.far, plan)


def test_earlier_holds_are_driven_before_the_selection_s_pages(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 1)
    requests = recorded_requests(monkeypatch)
    left = held(ends.near, ends.dst, 1)
    plan = jobs(ends.near, 2)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    transfers, _ = statuses(capsys)
    assert transfers[0]["transfer_id"] == left[0][0].transfer_id
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 3
    # One DST session for the earlier hold and every page.
    # The second page is the last.
    expected = ["select", "holds_page", "workflows", "send", "stage", "adopt_staged", "release"]
    assert same_requests(
        ops(requests), expected + ["hold_page", "send", "stage", "adopt_staged", "release"] * 2, DEADLINE
    )
    assert_in(ends.far, [*plan, left[0][1]])


# -- correction round 1: relay lifecycle, upload reads, bounded fetches, stops, remedies ------------------------------


def failing_relays(monkeypatch: pytest.MonkeyPatch, failing: int) -> None:
    """Make the *failing*-th relay of this process (``tempfile.mkdtemp``) fail with ENOSPC."""

    real = tempfile.mkdtemp
    made: list[int] = []

    def mkdtemp(*arguments: Any, **options: Any) -> str:
        made.append(1)
        if len(made) == failing:
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        return real(*arguments, **options)

    monkeypatch.setattr(tempfile, "mkdtemp", mkdtemp)


def test_a_relay_that_cannot_be_made_for_a_later_segment_stops_the_pair_keeping_the_earlier(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    failing_relays(monkeypatch, 2)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)  # a valid document
    # The first segment adopted and released.
    # The earlier segment adopted (or, still staged at the stop with a positive deadline, given up and held).
    assert all(adopted_or_abandoned(outcome, DEADLINE) for outcome in transfers[:2])
    assert [outcome["status"] for outcome in transfers[2:]] == ["failed"] * 2
    assert all("message" not in outcome for outcome in transfers[:2] if outcome["status"] == "adopted")
    assert all("relay could not be made" in outcome["message"] for outcome in transfers[2:])
    assert all("--resume" in outcome["message"] for outcome in transfers[2:])
    assert any("stopped" in error and "--resume" in error for error in errors)
    assert len(headers(requests, "stage")) == 1
    left = sorted(outcome["transfer_id"] for outcome in transfers if outcome["status"] != "adopted")
    assert sorted(outgoing(ends.near)) == left
    assert sum(find(ends.far, mapping["id"]) is not None for mapping, _, _ in plan) == 4 - len(left)


def test_a_relay_unreadable_during_the_upload_stops_the_pair(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    real = tarfile.TarFile.add
    added: list[int] = []

    def add(self: tarfile.TarFile, *arguments: Any, **options: Any) -> None:
        added.append(1)
        if len(added) == 1:
            raise OSError(errno.EIO, os.strerror(errno.EIO))
        real(self, *arguments, **options)

    monkeypatch.setattr(tarfile.TarFile, "add", add)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert any("relay could not be read" in error for error in errors)
    # No later fetch or adoption; everything held on SRC.
    assert len(headers(requests, "send")) == 1 and len(headers(requests, "stage")) == 1
    assert len(outgoing(ends.near)) == 4


def test_an_unexpected_error_in_a_later_segment_keeps_what_earlier_segments_settled(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    real = transfer_cli._push_segment
    pushed: list[int] = []

    def push_segment(*arguments: Any, **options: Any) -> None:
        pushed.append(1)
        if len(pushed) == 2:
            raise RuntimeError("an unexpected failure")
        real(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_push_segment", push_segment)
    plan = jobs(ends.near, 4)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    # The earlier segment adopted (or, still staged at the stop with a positive deadline, given up and held).
    assert all(adopted_or_abandoned(outcome, DEADLINE) for outcome in transfers[:2])
    assert [outcome["status"] for outcome in transfers[2:]] == ["failed"] * 2
    assert all("message" not in outcome for outcome in transfers[:2] if outcome["status"] == "adopted")
    assert all("an unexpected failure" in outcome["message"] for outcome in transfers[2:])
    assert any("stopped" in error for error in errors)
    assert len(outgoing(ends.near)) == sum(outcome["status"] != "adopted" for outcome in transfers)


def test_an_earlier_hold_refused_by_the_relay_gets_the_conditional_remedy(
    ends: Ends, capsys: pytest.CaptureFixture[str]
) -> None:
    left = held(ends.near, ends.dst, 2)
    os.mkfifo(Path(left[0][0].path) / "pipe")
    assert run(ends.project, "--json", "-q", "--resume", ends.src, ends.dst) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in statuses(capsys)[0]}
    refused = by_id[left[0][0].transfer_id]
    assert refused["status"] == "failed" and "this machine's relay" in refused["message"]
    assert "an earlier delivery may already have adopted it" in refused["message"]
    assert f"take it back on the host of {ends.src}" in refused["message"]
    assert by_id[left[1][0].transfer_id]["status"] == "adopted"


def test_the_relay_refuses_a_top_level_entry_that_is_no_directory(tmp_path: Path) -> None:
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        info = tarfile.TarInfo("a" * 32)
        info.size = 4
        archive.addfile(info, io.BytesIO(b"file"))

    class Stream(io.BytesIO):
        def finish(self) -> None:
            pass

    with pytest.raises(_landing.StreamRefused) as refused:
        transfer_cli._extract_relay(tmp_path / "relay", Stream(data.getvalue()))  # type: ignore[arg-type]
    assert refused.value.bundle == "a" * 32 and "not a directory" in str(refused.value)


def test_a_second_relay_refusal_in_a_segment_leaves_its_rest_unfetched(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    for index in (0, 1):
        ref = find(ends.near, plan[index][0]["id"])
        assert ref is not None
        os.mkfifo(ref.path / "pipe")
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, _ = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 3
    assert "refused twice" in transfers[2]["message"] and "--resume" in transfers[2]["message"]
    assert len(headers(requests, "send")) == 2 and "stage" not in ops(requests)
    assert len(outgoing(ends.near)) == 3


def test_fsync_and_workflow_attempts_last_across_pages(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 1)
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"1": {"refuse": {"sync_failed": True}}}))
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 3
    # Three pages: the workflows asked once for the pair; every adoption after the failed commit with fsync.
    assert ops(requests, SRC_OPS).count("hold_page") >= 3 and ops(requests).count("workflows") == 1
    assert [header.get("sync") for header in headers(requests, "stage")] == [None, "fsync", "fsync", "fsync"]
    assert_in(ends.far, plan)


def test_the_pinned_workspace_id_lasts_across_pages(
    ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 1)
    monkeypatch.setenv("HTTK_TEST_OTHER_ID_FROM_ADOPT", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    transfers, errors = statuses(capsys)
    assert [outcome["status"] for outcome in transfers] == ["adopted", "failed"]
    assert "not the expected workspace" in transfers[1]["message"]
    # Nothing more is held after the stop: the third job stays in SRC, unattempted.
    assert ops(requests, SRC_OPS).count("hold_page") == 2 and find(ends.near, plan[2][0]["id"]) is not None
    assert any("1 selected job(s) were not attempted" in error for error in errors)


def test_the_relay_goes_where_tmpdir_says(
    ends: Ends, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    chosen = tmp_path / "chosen-tmp"
    chosen.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", None)  # the default temporary location, read from the environment
    monkeypatch.setenv("TMPDIR", str(chosen))
    seen: list[str] = []
    real = transfer_cli._push_segment

    def push_segment(session: Any, settled: Any, holds: Any, base: Any = None, **options: Any) -> None:
        seen.append(str(base))
        real(session, settled, holds, base, **options)

    monkeypatch.setattr(transfer_cli, "_push_segment", push_segment)
    plan = jobs(ends.near, 2)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    assert [outcome["status"] for outcome in statuses(capsys)[0]] == ["adopted"] * 2
    assert (
        len(seen) == 1
        and Path(seen[0]).parent.parent == chosen
        and Path(seen[0]).parent.name.startswith("httk-transfer-")
    )
    assert relays_in(chosen) == []


@pytest.fixture(autouse=True)
def _no_relays_leak(tmp_path: Path) -> Iterator[None]:
    yield
    relays = tmp_path / "relays"
    if relays.exists():
        assert relays_in(relays) == []


# -- an end whose httk cannot serve the transfer session (plan §7.12) ----------------------------------------------

#: Far sides that cannot serve this side's transfer session: an ``httk`` without the command, and one of another
#: protocol (its hello names it).
_BAD_FAR_SIDES = {
    "old": (
        (
            "import sys\nprint(\"httk workflow: error: argument GROUP: invalid choice: 'transfer-session' (choose from 'run', 'session')\", file=sys.stderr)\n"
            "sys.exit(2)\n"
        ),
        "too old (or too new)",
    ),
    "new": (
        (
            f"import json, sys\nprint(json.dumps({{'format': 'httk-workflow-transfer-session', 'format_version': "
            f"{TRANSFER_PROTOCOL + 1}}}), flush=True)\nsys.stdin.read()\n"
        ),
        f"its transfer protocol is {TRANSFER_PROTOCOL + 1}, this side's {TRANSFER_PROTOCOL}",
    ),
}


@pytest.mark.parametrize("why", sorted(_BAD_FAR_SIDES))
@pytest.mark.parametrize("bad", ["src", "dst"])
def test_one_end_that_cannot_serve_the_session_stops_before_anything_is_selected_or_held(
    ends: Ends, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], bad: str, why: str
) -> None:
    source, message = _BAD_FAR_SIDES[why]
    script = tmp_path / f"{why}_httk.py"
    script.write_text(source, encoding="utf-8")
    name = "cluster" if bad == "src" else "other"
    bundle = resolve_remote(name, project=ends.project).bundle
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["httk_command"] = shlex.join([sys.executable, str(script)])
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
    requests = recorded_requests(monkeypatch)
    calls = adapter_calls(monkeypatch)
    plan = jobs(ends.near, 2)
    for argv in (selected(plan), ["--resume"]):
        if bad == "src" and argv == ["--resume"]:
            # Nothing selected: SRC's failure to open is the command's error (exit 2), as any other.
            assert run(ends.project, "--json", "-q", *argv, ends.src, ends.dst) == 2
            text = capsys.readouterr().err
        else:
            assert run(ends.project, "--json", "-q", *argv, ends.src, ends.dst) == 1
            document = outcomes(capsys)
            text = " ".join(document["errors"]) + " ".join(str(item.get("message")) for item in document["transfers"])
        assert f"the httk on {name} is too old (or too new) for this transfer: install the same httk version" in text
        assert message in text
        # Both ends opened and checked first: neither was asked anything, nothing held or changed on either.
        assert ops(requests) == [] and calls == []
        assert outgoing(ends.near) == [] and outgoing(ends.far) == []
        assert_in(ends.near, plan)
        assert all(find(ends.far, mapping["id"]) is None for mapping, _, _ in plan)
