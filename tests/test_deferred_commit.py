"""Deferred commit of a push's and a pull's landings (plan WORKFLOW_TRANSFER_SESSION §7.11, T6c-lite and its
amendment; transfer protocol 3).

A push's own ``stage`` sends ``defer_commit``: the far side answers with the landing's sync barrier open and commits
it first thing in the stage's ``adopt_staged`` (then the final scan, then the adoption). A commit failing there is a
validated ``sync_failed`` refusal that leaves a retry slot for the very next request: the client stages the same
bundles at once with ``retry_of`` and ``sync: fsync``, at the head of the far side's staging order, and adopts them
before any later stage. A relay never defers. A pull defers its own landings likewise and re-fetches a landing whose
commit failed at its adoption, before any later one is adopted.
"""

import argparse
import errno
import io
import json
import os
import sys
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _fs, _kernel, _landing, _moving
from httk.workflow.adapters import TRANSFER_PROTOCOL, TransferSession
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli._common import _modifiable
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import jobs, outcomes, outgoing, registered, run, selected
from test_moving import find
from test_paged_holds import family, ids
from test_remote_to_remote import Ends
from test_session_pull import PAGE, PATCHED_PULL, durable, landing_scratches
from test_session_push import local_project, writer
from test_staged_settle import (  # noqa: F401 (cluster, relay_ends: fixtures)
    PATCHED_STAGED,
    _far_owners_and_scratch,
    _once,
    cluster,
    relay_ends,
)
from test_sync_errors_retry import Syncfs

type Cluster = tuple[Path, Workspace, str, Workspace, str]
type Recorded = list[tuple[str, dict[str, Any], dict[str, Any] | None]]


def recorded(monkeypatch: pytest.MonkeyPatch) -> Recorded:
    """Record the op, header and response (``None`` when it raised) of every transfer-session request, in order."""

    seen: Recorded = []
    real = TransferSession.request

    def request(session: TransferSession, op: str, header: Any = None, **options: Any) -> dict[str, Any]:
        position, copied = len(seen), dict(header or {})
        seen.append((op, copied, None))
        response = real(session, op, header, **options)
        seen[position] = (op, copied, response)
        return response

    monkeypatch.setattr(TransferSession, "request", request)
    return seen


def late_failed(response: dict[str, Any] | None) -> bool:
    document = None if response is None else response.get("document")
    return (
        isinstance(document, dict)
        and response is not None
        and response.get("ok") is False
        and (document.get("sync_failed") is True)
    )


def adoption_order(requests: Recorded) -> list[list[str]]:
    """The transfer ids of each ``adopt_staged`` that adopted, in order."""

    return [
        header["transfer_ids"]
        for op, header, response in requests
        if op == "adopt_staged" and response and response["ok"]
    ]


def assert_retried_in_place(requests: Recorded) -> list[str]:
    """Every late ``sync_failed`` is followed at once by its retry (``stage`` with ``retry_of`` its token, ``sync:
    fsync``, no ``defer_commit``, the same ids) and then that retry's ``adopt_staged``; the tokens that failed late."""

    failed = []
    for index, (op, header, response) in enumerate(requests):
        if op != "adopt_staged" or not late_failed(response):
            continue
        failed.append(header["token"])
        retry_op, retry, staged = requests[index + 1]
        assert retry_op == "stage" and retry.get("retry_of") == header["token"], requests[index + 1]
        assert retry.get("sync") == "fsync" and "defer_commit" not in retry
        assert retry["transfer_ids"] == header["transfer_ids"]
        assert staged is not None and staged["ok"] is True
        adopt_op, adopt, _ = requests[index + 2]
        assert adopt_op == "adopt_staged" and adopt["token"] == staged["token"]
    return failed


def assert_deferred_until_the_first_retry(requests: Recorded) -> None:
    """Every first ``stage`` before the first retry defers its commit; every one after it fsyncs (sticky)."""

    retried = False
    for op, header, _ in requests:
        if op != "stage":
            continue
        if "retry_of" in header:
            retried = True
        elif retried:
            assert header.get("sync") == "fsync" and "defer_commit" not in header, header
        else:
            assert header.get("defer_commit") is True and "sync" not in header, header


def first_stages(requests: Recorded) -> list[list[str]]:
    """The transfer ids of each first ``stage`` (not a retry), in order."""

    return [header["transfer_ids"] for op, header, _ in requests if op == "stage" and "retry_of" not in header]


@pytest.fixture
def staged(cluster: Cluster) -> Cluster:  # noqa: F811
    """The stand-in ssh cluster of :func:`test_staged_settle.cluster` (its ``httk`` :data:`PATCHED_STAGED`)."""

    return cluster


@pytest.fixture
def relayed(relay_ends: Ends) -> Ends:  # noqa: F811
    """Two remote workspaces relayed through this machine (:func:`test_staged_settle.relay_ends`)."""

    return relay_ends


# -- the far side ---------------------------------------------------------------------------------------------------


#: :data:`PATCHED_STAGED`, and ``HTTK_TEST_SCAN_LOG``: a line ``final-scan`` appended to that file at each final scan
#: of a settle (with ``HTTK_TEST_ORDER_LOG`` naming the same file, ordered with the commits).
SCAN_PATCH = """
if argv[:2] == ["workflow", "transfer-session"] and os.environ.get("HTTK_TEST_SCAN_LOG"):
    from httk.workflow import _kernel

    real_scan_final = _kernel.scan_final

    def scan_final(*arguments, **options):
        with open(os.environ["HTTK_TEST_SCAN_LOG"], "a") as log:
            log.write("final-scan\\n")
        return real_scan_final(*arguments, **options)

    _kernel.scan_final = scan_final
"""
PATCHED_SCANS = PATCHED_STAGED.replace('sys.argv = ["httk", *argv]\n', SCAN_PATCH + 'sys.argv = ["httk", *argv]\n')
assert PATCHED_SCANS != PATCHED_STAGED


@pytest.fixture
def far_session(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, Workspace, str]:
    """The ``local`` adapter's bundle (its ``httk`` :data:`PATCHED_SCANS`), a local SRC and a durable DST forced to
    ``io.sync syncfs``."""

    _, bundle = local_project(tmp_path)
    wrapper = tmp_path / "patched_staged.py"
    wrapper.write_text(PATCHED_SCANS, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    return bundle, near, far, far_name


def opened(bundle: Path) -> TransferSession:
    session = TransferSession(bundle, timeout=60)
    session.open()
    return session


def stage(
    session: TransferSession, far_name: str, near: Workspace, holds: list[_moving.Hold], **header: Any
) -> dict[str, Any]:
    transfer_ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), transfer_ids)
    declared = sum(transfer_cli._measure(Path(hold.path))[0] for hold in holds)
    request = {"workspace": far_name, "transfer_ids": transfer_ids, "bytes": declared, **header}
    return session.request("stage", request, upload=writer(data))


def adopt(session: TransferSession, far_name: str, staged: dict[str, Any], **header: Any) -> dict[str, Any]:
    request = {"workspace": far_name, "token": staged["token"], "transfer_ids": staged["transfer_ids"], **header}
    return session.request("adopt_staged", request)


def commits(order: Path) -> int:
    return order.read_text().splitlines().count("commit") if order.exists() else 0


def test_a_stage_without_defer_commit_commits_before_it_answers(
    far_session: tuple[Path, Workspace, Workspace, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The regression pin: as before T6c, the stage itself commits, and its commit's failure is its own answer.
    bundle, near, far, far_name = far_session
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first barrier's commit, after its prime
    holds, plan = hold_jobs(near, 2)
    with opened(bundle) as session:
        refused = stage(session, far_name, near, holds[:1])
        assert refused["ok"] is False and refused["document"]["sync_failed"] is True and "token" not in refused
        staged = stage(session, far_name, near, holds[1:])
        assert staged["ok"] is True and commits(order) == 1  # committed (successfully) before it answered
        assert adopt(session, far_name, staged)["ok"] is True and commits(order) == 1
    assert find(far, plan[1][0]["id"]) is not None and find(far, plan[0][0]["id"]) is None


def test_a_deferred_stage_answers_open_and_its_adopt_staged_commits_first(
    far_session: tuple[Path, Workspace, Workspace, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, near, far, far_name = far_session
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    holds, plan = hold_jobs(near, 2)
    with opened(bundle) as session:
        staged = stage(session, far_name, near, holds, defer_commit=True)
        assert staged["ok"] is True
        # The opening echo's terminal and the stage's: validated, but nothing committed yet.
        assert order.read_text().splitlines() == ["terminal", "terminal"]
        adopted = adopt(session, far_name, staged)
        assert (
            adopted["ok"] is True and [entry["status"] for entry in adopted["document"]["bundles"]] == ["adopted"] * 2
        )
        assert order.read_text().splitlines() == ["terminal", "terminal", "commit"]
        assert "syncing " in adopted["stderr"]  # the commit's progress line, now the adoption's
        malformed = stage(session, far_name, near, holds, defer_commit="yes")
        assert malformed["ok"] is False and malformed["error"] == "stage needs defer_commit as a boolean"
    _once(far, plan)
    assert _far_owners_and_scratch(far) == ([], [])


@pytest.mark.parametrize("deadline", [0.0, 0.05], ids=["D0", "D0.05"])
def test_the_far_side_commits_before_its_final_scan(
    far_session: tuple[Path, Workspace, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    deadline: float,
) -> None:
    # The post-commit observation: the ids the first scan missed are listed again after the deferred commit, even at a
    # zero deadline (where the first scan was final already).
    bundle, near, far, far_name = far_session
    far.set_policy({"visibility_deadline_seconds": deadline})
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    monkeypatch.setenv("HTTK_TEST_SCAN_LOG", str(order))
    holds, plan = hold_jobs(near, 1)
    with opened(bundle) as session:
        staged = stage(session, far_name, near, holds, defer_commit=True)
        assert adopt(session, far_name, staged)["ok"] is True
    assert order.read_text().splitlines() == ["terminal", "terminal", "commit", "final-scan"]
    _once(far, plan)


def test_a_misused_adopt_staged_leaves_a_deferred_stage_pending_with_its_barrier_open(
    far_session: tuple[Path, Workspace, Workspace, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, near, far, far_name = far_session
    _other, other_name = registered(tmp_path, "other")
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    holds, plan = hold_jobs(near, 2)
    with opened(bundle) as session:
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:], defer_commit=True)
        refusals = [
            adopt(session, far_name, {**first, "token": "a" * 16}),  # unknown token
            adopt(session, far_name, second),  # out of order
            adopt(session, far_name, {**first, "transfer_ids": second["transfer_ids"]}),  # other ids
            adopt(session, other_name, first),  # another workspace
        ]
        for response in refusals:
            # Refused before anything was consumed: no document, so nothing the client could take back by.
            assert response["ok"] is False and "nothing was adopted" in response["error"], response
            assert "document" not in response and "sync_failed" not in response and "fatal" not in response
        assert commits(order) == 0  # neither barrier committed, both still open
        assert len(_far_owners_and_scratch(far)[0]) == 2
        # Each stage commits on its own barrier once rightly asked, in staging order.
        assert adopt(session, far_name, first)["ok"] is True and commits(order) == 1
        assert adopt(session, far_name, second)["ok"] is True and commits(order) == 2
    _once(far, plan)


def test_a_late_sync_failed_leaves_a_retry_slot_at_the_head_of_the_staging_order(
    far_session: tuple[Path, Workspace, Workspace, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, near, far, far_name = far_session
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first stage's prime is 1; its deferred commit 2
    holds, plan = hold_jobs(near, 2)
    with opened(bundle) as session:
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:], defer_commit=True)
        failed = adopt(session, far_name, first)
        assert failed["ok"] is False and failed["returncode"] == 2 and "fatal" not in failed
        document = failed["document"]
        assert document["sync_failed"] is True and document["bundles"] == [] and "refused" in document
        assert document["workspace_id"] == far.workspace_id
        # Disposed of before the answer: only the second stage's owner and landing remain.
        owners, scratch = _far_owners_and_scratch(far)
        assert len(owners) == 1 and len(scratch) == 1
        assert adopt(session, far_name, first)["ok"] is False  # consumed
    with opened(bundle) as session:  # a new far side: its own prime is 1, the first deferred commit 2
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:], defer_commit=True)
        assert late_failed(adopt(session, far_name, first))
        retry = stage(session, far_name, near, holds[:1], sync="fsync", retry_of=first["token"])
        assert retry["ok"] is True
        # At the head: adopted before the older second stage, which is refused as out of order meanwhile.
        assert "order" in adopt(session, far_name, second)["error"]
        assert adopt(session, far_name, retry)["ok"] is True
        assert adopt(session, far_name, second)["ok"] is True
    _once(far, plan)


@pytest.mark.parametrize("between", ["unknown op", "adopt_staged", "stage"])
def test_any_other_request_drops_the_retry_slot(
    far_session: tuple[Path, Workspace, Workspace, str], monkeypatch: pytest.MonkeyPatch, between: str
) -> None:
    bundle, near, far, far_name = far_session
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")
    holds, plan = hold_jobs(near, 3)
    with opened(bundle) as session:
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:2], defer_commit=True)
        assert late_failed(adopt(session, far_name, first))
        if between == "unknown op":
            assert session.request("nope")["ok"] is False
        elif between == "adopt_staged":
            assert adopt(session, far_name, {**second, "token": "b" * 16})["ok"] is False
        else:
            assert stage(session, far_name, near, holds[2:], defer_commit=True)["ok"] is True
        late = stage(session, far_name, near, holds[:1], sync="fsync", retry_of=first["token"])
        assert late["ok"] is False and "names no segment this session may retry now" in late["error"]
        assert "document" not in late and "capacity" not in late
        assert adopt(session, far_name, second)["ok"] is True
    assert find(far, plan[0][0]["id"]) is None and find(far, plan[1][0]["id"]) is not None


@pytest.mark.parametrize("misuse", ["other ids", "other token", "no fsync", "defer_commit"])
def test_an_unmatched_retry_of_is_refused_consuming_nothing(
    far_session: tuple[Path, Workspace, Workspace, str], monkeypatch: pytest.MonkeyPatch, misuse: str
) -> None:
    bundle, near, far, far_name = far_session
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")
    holds, plan = hold_jobs(near, 3)
    with opened(bundle) as session:
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:2], defer_commit=True)
        assert late_failed(adopt(session, far_name, first))
        header: dict[str, Any] = {"sync": "fsync", "retry_of": first["token"]}
        retried = holds[:1]
        if misuse == "other ids":
            retried = holds[2:]
        elif misuse == "other token":
            header["retry_of"] = second["token"]
        elif misuse == "no fsync":
            del header["sync"]
        else:
            header["defer_commit"] = True
        refused = stage(session, far_name, near, retried, **header)
        assert refused["ok"] is False and "nothing of it was used" in refused["error"], refused
        assert "document" not in refused and "capacity" not in refused and "token" not in refused
        # Nothing consumed: the second stage is still the oldest, and nothing of the refused upload landed.
        owners, scratch = _far_owners_and_scratch(far)
        assert len(owners) == 1 and len(scratch) == 1
        assert adopt(session, far_name, second)["ok"] is True
    assert [find(far, mapping["id"]) is not None for mapping, _, _ in plan] == [False, True, False]


def test_a_failed_deferred_commit_releases_its_capacity_and_ids_after_disposal(
    far_session: tuple[Path, Workspace, Workspace, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, near, far, far_name = far_session
    monkeypatch.setenv("HTTK_TEST_FAR_STAGES", "2")
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")
    holds, plan = hold_jobs(near, 3)
    with opened(bundle) as session:
        first = stage(session, far_name, near, holds[:1], defer_commit=True)
        second = stage(session, far_name, near, holds[1:2], defer_commit=True)
        # An open-barrier stage counts against the capacity like any other.
        full = stage(session, far_name, near, holds[2:], defer_commit=True)
        assert full["ok"] is False and full["capacity"] is True
        assert late_failed(adopt(session, far_name, first))
        # Its stage count, bytes and ids no longer count: the same bundles stage afresh (not as a retry).
        again = stage(session, far_name, near, holds[:1], defer_commit=True)
        assert again["ok"] is True
        assert adopt(session, far_name, second)["ok"] is True
        assert adopt(session, far_name, again)["ok"] is True
    assert [find(far, mapping["id"]) is not None for mapping, _, _ in plan] == [True, True, False]
    assert _far_owners_and_scratch(far) == ([], [])


def test_the_final_scan_observes_after_a_deferred_commit_even_at_a_zero_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    far.set_policy({"visibility_deadline_seconds": 0})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    events: list[tuple[str, bool]] = []
    real_scan, real_syncfs = _kernel._scan, _fs._syncfs

    def scan(*arguments: Any, final: bool = False) -> dict[str, _kernel.JobRef]:
        events.append(("scan", final))
        return real_scan(*arguments, final=final)

    def syncfs(descriptor: int) -> None:
        events.append(("syncfs", True))
        real_syncfs(descriptor)

    monkeypatch.setattr(_kernel, "_scan", scan)
    monkeypatch.setattr(_fs, "_syncfs", syncfs)
    arguments = argparse.Namespace(workspace=far_name, by_path=False, no_durable=False)
    context = CLIContext("httk", tmp_path)

    def resolve() -> Workspace:
        return _modifiable(arguments, context, action="adopt jobs into it")

    _document, staged = job_cli.stage_stream(resolve, io.BytesIO(data), defer_commit=True)
    assert staged is not None and not staged.synced and staged.state == "committed"
    # The prime, then the first scan (final already at a zero deadline); no commit yet.
    assert events == [("syncfs", True), ("scan", True)]
    with pytest.raises(ValueError, match="not durable yet"):
        staged.begin()  # never adopted before its commit
    events.clear()
    assert job_cli.sync_stage(staged) is None and staged.synced
    document, returncode, _unclosed = job_cli.adopt_stage(staged)
    entries = cast(list[dict[str, Any]], document["bundles"])
    assert returncode == 0 and [entry["status"] for entry in entries] == ["adopted"] * 2
    # The commit, then a final scan of the ids the first one missed, after it.
    assert events[:2] == [("syncfs", True), ("scan", True)]
    _once(far, plan)


def test_a_deferred_stage_abandoned_closes_its_descriptor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, _plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    arguments = argparse.Namespace(workspace=far_name, by_path=False, no_durable=False)
    context = CLIContext("httk", tmp_path)
    _document, staged = job_cli.stage_stream(
        lambda: _modifiable(arguments, context, action="adopt jobs into it"), io.BytesIO(data), defer_commit=True
    )
    assert staged is not None and staged.deferred is not None and staged.deferred.open
    assert staged.abandon() is True
    assert not staged.deferred.open and staged.state == "abandoned"
    with pytest.raises(ValueError, match="runs once"):
        staged.deferred.commit()


def test_begin_refuses_a_stage_whose_deferred_commit_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    arguments = argparse.Namespace(workspace=far_name, by_path=False, no_durable=False)
    context = CLIContext("httk", tmp_path)
    _document, staged = job_cli.stage_stream(
        lambda: _modifiable(arguments, context, action="adopt jobs into it"), io.BytesIO(data), defer_commit=True
    )
    assert staged is not None and not staged.synced
    Syncfs(monkeypatch, [errno.EIO])  # the deferred commit (the prime ran at the stage)
    with pytest.raises(_landing.CommitFailed):
        staged.sync()
    # Closed by the failed commit, yet never durable: no adoption may begin.
    assert staged.deferred is not None and not staged.deferred.open and not staged.synced
    with pytest.raises(ValueError, match="not durable yet"):
        staged.begin()
    assert staged.state == "committed" and staged.abandon() is True
    assert find(far, plan[0][0]["id"]) is None


def test_under_fsync_a_deferred_stage_commits_inline_and_holds_no_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Only a syncfs commit is deferred: under io.sync fsync every file was synced as it was written, and the stage
    # behaves exactly as before T6c (no barrier kept, no final scan after a commit at a zero deadline).
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "fsync"})
    far.set_policy({"visibility_deadline_seconds": 0})
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    scans: list[bool] = []
    real_scan = _kernel._scan

    def scan(*arguments: Any, final: bool = False) -> dict[str, _kernel.JobRef]:
        scans.append(final)
        return real_scan(*arguments, final=final)

    monkeypatch.setattr(_kernel, "_scan", scan)
    arguments = argparse.Namespace(workspace=far_name, by_path=False, no_durable=False)
    context = CLIContext("httk", tmp_path)
    _document, staged = job_cli.stage_stream(
        lambda: _modifiable(arguments, context, action="adopt jobs into it"), io.BytesIO(data), defer_commit=True
    )
    assert staged is not None and staged.deferred is None and staged.synced
    assert scans == [True]
    assert job_cli.sync_stage(staged) is None
    scans.clear()
    _adopted, returncode, _unclosed = job_cli.adopt_stage(staged)
    assert returncode == 0 and True not in scans  # no final scan re-listing the first scan's misses
    _once(far, plan)


def test_a_far_side_under_fsync_commits_a_defer_commit_stage_before_it_answers(
    far_session: tuple[Path, Workspace, Workspace, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle, near, far, far_name = far_session
    far.configure_settings({"io.sync": "fsync"})
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    holds, plan = hold_jobs(near, 1)
    with opened(bundle) as session:
        staged = stage(session, far_name, near, holds, defer_commit=True)
        assert staged["ok"] is True and commits(order) == 1  # inline, at the stage: nothing deferred
        assert adopt(session, far_name, staged)["ok"] is True and commits(order) == 1
    _once(far, plan)


def test_the_transfer_protocol_is_3() -> None:
    assert TRANSFER_PROTOCOL == 3


def test_a_far_side_of_protocol_2_stops_the_push_at_open(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = staged
    monkeypatch.setenv("HTTK_TEST_PROTOCOL", "2")  # a T7 httk there
    plan = jobs(near, 1)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    error = " ".join(document["errors"]) + " ".join(str(item.get("message")) for item in document["transfers"])
    assert "the httk on cluster is too old (or too new) for this transfer" in error
    assert "its transfer protocol is 2, this side's 3" in error
    assert outgoing(near) == [] and find(far, plan[0][0]["id"]) is None


# -- the client of a push -------------------------------------------------------------------------------------------


def hold_maturity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Adopt no staged segment for having matured: only to make room (the pair's :data:`_STAGES`) or at the end, so
    that every segment is staged before the first is adopted, whatever the host's speed (the receiver still waits out
    its own deadline)."""

    monkeypatch.setattr(transfer_cli, "_adopt_matured", lambda _pair: None)


def deferred_push(setup: Cluster, monkeypatch: pytest.MonkeyPatch, failing: str, count: int) -> list[Any]:
    """*count* single-bundle segments, all staged (:func:`hold_maturity`) before the first is adopted, with the far
    side's ``syncfs`` calls *failing* (1 is the first stage's prime, then the deferred commits in adoption order)."""

    _project, near, _near_name, far, _far_name = setup
    far.configure_settings({"io.sync": "syncfs"})
    far.set_policy({"visibility_deadline_seconds": 0.05})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", failing)
    hold_maturity(monkeypatch)
    return jobs(near, count)


@pytest.mark.parametrize("failing", [1, 2, 3], ids=["first", "middle", "last"])
def test_a_late_failure_is_retried_and_adopted_before_any_later_stage(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failing: int
) -> None:
    project, near, near_name, far, far_name = staged
    plan = deferred_push(staged, monkeypatch, str(failing + 1), 3)
    requests = recorded(monkeypatch)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    order = first_stages(requests)
    # These assertions hold whatever the maturity schedule (the hook only makes all three wait together): a first
    # stage defers its commit until the late failure and fsyncs after it (sticky), the one late failure is retried in
    # place, and the adoptions follow the staging order.
    assert_deferred_until_the_first_retry(requests)
    assert len(assert_retried_in_place(requests)) == 1
    # Adopted in staging order, the retried segment in its own place.
    assert adoption_order(requests) == order
    assert captured.err.count("could not make a staged segment durable") == 1
    _once(far, plan)
    assert outgoing(near) == []


def test_a_cascade_of_late_failures_is_retried_in_order_and_fsync_sticks(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = staged
    # One writeback error seen while three deferred stages are open fails each of their commits.
    plan = deferred_push(staged, monkeypatch, "2,3,4", 4)
    monkeypatch.setattr(transfer_cli, "_STAGES", 3)  # the fourth waits for the first adoption
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 4
    # The schedule this test needs (three deferred stages open at the first adoption), made by the hook.
    assert [op for op, _, _ in requests][:5] == ["workflows", "stage", "stage", "stage", "adopt_staged"]
    failed = assert_retried_in_place(requests)
    assert len(failed) == 3
    order = first_stages(requests)
    assert adoption_order(requests) == order
    headers = [header for op, header, _ in requests if op == "stage" and "retry_of" not in header]
    # The fourth, staged after the first failure, goes with an fsync of every file and nothing deferred.
    assert [(header.get("sync"), header.get("defer_commit")) for header in headers] == [
        (None, True),
        (None, True),
        (None, True),
        ("fsync", None),
    ]
    _once(far, plan)
    assert outgoing(near) == []


def test_a_double_failure_keeps_the_holds_and_the_pair_goes_on(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = staged
    plan = deferred_push(staged, monkeypatch, "2", 2)
    # The third stage request (the first segment's retry) is refused as a failed commit too: scripted, since a
    # conforming far side cannot answer so (the retry fsyncs every file inline, so its commit has nothing to fail).
    failed = {"refuse": {"sync_failed": True, "workspace_id": far.workspace_id}}
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"3": failed}))
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = first_stages(requests)
    # Sent, so never taken back: held for --resume, though this run made it; the pair went on.
    assert by_id[first[0]]["status"] == "failed" and "--resume" in by_id[first[0]]["message"]
    assert "its delivery again was refused too" in by_id[first[0]]["message"]
    assert by_id[second[0]]["status"] == "adopted"
    assert outgoing(near) == first
    monkeypatch.delenv("HTTK_TEST_SCRIPT")
    monkeypatch.delenv("HTTK_TEST_FAIL_SYNCFS")
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"]
    _once(far, plan)


def test_an_explicitly_selected_child_is_adopted_before_its_parent_across_a_late_retry(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = staged
    deferred_push(staged, monkeypatch, "2", 0)  # the child's segment's commit fails
    made = family(near, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *ids(made["c1"], made["parent"]), near_name, f"cluster:{far_name}") == 0
    document = outcomes(capsys)
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"] * 2 and document["errors"] == []
    assert len(assert_retried_in_place(requests)) == 1
    assert adoption_order(requests) == first_stages(requests)
    for name in ("c1", "parent"):
        assert find(far, made[name]["id"]) is not None and find(near, made[name]["id"]) is None


def test_a_retry_refused_by_an_inline_fsync_error_stops_the_pair_and_keeps_the_holds(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = staged
    plan = deferred_push(staged, monkeypatch, "2", 2)
    # The realistic failure of a retry: its inline fsync fails, a refusal naming no bundle (scripted here).
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"3": {"refuse": {"workspace_id": far.workspace_id}}}))
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = first_stages(requests)
    assert by_id[first[0]]["status"] == "failed" and "its delivery again was refused too" in by_id[first[0]]["message"]
    # The pair stopped: the second staged segment was given up, not adopted; both stay held (sent: never taken back).
    assert (
        by_id[second[0]]["status"] == "failed"
        and "not adopted, since the transfer stopped" in (by_id[second[0]]["message"])
    )
    assert sorted(outgoing(near)) == sorted(first + second)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)


def retry_answered(monkeypatch: pytest.MonkeyPatch, answer: Any) -> None:
    """Answer a late retry's ``stage`` (``retry_of``) on this side with *answer*: an exception to raise, or the
    ``(document, stderr)`` of :func:`_session_stage`; every other stage goes to the far side."""

    real = transfer_cli._session_stage

    def session_stage(*arguments: Any, **options: Any) -> Any:
        if options.get("retry_of") is None:
            return real(*arguments, **options)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(transfer_cli, "_session_stage", session_stage)


@pytest.mark.parametrize("refusal", ["capacity", "unclosed"])
def test_a_retry_refused_for_capacity_or_an_unclosed_session_stops_the_pair(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], refusal: str
) -> None:
    project, near, near_name, _far, far_name = staged
    plan = deferred_push(staged, monkeypatch, "2", 2)
    kind = transfer_cli._Capacity if refusal == "capacity" else transfer_cli._Unclosed
    retry_answered(monkeypatch, kind(f"a scripted {refusal} refusal"))
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = first_stages(requests)
    message = by_id[first[0]]["message"]
    assert by_id[first[0]]["status"] == "failed" and f"a scripted {refusal} refusal" in message
    assert "its delivery again was refused" in message and "--resume" in message
    assert "not adopted, since the transfer stopped" in by_id[second[0]]["message"]
    assert sorted(outgoing(near)) == sorted(first + second)


@pytest.mark.parametrize("marker", ["sync_failed", "stream_aborted", "token"])
def test_a_retry_answered_by_another_workspace_is_unknown_and_stops_the_pair(
    staged: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], marker: str
) -> None:
    project, near, near_name, far, far_name = staged
    plan = deferred_push(staged, monkeypatch, "2", 2)
    other = "00000000-0000-4000-8000-000000000001"
    answer: dict[str, Any]
    if marker == "token":  # the retry staged, but by another workspace
        answer = {"token": "t" * 16, "workspace_id": other, "matures_in": 0.0, "bytes": 0, "deferred": False}
    else:
        answer = {"bundles": [], "workspace_id": other, "refused": "a refusal of another workspace", marker: True}
    retry_answered(monkeypatch, (answer, ""))
    requests = recorded(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = first_stages(requests)
    # Held to the pin before it is classified: an unknown outcome, not a second failed commit that lets the pair go on
    # (nor a stage of another workspace that is adopted).
    message = by_id[first[0]]["message"]
    assert by_id[first[0]]["status"] == "failed"
    if marker == "token":
        assert f"staged the segment's retry in workspace {other}" in message and "--resume" in message
        assert not any(op == "adopt_staged" and header["token"] == "t" * 16 for op, header, _ in requests)
    else:
        assert "whether the jobs arrived is unknown" in message
        assert f"answered the retry of a staged segment as workspace {other}" in message
    assert "not adopted, since the transfer stopped" in by_id[second[0]]["message"]
    assert sorted(outgoing(near)) == sorted(first + second)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_relay_never_defers_its_commit(
    relayed: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    relayed.far.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # DST's first commit, after its prime
    requests = recorded(monkeypatch)
    plan = jobs(relayed.near, 2)
    argv = ["--json", "-q", *selected(plan), relayed.src, relayed.dst]
    assert run(relayed.project, *argv) == 0
    assert {outcome["status"] for outcome in outcomes(capsys)["transfers"]} == {"adopted"}
    headers = [header for op, header, _ in requests if op == "stage"]
    # Committed before the stage answered (its failure is the stage's own answer), retried from the same relay.
    assert all("defer_commit" not in header and "retry_of" not in header for header in headers)
    assert [header.get("sync") for header in headers] == [None, "fsync"]
    assert not any(late_failed(response) for op, _, response in requests if op == "adopt_staged")
    _once(relayed.far, plan)


# -- the client of a pull -------------------------------------------------------------------------------------------


@pytest.fixture
def pull_ssh(tmp_path: Path, remote: Remote) -> Cluster:
    """A project with the stand-in ssh cluster (its ``httk`` :data:`PATCHED_PULL`), a SRC there and a local DST."""

    project = tmp_path / "project"
    initialize_project(project, name="deferred-pull")
    fake_remote(project)
    wrapper = tmp_path / "patched_pull.py"
    wrapper.write_text(PATCHED_PULL, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def receptions(monkeypatch: pytest.MonkeyPatch, *, failing_fsync: bool = False) -> list[tuple[bool, bool]]:
    """Each pull landing: whether every file was fsynced as written, and whether its commit was deferred."""

    seen: list[tuple[bool, bool]] = []
    real = _landing.receive_tar

    def receive(workspace: Workspace, landing: Path, stream: Any, **options: Any) -> list[str]:
        seen.append((bool(options.get("force_fsync")), options.get("defer_commit") is not None))
        if failing_fsync and options.get("force_fsync"):
            raise _landing.StreamRefused("refused the stream, nothing of it was used: an fsync failed")
        return real(workspace, landing, stream, **options)

    monkeypatch.setattr(_landing, "receive_tar", receive)
    return seen


def deferred_pull(setup: Cluster, monkeypatch: pytest.MonkeyPatch) -> None:
    """Single-bundle fetches into a ``syncfs`` DST, all staged (:func:`hold_maturity`) before the first is adopted."""

    _project, near, _near_name, _far, _far_name = setup
    near.configure_settings({"io.sync": "syncfs"})
    near.set_policy({"visibility_deadline_seconds": 0.05})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    hold_maturity(monkeypatch)


def test_a_pull_commits_at_adoption_and_refetches_a_late_failure_before_any_later_stage(
    pull_ssh: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = pull_ssh
    deferred_pull(pull_ssh, monkeypatch)
    Syncfs(monkeypatch, [None, errno.EIO])  # the first landing's prime, then its deferred commit at its adoption
    landings = receptions(monkeypatch)
    requests = recorded(monkeypatch)
    plan = jobs(far, 3)
    assert durable(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    sent = [header["names"] for op, header, _ in requests if op == "send"]
    released = [header["transfer_ids"] for op, header, _ in requests if op == "release"]
    # Whatever the maturity schedule (the hook makes all three fetches wait together): the first fetch's commit, deferred
    # to its adoption, fails there; it is fetched again (once, with an fsync of every file) and adopted, so released,
    # before any later fetch; each landing defers its commit until then, and fsyncs (sticky) after it.
    retry = next(index for index, names in enumerate(sent) if index and names == sent[0])
    fetches = sent[:retry] + sent[retry + 1 :]
    assert len(sent) == 4 and released == fetches
    assert [op for op, _, _ in requests][: len(PAGE) + 1] == [*PAGE, "send"]
    assert landings[retry] == (True, False)
    assert all(landing == (False, True) for landing in landings[:retry])
    assert all(landing == (True, False) for landing in landings[retry:])
    assert captured.err.count("again, with an fsync of every file, before any later segment is adopted") == 1
    _once(near, plan)
    assert outgoing(far) == [] and landing_scratches(near) == []


def test_a_pull_s_failed_refetch_keeps_the_holds_and_stops(
    pull_ssh: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = pull_ssh
    deferred_pull(pull_ssh, monkeypatch)
    Syncfs(monkeypatch, [None, errno.EIO])
    receptions(monkeypatch, failing_fsync=True)
    requests = recorded(monkeypatch)
    plan = jobs(far, 2)
    assert durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second, retried = (header["names"][0] for op, header, _ in requests if op == "send")
    assert retried == first and "release" not in [op for op, _, _ in requests]
    assert by_id[first]["status"] == "failed" and "an fsync failed" in by_id[first]["message"]
    assert by_id[second]["status"] == "failed" and "not adopted, since the transfer stopped" in by_id[second]["message"]
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert sorted(outgoing(far)) == sorted([first, second]) and landing_scratches(near) == []


def test_a_pull_whose_failed_landing_cannot_be_discarded_keeps_the_holds_and_stops(
    pull_ssh: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = pull_ssh
    deferred_pull(pull_ssh, monkeypatch)
    Syncfs(monkeypatch, [None, errno.EIO])
    real = _moving.Stage.abandon

    def abandon(stage: _moving.Stage) -> bool:
        if stage.deferred is not None and not stage.deferred.open and not stage.synced:  # its commit failed
            raise OSError(errno.EIO, "an injected cleanup failure")
        return real(stage)

    monkeypatch.setattr(_moving.Stage, "abandon", abandon)
    requests = recorded(monkeypatch)
    plan = jobs(far, 2)
    assert durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = (header["names"][0] for op, header, _ in requests if op == "send")  # no re-fetch
    message = by_id[first]["message"]
    assert by_id[first]["status"] == "failed" and "could not be discarded" in message
    assert "an injected cleanup failure" in message and "--resume" in message
    assert "not adopted, since the transfer stopped" in by_id[second]["message"]
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert sorted(outgoing(far)) == sorted([first, second])


def barriers(monkeypatch: pytest.MonkeyPatch) -> list[_fs.Barrier]:
    """Every sync barrier this process enters."""

    entered: list[_fs.Barrier] = []
    real = _fs.Barrier.__enter__

    def enter(barrier: _fs.Barrier) -> _fs.Barrier:
        entered.append(barrier)
        return real(barrier)

    monkeypatch.setattr(_fs.Barrier, "__enter__", enter)
    return entered


@pytest.mark.parametrize("where", ["deferred commit", "modifiability check"])
def test_an_interrupt_before_a_pulled_stage_s_adoption_gives_it_up(
    pull_ssh: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], where: str
) -> None:
    project, near, near_name, far, far_name = pull_ssh
    deferred_pull(pull_ssh, monkeypatch)
    entered = barriers(monkeypatch)
    if where == "deferred commit":
        calls: list[int] = []
        real_syncfs = _fs._syncfs

        def syncfs(descriptor: int) -> None:
            calls.append(descriptor)
            real_syncfs(descriptor)
            if len(calls) == 2:  # the landing's deferred commit, at its adoption (its long syncfs)
                raise KeyboardInterrupt

        monkeypatch.setattr(_fs, "_syncfs", syncfs)
    else:
        real_check = transfer_cli.require_cli_modifiable

        def require_cli_modifiable(workspace: Workspace) -> None:
            if sys._getframe(1).f_code.co_name == "_adopt_local_stage":
                raise KeyboardInterrupt
            real_check(workspace)

        monkeypatch.setattr(transfer_cli, "require_cli_modifiable", require_cli_modifiable)
    plan = jobs(far, 1)
    with pytest.raises(KeyboardInterrupt):
        durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name)
    capsys.readouterr()
    # Given up: no owner, no landing scratch, no open descriptor left here; the hold kept on SRC.
    assert _far_owners_and_scratch(near) == ([], []) and landing_scratches(near) == []
    assert entered and all(barrier._descriptor is None for barrier in entered)
    assert find(near, plan[0][0]["id"]) is None and len(outgoing(far)) == 1


def test_an_interrupt_while_a_failed_commit_s_landing_is_disposed_of_gives_the_stage_up(
    pull_ssh: Cluster, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project, near, near_name, far, far_name = pull_ssh
    deferred_pull(pull_ssh, monkeypatch)
    entered = barriers(monkeypatch)
    armed: list[bool] = []
    calls: list[int] = []
    real_syncfs, real_discard = _fs._syncfs, _kernel.Owner.discard_scratch

    def syncfs(descriptor: int) -> None:
        calls.append(descriptor)
        if len(calls) == 2:  # the landing's deferred commit, at its adoption
            armed.append(True)
            raise OSError(errno.EIO, os.strerror(errno.EIO))
        real_syncfs(descriptor)

    def discard_scratch(owner: _kernel.Owner, *arguments: Any, **options: Any) -> Any:
        if armed:  # once: while the failed commit's landing is disposed of
            armed.clear()
            raise KeyboardInterrupt
        return real_discard(owner, *arguments, **options)

    monkeypatch.setattr(_fs, "_syncfs", syncfs)
    monkeypatch.setattr(_kernel.Owner, "discard_scratch", discard_scratch)
    requests = recorded(monkeypatch)
    plan = jobs(far, 1)
    with pytest.raises(KeyboardInterrupt):
        durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name)
    capsys.readouterr()
    # Given up all the same: no owner, no landing scratch, no open descriptor here; no re-fetch; the hold kept on SRC.
    assert _far_owners_and_scratch(near) == ([], []) and landing_scratches(near) == []
    assert entered and all(barrier._descriptor is None for barrier in entered)
    assert [op for op, _, _ in requests].count("send") == 1
    assert find(near, plan[0][0]["id"]) is None and len(outgoing(far)) == 1
