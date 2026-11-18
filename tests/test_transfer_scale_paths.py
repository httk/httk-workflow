"""Per-job costs a transfer of many jobs no longer pays (plan WORKFLOW_TRANSFER_SESSION §7.10, items 3 to 5, T5b).

The far side's ``holds_page`` reads one frozen enumeration of ``transfers/outgoing/`` per connection and workspace;
the client spools its report and renders it at the end byte for byte as the in-memory report did (whose
implementation is kept here as the reference); a page resolves each workflow root in the store once; and the far
side's directory listings during ``adopt``/``stage``/``adopt_staged`` count as progress.
"""

import collections
import copy
import errno
import gc
import io
import json
import os
import shutil
import sys
import uuid
import weakref
from collections.abc import Iterator
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, Self

import pytest

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _bundles, _frames, _kernel, _moving, _pool, _store
from httk.workflow._frames import FrameWriter
from httk.workflow.adapters import TransferSession
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import _transfer_session
from httk.workflow.workflow_cli._transfer_session import Request
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import at_once, jobs, run, selected
from test_job_transfer import registered as registered_now
from test_moving import WORKFLOW, assert_in, find, place
from test_paged_holds import PATCHED_PAGED, paged
from test_remote_to_remote import PATCHED_R2R
from test_session_push import PATCHED_HTTK, local_project, writer
from v3_helpers import cli_owner, job_mapping

_DELIVERED = transfer_cli._DELIVERED
_SHIPPED = transfer_cli._SHIPPED


def reference_report(
    outcomes: list[dict[str, Any]],
    errors: list[str],
    *,
    json_output: bool,
    source: str,
    empty: str = "no held transfers to drive",
    changed: int | None = None,
) -> int:
    """The report as it was rendered from the lists of every outcome and error (httk-workflow 5a770a6), unchanged."""

    missing = sorted({workflow for outcome in outcomes for workflow in outcome.get("missing_workflows", ())})
    # One warning per id: a root the adoption names as missing is left to its line below.
    shipped = {
        entry["id"]: entry
        for outcome in outcomes
        for entry in outcome.get("workflows", ())
        if not (entry["status"] == "missing" and entry["id"] in missing)
    }
    for status, text in _SHIPPED.items():
        if listed := sorted(workflow_id for workflow_id, entry in shipped.items() if entry["status"] == status):
            print(f"{text}: {', '.join(listed)}", file=sys.stderr)
    for workflow_id, entry in sorted(shipped.items()):
        if "reason" in entry:
            print(f"warning: workflow {workflow_id} {entry['status']}: {entry['reason']}", file=sys.stderr)
    if missing:
        print(f"warning: these workflows are not installed, so their jobs wait: {', '.join(missing)}", file=sys.stderr)
    if changed:
        print(
            f"{changed} selected job(s) changed since they were listed: left out; run again to select them",
            file=sys.stderr,
        )
    for error in errors:
        print(error, file=sys.stderr)
    if json_output:
        document: dict[str, object] = {"transfers": outcomes, "errors": errors}
        if changed is not None:
            document["changed"] = changed  # a paged selection's
        print(json.dumps(document, indent=2, sort_keys=True))
    else:
        if not outcomes and not errors:
            print(empty)
        for outcome in outcomes:
            print(
                f"{outcome['transfer_id']}\t{outcome['status']}\t{len(outcome['members'])} job(s)\t{outcome['destination']}"
            )
        if kept := sum(len(outcome["members"]) for outcome in outcomes if outcome.get("kept")):
            print(f"kept {kept} away copies in {source} (httk job list --state away; httk job recover JOB to resend)")
    for outcome in outcomes:
        if "message" in outcome:
            print(f"{outcome['transfer_id']}: {outcome['message']}", file=sys.stderr)
    failed = errors or any(outcome["status"] not in _DELIVERED or "message" in outcome for outcome in outcomes)
    return 1 if failed else 0


def rendered(render: Any, *arguments: Any, **options: Any) -> tuple[bytes, bytes, int]:
    """Standard output, standard error (encoded) and the return code of *render*."""

    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = render(*arguments, **options)
    return out.getvalue().encode(), err.getvalue().encode(), code


# -- the spooled report: generated cases ---------------------------------------------------------------------------


def _outcome(index: int, status: str = "adopted", **members: Any) -> dict[str, Any]:
    return {
        "transfer_id": f"{index:032x}",
        "members": [f"job-{index}-{member}" for member in range(1 + index % 3)],
        "destination": "cluster:dst",
        "status": status,
        **members,
    }


def _shipped(workflow_id: str, status: str, **members: Any) -> dict[str, Any]:
    return {"id": workflow_id, "status": status, **members}


#: Outcome lists and error lists, by name.
CASES: dict[str, tuple[list[dict[str, Any]], list[str]]] = {
    "empty": ([], []),
    "errors only": ([], ["one error", "another: with ünïcode and \"quotes\"\nand a newline"]),
    "one": ([_outcome(1)], []),
    "many": ([_outcome(index, "already_adopted" if index % 4 == 0 else "adopted") for index in range(25)], []),
    "unicode": (
        [_outcome(2, "failed", message="ståndard é ∂ 🚀 \\ \"x\""), {**_outcome(3), "destination": "här:ws"}],
        ["fel: ✗"],
    ),
    "kept": ([_outcome(4, kept=True), _outcome(5, kept=False), _outcome(6, kept=True)], []),
    "message": (
        [_outcome(7, message="the jobs are in X, but the hold was not released"), _outcome(8, "refused", message="r")],
        [],
    ),
    "delivered with a message": ([_outcome(30), _outcome(31, "already_adopted", message="not released")], []),
    "statuses": ([_outcome(9, "taken"), _outcome(10, "not_moved"), _outcome(11, "failed")], ["e1", "e2"]),
    "missing workflows": (
        [
            _outcome(12, missing_workflows=["b--1", "a--1"]),
            _outcome(13, missing_workflows=["a--1", "c--1"]),
            _outcome(14, missing_workflows=[]),
        ],
        [],
    ),
    "workflows": (
        [
            _outcome(
                15,
                workflows=[
                    _shipped("w--installed", "installed"),
                    _shipped("w--mismatch", "mismatch", reason="another tree"),
                    _shipped("w--missing", "missing"),
                    _shipped("w--failed", "failed", reason="the build failed"),
                    _shipped("w--present", "present"),
                ],
            ),
            # A later entry for the same id wins, unless it is a "missing" the adoption names too.
            _outcome(
                16,
                missing_workflows=["w--both"],
                workflows=[
                    _shipped("w--installed", "mismatch", reason="changed since"),
                    _shipped("w--both", "installed"),
                    _shipped("w--only-missing", "missing"),
                ],
            ),
            _outcome(
                17, missing_workflows=["w--only-missing"], workflows=[_shipped("w--both", "missing", reason="gone")]
            ),
        ],
        ["an error beside workflows"],
    ),
    "everything": (
        [
            _outcome(18, kept=True, workflows=[_shipped("w--x", "installed")]),
            _outcome(19, "failed", message="m", missing_workflows=["w--y"]),
            _outcome(20, published=[{"job_key": "k", "state": "ready", "path": "/p/ä"}], missing_workflows=[]),
        ],
        ["e"],
    ),
}


@pytest.mark.parametrize("case", list(CASES))
@pytest.mark.parametrize("changed", [None, 0, 3])
@pytest.mark.parametrize("json_output", [True, False], ids=["json", "text"])
def test_the_spooled_report_renders_what_the_reference_renders(
    case: str, changed: int | None, json_output: bool
) -> None:
    outcomes, errors = CASES[case]
    options: dict[str, Any] = {"json_output": json_output, "source": "src:ws", "changed": changed}
    if case == "empty":
        options["empty"] = "no ready job to move in src:ws"
    expected = rendered(reference_report, copy.deepcopy(outcomes), list(errors), **options)
    with transfer_cli._Spool() as spool:
        # In batches, as the transfers give them: some final at once, some concluded later.
        half = len(outcomes) // 2
        late: list[Any] = []

        def drive(concluded: Any) -> list[dict[str, Any]]:
            late.append(concluded)
            return [dict(item) for item in outcomes[:half]]

        spool.driven(drive)
        spool.add(dict(item) for item in outcomes[half:])
        spool.extend_errors(errors)
        late[0]()
        assert rendered(spool.report, **options) == expected
    if json_output:
        document = json.loads(expected[0])
        assert list(document) == (["changed"] if changed is not None else []) + ["errors", "transfers"]


def test_batches_keep_the_order_they_were_given_in_whenever_they_conclude() -> None:
    concluding: list[Any] = []

    def later(number: int) -> Any:
        def drive(concluded: Any) -> list[dict[str, Any]]:
            concluding.append(concluded)
            return [_outcome(number)]

        return drive

    with transfer_cli._Spool() as spool:
        spool.driven(later(1))
        spool.add([_outcome(2)])  # final at once, but after the first batch
        spool.driven(lambda concluded: concluded() or [_outcome(3)])  # concluded before it returned
        spool.driven(later(4))
        assert spool.outcomes == 0 and len(spool._waiting) == 4  # held behind the first
        concluding[0]()
        assert spool.outcomes == 3 and len(spool._waiting) == 1
        with pytest.raises(RuntimeError):
            spool.driven(lambda concluded: (_ for _ in ()).throw(RuntimeError("the drive failed")))
        spool.add([_outcome(5)])
        out, _err, code = rendered(spool.report, json_output=True, source="s")  # the fourth reported as it stands
    assert code == 0
    assert [item["transfer_id"] for item in json.loads(out)["transfers"]] == [f"{index:032x}" for index in range(1, 6)]


class _Outcome(dict[str, Any]):
    """An outcome whose life a weak reference can follow."""


def test_the_spool_keeps_no_outcome_once_it_is_spooled() -> None:
    with transfer_cli._Spool() as spool:
        batch = [_Outcome(_outcome(index, kept=index % 2 == 0)) for index in range(500)]
        references = [weakref.ref(item) for item in batch]
        spool.add(batch)
        del batch
        gc.collect()
        assert all(reference() is None for reference in references)
        assert not spool._waiting and spool.outcomes == 500
        out, _err, code = rendered(spool.report, json_output=False, source="s")
    assert code == 0 and out.decode().count("\tadopted\t") == 500


# -- the spooled report: real transfers against the reference ------------------------------------------------------


@pytest.fixture
def compared(monkeypatch: pytest.MonkeyPatch) -> list[tuple[bytes, bytes, int, bytes, bytes, int]]:
    """Render every transfer report of this process twice: from the spool, and with :func:`reference_report` from
    lists of the outcomes and errors as the transfer gave them (each batch as its drive returned it, so concluded in
    place later, as the in-memory report had them). Each report: the spool's stdout, stderr and return code, then the
    reference's; the spool's output is printed."""

    reports: list[tuple[bytes, bytes, int, bytes, bytes, int]] = []
    Spool = transfer_cli._Spool
    real_init, real_add, real_driven, real_error, real_report = (
        Spool.__init__,
        Spool.add,
        Spool.driven,
        Spool.error,
        Spool.report,
    )
    given: dict[int, tuple[list[dict[str, Any]], list[str]]] = {}

    def init(spool: Any) -> None:
        real_init(spool)
        given[id(spool)] = ([], [])

    def add(spool: Any, outcomes: Any) -> None:
        outcomes = list(outcomes)
        given[id(spool)][0].extend(outcomes)
        real_add(spool, outcomes)

    def driven(spool: Any, drive: Any, *arguments: Any, **options: Any) -> None:
        def recording(*arguments: Any, **options: Any) -> Any:
            outcomes = drive(*arguments, **options)
            given[id(spool)][0].extend(outcomes)
            return outcomes

        real_driven(spool, recording, *arguments, **options)

    def error(spool: Any, text: str) -> None:
        given[id(spool)][1].append(text)
        real_error(spool, text)

    def report(spool: Any, **options: Any) -> int:
        out, err, code = rendered(real_report, spool, **options)
        outcomes, errors = given.pop(id(spool))
        reports.append((out, err, code, *rendered(reference_report, outcomes, errors, **options)))
        sys.stdout.write(out.decode())
        sys.stderr.write(err.decode())
        return code

    for name, function in (("__init__", init), ("add", add), ("driven", driven), ("error", error), ("report", report)):
        monkeypatch.setattr(Spool, name, function)
    return reports


def _agree(reports: list[tuple[bytes, bytes, int, bytes, bytes, int]], count: int = 1) -> None:
    assert len(reports) == count
    for out, err, code, reference_out, reference_err, reference_code in reports:
        assert out == reference_out
        assert err == reference_err
        assert code == reference_code


# -- the spooled report when its files cannot be written -------------------------------------------------------------

#: The warning of a spool that could not be written, which the reference does not print.
SPOOL_WARNING = b"warning: the transfer report could not be spooled in "


class _Failing:
    """Spool files whose writes (or flushes) fail with ``ENOSPC`` as *fails* decides, by the count of writes over
    every spool file so far; a failing write writes half its line first."""

    def __init__(self, fails: Any, *, flush: bool = False) -> None:
        self.writes = 0
        self.failed = 0
        self.fails, self.flush = fails, flush

    def __call__(self) -> Any:
        failing = self

        class File(io.FileIO):
            def write(self, data: Any) -> int:
                failing.writes += 1
                if failing.fails(failing.writes):
                    failing.failed += 1
                    super().write(bytes(data)[: len(data) // 2])
                    raise OSError(errno.ENOSPC, "No space left on device")
                return super().write(data)

            def flush(self) -> None:
                if failing.flush and not self.closed:
                    failing.failed += 1
                    raise OSError(errno.ENOSPC, "No space left on device")
                super().flush()

        with transfer_cli.tempfile.TemporaryFile(buffering=0) as handle:  # unlinked: the duplicate keeps it
            return File(os.dup(handle.fileno()), "r+")


@pytest.mark.parametrize(
    "case",
    ["transient", "persistent", "flush"],
    ids=["a transient write failure", "a persistent failure from mid-run on", "a flush at render time"],
)
@pytest.mark.parametrize("json_output", [True, False], ids=["json", "text"])
def test_a_spool_that_cannot_be_written_reports_everything_truthfully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    compared: list[Any],
    case: str,
    json_output: bool,
) -> None:
    registered = at_once(registered_now, 0.3)
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    paged(monkeypatch, 1)
    plan = jobs(source, 6)
    failing = _Failing(
        {"transient": lambda count: count == 3, "persistent": lambda count: count >= 3, "flush": lambda _: False}[case],
        flush=case == "flush",
    )
    monkeypatch.setattr(transfer_cli, "_spool_file", failing)
    argv = [*(["--json"] if json_output else []), "-q", *selected(plan), source_name, target_name]
    assert run(tmp_path, *argv) == 0  # as without the failure: every job moved
    assert failing.failed >= 1
    _agree(compared)  # the report as from memory, byte for byte
    warnings = [line for line in capsys.readouterr().err.splitlines() if line.startswith(SPOOL_WARNING.decode())]
    assert len(warnings) == int(case != "flush")  # said once, while the transfer ran
    assert_in(target, plan)
    out = compared[0][0].decode()
    statuses = (
        [item["status"] for item in json.loads(out)["transfers"]]
        if json_output
        else [line.split("\t")[1] for line in out.splitlines()]
    )
    assert statuses == ["adopted"] * 6


class _PickyStderr:
    """A standard error whose writes of the spool's warning fail (on the same full disk, say)."""

    def __init__(self, stream: Any) -> None:
        self.stream, self.refused = stream, 0

    def write(self, text: str) -> int:
        if text.startswith(SPOOL_WARNING.decode()):
            self.refused += 1
            raise OSError(errno.ENOSPC, "No space left on device")
        return self.stream.write(text)

    def flush(self) -> None:
        self.stream.flush()


def test_a_spool_that_fails_while_its_warning_cannot_be_printed_still_reports_everything_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, compared: list[Any]
) -> None:
    registered = at_once(registered_now, 0.3)
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    paged(monkeypatch, 1)
    plan = jobs(source, 6)
    monkeypatch.setattr(transfer_cli, "_spool_file", _Failing(lambda count: count >= 3))
    stderr = _PickyStderr(sys.stderr)
    monkeypatch.setattr(sys, "stderr", stderr)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 0
    assert stderr.refused == 1
    _agree(compared)
    transfers = json.loads(compared[0][0])["transfers"]
    assert [item["status"] for item in transfers] == ["adopted"] * 6
    assert len({item["transfer_id"] for item in transfers}) == 6  # none twice, none replaced
    assert_in(target, plan)


def test_a_spool_that_cannot_be_read_back_stops_its_report_with_a_diagnostic_and_status_2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered_now(tmp_path, "a")
    target, target_name = registered_now(tmp_path, "b")
    plan = jobs(source, 3)
    real_open = open

    class Failing:
        """A reader of a spool file whose second line cannot be read (EIO)."""

        def __init__(self, descriptor: int, mode: str, closefd: bool) -> None:
            self.reader = real_open(descriptor, mode, closefd=closefd)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_exc_info: object) -> None:
            self.reader.close()

        def seek(self, offset: int) -> int:
            return self.reader.seek(offset)

        def __iter__(self) -> Iterator[bytes]:
            yield self.reader.readline()
            raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(transfer_cli, "open", Failing, raising=False)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 2
    captured = capsys.readouterr()
    assert captured.out.startswith('{\n  "changed": 0,\n  "errors": [],\n  "transfers": [\n    {')  # cut short
    with pytest.raises(ValueError):
        json.loads(captured.out)
    (diagnostic,) = [line for line in captured.err.splitlines() if "could not be read back" in line]
    assert "Input/output error" in diagnostic and "the report is incomplete" in diagnostic
    assert "`httk transfer status` shows what is still held" in diagnostic
    assert_in(target, plan)  # the transfers themselves are done
    assert _moving.held_names(source) == []


def test_a_spool_that_cannot_be_made_holds_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered_now(tmp_path, "a")
    target, target_name = registered_now(tmp_path, "b")
    plan = jobs(source, 2)

    def full() -> Any:
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(transfer_cli, "_spool_file", full)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) != 0
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "the transfer report cannot be spooled in" in captured.err and "nothing was held or moved" in captured.err
    assert_in(source, plan)
    assert _moving.held_names(source) == [] and all(find(target, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_batch_leaves_the_spool_s_queue_only_once_every_outcome_of_it_is_spooled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = transfer_cli._Spool._outcome
    calls: list[int] = []

    def outcome(spool: Any, item: dict[str, Any]) -> None:
        calls.append(1)
        if len(calls) == 2:
            raise RuntimeError("an unexpected failure while the second outcome is spooled")
        real(spool, item)

    monkeypatch.setattr(transfer_cli._Spool, "_outcome", outcome)
    batch = [_outcome(1), _outcome(2), _outcome(3)]
    with transfer_cli._Spool() as spool:
        with pytest.raises(RuntimeError):
            spool.add(batch)
        assert len(spool._waiting) == 1 and spool._waiting[0].written == 1  # kept, one of it spooled
        out, _err, code = rendered(spool.report, json_output=True, source="s")
    assert code == 0
    assert [item["transfer_id"] for item in json.loads(out)["transfers"]] == [f"{index:032x}" for index in (1, 2, 3)]


def _patched(tmp_path: Path, remote: Remote, script: str) -> None:
    wrapper = tmp_path / "patched.py"
    wrapper.write_text(script, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')


@pytest.mark.parametrize("deadline", [0, 0.3], ids=["D0", "D0.3"])
@pytest.mark.parametrize("json_output", [True, False], ids=["json", "text"])
def test_a_session_push_reports_what_the_reference_reports(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    compared: list[Any],
    deadline: float,
    json_output: bool,
) -> None:
    registered = at_once(registered_now, deadline)
    project = tmp_path / "project"
    initialize_project(project, name="spooled-push")
    fake_remote(project)
    _patched(tmp_path, remote, PATCHED_PAGED)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    paged(monkeypatch, 1)
    plan = jobs(near, 4)
    argv = [*(["--json"] if json_output else []), "-q", *selected(plan), near_name, f"cluster:{far_name}"]
    assert run(project, *argv) == 0
    _agree(compared)
    assert_in(far, plan)


@pytest.mark.parametrize("deadline", [0, 0.3], ids=["D0", "D0.3"])
def test_a_session_pull_with_a_stop_reports_what_the_reference_reports(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, compared: list[Any], deadline: float
) -> None:
    registered = at_once(registered_now, deadline)
    project = tmp_path / "project"
    initialize_project(project, name="spooled-pull")
    fake_remote(project)
    _patched(tmp_path, remote, PATCHED_PAGED)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    paged(monkeypatch, 1)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert_in(near, plan)
    # Then a pull whose third page cannot be held: a stop, its errors and the --resume remedy.
    more = jobs(far, 4)
    monkeypatch.setenv("HTTK_TEST_LOSE_HOLD_PAGE", "3")
    assert run(project, "-q", *selected(more), f"cluster:{far_name}", near_name) == 1
    _agree(compared, 2)
    assert b"--resume" in compared[1][1]


@pytest.mark.parametrize("deadline", [0, 0.3], ids=["D0", "D0.3"])
@pytest.mark.parametrize("json_output", [True, False], ids=["json", "text"])
def test_local_to_local_with_kept_copies_and_errors_reports_what_the_reference_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, compared: list[Any], deadline: float, json_output: bool
) -> None:
    registered = at_once(registered_now, deadline)
    source, source_name = registered(tmp_path, "a")
    _target, target_name = registered(tmp_path, "b")
    paged(monkeypatch, 2)
    plan = jobs(source, 5)
    hold = ["job", "transfer", "--hold", "--json", "--no-durable", *selected(plan[:3]), source_name, target_name]
    from httk.core.cli import CLIContext

    from httk.workflow.workflow_cli import command

    assert command(hold, CLIContext("httk", tmp_path)) == 0
    with cli_owner(source) as owner:
        ref = find(source, plan[3][0]["id"])
        assert ref is not None
        claimed = _kernel.claim(source, owner, ref)
        assert claimed is not None
        _moving.hold(source, owner, claimed, tree=True, destination_locator="nowhere:ws")  # cannot be reached
    json_flag = ["--json"] if json_output else []
    # Every hold, each where it records: the reachable ones kept as away copies, one an error.
    assert run(tmp_path, *json_flag, "-q", "--keep", "--resume", source_name) == 1
    _agree(compared)
    assert b"cannot reach its destination nowhere:ws" in compared[0][1]
    assert json_output or b"kept 3 away copies" in compared[0][0]


def test_remote_to_remote_over_sessions_reports_what_the_reference_reports(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, compared: list[Any]
) -> None:
    import tempfile

    registered = at_once(registered_now, 0.3)
    project = tmp_path / "project"
    initialize_project(project, name="spooled-r2r")
    fake_remote(project, host="src.example.test")
    fake_remote(project, name="other", host="dst.example.test")
    _patched(tmp_path, remote, PATCHED_R2R)
    near, near_name = registered(remote.root, "near")
    far, far_name = registered(remote.root, "far")
    relays = tmp_path / "relays"
    relays.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(relays))
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 2)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    plan = jobs(near, 5)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{near_name}", f"other:{far_name}") == 0
    _agree(compared)
    assert_in(far, plan)


# -- the frozen enumeration of holds_page ----------------------------------------------------------------------------


def _request(state: dict[str, Any], header: dict[str, Any]) -> Request:
    return Request(1, {"op": "holds_page", **header}, None, FrameWriter(io.BytesIO().write, lambda: None), state)


def _page(state: dict[str, Any], name: str, after: str | None, **header: Any) -> dict[str, Any]:
    return dict(_transfer_session._holds_page(_request(state, {"workspace": name, "after": after, **header})))


def _all_pages(state: dict[str, Any], name: str, **header: Any) -> list[list[str]]:
    pages: list[list[str]] = []
    after = None
    while True:
        page = _page(state, name, after, **header)
        pages.append([hold["transfer_id"] for hold in page["holds"]])
        if (after := page["next"]) is None:
            return pages


@pytest.fixture
def listings(monkeypatch: pytest.MonkeyPatch) -> collections.Counter[str]:
    """Count the listings of each workspace's ``transfers/outgoing/``, and the calls of ``held_names``."""

    counts: collections.Counter[str] = collections.Counter()
    real_list_names, real_held_names = _kernel.list_names, _moving.held_names

    def list_names(directory: Path) -> list[str]:
        if Path(directory).name == "outgoing" and Path(directory).parent.name == "transfers":
            counts["outgoing"] += 1
        return real_list_names(directory)

    def held_names(workspace: Any) -> list[str]:
        counts["held_names"] += 1
        return real_held_names(workspace)

    monkeypatch.setattr(_kernel, "list_names", list_names)
    monkeypatch.setattr(_moving, "held_names", held_names)
    return counts


def _hold_more(workspace: Workspace, count: int, locator: str = "x:y") -> list[str]:
    plan = jobs(workspace, count)
    with cli_owner(workspace) as owner:
        made = []
        for mapping, _, _ in plan:
            ref = find(workspace, mapping["id"])
            assert ref is not None
            claimed = _kernel.claim(workspace, owner, ref)
            assert claimed is not None
            made.append(_moving.hold(workspace, owner, claimed, tree=True, destination_locator=locator).transfer_id)
    return made


def test_holds_page_lists_outgoing_once_over_its_pages(tmp_path: Path, listings: collections.Counter[str]) -> None:
    far, far_name = registered_now(tmp_path, "far")
    made = sorted(_hold_more(far, 5))
    state: dict[str, Any] = {}
    pages = _all_pages(state, far_name, limit_holds=1)
    assert pages == [[transfer_id] for transfer_id in made]
    assert listings == {"outgoing": 1, "held_names": 1}  # K = 5 pages, one listing
    assert state["frozen_holds"] == {}  # exhausted: freed


def test_a_hold_made_after_the_freeze_is_not_answered_and_a_new_freeze_sees_it(
    tmp_path: Path, listings: collections.Counter[str]
) -> None:
    far, far_name = registered_now(tmp_path, "far")
    made = sorted(_hold_more(far, 4))
    state: dict[str, Any] = {}
    first = _page(state, far_name, None, limit_holds=1)
    assert [hold["transfer_id"] for hold in first["holds"]] == made[:1] and first["next"] == made[0]
    # A hold made after the freeze, after the cursor: a fresh listing would answer it.
    later = next(transfer_id for transfer_id in iter(lambda: _hold_more(far, 1)[0], None) if transfer_id > made[0])
    rest = []
    after = first["next"]
    while after is not None:
        page = _page(state, far_name, after, limit_holds=1)
        rest += [hold["transfer_id"] for hold in page["holds"]]
        after = page["next"]
    assert rest == made[1:] and later not in rest
    assert listings["held_names"] == 1
    # Exhausted, so freed: a new enumeration from null takes a new freeze, which sees it.
    assert later in [transfer_id for page in _all_pages(state, far_name) for transfer_id in page]
    assert listings["held_names"] == 2


def test_after_null_starts_a_new_enumeration_even_before_one_is_exhausted(
    tmp_path: Path, listings: collections.Counter[str]
) -> None:
    far, far_name = registered_now(tmp_path, "far")
    made = sorted(_hold_more(far, 3))
    state: dict[str, Any] = {}
    first = _page(state, far_name, None, limit_holds=1)
    assert first["next"] == made[0]  # not exhausted: the freeze is kept
    later = _hold_more(far, 1)[0]
    again = [transfer_id for page in _all_pages(state, far_name) for transfer_id in page]
    assert again == sorted([*made, later]) and listings["held_names"] == 2


def test_a_frozen_name_that_vanished_is_passed_over(tmp_path: Path) -> None:
    far, far_name = registered_now(tmp_path, "far")
    made = sorted(_hold_more(far, 4))
    state: dict[str, Any] = {}
    first = _page(state, far_name, None, limit_holds=1)
    shutil.rmtree(outgoing_dir(far) / made[2])  # gone since the freeze (released, say)
    page = _page(state, far_name, first["next"])
    assert [hold["transfer_id"] for hold in page["holds"]] == [made[1], made[3]] and page["next"] is None


def test_after_is_a_lower_bound(tmp_path: Path) -> None:
    far, far_name = registered_now(tmp_path, "far")
    made = sorted(_hold_more(far, 4))

    def answered(after: str | None) -> list[str]:
        return [hold["transfer_id"] for hold in _page({}, far_name, after)["holds"]]

    between = f"{int(made[1], 16) + 1:032x}"
    assert between not in made and between < made[2]
    assert answered(between) == made[2:]  # not in the list: the next greater entry
    assert answered(made[1]) == made[2:]  # in the list: the one after it
    assert answered("0" * 32) == made and answered(None) == made
    assert answered("f" * 32) == []
    # A cursor before the first entry, and the first entry itself.
    assert answered(f"{int(made[0], 16) - 1:032x}") == made and answered(made[0]) == made[1:]


def test_holds_page_cursor_skips_holds_bound_elsewhere_by_the_frozen_names(tmp_path: Path) -> None:
    far, far_name = registered_now(tmp_path, "far")
    mine = _hold_more(far, 3, "here:ws")
    _hold_more(far, 3, "elsewhere:ws")
    state: dict[str, Any] = {}
    seen = [
        transfer_id
        for page in _all_pages(state, far_name, destination="here:ws", limit_holds=1)
        for transfer_id in page
    ]
    assert seen == sorted(mine) and state["frozen_holds"] == {}


# -- workflows resolved once per root per page --------------------------------------------------------------------

OTHER = ("other--0123456789abcdef", "other")


@pytest.fixture
def resolutions(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """The roots of every ``resolve_calls`` this process makes."""

    seen: list[list[str]] = []
    real = _store.resolve_calls

    def resolve_calls(workspace: Any, workflow_ids: Any, **options: Any) -> Any:
        ids = list(workflow_ids)
        seen.append(ids)
        return real(workspace, ids, **options)

    monkeypatch.setattr(_store, "resolve_calls", resolve_calls)
    return seen


def _roots(resolutions: list[list[str]]) -> list[tuple[str, ...]]:
    """The resolutions of the jobs' workflow roots (an adoption resolves its bound workflows too: none here)."""

    return sorted(tuple(ids) for ids in resolutions if ids)


def _per_page(workspace: Workspace, size: int) -> list[tuple[str, ...]]:
    """The roots a page resolves once each, the holds read *size* at a time in transfer id order (a page may hold
    jobs of one workflow only)."""

    names = _moving.held_names(workspace)
    expected = []
    for start in range(0, len(names), size):
        roots: set[str] = set()
        for name in names[start : start + size]:
            hold = _moving.held_one(workspace, name, workflows=True)
            assert hold is not None and hold.workflows is not None
            roots.update(workflow_id for workflow_id, _, _ in hold.workflows)
        expected += [(root,) for root in roots]
    return sorted(expected)


def _two_workflows(workspace: Workspace, count: int) -> list[dict[str, Any]]:
    mappings = []
    for index in range(count):
        mapping = job_mapping(WORKFLOW if index % 2 else OTHER, {"start": "succeed"}, tag="solo", placement="p/q")
        place(workspace, mapping, "succeeded", 10 + index)
        mappings.append(mapping)
    return mappings


def test_a_page_of_new_holds_resolves_each_root_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resolutions: list[list[str]]
) -> None:
    source, source_name = registered_now(tmp_path, "a")
    target, target_name = registered_now(tmp_path, "b")
    source.configure_settings({"io.workers": 1})
    mappings = _two_workflows(source, 6)
    paged(monkeypatch, 3)  # two pages, each with both roots
    argv = [item for mapping in mappings for item in ("--job", str(mapping["id"]))]
    assert run(tmp_path, "--json", "-q", *argv, source_name, target_name) == 0
    assert all(find(target, mapping["id"]) is not None for mapping in mappings)
    assert _roots(resolutions) == sorted([(WORKFLOW[0],), (OTHER[0],)] * 2)


def test_earlier_holds_resolve_each_root_once_a_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, resolutions: list[list[str]]
) -> None:
    source, source_name = registered_now(tmp_path, "a")
    target, target_name = registered_now(tmp_path, "b")
    mappings = _two_workflows(source, 6)
    from httk.core.cli import CLIContext

    from httk.workflow.workflow_cli import command

    argv = [item for mapping in mappings for item in ("--job", str(mapping["id"]))]
    assert (
        command(
            ["job", "transfer", "--hold", "--no-durable", *argv, source_name, target_name], CLIContext("httk", tmp_path)
        )
        == 0
    )
    expected = _per_page(source, 3)
    del resolutions[:]
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 3)
    assert run(tmp_path, "--json", "-q", "--resume", source_name) == 0
    assert all(find(target, mapping["id"]) is not None for mapping in mappings)
    assert _roots(resolutions) == expected and len(expected) < 6


def test_holds_page_resolves_each_root_once_a_page(tmp_path: Path, resolutions: list[list[str]]) -> None:
    far, far_name = registered_now(tmp_path, "far")
    mappings = _two_workflows(far, 6)
    with cli_owner(far) as owner:
        for mapping in mappings:
            ref = find(far, mapping["id"])
            assert ref is not None
            claimed = _kernel.claim(far, owner, ref)
            assert claimed is not None
            _moving.hold(far, owner, claimed, tree=True, destination_locator="x:y")
    expected = _per_page(far, 3)
    del resolutions[:]
    pages = _all_pages({}, far_name, limit_holds=3)
    assert [len(page) for page in pages] == [3, 3]
    assert _roots(resolutions) == expected and len(expected) < 6


# -- listing progress on the far side --------------------------------------------------------------------------------

#: :data:`test_session_push.PATCHED_HTTK`, with every listing of a directory below ``jobs/`` slowed down by the
#: seconds of ``HTTK_TEST_SLOW_LISTING``.
SLOW_LISTINGS = """
if argv[:2] == ["workflow", "transfer-session"] and os.environ.get("HTTK_TEST_SLOW_LISTING"):
    from httk.workflow import _kernel as slow_kernel

    real_list_names = slow_kernel.list_names

    def slow_list_names(directory):
        if "jobs" in Path(directory).parts:
            time.sleep(float(os.environ["HTTK_TEST_SLOW_LISTING"]))
        return real_list_names(directory)

    slow_kernel.list_names = slow_list_names
"""
PATCHED_SLOW = PATCHED_HTTK.replace(
    'sys.argv = ["httk", *argv]\n', "from pathlib import Path\n" + SLOW_LISTINGS + 'sys.argv = ["httk", *argv]\n'
)
assert PATCHED_SLOW != PATCHED_HTTK


def test_slow_listings_of_adopt_staged_report_progress_within_a_short_stall_bound(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each listing below jobs/ takes 0.3 seconds; a bundle's presence lookup lists every unowned state's placement,
    # several seconds without a bundle adopted, against a stall bound of 1 second.
    _, bundle = local_project(tmp_path)
    _patched(tmp_path, remote, PATCHED_SLOW)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_SLOW_LISTING", "0.3")
    near, _ = registered_now(tmp_path, "near")
    far, far_name = registered_now(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    ids = [hold.transfer_id for hold in holds]
    declared = sum(transfer_cli._measure(Path(hold.path))[0] for hold in holds)
    session = TransferSession(bundle, needs={"stage", "adopt_staged"}, timeout=60)
    session.open()
    session._limit = 1.0
    with session:
        staged = session.request(
            "stage",
            {"workspace": far_name, "transfer_ids": ids, "bytes": declared},
            upload=writer(stream(outgoing_dir(near), ids)),
        )
        assert staged["ok"] is True, staged
        answer = session.request("adopt_staged", {"workspace": far_name, "token": staged["token"], "transfer_ids": ids})
    assert answer["ok"] is True, answer
    assert [entry["status"] for entry in answer["document"]["bundles"]] == ["adopted"] * 2
    assert_in(far, plan)


def test_listing_progress_tells_once_a_listed_directory_in_this_context_and_its_threads(tmp_path: Path) -> None:
    told: list[int] = []
    (tmp_path / "a").mkdir()
    _kernel.list_names(tmp_path)  # no listener: nothing told
    with _kernel.listing_progress(lambda: told.append(1)):
        _kernel.list_names(tmp_path)
        _kernel.list_names(tmp_path / "missing")
        _pool.run_ordered(lambda _index: _kernel.list_names(tmp_path), [0, 1], 2)  # in worker threads
    _kernel.list_names(tmp_path)
    assert len(told) == 4


# -- outcomes do not wait behind an idle pair ------------------------------------------------------------------------


def _waiting_peak(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, busy: int) -> tuple[int, bool]:
    """``--resume`` of one hold for B, first in transfer id order, then *busy* holds for C, a page each, with a
    positive deadline: the peak number of batches waiting in the report's queue, and whether B's one job was adopted
    before the transfer's end (:meth:`_Transfer.finish`)."""

    registered = at_once(registered_now, 0.3)
    source, source_name = registered(tmp_path / str(busy), "a")
    b, b_name = registered(tmp_path / str(busy), "b")
    _c, c_name = registered(tmp_path / str(busy), "c")
    idle = jobs(source, 1)
    with cli_owner(source) as owner:
        monkeypatch.setattr(_bundles, "uuid", type("U", (), {"uuid4": staticmethod(lambda: uuid.UUID(int=1))}))
        ref = find(source, idle[0][0]["id"])
        assert ref is not None
        claimed = _kernel.claim(source, owner, ref)
        assert claimed is not None
        first = _moving.hold(source, owner, claimed, tree=True, destination_locator=b_name)
        monkeypatch.setattr(_bundles, "uuid", uuid)
    _hold_more(source, busy, c_name)
    assert _moving.held_names(source)[0] == first.transfer_id  # B's page is the first
    paged(monkeypatch, 1)
    peak = [0]
    in_b: list[bool] = []
    real_drain, real_finish = transfer_cli._Spool._drain, transfer_cli._Transfer.finish

    def drain(spool: Any, *, force: bool = False) -> None:
        peak[0] = max(peak[0], len(spool._waiting))
        real_drain(spool, force=force)

    def finish(run_: Any, *arguments: Any, **options: Any) -> None:
        in_b.append(find(b, idle[0][0]["id"]) is not None)
        real_finish(run_, *arguments, **options)

    monkeypatch.setattr(transfer_cli._Spool, "_drain", drain)
    monkeypatch.setattr(transfer_cli._Transfer, "finish", finish)
    assert run(tmp_path, "--json", "-q", "--resume", source_name) == 0
    assert _moving.held_names(source) == []
    return peak[0], in_b == [True]


def test_outcomes_do_not_wait_in_memory_behind_an_idle_pair_s_staged_segment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    small, adopted_small = _waiting_peak(tmp_path, monkeypatch, 15)
    large, adopted_large = _waiting_peak(tmp_path, monkeypatch, 45)
    capsys.readouterr()
    assert adopted_small and adopted_large  # B's stage concluded while C's pages went on
    # Bounded by C's own pipeline (8 staged segments) and a deadline's worth of pages, whatever C's page count.
    assert small <= 12 and large <= 12, (small, large)


# -- a far side's missing_workflows is checked at the adoption ---------------------------------------------------------


@pytest.mark.parametrize("missing", ["w--1", [1], [None], {"w--1": True}, None])
def test_an_adoption_entry_s_missing_workflows_must_be_a_list_of_ids(missing: Any) -> None:
    bundle = "a" * 32
    entry = {"bundle": bundle, "status": "adopted", "published": [], "missing_workflows": missing}
    document = {"bundles": [entry], "workspace_id": str(uuid.uuid4())}
    with pytest.raises(ValueError, match="missing_workflows"):
        transfer_cli._checked_adoption(document, [bundle])


@pytest.mark.parametrize("missing", [[], ["w--1", "w--2"], "absent"])
def test_an_adoption_entry_s_missing_workflows_as_a_list_of_ids_or_absent_is_accepted(missing: Any) -> None:
    bundle = "a" * 32
    entry: dict[str, Any] = {"bundle": bundle, "status": "adopted", "published": []}
    if missing != "absent":
        entry["missing_workflows"] = missing
    document = {"bundles": [entry], "workspace_id": str(uuid.uuid4())}
    assert transfer_cli._checked_adoption(document, [bundle])["entries"] == {bundle: entry}
