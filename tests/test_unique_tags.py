"""Job creation refuses, skips or allows a tag already used in the same placement (``duplicate_tag``)."""

import logging
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path, PurePosixPath

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import DuplicateTagError, Workspace, _kernel
from httk.workflow.scaffold import new_job, new_jobs, scaffold_job
from httk.workflow.workflow_cli import command
from test_input_moves import _FRAMES, _origin, _unregister  # noqa: F401 - _unregister is an autouse fixture
from test_input_moves import _WORKFLOW as _MOVES
from test_input_moves import _workspace as _moves_workspace
from test_job_creation import assert_nothing_left, install, raw_package, ready, workspace_at
from test_workflow_cli_packages import _POSCAR, _cli_package

_RAW = "tests.raw"


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    workspace = workspace_at(tmp_path / "ws")
    install(workspace, raw_package(tmp_path / "package"))
    return workspace


def _in_state(ws: Workspace, state: str, tag: str) -> _kernel.JobRef:
    """Publish a job of the root placement straight into *state*, the way the kernel moves jobs between states."""

    with _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={}) as owner:
        staging = owner.scratch("submit") / "job"
        staging.mkdir()
        scaffold_job(ws, _RAW, staging, tag=tag)
        return _kernel.submit(ws, owner, staging, state=state)


def test_a_second_job_with_the_same_tag_and_placement_is_refused(ws: Workspace) -> None:
    first = new_job(ws, _RAW, tag="si", placement="p")
    with pytest.raises(DuplicateTagError) as refused:
        new_job(ws, _RAW, tag="si", placement="p")
    assert refused.value.tag == "si" and refused.value.placement == PurePosixPath("p")
    assert refused.value.existing.job_key == first.job_key and refused.value.existing.state == "ready"
    assert f"{first.job_key} (ready)" in str(refused.value) and "duplicate_tag='allow'" in str(refused.value)
    assert [ref.job_key for ref in ready(ws)] == [first.job_key]
    assert_nothing_left(ws)


def test_the_check_is_per_exact_placement(ws: Workspace) -> None:
    for placement in ("", "p", "p/q"):
        new_job(ws, _RAW, tag="si", placement=placement)
    assert len(ready(ws)) == 3
    with pytest.raises(DuplicateTagError):
        new_job(ws, _RAW, tag="si", placement="p/q")
    with pytest.raises(ValueError, match="duplicate_tag must be"):
        new_job(ws, _RAW, tag="si", duplicate_tag="ignore")  # type: ignore[call-overload]


def test_skip_returns_none_with_a_warning_and_allow_creates_another(
    ws: Workspace, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.scaffold")
    first = new_job(ws, _RAW, tag="si")
    assert new_job(ws, _RAW, tag="si", duplicate_tag="skip") is None
    assert f"skipped tag si: already {first.job_key} (ready)" in caplog.text
    jobs = list(new_jobs(ws, _RAW, [{"tag": "si"}, {"tag": "ge"}, {"tag": "ge"}], duplicate_tag="skip"))
    assert [job.tag for job in jobs] == ["ge"]
    assert len(ready(ws)) == 2
    second = new_job(ws, _RAW, tag="si", duplicate_tag="allow")
    assert second.job_key != first.job_key and len(ready(ws)) == 3
    assert_nothing_left(ws)


def test_every_unowned_state_and_owned_jobs_are_seen(ws: Workspace) -> None:
    for state in _kernel.UNOWNED_STATES:
        existing = _in_state(ws, state, f"in-{state}")
        with pytest.raises(DuplicateTagError) as refused:
            new_job(ws, _RAW, tag=f"in-{state}")
        assert refused.value.existing.state == state and refused.value.existing.job_key == existing.job_key

    job = new_job(ws, _RAW, tag="held", placement="p")
    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    held = _kernel.claim(ws, owner, job.ref)
    assert held is not None
    try:
        with pytest.raises(DuplicateTagError) as refused:
            new_job(ws, _RAW, tag="held", placement="p")
        assert refused.value.existing.state == _kernel.OWNED and refused.value.existing.job_key == job.job_key
        # An owned job's placement comes from its job.json, so another placement is free.
        assert new_job(ws, _RAW, tag="held", placement="q").tag == "held"
    finally:
        held.give_back()
        owner.close()


def test_a_campaign_refuses_its_own_duplicates_and_lists_each_directory_once(
    ws: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    listed: Counter[Path] = Counter()
    list_names = _kernel.list_names

    def counting(directory: Path) -> list[str]:
        listed[directory] += 1
        return list_names(directory)

    monkeypatch.setattr(_kernel, "list_names", counting)
    items = [{"tag": f"t{n}", "placement": f"p{n % 2}"} for n in range(6)] + [{"tag": "t2", "placement": "p0"}]
    jobs = new_jobs(ws, _RAW, items)  # type: ignore[arg-type]
    assert [job.tag for job in (next(jobs) for _ in range(6))] == [f"t{n}" for n in range(6)]
    with pytest.raises(DuplicateTagError) as refused:
        next(jobs)
    assert refused.value.existing.job_key.startswith("t2--") and refused.value.existing.state == "ready"
    states = {ws.jobs / state / f"p{n}" for state in _kernel.UNOWNED_STATES for n in range(2)}
    assert {directory: listed[directory] for directory in states} == dict.fromkeys(states, 1)
    assert listed[ws.jobs / _kernel.OWNED] == 1  # a free tag is looked up among the owned jobs too
    assert len(ready(ws)) == 6


def test_tagless_jobs_are_never_checked(ws: Workspace, monkeypatch: pytest.MonkeyPatch) -> None:
    listed: list[Path] = []
    list_names = _kernel.list_names

    def recording(directory: Path) -> list[str]:
        listed.append(directory)
        return list_names(directory)

    monkeypatch.setattr(_kernel, "list_names", recording)
    first, second = new_job(ws, _RAW), new_job(ws, _RAW)
    assert first.tag is None and second.tag is None and len(ready(ws)) == 2
    assert not {ws.jobs / state for state in (*_kernel.UNOWNED_STATES, _kernel.OWNED)} & set(listed)


def test_a_skipped_job_moves_its_moved_input_back(tmp_path: Path) -> None:
    ws = _moves_workspace(tmp_path)
    source = _origin(ws) / "run" / "traj.dat"
    script = {"script": {"start": "succeed"}}
    new_job(ws, _MOVES, parameters=script, tag="dup")
    skipped = new_job(
        ws, _MOVES, parameters=script, tag="dup", move_inputs={"trajectory": source}, duplicate_tag="skip"
    )
    assert skipped is None and source.read_bytes() == _FRAMES
    assert_nothing_left(ws)


def test_the_cli_refuses_skips_or_allows_a_duplicate_tag(
    ws: Workspace, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "tags")
    argv = ["job", "new", "--workspace", name, "--workflow", _RAW, "--tag", "x"]
    assert command(argv, context) == 0
    first = capsys.readouterr().out.split("\t")[0]
    assert command(argv, context) == 2
    assert capsys.readouterr().err == (
        f"httk: a job with tag 'x' already exists in placement '.': {first} (ready); "
        "pass --duplicate-tag allow to create another or --duplicate-tag skip to skip it\n"
    )

    # A real process: the library warning reaches stderr, nothing reaches stdout.
    skipped = subprocess.run(
        [sys.executable, "-m", "httk.core.cli", *argv, "--duplicate-tag", "skip"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        check=False,
    )
    assert (skipped.returncode, skipped.stdout) == (0, "")
    assert f"skipped tag x: already {first} (ready)" in skipped.stderr

    assert command([*argv, "--duplicate-tag", "allow"], context) == 0
    assert len(ready(ws)) == 2


def test_a_cli_batch_counts_skips_and_reports_a_refusal(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("httk.atomistic")
    context = CLIContext("httk", tmp_path)
    workspace_at(tmp_path / "ws")
    name = register_ws(context, tmp_path / "ws", "batch")
    structures = tmp_path / "structures"
    structures.mkdir()
    for file in ("one.vasp", "two.vasp"):
        (structures / file).write_text(_POSCAR, encoding="utf-8")
    package = _cli_package(tmp_path / "package")
    argv = ["job", "new", "--workspace", name, "--install", "--workflow-dir", str(package)]
    argv += ["--input-from", "structure", str(structures)]
    assert command(argv, context) == 0
    capsys.readouterr()
    assert command([*argv, "--duplicate-tag", "skip"], context) == 0
    captured = capsys.readouterr()
    assert captured.out == "" and "submitted 0 jobs (2 skipped)" in captured.err
    assert command(argv, context) == 2
    err = capsys.readouterr().err
    assert "submitted 0 of 2 jobs before failing" in err and "pass --duplicate-tag allow" in err


def test_fsck_reports_a_tag_shared_in_one_placement(ws: Workspace) -> None:
    first = new_job(ws, _RAW, tag="si", placement="p")
    second = new_job(ws, _RAW, tag="si", placement="p", duplicate_tag="allow")
    new_job(ws, _RAW, tag="si", placement="q")
    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    held = _kernel.claim(ws, owner, second.ref)
    assert held is not None
    try:
        report = ws.check()
    finally:
        held.give_back()
        owner.close()
    (finding,) = report.findings
    assert (finding.problem, finding.action) == ("duplicate_tag", "noted")
    assert finding.detail == (
        f"tag 'si' is shared by 2 jobs in placement 'p': {first.job_key} (ready), {second.job_key} (owned)"
    )
    assert report.unresolved == 0


def test_fsck_exits_zero_when_its_only_finding_is_a_duplicate_tag(
    ws: Workspace, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    new_job(ws, _RAW, tag="si")
    new_job(ws, _RAW, tag="si", duplicate_tag="allow")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "fsck")
    assert command(["workspace", "fsck", name], context) == 0
    assert "noted\tduplicate_tag\t" in capsys.readouterr().out
