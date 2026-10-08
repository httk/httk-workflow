"""Allocation identities, and asking a scheduler or a site probe whether a recorded allocation has ended.

Real Slurm is not available here: ``squeue`` is a fake command runner that returns what ``squeue`` prints.
"""

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _allocation
from httk.workflow._allocation import (
    Allocation,
    RecordedAllocation,
    allocation_ended,
    allocation_from_envelope,
    exec_allocation_ended,
    validate_identity,
)
from httk.workflow._slurm import SLURM, slurm_allocation, slurm_allocation_ended, slurm_identity
from httk.workflow.errors import FormatError

_ENVELOPE: dict[str, object] = {
    "format": "httk-workflow-allocation",
    "format_version": 1,
    "kind": "pbs",
    "nodes": [{"host": "n001", "procs": 4}],
}


class _Squeue:
    """A fake command runner answering one ``squeue`` call."""

    def __init__(
        self, stdout: str = "", returncode: int = 0, stderr: str = "", raises: BaseException | None = None
    ) -> None:
        self.stdout, self.returncode, self.stderr, self.raises = stdout, returncode, stderr, raises
        self.calls: list[list[str]] = []
        self.environments: list[dict[str, str]] = []

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(argv)
        self.environments.append(kwargs["env"])
        assert kwargs["timeout"] == 7.0 and kwargs["check"] is False and kwargs["text"] is True
        assert "shell" not in kwargs
        if self.raises is not None:
            raise self.raises
        return subprocess.CompletedProcess(argv, self.returncode, self.stdout, self.stderr)


def _ask(squeue: _Squeue, identity: dict[str, str] | None = None) -> bool | None:
    return slurm_allocation_ended(identity or {"job_id": "123"}, run=squeue, timeout=7.0)


# -- identities -----------------------------------------------------------------------------------------


def test_the_slurm_probe_records_the_job_and_its_cluster() -> None:
    environ = {"SLURM_JOB_ID": "123", "SLURM_CLUSTER_NAME": "tetralith", "SLURM_JOB_NUM_NODES": "1"}
    assert slurm_identity(environ) == {"job_id": "123", "cluster": "tetralith"}
    assert slurm_identity({"SLURM_JOB_ID": "123"}) == {"job_id": "123"}
    allocation = slurm_allocation(environ, run=_Squeue())
    assert allocation is not None and allocation.identity == {"job_id": "123", "cluster": "tetralith"}
    # A malformed value names no allocation anyone could ask about.
    assert slurm_identity({"SLURM_JOB_ID": "123_4"}) is None
    assert slurm_identity({"SLURM_JOB_ID": "123", "SLURM_CLUSTER_NAME": "a,b"}) is None
    assert slurm_identity({}) is None


def test_an_envelope_may_carry_a_bounded_identity() -> None:
    allocation = allocation_from_envelope({**_ENVELOPE, "identity": {"job_id": "4711.pbs01", "server": "pbs01"}})
    assert allocation.identity == {"job_id": "4711.pbs01", "server": "pbs01"}
    assert allocation_from_envelope(_ENVELOPE).identity is None
    assert allocation_from_envelope({**_ENVELOPE, "identity": None}).identity is None
    assert validate_identity({f"k{index}": "v" * 256 for index in range(16)}, "identity")


@pytest.mark.parametrize(
    "identity",
    [
        [],
        "4711",
        {f"k{index}": "v" for index in range(17)},
        {"Job": "1"},
        {"job--id": "1"},
        {"job_id": 1},
        {"job_id": None},
        {"job_id": "v" * 257},
        {"job_id": "é" * 129},
    ],
)
def test_envelope_identities_are_strict(identity: object) -> None:
    with pytest.raises(FormatError):
        allocation_from_envelope({**_ENVELOPE, "identity": identity})


def test_a_recorded_allocation_round_trips_and_is_read_leniently() -> None:
    allocation = Allocation("slurm", 5000.0, identity={"job_id": "9"}, probe="slurm")
    recorded = RecordedAllocation.from_allocation(allocation)
    valid = recorded.as_json()
    assert valid == {"probe": "slurm", "kind": "slurm", "identity": {"job_id": "9"}, "end_time": 5000.0}
    assert RecordedAllocation.from_record(json.loads(json.dumps(valid))) == recorded
    # An allocation that was not probed names its kind as the probe.
    assert RecordedAllocation.from_allocation(Allocation("host", None)).probe == "host"
    # Unknown members are ignored; an unusable identity or end time is dropped, which only proves less.
    assert RecordedAllocation.from_record({**valid, "extra": 1}) == recorded
    assert RecordedAllocation.from_record({**valid, "identity": {"job_id": 9}}) == RecordedAllocation(
        "slurm", "slurm", None, 5000.0
    )
    for end_time in (0, True, "soon"):
        assert RecordedAllocation.from_record({**valid, "end_time": end_time}) == RecordedAllocation(
            "slurm", "slurm", {"job_id": "9"}, None
        )
    # Without a usable probe and kind there is no allocation at all.
    unusable_members: tuple[object, ...] = (
        None,
        [],
        {**valid, "probe": ""},
        {**valid, "probe": "x" * 4097},
        {**valid, "kind": "Not A Label"},
    )
    for unusable in unusable_members:
        assert RecordedAllocation.from_record(unusable) is None


# -- Slurm ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state",
    ["COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY"],
)
def test_squeue_confirms_a_job_in_a_terminal_state(state: str) -> None:
    squeue = _Squeue(f"123|{state}\n")
    assert _ask(squeue) is True
    assert squeue.calls == [["squeue", "--noheader", "--jobs=123", "--states=all", "--format=%i|%T"]]


def test_squeue_runs_without_the_variables_that_filter_its_output(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("SQUEUE_STATES", "SQUEUE_PARTITION", "SQUEUE_USERS", "SQUEUE_FORMAT", "SLURM_CLUSTERS"):
        monkeypatch.setenv(name, "x")
    monkeypatch.setenv("SLURM_CONF", "/etc/slurm/slurm.conf")
    squeue = _Squeue("")
    assert _ask(squeue) is True
    (environment,) = squeue.environments
    assert not [name for name in environment if name.startswith("SQUEUE_") or name == "SLURM_CLUSTERS"]
    assert environment["SLURM_CONF"] == "/etc/slurm/slurm.conf"
    assert "--states=all" in squeue.calls[0]


@pytest.mark.parametrize("state", ["COMPLETING", "RUNNING", "PENDING", "SUSPENDED", "REQUEUED", "STOPPED"])
def test_a_job_that_may_still_run_processes_has_not_ended(state: str) -> None:
    assert _ask(_Squeue(f"123|{state}\n")) is False


def test_a_job_squeue_no_longer_knows_has_ended() -> None:
    assert _ask(_Squeue("")) is True
    assert _ask(_Squeue("\n  \n")) is True
    assert _ask(_Squeue(returncode=1, stderr="slurm_load_jobs error: Invalid job id specified\n")) is True


@pytest.mark.parametrize(
    "squeue",
    [
        _Squeue(returncode=1, stderr="slurm_load_jobs error: Unable to contact slurm controller\n"),
        _Squeue(returncode=1),
        _Squeue(raises=subprocess.TimeoutExpired(["squeue"], 7.0)),
        _Squeue(raises=FileNotFoundError("squeue")),
        _Squeue("123\n"),
        _Squeue("123|COMPLETED|extra\n"),
        _Squeue("123|completed\n"),
        _Squeue("garbage\n"),
        # Rows that do not include the job itself confirm nothing.
        _Squeue("124|COMPLETED\n"),
        _Squeue("123_4|COMPLETED\n"),
    ],
)
def test_anything_else_cannot_tell(squeue: _Squeue) -> None:
    assert _ask(squeue) is None


def test_another_cluster_is_queried_explicitly_and_its_heading_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLURM_CLUSTER_NAME", raising=False)
    squeue = _Squeue("CLUSTER: tetralith\n123|COMPLETED\n")
    assert _ask(squeue, {"job_id": "123", "cluster": "tetralith"}) is True
    assert squeue.calls == [
        ["squeue", "--noheader", "--jobs=123", "--clusters=tetralith", "--states=all", "--format=%i|%T"]
    ]
    assert _ask(_Squeue("CLUSTER: tetralith\n"), {"job_id": "123", "cluster": "tetralith"}) is True
    assert _ask(_Squeue("CLUSTER: tetralith\n123|RUNNING\n"), {"job_id": "123", "cluster": "tetralith"}) is False
    # Another cluster's heading is not this job's answer.
    assert _ask(_Squeue("CLUSTER: other\n123|COMPLETED\n"), {"job_id": "123", "cluster": "tetralith"}) is None
    # This process's own cluster needs no --clusters, which needs slurmdbd.
    monkeypatch.setenv("SLURM_CLUSTER_NAME", "tetralith")
    squeue = _Squeue("123|COMPLETED\n")
    assert _ask(squeue, {"job_id": "123", "cluster": "tetralith"}) is True
    assert squeue.calls == [["squeue", "--noheader", "--jobs=123", "--states=all", "--format=%i|%T"]]
    squeue = _Squeue("CLUSTER: other\n123|RUNNING\n")
    assert _ask(squeue, {"job_id": "123", "cluster": "other"}) is False
    assert "--clusters=other" in squeue.calls[0]


@pytest.mark.parametrize(
    "identity",
    [
        {"job_id": "abc"},
        {"job_id": "123; rm -rf /"},
        {"cluster": "x"},
        {"job_id": "1", "cluster": "a b"},
        {"job_id": "1", "x": "y"},
    ],
)
def test_an_identity_squeue_cannot_answer_is_never_asked(identity: dict[str, str]) -> None:
    squeue = _Squeue("")
    assert _ask(squeue, identity) is None
    assert squeue.calls == []


# -- site probes ----------------------------------------------------------------------------------------


def _probe(tmp_path: Path, body: str) -> Path:
    probe = tmp_path / "allocation"
    probe.write_text(f'#!/bin/sh\ncat > "$(dirname "$0")/query.json"\necho "$@" > "$(dirname "$0")/argv"\n{body}\n')
    probe.chmod(0o755)
    return probe


_RECORDED = RecordedAllocation("exec:/site/allocation", "pbs", {"job_id": "4711.pbs01"}, 1790000000.0)


@pytest.mark.parametrize("ended", [True, False])
def test_a_site_probe_answers_the_ended_query(tmp_path: Path, ended: bool) -> None:
    answer = {"format": "httk-workflow-allocation-status", "format_version": 1, "ended": ended}
    probe = _probe(tmp_path, f"echo '{json.dumps(answer)}'")
    assert exec_allocation_ended(str(probe), _RECORDED, run=subprocess.run, timeout=30.0) is ended
    assert (tmp_path / "argv").read_text() == "ended\n"
    assert json.loads((tmp_path / "query.json").read_text()) == {
        "format": "httk-workflow-allocation-query",
        "format_version": 1,
        "kind": "pbs",
        "identity": {"job_id": "4711.pbs01"},
        "end_time": 1790000000.0,
    }


@pytest.mark.parametrize(
    "body",
    [
        # An older probe ignores its argument and prints its allocation envelope.
        f"echo '{json.dumps(_ENVELOPE)}'",
        'echo \'{"format": "httk-workflow-allocation-status", "format_version": 1, "ended": true}\'; exit 1',
        'echo \'{"format": "httk-workflow-allocation-status", "format_version": 1, "ended": 1}\'',
        'echo \'{"format": "httk-workflow-allocation-status", "format_version": true, "ended": true}\'',
        'echo \'{"format": "httk-workflow-allocation-status", "format_version": 1, "ended": true, "x": 1}\'',
        "echo not json",
        "exit 3",
    ],
)
def test_any_other_site_probe_answer_cannot_tell(tmp_path: Path, body: str) -> None:
    assert exec_allocation_ended(str(_probe(tmp_path, body)), _RECORDED, run=subprocess.run, timeout=30.0) is None


def test_a_site_probe_that_hangs_or_is_missing_cannot_tell(tmp_path: Path) -> None:
    assert exec_allocation_ended(str(_probe(tmp_path, "sleep 30")), _RECORDED, run=subprocess.run, timeout=0.5) is None
    assert exec_allocation_ended(str(tmp_path / "missing"), _RECORDED, run=subprocess.run, timeout=30.0) is None


# -- dispatch -------------------------------------------------------------------------------------------


def test_the_recorded_probe_decides_who_is_asked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[tuple[str, object]] = []

    def slurm(identity: dict[str, str], *, run: object, timeout: float) -> bool:
        asked.append(("slurm", identity))
        return True

    def site(path: str, recorded: RecordedAllocation, *, run: object, timeout: float) -> bool:
        asked.append(("exec", path))
        return False

    monkeypatch.setattr(SLURM, "allocation_ended", slurm)
    monkeypatch.setattr(_allocation, "exec_allocation_ended", site)
    identity = {"job_id": "5"}
    assert allocation_ended(RecordedAllocation("slurm", "slurm", identity, None), timeout=1.0) is True
    assert allocation_ended(RecordedAllocation("auto", "slurm", identity, None), timeout=1.0) is True
    assert allocation_ended(RecordedAllocation("exec:/site/probe", "slurm", identity, None), timeout=1.0) is False
    assert asked == [("slurm", identity), ("slurm", identity), ("exec", "/site/probe")]
    asked.clear()
    # Nothing can tell for a host or unknown probe, or an allocation without an identity.
    for recorded in (
        RecordedAllocation("host", "host", None, None),
        RecordedAllocation("none", "none", None, None),
        RecordedAllocation("pbs", "pbs", identity, None),
        RecordedAllocation("slurm", "slurm", None, 5000.0),
        RecordedAllocation("exec:/site/probe", "pbs", None, 5000.0),
    ):
        assert not recorded.queryable
        assert allocation_ended(recorded, timeout=1.0) is None
    assert asked == []
