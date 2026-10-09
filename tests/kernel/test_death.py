"""Unit tests of :mod:`httk.workflow._death`: every rung of the death-proof ladder, with fake schedulers."""

import json
import os
import socket
import subprocess
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import _death
from httk.workflow._allocation import RecordedAllocation
from httk.workflow._death import (
    Evidence,
    Liveness,
    ProcessIdentity,
    SchedulerQueries,
    probe,
    process_gone,
    process_identity,
)

OWNER_ID = "manager-1"


class FakeScheduler(SchedulerQueries):
    """Answers by the Slurm job id of an allocation identity, and records what it was asked."""

    def __init__(self, answers: dict[str, bool | None] | None = None) -> None:
        super().__init__()
        self.answers = answers or {}
        self.asked: list[str] = []

    def ended(self, recorded: RecordedAllocation) -> bool | None:
        job = (recorded.identity or {}).get("job_id", "")
        self.asked.append(job)
        return self.answers.get(job)


def _allocation(job: str) -> dict[str, object]:
    return {"probe": "slurm", "kind": "slurm", "identity": {"job_id": job}, "end_time": None}


@pytest.fixture
def here() -> ProcessIdentity:
    return process_identity()


@pytest.fixture
def dead_process() -> Iterator[ProcessIdentity]:
    """A process group leader that has exited and been reaped."""

    child = subprocess.Popen(["sleep", "30"], start_new_session=True)
    identity = process_identity(child.pid)
    child.kill()
    child.wait()
    yield identity


@pytest.fixture
def owner_dir(tmp_path: Path) -> Path:
    path = tmp_path / "owners" / OWNER_ID
    path.mkdir(parents=True)
    return path


def _identity_json(identity: ProcessIdentity) -> dict[str, object]:
    return {
        "hostname": identity.hostname,
        "boot_id": identity.boot_id,
        "pid": identity.pid,
        "process_start_ticks": identity.start_ticks,
    }


def _write_owner(
    owner_dir: Path, identity: ProcessIdentity, allocation: str | None = None, changes: dict[str, object] | None = None
) -> None:
    document: dict[str, object] = {
        "format": "httk-workflow-owner",
        "format_version": 1,
        "owner_id": owner_dir.name,
        "kind": "manager",
        "label": None,
        **_identity_json(identity),
        "started_at": "2026-10-09T00:00:00Z",
        "allocation": None if allocation is None else _allocation(allocation),
        "pools": [],
        "capabilities": {},
        "prefixes": [],
        "resources": {},
    }
    document.update(changes or {})
    (owner_dir / "owner.json").write_text(json.dumps(document))


def _write_launch(
    owner_dir: Path,
    name: str,
    identity: ProcessIdentity | None,
    *,
    allocation: str | None = None,
    local: bool = False,
    raw: str | None = None,
) -> Path:
    directory = owner_dir / "launches" / name
    directory.mkdir(parents=True)
    (directory / "launch.json").write_text("{}")
    if raw is not None:
        (directory / "process.json").write_text(raw)
    elif identity is not None:
        record = {
            "attempt_id": name.partition(".")[0],
            "n": name.partition(".")[2],
            **_identity_json(identity),
            "pgid": identity.pid,
            "started_at": "2026-10-09T00:00:00Z",
            "allocation": None if allocation is None else _allocation(allocation),
            "ranks_local_only": local,
        }
        (directory / "process.json").write_text(json.dumps(record))
    return directory


def _no_sleep(seconds: float) -> None:
    assert seconds == 2.0


def _must_not_sleep(seconds: float) -> None:
    pytest.fail(f"an owner that is not dead must not wait out the visibility deadline ({seconds} s)")


def _probe(
    owner_dir: Path, here: ProcessIdentity, scheduler: SchedulerQueries | None = None
) -> tuple[Liveness, tuple[Evidence, ...]]:
    return probe(owner_dir, visibility_deadline=2.0, scheduler=scheduler or FakeScheduler(), here=here, sleep=_no_sleep)


def _rules(evidence: tuple[Evidence, ...]) -> list[str]:
    return [item.rule for item in evidence]


# The first rung: tombstone and records.


def test_tombstone_is_dead_even_with_a_malformed_owner(owner_dir: Path, here: ProcessIdentity) -> None:
    (owner_dir / "dead.json").write_text("{}")
    (owner_dir / "owner.json").write_text("not json")
    liveness, evidence = _probe(owner_dir, here)
    assert liveness is Liveness.DEAD
    assert _rules(evidence) == ["tombstone"]
    assert evidence[0].subject == f"owner {OWNER_ID}"


def test_tombstone_without_owner_record_is_dead(owner_dir: Path, here: ProcessIdentity) -> None:
    (owner_dir / "dead.json").write_text("{}")
    assert _probe(owner_dir, here)[0] is Liveness.DEAD


def test_no_record_is_unknown(owner_dir: Path, here: ProcessIdentity) -> None:
    liveness, evidence = _probe(owner_dir, here)
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence) == ["no_record"]


@pytest.mark.parametrize(
    "change",
    [
        {"format": "httk-workflow-manager"},
        {"format_version": 2},
        {"format_version": True},
        {"owner_id": "manager-2"},
        {"hostname": ""},
        {"pid": 0},
        {"pid": "12"},
        {"process_start_ticks": -1},
        {"boot_id": 7},
    ],
)
def test_malformed_owner_is_unknown(
    owner_dir: Path, here: ProcessIdentity, dead_process: ProcessIdentity, change: dict[str, object]
) -> None:
    _write_owner(owner_dir, dead_process, changes=change)
    liveness, evidence = _probe(owner_dir, here)
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence) == ["malformed_owner"]


@pytest.mark.parametrize("content", ["not json", "[]", "1" * 70000])
def test_unparsable_owner_is_unknown(owner_dir: Path, here: ProcessIdentity, content: str) -> None:
    (owner_dir / "owner.json").write_text(content)
    assert _rules(_probe(owner_dir, here)[1]) == ["malformed_owner"]


def test_symlinked_owner_record_is_unknown(owner_dir: Path, here: ProcessIdentity, tmp_path: Path) -> None:
    _write_owner(tmp_path, here)
    (owner_dir / "owner.json").symlink_to(tmp_path / "owner.json")
    assert _rules(_probe(owner_dir, here)[1]) == ["malformed_owner"]


# The owner itself.


def test_owner_running_here_is_alive(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, here)
    liveness, evidence = probe(
        owner_dir, visibility_deadline=2.0, scheduler=FakeScheduler(), here=here, sleep=_must_not_sleep
    )
    assert liveness is Liveness.ALIVE
    assert _rules(evidence) == ["process_alive"]


def test_owner_running_here_with_active_allocation_is_alive(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, here, allocation="7")
    liveness, evidence = _probe(owner_dir, here, FakeScheduler({"7": False}))
    assert liveness is Liveness.ALIVE
    assert _rules(evidence) == ["process_alive", "allocation_active"]


def test_owner_running_here_with_ended_allocation_is_alive(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, here, allocation="7")
    liveness, evidence = probe(
        owner_dir, visibility_deadline=2.0, scheduler=FakeScheduler({"7": True}), here=here, sleep=_must_not_sleep
    )
    assert liveness is Liveness.ALIVE
    assert _rules(evidence) == ["process_alive", "allocation_ended"]


def test_owner_on_another_host_without_allocation_is_unknown(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, replace(here, hostname="elsewhere"))
    liveness, evidence = _probe(owner_dir, here)
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence) == ["process_unprovable"]


def test_owner_on_another_host_with_unanswered_allocation_is_unknown(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, replace(here, hostname="elsewhere"), allocation="7")
    liveness, evidence = _probe(owner_dir, here, FakeScheduler({"7": None}))
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence) == ["process_unprovable", "allocation_unknown"]


def test_dead_owner_in_active_allocation_is_dead(
    owner_dir: Path, here: ProcessIdentity, dead_process: ProcessIdentity
) -> None:
    _write_owner(owner_dir, dead_process, allocation="7")
    scheduler = FakeScheduler({"7": False})
    liveness, evidence = _probe(owner_dir, here, scheduler)
    assert liveness is Liveness.DEAD
    assert _rules(evidence) == ["process_gone", "allocation_active"]


def test_owner_on_another_host_with_ended_allocation_is_dead(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, replace(here, hostname="elsewhere"), allocation="7")
    liveness, evidence = _probe(owner_dir, here, FakeScheduler({"7": True}))
    assert liveness is Liveness.DEAD
    assert _rules(evidence) == ["process_unprovable", "allocation_ended"]


def test_owner_from_an_earlier_boot_is_dead(owner_dir: Path, here: ProcessIdentity) -> None:
    _write_owner(owner_dir, replace(here, boot_id="an-earlier-boot"))
    assert _probe(owner_dir, here)[0] is Liveness.DEAD


# The launches of a dead owner.


@pytest.fixture
def dead_owner(owner_dir: Path, dead_process: ProcessIdentity) -> Path:
    _write_owner(owner_dir, dead_process)
    return owner_dir


def test_dead_owner_without_launches_is_dead(dead_owner: Path, here: ProcessIdentity) -> None:
    assert _probe(dead_owner, here)[0] is Liveness.DEAD
    (dead_owner / "launches").mkdir()
    assert _probe(dead_owner, here)[0] is Liveness.DEAD


def test_launch_in_ended_allocation_is_dead(dead_owner: Path, here: ProcessIdentity) -> None:
    _write_launch(dead_owner, "a1.0", replace(here, hostname="node7"), allocation="9")
    liveness, evidence = _probe(dead_owner, here, FakeScheduler({"9": True}))
    assert liveness is Liveness.DEAD
    assert _rules(evidence) == ["process_gone", "allocation_ended"]
    assert evidence[-1].subject == "launch a1.0"


@pytest.mark.parametrize("answer", [False, None])
def test_remote_ranks_without_ended_allocation_block(
    dead_owner: Path, here: ProcessIdentity, answer: bool | None
) -> None:
    _write_launch(dead_owner, "a1.0", here, allocation="9")
    liveness, evidence = _probe(dead_owner, here, FakeScheduler({"9": answer}))
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence)[-2:] == ["remote_ranks", "allocation_active" if answer is False else "allocation_unknown"]


def test_remote_ranks_with_gone_group_and_no_allocation_block(
    dead_owner: Path, here: ProcessIdentity, dead_process: ProcessIdentity
) -> None:
    _write_launch(dead_owner, "a1.0", dead_process, local=False)
    assert _probe(dead_owner, here)[0] is Liveness.UNKNOWN


def test_local_ranks_with_gone_group_are_dead(
    dead_owner: Path, here: ProcessIdentity, dead_process: ProcessIdentity
) -> None:
    _write_launch(dead_owner, "a1.0", dead_process, allocation="9", local=True)
    scheduler = FakeScheduler({"9": False})
    liveness, evidence = _probe(dead_owner, here, scheduler)
    assert liveness is Liveness.DEAD
    assert _rules(evidence)[-1] == "process_gone"
    assert evidence[-1].subject == "launch a1.0"


def test_local_ranks_with_running_group_block(dead_owner: Path, here: ProcessIdentity) -> None:
    group = replace(here, pid=os.getpgid(0), start_ticks=process_identity(os.getpgid(0)).start_ticks)
    _write_launch(dead_owner, "a1.0", group, local=True)
    liveness, evidence = _probe(dead_owner, here)
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence)[-1] == "process_alive"


def test_local_ranks_running_here_block_despite_ended_allocation(dead_owner: Path, here: ProcessIdentity) -> None:
    group = replace(here, pid=os.getpgid(0), start_ticks=process_identity(os.getpgid(0)).start_ticks)
    _write_launch(dead_owner, "a1.0", group, allocation="9", local=True)
    scheduler = FakeScheduler({"9": True})
    liveness, evidence = _probe(dead_owner, here, scheduler)
    assert liveness is Liveness.UNKNOWN
    launch = [item for item in evidence if item.subject == "launch a1.0"]
    assert _rules(tuple(launch)) == ["process_alive"]
    assert scheduler.asked == []


def test_local_ranks_with_ended_allocation_are_dead(dead_owner: Path, here: ProcessIdentity) -> None:
    _write_launch(dead_owner, "a1.0", replace(here, hostname="node7"), allocation="9", local=True)
    liveness, evidence = _probe(dead_owner, here, FakeScheduler({"9": True}))
    assert liveness is Liveness.DEAD
    assert _rules(evidence)[-1] == "allocation_ended"


def test_launch_without_process_record_never_started(dead_owner: Path, here: ProcessIdentity) -> None:
    _write_launch(dead_owner, "a1.0123456789abcdef0123456789abcdef", None)
    liveness, evidence = _probe(dead_owner, here)
    assert liveness is Liveness.DEAD
    assert _rules(evidence)[-1] == "not_started"


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[]",
        json.dumps({"hostname": "h", "pid": 5, "pgid": 5}),
        json.dumps({"hostname": "h", "pid": 5, "pgid": 0, "ranks_local_only": True}),
        json.dumps({"hostname": "h", "pid": 5, "pgid": 5, "ranks_local_only": "yes"}),
    ],
)
def test_malformed_process_record_blocks(dead_owner: Path, here: ProcessIdentity, raw: str) -> None:
    _write_launch(dead_owner, "a1.0", None, raw=raw)
    liveness, evidence = _probe(dead_owner, here)
    assert liveness is Liveness.UNKNOWN
    assert _rules(evidence)[-1] == "malformed_record"


def test_launch_entry_that_is_not_a_directory_blocks(dead_owner: Path, here: ProcessIdentity) -> None:
    (dead_owner / "launches").mkdir()
    (dead_owner / "launches" / "a1.0").write_text("")
    assert _rules(_probe(dead_owner, here)[1])[-1] == "malformed_record"


def test_launch_directory_removed_during_probe_is_dead(tmp_path: Path, here: ProcessIdentity) -> None:
    dead, evidence = _death._launch(tmp_path, "a1.0", FakeScheduler(), here)
    assert dead
    assert _rules(evidence) == ["removed"]


def test_one_blocking_launch_blocks_the_owner(
    dead_owner: Path, here: ProcessIdentity, dead_process: ProcessIdentity
) -> None:
    _write_launch(dead_owner, "a1.0", None)
    _write_launch(dead_owner, "a2.0", dead_process, local=True)
    _write_launch(dead_owner, "a3.0", here, allocation="9")
    liveness, evidence = _probe(dead_owner, here, FakeScheduler({"9": False}))
    assert liveness is Liveness.UNKNOWN
    blocking = [item for item in evidence if item.subject == "launch a3.0"]
    assert _rules(tuple(blocking)) == ["remote_ranks", "allocation_active"]


def test_launches_are_listed_after_the_settle_sleep(dead_owner: Path, here: ProcessIdentity) -> None:
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        # A record that becomes visible only during the visibility deadline.
        slept.append(seconds)
        _write_launch(dead_owner, "late.0", None, raw="not json")

    liveness, evidence = probe(dead_owner, visibility_deadline=3.5, scheduler=FakeScheduler(), here=here, sleep=sleep)
    assert slept == [3.5]
    assert liveness is Liveness.UNKNOWN
    assert evidence[-1] == Evidence("launch late.0", "malformed_record", evidence[-1].detail)


def test_probe_writes_nothing(dead_owner: Path, here: ProcessIdentity) -> None:
    _write_launch(dead_owner, "a1.0", None)
    before = sorted(str(path) for path in dead_owner.rglob("*"))
    assert _probe(dead_owner, here)[0] is Liveness.DEAD
    assert sorted(str(path) for path in dead_owner.rglob("*")) == before


# SchedulerQueries.


class FakeRun:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls += 1
        status = {"format": "httk-workflow-allocation-status", "format_version": 1, "ended": True}
        return subprocess.CompletedProcess(argv, 0, json.dumps(status), "")


def test_scheduler_queries_cache_for_sixty_seconds(tmp_path: Path) -> None:
    site_probe = tmp_path / "probe"
    site_probe.write_text("")
    recorded = RecordedAllocation(f"exec:{site_probe}", "pbs", {"job_id": "1"}, None)
    other = RecordedAllocation(f"exec:{site_probe}", "pbs", {"job_id": "2"}, None)
    now = [100.0]
    run = FakeRun()
    queries = SchedulerQueries(run=run, clock=lambda: now[0])
    assert queries.ended(recorded) is True
    now[0] = 159.9
    assert queries.ended(recorded) is True
    assert run.calls == 1
    assert queries.ended(other) is True
    assert run.calls == 2
    now[0] = 160.0
    assert queries.ended(recorded) is True
    assert run.calls == 3


def test_scheduler_queries_cannot_ask_without_the_client(tmp_path: Path) -> None:
    run = FakeRun()
    queries = SchedulerQueries(run=run)
    assert queries.ended(RecordedAllocation(f"exec:{tmp_path / 'missing'}", "pbs", {"job_id": "1"}, None)) is None
    assert queries.ended(RecordedAllocation("host", "host", None, None)) is None
    assert run.calls == 0


# process_identity and process_gone.


def test_process_identity_of_this_process(here: ProcessIdentity) -> None:
    assert here.hostname == socket.gethostname()
    assert here.pid == os.getpid()
    if Path("/proc/self/stat").exists():
        assert here.start_ticks is not None


def test_running_process_is_not_gone(here: ProcessIdentity) -> None:
    assert process_gone(here, here=here) is False
    group = replace(here, pid=os.getpgid(0), start_ticks=None)
    assert process_gone(group, here=here, group=True) is False


def test_exited_process_is_gone(here: ProcessIdentity, dead_process: ProcessIdentity) -> None:
    assert process_gone(dead_process, here=here) is True
    assert process_gone(dead_process, here=here, group=True) is True


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc start ticks")
def test_reused_pid_with_other_start_ticks_is_gone(here: ProcessIdentity) -> None:
    assert here.start_ticks is not None
    assert process_gone(replace(here, start_ticks=here.start_ticks + 1), here=here) is True


def test_other_boot_on_the_same_host_is_gone(here: ProcessIdentity) -> None:
    assert process_gone(replace(here, boot_id="other-boot"), here=replace(here, boot_id="this-boot")) is True


def test_other_host_is_undecidable(here: ProcessIdentity, dead_process: ProcessIdentity) -> None:
    assert process_gone(replace(dead_process, hostname="elsewhere"), here=here) is None
    assert process_gone(replace(here, pid=0), here=here) is None
