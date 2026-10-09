"""Multi-process stress of the job kernel (plan §9.2): claims, releases and requests under crashes, kills and recovery.

Worker processes act as managers on one workspace. They claim jobs, write
``state.json``, post and apply requests, publish new jobs and release jobs to
random states, while the harness SIGKILLs a random worker every half second
and every worker crashes itself, with a small probability, at each crash
point of :mod:`httk.workflow._fs`. Live workers probe their peers with the
real death oracle (same host, so a killed worker is provably gone) and recover
the dead ones. At the end the harness recovers whatever is left and checks the
invariants of the redesign note (§4.2) on the whole tree.

Every worker appends what it observes to its own JSON-lines log; a SIGKILL may
tear the last line, which the reader skips. ``HTTK_STRESS_SEED`` replays the
random choices of a run (not its process scheduling).
"""

import json
import multiprocessing
import os
import random
import re
import signal
import time
import traceback
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING, Any, TextIO

import pytest

from httk.workflow import _fs, _kernel
from httk.workflow._death import Liveness, SchedulerQueries
from httk.workflow._fs import Loc
from httk.workflow._kernel import (
    OWNED,
    OwnedJob,
    Owner,
    OwnerRecord,
    Release,
    attest_dead,
    claim,
    format_job_name,
    list_jobs,
    list_owners,
    parse_job_name,
    post_request,
    probe_owner,
    prune_empty_placements,
    recover,
    register_owner,
    submit,
)
from httk.workflow._state import UNOWNED_STATES, StateDoc, decode_state
from httk.workflow.errors import WorkflowError

if TYPE_CHECKING:
    from conftest import TestProfile as _TestProfile

VISIBILITY = 0.05
PLACEMENTS = ("alpha", "beta/gamma", "")
#: Chance that one crash point of ``_fs`` kills the worker: roughly one crash per worker every few seconds.
CRASH_PROBABILITY = 1 / 4000
KILL_EVERY = 0.5
#: A write temporary of one of the files in ``owners/<id>/`` (see ``_fs.write_file``).
OWNER_TEMPORARY = re.compile(r"\.(owner|heartbeat|dead)\.json\.[a-z2-7]{16}\.tmp")
CRASH_EXIT = 3
ERROR_EXIT = 2


@dataclass(frozen=True)
class StressWorkspace:
    """The kernel's view of the stressed workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


@dataclass(frozen=True)
class WorkerPlan:
    """What one worker process is told: where to work and log, and how to draw its random choices."""

    ws: StressWorkspace
    log: Path
    seed: int
    job_ids: tuple[str, ...]


class InjectedCrash(SystemExit):
    """A simulated crash at an ``_fs`` crash point; the worker exits without any cleanup."""


class Log:
    """One process's JSON-lines event log, flushed line by line so that a crash loses at most the line in flight."""

    def __init__(self, path: Path) -> None:
        self._stream: TextIO = open(path, "a", encoding="utf-8", buffering=1)  # noqa: SIM115 - lives as long as the process

    def write(self, event: str, **detail: object) -> None:
        self._stream.write(json.dumps({"event": event, **detail}) + "\n")

    def close(self) -> None:
        self._stream.close()


def read_events(directory: Path) -> Iterator[dict[str, Any]]:
    for path in sorted(directory.glob("*.jsonl")):
        lines = path.read_text(encoding="utf-8").splitlines()
        for number, line in enumerate(lines):
            try:
                yield {"log": path.name, **json.loads(line)}
            except json.JSONDecodeError:
                # Only the last line of a SIGKILLed worker may be torn.
                assert number == len(lines) - 1, f"{path}:{number + 1} is not JSON"


def fault_injector(rng: random.Random, probability: float, log: Log) -> _fs.Fault:
    """Log every job-directory name a rename creates (I2), and crash with *probability* at every crash point."""

    def fault(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        # Observed right after the rename: the source gone and the destination present mean this rename created
        # the name. A retried rename that did not happen is not logged, so a name logged twice was created twice.
        if (
            (op, phase) == ("rename", "after")
            and src is not None
            and dst is not None
            and "~" in dst.name()
            and _fs.exists(dst)
            and not _fs.exists(src)
        ):
            log.write("moved", name=dst.name())
        if rng.random() < probability:
            log.write("crash", op=op, phase=phase)
            raise InjectedCrash(CRASH_EXIT)

    return fault


def write_payload(directory: Path, rng: random.Random) -> str:
    """Write a minimal ``job.json`` the kernel accepts, at a random placement and priority."""

    job_id = str(uuid.uuid4())
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": job_id,
        "tag": rng.choice(["s", None]),
        "placement": rng.choice(PLACEMENTS),
        "priority": rng.randrange(1000),
    }
    (directory / "job.json").write_text(json.dumps(document), encoding="utf-8")
    return job_id


def new_job(ws: StressWorkspace, owner: Owner, rng: random.Random, log: Log) -> str:
    staging = owner.scratch("build") / "job"
    job_id = write_payload(staging, rng)
    log.write("staged", job=job_id)
    submit(ws, owner, staging)
    return job_id


def random_release_state(rng: random.Random) -> str:
    return "ready" if rng.random() < 0.5 else rng.choice(UNOWNED_STATES[1:])


def touched(doc: StateDoc, owner_id: str) -> StateDoc:
    """A small change of the document, validated like any ``state.json``."""

    mapping = doc.as_mapping()
    counters, history = mapping["counters"], mapping["history_tail"]
    assert isinstance(counters, dict) and isinstance(history, list)
    counters["activations"] = int(counters.get("activations", 0)) + 1
    history = [*history[-31:], {"owner_id": owner_id, "event": "claimed"}]
    mapping.update(owner_id=owner_id, counters=counters, history_tail=history)
    return StateDoc.from_mapping(mapping)


# -- the worker -------------------------------------------------------------------------------------------------------


def process_job(plan: WorkerPlan, owner: Owner, job: OwnedJob, rng: random.Random, log: Log) -> None:
    """What a manager does with a claimed job, reduced to the kernel calls: reconcile, write, apply, release."""

    ws = plan.ws
    job.remove_leftover_temporaries()
    doc = job.read_state()
    if (pending := job.pending_release()) is not None and doc is not None:
        # An earlier owner recorded this release and died before its move: finish it, re-applying nothing (I4).
        job.release(doc, pending)
        log.write("finished_release", job=job.job_id)
        return
    doc = touched(doc or StateDoc.empty(job.job_id), owner.owner_id)
    job.write_state(doc)
    if rng.random() < 0.3:
        job.append_log("claimed")
    if rng.random() < 0.3:
        target = job.job_id if rng.random() < 0.5 else rng.choice(plan.job_ids)
        request_id = str(uuid.uuid4())
        post_request(ws, {"job_id": target, "request_id": request_id, "action": "cancel"})
        log.write("posted", job=target, request=request_id)
    if rng.random() < 0.1:
        # An attempt with one launch directory, as a manager makes; recovery must discard a dead owner's.
        attempt = str(uuid.uuid4())
        job.begin_attempt(attempt)
        owner.launch_dir(attempt, "0")
        owner.remove_launch(attempt, "0")
        job.end_attempt(attempt)
    pending_ids = [path.name.split(".")[1] for path in job.request_files()]
    # A request whose id the document records as applied is never offered again (I4).
    assert not set(pending_ids) & set(doc.applied_requests), f"{job.job_id}: an applied request is pending again"
    applied = tuple(request_id for request_id in pending_ids if rng.random() < 0.7)
    job.release(doc, Release(random_release_state(rng), rng.randrange(1000), applied))
    log.write("applied", job=job.job_id, requests=list(applied))


def recovered_completely(ws: StressWorkspace, record: OwnerRecord) -> bool:
    """Whether a tombstoned owner has nothing left but its tombstone (else a recoverer died midway)."""

    leftovers = sorted(os.listdir(record.path)) if record.path.exists() else []
    return leftovers in ([], ["dead.json"]) and not (ws.jobs / OWNED / record.owner_id).exists()


def recover_peers(ws: StressWorkspace, owner: Owner, scheduler: SchedulerQueries, log: Log) -> None:
    """Probe every other owner with the real death oracle and recover the dead ones."""

    for record in list_owners(ws):
        peer = record.owner_id
        if peer == owner.owner_id:
            continue
        if record.tombstone is not None:
            if recovered_completely(ws, record):
                continue
        elif probe_owner(ws, peer, scheduler=scheduler) is Liveness.DEAD:
            log.write("declared_dead", owner=peer)
        else:
            continue
        try:
            report = recover(ws, owner, peer)
        except WorkflowError as exc:
            # Documented: concurrent recoverers may keep finding entries the other one is still moving.
            log.write("recover_conflict", owner=peer, error=type(exc).__name__, detail=str(exc))
            continue
        log.write("recovered", owner=peer, returned=len(report.returned), quarantined=len(report.quarantined))


def work(plan: WorkerPlan, owner: Owner, rng: random.Random, log: Log, stopped: Callable[[], bool]) -> None:
    ws, scheduler = plan.ws, SchedulerQueries()
    next_probe = time.monotonic() + rng.uniform(0.05, 0.3)
    while not stopped():
        # Fail-stop: a live worker declared dead by a peer would be a broken death proof (I3).
        owner.check_alive()
        if time.monotonic() >= next_probe:
            recover_peers(ws, owner, scheduler, log)
            next_probe = time.monotonic() + rng.uniform(0.1, 0.4)
        roll = rng.random()
        if roll < 0.05:
            owner.heartbeat()
        elif roll < 0.08:
            prune_empty_placements(ws, budget=8)
        elif roll < 0.12:
            new_job(ws, owner, rng, log)
        state = "ready" if rng.random() < 0.7 else rng.choice(UNOWNED_STATES[1:])
        refs = list(list_jobs(ws, state))
        offset = rng.randrange(len(refs)) if refs else 0
        for ref in (refs[offset:] + refs[:offset])[: rng.randint(1, 4)]:
            job = claim(ws, owner, ref)
            if job is not None:
                log.write("claimed", job=job.job_id, state=state)
                process_job(plan, owner, job, rng, log)


def worker(plan: WorkerPlan) -> None:
    """One manager process; exits 0 after a clean close, CRASH_EXIT after an injected crash, ERROR_EXIT on a bug."""

    stop = False

    def on_sigterm(signum: int, frame: FrameType | None) -> None:
        del signum, frame
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_sigterm)
    rng = random.Random(plan.seed)
    log = Log(plan.log)
    try:
        owner = register_owner(plan.ws, kind="manager", label=None, allocation=None, advertised={})
        log.write("registered", owner=owner.owner_id)
        _fs.set_fault_injector(fault_injector(rng, CRASH_PROBABILITY, log))
        work(plan, owner, rng, log, lambda: stop)
        # A clean stop: nothing is held between iterations, so close() returns and removes everything.
        _fs.set_fault_injector(fault_injector(rng, 0.0, log))
        owner.close()
        log.write("closed", owner=owner.owner_id)
    except InjectedCrash:
        os._exit(CRASH_EXIT)  # a crash runs no cleanup
    except BaseException:
        log.write("error", traceback=traceback.format_exc())
        os._exit(ERROR_EXIT)
    log.close()


# -- the harness ------------------------------------------------------------------------------------------------------


def job_directories(directory: Path) -> list[Path]:
    """Every job directory anywhere below *directory* (by its name); job directories are not entered."""

    found = []
    for path, names, _files in os.walk(directory):
        for name in list(names):
            if "~" in name:
                found.append(Path(path) / name)
                names.remove(name)
    return found


def job_ids_below(directory: Path) -> Counter[str]:
    return Counter(parse_job_name(path.name).job_id for path in job_directories(directory))


def seed_jobs(ws: StressWorkspace, count: int, rng: random.Random, log: Log) -> list[str]:
    with register_owner(ws, kind="cli", label="seed", allocation=None, advertised={}) as owner:
        return [new_job(ws, owner, rng, log) for _ in range(count)]


def final_recovery(ws: StressWorkspace, log: Log) -> tuple[list[str], list[str]]:
    """Recover every owner left behind by a dead worker; return the recovered and the unprovable owners."""

    recovered, unknown = [], []
    with register_owner(ws, kind="cli", label="final", allocation=None, advertised={}) as owner:
        scheduler = SchedulerQueries()
        for record in list_owners(ws):
            if record.owner_id == owner.owner_id:
                continue
            if record.tombstone is None:
                verdict = probe_owner(ws, record.owner_id, scheduler=scheduler)
            elif recovered_completely(ws, record):
                continue
            else:
                verdict = Liveness.DEAD
            # Every worker has exited and been reaped, so nothing may still look alive.
            assert verdict is not Liveness.ALIVE, f"owner {record.owner_id} is alive after every worker stopped"
            if verdict is Liveness.UNKNOWN:
                unknown.append(record.owner_id)
                continue
            report = recover(ws, owner, record.owner_id)
            log.write("recovered", owner=record.owner_id, returned=len(report.returned))
            recovered.append(record.owner_id)
    return recovered, unknown


def run_workers(
    ws: StressWorkspace, logs: Path, rng: random.Random, job_ids: Sequence[str], *, workers: int, seconds: float
) -> Counter[str]:
    """Run, kill and replace workers until the deadline, then stop them; return the exit statistics."""

    context = multiprocessing.get_context("spawn")
    started = 0
    stats: Counter[str] = Counter()

    def start() -> BaseProcess:
        nonlocal started
        plan = WorkerPlan(ws, logs / f"worker-{started:04d}.jsonl", rng.randrange(2**63), tuple(job_ids))
        started += 1
        process = context.Process(target=worker, args=(plan,), name=f"stress-{started}")
        process.start()
        return process

    def classify(process: BaseProcess, *, stopping: bool) -> None:
        code = process.exitcode
        if code == -signal.SIGKILL:
            stats["killed"] += 1
        elif code == CRASH_EXIT:
            stats["crashed"] += 1
        elif code == 0 and stopping:
            stats["closed"] += 1
        elif code == -signal.SIGTERM and stopping:
            # Stopped before it installed its handler: it may or may not have registered (the final pass sees).
            stats["terminated_early"] += 1
        else:
            stats[f"unexpected_exit_{code}"] += 1

    live = [start() for _ in range(workers)]
    now = time.monotonic()
    deadline, cap, next_kill = now + seconds, now + 3 * seconds, now + KILL_EVERY
    while True:
        for process in [process for process in live if not process.is_alive()]:
            live.remove(process)
            classify(process, stopping=False)
            live.append(start())
        now = time.monotonic()
        if now >= next_kill:
            victim = rng.choice(live)
            if victim.pid is not None:
                os.kill(victim.pid, signal.SIGKILL)
            # Reap at once: an unreaped zombie still answers kill(pid, 0), so peers could not prove it dead.
            victim.join()
            live.remove(victim)
            classify(victim, stopping=False)
            live.append(start())
            next_kill = now + KILL_EVERY
        # Run past the deadline (up to a cap) until both kinds of death happened, so recovery is exercised.
        if now >= cap or (now >= deadline and stats["killed"] and stats["crashed"]):
            break
        time.sleep(0.02)
    for process in live:
        process.terminate()
    for process in live:
        process.join(timeout=120)
        if process.is_alive():
            stats["hung"] += 1
            process.kill()
            process.join()
        else:
            classify(process, stopping=True)
    stats["started"] = started
    return stats


@pytest.mark.slow
def test_kernel_invariants_under_crashes_and_kills(tmp_path: Path, test_profile: "_TestProfile") -> None:
    workers = test_profile.scale(normal=6, extended=24)
    jobs = test_profile.scale(normal=120, extended=1500)
    seconds = test_profile.scale(normal=8.0, extended=60.0)
    seed = int(os.environ.get("HTTK_STRESS_SEED") or random.randrange(2**32))
    rng = random.Random(seed)
    root = tmp_path / "ws"
    ws = StressWorkspace(root, root / ".httk-workspace", root / "jobs", False, VISIBILITY)
    logs = tmp_path / "logs"
    logs.mkdir()
    parent_log = Log(logs / "parent.jsonl")
    _fs.set_fault_injector(fault_injector(rng, 0.0, parent_log))
    try:
        seeded = seed_jobs(ws, jobs, rng, parent_log)
        stats = run_workers(ws, logs, rng, seeded, workers=workers, seconds=seconds)
        recovered_at_end, unknown = final_recovery(ws, parent_log)
    finally:
        _fs.set_fault_injector(None)
        parent_log.close()
    events = list(read_events(logs))
    by_kind: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        by_kind.setdefault(str(event["event"]), []).append(event)

    def values(kind: str, key: str) -> list[str]:
        return [str(event[key]) for event in by_kind.get(kind, [])]

    context = f"seed {seed}, stats {dict(stats)}"
    # The run's summary, shown with -rP or -s.
    print(
        f"stress: {context}, crash events {len(by_kind.get('crash', []))}, "
        f"recovered by workers {len(by_kind.get('recovered', [])) - len(recovered_at_end)}, "
        f"recovered at end {len(recovered_at_end)}, unprovable {len(unknown)}, "
        f"claims {len(by_kind.get('claimed', []))}, releases {len(by_kind.get('applied', []))}, "
        f"finished releases {len(by_kind.get('finished_release', []))}, staged {len(by_kind.get('staged', []))}, "
        f"requests posted {len(by_kind.get('posted', []))}, "
        f"crash points {dict(Counter(f'{event['op']}/{event['phase']}' for event in by_kind.get('crash', [])))}"
    )

    # No worker hit a bug: a raised OwnerLost or OwnerDeclaredDead would mean a live owner lost its jobs (I3).
    errors = [str(event["traceback"]) for event in by_kind.get("error", [])]
    assert errors == [], f"{context}: worker errors:\n" + "\n".join(errors)
    assert not [key for key in stats if key.startswith("unexpected") or key == "hung"], context
    # Only the documented conflict of concurrent recoverers is tolerated, never a failed filesystem operation.
    assert set(values("recover_conflict", "error")) <= {"WorkflowError"}, context

    # The run exercised recovery: workers were killed and crashed, and their owners were recovered.
    assert stats["killed"] > 0 and stats["crashed"] > 0 and by_kind.get("crash"), context
    # Workers, not only the final pass, must have recovered peers: the parent logs its recoveries too.
    assert [event for event in by_kind.get("recovered", []) if event["log"] != "parent.jsonl"], context

    # I1: no job is lost or duplicated. Every published job is in exactly one directory of the whole tree,
    # and none is still owned once every dead owner is recovered.
    found = job_ids_below(root)
    duplicated = {job_id: count for job_id, count in found.items() if count > 1}
    assert duplicated == {}, f"{context}: jobs in more than one place: {duplicated}"
    published = set(seeded) | {parse_job_name(name).job_id for name in values("moved", "name")}
    assert published <= set(found), f"{context}: lost jobs {sorted(published - set(found))}"
    assert set(found) <= set(seeded) | set(values("staged", "job")), f"{context}: jobs from nowhere"
    assert job_ids_below(ws.jobs / OWNED) == Counter(), f"{context}: jobs still owned after final recovery"
    assert os.listdir(ws.jobs / OWNED) == [], f"{context}: owned/<id>/ directories left behind"
    # No job.json is ever damaged here, so nothing may be quarantined, not even an empty directory.
    quarantine = ws.control / "quarantine"
    quarantined = os.listdir(quarantine) if quarantine.exists() else []
    assert quarantined == [], f"{context}: quarantined {quarantined}"

    # I2: no job directory name is ever created twice; every move draws a fresh token.
    names = Counter(values("moved", "name"))
    assert [name for name, count in names.items() if count > 1] == [], context

    # I4: a request is applied at most once, however often its job changed hands mid-release.
    applied = Counter(request for event in by_kind.get("applied", []) for request in event["requests"])
    assert [request for request, count in applied.items() if count > 1] == [], context

    # Every state.json is whole and names its job: write_file never exposes a torn document.
    for directory in job_directories(root):
        if (directory / "state.json").exists():
            doc = decode_state((directory / "state.json").read_bytes())
            assert doc.job_id == parse_job_name(directory.name).job_id, f"{context}: {directory}"

    # Owners: a recovered owner leaves only its tombstone (dead stays dead), a cleanly closed one nothing.
    # The one accepted exception (§6 step 1) is an owner killed between creating its directory and writing
    # owner.json: unprovable, and it never owned anything.
    closed = set(values("closed", "owner"))
    owners = ws.control / "owners"
    for owner_id in sorted(os.listdir(owners)):
        contents = sorted(os.listdir(owners / owner_id))
        assert owner_id not in closed, f"{context}: closed owner {owner_id} left {contents}"
        if owner_id in unknown:
            assert all(name.startswith(".owner.json.") for name in contents), f"{context}: {owner_id} {contents}"
            assert not (ws.jobs / OWNED / owner_id).exists(), context
        else:
            assert contents == ["dead.json"], f"{context}: dead owner {owner_id} left {contents}"
    # No scratch survives: the dead owners' was taken and reconciled by their recoverers.
    assert os.listdir(ws.control / "tmp") == [], f"{context}: scratch left {os.listdir(ws.control / 'tmp')}"


# -- regressions the harness found ----------------------------------------------------------------------------------


def test_concurrently_returned_job_leaves_no_quarantine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "ws"
    ws = StressWorkspace(root, root / ".httk-workspace", root / "jobs", False, VISIBILITY)
    log = Log(tmp_path / "log.jsonl")
    dead = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})
    new_job(ws, dead, random.Random(0), log)
    (ref,) = list_jobs(ws, "ready")
    job = claim(ws, dead, ref)
    assert job is not None
    placement = job.placement()
    attest_dead(ws, dead.owner_id, by="operator", evidence=[], operator="test")
    recoverer = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})
    real_read_header = _kernel._read_header

    def rival_returns_it_first(directory: Path, expect_key: str | None = None) -> _kernel.JobHeader:
        # A concurrent recoverer returns the job between this recoverer's listing and its header read.
        if directory == job.path and directory.exists():
            rival_target = ws.jobs / "ready" / placement / format_job_name(ref.job_key, ref.priority, _fs.fresh_token())
            os.rename(directory, rival_target)
        return real_read_header(directory, expect_key)

    monkeypatch.setattr(_kernel, "_read_header", rival_returns_it_first)
    report = recover(ws, recoverer, dead.owner_id)
    log.close()
    assert report.returned == () and report.quarantined == ()
    assert job_ids_below(root) == Counter({job.job_id: 1})
    # The job was returned, never damaged: no quarantine directory may appear for it.
    assert not (ws.control / "quarantine").exists()


class _Crash(SystemExit):
    """A crash inside ``_fs.write_file``: unlike an ``Exception``, it leaves the write temporary behind."""


def test_recovery_leaves_only_the_tombstone(tmp_path: Path) -> None:
    root = tmp_path / "ws"
    ws = StressWorkspace(root, root / ".httk-workspace", root / "jobs", False, VISIBILITY)
    dead = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})

    def crash_before_replace(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
        del src, dst
        if (op, phase) == ("write", "before_replace"):
            raise _Crash(CRASH_EXIT)

    _fs.set_fault_injector(crash_before_replace)
    try:
        # The owner dies writing its heartbeat; the first prober to prove it dead dies writing the tombstone.
        with pytest.raises(_Crash):
            dead.heartbeat()
        with pytest.raises(_Crash):
            attest_dead(ws, dead.owner_id, by="probe", evidence=[])
    finally:
        _fs.set_fault_injector(None)
    assert len([name for name in os.listdir(dead.path) if OWNER_TEMPORARY.fullmatch(name)]) == 2
    attest_dead(ws, dead.owner_id, by="probe", evidence=[])
    recoverer = register_owner(ws, kind="manager", label=None, allocation=None, advertised={})
    recover(ws, recoverer, dead.owner_id)
    # Dead stays dead, and nothing else of the owner stays.
    assert os.listdir(dead.path) == ["dead.json"]
