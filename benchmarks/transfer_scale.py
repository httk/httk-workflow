"""Measure ``httk job transfer`` at scale: the T5 acceptance benchmark (plan WORKFLOW_TRANSFER_SESSION §7.10, item 6).

This is a script, not a test. One invocation measures one transfer of N single-job bundles (about ten small files
each) between two durable workspaces with the default policy (``io.sync=auto``, ``io.workers=8``) and the
visibility deadline D, over the loopback stand-in ``ssh`` of the test fixtures, and writes one JSON result::

    nice -n 10 python benchmarks/transfer_scale.py --out DIR --direction push --jobs 10000 \\
        --placement distributed [--deadline 5] [--prepopulate M] [--tree] [--strace-subset K]
    python benchmarks/transfer_scale.py --out DIR --summarize

``--direction`` is ``push`` (local to remote), ``pull`` (remote to local), ``rr`` (remote to remote, two remotes) or
``local`` (local to local); every remote end goes over its transfer session, which a transfer requires.
``--placement distributed`` spreads the jobs over 1,000 placements, ``flat`` puts them in one. ``--prepopulate M`` first places M other jobs in DST, in the same placement scheme. ``--tree`` moves N trees of
four jobs (a root and three descendants) with ``--tree`` instead of single jobs. ``--strace-subset K``, when
``strace`` is on ``PATH``, afterwards traces one more transfer of K jobs into the DST the run filled
(``strace -f -c``) and records per-job syscall counts; otherwise it records that they were not measured.
``--summarize`` writes ``DIR/summary.md`` from every result in ``DIR/results/``. Results from before transfer sessions
were required (``session`` ``off``, or no ``expected_counts``) are skipped, counted, and never matched as baselines.

Everything lives below ``--out`` (never ``/tmp``): ``results/<run>.json``, ``logs/<run>/`` (the timestamped progress
lines, the stand-in ssh's log, the per-process probe dumps and event files) and ``work/<run>/`` (the workspaces,
deleted after the run unless ``--keep-work``). The httk configuration and data homes are redirected into the run's
work directory (``HTTK_CONFIG_HOME``, ``HTTK_DATA_HOME``, as the test fixtures do); the script refuses to run when
the redirection is not in effect, and checks that the real ``workspaces.json`` is byte-identical before and after.

How each quantity is obtained (no source is changed):

- The CLI runs as a child process, and every far-side ``httk`` the stand-in ssh starts runs through the same
  wrapper (``_probe``), which wraps a few functions at run time before dispatching to ``httk.core.cli``:
  ``_moving.adopt`` (into DST), ``_moving.hold`` and ``_moving.release_hold`` (on SRC) append a timestamp a call
  to an event file (the time series: a page is every 256th adoption into DST, the CLI's page size);
  ``_kernel._pause`` (settle waiting), ``os.fsync``/``os.fdatasync``, ``_fs._syncfs`` and ``_fs.Barrier.commit``
  (barriers), ``os.listdir``/``os.scandir`` and ``_kernel.list_names``/``_kernel._subdirectories`` (listings,
  their time and the names they returned) are timed and counted; ``os.mkdir``, ``os.rename``/``os.replace``,
  ``os.open`` and ``open`` are counted. Times are summed over threads (thread-seconds), so with ``io.workers``
  pools a share of the wall time may exceed one. Each process dumps its counters, CPU time and ``VmHWM`` at exit.
- The client's and far sides' ``VmRSS`` are sampled once a second from ``/proc`` (the CLI's descendants).
- Connections are the stand-in ssh's log lines, starts those running ``httk`` (as the tests count them).
- Validity: exit status 0, every bundle reported ``adopted``, every job present in DST, none left in SRC, no hold
  left on SRC, every adoption seen by the probe, and the expected connections and
  starts (push and pull 1/1, ``rr`` 2/2, ``local`` 0/0). An invalid run is reported as invalid, not timed.
"""

import argparse
import builtins
import collections
import contextlib
import hashlib
import io
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Self

SCRIPT = Path(__file__).resolve()
SOURCE = SCRIPT.parents[1] / "src"
#: The CLI's page of holds (``_transfer._PAGE_HOLDS``), checked at run time.
PAGE = 256
#: The placements of ``--placement distributed``.
PLACEMENTS = 1000
#: An event line: a ``time.monotonic()`` stamp, fixed width, so a file's size counts its events.
EVENT_FORMAT = "{:020.6f}\n"
EVENT_WIDTH = 21
#: The far-side memory allowance per job of a long-lived process (plan §7.10 6(c), "declared structures"): the
#: selection snapshot (~250 B), a frozen earlier-hold name (16 B), and one entry in ``moving`` and in the
#: unconsumed ids (~120 B each as a set of id strings). Anything above it is "something else linear".
DECLARED_BYTES_PER_JOB = 512
#: Below this N, (b) and (c) are reported as indicative only: too few pages and too little declared growth.
CONCLUSIVE_JOBS = 100_000
WORKFLOW = ("demo--0123456789abcdef", "demo")
EXPECTED_COUNTS = {"push": (1, 1), "pull": (1, 1), "rr": (2, 2), "local": (0, 0)}
SYSCALLS = ("getdents64", "openat", "mkdir", "mkdirat", "rename", "renameat", "renameat2", "fsync", "syncfs")

FAKE_SSH = '''#!{python}
"""Stand-in for ssh that runs the remote command locally (the test fixtures' loopback, plus the host's name)."""

import json
import os
import pathlib
import subprocess
import sys
import time

VALUE_OPTIONS = {{"-p", "-o", "-i", "-l", "-F", "-c", "-m", "-b", "-D", "-E", "-e", "-I", "-J", "-S", "-W", "-w"}}


def main(argv):
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--":
            index += 1
            break
        if item in VALUE_OPTIONS:
            index += 2
            continue
        if item.startswith("-"):
            index += 1
            continue
        break
    if index >= len(argv):
        print("fake ssh: no destination", file=sys.stderr)
        return 255
    destination = argv[index]
    command = " ".join(argv[index + 1 :])
    with open(os.environ["HTTK_FAKE_SSH_LOG"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps({{"destination": destination, "command": command, "t": time.monotonic()}}) + "\\n")
    if not command:
        print("fake ssh: interactive sessions are not supported", file=sys.stderr)
        return 255
    root = pathlib.Path(os.environ["HTTK_FAKE_SSH_ROOT"])
    root.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["TS_FAR_HOST"] = destination
    return subprocess.run(["/bin/sh", "-c", command], cwd=root, env=environment).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''

HTTK_SHIM = '#!/bin/sh\nexec {python} {script} _probe far "$@"\n'


# -- the probe: runs inside the CLI and every far-side httk ----------------------------------------------------------


class _Listing:
    """``os.scandir``'s iterator, counting the names it yields and the time spent in it."""

    def __init__(self, probe: "_Probe", iterator: Any, spent: float) -> None:
        self._probe, self._iterator, self._spent, self._names, self._done = probe, iterator, spent, 0, False

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Any:
        start = time.perf_counter()
        try:
            entry = next(self._iterator)
        except StopIteration:
            self._spent += time.perf_counter() - start
            self._flush()
            raise
        self._spent += time.perf_counter() - start
        self._names += 1
        return entry

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        del exc_info
        self.close()

    def close(self) -> None:
        self._iterator.close()
        self._flush()

    def __del__(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if not self._done:
            self._done = True
            self._probe.add("scandir", self._spent, names=self._names)


class _Probe:
    """Run-time instrumentation of one httk process (see the module docstring); dumped as JSON at exit."""

    def __init__(self, role: str, directory: Path) -> None:
        self.role, self.directory = role, directory
        self.started = time.monotonic()
        self.lock = threading.RLock()  # the counted open() runs inside it too
        self.calls: collections.Counter[str] = collections.Counter()
        self.seconds: collections.defaultdict[str, float] = collections.defaultdict(float)
        self.names: collections.Counter[str] = collections.Counter()
        self.events: collections.Counter[str] = collections.Counter()
        self.streams: dict[str, io.TextIOWrapper] = {}
        self.roots: dict[str, str] = {}
        self.src = os.path.realpath(os.environ.get("TS_SRC_ROOT", "/nonexistent"))
        self.dst = os.path.realpath(os.environ.get("TS_DST_ROOT", "/nonexistent"))

    def add(self, name: str, spent: float = 0.0, *, names: int | None = None) -> None:
        with self.lock:
            self.calls[name] += 1
            self.seconds[name] += spent
            if names is not None:
                self.names[name] += names

    def event(self, kind: str) -> None:
        now = time.monotonic()
        with self.lock:
            stream = self.streams.get(kind)
            if stream is None:
                path = self.directory / f"ev-{kind}-{os.getpid()}.txt"
                stream = self.streams[kind] = open(path, "a", encoding="ascii", buffering=1 << 16)  # noqa: SIM115
            stream.write(EVENT_FORMAT.format(now))
            self.events[kind] += 1
            if self.events[kind] % PAGE == 0:
                stream.flush()  # the driver follows the progress by the files' sizes

    def root_of(self, workspace: Any) -> str:
        key = str(getattr(workspace, "root", ""))
        if key not in self.roots:
            self.roots[key] = os.path.realpath(key)
        return self.roots[key]

    def timed[**P, R](self, name: str, function: Callable[P, R], *, listing: bool = False) -> Callable[P, R]:
        def wrapper(*arguments: P.args, **options: P.kwargs) -> R:
            start = time.perf_counter()
            result: Any = None
            try:
                result = function(*arguments, **options)
                return result
            finally:
                names = len(result) if listing and isinstance(result, list) else None
                self.add(name, time.perf_counter() - start, names=names)

        return wrapper

    def counted[**P, R](self, name: str, function: Callable[P, R]) -> Callable[P, R]:
        def wrapper(*arguments: P.args, **options: P.kwargs) -> R:
            with self.lock:
                self.calls[name] += 1
            return function(*arguments, **options)

        return wrapper

    def install(self, *, full: bool) -> None:
        from httk.workflow import _fs, _kernel, _moving

        if full:
            self._wrap_calls(_fs, _kernel)
        self._wrap_moves(_moving)

    def _wrap_calls(self, _fs: Any, _kernel: Any) -> None:
        real_scandir = os.scandir

        def scandir(*arguments: Any, **options: Any) -> Any:
            start = time.perf_counter()
            iterator = real_scandir(*arguments, **options)
            return _Listing(self, iterator, time.perf_counter() - start)

        # Patched as attributes (setattr), so that no checker holds them to the stubs' overloads.
        patches: list[tuple[Any, str, Any]] = [
            (os, "scandir", scandir),
            (os, "listdir", self.timed("listdir", os.listdir, listing=True)),
            (os, "fsync", self.timed("fsync", os.fsync)),
            (os, "fdatasync", self.timed("fdatasync", os.fdatasync)),
            (os, "mkdir", self.counted("mkdir", os.mkdir)),
            (os, "rename", self.counted("rename", os.rename)),
            (os, "replace", self.counted("replace", os.replace)),
            (os, "open", self.counted("os_open", os.open)),
            (_fs, "_syncfs", self.timed("syncfs", _fs._syncfs)),
            (_fs.Barrier, "commit", self.timed("barrier_commit", _fs.Barrier.commit)),
            (_kernel, "_pause", self.timed("settle_pause", _kernel._pause)),
            (_kernel, "list_names", self.timed("kernel_list_names", _kernel.list_names, listing=True)),
            (_kernel, "_subdirectories", self.timed("kernel_subdirectories", _kernel._subdirectories, listing=True)),
        ]
        counted_open = self.counted("open", io.open)
        patches += [(builtins, "open", counted_open), (io, "open", counted_open)]
        for owner, name, value in patches:
            setattr(owner, name, value)

    def _wrap_moves(self, _moving: Any) -> None:
        adopt, hold, release_hold = _moving.adopt, _moving.hold, _moving.release_hold

        def adopted(workspace: Any, *arguments: Any, **options: Any) -> Any:
            report = adopt(workspace, *arguments, **options)
            if report is not None and not report.already_adopted and self.root_of(workspace) == self.dst:
                self.event("adopt")
            return report

        def held(workspace: Any, *arguments: Any, **options: Any) -> Any:
            result = hold(workspace, *arguments, **options)
            if self.root_of(workspace) == self.src:
                self.event("hold")
            return result

        def released(workspace: Any, *arguments: Any, **options: Any) -> Any:
            result = release_hold(workspace, *arguments, **options)
            if result and self.root_of(workspace) == self.src:
                self.event("release")
            return result

        for name, value in (("adopt", adopted), ("hold", held), ("release_hold", released)):
            setattr(_moving, name, value)

    def dump(self, code: object) -> None:
        import resource

        with self.lock:
            for stream in self.streams.values():
                stream.close()
            status = _proc_status(os.getpid())
            usage = resource.getrusage(resource.RUSAGE_SELF)
            document = {
                "role": self.role,
                "pid": os.getpid(),
                "ppid": os.getppid(),
                "host": os.environ.get("TS_FAR_HOST", "local"),
                "argv": sys.argv[3:23],
                "code": code if isinstance(code, int) else str(code),
                "started": self.started,
                "ended": time.monotonic(),
                "cpu_user": usage.ru_utime,
                "cpu_system": usage.ru_stime,
                "vmhwm_kib": status.get("VmHWM"),
                "vmrss_kib": status.get("VmRSS"),
                "calls": dict(self.calls),
                "seconds": dict(self.seconds),
                "names": dict(self.names),
                "events": dict(self.events),
            }
        path = self.directory / f"probe-{self.role}-{os.getpid()}.json"
        path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")


def _probe_main(role: str, argv: list[str]) -> int:
    probe = _Probe(role, Path(os.environ["TS_PROBE_DIR"]))
    probe.install(full=os.environ.get("TS_PROBE", "full") == "full")
    from httk.core.cli import main

    code: object = "exception"
    try:
        code = main(argv)
    except SystemExit as exc:
        code = exc.code
        raise
    finally:
        probe.dump(code)
    return int(code) if isinstance(code, int) else 1


# -- helpers --------------------------------------------------------------------------------------------------------


def _proc_status(pid: int) -> dict[str, int]:
    """``VmRSS``, ``VmHWM`` (KiB) and ``PPid`` of *pid*, empty once it is gone."""

    found: dict[str, int] = {}
    try:
        with open(f"/proc/{pid}/status", encoding="ascii", errors="replace") as stream:
            for line in stream:
                key, _, value = line.partition(":")
                if key in ("VmRSS", "VmHWM", "PPid"):
                    found[key] = int(value.split()[0])
    except (OSError, ValueError, IndexError):
        return {}
    return found


def _children() -> dict[int, list[int]]:
    tree: dict[int, list[int]] = collections.defaultdict(list)
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", encoding="ascii", errors="replace") as stream:
                fields = stream.read().rsplit(")", 1)[1].split()
        except (OSError, IndexError):
            continue
        tree[int(fields[1])].append(int(name))
    return tree


def _descendants(pid: int) -> list[int]:
    tree, found, pending = _children(), [], [pid]
    while pending:
        for child in tree.get(pending.pop(), ()):
            found.append(child)
            pending.append(child)
    return found


def _cmdline(pid: int) -> list[str]:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as stream:
            return [part.decode(errors="replace") for part in stream.read().split(b"\0") if part]
    except OSError:
        return []


def _far_host(pid: int) -> str | None:
    """The stand-in ssh destination of a far-side probe process, or ``None`` when *pid* is none."""

    arguments = _cmdline(pid)
    if len(arguments) < 3 or "_probe" not in arguments or "far" not in arguments:
        return None
    try:
        with open(f"/proc/{pid}/environ", "rb") as stream:
            for item in stream.read().split(b"\0"):
                if item.startswith(b"TS_FAR_HOST="):
                    return item.split(b"=", 1)[1].decode()
    except OSError:
        return None
    return "far"


def _busy() -> list[str]:
    """The command lines of running pytest or ``make ci`` processes (never run alongside them)."""

    found = []
    for name in os.listdir("/proc"):
        if name.isdigit() and int(name) != os.getpid():
            line = " ".join(_cmdline(int(name)))
            if re.search(r"(^|[/\s])pytest(\s|$)|-m pytest|\bmake\b.*\bci\b", line):
                found.append(f"{name}: {line[:120]}")
    return found


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def _real_registry() -> Path:
    """The real ``workspaces.json`` of this user, as ``httk.core.userdirs.config_home`` finds it unredirected."""

    override = os.environ.get("HTTK_CONFIG_HOME")
    if override:
        return Path(override).expanduser().resolve() / "workspaces.json"
    base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return (base / "httk").resolve() / "workspaces.json"


def _filesystem(path: Path) -> str:
    try:
        return subprocess.run(
            ["stat", "-f", "-c", "%T", str(path)], capture_output=True, text=True, check=False
        ).stdout.strip()
    except OSError as exc:
        return f"unknown ({exc})"


def _machine() -> dict[str, Any]:
    model = ""
    with contextlib.suppress(OSError):
        for line in Path("/proc/cpuinfo").read_text(encoding="ascii", errors="replace").splitlines():
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    memory = 0
    with contextlib.suppress(OSError, ValueError, IndexError):
        memory = int(Path("/proc/meminfo").read_text().split()[1]) // 1024
    return {
        "cpu": model,
        "cores": os.cpu_count(),
        "memory_mib": memory,
        "kernel": platform.release(),
        "python": platform.python_version(),
        "niceness": os.nice(0),
    }


def _statvfs(path: Path) -> dict[str, int]:
    stats = os.statvfs(path)
    return {
        "used_bytes": (stats.f_blocks - stats.f_bfree) * stats.f_frsize,
        "used_inodes": stats.f_files - stats.f_ffree,
    }


def _tree_usage(path: Path) -> tuple[int, int]:
    """Allocated bytes and inodes of the tree at *path* (directories included)."""

    allocated = inodes = 0
    for directory, names, files in os.walk(path):
        for name in [*names, *files]:
            with contextlib.suppress(OSError):
                allocated += os.lstat(os.path.join(directory, name)).st_blocks * 512
                inodes += 1
        if directory == str(path):
            allocated += os.lstat(directory).st_blocks * 512
            inodes += 1
    return allocated, inodes


def _placement(index: int, scheme: str) -> str:
    return f"bench/p{index % PLACEMENTS:03d}" if scheme == "distributed" else "bench/flat"


# -- making jobs (in worker processes) -------------------------------------------------------------------------------


def _mapping(tag: str, placement: str, priority: int, parent: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": str(uuid.uuid4()),
        "tag": tag,
        "name": "bench job",
        "placement": placement,
        "workflow": {"id": WORKFLOW[0], "name": WORKFLOW[1]},
        "initial_step": "start",
        "priority": priority,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1},
        "resources": {},
        "step_resources": {},
        "parameters": {"script": {"start": "succeed"}},
        "declarations": {},
        "declared": {},
        "environment": {},
        "parent": parent,
        "seal_succeeded": None,
    }


def _make_jobs(root: str, start: int, count: int, scheme: str, tree: bool) -> list[str]:
    """Place *count* jobs (or trees) numbered from *start* in the workspace at *root*: their job ids."""

    from httk.workflow import Workspace, _kernel
    from httk.workflow._job import JobDefinition
    from httk.workflow._state import Release, StateDoc
    from httk.workflow.models import make_job_key

    workspace = Workspace(root, durable=False)  # made quickly; the transfers run durable
    made: list[str] = []
    with _kernel.register_owner(workspace, kind="cli", label="bench setup", allocation=None, advertised={}) as owner:

        def place(mapping: dict[str, Any], state: str, priority: int, children: Iterable[dict[str, Any]] = ()) -> None:
            staging = owner.scratch("submit") / "job"
            staging.mkdir()
            (staging / "job.json").write_bytes(JobDefinition.from_mapping(mapping).encode())
            (staging / "input.txt").write_text(f"payload of {mapping['id']}\n", encoding="utf-8")
            (staging / "data").mkdir()
            for member in range(7):
                (staging / "data" / f"part{member}.txt").write_text(f"{mapping['id']} {member}\n" * 3, "utf-8")
            ref = _kernel.submit(workspace, owner, staging)
            owned = _kernel.claim(workspace, owner, ref)
            assert owned is not None
            doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
            entries = [
                {"job_id": c["id"], "job_key": make_job_key(str(c["id"]), c["tag"]), "placement": c["placement"]}
                for c in children
            ]
            owned.release(doc.with_children(entries), Release(state, priority))
            made.append(str(mapping["id"]))

        def link(parent: dict[str, Any]) -> dict[str, Any]:
            return {
                "workspace_id": workspace.workspace_id,
                "job_id": parent["id"],
                "job_key": make_job_key(str(parent["id"]), parent["tag"]),
                "placement": parent["placement"],
                "activation_id": str(uuid.uuid4()),
                "spawn_id": str(uuid.uuid4()),
            }

        for index in range(start, start + count):
            placement, priority = _placement(index, scheme), 10 + index % 900
            if not tree:
                place(_mapping("solo", placement, priority), "succeeded", priority)
                continue
            root_job = _mapping("root", placement, priority)
            first = _mapping("a", f"{placement}/a", priority, link(root_job))
            second = _mapping("b", f"{placement}/b", priority, link(root_job))
            grand = _mapping("g", f"{placement}/a/g", priority, link(first))
            place(grand, "cancelled", priority)
            place(second, "failed", priority)
            place(first, "succeeded", priority, (grand,))
            place(root_job, "paused", priority, (first, second))
    return made


def _populate(root: Path, start: int, count: int, scheme: str, tree: bool, workers: int) -> tuple[list[str], float]:
    """Make *count* jobs (trees) in parallel worker processes: their ids and the seconds it took."""

    began = time.monotonic()
    if count <= 0:
        return [], 0.0
    workers = max(1, min(workers, count))
    chunk = math.ceil(count / workers)
    made: list[str] = []
    with ProcessPoolExecutor(workers, mp_context=get_context("spawn")) as pool:
        futures = [
            pool.submit(_make_jobs, str(root), offset, min(chunk, start + count - offset), scheme, tree)
            for offset in range(start, start + count, chunk)
        ]
        for future in futures:
            made += future.result()
    return made, time.monotonic() - began


# -- the run --------------------------------------------------------------------------------------------------------


class _Sampler(threading.Thread):
    """Sample the CLI's and its far sides' ``VmRSS`` once a second, and follow the adoptions into DST."""

    def __init__(self, client: int, probes: Path, total: int, log: Path) -> None:
        super().__init__(daemon=True)
        self.client, self.probes, self.total, self.log = client, probes, total, log
        self.samples: list[dict[str, Any]] = []
        self.peaks: dict[int, dict[str, Any]] = {}
        self.stop = threading.Event()
        self.started_at = time.monotonic()

    def adopted(self) -> int:
        return sum(path.stat().st_size // EVENT_WIDTH for path in self.probes.glob("ev-adopt-*.txt"))

    def run(self) -> None:
        last_said = time.monotonic()
        while not self.stop.wait(1.0):
            now = time.monotonic()
            client = _proc_status(self.client)
            far: dict[str, int] = collections.defaultdict(int)
            for pid in _descendants(self.client):
                host = _far_host(pid)
                if host is None:
                    continue
                status = _proc_status(pid)
                if not status:
                    continue
                far[host] += status.get("VmRSS", 0)
                peak = self.peaks.setdefault(pid, {"host": host, "first": now, "vmrss_max_kib": 0})
                peak["last"] = now
                peak["vmrss_max_kib"] = max(peak["vmrss_max_kib"], status.get("VmRSS", 0))
                peak["vmhwm_kib"] = status.get("VmHWM", 0)
            adopted = self.adopted()
            self.samples.append(
                {
                    "t": round(now - self.started_at, 3),
                    "client_kib": client.get("VmRSS"),
                    "far_kib": dict(far),
                    "adopted": adopted,
                }
            )
            if now - last_said >= 60:
                last_said = now
                with open(self.log, "a", encoding="utf-8") as stream:
                    stream.write(f"{now - self.started_at:9.0f}s adopted {adopted}/{self.total}\n")


def _read_stderr(stream: Any, path: Path, started: float, tail: collections.deque[str]) -> None:
    with open(path, "w", encoding="utf-8") as out:
        for raw in stream:
            line = raw.decode(errors="replace").rstrip("\n")
            out.write(f"{time.monotonic() - started:.3f}\t{line}\n")
            tail.append(line)


def _strace_counts(output: Path, jobs: int) -> dict[str, Any]:
    """The syscall counts of an ``strace -c`` table at *output*, in total and per job."""

    counts: dict[str, int] = {}
    with contextlib.suppress(OSError):
        for line in output.read_text(encoding="utf-8", errors="replace").splitlines():
            fields = line.split()
            if len(fields) >= 5 and fields[-1] in SYSCALLS and fields[3].isdigit():
                counts[fields[-1]] = int(fields[3])
    return {
        "measured": bool(counts),
        "jobs": jobs,
        "calls": counts,
        "per_job": {name: round(value / jobs, 3) for name, value in counts.items()},
    }


def _remote(project: Path, name: str, host: str) -> None:
    from httk.workflow.adapters import add_remote

    bundle = add_remote(name, template="ssh", project=project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    settings = dict(metadata["settings"])
    settings.update({"host": host, "username": "someone"})
    metadata["settings"] = settings
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")


def _job_ids(root: Path, states: Iterable[str]) -> set[str]:
    from httk.workflow import Workspace, _kernel

    workspace = Workspace(root, durable=False)
    return {ref.job_id for state in states for ref in _kernel.list_jobs(workspace, state)}


def _page_times(stamps: list[float], started: float) -> list[float]:
    """The completion time (seconds after *started*) of every full page of *stamps*."""

    ordered = sorted(stamps)
    return [round(ordered[index] - started, 4) for index in range(PAGE - 1, len(ordered), PAGE)]


def _read_events(probes: Path, kind: str) -> list[float]:
    stamps: list[float] = []
    for path in probes.glob(f"ev-{kind}-*.txt"):
        stamps += [float(line) for line in path.read_text(encoding="ascii").split()]
    return stamps


def _slope(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 3:
        return None
    mean_x = sum(x for x, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    spread = sum((x - mean_x) ** 2 for x, _ in points)
    if spread == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / spread


def _throughput(pages: list[float], first_adoption: float | None) -> dict[str, Any]:
    """Pages per second over the first and the last 10% of pages (§7.10 6(b))."""

    if first_adoption is None or len(pages) < 3:
        return {"pages": len(pages)}
    ends = [first_adoption, *pages]
    durations = [ends[index + 1] - ends[index] for index in range(len(pages))]
    durations = durations[1:]  # the first page starts at the first adoption, not at a page end
    share = max(1, round(len(durations) / 10))
    first, last = sum(durations[:share]), sum(durations[-share:])
    return {
        "pages": len(pages),
        "window_pages": share,
        "first_jobs_per_second": round(share * PAGE / first, 3) if first > 0 else None,
        "last_jobs_per_second": round(share * PAGE / last, 3) if last > 0 else None,
        "first_over_last": round(last / first, 4) if first > 0 else None,
    }


def _roles(probes: list[dict[str, Any]], direction: str) -> dict[str, dict[str, Any]]:
    """Sum the probe counters by role: ``client``, ``far-src``, ``far-dst`` (``rr``) or ``far``."""

    summed: dict[str, dict[str, Any]] = {}
    for probe in probes:
        role = probe["role"]
        if role == "far" and direction == "rr":
            role = "far-src" if probe["host"].startswith("src.") else "far-dst"
        entry = summed.setdefault(
            role, {"processes": 0, "cpu_seconds": 0.0, "vmhwm_max_kib": 0, "calls": {}, "seconds": {}, "names": {}}
        )
        entry["processes"] += 1
        entry["cpu_seconds"] = round(entry["cpu_seconds"] + probe["cpu_user"] + probe["cpu_system"], 3)
        entry["vmhwm_max_kib"] = max(entry["vmhwm_max_kib"], probe.get("vmhwm_kib") or 0)
        for key in ("calls", "seconds", "names"):
            for name, value in probe[key].items():
                entry[key][name] = round(entry[key].get(name, 0) + value, 6)
    return summed


def _shares(roles: dict[str, dict[str, Any]], wall: float, jobs: int) -> dict[str, Any]:
    def total(key: str, *names: str) -> float:
        return sum(entry[key].get(name, 0) for entry in roles.values() for name in names)

    settle = total("seconds", "settle_pause")
    barrier = total("seconds", "fsync", "fdatasync", "syncfs")
    commits = total("seconds", "barrier_commit")
    listing = total("seconds", "listdir", "scandir")
    kernel_listing = total("seconds", "kernel_list_names", "kernel_subdirectories")
    names = total("names", "listdir", "scandir")
    kernel_names = total("names", "kernel_list_names", "kernel_subdirectories")
    per_job = {
        name: round(total("calls", name) / jobs, 3)
        for name in ("listdir", "scandir", "fsync", "syncfs", "mkdir", "rename", "replace", "os_open", "open")
    }
    return {
        "settle_wait_thread_seconds": round(settle, 3),
        "settle_wait_share": round(settle / wall, 4),
        "fsync_and_syncfs_thread_seconds": round(barrier, 3),
        "barrier_share": round(barrier / wall, 4),
        "barrier_commit_thread_seconds": round(commits, 3),
        "listing_thread_seconds": round(listing, 3),
        "listing_share": round(listing / wall, 4),
        "kernel_listing_thread_seconds": round(kernel_listing, 3),
        "names_examined": int(names),
        "names_examined_per_job": round(names / jobs, 2),
        "kernel_names_examined": int(kernel_names),
        "kernel_names_examined_per_job": round(kernel_names / jobs, 2),
        "python_calls_per_job": per_job,
    }


def _rss_slopes(
    samples: list[dict[str, Any]], peaks: dict[int, dict[str, Any]], total: int, wall: float
) -> dict[str, Any]:
    """Bytes of RSS a job, after warm-up (10% adopted) until every bundle was adopted, of the client and of every
    far side that lived at least half the run (``None`` when too few samples)."""

    window = [sample for sample in samples if total * 0.1 <= sample["adopted"] < total]
    slopes: dict[str, Any] = {}
    client = [(s["adopted"], s["client_kib"] * 1024) for s in window if s.get("client_kib")]
    slopes["client"] = _slope(client)
    hosts = {peak["host"] for peak in peaks.values() if peak["last"] - peak["first"] >= wall / 2}
    for host in sorted(hosts):
        points = [(s["adopted"], s["far_kib"][host] * 1024) for s in window if s["far_kib"].get(host)]
        slopes[f"far:{host}"] = _slope(points)
    return {name: None if value is None else round(value, 1) for name, value in slopes.items()}


def _caveat(jobs: int, why: str) -> str | None:
    """Why a run of *jobs* is too small for (b) or (c) to mean much, or ``None``."""

    if jobs >= CONCLUSIVE_JOBS:
        return None
    return f"N = {jobs} is too small to establish it ({why}); §7.10 sets it on N = 10^5 and 10^6"


def _acceptance(result: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate §7.10 6(a)-(e) for one valid *result*: each ``pass``, ``fail`` or ``not measured``, with numbers."""

    p = result["parameters"]
    verdicts: dict[str, Any] = {}
    deadline, wall = p["deadline"], result["wall_seconds"]
    if deadline <= 0.05:
        verdicts["a"] = {"verdict": "not measured", "why": "this run is the copy-time (D = 0.05 s) run"}
    else:
        same = [
            other
            for other in results
            if other.get("valid")
            and other["parameters"]["deadline"] <= 0.05
            and all(
                other["parameters"].get(key) == p[key]
                for key in ("direction", "placement", "jobs", "prepopulate", "tree", "probe")
            )
        ]
        if not same:
            verdicts["a"] = {"verdict": "not measured", "why": "no matching D = 0.05 s run"}
        else:
            copy = min(other["wall_seconds"] for other in same)
            bound = 1.1 * copy + 2 * deadline
            verdicts["a"] = {
                "verdict": "pass" if wall <= bound else "fail",
                "wall": wall,
                "copy": copy,
                "bound": round(bound, 2),
                "over_copy": round(wall - copy, 2),
            }
    throughput = result["throughput"]
    ratio = throughput.get("first_over_last")
    if ratio is None:
        verdicts["b"] = {"verdict": "not measured", "why": "fewer than three pages"}
    else:
        verdict = "pass" if ratio <= 1.2 else "fail"
        if verdict == "fail" and p["placement"] == "flat":
            verdict = "fail (flat: recorded as a limitation)"
        if p["jobs"] < CONCLUSIVE_JOBS:
            verdict = f"indicative {verdict}"
        verdicts["b"] = {
            "verdict": verdict,
            "caveat": _caveat(p["jobs"], f"a 10% window of {throughput.get('window_pages')} page(s)"),
            "last_page_time_over_first": ratio,
            "first_jobs_per_second": throughput.get("first_jobs_per_second"),
            "last_jobs_per_second": throughput.get("last_jobs_per_second"),
            "names_examined_per_job": result["shares"]["names_examined_per_job"],
        }
    slopes = {name: value for name, value in result["rss_slope_bytes_per_job"].items() if value is not None}
    if not slopes:
        verdicts["c"] = {"verdict": "not measured", "why": "too few RSS samples after warm-up"}
    else:
        worst = max(slopes.values())
        verdict = "pass" if worst <= DECLARED_BYTES_PER_JOB else "fail"
        allowed = DECLARED_BYTES_PER_JOB * p["jobs"] / 2**20
        verdicts["c"] = {
            "verdict": f"indicative {verdict}" if p["jobs"] < CONCLUSIVE_JOBS else verdict,
            "caveat": _caveat(p["jobs"], f"the declared structures allow only {allowed:.1f} MiB of growth"),
            "slopes_bytes_per_job": slopes,
            "declared_bytes_per_job": DECLARED_BYTES_PER_JOB,
        }
    strace = result.get("strace") or {}
    if strace.get("measured"):
        verdicts["d"] = {"verdict": "measured (compare across N)", "per_job": strace["per_job"]}
    else:
        verdicts["d"] = {
            "verdict": "not measured",
            "why": strace.get("reason", "no --strace-subset"),
            "python_calls_per_job": result["shares"]["python_calls_per_job"],
        }
    expected = result["expected_counts"]
    counts = [result["connections"], result["starts"]]
    verdicts["e"] = {
        "verdict": "pass" if counts == list(expected) else "fail",
        "counts": counts,
        "expected": list(expected),
    }
    return verdicts


def _ends(direction: str, work: Path) -> tuple[Path, Path]:
    remote = work / "remote"
    return {
        "push": (work / "near", remote / "far"),
        "pull": (remote / "far", work / "near"),
        "rr": (remote / "near", remote / "far"),
        "local": (work / "near", work / "far"),
    }[direction]


def _locators(direction: str, names: tuple[str, str]) -> tuple[str, str]:
    source, destination = names
    return {
        "push": (source, f"cluster:{destination}"),
        "pull": (f"cluster:{source}", destination),
        "rr": (f"cluster:{source}", f"other:{destination}"),
        "local": (source, destination),
    }[direction]


def run(arguments: argparse.Namespace) -> int:
    out = Path(arguments.out).expanduser().resolve()
    if out == Path("/tmp") or Path("/tmp") in out.parents:
        raise SystemExit("transfer_scale: --out must not be below /tmp")
    p = {
        "direction": arguments.direction,
        "jobs": arguments.jobs,
        "placement": arguments.placement,
        "deadline": arguments.deadline,
        "prepopulate": arguments.prepopulate,
        "tree": arguments.tree,
        "strace_subset": arguments.strace_subset,
        "probe": arguments.probe,
        "io_sync": arguments.io_sync,
    }
    stamp = time.strftime("%Y%m%dT%H%M%S")
    run_id = (
        f"{p['direction']}-{p['placement']}-n{p['jobs']}-d{p['deadline']:g}"
        f"-m{p['prepopulate']}{'-tree' if p['tree'] else ''}{'-light' if p['probe'] == 'light' else ''}"
        f"{'' if p['io_sync'] == 'auto' else '-' + p['io_sync']}-{stamp}"
    )
    work, logs, results = out / "work" / run_id, out / "logs" / run_id, out / "results"
    probes = logs / "probes"
    for directory in (work / "config", work / "data", work / "tmp", work / "remote", work / "bin", probes, results):
        directory.mkdir(parents=True, exist_ok=True)

    real = _real_registry()
    before = _sha256(real)
    environment = {
        "HTTK_CONFIG_HOME": str(work / "config"),
        "HTTK_DATA_HOME": str(work / "data"),
        "TMPDIR": str(work / "tmp"),
        "PATH": f"{work / 'bin'}{os.pathsep}{os.environ['PATH']}",
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH", "")])),
        "HTTK_FAKE_SSH_ROOT": str(work / "remote"),
        "HTTK_FAKE_SSH_LOG": str(logs / "ssh.jsonl"),
        "TS_PROBE_DIR": str(probes),
        "TS_PROBE": arguments.probe,
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
    }
    os.environ.update(environment)
    import tempfile

    tempfile.tempdir = str(work / "tmp")
    (work / "bin" / "ssh").write_text(FAKE_SSH.format(python=sys.executable), encoding="utf-8")
    (work / "bin" / "httk").write_text(HTTK_SHIM.format(python=sys.executable, script=SCRIPT), encoding="utf-8")
    for name in ("ssh", "httk"):
        (work / "bin" / name).chmod(0o755)

    from httk.workflow import Workspace, _fs, _kernel, _moving
    from httk.workflow.projects import initialize_project
    from httk.workflow.registry import create_workspace, workspaces_path
    from httk.workflow.workflow_cli import _transfer

    registry = workspaces_path().resolve()
    if work not in registry.parents or registry == real.resolve():
        raise SystemExit(f"transfer_scale: the httk config is not redirected (registry {registry}); refusing to run")
    if _transfer._PAGE_HOLDS != PAGE:
        raise SystemExit(f"transfer_scale: the CLI's page is {_transfer._PAGE_HOLDS} holds, not {PAGE}")

    while busy := _busy():
        print(f"transfer_scale: waiting for {len(busy)} pytest/make ci process(es): {busy[0]}", flush=True)
        time.sleep(30)

    project = work / "project"
    initialize_project(project, name="bench")
    if p["direction"] in ("push", "pull"):
        _remote(project, "cluster", "fake.example.test")
    elif p["direction"] == "rr":
        _remote(project, "cluster", "src.example.test")
        _remote(project, "other", "dst.example.test")
    source_root, destination_root = _ends(p["direction"], work)
    names = (f"src-{uuid.uuid4().hex[:8]}", f"dst-{uuid.uuid4().hex[:8]}")
    for name, root in zip(names, (source_root, destination_root), strict=True):
        create_workspace(name, root)
        Workspace(root).set_policy({"visibility_deadline_seconds": p["deadline"]})
        if p["io_sync"] != "auto":
            Workspace(root).set_setting("io.sync", p["io_sync"])
    os.environ["TS_SRC_ROOT"], os.environ["TS_DST_ROOT"] = str(source_root), str(destination_root)
    policy = {}
    for label, root in (("src", source_root), ("dst", destination_root)):
        workspace = Workspace(root)
        policy[label] = {
            "durable": workspace.durable,
            "visibility_deadline": workspace.visibility_deadline,
            "io_workers": workspace.io_workers,
            "io_sync": workspace.io_sync(),
            "sync_strategy": _fs.resolve_sync(root, workspace.io_sync())[1],
        }

    statvfs_start = _statvfs(work)
    expected, create_seconds = _populate(source_root, 0, p["jobs"], p["placement"], p["tree"], arguments.setup_workers)
    _, prepopulate_seconds = _populate(
        destination_root, p["jobs"], p["prepopulate"], p["placement"], False, arguments.setup_workers
    )
    began = time.monotonic()
    os.sync()
    sync_seconds = time.monotonic() - began
    statvfs_setup = _statvfs(work)
    sample = sorted(_job_ids(source_root, ["succeeded", "paused"]))[:1]
    one = (
        _kernel.locate(Workspace(source_root, durable=False), sample[0], placement_hint=None, exhaustive=True)
        if sample
        else None
    )
    job_bytes, job_inodes = _tree_usage(one.path) if one is not None else (0, 0)
    setup = {
        "create_seconds": round(create_seconds, 2),
        "create_seconds_per_job": round(create_seconds / max(1, len(expected)), 6),
        "prepopulate_seconds": round(prepopulate_seconds, 2),
        "sync_seconds": round(sync_seconds, 2),
        "setup_workers": arguments.setup_workers,
        "job_allocated_bytes": job_bytes,
        "job_inodes": job_inodes,
        "statvfs_setup_delta": {key: statvfs_setup[key] - statvfs_start[key] for key in statvfs_start},
    }
    print(f"transfer_scale: {run_id}: {len(expected)} jobs made in {create_seconds:.0f} s; transferring", flush=True)

    source_locator, destination_locator = _locators(p["direction"], names)
    state = "paused" if p["tree"] else "succeeded"
    command = [sys.executable, str(SCRIPT), "_probe", "client", "job", "transfer", "--json", "--state", state]
    command += ["--tree"] if p["tree"] else []
    command += [source_locator, destination_locator]
    stdout_path = logs / "stdout.json"
    tail: collections.deque[str] = collections.deque(maxlen=200)
    started = time.monotonic()
    with open(stdout_path, "wb") as stdout:
        client = subprocess.Popen(command, cwd=project, stdout=stdout, stderr=subprocess.PIPE)
        reader = threading.Thread(
            target=_read_stderr, args=(client.stderr, logs / "progress.log", started, tail), daemon=True
        )
        reader.start()
        sampler = _Sampler(client.pid, probes, len(expected), logs / "driver.log")
        sampler.started_at = started
        strace: dict[str, Any] = {"measured": False, "reason": "no --strace-subset"}
        if p["strace_subset"] and not shutil.which("strace"):
            strace = {"measured": False, "reason": "strace is not installed (not on PATH); not measured"}
        elif p["strace_subset"]:
            strace = {"measured": False, "reason": "the measured run was invalid; no traced subset"}
        sampler.start()
        _, status, usage = os.wait4(client.pid, 0)
        wall = time.monotonic() - started
        client.returncode = os.waitstatus_to_exitcode(status)
        sampler.stop.set()
        sampler.join()
        reader.join(timeout=60)

    code = client.returncode
    probe_documents = [json.loads(path.read_text(encoding="utf-8")) for path in probes.glob("probe-*.json")]
    adopt_stamps = _read_events(probes, "adopt")
    hold_stamps = _read_events(probes, "hold")
    release_stamps = _read_events(probes, "release")
    ssh_lines = []
    with contextlib.suppress(FileNotFoundError):
        ssh_lines = [json.loads(line) for line in (logs / "ssh.jsonl").read_text(encoding="utf-8").splitlines()]
    connections = len(ssh_lines)
    starts = sum(1 for line in ssh_lines if re.search(r"(^|\s)httk\s", line["command"]))

    problems: list[str] = []
    if code != 0:
        problems.append(f"exit status {code}")
    statuses: collections.Counter[str] = collections.Counter()
    try:
        document = json.loads(stdout_path.read_text(encoding="utf-8"))
        statuses.update(outcome["status"] for outcome in document["transfers"])
    except (ValueError, KeyError, TypeError) as exc:
        problems.append(f"no outcome document ({exc})")
    if statuses != collections.Counter({"adopted": p["jobs"]}):
        problems.append(f"outcomes {dict(statuses)}, not {p['jobs']} adopted")
    states = ("paused", "succeeded", "failed", "cancelled") if p["tree"] else ("succeeded",)
    arrived = _job_ids(destination_root, states)
    if missing := set(expected) - arrived:
        problems.append(f"{len(missing)} job(s) missing in DST")
    left = _job_ids(source_root, sorted(_kernel.UNOWNED_STATES))
    if stayed := set(expected) & left:
        problems.append(f"{len(stayed)} job(s) still in SRC")
    if held := _moving.held_names(Workspace(source_root, durable=False)):
        problems.append(f"{len(held)} hold(s) left on SRC")
    if len(adopt_stamps) != p["jobs"]:
        problems.append(f"the probe saw {len(adopt_stamps)} adoptions into DST, not {p['jobs']}")
    if not any(probe["role"] == "client" for probe in probe_documents):
        problems.append("no client probe dump")
    expected_counts = EXPECTED_COUNTS[p["direction"]]
    if (connections, starts) != expected_counts:
        problems.append(f"connections/starts {connections}/{starts}, not {expected_counts[0]}/{expected_counts[1]}")

    first_adoption = min(adopt_stamps) - started if adopt_stamps else None
    pages = _page_times(adopt_stamps, started)
    roles = _roles(probe_documents, p["direction"])
    result: dict[str, Any] = {
        "run": run_id,
        "valid": not problems,
        "problems": problems,
        "parameters": p,
        "machine": _machine(),
        "filesystem": _filesystem(work),
        "policy": policy,
        "setup": setup,
        "command": command[3:],
        "exit_status": code,
        "wall_seconds": round(wall, 3),
        "seconds_per_job": round(wall / max(1, p["jobs"]), 6),
        "first_adoption_seconds": None if first_adoption is None else round(first_adoption, 3),
        "page_source": "timestamps of _moving.adopt into DST (every 256th), from the adopting process's probe",
        "adopt_page_seconds": pages,
        "hold_page_seconds": _page_times(hold_stamps, started),
        "release_page_seconds": _page_times(release_stamps, started),
        "throughput": _throughput(pages, first_adoption),
        "shares": _shares(roles, wall, max(1, p["jobs"])),
        "roles": roles,
        "client_rusage_with_reaped_children": {"user": usage.ru_utime, "system": usage.ru_stime},
        "vmhwm_kib": {
            "client": max((d.get("vmhwm_kib") or 0 for d in probe_documents if d["role"] == "client"), default=None),
            "far_max": max((d.get("vmhwm_kib") or 0 for d in probe_documents if d["role"] == "far"), default=None),
        },
        "rss_samples": sampler.samples,
        "rss_processes": {str(pid): peak for pid, peak in sampler.peaks.items()},
        "rss_slope_bytes_per_job": _rss_slopes(sampler.samples, sampler.peaks, p["jobs"], wall),
        "connections": connections,
        "starts": starts,
        "expected_counts": expected_counts,
        "strace": strace,
        "stderr_tail": list(tail)[-20:],
        "statvfs_after_run_delta": {key: _statvfs(work)[key] - statvfs_start[key] for key in statvfs_start},
    }

    if p["strace_subset"] and shutil.which("strace") and not problems:
        result["strace"] = _traced_subset(arguments, p, project, source_root, names, logs)

    others, _skipped = load_results(results)
    if result["valid"]:
        result["acceptance"] = _acceptance(result, others)
    after = _sha256(real)
    result["real_registry"] = {"path": str(real), "sha256_before": before, "sha256_after": after}
    if before != after:
        result["valid"] = False
        result["problems"].append("the real workspaces.json changed")
    (results / f"{run_id}.json").write_text(json.dumps(result, indent=1, sort_keys=True), encoding="utf-8")
    if not arguments.keep_work:
        shutil.rmtree(work, ignore_errors=True)
    verdict = "VALID" if result["valid"] else f"INVALID ({'; '.join(result['problems'])})"
    print(f"transfer_scale: {run_id}: {verdict}, wall {wall:.1f} s", flush=True)
    if result["valid"]:
        for key, value in result["acceptance"].items():
            print(f"  ({key}) {value['verdict']}: {json.dumps({k: v for k, v in value.items() if k != 'verdict'})}")
    if before != after:
        raise SystemExit("transfer_scale: the real workspaces.json changed")
    return 0 if result["valid"] else 1


def _traced_subset(
    arguments: argparse.Namespace,
    p: dict[str, Any],
    project: Path,
    source_root: Path,
    names: tuple[str, str],
    logs: Path,
) -> dict[str, Any]:
    """One more transfer of ``--strace-subset`` K jobs into the DST the run filled, traced with ``strace -f -c``
    for its whole duration: its per-job syscall counts (the DST holds N + M jobs, so they compare across N)."""

    count = p["strace_subset"]
    _populate(source_root, p["jobs"] + p["prepopulate"], count, p["placement"], p["tree"], arguments.setup_workers)
    output = logs / "strace.txt"
    source_locator, destination_locator = _locators(p["direction"], names)
    state = "paused" if p["tree"] else "succeeded"
    command = ["strace", "-f", "-c", "-qq", "-o", str(output), "-e", f"trace={','.join(SYSCALLS)}"]
    command += [sys.executable, str(SCRIPT), "_probe", "client", "job", "transfer", "--json", "-q", "--state", state]
    command += ["--tree"] if p["tree"] else []
    command += [source_locator, destination_locator]
    completed = subprocess.run(command, cwd=project, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False)
    if completed.returncode != 0:
        return {"measured": False, "reason": f"the traced transfer failed: {completed.stderr[-500:]!r}"}
    return _strace_counts(output, count)


# -- the summary ----------------------------------------------------------------------------------------------------


def _current(result: object) -> bool:
    """Whether *result* is a result of this benchmark since transfer sessions are required: not one of a run with
    ``--session off``, nor one that recorded no ``expected_counts`` (both before §7.12, T7)."""

    if not isinstance(result, dict) or not isinstance(result.get("parameters"), dict):
        return False
    return result["parameters"].get("session") != "off" and result.get("expected_counts") is not None


def load_results(directory: Path) -> tuple[list[dict[str, Any]], int]:
    """Read the results in *directory* (``results/``): the current ones, by name, and how many others were skipped
    (:func:`_current`, or not readable as JSON)."""

    current: list[dict[str, Any]] = []
    skipped = 0
    for path in sorted(directory.glob("*.json")):
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            result = None
        if _current(result):
            current.append(result)
        else:
            skipped += 1
    return current, skipped


def _verdict(result: dict[str, Any], key: str) -> str:
    value = str(result.get("acceptance", {}).get(key, {}).get("verdict", "-"))
    mark = "~" if value.startswith("indicative ") else ""
    value = value.removeprefix("indicative ")
    return mark + {"not measured": "n/m", "fail (flat: recorded as a limitation)": "fail*"}.get(value, value)


def summarize(out: Path) -> Path:
    results, skipped = load_results(out / "results")
    for result in results:
        if result.get("valid"):
            result["acceptance"] = _acceptance(result, results)  # (a) once every D = 0.05 s run is in
    lines = [
        "# Transfer scale results",
        "",
        "One row a run (`benchmarks/transfer_scale.py`). Shares are thread-seconds over the wall time (the pools of",
        "`io.workers` can make them exceed 1). n/m: not measured. fail*: flat placement, recorded as a limitation.",
        "~: indicative only (N below 10^5: too few pages for (b), too little declared growth for (c)).",
        f"Skipped {skipped} result(s) from before transfer sessions were required (`session` off, or no",
        "`expected_counts`), or not readable: not listed and never matched as a baseline.",
        "",
        (
            "| run | valid | wall s | ms/job | first adopt s | settle | barrier | listing | names/job "
            "| client HWM MiB | far HWM MiB | conn/starts | a | b | c | d | e |"
        ),
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for result in results:
        shares = result.get("shares", {})
        hwm = result.get("vmhwm_kib", {})

        def mib(value: object) -> str:
            return f"{value / 1024:.0f}" if isinstance(value, (int, float)) and value else "-"

        lines.append(
            f"| {result['run']} | {'yes' if result['valid'] else 'NO'} | {result['wall_seconds']:.0f} "
            f"| {1000 * result['seconds_per_job']:.1f} | {result.get('first_adoption_seconds')} "
            f"| {shares.get('settle_wait_share', '-')} | {shares.get('barrier_share', '-')} "
            f"| {shares.get('listing_share', '-')} | {shares.get('names_examined_per_job', '-')} "
            f"| {mib(hwm.get('client'))} | {mib(hwm.get('far_max'))} "
            f"| {result['connections']}/{result['starts']} | {_verdict(result, 'a')} | {_verdict(result, 'b')} "
            f"| {_verdict(result, 'c')} | {_verdict(result, 'd')} | {_verdict(result, 'e')} |"
        )
    invalid = [result for result in results if not result["valid"]]
    if invalid:
        lines += ["", "## Invalid runs", ""]
        lines += [f"- {result['run']}: {'; '.join(result['problems'])}" for result in invalid]
    path = out / "summary.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    parser.add_argument("--out", required=True, help="the directory of the results, logs and work (never /tmp)")
    parser.add_argument("--summarize", action="store_true", help="write OUT/summary.md from OUT/results/ and stop")
    parser.add_argument("--direction", choices=("push", "pull", "rr", "local"), default="push")
    parser.add_argument("--jobs", type=int, default=200, help="N: the bundles (or trees) moved")
    parser.add_argument("--placement", choices=("flat", "distributed"), default="distributed")
    parser.add_argument("--deadline", type=float, default=5.0, help="D: the visibility deadline in seconds")
    parser.add_argument("--prepopulate", type=int, default=0, help="M: jobs placed in DST first")
    parser.add_argument("--tree", action="store_true", help="move N trees of four jobs with --tree")
    parser.add_argument("--strace-subset", type=int, default=0, metavar="K", help="trace K more jobs (strace)")
    parser.add_argument("--setup-workers", type=int, default=8, help="processes making the jobs")
    parser.add_argument("--keep-work", action="store_true", help="keep the run's workspaces")
    parser.add_argument(
        "--io-sync", choices=("auto", "syncfs", "fsync"), default="auto", help="the workspaces' io.sync setting"
    )
    parser.add_argument(
        "--probe",
        choices=("full", "light"),
        default="full",
        help="full: time and count the calls (default); light: only the adoption, hold and release events",
    )
    return parser


def main() -> int:
    if len(sys.argv) > 2 and sys.argv[1] == "_probe":
        return _probe_main(sys.argv[2], sys.argv[3:])
    arguments = _parser().parse_args()
    if arguments.summarize:
        print(summarize(Path(arguments.out).expanduser().resolve()))
        return 0
    if arguments.jobs < 1 or arguments.prepopulate < 0 or arguments.deadline < 0:
        raise SystemExit("transfer_scale: --jobs must be positive, --prepopulate and --deadline not negative")
    return run(arguments)


if __name__ == "__main__":
    sys.exit(main())
