"""Measure local many-job ``httk job`` commands: the L0b benchmark (plan WORKFLOW_LOCAL_COMMANDS §2, L0b).

This is a script, not a test. One invocation measures, on N jobs in a local workspace, the commands::

    job new --workspace WS --workflow URI --install --input-from structure DIR --duplicate-tag skip
    job request cancel --reason bench SEL...      (on the N fresh ready jobs)
    job delete --yes SEL...                       (on the N cancelled jobs)
    job request pause --reason bench SEL...       (on N fresh ready jobs, a second workspace)
    job request continue --reason bench SEL...    (on the N paused jobs)

and writes one JSON result::

    nice -n 10 python benchmarks/local_scale.py --out DIR --jobs 100 --selector prefix --io-sync auto \\
        [--backlog] [--repeat 3] [--traced 1] [--commands new,cancel,delete,pause,continue]
    python benchmarks/local_scale.py --out DIR --summarize

``--selector`` names the jobs on the command line: ``prefix`` (``si-NNN--``, the readable half of each job key),
``full`` (the job ids) or ``state`` (``--state ready``, ``--state paused``). ``job delete`` has no ``--state``, so
under ``state`` it is given the full ids, and the result says so. ``--io-sync`` is the workspaces' ``io.sync``.
``--backlog`` posts one ``set_priority`` request to every job before the measured cancel, without serving it (the
CLI always serves what it posts, so the driver posts with the CLI's own ``publish_job_requests``), so that the cancel
finds an older pending request on every job. ``job recover --all`` is not measured (see ``NOT_MEASURED``).

A repetition is a chain on fresh state: a new project and workspace (``project init``, ``workspace init``,
``workflow install`` of the local ``git+file`` workflow, the ``io.sync`` setting), then ``new``, ``cancel`` and
``delete`` in it, and a second fresh workspace populated by an unmeasured ``new`` for ``pause`` and ``continue``.
``--repeat K`` runs K plain chains, whose commands each run in a fresh subprocess timed from outside (the median is
reported; ``--repeat 0`` makes a traced-only run), and ``--traced T`` (default 1) more chains whose commands run
in-process traced (``_probe``): ``os.fsync``, ``os.fdatasync`` and ``_fs._syncfs`` are wrapped to count and time
every call and to keep its interval, from which the critical-path sync time is the wall-clock union of the
intervals (overlapping pooled calls count once); ``os.scandir``/``os.listdir`` count the directory entries they
return (the entries examined),
``glob.glob``/``glob.iglob``/``Path.glob``/``Path.rglob`` their calls and matches (their listings are already in the
scandir count); ``_fs.resolve_sync`` records the strategy each barrier resolved. The driver also records what
``_fs.resolve_sync`` gives for the workspace.

Validity (a run is INVALID when any check fails, and is reported as such): every command exits 0; its output
reports exactly N outcomes of the expected kind (``submitted N jobs``; N ``applied`` lines with the action;
N ``removed`` lines and ``removed N of N job(s)``); afterwards all N jobs are in the expected state and no other job
is anywhere, both as the kernel lists them and as an independent walk of the workspace finds them, which also
rejects any leftover: an entry under ``jobs/`` that is not a well-formed job directory (the benchmark's jobs have no
placement), anything in ``jobs/owned/``, and anything in ``.httk-workspace/{tmp,owners,requests,quarantine,
exchange-jobs}`` (scratch, trash, owner records, request files); under ``--backlog``, immediately before the cancel,
``requests/`` holds exactly one pending request a job, a ``set_priority`` to the backlog priority signed by the
run's operator, and after it every cancelled job has that priority (the older request was applied first); every
CLI process imported ``httk.workflow`` from this checkout's ``src``, and the ``src`` fingerprint (the source key)
is the run's before and after every measured command and at the end; and the real ``workspaces.json`` has the
same sha256 before and after. The benchmark sets ``HTTK_CONFIG_HOME`` and ``HTTK_DATA_HOME`` below the run's work
directory itself; before writing anything it refuses an ``--out`` inside the real configuration or data
directories, and it refuses to run when the httk user directories do not resolve below ``--out``.

Acceptance (plan §1): wall at most 2.5 s at N = 100 for ``new``, ``cancel`` and ``delete``; critical-path sync time
at most max(0.250 s, 10% of the wall), both of the traced run, compared unrounded; per-job cost at N = 1,000 at
most 1.25 times that at N = 100.

Everything lives below ``--out`` (never ``/tmp``): ``results/<run>.json``, ``logs/<run>/`` (each command's output)
and ``work/<run>/`` (deleted after the run unless ``--keep-work``). Each result records ``git rev-parse HEAD`` and
whether ``src/`` is dirty (with a digest of its changes); ``--summarize`` groups by that source key, and computes the
per-job flatness from N = 100 to N = 1,000 within a group.
"""

import argparse
import collections
import contextlib
import glob
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Self, TypeGuard

SCRIPT = Path(__file__).resolve()
REPOSITORY = SCRIPT.parents[1]
SOURCE = REPOSITORY / "src"
#: The default inputs: the workspace's si100 structures and the local workflows-vasp checkout (never the network).
STRUCTURES = REPOSITORY.parents[2] / "test" / "si100-project" / "structures"
WORKFLOW = f"git+file://{REPOSITORY.parent / 'workflows-vasp'}#vasp-static"
#: The result format of this benchmark (``--summarize`` skips anything else).
BENCHMARK, FORMAT_VERSION = "local_scale", 2
#: The formats ``--summarize`` reads. Format 1 results predate the per-command source fingerprint, the filesystem
#: census and the backlog precheck; the summary marks them.
FORMATS = (1, 2)
COMMANDS = ("new", "cancel", "delete", "pause", "continue")
#: The measured request commands: their action and the state ``--state`` selects them from.
REQUESTS = {"cancel": "ready", "pause": "ready", "continue": "paused"}
#: Where every job is after each command (``None``: gone).
AFTER: dict[str, str | None] = {"new": "ready", "cancel": "cancelled", "delete": None, "pause": "paused"}
AFTER["continue"] = "ready"
NOT_MEASURED = {
    "recover": (
        "job recover --all needs away copies, which only a job push or job transfer --keep into another workspace "
        "makes; at N = 100 that setup costs about 41 fsyncs a job (audit item F), several times the recover itself, "
        "on every repetition"
    ),
}
BACKLOG_PRIORITY = 321
#: Plan §1 targets: wall at N = 100 for these commands, the critical-path sync share, the per-job flatness.
WALL_TARGET_SECONDS, WALL_TARGET_JOBS, WALL_TARGET_COMMANDS = 2.5, 100, ("new", "cancel", "delete")
SYNC_SHARE_TARGET, SYNC_FLOOR_SECONDS = 0.10, 0.250
FLATNESS_TARGET, FLATNESS_SIZES = 1.25, (100, 1000)
#: The real registry's sha256 at the time this benchmark was written (informational: the guard is before == after).
KNOWN_REGISTRY_SHA256 = "e5cd22bfc3e594d9023fd53897aab3710ce2856de5c6cf07cfbac0d476b848ac"
SYNC_KINDS = ("fsync", "fdatasync", "syncfs")
OPERATOR = ("Bench Operator", "bench@example.invalid")
#: A job directory below ``jobs/<state>/``: ``<tag>--<job id>~p<priority>~<token>``.
JOB_DIRECTORY = re.compile(
    r"(?P<key>.+--(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}))~p[0-9]{3}~[a-z2-7]{16}"
)
#: A request file below ``.httk-workspace/requests/``: ``<job id>.<request id>.json``.
REQUEST_FILE = re.compile(r"(?P<job>[0-9a-f-]{36})\.(?P<request>[0-9a-f-]{36})\.json")
#: The control directories that hold nothing once a command is done: scratch and trash, owner records, requests,
#: quarantine and the exchange index.
CONTROL_EMPTY = ("tmp", "owners", "requests", "quarantine", "exchange-jobs")
#: Processes that distort the timing: test runs, ``make ci`` and other benchmark runs.
BUSY = r"(^|[/\s])pytest(\s|$)|-m pytest|\bmake\b.*\bci\b|transfer_scale\.py|local_scale\.py"


# -- the probe: runs inside each measured CLI process ---------------------------------------------------------------


def critical_path(intervals: Iterable[tuple[float, float]]) -> float:
    """The wall-clock length of the union of *intervals* (``(start, end)`` pairs): overlaps count once."""

    total = 0.0
    current: list[float] | None = None
    for start, end in sorted(intervals):
        if current is None or start > current[1]:
            if current is not None:
                total += current[1] - current[0]
            current = [start, end]
        else:
            current[1] = max(current[1], end)
    if current is not None:
        total += current[1] - current[0]
    return total


class _Listing:
    """``os.scandir``'s iterator, counting the entries it yields."""

    def __init__(self, probe: "_Probe", iterator: Any) -> None:
        self._probe, self._iterator, self._names, self._done = probe, iterator, 0, False

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Any:
        try:
            entry = next(self._iterator)
        except StopIteration:
            self._flush()
            raise
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
            self._probe.add("scandir", names=self._names)


class _Probe:
    """Run-time instrumentation of one CLI process (see the module docstring); dumped as JSON at exit."""

    def __init__(self) -> None:
        self.lock = threading.RLock()  # a listing's finalizer may run (and count) while the lock is held
        self.calls: collections.Counter[str] = collections.Counter()
        self.seconds: collections.defaultdict[str, float] = collections.defaultdict(float)
        self.names: collections.Counter[str] = collections.Counter()
        self.intervals: list[tuple[float, float]] = []
        self.strategies: collections.Counter[str] = collections.Counter()
        self.patched: list[str] = []

    def add(self, name: str, spent: float = 0.0, *, names: int | None = None) -> None:
        with self.lock:
            self.calls[name] += 1
            self.seconds[name] += spent
            if names is not None:
                self.names[name] += names

    def synced[**P, R](self, name: str, function: Callable[P, R]) -> Callable[P, R]:
        def wrapper(*arguments: P.args, **options: P.kwargs) -> R:
            start = time.perf_counter()
            try:
                return function(*arguments, **options)
            finally:
                end = time.perf_counter()
                with self.lock:
                    self.calls[name] += 1
                    self.seconds[name] += end - start
                    self.intervals.append((start, end))

        return wrapper

    def listed(self, name: str, function: Callable[..., list[Any]]) -> Callable[..., list[Any]]:
        def wrapper(*arguments: Any, **options: Any) -> list[Any]:
            result = function(*arguments, **options)
            self.add(name, names=len(result))
            return result

        return wrapper

    def matched(self, name: str, function: Callable[..., Iterable[Any]]) -> Callable[..., Iterator[Any]]:
        def wrapper(*arguments: Any, **options: Any) -> Iterator[Any]:
            self.add(name)
            for item in function(*arguments, **options):
                with self.lock:
                    self.names[name] += 1
                yield item

        return wrapper

    def install_listings(self) -> None:
        """Count the entries every directory listing returns (standard library only).

        ``os.scandir`` and ``os.listdir`` are replaced, and so is any reference to the real ``os.scandir`` that the
        standard library captured at import: Python 3.13's ``glob._StringGlobber.scandir``, through which
        ``Path.glob`` and ``Path.rglob`` list (3.12 and 3.14 look ``os.scandir`` up at each call).
        """

        real_scandir, real_iglob = os.scandir, glob.iglob

        def scandir(*arguments: Any, **options: Any) -> Any:
            return _Listing(self, real_scandir(*arguments, **options))

        def glob_list(*arguments: Any, **options: Any) -> list[Any]:
            found = list(real_iglob(*arguments, **options))
            self.add("glob", names=len(found))
            return found

        # Patched as attributes (setattr), so that no checker holds them to the stubs' overloads.
        patches: list[tuple[Any, str, Any]] = [
            (os, "scandir", scandir),
            (os, "listdir", self.listed("listdir", os.listdir)),
            (glob, "glob", glob_list),
            (glob, "iglob", self.matched("iglob", glob.iglob)),
            (pathlib.Path, "glob", self.matched("path_glob", pathlib.Path.glob)),
            (pathlib.Path, "rglob", self.matched("path_rglob", pathlib.Path.rglob)),
        ]
        globber = getattr(glob, "_StringGlobber", None)
        captured = None if globber is None else vars(globber).get("scandir")
        if isinstance(captured, staticmethod) and captured.__func__ is real_scandir:
            patches.append((globber, "scandir", staticmethod(scandir)))
        for owner, name, value in patches:
            setattr(owner, name, value)
            self.patched.append(f"{getattr(owner, '__name__', owner)}.{name}")

    def install(self) -> None:
        """Count and time the sync calls, record the resolved strategies, and count the listings."""

        from httk.workflow import _fs

        real_resolve = _fs.resolve_sync

        def resolve_sync(*arguments: Any, **options: Any) -> Any:
            result = real_resolve(*arguments, **options)
            with self.lock:
                self.strategies[str(result[1])] += 1
            return result

        patches: list[tuple[Any, str, Any]] = [
            (os, "fsync", self.synced("fsync", os.fsync)),
            (os, "fdatasync", self.synced("fdatasync", os.fdatasync)),
            (_fs, "_syncfs", self.synced("syncfs", _fs._syncfs)),
            (_fs, "resolve_sync", resolve_sync),
        ]
        for owner, name, value in patches:
            setattr(owner, name, value)
            self.patched.append(f"{getattr(owner, '__name__', owner)}.{name}")
        self.install_listings()

    def document(self) -> dict[str, Any]:
        with self.lock:
            sync = {kind: {"count": self.calls[kind], "seconds": self.seconds[kind]} for kind in SYNC_KINDS}
            return {
                "calls": dict(self.calls),
                "names": dict(self.names),
                "sync": sync,
                "sync_calls": sum(self.calls[kind] for kind in SYNC_KINDS),
                "sync_seconds": sum(self.seconds[kind] for kind in SYNC_KINDS),
                "critical_path_sync_seconds": critical_path(self.intervals),
                "entries_examined": self.names["scandir"] + self.names["listdir"],
                "strategies": dict(self.strategies),
                "patched": list(self.patched),
            }


def _probe_main(mode: str, out: str, argv: list[str]) -> int:
    """Run ``httk.core.cli.main(argv)``, traced when *mode* is ``trace``, and write a JSON dump to *out*."""

    probe = _Probe() if mode == "trace" else None
    if probe is not None:
        probe.install()
    from httk.core.cli import main

    began = time.perf_counter()
    code: object = "exception"
    try:
        code = main(argv)
    except SystemExit as exc:
        code = exc.code
        raise
    finally:
        module = sys.modules.get("httk.workflow")
        document = {
            "mode": mode,
            "code": code if isinstance(code, int) or code is None else str(code),
            "inside_wall_seconds": time.perf_counter() - began,
            "workflow_module": getattr(module, "__file__", None),
            **(probe.document() if probe is not None else {}),
        }
        Path(out).write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
    return int(code) if isinstance(code, int) else 0 if code is None else 1


# -- helpers --------------------------------------------------------------------------------------------------------


def real_registry(environment: Mapping[str, str]) -> Path:
    """The user's real ``workspaces.json``: under ``XDG_CONFIG_HOME`` or ``~/.config``, never an override."""

    base = Path(environment.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return (base / "httk").resolve() / "workspaces.json"


def protected_directories(environment: Mapping[str, str]) -> list[Path]:
    """The real configuration and data directories no run may write in: the XDG bases, httk's directories in them,
    and any ``HTTK_CONFIG_HOME``/``HTTK_DATA_HOME`` the caller had set."""

    config = Path(environment.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    data = Path(environment.get("XDG_DATA_HOME") or Path.home() / ".local" / "share")
    found = [config, config / "httk", data, data / "httk"]
    found += [Path(environment[name]) for name in ("HTTK_CONFIG_HOME", "HTTK_DATA_HOME") if environment.get(name)]
    return sorted({path.expanduser().resolve() for path in found})


def protected_problems(paths: Iterable[Path], environment: Mapping[str, str]) -> list[str]:
    """Why any of *paths* (resolved) lies inside a real configuration or data directory (empty when none does)."""

    problems = []
    for path in paths:
        resolved = path.expanduser().resolve()
        for protected in protected_directories(environment):
            if resolved == protected or protected in resolved.parents:
                problems.append(f"{resolved} is inside the real {protected}")
    return problems


def redirection_problems(out: Path, config: Path, data: Path, registry: Path, real: Path) -> list[str]:
    """Why the httk user directories are not redirected below *out* (empty when they are)."""

    problems = []
    for label, path in (("HTTK_CONFIG_HOME", config), ("HTTK_DATA_HOME", data), ("the registry", registry)):
        if out not in path.resolve().parents:
            problems.append(f"{label} resolves to {path}, not below {out}")
    if registry.resolve() == real.resolve():
        problems.append(f"the registry is the real one ({real})")
    return problems


def _sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return None


def _git(*arguments: str) -> bytes:
    return subprocess.run(["git", *arguments], cwd=REPOSITORY, capture_output=True, check=True).stdout


def source_key(head: str, src_digest: str | None) -> str:
    """The grouping key of a result: the commit, and the digest of the uncommitted ``src/`` changes if any."""

    return head[:12] + ("" if src_digest is None else f"+dirty.{src_digest[:12]}")


def _source_state() -> dict[str, Any]:
    """``git rev-parse HEAD`` and whether ``src/`` (the measured code) and the tree are dirty."""

    head = _git("rev-parse", "HEAD").decode().strip()
    src_status = _git("status", "--porcelain", "--untracked-files=all", "--", "src").decode()
    tree_status = _git("status", "--porcelain").decode()
    digest = None
    if src_status.strip():
        hasher = hashlib.sha256(_git("diff", "HEAD", "--", "src"))
        for line in sorted(src_status.splitlines()):
            if line.startswith("??"):
                path = REPOSITORY / line[3:]
                hasher.update(line.encode() + (_sha256(path) or "").encode())
        digest = hasher.hexdigest()
    return {
        "head": head,
        "src_dirty": digest is not None,
        "src_digest": digest,
        "src_changes": src_status.splitlines()[:50],
        "tree_dirty": bool(tree_status.strip()),
        "key": source_key(head, digest),
    }


def _cmdline(pid: int) -> list[str]:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as stream:
            return [part.decode(errors="replace") for part in stream.read().split(b"\0") if part]
    except OSError:
        return []


def _ancestors(pid: int) -> set[int]:
    """*pid* and its ancestors (the shells and drivers that started this run are not busy processes)."""

    found = set()
    while pid > 1 and pid not in found:
        found.add(pid)
        try:
            with open(f"/proc/{pid}/stat", encoding="ascii", errors="replace") as stream:
                pid = int(stream.read().rsplit(")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            break
    return found


def _busy() -> list[str]:
    """The command lines of running pytest, ``make ci`` or other benchmark processes (they distort the timing)."""

    found = []
    mine = _ancestors(os.getpid())
    for name in os.listdir("/proc"):
        if name.isdigit() and int(name) not in mine:
            line = " ".join(_cmdline(int(name)))
            if re.search(BUSY, line) and "_probe" not in line:
                found.append(f"{name}: {line[:120]}")
    return found


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
    zfs = None
    with contextlib.suppress(OSError):
        zfs = Path("/sys/module/zfs/version").read_text(encoding="ascii").strip()
    return {
        "cpu": model,
        "cores": os.cpu_count(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "zfs_module": zfs,
        "niceness": os.nice(0),
    }


def tag_width(jobs: int) -> int:
    """The digits of the structure numbers: ``si-001`` up to 999 jobs, ``si-0001`` from 1,000."""

    return max(3, len(str(jobs)))


def structure_names(jobs: int) -> list[str]:
    """The structure file names, whose stems are the job tags (``si-001.vasp`` gives the tag ``si-001``)."""

    width = tag_width(jobs)
    return [f"si-{index:0{width}d}.vasp" for index in range(1, jobs + 1)]


def _prepare_structures(directory: Path, source: Path, jobs: int) -> list[str]:
    """Copy *jobs* structures from *source*, cycling through its files under new names: the job tags."""

    sources = sorted(path for path in source.iterdir() if path.is_file())
    if not sources:
        raise SystemExit(f"local_scale: no structures in {source}")
    directory.mkdir(parents=True)
    names = structure_names(jobs)
    for index, name in enumerate(names):
        shutil.copyfile(sources[index % len(sources)], directory / name)
    return [name.removesuffix(".vasp") for name in names]


def selector_arguments(selector: str, command: str, jobs: Sequence[tuple[str, str]]) -> tuple[list[str], str]:
    """The command-line selection of *jobs* (``(tag, job_id)`` pairs, in tag order) for *command*, and the selector
    actually used (``job delete`` has no ``--state``: it is given the full ids)."""

    if command == "new":
        return [], "none"
    if selector == "prefix":
        return [f"{tag}--" for tag, _ in jobs], "prefix"
    if selector == "state" and command in REQUESTS:
        return ["--state", REQUESTS[command]], "state"
    used = "full" if selector == "full" else "full (job delete has no --state)"
    return [job_id for _, job_id in jobs], used


def command_arguments(
    command: str, workspace: Path, workflow: str, structures: Path, selection: list[str]
) -> list[str]:
    """The ``httk`` arguments of one measured *command*."""

    if command == "new":
        source = ["--input-from", "structure", str(structures), "--duplicate-tag", "skip"]
        return ["job", "new", "--workspace", str(workspace), "--workflow", workflow, "--install", *source]
    if command == "delete":
        return ["job", "delete", "--workspace", str(workspace), "--yes", *selection]
    return ["job", "request", command, "--workspace", str(workspace), "--reason", "bench", *selection]


def output_problems(command: str, jobs: int, stdout: str, stderr: str) -> list[str]:
    """Why the output of *command* does not report exactly *jobs* outcomes of the expected kind."""

    if command == "new":
        found = re.findall(r"^submitted (\d+) jobs?(.*)$", stderr, re.MULTILINE)
        if found != [(str(jobs), "")]:
            return [f"new reported {found or 'no submission line'}, not 'submitted {jobs} jobs'"]
        return []
    rows = [line.split("\t") for line in stdout.splitlines() if "\t" in line]
    if command == "delete":
        statuses = collections.Counter(row[2] for row in rows if len(row) >= 3)
        problems = [] if statuses == {"removed": jobs} else [f"delete outcomes {dict(statuses)}, not {jobs} removed"]
        if f"removed {jobs} of {jobs} job(s)" not in stdout.splitlines():
            problems.append(f"no 'removed {jobs} of {jobs} job(s)' line")
        return problems
    statuses = collections.Counter(row[2] for row in rows if len(row) >= 4)
    problems = [] if statuses == {"applied": jobs} else [f"{command} outcomes {dict(statuses)}, not {jobs} applied"]
    if wrong := [row for row in rows if len(row) >= 4 and not row[3].startswith(f"{command} applied")]:
        problems.append(f"{len(wrong)} outcome(s) not '{command} applied', e.g. {wrong[0][3]!r}")
    return problems


def census_problems(census: Mapping[str, set[str]], expected: str | None, ids: set[str], requests: int) -> list[str]:
    """Why the jobs by state (*census*) are not all *ids* in *expected* (gone when ``None``) and nothing else, with
    no request file left."""

    problems = []
    for state, found in sorted(census.items()):
        wanted = ids if state == expected else set()
        if found != wanted:
            problems.append(
                f"{state}: {len(found & wanted)} of {len(wanted)} expected, {len(found - wanted)} unexpected"
            )
    if expected is not None and expected not in census:
        problems.append(f"{expected}: not listed")
    if requests:
        problems.append(f"{requests} request file(s) left")
    return problems


def filesystem_census(root: Path, *, requests: bool = False) -> tuple[dict[str, set[str]], list[str]]:
    """The job ids by state of the workspace at *root*, found by walking it independently of the kernel, and every
    leftover: an entry of ``jobs/<state>/`` that is not a well-formed job directory (no placements), a job found
    twice, anything in ``jobs/owned/``, or anything in the control directories of :data:`CONTROL_EMPTY` (but
    ``requests/`` when *requests* is true)."""

    census: dict[str, set[str]] = {}
    leftovers: list[str] = []
    jobs = root / "jobs"
    seen: dict[str, str] = {}
    for state in sorted(os.listdir(jobs)):
        path = jobs / state
        if path.is_symlink() or not path.is_dir():
            leftovers.append(f"jobs/{state}: not a state directory")
            continue
        names = sorted(os.listdir(path))
        if state == "owned":
            leftovers += [f"jobs/owned/{name}" for name in names]
            continue
        census[state] = set()
        for name in names:
            found = JOB_DIRECTORY.fullmatch(name)
            if found is None or (path / name).is_symlink() or not (path / name).is_dir():
                leftovers.append(f"jobs/{state}/{name}: not a job directory")
            elif found["id"] in seen:
                leftovers.append(f"jobs/{state}/{name}: job {found['id']} is also in {seen[found['id']]}")
            else:
                seen[found["id"]] = state
                census[state].add(found["id"])
    control = root / ".httk-workspace"
    for directory in CONTROL_EMPTY:
        if directory == "requests" and requests:
            continue
        with contextlib.suppress(FileNotFoundError):
            leftovers += [f".httk-workspace/{directory}/{name}" for name in sorted(os.listdir(control / directory))]
    return census, leftovers


def leftover_problems(leftovers: Sequence[str]) -> list[str]:
    """One problem naming the first few *leftovers*, or none."""

    if not leftovers:
        return []
    shown = "; ".join(leftovers[:5])
    return [f"{len(leftovers)} leftover(s): {shown}{'; ...' if len(leftovers) > 5 else ''}"]


def backlog_problems(
    entries: Mapping[str, tuple[Mapping[str, Any] | None, str | None]], ids: set[str], operator: str, key: str
) -> list[str]:
    """Why ``requests/`` (*entries*: each file name with its parsed document, or ``None``, and the operator key its
    signature verified to, or ``None``) does not hold exactly one pending backlog request for each of *ids*: a
    ``set_priority`` to :data:`BACKLOG_PRIORITY` by *operator*, signed with *key*."""

    problems = []
    per_job: collections.Counter[str] = collections.Counter()
    for name, (document, verified) in sorted(entries.items()):
        found = REQUEST_FILE.fullmatch(name)
        if found is None:
            problems.append(f"requests/{name}: not a request file")
            continue
        per_job[found["job"]] += 1
        if document is None:
            problems.append(f"requests/{name}: unreadable")
            continue
        expected = {
            "job_id": found["job"],
            "request_id": found["request"],
            "action": "set_priority",
            "priority": BACKLOG_PRIORITY,
            "operator": operator,
        }
        if wrong := [field for field, value in expected.items() if document.get(field) != value]:
            problems.append(f"requests/{name}: unexpected {', '.join(wrong)}")
        if verified != key:
            problems.append(f"requests/{name}: signed by {verified}, not {key}")
    if missing := ids - set(per_job):
        problems.append(f"{len(missing)} job(s) without a pending backlog request")
    if extra := set(per_job) - ids:
        problems.append(f"{len(extra)} request(s) for jobs not in the run")
    if doubled := [job for job, count in per_job.items() if count > 1]:
        problems.append(f"{len(doubled)} job(s) with more than one pending request")
    return problems


def _census(root: Path) -> tuple[dict[str, set[str]], dict[str, Any], int]:
    """The job ids of the workspace at *root* by state (owned included), its references, and its request files."""

    from httk.workflow import Workspace, _kernel

    workspace = Workspace(root, durable=False)
    census: dict[str, set[str]] = {}
    refs: dict[str, Any] = {}
    for state in sorted(_kernel.UNOWNED_STATES):
        found = list(_kernel.list_jobs(workspace, state))
        census[state] = {ref.job_id for ref in found}
        refs.update((ref.job_id, ref) for ref in found)
    owned: set[str] = set()
    owned_root = workspace.jobs / _kernel.OWNED
    for owner in sorted(os.listdir(owned_root)) if owned_root.is_dir() else ():
        owned.update(ref.job_id for ref in _kernel._owned_refs(owned_root / owner, owner))
    census[_kernel.OWNED] = owned
    directory = workspace.control / "requests"
    requests = sum(1 for name in os.listdir(directory) if name.endswith(".json")) if directory.is_dir() else 0
    return census, refs, requests


def _median(values: Sequence[float]) -> float | None:
    return statistics.median(values) if values else None


# -- the run --------------------------------------------------------------------------------------------------------


class _Run:
    """One invocation: its directories, environment and chains."""

    def __init__(self, arguments: argparse.Namespace, out: Path, run_id: str) -> None:
        self.arguments, self.out, self.run_id = arguments, out, run_id
        self.jobs: int = arguments.jobs
        self.work, self.logs = out / "work" / run_id, out / "logs" / run_id
        self.structures = self.work / "structures"
        self.tags: set[str] = set()
        self.source_key = ""
        self.operator = f"{OPERATOR[0]} <{OPERATOR[1]}>"
        self.operator_key = ""
        self.problems: list[str] = []
        self.contention: list[dict[str, Any]] = []
        self.policy: dict[str, Any] = {}
        self.setup: dict[str, list[float]] = collections.defaultdict(list)

    def httk(self, argv: list[str], cwd: Path, log: Path, *, mode: str = "plain") -> dict[str, Any]:
        """Run ``httk ARGV`` in a fresh subprocess (through ``_probe``), timed from outside."""

        dump = log.with_suffix(".probe.json")
        command = [sys.executable, str(SCRIPT), "_probe", mode, str(dump), *argv]
        started = time.monotonic()
        try:
            with open(log.with_suffix(".out"), "wb") as stdout, open(log.with_suffix(".err"), "wb") as stderr:
                completed = subprocess.run(
                    command, cwd=cwd, stdout=stdout, stderr=stderr, timeout=self.arguments.timeout, check=False
                )
            code: int | str = completed.returncode
        except subprocess.TimeoutExpired:
            code = f"timeout after {self.arguments.timeout} s"
        wall = time.monotonic() - started
        probe: dict[str, Any] = {}
        with contextlib.suppress(OSError, ValueError):
            probe = json.loads(dump.read_text(encoding="utf-8"))
        return {
            "exit_status": code,
            "wall_seconds": wall,
            "stdout": log.with_suffix(".out").read_text(encoding="utf-8", errors="replace"),
            "stderr": log.with_suffix(".err").read_text(encoding="utf-8", errors="replace"),
            "probe": probe,
        }

    def checked(self, argv: list[str], cwd: Path, log: Path) -> float:
        """Run an unmeasured setup command, which must succeed: its wall time."""

        outcome = self.httk(argv, cwd, log)
        if outcome["exit_status"] != 0:
            raise RuntimeError(f"setup command failed ({outcome['exit_status']}): httk {' '.join(argv[:3])}; {log}")
        return outcome["wall_seconds"]

    def workspace(self, chain: Path, name: str) -> Path:
        """A fresh project and workspace in *chain* with the workflow installed and ``io.sync`` set."""

        project = chain / name
        project.mkdir(parents=True)
        root = project / "workspace"
        logs = self.logs / chain.name
        logs.mkdir(parents=True, exist_ok=True)
        began = time.monotonic()
        self.checked(["project", "init", "--name", f"bench-{name}", "."], project, logs / f"{name}-project")
        label = f"ls-{chain.name}-{name}-{uuid.uuid4().hex[:8]}"
        self.checked(["workspace", "init", "--name", label, str(root)], project, logs / f"{name}-workspace")
        install = ["workflow", "install", "--workspace", str(root), self.arguments.workflow]
        self.checked(install, project, logs / f"{name}-install")
        if self.arguments.io_sync != "auto":
            setting = ["workspace", "set", "-w", str(root), f"io.sync={self.arguments.io_sync}"]
            self.checked(setting, project, logs / f"{name}-io-sync")
        self.setup["workspace_seconds"].append(round(time.monotonic() - began, 3))
        from httk.workflow import Workspace, _fs

        workspace = Workspace(root, durable=False)
        resolved = _fs.resolve_sync(root, workspace.io_sync())
        self.policy.setdefault("io_sync", workspace.io_sync())
        self.policy.setdefault("io_workers", workspace.io_workers)
        self.policy.setdefault("resolved", set()).add(f"{resolved[0]}: {resolved[1]}")
        return root

    def step(
        self, chain: Path, root: Path, command: str, kind: str, ids: dict[str, str], *, measured: bool
    ) -> dict[str, Any]:
        """Run *command* on the workspace at *root* and check its outcome; *ids* maps tags to job ids."""

        jobs = sorted(ids.items())
        selection, used = selector_arguments(self.arguments.selector, command, jobs)
        argv = command_arguments(command, root, self.arguments.workflow, self.structures, selection)
        mode = "trace" if measured and kind == "trace" else "plain"
        before = _source_state()["key"] if measured else self.source_key
        outcome = self.httk(argv, root.parent, self.logs / chain.name / f"{root.parent.name}-{command}", mode=mode)
        after = _source_state()["key"] if measured else self.source_key
        problems = []
        if (before, after) != (self.source_key, self.source_key):
            problems.append(
                f"src changed: {before} before and {after} after the command, not the run's {self.source_key}"
            )
        if outcome["exit_status"] != 0:
            problems.append(f"exit status {outcome['exit_status']}")
        problems += output_problems(command, self.jobs, outcome["stdout"], outcome["stderr"])
        module = outcome["probe"].get("workflow_module")
        if module is None or SOURCE not in Path(module).resolve().parents:
            problems.append(f"the CLI imported httk.workflow from {module}, not from {SOURCE}")
        census, refs, requests = _census(root)
        expected_ids = set(ids.values()) if command != "new" else census.get("ready", set())
        if command == "new":
            tags = {refs[job].job_key.split("--", 1)[0] for job in expected_ids}
            if len(expected_ids) != self.jobs or tags != self.tags:
                problems.append(f"{len(expected_ids)} ready jobs with {len(tags & self.tags)} expected tags after new")
        problems += [
            f"kernel: {problem}" for problem in census_problems(census, AFTER[command], expected_ids, requests)
        ]
        walked, leftovers = filesystem_census(root)
        problems += [f"walk: {problem}" for problem in census_problems(walked, AFTER[command], expected_ids, 0)]
        problems += leftover_problems(leftovers)
        if command == "cancel" and self.arguments.backlog:
            priorities = collections.Counter(refs[job].priority for job in expected_ids if job in refs)
            if priorities != {BACKLOG_PRIORITY: self.jobs}:
                problems.append(f"backlog priorities {dict(priorities)}, not {self.jobs} x {BACKLOG_PRIORITY}")
        record = {
            "command": command,
            "measured": measured,
            "mode": mode,
            "selector": used,
            "exit_status": outcome["exit_status"],
            "wall_seconds": outcome["wall_seconds"],
            "problems": problems,
        }
        if mode == "trace":
            record["probe"] = outcome["probe"]
        if command == "new":
            record["ids"] = {ref.job_key.split("--", 1)[0]: job for job, ref in refs.items() if job in expected_ids}
        return record

    def backlog(self, root: Path, ids: set[str]) -> dict[str, Any]:
        """Post one ``set_priority`` request to every ready job, unserved (the CLI's own posting function), and check
        that exactly those requests are pending: the record of this unmeasured step."""

        from httk.core.identity import verify_document

        from httk.workflow import Workspace, _kernel
        from httk.workflow.workflow_cli._job import publish_job_requests

        began = time.monotonic()
        workspace = Workspace(root)
        refs = list(_kernel.list_jobs(workspace, "ready"))
        publish_job_requests(workspace, refs, action="set_priority", reason="bench backlog", priority=BACKLOG_PRIORITY)
        entries: dict[str, tuple[dict[str, Any] | None, str | None]] = {}
        directory = workspace.control / "requests"
        for name in os.listdir(directory):
            document: dict[str, Any] | None = None
            verified = None
            with contextlib.suppress(OSError, ValueError):
                document = json.loads((directory / name).read_text(encoding="utf-8"))
            if isinstance(document, dict):
                signature = verify_document(document)
                verified = signature.operator_key if signature.present and signature.valid else None
            else:
                document = None
            entries[name] = (document, verified)
        problems = backlog_problems(entries, ids, self.operator, self.operator_key)
        walked, leftovers = filesystem_census(root, requests=True)
        problems += [f"walk: {problem}" for problem in census_problems(walked, "ready", ids, 0)]
        problems += leftover_problems(leftovers)
        wall = time.monotonic() - began
        self.setup["backlog_seconds"].append(wall)
        return {
            "command": "backlog",
            "measured": False,
            "mode": "in-process",
            "selector": "none",
            "exit_status": 0,
            "wall_seconds": wall,
            "problems": problems,
        }

    def chain(self, index: int, kind: str, commands: Sequence[str]) -> dict[str, Any]:
        """One repetition on fresh state: the steps of *commands* and the unmeasured ones they need."""

        chain = self.work / f"c{index}-{kind}"
        steps: list[dict[str, Any]] = []

        def run(root: Path, command: str, ids: dict[str, str], *, measured: bool) -> dict[str, str] | None:
            record = self.step(chain, root, command, kind, ids, measured=measured)
            steps.append(record)
            if not measured:
                self.setup[f"{command}_seconds"].append(record["wall_seconds"])
            if record["problems"]:
                return None
            return record.get("ids", ids)

        first = [command for command in ("new", "cancel", "delete") if command in commands]
        if first:
            root = self.workspace(chain, "a")
            ids = run(root, "new", {}, measured="new" in commands)
            last = first[-1]
            if ids is not None and last in ("cancel", "delete") and self.arguments.backlog:
                steps.append(self.backlog(root, set(ids.values())))
                ids = None if steps[-1]["problems"] else ids
            if ids is not None and last in ("cancel", "delete"):
                ids = run(root, "cancel", ids, measured="cancel" in commands)
            if ids is not None and last == "delete":
                run(root, "delete", ids, measured=True)
        second = [command for command in ("pause", "continue") if command in commands]
        if second and not any(step["problems"] for step in steps):
            root = self.workspace(chain, "b")
            ids = run(root, "new", {}, measured=False)
            if ids is not None:
                ids = run(root, "pause", ids, measured="pause" in commands)
            if ids is not None and second[-1] == "continue":
                run(root, "continue", ids, measured=True)
        for step in steps:
            step.pop("ids", None)
        return {"index": index, "kind": kind, "steps": steps}


def aggregate(chains: Sequence[Mapping[str, Any]], commands: Sequence[str], jobs: int) -> dict[str, Any]:
    """Per measured command: the plain walls and their median, and the medians of the traced counters."""

    summary: dict[str, Any] = {}
    for command in commands:
        steps = [(chain["kind"], step) for chain in chains for step in chain["steps"] if step["command"] == command]
        steps = [(kind, step) for kind, step in steps if step["measured"]]
        plain = [step["wall_seconds"] for kind, step in steps if kind == "plain"]
        traced = [step for kind, step in steps if kind == "trace" and step.get("probe")]
        entry: dict[str, Any] = {
            "selector": sorted({step["selector"] for _, step in steps}),
            "plain_walls": plain,
            "wall_median": _median(plain),
            "wall_min": min(plain, default=None),
            "wall_max": max(plain, default=None),
            "ms_per_job": None if not plain else round(1000 * statistics.median(plain) / jobs, 3),
            "traced_runs": len(traced),
        }
        if traced:
            probes = [step["probe"] for step in traced]
            entry["traced"] = {
                "wall_median": _median([step["wall_seconds"] for step in traced]),
                "inside_wall_median": _median([probe["inside_wall_seconds"] for probe in probes]),
                "sync_calls": _median([probe["sync_calls"] for probe in probes]),
                "sync_seconds": _median([probe["sync_seconds"] for probe in probes]),
                "critical_path_sync_seconds": _median([probe["critical_path_sync_seconds"] for probe in probes]),
                "entries_examined": _median([probe["entries_examined"] for probe in probes]),
                "by_kind": {
                    kind: {
                        "count": _median([probe["sync"][kind]["count"] for probe in probes]),
                        "seconds": _median([probe["sync"][kind]["seconds"] for probe in probes]),
                    }
                    for kind in SYNC_KINDS
                },
                "calls": {
                    name: _median([probe["calls"].get(name, 0) for probe in probes])
                    for name in ("scandir", "listdir", "glob", "iglob", "path_glob", "path_rglob")
                },
                "glob_matches": _median(
                    [sum(probe["names"].get(name, 0) for name in ("glob", "iglob", "path_glob")) for probe in probes]
                ),
                "strategies": sorted({name for probe in probes for name in probe.get("strategies", {})}),
            }
            critical, wall = entry["traced"]["critical_path_sync_seconds"], entry["traced"]["wall_median"]
            entry["traced"]["critical_path_sync_share"] = critical / wall if wall else None
        summary[command] = entry
    return summary


def _comparable(result: Mapping[str, Any]) -> tuple[Any, ...]:
    p = result["parameters"]
    return (result["source"]["key"], p["selector"], p["io_sync"], p["backlog"])


def sync_allowance(wall: float) -> float:
    """The most critical-path sync time a command of *wall* seconds may spend: max(0.250 s, 10% of the wall)."""

    return max(SYNC_FLOOR_SECONDS, SYNC_SHARE_TARGET * wall)


def acceptance(result: Mapping[str, Any], others: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Plan §1's targets for every measured command of one valid *result*: ``pass``, ``fail`` or ``not measured``.

    The per-job flatness compares the N = 100 and N = 1,000 results of the same source key, selector, ``io.sync``
    and backlog (the most recent valid one of the other size among *others*).
    """

    jobs = result["parameters"]["jobs"]
    verdicts: dict[str, Any] = {}
    for command, entry in result["commands"].items():
        wall = entry["wall_median"]
        verdict: dict[str, Any] = {}
        if command not in WALL_TARGET_COMMANDS:
            verdict["wall"] = {"verdict": "not measured", "why": f"plan §1 sets no wall target for {command}"}
        elif jobs != WALL_TARGET_JOBS:
            verdict["wall"] = {"verdict": "not measured", "why": f"the wall target is set at N = {WALL_TARGET_JOBS}"}
        elif wall is None:
            verdict["wall"] = {"verdict": "not measured", "why": "no plain run"}
        else:
            verdict["wall"] = {
                "verdict": "pass" if wall <= WALL_TARGET_SECONDS else "fail",
                "wall": wall,
                "target": WALL_TARGET_SECONDS,
            }
        traced = entry.get("traced") or {}
        critical, traced_wall = traced.get("critical_path_sync_seconds"), traced.get("wall_median")
        if critical is None or traced_wall is None:
            verdict["sync"] = {"verdict": "not measured", "why": "no traced run"}
        else:
            allowance = sync_allowance(traced_wall)
            verdict["sync"] = {
                "verdict": "pass" if critical <= allowance else "fail",
                "critical_path_sync_seconds": critical,
                "traced_wall": traced_wall,
                "allowance": allowance,
                "target": f"max({SYNC_FLOOR_SECONDS} s, {SYNC_SHARE_TARGET} x wall)",
            }
        verdict["flatness"] = _flatness(result, others, command)
        verdicts[command] = verdict
    return verdicts


def _flatness(result: Mapping[str, Any], others: Sequence[Mapping[str, Any]], command: str) -> dict[str, Any]:
    jobs = result["parameters"]["jobs"]
    if jobs not in FLATNESS_SIZES:
        return {"verdict": "not measured", "why": f"flatness compares N = {FLATNESS_SIZES[0]} and {FLATNESS_SIZES[1]}"}
    partner_jobs = FLATNESS_SIZES[1] if jobs == FLATNESS_SIZES[0] else FLATNESS_SIZES[0]
    partners = [
        other
        for other in others
        if other.get("valid")
        and other["parameters"]["jobs"] == partner_jobs
        and _comparable(other) == _comparable(result)
        and (other.get("commands", {}).get(command) or {}).get("wall_median") is not None
    ]
    wall = (result["commands"].get(command) or {}).get("wall_median")
    if not partners or wall is None:
        return {"verdict": "not measured", "why": f"no valid N = {partner_jobs} run of the same source and case"}
    partner = max(partners, key=lambda other: str(other["run"]))
    per_job = {jobs: wall / jobs, partner_jobs: partner["commands"][command]["wall_median"] / partner_jobs}
    ratio = per_job[FLATNESS_SIZES[1]] / per_job[FLATNESS_SIZES[0]]
    return {
        "verdict": "pass" if ratio <= FLATNESS_TARGET else "fail",
        "ratio": round(ratio, 3),
        "ms_per_job": {str(size): round(1000 * value, 3) for size, value in sorted(per_job.items())},
        "partner": partner["run"],
        "target": FLATNESS_TARGET,
    }


def _commands(text: str) -> list[str]:
    chosen = [name.strip() for name in text.split(",") if name.strip()]
    if not chosen or any(name not in COMMANDS for name in chosen):
        raise argparse.ArgumentTypeError(f"a comma-separated subset of {', '.join(COMMANDS)}")
    return [name for name in COMMANDS if name in chosen]


def run(arguments: argparse.Namespace) -> int:
    if not arguments.workflow.startswith("git+file://"):
        raise SystemExit("local_scale: --workflow must be a local git+file:// URI (no network)")
    out = Path(arguments.out).expanduser().resolve()
    if out == Path("/tmp") or Path("/tmp") in out.parents:
        raise SystemExit("local_scale: --out must not be below /tmp")
    p = {
        "jobs": arguments.jobs,
        "selector": arguments.selector,
        "io_sync": arguments.io_sync,
        "backlog": arguments.backlog,
        "repeat": arguments.repeat,
        "traced": arguments.traced,
        "commands": arguments.commands,
        "workflow": arguments.workflow,
        "structures": str(Path(arguments.structures).resolve()),
    }
    stamp = time.strftime("%Y%m%dT%H%M%S")
    run_id = (
        f"n{p['jobs']}-{p['selector']}-{p['io_sync']}{'-backlog' if p['backlog'] else ''}"
        f"-k{p['repeat']}t{p['traced']}-{stamp}-{uuid.uuid4().hex[:4]}"
    )
    state = _Run(arguments, out, run_id)
    results = out / "results"
    # Before anything is written: never inside the real configuration or data directories.
    if refusal := protected_problems((out, state.work, state.logs, results), os.environ):
        raise SystemExit(f"local_scale: refusing --out: {'; '.join(refusal)}")
    for directory in (state.work / "config", state.work / "data", state.work / "tmp", state.logs, results):
        directory.mkdir(parents=True, exist_ok=True)

    real = real_registry(os.environ)
    before = _sha256(real)
    environment = {
        "HTTK_CONFIG_HOME": str(state.work / "config"),
        "HTTK_DATA_HOME": str(state.work / "data"),
        "TMPDIR": str(state.work / "tmp"),
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH", "")])),
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
    }
    os.environ.update(environment)
    sys.path.insert(0, str(SOURCE))
    import tempfile

    tempfile.tempdir = str(state.work / "tmp")
    from httk.core import userdirs

    from httk.workflow.registry import workspaces_path

    if refusal := redirection_problems(out, userdirs.config_home(), userdirs.data_home(), workspaces_path(), real):
        raise SystemExit(f"local_scale: the httk user directories are not redirected: {'; '.join(refusal)}")
    source = _source_state()
    state.source_key = source["key"]

    print(f"local_scale: {run_id}: preparing {p['jobs']} structures", flush=True)
    state.tags = set(_prepare_structures(state.structures, Path(p["structures"]), p["jobs"]))
    name, email = OPERATOR
    setup = ["setup", "--name", name, "--email", email, "--non-interactive"]
    initialized = state.httk(setup, state.work, state.logs / "setup")
    try:
        state.operator_key = str(json.loads(initialized["stdout"])["public_key"])
    except (ValueError, KeyError, TypeError) as exc:
        raise SystemExit(f"local_scale: httk setup failed ({initialized['exit_status']}): {exc}") from exc

    chains: list[dict[str, Any]] = []
    kinds = ["plain"] * p["repeat"] + ["trace"] * p["traced"]
    for index, kind in enumerate(kinds):
        waited = 0.0
        while (busy := _busy()) and waited < arguments.busy_timeout:
            print(f"local_scale: waiting for {len(busy)} busy process(es): {busy[0]}", flush=True)
            time.sleep(15)
            waited += 15
        if busy:
            state.contention.append({"chain": index, "busy": busy[:5]})
        began = time.monotonic()
        try:
            chain = state.chain(index, kind, p["commands"])
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            chain = {"index": index, "kind": kind, "steps": [], "error": str(exc)}
            state.problems.append(f"chain {index} ({kind}): {exc}")
        chains.append(chain)
        for step in chain["steps"]:
            state.problems += [f"chain {index} ({kind}) {step['command']}: {problem}" for problem in step["problems"]]
        walls = ", ".join(
            f"{step['command']} {step['wall_seconds']:.2f}" for step in chain["steps"] if step["measured"]
        )
        print(f"local_scale: chain {index} ({kind}) in {time.monotonic() - began:.0f} s: {walls}", flush=True)
        if state.problems and not arguments.keep_going:
            break

    final_source = _source_state()
    if final_source["key"] != state.source_key:
        state.problems.append(f"src changed during the run: {final_source['key']}, not {state.source_key}")
    policy = dict(state.policy)
    policy["resolved"] = sorted(policy.get("resolved", ()))
    result: dict[str, Any] = {
        "benchmark": BENCHMARK,
        "format_version": FORMAT_VERSION,
        "run": run_id,
        "valid": not state.problems,
        "problems": state.problems,
        "parameters": p,
        "source": source,
        "machine": _machine(),
        "filesystem": _filesystem(state.work),
        "policy": policy,
        "setup": {key: values for key, values in state.setup.items()},
        "contention": state.contention,
        "not_measured": NOT_MEASURED,
        "chains": chains,
        "commands": aggregate(chains, p["commands"], p["jobs"]),
    }
    if any(
        len(entry["plain_walls"]) != p["repeat"] or entry["traced_runs"] != p["traced"]
        for entry in result["commands"].values()
    ):
        result["valid"] = False
        result["problems"].append("not every chain measured every command")
    others, _skipped = load_results(results)
    if result["valid"]:
        result["acceptance"] = acceptance(result, others)
    after = _sha256(real)
    result["real_registry"] = {
        "path": str(real),
        "sha256_before": before,
        "sha256_after": after,
        "known_sha256": KNOWN_REGISTRY_SHA256,
        "matches_known": after == KNOWN_REGISTRY_SHA256,
    }
    if before != after:
        result["valid"] = False
        result["problems"].append("the real workspaces.json changed")
    (results / f"{run_id}.json").write_text(json.dumps(result, indent=1, sort_keys=True), encoding="utf-8")
    if not arguments.keep_work:
        shutil.rmtree(state.work, ignore_errors=True)
    verdict = "VALID" if result["valid"] else f"INVALID ({'; '.join(result['problems'][:5])})"
    print(f"local_scale: {run_id}: {verdict}", flush=True)
    for command, entry in result["commands"].items():
        traced = entry.get("traced") or {}
        print(
            f"  {command}: wall {entry['wall_median']} s, sync {traced.get('sync_calls')} calls "
            f"{traced.get('sync_seconds')} s, critical {traced.get('critical_path_sync_seconds')} s, "
            f"entries {traced.get('entries_examined')}",
            flush=True,
        )
        for key, value in (result.get("acceptance", {}).get(command) or {}).items():
            print(f"    {key}: {value['verdict']}", flush=True)
    if before != after:
        raise SystemExit("local_scale: the real workspaces.json changed")
    return 0 if result["valid"] else 1


# -- the summary ----------------------------------------------------------------------------------------------------


def _current(result: object) -> TypeGuard[dict[str, Any]]:
    """Whether *result* is a result of this benchmark in this format."""

    return (
        isinstance(result, dict)
        and result.get("benchmark") == BENCHMARK
        and result.get("format_version") in FORMATS
        and isinstance(result.get("parameters"), dict)
        and isinstance(result.get("source"), dict)
        and isinstance(result.get("commands"), dict)
    )


def load_results(directory: Path) -> tuple[list[dict[str, Any]], int]:
    """Read the results in *directory* (``results/``): the current ones, by name, and how many others were skipped
    (another benchmark or format, or not readable as JSON)."""

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


def _cell(value: object, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def _validity(result: Mapping[str, Any]) -> str:
    if not result["valid"]:
        return "NO"
    return "yes" if result.get("format_version") == FORMAT_VERSION else "yes*"


def _short(verdict: Mapping[str, Any] | None) -> str:
    value = str((verdict or {}).get("verdict", "-"))
    return {"not measured": "n/m"}.get(value, value)


def strategy(result: Mapping[str, Any], traced: Mapping[str, Any]) -> str:
    """The workspace's resolved ``io.sync`` strategy, and the barriers' when the traced command resolved any."""

    resolved = "/".join(sorted({entry.split(":", 1)[0] for entry in result.get("policy", {}).get("resolved", ())}))
    barriers = "/".join(sorted({entry.split(" ", 1)[0] for entry in traced.get("strategies") or ()}))
    return (resolved or "-") + (f" (barrier {barriers})" if barriers else "")


def summarize(out: Path) -> Path:
    """Write ``OUT/summary.md`` from every result in ``OUT/results/``, grouped by source key."""

    results, skipped = load_results(out / "results")
    for result in results:
        if result.get("valid"):
            result["acceptance"] = acceptance(result, results)  # flatness once both sizes are in
    lines = [
        "# Local many-job command results",
        "",
        "One row a measured command of a run (`benchmarks/local_scale.py`). wall: median of the plain runs (s);",
        "sync: fsync + fdatasync + syncfs calls and their summed seconds, critical: the wall-clock union of the sync",
        "calls (s), share: critical over the traced wall, entries: directory entries examined (scandir + listdir), all",
        "medians of the traced runs. Verdicts: wall <= 2.5 s at N = 100 (new, cancel, delete); sync: critical <=",
        "max(0.250 s, 10% of the traced wall), compared unrounded; flat: per-job cost at N = 1,000 <= 1.25 x that at",
        "N = 100 (n/m: not measured). valid yes*: a format-1 result, from before the per-command src fingerprint, the",
        "filesystem census and the backlog precheck.",
        f"Skipped {skipped} file(s) that are not results of this benchmark in a known format.",
    ]
    groups: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for result in results:
        groups[result["source"]["key"]].append(result)
    for key, members in sorted(groups.items()):
        dirty = "src dirty" if members[0]["source"].get("src_dirty") else "src clean"
        lines += [
            "",
            f"## Source {key} ({dirty})",
            "",
            (
                "| run | valid | N | selector | io.sync | backlog | command | wall s | ms/job | sync calls | sync s "
                "| critical s | share | entries | strategy | wall | sync | flat |"
            ),
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for result in sorted(members, key=lambda item: str(item["run"])):
            p = result["parameters"]
            for command in sorted(result["commands"], key=lambda name: (COMMANDS + (name,)).index(name)):
                entry = result["commands"][command]
                traced = entry.get("traced") or {}
                verdicts = (result.get("acceptance") or {}).get(command) or {}
                lines.append(
                    f"| {result['run']} | {_validity(result)} | {p['jobs']} | "
                    f"{'/'.join(entry.get('selector') or [p['selector']])} | {p['io_sync']} | "
                    f"{'yes' if p['backlog'] else 'no'} | {command} | {_cell(entry.get('wall_median'))} | "
                    f"{_cell(entry.get('ms_per_job'), 1)} | {_cell(traced.get('sync_calls'), 0)} | "
                    f"{_cell(traced.get('sync_seconds'))} | {_cell(traced.get('critical_path_sync_seconds'))} | "
                    f"{_cell(traced.get('critical_path_sync_share'), 3)} | {_cell(traced.get('entries_examined'), 0)} "
                    f"| {strategy(result, traced)} | {_short(verdicts.get('wall'))} | "
                    f"{_short(verdicts.get('sync'))} | {_short(verdicts.get('flatness'))} |"
                )
    invalid = [result for result in results if not result["valid"]]
    if invalid:
        lines += ["", "## Invalid runs", ""]
        lines += [f"- {result['run']}: {'; '.join(result['problems'][:10])}" for result in invalid]
    lines += ["", "## Not measured", ""]
    lines += [f"- {name}: {why}" for name, why in NOT_MEASURED.items()]
    path = out / "summary.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def parser() -> argparse.ArgumentParser:
    """The command-line parser."""

    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    parser.add_argument("--out", required=True, help="the directory of the results, logs and work (never /tmp)")
    parser.add_argument("--summarize", action="store_true", help="write OUT/summary.md from OUT/results/ and stop")
    parser.add_argument("--jobs", type=int, default=100, help="N: the jobs each command handles (100 or 1000)")
    parser.add_argument("--selector", choices=("prefix", "full", "state"), default="prefix")
    parser.add_argument("--io-sync", choices=("auto", "syncfs", "fsync"), default="auto")
    parser.add_argument("--backlog", action="store_true", help="give every job an older pending request first")
    parser.add_argument("--repeat", type=int, default=3, metavar="K", help="plain chains (median of K; 0: traced only)")
    parser.add_argument("--traced", type=int, default=1, metavar="T", help="traced chains (sync and listing counts)")
    parser.add_argument("--commands", type=_commands, default=list(COMMANDS), help="comma-separated subset to measure")
    parser.add_argument("--workflow", default=WORKFLOW, help="the local git+file:// workflow URI")
    parser.add_argument("--structures", default=str(STRUCTURES), help="the directory of POSCAR structures to copy")
    parser.add_argument("--timeout", type=float, default=7200.0, help="seconds one command may take")
    parser.add_argument("--busy-timeout", type=float, default=1800.0, help="seconds to wait for busy processes")
    parser.add_argument("--keep-work", action="store_true", help="keep the run's workspaces")
    parser.add_argument("--keep-going", action="store_true", help="run the remaining chains after an invalid step")
    return parser


def check_arguments(arguments: argparse.Namespace) -> None:
    """Refuse arguments no run can use.

    :raises SystemExit: For a nonpositive ``--jobs`` or ``--timeout``, a negative ``--repeat`` or ``--traced``, or
        no chain at all.
    """

    if arguments.jobs < 1 or arguments.timeout <= 0 or arguments.repeat < 0 or arguments.traced < 0:
        raise SystemExit("local_scale: --jobs and --timeout must be positive, --repeat and --traced not negative")
    if arguments.repeat + arguments.traced < 1:
        raise SystemExit("local_scale: --repeat 0 needs --traced 1 or more (a traced-only run)")


def main() -> int:
    if len(sys.argv) > 3 and sys.argv[1] == "_probe":
        return _probe_main(sys.argv[2], sys.argv[3], sys.argv[4:])
    arguments = parser().parse_args()
    if arguments.summarize:
        print(summarize(Path(arguments.out).expanduser().resolve()))
        return 0
    check_arguments(arguments)
    return run(arguments)


if __name__ == "__main__":
    sys.exit(main())
