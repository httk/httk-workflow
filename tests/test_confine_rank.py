"""The rank helper and inner exec of a confined launch, with the plumbing-only fake Bubblewrap."""

import builtins
import fcntl
import io
import os
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

import pytest

from httk.workflow import _confine_exec, _confine_rank
from httk.workflow._confine import ConfinementUnavailableError, confine_settings, default_readonly_paths, probe_bwrap
from httk.workflow._launch_protocol import (
    LaunchConfinement,
    LaunchRequest,
    TrustedLaunch,
    encode_request,
    encode_trusted,
    new_request_id,
    request_relative_path,
    trusted_name,
)
from httk.workflow._sandbox import PreparedSandbox
from test_pmi_proxy import INIT, REPLIES, SPAWN, serve_slurm

_FAKE_BWRAP = Path(__file__).with_name("fake_bwrap.py")
_TIMEOUT = 60.0
_WORKSPACE_ID = "0f6b6c2a-1d55-4c69-8d43-7c0a1b2c3d4e"


@dataclass
class _Launch:
    root: Path
    workspace: Path
    job: Path
    shm_root: Path
    pmix_root: Path
    launch_dir: Path
    trusted: TrustedLaunch

    @property
    def request_path(self) -> Path:
        return self.job / self.trusted.request

    @property
    def shm(self) -> Path:
        return self.shm_root / f"httk-{self.trusted.token}"

    def write(self, trusted: TrustedLaunch | None = None) -> None:
        if trusted is not None:
            self.trusted = trusted
        self.launch_dir.mkdir(parents=True, exist_ok=True)
        (self.launch_dir / "launch.json").write_bytes(encode_trusted(self.trusted))

    def request(self, argv: tuple[str, ...], cwd: str = ".", environment: dict[str, str] | None = None) -> None:
        self.request_path.parent.mkdir(parents=True, exist_ok=True)
        request = LaunchRequest(
            request_id=self.trusted.request_id,
            attempt_id=self.trusted.attempt_id,
            argv=argv,
            cwd=cwd,
            environment=tuple(sorted((environment or {"PATH": os.environ["PATH"]}).items())),
        )
        self.request_path.write_bytes(encode_request(request))


def _fake_bwrap(directory: Path) -> Path:
    wrapper = directory / "bwrap"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{_FAKE_BWRAP}" "$@"\n', encoding="utf-8")
    wrapper.chmod(0o755)
    return wrapper


def _host_shm_directory() -> Path:
    """Return a fresh private directory on the host's tmpfs, which rank-helper subprocesses accept as shm_root."""

    try:
        descriptor = os.open("/dev/shm", os.O_RDONLY | os.O_DIRECTORY)
        try:
            observed = _confine_rank._filesystem_type(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        observed = "unreadable"
    if observed != "tmpfs":
        pytest.skip("/dev/shm is not a tmpfs on this host")
    return Path(tempfile.mkdtemp(prefix="httk-test-", dir="/dev/shm"))


@pytest.fixture
def launch(tmp_path: Path) -> Iterator[_Launch]:
    root = tmp_path.resolve()
    workspace = root / "ws"
    job = workspace / "jobs" / "project" / f"relax--{uuid.uuid4()}"
    job.mkdir(parents=True)
    shm_root = _host_shm_directory()
    pmix_root = root / "pmix"
    pmix_root.mkdir()
    bin_directory = root / "bin"
    bin_directory.mkdir()
    request_id = new_request_id()
    attempt_id = str(uuid.uuid4())
    trusted = TrustedLaunch(
        request_id=request_id,
        attempt_id=attempt_id,
        workspace_id=_WORKSPACE_ID,
        workspace_root=workspace,
        placement="project",
        job_key=job.name,
        request=request_relative_path(attempt_id, request_id),
        confine=LaunchConfinement(
            bwrap=_fake_bwrap(bin_directory),
            block_userns=False,
            readonly_paths=(Path("/usr"),),
            devices=(),
            pmix_roots=(pmix_root,),
            shm_root=shm_root,
            environment=(("OMPI_MCA_btl_vader_single_copy_mechanism", "none"),),
        ),
        python=Path(sys.executable),
        token=secrets.token_hex(16),
    )
    launch_dir = workspace / ".httk-workspace" / "managers" / "m1" / "launches" / trusted_name(attempt_id, request_id)
    created = _Launch(root, workspace, job, shm_root, pmix_root, launch_dir, trusted)
    created.write()
    try:
        yield created
    finally:
        shutil.rmtree(shm_root, ignore_errors=True)


def _option_index(argv: list[str], *items: str) -> int:
    for index in range(len(argv) - len(items) + 1):
        if tuple(argv[index : index + len(items)]) == items:
            return index
    raise AssertionError(f"{items} not in {argv}")


@dataclass
class _Opened:
    workspace: Path
    workspace_fd: int
    job: Path
    job_fd: int
    shm_fd: int


@pytest.fixture
def opened(launch: _Launch) -> Iterator[_Opened]:
    trusted, workspace, workspace_fd = _confine_rank.read_launch(launch.launch_dir)
    job, job_fd = _confine_rank.open_job(trusted, workspace)
    shm_fd = os.open(launch.shm_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        yield _Opened(workspace, workspace_fd, job, job_fd, shm_fd)
    finally:
        for descriptor in (workspace_fd, job_fd, shm_fd):
            os.close(descriptor)


def _prepare(launch: _Launch, opened: _Opened, environ: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    prepared, environment = _confine_rank.prepare_rank_sandbox(
        launch.trusted,
        workspace=opened.workspace,
        workspace_fd=opened.workspace_fd,
        job=opened.job,
        job_fd=opened.job_fd,
        shm_fd=opened.shm_fd,
        environ=environ,
    )
    try:
        for descriptor in prepared.descriptors:
            assert not fcntl.fcntl(descriptor, fcntl.F_GETFD) & fcntl.FD_CLOEXEC
        return prepared.argv, environment
    finally:
        prepared.close()


def test_argv_keeps_host_network_and_binds_shm_after_dev(launch: _Launch, opened: _Opened) -> None:
    environ = {
        "PMI_RANK": "3",
        "PMIX_NAMESPACE": "ns",
        "OMPI_COMM_WORLD_SIZE": "4",
        "OPAL_PREFIX": "/opt/ompi",
        "SLURM_PROCID": "3",
        "SLURMD_NODENAME": "n1",
        "SLURM_CONF": "/etc/slurm.conf",
        "SLURM_JOB_NODELIST": "n[1-2]",
        "FOO": "bar",
        "HOME": "/home/u",
    }
    argv, environment = _prepare(launch, opened, environ)
    assert argv[0] == str(launch.trusted.confine.bwrap)
    assert argv[1:5] == ["--unshare-user", "--unshare-pid", "--unshare-ipc", "--unshare-uts"]
    assert "--unshare-net" not in argv and "--disable-userns" not in argv
    assert "--new-session" not in argv
    _option_index(argv, "--unshare-uts", "--die-with-parent", "--cap-drop", "ALL", "--tmpfs")
    assert "--clearenv" not in argv and "--setenv" not in argv
    assert environment == {
        "PMI_RANK": "3",
        "PMIX_NAMESPACE": "ns",
        "OMPI_COMM_WORLD_SIZE": "4",
        "OPAL_PREFIX": "/opt/ompi",
        "SLURM_PROCID": "3",
        "SLURMD_NODENAME": "n1",
        "OMPI_MCA_btl_vader_single_copy_mechanism": "none",
        "HOME": "/tmp/home",
        "TMPDIR": "/tmp",
    }
    tmpfs = _option_index(argv, "--tmpfs", "/tmp", "--dir", "/tmp/home")
    readonly = argv.index("/usr")
    workspace = _option_index(argv, "--ro-bind-fd", argv[argv.index(str(launch.workspace)) - 1], str(launch.workspace))
    job = _option_index(argv, "--bind-fd", argv[argv.index(str(launch.job)) - 1], str(launch.job))
    dev = _option_index(argv, "--proc", "/proc", "--dev", "/dev")
    shm = argv.index("/dev/shm")
    assert tmpfs < readonly < workspace < job < dev < shm
    assert argv[shm - 2] == "--bind-fd"
    separator = argv.index("--")
    assert argv[separator - 2 : separator] == ["--chdir", str(launch.job)]
    assert argv[separator + 1 :] == [
        sys.executable,
        "-I",
        "-m",
        "httk.workflow._confine_exec",
        "--request",
        str(launch.request_path),
        "--request-id",
        launch.trusted.request_id,
        "--attempt-id",
        launch.trusted.attempt_id,
    ]


def test_no_environment_value_appears_in_the_rank_argv(launch: _Launch, opened: _Opened) -> None:
    step = launch.pmix_root / "pmix-secret-dir"
    step.mkdir()
    secrets = {
        "PMI_RANK": "secret-rank-value",
        "PMIX_SERVER_URI": "secret-pmix-uri",
        "PMIX_SERVER_TMPDIR": str(step),
        "OMPI_MCA_secret": "secret-ompi",
        "SLURM_JOB_ID": "secret-job",
    }
    sensitive = ("secret-confine-value",)
    launch.trusted = replace(
        launch.trusted,
        confine=replace(launch.trusted.confine, environment=(("SITE_TOKEN", sensitive[0]),)),
    )
    argv, environment = _prepare(launch, opened, secrets)
    assert environment["SITE_TOKEN"] == sensitive[0] and environment["PMIX_SERVER_URI"] == "secret-pmix-uri"
    joined = "\0".join(argv)
    for name, value in environment.items():
        assert name not in argv, name
        if name != "PMIX_SERVER_TMPDIR" and value not in ("/tmp", "/tmp/home"):
            assert value not in joined, name
    assert argv.count(str(step)) == 1, "the PMIx directory appears only as its bind destination"


def test_argv_blocks_user_namespaces_when_the_launch_says_so(launch: _Launch, opened: _Opened) -> None:
    launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, block_userns=True))
    argv, _environment = _prepare(launch, opened, {})
    _option_index(argv, "--unshare-uts", "--disable-userns", "--assert-userns-disabled", "--die-with-parent")


def test_rank_environment_refuses_invalid_captured_variables(launch: _Launch) -> None:
    with pytest.raises(ValueError, match="invalid rank environment name"):
        _confine_rank.rank_environment({"PMIX_A-B": "1"}, launch.trusted)
    with pytest.raises(ValueError, match="exceeds"):
        _confine_rank.rank_environment({"PMI_A": "x" * (128 * 1024 + 1)}, launch.trusted)


def test_pmix_directory_is_bound_only_below_an_approved_root(launch: _Launch, opened: _Opened) -> None:
    step = launch.pmix_root / "spool" / "pmix.7.0"
    step.mkdir(parents=True)
    argv, environment = _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(step)})
    assert environment["PMIX_SERVER_TMPDIR"] == str(step)
    assert argv.count(str(step)) == 1
    index = argv.index(str(step))
    assert argv[index - 2] == "--bind-fd"
    assert index > argv.index("/dev/shm")
    parents = [argv[position + 1] for position in range(index - 2) if argv[position] == "--dir"]
    assert str(step.parent) in parents and str(launch.pmix_root) in parents
    assert parents.index(str(launch.pmix_root)) < parents.index(str(step.parent))

    outside = launch.root / "elsewhere"
    outside.mkdir()
    with pytest.raises(ValueError, match=r"confine\.pmix_roots"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(outside)})
    with pytest.raises(ValueError, match=r"confine\.pmix_roots"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(launch.pmix_root)})
    with pytest.raises(ValueError, match="absolute"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": "relative/pmix"})
    with pytest.raises(ValueError, match="unavailable"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(launch.pmix_root / "missing")})
    link = launch.pmix_root / "link"
    link.symlink_to(step)
    with pytest.raises(OSError):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(link)})
    escape = launch.pmix_root / "escape"
    escape.symlink_to(outside)
    with pytest.raises(ValueError, match=r"confine\.pmix_roots"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(escape)})


def test_pmix_directory_must_not_overlap_the_workspace(launch: _Launch, opened: _Opened) -> None:
    launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, pmix_roots=(launch.root,)))
    with pytest.raises(ValueError, match="overlaps"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(launch.job)})
    launch.trusted = replace(
        launch.trusted, confine=replace(launch.trusted.confine, pmix_roots=(launch.shm_root.parent,))
    )
    with pytest.raises(ValueError, match="overlaps"):
        _prepare(launch, opened, {"PMIX_SERVER_TMPDIR": str(launch.shm_root)})


def _subdirectory_device() -> Path | None:
    for path in ("/dev/pts/ptmx", "/dev/net/tun", "/dev/dri/card0"):
        if os.path.exists(path) and stat.S_ISCHR(os.stat(path).st_mode):
            return Path(path)
    return None


def test_devices_are_bound_after_dev_with_parent_dirs(launch: _Launch, opened: _Opened) -> None:
    devices = [Path("/dev/null")]
    nested = _subdirectory_device()
    if nested is not None:
        devices.append(nested)
    launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, devices=tuple(devices)))
    argv, _environment = _prepare(launch, opened, {})
    dev = _option_index(argv, "--proc", "/proc", "--dev", "/dev")
    null = argv.index("/dev/null")
    assert argv[null - 2] == "--bind-fd" and null > dev
    if nested is not None:
        parent = _option_index(argv, "--dir", str(nested.parent))
        assert dev < parent < argv.index(str(nested))
    launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, devices=(Path("/dev/shm"),)))
    with pytest.raises(ValueError, match="not a device"):
        _prepare(launch, opened, {})


def test_readonly_path_inside_the_workspace_is_refused(launch: _Launch, opened: _Opened) -> None:
    launch.trusted = replace(
        launch.trusted, confine=replace(launch.trusted.confine, readonly_paths=(launch.workspace,))
    )
    with pytest.raises(ValueError, match="inside the workspace"):
        _prepare(launch, opened, {})


def test_read_launch_refusals(launch: _Launch) -> None:
    with pytest.raises(ValueError, match="--launch-dir"):
        _confine_rank.read_launch(launch.launch_dir.parent)
    with pytest.raises(ValueError, match="--launch-dir"):
        _confine_rank.read_launch(Path("relative") / ".httk-workspace/managers/m/launches/x")
    # A trusted directory is named by attempt and request: neither alone is enough.
    for other_name in (
        trusted_name(launch.trusted.attempt_id, new_request_id()),
        trusted_name(str(uuid.uuid4()), launch.trusted.request_id),
        launch.trusted.request_id,
    ):
        other = launch.launch_dir.parent / other_name
        shutil.copytree(launch.launch_dir, other)
        with pytest.raises(ValueError, match="another attempt or request"):
            _confine_rank.read_launch(other)

    linked_workspace = launch.root / "linked"
    linked_workspace.symlink_to(launch.workspace)
    trusted, workspace, descriptor = _confine_rank.read_launch(
        linked_workspace / launch.launch_dir.relative_to(launch.workspace)
    )
    os.close(descriptor)
    assert trusted == launch.trusted and workspace == launch.workspace

    managers = launch.workspace / ".httk-workspace" / "managers"
    shutil.move(managers / "m1", launch.root / "m1")
    (managers / "m1").symlink_to(launch.root / "m1")
    with pytest.raises(OSError):
        _confine_rank.read_launch(launch.launch_dir)
    (managers / "m1").unlink()
    shutil.move(launch.root / "m1", managers / "m1")

    document = launch.launch_dir / "launch.json"
    document.rename(launch.root / "launch.json")
    document.symlink_to(launch.root / "launch.json")
    with pytest.raises(ValueError, match="symlink"):
        _confine_rank.read_launch(launch.launch_dir)
    document.unlink()
    (launch.root / "launch.json").rename(document)

    other_workspace = launch.root / "other"
    other_workspace.mkdir()
    launch.write(replace(launch.trusted, workspace_root=other_workspace))
    with pytest.raises(ValueError, match="workspace"):
        _confine_rank.read_launch(launch.launch_dir)


def test_open_job_refusals(launch: _Launch) -> None:
    workspace = launch.workspace
    with pytest.raises(ValueError, match="job key"):
        _confine_rank.open_job(replace(launch.trusted, job_key="plain"), workspace)
    nested = f"relax--{uuid.uuid4()}"
    with pytest.raises(ValueError, match="job directories never nest"):
        _confine_rank.open_job(replace(launch.trusted, placement=f"project/{nested}"), workspace)
    with pytest.raises(ValueError, match="placement"):
        _confine_rank.open_job(replace(launch.trusted, placement=".httk-workspace"), workspace)
    linked = workspace / "jobs" / "project" / f"linked--{uuid.uuid4()}"
    linked.symlink_to(launch.job)
    with pytest.raises(ValueError, match="symlink"):
        _confine_rank.open_job(replace(launch.trusted, job_key=linked.name), workspace)

    # Placement directories are operator layout and followed; the job's real path is used.
    scratch = launch.root / "scratch"
    scratch.mkdir()
    (workspace / "jobs" / "elsewhere").symlink_to(scratch)
    moved = scratch / launch.job.name
    moved.mkdir()
    job, descriptor = _confine_rank.open_job(replace(launch.trusted, placement="elsewhere"), workspace)
    os.close(descriptor)
    assert job == moved

    # Placement symlinks may point anywhere except the workspace's own control trees.
    for name in ("logs", "exchange", "postprocess", ".httk-workspace"):
        (workspace / name).mkdir(exist_ok=True)
        (workspace / name / launch.job.name).mkdir()
        (workspace / "jobs" / f"to-{name}").symlink_to(workspace / name)
        with pytest.raises(ValueError, match="control directory"):
            _confine_rank.open_job(replace(launch.trusted, placement=f"to-{name}"), workspace)


def test_shared_memory_is_shared_and_removed_by_the_last_rank(launch: _Launch) -> None:
    token = launch.trusted.token
    first = _confine_rank.join_shared_memory(launch.shm_root, token)
    second = _confine_rank.join_shared_memory(launch.shm_root, token)
    assert os.path.samestat(os.fstat(first.fd), os.fstat(second.fd))
    information = os.stat(launch.shm)
    assert stat.S_IMODE(information.st_mode) == 0o700 and information.st_uid == os.geteuid()
    _confine_rank.leave_shared_memory(first)
    assert launch.shm.is_dir()
    _confine_rank.leave_shared_memory(second)
    assert not launch.shm.exists()
    third = _confine_rank.join_shared_memory(launch.shm_root, token)
    assert launch.shm.is_dir()
    _confine_rank.leave_shared_memory(third)
    assert not launch.shm.exists()


def test_shared_memory_refuses_unsafe_existing_entries(launch: _Launch, monkeypatch: pytest.MonkeyPatch) -> None:
    token = launch.trusted.token
    launch.shm.mkdir(mode=0o755)
    launch.shm.chmod(0o755)
    with pytest.raises(ValueError, match="mode 0700"):
        _confine_rank.join_shared_memory(launch.shm_root, token)
    launch.shm.rmdir()
    target = launch.root / "target"
    target.mkdir(mode=0o700)
    launch.shm.symlink_to(target)
    with pytest.raises(ValueError, match="not a real directory"):
        _confine_rank.join_shared_memory(launch.shm_root, token)
    launch.shm.unlink()
    launch.shm.write_bytes(b"")
    with pytest.raises(ValueError, match="not a real directory"):
        _confine_rank.join_shared_memory(launch.shm_root, token)
    launch.shm.unlink()
    launch.shm.mkdir(mode=0o700)
    (launch.shm / ".lock").symlink_to(launch.root / "elsewhere")
    with pytest.raises(OSError):
        _confine_rank.join_shared_memory(launch.shm_root, token)
    (launch.shm / ".lock").unlink()
    real_uid = os.geteuid()
    monkeypatch.setattr(_confine_rank, "_check_shm_root", lambda _descriptor, _path: None)
    monkeypatch.setattr(os, "geteuid", lambda: real_uid + 1)
    with pytest.raises(ValueError, match="owned by this user"):
        _confine_rank.join_shared_memory(launch.shm_root, token)


def test_shared_memory_root_must_be_safe(launch: _Launch) -> None:
    launch.shm_root.chmod(0o777)
    try:
        with pytest.raises(ValueError, match="world-writable"):
            _confine_rank.join_shared_memory(launch.shm_root, launch.trusted.token)
    finally:
        launch.shm_root.chmod(0o700)
    linked = launch.root / "shm-link"
    linked.symlink_to(launch.shm_root)
    with pytest.raises(OSError):
        _confine_rank.join_shared_memory(linked, launch.trusted.token)


def _run_helper(
    launch: _Launch, environ: dict[str, str] | None = None, *, wait: bool = True, pass_fds: tuple[int, ...] = ()
) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [sys.executable, "-I", "-m", "httk.workflow._confine_rank", "--launch-dir", str(launch.launch_dir)],
        env={"PATH": os.environ["PATH"], **(environ or {})},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        pass_fds=pass_fds,
    )
    if wait:
        process.wait(timeout=_TIMEOUT)
    return process


def test_end_to_end_with_fake_bwrap(launch: _Launch) -> None:
    (launch.job / "sub").mkdir()
    script = 'pwd; echo "$FOO|$PMI_RANK|$OMPI_MCA_btl_vader_single_copy_mechanism|$HOME|$SLURM_CONF|$X"; exit 7'
    launch.request(("sh", "-c", script), cwd="sub", environment={"PATH": os.environ["PATH"], "FOO": "bar"})
    process = _run_helper(launch, {"PMI_RANK": "3", "SLURM_CONF": "/etc/slurm.conf", "X": "helper"})
    stdout, stderr = process.communicate()
    assert process.returncode == 7, stderr
    assert stdout.decode().splitlines() == [str(launch.job / "sub"), "bar|3|none|/tmp/home||"]
    assert not launch.shm.exists()


def test_helper_refuses_pmix_outside_the_roots(launch: _Launch) -> None:
    launch.request(("true",))
    outside = launch.root / "pmix-outside"
    outside.mkdir()
    process = _run_helper(launch, {"PMIX_SERVER_TMPDIR": str(outside)})
    _stdout, stderr = process.communicate()
    assert process.returncode == 2
    assert b"confine.pmix_roots" in stderr
    assert not launch.shm.exists()


def test_helper_reports_signal_exit_and_forwards_signals(launch: _Launch) -> None:
    launch.request(("sh", "-c", 'touch started; exec sleep 30'))
    process = _run_helper(launch, wait=False)
    try:
        _wait_for(launch.job / "started", process)
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=_TIMEOUT) == 128 + signal.SIGTERM
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
    assert not launch.shm.exists()


def _alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rpartition(")")[2].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return False
    return state != "Z"


def test_helper_signal_reaches_the_whole_rank_group(launch: _Launch) -> None:
    launch.request(("sh", "-c", 'sleep 30 & echo "$!" > bg.pid; echo "$$" > sh.pid; touch started; wait'))
    process = _run_helper(launch, wait=False)
    try:
        _wait_for(launch.job / "started", process)
        background = int((launch.job / "bg.pid").read_text())
        shell = int((launch.job / "sh.pid").read_text())
        assert os.getpgid(background) == os.getpgid(shell) == shell != os.getpgid(process.pid)
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=_TIMEOUT) == 128 + signal.SIGTERM
        assert not _alive(shell) and not _alive(background)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
    assert not launch.shm.exists()


def test_helper_keeps_shared_memory_until_the_rank_group_is_gone(launch: _Launch) -> None:
    # The survivor reports "started" only once its TERM trap is in place, so
    # the signal below cannot arrive before the trap is set.
    survivor = '(trap "" TERM; touch started; while [ ! -e release ]; do sleep 0.05; done; touch survived) &'
    launch.request(("sh", "-c", f"{survivor} wait"))
    process = _run_helper(launch, wait=False)
    try:
        _wait_for(launch.job / "started", process)
        process.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        assert process.poll() is None, "the helper exited while a rank process was alive"
        assert launch.shm.is_dir()
        (launch.job / "release").touch()
        assert process.wait(timeout=_TIMEOUT) == 128 + signal.SIGTERM
        assert (launch.job / "survived").exists()
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
    assert not launch.shm.exists()


def _wait_for(path: Path, *processes: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + _TIMEOUT
    while not path.exists():
        for process in processes:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                pytest.fail(f"helper exited with {process.returncode}: {stdout!r} {stderr!r}")
        assert time.monotonic() < deadline, f"{path} did not appear"
        time.sleep(0.02)


def test_concurrent_helpers_share_shm_and_the_last_removes_it(launch: _Launch) -> None:
    script = 'touch "started.$PMI_RANK"; while [ ! -e "release.$PMI_RANK" ]; do sleep 0.02; done'
    launch.request(("sh", "-c", script))
    first = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    second = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
    try:
        _wait_for(launch.job / "started.0", first, second)
        _wait_for(launch.job / "started.1", first, second)
        assert launch.shm.is_dir()
        (launch.job / "release.0").touch()
        assert first.wait(timeout=_TIMEOUT) == 0
        assert launch.shm.is_dir(), "the first rank removed shared memory still in use"
        (launch.job / "release.1").touch()
        assert second.wait(timeout=_TIMEOUT) == 0
        assert not launch.shm.exists()
    finally:
        for process in (first, second):
            if process.poll() is None:
                process.kill()
            process.communicate()


def _exec(launch: _Launch, monkeypatch: pytest.MonkeyPatch, **changes: str) -> int:
    monkeypatch.chdir(launch.root)
    arguments = {
        "--request": str(launch.request_path),
        "--request-id": launch.trusted.request_id,
        "--attempt-id": launch.trusted.attempt_id,
    } | changes
    return _confine_exec.main([item for pair in arguments.items() for item in pair])


def test_inner_exec_refusals(
    launch: _Launch, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    launch.request(("/nonexistent/program",))
    assert _exec(launch, monkeypatch, **{"--request-id": new_request_id()}) == 2
    assert _exec(launch, monkeypatch, **{"--attempt-id": str(uuid.uuid4())}) == 2
    assert _exec(launch, monkeypatch, **{"--attempt-id": "x"}) == 2
    assert _exec(launch, monkeypatch, **{"--request": str(launch.job / "request.json")}) == 2
    assert _exec(launch, monkeypatch, **{"--request": f"relative/{launch.trusted.request}"}) == 2
    assert "refused" in capsys.readouterr().err

    assert _exec(launch, monkeypatch) == 127
    assert "cannot execute" in capsys.readouterr().err

    elsewhere = launch.root / "elsewhere"
    (elsewhere / "inner").mkdir(parents=True)
    (launch.job / "link").symlink_to(elsewhere)
    launch.request(("true",), cwd="link/inner")
    assert _exec(launch, monkeypatch) == 2
    launch.request(("true",), cwd="missing")
    assert _exec(launch, monkeypatch) == 2

    real = launch.root / "request.json"
    launch.request_path.rename(real)
    launch.request_path.symlink_to(real)
    assert _exec(launch, monkeypatch) == 2
    assert "symlink" in capsys.readouterr().err
    launch.request_path.unlink()
    launch.request_path.write_bytes(b" " * (1024 * 1024 + 1))
    assert _exec(launch, monkeypatch) == 2
    assert "exceeds" in capsys.readouterr().err

    launch.request_path.unlink()
    launch_directory = launch.request_path.parent
    moved = launch.root / "launch-moved"
    launch_directory.rename(moved)
    launch_directory.symlink_to(moved)
    launch.request(("true",))
    assert _exec(launch, monkeypatch) == 2


def test_open_pmi_relay_needs_an_open_socket(tmp_path: Path) -> None:
    assert _confine_rank.open_pmi_relay({}) is None
    assert _confine_rank.open_pmi_relay({"PMI_FD": ""}) is None
    closed = os.open(tmp_path, os.O_RDONLY)
    os.close(closed)
    with (tmp_path / "file").open("wb") as regular:
        for raw in ("abc", "-1", " 3", "3 ", "+3", "1234567890", str(closed), str(regular.fileno())):
            with pytest.raises(ValueError, match="is not an open socket"):
                _confine_rank.open_pmi_relay({"PMI_FD": raw})
    slurm, server = socket.socketpair()
    try:
        os.set_inheritable(server.fileno(), True)
        opened = _confine_rank.open_pmi_relay({"PMI_FD": str(server.fileno())})
        assert opened is not None
        assert opened[0] == server.fileno() and len(set(opened)) == 3
        assert not os.get_inheritable(opened[0])
        for descriptor in opened[1:]:
            assert stat.S_ISSOCK(os.fstat(descriptor).st_mode)
            os.close(descriptor)
    finally:
        slurm.close()
        server.close()


def _with_mode(launch: _Launch, mode: Literal["on", "off", "auto"]) -> TrustedLaunch:
    return replace(launch.trusted, confine=replace(launch.trusted.confine, block_mpi_spawn=mode))


def test_slurm_pmi_fd_parses_the_descriptor_number(tmp_path: Path) -> None:
    assert _confine_rank.slurm_pmi_fd({}) is None
    assert _confine_rank.slurm_pmi_fd({"PMI_FD": ""}) is None
    for raw in ("abc", "-1", " 3", "1234567890"):
        with pytest.raises(ValueError, match="is not a descriptor number"):
            _confine_rank.slurm_pmi_fd({"PMI_FD": raw})
    descriptor = os.open(tmp_path, os.O_RDONLY)
    try:
        assert _confine_rank.slurm_pmi_fd({"PMI_FD": str(descriptor)}) == descriptor
    finally:
        os.close(descriptor)
    with pytest.raises(ValueError, match="is not an open descriptor"):
        _confine_rank.slurm_pmi_fd({"PMI_FD": str(descriptor)})


@pytest.mark.parametrize(
    ("mode", "has_fd", "relayed"),
    [("on", True, True), ("on", False, True), ("off", True, False), ("auto", True, True), ("auto", False, False)],
)
def test_prepare_pmi_plumbing_per_mode(
    launch: _Launch, opened: _Opened, mode: Literal["on", "off", "auto"], has_fd: bool, relayed: bool
) -> None:
    trusted = _with_mode(launch, mode)
    other, passed = socket.socketpair()
    pmi_fd = passed.detach() if has_fd else None
    environ = {"PMI_PORT": "node1:4242", "PMI_RANK": "3"}
    if has_fd:  # never the passed number, so a reassigned PMI_FD shows
        environ["PMI_FD"] = "99"
    assert _confine_rank.blocks_mpi_spawn(trusted, environ) is relayed
    prepared, environment = _confine_rank.prepare_rank_sandbox(
        trusted,
        workspace=opened.workspace,
        workspace_fd=opened.workspace_fd,
        job=opened.job,
        job_fd=opened.job_fd,
        shm_fd=opened.shm_fd,
        environ=environ,
        pmi_fd=pmi_fd,
    )
    try:
        if pmi_fd is not None:
            assert pmi_fd in prepared.descriptors
            assert os.get_inheritable(pmi_fd)
        assert environment.get("PMI_FD") == ((str(pmi_fd) if relayed else "99") if has_fd else None)
        assert environment.get("PMI_PORT") == (None if relayed else "node1:4242")
        assert environment["PMI_RANK"] == "3"
    finally:
        prepared.close()
        other.close()
        if pmi_fd is None:
            passed.close()
    if pmi_fd is not None:
        with pytest.raises(OSError):
            os.fstat(pmi_fd)


class _UnstartableThread(threading.Thread):
    def start(self) -> None:
        raise RuntimeError("can't start new thread")


def test_run_refuses_when_the_pmi_relay_cannot_start(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(_confine_rank.threading, "Thread", _UnstartableThread)
    saved = {signum: signal.getsignal(signum) for signum in _confine_rank._FORWARDED_SIGNALS}
    first, second = socket.socketpair()
    pmi = (first.detach(), second.detach())
    prepared = PreparedSandbox([sys.executable, "-c", "import time; time.sleep(60)"], ())
    started = time.monotonic()
    try:
        assert _confine_rank._run(prepared, dict(os.environ), pmi) == 2
    finally:
        for signum, handler in saved.items():
            signal.signal(signum, handler)
    assert time.monotonic() - started < 30  # the rank group was killed, not waited out
    assert "refused: cannot start the PMI relay: can't start new thread" in capsys.readouterr().err
    for descriptor in pmi:
        with pytest.raises(OSError):
            os.fstat(descriptor)


_PMI_CLIENT = """
import os, sys
fd = int(os.environ["PMI_FD"])
def ask(data):
    os.write(fd, data)
    reply = b""
    while not reply.endswith(b"\\n"):
        chunk = os.read(fd, 1)
        if not chunk:
            break
        reply += chunk
    print(reply.decode().strip())
for request in sys.argv[2:]:
    ask(request.encode())
original = int(sys.argv[1])
try:
    os.fstat(original)
    print("original open")
except OSError:
    print("original closed")
print("distinct" if fd != original else "same")
print("PMI_PORT=" + os.environ.get("PMI_PORT", "-"))
"""


def _pmi_session(
    launch: _Launch, python: str, answer: Callable[[socket.socket, bytes], None] | None = None
) -> tuple[subprocess.Popen[bytes], list[bytes], threading.Thread]:
    slurm, helper_end = socket.socketpair()
    original = helper_end.fileno()
    requests = (INIT, SPAWN % (1, 1), b"cmd=barrier_in\n", b"cmd=finalize\n")
    launch.request((python, "-I", "-c", _PMI_CLIENT, str(original), *(item.decode() for item in requests)))
    received: list[bytes] = []
    server = threading.Thread(target=serve_slurm, args=(slurm, received, answer), daemon=True)
    server.start()
    try:
        environ = {"PMI_FD": str(original), "PMI_PORT": "node1:4242"}
        process = _run_helper(launch, environ, wait=False, pass_fds=(original,))
    finally:
        helper_end.close()
    process.wait(timeout=_TIMEOUT)
    server.join(5)
    slurm.close()
    return process, received, server


def _assert_pmi_session(process: subprocess.Popen[bytes], received: list[bytes], server: threading.Thread) -> None:
    stdout, stderr = process.communicate()
    assert process.returncode == 0, stderr
    assert stdout.decode().splitlines() == [
        REPLIES[b"init"].decode().strip(),
        "cmd=spawn_result rc=-1",
        REPLIES[b"barrier_in"].decode().strip(),
        REPLIES[b"finalize"].decode().strip(),
        "original closed",
        "distinct",
        "PMI_PORT=-",
    ]
    assert not server.is_alive()  # the helper closed Slurm's socket when it exited
    assert received == [INIT, b"cmd=barrier_in\n", b"cmd=finalize\n"]
    assert b"httk-workflow rank: PMI refused MPI_Comm_spawn" in stderr


def test_helper_relays_pmi_and_refuses_spawn(launch: _Launch) -> None:
    _assert_pmi_session(*_pmi_session(launch, sys.executable))
    assert not launch.shm.exists()


def _answer_spawn(slurm: socket.socket, line: bytes) -> None:
    if line == b"endcmd\n":
        slurm.sendall(b"cmd=spawn_result rc=0\n")
    elif (name := line[4:].split(b" ", 1)[0].strip()) in REPLIES:
        slurm.sendall(REPLIES[name])


def test_block_mpi_spawn_off_passes_slurms_pmi_fd(launch: _Launch) -> None:
    launch.write(_with_mode(launch, "off"))
    process, received, server = _pmi_session(launch, sys.executable, _answer_spawn)
    stdout, stderr = process.communicate()
    assert process.returncode == 0, stderr
    assert stdout.decode().splitlines() == [
        REPLIES[b"init"].decode().strip(),
        "cmd=spawn_result rc=0",
        REPLIES[b"barrier_in"].decode().strip(),
        REPLIES[b"finalize"].decode().strip(),
        "original open",
        "same",
        "PMI_PORT=node1:4242",
    ]
    assert not server.is_alive()
    assert received == [INIT, *(SPAWN % (1, 1)).splitlines(keepends=True), b"cmd=barrier_in\n", b"cmd=finalize\n"]
    assert b"PMI" not in stderr


def test_block_mpi_spawn_auto_without_pmi_fd_keeps_pmi_port(launch: _Launch) -> None:
    launch.write(_with_mode(launch, "auto"))
    launch.request(("sh", "-c", 'echo "$PMI_PORT|${PMI_FD-unset}"'))
    process = _run_helper(launch, {"PMI_PORT": "node1:4242"})
    stdout, stderr = process.communicate()
    assert process.returncode == 0, stderr
    assert stdout.decode().splitlines() == ["node1:4242|unset"]


def test_helper_refuses_a_pmi_fd_that_is_not_a_socket(launch: _Launch) -> None:
    launch.request(("sh", "-c", "touch ran"))
    read, write = os.pipe()
    try:
        process = _run_helper(launch, {"PMI_FD": str(read)}, wait=False, pass_fds=(read,))
        _stdout, stderr = process.communicate(timeout=_TIMEOUT)
    finally:
        os.close(read)
        os.close(write)
    assert process.returncode == 2
    assert b"refused: PMI_FD" in stderr
    assert not (launch.job / "ran").exists()
    assert not launch.shm.exists()


def _use_real_bwrap(launch: _Launch) -> None:
    bwrap = shutil.which("bwrap")
    if bwrap is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    settings = confine_settings({"manager.confine": "bwrap"})
    try:
        block_userns = probe_bwrap(settings)
    except ConfinementUnavailableError as exc:
        detail = str(exc).lower()
        if not any(
            phrase in detail
            for phrase in ("operation not permitted", "permission denied", "no permissions to create", "user namespace")
        ):
            pytest.fail(f"Bubblewrap sandbox failed unexpectedly: {exc}")
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail(f"required Bubblewrap sandbox is unavailable: {exc}")
        pytest.skip(f"Bubblewrap user namespaces are unavailable: {exc}")
    launch.write(
        replace(
            launch.trusted,
            confine=replace(
                launch.trusted.confine,
                bwrap=Path(bwrap).resolve(),
                block_userns=block_userns,
                readonly_paths=default_readonly_paths(),
            ),
        )
    )


def test_real_bwrap_rank_writes_only_its_job_directory(launch: _Launch) -> None:
    _use_real_bwrap(launch)
    sibling = launch.workspace / "jobs" / "project" / f"other--{uuid.uuid4()}"
    sibling.mkdir()
    script = (
        'for target in "$1/own" "$2/x" "$3/x" "/dev/shm/x"; do\n'
        '  if (echo data > "$target") 2>/dev/null; then echo "writable $target"; else echo "refused $target"; fi\n'
        "done\n"
    )
    launch.request(("/bin/sh", "-c", script, "sh", str(launch.job), str(sibling), str(launch.workspace)))
    process = _run_helper(launch)
    stdout, stderr = process.communicate()
    assert process.returncode == 0, stderr
    assert stdout.decode().splitlines() == [
        f"writable {launch.job}/own",
        f"refused {sibling}/x",
        f"refused {launch.workspace}/x",
        "writable /dev/shm/x",
    ]
    assert not (sibling / "x").exists() and not (launch.workspace / "x").exists()
    assert not launch.shm.exists()


def test_real_bwrap_rank_relays_pmi_and_refuses_spawn(launch: _Launch) -> None:
    _use_real_bwrap(launch)
    python = shutil.which("python3", path="/usr/bin:/bin")
    if python is None:
        pytest.skip("no system python3 to run the PMI client in the sandbox")
    _assert_pmi_session(*_pmi_session(launch, python))
    assert not launch.shm.exists()


_FDINFO = "pos:\t0\nflags:\t02500000\nmnt_id:\t771\nino:\t1\n"
_MOUNTINFO = (
    "22 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw\n"
    "771 862 0:61 / /dev/shm rw,nosuid,nodev,noexec,relatime shared:2 - tmpfs shm rw,size=1048576k\n"
    "900 862 0:70 / /mnt/with\\040space rw,relatime - tmpfs tmp rw\n"
)


def _fake_proc(monkeypatch: pytest.MonkeyPatch, fdinfo: str, mountinfo: str) -> None:
    real_open = builtins.open

    def fake_open(path: object, *args: object, **kwargs: object) -> object:
        text = {"/proc/self/mountinfo": mountinfo}.get(str(path))
        if str(path).startswith("/proc/self/fdinfo/"):
            text = fdinfo
        if text is None:
            return real_open(path, *args, **kwargs)  # type: ignore[call-overload]
        return io.StringIO(text)

    monkeypatch.setattr(builtins, "open", fake_open)


def test_filesystem_type_is_read_from_fdinfo_and_mountinfo(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_proc(monkeypatch, _FDINFO, _MOUNTINFO)
    assert _confine_rank._filesystem_type(5) == "tmpfs"
    _fake_proc(monkeypatch, _FDINFO.replace("771", "900"), _MOUNTINFO)
    assert _confine_rank._filesystem_type(5) == "tmpfs"
    _fake_proc(monkeypatch, _FDINFO.replace("771", "22"), _MOUNTINFO)
    assert _confine_rank._filesystem_type(5) == "ext4"
    _fake_proc(monkeypatch, _FDINFO.replace("771", "5"), _MOUNTINFO)
    with pytest.raises(OSError, match="not in mountinfo"):
        _confine_rank._filesystem_type(5)


def test_shm_root_that_is_not_a_tmpfs_is_refused(launch: _Launch, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_confine_rank, "_filesystem_type", lambda _descriptor: "ext4")
    with pytest.raises(ValueError, match=r"confine\.shm_root .* must be a tmpfs, not ext4"):
        _confine_rank.join_shared_memory(launch.shm_root, launch.trusted.token)
    descriptor = os.open(launch.shm_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        monkeypatch.setattr(
            _confine_rank, "_filesystem_type", lambda _descriptor: (_ for _ in ()).throw(OSError("no /proc"))
        )
        with pytest.raises(ValueError, match="cannot be read: no /proc"):
            _confine_rank._check_shm_root(descriptor, launch.shm_root)
    finally:
        os.close(descriptor)


def test_the_host_shm_directory_passes_the_check(launch: _Launch) -> None:
    # The fixture skips when /dev/shm is no tmpfs; a directory made there must then pass for real.
    descriptor = os.open(launch.shm_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        _confine_rank._check_shm_root(descriptor, launch.shm_root)
    finally:
        os.close(descriptor)
