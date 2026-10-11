"""The rank helper and inner exec of a confined launch, with the plumbing-only fake Bubblewrap."""

import builtins
import ctypes
import ctypes.util
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

from fake_bwrap import require_namespaces, write_wrapper
from httk.workflow import Workspace, _confine_exec, _confine_keeper, _confine_rank, _fs, _kernel
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
from test_pmi_proxy import INIT, REPLIES, SPAWN, serve_slurm

_TIMEOUT = 60.0
_WORKSPACE_ID = "0f6b6c2a-1d55-4c69-8d43-7c0a1b2c3d4e"


@dataclass
class _Launch:
    root: Path
    workspace: Path
    owner_id: str
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
    return write_wrapper(directory / "bwrap")


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
    Workspace.initialize(workspace, durable=False, policy={"visibility_deadline_seconds": 0.05})
    owner_id = uuid.uuid4().hex
    job_key = f"relax--{uuid.uuid4()}"
    job = workspace / "jobs" / "owned" / owner_id / _kernel.format_job_name(job_key, 500, _fs.fresh_token(), "ready")
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
        job_key=job_key,
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
    launches = workspace / ".httk-workspace" / "owners" / owner_id / "launches"
    launch_dir = launches / trusted_name(attempt_id, request_id)
    created = _Launch(root, workspace, owner_id, job, shm_root, pmix_root, launch_dir, trusted)
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
    job, job_fd = _confine_rank.open_job(trusted, workspace, launch.owner_id)
    shm_fd = os.open(launch.shm_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        yield _Opened(workspace, workspace_fd, job, job_fd, shm_fd)
    finally:
        for descriptor in (workspace_fd, job_fd, shm_fd):
            os.close(descriptor)


def _prepare(launch: _Launch, opened: _Opened, environ: dict[str, str]) -> tuple[list[str], dict[str, str]]:
    environment = _confine_rank.prepare_rank_environment(launch.trusted, environ)
    prepared = _confine_rank.prepare_keeper_sandbox(
        launch.trusted,
        workspace=opened.workspace,
        workspace_fd=opened.workspace_fd,
        job=opened.job,
        job_fd=opened.job_fd,
        shm_fd=opened.shm_fd,
        environment=environment,
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
    workspace_tmpfs = _option_index(argv, "--tmpfs", str(launch.workspace))
    readonly = argv.index("/usr")
    jobs = _option_index(
        argv, "--ro-bind-fd", argv[argv.index(str(launch.workspace / "jobs")) - 1], str(launch.workspace / "jobs")
    )
    workflows = _option_index(
        argv,
        "--ro-bind-fd",
        argv[argv.index(str(launch.workspace / "workflows")) - 1],
        str(launch.workspace / "workflows"),
    )
    format_copy = _option_index(
        argv,
        "--ro-bind-data",
        argv[argv.index(str(launch.workspace / ".httk-workspace" / "format.json")) - 1],
        str(launch.workspace / ".httk-workspace" / "format.json"),
    )
    job = _option_index(argv, "--bind-fd", argv[argv.index(str(launch.job)) - 1], str(launch.job))
    dev = _option_index(argv, "--dev", "/dev")
    shm = argv.index("/dev/shm")
    proc = _option_index(argv, "--proc", "/proc")
    assert tmpfs < readonly < workspace_tmpfs < jobs < workflows < format_copy < job < dev < shm < proc
    assert argv[workspace_tmpfs : workspace_tmpfs + 2] == ["--tmpfs", str(launch.workspace)]
    assert workspace_tmpfs + 2 == jobs
    assert not any(
        option in {"--ro-bind-fd", "--bind-fd", "--ro-bind-data"} and argv[index + 2] == str(launch.workspace)
        for index, option in enumerate(argv[:-2])
    )
    assert argv.count("--tmpfs") == 2
    assert [argv[index + 1] for index, option in enumerate(argv) if option == "--tmpfs"] == [
        "/tmp",
        str(launch.workspace),
    ]
    remount = _option_index(argv, "--remount-ro", str(launch.workspace), "--remount-ro", "/")
    assert argv[remount : remount + 4] == ["--remount-ro", str(launch.workspace), "--remount-ro", "/"]
    assert argv[remount + 4 : argv.index("--chdir")] == []
    assert argv[shm - 2] == "--bind-fd"
    separator = argv.index("--")
    assert argv[separator - 2 : separator] == ["--chdir", "/"]
    assert argv[separator + 1 :] == [sys.executable, "-I", "-S", "-c", _confine_keeper.KEEPER_PROGRAM]
    assert _confine_rank.inner_command(launch.trusted, launch.job) == [
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


def test_keeper_descriptors_match_bind_options(launch: _Launch, opened: _Opened) -> None:
    prepared = _confine_rank.prepare_keeper_sandbox(
        launch.trusted,
        workspace=opened.workspace,
        workspace_fd=opened.workspace_fd,
        job=opened.job,
        job_fd=opened.job_fd,
        shm_fd=opened.shm_fd,
        environment={},
    )
    try:
        bind_descriptors = {
            prepared.argv[index + 1]
            for index, option in enumerate(prepared.argv[:-1])
            if option in {"--bind-fd", "--ro-bind-fd", "--ro-bind-data"}
        }
        assert bind_descriptors == {str(descriptor) for descriptor in prepared.descriptors}
    finally:
        prepared.close()


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
    dev = _option_index(argv, "--dev", "/dev")
    null = argv.index("/dev/null")
    descriptor = argv[null - 1].removeprefix("/proc/self/fd/")
    assert argv[null - 2] == "--dev-bind" and null > dev
    assert argv[null + 1 : null + 4] == ["--ro-bind-fd", descriptor, "/proc/.httk-devices/0"]
    assert null < _option_index(argv, "--proc", "/proc")
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
    with pytest.raises(ValueError, match="contains or lies inside the workspace"):
        _prepare(launch, opened, {})


def test_readonly_ancestor_of_the_workspace_is_refused(launch: _Launch, opened: _Opened) -> None:
    alias = launch.root / "workspace-parent-alias"
    alias.symlink_to(launch.workspace.parent, target_is_directory=True)
    for path in (launch.workspace.parent, alias):
        launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, readonly_paths=(path,)))
        with pytest.raises(ValueError, match="contains or lies inside the workspace"):
            _prepare(launch, opened, {})


def test_shm_root_must_not_overlap_a_readonly_path(launch: _Launch, opened: _Opened) -> None:
    launch.trusted = replace(
        launch.trusted,
        confine=replace(launch.trusted.confine, readonly_paths=(launch.shm_root,)),
    )
    with pytest.raises(ValueError, match="confine.shm_root"):
        _prepare(launch, opened, {})


def test_shm_root_must_not_overlap_the_workspace(launch: _Launch, opened: _Opened) -> None:
    shm_root = launch.workspace / "inside-shm"
    shm_root.mkdir()
    launch.trusted = replace(launch.trusted, confine=replace(launch.trusted.confine, shm_root=shm_root))
    with pytest.raises(ValueError, match="confine.shm_root.*workspace"):
        _prepare(launch, opened, {})


def test_read_launch_refusals(launch: _Launch) -> None:
    with pytest.raises(ValueError, match="--launch-dir"):
        _confine_rank.read_launch(launch.launch_dir.parent)
    with pytest.raises(ValueError, match="--launch-dir"):
        _confine_rank.read_launch(Path("relative") / ".httk-workspace/owners/m/launches/x")
    with pytest.raises(ValueError, match="--launch-dir"):
        _confine_rank.read_launch(launch.workspace / ".httk-workspace/managers/m/launches" / launch.launch_dir.name)
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

    owners, owner = launch.workspace / ".httk-workspace" / "owners", launch.owner_id
    shutil.move(owners / owner, launch.root / owner)
    (owners / owner).symlink_to(launch.root / owner)
    with pytest.raises(OSError):
        _confine_rank.read_launch(launch.launch_dir)
    (owners / owner).unlink()
    shutil.move(launch.root / owner, owners / owner)

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
    workspace, owner = launch.workspace, launch.owner_id
    with pytest.raises(ValueError, match="job key"):
        _confine_rank.open_job(replace(launch.trusted, job_key="plain"), workspace, owner)
    nested = f"relax--{uuid.uuid4()}"
    with pytest.raises(ValueError, match="job directories never nest"):
        _confine_rank.open_job(replace(launch.trusted, placement=f"project/{nested}"), workspace, owner)
    # The job is found by its key among the owner's own jobs only.
    with pytest.raises(OSError):
        _confine_rank.open_job(launch.trusted, workspace, uuid.uuid4().hex)
    other = f"other--{uuid.uuid4()}"
    with pytest.raises(ValueError, match="holds 0 entries"):
        _confine_rank.open_job(replace(launch.trusted, job_key=other), workspace, owner)
    second = launch.job.with_name(_kernel.format_job_name(launch.trusted.job_key, 400, _fs.fresh_token(), "ready"))
    second.mkdir()
    with pytest.raises(ValueError, match="holds 2 entries"):
        _confine_rank.open_job(launch.trusted, workspace, owner)
    second.rmdir()
    linked = launch.job.with_name(_kernel.format_job_name(other, 500, _fs.fresh_token(), "ready"))
    linked.symlink_to(launch.job)
    with pytest.raises(ValueError, match="symlink"):
        _confine_rank.open_job(replace(launch.trusted, job_key=other), workspace, owner)
    job, descriptor = _confine_rank.open_job(launch.trusted, workspace, owner)
    os.close(descriptor)
    assert job == launch.job


def test_shared_memory_is_shared_and_kept_for_the_manager(launch: _Launch) -> None:
    token = launch.trusted.token
    first = _confine_rank.join_shared_memory(launch.shm_root, token)
    second = _confine_rank.join_shared_memory(launch.shm_root, token)
    assert os.path.samestat(os.fstat(first.fd), os.fstat(second.fd))
    for path in (launch.shm, launch.shm / "shm", launch.shm / "ns"):
        information = os.stat(path)
        assert stat.S_IMODE(information.st_mode) == 0o700 and information.st_uid == os.geteuid()
    assert os.path.samestat(os.fstat(first.shm_fd), os.stat(launch.shm / "shm"))
    _confine_rank.leave_shared_memory(first)
    _confine_rank.leave_shared_memory(second)
    # No rank removes it: the manager does at the reap, a later rank helper's sweep once the record is gone.
    assert launch.shm.is_dir()


def test_the_sweep_removes_only_unrecorded_shared_memory(launch: _Launch) -> None:
    stale = launch.shm_root / f"httk-{secrets.token_hex(16)}"
    stale.mkdir(mode=0o700)
    (stale / "segment").write_bytes(b"x")
    (stale / "ns" / "0").mkdir(parents=True)
    os.mkfifo(stale / "ns" / "0" / _confine_keeper.MEMBERS)
    launch.shm.mkdir(mode=0o700)
    unrelated = launch.shm_root / "not-httk"
    unrelated.mkdir()
    _confine_rank.sweep_shared_memory(launch.workspace, launch.shm_root)
    assert not stale.exists() and launch.shm.is_dir() and unrelated.is_dir()
    shutil.rmtree(launch.launch_dir)
    _confine_rank.sweep_shared_memory(launch.workspace, launch.shm_root)
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
    (launch.shm / "ns").symlink_to(target)
    with pytest.raises(ValueError, match="not a real directory"):
        _confine_rank.join_shared_memory(launch.shm_root, token)
    (launch.shm / "ns").unlink()
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
    require_namespaces()
    (launch.job / "sub").mkdir()
    script = 'pwd; echo "$FOO|$PMI_RANK|$OMPI_MCA_btl_vader_single_copy_mechanism|$HOME|$SLURM_CONF|$X"; exit 7'
    launch.request(("sh", "-c", script), cwd="sub", environment={"PATH": os.environ["PATH"], "FOO": "bar"})
    process = _run_helper(launch, {"PMI_RANK": "3", "SLURM_CONF": "/etc/slurm.conf", "X": "helper"})
    stdout, stderr = process.communicate()
    assert process.returncode == 7, stderr
    assert stdout.decode().splitlines() == [str(launch.job / "sub"), "bar|3|none|/tmp/home||"]
    assert launch.shm.is_dir()


def test_helper_refuses_pmix_outside_the_roots(launch: _Launch) -> None:
    launch.request(("true",))
    outside = launch.root / "pmix-outside"
    outside.mkdir()
    process = _run_helper(launch, {"PMIX_SERVER_TMPDIR": str(outside)})
    _stdout, stderr = process.communicate()
    assert process.returncode == 2
    assert b"confine.pmix_roots" in stderr
    assert launch.shm.is_dir()


def test_helper_reports_signal_exit_and_forwards_signals(launch: _Launch) -> None:
    require_namespaces()
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
    assert launch.shm.is_dir()


def _alive(pid: int) -> bool:
    try:
        state = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rpartition(")")[2].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return False
    return state != "Z"


def _wait_gone(*pids: int) -> bool:
    """Wait until every process in *pids* is gone, returning false at the test timeout."""

    deadline = time.monotonic() + _TIMEOUT
    while any(_alive(pid) for pid in pids):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)
    return True


def _pids_running(marker: str) -> list[int]:
    """Return the host pids whose command line contains *marker*; ranks see other pids in their namespace."""

    found = []
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                if marker.encode() in (entry / "cmdline").read_bytes() and _alive(int(entry.name)):
                    found.append(int(entry.name))
            except OSError:
                pass
    return found


def test_helper_signal_reaches_the_whole_rank_group(launch: _Launch) -> None:
    require_namespaces()
    marker = f"rank-{uuid.uuid4().hex}"
    pause = f"30.{secrets.randbelow(10**6):06d}"
    launch.request(("sh", "-c", f"sleep {pause} & touch started; wait", marker))
    process = _run_helper(launch, wait=False)
    try:
        _wait_for(launch.job / "started", process)
        (shell,) = _pids_running(marker)
        (background,) = _pids_running(f"sleep\0{pause}")
        assert os.getpgid(background) == os.getpgid(shell) != os.getpgid(process.pid)
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=_TIMEOUT) == 128 + signal.SIGTERM
        assert not _alive(shell) and not _alive(background)
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
    assert launch.shm.is_dir()


def test_helper_waits_until_the_rank_group_is_gone(launch: _Launch) -> None:
    require_namespaces()
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
        (launch.job / "release").touch()
        assert process.wait(timeout=_TIMEOUT) == 128 + signal.SIGTERM
        assert (launch.job / "survived").exists()
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate()
    assert launch.shm.is_dir()


def _wait_for(path: Path, *processes: subprocess.Popen[bytes]) -> None:
    deadline = time.monotonic() + _TIMEOUT
    while not path.exists():
        for process in processes:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                pytest.fail(f"helper exited with {process.returncode}: {stdout!r} {stderr!r}")
        assert time.monotonic() < deadline, f"{path} did not appear"
        time.sleep(0.02)


def test_concurrent_helpers_share_shm(launch: _Launch) -> None:
    require_namespaces()
    script = (
        'touch "started.$PMI_RANK"; while [ ! -e "release.$PMI_RANK" ]; do sleep 0.02; done; touch "done.$PMI_RANK"'
    )
    launch.request(("sh", "-c", script))
    first = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    second = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
    try:
        _wait_for(launch.job / "started.0", first, second)
        _wait_for(launch.job / "started.1", first, second)
        assert launch.shm.is_dir()
        (launch.job / "release.0").touch()
        _wait_for(launch.job / "done.0", first, second)
        time.sleep(0.2)
        assert launch.shm.is_dir(), "the first rank removed shared memory still in use"
        assert first.wait(timeout=_TIMEOUT) == 0
        assert second.poll() is None
        (launch.job / "release.1").touch()
        assert second.wait(timeout=_TIMEOUT) == 0
        assert launch.shm.is_dir(), "a rank removed shared memory its launch record still names"
    finally:
        for process in (first, second):
            if process.poll() is None:
                process.kill()
            process.communicate()


# -- the node's keeper namespaces ------------------------------------------------------------------------

# A rank that registers itself under its pid in REGISTRY as Intel and LLVM OpenMP do in /dev/shm (O_EXCL, so
# equal pids collide), then creates a System V shared-memory segment of KEY and a marker file in REGISTRY and
# waits for READERS attaching ranks, or attaches the segment and reports what it saw and whether the marker
# is visible.
_SYSV_RANK = """
import ctypes, glob, os, sys, time
rank, key, mode, registry, readers = os.environ["PMI_RANK"], int(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
libc = ctypes.CDLL(None, use_errno=True)
libc.shmat.restype = ctypes.c_void_p
libc.shmat.argtypes = (ctypes.c_int, ctypes.c_void_p, ctypes.c_int)
open("pid." + rank, "w").write(str(os.getpid()))
os.close(os.open(f"{registry}/__KMP_REGISTERED_LIB_{os.getpid()}_{os.getuid()}", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
if mode == "create":
    open(f"{registry}/launch-marker", "w").close()
    shmid = libc.shmget(key, 4096, 0o3600)  # IPC_CREAT | IPC_EXCL | 0600
    assert shmid >= 0, ctypes.get_errno()
    ctypes.memmove(libc.shmat(shmid, None, 0), b"from-rank-" + rank.encode(), 11)
    open("ready", "w").close()
    while len(glob.glob("read.*")) < readers:
        time.sleep(0.02)
    libc.shmctl(shmid, 0, None)  # IPC_RMID
else:
    while not os.path.exists("ready"):
        time.sleep(0.02)
    shmid = libc.shmget(key, 0, 0)
    seen = "absent" if shmid < 0 else ctypes.string_at(libc.shmat(shmid, None, 0), 11).decode()
    seen += "|shared" if os.path.exists(f"{registry}/launch-marker") else "|private"
    open("seen." + rank, "w").write(seen)
    open("read." + rank, "w").close()
"""


def _host_segment_exists(key: int) -> bool:
    libc = ctypes.CDLL(None, use_errno=True)
    return libc.shmget(key, 0, 0) >= 0


def _keeper_record(launch: _Launch, generation: int = 0) -> _confine_keeper.KeeperRecord:
    return _confine_keeper.KeeperRecord.decode(
        (launch.shm / "ns" / str(generation) / _confine_keeper.KEEPER_FILE).read_bytes()
    )


def _parent(pid: int) -> int:
    return int(Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rpartition(")")[2].split()[1])


def _sibling_launch(launch: _Launch) -> _Launch:
    """Return a second launch of the same job, with its own request and launch token."""

    request_id = new_request_id()
    trusted = replace(
        launch.trusted,
        request_id=request_id,
        request=request_relative_path(launch.trusted.attempt_id, request_id),
        token=secrets.token_hex(16),
    )
    launch_dir = launch.launch_dir.with_name(trusted_name(trusted.attempt_id, request_id))
    sibling = replace(launch, launch_dir=launch_dir, trusted=trusted)
    sibling.write()
    return sibling


def _sysv_request(
    launch: _Launch, key: int, mode: str, *, readers: int = 1, python: str = sys.executable, registry: str = ""
) -> None:
    if not registry:
        # One registry per launch, as each launch has its own /dev/shm: separate launches may reuse pids.
        (launch.job / f"registry-{launch.trusted.token}").mkdir(exist_ok=True)
        registry = str(launch.job / f"registry-{launch.trusted.token}")
    launch.request((python, "-I", "-c", _SYSV_RANK, str(key), mode, registry, str(readers)))


def _finish(*processes: subprocess.Popen[bytes]) -> list[tuple[int, bytes]]:
    results = []
    for process in processes:
        try:
            _stdout, stderr = process.communicate(timeout=_TIMEOUT)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
        results.append((process.returncode, stderr))
    return results


def test_ranks_of_a_launch_share_pids_and_system_v_shared_memory(launch: _Launch) -> None:
    require_namespaces()
    key = secrets.randbelow(2**30) + 1
    _sysv_request(launch, key, "create")
    creator = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    _wait_for(launch.job / "ready", creator)
    _sysv_request(launch, key, "attach")
    attacher = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
    record = _keeper_record(launch)
    results = _finish(creator, attacher)
    assert results == [(0, b""), (0, b"")], results
    assert (launch.job / "seen.1").read_text() == "from-rank-0|shared"
    # Distinct pids in one PID namespace, so pid-named registrations do not collide.
    assert (launch.job / "pid.0").read_text() != (launch.job / "pid.1").read_text()
    assert len(list((launch.job / f"registry-{launch.trusted.token}").glob("__KMP_REGISTERED_LIB_*"))) == 2
    # The segment lived in the keeper's IPC namespace, and the keeper ended with the launch's last rank.
    assert not _host_segment_exists(key)
    assert _wait_gone(record.pid)
    assert not (launch.shm / "ns" / "1").exists()


def test_separate_launches_do_not_share_system_v_objects(launch: _Launch) -> None:
    require_namespaces()
    key = secrets.randbelow(2**30) + 1
    _sysv_request(launch, key, "create")
    creator = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    _wait_for(launch.job / "ready", creator)
    other = _sibling_launch(launch)
    _sysv_request(other, key, "attach")
    attacher = _run_helper(other, {"PMI_RANK": "1"}, wait=False)
    assert _finish(creator, attacher) == [(0, b""), (0, b"")]
    assert (launch.job / "seen.1").read_text() == "absent|private"
    assert _keeper_record(launch).namespaces["ipc"] != _keeper_record(other).namespaces["ipc"]


def test_cancelled_ranks_end_their_keeper(launch: _Launch) -> None:
    require_namespaces()
    marker = f"rank-{uuid.uuid4().hex}"
    launch.request(("sh", "-c", 'touch "started.$PMI_RANK"; exec sleep 60', marker))
    helpers = [_run_helper(launch, {"PMI_RANK": str(rank)}, wait=False) for rank in range(2)]
    try:
        for rank in range(2):
            _wait_for(launch.job / f"started.{rank}", *helpers)
        record = _keeper_record(launch)
        for helper in helpers:
            helper.send_signal(signal.SIGTERM)
        assert [code for code, _stderr in _finish(*helpers)] == [128 + signal.SIGTERM] * 2
    finally:
        for helper in helpers:
            if helper.poll() is None:
                helper.kill()
    assert _wait_gone(record.pid) and not _pids_running(marker)
    assert (launch.shm / "ns" / "0" / _confine_keeper.CLOSING).is_dir()


def test_a_killed_leading_helper_leaves_the_other_ranks_running(launch: _Launch) -> None:
    require_namespaces()
    marker = f"rank-{uuid.uuid4().hex}"
    launch.request(("sh", "-c", 'touch "started.$PMI_RANK"; while [ ! -e release ]; do sleep 0.02; done', marker))
    first = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    helpers = [first]
    try:
        _wait_for(launch.job / "started.0", first)
        record = _keeper_record(launch)
        supervisor = _parent(_parent(record.pid))
        assert _parent(supervisor) == first.pid
        follower = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
        helpers.append(follower)
        _wait_for(launch.job / "started.1", *helpers)
        leader = first
        leader.kill()
        (launch.job / "release").touch()
        assert follower.wait(timeout=_TIMEOUT) == 0
    finally:
        _finish(*helpers)
    assert _wait_gone(supervisor)
    assert not _alive(record.pid) and not _pids_running(marker)


def test_a_failing_leader_rank_is_reported_at_once(launch: _Launch) -> None:
    require_namespaces()
    launch.request(
        (
            "sh",
            "-c",
            'touch "started.$PMI_RANK"; [ "$PMI_RANK" = 0 ] && exit 3; while [ ! -e release ]; do sleep 0.02; done',
        )
    )
    first = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    second: subprocess.Popen[bytes] | None = None
    try:
        _wait_for(launch.shm / "ns" / "0" / _confine_keeper.KEEPER_FILE, first)
        second = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
        _wait_for(launch.job / "started.1", second)
        assert first.wait(timeout=_TIMEOUT) == 3
        assert second.poll() is None
        (launch.job / "release").touch()
        assert second.wait(timeout=_TIMEOUT) == 0
    finally:
        _finish(first, *((second,) if second is not None else ()))
    assert _wait_gone(_keeper_record(launch).pid)


def test_a_killed_supervisor_ends_every_rank(launch: _Launch) -> None:
    require_namespaces()
    marker = f"rank-{uuid.uuid4().hex}"
    launch.request(("sh", "-c", 'touch "started.$PMI_RANK"; exec sleep 60', marker))
    helpers = [_run_helper(launch, {"PMI_RANK": str(rank)}, wait=False) for rank in range(2)]
    try:
        for rank in range(2):
            _wait_for(launch.job / f"started.{rank}", *helpers)
        record = _keeper_record(launch)
        os.kill(_parent(_parent(record.pid)), signal.SIGKILL)
        assert [code for code, _stderr in _finish(*helpers)] == [128 + signal.SIGKILL] * 2
    finally:
        for helper in helpers:
            if helper.poll() is None:
                helper.kill()
    assert not _pids_running(marker)


def test_the_keeper_has_no_control_pipe(launch: _Launch) -> None:
    require_namespaces()
    launch.request(("sh", "-c", 'touch started; exec sleep 60'))
    helper = _run_helper(launch, wait=False)
    try:
        _wait_for(launch.job / "started", helper)
        assert os.readlink(f"/proc/{_keeper_record(launch).pid}/fd/0") == "/dev/null"
    finally:
        if helper.poll() is None:
            helper.kill()
        helper.communicate()


def test_a_rank_inherits_only_its_standard_streams(launch: _Launch) -> None:
    require_namespaces()
    launch.request((sys.executable, "-c", "import os; print(sorted(int(n) for n in os.listdir('/proc/self/fd')) )"))
    helper = _run_helper(launch)
    stdout, stderr = helper.communicate()
    assert helper.returncode == 0, stderr
    assert stdout == b"[0, 1, 2, 3]\n"


def test_a_killed_keeper_kills_its_ranks(launch: _Launch) -> None:
    require_namespaces()
    marker = f"rank-{uuid.uuid4().hex}"
    launch.request(("sh", "-c", 'touch "started.$PMI_RANK"; exec sleep 60', marker))
    helpers = [_run_helper(launch, {"PMI_RANK": str(rank)}, wait=False) for rank in range(2)]
    try:
        for rank in range(2):
            _wait_for(launch.job / f"started.{rank}", *helpers)
        os.kill(_keeper_record(launch).pid, signal.SIGKILL)
        assert [code for code, _stderr in _finish(*helpers)] == [128 + signal.SIGKILL] * 2
    finally:
        for helper in helpers:
            if helper.poll() is None:
                helper.kill()
    assert not _pids_running(marker)


def test_a_rank_that_cannot_join_is_refused_and_the_others_finish(launch: _Launch) -> None:
    require_namespaces()
    launch.request(("sh", "-c", 'touch "started.$PMI_RANK"; while [ ! -e release ]; do sleep 0.02; done'))
    first = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    _wait_for(launch.job / "started.0", first)
    step = launch.pmix_root / "pmix.1.0"
    step.mkdir()
    second = _run_helper(launch, {"PMI_RANK": "1", "PMIX_SERVER_TMPDIR": str(step)}, wait=False)
    code, stderr = _finish(second)[0]
    assert code == 2 and b"PMIX_SERVER_TMPDIR differs" in stderr
    (launch.job / "release").touch()
    assert _finish(first) == [(0, b"")]
    assert _wait_gone(_keeper_record(launch).pid)


def test_a_rank_after_its_node_finished_starts_a_new_generation(launch: _Launch) -> None:
    require_namespaces()
    launch.request(("sh", "-c", 'echo "$PMI_RANK" >> ran'))
    assert _finish(_run_helper(launch, {"PMI_RANK": "0"}, wait=False)) == [(0, b"")]
    assert _finish(_run_helper(launch, {"PMI_RANK": "1"}, wait=False)) == [(0, b"")]
    assert (launch.job / "ran").read_text().split() == ["0", "1"]
    assert (launch.shm / "ns" / "0" / _confine_keeper.CLOSING).is_dir()
    assert _keeper_record(launch, 1).pid != _keeper_record(launch, 0).pid


def test_a_keeper_without_namespaces_of_its_own_is_refused(launch: _Launch) -> None:
    require_namespaces()
    launch.write(
        replace(
            launch.trusted,
            confine=replace(
                launch.trusted.confine,
                bwrap=write_wrapper(launch.root / "bin" / "bwrap-without-namespaces", namespaces=False),
            ),
        )
    )
    launch.request(("sh", "-c", "touch ran"))
    code, stderr = _finish(_run_helper(launch, {"PMI_RANK": "0"}, wait=False))[0]
    assert code == 2 and b"namespace keeper on this node failed" in stderr and b"confinement refused" in stderr
    # The failure is recorded, so the generation's other ranks are refused too.
    code, stderr = _finish(_run_helper(launch, {"PMI_RANK": "1"}, wait=False))[0]
    assert code == 2 and b"namespace keeper on this node failed" in stderr and b"confinement refused" in stderr
    assert not (launch.job / "ran").exists()


def test_a_dead_supervisor_fails_the_generation_with_its_log(launch: _Launch) -> None:
    python = launch.root / "bin" / "python-supervisor-dies"
    python.write_text(
        "#!/bin/sh\n"
        'for argument; do [ "$argument" = "httk.workflow._confine_keeper" ] && { echo supervisor-boom >&2; exit 3; }; done\n'
        f'exec "{sys.executable}" "$@"\n',
        encoding="utf-8",
    )
    python.chmod(0o755)
    launch.write(replace(launch.trusted, python=python))
    launch.request(("true",))
    code, stderr = _finish(_run_helper(launch, {"PMI_RANK": "0"}, wait=False))[0]
    assert code == 2 and b"exited with status 3" in stderr and b"supervisor-boom" in stderr
    code, stderr = _finish(_run_helper(launch, {"PMI_RANK": "1"}, wait=False))[0]
    assert code == 2 and b"namespace keeper on this node failed" in stderr and b"supervisor-boom" in stderr
    assert not (launch.shm / "ns" / "1").exists()


def _prepared_generation(ns: Path, name: str, *, reader: bool) -> int | None:
    """Create a generation directory the way a leader does, holding its read end when *reader*."""

    generation = ns / name
    generation.mkdir(mode=0o700, parents=True)
    os.mkfifo(generation / _confine_keeper.MEMBERS, 0o600)
    if not reader:
        return None
    return os.open(generation / _confine_keeper.MEMBERS, os.O_RDONLY | os.O_NONBLOCK)


def _never_start() -> _confine_rank.PreparedSandbox:
    raise AssertionError("this helper must not lead")


def test_a_generation_whose_leader_is_gone_or_closing_is_skipped(tmp_path: Path) -> None:
    ns = tmp_path / "ns"
    _prepared_generation(ns, "0", reader=False)  # the leader died before publishing its keeper
    reader = _prepared_generation(ns, "1", reader=True)
    (ns / "1" / _confine_keeper.CLOSING).mkdir()
    (ns / "1" / _confine_keeper.KEEPER_FILE).write_text("{}")
    failed = _prepared_generation(ns, "2", reader=True)
    (ns / "2" / _confine_keeper.FAILED_FILE).write_text("no namespaces here")
    ns_fd = os.open(ns, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(ValueError, match="keeper on this node failed: no namespaces here"):
            _confine_keeper.join(ns_fd, prepare=_never_start, pmix=None, python=Path(sys.executable))
    finally:
        for descriptor in (ns_fd, reader, failed):
            assert descriptor is not None
            os.close(descriptor)


def test_a_follower_times_out_without_a_keeper_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_confine_keeper, "KEEPER_WAIT_SECONDS", 0.2)
    ns = tmp_path / "ns"
    reader = _prepared_generation(ns, "0", reader=True)
    (ns / "0" / _confine_keeper.SUPERVISOR_LOG).write_text("known supervisor line\n")
    ns_fd = os.open(ns, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(ValueError, match="timed out waiting.*known supervisor line"):
            _confine_keeper.join(ns_fd, prepare=_never_start, pmix=None, python=Path(sys.executable))
    finally:
        for descriptor in (ns_fd, reader):
            assert descriptor is not None
            os.close(descriptor)


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
    launch: _Launch, mode: Literal["on", "off", "auto"], has_fd: bool, relayed: bool
) -> None:
    trusted = _with_mode(launch, mode)
    pmi_fd = 42 if has_fd else None
    environ = {"PMI_PORT": "node1:4242", "PMI_RANK": "3"}
    if has_fd:  # never the passed number, so a reassigned PMI_FD shows
        environ["PMI_FD"] = "99"
    assert _confine_rank.blocks_mpi_spawn(trusted, environ) is relayed
    environment = _confine_rank.prepare_rank_environment(trusted, environ, pmi_fd)
    assert environment.get("PMI_FD") == ((str(pmi_fd) if relayed else "99") if has_fd else None)
    assert environment.get("PMI_PORT") == (None if relayed else "node1:4242")
    assert environment["PMI_RANK"] == "3"


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
    started = time.monotonic()
    try:
        command = [sys.executable, "-c", "import time; time.sleep(60)"]
        assert _confine_rank._run(command, environment=dict(os.environ), pmi=pmi) == 2
    finally:
        for signum, handler in saved.items():
            signal.signal(signum, handler)
    assert time.monotonic() - started < 30  # the rank group was killed, not waited out
    assert "refused: cannot start the PMI relay: can't start new thread" in capsys.readouterr().err
    for descriptor in pmi:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_run_signal_handler_stops_forwarding_after_the_group_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = {signum: signal.getsignal(signum) for signum in _confine_rank._FORWARDED_SIGNALS}
    try:
        assert _confine_rank._run([sys.executable, "-c", "pass"], environment=dict(os.environ)) == 0
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler) and handler is not signal.SIG_DFL
        calls: list[tuple[int, int]] = []
        monkeypatch.setattr(_confine_rank.os, "killpg", lambda group, signum: calls.append((group, signum)))
        handler(signal.SIGTERM, None)
        assert calls == []
    finally:
        for signum, handler in saved.items():
            signal.signal(signum, handler)


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
    require_namespaces()
    _assert_pmi_session(*_pmi_session(launch, sys.executable))
    assert launch.shm.is_dir()


def _answer_spawn(slurm: socket.socket, line: bytes) -> None:
    if line == b"endcmd\n":
        slurm.sendall(b"cmd=spawn_result rc=0\n")
    elif (name := line[4:].split(b" ", 1)[0].strip()) in REPLIES:
        slurm.sendall(REPLIES[name])


def test_block_mpi_spawn_off_passes_slurms_pmi_fd(launch: _Launch) -> None:
    require_namespaces()
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
    require_namespaces()
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
    assert launch.shm.is_dir()


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
    daemon = launch.workspace / ".httk-workspace" / "daemon"
    daemon.mkdir()
    (daemon / "response.seed").write_text("private-seed", encoding="utf-8")
    visible = launch.workspace / "visible.txt"
    visible.write_text("workspace-file", encoding="utf-8")
    canary = launch.root / "host-canary.txt"
    canary.write_text("host-only", encoding="utf-8")
    sibling = launch.workspace / "jobs" / "ready" / "project" / f"other--{uuid.uuid4()}~p500~{_fs.fresh_token()}"
    sibling.mkdir(parents=True)
    script = (
        'for target in "$1/own" "$2/x" "$3/x" "/dev/shm/x"; do\n'
        '  if (echo data > "$target") 2>/dev/null; then echo "writable $target"; else echo "refused $target"; fi\n'
        "done\n"
        'if cat "$3/.httk-workspace/daemon/response.seed" >/dev/null 2>&1; then echo "seed readable"; '
        'else echo "seed absent"; fi\n'
        'if cat "$3/visible.txt" >/dev/null 2>&1; then echo "workspace readable"; '
        'else echo "workspace missing"; fi\n'
        'if test -e "$4"; then echo "canary visible"; else echo "canary absent"; fi\n'
    )
    launch.request(("/bin/sh", "-c", script, "sh", str(launch.job), str(sibling), str(launch.workspace), str(canary)))
    process = _run_helper(launch)
    stdout, stderr = process.communicate()
    assert process.returncode == 0, stderr
    assert stdout.decode().splitlines() == [
        f"writable {launch.job}/own",
        f"refused {sibling}/x",
        f"refused {launch.workspace}/x",
        "writable /dev/shm/x",
        "seed absent",
        "workspace missing",
        "canary absent",
    ]
    assert not (sibling / "x").exists() and not (launch.workspace / "x").exists()
    assert launch.shm.is_dir()


def test_real_bwrap_rank_relays_pmi_and_refuses_spawn(launch: _Launch) -> None:
    _use_real_bwrap(launch)
    python = shutil.which("python3", path="/usr/bin:/bin")
    if python is None:
        pytest.skip("no system python3 to run the PMI client in the sandbox")
    _assert_pmi_session(*_pmi_session(launch, python))
    assert launch.shm.is_dir()


def _system_python() -> str:
    python = shutil.which("python3", path="/usr/bin:/bin")
    if python is None:
        pytest.skip("no system python3 to run in the sandbox")
    return python


def test_real_bwrap_ranks_share_namespaces_only_within_their_launch(launch: _Launch) -> None:
    _use_real_bwrap(launch)
    python = _system_python()
    key = secrets.randbelow(2**30) + 1
    _sysv_request(launch, key, "create", readers=2, python=python, registry="/dev/shm")
    creator = _run_helper(launch, {"PMI_RANK": "0"}, wait=False)
    _wait_for(launch.job / "ready", creator)
    _sysv_request(launch, key, "attach", python=python, registry="/dev/shm")
    attacher = _run_helper(launch, {"PMI_RANK": "1"}, wait=False)
    other = _sibling_launch(launch)
    _sysv_request(other, key, "attach", python=python, registry="/dev/shm")
    outsider = _run_helper(other, {"PMI_RANK": "2"}, wait=False)
    record = _keeper_record(launch)
    assert _finish(creator, attacher, outsider) == [(0, b""), (0, b""), (0, b"")]
    assert (launch.job / "seen.1").read_text() == "from-rank-0|shared"
    assert (launch.job / "seen.2").read_text() == "absent|private"
    assert (launch.job / "pid.0").read_text() != (launch.job / "pid.1").read_text()
    registrations = sorted(path.name for path in (launch.shm / "shm").glob("__KMP_REGISTERED_LIB_*"))
    assert len(registrations) == 2, registrations
    assert [path.name for path in (other.shm / "shm").glob("__KMP_REGISTERED_LIB_*")] != []
    assert not _host_segment_exists(key)
    assert _wait_gone(record.pid)


# Load an LLVM or Intel OpenMP runtime, whose initialization registers it in /dev/shm under the process id,
# and stay initialized until the other rank is. With a PID namespace per rank both ranks were pid 2, and the
# second runtime found the first one's registration and aborted (OMP: Error #13 or #15).
_OPENMP_RANK = """
import ctypes, os, sys, time
rank = os.environ["PMI_RANK"]
threads = ctypes.CDLL(sys.argv[1]).omp_get_max_threads()
def registrations():
    return len([name for name in os.listdir("/dev/shm") if name.startswith("__KMP_REGISTERED_LIB_")])
deadline = time.monotonic() + 30
while registrations() < 2 and time.monotonic() < deadline:
    time.sleep(0.02)
seen = registrations()
open("omp." + rank, "w").close()  # a runtime unregisters when its process exits: keep both alive until here
while not os.path.exists("omp." + ("1" if rank == "0" else "0")) and time.monotonic() < deadline:
    time.sleep(0.02)
print(threads > 0, seen)
"""


def test_real_bwrap_openmp_ranks_initialize_concurrently(launch: _Launch) -> None:
    _use_real_bwrap(launch)
    python = _system_python()
    runtime = ctypes.util.find_library("omp") or ctypes.util.find_library("iomp5")
    if runtime is None:
        pytest.skip("no LLVM or Intel OpenMP runtime is installed")
    launch.request((python, "-I", "-c", _OPENMP_RANK, runtime))
    helpers = [_run_helper(launch, {"PMI_RANK": str(rank)}, wait=False) for rank in range(2)]
    outputs = [helper.communicate(timeout=_TIMEOUT) for helper in helpers]
    assert [helper.returncode for helper in helpers] == [0, 0], outputs
    assert [stdout.decode().split() for stdout, _stderr in outputs] == [["True", "2"], ["True", "2"]], outputs


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
