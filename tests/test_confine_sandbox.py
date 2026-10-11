"""Build, probe and (where the host allows it) run the Bubblewrap attempt sandbox."""

import logging
import os
import shutil
import signal
import stat
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from httk.workflow import _confine_rank
from httk.workflow._confine import (
    ConfinementUnavailableError,
    ConfineSettings,
    confine_settings,
    filtered_attempt_environment,
    prepare_attempt_sandbox,
    probe_bwrap,
)
from httk.workflow._sandbox import (
    PreparedSandbox,
    device_parent_dirs,
    open_directory_nofollow,
    pidfd_open,
    pidfd_send_signal,
    workspace_view,
)


@pytest.fixture(autouse=True)
def _shm_roots_are_tmpfs(monkeypatch: pytest.MonkeyPatch) -> None:
    # The tests use tmp_path directories as confine.shm_root; the host tmpfs check is covered in test_confine_rank.
    monkeypatch.setattr(_confine_rank, "_filesystem_type", lambda _descriptor: "tmpfs")


_REQUIRED_HELP = (
    "--bind-fd --clearenv --new-session --ro-bind-data --ro-bind-fd --unshare-ipc --unshare-net "
    "--unshare-pid --unshare-user --unshare-uts --cap-drop --proc --dev --tmpfs --dir --remount-ro --chdir"
)


class _Layout:
    def __init__(self, root: Path) -> None:
        self.workspace = root / "ws"
        self.control = self.workspace / ".httk-workspace"
        self.job = self.workspace / "jobs" / "a"
        self.sibling = self.workspace / "jobs" / "b"
        self.workdir = self.job / "payload"
        self.readonly = root / "ro"
        self.workflows = self.workspace / "workflows"
        for path in (self.control, self.workdir, self.sibling, self.workflows, self.readonly):
            path.mkdir(parents=True)
        (self.control / "format.json").write_text("{}\n", encoding="utf-8")
        self.workspace_fd = open_directory_nofollow(self.workspace)
        self.job_fd = open_directory_nofollow(self.job)

    def close(self) -> None:
        os.close(self.workspace_fd)
        os.close(self.job_fd)


@pytest.fixture
def layout(tmp_path: Path) -> Iterator[_Layout]:
    created = _Layout(tmp_path)
    try:
        yield created
    finally:
        created.close()


def _settings(**changes: object) -> ConfineSettings:
    values: dict[str, object] = {
        "mode": "bwrap",
        "readonly_paths": (),
        "isolate_network": True,
        "bwrap": Path("/opt/bwrap/bin/bwrap"),
        "devices": (),
        "pmix_roots": (),
        "shm_root": Path("/dev/shm"),
        "environment": (),
    }
    values.update(changes)
    return ConfineSettings(**values)  # type: ignore[arg-type]


def _prepare(
    layout: _Layout, settings: ConfineSettings, *, block_userns: bool = False, **changes: object
) -> PreparedSandbox:
    arguments: dict[str, object] = {
        "workspace_root": layout.workspace,
        "workspace_fd": layout.workspace_fd,
        "job_path": layout.job,
        "job_fd": layout.job_fd,
        "workdir": layout.workdir,
        "environment": filtered_attempt_environment({"PATH": "/usr/bin:/bin", "SLURM_JOB_ID": "7", "HOME": "/home/u"}),
        "block_userns": block_userns,
    }
    arguments.update(changes)
    return prepare_attempt_sandbox(settings, **arguments)  # type: ignore[arg-type]


def _open_descriptors() -> set[int]:
    return {int(name) for name in os.listdir("/proc/self/fd")}


@pytest.mark.parametrize("fallback", (False, True))
def test_pidfd_helpers_work_with_and_without_stdlib_wrappers(monkeypatch: pytest.MonkeyPatch, fallback: bool) -> None:
    if fallback:
        monkeypatch.delattr(os, "pidfd_open", raising=False)
        monkeypatch.delattr(signal, "pidfd_send_signal", raising=False)
    descriptor = pidfd_open(os.getpid())
    try:
        pidfd_send_signal(descriptor, 0)
    finally:
        os.close(descriptor)
    child = subprocess.Popen(("true",))
    child.wait()
    with pytest.raises(ProcessLookupError):
        pidfd_open(child.pid)


def _option_index(argv: list[str], *items: str) -> int:
    for index in range(len(argv) - len(items) + 1):
        if tuple(argv[index : index + len(items)]) == items:
            return index
    raise AssertionError(f"{items} not in {argv}")


def test_argv_order_binds_and_descriptors(layout: _Layout) -> None:
    settings = _settings(readonly_paths=(layout.readonly,), devices=(Path("/dev/null"),))
    prepared = _prepare(layout, settings)
    try:
        argv = prepared.argv
        assert argv[:12] == [
            "/opt/bwrap/bin/bwrap",
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-net",
            "--cap-drop",
            "ALL",
            "--tmpfs",
            "/tmp",
            "--dir",
            "/tmp/home",
        ]
        assert "--new-session" not in argv and "--die-with-parent" not in argv
        assert "--disable-userns" not in argv and "--assert-userns-disabled" not in argv
        assert argv[-3:] == ["--chdir", str(layout.workdir), "--"]

        readonly_index = _option_index(
            argv, "--ro-bind-fd", argv[_option_index(argv, "--ro-bind-fd") + 1], str(layout.readonly)
        )
        readonly = int(argv[readonly_index + 1])
        jobs_index = next(
            index
            for index in range(len(argv))
            if argv[index : index + 1] == ["--ro-bind-fd"] and argv[index + 2] == str(layout.workspace / "jobs")
        )
        jobs_descriptor = int(argv[jobs_index + 1])
        workflows_index = next(
            index
            for index in range(len(argv))
            if argv[index : index + 1] == ["--ro-bind-fd"] and argv[index + 2] == str(layout.workflows)
        )
        format_index = next(
            index
            for index in range(len(argv))
            if argv[index : index + 1] == ["--ro-bind-data"] and argv[index + 2] == str(layout.control / "format.json")
        )
        job = int(argv[argv.index(str(layout.job)) - 1])
        view = [
            jobs_index,
            workflows_index,
            format_index,
        ]
        workspace_descriptors = (jobs_descriptor, int(argv[workflows_index + 1]), int(argv[format_index + 1]))
        order = [
            _option_index(argv, "--tmpfs", "/tmp", "--dir", "/tmp/home"),
            _option_index(argv, "--ro-bind-fd", str(readonly), str(layout.readonly)),
            _option_index(argv, "--tmpfs", str(layout.workspace)),
            *view,
            _option_index(argv, "--bind-fd", str(job), str(layout.job)),
            _option_index(argv, "--proc", "/proc", "--dev", "/dev"),
        ]
        device = int(argv[argv.index("/dev/null") - 1])
        order += [
            _option_index(argv, "--bind-fd", str(device), "/dev/null"),
            _option_index(argv, "--remount-ro", "/"),
            len(argv) - 3,
        ]
        assert order == sorted(order)
        assert not any(
            option in {"--ro-bind-fd", "--bind-fd", "--ro-bind-data"} and argv[index + 2] == str(layout.workspace)
            for index, option in enumerate(argv[:-2])
        )
        assert argv.count("--tmpfs") == 2
        assert [argv[index + 1] for index, option in enumerate(argv) if option == "--tmpfs"] == [
            "/tmp",
            str(layout.workspace),
        ]
        assert argv[view[0] - 2 : view[0]] == ["--tmpfs", str(layout.workspace)]
        remount = _option_index(argv, "--remount-ro", str(layout.workspace), "--remount-ro", "/")
        assert argv[remount : remount + 4] == ["--remount-ro", str(layout.workspace), "--remount-ro", "/"]
        assert argv[remount + 4 : argv.index("--chdir")] == []
        assert prepared.descriptors[:3] == (readonly, jobs_descriptor, int(argv[workflows_index + 1]))
        assert prepared.descriptors[3:] == (int(argv[format_index + 1]), job, device)
        descriptor_operands = {
            int(argv[index + 1])
            for index, option in enumerate(argv)
            if option in {"--bind-fd", "--ro-bind-fd", "--ro-bind-data"}
        }
        assert set(prepared.descriptors) == descriptor_operands
        descriptors = prepared.descriptors
        workspace = workspace_descriptors[0]
        for descriptor in prepared.descriptors:
            assert os.get_inheritable(descriptor)
        assert os.fstat(workspace).st_ino == (layout.workspace / "jobs").stat().st_ino
        assert os.fstat(job).st_ino == layout.job.stat().st_ino
        assert stat.S_ISCHR(os.fstat(device).st_mode)
    finally:
        prepared.close()
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    # The caller's descriptors are duplicated, never consumed.
    os.fstat(layout.workspace_fd)
    os.fstat(layout.job_fd)


def test_the_device_binds_come_last(layout: _Layout) -> None:
    prepared = _prepare(layout, _settings(devices=(Path("/dev/null"),)))
    try:
        assert len(prepared.descriptors) == 5  # workspace view, job, device
        assert prepared.argv[_option_index(prepared.argv, "--remount-ro", str(layout.workspace)) - 1] == str(
            Path("/dev/null")
        )
    finally:
        prepared.close()


def test_no_environment_value_enters_the_world_readable_argv(layout: _Layout) -> None:
    secrets = {
        "OPERATOR_TOKEN": "operator-token-0123456789",
        "HTTK_WORKFLOW_CONTEXT": '{"settings": {"site.api_key": "context-secret-9876"}}',
        "PATH": "/opt/secret-path/bin",
    }
    prepared = _prepare(
        layout, _settings(readonly_paths=(layout.readonly,)), environment=filtered_attempt_environment(secrets)
    )
    try:
        assert "--setenv" not in prepared.argv and "--clearenv" not in prepared.argv
        for value in secrets.values():
            assert not any(value in item for item in prepared.argv)
        assert not any("SECRET" in item.upper() or "TOKEN" in item.upper() for item in prepared.argv)
    finally:
        prepared.close()


def test_network_and_userns_options_are_toggled(layout: _Layout) -> None:
    prepared = _prepare(layout, _settings(isolate_network=False), block_userns=True)
    try:
        assert prepared.argv[1:9] == [
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--disable-userns",
            "--assert-userns-disabled",
            "--cap-drop",
            "ALL",
        ]
        assert "--unshare-net" not in prepared.argv
    finally:
        prepared.close()


def test_device_parents_are_created_once_on_the_private_dev() -> None:
    created: set[Path] = set()
    assert device_parent_dirs(Path("/dev/infiniband/uverbs0"), created) == ["--dir", "/dev/infiniband"]
    assert device_parent_dirs(Path("/dev/infiniband/uverbs1"), created) == []
    assert device_parent_dirs(Path("/dev/a/b/c"), created) == ["--dir", "/dev/a", "--dir", "/dev/a/b"]
    assert device_parent_dirs(Path("/dev/nvidia0"), created) == []


def test_device_in_a_subdirectory_gets_its_parent_dir(layout: _Layout) -> None:
    device = next(
        (
            Path(path)
            for path in ("/dev/pts/ptmx", "/dev/net/tun", "/dev/dri/card0")
            if os.path.exists(path) and stat.S_ISCHR(os.stat(path).st_mode)
        ),
        None,
    )
    if device is None:
        pytest.skip("no character device below a /dev subdirectory on this host")
    prepared = _prepare(layout, _settings(devices=(device,)))
    try:
        index = _option_index(prepared.argv, "--dir", str(device.parent))
        assert index > _option_index(prepared.argv, "--proc", "/proc", "--dev", "/dev")
        assert prepared.argv[index + 2] == "--bind-fd" and prepared.argv[index + 4] == str(device)
    finally:
        prepared.close()


def test_merged_usr_links_follow_the_readonly_binds(layout: _Layout) -> None:
    prepared = _prepare(layout, _settings(readonly_paths=(Path("/usr"),)))
    try:
        for link in ("/bin", "/lib", "/lib64", "/sbin"):
            if Path(link).is_symlink() and Path(link).resolve().is_relative_to("/usr"):
                index = _option_index(prepared.argv, "--symlink", os.readlink(link), link)
                jobs_bind = next(
                    option
                    for option in range(len(prepared.argv))
                    if prepared.argv[option : option + 1] == ["--ro-bind-fd"]
                    and prepared.argv[option + 2] == str(layout.workspace / "jobs")
                )
                assert index < jobs_bind
    finally:
        prepared.close()


@pytest.mark.parametrize("inside", ["", ".httk-workspace", "jobs/a", "jobs/b"])
def test_readonly_path_inside_the_workspace_is_refused(layout: _Layout, inside: str) -> None:
    before = _open_descriptors()
    path = layout.workspace / inside if inside else layout.workspace
    with pytest.raises(ValueError, match="contains or lies inside the workspace"):
        _prepare(layout, _settings(readonly_paths=(layout.readonly, path)))
    assert _open_descriptors() == before


def test_readonly_ancestor_and_symlinked_ancestor_are_refused(layout: _Layout) -> None:
    alias = layout.readonly.parent / "workspace-parent-alias"
    alias.symlink_to(layout.workspace.parent, target_is_directory=True)
    for path in (layout.workspace.parent, alias):
        with pytest.raises(ValueError, match="contains or lies inside the workspace"):
            _prepare(layout, _settings(readonly_paths=(path,)))


def test_readonly_symlink_to_shm_root_is_refused(layout: _Layout) -> None:
    shm_root = layout.readonly / "shm-root"
    shm_root.mkdir()
    alias = layout.readonly.parent / "shm-alias"
    alias.symlink_to(shm_root, target_is_directory=True)
    with pytest.raises(ValueError, match=r"confine\.shm_root .*confine\.readonly_paths entry"):
        _prepare(layout, _settings(shm_root=shm_root, readonly_paths=(alias,)))


def test_disjoint_shm_root_and_readonly_path_prepare(layout: _Layout) -> None:
    shm_root = layout.readonly.parent / "shm-root"
    shm_root.mkdir()
    prepared = _prepare(layout, _settings(shm_root=shm_root, readonly_paths=(layout.readonly,)))
    prepared.close()


def test_descriptors_are_closed_on_every_error(layout: _Layout) -> None:
    before = _open_descriptors()
    with pytest.raises(ValueError, match="unavailable"):
        _prepare(layout, _settings(readonly_paths=(layout.readonly, layout.readonly / "missing")))
    assert _open_descriptors() == before
    plain = layout.readonly / "plain"
    plain.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="not a device node"):
        _prepare(layout, _settings(readonly_paths=(layout.readonly,), devices=(Path("/dev/null"), plain)))
    assert _open_descriptors() == before


@pytest.mark.parametrize("missing", ["workflows", "format"])
def test_workspace_view_missing_path_closes_descriptors(layout: _Layout, missing: str) -> None:
    path = layout.workflows if missing == "workflows" else layout.control / "format.json"
    if path.is_dir():
        path.rmdir()
    else:
        path.unlink()
    before = _open_descriptors()
    with pytest.raises(ValueError, match="confinement refused"):
        _prepare(layout, _settings())
    assert _open_descriptors() == before


def test_workspace_view_symlinked_jobs_closes_descriptors(layout: _Layout, tmp_path: Path) -> None:
    jobs = layout.workspace / "jobs"
    jobs.rename(tmp_path / "jobs-real")
    jobs.symlink_to(tmp_path / "jobs-real", target_is_directory=True)
    before = _open_descriptors()
    with pytest.raises(ValueError, match="confinement refused"):
        _prepare(layout, _settings())
    assert _open_descriptors() == before


@pytest.mark.parametrize("workspace_root", [Path("/"), Path("/tmp"), Path("/tmp/home")])
def test_workspace_view_refuses_reserved_roots(workspace_root: Path) -> None:
    workspace_fd = open_directory_nofollow(Path("/"))
    try:
        with pytest.raises(ValueError, match="a confined workspace may not be rooted at"):
            workspace_view(workspace_fd, workspace_root)
    finally:
        os.close(workspace_fd)


def test_misplaced_paths_and_environment_are_refused(layout: _Layout) -> None:
    before = _open_descriptors()
    with pytest.raises(ValueError, match="not inside the workspace"):
        _prepare(layout, _settings(), job_path=layout.readonly)
    with pytest.raises(ValueError, match="not inside the workspace"):
        _prepare(layout, _settings(), job_path=layout.workspace)
    with pytest.raises(ValueError, match="not inside the job directory"):
        _prepare(layout, _settings(), workdir=layout.sibling)
    with pytest.raises(ValueError, match="absolute"):
        _prepare(layout, _settings(), workdir=Path("relative"))
    with pytest.raises(ValueError, match="filtered with filtered_attempt_environment"):
        _prepare(layout, _settings(), environment={"PATH": "/usr/bin", "SLURM_JOB_ID": "7"})
    with pytest.raises(ValueError, match="not a directory"):
        plain = os.open(os.devnull, os.O_RDONLY | os.O_CLOEXEC)
        try:
            _prepare(layout, _settings(), job_fd=plain)
        finally:
            os.close(plain)
    with pytest.raises(ConfinementUnavailableError, match="confine.bwrap"):
        _prepare(layout, _settings(bwrap=None))
    assert _open_descriptors() == before


def _fake_bwrap(directory: Path, *, userns_listed: bool = True, failure: str | None = None) -> Path:
    directory.mkdir(exist_ok=True)
    log = directory / "calls.log"
    options = _REQUIRED_HELP + (" --disable-userns --assert-userns-disabled" if userns_listed else "")
    fail_userns = failure == "userns"
    fail_all = failure == "all"
    script = directory / "bwrap"
    script.write_text(
        "#!/bin/sh\n"
        f'if [ "$1" = --help ]; then echo "usage: bwrap {options}"; exit 0; fi\n'
        f'echo "$*" >> {log}\n'
        'case " $* " in *" --disable-userns "*) '
        + ("echo 'bwrap: setting up uid map: Permission denied' >&2; exit 1;; esac\n" if fail_userns else ";; esac\n")
        + (
            "echo 'bwrap: Can'\\''t mount proc on /newroot/proc: Operation not permitted' >&2; exit 1\n"
            if fail_all
            else ""
        )
        + "exit 0\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def _calls(script: Path) -> list[list[str]]:
    log = script.parent / "calls.log"
    return [line.split(" ") for line in log.read_text(encoding="utf-8").splitlines()] if log.exists() else []


@pytest.fixture
def fake_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "fakebin"
    directory.mkdir()
    monkeypatch.setenv("PATH", f"{directory}:{os.environ.get('PATH', '/usr/bin:/bin')}")
    return directory


def test_probe_blocks_userns_when_the_functional_probe_passes(fake_path: Path) -> None:
    script = _fake_bwrap(fake_path)
    settings = confine_settings({"manager.confine": "bwrap"})
    assert settings.bwrap == script.resolve()
    assert probe_bwrap(settings) is True
    [call] = _calls(script)
    assert call[:6] == [
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--unshare-net",
        "--disable-userns",
    ]
    assert "--assert-userns-disabled" in call
    assert "--clearenv" not in call and "--setenv" not in call
    tail = call[call.index("ALL") + 1 :]
    assert tail[:9] == ["--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--", tail[8]]
    assert Path(tail[8]).name == "true"


def test_probe_drops_the_userns_block_with_a_warning(fake_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    script = _fake_bwrap(fake_path, failure="userns")
    settings = confine_settings({"manager.confine": "bwrap", "confine.isolate_network": "false"})
    with caplog.at_level(logging.WARNING, logger="httk.workflow._confine"):
        assert probe_bwrap(settings) is False
    first, second = _calls(script)
    assert "--disable-userns" in first and "--disable-userns" not in second
    assert "--unshare-net" not in first and "--unshare-net" not in second
    assert "refuses --disable-userns" in caplog.text and "Permission denied" in caplog.text


def test_probe_without_listed_userns_options_warns(fake_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    script = _fake_bwrap(fake_path, userns_listed=False)
    with caplog.at_level(logging.WARNING, logger="httk.workflow._confine"):
        assert probe_bwrap(confine_settings({"manager.confine": "bwrap"})) is False
    [call] = _calls(script)
    assert "--disable-userns" not in call
    assert "lacks --disable-userns" in caplog.text


def test_probe_failure_refuses_with_the_requirement_and_stderr(fake_path: Path) -> None:
    _fake_bwrap(fake_path, failure="all")
    with pytest.raises(ConfinementUnavailableError, match="user namespaces and a PID namespace") as raised:
        probe_bwrap(confine_settings({"manager.confine": "bwrap"}))
    assert "Operation not permitted" in str(raised.value)


def test_probe_refuses_missing_or_featureless_bwrap(tmp_path: Path) -> None:
    with pytest.raises(ConfinementUnavailableError, match="confine.bwrap"):
        probe_bwrap(_settings(bwrap=None))
    with pytest.raises(ConfinementUnavailableError, match="confine.bwrap"):
        probe_bwrap(_settings(bwrap=tmp_path / "missing"))
    featureless = tmp_path / "bwrap"
    featureless.write_text("#!/bin/sh\necho usage: bwrap --unshare-user\n", encoding="utf-8")
    featureless.chmod(0o755)
    with pytest.raises(ConfinementUnavailableError, match="lacks required confinement features"):
        probe_bwrap(_settings(bwrap=featureless))


def _unsupported_namespace_failure(detail: str) -> None:
    lowered = detail.lower()
    expected = any(
        phrase in lowered
        for phrase in ("operation not permitted", "permission denied", "no permissions to create", "user namespace")
    )
    if not expected:
        pytest.fail(f"Bubblewrap sandbox failed unexpectedly: {detail}")
    if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
        pytest.fail(f"required Bubblewrap sandbox is unavailable: {detail}")
    pytest.skip(f"Bubblewrap user namespaces are unavailable: {detail}")


def test_real_attempt_sandbox_confines_writes_and_follows_killpg(layout: _Layout) -> None:
    if shutil.which("bwrap") is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    settings = confine_settings({"manager.confine": "bwrap"})
    try:
        block_userns = probe_bwrap(settings)
    except ConfinementUnavailableError as exc:
        _unsupported_namespace_failure(str(exc))
        raise
    environment = filtered_attempt_environment({"PATH": "/usr/bin:/bin"})

    prepared = _prepare(layout, settings, block_userns=block_userns, workdir=layout.job, environment=environment)
    script = (
        'for target in "$1/own" "$2/x" "$3/.httk-workspace/x" "$3/x"; do\n'
        '  if (echo data > "$target") 2>/dev/null; then echo "writable $target"; else echo "refused $target"; fi\n'
        "done\n"
        'echo "confined=$HTTK_WORKFLOW_CONFINED home=$HOME cwd=$(pwd)"\n'
    )
    try:
        result = subprocess.run(
            [
                *prepared.argv,
                "/bin/sh",
                "-c",
                script,
                "sh",
                str(layout.job),
                str(layout.sibling),
                str(layout.workspace),
            ],
            pass_fds=prepared.descriptors,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
            check=False,
        )
    finally:
        prepared.close()
    if result.returncode != 0:
        _unsupported_namespace_failure(result.stderr or result.stdout)
    assert result.stdout.splitlines() == [
        f"writable {layout.job}/own",
        f"refused {layout.sibling}/x",
        f"refused {layout.control}/x",
        f"refused {layout.workspace}/x",
        f"confined=1 home=/tmp/home cwd={layout.job}",
    ]
    assert (layout.job / "own").read_text(encoding="utf-8") == "data\n"
    assert not any(path.exists() for path in (layout.sibling / "x", layout.control / "x", layout.workspace / "x"))

    prepared = _prepare(layout, settings, block_userns=block_userns, workdir=layout.job, environment=environment)
    try:
        process = subprocess.Popen(
            [*prepared.argv, "/bin/sh", "-c", 'echo started > "$1/started"; exec sleep 60', "sh", str(layout.job)],
            pass_fds=prepared.descriptors,
            stdin=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=environment,
            start_new_session=True,
        )
    finally:
        prepared.close()
    try:
        deadline = time.monotonic() + 20
        while not (layout.job / "started").exists():
            if process.poll() is not None or time.monotonic() > deadline:
                assert process.stderr is not None
                pytest.fail(f"sandboxed command did not start: {process.stderr.read().decode(errors='replace')}")
            time.sleep(0.05)
        os.killpg(process.pid, signal.SIGTERM)
        assert process.wait(timeout=10) != 0
        deadline = time.monotonic() + 5
        while True:
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                break
            assert time.monotonic() < deadline, "a sandboxed process survived killpg"
            time.sleep(0.05)
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        if process.stderr is not None:
            process.stderr.close()


def test_real_attempt_exposes_only_the_selected_workspace_view(layout: _Layout) -> None:
    if shutil.which("bwrap") is None:
        if os.environ.get("HTTK_REQUIRE_DAEMON_SANDBOX") == "1":
            pytest.fail("required Bubblewrap executable is unavailable")
        pytest.skip("Bubblewrap executable is unavailable")
    daemon = layout.control / "daemon"
    daemon.mkdir()
    (daemon / "response.seed").write_text("private-seed", encoding="utf-8")
    (layout.workspace / "exchange").mkdir()
    (layout.workspace / "exchange" / "x").write_text("exchange", encoding="utf-8")
    (layout.sibling / "visible.txt").write_text("sibling", encoding="utf-8")
    runner = layout.workflows / "w" / "package"
    runner.mkdir(parents=True)
    (runner / "runner.sh").write_text("runner", encoding="utf-8")
    settings = confine_settings({"manager.confine": "bwrap"})
    try:
        block_userns = probe_bwrap(settings)
    except ConfinementUnavailableError as exc:
        _unsupported_namespace_failure(str(exc))
        raise
    environment = filtered_attempt_environment({"PATH": "/usr/bin:/bin"})
    prepared = _prepare(
        layout,
        settings,
        block_userns=block_userns,
        workdir=layout.job,
        environment=environment,
    )
    format_content = (layout.control / "format.json").read_text(encoding="utf-8").strip()
    script = (
        'printf "format=%s\\n" "$(cat "$1/.httk-workspace/format.json")"\n'
        'test -r "$1/jobs/b/visible.txt" && echo sibling=readable\n'
        'test -r "$1/workflows/w/package/runner.sh" && echo workflow=readable\n'
        'test ! -e "$1/.httk-workspace/daemon/response.seed" && echo daemon=absent\n'
        'test ! -e "$1/exchange/x" && echo exchange=absent\n'
        'if mkdir "$1/.httk-workspace/x" 2>/dev/null; then echo control=write; else echo control=readonly; fi\n'
        'if touch "$1/planted" 2>/dev/null; then echo workspace=write; else echo workspace=readonly; fi\n'
        'if touch "$1/jobs/planted" 2>/dev/null; then echo jobs=write; else echo jobs=readonly; fi\n'
        'touch "$2/own.txt" && echo job=writable\n'
        'touch /tmp/home/t && echo tmp=writable\n'
    )
    try:
        result = subprocess.run(
            [*prepared.argv, "/bin/sh", "-c", script, "sh", str(layout.workspace), str(layout.job)],
            pass_fds=prepared.descriptors,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
            check=False,
        )
    finally:
        prepared.close()
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        f"format={format_content}",
        "sibling=readable",
        "workflow=readable",
        "daemon=absent",
        "exchange=absent",
        "control=readonly",
        "workspace=readonly",
        "jobs=readonly",
        "job=writable",
        "tmp=writable",
    ]
