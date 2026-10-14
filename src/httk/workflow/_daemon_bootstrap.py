"""Isolated Bubblewrap bootstrap of the workspace daemon broker."""

import argparse
import fcntl
import json
import os
import re
import runpy
import socket
import stat
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

# The broker runs only trusted code, so it sees the host read-only; its writable mounts and policy data
# sit on the private /tmp, since a read-only / takes no new directories.
_HOST_EXCHANGE_DESTINATION = "/tmp/daemon-exchange"
_HOST_STATE_DESTINATION = "/tmp/control"
_HOST_DAEMON_DESTINATION = "/tmp/daemon"
_HOST_POLICY_DESTINATION = "/tmp/daemon-policy.json"
_FIXED_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/tmp/home",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
}
# The trusted operator environment reaches the broker; scheduler input variables would change submissions,
# and interpreter or loader variables would change the broker's Python or the host Slurm clients.
_OPERATOR_DROPPED_PREFIXES = ("SBATCH_", "SALLOC_", "SRUN_", "SLURM_", "PYTHON")
_OPERATOR_DROPPED = frozenset({"BASH_ENV", "ENV", "LD_PRELOAD", "LD_LIBRARY_PATH"})
_OPERATOR_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_OPERATOR_LIMIT = 512
_OPERATOR_BYTES = 256 * 1024
# The broker's home and cache lie on its private /tmp: the host view, including the passwd home, is read-only.
_BROKER_ENVIRONMENT = {"HOME": "/tmp/home", "XDG_CACHE_HOME": "/tmp/home/.cache", "TMPDIR": "/tmp"}
_BROKER_UNSET = ("XDG_RUNTIME_DIR", "XDG_CONFIG_HOME", "HTTK_CONFIG_HOME")
# Recovery waits out the workspace visibility deadline a few times per dead owner.
_RECOVERY_TIMEOUT = 300
_RESERVED_SANDBOX_PATHS = tuple(
    Path(path)
    for path in (
        _HOST_STATE_DESTINATION,
        _HOST_DAEMON_DESTINATION,
        _HOST_EXCHANGE_DESTINATION,
        _HOST_POLICY_DESTINATION,
        _FIXED_ENVIRONMENT["HOME"],
    )
)


def _operator_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """Filter the trusted operator environment passed to the broker and its Slurm clients.

    :param environ: The operator environment, usually ``os.environ``.
    :return: The kept variables in name order, bounded in count and size.
    """

    kept: dict[str, str] = {}
    total = 0
    for name in sorted(environ):
        value = environ[name]
        if (
            (name.startswith(_OPERATOR_DROPPED_PREFIXES) and name != "SLURM_CONF")
            or name in _OPERATOR_DROPPED
            or not _OPERATOR_NAME.match(name)
            or "\0" in value
        ):
            continue
        total += len(os.fsencode(name)) + len(os.fsencode(value)) + 2
        # ponytail: a full bound drops the alphabetically last variables; rank them if a site ever hits it.
        if len(kept) == _OPERATOR_LIMIT or total > _OPERATOR_BYTES:
            print(
                f"httk workspace daemon: operator environment beyond {_OPERATOR_LIMIT} variables or "
                f"{_OPERATOR_BYTES} bytes was dropped from {name}",
                file=sys.stderr,
            )
            break
        kept[name] = value
    return kept


def _source_path() -> Path:
    return Path(__file__).absolute()


def _is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(path == root or path.is_relative_to(root) for root in roots)


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left.is_relative_to(right) or right.is_relative_to(left)


def _check_reserved_sandbox_paths(workspace: Path, config_home: Path) -> None:
    """Refuse workspace and operator paths that overlap private sandbox destinations.

    :param workspace: The resolved workspace root.
    :param config_home: The resolved operator configuration directory.
    :raises ValueError: If either path overlaps a sandbox service destination.
    """

    for label, path in (("workspace", workspace), ("HTTK_CONFIG_HOME", config_home)):
        reserved = next((candidate for candidate in _RESERVED_SANDBOX_PATHS if _overlap(path, candidate)), None)
        if reserved is not None:
            raise ValueError(f"{label} overlaps reserved daemon sandbox path {reserved}")


def _check_protected_file(path: Path, mutable_roots: tuple[Path, ...] = ()) -> os.stat_result:
    try:
        information = path.lstat()
    except OSError as exc:
        raise ValueError(f"trusted source is unavailable: {path}") from exc
    if not stat.S_ISREG(information.st_mode):
        raise ValueError(f"trusted source must be a regular non-symlink file: {path}")
    if information.st_mode & 0o002:
        raise ValueError(f"trusted source must not be world-writable: {path}")
    if _is_within(path, mutable_roots):
        raise ValueError(f"trusted source must be outside daemon mutable roots: {path}")
    return information


def _trusted_module(name: str) -> dict[str, Any]:
    # Site-packages are disabled here, so sibling modules load by path once their file is checked.
    source = _source_path().with_name(name)
    _check_protected_file(source)
    return runpy.run_path(str(source))


def _policy_api() -> dict[str, Any]:
    return _trusted_module("_daemon_policy.py")


_SANDBOX = _trusted_module("_sandbox.py")
if TYPE_CHECKING:
    from ._sandbox import BWRAP_USERNS_BLOCK as _BWRAP_USERNS_BLOCK
    from ._sandbox import F_ADD_SEALS as _F_ADD_SEALS
    from ._sandbox import SEALS as _SEALS
    from ._sandbox import PreparedSandbox as _PreparedSandbox
    from ._sandbox import make_inheritable as _make_inheritable
    from ._sandbox import memfd_create as _memfd_create
    from ._sandbox import open_directory_nofollow as _open_directory_nofollow
else:
    _F_ADD_SEALS = _SANDBOX["F_ADD_SEALS"]
    _SEALS = _SANDBOX["SEALS"]
    _BWRAP_USERNS_BLOCK = _SANDBOX["BWRAP_USERNS_BLOCK"]
    _PreparedSandbox = _SANDBOX["PreparedSandbox"]
    _open_directory_nofollow = _SANDBOX["open_directory_nofollow"]
    _memfd_create = _SANDBOX["memfd_create"]
    _make_inheritable = _SANDBOX["make_inheritable"]


def _load_policy_once(path: Path) -> tuple[Any, bytes]:
    api = _policy_api()
    loader: Any = api.get("_load_policy_with_bytes")
    if not callable(loader):
        raise RuntimeError("installed daemon policy module has no isolated loader")
    policy, data = cast(tuple[Any, bytes], loader(path))
    return policy, data


def _resolve_declared_path(path: Path, *, strict: bool) -> Path:
    try:
        return path.resolve(strict=strict)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"approved runtime path is unavailable: {path}") from exc


def _mutable_roots(policy: Any) -> tuple[Path, ...]:
    # Trusted code and commands lie outside the whole workspace, which now also holds the daemon directory.
    return (_resolve_declared_path(policy.workspace, strict=False),)


def _check_command(path: Path, mutable_roots: tuple[Path, ...]) -> None:
    resolved = _resolve_declared_path(path, strict=True)
    if any(_overlap(resolved, mutable_root) for mutable_root in mutable_roots):
        raise ValueError(f"trusted command resolves across a mutable root: {path}")
    information = resolved.stat()
    if not stat.S_ISREG(information.st_mode) or information.st_mode & 0o111 == 0:
        raise ValueError(f"trusted command must be an executable regular file: {path}")
    if information.st_uid not in (0, os.geteuid()) or information.st_mode & 0o002:
        raise ValueError(f"trusted command has unprotected ownership or mode: {path}")


def _policy_snapshot(data: bytes) -> int:
    descriptor = _memfd_create("httk-daemon-policy")
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("policy snapshot write made no progress")
            offset += written
        os.lseek(descriptor, 0, os.SEEK_SET)
        fcntl.fcntl(descriptor, _F_ADD_SEALS, _SEALS)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _host_identity() -> str:
    """Return the bootstrap host's identity for the daemon owner record.

    The owner's PID comes from Bubblewrap ``--info-fd`` and its start ticks from the namespace ``/proc/1/stat``:
    both name Bubblewrap's init, the sandbox's PID 1, which normally dies with the Bubblewrap monitor
    (``--die-with-parent``) and whose death means the kernel has killed and reaped every process in the namespace — which the monitor's
    death alone does not establish.

    :return: Compact JSON with ``hostname`` and ``boot_id``.
    :raises ValueError: If the host boot identity cannot be read.
    """

    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError("cannot read this host's boot identity") from exc
    if not boot_id or len(boot_id) > 64:
        raise ValueError("host boot identity is malformed")
    return json.dumps({"hostname": socket.gethostname(), "boot_id": boot_id}, separators=(",", ":"))


def _host_config_home() -> Path:
    override = os.environ.get("HTTK_CONFIG_HOME")
    if override:
        return Path(override).expanduser().resolve()
    base = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))).expanduser()
    return (base / "httk").resolve()


def _base_bwrap_argv(policy: Any, *, block_userns: bool) -> list[str]:
    argv = [
        str(policy.bwrap),
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
    ]
    if block_userns:
        argv += _BWRAP_USERNS_BLOCK
    argv += [
        "--cap-drop",
        "ALL",
        "--new-session",
        "--die-with-parent",
        "--clearenv",
    ]
    environment = dict(_FIXED_ENVIRONMENT) | _operator_environment(os.environ) | _BROKER_ENVIRONMENT
    for name in _BROKER_UNSET:
        environment.pop(name, None)
    if policy.slurm_conf is not None:
        environment["SLURM_CONF"] = str(policy.slurm_conf)
    environment["HTTK_CONFIG_HOME"] = str(_host_config_home())
    for name, value in environment.items():
        argv += ["--setenv", name, value]
    # Recursive, so host submounts (/software, /etc, the munge socket) come in read-only as well.
    argv += ["--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--dir", "/tmp/home"]
    argv += ["--dir", "/tmp/home/.cache"]
    return argv


def _inside_command(arguments: argparse.Namespace, policy: Any, policy_source: Path, *, scope_fd: int) -> list[str]:
    result = [
        str(policy.python),
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        _HOST_POLICY_DESTINATION,
        "--policy-source",
        str(policy_source),
        "--execution-scope-fd",
        str(scope_fd),
        "--host-identity",
        _host_identity(),
    ]
    if arguments.check:
        result.append("--check")
    elif arguments.once:
        result.append("--once")
    return result


def _prepare_sandbox(
    arguments: argparse.Namespace, policy: Any, policy_data: bytes, policy_source: Path
) -> _PreparedSandbox:
    workspace = _resolve_declared_path(policy.workspace, strict=False)
    if workspace in (Path("/"), Path("/tmp")):
        raise ValueError(f"daemon workspace may not be rooted at {workspace}")
    config_home = _host_config_home()
    _check_reserved_sandbox_paths(workspace, config_home)
    mutable_roots = (policy.workspace,)
    resolved_mutable = _mutable_roots(policy)
    source = _source_path()
    _check_protected_file(source, mutable_roots)
    _check_protected_file(source.with_name("_daemon_policy.py"), mutable_roots)
    _check_protected_file(source.with_name("_sandbox.py"), mutable_roots)
    _check_protected_file(policy_source, (policy.exchange, policy.broker))
    for command in (policy.python, policy.bwrap, policy.sbatch, policy.squeue, policy.scancel):
        _check_command(command, resolved_mutable)
    if policy.slurm_conf is not None:
        resolved_slurm_conf = _resolve_declared_path(policy.slurm_conf, strict=True)
        if any(_overlap(resolved_slurm_conf, mutable_root) for mutable_root in resolved_mutable):
            raise ValueError("Slurm configuration resolves across a mutable root")
    block_userns = cast(bool, _SANDBOX["check_bwrap"](policy.bwrap, environment=_FIXED_ENVIRONMENT))
    if not block_userns:
        # Mounts stay locked either way; only nested user namespaces (kernel attack surface) remain open.
        print(
            "daemon bootstrap: warning: this Bubblewrap lacks --disable-userns (0.8.0+); "
            "sandboxed code can create nested user namespaces",
            file=sys.stderr,
            flush=True,
        )

    descriptors: list[int] = []
    argv = _base_bwrap_argv(policy, block_userns=block_userns)
    try:
        try:
            workspace_descriptor = _open_directory_nofollow(policy.workspace)
        except OSError as exc:
            raise ValueError(f"cannot open {policy.workspace} as a real workspace directory ({exc.strerror})") from exc
        descriptors.append(workspace_descriptor)
        workspace = _resolve_declared_path(policy.workspace, strict=True)
        if workspace in (Path("/"), Path("/tmp"), Path("/tmp/home")):
            raise ValueError(f"daemon workspace may not be rooted at {workspace}")

        # Private /tmp hides host workspaces beneath /tmp, so recreate only its parent chain before the bind.
        if workspace.is_relative_to(Path("/tmp")):
            parts = workspace.parent.parts[2:]
            parent = Path("/tmp")
            for part in parts:
                parent /= part
                argv += ["--dir", str(parent)]
            argv += ["--dir", str(workspace)]
        argv += ["--ro-bind-fd", str(workspace_descriptor), str(workspace)]

        def bind_workspace_directory(relative: str, destination: str | None = None, *, readonly: bool = False) -> int:
            parts = Path(relative).parts
            current = workspace_descriptor
            owned: list[int] = []
            try:
                for part in parts:
                    current = os.open(
                        part,
                        _SANDBOX["O_PATH"] | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                        dir_fd=current,
                    )
                    owned.append(current)
                descriptor = current
                descriptors.append(descriptor)
            except BaseException:
                for opened in owned:
                    os.close(opened)
                raise
            for opened in owned[:-1]:
                os.close(opened)
            option = "--ro-bind-fd" if readonly else "--bind-fd"
            target = workspace / relative
            argv.extend([option, str(descriptor), str(target)])
            if destination is not None:
                # Bubblewrap closes a --bind-fd descriptor once it is mounted, so the second bind needs its own.
                descriptors.append(os.dup(descriptor))
                argv.extend(["--dir", str(destination), option, str(descriptors[-1]), str(destination)])
            return descriptor

        try:
            bind_workspace_directory("exchange", _HOST_EXCHANGE_DESTINATION)
        except OSError as exc:
            raise ValueError(
                f"cannot open {policy.exchange} as a real directory ({exc.strerror}); "
                "the daemon serves the workspace's exchange, which 'httk workspace exchange enable' creates"
            ) from exc
        bind_workspace_directory("jobs")
        # Keep the private daemon tree read-only, then overlay only its broker ledger directory writable below.
        bind_workspace_directory(".httk-workspace/daemon", _HOST_DAEMON_DESTINATION, readonly=True)
        control_paths = (
            ".httk-workspace/owners",
            ".httk-workspace/requests",
            ".httk-workspace/tmp",
            ".httk-workspace/quarantine",
            ".httk-workspace/exchange-jobs",
            ".httk-workspace/daemon/broker",
        )
        for relative in control_paths:
            destination = _HOST_STATE_DESTINATION if relative.endswith("/broker") else None
            bind_workspace_directory(relative, destination)

        writable_paths = [workspace / relative for relative in ("jobs", *control_paths, "exchange")]
        if any(_overlap(config_home, path) for path in writable_paths):
            raise ValueError("HTTK_CONFIG_HOME overlaps a daemon writable workspace directory")
        if config_home == Path("/tmp"):
            raise ValueError("HTTK_CONFIG_HOME may not be /tmp inside the daemon sandbox")
        if config_home.is_relative_to(Path("/tmp")) and config_home.exists():
            if not config_home.is_relative_to(workspace):
                config_parent = Path("/tmp")
                for part in config_home.parent.parts[2:]:
                    config_parent /= part
                    argv += ["--dir", str(config_parent)]
                argv += ["--dir", str(config_home)]
            try:
                config_fd = _open_directory_nofollow(config_home)
            except OSError as exc:
                raise ValueError(f"cannot open HTTK_CONFIG_HOME as a real directory ({exc.strerror})") from exc
            descriptors.append(config_fd)
            argv += ["--ro-bind-fd", str(config_fd), str(config_home)]

        if workspace.is_relative_to(Path("/tmp")):
            project_metadata = next(
                (
                    candidate / "httk_project"
                    for candidate in (workspace.parent, *workspace.parent.parents)
                    if (candidate / "httk_project").is_dir()
                ),
                None,
            )
            if project_metadata is not None and not project_metadata.is_relative_to(workspace):
                argv += ["--dir", str(project_metadata)]
                try:
                    project_fd = _open_directory_nofollow(project_metadata)
                except OSError as exc:
                    raise ValueError(f"cannot open enclosing httk_project metadata ({exc.strerror})") from exc
                descriptors.append(project_fd)
                argv += ["--ro-bind-fd", str(project_fd), str(project_metadata)]

        policy_fd = _policy_snapshot(policy_data)
        descriptors.append(policy_fd)
        argv += ["--ro-bind-data", str(policy_fd), _HOST_POLICY_DESTINATION]
        scope_writer_fd = _memfd_create("httk-daemon-execution-scope")
        descriptors.append(scope_writer_fd)
        os.fchmod(scope_writer_fd, 0o600)
        scope_reader_fd = os.open(f"/proc/self/fd/{scope_writer_fd}", os.O_RDONLY | os.O_CLOEXEC)
        descriptors.append(scope_reader_fd)
        os.set_inheritable(scope_writer_fd, True)
        os.set_inheritable(scope_reader_fd, True)
        argv += ["--info-fd", str(scope_writer_fd)]
        argv += ["--chdir", "/"]
        argv += ["--", *_inside_command(arguments, policy, policy_source, scope_fd=scope_reader_fd)]
        return _PreparedSandbox(argv, tuple(descriptors))
    except BaseException:
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _open_descriptor_numbers() -> set[int]:
    try:
        return {int(name) for name in os.listdir("/proc/self/fd") if name.isdigit()}
    except OSError as exc:
        raise RuntimeError("cannot enumerate inherited descriptors through /proc/self/fd") from exc


def _exec_prepared(prepared: _PreparedSandbox) -> None:
    try:
        preserved = {0, 1, 2, *prepared.descriptors}
        for descriptor in _open_descriptor_numbers() - preserved:
            try:
                os.close(descriptor)
            except OSError:
                pass
        _make_inheritable(prepared.descriptors)
        os.execve(prepared.argv[0], prepared.argv, {})
    except BaseException:
        prepared.close()
        raise


def _recover_dead_owners(policy: Any) -> None:
    """Recover this host's dead daemon owners on the host, where their death can be proven, before confinement.

    The recovery runs with the trusted policy interpreter, since this bootstrap has no site-packages. A failure is
    reported and does not stop the daemon: the owners stay for a later start.

    :param policy: The validated daemon policy.
    """

    command = [
        str(policy.python),
        "-I",
        "-m",
        "httk.workflow._daemon_jobs",
        "--workspace",
        str(policy.workspace),
        "--workspace-id",
        str(policy.workspace_id),
    ]
    environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"} | _operator_environment(os.environ)
    try:
        code = subprocess.run(
            command, stdin=subprocess.DEVNULL, cwd="/", env=environment, timeout=_RECOVERY_TIMEOUT, check=False
        ).returncode
    except (OSError, subprocess.TimeoutExpired) as exc:
        code, detail = None, str(exc)
    else:
        detail = f"exit status {code}"
    if code != 0:
        print(f"daemon bootstrap: warning: dead daemon owner recovery failed ({detail})", file=sys.stderr, flush=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workspace-daemon-bootstrap")
    parser.add_argument("--policy", required=True, metavar="ABS")
    parser.add_argument("--workspace", required=True, metavar="ABS")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--once", action="store_true")
    return parser


def _validate_arguments(arguments: argparse.Namespace, policy: Any) -> Path:
    policy_source = Path(arguments.policy)
    if not policy_source.is_absolute() or ".." in policy_source.parts or "\0" in str(policy_source):
        raise ValueError("--policy must be an absolute local path")
    workspace = Path(arguments.workspace)
    if not workspace.is_absolute() or ".." in workspace.parts or "\0" in str(workspace):
        raise ValueError("--workspace must be an absolute local path")
    if workspace != policy.workspace:
        raise ValueError("--workspace must exactly match the policy workspace")
    return policy_source


def main(argv: list[str] | None = None) -> int:
    """Validate the policy and replace this process with the sandboxed broker.

    :param argv: Bootstrap arguments, or process arguments when omitted.
    :return: Two on a validation or startup refusal; on success this process becomes Bubblewrap.
    """

    arguments = _parser().parse_args(sys.argv[1:] if argv is None else argv)
    try:
        policy_path = Path(arguments.policy)
        if not policy_path.is_absolute():
            raise ValueError("--policy must be an absolute local path")
        policy, policy_data = _load_policy_once(policy_path)
        policy_source = _validate_arguments(arguments, policy)
        prepared = _prepare_sandbox(arguments, policy, policy_data, policy_source)
        try:
            if not arguments.check:
                _recover_dead_owners(policy)
            _exec_prepared(prepared)
        finally:
            prepared.close()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon bootstrap: {exc}", file=sys.stderr, flush=True)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
