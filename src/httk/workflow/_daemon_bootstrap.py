"""Isolated Bubblewrap bootstrap of the workspace daemon broker."""

import argparse
import fcntl
import os
import re
import runpy
import stat
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

# The broker runs only trusted code, so it sees the host read-only; its writable mounts and policy data
# sit on the private /tmp, since a read-only / takes no new directories.
_HOST_EXCHANGE_DESTINATION = "/tmp/daemon-exchange"
_HOST_STATE_DESTINATION = "/tmp/control"
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
_BROKER_UNSET = ("XDG_RUNTIME_DIR", "XDG_CONFIG_HOME")


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


def _load_policy_once(path: Path) -> tuple[Any, bytes, Any]:
    api = _policy_api()
    loader: Any = api.get("_load_policy_with_bytes")
    check_private_paths: Any = api.get("check_private_paths")
    if not callable(loader) or not callable(check_private_paths):
        raise RuntimeError("installed daemon policy module has no isolated loader or private-path check")
    policy, data = cast(tuple[Any, bytes], loader(path))
    return policy, data, check_private_paths


def _resolve_declared_path(path: Path, *, strict: bool) -> Path:
    try:
        return path.resolve(strict=strict)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"approved runtime path is unavailable: {path}") from exc


def _mutable_roots(policy: Any) -> tuple[Path, ...]:
    # Trusted code and commands must lie outside the whole workspace (jobs write there, not only the
    # exchange the broker binds) and outside the broker's private state.
    return tuple(_resolve_declared_path(path, strict=False) for path in (policy.workspace, policy.state))


def _check_command(path: Path, mutable_roots: tuple[Path, ...]) -> None:
    resolved = _resolve_declared_path(path, strict=True)
    if any(_overlap(resolved, mutable_root) for mutable_root in mutable_roots):
        raise ValueError(f"trusted command resolves across a mutable root: {path}")
    information = resolved.stat()
    if not stat.S_ISREG(information.st_mode) or information.st_mode & 0o111 == 0:
        raise ValueError(f"trusted command must be an executable regular file: {path}")
    if information.st_uid not in (0, os.geteuid()) or information.st_mode & 0o002:
        raise ValueError(f"trusted command has unprotected ownership or mode: {path}")


def _check_bwrap(path: Path) -> bool:
    """Check the Bubblewrap options from its help text; return whether it can block nested user namespaces."""

    return cast(bool, _SANDBOX["check_bwrap"](path, environment=_FIXED_ENVIRONMENT))


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
    for name, value in environment.items():
        argv += ["--setenv", name, value]
    # Recursive, so host submounts (/software, /etc, the munge socket) come in read-only as well.
    argv += ["--ro-bind", "/", "/", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--dir", "/tmp/home"]
    argv += ["--dir", "/tmp/home/.cache"]
    return argv


def _inside_command(arguments: argparse.Namespace, policy: Any, policy_source: Path) -> list[str]:
    result = [
        str(policy.python),
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        _HOST_POLICY_DESTINATION,
        "--policy-source",
        str(policy_source),
    ]
    if arguments.check:
        result.append("--check")
    elif arguments.once:
        result.append("--once")
    return result


def _prepare_sandbox(
    arguments: argparse.Namespace, policy: Any, policy_data: bytes, policy_source: Path, check_private_paths: Any
) -> _PreparedSandbox:
    mutable_roots = (policy.workspace, policy.state)
    resolved_mutable = _mutable_roots(policy)
    source = _source_path()
    _check_protected_file(source, mutable_roots)
    _check_protected_file(source.with_name("_daemon_policy.py"), mutable_roots)
    _check_protected_file(source.with_name("_sandbox.py"), mutable_roots)
    _check_protected_file(policy_source, mutable_roots)
    # State and snapshots stay outside the workspace on every entry, symlinks followed: inside the exchange
    # they would hand the client the ledger and configuration, elsewhere in the workspace the keys to jobs.
    check_private_paths(policy)
    for command in (policy.python, policy.bwrap, policy.sbatch, policy.squeue, policy.scancel):
        _check_command(command, resolved_mutable)
    if policy.slurm_conf is not None:
        resolved_slurm_conf = _resolve_declared_path(policy.slurm_conf, strict=True)
        if any(_overlap(resolved_slurm_conf, mutable_root) for mutable_root in resolved_mutable):
            raise ValueError("Slurm configuration resolves across a mutable root")
    block_userns = _check_bwrap(policy.bwrap)
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
        # Two writable binds, each taken from a no-follow descriptor: the workspace's exchange, the only
        # client-writable path the broker reaches (it opens everything below it descriptor-anchored and
        # no-follow), and the broker's private state. The rest of the workspace is not visible for writing.
        for source_path, destination in (
            (policy.exchange, _HOST_EXCHANGE_DESTINATION),
            (policy.state, _HOST_STATE_DESTINATION),
        ):
            try:
                descriptor = _open_directory_nofollow(source_path)
            except OSError as exc:
                raise ValueError(
                    f"cannot open {source_path} as a real directory ({exc.strerror}); "
                    "the daemon serves the workspace's exchange, which 'httk workspace exchange enable' creates"
                ) from exc
            descriptors.append(descriptor)
            argv += ["--dir", destination, "--bind-fd", str(descriptor), destination]
        policy_fd = _policy_snapshot(policy_data)
        descriptors.append(policy_fd)
        argv += ["--ro-bind-data", str(policy_fd), _HOST_POLICY_DESTINATION]
        argv += ["--chdir", "/"]
        argv += ["--", *_inside_command(arguments, policy, policy_source)]
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
        policy, policy_data, check_private_paths = _load_policy_once(policy_path)
        policy_source = _validate_arguments(arguments, policy)
        prepared = _prepare_sandbox(arguments, policy, policy_data, policy_source, check_private_paths)
        try:
            _exec_prepared(prepared)
        finally:
            prepared.close()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon bootstrap: {exc}", file=sys.stderr, flush=True)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
