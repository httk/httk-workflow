"""Isolated Bubblewrap bootstrap for the confined workspace daemon."""

import argparse
import fcntl
import os
import re
import runpy
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

_HANDLE = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_BWRAP_OPTIONS = frozenset(
    {
        "--assert-userns-disabled",
        "--bind-fd",
        "--clearenv",
        "--disable-userns",
        "--new-session",
        "--ro-bind-data",
        "--ro-bind-fd",
        "--unshare-ipc",
        "--unshare-net",
        "--unshare-pid",
        "--unshare-user",
        "--unshare-uts",
    }
)
_FIXED_ENVIRONMENT = {
    "PATH": "/usr/bin:/bin",
    "HOME": "/tmp/home",
    "TMPDIR": "/tmp",
    "LANG": "C.UTF-8",
}


@dataclass(slots=True)
class _PreparedSandbox:
    argv: list[str]
    descriptors: tuple[int, ...]

    def close(self) -> None:
        descriptors, self.descriptors = self.descriptors, ()
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(frozen=True, slots=True)
class _ResolvedRoots:
    mutable: tuple[Path, ...]
    readonly: tuple[Path, ...]
    broker: tuple[Path, ...]


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
    if information.st_mode & 0o022:
        raise ValueError(f"trusted source must not be writable by group or other: {path}")
    if _is_within(path, mutable_roots):
        raise ValueError(f"trusted source must be outside daemon mutable roots: {path}")
    return information


def _policy_api() -> dict[str, Any]:
    source = _source_path().with_name("_daemon_policy.py")
    _check_protected_file(source)
    return runpy.run_path(str(source))


def _load_policy_once(path: Path) -> tuple[Any, bytes]:
    api = _policy_api()
    loader: Any = api.get("_load_policy_with_bytes")
    if not callable(loader):
        raise RuntimeError("installed daemon policy module has no isolated loader")
    policy, data = cast(tuple[Any, bytes], loader(path))
    return policy, data


def _open_directory_nofollow(path: Path) -> int:
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            previous_descriptor, descriptor = descriptor, next_descriptor
            os.close(previous_descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _resolve_declared_path(path: Path, *, strict: bool) -> Path:
    try:
        return path.resolve(strict=strict)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"approved runtime path is unavailable: {path}") from exc


def _resolved_roots(policy: Any, mode: str) -> _ResolvedRoots:
    mutable = tuple(
        _resolve_declared_path(path, strict=False)
        for path in (policy.workspace, policy.requests, policy.responses, policy.state)
    )
    readonly = tuple(_resolve_declared_path(path, strict=True) for path in policy.readonly_paths)
    broker = tuple(_resolve_declared_path(path, strict=mode == "broker") for path in policy.broker_paths)
    if Path("/") in {*readonly, *broker}:
        raise ValueError("approved runtime paths must not resolve to the filesystem root")
    if any(_overlap(runtime_root, mutable_root) for runtime_root in (*readonly, *broker) for mutable_root in mutable):
        raise ValueError("resolved runtime paths must be disjoint from mutable roots")
    if any(_overlap(readonly_root, broker_root) for readonly_root in readonly for broker_root in broker):
        raise ValueError("resolved readonly and broker roots must be pairwise disjoint")
    return _ResolvedRoots(mutable, readonly, broker)


def _check_command(path: Path, approved: tuple[Path, ...], mutable_roots: tuple[Path, ...]) -> None:
    resolved = _resolve_declared_path(path, strict=True)
    if any(_overlap(resolved, mutable_root) for mutable_root in mutable_roots):
        raise ValueError(f"trusted command resolves across a mutable root: {path}")
    if not _is_within(resolved, approved):
        raise ValueError(f"trusted command resolves outside its approved roots: {path}")
    information = resolved.stat()
    if not stat.S_ISREG(information.st_mode) or information.st_mode & 0o111 == 0:
        raise ValueError(f"trusted command must be an executable regular file: {path}")


def _check_bwrap(path: Path) -> None:
    environment = dict(_FIXED_ENVIRONMENT)
    try:
        version = subprocess.run(
            [str(path), "--version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=environment,
        )
        help_result = subprocess.run(
            [str(path), "--help"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("Bubblewrap feature check failed") from exc
    match = re.search(r"(?:bubblewrap\s+)?(\d+)\.(\d+)(?:\.(\d+))?", version.stdout)
    if version.returncode != 0 or match is None or tuple(int(item or 0) for item in match.groups()) < (0, 9, 0):
        raise ValueError("Bubblewrap 0.9.0 or newer is required")
    help_text = help_result.stdout + help_result.stderr
    if help_result.returncode != 0 or any(option not in help_text for option in _BWRAP_OPTIONS):
        raise ValueError("Bubblewrap lacks required confinement features")


def _policy_snapshot(data: bytes) -> int:
    if not hasattr(os, "memfd_create"):
        raise RuntimeError("the daemon bootstrap requires Linux memfd support")
    descriptor = os.memfd_create("httk-daemon-policy", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        os.fchmod(descriptor, 0o600)
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("policy snapshot write made no progress")
            offset += written
        os.lseek(descriptor, 0, os.SEEK_SET)
        seals = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_WRITE
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS, seals)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _base_bwrap_argv(policy: Any, mode: str) -> list[str]:
    argv = [
        str(policy.bwrap),
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
    ]
    if mode == "payload":
        argv.append("--unshare-net")
    argv += [
        "--disable-userns",
        "--assert-userns-disabled",
        "--cap-drop",
        "ALL",
        "--new-session",
        "--die-with-parent",
        "--clearenv",
    ]
    for name, value in _FIXED_ENVIRONMENT.items():
        argv += ["--setenv", name, value]
    if mode == "broker" and policy.slurm_conf is not None:
        argv += ["--setenv", "SLURM_CONF", str(policy.slurm_conf)]
    # Bubblewrap starts from its own empty tmpfs root; only these private paths
    # and the descriptor-backed mounts below are then added to it.
    argv += ["--dir", "/workspace", "--tmpfs", "/tmp", "--dir", "/tmp/home"]
    return argv


def _inside_command(arguments: argparse.Namespace, policy: Any, policy_source: Path) -> list[str]:
    if arguments.mode == "payload":
        return [
            str(policy.python),
            "-I",
            "-m",
            "httk.workflow._daemon_payload",
            "--profile",
            arguments.profile,
            "--handle",
            arguments.handle,
        ]
    result = [
        str(policy.python),
        "-I",
        "-m",
        "httk.workflow._daemon_service",
        "--policy",
        "/daemon-policy.json",
        "--policy-source",
        str(policy_source),
    ]
    if arguments.check:
        result.append("--check")
    elif arguments.initialize:
        result.append("--initialize")
    elif arguments.once:
        result.append("--once")
    return result


def _prepare_sandbox(
    arguments: argparse.Namespace, policy: Any, policy_data: bytes, policy_source: Path
) -> _PreparedSandbox:
    mutable_roots = (policy.workspace, policy.requests, policy.responses, policy.state)
    resolved_roots = _resolved_roots(policy, arguments.mode)
    source = _source_path()
    _check_protected_file(source, mutable_roots)
    _check_protected_file(source.with_name("_daemon_policy.py"), mutable_roots)
    _check_protected_file(policy_source, mutable_roots)
    _check_command(policy.python, resolved_roots.readonly, resolved_roots.mutable)
    _check_command(policy.bwrap, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
    if arguments.mode == "broker":
        for command in (policy.sbatch, policy.squeue, policy.scancel):
            _check_command(command, (*resolved_roots.readonly, *resolved_roots.broker), resolved_roots.mutable)
        if policy.slurm_conf is not None:
            resolved_slurm_conf = _resolve_declared_path(policy.slurm_conf, strict=True)
            if not _is_within(resolved_slurm_conf, resolved_roots.broker) or any(
                _overlap(resolved_slurm_conf, mutable_root) for mutable_root in resolved_roots.mutable
            ):
                raise ValueError("Slurm configuration resolves outside broker-only runtime roots")
    _check_bwrap(policy.bwrap)

    descriptors: list[int] = []
    argv = _base_bwrap_argv(policy, arguments.mode)
    try:
        workspace_fd = _open_directory_nofollow(policy.workspace)
        descriptors.append(workspace_fd)
        argv += ["--bind-fd" if arguments.mode == "payload" else "--ro-bind-fd", str(workspace_fd), "/workspace"]
        if arguments.mode == "broker":
            for source_path, destination in (
                (policy.requests, "/requests"),
                (policy.responses, "/responses"),
                (policy.state, "/control"),
            ):
                descriptor = _open_directory_nofollow(source_path)
                descriptors.append(descriptor)
                argv += ["--dir", destination, "--bind-fd", str(descriptor), destination]

        runtime_paths = list(zip(policy.readonly_paths, resolved_roots.readonly, strict=True))
        if arguments.mode == "broker":
            runtime_paths.extend(zip(policy.broker_paths, resolved_roots.broker, strict=True))
        for runtime_path, resolved_path in runtime_paths:
            descriptor = os.open(resolved_path, os.O_PATH | os.O_CLOEXEC)
            descriptors.append(descriptor)
            argv += ["--ro-bind-fd", str(descriptor), str(runtime_path)]

        policy_fd = _policy_snapshot(policy_data)
        descriptors.append(policy_fd)
        argv += ["--ro-bind-data", str(policy_fd), "/daemon-policy.json"]
        argv += ["--proc", "/proc", "--dev", "/dev", "--chdir", "/workspace" if arguments.mode == "payload" else "/"]
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
        for descriptor in prepared.descriptors:
            os.set_inheritable(descriptor, True)
        os.execve(prepared.argv[0], prepared.argv, {})
    except BaseException:
        prepared.close()
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workspace-daemon-bootstrap")
    parser.add_argument("--policy", required=True, metavar="ABS")
    parser.add_argument("--workspace", metavar="ABS")
    parser.add_argument("--mode", required=True, choices=("broker", "payload"))
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true")
    modes.add_argument("--initialize", action="store_true")
    modes.add_argument("--once", action="store_true")
    parser.add_argument("--profile")
    parser.add_argument("--handle")
    return parser


def _validate_arguments(arguments: argparse.Namespace, policy: Any) -> Path:
    policy_source = Path(arguments.policy)
    if not policy_source.is_absolute() or ".." in policy_source.parts or "\0" in str(policy_source):
        raise ValueError("--policy must be an absolute local path")
    if arguments.mode == "broker":
        if arguments.workspace is None:
            raise ValueError("broker mode requires --workspace")
        workspace = Path(arguments.workspace)
        if not workspace.is_absolute() or ".." in workspace.parts or "\0" in str(workspace):
            raise ValueError("--workspace must be an absolute local path")
        if workspace != policy.workspace:
            raise ValueError("--workspace must exactly match the policy workspace")
        if arguments.profile is not None or arguments.handle is not None:
            raise ValueError("broker mode forbids payload arguments")
    else:
        if arguments.workspace is not None or arguments.check or arguments.initialize or arguments.once:
            raise ValueError("payload mode forbids broker arguments")
        if type(arguments.profile) is not str or _PROFILE.fullmatch(arguments.profile) is None:
            raise ValueError("payload mode requires a valid --profile")
        policy.profile(arguments.profile)
        if type(arguments.handle) is not str or _HANDLE.fullmatch(arguments.handle) is None:
            raise ValueError("payload mode requires a valid --handle")
    return policy_source


def main(argv: list[str] | None = None) -> int:
    """Validate policy and replace this process with Bubblewrap.

    :param argv: Bootstrap arguments, or process arguments when omitted.
    :return: Two on a validation or startup refusal; success replaces the process.
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
            _exec_prepared(prepared)
        finally:
            prepared.close()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon bootstrap: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
