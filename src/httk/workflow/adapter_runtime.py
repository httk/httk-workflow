"""Packaged implementations used by the maintained adapter dispatcher.

The single ``adapter`` executable in each ``adapter_templates`` bundle executes
this module with one versioned JSON request file. The operation to run is read
from that request's ``operation`` member; the implementation that runs is
selected by the ``kind`` recorded in the bundle's ``remote.json``:

``local``
    Files are copied in this filesystem and commands run in this process tree.
``local-slurm``
    Files stay local; the manager is submitted with a generated ``sbatch``
    batch script.
``ssh-slurm``
    Files move with ``rsync`` over ``ssh`` and commands run on the configured
    host, where the manager is submitted with ``sbatch``.

Any other kind is refused rather than silently executed in the wrong place.

The ``start-manager`` operation reads its scheduler profile from the target
workspace's ``.httk-workflow/format.json``: ``slurm.*`` settings become Slurm
directives and ``manager.workers`` supplies the default worker count.

Every subprocess started here is an argument vector; no shell is ever handed an
interpolated string. ``ssh`` is the one unavoidable exception, because it always
concatenates its command words and lets a login shell on the far side parse the
result. All remote command strings, and the one line of the generated batch
script that runs the manager, are therefore built by ``_shell_command``,
which quotes element-wise. Nothing else in this module may compose a command
string from request or settings values.
"""

import json
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol

#: Where generated batch scripts and their Slurm output files are kept, relative
#: to the workspace the manager is started for.
BATCH_DIRECTORY = ".httk-workflow/batch"

#: Workspace settings mapped onto ``#SBATCH`` directives, in written order.
BATCH_DIRECTIVES = (
    ("slurm.account", "--account"),
    ("slurm.partition", "--partition"),
    ("slurm.time_limit", "--time"),
    ("slurm.nodes", "--nodes"),
    ("slurm.cpus_per_task", "--cpus-per-task"),
    ("slurm.reservation", "--reservation"),
)

SUPPORTED_KINDS = ("local", "local-slurm", "ssh-slurm")

#: The bundle metadata file.
METADATA_FILE = "remote.json"

#: The result format one adapter operation prints. The name is protocol and
#: keeps its historical spelling: an adapter written against an earlier release
#: prints exactly this, and every side already agrees on it.
RESULT_FORMAT = "httk-computer-result"

_CONNECT_TIMEOUT = 20
_WORKSPACE_FORMAT_MAX_BYTES = 1024 * 1024
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")
_FALSE_SETTINGS = frozenset({"0", "false", "no", "off"})
_SUBMITTED_JOB = re.compile(r"Submitted batch job (\d+)")
_WHITESPACE = re.compile(r"\s")

_MISSING_HTTK = (
    "httk-workflow is not available on the target; install it there, for "
    "example with 'pipx install httk-workflow', or configure the remote with "
    "httk_command=COMMAND, or opt into an automatic attempt with bootstrap=pip"
)


class _Runner(Protocol):
    """Run one argument vector where the adapter's work belongs."""

    def __call__(self, argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]": ...


def _result(operation: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "format": RESULT_FORMAT,
                "format_version": 1,
                "operation": operation,
                "ok": True,
                **values,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _refusal(operation: str, message: str) -> None:
    print(
        json.dumps(
            {
                "error": message,
                "format": RESULT_FORMAT,
                "format_version": 1,
                "operation": operation,
                "ok": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _adapter_kind(request: Mapping[str, object]) -> str | None:
    adapter_dir = request.get("adapter_dir")
    if not isinstance(adapter_dir, str) or not adapter_dir:
        return None
    root = Path(adapter_dir).expanduser()
    metadata_path = root / METADATA_FILE
    if not metadata_path.is_file():
        return None
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError(f"adapter JSON must be an object: {metadata_path}")
    kind = metadata.get("kind")
    return kind if isinstance(kind, str) and kind else None


def _shell_command(argv: Sequence[str], *, cwd: str | None = None) -> str:
    """Render *argv* for a shell that only ever receives one string.

    ``ssh`` joins the command words it is given and the remote login shell parses
    the result, so element-wise :func:`shlex.quote` is the only construction that
    keeps request and settings values out of that parser. The generated batch
    script uses the same helper for the same reason.
    """

    quoted = " ".join(shlex.quote(item) for item in argv)
    if cwd is None:
        return quoted
    return f"cd {shlex.quote(cwd)} && {quoted}"


def _settings(request: Mapping[str, object]) -> dict[str, Any]:
    value = request.get("remote_settings")
    return dict(value) if isinstance(value, Mapping) else {}


def _text(settings: Mapping[str, object], key: str) -> str | None:
    value = settings.get(key)
    if value is None or isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    text = str(value).strip()
    return text or None


def _flag(settings: Mapping[str, object], key: str, *, default: bool) -> bool:
    text = _text(settings, key)
    return default if text is None else text.lower() not in _FALSE_SETTINGS


def _argv(request: Mapping[str, object], operation: str) -> list[str]:
    raw = request.get("argv")
    if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
        raise ValueError(f"{operation} argv must be a nonempty string array")
    return [str(item) for item in raw]


def _cwd(request: Mapping[str, object]) -> str | None:
    value = request.get("cwd")
    return None if value is None else str(value)


def _paths(request: Mapping[str, object]) -> tuple[str, str]:
    source = request.get("source")
    destination = request.get("destination")
    if not isinstance(source, str) or not source or not isinstance(destination, str) or not destination:
        raise ValueError("transfer requires nonempty source and destination paths")
    return source, destination


def _files(request: Mapping[str, object]) -> list[str] | None:
    """Return the optional workspace-relative batch of paths to transfer."""

    raw = request.get("files")
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
        raise ValueError("transfer files must be an array of nonempty relative paths")
    for item in raw:
        if Path(str(item)).is_absolute() or ".." in Path(str(item)).parts:
            raise ValueError(f"transfer file is not workspace relative: {item}")
    return [str(item) for item in raw]


def _copy(source: Path, destination: Path) -> None:
    if source.is_dir():
        if destination.exists():
            source_manifest = source / ".httk-transfer" / "manifest.json"
            destination_manifest = destination / ".httk-transfer" / "manifest.json"
            if (
                not destination.is_dir()
                or not source_manifest.is_file()
                or not destination_manifest.is_file()
                or source_manifest.read_bytes() != destination_manifest.read_bytes()
            ):
                raise FileExistsError(destination)
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, symlinks=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _httk_prefix(settings: Mapping[str, object]) -> list[str] | None:
    """Split the optional ``httk_command`` remote setting into an argument vector.

    The value is parsed once, here, and every element is quoted again before it
    reaches a remote shell; it is never interpolated into a command string.
    """

    command = _text(settings, "httk_command")
    if command is None:
        return None
    prefix = shlex.split(command)
    if not prefix:
        raise ValueError("remote setting httk_command must not be empty")
    return prefix


def _local_httk(argv: Sequence[str], settings: Mapping[str, object]) -> list[str]:
    arguments = list(argv)
    if arguments and arguments[0] == "httk":
        prefix = _httk_prefix(settings)
        if prefix is not None:
            return [*prefix, *arguments[1:]]
        if shutil.which("httk") is None:
            return [sys.executable, "-m", "httk.core.cli", *arguments[1:]]
    return arguments


def _remote_httk(argv: Sequence[str], settings: Mapping[str, object]) -> list[str]:
    arguments = list(argv)
    if arguments and arguments[0] == "httk":
        prefix = _httk_prefix(settings)
        if prefix is not None:
            return [*prefix, *arguments[1:]]
    return arguments


def _ssh_transport(settings: Mapping[str, object]) -> list[str]:
    """Return the ``ssh`` argument vector that carries no remote command."""

    argv = ["ssh"]
    port = _text(settings, "port")
    if port is not None:
        if not port.isdigit():
            raise ValueError(f"remote setting port must be numeric: {port!r}")
        argv += ["-p", port]
    argv += ["-o", "BatchMode=yes", "-o", f"ConnectTimeout={_CONNECT_TIMEOUT}"]
    return argv


def _ssh_destination(settings: Mapping[str, object]) -> str:
    host = _text(settings, "host")
    if host is None:
        raise ValueError("the ssh-slurm adapter needs a remote setting host=HOSTNAME")
    if _WHITESPACE.search(host):
        raise ValueError(f"remote setting host must not contain whitespace: {host!r}")
    username = _text(settings, "username")
    if username is None:
        return host
    if _WHITESPACE.search(username):
        raise ValueError(f"remote setting username must not contain whitespace: {username!r}")
    return f"{username}@{host}"


def _ssh_run(
    settings: Mapping[str, object],
    argv: Sequence[str],
    *,
    cwd: str | None = None,
) -> "subprocess.CompletedProcess[str]":
    command = [
        *_ssh_transport(settings),
        "--",
        _ssh_destination(settings),
        _shell_command(_remote_httk(argv, settings), cwd=cwd),
    ]
    return subprocess.run(
        command,
        text=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _runner(kind: str, settings: Mapping[str, object]) -> _Runner:
    if kind == "ssh-slurm":

        def remote(argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]":
            return _ssh_run(settings, argv, cwd=cwd)

        return remote

    def local(argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]":
        return subprocess.run(
            _local_httk(argv, settings),
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )

    return local


@lru_cache(maxsize=1)
def _rsync_help() -> str:
    completed = subprocess.run(["rsync", "--help"], text=True, capture_output=True, check=False)
    return completed.stdout + completed.stderr


def _rsync_transport(settings: Mapping[str, object]) -> str:
    """Return the ``-e`` value; rsync splits it on whitespace and nothing else."""

    transport = _ssh_transport(settings)
    if any(_WHITESPACE.search(part) for part in transport):
        raise ValueError("ssh transport arguments must not contain whitespace")
    return " ".join(transport)


def _ensure_remote_directory(settings: Mapping[str, object], directory: str) -> None:
    completed = _ssh_run(settings, ["mkdir", "-p", directory])
    if completed.returncode != 0:
        raise RuntimeError(f"cannot create remote directory {directory}: {completed.stderr.strip()}")


def _rsync(
    settings: Mapping[str, object],
    *,
    source: str,
    destination: str,
    directory: bool,
    files: list[str] | None,
    push: bool,
) -> None:
    """Transfer one tree, or one explicit batch of files, with real rsync."""

    if files is not None:
        directory = True
    argv = ["rsync", "--archive", "--protect-args", "-e", _rsync_transport(settings)]
    mkpath = "--mkpath" in _rsync_help()
    if mkpath:
        argv.append("--mkpath")
    else:
        parent = destination.rstrip("/") if directory else str(Path(destination).parent)
        if push:
            _ensure_remote_directory(settings, parent)
        else:
            Path(parent).expanduser().mkdir(parents=True, exist_ok=True)
    listing: Path | None = None
    try:
        if files is not None:
            descriptor, name = tempfile.mkstemp(prefix="httk-adapter-files-", suffix=".txt")
            listing = Path(name)
            with open(descriptor, "w", encoding="utf-8") as stream:
                stream.write("".join(f"{item}\n" for item in files))
            argv.append(f"--files-from={listing}")
        local_side = source if push else destination
        remote_side = destination if push else source
        if directory:
            local_side = f"{local_side.rstrip('/')}/"
            remote_side = f"{remote_side.rstrip('/')}/"
        remote_side = f"{_ssh_destination(settings)}:{remote_side}"
        argv += [local_side, remote_side] if push else [remote_side, local_side]
        completed = subprocess.run(argv, text=True, capture_output=True, stdin=subprocess.DEVNULL, check=False)
    finally:
        if listing is not None:
            listing.unlink(missing_ok=True)
    if completed.returncode != 0:
        raise RuntimeError(f"rsync failed ({completed.returncode}): {completed.stderr.strip()}")
    if completed.stderr:
        print(completed.stderr.rstrip("\n"), file=sys.stderr)


def _batch_value(key: str, value: str) -> str:
    if _CONTROL_CHARACTER.search(value):
        raise ValueError(f"remote setting {key} must not contain control characters")
    return value


def _with_workers(argv: Sequence[str], workers: str | None) -> list[str]:
    """Append the configured worker count unless the caller already chose one."""

    arguments = list(argv)
    if workers is None or "--workers" in arguments:
        return arguments
    if not workers.isdigit() or int(workers) < 1:
        raise ValueError(f"workspace setting manager.workers must be a positive integer: {workers!r}")
    return [*arguments, "--workers", workers]


def _manager_workspace(argv: Sequence[str]) -> str | None:
    arguments = list(argv)
    for index in range(1, len(arguments) - 1):
        if arguments[index] == "run" and arguments[index - 1] == "manager":
            candidate = arguments[index + 1]
            return None if candidate.startswith("-") else candidate
    return None


def _workspace(request: Mapping[str, object], settings: Mapping[str, object], argv: Sequence[str]) -> str:
    """Return the real path; argv fallback is for hand-written path vectors only."""

    value = request.get("workspace")
    if isinstance(value, str) and value:
        return value
    derived = _manager_workspace(argv) or _text(settings, "workspace")
    if derived is None:
        raise ValueError("start-manager needs a workspace path in the request")
    return derived


def _count(request: Mapping[str, object]) -> int:
    value = request.get("count", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("start-manager count must be a positive integer")
    return value


def _workspace_settings(kind: str, settings: Mapping[str, object], workspace: str) -> dict[str, Any]:
    path = Path(workspace) / ".httk-workflow" / "format.json"
    try:
        if kind == "ssh-slurm":
            completed = _ssh_run(settings, ["head", "-c", str(_WORKSPACE_FORMAT_MAX_BYTES), str(path)])
            if completed.returncode != 0:
                raise ValueError(completed.stderr.strip() or completed.returncode)
            raw = completed.stdout
            if len(raw.encode("utf-8")) >= _WORKSPACE_FORMAT_MAX_BYTES:
                raise ValueError("workspace format too large")
        else:
            with path.open("rb") as stream:
                encoded = stream.read(_WORKSPACE_FORMAT_MAX_BYTES)
            if len(encoded) >= _WORKSPACE_FORMAT_MAX_BYTES:
                raise ValueError("workspace format too large")
            raw = encoded.decode("utf-8")
        document = json.loads(raw)
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read workspace format for {workspace}: {path}: {exc}") from exc
    if not isinstance(document, Mapping):
        raise ValueError(f"workspace format for {workspace} is not a JSON object: {path}")
    settings = document.get("settings", {})
    if not isinstance(settings, Mapping):
        raise ValueError(f"workspace settings for {workspace} are not a JSON object: {path}")
    return dict(settings)


def _batch_script(
    argv: Sequence[str],
    *,
    settings: Mapping[str, object],
    workspace: str,
    directory: str,
) -> str:
    lines = [
        # Login shell so an ``environment.prelude`` that runs ``module load`` works
        # here exactly as it does on the ``bash -lc`` local path and the ``bash -l``
        # executor path. Login profiles are sourced before the body, so a non-zero
        # command in a profile never trips the ``set -e`` below.
        "#!/bin/bash -l",
        "# Generated by the httk workflow start-manager adapter operation.",
        "#SBATCH --job-name=httk-manager",
    ]
    for key, directive in BATCH_DIRECTIVES:
        value = _text(settings, key)
        if value is not None:
            lines.append(f"#SBATCH {directive}={_batch_value(key, value)}")
    lines += [
        f"#SBATCH --chdir={_batch_value('workspace', workspace)}",
        f"#SBATCH --output={_batch_value('workspace', directory)}/manager-%j.out",
        f"#SBATCH --error={_batch_value('workspace', directory)}/manager-%j.err",
        "",
        # ``set -e`` aborts on a failing prelude line; no ``-u``, which breaks
        # module scripts and only ever guarded the fully quoted exec line below.
        "set -e",
    ]
    prelude = _text(settings, "environment.prelude")
    if prelude:
        lines.append(prelude)
    lines += [
        # One quoted line, built by the single helper every command string uses.
        f"exec {_shell_command(argv)}",
        "",
    ]
    return "\n".join(lines)


def _submit(
    kind: str,
    settings: Mapping[str, object],
    *,
    script: str,
    workspace: str,
    directory: str,
    count: int,
) -> dict[str, object]:
    """Write the batch script where sbatch runs and submit it *count* times."""

    name = f"manager-{uuid.uuid4().hex}.sbatch"
    if kind == "ssh-slurm":
        remote = f"{directory}/{name}"
        _ensure_remote_directory(settings, directory)
        descriptor, temporary_name = tempfile.mkstemp(prefix="httk-adapter-batch-", suffix=".sbatch")
        temporary = Path(temporary_name)
        try:
            with open(descriptor, "w", encoding="utf-8") as stream:
                stream.write(script)
            temporary.chmod(0o700)
            _rsync(settings, source=str(temporary), destination=remote, directory=False, files=None, push=True)
        finally:
            temporary.unlink(missing_ok=True)
        path = remote
    else:
        local_directory = Path(directory).expanduser()
        local_directory.mkdir(parents=True, exist_ok=True)
        local_path = local_directory / name
        local_path.write_text(script, encoding="utf-8")
        local_path.chmod(0o700)
        path = str(local_path)
    run = _runner(kind, settings)
    job_ids: list[str] = []
    for index in range(count):
        completed = run(["sbatch", path], cwd=workspace)
        if completed.returncode != 0:
            raise RuntimeError(
                f"sbatch submission {index + 1} of {count} failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        if completed.stderr:
            print(completed.stderr.rstrip("\n"), file=sys.stderr)
        match = _SUBMITTED_JOB.search(completed.stdout)
        job_ids.append(match.group(1) if match is not None else completed.stdout.strip())
    return {"job_ids": job_ids, "script": path, "count": count}


def _probe_httk(kind: str, run: _Runner, settings: Mapping[str, object]) -> tuple[list[str], str] | None:
    """Return the working httk argument vector and its version, if any."""

    resolve = _remote_httk if kind == "ssh-slurm" else _local_httk
    candidates: list[list[str]] = [resolve(["httk"], settings)]
    if _httk_prefix(settings) is None:
        candidates.append(["python3", "-m", "httk.core.cli"])
    for candidate in candidates:
        version = run([*candidate, "--version"])
        if version.returncode != 0:
            continue
        # A recorded version only proves httk-core answered; the workflow group
        # exists exactly when this package is installed beside it.
        workflow = run([*candidate, "workflow", "workspace", "--help"])
        if workflow.returncode == 0:
            return candidate, version.stdout.strip()
    return None


def _install(kind: str, request: Mapping[str, object]) -> None:
    settings = _settings(request)
    run = _runner(kind, settings)
    if kind == "ssh-slurm":
        reachable = run(["true"])
        if reachable.returncode != 0:
            _refusal("install", f"cannot reach {_ssh_destination(settings)}: {reachable.stderr.strip()}")
            return
    found = _probe_httk(kind, run, settings)
    bootstrapped = False
    if found is None and _text(settings, "bootstrap") == "pip":
        # Opt-in only: installing software on someone else's cluster account is
        # never the default behaviour of a configuration operation.
        attempt = run(["python3", "-m", "pip", "install", "--user", "httk-workflow"])
        bootstrapped = attempt.returncode == 0
        if attempt.stderr:
            print(attempt.stderr.rstrip("\n"), file=sys.stderr)
        found = _probe_httk(kind, run, settings)
    if found is None:
        _refusal("install", _MISSING_HTTK)
        return
    command, version = found
    values: dict[str, object] = {
        "installed": True,
        "bootstrapped": bootstrapped,
        "httk_command": command,
        "httk_version": version,
    }
    _result("install", **values)


def _configure(kind: str, request: Mapping[str, object]) -> None:
    if kind != "ssh-slurm":
        _result("configure", configured=True)
        return
    # The command line stores settings only after this operation succeeds, so the
    # pending values are merged in here; otherwise the first configuration of a
    # host could never be verified.
    settings = _settings(request)
    pending = request.get("settings")
    if isinstance(pending, Mapping):
        settings.update({str(key): value for key, value in pending.items()})
    if _text(settings, "host") is None or not _flag(settings, "check_connectivity", default=True):
        _result("configure", configured=True, connectivity="skipped")
        return
    completed = _ssh_run(settings, ["true"])
    if completed.returncode != 0:
        _refusal(
            "configure",
            f"cannot reach {_ssh_destination(settings)}: {completed.stderr.strip() or completed.returncode}; "
            "set check_connectivity=no to configure the remote anyway",
        )
        return
    _result("configure", configured=True, connectivity="ok")


def _transfer(kind: str, operation: str, request: Mapping[str, object]) -> None:
    source, destination = _paths(request)
    if kind == "ssh-slurm":
        settings = _settings(request)
        raw = request.get("directory")
        push = operation == "push"
        if isinstance(raw, bool):
            directory = raw
        else:
            # Only the local side can be inspected; workflow bundles are trees.
            directory = Path(source).expanduser().is_dir() if push else True
        _rsync(
            settings,
            source=source,
            destination=destination,
            directory=directory,
            files=_files(request),
            push=push,
        )
        _result(operation, path=destination)
        return
    resolved = Path(destination).expanduser().resolve()
    _copy(Path(source).expanduser().resolve(), resolved)
    _result(operation, path=str(resolved))


def _invoke(kind: str, operation: str, request: Mapping[str, object]) -> None:
    settings = _settings(request)
    argv = _argv(request, operation)
    cwd = _cwd(request) if operation == "invoke" else None
    completed = _runner(kind, settings)(argv, cwd=cwd)
    _result(operation, returncode=completed.returncode, stdout=completed.stdout, stderr=completed.stderr)


def _start_manager(kind: str, request: Mapping[str, object]) -> None:
    settings = _settings(request)
    raw_argv = _argv(request, "start-manager")
    workspace = _workspace(request, settings, raw_argv)
    workspace_settings = _workspace_settings(kind, settings, workspace)
    argv = _with_workers(_argv(request, "start-manager"), _text(workspace_settings, "manager.workers"))
    if kind == "local":
        count = _count(request)
        manager_argv = _local_httk(argv, settings)
        prelude = _text(workspace_settings, "environment.prelude")
        if prelude:
            launch_argv = ["bash", "-lc", "set -e\n" + prelude + '\nexec "$@"', "bash", *manager_argv]
        else:
            launch_argv = manager_argv
        pids = [
            subprocess.Popen(
                launch_argv,
                cwd=_cwd(request),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            ).pid
            for _ in range(count)
        ]
        # ``pid`` names the first one, so a caller that only ever started a
        # single manager reads the same field it always did.
        _result("start-manager", pid=pids[0], pids=pids, count=count)
        return
    directory = f"{workspace.rstrip('/')}/{BATCH_DIRECTORY}"
    script = _batch_script(
        _remote_httk(argv, settings) if kind == "ssh-slurm" else _local_httk(argv, settings),
        settings=workspace_settings,
        workspace=workspace,
        directory=directory,
    )
    submitted = _submit(
        kind,
        settings,
        script=script,
        workspace=workspace,
        directory=directory,
        count=_count(request),
    )
    _result("start-manager", **submitted)


def main(argv: list[str] | None = None) -> int:
    """Dispatch one adapter request file.

    :param argv: The dispatcher arguments, or the process arguments when omitted.
    :return: Zero after writing an adapter result, or a nonzero refusal status.
    """
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("adapter dispatcher expects one REQUEST.json path", file=sys.stderr)
        return 2
    (request_name,) = arguments
    try:
        request = json.loads(Path(request_name).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"adapter dispatcher: {exc}", file=sys.stderr)
        return 2
    operation = request.get("operation") if isinstance(request, dict) else None
    if not isinstance(operation, str) or not operation:
        print("adapter request must carry a nonempty string operation", file=sys.stderr)
        return 2
    try:
        kind = _adapter_kind(request) or "local"
        if kind not in SUPPORTED_KINDS:
            _refusal(
                operation,
                f"adapter kind {kind!r} is not implemented; "
                f"the packaged operations support {', '.join(SUPPORTED_KINDS)} - refusing",
            )
            return 0
        if operation == "configure":
            _configure(kind, request)
        elif operation == "install":
            _install(kind, request)
        elif operation in {"push", "pull"}:
            _transfer(kind, operation, request)
        elif operation in {"invoke", "status"}:
            _invoke(kind, operation, request)
        elif operation == "start-manager":
            _start_manager(kind, request)
        else:
            raise ValueError(f"unsupported maintained adapter operation: {operation}")
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"adapter {operation}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
