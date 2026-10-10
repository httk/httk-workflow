"""Packaged implementations used by the maintained adapter dispatcher.

The single ``adapter`` executable in each ``adapter_templates`` bundle executes
this module with one versioned JSON request file. The operation to run is read
from that request's ``operation`` member; the implementation that runs is
selected by the ``kind`` recorded in the bundle's ``remote.json``:

``local``
    Files are copied in this filesystem and commands run in this process tree.
``ssh``
    Files move as one uncompressed ``tar`` stream over a single ``ssh``
    connection per push or pull (the remote needs a POSIX ``tar``), and commands
    run on the configured host.
``mount``
    Files are copied through a locally mounted view of the remote filesystem
    (sshfs, NFS, any shared mount) and commands run on the remote through a
    configurable executor command (an argv prefix such as
    ``ssh -o BatchMode=yes host`` or ``/abs/path/bin/hpc run``). A ``files`` batch
    is copied entry by entry, recursing into a directory entry and dereferencing a
    symlinked file, unlike the ``ssh`` tar stream, which keeps it a symlink.
    The transfer driver sends a ``files`` batch on every push and pull.

Any other kind is refused rather than silently executed in the wrong place.

Every subprocess started here is an argument vector; no shell is ever handed an
interpolated string. ``ssh`` and the ``mount`` executor are the two unavoidable
exceptions, because each always concatenates its command words and lets a shell
on the far side parse the result. All remote command strings are therefore built by
``_shell_command``, which quotes element-wise; the executor is split once with
``shlex.split`` and never interpolated. Nothing else in this module may compose a
command string from request or settings values.
"""

import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import IO, Any, Protocol

SUPPORTED_KINDS = ("local", "ssh", "mount")

#: The bundle metadata file.
METADATA_FILE = "remote.json"

#: The result format one adapter operation prints. The name is protocol and
#: keeps its historical spelling: an adapter written against an earlier release
#: prints exactly this, and every side already agrees on it.
RESULT_FORMAT = "httk-computer-result"

_CONNECT_TIMEOUT = 20
_FALSE_SETTINGS = frozenset({"0", "false", "no", "off"})
_WHITESPACE = re.compile(r"\s")
# OpenSSH's own denial lists the methods it tried; this keeps a remote command's
# "mkdir: ...: Permission denied" or tar's "...: Permission denied" unhinted.
_SSH_DENIED = re.compile(r"Permission denied \([a-z,-]+\)")

_MISSING_HTTK = (
    "httk-workflow is not available on the target; log in there and make sure "
    "httk is installed and reachable from a non-interactive shell (for example "
    "with 'pipx install httk-workflow'), or configure the remote with "
    "httk_command=COMMAND, or set a prelude=... that loads the environment "
    "(e.g. 'module load ...; source ~/venv/bin/activate') before each command"
)


class _Runner(Protocol):
    """Run one argument vector where the adapter's work belongs."""

    def __call__(self, argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]": ...


def _result(operation: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "format": RESULT_FORMAT,
                "format_version": 2,
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
                "format_version": 2,
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
    keeps request and settings values out of that parser. Scheduler launchers
    are a separate protocol and are not composed here.
    """

    quoted = " ".join(shlex.quote(item) for item in argv)
    if cwd is None:
        return quoted
    return f"cd {shlex.quote(cwd)} && {quoted}"


def _settings(request: Mapping[str, object]) -> dict[str, Any]:
    value = request.get("remote_settings")
    settings = dict(value) if isinstance(value, Mapping) else {}
    pending = request.get("settings")
    if isinstance(pending, Mapping):
        settings.update({str(key): value for key, value in pending.items()})
    return settings


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
            # A repeated copy of one bundle is a no-op: both carry the same bundle.json.
            source_manifest = source / "bundle.json"
            destination_manifest = destination / "bundle.json"
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


def _terminal_present() -> bool:
    """Return whether this process may prompt on its controlling terminal.

    ``ssh`` reads passwords and keyboard-interactive answers from ``/dev/tty``.
    A terminal on stdin is not enough: a backgrounded process that reads
    ``/dev/tty`` is stopped by ``SIGTTIN``, so the process must also be in the
    terminal's foreground process group. Anything else -- an unattended
    manager, a cron job, a backgrounded shell job -- must never block on a
    prompt and keeps batch mode.

    :return: True only for a foreground process attached to a terminal.
    """

    try:
        if sys.stdin is None:  # started with fd 0 closed
            return False
        return sys.stdin.isatty() and os.tcgetpgrp(sys.stdin.fileno()) == os.getpgrp()
    except (OSError, ValueError):
        return False


def _interactive_auth(settings: Mapping[str, object]) -> str:
    """Return the ``interactive_auth`` mode: ``no``, ``yes`` or ``per-call``.

    :param settings: The persisted remote settings and their credentials.
    :return: The normalised mode; ``no`` when the setting is absent.
    :raises ValueError: If the setting names no known mode.
    """

    value = _text(settings, "interactive_auth")
    if value is None or value.lower() in _FALSE_SETTINGS:
        return "no"
    if value.lower() in {"yes", "true", "1", "on"}:
        return "yes"
    if value.lower() == "per-call":
        return "per-call"
    raise ValueError(f"remote setting interactive_auth must be no, yes or per-call: {value!r}")


def _control_directory() -> Path:
    """Return the private directory that holds httk's ssh control sockets.

    The fallback is ``/tmp`` itself, not ``tempfile.gettempdir()``: OpenSSH
    refuses a control socket path of 108 bytes or more, and a long ``TMPDIR``
    (job scratch, containers) would exceed it once ``%C`` adds 40 characters.

    :return: ``$XDG_RUNTIME_DIR/httk``, or ``/tmp/httk-ssh-UID``.
    :raises ValueError: If the directory exists but is not one this user owns.
    """

    runtime = os.environ.get("XDG_RUNTIME_DIR")
    directory = Path(runtime) / "httk" if runtime else Path("/tmp") / f"httk-ssh-{os.getuid()}"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    status = os.lstat(directory)
    if not stat.S_ISDIR(status.st_mode) or status.st_uid != os.getuid():
        raise ValueError(f"ssh control directory is not a directory owned by this user: {directory}")
    return directory


def _ssh_transport(settings: Mapping[str, object]) -> list[str]:
    """Return the ``ssh`` argument vector that carries no remote command.

    By default ssh runs with ``BatchMode=yes``: key-based login only, never a
    prompt. The ``interactive_auth`` setting lifts that only while a foreground
    terminal is attached (:func:`_terminal_present`), so ssh can ask for a
    password or second factor on ``/dev/tty``; httk never sees the answer.
    ``yes`` also shares one connection for ten minutes through a control socket,
    so the following ssh calls do not prompt again; ``per-call`` lets
    every call prompt, for sites that forbid connection sharing.

    :param settings: The persisted remote settings and their credentials.
    :return: The ssh argument vector without destination or command.
    :raises ValueError: If ``port`` or ``interactive_auth`` is malformed.
    """

    argv = ["ssh"]
    port = _text(settings, "port")
    if port is not None:
        if not port.isdigit():
            raise ValueError(f"remote setting port must be numeric: {port!r}")
        argv += ["-p", port]
    mode = _interactive_auth(settings)
    if mode == "no" or not _terminal_present():
        return [*argv, "-o", "BatchMode=yes", "-o", f"ConnectTimeout={_CONNECT_TIMEOUT}"]
    # OpenSSH counts prompts per method, and a site may enter keyboard-interactive twice
    # (password, then one-time code); 2 allows that and bounds a typo to one retry.
    argv += ["-o", f"ConnectTimeout={_CONNECT_TIMEOUT}", "-o", "NumberOfPasswordPrompts=2"]
    if mode == "yes":
        argv += [
            "-o",
            "ControlMaster=auto",
            "-o",
            "ControlPersist=600",
            "-o",
            f"ControlPath={_control_directory()}/%C",
        ]
    return argv


def _login_hint(settings: Mapping[str, object], completed: "subprocess.CompletedProcess[str]") -> str:
    """Return a hint to append when ssh was denied authentication, else ``""``.

    Only ssh's own failure status 255 counts: a remote command that itself ran
    ssh forwards the same denial text with its own status.

    :param settings: The persisted remote settings and their credentials.
    :param completed: The finished ssh call.
    :return: The hint, starting with ``"; "``, or an empty string.
    """

    if completed.returncode != 255 or not _SSH_DENIED.search(completed.stderr):
        return ""
    if _interactive_auth(settings) == "no":
        return (
            "; ssh ran without a terminal prompt (BatchMode): if this site asks for a password or a "
            "second factor, keep a shared ssh connection open or set interactive_auth=yes "
            "(see the remotes guide)"
        )
    if _terminal_present():
        return ""  # ssh prompted; the answer itself was refused
    return "; interactive_auth is set but no terminal is attached: run this command from a foreground terminal"


def _ssh_destination(settings: Mapping[str, object]) -> str:
    host = _text(settings, "host")
    if host is None:
        raise ValueError("the ssh adapter needs a remote setting host=HOSTNAME")
    if _WHITESPACE.search(host):
        raise ValueError(f"remote setting host must not contain whitespace: {host!r}")
    username = _text(settings, "username")
    if username is None:
        return host
    if _WHITESPACE.search(username):
        raise ValueError(f"remote setting username must not contain whitespace: {username!r}")
    return f"{username}@{host}"


def _remote_prelude(settings: Mapping[str, object]) -> str:
    """Return the shell prelude to run before every remote command, or ``""``.

    ``ssh`` runs its command in a *non-interactive* shell on the far side, so the
    environment a site keeps in interactive rc files (``module load`` lines, a
    virtualenv activation) is not applied and ``httk`` is often not even on
    ``PATH``. The optional ``prelude`` remote setting carries exactly that setup
    for httk's ssh commands only, so it need not go in ``~/.bashrc`` where it
    would also affect every other tool that logs in over ssh.
    """

    prelude = _text(settings, "prelude")
    return prelude.strip() if prelude else ""


def _remote_command(settings: Mapping[str, object], argv: Sequence[str], *, cwd: str | None = None) -> str:
    """Build the single shell command string a remote transport parses.

    Both the ``ssh`` transport and the ``mount`` executor concatenate their words
    and hand the result to a shell on the far side, so ``ssh`` and ``mount`` share
    this one composition: the argument vector quoted element-wise by
    :func:`_shell_command`, prefixed by the optional ``prelude`` run under
    ``set -e`` (a failing setup line -- a bad module, a missing venv -- then
    aborts before the command instead of running it in a half-configured shell).

    :param settings: The persisted remote settings and their credentials.
    :param argv: The command to run on the far side.
    :param cwd: The remote working directory, or the login default when omitted.
    :return: One command string for a single remote shell.
    """

    command = _shell_command(_remote_httk(argv, settings), cwd=cwd)
    prelude = _remote_prelude(settings)
    if prelude:
        command = "set -e\n" + prelude + "\n" + command
    return command


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
        _remote_command(settings, argv, cwd=cwd),
    ]
    return subprocess.run(
        command,
        text=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _exec_command(settings: Mapping[str, object]) -> list[str]:
    """Split the ``mount`` executor prefix into an argument vector.

    The value is parsed once, here, exactly like ``httk_command``; the adapter
    appends the single remote command string built by :func:`_remote_command`
    and never interpolates the executor into a command line.

    :param settings: The persisted remote settings and their credentials.
    :return: The executor argument vector.
    :raises ValueError: If the executor is missing or empty.
    """

    command = _text(settings, "exec_command")
    if command is None:
        raise ValueError("the mount adapter needs an exec_command=... that runs one shell command line on the remote")
    prefix = shlex.split(command)
    if not prefix:
        raise ValueError("remote setting exec_command must not be empty")
    return prefix


def _absolute_root(settings: Mapping[str, object], key: str) -> Path:
    """Return an absolute, lexically normalised ``mount`` path setting.

    :param settings: The persisted remote settings and their credentials.
    :param key: The setting name to read (``mount_root`` or ``remote_root``).
    :return: The normalised absolute path.
    :raises ValueError: If the setting is missing or is not absolute.
    """

    value = _text(settings, key)
    if value is None or not Path(value).is_absolute():
        raise ValueError(f"the mount adapter needs an absolute remote setting {key}=PATH")
    return Path(os.path.normpath(value))


def _mounted_path(settings: Mapping[str, object], remote_path: str) -> Path:
    """Map a remote-spelled path onto the local mount, refusing an escape.

    A path outside the mounted tree is a refusal, never a local copy: the mount is
    required to actually exist (so a push cannot silently create a local tree when
    the filesystem is not mounted), the remote-spelled path is normalised (so
    ``..`` cannot climb out), required to lie inside ``remote_root``, and
    rewritten to the same location under ``mount_root``. The mapped path is
    finally resolved with ``realpath`` and required to stay under the mount, so a
    symlink inside the tree cannot lead a copy out onto the local filesystem.

    :param settings: The persisted remote settings and their credentials.
    :param remote_path: The path as the remote machine spells it.
    :return: The local path on the mount that names the same tree.
    :raises ValueError: If the mount is absent or the path escapes the mount.
    """

    mount_root = _absolute_root(settings, "mount_root")
    if not mount_root.is_dir():
        raise ValueError(f"mount_root is not an existing directory (is the filesystem mounted?): {mount_root}")
    remote_root = _absolute_root(settings, "remote_root")
    resolved = Path(os.path.normpath(remote_path))
    if not resolved.is_absolute() or not resolved.is_relative_to(remote_root):
        raise ValueError(f"remote path is outside the mounted tree (remote_root={remote_root}): {remote_path}")
    mapped = mount_root / resolved.relative_to(remote_root)
    if not Path(os.path.realpath(mapped)).is_relative_to(Path(os.path.realpath(mount_root))):
        raise ValueError(f"remote path leaves the mount through a symlink: {remote_path}")
    return mapped


def _mount_run(
    settings: Mapping[str, object],
    argv: Sequence[str],
    *,
    cwd: str | None = None,
) -> "subprocess.CompletedProcess[str]":
    command = [*_exec_command(settings), _remote_command(settings, argv, cwd=cwd)]
    return subprocess.run(
        command,
        text=True,
        capture_output=True,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _mount_transfer(source: Path, destination: Path, *, files: list[str] | None) -> None:
    """Copy one tree, or one explicit batch of files, across the mount.

    :param source: The tree or file to copy from (already mapped to the mount).
    :param destination: The tree or file to copy to (already mapped to the mount).
    :param files: The optional workspace-relative batch, or ``None`` for the tree.
    """

    if files is None:
        _copy(source, destination)
        return
    for relative in files:
        _copy(source / relative, destination / relative)


def _runner(kind: str, settings: Mapping[str, object]) -> _Runner:
    if kind == "ssh":

        def remote(argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]":
            return _ssh_run(settings, argv, cwd=cwd)

        return remote

    if kind == "mount":

        def mounted(argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]":
            return _mount_run(settings, argv, cwd=cwd)

        return mounted

    def local(argv: Sequence[str], *, cwd: str | None = None) -> "subprocess.CompletedProcess[str]":
        return subprocess.run(
            _local_httk(argv, settings),
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )

    return local


def _ssh_tar_argv(settings: Mapping[str, object], command: str) -> list[str]:
    """Return the ssh call that runs one ``tar`` command line on the remote.

    The ``prelude`` is deliberately left out: ``tar`` is a system binary, and a
    prelude that prints would corrupt the archive on the connection's stdout.

    :param settings: The persisted remote settings and their credentials.
    :param command: The remote command line, built by :func:`_shell_command`.
    :return: The local ssh argument vector.
    """

    return [*_ssh_transport(settings), "--", _ssh_destination(settings), command]


def _ssh_tar_status(
    settings: Mapping[str, object], operation: str, argv: list[str], returncode: int, errors: IO[bytes]
) -> None:
    """Raise when the ssh tar call failed; otherwise pass its stderr on.

    :param settings: The persisted remote settings and their credentials.
    :param operation: ``push`` or ``pull``.
    :param argv: The ssh argument vector that ran.
    :param returncode: Its exit status.
    :param errors: The temporary file that holds its stderr.
    :raises RuntimeError: If ssh or the remote tar exited nonzero.
    """

    errors.seek(0)
    stderr = errors.read().decode("utf-8", errors="replace").strip()
    if returncode != 0:
        completed = subprocess.CompletedProcess(argv, returncode, "", stderr)
        raise RuntimeError(f"ssh tar {operation} failed ({returncode}): {stderr}{_login_hint(settings, completed)}")
    if stderr:
        print(stderr, file=sys.stderr)


def _tar_push(
    settings: Mapping[str, object], *, source: str, destination: str, directory: bool, files: list[str] | None
) -> None:
    """Stream one tree, one file, or one explicit batch of paths over a single ssh connection.

    The archive is written straight into ssh's stdin and never staged on disk;
    ssh's stderr goes to a temporary file so a chatty remote cannot stall the writer.

    :param settings: The persisted remote settings and their credentials.
    :param source: The local tree or file.
    :param destination: The remote directory, or the remote file path for a single file.
    :param directory: Whether *source* is a tree rather than one file.
    :param files: The optional batch of paths relative to *source*, each sent recursively.
    :raises RuntimeError: If ssh or the remote tar failed.
    """

    local = Path(source).expanduser()
    if files is not None:
        target, members = destination, [(local / item, item) for item in files]
    elif directory:
        target, members = destination, [(local, ".")]
    else:
        target, members = str(PurePosixPath(destination).parent), [(local, PurePosixPath(destination).name)]
    command = f"{_shell_command(['mkdir', '-p', target])} && {_shell_command(['tar', '-xpf', '-', '-C', target])}"
    argv = _ssh_tar_argv(settings, command)
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=errors)
        assert process.stdin is not None
        try:
            with process.stdin as pipe, tarfile.open(fileobj=pipe, mode="w|") as archive:
                for path, name in members:
                    archive.add(path, arcname=name)
        except BrokenPipeError:
            pass  # the far side stopped reading; its exit status and stderr say why
        except BaseException:
            process.kill()
            process.wait()
            raise
        _ssh_tar_status(settings, "push", argv, process.wait(), errors)


def _extract(stream: IO[bytes], target: Path, *, rename: tuple[str, str] | None = None) -> None:
    """Extract one tar stream under *target*, member by member.

    tarfile's extraction filters are the trust boundary. Every member except a
    symlink goes through the ``data`` filter, which refuses names and hard-link
    targets outside *target* (``..``, through an already-extracted symlink) and
    device nodes and FIFOs, clears setuid/setgid and group/other write bits, and
    leaves directory modes at their defaults. Symlinks go through the ``tar``
    filter, which still refuses a name outside *target* but keeps an absolute
    link target, which job trees legitimately carry. A leading ``/`` is
    stripped, so such a member lands inside *target*.

    :param stream: The binary tar stream.
    :param target: The local directory to extract into.
    :param rename: ``(member, name)`` for a single file: the only member accepted, a regular file written as *name*.
    :raises tarfile.TarError: If the stream is malformed or a member is refused.
    """

    def checked(member: tarfile.TarInfo, path: str) -> tarfile.TarInfo:
        if rename is not None:
            # The landing directory holds the user's other files, so only one regular file may arrive there.
            if member.name != rename[0] or not member.isreg():
                raise tarfile.FilterError(f"unexpected member in a single-file pull: {member.name!r}")
            member = member.replace(name=rename[1], deep=False)
        # A hard link is checked by data_filter only: tar_filter would let one alias a file outside *target*.
        return tarfile.tar_filter(member, path) if member.issym() else tarfile.data_filter(member, path)

    # ponytail: tarfile reads a stream cut exactly at a member boundary as a complete archive; only a
    # remote tar that exits 0 after such a cut slips through, as every dropped connection fails ssh's status.
    with tarfile.open(fileobj=stream, mode="r|") as archive:
        archive.extractall(target, filter=checked)


def _tar_pull(
    settings: Mapping[str, object], *, source: str, destination: str, directory: bool, files: list[str] | None
) -> None:
    """Receive one tree, one file, or one explicit batch of paths over a single ssh connection.

    :param settings: The persisted remote settings and their credentials.
    :param source: The remote directory, or the remote file for a single file.
    :param destination: The local directory, or the local file path for a single file.
    :param directory: Whether *source* is a tree rather than one file.
    :param files: The optional batch of paths relative to *source*.
    :raises RuntimeError: If ssh or the remote tar failed, or the stream was malformed.
    """

    local = Path(destination).expanduser()
    rename: tuple[str, str] | None = None
    if files is not None:
        remote, names, target = source, files, local
    elif directory:
        remote, names, target = source, ["."], local
    else:
        remote, names, target = str(PurePosixPath(source).parent), [PurePosixPath(source).name], local.parent
        rename = (names[0], local.name)
    argv = _ssh_tar_argv(settings, _shell_command(["tar", "-cf", "-", "-C", remote, "--", *names]))
    target.mkdir(parents=True, exist_ok=True)
    broken: Exception | None = None
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=errors)
        assert process.stdout is not None
        try:
            with process.stdout as stream:
                _extract(stream, target, rename=rename)
        except (tarfile.TarError, EOFError) as exc:
            broken = exc
        except BaseException:
            process.kill()
            process.wait()
            raise
        returncode = process.wait()
        # A refused member closed the stream early, so ssh's own failure would only hide the reason.
        if not isinstance(broken, tarfile.FilterError):
            _ssh_tar_status(settings, "pull", argv, returncode, errors)
    if broken is not None:
        raise RuntimeError(f"ssh tar pull failed: unusable tar stream: {broken}")


def _probe_httk(kind: str, run: _Runner, settings: Mapping[str, object]) -> tuple[list[str], str] | None:
    """Return the working httk argument vector and its version, if any."""

    resolve = _local_httk if kind == "local" else _remote_httk
    candidates: list[list[str]] = [resolve(["httk"], settings)]
    if _httk_prefix(settings) is None:
        candidates.append(["python3", "-m", "httk.core.cli"])
    for candidate in candidates:
        version = run([*candidate, "--version"])
        if version.returncode != 0:
            continue
        # A recorded version only proves httk-core answered; the workflow group
        # exists exactly when this package is installed beside it.
        workflow = run([*candidate, "workspace", "--help"])
        if workflow.returncode == 0:
            return candidate, version.stdout.strip()
    return None


def _install(kind: str, request: Mapping[str, object]) -> None:
    """Verify the target has a working httk; the protocol keeps the name ``install``."""

    settings = _settings(request)
    run = _runner(kind, settings)
    if kind == "ssh":
        reachable = run(["true"])
        if reachable.returncode != 0:
            _refusal(
                "install",
                f"cannot reach {_ssh_destination(settings)}: {reachable.stderr.strip()}"
                f"{_login_hint(settings, reachable)}",
            )
            return
    elif kind == "mount":
        reachable = run(["true"])
        if reachable.returncode != 0:
            _refusal(
                "install",
                f"cannot reach the mount remote through exec_command ({_exec_command(settings)[0]!r}): "
                f"{reachable.stderr.strip() or reachable.returncode}",
            )
            return
    found = _probe_httk(kind, run, settings)
    if found is None:
        _refusal("install", _MISSING_HTTK)
        return
    command, version = found
    _result("install", installed=True, httk_command=command, httk_version=version)


def _configure(kind: str, request: Mapping[str, object]) -> None:
    if kind == "local":
        _result("configure", configured=True)
        return
    settings = _settings(request)
    if kind == "mount":
        _configure_mount(settings)
        return
    if _text(settings, "host") is None or not _flag(settings, "check_connectivity", default=True):
        _result("configure", configured=True, connectivity="skipped")
        return
    completed = _ssh_run(settings, ["true"])
    if completed.returncode != 0:
        _refusal(
            "configure",
            f"cannot reach {_ssh_destination(settings)}: {completed.stderr.strip() or completed.returncode}"
            f"{_login_hint(settings, completed)}; set check_connectivity=no to configure the remote anyway",
        )
        return
    _result("configure", configured=True, connectivity="ok")


def _configure_mount(settings: Mapping[str, object]) -> None:
    """Validate a ``mount`` remote and, unless disabled, prove it is usable.

    :param settings: The stored settings merged with the pending ones.
    """

    for key in ("mount_root", "remote_root"):
        value = _text(settings, key)
        if value is None:
            _refusal("configure", f"the mount adapter needs a remote setting {key}=PATH")
            return
        if not Path(value).is_absolute():
            _refusal("configure", f"remote setting {key} must be an absolute path: {value!r}")
            return
    try:
        program = _exec_command(settings)[0]
    except ValueError as exc:
        _refusal("configure", str(exc))
        return
    if shutil.which(program) is None and not Path(program).is_file():
        _refusal("configure", f"exec_command program is neither on PATH nor an existing file: {program!r}")
        return
    connectivity = "skipped"
    if _flag(settings, "check_connectivity", default=True):
        completed = _mount_run(settings, ["true"])
        if completed.returncode != 0:
            _refusal(
                "configure",
                f"cannot reach the mount remote through exec_command ({program!r}): "
                f"{completed.stderr.strip() or completed.returncode}; "
                "set check_connectivity=no to configure the remote anyway",
            )
            return
        connectivity = "ok"
    mount = "skipped"
    if _flag(settings, "check_mount", default=True):
        mount_root = _absolute_root(settings, "mount_root")
        if not mount_root.is_dir():
            _refusal(
                "configure",
                f"mount_root is not an existing directory: {mount_root}; "
                "set check_mount=no to configure the remote before the filesystem is mounted",
            )
            return
        mount = "ok"
    _result("configure", configured=True, connectivity=connectivity, mount=mount)


def _transfer(kind: str, operation: str, request: Mapping[str, object]) -> None:
    source, destination = _paths(request)
    push = operation == "push"
    if kind == "ssh":
        settings = _settings(request)
        raw = request.get("directory")
        if isinstance(raw, bool):
            directory = raw
        else:
            # Only the local side can be inspected; workflow bundles are trees.
            directory = Path(source).expanduser().is_dir() if push else True
        files = _files(request)
        (_tar_push if push else _tar_pull)(
            settings, source=source, destination=destination, directory=directory or files is not None, files=files
        )
        _result(operation, path=destination)
        return
    if kind == "mount":
        # The remote-spelled path is the destination on push, the source on pull;
        # it is mapped onto the mount before any byte is copied, so a path outside
        # the mounted tree is refused rather than copied somewhere local.
        settings = _settings(request)
        files = _files(request)
        if push:
            _mount_transfer(Path(source).expanduser(), _mounted_path(settings, destination), files=files)
        else:
            _mount_transfer(_mounted_path(settings, source), Path(destination).expanduser(), files=files)
        _result(operation, path=destination)
        return
    resolved = Path(destination).expanduser().resolve()
    _mount_transfer(Path(source).expanduser().resolve(), resolved, files=_files(request))
    _result(operation, path=str(resolved))


def _invoke(kind: str, operation: str, request: Mapping[str, object]) -> None:
    settings = _settings(request)
    argv = _argv(request, operation)
    cwd = _cwd(request) if operation == "invoke" else None
    completed = _runner(kind, settings)(argv, cwd=cwd)
    stderr = completed.stderr
    hint = _login_hint(settings, completed) if kind == "ssh" else ""
    if hint:
        stderr = f"{stderr.rstrip()}{hint}\n"
    _result(operation, returncode=completed.returncode, stdout=completed.stdout, stderr=stderr)


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
        else:
            raise ValueError(f"unsupported maintained adapter operation: {operation}")
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"adapter {operation}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
