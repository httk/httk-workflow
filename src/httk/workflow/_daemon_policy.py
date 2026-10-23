"""Strict runtime policy for the workspace daemon."""

import base64
import hashlib
import json
import math
import os
import re
import shlex
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

MAX_POLICY_BYTES = 64 * 1024
_FORMAT = "httk-workspace-daemon-policy"
_FORMAT_VERSION = 6
_HEX_ID = re.compile(r"[0-9a-f]{32}\Z")
_LAUNCHER_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_SLURM_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")
_SETTING_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
#: The exchange directory below the workspace root (the bootstrap loads this module by path, so no import).
EXCHANGE_DIRECTORY = "exchange"
#: The workspace control directory (the bootstrap loads this module by path, so no import).
WORKSPACE_DIRECTORY = ".httk-workspace"
#: The daemon's private directory below the workspace control directory (the bootstrap loads this module by path).
DAEMON_DIRECTORY = "daemon"
#: The response-signing seed filename (the bootstrap loads this module by path, so no import).
RESPONSE_SEED_NAME = "response.seed"


def _integer(value: object, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer from {minimum} through {maximum}")
    return value


def _number(value: object, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number")
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be finite and from {minimum:g} through {maximum:g}") from exc
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValueError(f"{name} must be finite and from {minimum:g} through {maximum:g}")
    return result


def _name(value: object, name: str, pattern: re.Pattern[str]) -> str:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise ValueError(f"invalid {name}")
    return value


def _path(value: object, name: str) -> Path:
    if not isinstance(value, Path):
        raise ValueError(f"{name} must be a Path")
    text = str(value)
    if not value.is_absolute() or "\0" in text or ".." in value.parts:
        raise ValueError(f"{name} must be an absolute path without '..' or NUL")
    return value


def _authorized_keys(value: object) -> tuple[str, ...]:
    """Return canonical unique Ed25519 public keys from an immutable sequence."""

    if not isinstance(value, tuple):
        raise ValueError("authorized_keys must be a tuple")
    result: list[str] = []
    for key in value:
        if type(key) is not str or not key.startswith("ed25519:"):
            raise ValueError("authorized_keys entries must be canonical Ed25519 public keys")
        encoded = key.removeprefix("ed25519:")
        try:
            raw = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise ValueError("authorized_keys entries must be canonical Ed25519 public keys") from exc
        if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != encoded:
            raise ValueError("authorized_keys entries must be canonical Ed25519 public keys")
        if key in result:
            raise ValueError("authorized_keys entries must be unique")
        result.append(key)
    return tuple(result)


type SettingValue = str | int | float | None

#: The launcher setting that selects the sbatch ``--export`` mode of a daemon manager submission.
SLURM_EXPORT_SETTING = "slurm.export"
#: The launcher settings ``daemon.json`` publishes to clients; everything else stays private to the operator.
PUBLISHED_LAUNCHER_SETTINGS = (
    "manager.workers",
    "slurm.partition",
    "slurm.nodes",
    "slurm.ntasks",
    "slurm.ntasks_per_node",
    "slurm.cpus_per_task",
    "slurm.mem",
    "slurm.gres",
    "slurm.time_limit",
    "slurm.time_limit_max",
)
#: The longest published launcher setting string, in characters.
MAX_PUBLISHED_TEXT = 256
_PUBLISHED_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def valid_published_value(value: object) -> bool:
    """Return whether *value* is a launcher setting value ``daemon.json`` may publish and clients accept.

    :param value: The value to check.
    :return: True for a string of at most :data:`MAX_PUBLISHED_TEXT` characters without control
        characters, an integer that is not a bool, or a finite float.
    """

    if type(value) is str:
        return len(value) <= MAX_PUBLISHED_TEXT and _PUBLISHED_CONTROL.search(value) is None
    return type(value) is int or (type(value) is float and math.isfinite(value))


#: The only accepted ``slurm.export`` values, case-sensitive and never stripped or coerced. ``ALL`` is
#: excluded on purpose: it would leak the broker's sandbox environment (HOME=/tmp/home and the rest) into
#: the submitted manager.
SLURM_EXPORT_VALUES = ("NONE", "NIL")
#: The export mode used when ``slurm.export`` is unset.
DEFAULT_SLURM_EXPORT = "NONE"


def validate_slurm_export(value: object) -> str:
    """Return a valid ``slurm.export`` value, refusing anything but ``NONE`` or ``NIL``.

    :param value: The value to check, used exactly as given: no stripping, no case folding.
    :return: The value unchanged.
    :raises ValueError: If the value is not exactly ``NONE`` or ``NIL``.
    """

    if type(value) is not str or value not in SLURM_EXPORT_VALUES:
        raise ValueError(
            f"launcher setting {SLURM_EXPORT_SETTING} must be exactly {' or '.join(SLURM_EXPORT_VALUES)} "
            f"(case-sensitive, no surrounding whitespace): {value!r}"
        )
    return value


#: The launcher setting holding free-form ``sbatch`` options, a string of shell words.
SBATCH_OPTIONS_SETTING = "slurm.sbatch_options"
#: Options httk sets itself (job bookkeeping and the script contract), or that make ``sbatch`` exit without
#: submitting a job, refused in ``slurm.sbatch_options``.
_MANAGED_SBATCH_OPTIONS = (
    "job-name",
    "chdir",
    "output",
    "error",
    "input",
    "export",
    "export-file",
    "parsable",
    "clusters",
    "requeue",
    "no-requeue",
    "wrap",
    "array",
    "uid",
    "gid",
    "get-user-env",
    "test-only",
    "wait",
    "help",
    "usage",
    "version",
)
#: Options with a keyed launcher setting, refused in ``slurm.sbatch_options`` in favour of that setting.
_KEYED_SBATCH_OPTIONS = {
    "account": "slurm.account",
    "partition": "slurm.partition",
    "time": "slurm.time_limit",
    "nodes": "slurm.nodes",
    "ntasks": "slurm.ntasks",
    "cpus-per-task": "slurm.cpus_per_task",
    "ntasks-per-node": "slurm.ntasks_per_node",
    "mem": "slurm.mem",
    "gres": "slurm.gres",
    "reservation": "slurm.reservation",
}
_SBATCH_OPTION = re.compile(r"--([A-Za-z][A-Za-z0-9-]*)(?:=(.*))?")


def sbatch_option_lines(value: object) -> tuple[str, ...]:
    """Return the ``#SBATCH`` line bodies of a ``slurm.sbatch_options`` value, refusing what httk cannot pass.

    The value is a string of shell words, each ``--name`` or ``--name=value``. A name httk manages or that has a
    keyed ``slurm.*`` setting is refused, and so is any abbreviation of one, because ``sbatch`` accepts
    unambiguous prefixes of long options.

    :param value: The setting value; ``None`` or a blank string yields no lines.
    :return: One ``--name`` or ``--name=<shell-quoted value>`` per word, in the given order.
    :raises ValueError: If the value is not a string, does not split into shell words, or a word is malformed,
        repeated, or names a refused option.
    """

    if value is None:
        return ()
    setting = SBATCH_OPTIONS_SETTING
    if type(value) is not str:
        raise ValueError(f"launcher setting {setting} must be a string of --name=value words: {value!r}")
    if _PUBLISHED_CONTROL.search(value):
        raise ValueError(f"launcher setting {setting} must not contain control characters")
    if "\\" in value:
        raise ValueError(f"launcher setting {setting} must not contain backslashes")
    try:
        words = shlex.split(value)
    except ValueError as exc:
        raise ValueError(f"launcher setting {setting} is not valid shell words: {exc}") from None
    lines: list[str] = []
    names: set[str] = set()
    for word in words:
        match = _SBATCH_OPTION.fullmatch(word)
        if match is None:
            raise ValueError(f"launcher setting {setting} word {word!r} is not a long option: use --name=value")
        name, option_value = match.group(1), match.group(2)
        if option_value == "":
            raise ValueError(f"launcher setting {setting} option --{name} has an empty value")
        if name in names:
            raise ValueError(f"launcher setting {setting} repeats option --{name}")
        names.add(name)
        for denied in (*_KEYED_SBATCH_OPTIONS, *_MANAGED_SBATCH_OPTIONS):
            if denied.startswith(name):
                option = f"--{name}" if name == denied else f"--{name} (--{denied})"
                reason = (
                    f"use {_KEYED_SBATCH_OPTIONS[denied]}" if denied in _KEYED_SBATCH_OPTIONS else "reserved by httk"
                )
                raise ValueError(f"launcher setting {setting} must not set {option}: {reason}")
        lines.append(f"--{name}" if option_value is None else f"--{name}={shlex.quote(option_value)}")
    return tuple(lines)


def _setting_value(key: str, value: object) -> SettingValue:
    if value is None or (type(value) is str and "\0" not in value) or type(value) is int:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    raise ValueError(f"launcher setting {key} must be a JSON scalar without NUL")


@dataclass(frozen=True, slots=True)
class ApprovedLauncher:
    """Hold the frozen content of one approved global ``slurm`` launcher.

    :param name: Launcher name, which daemon requests use as their configuration name.
    :param settings: The bundle's ``settings`` as sorted key/value pairs, exactly as approved.
    :param digest: SHA-256 of the canonical ``launcher.json`` content of the bundle.
    """

    name: str
    settings: tuple[tuple[str, SettingValue], ...]
    digest: str

    def __post_init__(self) -> None:
        _name(self.name, "launcher name", _LAUNCHER_NAME)
        _name(self.digest, "launcher digest", _SHA256)
        if not isinstance(self.settings, tuple):
            raise ValueError("launcher settings must be a tuple of key/value pairs")
        keys: list[str] = []
        for item in self.settings:
            if not isinstance(item, tuple) or len(item) != 2:
                raise ValueError("launcher settings must be a tuple of key/value pairs")
            key, value = item
            _name(key, "launcher setting name", _SETTING_KEY)
            _setting_value(key, value)
            # Freeze/decode guard: a frozen or snapshot-decoded slurm.export must be NONE or NIL (a null
            # reads as unset and resolves to the default). This is the broker/bootstrap decode entry.
            if key == SLURM_EXPORT_SETTING and value is not None:
                validate_slurm_export(value)
            keys.append(key)
        if keys != sorted(set(keys)):
            raise ValueError("launcher settings must be sorted with unique keys")


@dataclass(frozen=True, slots=True)
class Policy:
    """Hold the complete trusted configuration for one daemon enrollment.

    :param workspace: Uploaded workspace root; its ``exchange`` directory is the one client-writable mount.
    :param workspace_id: Canonical workspace UUID.
    :param enrollment_id: Enrollment identifier as 32 lowercase hexadecimal digits.
    :param bwrap: Approved Bubblewrap executable for the broker sandbox, run on the host.
    :param python: Approved Python executable that runs the broker and the submitted managers.
    :param sbatch: Approved Slurm submission executable.
    :param squeue: Approved Slurm query executable.
    :param scancel: Approved Slurm cancellation executable.
    :param cluster: Fixed Slurm cluster name.
    :param launchers: Approved global ``slurm`` launchers, frozen at approval.
    :param authorized_keys: Canonical Ed25519 keys authorized to issue requests.
    :param slurm_conf: Optional fixed Slurm configuration path.
    :param max_records: Maximum durable request records the enrollment's ledger retains at once; retiring
        settled history (``retention.daemon_history_days``) frees them.
    :param max_submissions: Maximum manager starts the enrollment's ledger retains at once, freed the same way.
    :param poll_seconds: Mailbox polling interval in seconds.
    :param command_timeout: Slurm command timeout in seconds.
    :param max_output_bytes: Maximum captured command output in bytes.
    :param request_max_age: Maximum signed request lifetime in seconds.
    :param sacct: Optional Slurm accounting executable, used only to report how manager jobs ended.
    """

    workspace: Path
    workspace_id: str
    enrollment_id: str
    bwrap: Path
    python: Path
    sbatch: Path
    squeue: Path
    scancel: Path
    cluster: str
    launchers: tuple[ApprovedLauncher, ...]
    authorized_keys: tuple[str, ...] = ()
    slurm_conf: Path | None = None
    max_records: int = 4096
    max_submissions: int = 128
    poll_seconds: float = 1.0
    command_timeout: float = 30.0
    max_output_bytes: int = 65_536
    request_max_age: int = 3600
    sacct: Path | None = None

    def __post_init__(self) -> None:
        _path(self.workspace, "workspace")
        if self.workspace == Path("/"):
            raise ValueError("the workspace must not be the filesystem root")

        if type(self.workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        _name(self.enrollment_id, "enrollment_id", _HEX_ID)
        _name(self.cluster, "cluster", _SLURM_NAME)
        for name, value in (
            ("bwrap", self.bwrap),
            ("python", self.python),
            ("sbatch", self.sbatch),
            ("squeue", self.squeue),
            ("scancel", self.scancel),
        ):
            _path(value, name)
        if self.slurm_conf is not None:
            _path(self.slurm_conf, "slurm_conf")
        if self.sacct is not None:
            _path(self.sacct, "sacct")

        if not isinstance(self.launchers, tuple) or not all(
            isinstance(launcher, ApprovedLauncher) for launcher in self.launchers
        ):
            raise ValueError("launchers must be a tuple of ApprovedLauncher values")
        names = [launcher.name for launcher in self.launchers]
        if len(set(names)) != len(names):
            raise ValueError("launcher names must be unique")
        _authorized_keys(self.authorized_keys)
        _integer(self.max_records, "max_records", 1, 100_000)
        _integer(self.max_submissions, "max_submissions", 1, self.max_records)
        _number(self.poll_seconds, "poll_seconds", 0.05, 60.0)
        _number(self.command_timeout, "command_timeout", 0.1, 600.0)
        _integer(self.max_output_bytes, "max_output_bytes", 1024, 1_048_576)
        _integer(self.request_max_age, "request_max_age", 1, 86_400)

    @property
    def exchange(self) -> Path:
        """The workspace's exchange directory, the only client-writable path the broker reaches."""

        return self.workspace / EXCHANGE_DIRECTORY

    @property
    def daemon(self) -> Path:
        """The daemon's private workspace directory."""

        return self.workspace / WORKSPACE_DIRECTORY / DAEMON_DIRECTORY

    @property
    def broker(self) -> Path:
        """The broker-writable directory containing its ledger."""

        return self.daemon / "broker"

    @property
    def snapshots(self) -> Path:
        """Directory of immutable runtime policy snapshots."""

        return self.daemon / "snapshots"

    @property
    def response_seed(self) -> Path:
        """The daemon's private response-signing seed."""

        return self.daemon / RESPONSE_SEED_NAME

    @property
    def requests(self) -> Path:
        """Signed request mailbox inside the exchange."""

        return self.exchange / "requests"

    @property
    def responses(self) -> Path:
        """Signed response mailbox inside the exchange."""

        return self.exchange / "responses"

    def launcher(self, name: str) -> ApprovedLauncher:
        """Return the named approved launcher.

        :param name: Launcher name.
        :return: Matching approved launcher.
        :raises ValueError: If the launcher is not approved.
        """

        for launcher in self.launchers:
            if launcher.name == name:
                return launcher
        raise ValueError(f"unknown daemon launcher: {name!r}")

    def configuration_digest(self, name: str) -> str:
        """Return the digest of one complete execution configuration.

        :param name: Approved launcher name.
        :return: Lowercase SHA-256 hexadecimal digest.
        :raises ValueError: If the launcher is not approved.
        """

        # sacct only reports how managers ended, and the other Slurm clients only query or cancel.
        execution = {
            "launcher_digest": self.launcher(name).digest,
            "workspace": str(self.workspace),
            "workspace_id": self.workspace_id,
            "python": str(self.python),
            "sbatch": str(self.sbatch),
            "cluster": self.cluster,
            "slurm_conf": None if self.slurm_conf is None else str(self.slurm_conf),
            "configuration_name": name,
        }
        canonical = json.dumps(execution, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(canonical).hexdigest()


def _object_without_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_constant(_: str) -> object:
    raise ValueError("nonfinite JSON value")


def _parse_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nonfinite JSON value")
    return result


def _open_nofollow(path: Path) -> int:
    if not path.is_absolute() or ".." in path.parts or "\0" in str(path):
        raise ValueError("policy path must be absolute without '..' or NUL")
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:-1]:
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
    try:
        result = os.open(
            path.name,
            os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=descriptor,
        )
    except BaseException:
        os.close(descriptor)
        raise
    try:
        os.close(descriptor)
    except BaseException:
        os.close(result)
        raise
    return result


def _read_policy_bytes(path: Path) -> bytes:
    descriptor = _open_nofollow(path)
    try:
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("policy source must be a regular file")
        if information.st_mode & 0o002:
            raise ValueError("policy source must not be world-writable")
        if information.st_size > MAX_POLICY_BYTES:
            raise ValueError("policy document is too large")
        data = bytearray()
        while len(data) <= MAX_POLICY_BYTES:
            chunk = os.read(descriptor, MAX_POLICY_BYTES + 1 - len(data))
            if not chunk:
                return bytes(data)
            data.extend(chunk)
        raise ValueError("policy document is too large")
    finally:
        os.close(descriptor)


def _json_path(value: object, name: str) -> Path:
    if type(value) is not str:
        raise ValueError(f"{name} must be a string")
    return _path(Path(value), name)


def policy_document(policy: Policy) -> dict[str, object]:
    """Return the complete canonicalizable runtime policy document.

    :param policy: Validated runtime policy.
    :return: JSON-compatible policy object with explicit defaults.
    """

    result: dict[str, object] = {
        "format": _FORMAT,
        "format_version": _FORMAT_VERSION,
        "workspace": str(policy.workspace),
        "workspace_id": policy.workspace_id,
        "enrollment_id": policy.enrollment_id,
        "bwrap": str(policy.bwrap),
        "python": str(policy.python),
        "sbatch": str(policy.sbatch),
        "squeue": str(policy.squeue),
        "scancel": str(policy.scancel),
        "cluster": policy.cluster,
        "launchers": {
            launcher.name: {"settings": dict(launcher.settings), "digest": launcher.digest}
            for launcher in policy.launchers
        },
        "authorized_keys": list(policy.authorized_keys),
        "max_records": policy.max_records,
        "max_submissions": policy.max_submissions,
        "poll_seconds": policy.poll_seconds,
        "command_timeout": policy.command_timeout,
        "max_output_bytes": policy.max_output_bytes,
        "request_max_age": policy.request_max_age,
    }
    if policy.slurm_conf is not None:
        result["slurm_conf"] = str(policy.slurm_conf)
    if policy.sacct is not None:
        result["sacct"] = str(policy.sacct)
    return result


def _decode_launcher(name: object, raw: object) -> ApprovedLauncher:
    if type(name) is not str or not isinstance(raw, dict) or set(raw) != {"settings", "digest"}:
        raise ValueError("launcher entries must hold exactly settings and digest")
    settings = raw["settings"]
    if not isinstance(settings, dict):
        raise ValueError("launcher settings must be an object")
    return ApprovedLauncher(name, tuple(sorted(settings.items())), raw["digest"])


def _decode_policy(data: bytes) -> Policy:
    if type(data) is not bytes or len(data) > MAX_POLICY_BYTES:
        raise ValueError("invalid policy document")
    if any(data.startswith(bom) for bom in _BOMS):
        raise ValueError("policy must be UTF-8 without a BOM")
    try:
        value = json.loads(
            data.decode("utf-8", errors="strict"),
            object_pairs_hook=_object_without_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_float,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise ValueError("invalid policy document") from exc
    if not isinstance(value, dict):
        raise ValueError("policy must be a JSON object")
    required = {
        "format",
        "format_version",
        "workspace",
        "workspace_id",
        "enrollment_id",
        "bwrap",
        "python",
        "sbatch",
        "squeue",
        "scancel",
        "cluster",
        "launchers",
        "authorized_keys",
    }
    optional = {
        "slurm_conf",
        "max_records",
        "max_submissions",
        "poll_seconds",
        "command_timeout",
        "max_output_bytes",
        "request_max_age",
        "sacct",
    }
    if "authorized_keys" not in value:
        raise ValueError("policy authorized_keys is required and must be a nonempty array")
    if (
        value.get("format") != _FORMAT
        or type(value.get("format_version")) is not int
        or value["format_version"] != _FORMAT_VERSION
    ):
        raise ValueError("unsupported policy format or version")
    if not required <= set(value) or not set(value) <= required | optional:
        raise ValueError("policy fields are missing or unknown")
    for name in ("workspace_id", "enrollment_id", "cluster"):
        if type(value[name]) is not str:
            raise ValueError(f"{name} must be a string")
    raw_launchers = value["launchers"]
    if not isinstance(raw_launchers, dict):
        raise ValueError("launchers must be an object")
    raw_authorized_keys = value["authorized_keys"]
    if not isinstance(raw_authorized_keys, list) or not raw_authorized_keys:
        raise ValueError("policy authorized_keys must be a nonempty array")
    kwargs = {
        name: value[name]
        for name in (
            "max_records",
            "max_submissions",
            "poll_seconds",
            "command_timeout",
            "max_output_bytes",
            "request_max_age",
        )
        if name in value
    }
    return Policy(
        workspace=_json_path(value["workspace"], "workspace"),
        workspace_id=value["workspace_id"],
        enrollment_id=value["enrollment_id"],
        bwrap=_json_path(value["bwrap"], "bwrap"),
        python=_json_path(value["python"], "python"),
        sbatch=_json_path(value["sbatch"], "sbatch"),
        squeue=_json_path(value["squeue"], "squeue"),
        scancel=_json_path(value["scancel"], "scancel"),
        cluster=value["cluster"],
        launchers=tuple(_decode_launcher(name, raw) for name, raw in raw_launchers.items()),
        authorized_keys=_authorized_keys(tuple(raw_authorized_keys)),
        slurm_conf=_json_path(value["slurm_conf"], "slurm_conf") if "slurm_conf" in value else None,
        sacct=_json_path(value["sacct"], "sacct") if "sacct" in value else None,
        **kwargs,
    )


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW


def _open_directory(path: Path, base: int | None = None) -> int:
    """Open a directory component by component without following any symlink.

    :param path: Absolute path, or a path relative to ``base``.
    :param base: Optional directory descriptor that anchors a relative path.
    :return: An owned directory descriptor.
    """

    descriptor = os.open(path.anchor or "/", _DIRECTORY_FLAGS) if base is None else os.dup(base)
    try:
        for component in path.parts[1:] if base is None else path.parts:
            next_descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            previous, descriptor = descriptor, next_descriptor
            os.close(previous)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _load_policy_with_bytes(path: Path) -> tuple[Policy, bytes]:
    data = _read_policy_bytes(path)
    return _decode_policy(data), data


def load_policy(path: Path) -> Policy:
    """Read and validate one bounded daemon policy.

    :param path: Absolute path to the policy JSON file.
    :return: Immutable validated policy.
    :raises OSError: If the policy cannot be opened or read.
    :raises ValueError: If the policy is malformed or violates the daemon contract.
    """

    if not isinstance(path, Path):
        raise ValueError("policy path must be a Path")
    return _load_policy_with_bytes(path)[0]


__all__ = [
    "MAX_PUBLISHED_TEXT",
    "PUBLISHED_LAUNCHER_SETTINGS",
    "ApprovedLauncher",
    "Policy",
    "load_policy",
    "policy_document",
    "valid_published_value",
]
