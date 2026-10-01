"""Strict operator policy for the confined workspace daemon."""

import json
import math
import os
import re
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

MAX_POLICY_BYTES = 64 * 1024
_FORMAT = "httk-workspace-daemon-policy"
_FORMAT_VERSION = 1
_HEX_ID = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_SLURM_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")
_RESERVED_ANCESTOR_TARGETS = tuple(
    Path(path)
    for path in (
        "/tmp",
        "/workspace",
        "/requests",
        "/responses",
        "/control",
        "/proc",
        "/dev",
        "/daemon-policy.json",
    )
)
_RESERVED_DESCENDANT_TARGETS = tuple(
    Path(path)
    for path in (
        "/workspace",
        "/requests",
        "/responses",
        "/control",
        "/proc",
        "/dev",
        "/daemon-policy.json",
        "/tmp/home",
    )
)


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


def _contains(root: Path, candidate: Path) -> bool:
    return candidate == root or candidate.is_relative_to(root)


def _overlap(left: Path, right: Path) -> bool:
    return _contains(left, right) or _contains(right, left)


def _covered(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(_contains(root, path) for root in roots)


def _overlaps_reserved_destination(path: Path) -> bool:
    return any(_contains(path, target) for target in _RESERVED_ANCESTOR_TARGETS) or any(
        _contains(target, path) for target in _RESERVED_DESCENDANT_TARGETS
    )


@dataclass(frozen=True, slots=True)
class Profile:
    """Define one bounded serial Slurm manager profile.

    :param name: Profile name accepted by daemon requests.
    :param cpus: CPU capacity exposed to the single manager worker.
    :param memory_mb: Memory capacity in MiB.
    :param time_minutes: Slurm time limit in minutes.
    :param partition: Optional fixed Slurm partition.
    :param account: Optional fixed Slurm account.
    """

    name: str
    cpus: int
    memory_mb: int
    time_minutes: int
    partition: str | None = None
    account: str | None = None

    def __post_init__(self) -> None:
        _name(self.name, "profile name", _PROFILE_NAME)
        _integer(self.cpus, "cpus", 1, 1024)
        _integer(self.memory_mb, "memory_mb", 1, 1_048_576)
        _integer(self.time_minutes, "time_minutes", 1, 10_080)
        for field_name, value in (("partition", self.partition), ("account", self.account)):
            if value is not None:
                _name(value, field_name, _SLURM_NAME)


@dataclass(frozen=True, slots=True)
class Policy:
    """Hold the complete trusted configuration for one daemon enrollment.

    :param workspace: Uploaded workspace root.
    :param workspace_id: Canonical workspace UUID.
    :param enrollment_id: Enrollment identifier as 32 lowercase hexadecimal digits.
    :param requests: Broker request mailbox root.
    :param responses: Broker response mailbox root.
    :param state: Broker state root.
    :param bwrap: Approved Bubblewrap executable.
    :param python: Approved Python executable visible to payloads.
    :param sbatch: Approved Slurm submission executable.
    :param squeue: Approved Slurm query executable.
    :param scancel: Approved Slurm cancellation executable.
    :param cluster: Fixed Slurm cluster name.
    :param readonly_paths: Runtime roots mounted into both sandbox roles.
    :param broker_paths: Privileged runtime roots mounted only into the broker.
    :param profiles: Allowed resource profiles.
    :param slurm_conf: Optional fixed Slurm configuration path.
    :param max_records: Maximum mailbox records retained per enrollment.
    :param max_submissions: Maximum accepted submissions per enrollment.
    :param poll_seconds: Mailbox polling interval in seconds.
    :param command_timeout: Slurm command timeout in seconds.
    :param max_output_bytes: Maximum captured command output in bytes.
    """

    workspace: Path
    workspace_id: str
    enrollment_id: str
    requests: Path
    responses: Path
    state: Path
    bwrap: Path
    python: Path
    sbatch: Path
    squeue: Path
    scancel: Path
    cluster: str
    readonly_paths: tuple[Path, ...]
    broker_paths: tuple[Path, ...]
    profiles: tuple[Profile, ...]
    slurm_conf: Path | None = None
    max_records: int = 4096
    max_submissions: int = 128
    poll_seconds: float = 1.0
    command_timeout: float = 30.0
    max_output_bytes: int = 65_536

    def __post_init__(self) -> None:
        mutable = tuple(
            _path(value, name)
            for name, value in (
                ("workspace", self.workspace),
                ("requests", self.requests),
                ("responses", self.responses),
                ("state", self.state),
            )
        )
        for index, left in enumerate(mutable):
            for right in mutable[index + 1 :]:
                if _overlap(left, right):
                    raise ValueError("workspace, requests, responses, and state must be pairwise disjoint")

        if type(self.workspace_id) is not str:
            raise ValueError("invalid workspace_id")
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise ValueError("invalid workspace_id") from exc
        _name(self.enrollment_id, "enrollment_id", _HEX_ID)
        _name(self.cluster, "cluster", _SLURM_NAME)

        commands = tuple(
            _path(value, name)
            for name, value in (
                ("bwrap", self.bwrap),
                ("python", self.python),
                ("sbatch", self.sbatch),
                ("squeue", self.squeue),
                ("scancel", self.scancel),
            )
        )
        if not isinstance(self.readonly_paths, tuple) or not isinstance(self.broker_paths, tuple):
            raise ValueError("readonly_paths and broker_paths must be tuples")
        readonly = tuple(_path(value, "readonly_paths entry") for value in self.readonly_paths)
        broker = tuple(_path(value, "broker_paths entry") for value in self.broker_paths)
        if len(set(readonly)) != len(readonly) or len(set(broker)) != len(broker):
            raise ValueError("approved path lists must not contain duplicates")
        if Path("/") in {*readonly, *broker}:
            raise ValueError("the filesystem root cannot be an approved runtime path")
        if any(_overlaps_reserved_destination(root) for root in (*readonly, *broker)):
            raise ValueError("approved runtime paths must not overlap reserved sandbox destinations")
        if any(_overlap(readonly_root, broker_root) for readonly_root in readonly for broker_root in broker):
            raise ValueError("readonly_paths and broker_paths must be pairwise disjoint")
        for root in (*readonly, *broker):
            if any(_overlap(root, item) for item in mutable):
                raise ValueError("runtime paths must be disjoint from mutable roots")

        bwrap, python, sbatch, squeue, scancel = commands
        if not _covered(python, readonly):
            raise ValueError("python must be within readonly_paths")
        if not _covered(bwrap, (*readonly, *broker)):
            raise ValueError("bwrap must be within an approved runtime path")
        if any(not _covered(command, (*readonly, *broker)) for command in (sbatch, squeue, scancel)):
            raise ValueError("Slurm commands must be within an approved runtime path")
        if self.slurm_conf is not None:
            slurm_conf = _path(self.slurm_conf, "slurm_conf")
            if not _covered(slurm_conf, broker):
                raise ValueError("slurm_conf must be within broker_paths")

        if not isinstance(self.profiles, tuple) or not all(isinstance(profile, Profile) for profile in self.profiles):
            raise ValueError("profiles must be a tuple of Profile values")
        names = [profile.name for profile in self.profiles]
        if len(set(names)) != len(names):
            raise ValueError("profile names must be unique")
        _integer(self.max_records, "max_records", 1, 100_000)
        _integer(self.max_submissions, "max_submissions", 1, self.max_records)
        _number(self.poll_seconds, "poll_seconds", 0.05, 60.0)
        _number(self.command_timeout, "command_timeout", 0.1, 600.0)
        _integer(self.max_output_bytes, "max_output_bytes", 1024, 1_048_576)

    def profile(self, name: str) -> Profile:
        """Return the named profile.

        :param name: Profile name.
        :return: Matching profile.
        :raises ValueError: If the profile is unknown.
        """

        for profile in self.profiles:
            if profile.name == name:
                return profile
        raise ValueError(f"unknown daemon profile: {name!r}")


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
        if information.st_mode & 0o022:
            raise ValueError("policy source must not be writable by group or other")
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
        "requests",
        "responses",
        "state",
        "bwrap",
        "python",
        "sbatch",
        "squeue",
        "scancel",
        "cluster",
        "readonly_paths",
        "broker_paths",
        "profiles",
    }
    optional = {
        "slurm_conf",
        "max_records",
        "max_submissions",
        "poll_seconds",
        "command_timeout",
        "max_output_bytes",
    }
    if not required <= set(value) or not set(value) <= required | optional:
        raise ValueError("policy fields are missing or unknown")
    if (
        value["format"] != _FORMAT
        or type(value["format_version"]) is not int
        or value["format_version"] != _FORMAT_VERSION
    ):
        raise ValueError("unsupported policy format or version")
    for name in ("workspace_id", "enrollment_id", "cluster"):
        if type(value[name]) is not str:
            raise ValueError(f"{name} must be a string")
    for name in ("readonly_paths", "broker_paths"):
        if not isinstance(value[name], list):
            raise ValueError(f"{name} must be an array")
    raw_profiles = value["profiles"]
    if not isinstance(raw_profiles, dict):
        raise ValueError("profiles must be an object")
    profiles: list[Profile] = []
    for profile_name, raw in raw_profiles.items():
        if type(profile_name) is not str or not isinstance(raw, dict):
            raise ValueError("invalid profile entry")
        profile_required = {"cpus", "memory_mb", "time_minutes"}
        profile_optional = {"partition", "account"}
        if not profile_required <= set(raw) or not set(raw) <= profile_required | profile_optional:
            raise ValueError("profile fields are missing or unknown")
        profiles.append(
            Profile(
                profile_name,
                raw["cpus"],
                raw["memory_mb"],
                raw["time_minutes"],
                raw.get("partition"),
                raw.get("account"),
            )
        )
    kwargs = {
        name: value[name]
        for name in ("max_records", "max_submissions", "poll_seconds", "command_timeout", "max_output_bytes")
        if name in value
    }
    return Policy(
        workspace=_json_path(value["workspace"], "workspace"),
        workspace_id=value["workspace_id"],
        enrollment_id=value["enrollment_id"],
        requests=_json_path(value["requests"], "requests"),
        responses=_json_path(value["responses"], "responses"),
        state=_json_path(value["state"], "state"),
        bwrap=_json_path(value["bwrap"], "bwrap"),
        python=_json_path(value["python"], "python"),
        sbatch=_json_path(value["sbatch"], "sbatch"),
        squeue=_json_path(value["squeue"], "squeue"),
        scancel=_json_path(value["scancel"], "scancel"),
        cluster=value["cluster"],
        readonly_paths=tuple(_json_path(item, "readonly_paths entry") for item in value["readonly_paths"]),
        broker_paths=tuple(_json_path(item, "broker_paths entry") for item in value["broker_paths"]),
        profiles=tuple(profiles),
        slurm_conf=_json_path(value["slurm_conf"], "slurm_conf") if "slurm_conf" in value else None,
        **kwargs,
    )


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


__all__ = ["Policy", "Profile", "load_policy"]
