"""Strict runtime policy for the confined workspace daemon."""

import base64
import errno
import hashlib
import json
import math
import os
import re
import secrets
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

MAX_POLICY_BYTES = 64 * 1024
_FORMAT = "httk-workspace-daemon-policy"
_FORMAT_VERSION = 3
_HEX_ID = re.compile(r"[0-9a-f]{32}\Z")
_PROFILE_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_SLURM_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_GRES = re.compile(r"[A-Za-z0-9_.:,=+-]{1,255}\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_BOMS = (b"\x00\x00\xfe\xff", b"\xff\xfe\x00\x00", b"\xef\xbb\xbf", b"\xfe\xff", b"\xff\xfe")
_RESERVED_ANCESTOR_TARGETS = tuple(
    Path(path)
    for path in (
        "/tmp",
        "/workspace",
        "/proc",
        "/dev",
        "/daemon-policy.json",
    )
)
_RESERVED_DESCENDANT_TARGETS = tuple(
    Path(path)
    for path in (
        "/workspace",
        "/proc",
        "/dev",
        "/daemon-policy.json",
        "/tmp/home",
    )
)
_DEVICE_EXCLUSIONS = tuple(
    Path(path)
    for path in (
        "/dev/fd",
        "/dev/pts",
        "/dev/shm",
        "/dev/stdin",
        "/dev/stdout",
        "/dev/stderr",
    )
)
_MPI_CONTROL_DESTINATION = Path("/run/httk-mpi")


_HARD_LIMIT = 2**31 - 1


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


def _reserved_destination(path: Path, *, mpi: bool) -> Path | None:
    for target in _RESERVED_ANCESTOR_TARGETS:
        if _contains(path, target):
            return target
    for target in _RESERVED_DESCENDANT_TARGETS:
        if _contains(target, path):
            return target
    return _MPI_CONTROL_DESTINATION if mpi and _overlap(path, _MPI_CONTROL_DESTINATION) else None


def _environment(value: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, tuple):
        raise ValueError("environment must be a tuple of name/value pairs")
    if len(value) > 128:
        raise ValueError("MPI environment has too many entries")
    result: list[tuple[str, str]] = []
    names: set[str] = set()
    total_bytes = 0
    for item in value:
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError("environment must be a tuple of name/value pairs")
        name, setting = item
        if type(name) is not str or _ENVIRONMENT_NAME.fullmatch(name) is None:
            raise ValueError("invalid MPI environment name")
        if type(setting) is not str or "\0" in setting or len(setting.encode("utf-8")) > 4096:
            raise ValueError("invalid MPI environment value")
        total_bytes += len(name) + len(setting.encode("utf-8"))
        if total_bytes > 64 * 1024:
            raise ValueError("MPI environment is too large")
        if name in names:
            raise ValueError("MPI environment names must be unique")
        if name.startswith(("PMI_", "PMIX_", "SLURM_", "SLURMD_", "HTTK_DAEMON_MPI_")):
            raise ValueError("MPI environment must not override rank or server identity")
        if name.startswith("OMPI_") and not name.startswith("OMPI_MCA_"):
            raise ValueError("MPI environment must not override Open MPI identity")
        if name.startswith("OPAL_") and not name.startswith("OPAL_MCA_"):
            raise ValueError("MPI environment must not override Open MPI identity")
        names.add(name)
        result.append((name, setting))
    return tuple(result)


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


@dataclass(frozen=True, slots=True)
class MPIProfile:
    """Define fixed MPI geometry for one protected profile.

    :param nodes: Number of allocated nodes.
    :param ranks: Total number of MPI ranks.
    :param ntasks_per_node: Optional fixed Slurm placement bound.
    """

    nodes: int
    ranks: int
    ntasks_per_node: int | None = None

    def __post_init__(self) -> None:
        _integer(self.nodes, "MPI nodes", 1, 4096)
        _integer(self.ranks, "MPI ranks", self.nodes, 65_536)
        if self.ntasks_per_node is not None:
            _integer(self.ntasks_per_node, "MPI tasks per node", 1, 65_536)
            if self.nodes * self.ntasks_per_node < self.ranks:
                raise ValueError("MPI tasks per node cannot accommodate all ranks")


@dataclass(frozen=True, slots=True)
class MPISettings:
    """Hold protected MPI launcher and containment settings.

    :param srun: Approved Slurm step launcher.
    :param control_root: Host parent for private allocation control directories.
    :param pmix_roots: Approved parents for per-step PMIx directories.
    :param shm_root: Host shared-memory parent for private per-node directories.
    :param devices: Explicit host device paths exposed to MPI ranks.
    :param environment: Protected environment entries applied to MPI ranks.
    :param max_steps: Maximum unique application requests per allocation.
    :param termination_grace: Deadline for reaping local step launchers, in seconds.
    """

    srun: Path
    control_root: Path
    pmix_roots: tuple[Path, ...] = ()
    shm_root: Path = Path("/dev/shm")
    devices: tuple[Path, ...] = ()
    environment: tuple[tuple[str, str], ...] = ()
    max_steps: int = 128
    termination_grace: float = 10.0

    def __post_init__(self) -> None:
        _path(self.srun, "mpi.srun")
        _path(self.control_root, "mpi.control_root")
        _path(self.shm_root, "mpi.shm_root")
        if not isinstance(self.pmix_roots, tuple) or not isinstance(self.devices, tuple):
            raise ValueError("mpi path collections must be tuples")
        pmix_roots = tuple(_path(path, "mpi.pmix_roots entry") for path in self.pmix_roots)
        devices = tuple(_path(path, "mpi.devices entry") for path in self.devices)
        if len(set(pmix_roots)) != len(pmix_roots) or len(set(devices)) != len(devices):
            raise ValueError("MPI path collections must not contain duplicates")
        if Path("/") in pmix_roots or self.control_root == Path("/") or self.shm_root == Path("/"):
            raise ValueError("MPI roots must not be the filesystem root")
        for device in devices:
            if device == Path("/dev") or not device.is_relative_to("/dev"):
                raise ValueError("MPI devices must be below /dev")
            if any(_overlap(device, excluded) for excluded in _DEVICE_EXCLUSIONS):
                raise ValueError("MPI devices must not replace private device mounts")
        _environment(self.environment)
        _integer(self.max_steps, "mpi.max_steps", 1, 65_536)
        _number(self.termination_grace, "mpi.termination_grace", 0.1, 60.0)


@dataclass(frozen=True, slots=True)
class Profile:
    """Define one bounded Slurm manager profile.

    :param name: Profile name accepted by daemon requests.
    :param cpus: Serial worker CPUs, or CPUs per rank for an MPI profile, or ``None`` to leave it to Slurm's defaults.
    :param memory_mb: Memory capacity in MiB per allocated node, or ``None`` to leave it to Slurm's defaults.
    :param time_minutes: Slurm time limit in minutes, or ``None`` to leave it to Slurm's defaults.
    :param partition: Optional fixed Slurm partition.
    :param account: Optional fixed Slurm account.
    :param mpi: Optional fixed MPI allocation geometry.
    :param workers: Concurrent attempts in the single manager.
    :param prelude: Frozen shell prelude run before the manager.
    :param manager_command: Optional frozen manager executable.
    :param gres: Optional fixed Slurm generic resources.
    :param reservation: Optional fixed Slurm reservation.
    """

    name: str
    cpus: int | None = None
    memory_mb: int | None = None
    time_minutes: int | None = None
    partition: str | None = None
    account: str | None = None
    mpi: MPIProfile | None = None
    workers: int = 1
    prelude: str = ""
    manager_command: str | None = None
    gres: str | None = None
    reservation: str | None = None

    def __post_init__(self) -> None:
        _name(self.name, "profile name", _PROFILE_NAME)
        for number, label in ((self.cpus, "cpus"), (self.memory_mb, "memory_mb"), (self.time_minutes, "time_minutes")):
            if number is not None:
                _integer(number, label, 1, _HARD_LIMIT)
        for field_name, value in (
            ("partition", self.partition),
            ("account", self.account),
            ("reservation", self.reservation),
        ):
            if value is not None:
                _name(value, field_name, _SLURM_NAME)
        if self.gres is not None:
            _name(self.gres, "gres", _GRES)
        if self.mpi is not None and not isinstance(self.mpi, MPIProfile):
            raise ValueError("mpi must be an MPIProfile")
        _integer(self.workers, "workers", 1, 1024)
        if self.mpi is not None and self.workers != 1:
            raise ValueError("MPI profiles require exactly one manager worker")
        if type(self.prelude) is not str or "\0" in self.prelude:
            raise ValueError("prelude must be a string without NUL")
        if self.manager_command is not None and (
            type(self.manager_command) is not str or not self.manager_command.strip() or "\0" in self.manager_command
        ):
            raise ValueError("manager_command must be a nonempty string without NUL")


@dataclass(frozen=True, slots=True)
class Policy:
    """Hold the complete trusted configuration for one daemon enrollment.

    :param workspace: Uploaded workspace root.
    :param workspace_id: Canonical workspace UUID.
    :param enrollment_id: Enrollment identifier as 32 lowercase hexadecimal digits.
    :param exchange: Client exchange directory, a sibling of the workspace in a dedicated parent.
    :param state: Broker state root.
    :param snapshots: Directory of immutable runtime policy snapshots.
    :param bwrap: Approved Bubblewrap executable, run on the host.
    :param python: Approved Python executable visible to payloads.
    :param sbatch: Approved Slurm submission executable.
    :param squeue: Approved Slurm query executable.
    :param scancel: Approved Slurm cancellation executable.
    :param cluster: Fixed Slurm cluster name.
    :param readonly_paths: Runtime roots mounted into payload and MPI rank sandboxes; the broker and the MPI
        allocation service see the whole host read-only.
    :param profiles: Allowed resource profiles.
    :param authorized_keys: Canonical Ed25519 keys authorized to issue requests.
    :param slurm_conf: Optional fixed Slurm configuration path.
    :param max_records: Maximum mailbox records retained per enrollment.
    :param max_submissions: Maximum accepted submissions per enrollment.
    :param poll_seconds: Mailbox polling interval in seconds.
    :param command_timeout: Slurm command timeout in seconds.
    :param max_output_bytes: Maximum captured command output in bytes.
    :param request_max_age: Maximum signed request lifetime in seconds.
    :param mpi: Optional protected MPI launcher and containment settings.
    """

    workspace: Path
    workspace_id: str
    enrollment_id: str
    exchange: Path
    state: Path
    snapshots: Path
    bwrap: Path
    python: Path
    sbatch: Path
    squeue: Path
    scancel: Path
    cluster: str
    readonly_paths: tuple[Path, ...]
    profiles: tuple[Profile, ...]
    authorized_keys: tuple[str, ...] = ()
    slurm_conf: Path | None = None
    max_records: int = 4096
    max_submissions: int = 128
    poll_seconds: float = 1.0
    command_timeout: float = 30.0
    max_output_bytes: int = 65_536
    request_max_age: int = 3600
    mpi: MPISettings | None = None

    def __post_init__(self) -> None:
        mutable = tuple(
            _path(value, name)
            for name, value in (
                ("workspace", self.workspace),
                ("exchange", self.exchange),
                ("state", self.state),
            )
        )
        snapshots = _path(self.snapshots, "snapshots")
        if self.exchange.parent != self.workspace.parent or self.exchange == self.workspace:
            raise ValueError("workspace and exchange must be siblings in a dedicated directory")
        root = self.root
        if root == Path("/"):
            raise ValueError("the daemon parent directory must not be the filesystem root")
        if _overlap(self.state, snapshots):
            raise ValueError("state and snapshots must be disjoint")
        for name, value in (("state", self.state), ("snapshots", snapshots)):
            if _overlap(root, value):
                raise ValueError(f"daemon parent {root} must be disjoint from {name} {value}")

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
        if not isinstance(self.readonly_paths, tuple):
            raise ValueError("readonly_paths must be a tuple")
        readonly = tuple(_path(value, "readonly_paths entry") for value in self.readonly_paths)
        if len(set(readonly)) != len(readonly):
            raise ValueError("approved path lists must not contain duplicates")
        if Path("/") in readonly:
            raise ValueError("the filesystem root cannot be an approved runtime path")
        for runtime in readonly:
            target = _reserved_destination(runtime, mpi=self.mpi is not None)
            if target is not None:
                raise ValueError(
                    "approved runtime paths must not overlap reserved sandbox destinations: "
                    f"{runtime} overlaps the reserved destination {target}"
                )
            if _overlap(root, runtime):
                raise ValueError(f"daemon parent {root} must be disjoint from runtime path {runtime}")
            for name, value in (("state", self.state), ("snapshots", snapshots)):
                if _overlap(runtime, value):
                    raise ValueError(f"runtime path {runtime} must be disjoint from {name} {value}")

        python = commands[1]
        if not _covered(python, readonly):
            raise ValueError("python must be within readonly_paths")
        if self.slurm_conf is not None:
            _path(self.slurm_conf, "slurm_conf")

        if self.mpi is not None:
            if not isinstance(self.mpi, MPISettings):
                raise ValueError("mpi must be MPISettings")
            mpi = self.mpi
            protected_roots = (mpi.control_root, *mpi.pmix_roots, mpi.shm_root)
            for root in protected_roots:
                if any(_overlap(root, item) for item in mutable):
                    raise ValueError("MPI roots must be disjoint from mutable roots")
                if any(_overlap(root, item) for item in readonly):
                    raise ValueError("MPI roots must be disjoint from approved runtime paths")
            for index, left in enumerate(protected_roots):
                for right in protected_roots[index + 1 :]:
                    if _overlap(left, right):
                        raise ValueError("MPI roots must be pairwise disjoint")
            for device in mpi.devices:
                if any(_overlap(device, item) for item in mutable):
                    raise ValueError("MPI devices must be disjoint from mutable roots")

        if not isinstance(self.profiles, tuple) or not all(isinstance(profile, Profile) for profile in self.profiles):
            raise ValueError("profiles must be a tuple of Profile values")
        names = [profile.name for profile in self.profiles]
        if len(set(names)) != len(names):
            raise ValueError("profile names must be unique")
        if any(profile.mpi is not None for profile in self.profiles) and self.mpi is None:
            raise ValueError("MPI profiles require policy MPI settings")
        _authorized_keys(self.authorized_keys)
        _integer(self.max_records, "max_records", 1, 100_000)
        _integer(self.max_submissions, "max_submissions", 1, self.max_records)
        _number(self.poll_seconds, "poll_seconds", 0.05, 60.0)
        _number(self.command_timeout, "command_timeout", 0.1, 600.0)
        _integer(self.max_output_bytes, "max_output_bytes", 1024, 1_048_576)
        _integer(self.request_max_age, "request_max_age", 1, 86_400)

    @property
    def root(self) -> Path:
        """Dedicated parent directory holding exactly the workspace and the exchange."""

        return self.exchange.parent

    @property
    def jobs(self) -> Path:
        """Private directory of Slurm batch job output, which no job sandbox mounts."""

        return self.snapshots / "jobs"

    @property
    def requests(self) -> Path:
        """Signed request mailbox inside the exchange."""

        return self.exchange / "requests"

    @property
    def responses(self) -> Path:
        """Signed response mailbox inside the exchange."""

        return self.exchange / "responses"

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

    def configuration_digest(self, name: str) -> str:
        """Return the digest of one complete execution configuration.

        :param name: Approved configuration name.
        :return: Lowercase SHA-256 hexadecimal digest.
        :raises ValueError: If the configuration is unknown.
        """

        document = policy_document(self)
        mpi = document.get("mpi")
        if isinstance(mpi, dict):
            mpi = {key: value for key, value in mpi.items() if key != "max_steps"}
        execution = {
            "workspace": document["workspace"],
            "workspace_id": document["workspace_id"],
            "bwrap": document["bwrap"],
            "python": document["python"],
            "bootstrap": str(Path(__file__).with_name("_daemon_bootstrap.py")),
            "sbatch": document["sbatch"],
            "squeue": document["squeue"],
            "scancel": document["scancel"],
            "cluster": document["cluster"],
            "slurm_conf": document.get("slurm_conf"),
            "readonly_paths": document["readonly_paths"],
            "configuration_name": name,
            "configuration": _profile_document(self.profile(name)),
            "mpi": mpi,
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


def _profile_document(profile: Profile) -> dict[str, object]:
    result: dict[str, object] = {
        "workers": profile.workers,
        "prelude": profile.prelude,
        "manager_command": profile.manager_command,
    }
    for key, number in (
        ("cpus", profile.cpus),
        ("memory_mb", profile.memory_mb),
        ("time_minutes", profile.time_minutes),
    ):
        if number is not None:
            result[key] = number
    if profile.partition is not None:
        result["partition"] = profile.partition
    if profile.account is not None:
        result["account"] = profile.account
    if profile.gres is not None:
        result["gres"] = profile.gres
    if profile.reservation is not None:
        result["reservation"] = profile.reservation
    if profile.mpi is not None:
        result["mpi"] = {
            "nodes": profile.mpi.nodes,
            "ranks": profile.mpi.ranks,
            "ntasks_per_node": profile.mpi.ntasks_per_node,
        }
    return result


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
        "exchange": str(policy.exchange),
        "state": str(policy.state),
        "snapshots": str(policy.snapshots),
        "bwrap": str(policy.bwrap),
        "python": str(policy.python),
        "sbatch": str(policy.sbatch),
        "squeue": str(policy.squeue),
        "scancel": str(policy.scancel),
        "cluster": policy.cluster,
        "readonly_paths": [str(path) for path in policy.readonly_paths],
        "profiles": {profile.name: _profile_document(profile) for profile in policy.profiles},
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
    if policy.mpi is not None:
        result["mpi"] = {
            "srun": str(policy.mpi.srun),
            "control_root": str(policy.mpi.control_root),
            "pmix_roots": [str(path) for path in policy.mpi.pmix_roots],
            "shm_root": str(policy.mpi.shm_root),
            "devices": [str(path) for path in policy.mpi.devices],
            "environment": dict(policy.mpi.environment),
            "max_steps": policy.mpi.max_steps,
            "termination_grace": policy.mpi.termination_grace,
        }
    return result


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
        "exchange",
        "state",
        "snapshots",
        "bwrap",
        "python",
        "sbatch",
        "squeue",
        "scancel",
        "cluster",
        "readonly_paths",
        "profiles",
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
        "mpi",
    }
    if "authorized_keys" not in value:
        raise ValueError("policy authorized_keys is required and must be a nonempty array")
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
    if not isinstance(value["readonly_paths"], list):
        raise ValueError("readonly_paths must be an array")
    raw_profiles = value["profiles"]
    if not isinstance(raw_profiles, dict):
        raise ValueError("profiles must be an object")
    profiles: list[Profile] = []
    for profile_name, raw in raw_profiles.items():
        if type(profile_name) is not str or not isinstance(raw, dict):
            raise ValueError("invalid profile entry")
        profile_optional = {
            "cpus",
            "memory_mb",
            "time_minutes",
            "partition",
            "account",
            "gres",
            "reservation",
            "mpi",
            "workers",
            "prelude",
            "manager_command",
        }
        if not set(raw) <= profile_optional:
            raise ValueError("profile fields are unknown")
        if "mpi" in raw:
            raw_mpi_profile = raw["mpi"]
            if (
                not isinstance(raw_mpi_profile, dict)
                or not {"nodes", "ranks"} <= set(raw_mpi_profile)
                or not set(raw_mpi_profile) <= {"nodes", "ranks", "ntasks_per_node"}
            ):
                raise ValueError("MPI profile fields are missing or unknown")
            mpi_profile = MPIProfile(
                raw_mpi_profile["nodes"], raw_mpi_profile["ranks"], raw_mpi_profile.get("ntasks_per_node")
            )
        else:
            mpi_profile = None
        profiles.append(
            Profile(
                profile_name,
                raw.get("cpus"),
                raw.get("memory_mb"),
                raw.get("time_minutes"),
                raw.get("partition"),
                raw.get("account"),
                mpi_profile,
                raw.get("workers", 1),
                raw.get("prelude", ""),
                raw.get("manager_command"),
                raw.get("gres"),
                raw.get("reservation"),
            )
        )
    raw_authorized_keys = value["authorized_keys"]
    if not isinstance(raw_authorized_keys, list) or not raw_authorized_keys:
        raise ValueError("policy authorized_keys must be a nonempty array")
    mpi: MPISettings | None
    if "mpi" not in value:
        mpi = None
    else:
        raw_mpi = value["mpi"]
        if not isinstance(raw_mpi, dict):
            raise ValueError("mpi must be an object")
        mpi_required = {"srun", "control_root"}
        mpi_optional = {
            "pmix_roots",
            "shm_root",
            "devices",
            "environment",
            "max_steps",
            "termination_grace",
        }
        if not mpi_required <= set(raw_mpi) or not set(raw_mpi) <= mpi_required | mpi_optional:
            raise ValueError("MPI settings fields are missing or unknown")
        for name in ("pmix_roots", "devices"):
            if name in raw_mpi and not isinstance(raw_mpi[name], list):
                raise ValueError(f"mpi.{name} must be an array")
        raw_environment = raw_mpi.get("environment", {})
        if not isinstance(raw_environment, dict):
            raise ValueError("mpi.environment must be an object")
        mpi = MPISettings(
            srun=_json_path(raw_mpi["srun"], "mpi.srun"),
            control_root=_json_path(raw_mpi["control_root"], "mpi.control_root"),
            pmix_roots=tuple(_json_path(item, "mpi.pmix_roots entry") for item in raw_mpi.get("pmix_roots", [])),
            shm_root=_json_path(raw_mpi.get("shm_root", "/dev/shm"), "mpi.shm_root"),
            devices=tuple(_json_path(item, "mpi.devices entry") for item in raw_mpi.get("devices", [])),
            environment=tuple(raw_environment.items()),
            max_steps=raw_mpi.get("max_steps", 128),
            termination_grace=raw_mpi.get("termination_grace", 10.0),
        )
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
        exchange=_json_path(value["exchange"], "exchange"),
        state=_json_path(value["state"], "state"),
        snapshots=_json_path(value["snapshots"], "snapshots"),
        bwrap=_json_path(value["bwrap"], "bwrap"),
        python=_json_path(value["python"], "python"),
        sbatch=_json_path(value["sbatch"], "sbatch"),
        squeue=_json_path(value["squeue"], "squeue"),
        scancel=_json_path(value["scancel"], "scancel"),
        cluster=value["cluster"],
        readonly_paths=tuple(_json_path(item, "readonly_paths entry") for item in value["readonly_paths"]),
        profiles=tuple(profiles),
        authorized_keys=_authorized_keys(tuple(raw_authorized_keys)),
        slurm_conf=_json_path(value["slurm_conf"], "slurm_conf") if "slurm_conf" in value else None,
        mpi=mpi,
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


def _check_parent_names(policy: Policy, names: list[str]) -> None:
    for name in sorted(names):
        if name not in (policy.workspace.name, policy.exchange.name):
            raise ValueError(
                f"daemon parent {policy.root} must contain only {policy.workspace.name} and "
                f"{policy.exchange.name}; found {name}"
            )


def _remove_probe(name: str, directory: int, probe: int) -> bool:
    """Unlink the probe from ``directory`` only while that name is still the probe's inode."""

    try:
        found = os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return False
    mine = os.fstat(probe)
    if (found.st_dev, found.st_ino) != (mine.st_dev, mine.st_ino):
        return False
    # ponytail: a same-principal swap between this stat and the unlink can drop one foreign name inside the
    # workspace or exchange; the payload already owns both, and nothing is ever moved out of the workspace.
    os.unlink(name, dir_fd=directory)
    return True


def check_layout(policy: Policy, *, probe: bool = True) -> None:
    """Verify the enforced on-disk layout of the workspace and its exchange.

    The dedicated parent must hold exactly the workspace and the exchange on one device.
    With ``probe``, a fresh file created in the exchange must also rename into the
    workspace staging directory, which proves one filesystem and one mount. Nothing is
    ever renamed from the workspace into the exchange.

    :param policy: Validated runtime policy.
    :param probe: Whether to run the rename probe.
    :raises OSError: If a directory cannot be opened without following symlinks.
    :raises ValueError: If the parent holds other entries or the probe fails or is tampered with.
    """

    exdev = "workspace and exchange must be renameable into each other (same filesystem, one mount)"
    staging_path = policy.workspace / ".httk-workspace" / "exchange"
    descriptors: list[int] = []
    try:
        root = _open_directory(policy.root)
        descriptors.append(root)
        _check_parent_names(policy, os.listdir(root))
        workspace = _open_directory(Path(policy.workspace.name), root)
        descriptors.append(workspace)
        exchange = _open_directory(Path(policy.exchange.name), root)
        descriptors.append(exchange)
        if os.fstat(workspace).st_dev != os.fstat(exchange).st_dev:
            raise ValueError(exdev)
        try:
            staging = _open_directory(Path(".httk-workspace", "exchange"), workspace)
        except FileNotFoundError as exc:
            raise ValueError(
                f"workspace staging directory {staging_path} is missing; recreate it with "
                "'httk workspace daemon WORKSPACE --reload'"
            ) from exc
        descriptors.append(staging)
        if not probe:
            return
        name = f".probe-{secrets.token_hex(16)}"
        try:
            probe_fd = os.open(
                name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=exchange
            )
        except OSError as exc:
            raise ValueError(f"cannot create the layout probe in {policy.exchange}: {exc.strerror}") from exc
        descriptors.append(probe_fd)
        # The probe uses the movers' plain same-filesystem rename.
        try:
            os.rename(name, name, src_dir_fd=exchange, dst_dir_fd=staging)
        except OSError as exc:
            _remove_probe(name, exchange, probe_fd)
            if exc.errno == errno.EXDEV:
                raise ValueError(exdev) from exc
            raise ValueError(
                f"cannot rename the layout probe from {policy.exchange} into {staging_path}: {exc.strerror}"
            ) from exc
        try:
            removed = _remove_probe(name, staging, probe_fd)
        except OSError as exc:
            raise ValueError(f"cannot remove the layout probe from {staging_path}: {exc.strerror}") from exc
        if not removed:
            raise ValueError("layout probe was tampered with")
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


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


__all__ = ["MPIProfile", "MPISettings", "Policy", "Profile", "check_layout", "load_policy", "policy_document"]
