"""Execution workspace creation, submission, marker discovery, and transitions."""

import bisect
import errno
import logging
import os
import re
import shutil
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Self

from httk.core.digests import sha256_file, tree_digest

from ._kernel import OWNED
from ._state import UNOWNED_STATES
from ._util import (
    fsync_directory,
    read_json,
    utc_now,
    visibility_attempts,
    write_json_atomic,
)
from .errors import (
    FormatError,
    SealedError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
    WorkspaceCorruptionError,
    WorkspaceUnavailableError,
)
from .journal import JournalWriter, read_record
from .models import (
    ACTIVE_STATE_KINDS,
    CARRIED_PROVENANCE_MEMBERS,
    CORE_PROFILE,
    CORE_STATE_KINDS,
    EXCHANGE_EXTENSION,
    JOBS_DIRECTORY,
    STATE_KINDS,
    SUPPORTED_EXTENSIONS,
    WORKSPACE_DIRECTORY,
    JobDefinition,
    Marker,
    WorkspacePolicy,
    check_job_placement,
    is_payload_private,
    marker_basename,
    normalize_placement,
    parse_job_key,
    placement_text,
    validate_runner_path,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .fsck import FsckReport
    from .gc import GcReport

_LOGGER = logging.getLogger(__name__)
RUNNER_TREE_ENTRY = "run"
#: The ``format.json`` ``layout`` of a workspace with ``jobs/<state>/`` job directories and owners (plan §3).
LAYOUT = "jobs-v3"
WORKFLOWS_DIRECTORY = "workflows"


def _validate_setting_key(key: str) -> str:
    """Return one application-setting key, refusing an ill-formed one.

    Application settings are a flat, dotted-name map (``vasp.command``,
    ``vasp.pseudo_library``) of small values a runner resolves at execution
    time — never nested objects, which belong in a job's ``parameters`` instead.
    """

    if not isinstance(key, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", key) is None:
        raise ValueError(f"application setting name must be a nonempty dotted identifier: {key!r}")
    return key


def _validate_setting_value(key: str, value: object) -> object:
    """Return one application-setting value, refusing a non-scalar.

    Boolean values are refused even though ``bool`` is a subclass of ``int``;
    application settings are textual or numeric configuration values.
    """

    if isinstance(value, str) and "\0" in value:
        raise ValueError(f"application setting {key!r} must not contain NUL characters")
    if value is None or (isinstance(value, (str, int, float)) and not isinstance(value, bool)):
        return value
    raise ValueError(f"application setting {key!r} must be a JSON scalar, not {type(value).__name__}")


def _setting_variable_name(key: str) -> str:
    return "HTTK_" + key.upper().replace(".", "_")


def _section(document: Mapping[str, object], name: str) -> object:
    """Return a required ``format.json`` section, refusing a document without it."""

    if name not in document:
        raise FormatError(f"workspace format.json has no {name!r} section")
    return document[name]


def _refuse_format(root: object, version: object, profile: object) -> FormatError:
    return FormatError(
        f"workspace at {root} uses httk-workflow-filesystem format version {version} (profile {profile!r}); "
        f"this version of httk-workflow reads only format 3 ({CORE_PROFILE}). "
        "There is no migration: remove old per-user state with `httk system reset` "
        "and recreate the workspace with `httk workspace init`."
    )


def _extensions(document: Mapping[str, object]) -> frozenset[str]:
    """Return the enabled extensions of a ``format.json`` document, refusing unsupported ones."""

    raw = document.get("extensions", [])
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise FormatError("workspace extensions must be an array of strings")
    extensions = frozenset(raw)
    unsupported = extensions - SUPPORTED_EXTENSIONS
    if unsupported:
        raise UnsupportedExtensionError(f"unsupported enabled extensions: {', '.join(sorted(unsupported))}")
    return extensions


def _validate_settings(raw: object) -> dict[str, object]:
    """Return a validated flat map of application settings."""

    if not isinstance(raw, Mapping):
        raise FormatError("workspace settings must be a JSON object")
    return {_validate_setting_key(key): _validate_setting_value(str(key), value) for key, value in raw.items()}


def _validate_workflow_prelude_id(workflow_id: str) -> str:
    """Return one workflow-prelude key, refusing an ill-formed one.

    The key mirrors a ``[workflow].name``: a nonempty string with no whitespace,
    because a prelude map is keyed by the workflow it initializes the
    environment for.
    """

    if not isinstance(workflow_id, str) or not workflow_id or any(character.isspace() for character in workflow_id):
        raise ValueError(f"workflow prelude id must be a nonempty whitespace-free string: {workflow_id!r}")
    return workflow_id


def _validate_workflow_prelude_value(workflow_id: str, value: object) -> str:
    """Return one workflow-prelude value, refusing a non-string or NUL."""

    if not isinstance(value, str):
        raise ValueError(f"workflow prelude {workflow_id!r} must be a string, not {type(value).__name__}")
    if "\0" in value:
        raise ValueError(f"workflow prelude {workflow_id!r} must not contain NUL characters")
    return value


def _validate_workflow_preludes(raw: object) -> dict[str, str]:
    """Return a validated map of workflow id to shell prelude text."""

    if not isinstance(raw, Mapping):
        raise FormatError("workspace workflow_preludes must be a JSON object")
    return {
        _validate_workflow_prelude_id(str(key)): _validate_workflow_prelude_value(str(key), value)
        for key, value in raw.items()
    }


# Anything below state/ that cannot possibly be a marker basename is ignored
# silently: NFS silly-renames, editor droppings, and other foreign files are
# not protocol entries and must never stop a scan or reach quarantine.
_MARKER_SHAPE_PATTERN = re.compile(r"\.p[0-9]{3}\.g[0-9a-z]+\.")

#: How many directory entries a streaming scheduling scan visits before it
#: takes a heartbeat opportunity. A walk of one enormous flat directory keeps a
#: manager's lease alive from inside the walk exactly as a walk across many
#: kinds does, rather than only between passes.
DISCOVERY_HEARTBEAT_STRIDE = 512

#: How many active-job locations one workspace instance caches before it evicts
#: the least recently touched. Terminal jobs are never indexed, so this only
#: bounds the working set: finished history never makes the index proportional
#: to the workspace's whole history.
DEFAULT_MARKER_INDEX_CAPACITY = 1 << 20


def _scandir_sorted(directory: Path) -> list[os.DirEntry[str]]:
    """Return one directory's entries by name, or nothing if it is not there.

    A directory that a concurrent transition renamed or removed while the walk
    was reaching it reads as empty rather than as an error: an incremental scan
    tolerates the churn of the very managers it runs beside, consistent with
    how a vanished marker becomes a silent miss rather than a fault.
    """

    try:
        with os.scandir(directory) as scan:
            entries = list(scan)
    except (FileNotFoundError, NotADirectoryError):
        return []
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR):
            return []
        raise
    entries.sort(key=lambda item: item.name)
    return entries


def _safe_is_dir(entry: os.DirEntry[str]) -> bool:
    """Report whether *entry* is a directory, tolerating its disappearance."""

    try:
        return entry.is_dir()
    except OSError:
        return False


def _cursor_relation(position: tuple[str, ...], after: tuple[str, ...] | None) -> str:
    """Classify a subtree at *position* against a resume cursor *after*.

    ``"descend"`` walks the whole subtree, ``"resume"`` walks it carrying the
    cursor because the resume point is inside it, and ``"skip"`` prunes it
    without a single ``scandir`` because every position it could contain was
    already consumed. This is what keeps resuming an interrupted walk cheap.
    """

    if after is None:
        return "descend"
    head = after[: len(position)]
    if position < head:
        return "skip"
    if position > head:
        return "descend"
    return "resume"


@dataclass(frozen=True)
class MarkerFault:
    """Describe one state entry shaped like a marker that cannot be interpreted.

    :param path: Identify the unusable state entry.
    :param reason: Explain why the entry could not be interpreted.
    """

    path: Path
    reason: str


@dataclass(eq=False)
class _WriterScope:
    """One installed journal writer scope; *writer* opens on first use when the scope owns it."""

    writer: JournalWriter | None
    owned: bool


@dataclass(frozen=True)
class _IndexEntry:
    """Where one job's marker was last observed, as a cache of the state tree.

    The three members are exactly what rebuilds the marker path, and every one
    of them is derived: nothing here is authoritative, and a hit is confirmed
    against the filesystem before it is used.
    """

    kind: str
    placement: PurePosixPath
    basename: str


def _marker_shaped(name: str) -> bool:
    """Report whether *name* is shaped enough like a marker to be validated."""

    return not name.startswith(".") and _MARKER_SHAPE_PATTERN.search(name) is not None


def _runner_content_digest(source: Path) -> tuple[bool, str, Callable[[str], bool] | None]:
    """Classify a runner source and return the digest it is published under.

    This is the one digest rule of the workspace runner store: a runner file is
    hashed by its bytes, and a directory by its source tree with the build
    artifacts its manifest declares left out, because those are never
    published. :meth:`Workspace.publish_runner`, the runner staging of
    :meth:`httk.workflow.Attempt.call`, and the manager that publishes a staged
    runner at commit all hash through it, so a staged runner verifies against
    exactly the digest its publication will report.

    :param source: The runner file or directory.
    :return: Whether the source is a directory, its digest, and the build-artifact
        excluder of a directory whose manifest declares a build (otherwise ``None``).
    :raises httk.workflow.errors.FormatError: If the source is a symlink, neither a
        regular file nor a directory, or a tree with an invalid build manifest,
        a symlink, or a special file.
    """

    if source.is_symlink():
        raise FormatError(f"a published runner must be a regular file or directory: {source}")
    if source.is_dir():
        from .packages import artifact_excluder, read_build_spec, source_tree_digest

        try:
            build = read_build_spec(source)
            digest = source_tree_digest(source)
        except ValueError as exc:
            raise FormatError(f"invalid build manifest for {source}: {exc}") from exc
        return True, digest, artifact_excluder(build) if build is not None else None
    if not source.is_file():
        raise FormatError(f"a published runner must be a regular file or directory: {source}")
    try:
        return False, sha256_file(source), None
    except ValueError as exc:
        raise FormatError(f"a published runner must be a regular file or directory: {source}: {exc}") from exc


def _existing_runner_digest(target: Path, is_directory: bool) -> str:
    """Return the digest of an existing runner entry of the expected kind."""

    if target.is_symlink() or is_directory != target.is_dir():
        raise FormatError(f"workspace runner store entry type does not match the published runner: {target}")
    if is_directory:
        return tree_digest(target)
    if not target.is_file():
        raise FormatError(f"workspace runner store entry is not a regular file: {target}")
    return sha256_file(target)


def _artifact_ignore(
    source: Path, exclude: Callable[[str], bool] | None
) -> Callable[[str, list[str]], set[str]] | None:
    """Return a :func:`shutil.copytree` ignore callable leaving out excluded build artifacts."""

    if exclude is None:
        return None

    def ignore(directory: str, names: list[str]) -> set[str]:
        return {
            name for name in names if exclude(PurePosixPath(os.path.relpath(Path(directory) / name, source)).as_posix())
        }

    return ignore


def _stage_runner(
    source: str | os.PathLike[str], stage: Path, name: str | PurePosixPath, *, store: Path | None = None
) -> dict[str, object]:
    """Copy a runner into a staging directory instead of publishing it to the store.

    This is :meth:`Workspace.publish_runner` for a caller that must not write
    the workspace runner store: :meth:`httk.workflow.Attempt.call` stages the
    runner of a called workflow into its outcome draft, and the trusted manager
    publishes it when it commits the outcome. The copy at ``stage/<name>`` holds
    exactly what publication would install (no symlinks, no declared build
    artifacts), it is verified against the source digest after copying, and the
    returned reference is the one publication will return. Staging the same
    content under the same name again is a no-op.

    *store* is the store entry the runner will be published at. The store is
    readable where the caller runs, so a different runner already published
    under *name* is refused here, early and catchable, exactly as
    :meth:`Workspace.publish_runner` would refuse it; the manager's check at
    commit stays the authority, since another job may publish meanwhile.

    :param source: The runner file or directory.
    :param stage: The staging directory, the store root of the staged copies.
    :param name: The store name the runner will be published under.
    :param store: The workspace store entry for *name*, checked when given.
    :return: The workspace runner reference ``{source, path, sha256}``.
    :raises FileExistsError: If the store already holds, or the stage already
        holds, different content under *name*.
    :raises httk.workflow.errors.FormatError: If the source or *name* is invalid,
        or the source changed while it was copied.
    """

    source_path = Path(source).expanduser()
    is_directory, digest, exclude = _runner_content_digest(source_path)
    relative = validate_runner_path(str(PurePosixPath(name)), "workspace")
    if store is not None and (store.exists() or store.is_symlink()):
        published = _existing_runner_digest(store, is_directory)
        if published != digest:
            raise FileExistsError(
                f"workspace runner {relative.as_posix()} already holds a different digest {published}"
            )
    target = stage.joinpath(*relative.parts)
    if target.exists() or target.is_symlink():
        existing = _existing_runner_digest(target, is_directory)
        if existing != digest:
            raise FileExistsError(f"staged runner {relative.as_posix()} already holds a different digest {existing}")
        return {"source": "workspace", "path": relative.as_posix(), "sha256": digest}
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid.uuid4()}.tmp"
    try:
        if is_directory:
            shutil.copytree(source_path, temporary, symlinks=True, ignore=_artifact_ignore(source_path, exclude))
        else:
            shutil.copy2(source_path, temporary, follow_symlinks=False)
        # The digest pins what was copied: a source changed meanwhile is refused here.
        copied = _runner_content_digest(temporary)[1]
        if copied != digest:
            raise FormatError(f"runner {source_path} changed while it was staged")
        os.replace(temporary, target)
    finally:
        if temporary.is_dir() and not temporary.is_symlink():
            shutil.rmtree(temporary)
        else:
            temporary.unlink(missing_ok=True)
    return {"source": "workspace", "path": relative.as_posix(), "sha256": digest}


class Workspace:
    """Attach to one self-contained httk workflow filesystem workspace.

    :param root: Locate the workspace root.
    :param mutable: Preserve the attachment mutability option accepted by callers.
    :param durable: Enable storage-crash durability for filesystem publications.
    :param marker_index_capacity: Bound the in-memory active-marker index.
    :raises ValueError: If the marker index capacity is not positive.
    :raises httk.workflow.errors.FormatError: If the workspace format or identity is invalid.
    :raises httk.workflow.errors.UnsupportedExtensionError: If the workspace uses unsupported extensions or a profile.
    """

    # Whether a manager attached through this instance may serve the exchange
    # extension; a view restricted to some jobs (job debug) never does.
    _serves_exchange = True

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        mutable: bool = True,
        durable: bool = True,
        marker_index_capacity: int = DEFAULT_MARKER_INDEX_CAPACITY,
    ) -> None:
        if marker_index_capacity < 1:
            raise ValueError("marker_index_capacity must be positive")
        self._marker_index_capacity = marker_index_capacity
        self.root = Path(root).resolve()
        self.jobs = self.root / JOBS_DIRECTORY
        self.control = self.root / WORKSPACE_DIRECTORY
        self.runners = self.control / "runners"
        self.runner_builds = self.control / "runner-builds"
        self.durable = durable
        self._reported_faults: set[Path] = set()
        self.format = read_json(self.control / "format.json")
        if self.format.get("format") != "httk-workflow-filesystem" or self.format.get("format_version") != 3:
            raise _refuse_format(self.root, self.format.get("format_version"), self.format.get("core_profile"))
        self.core_profile = self.format.get("core_profile")
        if self.core_profile != CORE_PROFILE:
            raise _refuse_format(self.root, self.format.get("format_version"), self.format.get("core_profile"))
        if self.format.get("layout") != LAYOUT:
            # Plan P8: the jobs-v3 layout replaces the unreleased one under the same format number, without migration.
            raise FormatError(
                f"workspace at {self.root} uses an older job layout (format.json has no \"layout\": \"{LAYOUT}\"); "
                "there is no migration: re-create the workspace with `httk workspace init` and submit its jobs again"
            )
        self._policy = WorkspacePolicy.from_mapping(_section(self.format, "policy"))
        _validate_settings(_section(self.format, "settings"))
        _validate_workflow_preludes(_section(self.format, "workflow_preludes"))
        self.extensions = _extensions(self.format)
        if self.format.get("record_ref_encoding") != "hwref-v2":
            raise UnsupportedExtensionError("unsupported record reference encoding")
        self.workspace_id = str(self.format.get("workspace_id"))
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except ValueError as exc:
            raise FormatError("workspace_id must be a canonical UUID") from exc
        # Job id -> where this instance last saw that job's marker, most recently
        # touched last. It is a pure cache of the state tree, built lazily from
        # one scan and maintained by every rename this instance performs; see
        # :meth:`find_marker_by_id`. Only active jobs are cached and the map is
        # capped, so it tracks the working set rather than the whole history: a
        # job that reaches a terminal kind is evicted, and interactive lookups of
        # a finished job pay one exhaustive scan instead.
        self._marker_index: OrderedDict[str, _IndexEntry] | None = None
        # Job ids the last complete scan found more than one marker for. That is
        # workspace corruption, and the lookup that meets one must say so rather
        # than pick a winner.
        self._marker_duplicates: frozenset[str] = frozenset()
        # Installed journal writer scopes, innermost last; see _journal_writer_scope.
        self._writer_scopes: list[_WriterScope] = []

    def __repr__(self) -> str:
        return f"Workspace(root={str(self.root)!r}, workspace_id={self.workspace_id!r})"

    def ensure_directory(self, path: Path) -> Path:
        """Create a directory below the workspace root.

        :param path: Identify the directory to create.
        :return: The created directory path.
        :raises ValueError: If the path is outside the workspace root.
        """

        path = Path(path)
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise ValueError(f"directory must be below workspace root: {path}") from exc
        path.mkdir(parents=True, exist_ok=True)
        return path

    @classmethod
    def initialize(
        cls,
        root: str | os.PathLike[str],
        *,
        extensions: Iterable[str] = (),
        durable: bool = True,
        policy: Mapping[str, object] | None = None,
    ) -> "Workspace":
        """Create and return a new workspace.

        :param root: Locate the new workspace root.
        :param extensions: Enable optional workspace extensions.
        :param durable: Enable storage-crash durability for filesystem publications.
        :param policy: Override the default workspace policy values.
        :return: The initialized workspace.
        :raises httk.workflow.errors.FormatError: If the filesystem cannot satisfy the workspace profile or
            ``root`` exists and is not an empty directory.
        :raises httk.workflow.errors.UnsupportedExtensionError: If an extension is not supported.
        :raises httk.workflow.errors.SealedError: If the enclosing project is sealed.
        """

        from .projects import discover_project
        from .seals import is_project_sealed

        initial_policy = WorkspacePolicy.from_mapping({} if policy is None else policy)
        root_path = Path(root).resolve()
        project = discover_project(root_path)
        if project is not None and is_project_sealed(project):
            raise SealedError(f"project at {project} is sealed; cannot initialize a workspace under it")
        if (
            root_path.exists()
            and not (
                root_path / WORKSPACE_DIRECTORY
            ).exists()  # a half-made workspace keeps the FileExistsError race path
            and (not root_path.is_dir() or any(root_path.iterdir()))
        ):
            raise FormatError(
                f"{root_path} is not empty: a workspace is a directory of its own; initialize it in a new or "
                "empty directory, e.g. `httk workspace init --name default workspace`"
            )
        root_path.mkdir(parents=True, exist_ok=True)
        extension_set = frozenset(extensions)
        unsupported = extension_set - SUPPORTED_EXTENSIONS
        if unsupported:
            raise UnsupportedExtensionError(f"unsupported extensions: {', '.join(sorted(unsupported))}")
        name_max = os.pathconf(root_path, "PC_NAME_MAX")
        if name_max < 213:
            raise FormatError(f"filesystem NAME_MAX {name_max} is below the {CORE_PROFILE} requirement of 213")
        control = root_path / WORKSPACE_DIRECTORY
        control.mkdir(exist_ok=False)
        for relative in (
            "owners",
            "requests",
            "tmp",
            "quarantine",
            "transfers/outgoing",
            "transfers/incoming",
            "exchange-jobs",
        ):
            (control / relative).mkdir(parents=True, exist_ok=True)
        for state in (*UNOWNED_STATES, OWNED):
            (root_path / JOBS_DIRECTORY / state).mkdir(parents=True, exist_ok=True)
        (root_path / WORKFLOWS_DIRECTORY).mkdir(exist_ok=True)
        write_json_atomic(
            control / "format.json",
            {
                "format": "httk-workflow-filesystem",
                "format_version": 3,
                "layout": LAYOUT,
                "core_profile": CORE_PROFILE,
                # The exchange extension is recorded by enabling it below, once its directory exists.
                "extensions": sorted(extension_set - {EXCHANGE_EXTENSION}),
                "record_ref_encoding": "hwref-v2",
                "workspace_id": str(uuid.uuid4()),
                "created_at": utc_now(),
                "policy": initial_policy.as_mapping(),
                "settings": {},
                "workflow_preludes": {},
            },
            durable=durable,
        )
        # A workspace inside a project is a project member: core's project verbs
        # (seal, manifest, repair, verify) discover it through members.json.
        if project is not None:
            from httk.core.project.members import register_project_member

            register_project_member(project, root_path, "workspace")
        workspace = cls(root_path, durable=durable)
        if EXCHANGE_EXTENSION in extension_set:
            from ._exchange import enable_exchange

            enable_exchange(workspace)
        return workspace

    @classmethod
    def discover(cls, start: str | os.PathLike[str] | None = None) -> Path | None:
        """Find the nearest workspace root at or above *start*.

        Discovery walks from *start* and its parents, treating a file start as
        its containing directory.

        :param start: Directory or file from which to begin the upward search, or None for the current directory.
        :return: Nearest workspace root, or None when no workspace marker is found.
        """

        path = Path.cwd() if start is None else Path(start)
        path = path.expanduser().resolve()
        if path.is_file():
            path = path.parent
        for candidate in (path, *path.parents):
            if (candidate / WORKSPACE_DIRECTORY / "format.json").is_file():
                return candidate
        return None

    @classmethod
    def default(cls) -> Self:
        """Resolve the enclosing, project, or per-user default workspace, creating it if needed.

        :return: The default workspace.
        """

        discovered = cls.discover()
        if discovered is not None:
            return cls(discovered)

        from . import registry

        binding = registry.default_workspace()
        assert binding.path is not None
        return cls(binding.path)

    @property
    def policy(self) -> WorkspacePolicy:
        """Return the tunables this workspace publishes to every attacher.

        :return: The current workspace policy.
        """

        return self._policy

    @property
    def workflows(self) -> Path:
        """Return the store of installed workflows, ``<root>/workflows``.

        :return: The store directory.
        """

        return self.root / WORKFLOWS_DIRECTORY

    @property
    def visibility_deadline(self) -> float:
        """Return how long a metadata visibility retry may keep probing.

        :return: The workspace metadata visibility deadline.
        """

        return self._policy.visibility_deadline_seconds

    def _require_unsealed(self, marker: Marker | None = None) -> None:
        """Refuse a mutation when the workspace, its project, or a job is sealed.

        Seals are enforced at the write funnels rather than at attach, so a
        sealed tree stays fully readable and every maintenance operation keeps
        working; only the operations that would change sealed bytes are refused.

        :param marker: When given, also refuse if that marker's job carries a seal.
        :raises httk.workflow.errors.SealedError: If a covering level is sealed.
        """

        from .projects import discover_project
        from .seals import is_job_sealed, is_project_sealed, is_workspace_sealed

        if is_workspace_sealed(self):
            raise SealedError(f"workspace {self.workspace_id} is sealed; unseal it first")
        project = discover_project(self.root)
        if project is not None and is_project_sealed(project):
            raise SealedError(f"project at {project} is sealed; unseal it first")
        if marker is not None and is_job_sealed(self.payload_path(marker.placement, marker.job_key)):
            raise SealedError(f"job {marker.job_key} is sealed; unseal it first")

    def set_policy(self, changes: Mapping[str, object]) -> WorkspacePolicy:
        """Validate *changes*, merge them into the stored policy, and publish it.

        The write is an ordinary read-modify-write of ``format.json`` through an
        exclusively created temporary file and a rename, so a reader never sees
        a torn object. It is deliberately not serialized against another writer:
        policy is administrative, changes are rare, and last writer wins.

        :param changes: Supply policy values to validate and merge.
        :return: The resulting workspace policy.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        self._require_unsealed()
        stored = read_json(self.control / "format.json")
        merged = WorkspacePolicy.from_mapping(_section(stored, "policy")).updated(changes)
        stored["policy"] = merged.as_mapping()
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        self._policy = merged
        _LOGGER.info(
            "workspace %s policy updated: %s",
            self.workspace_id,
            ", ".join(f"{key}={value!r}" for key, value in sorted(changes.items())),
            extra={"event": "policy_updated", "workspace_id": self.workspace_id},
        )
        return merged

    @property
    def settings(self) -> dict[str, object]:
        """Return this workspace's application settings, a flat dotted map.

        Application settings are distinct from :attr:`policy`, which tunes the
        engine. These are the values an application step resolves at run time —
        the VASP command, a pseudopotential library — one layer of the
        job-parameters → environment → workspace → default resolution a runner reads
        through :meth:`~httk.workflow.sdk.Attempt.setting`.

        :return: The workspace's application settings.
        """

        return _validate_settings(_section(self.format, "settings"))

    def refresh_format(self) -> dict[str, object]:
        """Re-read ``format.json`` and refresh the enabled :attr:`extensions` from it.

        Extensions can be enabled while processes are attached (``httk
        workspace exchange enable``), so a long-running reader re-reads them
        rather than trusting what it saw at attach. The format, profile and
        workspace identity must still be the attached ones; the policy keeps the
        value read at attach.

        :return: The re-read ``format.json`` document.
        :raises httk.workflow.errors.FormatError: If ``format.json`` cannot be read, is not
            this workspace's format 3 document, or its extensions are malformed.
        :raises httk.workflow.errors.UnsupportedExtensionError: If it enables an unsupported extension.
        """

        stored = read_json(self.control / "format.json")
        if stored.get("format") != "httk-workflow-filesystem" or stored.get("format_version") != 3:
            raise _refuse_format(self.root, stored.get("format_version"), stored.get("core_profile"))
        if stored.get("core_profile") != CORE_PROFILE:
            raise _refuse_format(self.root, stored.get("format_version"), stored.get("core_profile"))
        if stored.get("workspace_id") != self.workspace_id:
            raise FormatError(
                f"workspace format.json at {self.root} now names workspace {stored.get('workspace_id')!r}, "
                f"not the attached {self.workspace_id}"
            )
        self.extensions = _extensions(stored)
        self.format = stored
        return stored

    def read_settings(self) -> dict[str, object]:
        """Read and validate the current settings from disk, refreshing the enabled extensions too.

        :return: The current application settings.
        :raises httk.workflow.errors.FormatError: If ``format.json`` or the stored settings are not valid.
        :raises httk.workflow.errors.UnsupportedExtensionError: If ``format.json`` now enables an
            unsupported extension.
        """

        return _validate_settings(_section(self.refresh_format(), "settings"))

    @staticmethod
    def _check_setting_collision(key: str, settings: Mapping[str, object]) -> None:
        variable = _setting_variable_name(key)
        for existing in settings:
            if existing != key and _setting_variable_name(existing) == variable:
                raise ValueError(f"application settings {existing!r} and {key!r} derive the same variable {variable}")

    def set_setting(self, key: str, value: object) -> dict[str, object]:
        """Store one application setting and return the resulting map.

        The write is the same read-modify-write of ``format.json`` that
        :meth:`set_policy` uses: an exclusively created temporary and a rename,
        so a reader never sees a torn object, and last writer wins.

        :param key: Name the application setting to store.
        :param value: Supply the setting value.
        :return: The resulting application settings.
        :raises ValueError: If the setting name or value is invalid, or its environment name collides.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        return self.configure_settings({key: value})

    def unset_setting(self, key: str) -> dict[str, object]:
        """Remove one application setting, refusing one that is not set.

        :param key: Name the application setting to remove.
        :return: The resulting application settings.
        :raises ValueError: If the setting is not set.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        return self.configure_settings({}, unset=(key,))

    def configure_settings(self, changes: Mapping[str, object], *, unset: Sequence[str] = ()) -> dict[str, object]:
        """Remove named settings, then merge changes after validating the complete candidate.

        :param changes: Application setting values to store.
        :param unset: Existing setting names to remove before applying changes.
        :return: The resulting application settings.
        :raises ValueError: If a name or value is invalid, a setting is absent, or environment names collide.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        self._require_unsealed()
        stored = read_json(self.control / "format.json")
        settings = _validate_settings(_section(stored, "settings"))
        for key in unset:
            if key not in settings:
                raise ValueError(f"application setting is not set: {key}")
            del settings[key]
        for key, value in changes.items():
            _validate_setting_key(key)
            _validate_setting_value(key, value)
            self._check_setting_collision(key, settings)
            settings[key] = value
        stored["settings"] = settings
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(settings)

    def seed_settings(self, seeds: Mapping[str, object]) -> dict[str, object]:
        """Merge *seeds* into the settings, keeping any value already set.

        Seeding happens once, when a workspace bound to a remote is created: the
        remote definition's whitelisted settings become the workspace's
        starting application settings. An explicit setting already present is
        never overwritten, so a value the operator chose outlives a reseed.

        :param seeds: Supply initial application settings to merge.
        :return: The resulting application settings.
        :raises ValueError: If a supplied setting is invalid.
        """

        merged = _validate_settings(seeds)
        stored = read_json(self.control / "format.json")
        current = _validate_settings(_section(stored, "settings"))
        for key, value in merged.items():
            if any(
                existing != key and _setting_variable_name(existing) == _setting_variable_name(key)
                for existing in current
            ):
                continue
            current.setdefault(key, value)
        stored["settings"] = current
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(current)

    def read_workflow_preludes(self) -> dict[str, str]:
        """Read and validate the workflow-in-workspace preludes from disk.

        A workflow prelude is shell text run to initialize the environment
        before each launch of a runner for that workflow.

        :return: The current map of workflow id to prelude text.
        :raises httk.workflow.errors.FormatError: If the stored preludes are not valid.
        """

        return _validate_workflow_preludes(_section(read_json(self.control / "format.json"), "workflow_preludes"))

    def set_workflow_prelude(self, workflow_id: str, value: str) -> dict[str, str]:
        """Store one workflow prelude and return the resulting map.

        The write is the same read-modify-write of ``format.json`` that
        :meth:`set_setting` uses: an exclusively created temporary and a rename,
        so a reader never sees a torn object, and last writer wins.

        :param workflow_id: Name the workflow whose prelude to store.
        :param value: Supply the shell prelude text.
        :return: The resulting map of workflow id to prelude text.
        :raises ValueError: If the workflow id or prelude value is invalid.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        self._require_unsealed()
        _validate_workflow_prelude_id(workflow_id)
        _validate_workflow_prelude_value(workflow_id, value)
        stored = read_json(self.control / "format.json")
        preludes = _validate_workflow_preludes(_section(stored, "workflow_preludes"))
        preludes[workflow_id] = value
        stored["workflow_preludes"] = preludes
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(preludes)

    def unset_workflow_prelude(self, workflow_id: str) -> dict[str, str]:
        """Remove one workflow prelude, refusing one that is not set.

        :param workflow_id: Name the workflow whose prelude to remove.
        :return: The resulting map of workflow id to prelude text.
        :raises ValueError: If the prelude is not set.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        self._require_unsealed()
        stored = read_json(self.control / "format.json")
        preludes = _validate_workflow_preludes(_section(stored, "workflow_preludes"))
        if workflow_id not in preludes:
            raise ValueError(f"workflow prelude is not set: {workflow_id}")
        del preludes[workflow_id]
        stored["workflow_preludes"] = preludes
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(preludes)

    def open_journal_writer(self, *, writer_id: str | None = None) -> JournalWriter:
        """Open one exclusive journal writer configured by workspace policy.

        :param writer_id: Reuse a canonical writer identity when one is supplied.
        :return: The exclusive journal writer.
        :raises ValueError: If the writer identity is not canonical.
        """

        return JournalWriter(
            self.control,
            writer_id=writer_id,
            durable=self.durable,
            maximum_segment_bytes=self._policy.journal_segment_bytes,
        )

    @contextmanager
    def _journal_writer_scope(self, writer: JournalWriter | None = None) -> Iterator[None]:
        """Let every transfer transition of this instance reuse one journal writer.

        Inside the scope :meth:`_transition_writer` yields *writer*, or one writer
        the scope opens on first use and closes on exit. A scope without a writer
        inside another scope reuses the outer one. Scopes may exit in any order:
        each removes only itself. The writer stays unlocked: managers and CLI
        commands are single-threaded, and a writer never crosses processes.
        """

        if writer is None and self._writer_scopes:
            yield
            return
        scope = _WriterScope(writer, owned=writer is None)
        self._writer_scopes.append(scope)
        try:
            yield
        finally:
            self._writer_scopes.remove(scope)
            if scope.owned and scope.writer is not None:
                scope.writer.close()

    @contextmanager
    def _transition_writer(self) -> Iterator[JournalWriter]:
        """Yield the innermost scope's journal writer, or open and close one as before scopes."""

        if not self._writer_scopes:
            with self.open_journal_writer() as writer:
                yield writer
            return
        scope = self._writer_scopes[-1]
        if scope.writer is None:
            scope.writer = self.open_journal_writer()
        yield scope.writer

    def check(
        self,
        *,
        repair: bool = False,
        quarantine_unrepairable: bool = False,
    ) -> "FsckReport":
        """Verify that every marker resolves to its journal frame.

        :param repair: Repair recoverable marker or journal inconsistencies.
        :param quarantine_unrepairable: Quarantine entries that cannot be repaired.
        :return: The workspace check report.
        """

        from .fsck import check_workspace

        return check_workspace(self, repair=repair, quarantine_unrepairable=quarantine_unrepairable)

    def collect_garbage(
        self,
        *,
        dry_run: bool = False,
        now: float | None = None,
        categories: Sequence[str] | None = None,
        journal_writer: JournalWriter | None = None,
        sizes: bool = True,
    ) -> "GcReport":
        """Collect the disk this workspace's retention policy permits freeing.

        :param dry_run: Report eligible removals without changing the workspace.
        :param now: Use this timestamp when evaluating retention deadlines.
        :param categories: Restrict collection to these categories.
        :param journal_writer: Append a collection frame to this already-open
            writer instead of opening a new writer.
        :param sizes: Whether to calculate byte estimates.
        :return: The garbage-collection report.
        """

        from .gc import collect_garbage

        return collect_garbage(
            self,
            dry_run=dry_run,
            now=now,
            categories=categories,
            journal_writer=journal_writer,
            sizes=sizes,
        )

    def runner_store_path(self, path: str | PurePosixPath) -> Path:
        """Return the store location of one workspace runner.

        The store is flat and name-keyed below ``.httk-workspace/runners/``.
        Relative subdirectories are permitted so a campaign can group runners,
        but a name can never escape the store.

        :param path: Name the runner within the workspace store.
        :return: The runner's store path.
        :raises httk.workflow.errors.FormatError: If the runner path is invalid or escapes the store.
        """

        relative = validate_runner_path(str(PurePosixPath(path)), "workspace")
        resolved = self.runners.joinpath(*relative.parts)
        root = self.runners.resolve()
        if not Path(os.path.normpath(resolved)).is_relative_to(root):
            raise FormatError(f"runner name must remain below the workspace runner store: {relative}")
        return resolved

    def publish_runner(
        self,
        source: str | os.PathLike[str],
        *,
        name: str | PurePosixPath | None = None,
        replace: bool = False,
    ) -> dict[str, object]:
        """Install one runner in the workspace store and describe the reference.

        Publication is content addressed: republishing identical bytes is an
        idempotent no-op, and replacing a name whose content differs requires
        *replace* so a live campaign referring to the old digest can never be
        changed underneath by accident.

        :param source: Locate the runner file or directory to publish.
        :param name: Choose the store name, defaulting to the source name.
        :param replace: Replace a different existing runner with the same name.
        :return: The published runner reference.
        :raises FileExistsError: If a different runner already has the target name.
        :raises httk.workflow.errors.FormatError: If the source, target name, or entry type is invalid.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If a concurrent publication installed
            different content under the name first (it is kept).
        """

        self._require_unsealed()
        source_path = Path(source).expanduser()
        is_directory, digest, exclude = _runner_content_digest(source_path)
        target = self.runner_store_path(name if name is not None else source_path.name)
        if not is_directory and target.name == RUNNER_TREE_ENTRY and target.parent != self.runners:
            raise FormatError(
                "a file runner named 'run' cannot be published below a store subdirectory; "
                "that name is reserved for directory runner entry points"
            )
        if target.exists() or target.is_symlink():
            existing = _existing_runner_digest(target, is_directory)
            if existing == digest:
                _LOGGER.debug("workspace runner %s already holds digest %s", target, digest)
            elif not replace:
                raise FileExistsError(
                    f"workspace runner {target.relative_to(self.runners).as_posix()} already holds a "
                    f"different digest {existing}; pass replace to overwrite it"
                )
            elif is_directory:
                self._install_runner_tree(source_path, target, digest=digest, replace=True, exclude=exclude)
            else:
                self._install_runner_file(source_path, target, digest=digest, replace=True)
        elif is_directory:
            self._install_runner_tree(source_path, target, digest=digest, replace=False, exclude=exclude)
        else:
            self._install_runner_file(source_path, target, digest=digest, replace=False)
        relative = target.relative_to(self.runners)
        _LOGGER.info(
            "published workspace runner %s with digest %s",
            relative.as_posix(),
            digest,
            extra={"event": "runner_published", "runner": relative.as_posix(), "sha256": digest},
        )
        return {"source": "workspace", "path": relative.as_posix(), "sha256": digest}

    def _install_runner_file(self, source: Path, target: Path, *, digest: str, replace: bool) -> None:
        """Install one store file: replace an existing entry only with *replace*, else link it in exclusively.

        Without *replace* the prepared inode is linked under the name, which
        fails if anything holds it: a concurrent publication's winner is kept,
        and accepted only when it holds the same *digest*.
        """

        self.ensure_directory(target.parent)
        staging = self.control / "tmp" / f"runner.{uuid.uuid4()}"
        staging.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, staging)
        staging.chmod(0o555)
        try:
            if replace:
                os.replace(staging, target)
                return
            try:
                os.link(staging, target, follow_symlinks=False)
            except FileExistsError:
                self._require_runner_digest(target, digest, is_directory=False)
        finally:
            staging.unlink(missing_ok=True)

    def _require_runner_digest(self, target: Path, digest: str, *, is_directory: bool) -> None:
        """Accept a store entry another publication installed first only when it holds *digest*."""

        existing = _existing_runner_digest(target, is_directory)
        if existing != digest:
            raise WorkspaceCorruptionError(
                f"workspace runner {target.relative_to(self.runners).as_posix()} was published concurrently "
                f"with digest {existing}, not {digest}; it is kept"
            )

    def _install_runner_tree(
        self,
        source: Path,
        target: Path,
        *,
        digest: str,
        replace: bool,
        exclude: Callable[[str], bool] | None = None,
    ) -> None:
        """Install a runner tree, replacing an existing tree only with *replace*.

        Without *replace* the prepared tree is renamed onto the name, which
        fails onto a non-empty directory: a concurrent publication's winner is
        kept, and accepted only when it holds the same *digest*.
        """

        self.ensure_directory(target.parent)
        staging = self.control / "tmp" / f"runner.{uuid.uuid4()}"
        staging.parent.mkdir(parents=True, exist_ok=True)
        old: Path | None = None
        try:
            try:
                shutil.copytree(source, staging, symlinks=False, ignore=_artifact_ignore(source, exclude))
                for entry in sorted(staging.rglob("*")):
                    entry.chmod(0o555)
                # Linux requires write permission on a directory itself to rename it;
                # the installed root is made read-only immediately after the rename.
                staging.chmod(0o755)
                if not replace:
                    try:
                        os.rename(staging, target)
                    except OSError as exc:
                        if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOTDIR, errno.EISDIR}:
                            raise
                        self._require_runner_digest(target, digest, is_directory=True)
                    else:
                        target.chmod(0o555)
                    return
                if target.exists():
                    target.chmod(0o755)
                    old = staging.parent / f"runner-old.{uuid.uuid4()}"
                    os.replace(target, old)
                    # Explicit replacement has a small non-atomic window while the old tree is aside.
                os.replace(staging, target)
                target.chmod(0o555)
                if old is not None:
                    for entry in sorted(old.rglob("*")):
                        entry.chmod(0o755 if entry.is_dir() else 0o644)
                    old.chmod(0o755)
                    shutil.rmtree(old)
                    old = None
            finally:
                if staging.exists():
                    entries = [staging, *staging.rglob("*")]
                    for entry in sorted(entries, key=lambda path: len(path.parts)):
                        entry.chmod(0o755 if entry.is_dir() else 0o644)
                    shutil.rmtree(staging)
        finally:
            if old is not None and not target.exists():
                os.replace(old, target)
                target.chmod(0o555)

    def detach(
        self,
        job_id: str,
        *,
        marker: Marker | None = None,
        waiting_parent_map: Mapping[str, set[str]] | None = None,
        destination_workspace_id: str,
        destination_remote: str | None = None,
        destination_placement: str | PurePosixPath | None = None,
        transfer_id: str | None = None,
        with_tree: bool = False,
    ) -> Path:
        """Seal one quiescent job as a detached transfer bundle.

        :param job_id: Identify the job to detach.
        :param marker: An already resolved source marker, when available.
        :param waiting_parent_map: Reuse a precomputed waiting-parent map.
        :param destination_workspace_id: Identify the destination workspace.
        :param destination_remote: Name the destination remote, when applicable.
        :param destination_placement: Choose the destination placement.
        :param transfer_id: Reuse a transfer identity when resuming a publication.
        :param with_tree: Seal a job whose bound children the caller transfers with it; the
            caller must then fence every member, root first, as :func:`~httk.workflow.transfers.offer_transfers`
            does with the candidates :func:`~httk.workflow.transfers.select_transfer_jobs` returns.
        :return: The sealed transfer bundle path.
        """

        from .transfers import detach_job

        return detach_job(
            self,
            job_id,
            marker=marker,
            waiting_parent_map=waiting_parent_map,
            destination_workspace_id=destination_workspace_id,
            destination_remote=destination_remote,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
            with_tree=with_tree,
        )

    def detach_from_parent(self, job_id: str, *, operator: str | None = None) -> bool:
        """Make one spawned child independent of its parent, permanently.

        A detached child no longer travels with its parent's tree, may be
        transferred on its own, and reads no parent through ``Attempt.parent``.
        It stays in any join that references it. The detachment is a file in the
        payload's reserved tree metadata, not a state transition, so it applies
        to a job in any state except ``transferring``, sealed or not.

        :param job_id: Identify the child job.
        :param operator: Record who detached it.
        :return: Whether this call detached it (``False`` when it already was).
        :raises ValueError: If the job is missing, has no parent, or is transferring.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        from ._job_tree import mark_detached

        self._require_unsealed()
        marker = self.find_marker_by_id(job_id)
        if marker is None:
            raise ValueError(f"no job {job_id} in this workspace")
        if marker.kind == "transferring":
            raise ValueError(f"job {marker.job_key} is transferring and cannot be detached")
        if self.load_job(marker).parent is None:
            raise ValueError(f"job {marker.job_key} has no parent to detach from")
        return mark_detached(
            self.payload_path(marker.placement, marker.job_key), operator=operator, durable=self.durable
        )

    def eject(self, job_id: str, target: str | os.PathLike[str]) -> Path:
        """Move one quiescent job out of this workspace to a free-standing job directory.

        :param job_id: Identify the job to eject.
        :param target: The new job directory, or an existing directory to eject into.
        :return: The free-standing job directory.
        """

        from .transfers import eject_job

        return eject_job(self, job_id, target)

    def adopt(self, directory: str | os.PathLike[str], *, placement: str | PurePosixPath | None = None) -> Marker:
        """Move one free-standing (ejected) job directory into this workspace.

        :param directory: The free-standing job directory.
        :param placement: Place the job here instead of where it was ejected from.
        :return: The adopted job's marker.
        """

        from .transfers import adopt_job

        return adopt_job(self, directory, placement=placement)

    def import_bundle(self, bundle: str | os.PathLike[str]) -> dict[str, object]:
        """Import a validated detached transfer bundle.

        :param bundle: Locate the detached transfer bundle.
        :return: The imported transfer description.
        """

        from .transfers import import_bundle

        return import_bundle(self, bundle)

    def acknowledge_transfer(self, acknowledgement: Mapping[str, object]) -> Path:
        """Retire a source bundle after destination acknowledgement.

        :param acknowledgement: Supply the destination acknowledgement.
        :return: The retired source bundle identity path (normally already removed).
        """

        from .transfers import acknowledge_transfer

        return acknowledge_transfer(self, acknowledgement)

    def recover_transfers(self) -> list[dict[str, object]]:
        """Recover or report interrupted detached-transfer publications.

        :return: Descriptions of recovered or still-pending transfers.
        """

        from .transfers import recover_transfers

        return recover_transfers(self)

    def state_directory(self, kind: str, placement: PurePosixPath) -> Path:
        """Return the state directory for a kind and placement.

        :param kind: Select the state kind.
        :param placement: Select the placement below that kind.
        :return: The state directory path.
        :raises ValueError: If the state kind is unknown.
        """
        if kind not in STATE_KINDS:
            raise ValueError(f"unknown state kind: {kind}")
        return (self.control / "state" / kind).joinpath(*placement.parts)

    def marker_path(
        self,
        kind: str,
        placement: PurePosixPath,
        job_key: str,
        priority: int,
        generation: int,
        record_ref: str,
    ) -> Path:
        """Build the path of one state marker.

        :param kind: Select the state kind.
        :param placement: Select the marker placement.
        :param job_key: Identify the job.
        :param priority: Set the marker priority.
        :param generation: Set the state generation.
        :param record_ref: Identify the journal record.
        :return: The marker path.
        :raises ValueError: If the state kind is unknown.
        """
        return self.state_directory(kind, placement) / marker_basename(job_key, priority, generation, record_ref)

    def payload_path(self, placement: PurePosixPath, job_key: str) -> Path:
        """Return the payload path for one job placement.

        :param placement: Select the payload placement.
        :param job_key: Identify the job payload.
        :return: The payload directory path.
        """
        return self.jobs.joinpath(*placement.parts, job_key)

    def _iter_state_subtree(
        self,
        directory: Path,
        rel: tuple[str, ...],
        after: tuple[str, ...] | None,
        *,
        flat: bool = False,
    ) -> Iterator[tuple[tuple[str, ...], "Marker | MarkerFault | None"]]:
        """Yield every entry below *directory* in stable pre-order, past *after*.

        ``directory`` sits at position ``rel`` relative to ``state/<kind>``.
        Each tuple is ``(position, entry)``: ``entry`` is the parsed marker or
        the fault of a marker-shaped file and ``None`` for every other visited
        entry — a subdirectory the walk descends, a foreign file, or a marker an
        earlier tick already consumed. The caller therefore counts one yield per
        directory entry examined, which is exactly the discovery budget.

        ``after`` prunes: an entry not strictly greater than it is skipped, and a
        subtree entirely at or below it is never opened, so resuming a walk costs
        the fan-out along the cursor path rather than a re-scan of all that came
        before it. The walk holds one directory's entries in memory at a time,
        never a materialized list of the tree, and never sorts globally.
        """

        state_root = self.control / "state"
        for entry in _scandir_sorted(directory):
            position = rel + (entry.name,)
            if _safe_is_dir(entry):
                if flat:
                    continue
                relation = _cursor_relation(position, after)
                if relation == "skip":
                    continue
                yield position, None
                yield from self._iter_state_subtree(
                    directory / entry.name, position, after if relation == "resume" else None
                )
                continue
            if after is not None and position <= after:
                yield position, None
                continue
            if not _marker_shaped(entry.name):
                _LOGGER.debug("ignoring foreign file below state: %s", directory / entry.name)
                yield position, None
                continue
            path = directory / entry.name
            try:
                visible = path.is_file()
                regular = entry.is_file(follow_symlinks=False)
            except OSError:
                visible = regular = False
            if not visible or not regular:
                # Marker discovery must not follow a symlink: ownership is
                # meaningful only for the marker inode itself.
                yield position, None
                continue
            try:
                yield position, Marker.from_path(state_root, path)
            except (WorkflowError, ValueError) as exc:
                yield position, MarkerFault(path=path, reason=str(exc))

    def _subtree_roots(self, kind: str, roots: Sequence[PurePosixPath]) -> list[tuple[Path, tuple[str, ...]]]:
        """Return the ``(directory, position)`` roots a walk of *kind* starts at.

        With no placement assignment the walk starts at the whole kind; a
        deployment that restricts a manager to placement subtrees starts one walk
        per assigned prefix, so disjoint managers never scan each other's trees.
        """

        base = self.control / "state" / kind
        if not roots:
            return [(base, ())]
        return [(base.joinpath(*prefix.parts), prefix.parts) for prefix in roots]

    def walk_markers(
        self,
        kinds: Iterable[str] | None = None,
        *,
        roots: Sequence[PurePosixPath] = (),
        heartbeat: Callable[[], None] | None = None,
        heartbeat_every: int = DISCOVERY_HEARTBEAT_STRIDE,
    ) -> Iterator[Marker]:
        """Stream every schedulable marker of *kinds*, exhaustively.

        This is the streaming, cursorless counterpart of a bounded pass: it walks
        the same scandir tree with no discovery budget, reports every fault, and
        takes a heartbeat opportunity every *heartbeat_every* entries so a long
        exhaustive pass — polling running attempts, recovering claims — keeps its
        lease alive from inside the walk. A pass MAY restrict itself to placement
        *roots*; the debug workspace narrows what it surfaces through its private
        ``_scheduling_includes`` hook.

        :param kinds: Restrict the walk to these state kinds.
        :param roots: Restrict the walk to these placement roots.
        :param heartbeat: Call this function during a long walk.
        :param heartbeat_every: Set how many entries to examine between heartbeat opportunities.
        :yield: Each schedulable marker found during the walk.
        """

        visited = 0
        for kind in tuple(kinds or CORE_STATE_KINDS):
            for start, rel in self._subtree_roots(kind, roots):
                for _position, entry in self._iter_state_subtree(start, rel, None):
                    visited += 1
                    if heartbeat is not None and visited % heartbeat_every == 0:
                        heartbeat()
                    if isinstance(entry, MarkerFault):
                        self.report_marker_fault(entry)
                    elif isinstance(entry, Marker) and self._scheduling_includes(entry):
                        yield entry

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield every marker below ``state/``, reporting damage per entry.

        One unusable entry must never hide the rest of the workspace, so a
        marker-shaped basename that fails validation is reported as a
        :class:`MarkerFault` instead of aborting the scan. This is the exhaustive
        walk the workspace tools (fsck, collection, status, collect) use; the
        scheduling passes use the bounded :class:`~httk.workflow.workspace.MarkerStream` instead.

        :param kinds: Restrict the scan to these state kinds.
        :yield: Each valid marker or reported marker fault.
        """

        for kind in tuple(kinds or CORE_STATE_KINDS):
            for _position, entry in self._iter_state_subtree(self.control / "state" / kind, (), None):
                if entry is not None:
                    yield entry

    def scan_markers(self, kinds: Iterable[str] | None = None) -> Iterable[Marker]:
        """Yield every valid marker below ``state/``.

        :param kinds: Restrict the scan to these state kinds.
        :yield: Each valid marker.
        """
        for entry in self.scan_marker_entries(kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def _scheduling_includes(self, marker: Marker) -> bool:
        """Whether a scheduling scan of this workspace may surface *marker*.

        Production managers see the whole workspace; the private manager behind
        the foreground debug runner overrides this to the one job it is driving,
        so its streaming passes never schedule a marker outside that scope.
        """

        return True

    def report_marker_fault(self, fault: MarkerFault) -> None:
        """Report an uninterpretable state entry loudly once, then quietly.

        A marker whose basename or placement cannot be parsed is workspace
        corruption rather than a job state: the core profile leaves its repair
        to an explicit workspace tool, so a manager only reports it and never
        schedules or relocates it.

        :param fault: Describe the unusable state entry to report.
        """

        message = "unusable state entry %s: %s (repair it with a workspace tool)"
        if fault.path in self._reported_faults:
            _LOGGER.debug(message, fault.path, fault.reason)
            return
        self._reported_faults.add(fault.path)
        _LOGGER.error(message, fault.path, fault.reason, extra={"event": "marker_fault", "entry": str(fault.path)})

    # -- the derived job-id index -------------------------------------------
    #
    # ``find_marker_by_id`` and ``find_markers`` are asked one question — where
    # is this job right now — by join evaluation, by every operator request, by
    # ``job show``/``job why``, and by transfers. Answering it by walking the
    # whole state tree costs one rglob per state kind per question, which is what
    # made a waiting parent's per-tick cost grow with its number of children.
    #
    # The answer is cached per workspace instance in memory only. There is
    # deliberately no on-disk index: two managers write one workspace, and a
    # shared file would need a durability and invalidation story that the
    # authoritative state tree already provides for free. The cache is therefore
    # never trusted negatively — a miss rescans before absence is declared — and
    # a hit is confirmed against the filesystem before it is returned, so a
    # marker another manager moved is detected rather than reported stale.
    #
    # It costs a few hundred bytes per job of this workspace, which is the price
    # the specification's "keep an in-memory job-key-to-marker map" advice always
    # implied; a process that must not pay it can drop the whole index at any
    # moment with :meth:`invalidate_marker_index`, at the cost of one rescan.

    def _trim_index(self, index: "OrderedDict[str, _IndexEntry]") -> None:
        """Evict least-recently-touched entries until the cap is satisfied."""

        while len(index) > self._marker_index_capacity:
            index.popitem(last=False)

    def _index_note(self, marker: Marker) -> None:
        """Record where this instance just observed one job's marker.

        The index covers only the *active* kinds — a job that reaches a terminal
        kind, or moves into ``relocating`` or ``transferring``, is evicted rather
        than recorded, so the map tracks the working set and not the workspace's
        whole history. A recorded job is moved to the most-recently-touched end
        and the cap is enforced, so the index never grows without bound.
        """

        if self._marker_index is None:
            return
        if marker.kind not in ACTIVE_STATE_KINDS:
            self._marker_index.pop(marker.job_id, None)
            return
        self._marker_index[marker.job_id] = _IndexEntry(
            kind=marker.kind,
            placement=marker.placement,
            basename=marker.path.name,
        )
        self._marker_index.move_to_end(marker.job_id)
        self._trim_index(self._marker_index)

    def _index_note_path(self, path: Path) -> None:
        """Record one marker this instance published or moved by raw path.

        Marker publication that does not go through :meth:`transition` — a
        submission, a registered child, an imported transfer bundle — still
        renames into ``state/``, so the index is refreshed from the destination
        path rather than from a parsed marker the caller may not have built.
        """

        state_root = self.control / "state"
        try:
            relative = path.relative_to(state_root)
        except ValueError:
            return
        if len(relative.parts) < 3:
            return
        try:
            self._index_note(Marker.from_path(state_root, path))
        except (WorkflowError, ValueError):
            _LOGGER.debug("not indexing an uninterpretable marker publication: %s", path)

    def _index_forget(self, job_id: str) -> None:
        """Drop one job from the index, for instance when it is quarantined."""

        if self._marker_index is not None:
            self._marker_index.pop(job_id, None)

    def invalidate_marker_index(self) -> None:
        """Drop the cached job-id index, so the next lookup rebuilds it."""

        self._marker_index = None
        self._marker_duplicates = frozenset()

    def _rebuild_marker_index(self, wanted: str | None = None) -> Marker | None:
        """Rebuild the active index from one complete scan and locate *wanted*.

        The scan is deliberately the base-class one: a subclass may narrow what
        a manager *scans* for scheduling, but never what the workspace may look
        up, and an index built from a narrowed scan would answer "absent" for a
        job that is plainly there. Every core kind is walked so duplicates and a
        terminal *wanted* job are found, but only active jobs are cached — a
        finished job is located and returned without being kept, so the index
        never carries the history of a workspace that has run for years.
        """

        index: OrderedDict[str, _IndexEntry] = OrderedDict()
        duplicates: set[str] = set()
        seen: set[str] = set()
        found: Marker | None = None
        for entry in Workspace.scan_marker_entries(self, CORE_STATE_KINDS):
            if isinstance(entry, MarkerFault):
                self.report_marker_fault(entry)
                continue
            if entry.job_id in seen:
                duplicates.add(entry.job_id)
            seen.add(entry.job_id)
            if entry.kind in ACTIVE_STATE_KINDS:
                index[entry.job_id] = _IndexEntry(
                    kind=entry.kind,
                    placement=entry.placement,
                    basename=entry.path.name,
                )
            if wanted is not None and entry.job_id == wanted:
                found = entry
        self._trim_index(index)
        self._marker_index = index
        self._marker_duplicates = frozenset(duplicates)
        _LOGGER.debug(
            "rebuilt the marker index of workspace %s with %d active jobs of %d seen",
            self.workspace_id,
            len(index),
            len(seen),
        )
        return found

    def _index_marker(self, job_id: str, entry: "_IndexEntry") -> Marker | None:
        """Return the marker one index entry names, if it is still there."""

        path = self.state_directory(entry.kind, entry.placement) / entry.basename
        if not path.is_file():
            return None
        try:
            marker = Marker.from_path(self.control / "state", path)
        except (WorkflowError, ValueError):
            return None
        return marker if marker.job_id == job_id else None

    def find_markers(self, job_key: str, kinds: Iterable[str] | None = None) -> list[Marker]:
        """Find all current markers for a job key.

        :param job_key: Identify the job to find.
        :param kinds: Restrict the search to these state kinds.
        :return: The matching current markers.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If more than one current marker identifies the job.
        """
        selected = tuple(kinds or CORE_STATE_KINDS)
        if set(selected) <= set(CORE_STATE_KINDS):
            try:
                job_id = parse_job_key(job_key)[1]
            except FormatError:
                job_id = None
            if job_id is not None:
                marker = self.find_marker_by_id(job_id)
                if marker is None or marker.job_key != job_key:
                    return []
                return [marker] if marker.kind in selected else []
        return [marker for marker in self.scan_markers(selected) if marker.job_key == job_key]

    def find_marker_by_id(self, job_id: str, *, kinds: Iterable[str] | None = None) -> Marker | None:
        """Return the one current marker of *job_id*, or ``None`` if it has none.

        Resolution follows the specified ladder: the in-memory index, then a
        targeted probe of the finite state set at the placement the index last
        saw, then one complete rescan. Absence is only ever reported after that
        rescan, so a job another actor has just created or moved is never
        mistaken for a job that does not exist.

        By default only the core kinds are searched, so a job that is
        ``transferring`` (or ``relocating``) reads as absent. Naming those kinds
        in *kinds* also rescans them completely, and a marker found there
        together with a core marker is the corruption of two current markers.

        :param job_id: Identify the job to find.
        :param kinds: The state kinds to accept the marker in; the core kinds when omitted.
        :return: The current marker, or ``None`` when the job has no marker in *kinds*.
        :raises ValueError: If *kinds* names an unknown state kind.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If more than one current marker identifies the job.
        """

        if kinds is None:
            return self._find_core_marker_by_id(job_id)
        selected = frozenset(kinds)
        unknown = selected - set(STATE_KINDS)
        if unknown:
            raise ValueError(f"unknown state kinds: {', '.join(sorted(unknown))}")
        extra = tuple(kind for kind in STATE_KINDS if kind in selected and kind not in CORE_STATE_KINDS)
        found = self._find_core_marker_by_id(job_id)
        if extra:
            for entry in Workspace.scan_marker_entries(self, extra):
                if isinstance(entry, MarkerFault):
                    self.report_marker_fault(entry)
                    continue
                if entry.job_id != job_id:
                    continue
                if found is not None:
                    raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
                found = entry
        return found if found is not None and found.kind in selected else None

    def _find_core_marker_by_id(self, job_id: str) -> Marker | None:
        """Run the index, probe and rescan ladder of :meth:`find_marker_by_id` over the core kinds."""

        index = self._marker_index
        if index is not None:
            entry = index.get(job_id)
            if entry is not None and job_id not in self._marker_duplicates:
                marker = self._index_marker(job_id, entry)
                if marker is not None:
                    index.move_to_end(job_id)
                    return marker
                # The cached location is stale. The job most likely only changed
                # kind, and ordinary transitions never change placement, so the
                # finite state set at that placement is the cheap next probe.
                index.pop(job_id, None)
                probed = self._probe_placement(job_id, entry.placement)
                if probed is not None:
                    self._index_note(probed)
                    return probed
        found = self._rebuild_marker_index(job_id)
        if job_id in self._marker_duplicates:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        return found

    def _probe_placement(self, job_id: str, placement: PurePosixPath) -> Marker | None:
        """Find one job id by checking every state kind at one placement."""

        state_root = self.control / "state"
        matches: list[Marker] = []
        for kind in CORE_STATE_KINDS:
            directory = self.state_directory(kind, placement)
            if not directory.is_dir():
                continue
            for path in directory.glob(f"*{job_id}.p???.g*.*"):
                if not path.is_file():
                    continue
                try:
                    marker = Marker.from_path(state_root, path)
                except (WorkflowError, ValueError) as exc:
                    self.report_marker_fault(MarkerFault(path=path, reason=str(exc)))
                    continue
                if marker.job_id == job_id:
                    matches.append(marker)
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        return matches[0] if matches else None

    def find_marker_at(self, job_key: str, placement: PurePosixPath) -> Marker | None:
        """Find *job_key* by checking the finite state set at a placement.

        This is the first rung of the resolution ladder: a join child carrying a
        placement hint is resolved here, without the index and without a scan.
        The index is used only as a shortcut when it already names this job at
        exactly this placement, which turns the bounded directory sweep below
        into one confirmed lookup.

        :param job_key: Identify the job to find.
        :param placement: Restrict the lookup to this placement.
        :return: The matching current marker, or ``None`` when none exists.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If more than one marker exists at the placement.
        """

        index = self._marker_index
        if index is not None:
            try:
                job_id = parse_job_key(job_key)[1]
            except FormatError:
                job_id = None
            entry = None if job_id is None else index.get(job_id)
            if job_id is not None and entry is not None and entry.placement == placement:
                marker = self._index_marker(job_id, entry)
                if marker is not None and marker.job_key == job_key:
                    return marker
        matches: list[Marker] = []
        state_root = self.control / "state"
        for kind in CORE_STATE_KINDS:
            directory = self.state_directory(kind, placement)
            if not directory.is_dir():
                continue
            for path in directory.glob(f"{job_key}.p???.g*.*"):
                if not path.is_file():
                    continue
                try:
                    matches.append(Marker.from_path(state_root, path))
                except (WorkflowError, ValueError) as exc:
                    self.report_marker_fault(MarkerFault(path=path, reason=str(exc)))
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_key} has multiple markers at {placement}")
        if matches:
            self._index_note(matches[0])
        return matches[0] if matches else None

    def load_job(self, marker: Marker) -> JobDefinition:
        """Load and validate the job definition referenced by a marker.

        ``job.json`` is read bounded and without following a symlink or
        blocking on a special file (:meth:`~httk.workflow.protocol.JobDefinition.from_path`), so a job
        that replaces its own definition fails instead of stalling the reader.

        :param marker: Identify the job payload to load.
        :return: The validated job definition.
        :raises httk.workflow.errors.FormatError: If ``job.json`` is unreadable, a symlink, not a
            regular file, too large or invalid, or its identity disagrees with the marker.
        """
        path = self.payload_path(marker.placement, marker.job_key) / "job.json"
        job = JobDefinition.from_path(path)
        if job.job_key != marker.job_key:
            raise FormatError("job.json identity disagrees with marker")
        if job.priority != marker.priority and marker.generation == 0:
            raise FormatError("submitted marker priority disagrees with job.json")
        return job

    def read_state(self, marker: Marker) -> dict[str, Any]:
        """Read and validate the state frame referenced by a marker.

        :param marker: Identify the state frame to read.
        :return: The validated state frame.
        :raises httk.workflow.errors.FormatError: If the marker's record reference is invalid.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If the journal frame disagrees with the marker.
        :raises httk.workflow.errors.WorkspaceUnavailableError: If the journal record remains incoherently visible.
        """
        if marker.record_ref == "init":
            return {
                "format": "httk-workflow-state",
                "format_version": 3,
                "workspace_id": self.workspace_id,
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "placement": placement_text(marker.placement),
                "state_generation": 0,
                "kind": "submitted",
                "previous_record_ref": None,
                "created_at": None,
                "priority": marker.priority,
            }
        frame = read_record(self.control, marker.record_ref, deadline_seconds=self.visibility_deadline)
        if (
            frame.get("format") != "httk-workflow-state"
            or frame.get("format_version") != 3
            or frame.get("workspace_id") != self.workspace_id
            or frame.get("job_key") != marker.job_key
            or frame.get("state_generation") != marker.generation
            or frame.get("kind") != marker.kind
        ):
            raise WorkspaceCorruptionError(f"state frame disagrees with marker {marker.path}")
        return frame

    def _inherited_origin(self, marker: Marker) -> str | None:
        """Return the ``origin`` a job's first frame inherits from its ``job.json`` parent, if any.

        A child of a job that arrived through the exchange inbox belongs to that
        tree: it carries the parent's exchange origin from its first frame, so
        the exchange's return pass takes it along, and never a job that did not
        (an orphan whose absent parent a client claims to be). A definition or
        parent that is absent or cannot be read passes nothing on, which only
        ever keeps a tree here.

        :param marker: The job's marker (its first frame is about to be written).
        :return: ``"exchange"``, or ``None``.
        """

        try:
            parent = self.load_job(marker).parent
            parent_id = None if parent is None else parent.get("job_id")
            if not isinstance(parent_id, str):
                return None
            parent_marker = self.find_marker_by_id(parent_id)
            if parent_marker is None:
                return None
            origin = self.read_state(parent_marker).get("origin")
        except (WorkflowError, OSError):
            return None
        return "exchange" if origin == "exchange" else None

    def transition(
        self,
        writer: JournalWriter,
        marker: Marker,
        kind: str,
        updates: Mapping[str, object],
        *,
        priority: int | None = None,
        allow_sealed: bool = False,
    ) -> Marker:
        """Append a state frame and atomically move *marker* to it.

        The provenance members of :data:`~httk.workflow.models.CARRIED_PROVENANCE_MEMBERS`
        are read from the current frame and repeated in the new one unless
        *updates* sets them. A current frame that cannot be read makes the
        transition fail rather than silently drop that provenance. A job's
        first frame (from its ``init`` marker) inherits its parent's exchange
        origin unless *updates* sets ``origin``.

        :param writer: Append the new state frame through this journal writer.
        :param marker: Identify the current marker to advance.
        :param kind: Select the next state kind.
        :param updates: Add state members to the new frame.
        :param priority: Override the marker priority when supplied.
        :param allow_sealed: Move a sealed job anyway, for the transfer paths whose
            seal travels with the payload rather than being changed underneath it.
        :return: The marker after the transition.
        :raises ValueError: If the next state kind is unknown.
        :raises httk.workflow.errors.SealedError: If the job, workspace, or project is sealed.
        :raises httk.workflow.errors.WorkspaceCorruptionError: If the state generation is exhausted,
            or the current frame disagrees with *marker*.
        :raises httk.workflow.errors.FormatError: If the current frame cannot be read.
        :raises httk.workflow.errors.TransitionLostError: If another actor moved the marker first.
        :raises httk.workflow.errors.WorkspaceUnavailableError: If the marker move cannot be resolved,
            or the current frame remains incoherently visible.
        """

        # allow_sealed bypasses only the job-level seal (the transfer paths carry
        # the seal with the payload); a sealed workspace or project always refuses.
        self._require_unsealed(None if allow_sealed else marker)
        next_priority = marker.priority if priority is None else priority
        generation = marker.generation + 1
        if generation > (1 << 64) - 1:
            raise WorkspaceCorruptionError("state generation exhausted")
        previous = None if marker.record_ref == "init" else self.read_state(marker)
        frame: dict[str, object] = {
            "format": "httk-workflow-state",
            "format_version": 3,
            "workspace_id": self.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": placement_text(marker.placement),
            "state_generation": generation,
            "kind": kind,
            "previous_record_ref": None if marker.record_ref == "init" else marker.record_ref,
            "created_at": utc_now(),
            "priority": next_priority,
        }
        if previous is not None:
            for name in CARRIED_PROVENANCE_MEMBERS:
                if name in previous:
                    frame[name] = previous[name]
        elif "origin" not in updates:
            # A job's first frame, however it is written (registered, failed,
            # cancelled or paused while submitted), inherits its parent's origin.
            origin = self._inherited_origin(marker)
            if origin is not None:
                frame["origin"] = origin
        frame.update(updates)
        record_ref = writer.append(frame)
        destination = self.marker_path(kind, marker.placement, marker.job_key, next_priority, generation, record_ref)
        self.ensure_directory(destination.parent)
        _LOGGER.debug(
            "moving marker %s from %s to %s at generation %d",
            marker.job_key,
            marker.kind,
            kind,
            generation,
        )
        return self._verified_marker_rename(marker, destination)

    def repoint_marker(self, writer: JournalWriter, marker: Marker, frame: Mapping[str, object]) -> Marker:
        """Publish a repair frame for *marker* and move the marker onto it.

        This is the repair counterpart of :meth:`transition`. The caller supplies
        the complete frame because what needs repairing is precisely the frame
        the marker references now, which cannot be read and therefore cannot be
        carried forward automatically. The frame must still name this marker's
        job and kind at the next generation, so a repair can never disguise a
        state change as a repair.

        :param writer: Append the repair frame through this journal writer.
        :param marker: Identify the damaged marker to repair.
        :param frame: Supply the complete replacement state frame.
        :return: The marker after the repair frame is published.
        :raises httk.workflow.errors.FormatError: If the repair frame changes required marker identity.
        """

        generation = marker.generation + 1
        expected: Mapping[str, object] = {
            "workspace_id": self.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "kind": marker.kind,
            "state_generation": generation,
        }
        for name, value in expected.items():
            if frame.get(name) != value:
                raise FormatError(f"a repair frame must keep {name} at {value!r}, not {frame.get(name)!r}")
        record_ref = writer.append(frame)
        destination = self.marker_path(
            marker.kind, marker.placement, marker.job_key, marker.priority, generation, record_ref
        )
        return self._verified_marker_rename(marker, destination)

    def _verified_marker_rename(self, marker: Marker, destination: Path) -> Marker:
        source = marker.path
        state_root = self.control / "state"
        last_error: OSError | None = None
        for attempt in visibility_attempts(self.visibility_deadline):
            try:
                self.ensure_directory(destination.parent)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "marker rename %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.is_file():
                moved = Marker.from_path(state_root, destination)
                if self.durable:
                    directories = (
                        (destination.parent,)
                        if destination.parent == source.parent
                        else (
                            destination.parent,
                            source.parent,
                        )
                    )
                    for directory in directories:
                        fsync_directory(directory)
                self._index_note(moved)
                return moved
            if source.is_file():
                _LOGGER.debug("marker %s is not yet visible at %s; retrying", source, destination)
                continue
            # Every kind counts: a winner that moved the marker to transferring
            # (a fence) must read as a lost race too, not as a marker that is
            # still on its way.
            current = self.find_marker_by_id(marker.job_id, kinds=STATE_KINDS)
            if current is not None and current.job_key == marker.job_key:
                raise TransitionLostError(f"another transition moved {source} to {current.path}")
            if current is not None:
                raise WorkspaceCorruptionError(f"job {marker.job_id} has markers under two job keys")
        detail = f": {last_error}" if last_error is not None else ""
        raise WorkspaceUnavailableError(f"cannot resolve marker rename {source} -> {destination}{detail}")

    def _publish_path(self, source: Path, destination: Path) -> None:
        last_error: OSError | None = None
        for attempt in visibility_attempts(self.visibility_deadline):
            try:
                self.ensure_directory(destination.parent)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "publication %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.exists():
                if not source.exists():
                    # Every marker publication that does not go through a
                    # transition — submission, child registration, transfer
                    # import — lands here, so this is where the index learns
                    # about it.
                    if self.durable:
                        # The rename installed a new name in the destination
                        # directory: a submitted or child marker, a published
                        # payload tree. A durable workspace synchronizes that
                        # directory entry so the publication survives a crash,
                        # not only a process interruption.
                        fsync_directory(destination.parent)
                    self._index_note_path(destination)
                    return
                try:
                    if source.samefile(destination):
                        continue
                except OSError:
                    pass
                raise FileExistsError(f"publication destination already exists: {destination}")
        detail = f": {last_error}" if last_error else ""
        raise WorkspaceUnavailableError(f"cannot resolve publication {source} -> {destination}{detail}")

    def submit(
        self,
        source: str | os.PathLike[str],
        placement: str | PurePosixPath,
        *,
        move: bool = False,
    ) -> Marker:
        """Copy or move a complete payload into the workspace and publish it.

        :param source: Locate the complete job payload to submit.
        :param placement: Select the job placement.
        :param move: Move the source instead of copying it.
        :return: The submitted job marker.
        :raises FileExistsError: If the target payload already exists.
        :raises httk.workflow.workspace.WorkspaceOperationError: If a move crosses filesystems.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        :raises httk.workflow.errors.FormatError: If the placement is invalid or names a job
            directory (:func:`~httk.workflow.protocol.check_job_placement`), or ``job.json`` is invalid,
            or, for a move, a symlink or not a regular file.
        """

        self._require_unsealed()
        normalized_placement = normalize_placement(placement)
        check_job_placement(normalized_placement)
        source_path = Path(source).resolve()
        definition = source_path / "job.json"
        if move and definition.is_symlink():
            # A moved payload keeps the link, and the manager never follows one.
            raise FormatError(
                f"cannot move {source_path} into the workspace: its job.json is a symlink; "
                "replace it with the file, or submit a copy instead"
            )
        # The source is the user's own directory, which a copy dereferences, so
        # a symlinked job.json there is followed unless the payload is moved.
        job = JobDefinition.from_path(definition, follow_symlinks=not move)
        target = self.payload_path(normalized_placement, job.job_key)
        if target.exists():
            raise FileExistsError(target)
        staging = self.control / "tmp" / f"submit.{uuid.uuid4()}"
        if move:
            try:
                os.rename(source_path, staging)
            except OSError as exc:
                if exc.errno == errno.EXDEV:
                    raise WorkspaceOperationError("move submission must remain on one filesystem") from exc
                raise
        else:
            shutil.copytree(source_path, staging, symlinks=False)
        self.ensure_directory(target.parent)
        self._publish_path(staging, target)
        temporary_marker = self.control / "tmp" / f"marker.{uuid.uuid4()}"
        temporary_marker.touch(exist_ok=False)
        if self.durable:
            fsync_directory(temporary_marker.parent)
        destination = self.marker_path("submitted", normalized_placement, job.job_key, job.priority, 0, "init")
        self._publish_path(temporary_marker, destination)
        return Marker.from_path(self.control / "state", destination)

    def validate_job_payload(self, marker: Marker) -> JobDefinition:
        """Perform manager-side immutable submission validation.

        :param marker: Identify the submitted payload to validate.
        :return: The validated job definition.
        """

        job = self.load_job(marker)
        return job

    def quarantine(self, path: Path, *, reason: str) -> Path:
        """Move a malformed protocol entry into the canonical quarantine.

        :param path: Locate the malformed protocol entry.
        :param reason: Record why the entry was quarantined.
        :return: The quarantine directory containing the entry.
        """

        identifier = f"{int(time.time())}-{uuid.uuid4()}"
        destination = self.control / "quarantine" / identifier
        self.ensure_directory(destination)
        moved = destination / "entry"
        os.rename(path, moved)
        # A quarantined marker leaves the state tree, so a cached location for it
        # would be a hit that resolves to nothing on every later lookup.
        try:
            self._index_forget(Marker.from_path(self.control / "state", path).job_id)
        except (WorkflowError, ValueError):
            pass
        write_json_atomic(
            destination / "report.json",
            {"original_path": str(path), "reason": reason, "quarantined_at": utc_now()},
            durable=self.durable,
        )
        _LOGGER.warning(
            "quarantined %s as %s: %s",
            path,
            destination,
            reason,
            extra={"event": "quarantine", "entry": str(path), "quarantine": str(destination)},
        )
        return destination

    def payload_digest(self, marker: Marker) -> str:
        """Return the digest of one payload, ignoring runner-private entries.

        :param marker: Identify the job payload to digest.
        :return: The payload digest.
        """

        return tree_digest(
            self.payload_path(marker.placement, marker.job_key),
            skip=is_payload_private,
        )

    def publish_request(self, request: Mapping[str, object]) -> Path:
        """Atomically publish an operator request.

        :param request: Supply the operator request to publish.
        :return: The ready request path.
        """

        temporary = self.control / "requests" / "tmp" / f"{uuid.uuid4()}.json"
        ready = self.control / "requests" / "ready" / temporary.name
        write_json_atomic(temporary, dict(request), durable=self.durable, mode=0o644)
        self._publish_path(temporary, ready)
        return ready


class MarkerStream:
    """A resumable, bounded, fair scandir walk of one active state kind.

    One stream serves one scheduling pass. It is workspace-level and reusable,
    but its cursor and rotation live only in the manager that holds it: nothing
    is written to disk, so two managers of one workspace never contend on a
    shared position, and a restarted manager simply begins a fresh cycle.

    Fairness is round-robin over the top-level placement roots the pass is
    assigned — or, with no assignment, the first-level components that exist
    under the kind. Each :meth:`advance` serves the roots in rotation, keeps a
    per-root resume position, and moves the rotation pointer on, so a subtree
    holding many markers can never starve a smaller sibling. Priority is therefore
    best-within-window rather than exact-global: bounded discovery
    is the whole point, and rotation is what guarantees the starved window is
    reached on a later tick.

    :param workspace: Provide the workspace whose state tree is scanned.
    :param kind: Select the active state kind to scan.
    :param prefixes: Restrict scanning to these placement prefixes.
    """

    def __init__(
        self,
        workspace: Workspace,
        kind: str,
        *,
        prefixes: Sequence[PurePosixPath] = (),
    ) -> None:
        self.workspace = workspace
        self.kind = kind
        self._prefixes = tuple(prefixes)
        # Root position -> the DFS position the last tick stopped after in that
        # root's subtree. A root absent from the map restarts from its
        # beginning, which is exactly what a completed cycle wants.
        self._cursors: dict[tuple[str, ...], tuple[str, ...]] = {}
        # The root the next advance begins its rotation at.
        self._rotation: tuple[str, ...] | None = None

    def _roots(self) -> list[tuple[str, ...]]:
        """Return the rotation roots, sorted, for one advance."""

        if self._prefixes:
            return sorted(prefix.parts for prefix in self._prefixes)
        base = self.workspace.control / "state" / self.kind
        entries = _scandir_sorted(base)
        roots: list[tuple[str, ...]] = [(entry.name,) for entry in entries if _safe_is_dir(entry)]
        # The empty root: markers of the empty placement sit directly in ``state/<kind>/``.
        if any(not _safe_is_dir(entry) and _marker_shaped(entry.name) for entry in entries):
            roots.append(())
        return sorted(roots)

    def advance(
        self,
        *,
        processing_budget: int,
        discovery_budget: int,
        heartbeat: Callable[[], None] | None = None,
        heartbeat_every: int = DISCOVERY_HEARTBEAT_STRIDE,
    ) -> list[Marker]:
        """Return up to *processing_budget* markers, visiting a bounded window.

        At most *processing_budget* markers are collected and at most
        *discovery_budget* *new* directory entries are visited; *heartbeat* is
        called every *heartbeat_every* entries examined, including the entries a
        resume re-lists on its way past the cursor. Roots are served in rotation
        with per-root resume, so the next tick continues where this one stopped
        and the rotation pointer advances so no root starves.

        A resume re-lists the entries at or before the cursor — ``scandir``
        cannot seek — so those are heartbeated but counted against neither the
        discovery budget nor the cursor. That keeps forward progress guaranteed:
        every advance either processes ``processing_budget`` markers past the
        cursor or exhausts the subtree, and never spends its whole budget
        re-skipping a directory it has already consumed.

        :param processing_budget: Bound the number of markers returned.
        :param discovery_budget: Bound the number of new directory entries visited.
        :param heartbeat: Call this function during a long walk.
        :param heartbeat_every: Set how many entries to examine between heartbeat opportunities.
        :return: The markers found within the bounded window.
        """

        roots = self._roots()
        if not roots:
            self._cursors.clear()
            self._rotation = None
            return []
        known = set(roots)
        self._cursors = {key: value for key, value in self._cursors.items() if key in known}
        start = bisect.bisect_left(roots, self._rotation) if self._rotation is not None else 0
        if start >= len(roots):
            start = 0
        order = roots[start:] + roots[:start]
        base = self.workspace.control / "state" / self.kind
        collected: list[Marker] = []
        visited = 0
        examined = 0
        last_root = order[0]
        for root in order:
            if len(collected) >= processing_budget or visited >= discovery_budget:
                break
            last_root = root
            after = self._cursors.get(root)
            reached_end = True
            for position, entry in self.workspace._iter_state_subtree(
                base.joinpath(*root), root, after, flat=not root and not self._prefixes
            ):
                examined += 1
                if heartbeat is not None and examined % heartbeat_every == 0:
                    heartbeat()
                if after is not None and position <= after:
                    # An entry the walk re-lists while seeking past the cursor:
                    # already consumed on an earlier advance, so it neither
                    # advances the cursor nor spends the discovery budget.
                    continue
                visited += 1
                self._cursors[root] = position
                if isinstance(entry, MarkerFault):
                    self.workspace.report_marker_fault(entry)
                elif isinstance(entry, Marker) and self.workspace._scheduling_includes(entry):
                    collected.append(entry)
                if len(collected) >= processing_budget or visited >= discovery_budget:
                    reached_end = False
                    break
            if reached_end:
                # This root's subtree was exhausted within the budget, so the
                # next cycle restarts it rather than resuming a spent cursor.
                self._cursors.pop(root, None)
        self._rotation = roots[(roots.index(last_root) + 1) % len(roots)]
        return collected


class WorkspaceOperationError(WorkspaceUnavailableError):
    """A workspace operation could not be completed."""
