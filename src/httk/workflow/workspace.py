"""Execution workspace creation, attachment, format, policy and application settings."""

import json
import logging
import os
import re
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from . import _fs
from ._kernel import OWNED
from ._state import UNOWNED_STATES
from ._util import json_bytes, utc_now
from .errors import FormatError, SealedError, UnsupportedExtensionError, WorkflowError
from .models import (
    CORE_PROFILE,
    EXCHANGE_EXTENSION,
    JOBS_DIRECTORY,
    SUPPORTED_EXTENSIONS,
    WORKSPACE_DIRECTORY,
    WorkspacePolicy,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .fsck import FsckReport
    from .gc import GcReport

_LOGGER = logging.getLogger(__name__)
#: The ``format.json`` ``layout`` of a workspace with ``jobs/<state>/`` job directories and owners (plan §3).
LAYOUT = "jobs-v3"
WORKFLOWS_DIRECTORY = "workflows"


#: The largest accepted ``format.json``.
_FORMAT_LIMIT = 1 << 20
#: How many times a ``format.json`` read-modify-write is redone when a concurrent writer replaced it.
_FORMAT_ATTEMPTS = 5


def _read_format(control: Path) -> dict[str, Any]:
    """Read ``<control>/format.json``, bounded and without following a symlink."""

    path = control / "format.json"
    try:
        data = _fs.read_bounded(_fs.loc(path), _FORMAT_LIMIT)
        value = None if data is None else json.loads(data)
    except (OSError, ValueError, _fs.UnsafePath) as exc:
        raise FormatError(f"cannot read JSON object {path}: {exc}") from exc
    if data is None:
        raise FormatError(f"cannot read JSON object {path}: it does not exist")
    if not isinstance(value, dict):
        raise FormatError(f"expected JSON object in {path}")
    return value


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


class Workspace:
    """Attach to one self-contained httk workflow filesystem workspace.

    :param root: Locate the workspace root.
    :param durable: Enable storage-crash durability for filesystem publications.
    :raises httk.workflow.errors.FormatError: If the workspace format or identity is invalid.
    :raises httk.workflow.errors.UnsupportedExtensionError: If the workspace uses unsupported extensions or a profile.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        durable: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        self.jobs = self.root / JOBS_DIRECTORY
        self.control = self.root / WORKSPACE_DIRECTORY
        self.durable = durable
        self.format = _read_format(self.control)
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

    def __repr__(self) -> str:
        return f"Workspace(root={str(self.root)!r}, workspace_id={self.workspace_id!r})"

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
            "exchange-jobs",
        ):
            (control / relative).mkdir(parents=True, exist_ok=True)
        for state in (*UNOWNED_STATES, OWNED):
            (root_path / JOBS_DIRECTORY / state).mkdir(parents=True, exist_ok=True)
        (root_path / WORKFLOWS_DIRECTORY).mkdir(exist_ok=True)
        document = {
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
        }
        _fs.write_file(_fs.loc(control / "format.json"), json_bytes(document) + b"\n", durable=durable)
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

    def _update_format[T](self, mutate: Callable[[dict[str, Any]], T]) -> T:
        """Read-modify-write ``format.json``: apply *mutate* to a fresh read, write it with ``write_file`` and
        verify by re-reading it, redoing all three when a concurrent writer replaced it in between.

        Writers are not serialized (there is no lock): one that read before this write may still replace it later.

        :param mutate: Changes the document in place; what it returns is returned.
        :return: What *mutate* returned for the document that was verified.
        :raises httk.workflow.errors.WorkflowError: When concurrent writers replaced every attempt.
        """

        for _ in range(_FORMAT_ATTEMPTS):
            stored = _read_format(self.control)
            result = mutate(stored)
            written = json_bytes(stored)
            _fs.write_file(_fs.loc(self.control / "format.json"), written + b"\n", durable=self.durable)
            if json_bytes(_read_format(self.control)) == written:
                self.format = stored
                return result
        raise WorkflowError(f"concurrent writers kept replacing {self.control / 'format.json'}; run the command again")

    def _add_extension(self, name: str) -> None:
        """Record *name* in ``format.json`` ``extensions`` (a verified read-modify-write) and re-read the extensions."""

        def add(stored: dict[str, Any]) -> None:
            extensions = stored.get("extensions", [])
            if not isinstance(extensions, list):
                raise FormatError("workspace extensions must be an array of strings")
            stored["extensions"] = sorted({*extensions, name})

        self._update_format(add)
        self.refresh_format()

    def set_policy(self, changes: Mapping[str, object]) -> WorkspacePolicy:
        """Validate *changes*, merge them into the stored policy, and publish it.

        The write is a read-modify-write of ``format.json`` through an
        exclusively created temporary file and a rename, so a reader never sees
        a torn object, verified by re-reading it and redone when a concurrent
        writer replaced it. It is deliberately not serialized against another
        writer: policy is administrative, changes are rare, and last writer wins.

        :param changes: Supply policy values to validate and merge.
        :return: The resulting workspace policy.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        from .seals import require_cli_modifiable

        require_cli_modifiable(self)

        def merge(stored: dict[str, Any]) -> WorkspacePolicy:
            merged = WorkspacePolicy.from_mapping(_section(stored, "policy")).updated(changes)
            stored["policy"] = merged.as_mapping()
            return merged

        merged = self._update_format(merge)
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

        stored = _read_format(self.control)
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
        so a reader never sees a torn object, verified by re-reading, and last
        writer wins.

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

        from .seals import require_cli_modifiable

        require_cli_modifiable(self)

        def configure(stored: dict[str, Any]) -> dict[str, object]:
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
            return dict(settings)

        return self._update_format(configure)

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

        def seed(stored: dict[str, Any]) -> dict[str, object]:
            current = _validate_settings(_section(stored, "settings"))
            for key, value in merged.items():
                if any(
                    existing != key and _setting_variable_name(existing) == _setting_variable_name(key)
                    for existing in current
                ):
                    continue
                current.setdefault(key, value)
            stored["settings"] = current
            return dict(current)

        return self._update_format(seed)

    def read_workflow_preludes(self) -> dict[str, str]:
        """Read and validate the workflow-in-workspace preludes from disk.

        A workflow prelude is shell text run to initialize the environment
        before each launch of a runner for that workflow.

        :return: The current map of workflow id to prelude text.
        :raises httk.workflow.errors.FormatError: If the stored preludes are not valid.
        """

        return _validate_workflow_preludes(_section(_read_format(self.control), "workflow_preludes"))

    def set_workflow_prelude(self, workflow_id: str, value: str) -> dict[str, str]:
        """Store one workflow prelude and return the resulting map.

        The write is the same read-modify-write of ``format.json`` that
        :meth:`set_setting` uses: an exclusively created temporary and a rename,
        so a reader never sees a torn object, verified by re-reading, and last
        writer wins.

        :param workflow_id: Name the workflow whose prelude to store.
        :param value: Supply the shell prelude text.
        :return: The resulting map of workflow id to prelude text.
        :raises ValueError: If the workflow id or prelude value is invalid.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        from .seals import require_cli_modifiable

        require_cli_modifiable(self)
        _validate_workflow_prelude_id(workflow_id)
        _validate_workflow_prelude_value(workflow_id, value)

        def set_prelude(stored: dict[str, Any]) -> dict[str, str]:
            preludes = _validate_workflow_preludes(_section(stored, "workflow_preludes"))
            preludes[workflow_id] = value
            stored["workflow_preludes"] = preludes
            return dict(preludes)

        return self._update_format(set_prelude)

    def unset_workflow_prelude(self, workflow_id: str) -> dict[str, str]:
        """Remove one workflow prelude, refusing one that is not set.

        :param workflow_id: Name the workflow whose prelude to remove.
        :return: The resulting map of workflow id to prelude text.
        :raises ValueError: If the prelude is not set.
        :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
        """

        from .seals import require_cli_modifiable

        require_cli_modifiable(self)

        def unset_prelude(stored: dict[str, Any]) -> dict[str, str]:
            preludes = _validate_workflow_preludes(_section(stored, "workflow_preludes"))
            if workflow_id not in preludes:
                raise ValueError(f"workflow prelude is not set: {workflow_id}")
            del preludes[workflow_id]
            stored["workflow_preludes"] = preludes
            return dict(preludes)

        return self._update_format(unset_prelude)

    def check(self, *, repair: bool = False) -> "FsckReport":
        """Check the workspace's job tree (:func:`httk.workflow.fsck.check_workspace`) while nothing else uses it.

        :param repair: Repair what the check can (refused unless the workspace is quiescent).
        :return: The workspace check report.
        :raises httk.workflow.errors.WorkflowError: With *repair*, unless the workspace is quiescent.
        """

        from .fsck import check_workspace

        return check_workspace(self, repair=repair)

    def collect_garbage(
        self,
        *,
        dry_run: bool = False,
        now: float | None = None,
        categories: Sequence[str] | None = None,
        sizes: bool = True,
    ) -> "GcReport":
        """Collect the disk this workspace's retention policy permits freeing.

        :param dry_run: Report eligible removals without changing the workspace.
        :param now: Use this timestamp when evaluating retention deadlines.
        :param categories: Restrict collection to these categories.
        :param sizes: Whether to calculate byte estimates.
        :return: The garbage-collection report.
        """

        from .gc import collect_garbage

        return collect_garbage(
            self,
            dry_run=dry_run,
            now=now,
            categories=categories,
            sizes=sizes,
        )
