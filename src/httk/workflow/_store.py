"""The workspace store of installed workflows: ``workflows/<slug>--<h16>/``.

A job references a workflow by id, and that workflow (and the transitive
closure of its ``[workflow.calls]``) must be installed here before a manager
starts the job. One installation is::

    workflows/<slug>--<h16>/
    ├── install.json        format httk-workflow-install, version 1
    ├── package/            the package tree (sources only, no build artifacts, no .git)
    └── builds/<platform>/  build.json and artifacts/ of one platform's build

``h16`` is ``sha256(id)[:16]`` and ``slug`` the sanitized short name. Ids are
a commit-pinned git URI, ``local:<name>`` or ``adhoc:<stem>@<sha12>``.

Install, uninstall and build provide **no concurrency protection**, by
decision: users do not change workflows that jobs are using, and two
operators installing the same id concurrently is not handled. Every tree is
assembled in an owner scratch and moved into place with
:func:`httk.workflow._fs.move_owned`. A replaced installation is first moved
aside to ``<slug>--<h16>.old.<token>/``. A crash between the two renames
leaves only that moved-aside tree, which lookups then use as the installation
and ``httk workspace gc`` moves back into place; gc removes a moved-aside tree
only once the installation exists. A replaced or removed installation is
discarded from the owner's scratch.

:func:`install_copy`, which installs another workspace's store entry under its
id, is the exception: it never replaces an installation, and its final rename
fails on an occupied name, so of concurrent copies of one id the first wins.
"""

import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import signal
import stat
import subprocess
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from httk.core.building import DEFAULT_TAG, BuildSpec, execute_build
from httk.core.digests import sha256_file, tree_digest
from httk.core.git_sources import parse_git_uri

from httk.workflow import _fs, git_workflows, scaffold
from httk.workflow._job import JobDefinition
from httk.workflow._kernel import KernelWorkspace, Owner, reconcile_scratch
from httk.workflow._state import _thaw
from httk.workflow.errors import RunnerResolutionError, WorkflowError
from httk.workflow.models import (
    EMBEDDED_WORKFLOW_PREFIX,
    JOB_WORKFLOW_DIRECTORY,
    WORKFLOW_ID_PREFIXES,
    is_workflow_id,
)
from httk.workflow.packages import (
    MANIFEST_NAME,
    artifact_excluder,
    parse_workflow_manifest,
    read_build_spec,
    source_tree_digest,
)
from httk.workflow.scaffold import WorkflowProvider

__all__ = [
    "RUNNER_COPY",
    "ClosureReport",
    "Installed",
    "build",
    "built",
    "closure",
    "embedded",
    "install",
    "install_copy",
    "list_installed",
    "lookup",
    "platform_tag",
    "resolve_calls",
    "runner_copy",
    "slot_default_notices",
    "source_digest",
    "stale_replacements",
    "uninstall",
]

_LOGGER = logging.getLogger(__name__)
_FORMAT = "httk-workflow-install"
_BUILD_FORMAT = "httk-workflow-build"
_RECORD_LIMIT = 1 << 20
_BUILTINS = frozenset({"cwl", "pwd", "jobflow", "httk-v1"})
# The one workflow variable a build sees: the installed language SDK directory.
_LANGUAGES_DIR = {"HTTK_WORKFLOW_LANGUAGES_DIR": str(Path(__file__).with_name("languages"))}
_PLATFORM_PROBE_TIMEOUT = 30.0
_PROBE_STDERR_TAIL = 1024
_PLATFORM_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], str] = {}
_INSTALLATION = re.compile(r"[a-z0-9._-]*--[0-9a-f]{16}")
_REPLACED = re.compile(r"[a-z0-9._-]*--[0-9a-f]{16}\.old\.[a-z2-7]{16}")
#: Where an unlocked job keeps its own copy of its installation's package: a top-level payload member.
RUNNER_COPY = JOB_WORKFLOW_DIRECTORY


@dataclass(frozen=True)
class Installed:
    """One installed workflow.

    :param id: The workflow id.
    :param name: The short name.
    :param directory: ``workflows/<slug>--<h16>/``.
    :param record: The ``install.json`` content.
    :param package_override: The package tree used instead of the installed one (an unlocked job's runner
        copy, see :func:`runner_copy`), or ``None``; builds stay the installation's.
    :param embedded: Whether this is a job's embedded workflow (see :func:`embedded`), which has no store tree or
        builds: :attr:`installed_package` and :meth:`build_dir` raise ``ValueError``.
    """

    id: str
    name: str
    directory: Path
    record: Mapping[str, object]
    package_override: Path | None = None
    embedded: bool = False
    _cache: dict[str, WorkflowProvider] = field(default_factory=dict, init=False, repr=False, compare=False)

    @property
    def installed_package(self) -> Path:
        """The installed package tree in the store, also under an override: the only one a manager reads."""

        if self.embedded:
            raise ValueError(f"the embedded workflow {self.id} has no store installation")
        return self.directory / "package"

    @property
    def package(self) -> Path:
        """The package tree: the installed one, or the override."""

        return self.installed_package if self.package_override is None else self.package_override

    def provider(self) -> WorkflowProvider:
        """Return the parsed provider of the installed package, whose ``workflow_id`` is this id.

        :return: The provider (parsed once per instance).
        :raises ValueError: If the installed manifest is invalid.
        """

        if "provider" not in self._cache:
            self._cache["provider"] = parse_workflow_manifest(self.package, _uri=self.id)
        return self._cache["provider"]

    def build_dir(self, platform: str) -> Path | None:
        """Return ``builds/<platform>/`` when this platform has a registered build.

        :param platform: The platform tag (see :func:`platform_tag`).
        :return: The build directory, or ``None`` when not built.
        :raises ValueError: For an embedded workflow, which is never built.
        """

        if self.embedded:
            raise ValueError(f"the embedded workflow {self.id} is never built")
        path = self.directory / "builds" / platform
        return path if (path / "build.json").is_file() and not path.is_symlink() else None


def runner_copy(installed: Installed, payload: Path) -> Installed | None:
    """Return *installed* as launched from the job's own runner copy (:data:`RUNNER_COPY`), when it has one.

    :param installed: The job's effective installation.
    :param payload: The job directory.
    :return: The installation with its package overridden by the copy, or ``None`` without a copy (or when
        the copy is a symlink or not a directory: never followed).
    """

    try:
        os.close(_fs.open_dir_under(payload, RUNNER_COPY))
    except (FileNotFoundError, _fs.UnsafePath):
        return None
    return replace(installed, package_override=payload / RUNNER_COPY)


def embedded(job: JobDefinition, payload: Path, *, require_tree: bool = True) -> Installed | None:
    """Return the view of the workflow *job* embeds in its own ``workflow/``, when it embeds one.

    The view is derived from *job* and *payload* alone: its ``directory`` and package are ``<payload>/workflow``,
    its ``record`` is a plain copy of ``job.json``'s ``embedded`` member with ``build: false``, and it has no store
    tree or builds. Two jobs may embed different trees under one id, so a view is never cached or looked up by id.
    Nothing is read from the tree here; verify it with :func:`httk.workflow._embedded.embedded_digest` before any
    trusted use.

    :param job: The job.
    :param payload: Its payload directory.
    :param require_tree: Return ``None`` when ``workflow/`` is missing, a symlink or not a directory (never
        followed); without it, the view of the record alone is returned whatever the tree.
    :return: The view, or ``None`` when the job embeds no workflow (or the tree is required and unusable).
    """

    if job.embedded is None:
        return None
    try:
        if require_tree:
            os.close(_fs.open_dir_under(payload, RUNNER_COPY))
    except (FileNotFoundError, _fs.UnsafePath):
        return None
    tree = payload / RUNNER_COPY
    record = {**cast(dict[str, object], _thaw(job.embedded)), "build": False}
    return Installed(job.workflow_id, job.workflow_name, tree, record, package_override=tree, embedded=True)


@dataclass(frozen=True)
class ClosureReport:
    """What a workflow's transitive call closure lacks.

    :param missing: Ids (or unresolved references) that are not installed.
    :param unbuilt: Installed ids with a build section but no build for the platform asked about.
    """

    missing: tuple[str, ...]
    unbuilt: tuple[str, ...]

    @property
    def ok(self) -> bool:
        """Whether nothing is missing or unbuilt."""

        return not self.missing and not self.unbuilt


def _workflows(workspace: KernelWorkspace) -> Path:
    return workspace.root / "workflows"


def _h16(workflow_id: str) -> str:
    return hashlib.sha256(workflow_id.encode()).hexdigest()[:16]


def _dirname(workflow_id: str, name: str) -> str:
    return f"{re.sub(r'[^a-z0-9._-]', '-', name.lower())[:48]}--{_h16(workflow_id)}"


def _read(directory: Path) -> Installed | None:
    try:
        data = _fs.read_bounded(_fs.loc(directory / "install.json"), _RECORD_LIMIT)
        record = None if data is None else json.loads(data)
    except (OSError, ValueError, RecursionError) as exc:
        _LOGGER.debug("skipping unreadable installation %s: %s", directory, exc)
        return None
    if (
        not isinstance(record, dict)
        or record.get("format") != _FORMAT
        or not isinstance(record.get("id"), str)
        or not isinstance(record.get("name"), str)
    ):
        return None
    return Installed(record["id"], record["name"], directory, record)


def _subdirectories(path: Path) -> list[Path]:
    try:
        with os.scandir(path) as scan:
            return sorted(path / entry.name for entry in scan if entry.is_dir(follow_symlinks=False))
    except FileNotFoundError:
        return []


def _ctime(path: Path) -> float | None:
    try:
        return os.lstat(path).st_ctime
    except FileNotFoundError:
        return None


def _only_copies(directories: list[Path]) -> dict[str, Path]:
    # A crash between a reinstall's two renames leaves <slug>--<h16>.old.<token> and no <slug>--<h16>: the newest
    # such tree is then the installation.
    names = {path.name for path in directories}
    aside = [
        (moved, path)
        for path in directories
        if _REPLACED.fullmatch(path.name)
        and path.name.partition(".old.")[0] not in names
        and (moved := _ctime(path)) is not None
    ]
    # Oldest first, so the newest tree of each installation is the one kept.
    return {path.name.partition(".old.")[0]: path for _, path in sorted(aside)}


def _installations(workspace: KernelWorkspace) -> list[Path]:
    directories = _subdirectories(_workflows(workspace))
    primaries = [path for path in directories if _INSTALLATION.fullmatch(path.name)]
    return sorted([*primaries, *_only_copies(directories).values()])


def stale_replacements(workspace: KernelWorkspace, cutoff: float, *, writable: bool) -> list[Path]:
    """Return the trees a reinstall moved aside, ``workflows/<slug>--<h16>.old.<token>/``, that may be removed.

    A moved-aside tree whose installation is missing is the only copy (a crash between the reinstall's two
    renames): it is never returned, and with *writable* the newest one is moved back into place.

    :param workspace: The workspace.
    :param cutoff: Only entries moved aside (their ``ctime``) before this epoch time.
    :param writable: Prepare for removal (not a dry run): restore an only copy, and make each returned entry's
        directories writable, so a removal that does not chmod succeeds.
    :return: The entries.
    """

    directories = _subdirectories(_workflows(workspace))
    if writable:
        for primary, path in _only_copies(directories).items():
            if (_ctime(path) or cutoff) < cutoff:
                try:
                    _fs.move_owned(_fs.loc(path), _fs.loc(path.with_name(primary)), durable=workspace.durable)
                except _fs.MoveFailed as exc:
                    # A reinstall landed meanwhile (store operations are not concurrency-protected): later sweep.
                    _LOGGER.warning("cannot move %s back into place: %s", path, exc)
        directories = _subdirectories(_workflows(workspace))
    restoring = set(_only_copies(directories).values())
    stale = []
    for path in directories:
        moved = _ctime(path)
        if _REPLACED.fullmatch(path.name) and path not in restoring and moved is not None and moved < cutoff:
            if writable:
                _writable(path)
            stale.append(path)
    return stale


def list_installed(workspace: KernelWorkspace) -> list[Installed]:
    """List the installed workflows, skipping directories without a readable ``install.json``.

    An installation a crashed reinstall left only moved aside (``<slug>--<h16>.old.<token>/``) is listed there.

    :param workspace: The workspace.
    :return: The installations, in directory-name order.
    """

    return [found for directory in _installations(workspace) if (found := _read(directory)) is not None]


def _git_reference(text: str) -> str | None:
    """Return the tag or branch a ``git+…`` source text names; ``None`` for a commit, no ref or another source."""

    if not text.startswith("git+"):
        return None
    uri = parse_git_uri(text)
    return None if uri.pinned else uri.ref


def _selected_by(found: Installed, ref: str) -> bool:
    """Whether *ref* is the reference *found* was installed from, or a prefix (7+ digits) of its pinned commit."""

    if found.record.get("reference") == ref:
        return True
    try:
        commit = parse_git_uri(found.id).ref if found.id.startswith("git+") else None
    except ValueError:
        return False
    return len(ref) >= 7 and commit is not None and commit.startswith(ref.lower())


def _installed_order(found: Installed) -> tuple[float, str]:
    try:
        stamp = datetime.fromisoformat(cast(str, found.record.get("installed_at"))).timestamp()
    except (TypeError, ValueError):
        stamp = float("-inf")
    return stamp, found.directory.name


def _candidates(installed: list[Installed], text: str) -> list[Installed]:
    """Return the installations a short name, else ``NAME@REF``, selects."""

    named = [found for found in installed if found.name == text]
    name, _, ref = text.rpartition("@")
    if not named and name and ref:
        named = [found for found in installed if found.name == name and _selected_by(found, ref)]
    return named


def lookup(workspace: KernelWorkspace, id_or_name: str) -> Installed | None:
    """Return the installation with this exact id, else the newest one with this short name.

    Id-shaped text (``local:``, ``adhoc:``, ``git+``) only ever matches an id, and a job's ``embedded:`` id never
    matches anything: an embedded workflow is never installed. Text that no installation
    has as its short name is read as ``NAME@REF``, which selects among the installations of ``NAME`` by the
    tag or branch an installation was made from, or by a prefix of at least seven characters of its pinned
    commit. Several installations selected alike resolve to the most recently installed one.

    :param workspace: The workspace.
    :param id_or_name: An id, a short name, or ``NAME@REF``.
    :return: The installation, or ``None``.
    """

    if id_or_name.startswith(EMBEDDED_WORKFLOW_PREFIX):
        return None
    # The slug of a git id's short name is not derivable from the id, so the
    # listing is scanned for the h16 suffix; only matching install.json files are read.
    suffix = f"--{_h16(id_or_name)}"
    for directory in _installations(workspace):
        stem = directory.name.partition(".old.")[0]
        if stem.endswith(suffix) and (found := _read(directory)) is not None and found.id == id_or_name:
            return found
    if id_or_name.startswith(WORKFLOW_ID_PREFIXES):  # id-shaped text never selects by short name (see _resolve)
        return None  # a short name spelled like an id must not stand in for an uninstalled id
    return max(_candidates(list_installed(workspace), id_or_name), key=_installed_order, default=None)


def closure(
    workspace: KernelWorkspace, workflow_id: str, *, check_builds: bool, also: Iterable[str] = ()
) -> ClosureReport:
    """Check that a workflow and its transitive calls are installed (and built for this platform).

    With *check_builds*, each installed package with a ``[workflow.build]`` is
    unbuilt when it has no build for its own :func:`platform_tag`, since
    packages differ in their platform probe.

    :param workspace: The workspace.
    :param workflow_id: The root workflow id (or name); a job's ``embedded:`` id is never reported missing (its
        fixed calls belong in *also*).
    :param check_builds: Also report packages without a build for this platform.
    :param also: Further roots whose closures are joined in, such as the workflows a job's call slots are bound to.
    :return: The missing and unbuilt ids.
    :raises httk.workflow.errors.RunnerResolutionError: If a package's platform probe fails.
    :raises ValueError: If an installed package's build section is malformed.
    """

    missing: list[str] = []
    unbuilt: list[str] = []
    for reference, found in resolve_calls(workspace, [workflow_id, *also]).items():
        if found is None:
            if not reference.startswith(EMBEDDED_WORKFLOW_PREFIX):  # a job's own workflow, never in the store
                missing.append(reference)
            continue
        spec = read_build_spec(found.package) if check_builds and found.record.get("build") is True else None
        if spec is not None and found.build_dir(platform_tag(spec)) is None:
            unbuilt.append(found.id)
    return ClosureReport(tuple(sorted(missing)), tuple(sorted(unbuilt)))


def built(found: Installed) -> bool | None:
    """Report whether an installation's ``[workflow.build]`` has a build for this platform.

    :param found: The installation.
    :return: ``None`` without a build section; ``False`` also when its build section or platform probe fails.
    """

    if found.record.get("build") is not True:
        return None
    try:
        spec = read_build_spec(found.package)
        return spec is None or found.build_dir(platform_tag(spec)) is not None
    except (RunnerResolutionError, ValueError, OSError) as exc:
        _LOGGER.info("workflow %s counts as unbuilt: %s", found.id, exc)
        return False


def resolve_calls(
    workspace: KernelWorkspace, workflow_ids: Iterable[str], *, include_slot_defaults: bool = False
) -> dict[str, Installed | None]:
    """Resolve workflows and the transitive closure of their recorded fixed ``[workflow.calls]``.

    :param workspace: The workspace.
    :param workflow_ids: The root workflow ids (or names).
    :param include_slot_defaults: Also follow the recorded default of every call slot.
    :return: Every reference met, to its installation (see :func:`lookup`) or ``None`` when not installed.
    """

    resolved: dict[str, Installed | None] = {}
    pending = list(workflow_ids)
    while pending:
        reference = pending.pop()
        if reference in resolved:
            continue
        found = resolved[reference] = lookup(workspace, reference)
        calls = None if found is None else found.record.get("calls")
        if isinstance(calls, dict):
            pending.extend(str(callee) for callee in calls.values())
        slots = None if found is None or not include_slot_defaults else found.record.get("slots")
        if isinstance(slots, dict):
            pending.extend(str(default) for default in slots.values() if default is not None)
    return resolved


def _environment() -> dict[str, str]:
    """Return the build environment without workflow runtime variables, except the language SDK directory."""

    environment = {key: value for key, value in os.environ.items() if not key.startswith("HTTK_WORKFLOW_")}
    environment.update(_LANGUAGES_DIR)
    return environment


def platform_tag(spec: BuildSpec) -> str:
    """Return the local platform tag declared by a build specification.

    The probe runs exactly as :func:`httk.core.building.platform_tag` runs it,
    with the build environment and the same tag derivation, so it names the
    directory a build was registered under, but bounded: standard input is
    ``/dev/null``, the working directory is ``/``, and a probe still running
    after 30 seconds is killed with its process group. Tags are memoized per
    probe command and environment.

    :param spec: The build specification naming the probe.
    :return: The sanitized platform tag (``any`` without a probe).
    :raises httk.workflow.errors.RunnerResolutionError: ``runner_build_failed`` if the probe cannot
        run or fails, ``runner_not_built`` if it does not finish in time.
    """

    if spec.platform is None:
        return DEFAULT_TAG
    environment = _environment()
    cache_key = (spec.platform, tuple(sorted(environment.items())))
    if (cached := _PLATFORM_CACHE.get(cache_key)) is not None:
        return cached
    try:
        argv = shlex.split(spec.platform)
        if not argv:
            raise ValueError("empty command")
        with subprocess.Popen(
            argv,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd="/",
            text=True,
            start_new_session=True,
        ) as process:
            try:
                raw, stderr = process.communicate(timeout=_PLATFORM_PROBE_TIMEOUT)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except OSError:
                    pass
                process.wait()
                raise RunnerResolutionError(
                    "runner_not_built",
                    f"platform probe {spec.platform!r} did not finish within {_PLATFORM_PROBE_TIMEOUT:g} s "
                    "and was killed; no build can be selected without its tag",
                ) from None
    except (OSError, ValueError) as exc:
        raise RunnerResolutionError("runner_build_failed", f"platform probe {spec.platform!r} failed: {exc}") from exc
    if process.returncode != 0:
        tail = stderr[-_PROBE_STDERR_TAIL:].strip()
        detail = f"; stderr: {tail}" if tail else ""
        raise RunnerResolutionError(
            "runner_build_failed",
            f"platform probe {spec.platform!r} failed with exit code {process.returncode}{detail}",
        )
    # The tag derivation of httk.core.building.platform_tag, which execute_build uses.
    digest = hashlib.sha256(raw.encode()).hexdigest()
    tag = re.sub(r"[^A-Za-z0-9._-]", "-", raw.strip())
    tag = "h" + digest[:16] if not tag or tag in {".", ".."} or len(tag) > 64 else f"{tag}.{digest[:8]}"
    _PLATFORM_CACHE[cache_key] = tag
    return tag


def _writable(root: Path) -> None:
    # Copies keep read-only source modes and builds may leave read-only trees; _fs removal does not chmod.
    for directory, _, _ in os.walk(root):
        mode = os.lstat(directory).st_mode
        if stat.S_ISDIR(mode):
            os.chmod(directory, mode | stat.S_IRWXU)


@contextmanager
def _scratch(owner: Owner) -> Iterator[Path]:
    path = owner.scratch("build")
    try:
        yield path
    finally:
        _writable(path)
        reconcile_scratch(owner, path)


def _not_source(spec: BuildSpec | None) -> Callable[[str], bool]:
    """The relative paths of a package that are not its sources: ``.git`` anywhere and the declared artifacts."""

    is_artifact = artifact_excluder(spec)
    return lambda relative: relative.rpartition("/")[2] == ".git" or is_artifact(relative)


def source_digest(package: Path, *, declared_by: Path | None = None) -> str:
    """Return the ``tree_sha256`` of a package's sources: the tree as :func:`_copy_package` copies it.

    :param package: The package tree.
    :param declared_by: The package whose ``[workflow.build]`` declares the artifacts to leave out (*package*
        by default); a manager digests an unlocked job's runner copy with the store installation's declaration,
        never parsing the job-writable copy.
    :return: The digest.
    :raises ValueError: If the tree holds a symlink or special file, or the build section is malformed.
    """

    return tree_digest(package, exclude=_not_source(read_build_spec(declared_by or package)))


def _copy_package(source: Path, destination: Path, spec: BuildSpec | None) -> None:
    """Copy a package's sources, refusing symlinks and special files, skipping ``.git`` and build artifacts."""

    skipped_source = _not_source(spec)

    def ignore(directory: str, names: list[str]) -> set[str]:
        skipped: set[str] = set()
        for name in names:
            path = Path(directory, name)
            relative = path.relative_to(source).as_posix()
            if skipped_source(relative):
                skipped.add(name)
            elif not (stat.S_ISREG(mode := os.lstat(path).st_mode) or stat.S_ISDIR(mode)):
                raise ValueError(f"workflow package {source} contains a symlink or special file: {relative}")
        return skipped

    shutil.copytree(source, destination, ignore=ignore)
    _writable(destination)


def _build_into(
    package: Path, spec: BuildSpec, builds: Path, work: Path, stamp: Mapping[str, object], *, durable: bool
) -> Path:
    """Build a copy of *package* in *work* and register its artifacts as ``builds/<tag>/``."""

    source = work / "build-src"
    shutil.copytree(package, source)
    _writable(source)
    result = execute_build(source, spec, strip_env_prefixes=("HTTK_WORKFLOW_",), env=_LANGUAGES_DIR)
    target = builds / result.tag
    for relative in result.artifact_files:
        destination = target / "artifacts" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / relative, destination)
    document = {
        "format": _BUILD_FORMAT,
        "format_version": 1,
        **stamp,
        "command": spec.command,
        "platform": spec.platform,
        "platform_output": result.platform_output,
        "platform_tag": result.tag,
        "built_at": datetime.now(UTC).isoformat(),
    }
    _fs.write_file(_fs.loc(target / "build.json"), _encode(document), durable=durable)
    return target


def _encode(value: object) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


#: The instantiate member of an ad hoc package whose runner declares an SDK instantiate hook.
_ADHOC_INSTANTIATE = """# The instantiate hook of the ad hoc runner `run` beside this file (generated at install).
from pathlib import Path

from httk.workflow.scaffold import _runner_instantiate

instantiate = _runner_instantiate(Path(__file__).with_name("run"))
"""


def _adhoc_package(runner: Path, destination: Path, initial_step: str | None) -> tuple[str, str]:
    """Write a one-runner package for a runner file, carrying its described inputs and hook; return its id and name."""

    preserve = runner.suffix in {".sh", ".bash"} or scaffold._has_bash_shebang(runner)
    described = scaffold.describe_runner(runner, preserve_registration_order=preserve)
    steps = tuple(cast(list[str], described["steps"]))
    initial = scaffold._initial_step(runner, steps, initial_step)
    destination.mkdir()
    shutil.copyfile(runner, destination / "run")
    os.chmod(destination / "run", 0o755)
    # JSON strings and arrays are valid TOML basic strings, quoted keys and arrays.
    manifest = (
        f"[workflow]\nname = {json.dumps(runner.stem)}\n\n"
        f"[workflow.runner]\nsteps = {json.dumps(list(steps))}\ninitial_step = {json.dumps(initial)}\n"
    )
    for name, target in cast(dict[str, str | None], described.get("inputs", {})).items():
        manifest += f"\n[workflow.inputs.{json.dumps(name)}]\n"
        manifest += "" if target is None else f"destination = {json.dumps(target)}\n"
    if described.get("instantiate"):
        if preserve:
            raise ValueError(f"the instantiate hook is Python-SDK-only: {runner}")
        (destination / "instantiate.py").write_text(_ADHOC_INSTANTIATE, encoding="utf-8")
        manifest += '\n[workflow.instantiate]\nfile = "instantiate.py"\n'
    (destination / MANIFEST_NAME).write_text(manifest, encoding="utf-8")
    return f"adhoc:{runner.stem}@{sha256_file(runner)[:12]}", runner.stem


def _document_package(source: Path, language: str | None, destination: Path) -> tuple[str, str] | None:
    """Write a package for a bare format document (or document directory); ``None`` when it is not one.

    The package names the document and declares its ports as inputs and record outputs, so
    a job of it is prepared by the format's realization and run by its built-in runner.
    """

    from httk.workflow import compat

    if language is None:
        lang = compat.match_document(source) if source.is_file() else None
        if lang is None:
            return None
    else:
        lang = compat.language(language)
        if (lang.document_policy == "forbidden") != source.is_dir():
            kind = "a directory" if lang.document_policy == "forbidden" else "a document file"
            raise ValueError(f"workflow format {lang.name!r} expects {kind}: {source}")
    stem = source.name if source.is_dir() else source.stem
    name = f"{lang.name}.{scaffold._sanitize_tag(stem) or 'document'}"
    ports = lang.ports(source)
    if source.is_dir():
        shutil.copytree(source, destination, symlinks=True)
        _writable(destination)
        document, digest = "", source_tree_digest(destination)
    else:
        destination.mkdir()
        shutil.copyfile(source, destination / source.name)
        document, digest = f"document = {json.dumps(source.name)}\n", sha256_file(source)
    # JSON strings are valid TOML basic strings and quoted keys.
    manifest = (
        f"[workflow]\nname = {json.dumps(name)}\n"
        f"description = {json.dumps(f'the {lang.name} document {source.name}')}\n\n"
        f"[workflow.runner]\nformat = {json.dumps(lang.name)}\n{document}"
    )
    manifest += "".join(f"\n[workflow.inputs.{json.dumps(port)}]\n" for port in ports.inputs)
    manifest += "".join(f'\n[workflow.outputs.{json.dumps(port)}]\nentry_type = "records"\n' for port in ports.outputs)
    (destination / MANIFEST_NAME).write_text(manifest, encoding="utf-8")
    return f"adhoc:{name}@{digest[:12]}", name


def _resolve(
    source: str | os.PathLike[str],
    work: Path,
    initial_step: str | None,
    *,
    by_name: bool = False,
    language: str | None = None,
) -> tuple[str, str, WorkflowProvider, str]:
    """Resolve an install source to its id, short name, provider and recorded source text.

    With *by_name* (a declared call) the source is a workflow name or git URI, never a path,
    so a directory of that name where the install runs is not taken for it.
    """

    path = Path(source).expanduser()
    text = os.fspath(source)
    manifest = path.is_dir() and (path / MANIFEST_NAME).is_file()
    if manifest and language is not None:
        raise ValueError("a format applies only to a bare document or directory; a package names its own format")
    if not by_name and not manifest and path.exists() and not path.is_symlink():
        found = _document_package(path.resolve(), language, work / "adhoc")
        if found is not None:
            return found[0], found[1], parse_workflow_manifest(work / "adhoc"), str(path.resolve())
    if not by_name and manifest:
        provider = parse_workflow_manifest(path)
        return f"local:{provider.workflow_id}", provider.workflow_id, provider, str(path.resolve())
    if text.startswith("git+"):
        provider = git_workflows.fetch_workflow(text)
        return provider.workflow_id, provider.name or provider.workflow_id, provider, text
    if not by_name and path.is_file() and not path.is_symlink():
        workflow_id, name = _adhoc_package(path.resolve(), work / "adhoc", initial_step)
        return workflow_id, name, parse_workflow_manifest(work / "adhoc"), str(path.resolve())
    known = scaffold.workflow_provider(text)
    if known is None:
        raise ValueError(f"no workflow package, git URI, runner file or known workflow name: {text!r}")
    if known.directory is None:
        raise ValueError(
            f"workflow {text!r} is registered in-process only and cannot be installed; install its package directory"
        )
    if known.definition_uri is not None:
        return known.definition_uri, known.name or known.workflow_id, known, text
    return f"local:{known.workflow_id}", known.workflow_id, known, text


def _runner(provider: WorkflowProvider) -> dict[str, object]:
    builtin = None if provider.language is None else provider.language.replace("_", "-")
    if builtin is not None and builtin not in _BUILTINS:
        raise ValueError(f"workflow format {provider.language!r} is not a built-in realization")
    command = None if builtin is not None or provider.command is None else list(provider.command)
    plain = builtin is None and command is None and provider.runnable
    return {"command": command, "entry": provider.entry if plain else None, "builtin": builtin}


def _record(
    workflow_id: str,
    name: str,
    source: str,
    reference: str | None,
    tree_sha256: str,
    calls: Mapping[str, str],
    slots: Mapping[str, str | None],
    provider: WorkflowProvider,
) -> dict[str, object]:
    """Return the ``install.json`` content of one installation."""

    return {
        "format": _FORMAT,
        "format_version": 1,
        "id": workflow_id,
        "name": name,
        "source": source,
        "reference": reference,
        "tree_sha256": tree_sha256,
        "installed_at": datetime.now(UTC).isoformat(),
        "calls": dict(calls),
        "slots": dict(slots),
        "requires": list(provider.requires),
        "runner": _runner(provider),
        "steps": list(provider.steps),
        "initial_step": provider.initial_step,
        "build": provider.build is not None,
    }


def install(
    workspace: KernelWorkspace,
    owner: Owner,
    source: str | os.PathLike[str],
    *,
    calls: bool = True,
    build: bool = True,
    initial_step: str | None = None,
    language: str | None = None,
    siblings: Mapping[str, WorkflowProvider] | None = None,
    reference: str | None = None,
    by_name: bool = False,
) -> Installed:
    """Install (or reinstall, replacing) a workflow package in the workspace.

    *source* is, in this order: a directory holding ``httk_workflow.toml``
    (id ``local:<name>``); a ``git+…`` URI (id = the canonical commit-pinned
    URI); a bare document of a workflow format, or a runner file (an ad hoc
    package, id ``adhoc:<name>@<sha12>``); or a
    workflow name known on this machine with a package directory. A
    reinstall moves the old installation aside to ``<slug>--<h16>.old.<token>/``,
    moves the new one into place and only then removes the old tree.

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch assembles the installation.
    :param source: The source.
    :param calls: Install every ``[workflow.calls]`` reference first (recursively) unless already installed. A
        call slot's default is never installed: ``install.json`` ``slots`` records its installed id when it is
        installed, else the reference as written (see :func:`slot_default_notices`).
    :param build: Build for the current platform when the package declares ``[workflow.build]``.
    :param initial_step: The default step of an ad hoc package (runner file sources only); required
        when the runner registers several steps and none of them is ``start``.
    :param language: Force a workflow format for a bare document or directory.
    :param siblings: Give the providers of one plugin source, keyed by id, short name and alias: a source
        or call reference among the keys is that provider, so calls among them bind to each other.
    :param reference: Record this tag or branch as the one the source was installed from (``NAME@REF``
        selects by it); by default that of a ``git+…@REF`` source, ``None`` for a commit or no ref.
    :param by_name: Take *source* as a workflow name or git URI only, never as a path where the install runs.
    :return: The installation.
    :raises ValueError: For an unresolvable or invalid source, a job's ``embedded:`` id, a symlink or special file
        in the package, a package registered in-process only, a call cycle, or a fixed-call target with a
        call slot that has no default.
    """

    if os.fspath(source).startswith(EMBEDDED_WORKFLOW_PREFIX):
        raise ValueError(f"an embedded workflow lives in its job's workflow/ and is never installed: {source}")
    return _install(
        workspace,
        owner,
        source,
        calls=calls,
        build=build,
        stack=(),
        initial_step=initial_step,
        language=language,
        siblings=siblings or {},
        reference=reference,
        by_name=by_name,
    )


def _install(
    workspace: KernelWorkspace,
    owner: Owner,
    source: str | os.PathLike[str],
    *,
    calls: bool,
    build: bool,
    stack: tuple[str, ...],
    initial_step: str | None = None,
    language: str | None = None,
    siblings: Mapping[str, WorkflowProvider],
    reference: str | None = None,
    caller: str | None = None,
    by_name: bool = False,
) -> Installed:
    with _scratch(owner) as work:
        sibling = siblings.get(os.fspath(source))
        if sibling is not None:
            workflow_id, name, provider = sibling.workflow_id, sibling.name or sibling.workflow_id, sibling
            origin = os.fspath(source)
        else:
            workflow_id, name, provider, origin = _resolve(
                source, work, initial_step, by_name=by_name or bool(stack), language=language
            )
        if reference is None:
            reference = _git_reference(origin)
        if workflow_id in stack:
            raise ValueError(f"workflow calls form a cycle: {' -> '.join((*stack, workflow_id))}")
        if caller is not None:
            _require_defaults(
                workflow_id, {alias: slot.default for alias, slot in (provider.slots or {}).items()}, caller
            )
        if stack and (installed := lookup(workspace, workflow_id)) is not None and installed.id == workflow_id:
            return installed  # a callee already installed under its resolved id is kept

        def installed_callee(written: str, caller: str) -> str:
            # A sibling binds by its pinned id, so another installation of its name never answers.
            bound = written if (callee := siblings.get(written)) is None else callee.workflow_id
            found = lookup(workspace, bound)
            if found is None and calls:
                found = _install(
                    workspace,
                    owner,
                    bound,
                    calls=True,
                    build=build,
                    stack=(*stack, workflow_id),
                    siblings=siblings,
                    # A sibling comes from the same plugin source, so it was installed from the same reference.
                    reference=None if callee is None else reference,
                    caller=caller,
                )
            elif found is not None:
                _require_defaults(found.id, found.record.get("slots"), caller)
            # Without calls, an uninstalled reference is recorded as written; closure() reports it missing.
            return written if found is None else found.id

        recorded = {
            alias: installed_callee(written, f"[workflow.calls].{alias} of {workflow_id}")
            for alias, written in (provider.calls or {}).items()
        }

        def installed_default(written: str) -> str:
            # Never installed here: a default is the job creator's convenience, not a dependency.
            found = lookup(workspace, written if (callee := siblings.get(written)) is None else callee.workflow_id)
            return written if found is None else found.id

        defaults = {
            alias: None if slot.default is None else installed_default(slot.default)
            for alias, slot in (provider.slots or {}).items()
        }
        assert provider.directory is not None  # _resolve refuses providers without a package directory
        tree = work / "tree"
        tree.mkdir()
        _copy_package(provider.directory, tree / "package", provider.build)
        tree_sha256 = source_digest(tree / "package")
        record = _record(workflow_id, name, origin, reference, tree_sha256, recorded, defaults, provider)
        _fs.write_file(_fs.loc(tree / "install.json"), _encode(record), durable=workspace.durable)
        if build and provider.build is not None:
            stamp = {"id": workflow_id, "tree_sha256": tree_sha256}
            _build_into(tree / "package", provider.build, tree / "builds", work, stamp, durable=workspace.durable)
        target = _workflows(workspace) / _dirname(workflow_id, name)
        # A replaced installation is moved aside beside it, outside the scratch, so a crash between the two
        # renames leaves it as <target>.old.<token>: lookups use it while <target> is missing, gc restores it.
        old = target.with_name(f"{target.name}.old.{_fs.fresh_token()}")
        replacing = _fs.exists(_fs.loc(target))
        if replacing:
            _fs.move_owned(_fs.loc(target), _fs.loc(old), durable=workspace.durable)
        _fs.move_owned(_fs.loc(tree), _fs.loc(target), durable=workspace.durable)
        if replacing:
            _fs.move_owned(_fs.loc(old), _fs.loc(work / "old"), durable=workspace.durable)
    _LOGGER.info("installed workflow %s in %s", workflow_id, target)
    return Installed(workflow_id, name, target, record)


def slot_default_notices(workspace: KernelWorkspace, installed: Installed) -> list[str]:
    """Return a notice for each call slot default of an installation that is not installed in the workspace.

    :param workspace: The workspace.
    :param installed: The installation.
    :return: The notices, without a ``notice:`` prefix.
    """

    slots = installed.record.get("slots")
    return [
        f"the default workflow {default} of call slot {alias!r} ({installed.name}) is not installed; "
        f"install it (httk workflow install {default}) or bind another with --call {alias}=WORKFLOW"
        for alias, default in (slots.items() if isinstance(slots, dict) else ())
        if isinstance(default, str) and lookup(workspace, default) is None
    ]


def _require_defaults(workflow_id: str, slots: object, caller: str) -> None:
    """Refuse a fixed-call target with a call slot that has no default: nothing could bind it."""

    missing = sorted(alias for alias, default in slots.items() if default is None) if isinstance(slots, dict) else []
    if missing:
        raise ValueError(
            f"{caller} names {workflow_id}, whose call slot {missing[0]!r} has no default; "
            "a workflow called through a fixed call must give every call slot a default"
        )


def _copied_record(entry: Path) -> tuple[str, str | None, dict[str, str], dict[str, str | None], str]:
    """Read and check the ``install.json`` of a copied store entry: its id, reference, calls, slots and tree digest.

    A record written before call slots existed has no ``slots`` member, which means none.
    """

    try:
        data = _fs.read_bounded(_fs.loc(entry / "install.json"), _RECORD_LIMIT)
        record = None if data is None else json.loads(data)
    except (OSError, ValueError, RecursionError, WorkflowError) as exc:
        raise ValueError(f"{entry}: unreadable install.json: {exc}") from exc
    if not isinstance(record, dict) or record.get("format") != _FORMAT or record.get("format_version") != 1:
        raise ValueError(f"{entry}: install.json is not an {_FORMAT} version 1 record")
    workflow_id, reference, calls, digest = (record.get(key) for key in ("id", "reference", "calls", "tree_sha256"))
    if not isinstance(workflow_id, str) or not is_workflow_id(workflow_id):
        raise ValueError(f"{entry}: not a workflow id: {workflow_id!r}")
    if workflow_id.startswith(EMBEDDED_WORKFLOW_PREFIX):
        raise ValueError(f"{entry}: an embedded workflow id is never installed: {workflow_id}")
    if workflow_id.startswith("git+") and not parse_git_uri(workflow_id).pinned:
        raise ValueError(f"{entry}: the git workflow id is not pinned to a commit: {workflow_id}")
    if reference is not None and not isinstance(reference, str):
        raise ValueError(f"{entry}: reference must be a string or null")
    if not isinstance(calls, dict) or not all(isinstance(value, str) for value in calls.values()):
        raise ValueError(f"{entry}: calls must map aliases to workflow references")
    slots = record.get("slots", {})
    if not isinstance(slots, dict) or not all(value is None or isinstance(value, str) for value in slots.values()):
        raise ValueError(f"{entry}: slots must map aliases to default workflow references or null")
    if not isinstance(digest, str):
        raise ValueError(f"{entry}: tree_sha256 must be a string")
    return workflow_id, reference, cast(dict[str, str], calls), cast(dict[str, str | None], slots), digest


def _compared(found: Installed, tree_sha256: str) -> tuple[str, Installed]:
    return "present" if found.record.get("tree_sha256") == tree_sha256 else "mismatch", found


def install_copy(workspace: KernelWorkspace, owner: Owner, entry: Path, *, source_label: str) -> tuple[str, Installed]:
    """Install a copy of another workspace's store entry under its recorded id, never replacing an installation.

    *entry* is a copied ``workflows/<slug>--<h16>/`` holding ``install.json`` and ``package/``; anything else in
    it (``builds/``) is ignored. Only the record's ``id``, ``reference``, ``calls`` and ``slots`` are taken;
    everything else is derived from the copied package, whose tree digest must equal the recorded
    ``tree_sha256``. The copy is recorded as installed now from *source_label*, so it becomes this workspace's
    newest installation of its short name. It is not built.

    An installation already under the id (also one only a crashed reinstall's moved-aside tree holds) is left
    untouched: ``"present"`` when its tree digest is the copy's, else ``"mismatch"``. The copy is moved into
    place by a rename that fails on an occupied name, so one another actor lands meanwhile is reported alike.

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch assembles the installation.
    :param entry: The copied store entry.
    :param source_label: The ``source`` to record, such as ``"transfer from WS"``.
    :return: ``"installed"``, ``"present"`` or ``"mismatch"``, and the installation now under the id.
    :raises ValueError: If the copy is refused: an unreadable or invalid record, an id without a known prefix, a
        ``embedded:`` id or a git id not pinned to a commit, calls or call slots other than the manifest's, a symlink
        or special file in the package, or a tree digest other than the recorded one.
    """

    workflow_id, reference, calls, slots, claimed = _copied_record(entry)
    existing = lookup(workspace, workflow_id)
    if existing is not None:
        return _compared(existing, claimed)
    source = entry / "package"
    try:
        is_directory = stat.S_ISDIR(os.lstat(source).st_mode)
    except OSError as exc:
        raise ValueError(f"{entry}: no package: {exc}") from exc
    if not is_directory:
        raise ValueError(f"{entry}: package is a symlink or not a directory")
    with _scratch(owner) as work:
        tree = work / "tree"
        _fs.make_dirs(tree, durable=workspace.durable)
        _copy_package(source, tree / "package", read_build_spec(source))
        # Everything recorded is derived from the copy itself, never from the copied record or the source tree.
        tree_sha256 = source_tree_digest(tree / "package")
        if tree_sha256 != claimed:
            raise ValueError(f"{entry}: tree digest mismatch: the package has {tree_sha256}, the record {claimed}")
        provider = parse_workflow_manifest(tree / "package", _uri=workflow_id)
        name = provider.name or provider.workflow_id
        if (workflow_id.startswith("local:") and workflow_id != f"local:{name}") or (
            workflow_id.startswith("adhoc:") and not workflow_id.startswith(f"adhoc:{name}@")
        ):
            raise ValueError(f"{entry}: the id {workflow_id} does not name the package's workflow {name!r}")
        if set(calls) != set(provider.calls or {}):
            raise ValueError(f"{entry}: the recorded calls {sorted(calls)} are not the manifest's")
        if set(slots) != set(provider.slots or {}):
            raise ValueError(f"{entry}: the recorded call slots {sorted(slots)} are not the manifest's")
        record = _record(workflow_id, name, source_label, reference, tree_sha256, calls, slots, provider)
        _fs.write_file(_fs.loc(tree / "install.json"), _encode(record), durable=workspace.durable)
        target = _workflows(workspace) / _dirname(workflow_id, name)
        try:
            # Rename as CAS, without install's move-aside: an occupied target is another actor's installation.
            _fs.move_owned(_fs.loc(tree), _fs.loc(target), durable=workspace.durable)
        except _fs.MoveFailed as exc:
            if exc.errno not in _fs._OCCUPIED_ERRNOS:
                raise
            winner = lookup(workspace, workflow_id)
            if winner is None:
                raise ValueError(f"{target} is occupied, but not by an installation of {workflow_id}") from exc
            return _compared(winner, tree_sha256)  # the scratch discards this copy
    _LOGGER.info("installed a copy of workflow %s in %s", workflow_id, target)
    return "installed", Installed(workflow_id, name, target, record)


def uninstall(workspace: KernelWorkspace, owner: Owner, id_or_name: str) -> Installed:
    """Remove an installed workflow (jobs referencing it are not checked).

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch receives the removed tree.
    :param id_or_name: The id, short name or ``NAME@REF`` (see :func:`lookup`).
    :return: The removed installation.
    :raises ValueError: If nothing matches, or the short name or ``NAME@REF`` selects several installations.
    """

    found = lookup(workspace, id_or_name)
    if found is None:
        raise ValueError(f"workflow {id_or_name!r} is not installed")
    if found.id != id_or_name:
        # Removing "the newest" of several is too easy to get wrong; build may use it, uninstall may not.
        shared = [other.id for other in _candidates(list_installed(workspace), id_or_name)]
        if len(shared) > 1:
            raise ValueError(
                f"workflow {id_or_name!r} selects {len(shared)} installations; "
                f"select one by id or by a prefix of its pinned commit (NAME@COMMIT): {', '.join(shared)}"
            )
    with _scratch(owner) as work:
        # Moved into the scratch first: the scratch cleanup makes it writable and discards it there.
        _fs.move_owned(_fs.loc(found.directory), _fs.loc(work / "old"), durable=workspace.durable)
    return found


def build(workspace: KernelWorkspace, owner: Owner, id_or_name: str) -> Path:
    """(Re)build an installed workflow for this platform into ``builds/<platform>/``.

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch hosts the build.
    :param id_or_name: The id, short name (the newest installation of it) or ``NAME@REF`` (see :func:`lookup`).
    :return: The build directory.
    :raises ValueError: If the workflow is not installed or declares no ``[workflow.build]``.
    :raises httk.core.building.BuildError: If the build fails.
    """

    found = lookup(workspace, id_or_name)
    if found is None:
        raise ValueError(f"workflow {id_or_name!r} is not installed")
    spec = found.provider().build
    if spec is None:
        raise ValueError(f"workflow {found.id} declares no [workflow.build]")
    with _scratch(owner) as work:
        stamp = {"id": found.id, "tree_sha256": found.record.get("tree_sha256")}
        built = _build_into(found.package, spec, work / "builds", work, stamp, durable=workspace.durable)
        target = found.directory / "builds" / built.name
        if _fs.exists(_fs.loc(target)):
            _fs.move_owned(_fs.loc(target), _fs.loc(work / "old"), durable=workspace.durable)
        _fs.move_owned(_fs.loc(built), _fs.loc(target), durable=workspace.durable)
    return target
