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
aside to ``<slug>--<h16>.old.<token>/``, so a crash leaves one version or the
other (``httk workspace gc`` removes a stale one); a replaced or removed
installation is discarded from the owner's scratch.
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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from httk.core.building import DEFAULT_TAG, BuildSpec, execute_build
from httk.core.digests import sha256_file

from httk.workflow import _fs, git_workflows, scaffold
from httk.workflow._kernel import KernelWorkspace, Owner, reconcile_scratch
from httk.workflow.errors import RunnerResolutionError
from httk.workflow.packages import (
    MANIFEST_NAME,
    artifact_excluder,
    parse_workflow_manifest,
    read_build_spec,
    source_tree_digest,
)
from httk.workflow.scaffold import WorkflowProvider

__all__ = [
    "ClosureReport",
    "Installed",
    "build",
    "closure",
    "install",
    "list_installed",
    "lookup",
    "platform_tag",
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


@dataclass(frozen=True)
class Installed:
    """One installed workflow.

    :param id: The workflow id.
    :param name: The short name.
    :param directory: ``workflows/<slug>--<h16>/``.
    :param record: The ``install.json`` content.
    """

    id: str
    name: str
    directory: Path
    record: Mapping[str, object]
    _cache: dict[str, WorkflowProvider] = field(default_factory=dict, init=False, repr=False, compare=False)

    @property
    def package(self) -> Path:
        """The installed package tree."""

        return self.directory / "package"

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
        """

        path = self.directory / "builds" / platform
        return path if (path / "build.json").is_file() and not path.is_symlink() else None


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


def _installations(workspace: KernelWorkspace) -> list[Path]:
    # Only <slug>--<h16> names: a replaced tree moved aside as <slug>--<h16>.old.<token> is not installed.
    return [path for path in _subdirectories(_workflows(workspace)) if _INSTALLATION.fullmatch(path.name)]


def stale_replacements(workspace: KernelWorkspace, cutoff: float, *, writable: bool) -> list[Path]:
    """Return the installations a crashed reinstall left moved aside, ``workflows/<slug>--<h16>.old.<token>/``.

    :param workspace: The workspace.
    :param cutoff: Only entries moved aside (their ``ctime``) before this epoch time.
    :param writable: Make each one's directories writable, so a removal that does not chmod succeeds.
    :return: The entries.
    """

    stale = []
    for path in _subdirectories(_workflows(workspace)):
        try:
            moved = os.lstat(path).st_ctime
        except FileNotFoundError:
            continue
        if _REPLACED.fullmatch(path.name) and moved < cutoff:
            if writable:
                _writable(path)
            stale.append(path)
    return stale


def list_installed(workspace: KernelWorkspace) -> list[Installed]:
    """List the installed workflows, skipping directories without a readable ``install.json``.

    :param workspace: The workspace.
    :return: The installations, in directory-name order.
    """

    return [found for directory in _installations(workspace) if (found := _read(directory)) is not None]


def lookup(workspace: KernelWorkspace, id_or_name: str) -> Installed | None:
    """Return the installation with this exact id, else the one with this short name.

    :param workspace: The workspace.
    :param id_or_name: An id or a short name.
    :return: The installation, or ``None``.
    :raises ValueError: If several installations share the name; the message lists their ids.
    """

    # The slug of a git id's short name is not derivable from the id, so the
    # listing is scanned for the h16 suffix; only matching install.json files are read.
    suffix = f"--{_h16(id_or_name)}"
    for directory in _installations(workspace):
        if directory.name.endswith(suffix) and (found := _read(directory)) is not None and found.id == id_or_name:
            return found
    named = [found for found in list_installed(workspace) if found.name == id_or_name]
    if len(named) > 1:
        raise ValueError(f"workflow name {id_or_name!r} is ambiguous; use an id: {', '.join(f.id for f in named)}")
    return named[0] if named else None


def closure(workspace: KernelWorkspace, workflow_id: str, *, check_builds: bool) -> ClosureReport:
    """Check that a workflow and its transitive calls are installed (and built for this platform).

    With *check_builds*, each installed package with a ``[workflow.build]`` is
    unbuilt when it has no build for its own :func:`platform_tag`, since
    packages differ in their platform probe.

    :param workspace: The workspace.
    :param workflow_id: The root workflow id (or name).
    :param check_builds: Also report packages without a build for this platform.
    :return: The missing and unbuilt ids.
    :raises httk.workflow.errors.RunnerResolutionError: If a package's platform probe fails.
    :raises ValueError: If an installed package's build section is malformed.
    """

    missing: list[str] = []
    unbuilt: list[str] = []
    seen: set[str] = set()
    pending = [workflow_id]
    while pending:
        reference = pending.pop()
        if reference in seen:
            continue
        seen.add(reference)
        try:
            found = lookup(workspace, reference)
        except ValueError:
            found = None
        if found is None:
            missing.append(reference)
            continue
        spec = read_build_spec(found.package) if check_builds and found.record.get("build") is True else None
        if spec is not None and found.build_dir(platform_tag(spec)) is None:
            unbuilt.append(found.id)
        calls = found.record.get("calls")
        if isinstance(calls, dict):
            pending.extend(str(callee) for callee in calls.values())
    return ClosureReport(tuple(sorted(missing)), tuple(sorted(unbuilt)))


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


def _copy_package(source: Path, destination: Path, spec: BuildSpec | None) -> None:
    """Copy a package's sources, refusing symlinks and special files, skipping ``.git`` and build artifacts."""

    is_artifact = artifact_excluder(spec)

    def ignore(directory: str, names: list[str]) -> set[str]:
        skipped: set[str] = set()
        for name in names:
            path = Path(directory, name)
            relative = path.relative_to(source).as_posix()
            if name == ".git" or is_artifact(relative):
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


def install(
    workspace: KernelWorkspace,
    owner: Owner,
    source: str | os.PathLike[str],
    *,
    calls: bool = True,
    build: bool = True,
    initial_step: str | None = None,
    language: str | None = None,
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
    :param calls: Install every ``[workflow.calls]`` reference first (recursively) unless already installed.
    :param build: Build for the current platform when the package declares ``[workflow.build]``.
    :param initial_step: The default step of an ad hoc package (runner file sources only); required
        when the runner registers several steps and none of them is ``start``.
    :param language: Force a workflow format for a bare document or directory.
    :return: The installation.
    :raises ValueError: For an unresolvable or invalid source, a symlink or special file in the
        package, a package registered in-process only, or a call cycle.
    """

    return _install(
        workspace, owner, source, calls=calls, build=build, stack=(), initial_step=initial_step, language=language
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
) -> Installed:
    with _scratch(owner) as work:
        workflow_id, name, provider, origin = _resolve(
            source, work, initial_step, by_name=bool(stack), language=language
        )
        if workflow_id in stack:
            raise ValueError(f"workflow calls form a cycle: {' -> '.join((*stack, workflow_id))}")
        if stack and (installed := lookup(workspace, workflow_id)) is not None and installed.id == workflow_id:
            return installed  # a callee already installed under its resolved id is kept
        recorded: dict[str, str] = {}
        for alias, reference in (provider.calls or {}).items():
            found = lookup(workspace, reference)
            if found is None and calls:
                found = _install(workspace, owner, reference, calls=True, build=build, stack=(*stack, workflow_id))
            # Without calls, an uninstalled reference is recorded as written; closure() reports it missing.
            recorded[alias] = reference if found is None else found.id
        assert provider.directory is not None  # _resolve refuses providers without a package directory
        tree = work / "tree"
        tree.mkdir()
        _copy_package(provider.directory, tree / "package", provider.build)
        tree_sha256 = source_tree_digest(tree / "package")
        record: dict[str, object] = {
            "format": _FORMAT,
            "format_version": 1,
            "id": workflow_id,
            "name": name,
            "source": origin,
            "tree_sha256": tree_sha256,
            "installed_at": datetime.now(UTC).isoformat(),
            "calls": recorded,
            "requires": list(provider.requires),
            "runner": _runner(provider),
            "steps": list(provider.steps),
            "initial_step": provider.initial_step,
            "build": provider.build is not None,
        }
        _fs.write_file(_fs.loc(tree / "install.json"), _encode(record), durable=workspace.durable)
        if build and provider.build is not None:
            stamp = {"id": workflow_id, "tree_sha256": tree_sha256}
            _build_into(tree / "package", provider.build, tree / "builds", work, stamp, durable=workspace.durable)
        target = _workflows(workspace) / _dirname(workflow_id, name)
        # A replaced installation is moved aside beside it, outside the scratch, so a crash between the two
        # renames leaves it as <target>.old.<token> (which lookups ignore and gc sweeps), never neither version.
        old = target.with_name(f"{target.name}.old.{_fs.fresh_token()}")
        replacing = _fs.exists(_fs.loc(target))
        if replacing:
            _fs.move_owned(_fs.loc(target), _fs.loc(old), durable=workspace.durable)
        _fs.move_owned(_fs.loc(tree), _fs.loc(target), durable=workspace.durable)
        if replacing:
            _fs.move_owned(_fs.loc(old), _fs.loc(work / "old"), durable=workspace.durable)
    _LOGGER.info("installed workflow %s in %s", workflow_id, target)
    return Installed(workflow_id, name, target, record)


def uninstall(workspace: KernelWorkspace, owner: Owner, id_or_name: str) -> Installed:
    """Remove an installed workflow (jobs referencing it are not checked).

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch receives the removed tree.
    :param id_or_name: The id or unique short name.
    :return: The removed installation.
    :raises ValueError: If nothing matches or the name is ambiguous.
    """

    found = lookup(workspace, id_or_name)
    if found is None:
        raise ValueError(f"workflow {id_or_name!r} is not installed")
    with _scratch(owner) as work:
        # Moved into the scratch first: the scratch cleanup makes it writable and discards it there.
        _fs.move_owned(_fs.loc(found.directory), _fs.loc(work / "old"), durable=workspace.durable)
    return found


def build(workspace: KernelWorkspace, owner: Owner, id_or_name: str) -> Path:
    """(Re)build an installed workflow for this platform into ``builds/<platform>/``.

    :param workspace: The workspace.
    :param owner: The registered owner whose scratch hosts the build.
    :param id_or_name: The id or unique short name.
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
