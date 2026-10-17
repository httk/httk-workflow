"""Offer the workflow packages of workflow repositories pip-installed into this Python environment.

A workflow repository installed with ``pip install CLONE`` or ``pip install -e
CLONE`` carries ``httk_workflow_repository.json`` in its ``.dist-info``: the
canonical repository URI and either a git bundle (a wheel) or the absolute
path of the clone (an editable install). Its packages, the top-level
directories holding ``httk_workflow.toml``, are offered like fetched git
workflows under ``str(GitUri(repository, commit, directory))``: the HEAD
commit's by short name and pinned URI, every ``wf-v*`` tag's by pinned URI.

Trees are seeded into the shared ``httk.core.git_sources`` checkout cache from
the local source with local git only, never the network, and no installed
entry is written, so ``pip uninstall`` removes the workflows again. Discovery
and loaded versions are memoized per process (HEAD versions eagerly, tag
versions on first use); :func:`_reset_pip_repository_cache` clears them.
Broken sources are skipped with one warning; nothing here raises.
"""

import importlib.metadata
import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TypeGuard

from httk.core.git_sources import GitUri, fetch_git_checkout, local_git_refs, parse_git_uri
from httk.core.userdirs import data_home

from .scaffold import WorkflowProvider

_LOGGER = logging.getLogger(__name__)

_RECORD = "httk_workflow_repository.json"
_TAGS = "wf-v*"
_MANIFEST = "httk_workflow.toml"
_FORMAT = "httk-workflow-repository"
_COMMIT = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


@dataclass(frozen=True)
class _Source:
    """One pip-installed workflow repository: its bundle file or clone, and the commits it offers."""

    distribution: str
    repository: str
    location: Path
    head: str
    tags: Mapping[str, str]


_HOME: Path | None = None
_SOURCES: tuple[_Source, ...] | None = None
_VERSIONS: dict[tuple[str, str], Mapping[str, WorkflowProvider]] = {}
_NAMES: tuple[Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]] | None = None


def _reset_pip_repository_cache() -> None:
    """Forget discovered pip repositories and loaded versions (after installs and in tests)."""

    global _HOME, _SOURCES, _NAMES
    _HOME, _SOURCES, _NAMES = None, None, None
    _VERSIONS.clear()


def _is_commit(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and _COMMIT.fullmatch(value) is not None


def _read_source(distribution: importlib.metadata.Distribution, name: str, text: str) -> _Source:
    """Validate one record; raise ValueError naming what is wrong."""

    record = json.loads(text)
    if not isinstance(record, dict) or record.get("format") != _FORMAT or record.get("format_version") != 1:
        raise ValueError(f"{_RECORD} is not a format_version 1 {_FORMAT} record")
    repository = record.get("repository")
    if not isinstance(repository, str):
        raise ValueError(f"repository must be a git repository URI: {repository!r}")
    parsed = parse_git_uri(repository)
    if parsed.ref is not None or parsed.subdir is not None or str(parsed) != repository:
        raise ValueError(f"repository is not a canonical git repository URI without @ref or #subdir: {repository!r}")
    if record.get("source") == "clone":
        clone = record.get("clone")
        if not isinstance(clone, str) or not Path(clone).is_absolute():
            raise ValueError(f"clone must be an absolute path: {clone!r}")
        refs = local_git_refs(Path(clone), _TAGS)
        return _Source(name, repository, Path(clone), refs.head, refs.tags)
    if record.get("source") != "bundle":
        raise ValueError(f"source must be 'bundle' or 'clone': {record.get('source')!r}")
    head, tags, bundle = record.get("head"), record.get("tags"), record.get("bundle")
    if not isinstance(tags, dict) or not all(
        isinstance(tag, str) and tag.startswith("wf-v") and _is_commit(commit) for tag, commit in tags.items()
    ):
        raise ValueError("tags must map wf-v* tag names to full lowercase commit hashes")
    if not _is_commit(head):
        raise ValueError(f"head must be a full lowercase commit hash: {head!r}")
    files = distribution.files or ()
    parents = {file.parent for file in files if file.name == _RECORD}
    location = next(
        (Path(str(file.locate())) for file in files if file.name == bundle and file.parent in parents), None
    )
    if location is None or not location.is_file():
        raise ValueError(f"bundle {bundle!r} is not an installed file next to {_RECORD}")
    return _Source(name, repository, location, head, MappingProxyType(dict(tags)))


def _sources() -> tuple[_Source, ...]:
    """Return the valid pip-installed repositories; the first distribution of a name wins."""

    global _HOME, _SOURCES
    if _HOME != data_home():  # trees and providers live under the data home
        _reset_pip_repository_cache()
        _HOME = data_home()
    if _SOURCES is None:
        seen: set[str] = set()
        found: list[_Source] = []
        for distribution in importlib.metadata.distributions():
            name: str | None = None
            try:
                name = distribution.name  # reads METADATA: a damaged one raises (e.g. UnicodeDecodeError)
                if not isinstance(name, str) or (normalized := re.sub(r"[-_.]+", "-", name).lower()) in seen:
                    continue
                seen.add(normalized)
                text = distribution.read_text(_RECORD)
                if text is not None:
                    found.append(_read_source(distribution, name, text))
            except (OSError, ValueError, TypeError) as exc:
                label = name or str(getattr(distribution, "_path", "<unknown>"))
                _LOGGER.warning("Skipping the workflow repository of Python distribution %r: %s", label, exc)
        _SOURCES = tuple(found)
    return _SOURCES


def _seed(repository: str, commit: str) -> Path:
    """Cache one commit's tree from a pip source of its repository, trying each in turn."""

    sources = [source for source in _sources() if source.repository == repository]
    sources.sort(key=lambda source: commit not in (source.head, *source.tags.values()))
    error: Exception = ValueError(f"no pip-installed source of {repository}")
    for source in sources:
        try:
            return fetch_git_checkout(GitUri(repository, commit, None), source=source.location)[1]
        except (OSError, ValueError) as exc:
            error = exc
    raise error


def _version(repository: str, commit: str) -> Mapping[str, WorkflowProvider]:
    """Return one offered commit's packages by directory, materializing the commit on first use."""

    key = (repository, commit)
    if key in _VERSIONS:
        return _VERSIONS[key]
    from .packages import load_workflow_package, source_tree_digest

    packages: dict[str, WorkflowProvider] = {}
    try:
        tree = _seed(repository, commit)
        directories = sorted(entry for entry in tree.iterdir() if not entry.is_symlink() and entry.is_dir())
    except (OSError, ValueError) as exc:
        _LOGGER.warning("Skipping pip-installed workflow repository %s at %s: %s", repository, commit, exc)
        directories = []
    for directory in directories:
        manifest = directory / _MANIFEST
        if not manifest.is_symlink() and not manifest.is_file():
            continue
        uri = str(GitUri(repository, commit, directory.name))
        try:
            if manifest.is_symlink():
                raise ValueError(f"{_MANIFEST} is a symlink")
            source_tree_digest(directory)  # refuse what the git route refuses (e.g. a symlink), as git_workflows does
            packages[directory.name] = load_workflow_package(directory, register=False, _uri=uri)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            _LOGGER.warning("Skipping pip-installed workflow %s: %s", uri, exc)
    _VERSIONS[key] = MappingProxyType(packages)
    return _VERSIONS[key]


def _pip_names() -> tuple[Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]]:
    """Return installed-version (HEAD) providers by short name and alias, and names several URIs claim."""

    global _NAMES
    sources = _sources()
    if _NAMES is None:
        claims: dict[str, dict[str, WorkflowProvider]] = {}
        for source in sources:
            for provider in _version(source.repository, source.head).values():
                for name in (provider.name, provider.alias):
                    if name is not None:
                        claims.setdefault(name, {})[provider.workflow_id] = provider
        winners = {name: next(iter(by_uri.values())) for name, by_uri in claims.items() if len(by_uri) == 1}
        conflicts = {name: tuple(sorted(by_uri)) for name, by_uri in claims.items() if len(by_uri) > 1}
        _NAMES = (MappingProxyType(winners), MappingProxyType(conflicts))
    return _NAMES


def _offering(uri: GitUri) -> _Source | None:
    """Return the first source offering the URI's commit (its HEAD or a tag) of its repository."""

    return next(
        (s for s in _sources() if s.repository == uri.repository and uri.ref in (s.head, *s.tags.values())), None
    )


def _pip_provider(text: str) -> WorkflowProvider | None:
    """Return the pip-offered provider of exactly this pinned URI; never fetch, never raise."""

    try:
        uri = parse_git_uri(text)
    except ValueError:
        return None
    if not uri.pinned or uri.subdir is None or _offering(uri) is None:
        return None
    assert uri.ref is not None
    return _version(uri.repository, uri.ref).get(uri.subdir)


def _pip_owner(uri_text: str) -> str | None:
    """Return the distribution offering a pinned URI's commit, or ``None``."""

    try:
        source = _offering(parse_git_uri(uri_text))
    except ValueError:
        return None
    return None if source is None else source.distribution


def _pip_tag_uris(uri_text: str) -> dict[str, str]:
    """Return ``{tag: uri}`` of the tags whose commit has the package of a HEAD package URI."""

    uri = parse_git_uri(uri_text)
    tags: dict[str, str] = {}
    for source in _sources():
        if source.repository == uri.repository and source.head == uri.ref and uri.subdir is not None:
            # ponytail: a listing materializes every wf-v* tag once per process; read tag trees
            # with git ls-tree instead if repositories grow many tags.
            for tag, commit in sorted(source.tags.items()):
                if uri.subdir in _version(source.repository, commit):
                    tags.setdefault(tag, str(GitUri(source.repository, commit, uri.subdir)))
    return tags


def _pip_tag_commit(repository: str, tag: str) -> str | None:
    """Return a tag's commit when every pip source of the repository that has the tag agrees."""

    commits = {source.tags[tag] for source in _sources() if source.repository == repository and tag in source.tags}
    return commits.pop() if len(commits) == 1 else None


def _pip_seed(uri: GitUri) -> bool:
    """Cache a pinned URI's commit from any pip source of its repository; whether it worked."""

    assert uri.ref is not None
    try:
        _seed(uri.repository, uri.ref)
    except (OSError, ValueError):
        return False
    return True
