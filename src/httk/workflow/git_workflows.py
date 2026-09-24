"""Reference workflow packages in Git repositories by URI.

A workflow URI has the form ``git+https://HOST/PATH[@REF][#SUBDIR]`` (see
``httk.core.git_sources.parse_git_uri``): the repository is cloned at
``REF``, and the workflow is the ``httk_workflow.toml`` package in ``SUBDIR``
(or in the repository root). The canonical URI always carries the full commit
hash; it is the workflow id a job records and identifies the workflow
*definition* (its code).

:func:`fetch_workflow` installs the workflow through the shared
``httk.core.git_sources`` cache (kind ``"workflows"``), and
:func:`fetched_workflows` reads the installed entries without running git.
"""

import logging
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

from httk.core.git_sources import (
    InstalledGitMember,
    install_git_member,
    installed_git_members,
    parse_git_uri,
    uninstall_git_members,
)
from httk.core.userdirs import data_home

from .scaffold import WorkflowProvider

_LOGGER = logging.getLogger(__name__)

__all__ = ["fetch_workflow", "fetched_workflows"]

_KIND = "workflows"
_MANIFEST = "httk_workflow.toml"


def _load(uri: str, package: Path) -> WorkflowProvider:
    from .packages import load_workflow_package

    return load_workflow_package(package, register=False, _uri=uri)


def _claims(package: Path) -> tuple[str, ...]:
    """Validate a fetched package before it is installed and return its short names."""

    from .packages import load_workflow_package, source_tree_digest

    provider = load_workflow_package(package, register=False)
    source_tree_digest(package)  # refuse a tree that could never be published (e.g. a symlink)
    return tuple(name for name in (provider.workflow_id, provider.alias) if name is not None)


def fetch_workflow(reference: str) -> WorkflowProvider:
    """Fetch, install and return the workflow a git URI names.

    A pinned URI whose commit is already cached runs no git at all. Every call
    refreshes the installed entry's reference time, which decides which commit
    the workflow's short name means.

    :param reference: Supply a ``git+…`` workflow URI.
    :return: The provider, whose ``workflow_id`` is the canonical URI.
    :raises ValueError: If the URI is invalid, git fails, or the package is missing or invalid.
    """

    installed = install_git_member(_KIND, reference, _MANIFEST, _claims)
    _reset_fetched_workflow_cache()
    provider = _load(installed.uri, installed.path)
    from .scaffold import _claimed_names

    claimed = _claimed_names()
    for name in (provider.name, provider.alias):
        if name is not None and name in claimed:
            _LOGGER.warning(
                "installed workflow short name %r is shadowed by another workflow; reference it by its URI %s",
                name,
                installed.uri,
            )
    return provider


def _uninstall_workflows(selector: str) -> tuple[InstalledGitMember, ...]:
    """Forget installed git workflows by short name or URI, without running git.

    A pinned URI removes that entry, an unpinned URI or a short name removes
    its whole lineage (repository and subdirectory); cached checkouts stay.

    :param selector: Give a ``git+…`` URI or a short name.
    :return: The removed entries.
    :raises ValueError: If the selector is invalid, ambiguous, or matches nothing.
    """

    try:
        return uninstall_git_members(_KIND, selector)
    finally:
        _reset_fetched_workflow_cache()


type _Entry = tuple[WorkflowProvider, tuple[str, str | None], str]
type _Data = tuple[Mapping[str, _Entry], Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]]
_FETCHED_CACHE: tuple[Path, _Data] | None = None


def _fetched_data() -> _Data:
    """Load installed entries: by URI, winners by short name, conflicted names."""

    global _FETCHED_CACHE
    directory = data_home() / _KIND / "installed"
    if _FETCHED_CACHE is not None and _FETCHED_CACHE[0] == directory:
        return _FETCHED_CACHE[1]
    entries: dict[str, _Entry] = {}
    for member in installed_git_members(_KIND):
        try:
            entries[member.uri] = (
                _load(member.uri, member.path),
                (member.repository, member.subdir),
                member.referenced_at,
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            _LOGGER.warning("Skipping installed workflow %s: %s", member.uri, exc)
    claims: dict[str, list[str]] = {}
    for key, (provider, _, _) in entries.items():
        for name in (provider.name, provider.alias):
            if name is not None:
                claims.setdefault(name, []).append(key)
    winners: dict[str, WorkflowProvider] = {}
    conflicts: dict[str, tuple[str, ...]] = {}
    for name, keys in claims.items():
        if len({entries[key][1] for key in keys}) > 1:
            conflicts[name] = tuple(sorted(keys))
        else:
            winners[name] = entries[max(keys, key=lambda key: entries[key][2])][0]
    data: _Data = (MappingProxyType(entries), MappingProxyType(winners), MappingProxyType(conflicts))
    _FETCHED_CACHE = (directory, data)
    return data


def fetched_workflows() -> Mapping[str, WorkflowProvider]:
    """Return the installed git workflows keyed by canonical URI, without running git.

    :return: The installed providers by canonical URI.
    """

    return MappingProxyType({key: entry[0] for key, entry in _fetched_data()[0].items()})


def _installed_provider(text: str) -> WorkflowProvider | None:
    """Return the installed provider for exactly this pinned URI; never fetch, never raise."""

    try:
        uri = parse_git_uri(text)
    except ValueError:
        return None
    entry = _fetched_data()[0].get(str(uri)) if uri.pinned else None
    return None if entry is None else entry[0]


def _fetched_names() -> tuple[Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]]:
    """Return short-name winners and the names claimed by several lineages."""

    data = _fetched_data()
    return data[1], data[2]


def _fetched_provider(name: str) -> WorkflowProvider | None:
    """Return the installed workflow a short name means, latest reference wins."""

    winners, conflicts = _fetched_names()
    if name in conflicts:
        raise ValueError(
            f"workflow name {name!r} is claimed by several installed workflows ({', '.join(conflicts[name])}); "
            "reference one by its URI"
        )
    return winners.get(name)


def _reset_fetched_workflow_cache() -> None:
    """Clear cached installed-workflow discovery (after installs and in tests)."""

    global _FETCHED_CACHE
    _FETCHED_CACHE = None
