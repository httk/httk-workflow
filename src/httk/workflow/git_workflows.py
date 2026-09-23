"""Reference workflow packages in Git repositories by IRI.

A workflow IRI has the form ``git+https://HOST/PATH[@REF][#SUBDIR]``: the
repository is cloned at ``REF`` (a branch, tag, abbreviated or full commit hash,
or the remote default branch when omitted), and the workflow is the
``httk_workflow.toml`` package in ``SUBDIR`` (or in the repository root). The
canonical IRI always carries the full commit hash, and it is the workflow id a
job records. The repository path is kept verbatim, so ``…/repo`` and
``…/repo.git`` are distinct IRIs.

Referencing an IRI with :func:`fetch_workflow` installs it under
``data_home() / "workflows"``: the checkout tree is cached per repository and
commit, and one ``installed/*.json`` entry records each referenced IRI.
:func:`fetched_workflows` reads those entries and never runs git.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

from httk.core.userdirs import data_home

from .scaffold import WorkflowProvider

_LOGGER = logging.getLogger(__name__)

__all__ = ["WorkflowIri", "fetch_workflow", "fetched_workflows", "parse_workflow_iri"]

_SCHEMES = frozenset({"https", "http", "file"})
_FULL_HASH = re.compile(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}")
_CHECKOUT_FORMAT = "httk-workflow-git-checkout"
_INSTALLED_FORMAT = "httk-workflow-installed"
_MANIFEST = "httk_workflow.toml"


@dataclass(frozen=True)
class WorkflowIri:
    """One parsed git workflow IRI.

    :param repository: Give the canonical ``git+scheme://authority/path`` repository.
    :param ref: Give the branch, tag or commit, or ``None`` for the default branch.
    :param subdir: Give the workflow package directory, or ``None`` for the root.
    """

    repository: str
    ref: str | None
    subdir: str | None

    @property
    def pinned(self) -> bool:
        """Whether the ref is a full commit hash."""

        return self.ref is not None and _FULL_HASH.fullmatch(self.ref) is not None

    def __str__(self) -> str:
        """Return the IRI text.

        :return: ``repository[@ref][#subdir]``.
        """

        ref = f"@{self.ref}" if self.ref is not None else ""
        subdir = f"#{self.subdir}" if self.subdir is not None else ""
        return f"{self.repository}{ref}{subdir}"


def parse_workflow_iri(text: str) -> WorkflowIri:
    """Parse and canonicalize a ``git+scheme://authority/path[@ref][#subdir]`` IRI.

    :param text: Supply the IRI text.
    :return: The parsed IRI with a lowercased host and hash and no trailing slashes.
    :raises ValueError: If the text is not a supported git workflow IRI.
    """

    if not isinstance(text, str) or not text.startswith("git+"):
        raise ValueError(f"a git workflow IRI must start with 'git+': {text!r}")
    if any(character.isspace() or ord(character) < 32 for character in text):
        raise ValueError(f"git workflow IRI must not contain whitespace or control characters: {text!r}")
    body = text[4:]
    scheme, separator, rest = body.partition("://")
    scheme = scheme.lower()
    if not separator or scheme not in _SCHEMES:
        raise ValueError(
            f"unsupported git workflow IRI {text!r}: only git+https://, git+http:// and git+file:// "
            "are supported (not ssh or git@host:path)"
        )
    rest, hashmark, fragment = rest.partition("#")
    if "?" in rest:
        raise ValueError(f"git workflow IRI must not contain a query: {text!r}")
    authority, slash, path = rest.partition("/")
    path = slash + path
    if "@" in authority:
        raise ValueError(f"git workflow IRI must not carry userinfo or credentials: {text!r}")
    if not authority and scheme != "file":
        raise ValueError(f"git workflow IRI must name a host: {text!r}")
    ref: str | None = None
    if "@" in path:
        path, ref = path.rsplit("@", 1)
        if not ref:
            raise ValueError(f"git workflow IRI has an empty ref after '@': {text!r}")
        if ref.startswith("-"):
            raise ValueError(f"git workflow IRI ref must not start with '-': {text!r}")
        if _FULL_HASH.fullmatch(ref):
            ref = ref.lower()
    path = path.rstrip("/")
    if not path:
        raise ValueError(f"git workflow IRI must name a repository path: {text!r}")
    subdir: str | None = None
    if hashmark:
        if not fragment:
            raise ValueError(f"git workflow IRI has an empty subdirectory after '#': {text!r}")
        subdir = fragment.removesuffix("/")
        if subdir.startswith("/") or "\\" in subdir or any(part in {"", ".", ".."} for part in subdir.split("/")):
            raise ValueError(f"git workflow IRI subdirectory must be a plain relative path: {fragment!r}")
    return WorkflowIri(f"git+{scheme}://{authority.lower()}{path}", ref, subdir)


def _root() -> Path:
    return data_home() / "workflows"


def _checkouts(repository: str) -> Path:
    return _root() / "git" / hashlib.sha256(repository.encode()).hexdigest()[:16]


def _installed_path(iri: str) -> Path:
    return _root() / "installed" / f"{hashlib.sha256(iri.encode()).hexdigest()[:32]}.json"


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _write_json(path: Path, document: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _git(cwd: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull, GIT_TERMINAL_PROMPT="0")
    try:
        result = subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", *arguments],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
            env=environment,
        )
    except FileNotFoundError as exc:
        raise ValueError("git is not available on PATH") from exc
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ValueError(f"git {' '.join(arguments)} failed ({result.returncode}): {detail}")
    return result


def _checkout(iri: WorkflowIri) -> tuple[str, Path]:
    """Return the commit and cached tree of *iri*, cloning only on a cache miss."""

    parent = _checkouts(iri.repository)
    if iri.pinned:
        assert iri.ref is not None
        if (parent / iri.ref).is_dir():
            return iri.ref, parent / iri.ref
    parent.mkdir(parents=True, exist_ok=True)
    url = iri.repository[len("git+") :]
    scratch = Path(tempfile.mkdtemp(prefix=".fetch-", dir=parent))
    try:
        work = scratch / "tree"
        if iri.pinned:
            assert iri.ref is not None
            work.mkdir()
            _git(work, "init", "-q")
            _git(work, "remote", "add", "origin", url)
            if _git(work, "fetch", "--depth", "1", "origin", iri.ref, check=False).returncode == 0:
                _git(work, "checkout", "-q", "--detach", "FETCH_HEAD")
            else:
                _git(work, "fetch", "origin")
                _git(work, "checkout", "-q", "--detach", iri.ref)
            if _git(work, "rev-parse", "HEAD").stdout.strip().lower() != iri.ref:
                raise ValueError(f"{iri.repository}: fetched commit does not match {iri.ref}")
        elif iri.ref is None:
            _git(scratch, "clone", "-q", "--depth", "1", url, str(work))
        elif _git(scratch, "clone", "-q", "--depth", "1", "--branch", iri.ref, url, str(work), check=False).returncode:
            shutil.rmtree(work, ignore_errors=True)
            _git(scratch, "clone", "-q", url, str(work))
            commit = _git(work, "rev-parse", "--verify", "--end-of-options", f"{iri.ref}^{{commit}}").stdout.strip()
            _git(work, "checkout", "-q", "--detach", commit)
        commit = _git(work, "rev-parse", "HEAD").stdout.strip().lower()
        tree = parent / commit
        if not tree.is_dir():
            shutil.rmtree(work / ".git")
            _write_json(
                parent / f"{commit}.json",
                {
                    "format": _CHECKOUT_FORMAT,
                    "format_version": 1,
                    "repository": iri.repository,
                    "commit": commit,
                    "fetched_at": _now(),
                },
            )
            try:
                os.rename(work, tree)
            except OSError:
                if not tree.is_dir():  # a concurrent fetch that won the rename is fine
                    raise
        return commit, tree
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def _package(tree: Path, iri: WorkflowIri) -> Path:
    """Return the workflow package directory *iri* names inside *tree*."""

    package = tree
    for part in iri.subdir.split("/") if iri.subdir is not None else ():
        package = package / part
        if package.is_symlink() or not package.is_dir():
            raise ValueError(f"{iri.subdir!r} is not a directory in {iri.repository} at {iri.ref}")
    if not package.resolve().is_relative_to(tree.resolve()):
        raise ValueError(f"{iri.subdir!r} leaves the checkout of {iri.repository} at {iri.ref}")
    if not (package / _MANIFEST).is_file():
        if iri.subdir is not None:
            raise ValueError(f"{iri.subdir!r} has no {_MANIFEST} in {iri.repository} at {iri.ref}")
        candidates = sorted(
            entry.name
            for entry in tree.iterdir()
            if entry.is_dir() and not entry.is_symlink() and (entry / _MANIFEST).is_file()
        )
        found = f"; subdirectories with one: {', '.join(candidates)}" if candidates else ""
        raise ValueError(
            f"{iri.repository} at {iri.ref} has no top-level {_MANIFEST}; "
            f"name the workflow directory with #subdir{found}"
        )
    return package


def _provider(iri: WorkflowIri, tree: Path, *, check: bool = False) -> WorkflowProvider:
    from .packages import load_workflow_package, source_tree_digest

    package = _package(tree, iri)
    if check:
        source_tree_digest(package)  # refuse a tree that could never be published (e.g. a symlink)
    return load_workflow_package(package, register=False, _iri=str(iri))


def fetch_workflow(reference: str) -> WorkflowProvider:
    """Fetch, install and return the workflow a git IRI names.

    A pinned IRI whose commit is already cached runs no git at all. Every call
    refreshes the installed entry's ``referenced_at``, which decides which
    commit the workflow's short name means.

    :param reference: Supply a ``git+…`` workflow IRI.
    :return: The provider, whose ``workflow_id`` is the canonical IRI.
    :raises ValueError: If the IRI is invalid, git fails, or the package is missing or invalid.
    """

    iri = parse_workflow_iri(reference)
    commit, tree = _checkout(iri)
    canonical = WorkflowIri(iri.repository, commit, iri.subdir)
    provider = _provider(canonical, tree, check=True)
    _write_json(
        _installed_path(str(canonical)),
        {
            "format": _INSTALLED_FORMAT,
            "format_version": 1,
            "iri": str(canonical),
            "repository": canonical.repository,
            "commit": commit,
            "subdir": canonical.subdir,
            "referenced_at": _now(),
        },
    )
    _reset_fetched_workflow_cache()
    from .scaffold import _claimed_names

    claimed = _claimed_names()
    for name in (provider.name, provider.alias):
        if name is not None and name in claimed:
            _LOGGER.warning(
                "fetched workflow short name %r is shadowed by another workflow; reference it by its IRI %s",
                name,
                canonical,
            )
    return provider


type _Entry = tuple[WorkflowProvider, tuple[str, str | None], str]
type _Data = tuple[Mapping[str, _Entry], Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]]
_FETCHED_CACHE: tuple[Path, _Data] | None = None


def _fetched_data() -> _Data:
    """Load installed entries: by IRI, winners by short name, conflicted names."""

    global _FETCHED_CACHE
    directory = _root() / "installed"
    if _FETCHED_CACHE is not None and _FETCHED_CACHE[0] == directory:
        return _FETCHED_CACHE[1]
    entries: dict[str, _Entry] = {}
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(document, dict) or document.get("format") != _INSTALLED_FORMAT:
                raise ValueError("not an installed workflow entry")
            if document.get("format_version") != 1:
                raise ValueError(f"unsupported format_version {document.get('format_version')!r}")
            iri = parse_workflow_iri(document["iri"])
            referenced_at = document["referenced_at"]
            if (
                not iri.pinned
                or str(iri) != document["iri"]
                or path != _installed_path(str(iri))
                or document.get("repository") != iri.repository
                or document.get("commit") != iri.ref
                or document.get("subdir") != iri.subdir
                or not isinstance(referenced_at, str)
            ):
                raise ValueError("inconsistent installed workflow entry")
            assert iri.ref is not None
            tree = _checkouts(iri.repository) / iri.ref
            if not tree.is_dir():
                raise ValueError(f"cached checkout is missing: {tree}")
            entries[str(iri)] = (_provider(iri, tree), (iri.repository, iri.subdir), referenced_at)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            _LOGGER.warning("Skipping installed workflow entry %s: %s", path, exc)
    claims: dict[str, list[str]] = {}
    for key, (provider, _, _) in entries.items():
        for name in {provider.name, provider.alias} - {None}:
            assert name is not None
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
    """Return the installed git workflows keyed by canonical IRI, without running git.

    :return: The installed providers by canonical IRI.
    """

    return MappingProxyType({key: entry[0] for key, entry in _fetched_data()[0].items()})


def _installed_provider(text: str) -> WorkflowProvider | None:
    """Return the installed provider for exactly this pinned IRI; never fetch."""

    try:
        iri = parse_workflow_iri(text)
    except ValueError:
        return None
    entry = _fetched_data()[0].get(str(iri)) if iri.pinned else None
    return None if entry is None else entry[0]


def _fetched_names() -> tuple[Mapping[str, WorkflowProvider], Mapping[str, tuple[str, ...]]]:
    """Return short-name winners and the names claimed by several lineages."""

    data = _fetched_data()
    return data[1], data[2]


def _fetched_provider(name: str) -> WorkflowProvider | None:
    """Return the fetched workflow a short name means, latest reference wins."""

    winners, conflicts = _fetched_names()
    if name in conflicts:
        raise ValueError(
            f"workflow name {name!r} is claimed by several fetched workflows ({', '.join(conflicts[name])}); "
            "reference one by its IRI"
        )
    return winners.get(name)


def _reset_fetched_workflow_cache() -> None:
    """Clear cached installed-workflow discovery (after installs and in tests)."""

    global _FETCHED_CACHE
    _FETCHED_CACHE = None
