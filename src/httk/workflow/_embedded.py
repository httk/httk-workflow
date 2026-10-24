"""Embedded workflows: the framed digest of a job's own ``workflow/`` tree.

A job with an embedded workflow keeps its workflow package in ``<payload>/workflow/`` instead of naming a store
installation; ``job.json`` pins the tree by :func:`embedded_digest`, which is never the store's ``tree_digest``.
"""

import errno
import hashlib
import os
import stat
from collections.abc import Callable
from pathlib import Path, PurePosixPath

from httk.workflow import _fs
from httk.workflow.errors import FormatError
from httk.workflow.models import EMBEDDED_WORKFLOW_PREFIX

__all__ = ["embedded_digest", "embedded_id", "embedded_problem"]

_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


def embedded_id(name: str, tree_sha256: str) -> str:
    """Return the id of an embedded workflow: ``embedded:<name>@<first 12 hex digits of its digest>``.

    :param name: The workflow's short name.
    :param tree_sha256: Its :func:`embedded_digest`.
    :return: The id.
    """

    return f"{EMBEDDED_WORKFLOW_PREFIX}{name}@{tree_sha256[:12]}"


def embedded_problem(tree: Path, tree_sha256: str) -> str | None:
    """Return why a locked embedded-workflow job may not launch from *tree*, in the manager's words, or ``None``.

    :param tree: The job's ``workflow/``.
    :param tree_sha256: The digest ``job.json``'s ``embedded`` member froze at creation.
    :return: The problem, or ``None`` when *tree* has that digest.
    """

    try:
        actual = embedded_digest(tree)
    except FileNotFoundError:
        return "the job's workflow/ is missing"
    except (FormatError, OSError) as exc:
        return f"the job's workflow/ cannot be verified: {exc}"
    if actual != tree_sha256:
        return f"workflow/ changed since the job was created: digest {actual}, created {tree_sha256}"
    return None


def embedded_digest(directory: Path) -> str:
    """Return the framed SHA-256 of an embedded workflow tree, refusing anything but plain directories and files.

    The tree is walked descriptor-anchored, never following a symlink, in sorted name order per directory. Each
    directory contributes ``D\\0<path>\\n`` and each file ``F\\0<path>\\0<owner-exec 0|1>\\0<size>\\0<sha256>\\n``,
    with ``<path>`` relative to the root and ``<size>`` the bytes hashed, so no two trees share a digest
    without a SHA-256 collision.

    :param directory: The tree's root (a symlink there is refused).
    :return: The 64 hex digit digest.
    :raises FileNotFoundError: If *directory* does not exist.
    :raises httk.workflow.errors.FormatError: Naming the path of a symlink, special file, hard-linked file or
        ``__pycache__`` entry, of an entry deeper than :data:`httk.workflow._fs.DEFAULT_LIMITS`, or when the root
        is a symlink or not a directory.
    """

    digest = hashlib.sha256()
    try:
        root = _fs.open_dir(directory)
    except _fs.UnsafePath as exc:
        raise FormatError(f"embedded workflow tree {directory}: {exc}") from exc
    try:
        _walk(root, PurePosixPath(), digest.update)
    finally:
        os.close(root)
    return digest.hexdigest()


def _walk(descriptor: int, relative: PurePosixPath, update: Callable[[bytes], None]) -> None:
    for name in sorted(os.listdir(descriptor)):
        path = relative / name
        encoded = os.fsencode(path.as_posix())
        if name == "__pycache__":
            raise FormatError(f"embedded workflow tree holds bytecode: {path}; remove every __pycache__ directory")
        if len(path.parts) > _fs.DEFAULT_LIMITS.depth:
            raise FormatError(f"embedded workflow tree is deeper than {_fs.DEFAULT_LIMITS.depth} levels: {path}")
        mode = os.lstat(name, dir_fd=descriptor).st_mode
        if stat.S_ISDIR(mode):
            update(b"D\0" + encoded + b"\n")
            try:
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            except OSError as exc:
                raise FormatError(f"embedded workflow tree entry changed while digested: {path}") from exc
            try:
                _walk(child, path, update)
            finally:
                os.close(child)
        elif stat.S_ISREG(mode):
            update(b"F\0" + encoded + b"\0" + _file_frame(descriptor, name, path) + b"\n")
        else:
            kind = "a symlink" if stat.S_ISLNK(mode) else "a special file"
            raise FormatError(f"embedded workflow tree holds {kind}: {path}")


def _file_frame(descriptor: int, name: str, path: PurePosixPath) -> bytes:
    try:
        source = os.open(name, _FILE_FLAGS, dir_fd=descriptor)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise FormatError(f"embedded workflow tree holds a symlink: {path}") from exc
        raise
    try:
        opened = os.fstat(source)
        if not stat.S_ISREG(opened.st_mode):
            raise FormatError(f"embedded workflow tree holds a special file: {path}")
        if opened.st_nlink > 1:
            raise FormatError(f"embedded workflow tree holds a hard-linked file: {path}")
        content = hashlib.sha256()
        size = 0
        while chunk := os.read(source, 1 << 20):
            content.update(chunk)
            size += len(chunk)
    finally:
        os.close(source)
    executable = 1 if opened.st_mode & stat.S_IXUSR else 0
    return f"{executable}\0{size}\0{content.hexdigest()}".encode()
