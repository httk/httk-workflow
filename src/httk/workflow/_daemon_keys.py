"""Protected response-signing key lifecycle for the workspace daemon."""

import base64
import os
import stat
from pathlib import Path

from httk.core.crypto import ed25519_generate_seed, ed25519_public_key

from . import _fs

RESPONSE_SEED_NAME = "response.seed"
_ENCODED_SEED_BYTES = 44


def _open_directory(path: Path) -> int:
    """Open an absolute directory without following any path component."""

    if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts or "\0" in str(path):
        raise ValueError("response key directory must be an absolute Path without '..' or NUL")
    descriptor = os.open(path.anchor or "/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:]:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            previous, descriptor = descriptor, next_descriptor
            os.close(previous)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _decode_seed(data: bytes) -> bytes:
    """Decode the one canonical base64 seed file representation."""

    encoded = data[:-1] if data.endswith(b"\n") else data
    if len(encoded) != _ENCODED_SEED_BYTES or b"\n" in encoded or b"\r" in encoded:
        raise ValueError("daemon response seed is not canonical base64")
    try:
        seed = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("daemon response seed is not canonical base64") from exc
    if len(seed) != 32 or base64.b64encode(seed) != encoded:
        raise ValueError("daemon response seed is not a standard 32-byte Ed25519 seed")
    return seed


def response_seed_path(directory: Path) -> Path:
    """Return the protected response seed path below a state directory.

    :param directory: Protected daemon state directory.
    :return: Absolute path of the response-signing seed.
    """

    if not isinstance(directory, Path) or not directory.is_absolute() or ".." in directory.parts:
        raise ValueError("response key directory must be an absolute Path without '..'")
    return directory / RESPONSE_SEED_NAME


def initialize_response_seed(directory: Path) -> Path:
    """Exclusively create and synchronize a protected response-signing seed.

    :param directory: Existing protected daemon state directory.
    :return: Path of the newly created seed.
    :raises FileExistsError: If this enrollment already has a response seed.
    """

    directory_fd = _open_directory(directory)
    try:
        data = base64.b64encode(ed25519_generate_seed()) + b"\n"
        descriptor = _fs.create_exclusive(_fs.anchored(directory_fd, RESPONSE_SEED_NAME), data, durable=True)
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
    finally:
        os.close(directory_fd)
    return response_seed_path(directory)


def read_response_seed(path: Path) -> bytes:
    """Read and validate one protected response-signing seed without symlinks.

    :param path: Absolute response seed path.
    :return: Raw 32-byte Ed25519 seed.
    :raises OSError: If the seed cannot be opened safely.
    :raises ValueError: If its type, mode, ownership, or encoding is invalid.
    """

    if not isinstance(path, Path) or path.name != RESPONSE_SEED_NAME:
        raise ValueError(f"response seed path must end in {RESPONSE_SEED_NAME}")
    directory_fd = _open_directory(path.parent)
    descriptor = -1
    try:
        descriptor = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=directory_fd,
        )
        information = os.fstat(descriptor)
        if not stat.S_ISREG(information.st_mode):
            raise ValueError("daemon response seed must be a regular file")
        if stat.S_IMODE(information.st_mode) != 0o600:
            raise ValueError("daemon response seed must have mode 0600")
        if information.st_uid != os.geteuid():
            raise ValueError("daemon response seed must be owned by the daemon user")
        data = bytearray()
        while len(data) <= _ENCODED_SEED_BYTES + 1:
            chunk = os.read(descriptor, _ENCODED_SEED_BYTES + 2 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _ENCODED_SEED_BYTES + 1:
                raise ValueError("daemon response seed is too large")
        return _decode_seed(bytes(data))
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory_fd)


def response_public_key(path: Path) -> str:
    """Return the canonical public key for a validated response seed.

    :param path: Protected response seed path.
    :return: Canonical ``ed25519:`` public key.
    """

    return "ed25519:" + base64.b64encode(ed25519_public_key(read_response_seed(path))).decode("ascii")


__all__ = [
    "RESPONSE_SEED_NAME",
    "initialize_response_seed",
    "read_response_seed",
    "response_public_key",
    "response_seed_path",
]
