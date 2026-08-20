"""Manage XDG configuration and identity for workflow commands.

The per-user configuration and data directory functions are provided by
``httk.core.userdirs`` and re-exported here for workflow callers.
"""

import base64
import configparser
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from httk.core.crypto import (
    ed25519_generate_seed,
    ed25519_public_key,
    ed25519_sign,
    ed25519_verify,
)
from httk.core.userdirs import config_home, data_home

from ._util import json_bytes, write_json_atomic

__all__ = [
    "CONFIG_FORMAT",
    "CONFIG_FORMAT_VERSION",
    "CONFIG_KEYS",
    "IDENTITY_KEY_MEMBER",
    "IDENTITY_SIGNATURE_DOMAIN",
    "IDENTITY_SIGNATURE_MEMBER",
    "ConfigKey",
    "DocumentSignature",
    "config_home",
    "config_path",
    "data_home",
    "ensure_identity_key",
    "identity_key_paths",
    "identity_public_key",
    "identity_seed",
    "import_v1_configuration",
    "initialize_config",
    "keys_home",
    "machine_names",
    "read_config",
    "remotes_home",
    "set_config_key",
    "settable_config_keys",
    "sign_document",
    "signature_digest",
    "unset_config_key",
    "verify_document",
    "write_config",
]

CONFIG_FORMAT = "httk-config"
CONFIG_FORMAT_VERSION = 2

#: Domain separation of every detached identity signature, so a digest signed
#: for one purpose can never be replayed as a signature of something else.
IDENTITY_SIGNATURE_DOMAIN = b"httk-workflow-identity-v2\0"
#: The members an identity signature adds to the document it signs.
IDENTITY_KEY_MEMBER = "operator_key"
IDENTITY_SIGNATURE_MEMBER = "signature"


@dataclass(frozen=True)
class ConfigKey:
    """Describe one member the user configuration is allowed to carry.

    :param name: Configuration member name.
    :param description: Human-readable explanation shown to operators.
    :param settable: Whether ``config set`` may change the member.
    """

    name: str
    description: str
    settable: bool = True


#: Every member ``config.json`` may hold. A key that nothing reads is a typo
#: that silently does nothing, so ``config set`` refuses anything not listed
#: here, and the members that describe or derive the document are not settable.
CONFIG_KEYS: Mapping[str, ConfigKey] = {
    "machine_names": ConfigKey("machine_names", "comma-separated names this machine answers to"),
    "name": ConfigKey("name", "the operator's name, recorded on requests and in reports"),
    "email": ConfigKey("email", "the operator's email address"),
    "format": ConfigKey("format", "the document format; written by httk", settable=False),
    "format_version": ConfigKey("format_version", "the document format version; written by httk", settable=False),
    "imported_from": ConfigKey("imported_from", "the legacy tree config import-v1 read", settable=False),
    "legacy_public_key": ConfigKey("legacy_public_key", "the imported legacy public identity", settable=False),
}


def settable_config_keys() -> tuple[str, ...]:
    """Return the configuration keys ``config set`` accepts, in order.

    :return: Settable configuration member names.
    """

    return tuple(sorted(name for name, key in CONFIG_KEYS.items() if key.settable))


def remotes_home() -> Path:
    """Return where this user's remote definitions live.

    :return: Per-user remote definition directory.
    """

    return config_home() / "remotes"


def keys_home() -> Path:
    """Return where this user's identity keys live.

    :return: Per-user identity key directory.
    """

    return config_home() / "keys"


def config_path() -> Path:
    """Return the path of this user's configuration file.

    :return: User configuration path.
    """
    return config_home() / "config.json"


def read_config() -> dict[str, object]:
    """Read the user configuration, returning an empty mapping if absent.

    A document of an unrecognized format or version is refused by name rather
    than read as if its members meant what this implementation means by them.

    :return: Configuration members, or an empty mapping when no file exists.
    :raises ValueError: If the file is not a supported configuration document.
    """

    path = config_path()
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"configuration is not a JSON object: {path}")
    recorded_format = value.get("format")
    if recorded_format != CONFIG_FORMAT:
        raise ValueError(f"configuration is not a {CONFIG_FORMAT} document but {recorded_format!r}: {path}")
    version = value.get("format_version")
    if version != CONFIG_FORMAT_VERSION:
        raise ValueError(
            f"configuration {path} uses {CONFIG_FORMAT} version {version!r}, "
            f"but this implementation reads version {CONFIG_FORMAT_VERSION}"
        )
    return value


def machine_names() -> frozenset[str]:
    """Return the configured names by which this machine is addressed.

    :return: Names configured for this machine.
    :raises ValueError: If ``machine_names`` is not a valid comma-separated value.
    """

    value = read_config().get("machine_names")
    if value is None:
        return frozenset()
    if not isinstance(value, str):
        raise ValueError("configuration key 'machine_names' must be a comma-separated string")
    names = [item.strip() for item in value.split(",")]
    if any(not item for item in names):
        raise ValueError("configuration key 'machine_names' contains an empty name")
    return frozenset(names)


def write_config(values: Mapping[str, object]) -> Path:
    """Write a versioned user configuration.

    :param values: Configuration members to write.
    :return: Path of the written configuration file.
    """

    value = dict(values)
    value.setdefault("format", CONFIG_FORMAT)
    value.setdefault("format_version", CONFIG_FORMAT_VERSION)
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, value)
    return path


def _settable(key: str) -> ConfigKey:
    """Return the registry entry of one settable key, or say what is legal."""

    entry = CONFIG_KEYS.get(key)
    legal = ", ".join(settable_config_keys())
    if entry is None:
        raise ValueError(f"unknown configuration key {key!r}; the settable keys are {legal}")
    if not entry.settable:
        raise ValueError(
            f"configuration key {key!r} is written by httk and cannot be set; the settable keys are {legal}"
        )
    return entry


def set_config_key(key: str, value: str) -> Path:
    """Set one registered configuration key and return the written path.

    :param key: Settable configuration member name.
    :param value: New member value.
    :return: Path of the written configuration file.
    :raises ValueError: If the key is not settable or its value is invalid.
    """

    _settable(key)
    if key == "machine_names":
        names = [item.strip() for item in value.split(",")]
        if any(not item for item in names):
            raise ValueError("configuration key 'machine_names' contains an empty name")
    values = read_config()
    values[key] = value
    return write_config(values)


def unset_config_key(key: str) -> Path:
    """Remove one registered configuration key and return the written path.

    :param key: Settable configuration member name.
    :return: Path of the written configuration file.
    :raises ValueError: If the key is not settable or is not configured.
    """

    _settable(key)
    values = read_config()
    if key not in values:
        raise ValueError(f"configuration key is not set: {key}")
    del values[key]
    return write_config(values)


def initialize_config(*, name: str, email: str) -> dict[str, object]:
    """Create or update the user identity and ensure a signing key exists.

    :param name: Operator name to record in the configuration.
    :param email: Operator email address to record in the configuration.
    :return: The resulting configuration members.
    """

    values = read_config()
    values.update(
        {
            "format": CONFIG_FORMAT,
            "format_version": CONFIG_FORMAT_VERSION,
            "name": name,
            "email": email,
        }
    )
    write_config(values)
    ensure_identity_key()
    return values


def identity_key_paths() -> tuple[Path, Path]:
    """Return the paths of the local identity seed and public key.

    :return: Seed path followed by public-key path.
    """
    root = keys_home()
    return root / "identity.seed", root / "identity.pub"


def ensure_identity_key() -> tuple[Path, Path]:
    """Create the user's standard Ed25519 identity key if it is absent.

    :return: Seed path followed by public-key path.
    :raises ValueError: If an existing seed is not a standard Ed25519 seed.
    """

    private_path, public_path = identity_key_paths()
    if private_path.exists():
        encoded = private_path.read_text(encoding="ascii").strip()
        seed = base64.b64decode(encoded, validate=True)
        if len(seed) != 32:
            raise ValueError(
                f"identity key is not a standard 32-byte Ed25519 seed: {private_path}; "
                "use config import-v1 explicitly for legacy material"
            )
    else:
        seed = ed25519_generate_seed()
        private_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(private_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as stream:
            stream.write(base64.b64encode(seed).decode("ascii") + "\n")
        os.chmod(private_path, 0o600)
    public_path.write_text(base64.b64encode(ed25519_public_key(seed)).decode("ascii") + "\n", encoding="ascii")
    return private_path, public_path


def identity_seed() -> bytes | None:
    """Return the local identity seed, or ``None`` when no key was created.

    Nothing here creates a key. An installation that never ran ``config init``
    simply has no identity, and every caller treats that as *unsigned* rather
    than as an error, which is what keeps a mixed deployment working.

    :return: The local seed, or no value when no valid key exists.
    """

    private_path, _ = identity_key_paths()
    try:
        seed = base64.b64decode(private_path.read_text(encoding="ascii").strip(), validate=True)
    except (OSError, UnicodeError, ValueError):
        return None
    return seed if len(seed) == 32 else None


def identity_public_key() -> str | None:
    """Return the recorded local identity public key, or ``None``.

    :return: Encoded public key, or ``None`` when no identity exists.
    """

    seed = identity_seed()
    return None if seed is None else "ed25519:" + base64.b64encode(ed25519_public_key(seed)).decode("ascii")


def signature_digest(document: Mapping[str, object]) -> bytes:
    """Return the domain-separated digest one identity signature covers.

    The digest covers the whole document except the signature itself, in the
    same canonical JSON every other httk document is hashed as, so the signing
    key and the signed members travel together and neither can be swapped.

    :param document: Document whose detached signature is being calculated.
    :return: Domain-separated digest of the unsigned document.
    """

    body = {name: value for name, value in document.items() if name != IDENTITY_SIGNATURE_MEMBER}
    return hashlib.sha256(IDENTITY_SIGNATURE_DOMAIN + json_bytes(body)).digest()


def sign_document(document: Mapping[str, object]) -> dict[str, object]:
    """Return *document* with a detached identity signature, when one is possible.

    Signing is optional by construction: a caller with no identity key returns
    the document unchanged, and a verifier accepts an unsigned document. The
    signature is attribution — it says which identity published this — and never
    authorization: nothing is permitted because a document is signed.

    :param document: Document to copy and optionally sign.
    :return: Document with identity members when a local key exists.
    """

    seed = identity_seed()
    if seed is None:
        return dict(document)
    body = {
        **{name: value for name, value in document.items() if name != IDENTITY_SIGNATURE_MEMBER},
        IDENTITY_KEY_MEMBER: "ed25519:" + base64.b64encode(ed25519_public_key(seed)).decode("ascii"),
    }
    signature = ed25519_sign(seed, signature_digest(body))
    return {**body, IDENTITY_SIGNATURE_MEMBER: base64.b64encode(signature).decode("ascii")}


@dataclass(frozen=True)
class DocumentSignature:
    """Report what checking one document's optional identity signature established.

    :param present: Whether the document carried a signature block.
    :param valid: Whether the carried signature verified.
    :param operator_key: Encoded key from the signature, when present.
    :param reason: Explanation when the signature is absent or invalid.
    """

    present: bool
    valid: bool
    operator_key: str | None = None
    reason: str | None = None


def verify_document(document: Mapping[str, object]) -> DocumentSignature:
    """Check the optional identity signature of *document*.

    An absent signature is reported as absent rather than as a failure, so a
    document published by an installation without an identity key stays usable.
    A signature that is present and does not verify is a failure: it is either
    damaged or forged, and neither is something to act on.

    :param document: Document whose optional signature is checked.
    :return: Signature presence and verification result.
    """

    key = document.get(IDENTITY_KEY_MEMBER)
    signature = document.get(IDENTITY_SIGNATURE_MEMBER)
    if key is None and signature is None:
        return DocumentSignature(False, False)
    if not isinstance(key, str) or not isinstance(signature, str):
        return DocumentSignature(True, False, reason="the signature block is incomplete")
    text = key.removeprefix("ed25519:") if key.startswith("ed25519:") else key
    try:
        public_key = base64.b64decode(text, validate=True)
        raw_signature = base64.b64decode(signature, validate=True)
    except ValueError:
        return DocumentSignature(True, False, key, "the signature block is not valid base64")
    if len(public_key) != 32:
        return DocumentSignature(True, False, key, "the operator key is not a 32-byte Ed25519 key")
    if not ed25519_verify(public_key, signature_digest(document), raw_signature):
        return DocumentSignature(True, False, key, "the signature does not verify against the document")
    return DocumentSignature(True, True, key)


def import_v1_configuration(source: str | os.PathLike[str] | None = None) -> dict[str, object]:
    """Import safe metadata and public identity from a legacy ``~/.httk`` tree.

    Legacy 64-byte private material is deliberately left untouched.

    :param source: Legacy configuration root, or the default legacy home.
    :return: Imported configuration members.
    :raises FileNotFoundError: If the legacy configuration file is absent.
    """

    root = Path(source).expanduser().resolve() if source is not None else (Path.home() / ".httk").resolve()
    legacy_config = root / "config"
    if not legacy_config.is_file():
        raise FileNotFoundError(legacy_config)
    parser = configparser.ConfigParser()
    parser.read(legacy_config, encoding="utf-8")
    current = read_config()
    current.update(
        {
            "format": "httk-config",
            "format_version": 2,
            "name": parser.get("main", "name", fallback=str(current.get("name", ""))),
            "email": parser.get("main", "email", fallback=str(current.get("email", ""))),
            "imported_from": str(root),
        }
    )
    legacy_public = root / "keys" / "key1.pub"
    if legacy_public.is_file():
        public_target = keys_home() / "legacy-identity.pub"
        public_target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        public_target.write_bytes(legacy_public.read_bytes())
        current["legacy_public_key"] = str(public_target)
    write_config(current)
    return current
