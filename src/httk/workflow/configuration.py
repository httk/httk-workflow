"""Manage XDG configuration for workflow commands.

The per-user configuration and data directory functions are provided by
``httk.core.userdirs`` and re-exported here for workflow callers. Operator
identity — signing keys, named identities, and signatures — lives in
``httk.core.identity``; the workflow configuration below holds only
machine-level settings such as ``machine_names``.
"""

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from httk.core.identity import import_v1_identity
from httk.core.userdirs import config_home, data_home

from ._util import write_json_atomic

__all__ = [
    "CONFIG_FORMAT",
    "CONFIG_FORMAT_VERSION",
    "CONFIG_KEYS",
    "ConfigKey",
    "config_home",
    "config_path",
    "data_home",
    "import_v1_configuration",
    "launchers_home",
    "machine_names",
    "read_config",
    "remotes_home",
    "set_config_key",
    "settable_config_keys",
    "unset_config_key",
    "write_config",
]

CONFIG_FORMAT = "httk-config"
CONFIG_FORMAT_VERSION = 2


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
#: Operator identity (name, email, named identities, and the default) lives in
#: ``identity.json`` managed by ``httk.core.identity``, not here.
CONFIG_KEYS: Mapping[str, ConfigKey] = {
    "machine_names": ConfigKey("machine_names", "comma-separated names this machine answers to"),
    "format": ConfigKey("format", "the document format; written by httk", settable=False),
    "format_version": ConfigKey("format_version", "the document format version; written by httk", settable=False),
    "imported_from": ConfigKey("imported_from", "the legacy tree config import-v1 read", settable=False),
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


def launchers_home() -> Path:
    """Return where this user's manager launcher definitions live.

    :return: Per-user manager launcher definition directory.
    """

    return config_home() / "launchers"


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


def import_v1_configuration(source: str | os.PathLike[str] | None = None) -> dict[str, object]:
    """Import safe metadata and public identity from a legacy ``~/.httk`` tree.

    The operator name, email, and public identity are recorded in the per-user
    identity configuration via ``httk.core.identity.import_v1_identity``; the
    workflow configuration records only where the import came from. Legacy
    64-byte private material is deliberately left untouched.

    :param source: Legacy configuration root, or the default legacy home.
    :return: Imported configuration and identity members.
    :raises FileNotFoundError: If the legacy configuration file is absent.
    """

    root = Path(source).expanduser().resolve() if source is not None else (Path.home() / ".httk").resolve()
    identity_values = import_v1_identity(root)

    current = read_config()
    current.update(
        {
            "format": CONFIG_FORMAT,
            "format_version": CONFIG_FORMAT_VERSION,
            "imported_from": str(root),
        }
    )
    write_config(current)
    return {**current, **identity_values}
