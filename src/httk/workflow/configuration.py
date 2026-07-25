"""XDG configuration and identity management for workflow commands."""

import base64
import configparser
import json
import os
from collections.abc import Mapping
from pathlib import Path

from httk.core import ed25519_generate_seed, ed25519_public_key

from ._util import write_json_atomic


def config_home() -> Path:
    """Return the httk configuration directory."""

    override = os.environ.get("HTTK_CONFIG_HOME")
    if override:
        return Path(override).expanduser().resolve()
    base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return (base / "httk").resolve()


def data_home() -> Path:
    """Return the httk data directory."""

    override = os.environ.get("HTTK_DATA_HOME")
    if override:
        return Path(override).expanduser().resolve()
    base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return (base / "httk").resolve()


def config_path() -> Path:
    return config_home() / "config.json"


def read_config() -> dict[str, object]:
    """Read the user configuration, returning an empty mapping if absent."""

    path = config_path()
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"configuration is not a JSON object: {path}")
    return value


def write_config(values: Mapping[str, object]) -> Path:
    """Write a versioned user configuration."""

    value = dict(values)
    value.setdefault("format", "httk-config")
    value.setdefault("format_version", 1)
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, value)
    return path


def initialize_config(*, name: str, email: str) -> dict[str, object]:
    """Create or update the user identity and ensure a signing key exists."""

    values = read_config()
    values.update(
        {
            "format": "httk-config",
            "format_version": 1,
            "name": name,
            "email": email,
        }
    )
    write_config(values)
    ensure_identity_key()
    return values


def identity_key_paths() -> tuple[Path, Path]:
    root = data_home() / "keys"
    return root / "identity.seed", root / "identity.pub"


def ensure_identity_key() -> tuple[Path, Path]:
    """Create the user's standard Ed25519 identity key if it is absent."""

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
        private_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(private_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="ascii") as stream:
            stream.write(base64.b64encode(seed).decode("ascii") + "\n")
        os.chmod(private_path, 0o600)
    public_path.write_text(base64.b64encode(ed25519_public_key(seed)).decode("ascii") + "\n", encoding="ascii")
    return private_path, public_path


def import_v1_configuration(source: str | os.PathLike[str] | None = None) -> dict[str, object]:
    """Import safe metadata and public identity from a legacy ``~/.httk`` tree.

    Legacy 64-byte private material is deliberately left untouched.
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
            "format_version": 1,
            "name": parser.get("main", "name", fallback=str(current.get("name", ""))),
            "email": parser.get("main", "email", fallback=str(current.get("email", ""))),
            "imported_from": str(root),
        }
    )
    legacy_public = root / "keys" / "key1.pub"
    if legacy_public.is_file():
        public_target = data_home() / "keys" / "legacy-identity.pub"
        public_target.parent.mkdir(parents=True, exist_ok=True)
        public_target.write_bytes(legacy_public.read_bytes())
        current["legacy_public_key"] = str(public_target)
    write_config(current)
    return current
