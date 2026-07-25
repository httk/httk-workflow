"""Project discovery and versioned project metadata."""

import base64
import configparser
import json
import os
import uuid
from collections.abc import Iterable
from pathlib import Path

from httk.core import ed25519_generate_seed, ed25519_public_key

from ._util import write_json_atomic
from .workspace import WorkflowWorkspace

PROJECT_DIRECTORY = ".httk-project"
PROJECT_FILE = "project.json"
DEFAULT_MANIFEST_EXCLUSIONS = (
    ".httk-project/keys/*.seed",
    ".httk-project/keys/*.priv",
    ".httk-project/computers/**/credentials*",
    ".httk-project/manifest.jsonl.bz2",
    ".httk-workflow",
    ".httk-workflow/**",
    "**/.httk-attempt.*",
    "**/.httk-attempt.*/**",
)


def discover_project(start: str | os.PathLike[str] | None = None) -> Path | None:
    """Find the nearest project root at or above *start*."""

    path = Path.cwd() if start is None else Path(start)
    path = path.expanduser().resolve()
    if path.is_file():
        path = path.parent
    for candidate in (path, *path.parents):
        if (candidate / PROJECT_DIRECTORY / PROJECT_FILE).is_file():
            return candidate
    return None


def require_project(start: str | os.PathLike[str] | None = None) -> Path:
    project = discover_project(start)
    if project is None:
        raise ValueError("no .httk-project project exists at or above the working directory")
    return project


def read_project(root: str | os.PathLike[str]) -> dict[str, object]:
    path = Path(root).resolve() / PROJECT_DIRECTORY / PROJECT_FILE
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise ValueError(f"project metadata is not a JSON object: {path}")
    if value.get("format") != "httk-project" or value.get("format_version") != 1:
        raise ValueError("unsupported httk project format")
    return value


def _write_project_key(control: Path) -> None:
    seed = ed25519_generate_seed()
    key_dir = control / "keys"
    key_dir.mkdir()
    private_path = key_dir / "project.seed"
    descriptor = os.open(private_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="ascii") as stream:
        stream.write(base64.b64encode(seed).decode("ascii") + "\n")
    os.chmod(private_path, 0o600)
    (key_dir / "project.pub").write_text(
        base64.b64encode(ed25519_public_key(seed)).decode("ascii") + "\n",
        encoding="ascii",
    )


def initialize_project(
    root: str | os.PathLike[str],
    *,
    name: str,
    description: str = "",
    default_queue: str | None = None,
    manifest_exclusions: Iterable[str] = (),
) -> dict[str, object]:
    """Initialize project metadata, keys, and a detached-transfer workspace."""

    project = Path(root).expanduser().resolve()
    project.mkdir(parents=True, exist_ok=True)
    control = project / PROJECT_DIRECTORY
    control.mkdir(exist_ok=False)
    metadata: dict[str, object] = {
        "format": "httk-project",
        "format_version": 1,
        "project_id": str(uuid.uuid4()),
        "name": name,
        "description": description,
        "default_queue": default_queue,
        "manifest_exclusions": list(manifest_exclusions),
    }
    write_json_atomic(control / PROJECT_FILE, metadata)
    _write_project_key(control)
    (control / "computers").mkdir()
    try:
        WorkflowWorkspace.initialize(project, extensions=("detached-transfer-v1",))
    except Exception:
        # Leave a recognizable project rather than guessing whether it is safe
        # to remove a directory that may already contain user files.
        metadata["workspace_initialization_failed"] = True
        write_json_atomic(control / PROJECT_FILE, metadata)
        raise
    return metadata


def import_v1_project(
    root: str | os.PathLike[str],
    *,
    source: str | os.PathLike[str] | None = None,
    name: str | None = None,
) -> dict[str, object]:
    """Create v2 project metadata from a legacy ``ht.project`` directory."""

    project = Path(root).expanduser().resolve()
    legacy = Path(source).expanduser().resolve() if source is not None else project / "ht.project"
    if not legacy.is_dir():
        raise FileNotFoundError(legacy)
    parser = configparser.ConfigParser()
    parser.read(legacy / "config", encoding="utf-8")
    project_name = name if name is not None else str(parser.get("main", "project_name", fallback=project.name))
    metadata = initialize_project(project, name=project_name)
    metadata["imported_from"] = str(legacy)
    public_keys: list[str] = []
    destination = project / PROJECT_DIRECTORY / "keys" / "legacy-public"
    for public in sorted((legacy / "keys").glob("*.pub")) if (legacy / "keys").is_dir() else ():
        destination.mkdir(exist_ok=True)
        target = destination / public.name
        target.write_bytes(public.read_bytes())
        public_keys.append(str(target.relative_to(project)))
    metadata["legacy_public_keys"] = public_keys
    metadata["legacy_queue_imported"] = False
    write_json_atomic(project / PROJECT_DIRECTORY / PROJECT_FILE, metadata)
    return metadata


def project_exclusions(metadata: dict[str, object]) -> tuple[str, ...]:
    configured = metadata.get("manifest_exclusions", [])
    if not isinstance(configured, list) or not all(isinstance(item, str) for item in configured):
        raise ValueError("manifest_exclusions must be an array of strings")
    return (*DEFAULT_MANIFEST_EXCLUSIONS, *(str(item) for item in configured))
