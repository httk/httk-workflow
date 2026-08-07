"""Expose workflow project policy over the project anchor owned by :mod:`httk.core.project`.

The project anchor — the ``httk_project`` directory, ``project.json`` and its
validation, upward discovery, the identity key, and key pinning and trust —
moved to *httk-core* as :mod:`httk.core.project` so a core-only installation has
projects. This module re-exports that API unchanged (this is an internal move,
so no deprecation is warranted and every public name keeps working) and keeps
the pieces that are workflow policy rather than anchor:

* :data:`DEFAULT_MANIFEST_EXCLUSIONS` and :func:`project_exclusions` — what a
  signed manifest never records, which is a property of the manifest format and
  therefore stays here beside :mod:`httk.workflow.manifests`.
* :func:`initialize_project` and :func:`import_v1_project` — the anchor plus the
  project's registered default workflow workspace.
"""

import os
from collections.abc import Iterable

# The anchor API, re-exported so existing imports of httk.workflow.projects keep
# resolving. httk.core.project owns every one of these now.
from httk.core.project import (
    PROJECT_DIRECTORY,
    PROJECT_FILE,
    PUBLIC_KEY_PREFIX,
    LegacyProjectError,
    canonical_public_key,
    discover_project,
    format_public_key,
    key_fingerprint,
    parse_public_key,
    pin_project_key,
    pinned_project_key,
    project_public_key_path,
    read_project,
    read_project_section,
    read_public_key_file,
    require_project,
    trust_project_key,
    trusted_project_keys,
    write_project_section,
)
from httk.core.project import import_v1_project as _import_v1_anchor
from httk.core.project import initialize_project as _initialize_anchor

__all__ = [
    "DEFAULT_MANIFEST_EXCLUSIONS",
    "PROJECT_DIRECTORY",
    "PROJECT_FILE",
    "PUBLIC_KEY_PREFIX",
    "LegacyProjectError",
    "canonical_public_key",
    "discover_project",
    "format_public_key",
    "import_v1_project",
    "initialize_project",
    "key_fingerprint",
    "parse_public_key",
    "pin_project_key",
    "pinned_project_key",
    "project_exclusions",
    "project_public_key_path",
    "read_project",
    "read_project_section",
    "read_public_key_file",
    "require_project",
    "trust_project_key",
    "trusted_project_keys",
    "write_project_section",
]

#: Paths a signed project manifest never records.
#:
#: The patterns are matched with :func:`fnmatch.fnmatchcase` against a relative
#: POSIX path, and there ``*`` spans ``/`` as well. A ``**/`` prefix therefore
#: only matches a path that *has* a slash, so every runner-private name is also
#: listed in its bare root-level form; otherwise a ``.httk-attempt.*`` directory
#: that happens to sit at the project root would be signed as project content.
#:
#: This is manifest policy — a property of the signed-manifest format that lives
#: with it in *httk-workflow*, not part of the anchor. The anchor only validates
#: and passes through the project's own ``manifest_exclusions`` member.
DEFAULT_MANIFEST_EXCLUSIONS = (
    # project.json holds the trust anchors a manifest is checked against, and an
    # anchor that lives inside what it authenticates cannot be adopted without
    # invalidating it. The two members a manifest actually depends on — the
    # project identity and the exclusion list — are carried in the signed
    # manifest header instead, and the header's project_id is compared with this
    # file's on every verification.
    f"{PROJECT_DIRECTORY}/project.json",
    f"{PROJECT_DIRECTORY}/keys/*.seed",
    f"{PROJECT_DIRECTORY}/keys/*.priv",
    f"{PROJECT_DIRECTORY}/remotes/**/credentials*",
    f"{PROJECT_DIRECTORY}/computers/**/credentials*",
    f"{PROJECT_DIRECTORY}/manifest.jsonl.bz2",
    # A stale pre-release anchor may remain after a copy or partial migration;
    # never publish its credentials even though discovery no longer accepts it.
    ".httk-project/project.json",
    ".httk-project/keys/*.seed",
    ".httk-project/keys/*.priv",
    ".httk-project/remotes/**/credentials*",
    ".httk-project/computers/**/credentials*",
    ".httk-project/manifest.jsonl.bz2",
    ".httk-workflow",
    ".httk-workflow/**",
    ".httk-attempt.*",
    ".httk-attempt.*/**",
    ".httk-job",
    ".httk-job/**",
    "**/.httk-attempt.*",
    "**/.httk-attempt.*/**",
    "**/.httk-job",
    "**/.httk-job/**",
)


def initialize_project(
    root: str | os.PathLike[str],
    *,
    name: str,
    description: str = "",
    manifest_exclusions: Iterable[str] = (),
) -> dict[str, object]:
    """Initialize the project anchor without creating a workspace.

    :param root: Directory in which to create the project anchor.
    :param name: Project name.
    :param description: Optional project description.
    :param manifest_exclusions: Relative paths excluded from the manifest.
    :return: Created project metadata.
    """

    return _initialize_anchor(
        root,
        name=name,
        description=description,
        manifest_exclusions=manifest_exclusions,
    )


def import_v1_project(
    root: str | os.PathLike[str],
    *,
    source: str | os.PathLike[str] | None = None,
    name: str | None = None,
) -> dict[str, object]:
    """Create v2 project metadata from a legacy ``ht.project`` directory.

    :param root: Directory in which to create the project anchor.
    :param source: Legacy project directory, or the default legacy location.
    :param name: Replacement project name, or the legacy name.
    :return: Created project metadata.
    """

    return _import_v1_anchor(root, source=source, name=name)


def project_exclusions(metadata: dict[str, object]) -> tuple[str, ...]:
    """Return default and configured manifest exclusions.

    :param metadata: Project metadata containing optional exclusions.
    :return: Exclusion patterns applied to the signed manifest.
    :raises ValueError: If configured exclusions are not strings in an array.
    """
    configured = metadata.get("manifest_exclusions", [])
    if not isinstance(configured, list) or not all(isinstance(item, str) for item in configured):
        raise ValueError("manifest_exclusions must be an array of strings")
    return (*DEFAULT_MANIFEST_EXCLUSIONS, *(str(item) for item in configured))
