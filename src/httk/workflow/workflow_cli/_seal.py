"""The read-only top-level ``seal verify`` command.

The three ``seal``/``unseal`` verbs that write and remove seals live beside the
subjects they act on — ``job seal`` in the job group, ``workspace seal`` in the
workspace group, ``project seal`` in the project group. This module carries only
the one verb that belongs to no single level: verifying a whole sealed tree.
"""

import argparse
import json
from pathlib import Path
from typing import Any

from httk.core.identity import local_public_keys
from httk.core.project.manifests import resolve_trusted_keys

from ..projects import discover_project
from ..seals import VALID_TRUSTED, VALID_UNKNOWN_KEY, verify_tree
from ._common import _group, _leaf


def _default_trusted_keys(path: Path, explicit: list[str]) -> list[str]:
    """Return the trust anchors ``seal verify`` uses by default.

    A tree sealed by its own project or its own operator identity should verify
    as trusted without the operator having to name a key: the project's pinned
    keys, plus every explicitly supplied key, plus the local identities' public
    keys when they exist.

    :param path: The tree to verify, from which the project is discovered.
    :param explicit: The ``--trusted-key`` values, keys or ``*.pub`` files.
    :return: The trust anchors to classify signers against.
    :raises ValueError: If an explicit key cannot be canonicalized.
    """

    project = discover_project(path)
    trusted = list(resolve_trusted_keys(project, trusted_keys=explicit))
    for identity_key in local_public_keys():
        if identity_key not in trusted:
            trusted.append(identity_key)
    return trusted


def handle_seal_verify(arguments: argparse.Namespace, context: Any) -> int:
    """Verify the seal at PATH and, unless shallow, every seal it references."""

    path = (Path(context.cwd) / Path(arguments.path).expanduser()).resolve()
    trusted = _default_trusted_keys(path, list(arguments.trusted_key))
    report = verify_tree(path, trusted_keys=trusted, deep=not arguments.shallow)
    # Two independent axes, exactly as a signed manifest reports them: whether the
    # seals still describe the tree (report.ok), and whether every signer is a
    # trusted anchor. Exit codes mirror manifests.VERDICT_EXIT_CODES.
    verdicts = [str(entry["verdict"]) for entry in report.entries]
    trusted_only = bool(report.entries) and all(verdict == VALID_TRUSTED for verdict in verdicts)
    if not report.ok:
        exit_code, final = 1, "FAILED"
    elif VALID_UNKNOWN_KEY in verdicts:
        exit_code, final = 3, "UNTRUSTED"
    else:
        exit_code, final = 0, "ok"
    if arguments.json:
        document = {"entries": list(report.entries), "ok": report.ok, "trusted": trusted_only}
        print(json.dumps(document, indent=2, sort_keys=True))
        return exit_code
    for entry in report.entries:
        print(f"{entry['level']}\t{entry['subject']}\t{entry['verdict']}\t{entry['reason'] or '-'}")
        discrepancies = entry["discrepancies"]
        assert isinstance(discrepancies, list)
        for discrepancy in discrepancies:
            print(f"  {discrepancy['kind']}\t{discrepancy['path']}")
    print(final)
    return exit_code


def build_seal_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the top-level ``seal`` group and its one ``verify`` leaf."""

    _, group = _group(
        subparsers,
        "seal",
        summary="verify a sealed job, workspace, or project tree",
        description=(
            "Verify seals. Writing seals lives beside each level (job seal, workspace seal, "
            "project seal); this group verifies a tree that is already sealed"
        ),
    )
    verify = _leaf(
        group,
        "verify",
        summary="verify a sealed tree against its seals",
        description="Verify the seal at PATH and, unless --shallow, every seal it references",
        handler=handle_seal_verify,
    )
    verify.add_argument(
        "path",
        metavar="PATH",
        nargs="?",
        default=".",
        help="a project root, workspace root, or job payload to verify (default: the working directory)",
    )
    verify.add_argument("--json", action="store_true", help="print the report as one JSON document")
    verify.add_argument(
        "--trusted-key",
        action="append",
        default=[],
        metavar="KEY_OR_FINGERPRINT",
        help=(
            "trust this key as well: an ed25519:BASE64 value, a sha256: fingerprint, or the path of a "
            "*.pub file (repeatable). The project's pinned keys and all local identities are always trusted"
        ),
    )
    verify.add_argument(
        "--shallow",
        action="store_true",
        help="verify only the top seal, not every seal it references",
    )
