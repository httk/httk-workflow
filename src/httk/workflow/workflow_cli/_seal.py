"""What ``job verify`` and ``workspace verify --content`` share: the default trust anchors and the verdict report.

The ``seal``/``unseal``/``verify`` verbs live beside the subjects they act on: ``job`` and ``workspace`` here,
``project`` in httk-core.
"""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from httk.core.identity import local_public_keys
from httk.core.project.manifests import resolve_trusted_keys

from ..projects import discover_project
from ..seals import VALID_TRUSTED, VALID_UNKNOWN_KEY


def _default_trusted_keys(path: Path, explicit: list[str]) -> list[str]:
    """Return the trust anchors a seal verification uses by default.

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


def render_seal_entries(entries: Sequence[Mapping[str, object]], *, as_json: bool) -> int:
    """Print the verdicts of a seal verification and return its exit status.

    Two independent axes, exactly as a signed manifest reports them: whether the seals still describe the tree, and
    whether every signer is a trusted anchor. Exit codes mirror the manifest verdicts (``VERDICT_EXIT_CODES``):
    0 when every seal is valid and trusted, 3 when one is valid but signed by an unknown key, 1 otherwise.

    :param entries: The verdict entries (``SealVerification.as_entry``).
    :param as_json: Print one JSON document (``entries``, ``ok``, ``trusted``) instead of one line per entry.
    :return: The exit status.
    """

    verdicts = [str(entry["verdict"]) for entry in entries]
    ok = bool(entries) and all(bool(entry["valid"]) for entry in entries)
    trusted_only = bool(entries) and all(verdict == VALID_TRUSTED for verdict in verdicts)
    if not ok:
        exit_code, final = 1, "FAILED"
    elif VALID_UNKNOWN_KEY in verdicts:
        exit_code, final = 3, "UNTRUSTED"
    else:
        exit_code, final = 0, "ok"
    if as_json:
        print(json.dumps({"entries": list(entries), "ok": ok, "trusted": trusted_only}, indent=2, sort_keys=True))
        return exit_code
    for entry in entries:
        print(f"{entry['level']}\t{entry['subject']}\t{entry['verdict']}\t{entry['reason'] or '-'}")
        discrepancies = entry["discrepancies"]
        assert isinstance(discrepancies, list)
        for discrepancy in discrepancies:
            print(f"  {discrepancy['kind']}\t{discrepancy['path']}")
    print(final)
    return exit_code
