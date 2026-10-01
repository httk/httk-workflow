"""Execute one workspace MPI application after rank containment is active."""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from ._daemon_mpi_protocol import read_manifest
from ._daemon_policy import load_policy

_POLICY_PATH = Path("/daemon-policy.json")
_WORKSPACE = Path("/workspace")


def _application_directory(cwd: str) -> Path:
    """Resolve a manifest cwd and require it to remain below the workspace."""

    workspace = _WORKSPACE.resolve(strict=True)
    target = (_WORKSPACE / cwd).resolve(strict=True)
    if target != workspace and not target.is_relative_to(workspace):
        raise ValueError("MPI application cwd escapes /workspace")
    if not target.is_dir():
        raise ValueError("MPI application cwd is not a directory")
    return target


def _run(profile_name: str, handle: str, request_id: str) -> int:
    """Validate the confined rank context and replace it with the application."""

    policy = load_policy(_POLICY_PATH)
    profile = policy.profile(profile_name)
    if policy.mpi is None or profile.mpi is None:
        raise ValueError("selected daemon profile is not configured for MPI")
    manifest = read_manifest(_WORKSPACE, handle, request_id)
    if manifest.workspace_id != policy.workspace_id:
        raise ValueError("MPI manifest workspace identity does not match policy")
    directory = _application_directory(manifest.cwd)
    environment = dict(os.environ)
    environment.update(policy.mpi.environment)
    environment.update(manifest.environment)
    os.chdir(directory)
    try:
        os.execvpe(manifest.argv[0], list(manifest.argv), environment)
    except OSError as exc:
        print(f"daemon MPI application exec failed: {exc}", file=sys.stderr)
        return 127
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Validate one in-sandbox manifest and execute its application.

    :param argv: Internal rank arguments, or process arguments when omitted.
    :return: A bounded setup or execution failure status if exec does not replace the process.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--handle", required=True)
    parser.add_argument("--request-id", required=True)
    arguments = parser.parse_args(argv)
    try:
        return _run(arguments.profile, arguments.handle, arguments.request_id)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"daemon MPI rank: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
