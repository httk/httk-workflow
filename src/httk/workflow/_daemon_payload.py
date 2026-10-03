"""Start one manager after the trusted bootstrap has entered Bubblewrap."""

import argparse
import os
import re
import shlex
from collections.abc import Sequence
from pathlib import Path

from ._daemon_policy import Policy, Profile, load_policy
from .workspace import Workspace


def _manager_command(policy: Policy, profile: Profile) -> list[str]:
    mpi = profile.mpi
    executable = (
        [profile.manager_command]
        if profile.manager_command is not None
        else [str(policy.python), "-I", "-m", "httk.core.cli"]
    )
    command = [
        *executable,
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        "/workspace",
        "--count",
        "1",
        "--workers",
        str(profile.workers),
        "--worker-resource",
        "procs",
        str(profile.cpus * mpi.ranks if mpi is not None else profile.cpus),
        "--worker-resource",
        "mem",
        str(profile.memory_mb * mpi.nodes if mpi is not None else profile.memory_mb),
    ]
    if mpi is not None:
        command += ["--worker-resource", "nodes", str(mpi.nodes), "--worker-resource", "mpi_ranks", str(mpi.ranks)]
    # Never probe an allocation inside the sandbox: the profile fixes the capacity.
    return [*command, "--allocation", "none", "--idle"]


def main(argv: Sequence[str] | None = None) -> int:
    """Run the selected prelude and manager inside the existing payload sandbox.

    :param argv: Internal bootstrap arguments, or the process arguments.
    :return: A failure status if the payload does not match its enrollment.
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--handle", required=True)
    arguments = parser.parse_args(argv)
    if re.fullmatch(r"[0-9a-f]{32}", arguments.handle) is None:
        return 2
    policy = load_policy(Path("/daemon-policy.json"))
    profile = policy.profile(arguments.profile)
    if profile.mpi is not None:
        os.environ["HTTK_DAEMON_MPI_HANDLE"] = arguments.handle
        os.environ["HTTK_DAEMON_MPI_PROFILE"] = profile.name
    workspace = Workspace("/workspace")
    if workspace.workspace_id != policy.workspace_id:
        return 2
    # These redirections occur after containment, even if workspace paths are symlinks.
    log = workspace.control / f"daemon-manager-{arguments.handle}.log"
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC, 0o600)
    try:
        os.dup2(descriptor, 1)
        os.dup2(descriptor, 2)
    finally:
        if descriptor not in (1, 2):
            os.close(descriptor)
    os.chdir(workspace.root)
    script = "set -e\n" + profile.prelude + "\nexec " + shlex.join(_manager_command(policy, profile)) + "\n"
    os.execve("/bin/bash", ["/bin/bash", "--noprofile", "--norc", "-c", script], dict(os.environ))


if __name__ == "__main__":
    raise SystemExit(main())
