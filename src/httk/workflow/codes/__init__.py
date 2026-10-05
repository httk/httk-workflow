"""Build support packages for simulation codes on the httk-workflow runtime.

*httk-workflow* bundles no support for any particular simulation code. Each code
is supported by its own distribution, conventionally ``httk-workflow-<code>``
(for example *httk-workflow-vasp*), which provides:

* the code library ``httk.codes.<code>`` in the ``httk.codes`` PEP 420
  namespace: the Python helpers a runner calls, a Bash API file such as
  ``httk-<code>.sh`` beside them, and a bridge module;
* the registry package ``httk.registry.codes.<code>``, which calls
  :func:`httk.core.register.register_code` so the code is discovered at
  ``import httk.core``.

The **bridge contract**: the registered bridge module defines
``add_commands(subparsers)``, which adds subcommands named ``<code>-<verb>`` to
the argparse subparsers of the shell bridge (:mod:`httk.workflow.shell_bridge`),
and ``run_command(namespace) -> int``, which runs one of them. It returns ``0``
on success and :data:`BRIDGE_ABSENT` for a legitimately absent answer; an
exception it raises is reported as a refusal (exit code ``2``).

The **Bash API contract**: the registered Bash API file is exported to every
attempt, and to describe mode, as ``HTTK_WORKFLOW_<CODE>_BASH_API`` (for
example ``HTTK_WORKFLOW_VASP_BASH_API``); a Bash runner sources it after the
generic ``HTTK_WORKFLOW_BASH_API``, and each function it defines is one
invocation of one ``<code>-<verb>`` bridge subcommand through the generic
library's ``_httk_workflow_bridge``.

This module is the toolkit such a package builds on: the supervision, replay,
state-directory, and JSON helpers of the runtime, promoted to one public home,
plus the registry view of the installed codes.
"""

import os
import shlex
from collections.abc import Sequence

from httk.core.register import CodeSupport, code_support, known_codes

from .._util import read_json, utc_now, write_json_atomic
from ..errors import RunnerResolutionError
from ..models import JOB_STATE_DIRECTORY
from ..runtime_builders import ReplayableWorkdirBatch
from ..supervision import (
    CheckerSpec,
    Diagnostic,
    FollowSource,
    ProcessReport,
    ProcessSupervisor,
    SourceEvent,
)

__all__ = [
    "BRIDGE_ABSENT",
    "JOB_STATE_DIRECTORY",
    "CheckerSpec",
    "CodeSupport",
    "Diagnostic",
    "FollowSource",
    "ProcessReport",
    "ProcessSupervisor",
    "ReplayableWorkdirBatch",
    "SourceEvent",
    "code_environment",
    "installed_codes",
    "launch_command",
    "read_json",
    "utc_now",
    "write_json_atomic",
]

#: The shell bridge's uniform "legitimately absent" exit code.
BRIDGE_ABSENT: int = 1


#: Parallel-start programs a code command must not name itself.
_LAUNCHERS = frozenset({"srun", "mpirun", "mpiexec", "mpiexec.hydra", "orterun", "mpprun", "aprun", "jsrun", "ibrun"})


def launch_command(argv: Sequence[str], *, launch: bool = True) -> list[str]:
    """Return ``argv`` with the attempt's launch prefix prepended.

    The prefix is the shell-quoted argv in ``HTTK_WORKFLOW_LAUNCH``, set by the
    workflow manager; it is absent or blank when the attempt has none.

    :param argv: The code command, naming only the program (for example ``["vasp_std"]``).
    :param launch: Whether to apply the launch prefix; ``False`` returns ``argv`` as given.
    :return: The command to run.
    :raises ValueError: If ``argv`` is empty, or starts with a parallel launcher
        while a launch prefix would be prepended.
    """

    if not argv:
        raise ValueError("empty command")
    prefix = os.environ.get("HTTK_WORKFLOW_LAUNCH", "").strip() if launch else ""
    if not prefix:
        return list(argv)
    if os.path.basename(argv[0]) in _LAUNCHERS:
        raise ValueError(
            f"the command starts with the launcher {argv[0]!r}, but this attempt already has a launch prefix; "
            "set the command to the bare program (for example 'vasp_std') and configure the parallel start "
            "in the manager.launch_template setting"
        )
    return [*shlex.split(prefix), *argv]


def installed_codes() -> tuple[CodeSupport, ...]:
    """Return every registered code-support package, sorted by name.

    :return: The registered code-support metadata.
    """

    return tuple(code_support(name) for name in known_codes())


def code_environment() -> dict[str, str]:
    """Return the ``HTTK_WORKFLOW_<CODE>_BASH_API`` variables of the installed codes.

    :return: One variable per installed code that registers a Bash API.
    :raises httk.workflow.errors.RunnerResolutionError: With code ``code_support_unavailable`` if an
        installed code's Bash API resource cannot be found.
    """

    environment: dict[str, str] = {}
    for code in installed_codes():
        try:
            path = code.bash_api_path()
        except (ImportError, OSError) as exc:
            raise RunnerResolutionError(
                "code_support_unavailable",
                f"code {code.name!r} Bash API {code.bash_api!r} is unavailable: {exc}",
            ) from exc
        if path is not None:
            environment[f"HTTK_WORKFLOW_{code.name.upper()}_BASH_API"] = str(path)
    return environment
