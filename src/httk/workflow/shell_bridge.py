"""The public name of the command bridge behind the native Bash API.

Every function of the packaged ``shell/httk-workflow.sh`` library is one
invocation of one subcommand of this bridge, and every subcommand publishes
through :class:`httk.workflow.Attempt` and
:class:`httk.workflow.runtime_builders.OutcomeDraft`. A Bash runner and a Python
runner therefore write the same bytes, because they write through exactly one
implementation.

This module is the documented surface of that contract. The implementation lives
in the private ``httk.workflow._shell_bridge``, which is what the shell library
still executes; both names refer to the same objects, and the private one keeps
working. See the native Bash API guide in the httk-workflow documentation for
the subcommands themselves.

Exit codes are uniform across every subcommand: ``0`` when the call succeeded,
``1`` when the answer is legitimately absent — an unset state key, a missing
input with no default, a null child field — and ``2`` when the call is refused,
which covers bad usage, a protocol violation, and a corrupt attempt context.
The supervised-command and VASP subcommands additionally report the classified
outcome of the program they ran.
"""

from ._shell_bridge import (
    ABSENT,
    REFUSED,
    RUNNER_STEPS_VARIABLE,
    RUNNER_WORKFLOW_VARIABLE,
    main,
)

__all__ = [
    "ABSENT",
    "REFUSED",
    "RUNNER_STEPS_VARIABLE",
    "RUNNER_WORKFLOW_VARIABLE",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(main())
