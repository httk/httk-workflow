"""Per-code helper APIs for native workflows, one subpackage per simulation code.

Each subpackage, such as :mod:`httk.workflow.codes.vasp`, collects everything
*httk-workflow* offers for that code: a dependency-free Python library, a
packaged Bash API file beside it, and the shell-bridge subcommands that Bash
file calls. A new code adds a sibling subpackage and registers its bridge
commands in ``httk.workflow._shell_bridge``.
"""
