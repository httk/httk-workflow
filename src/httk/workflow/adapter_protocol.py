"""The public name of the remote-adapter protocol implementation.

A remote adapter is a versioned directory with one executable ``adapter``
program. Every operation -- ``configure``, ``install``, ``invoke``, ``push``,
``pull`` and ``status`` -- runs that one program, which reads
one JSON request file, learns which operation to perform from the request's
``operation`` member, and prints one JSON result. The ``install`` operation
keeps its historical protocol spelling but only ever *verifies* that the target
can run httk (the ``remote check`` CLI verb); adapters never install software.
The maintained ``local``, ``ssh`` and ``mount`` templates implement that protocol
by executing this module, which selects its behaviour from the ``kind`` recorded
in the bundle's ``remote.json`` and refuses any other kind rather than running it
in the wrong place. The ``mount`` kind moves files through a locally mounted view
of the remote filesystem and runs commands through a configurable executor.

This module is the documented surface of that contract. The implementation
lives in :mod:`httk.workflow.adapter_runtime`, which is what the packaged
``adapter`` dispatcher executes; both names refer to the same objects. See
the workflow CLI guide in the httk-workflow documentation for the operations and
the settings each one uses.

Every subprocess started by the implementation is an argument vector, so no
shell ever parses a value that came from a request or from settings. ``ssh`` and
the ``mount`` executor are the two unavoidable exceptions, because each joins its
command words for a shell on the far side; every remote command string is
therefore built by a single element-wise quoting helper.
"""

from .adapter_runtime import SUPPORTED_KINDS, main

__all__ = [
    "SUPPORTED_KINDS",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(main())
