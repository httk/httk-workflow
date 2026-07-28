"""The public name of the remote-adapter protocol implementation.

A remote adapter is a versioned directory with one executable ``adapter``
program. Every operation -- ``configure``, ``install``, ``invoke``, ``push``,
``pull``, ``start-manager`` and ``status`` -- runs that one program, which reads
one JSON request file, learns which operation to perform from the request's
``operation`` member, and prints one JSON result. The maintained ``local``,
``local-slurm`` and ``ssh-slurm`` templates implement that protocol by executing
this module, which selects its behaviour from the ``kind`` recorded in the
bundle's ``remote.json`` and refuses any other kind rather than running it in
the wrong place.

This module is the documented surface of that contract. The implementation
lives in :mod:`httk.workflow.adapter_runtime`, which is what the packaged
``adapter`` dispatcher executes; both names refer to the same objects. See
the workflow CLI guide in the httk-workflow documentation for the operations and
the settings each one uses.

Every subprocess started by the implementation is an argument vector, so no
shell ever parses a value that came from a request or from settings. ``ssh`` is
the one unavoidable exception, because it always joins its command words for a
login shell on the far side; every remote command string is therefore built by a
single element-wise quoting helper.
"""

from .adapter_runtime import (
    BATCH_DIRECTIVES,
    BATCH_DIRECTORY,
    SUPPORTED_KINDS,
    main,
)

__all__ = [
    "BATCH_DIRECTIVES",
    "BATCH_DIRECTORY",
    "SUPPORTED_KINDS",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    raise SystemExit(main())
