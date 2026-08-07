"""Resolve the runners this distribution ships and how jobs name them.

A packaged runner lives with the science it implements: the four VASP runners are
modules of :mod:`httk.workflow.vasp.runners`, and that is also the module the
reserved ``pkg:`` form of a ``job.json`` names them in. What stays here is the
generic half — which runner files are packaged, where each one is installed, and
the ``runner`` member that references one — so a further domain ships a
subpackage of its own and one row of :data:`RUNNERS` rather than a second copy of
this machinery.

A packaged runner is usable in three ways without being copied into a payload: as
an installed runner through the reserved ``pkg:`` form, as a workspace runner
published by digest, or as a file to copy and edit. :func:`runner_reference`
builds the ``runner`` member of a ``job.json`` for the first two.
"""

import importlib
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from .._util import sha256_file

#: The subpackage the four packaged VASP runners are modules of.
VASP_RUNNER_PACKAGE = "httk.workflow.vasp.runners"

#: Every runner file this distribution packages, mapped to the module whose
#: reserved ``pkg:`` form names it. The owning modules are named rather than
#: imported, so resolving one runner file never drags another domain's helpers
#: into a process that only asked where a file is.
RUNNERS: Mapping[str, str] = MappingProxyType(
    {
        "vasp_relax.py": VASP_RUNNER_PACKAGE,
        "vasp_relax.sh": VASP_RUNNER_PACKAGE,
        "vasp_relax_static.py": VASP_RUNNER_PACKAGE,
        "vasp_static.py": VASP_RUNNER_PACKAGE,
    }
)


def runner_package(name: str) -> str:
    """Return the module the reserved ``pkg:`` form names one packaged runner in.

    :param name: Packaged runner filename.
    :return: Module containing the runner.
    :raises ValueError: If no packaged runner has that name.
    """

    package = RUNNERS.get(name)
    if package is None:
        raise ValueError(f"unknown packaged runner {name!r}; packaged runners: {', '.join(RUNNERS)}")
    return package


def runner_path(name: str) -> Path:
    """Return the installed file of one packaged runner.

    :param name: Packaged runner filename.
    :return: Installed runner path.
    :raises ValueError: If the runner is unknown or has no installed location.
    """

    module = importlib.import_module(runner_package(name))
    location = getattr(module, "__file__", None)
    if location is None:  # pragma: no cover - only a namespace package has none
        raise ValueError(f"runner module {runner_package(name)} has no installed location")
    return Path(location).with_name(name)


def runner_reference(name: str, *, source: str = "installed") -> dict[str, object]:
    """Return the ``runner`` member of a ``job.json`` running one packaged runner.

    The digest is taken from the installed bytes, which is what both an installed
    and a freshly published workspace reference must pin. Use ``source``
    ``workspace`` after publishing the same file with
    :meth:`httk.workflow.Workspace.publish_runner`.

    :param name: Packaged runner filename.
    :param source: Whether the reference targets the installed or workspace runner.
    :return: Runner configuration for a job document.
    :raises ValueError: If the runner or source is unsupported.
    """

    path = runner_path(name)
    if source == "installed":
        location = f"pkg:{runner_package(name)}/{PurePosixPath(name)}"
    elif source == "workspace":
        location = name
    else:
        raise ValueError("a packaged runner is referenced as an installed or a workspace runner")
    return {
        "executor": "path",
        "source": source,
        "path": location,
        "sha256": sha256_file(path),
        "arguments": [],
    }
