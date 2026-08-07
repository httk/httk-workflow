"""Compatibility support for reading instantiated httk v1 task trees."""

import os
import sys
from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path

from httk.workflow.protocol import FormatError

__all__ = [
    "V1_PRIORITY_MAP",
    "V1FinishedTask",
    "bundled_v1_root",
    "code_of",
    "collect_finished_tree",
    "finished_tasks",
    "legacy_priority",
    "run_directory",
    "task_file",
]

V1_PRIORITY_MAP = {1: 100, 2: 300, 3: 500, 4: 700, 5: 900}


def bundled_v1_root() -> Path:
    """Return the packaged compatibility ``HTTK_DIR`` root.

    :return: The packaged v1 runtime root.
    """

    return Path(str(files("httk.workflow.compat.v1").joinpath("v1_runtime")))


def legacy_priority(value: int) -> int:
    """Map a legacy priority from 1 through 5 onto the v2 range.

    :param value: Map this legacy priority.
    :return: The corresponding v2 priority.
    :raises ValueError: If the priority is outside the legacy range.
    """

    try:
        return V1_PRIORITY_MAP[value]
    except KeyError as exc:
        raise ValueError("legacy priority must be 1 through 5") from exc


def _execute_instantiator(payload: Path, globals_: Mapping[str, object]) -> None:
    """Execute the trusted legacy instantiator in its payload directory."""

    script = payload / "ht.instantiate.py"
    if not script.is_file():
        raise FormatError("instantiate_globals were supplied but ht.instantiate.py is missing")
    namespace = dict(globals_)
    namespace.setdefault("__file__", str(script))
    namespace.setdefault("__name__", "__httk_v1_instantiate__")
    old_cwd = Path.cwd()
    old_argv = sys.argv
    try:
        os.chdir(payload)
        sys.argv = [str(script)]
        code = compile(script.read_bytes(), str(script), "exec")
        exec(code, namespace, namespace)  # noqa: S102 - v1 definitions are trusted input
    finally:
        sys.argv = old_argv
        os.chdir(old_cwd)
    script.unlink()


from .reader import (
    V1FinishedTask,
    code_of,
    collect_finished_tree,
    finished_tasks,
    run_directory,
    task_file,
)
