"""Compatibility with httk v1: run v1 task packages and read finished v1 task trees.

:mod:`httk.workflow.compat.v1.realization` realizes a v1 task package
declaring ``format = "httk-v1"`` through the ordinary runner path; it is
imported only by the format registry, not by this package. The reader
functions here collect finished v1 result trees.
"""

from importlib.resources import files
from pathlib import Path

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


from .reader import (
    V1FinishedTask,
    code_of,
    collect_finished_tree,
    finished_tasks,
    run_directory,
    task_file,
)
