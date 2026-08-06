"""Dispatch harvested workflow records to owning-domain interpreters."""

from collections.abc import Callable
from dataclasses import dataclass

import httk.core

from .harvesting import HarvestRecord

__all__ = ["InterpretedRun", "register_interpreter", "registered_interpreters", "interpret"]  # noqa: RUF022


@dataclass(frozen=True)
class InterpretedRun:
    """A stored run together with the entries and products it interprets.

    ``entries`` are what ``SqlStore.save()`` and entry providers consume; the
    run's edges reference entries by their served ids.
    """

    run: httk.core.Run
    entries: tuple[object, ...] = ()
    products: tuple[httk.core.ProductLink, ...] = ()


_INTERPRETERS: dict[str, Callable[[HarvestRecord], InterpretedRun]] = {}


def register_interpreter(*, workflow: str, interpreter: Callable[[HarvestRecord], InterpretedRun]) -> None:
    """Register the interpreter for one workflow name."""

    if workflow in _INTERPRETERS:
        raise ValueError(f"an interpreter is already registered for workflow {workflow!r}")
    _INTERPRETERS[workflow] = interpreter


def registered_interpreters() -> tuple[str, ...]:
    """Return registered workflow names in registration order."""

    return tuple(_INTERPRETERS)


def interpret(record: HarvestRecord) -> InterpretedRun:
    """Interpret *record* with the interpreter registered for its workflow."""

    workflow = record.job.get("workflow")
    interpreter = _INTERPRETERS.get(workflow) if isinstance(workflow, str) else None
    if interpreter is None:
        known = ", ".join(_INTERPRETERS) or "none"
        raise ValueError(
            f"no interpreter is registered for workflow {workflow!r}; registered workflows: {known}. "
            "Import the owning workflow module first (for example, import httk.workflow.vasp)."
        )
    return interpreter(record)
