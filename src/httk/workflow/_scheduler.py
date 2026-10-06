"""Private scheduler contracts and the maintained scheduler lookup."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ._allocation import Allocation, Run
    from ._manager_binding import Placement


class Scheduler(Protocol):
    """The scheduler hooks needed by allocation probing and local launch."""

    name: str

    def detects(self, environ: Mapping[str, str]) -> bool:
        """Return whether *environ* belongs to this scheduler."""

        ...

    def counts(self, environ: Mapping[str, str]) -> dict[str, int]:
        """Return aggregate consumable capacities from *environ*."""

        ...

    def end_time(self, environ: Mapping[str, str]) -> float | None:
        """Return the allocation end epoch second, when available."""

        ...

    def probe_allocation(
        self, environ: Mapping[str, str], *, run: "Run", cpu_slots: bool = False
    ) -> "Allocation | None":
        """Probe nodes and metadata for the active allocation."""

        ...

    def step_argv(
        self,
        placement: "Placement",
        *,
        nodefile: str,
        gpus_present: bool = False,
        cpus_per_proc: int = 1,
        mem: int | None = None,
        mpi: str | None = None,
    ) -> list[str] | None:
        """Return the default parallel-step argv for one placement."""

        ...


def _maintained() -> tuple[Scheduler, ...]:
    from ._slurm import SLURM

    return (SLURM,)


def scheduler_for(name: str) -> Scheduler | None:
    """Return the maintained scheduler named *name*, if one exists."""

    return next((scheduler for scheduler in _maintained() if scheduler.name == name), None)


def detect_scheduler(environ: Mapping[str, str]) -> Scheduler | None:
    """Return the first maintained scheduler detected in *environ*."""

    return next((scheduler for scheduler in _maintained() if scheduler.detects(environ)), None)
