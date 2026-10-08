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

    def can_query(self) -> bool:
        """Return whether the client :meth:`allocation_ended` runs is installed on this host.

        Without it the scheduler is not asked, and a recorded allocation end counts after the plain grace.
        """

        ...

    def allocation_ended(self, identity: Mapping[str, str], *, run: "Run", timeout: float) -> bool | None:
        """Report whether the allocation an identity names has ended, as far as the scheduler can confirm.

        This is the hook that lets a launch be taken over without an operator: a
        commit takeover, an attempt takeover or a cancellation waits until every
        recorded launch of the attempt is proven to have ended, and launches whose
        ranks run on other hosts than the manager's are proven ended only by this
        answer or by their allocation's recorded end time. Any launch technology
        that places ranks on other hosts must implement it.

        :param identity: The allocation's identity, as its probe recorded it.
        :param run: The command runner, called without a shell.
        :param timeout: Seconds the query may take.
        :return: ``True`` only when the scheduler confirms the allocation ended, which
            means every process it started has ended; ``False`` while it is still
            active; ``None`` when the scheduler cannot tell (an error, a timeout,
            output it cannot parse, or an identity it does not understand).
        """

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
