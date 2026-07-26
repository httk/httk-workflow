"""Runner-backend contracts used by the workflow manager."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from .errors import FormatError
from .models import JobDefinition, Marker

if TYPE_CHECKING:
    from .workspace import WorkflowWorkspace


@dataclass(frozen=True)
class AttemptLaunch:
    """Paths and context needed to construct an attempt command.

    ``runner`` is the executable the manager resolved for this attempt. A
    payload runner is the file inside ``payload``; a shared runner is the
    verified copy the manager staged below ``control``, never the original.
    """

    job: JobDefinition
    marker: Marker
    payload: Path
    workdir: Path
    control: Path
    context_path: Path
    context: Mapping[str, Any]
    runner: Path | None = None

    @property
    def runner_command(self) -> Path:
        """Return the executable to run, defaulting to the payload runner."""

        if self.runner is not None:
            return self.runner
        return self.payload.joinpath(*self.job.runner_path.parts)


@dataclass(frozen=True)
class OutcomeCommit:
    """A published outcome being committed by the authoritative manager."""

    job: JobDefinition
    marker: Marker
    payload: Path
    outcome_path: Path
    outcome: Mapping[str, Any]


class RunnerBackend(Protocol):
    """Execution behavior selected by ``runner.backend`` in ``job.json``."""

    name: str

    def validate(self, job: JobDefinition, payload: Path) -> None:
        """Validate backend-specific immutable payload requirements."""

    def command(self, launch: AttemptLaunch) -> Sequence[str]:
        """Return the command argument vector for one attempt."""
        raise NotImplementedError

    def commit_outcome(self, commit: OutcomeCommit) -> None:
        """Complete backend-specific idempotent work before the marker advances."""

    def reconcile(self, workspace: "WorkflowWorkspace") -> None:
        """Repair backend-specific derived views; never alter authoritative state."""

    def marker_changed(self, workspace: "WorkflowWorkspace", marker: Marker) -> None:
        """Refresh derived views after an authoritative marker transition."""


class PathRunnerBackend:
    """The normal backend which directly executes ``runner.path``."""

    name = "path"

    def validate(self, job: JobDefinition, payload: Path) -> None:
        if job.runner_source != "payload":
            # A shared runner lives outside the immutable payload, so it is
            # resolved, staged, and digest-verified per attempt instead.
            return
        runner = payload.joinpath(*job.runner_path.parts)
        if not runner.is_file():
            raise FormatError(f"runner does not exist or is not a regular file: {job.runner_path}")

    def command(self, launch: AttemptLaunch) -> Sequence[str]:
        return [str(launch.runner_command), *launch.job.runner_arguments]

    def commit_outcome(self, commit: OutcomeCommit) -> None:
        return

    def reconcile(self, workspace: "WorkflowWorkspace") -> None:
        return

    def marker_changed(self, workspace: "WorkflowWorkspace", marker: Marker) -> None:
        return
