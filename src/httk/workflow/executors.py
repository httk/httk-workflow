"""Define runner-executor contracts used by the workflow manager."""

import os
import shlex
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from ._jobdir import JobDirectory
from .errors import FormatError
from .models import JobDefinition, Marker

if TYPE_CHECKING:
    from .workspace import Workspace

__all__ = [
    "AttemptLaunch",
    "OutcomeCommit",
    "PathRunnerExecutor",
    "RunnerExecutor",
]


@dataclass(frozen=True)
class AttemptLaunch:
    """Paths and context needed to construct an attempt command.

    ``runner`` is the executable the manager resolved for this attempt. A
    payload runner is the file inside ``payload``; a shared file runner is the
    verified ``/dev/fd/N`` alias and a shared tree runner is its verified entry
    at the original source path.

    :param job: Immutable job definition being attempted.
    :param marker: Authoritative marker for the attempt.
    :param payload: Immutable job payload directory.
    :param workdir: Directory in which the runner executes.
    :param control: Attempt-control directory.
    :param context: Runner context members.
    :param runner: Resolved executable, or no value to use the payload runner.
    :param workflow_prelude: Shell text run in a login shell before the runner, or empty for none.
    :param command: The expanded package command run in place of *runner*, when the job declares one.
    :param control_writer: Create a new file in the attempt-control directory and
        return its path. The manager supplies one that writes through its pinned,
        no-follow handle of the attempt-control directory; an executor writing
        attempt-control files should prefer it over a path write. When it is
        absent the executor anchors at *control* itself.
    """

    job: JobDefinition
    marker: Marker
    payload: Path
    workdir: Path
    control: Path
    context: Mapping[str, Any]
    runner: Path | None = None
    workflow_prelude: str = ""
    command: tuple[str, ...] | None = None
    control_writer: Callable[[str, bytes], Path] | None = None

    def write_control_file(self, name: str, data: bytes) -> Path:
        """Write one file into the attempt-control directory without following links.

        :param name: The file name, a single path component.
        :param data: The file content.
        :return: The path of the written file.
        :raises httk.workflow.errors.FormatError: If the name is invalid or the
            control directory holds a symlink or directory at it.
        :raises OSError: If the file cannot be written.
        """

        if self.control_writer is not None:
            return self.control_writer(name, data)
        with JobDirectory.at(self.control) as control:
            control.write_atomic(name, data, mode=0o666)
        return self.control / name

    @property
    def runner_command(self) -> Path:
        """Return the executable to run, defaulting to the payload runner."""

        if self.runner is not None:
            return self.runner
        return self.payload.joinpath(*self.job.runner_path.parts)


@dataclass(frozen=True)
class OutcomeCommit:
    """Describe a published outcome being committed by the authoritative manager.

    :param job: Immutable job definition producing the outcome.
    :param marker: Authoritative marker being committed.
    :param payload: Immutable job payload directory.
    :param outcome_path: Path of the published outcome.
    :param outcome: Outcome members.
    """

    job: JobDefinition
    marker: Marker
    payload: Path
    outcome_path: Path
    outcome: Mapping[str, Any]


class RunnerExecutor(Protocol):
    """Execution behavior selected by ``runner.executor`` in ``job.json``."""

    name: str

    def validate(self, job: JobDefinition, payload: Path) -> None:
        """Validate executor-specific immutable payload requirements."""

    def command(self, launch: AttemptLaunch) -> Sequence[str]:
        """Return the command argument vector for one attempt."""
        raise NotImplementedError

    def commit_outcome(self, commit: OutcomeCommit) -> None:
        """Complete executor-specific idempotent work before the marker advances."""

    def reconcile(self, workspace: "Workspace") -> None:
        """Repair executor-specific derived views; never alter authoritative state."""

    def marker_changed(self, workspace: "Workspace", marker: Marker) -> None:
        """Refresh derived views after an authoritative marker transition."""


class PathRunnerExecutor:
    """Execute the runner path directly from the payload or shared store."""

    name = "path"

    def validate(self, job: JobDefinition, payload: Path) -> None:
        """Validate the payload runner when the job runs one.

        :param job: Job definition whose runner is checked.
        :param payload: Immutable job payload directory.
        :return: ``None``.
        :raises httk.workflow.errors.FormatError: If a payload runner is missing,
            not a regular file, or reached through a symlink.
        """
        if job.runner_source != "payload":
            # A shared runner lives outside the immutable payload, so it is
            # resolved and digest-verified per attempt instead.
            return
        # The manager executes a payload runner through a no-follow descriptor,
        # so a runner reached through a symlink is refused here already.
        try:
            with JobDirectory.at(payload) as directory:
                information = directory.stat(job.runner_path)
        except (FormatError, OSError):
            information = None
        if information is None or not stat.S_ISREG(information.st_mode):
            raise FormatError(f"runner does not exist or is not a regular file: {job.runner_path}")

    def command(self, launch: AttemptLaunch) -> Sequence[str]:
        """Build the command for one path-runner attempt.

        When the launch carries a workflow prelude, the runner is wrapped in a
        login shell that runs the prelude (under ``set -e``) then execs the
        runner, so a failing prelude line aborts before the runner starts and
        the runner inherits the initialized environment. The login profiles may
        reset ``PATH``, so the manager's interpreter directory is put first again
        before the prelude, which keeps the last word.

        :param launch: Attempt paths and runner context.
        :return: Argument vector for the runner process.
        """
        program = list(launch.command) if launch.command is not None else [str(launch.runner_command)]
        base = [*program, *launch.job.runner_arguments]
        if not launch.workflow_prelude.strip():
            return base
        interpreter = shlex.quote(os.path.dirname(sys.executable))
        script_path = launch.write_control_file(
            "prelude.sh",
            (f'set -e\nexport PATH={interpreter}:"$PATH"\n' + launch.workflow_prelude + '\nexec "$@"\n').encode(
                "utf-8"
            ),
        )
        return ["bash", "-l", str(script_path), *base]

    def commit_outcome(self, commit: OutcomeCommit) -> None:
        """Complete path-executor outcome work before the marker advances.

        :param commit: Published outcome and its job context.
        :return: ``None``.
        """
        return

    def reconcile(self, workspace: "Workspace") -> None:
        """Reconcile path-executor derived state.

        :param workspace: Workspace whose derived state is reconciled.
        :return: ``None``.
        """
        return

    def marker_changed(self, workspace: "Workspace", marker: Marker) -> None:
        """Refresh path-executor state after a marker transition.

        :param workspace: Workspace containing the changed marker.
        :param marker: Newly authoritative marker.
        :return: ``None``.
        """
        return
