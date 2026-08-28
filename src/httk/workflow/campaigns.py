"""Campaigns: a thin partition map over many registered workspaces.

A *campaign* is how one project spreads a very large body of work across many
workspaces without a new scheduler, a new graph, or any engine-level sharding.
It is a convention plus a few helpers, stored in the project as a ``campaign``
member of ``project.json``:

* **partitions** — a map of partition name to the registered workspace that
  partition's jobs live in. A partition is just a named bucket; the workspace it
  points at is the same kind of workspace every other command drives.
* **assignment** — how a root job's partition is chosen: ``round-robin`` spreads
  by submission order, ``hash`` spreads deterministically by a tag or job key,
  and ``explicit`` lets the caller name the partition outright.

Two rules make this safe at scale without new machinery:

* **Root jobs are assigned per policy.** :func:`assign_partition` picks a
  partition; :func:`campaign_submit` submits the root there.
* **Dynamically spawned children always inherit their parent's workspace.** That
  is already true of the execution engine — a child is scaffolded into the same
  workspace its parent runs in — so a campaign never has to re-route a subtree,
  and this module deliberately changes nothing about spawning.

Collect and management simply cross the partition list: :func:`campaign_collect`
chains :func:`~httk.workflow.collecting.job_records` across every partition lazily,
and :func:`campaign_managers` starts a manager per selected partition — through
the workspace launcher locally, or by invoking the command on a remote owner.
"""

import argparse
import hashlib
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from httk.core.cli import CLIContext

from .collecting import DEFAULT_COLLECT_STATES, JobRecord, job_records
from .errors import FormatError
from .models import validate_resources
from .projects import read_project_section, write_project_section
from .registry import LOCAL_REMOTE, WorkspaceBinding, resolve_workspace
from .scaffold import JobItem, ScaffoldedJob, new_job, new_jobs
from .workspace import Workspace

__all__ = [
    "ASSIGNMENT_POLICIES",
    "CAMPAIGN_SECTION",
    "CampaignConfig",
    "assign_partition",
    "campaign_collect",
    "campaign_managers",
    "campaign_partitions",
    "campaign_submit",
    "campaign_submit_many",
    "partition_workspace",
    "read_campaign",
    "write_campaign",
]

#: The ``project.json`` member the campaign map lives in.
CAMPAIGN_SECTION = "campaign"

#: The partition-assignment policies a campaign may use.
ASSIGNMENT_POLICIES = ("round-robin", "hash", "explicit")


@dataclass(frozen=True)
class CampaignConfig:
    """Store one project's partitions and root-assignment policy.

    :param partitions: Map partition names to registered workspace names.
    :param assignment: Select the root-job assignment policy.
    """

    partitions: Mapping[str, str]
    assignment: str

    def ordered_partitions(self) -> tuple[str, ...]:
        """Return partition names in the stable helper iteration order.

        :return: The sorted partition names.
        """

        return tuple(sorted(self.partitions))


def _validate(partitions: object, assignment: object) -> CampaignConfig:
    """Return a validated campaign configuration, or raise on a bad one."""

    if not isinstance(partitions, Mapping):
        raise ValueError("campaign partitions must be a JSON object")
    validated: dict[str, str] = {}
    for name, workspace in partitions.items():
        if not isinstance(name, str) or not name:
            raise ValueError("a campaign partition name must be a nonempty string")
        if not isinstance(workspace, str) or not workspace:
            raise ValueError(f"campaign partition {name!r} must name a registered workspace")
        validated[name] = workspace
    if assignment not in ASSIGNMENT_POLICIES:
        raise ValueError(f"campaign assignment must be one of {', '.join(ASSIGNMENT_POLICIES)}, not {assignment!r}")
    return CampaignConfig(partitions=validated, assignment=str(assignment))


def read_campaign(project: str | os.PathLike[str] | None = None) -> CampaignConfig:
    """Read and validate one project's campaign configuration.

    :param project: Locate the project whose campaign to read.
    :return: The validated campaign configuration.
    :raises ValueError: If the stored campaign is malformed.
    """

    section = read_project_section(_require_root(project), CAMPAIGN_SECTION)
    if not section:
        return CampaignConfig(partitions={}, assignment="hash")
    return _validate(section.get("partitions", {}), section.get("assignment", "hash"))


def write_campaign(
    partitions: Mapping[str, str],
    *,
    assignment: str = "hash",
    project: str | os.PathLike[str] | None = None,
) -> CampaignConfig:
    """Store one project's campaign configuration and return it.

    :param partitions: Map partition names to registered workspace names.
    :param assignment: Select the root-job assignment policy.
    :param project: Locate the project whose campaign to write.
    :return: The validated configuration that was stored.
    :raises ValueError: If the configuration is malformed.
    """

    config = _validate(partitions, assignment)
    write_project_section(
        _require_root(project),
        CAMPAIGN_SECTION,
        {"partitions": dict(config.partitions), "assignment": config.assignment},
    )
    return config


def campaign_partitions(project: str | os.PathLike[str] | None = None) -> dict[str, str]:
    """Return one project's partition-to-workspace map.

    :param project: Locate the project whose campaign to read.
    :return: A copy of the partition map.
    """

    return dict(read_campaign(project).partitions)


def assign_partition(key: str, *, index: int = 0, project: str | os.PathLike[str] | None = None) -> str:
    """Return the partition one root job is assigned, per the project policy.

    ``explicit`` reads *key* as the partition name outright. ``hash`` maps *key*
    — a tag or job key — to a partition deterministically, so the same key always
    lands in the same partition. ``round-robin`` spreads by *index*, the position
    of the job in a submission batch, so a batch fans out evenly. The partition
    order is stable (sorted), so both content and position map reproducibly.

    :param key: Supply the tag, job key, or explicit partition name.
    :param index: Supply the batch position for round-robin assignment.
    :param project: Locate the project whose campaign to read.
    :return: The selected partition name.
    :raises ValueError: If no partitions exist or an explicit partition is
        unknown.
    """

    config = read_campaign(project)
    names = config.ordered_partitions()
    if not names:
        raise ValueError("this project defines no campaign partitions")
    if config.assignment == "explicit":
        if key not in config.partitions:
            raise ValueError(f"unknown campaign partition: {key}; defined: {', '.join(names)}")
        return key
    if config.assignment == "hash":
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return names[int.from_bytes(digest[:8], "big") % len(names)]
    return names[index % len(names)]


def partition_workspace(partition: str, *, project: str | os.PathLike[str] | None = None) -> str:
    """Return the registered workspace name one partition points at.

    :param partition: Identify the campaign partition.
    :param project: Locate the project whose campaign to read.
    :return: The registered workspace name.
    :raises ValueError: If the partition is unknown.
    """

    partitions = read_campaign(project).partitions
    if partition not in partitions:
        raise ValueError(f"unknown campaign partition: {partition}")
    return partitions[partition]


def _local_partition_workspace(partition: str, project: str | os.PathLike[str] | None) -> Workspace:
    """Return the local workspace of one partition, refusing a remote one."""

    name = partition_workspace(partition, project=project)
    binding = resolve_workspace(name, project=project)
    if binding.remote != LOCAL_REMOTE:
        raise ValueError(
            f"campaign partition {partition!r} is the remote workspace {name!r} on {binding.remote!r}; "
            "submit locally and move it with `httk workflow transfer`, or run this on the remote"
        )
    assert binding.path is not None
    return Workspace(binding.path)


def campaign_submit(
    workflow: str,
    *,
    key: str,
    index: int = 0,
    project: str | os.PathLike[str] | None = None,
    **job: object,
) -> ScaffoldedJob:
    r"""Assign *key* to a partition and submit one root job into its workspace.

    The child jobs this root later spawns inherit its workspace automatically, so
    the whole subtree stays in the partition its root was assigned to.

    :param workflow: Identify the workflow to submit.
    :param key: Supply the assignment key.
    :param index: Supply the batch position for round-robin assignment.
    :param project: Locate the project whose campaign to use.
    :param \*\*job: Supply the root job fields.
    :return: The scaffolded root job.
    :raises ValueError: If the campaign partition is remote or submission fails.
    """

    partition = assign_partition(key, index=index, project=project)
    workspace = _local_partition_workspace(partition, project)
    return new_job(workspace, workflow, **job)  # type: ignore[arg-type]


def campaign_submit_many(
    workflow: str,
    items: Iterable[JobItem],
    *,
    key: str,
    index: int = 0,
    project: str | os.PathLike[str] | None = None,
    **shared: object,
) -> list[ScaffoldedJob]:
    r"""Submit a batch of root jobs into the partition *key* is assigned.

    The batch shares one partition; use one call per key to spread a campaign
    across partitions, or ``round-robin`` assignment with distinct indices.

    :param workflow: Identify the workflow to submit.
    :param items: Supply the job items to submit.
    :param key: Supply the assignment key.
    :param index: Supply the batch position for round-robin assignment.
    :param project: Locate the project whose campaign to use.
    :param \*\*shared: Supply fields shared by every submitted job.
    :return: The scaffolded jobs in input order.
    :raises ValueError: If the campaign partition is remote or submission fails.
    """

    partition = assign_partition(key, index=index, project=project)
    workspace = _local_partition_workspace(partition, project)
    return list(new_jobs(workspace, workflow, items, **shared))  # type: ignore[arg-type]


def _selected(config: CampaignConfig, partitions: Sequence[str] | None) -> tuple[str, ...]:
    """Return the partitions a cross-partition helper acts on, in stable order."""

    if partitions is None:
        return config.ordered_partitions()
    for name in partitions:
        if name not in config.partitions:
            raise ValueError(f"unknown campaign partition: {name}")
    return tuple(sorted(partitions))


def campaign_collect(
    *,
    states: Iterable[str] = DEFAULT_COLLECT_STATES,
    placement: str | None = None,
    partitions: Sequence[str] | None = None,
    project: str | os.PathLike[str] | None = None,
) -> Iterator[JobRecord]:
    """Collect the finished jobs of every partition, one workspace after another.

    The partitions are visited in stable order and each is collected lazily, so a
    campaign spread over many workspaces streams as one job_records without ever
    materializing more than the record in hand. Remote partitions are refused
    and must be fetched home beforehand; collection crosses only local
    partitions.

    :param states: Select the stopped state kinds to collect.
    :param placement: Restrict collection to this placement and descendants.
    :param partitions: Select partitions, or use all partitions.
    :param project: Locate the project whose campaign to collect.
    :yields: Job records from the selected local partitions.
    :raises ValueError: If a selected partition is remote or unknown.
    """

    config = read_campaign(project)
    for partition in _selected(config, partitions):
        name = config.partitions[partition]
        binding = resolve_workspace(name, project=project)
        if binding.remote != LOCAL_REMOTE:
            raise ValueError(
                f"campaign partition {partition!r} is the remote workspace {name!r} on {binding.remote!r}; "
                "fetch it home with `httk workflow transfer` before collecting the campaign"
            )
        assert binding.path is not None
        yield from job_records(Workspace(binding.path, mutable=False), states=states, placement=placement)


def campaign_managers(
    *,
    partitions: Sequence[str] | None = None,
    workers: int | None = None,
    resources: Mapping[str, int] | None = None,
    count: int | None = None,
    launcher: str | None = None,
    poll_interval: float = 1.0,
    idle_timeout: float = 3600.0,
    adapter_timeout: float | None = None,
    project: str | os.PathLike[str] | None = None,
) -> list[dict[str, object]]:
    """Start a manager per selected partition and report what each did.

    A local partition follows its workspace launcher; a remote partition invokes
    ``httk workflow manager run`` on the owning machine over its adapter.
    The report has one row per partition, so a caller sees where work ran.

    :param partitions: Select partitions, or use all partitions.
    :param workers: Limit concurrent workers for local managers.
    :param resources: Resource capacities advertised by each manager.
    :param count: Explicit number of managers to start at each launch site; when
        omitted, each workspace's ``manager.count`` setting applies.
    :param launcher: Use this launcher for every selected partition.
    :param poll_interval: Set the local manager polling interval.
    :param idle_timeout: Set the local manager idle timeout.
    :param adapter_timeout: Set the remote adapter timeout.
    :param project: Locate the project whose campaign to manage.
    :return: One management report row per selected partition.
    :raises ValueError: If a selected partition cannot be managed.
    """

    from .adapters import resolve_remote, run_adapter

    try:
        manager_resources = validate_resources({} if resources is None else resources, "manager.resources")
    except FormatError as exc:
        raise ValueError(str(exc)) from exc
    config = read_campaign(project)
    report: list[dict[str, object]] = []
    for partition in _selected(config, partitions):
        name = config.partitions[partition]
        binding: WorkspaceBinding = resolve_workspace(name, project=project)
        if binding.remote == LOCAL_REMOTE:
            assert binding.path is not None
            from .workflow_cli._manager import launch_workspace_managers

            options = argparse.Namespace(
                pool=[],
                capability=[],
                placement_prefix=[],
                workers=workers,
                worker_resource=[[resource, str(capacity)] for resource, capacity in manager_resources.items()],
                count=count,
                launcher=launcher,
                lease_seconds=None,
                heartbeat_interval=30.0,
                poll_interval=poll_interval,
                join_grace_seconds=3600.0,
                idle=False,
                idle_timeout=idle_timeout,
                unsafe_persistent_takeover=False,
                unsafe_isolated_takeover=False,
                takeover_grace_factor=2.0,
                runner_search_path=[],
                drain_timeout=30.0,
                gc_interval=None,
                log_level=None,
                log_file=None,
                json_logs=False,
                adapter_timeout=adapter_timeout,
                inline=False,
                detach=False,
                no_durable=False,
            )
            mode, result = launch_workspace_managers(
                Path(binding.path), options, CLIContext("httk", Path(project) if project is not None else Path.cwd())
            )
            report.append({"partition": partition, "workspace": name, "mode": mode, "result": result})
        else:
            target = resolve_remote(binding.remote, project=project)
            remote_name = binding.name.split(":", 1)[1]
            options = argparse.Namespace(
                pool=[],
                capability=[],
                placement_prefix=[],
                workers=workers,
                worker_resource=[[resource, str(capacity)] for resource, capacity in manager_resources.items()],
                count=count,
                launcher=launcher,
                lease_seconds=None,
                heartbeat_interval=30.0,
                poll_interval=poll_interval,
                join_grace_seconds=3600.0,
                idle=False,
                idle_timeout=idle_timeout,
                unsafe_persistent_takeover=False,
                unsafe_isolated_takeover=False,
                takeover_grace_factor=2.0,
                runner_search_path=[],
                drain_timeout=30.0,
                gc_interval=None,
                log_level=None,
                log_file=None,
                json_logs=False,
                no_durable=False,
            )
            from .workflow_cli._manager import _remote_manager_argv

            result = run_adapter(
                target.bundle,
                "invoke",
                {"argv": _remote_manager_argv(options, remote_name)},
                timeout=adapter_timeout,
            )
            report.append(
                {
                    "partition": partition,
                    "workspace": name,
                    "mode": "remote",
                    "returncode": int(result.get("returncode", 1) or 0),
                    "stdout": str(result.get("stdout", "")),
                    "stderr": str(result.get("stderr", "")),
                }
            )
    return report


def _require_root(project: str | os.PathLike[str] | None) -> str:
    """Return the project root, refusing when there is none."""

    from .projects import require_project

    return str(require_project(project))
