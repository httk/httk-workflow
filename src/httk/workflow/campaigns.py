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

Harvest and management simply cross the partition list: :func:`campaign_harvest`
chains :func:`~httk.workflow.harvesting.harvest` across every partition lazily,
and :func:`campaign_managers` starts a manager per selected partition — in this
process for a local partition, through the remote's scheduler for a remote one.
"""

import hashlib
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass

from .harvesting import DEFAULT_HARVEST_STATES, HarvestRecord, harvest
from .projects import read_project_section, write_project_section
from .registry import LOCAL_REMOTE, WorkspaceBinding, resolve_workspace
from .scaffold import JobItem, ScaffoldedJob, new_job, new_jobs
from .workspace import Workspace

__all__ = [
    "ASSIGNMENT_POLICIES",
    "CAMPAIGN_SECTION",
    "CampaignConfig",
    "assign_partition",
    "campaign_harvest",
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
    """One project's campaign map: its partitions and how roots are assigned."""

    partitions: Mapping[str, str]
    assignment: str

    def ordered_partitions(self) -> tuple[str, ...]:
        """Return the partition names in the stable order helpers iterate."""

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
    """Read and validate one project's campaign configuration."""

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
    """Store one project's campaign configuration and return it."""

    config = _validate(partitions, assignment)
    write_project_section(
        _require_root(project),
        CAMPAIGN_SECTION,
        {"partitions": dict(config.partitions), "assignment": config.assignment},
    )
    return config


def campaign_partitions(project: str | os.PathLike[str] | None = None) -> dict[str, str]:
    """Return the partition-name to workspace-name map of one project."""

    return dict(read_campaign(project).partitions)


def assign_partition(key: str, *, index: int = 0, project: str | os.PathLike[str] | None = None) -> str:
    """Return the partition one root job is assigned, per the project policy.

    ``explicit`` reads *key* as the partition name outright. ``hash`` maps *key*
    — a tag or job key — to a partition deterministically, so the same key always
    lands in the same partition. ``round-robin`` spreads by *index*, the position
    of the job in a submission batch, so a batch fans out evenly. The partition
    order is stable (sorted), so both content and position map reproducibly.
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
    """Return the registered workspace name one partition points at."""

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
    return Workspace(binding.path)


def campaign_submit(
    template: str,
    *,
    key: str,
    index: int = 0,
    project: str | os.PathLike[str] | None = None,
    **job: object,
) -> ScaffoldedJob:
    """Assign *key* to a partition and submit one root job into its workspace.

    The child jobs this root later spawns inherit its workspace automatically, so
    the whole subtree stays in the partition its root was assigned to.
    """

    partition = assign_partition(key, index=index, project=project)
    workspace = _local_partition_workspace(partition, project)
    return new_job(workspace, template, **job)  # type: ignore[arg-type]


def campaign_submit_many(
    template: str,
    items: Iterable[JobItem],
    *,
    key: str,
    index: int = 0,
    project: str | os.PathLike[str] | None = None,
    **shared: object,
) -> list[ScaffoldedJob]:
    """Submit a batch of root jobs into the partition *key* is assigned.

    The batch shares one partition; use one call per key to spread a campaign
    across partitions, or ``round-robin`` assignment with distinct indices.
    """

    partition = assign_partition(key, index=index, project=project)
    workspace = _local_partition_workspace(partition, project)
    return list(new_jobs(workspace, template, items, **shared))  # type: ignore[arg-type]


def _selected(config: CampaignConfig, partitions: Sequence[str] | None) -> tuple[str, ...]:
    """Return the partitions a cross-partition helper acts on, in stable order."""

    if partitions is None:
        return config.ordered_partitions()
    for name in partitions:
        if name not in config.partitions:
            raise ValueError(f"unknown campaign partition: {name}")
    return tuple(sorted(partitions))


def campaign_harvest(
    *,
    states: Iterable[str] = DEFAULT_HARVEST_STATES,
    placement: str | None = None,
    partitions: Sequence[str] | None = None,
    project: str | os.PathLike[str] | None = None,
) -> Iterator[HarvestRecord]:
    """Harvest the finished jobs of every partition, one workspace after another.

    The partitions are visited in stable order and each is harvested lazily, so a
    campaign spread over many workspaces streams as one harvest without ever
    materializing more than the record in hand. Remote partitions are fetched
    home first — a remote workspace is harvested where it runs — so this crosses
    only the local partitions and names any remote one it skips.
    """

    config = read_campaign(project)
    for partition in _selected(config, partitions):
        name = config.partitions[partition]
        binding = resolve_workspace(name, project=project)
        if binding.remote != LOCAL_REMOTE:
            raise ValueError(
                f"campaign partition {partition!r} is the remote workspace {name!r} on {binding.remote!r}; "
                "fetch it home with `httk workflow transfer` before harvesting the campaign"
            )
        yield from harvest(Workspace(binding.path, mutable=False), states=states, placement=placement)


def campaign_managers(
    *,
    partitions: Sequence[str] | None = None,
    workers: int | None = None,
    count: int = 1,
    poll_interval: float = 1.0,
    idle_timeout: float = 3600.0,
    adapter_timeout: float | None = None,
    project: str | os.PathLike[str] | None = None,
) -> list[dict[str, object]]:
    """Start a manager per selected partition and report what each did.

    A local partition is served in this process until it runs out of claimable
    work; a remote partition's managers are submitted through its scheduler over
    the adapter, exactly as ``httk workflow manager run`` does for one workspace.
    The report has one row per partition, so a caller sees where work ran.
    """

    from .adapters import resolve_remote, run_adapter
    from .manager import TaskManager

    # The frozen remote-manager spelling, duplicated from the CLI's REMOTE_MANAGER_COMMAND
    # so this module never imports the CLI it is invoked from.
    remote_manager_command = ("httk", "workflow", "manager", "run")
    config = read_campaign(project)
    report: list[dict[str, object]] = []
    for partition in _selected(config, partitions):
        name = config.partitions[partition]
        binding: WorkspaceBinding = resolve_workspace(name, project=project)
        if binding.remote == LOCAL_REMOTE:
            with TaskManager(Workspace(binding.path), maximum_workers=workers or 1) as manager:
                manager.run_until_idle(timeout=idle_timeout, poll_interval=poll_interval)
            report.append({"partition": partition, "workspace": name, "mode": "local", "ran": True})
        else:
            target = resolve_remote(binding.remote, project=project)
            manager_argv = [*remote_manager_command, binding.path, "--by-path"]
            if workers is not None:
                manager_argv += ["--workers", str(workers)]
            result = run_adapter(
                target.bundle,
                "start-manager",
                {"remote_settings": {}, "argv": manager_argv, "workspace": binding.path, "count": count},
                timeout=adapter_timeout,
            )
            report.append({"partition": partition, "workspace": name, "mode": "remote", "result": result})
    return report


def _require_root(project: str | os.PathLike[str] | None) -> str:
    """Return the project root, refusing when there is none."""

    from .projects import require_project

    return str(require_project(project))
