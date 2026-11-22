"""Campaign command group, and the ``--campaign`` target of ``job new``, ``collect`` and ``run``."""

import argparse
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from httk.core.cli import CLIContext

from .. import scaffold
from ..campaigns import (
    _COLLECT_HINT,
    ASSIGNMENT_POLICIES,
    _local_partition_workspace,
    _selected,
    assign_partition,
    campaign_managers,
    read_campaign,
    remove_campaign,
    write_campaign,
)
from ..scaffold import JobItem
from ..seals import require_cli_modifiable
from ..workspace import Workspace
from ._common import (
    _durable,
    _group,
    _leaf,
    _pairs,
    _setting_pair,
    confirm,
)

# ---------------------------------------------------------------------------
# campaign
# ---------------------------------------------------------------------------


def handle_campaign_init(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Define this project's campaign partition map and assignment policy."""

    partitions = dict(_pairs(arguments.partition, "a partition"))
    config = write_campaign(partitions, assignment=arguments.assignment, project=context.cwd)
    print(
        json.dumps(
            {"partitions": dict(config.partitions), "assignment": config.assignment},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def handle_campaign_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Show this project's campaign partition map."""

    config = read_campaign(context.cwd)
    document = {"partitions": dict(config.partitions), "assignment": config.assignment}
    if arguments.json:
        print(json.dumps(document, indent=2, sort_keys=True))
        return 0
    print(f"assignment\t{config.assignment}")
    for partition in config.ordered_partitions():
        print(f"{partition}\t{config.partitions[partition]}")
    return 0


def _configure_campaign(changes: list[tuple[str, str]], arguments: argparse.Namespace, context: CLIContext) -> int:
    """Validate all requested campaign changes before saving them together."""

    config = read_campaign(context.cwd)
    partitions = dict(config.partitions)
    assignment = config.assignment
    for operation, item in changes:
        key, separator, value = item.partition("=")
        if operation == "unset" and separator:
            raise ValueError(f"unset expects KEY, not KEY=VALUE: {item!r}")
        if key == "assignment":
            assignment = value if operation == "set" else "hash"
        elif key.startswith("partitions.") and key.removeprefix("partitions."):
            name = key.removeprefix("partitions.")
            if operation == "set":
                partitions[name] = value
            elif name in partitions:
                del partitions[name]
            else:
                raise ValueError(f"campaign partition is not set: {name}")
        else:
            raise ValueError(f"unknown campaign setting {key!r}; use assignment or partitions.NAME")
    write_campaign(partitions, assignment=assignment, project=context.cwd)
    arguments.json = True
    return handle_campaign_show(arguments, context)


def handle_campaign_set(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Set campaign assignment and partitions, saving only after every change is valid."""

    return _configure_campaign([("set", item) for item in arguments.pairs], arguments, context)


def handle_campaign_unset(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove campaign partitions or reset the assignment, saving only after every change is valid."""

    return _configure_campaign([("unset", key) for key in arguments.keys], arguments, context)


def handle_campaign_remove(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Remove this project's campaign map while preserving its jobs and workspaces."""

    if not confirm("remove the campaign configuration?", yes=arguments.yes):
        return 1
    remove_campaign(context.cwd)
    return 0


def campaign_shares(
    arguments: argparse.Namespace, context: CLIContext
) -> Callable[[Sequence[JobItem]], list[tuple[Workspace, list[JobItem]]]]:
    """Check ``job new --campaign`` and return what splits its jobs over the partitions they are assigned.

    ``--partition`` names the partition outright (required by ``explicit`` assignment); otherwise ``hash`` hashes
    each job's tag as given (``--tag`` or an input's, never one an instantiate hook derives later) and
    ``round-robin`` numbers the jobs from ``--campaign-index``. The split keeps each partition's jobs in order, the
    partitions in the order their first job came. Before anything is submitted it refuses, in every partition, a
    workflow that is not installed (without ``--install`` or ``--embed-workflow``) and, with ``--duplicate-tag
    refuse``, a tag already taken in the placement or given twice to one partition's jobs, so such a refusal leaves
    no partition partly submitted.
    """

    if arguments.workspace is not None:
        raise ValueError("--campaign submits into the partitions' workspaces; it cannot be combined with --workspace")
    source: str = arguments.source or ""
    path = Path(source).expanduser()
    if not source or path.is_file() or ("/" in source and not source.startswith("git+") and not path.is_dir()):
        raise ValueError(
            "--campaign takes a workflow name, git URI or package directory, not a runner file, document or "
            "--command; install it into the partitions' workspaces (httk workflow install -w WS FILE) and give "
            "its name, or create the job without --campaign"
        )
    project = context.cwd
    config = read_campaign(project)
    if arguments.partition is None and config.assignment == "explicit":
        raise ValueError("this campaign assigns partitions explicitly; name one with --partition NAME")
    if arguments.campaign_index is not None and (arguments.partition is not None or config.assignment != "round-robin"):
        raise ValueError("--campaign-index applies only to round-robin assignment, without --partition")
    durable = _durable(arguments)
    opened: dict[str, Workspace] = {}

    def workspace(partition: str) -> Workspace:
        if partition not in opened:
            found = Workspace(_local_partition_workspace(partition, project).root, durable=durable)
            require_cli_modifiable(found)
            opened[partition] = found
        return opened[partition]

    def precheck(partition: str, share: Sequence[JobItem]) -> None:
        into = workspace(partition)
        if not arguments.install and not arguments.embed_workflow:
            scaffold._installed(into, source, owner=None, install=False, step=arguments.step, language=arguments.format)
        if arguments.duplicate_tag == "refuse":
            tags = [str(tag) for item in share if (tag := item.get("tag"))]
            for tag in [tag for tag, count in Counter(tags).items() if count > 1][:1]:
                raise ValueError(
                    f"two jobs for campaign partition {partition!r} have the tag {tag!r}; nothing was submitted "
                    "(pass --duplicate-tag allow to create both, or skip to create the first)"
                )
            scaffold._refuse_taken_tags(into, arguments.placement, tags)

    def split(batch: Sequence[JobItem]) -> list[tuple[Workspace, list[JobItem]]]:
        shares: dict[str, list[JobItem]] = {}
        for position, item in enumerate(batch):
            if arguments.partition is not None:
                partition = arguments.partition
            elif config.assignment == "hash":
                tag = item.get("tag")
                if not tag:
                    raise ValueError(
                        "hash campaign assignment hashes the job's tag, and no input supplies one; give --tag TAG"
                    )
                partition = assign_partition(str(tag), config=config)
            else:
                partition = assign_partition("", index=(arguments.campaign_index or 0) + position, config=config)
            shares.setdefault(partition, []).append(item)
        for partition, share in shares.items():
            precheck(partition, share)
        return [(workspace(partition), share) for partition, share in shares.items()]

    return split


def campaign_workspaces(arguments: argparse.Namespace, context: CLIContext) -> list[Workspace]:
    """Return the local workspaces of the partitions ``--partition`` selects (default: all), in stable order.

    :param arguments: The parsed ``collect`` arguments.
    :param context: The invocation context, whose directory locates the project.
    :return: The selected partitions' workspaces.
    :raises ValueError: If a partition is unknown or remote, or the campaign has no partitions.
    """

    config = read_campaign(context.cwd)
    if not config.partitions:
        raise ValueError("this project defines no campaign partitions")
    return [
        _local_partition_workspace(partition, context.cwd, _COLLECT_HINT)
        for partition in _selected(config, arguments.partition or None)
    ]


#: ``run`` options a ``--campaign`` run passes to each partition's managers.
_CAMPAIGN_RUN_OPTIONS = frozenset(
    {"workers", "launcher", "worker_resource", "count", "idle_timeout", "adapter_timeout"}
)
#: ``run`` destinations whose flag is not ``--`` plus the destination with hyphens.
_RUN_FLAGS = {"placement_prefix": "--placement", "idle": "--keep-running"}


def run_campaign(arguments: argparse.Namespace, context: CLIContext, defaults: Mapping[str, object]) -> int:
    """Start a manager per selected partition of this campaign: ``run --campaign``.

    :param arguments: The parsed ``run`` arguments.
    :param context: The invocation context.
    :param defaults: Every ``run`` option's default; one changed outside :data:`_CAMPAIGN_RUN_OPTIONS` is refused.
    :return: 1 when a partition's launch failed, else 0.
    """

    from ._manager import _worker_resources

    for name, default in defaults.items():
        if name not in _CAMPAIGN_RUN_OPTIONS and getattr(arguments, name, default) != default:
            flag = _RUN_FLAGS.get(name, f"--{name.replace('_', '-')}")
            raise ValueError(
                f"{flag} does not apply with --campaign, which starts each partition's managers in its own workspace "
                "with --workers, --launcher, --worker-resource, --count, --idle-timeout and --adapter-timeout only"
            )
    report = campaign_managers(
        partitions=arguments.partition or None,
        workers=arguments.workers,
        resources=_worker_resources(arguments.worker_resource),
        count=arguments.count,
        launcher=arguments.launcher,
        idle_timeout=arguments.idle_timeout,
        adapter_timeout=arguments.adapter_timeout,
        project=context.cwd,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if any(_campaign_launch_failed(row) for row in report) else 0


def _campaign_launch_failed(row: Mapping[str, object]) -> bool:
    """Return whether one campaign manager launch reported a failure."""

    returncode = row.get("returncode")
    if isinstance(returncode, int) and returncode != 0:
        return True
    result = row.get("result")
    if isinstance(result, int) and result != 0:
        return True
    return isinstance(result, Mapping) and result.get("ok") is False


def build_campaign_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``campaign`` group: a partition map over many workspaces."""

    _, group = _group(
        subparsers,
        "campaign",
        summary="partition a large campaign across many workspaces",
        description=(
            "Define a campaign that partitions its jobs across many registered workspaces; job new, collect and "
            "run take --campaign to act on its partitions"
        ),
    )

    init = _leaf(
        group,
        "init",
        summary="define the partition map and assignment policy",
        description="Define this project's campaign partitions and how root jobs are assigned to them",
        handler=handle_campaign_init,
    )
    init.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME=WORKSPACE",
        help="map one partition name to a registered workspace (repeatable)",
    )
    init.add_argument(
        "--assignment",
        choices=ASSIGNMENT_POLICIES,
        default="hash",
        help="how root jobs pick a partition (default: hash)",
    )

    show = _leaf(
        group,
        "show",
        summary="show the partition map",
        description="Show this project's campaign partition map and assignment policy",
        handler=handle_campaign_show,
    )
    show.add_argument("--json", action="store_true", help="print the campaign as one JSON object")

    store = _leaf(
        group,
        "set",
        summary="set campaign assignment and partitions",
        description="Set campaign assignment and partitions, saving only after every change is valid",
        handler=handle_campaign_set,
    )
    store.add_argument(
        "pairs", metavar="KEY=VALUE", nargs="+", type=_setting_pair, help="assignment or partitions.NAME"
    )
    store.set_defaults(pairs_parser=store)
    clear = _leaf(
        group,
        "unset",
        summary="remove partitions or reset the assignment",
        description="Remove campaign partitions or reset the assignment to hash, saving only when every change is valid",
        handler=handle_campaign_unset,
    )
    clear.add_argument("keys", metavar="KEY", nargs="+", help="partitions.NAME, or assignment to reset it to hash")
    remove = _leaf(
        group,
        "remove",
        summary="remove the campaign configuration",
        description="Clear the project's campaign configuration; workspaces and jobs remain intact",
        handler=handle_campaign_remove,
    )
    remove.add_argument("--yes", action="store_true", help="skip the confirmation")
