"""Campaign command group."""

from ._common import *
from ._common import (
    _add_adapter_timeout,
    _group,
    _json_value,
    _leaf,
    _pairs,
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


def handle_campaign_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Assign one root job to a partition and submit it into that workspace."""

    inputs = {name: _json_value(text, f"job input {name!r}") for name, text in _pairs(arguments.inputs, "a job input")}
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    job = campaign_submit(
        arguments.template,
        key=arguments.key,
        index=arguments.index,
        project=context.cwd,
        inputs=inputs,
        files=files,
        tag=arguments.tag,
        placement=arguments.placement or DEFAULT_PLACEMENT,
        priority=arguments.priority,
        name=arguments.name,
    )
    if arguments.json:
        print(json.dumps(job.as_mapping(), indent=2))
        return 0
    print(f"{job.job_key}\t{job.payload}")
    return 0


def handle_campaign_harvest(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Harvest every partition of this campaign, one workspace after another."""

    records = campaign_harvest(
        states=arguments.state or DEFAULT_HARVEST_STATES,
        placement=arguments.placement,
        partitions=arguments.partition or None,
        project=context.cwd,
    )
    if arguments.json:
        print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
        return 0
    for record in records:
        print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
    return 0


def handle_campaign_start_managers(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Start a manager per selected partition of this campaign."""

    report = campaign_managers(
        partitions=arguments.partition or None,
        workers=arguments.workers,
        count=arguments.count,
        idle_timeout=arguments.idle_timeout,
        adapter_timeout=arguments.adapter_timeout,
        project=context.cwd,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def build_campaign_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``campaign`` group: a partition map over many workspaces."""

    _, group = _group(
        subparsers,
        "campaign",
        summary="partition a large campaign across many workspaces",
        description="Define and drive a campaign that partitions its jobs across many registered workspaces",
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

    submit = _leaf(
        group,
        "submit",
        summary="submit one root job to its assigned partition",
        description="Assign one root job to a partition by policy and submit it into that partition's workspace",
        handler=handle_campaign_submit,
    )
    submit.add_argument(
        "--template",
        metavar="TEMPLATE",
        required=True,
        help="the runner template to scaffold",
    )
    submit.add_argument(
        "--key",
        metavar="KEY",
        required=True,
        help="the tag or key the assignment policy hashes",
    )
    submit.add_argument(
        "--index",
        type=int,
        default=0,
        metavar="N",
        help="the batch position, for round-robin (default: 0)",
    )
    submit.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one job input (repeatable)",
    )
    submit.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="one staged file (repeatable)",
    )
    submit.add_argument("--tag", metavar="TAG", help="the job tag")
    submit.add_argument("--placement", metavar="PLACEMENT", help="where the job lands in its workspace")
    submit.add_argument("--priority", type=int, metavar="PRIORITY", help="the job priority")
    submit.add_argument("--name", metavar="NAME", help="a human name for the job")
    submit.add_argument(
        "--json",
        action="store_true",
        help="print the scaffolded job as one JSON object",
    )

    harvest_parser = _leaf(
        group,
        "harvest",
        summary="harvest every partition of the campaign",
        description="Harvest the finished jobs of every campaign partition, one workspace after another",
        handler=handle_campaign_harvest,
    )
    harvest_parser.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME",
        help="harvest only this partition (repeatable, default: all of them)",
    )
    harvest_parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to harvest (repeatable, default: {', '.join(DEFAULT_HARVEST_STATES)})",
    )
    harvest_parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        help="harvest only jobs at or below this placement",
    )
    harvest_parser.add_argument("--json", action="store_true", help="print every record as one JSON array")

    managers = _leaf(
        group,
        "start-managers",
        summary="start a manager per selected partition",
        description="Start a manager for each selected partition: in-process for local ones, via the scheduler for remote ones",
        handler=handle_campaign_start_managers,
    )
    managers.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME",
        help="start a manager only for this partition (repeatable, default: all of them)",
    )
    managers.add_argument("--workers", type=int, metavar="COUNT", help="workers per manager")
    managers.add_argument(
        "--count",
        type=int,
        default=1,
        metavar="COUNT",
        help="remote managers to submit per partition (default: 1)",
    )
    managers.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="cap how long each local manager waits for the campaign to become idle (default: 3600)",
    )
    _add_adapter_timeout(managers)
