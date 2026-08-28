"""Campaign command group."""

from ._common import *
from ._common import (
    _add_adapter_timeout,
    _group,
    _json_value,
    _leaf,
    _load_inputs,
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

    parameters = {
        name: _json_value(text, f"job parameter {name!r}")
        for name, text in _pairs(arguments.parameters, "a job parameter")
    }
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    inputs, items, input_tag = _load_inputs(arguments.inputs, arguments.input_from)
    shared = {
        "inputs": inputs,
        "files": files,
        "parameters": parameters,
        "tag": arguments.tag or input_tag,
        "placement": arguments.placement or DEFAULT_PLACEMENT,
        "priority": arguments.priority,
        "name": arguments.name,
    }
    if items:
        for item in items:
            item["tag"] = arguments.tag or item.get("tag")
        jobs = campaign_submit_many(
            arguments.workflow, items, key=arguments.key, index=arguments.index, project=context.cwd, **shared
        )
    else:
        jobs = [
            campaign_submit(arguments.workflow, key=arguments.key, index=arguments.index, project=context.cwd, **shared)
        ]
    if arguments.json:
        print(json.dumps([job.as_mapping() for job in jobs] if len(jobs) > 1 else jobs[0].as_mapping(), indent=2))
        return 0
    for job in jobs:
        print(f"{job.job_key}\t{job.payload}")
    return 0


def handle_campaign_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Collect every partition of this campaign, one workspace after another."""

    from ..collecting import CollectedJob, job_records
    from ..collecting import collect as collect_jobs
    from ._collect import _collected_mapping, _emit_collect_summary, _store_collected

    if arguments.into is not None and arguments.raw:
        raise ValueError("--into cannot be combined with --raw")
    if arguments.into is not None and arguments.id_base is None:
        raise ValueError("--id-base is required with --into")
    config = read_campaign(context.cwd)
    selected = sorted(arguments.partition or config.partitions)
    states = arguments.state or DEFAULT_COLLECT_STATES
    skipped = 0

    def _skip(_job_key: str) -> None:
        nonlocal skipped
        skipped += 1

    def _workspace(partition: str) -> Workspace:
        binding = resolve_workspace(config.partitions[partition], project=context.cwd)
        if binding.remote != LOCAL_REMOTE:
            raise ValueError(
                f"campaign partition {partition!r} is the remote workspace {config.partitions[partition]!r} on "
                f"{binding.remote!r}; fetch it home with `httk workflow transfer` before collecting the campaign"
            )
        assert binding.path is not None
        return Workspace(binding.path, mutable=False)

    if arguments.raw:
        collected = 0
        for partition in selected:
            for record in job_records(
                _workspace(partition), states=states, placement=arguments.placement, on_skipped=_skip
            ):
                print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
                collected += 1
        return _emit_collect_summary(
            collected=collected, degraded=0, unfulfilled_roles=0, storage_errors=0, skipped_unreadable=skipped
        )

    items: list[CollectedJob] = []
    for partition in selected:
        items.extend(
            collect_jobs(
                _workspace(partition),
                states=states,
                placement=arguments.placement,
                allow_job_collector=arguments.allow_job_collector,
                on_skipped=_skip,
            )
        )
    reports = (
        _store_collected(items, arguments.into, id_base=arguments.id_base, id_series=arguments.id_series)
        if arguments.into is not None
        else [_collected_mapping(item) for item in items]
    )
    for report in reports:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    degraded = sum(1 for item in items if item.missing_collector is not None)
    return _emit_collect_summary(
        collected=len(items) - degraded,
        degraded=degraded,
        unfulfilled_roles=sum(len(item.unfulfilled) for item in items),
        storage_errors=sum(1 for report in reports if report.get("storage_error") is not None),
        skipped_unreadable=skipped,
    )


def handle_campaign_start_managers(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Start a manager per selected partition of this campaign."""

    from ._manager import _worker_resources

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
        "--workflow",
        metavar="WORKFLOW",
        required=True,
        help="the workflow id, alias, or path of a runner file to scaffold",
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
        "--parameter",
        action="append",
        default=[],
        dest="parameters",
        metavar="NAME=VALUE",
        help="one implementation parameter (repeatable)",
    )
    submit.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="one staged file (repeatable)",
    )
    submit.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one declared input value to stage (repeatable)",
    )
    submit.add_argument(
        "--input-from",
        action="append",
        nargs="+",
        default=[],
        metavar=("NAME", "SOURCE"),
        help="load a declared input from one or more files, or readable files in a directory (repeatable)",
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

    collect_parser = _leaf(
        group,
        "collect",
        summary="collect every partition of the campaign",
        description="Collect the finished jobs of every campaign partition, one workspace after another",
        handler=handle_campaign_collect,
    )
    collect_parser.add_argument(
        "--partition",
        action="append",
        default=[],
        metavar="NAME",
        help="collect only this partition (repeatable, default: all of them)",
    )
    collect_parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help=f"state kind to collect (repeatable, default: {', '.join(DEFAULT_COLLECT_STATES)})",
    )
    collect_parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        help="collect only jobs at or below this placement",
    )
    collect_parser.add_argument("--raw", action="store_true", help="print raw collect records")
    collect_parser.add_argument(
        "--allow-job-collector",
        action="store_true",
        help="allow collectors loaded and verified from a pinned workspace workflow tree",
    )
    collect_parser.add_argument(
        "--into",
        metavar="PATH",
        help="save collected entries, runs, and products into a file-backed SQLite store",
    )
    collect_parser.add_argument(
        "--id-base",
        metavar="BASE",
        help="entry-id namespace base (required with --into)",
    )
    collect_parser.add_argument(
        "--id-series",
        metavar="SERIES",
        default="1",
        help="entry-id campaign series (default: 1)",
    )

    managers = _leaf(
        group,
        "start-managers",
        summary="start a manager per selected partition",
        description="Start a manager for each selected partition: through its launcher locally, or on the remote owner",
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
    managers.add_argument("--launcher", metavar="NAME", help="use launcher NAME for every selected partition")
    managers.add_argument(
        "--worker-resource",
        nargs=2,
        action="append",
        default=[],
        metavar=("NAME", "COUNT"),
        help="advertise COUNT units of resource NAME to the scheduler (repeatable; procs and mem are shared fairly among --workers)",
    )
    managers.add_argument(
        "--count",
        type=int,
        default=None,
        metavar="COUNT",
        help="managers to start per partition (default: each workspace's manager.count, or 1)",
    )
    managers.add_argument(
        "--idle-timeout",
        type=float,
        default=3600.0,
        metavar="SECONDS",
        help="cap how long each local manager waits for the campaign to become idle (default: 3600)",
    )
    _add_adapter_timeout(managers)
