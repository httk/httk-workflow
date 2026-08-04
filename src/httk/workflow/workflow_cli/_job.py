"""Runner and job command groups."""

from ._common import *
from ._common import (
    _durable,
    _group,
    _json_value,
    _leaf,
    _local_root,
    _pairs,
)

# ---------------------------------------------------------------------------
# runner
# ---------------------------------------------------------------------------


def handle_runner_publish(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish one runner file into a workspace runner store."""

    reference = Workspace(_local_root(arguments, context, action="publish a runner into it")).publish_runner(
        arguments.file,
        name=arguments.name,
        replace=arguments.replace,
    )
    print(json.dumps(reference, indent=2, sort_keys=True))
    return 0


def handle_runner_describe(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Report the runners a workspace has published, with their digests."""

    workspace = Workspace(_local_root(arguments, context, action="read its runners"), mutable=False)
    store = workspace.runners
    if arguments.name is not None:
        target = workspace.runner_store_path(arguments.name)
        if not target.is_file():
            raise ValueError(f"no such workspace runner: {arguments.name}")
        found = [target]
    else:
        found = sorted(path for path in store.rglob("*") if path.is_file()) if store.is_dir() else []
    references = [
        {
            "source": "workspace",
            "path": path.relative_to(store).as_posix(),
            "sha256": sha256_file(path),
        }
        for path in found
    ]
    if arguments.json:
        print(json.dumps(references, indent=2, sort_keys=True))
        return 0
    for reference in references:
        print(f"{reference['path']}\t{reference['sha256']}")
    return 0


def build_runner_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``runner`` group: the shared runners a workspace publishes."""

    _, group = _group(
        subparsers,
        "runner",
        summary="publish and describe the shared runners of a workspace",
        description="Manage the shared runners one workspace publishes for its jobs",
    )

    publish = _leaf(
        group,
        "publish",
        summary="publish one runner into a workspace runner store",
        description="Publish one runner into a workspace runner store, pinned by digest",
        handler=handle_runner_publish,
    )
    publish.add_argument("file", metavar="FILE", help="the runner file to publish")
    publish.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        required=True,
        help="the workspace to publish into",
    )
    publish.add_argument(
        "--name",
        metavar="NAME",
        help="store name, including any subdirectory (default: the file name)",
    )
    publish.add_argument(
        "--replace",
        action="store_true",
        help="overwrite a stored runner of the same name whose content differs",
    )

    describe = _leaf(
        group,
        "describe",
        summary="report the published runners and their digests",
        description="Report the runners a workspace has published, as the references a job pins",
        handler=handle_runner_describe,
    )
    describe.add_argument(
        "name",
        metavar="NAME",
        nargs="?",
        help="one store name (default: every published runner)",
    )
    describe.add_argument("--workspace", metavar="WORKSPACE", required=True, help="the workspace to read")
    describe.add_argument("--json", action="store_true", help="print the references as one JSON array")


# ---------------------------------------------------------------------------
# job
# ---------------------------------------------------------------------------


def handle_job_new(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Scaffold and submit one job per template, structure, or both."""

    workspace = Workspace(_local_root(arguments, context, action="submit into it"))
    inputs = {name: _json_value(text, f"job input {name!r}") for name, text in _pairs(arguments.inputs, "a job input")}
    files: dict[str, str | Path] = {name: Path(text) for name, text in _pairs(arguments.files, "a staged file")}
    shared: dict[str, Any] = {
        "inputs": inputs,
        "placement": arguments.placement,
        "priority": arguments.priority,
        "workdir_mode": arguments.workdir_mode,
        "data_mode": arguments.data_mode,
        "publish": arguments.publish,
        "step": arguments.step,
        "name": arguments.name,
    }
    structures = Path(arguments.structures).expanduser() if arguments.structures else None
    if structures is not None and structures.is_dir():
        found = structure_files(structures)
        if not found:
            raise ValueError(f"no {' or '.join(STRUCTURE_PATTERNS)} file in {structures}")
        items: list[JobItem] = [
            {
                "files": {**files, "POSCAR": path},
                "tag": arguments.tag or structure_tag(path) or f"structure-{index:04d}",
            }
            for index, path in enumerate(found)
        ]
        results: Iterator[ScaffoldedJob] = new_jobs(workspace, arguments.template, items, **shared)
    else:
        tag = arguments.tag
        if structures is not None:
            files["POSCAR"] = structures
            tag = tag or structure_tag(structures)
        results = iter([new_job(workspace, arguments.template, files=files, tag=tag, **shared)])
    if arguments.json:
        # One self-describing report per job, as an array, exactly as `harvest
        # --json` prints one array of records.
        print(json.dumps([job.as_mapping() for job in results], indent=2))
        return 0
    for job in results:
        # One tab-separated line per job, so a shell reads the key of one job with
        # cut and a campaign streams as it is submitted.
        print(f"{job.job_key}\t{job.payload}")
    return 0


def add_job_submit_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job submit`, shared with ``httk-taskmanager submit``."""

    add_workspace_argument(parser, help_text="the workspace to submit into")
    parser.add_argument("source", metavar="SOURCE", help="the complete payload directory to submit")
    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        required=True,
        help="where the job lands in the tree",
    )
    parser.add_argument("--move", action="store_true", help="rename rather than copy the source")
    add_durability_arguments(parser)


def handle_job_submit(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Submit one prepared payload directory and print its marker."""

    workspace = Workspace(
        _local_root(arguments, context, action="submit into it"),
        durable=_durable(arguments),
    )
    marker = workspace.submit(arguments.source, arguments.placement, move=arguments.move)
    print(marker.path)
    return 0


def add_job_request_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare :command:`job request`, shared with ``httk-taskmanager request``."""

    add_workspace_argument(parser, help_text="the workspace holding the job")
    parser.add_argument("job_id", metavar="JOB_ID", help="the UUID of the job the request is about")
    parser.add_argument(
        "action",
        metavar="ACTION",
        choices=("continue", "override_step", "cancel", "set_priority", "pause"),
        help="continue, override_step, cancel, set_priority, or pause",
    )
    parser.add_argument(
        "--operator",
        metavar="NAME",
        required=True,
        help="who is asking, recorded in the state frame",
    )
    parser.add_argument(
        "--reason",
        metavar="TEXT",
        required=True,
        help="why, recorded in the state frame",
    )
    parser.add_argument(
        "--priority",
        type=int,
        metavar="PRIORITY",
        help="the new priority, for set_priority",
    )
    parser.add_argument("--step", metavar="STEP", help="the step to resume at, for override_step")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "accept the hazard of reviving a job a decided join already consumed; "
            "the hazard is journalled in the resulting state frame"
        ),
    )
    add_durability_arguments(parser)


def handle_job_request(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Publish one operator request against a job and print its path."""

    workspace = Workspace(
        _local_root(arguments, context, action="request against it"),
        durable=_durable(arguments),
    )
    marker = workspace.find_marker_by_id(arguments.job_id)
    if marker is None:
        raise ValueError(f"job does not exist: {arguments.job_id}")
    request: dict[str, object] = {
        "format": "httk-workflow-request",
        "format_version": 1,
        "request_id": str(uuid.uuid4()),
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "placement": marker.placement.as_posix(),
        "expected_generation": marker.generation,
        "expected_record_ref": marker.record_ref,
        "action": arguments.action,
        "operator": arguments.operator,
        "reason": arguments.reason,
        "created_at": utc_now(),
    }
    if arguments.priority is not None:
        request["priority"] = arguments.priority
    if arguments.step is not None:
        request["step"] = arguments.step
    if arguments.force:
        request["force"] = True
    # Attribution, when this installation has an identity key: the manager
    # verifies a signature that is there and accepts a request that has none.
    print(workspace.publish_request(sign_document(request)))
    return 0


def handle_job_list(arguments: argparse.Namespace, context: CLIContext) -> int:
    """List the jobs of one workspace as a cheap table."""

    workspace = Workspace(_local_root(arguments, context, action="list its jobs"), mutable=False)
    rows = list_jobs(workspace, kinds=arguments.kind, placement=arguments.placement)
    if arguments.json:
        print(json.dumps({"format": JOB_LIST_FORMAT, "format_version": 1, "jobs": rows}, indent=2))
        return 0
    print(render_rows(rows))
    return 0


def handle_job_show(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Describe one job completely from its authoritative state."""

    workspace = Workspace(_local_root(arguments, context, action="show its jobs"), mutable=False)
    report = describe_job(workspace, resolve_job(workspace, arguments.job))
    print(json.dumps(report, indent=2, sort_keys=True) if arguments.json else render_job(report))
    return 0


def handle_job_log(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Print the recorded transition history of one job, oldest first."""

    if arguments.limit is not None and arguments.limit < 1:
        raise ValueError("--limit must be positive")
    workspace = Workspace(_local_root(arguments, context, action="read its job log"), mutable=False)
    frames = job_frames(workspace, resolve_job(workspace, arguments.job), limit=arguments.limit)
    if arguments.json:
        print(
            json.dumps(
                {"format": JOB_HISTORY_FORMAT, "format_version": 1, "frames": frames},
                indent=2,
            )
        )
        return 0
    print(render_frames(frames))
    return 0


def handle_job_why(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Explain why one job is, or is not, making progress."""

    workspace = Workspace(_local_root(arguments, context, action="explain its jobs"), mutable=False)
    diagnosis = explain_job(workspace, resolve_job(workspace, arguments.job))
    print(json.dumps(diagnosis.as_mapping(), indent=2, sort_keys=True) if arguments.json else diagnosis.render())
    return 0


def handle_job_debug(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Drive one job to a terminal state in the foreground."""

    # The transitions of the debugged job are reported by the debug runner
    # itself, so the private manager's own log stays quiet unless asked for.
    configure_logging(level=arguments.log_level)
    workspace = Workspace(_local_root(arguments, context, action="debug in it"))
    outcome = debug_job(
        workspace,
        arguments.job,
        placement=arguments.placement,
        step=arguments.step,
        follow_children=arguments.follow_children,
        timeout=arguments.timeout,
    )
    return outcome.exit_code


def _add_job_selector(parser: argparse.ArgumentParser) -> None:
    """Add the workspace and job selector every inspection command shares."""

    add_workspace_argument(parser, help_text="the workspace holding the job")
    parser.add_argument("job", metavar="JOB", help="job UUID, job key, or any unique prefix of either")


def build_job_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``job`` group: making jobs, and finding out about them."""

    _, group = _group(
        subparsers,
        "job",
        summary="create, submit, inspect, and debug individual jobs",
        description="Create, submit, inspect, and debug the jobs of one workflow workspace",
    )

    new = _leaf(
        group,
        "new",
        summary="scaffold and submit jobs from a runner template",
        description="Scaffold and submit jobs from a runner template",
        handler=handle_job_new,
    )
    add_workspace_argument(new, help_text="the workspace to submit into")
    new.add_argument(
        "--template",
        metavar="TEMPLATE",
        required=True,
        help=f"a registered template ({', '.join(registered_templates())}) or the path of a runner file",
    )
    new.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        metavar="NAME=VALUE",
        help="one job input; VALUE is JSON when it parses as JSON and a string otherwise, "
        "and NAME=@FILE reads a JSON file (repeatable)",
    )
    new.add_argument(
        "--file",
        action="append",
        default=[],
        dest="files",
        metavar="NAME=PATH",
        help="stage PATH in the payload as NAME; a bare NAME lands in files/ (repeatable)",
    )
    new.add_argument(
        "--from",
        dest="structures",
        metavar="PATH",
        help="a structure file staged as files/POSCAR, or a directory of "
        f"{' / '.join(STRUCTURE_PATTERNS)} files, one job each",
    )
    new.add_argument(
        "--tag",
        metavar="TAG",
        help="the readable half of the job key (default: derived from --from)",
    )
    new.add_argument("--name", metavar="NAME", help="the human-readable job name")
    new.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default=DEFAULT_PLACEMENT,
        help=f"placement subtree (default: {DEFAULT_PLACEMENT})",
    )
    new.add_argument(
        "--priority",
        type=int,
        metavar="PRIORITY",
        help="scheduling priority (default: the template's)",
    )
    new.add_argument(
        "--step",
        metavar="STEP",
        help="the step the job starts at (default: the template's own)",
    )
    new.add_argument(
        "--data-mode",
        choices=("none", "transactional"),
        help="the job's data mode (default: what the template needs)",
    )
    new.add_argument(
        "--workdir-mode",
        choices=("persistent", "isolated"),
        default="persistent",
        help="the job's working-directory mode (default: persistent)",
    )
    new.add_argument(
        "--publish",
        choices=("workspace", "installed"),
        default="workspace",
        help="publish the runner into the workspace store (default), or reference a packaged one where it is installed",
    )
    new.add_argument("--json", action="store_true", help="print one JSON report per job, as an array")

    add_job_submit_arguments(
        _leaf(
            group,
            "submit",
            summary="submit a complete payload directory",
            description="Submit one complete payload directory into a workspace",
            handler=handle_job_submit,
        )
    )
    add_job_request_arguments(
        _leaf(
            group,
            "request",
            summary="publish an operator request",
            description="Publish one operator request against a job of a workspace",
            handler=handle_job_request,
        )
    )

    listing = _leaf(
        group,
        "list",
        summary="list the jobs of a workspace",
        description="List the jobs of one workflow workspace",
        handler=handle_job_list,
    )
    add_workspace_argument(listing, help_text="the workspace to list")
    listing.add_argument(
        "--kind",
        action="append",
        metavar="KIND",
        choices=STATE_KINDS,
        help="state kind to list (repeatable, default: every kind)",
    )
    listing.add_argument(
        "--placement",
        metavar="PLACEMENT",
        help="list only jobs at or below this placement",
    )
    listing.add_argument("--json", action="store_true", help="print the rows as one JSON document")

    show = _leaf(
        group,
        "show",
        summary="describe one job from its authoritative state",
        description="Describe one job from its authoritative state",
        handler=handle_job_show,
    )
    _add_job_selector(show)
    show.add_argument("--json", action="store_true", help="print the description as one JSON document")

    log = _leaf(
        group,
        "log",
        summary="print the transition history of one job",
        description="Print the recorded transition history of one job, oldest first",
        handler=handle_job_log,
    )
    _add_job_selector(log)
    log.add_argument(
        "--limit",
        type=int,
        metavar="COUNT",
        help="read at most this many frames, newest first",
    )
    log.add_argument("--json", action="store_true", help="print the frames as one JSON document")

    why = _leaf(
        group,
        "why",
        summary="explain why one job is not running",
        description="Explain why one job is, or is not, making progress",
        handler=handle_job_why,
    )
    _add_job_selector(why)
    why.add_argument("--json", action="store_true", help="print the diagnosis as one JSON document")

    debug = _leaf(
        group,
        "debug",
        summary="drive one job to a terminal state in the foreground",
        description="Drive one job to a terminal state in the foreground, reporting every transition",
        handler=handle_job_debug,
    )
    add_workspace_argument(debug, help_text="the workspace to debug in")
    debug.add_argument(
        "job",
        metavar="JOB",
        help="a payload directory to submit, or a selector of an existing job",
    )
    debug.add_argument("--step", metavar="STEP", help="initial step of a freshly submitted payload")
    debug.add_argument(
        "--placement",
        metavar="PLACEMENT",
        default="debug",
        help="placement of a freshly submitted payload (default: debug)",
    )
    debug.add_argument(
        "--follow-children",
        action="store_true",
        help="drive spawned children depth first",
    )
    debug.add_argument(
        "--timeout",
        type=float,
        metavar="SECONDS",
        default=3600.0,
        help="give up driving the job after this long (default: 3600)",
    )
    debug.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        default="error",
        help="log level of the private manager on the console (default: error)",
    )
