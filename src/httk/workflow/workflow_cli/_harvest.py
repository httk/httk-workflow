"""Harvest command group."""

from ._common import *
from ._common import (
    _leaf,
    _local_root,
)

# ---------------------------------------------------------------------------
# harvest
# ---------------------------------------------------------------------------


def handle_harvest(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream the finished jobs of one workspace as harvest records."""

    workspace = Workspace(_local_root(arguments, context, action="harvest it"), mutable=False)
    records = harvest(
        workspace,
        states=arguments.state or DEFAULT_HARVEST_STATES,
        placement=arguments.placement,
    )
    if arguments.json:
        print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
        return 0
    for record in records:
        print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
    return 0


def build_harvest_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare ``harvest``: the one leaf that is a command rather than a group."""

    parser = _leaf(
        subparsers,
        "harvest",
        summary="stream the finished jobs of a workspace as records",
        description="Harvest the finished jobs of one workflow workspace",
        handler=handle_harvest,
    )
    parser.add_argument("workspace", metavar="WORKSPACE", help="the workspace to harvest")
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=HARVESTABLE_KINDS,
        help=f"state kind to harvest (repeatable, default: {', '.join(DEFAULT_HARVEST_STATES)})",
    )
    parser.add_argument(
        "--placement",
        metavar="PLACEMENT",
        help="harvest only jobs at or below this placement",
    )
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--jsonl",
        action="store_true",
        help="print one record object per line, streaming (the default)",
    )
    output.add_argument(
        "--json",
        action="store_true",
        help="print one JSON array of every record, which materializes the whole harvest",
    )
