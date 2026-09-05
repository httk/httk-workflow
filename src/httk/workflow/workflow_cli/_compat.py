"""Import and v1 compatibility command groups."""

import argparse
import json
import sys

from httk.workflow.collecting import _CollectEnvironmentError
from httk.workflow.compat.v1 import collect_finished_tree

from ._collect import _collected_mapping, _positive_int, _store_collected
from ._common import CLIContext, _group, _leaf


def handle_v1_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Collect finished directories from pre-existing httk v1 trees."""

    del context
    if arguments.into is not None and arguments.id_base is None:
        raise ValueError("--id-base is required with --into")
    failed = False
    for root in arguments.roots:
        try:
            stats: dict[str, object] = {}
            collected_items = collect_finished_tree(
                root,
                workflow_dir=arguments.workflow_dir,
                stats=stats,
                fail_fast=arguments.fail_fast,
                batch_size=arguments.batch_size,
            )
            if arguments.into is not None:
                # --into retains the root sweep for its provenance storage pass.
                items = list(collected_items)
                reports = _store_collected(
                    items, arguments.into, id_base=arguments.id_base, id_series=arguments.id_series
                )
                for item, report in zip(items, reports):
                    if not arguments.degraded or item.missing_collector is not None:
                        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
                finished = len(items)
            else:
                finished = 0
                for item in collected_items:
                    finished += 1
                    if not arguments.degraded or item.missing_collector is not None:
                        print(json.dumps(_collected_mapping(item), sort_keys=True, separators=(",", ":")))
            unfinished = stats.get("unfinished_by_status")
            print(
                json.dumps(
                    {
                        "format": "httk-workflow-v1-collect-summary",
                        "format_version": 2,
                        "finished": finished,
                        "unfinished_by_status": dict(sorted(unfinished.items()))
                        if isinstance(unfinished, dict)
                        else {},
                        "skipped_no_rundir": stats.get("skipped_no_rundir", 0),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        except _CollectEnvironmentError:
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            failed = True
            print(f"{root}: {exc}", file=sys.stderr)
            if arguments.fail_fast:
                break
    return 1 if failed else 0


def add_v1_collect_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare v1 collect."""

    parser.add_argument("roots", metavar="ROOT", nargs="+", help="roots of finished httk v1 result trees")
    parser.add_argument(
        "--workflow-dir",
        required=True,
        metavar="PKG",
        help="the directory workflow package providing the collect hook",
    )
    parser.add_argument("--into", metavar="PATH", help="save collected entries, runs, and products to SQLite")
    parser.add_argument("--degraded", action="store_true", help="print only degraded per-task lines")
    parser.add_argument("--fail-fast", action="store_true", help="stop at the first degraded task")
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        default=64,
        metavar="N",
        help="validated compatibility window size (default: 64)",
    )
    parser.add_argument(
        "--id-base",
        metavar="BASE",
        help="entry-id namespace base (required with --into)",
    )
    parser.add_argument(
        "--id-series",
        metavar="SERIES",
        default="1",
        help="entry-id campaign series (default: 1)",
    )


def build_v1_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
) -> None:
    """Declare the ``v1`` group for collecting an old v1 result tree."""

    _, group = _group(
        subparsers,
        "v1",
        summary="collect httk v1 result trees",
        description="Collect an old v1 result tree",
    )
    add_v1_collect_arguments(
        _leaf(
            group,
            "collect",
            summary="collect a finished v1 result tree",
            description="Collect finished tasks from a pre-existing httk v1 result tree",
            handler=handle_v1_collect,
        )
    )
