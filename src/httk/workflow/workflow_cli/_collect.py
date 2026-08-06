"""The collect command."""

import argparse
import json

from httk.core import Run

from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, CollectedJob, collect, job_records
from ._common import *
from ._common import _leaf, _local_root


def _edge_counts(run: Run) -> dict[str, int]:
    return {side: len(getattr(run, side)) for side in ("inputs", "artifacts", "outputs")}


def _collected_mapping(item: CollectedJob) -> dict[str, object]:
    outputs = item.outputs
    return {
        "format": "httk-workflow-collected",
        "format_version": 1,
        "job_id": item.record.job_id,
        "job_key": item.record.job_key,
        "workflow": item.workflow_id,
        "outputs": {
            role: {"type": getattr(value, "type", ""), "id": getattr(value, "id", "")}
            for role, value in outputs.items()
        },
        "unfulfilled": list(item.unfulfilled),
        "missing_postprocessor": item.missing_postprocessor,
        "run": {
            "workflow_declaration_uri": item.run.workflow_declaration_uri,
            "edges": _edge_counts(item.run),
        },
        "products": [
            {
                "source_type": product.source_type,
                "source_id": product.source_id,
                "target_type": product.target_type,
                "target_id": product.target_id,
                "label": product.label,
                "workflow_declaration_uri": product.workflow_declaration_uri,
            }
            for product in item.products
        ],
    }


def handle_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream collected workflow summaries, or raw job records."""

    workspace = Workspace(_local_root(arguments, context, action="collect from"), mutable=False)
    if arguments.raw or arguments.json:
        records = job_records(
            workspace, states=arguments.state or DEFAULT_COLLECT_STATES, placement=arguments.placement
        )
        if arguments.json:
            print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
            return 0
        for record in records:
            print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
        return 0
    for item in collect(
        workspace,
        states=arguments.state or DEFAULT_COLLECT_STATES,
        placement=arguments.placement,
        allow_job_postprocessor=arguments.allow_job_postprocessor,
    ):
        print(json.dumps(_collected_mapping(item), sort_keys=True, separators=(",", ":")))
    return 0


def build_collect_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    parser = _leaf(
        subparsers,
        "collect",
        summary="stream collected workflow summaries",
        description="Collect finished jobs from one workflow workspace",
        handler=handle_collect,
    )
    add_workspace_argument(parser, help_text="the workspace to collect from")
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help=f"state kind to collect (repeatable, default: {', '.join(DEFAULT_COLLECT_STATES)})",
    )
    parser.add_argument("--placement", metavar="PLACEMENT", help="collect only jobs at or below this placement")
    parser.add_argument("--raw", action="store_true", help="print raw collect records instead of summaries")
    parser.add_argument("--jsonl", action="store_true", dest="raw", help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--allow-job-postprocessor",
        action="store_true",
        help="allow job postprocessors (reserved; not functional until the tree fallback phase)",
    )
