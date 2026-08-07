"""The postprocess command."""

from __future__ import annotations

import argparse
import json
from typing import Any

from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, job_records
from ..packages import load_workflow_package
from ..postprocessing import DEFAULT_POSTPROCESS_TIMEOUT, PostprocessResult, run_postprocess_script
from ..scaffold import registered_workflow
from ..workspace import Workspace
from ._common import _leaf, _local_root, add_workspace_argument


def _result_mapping(result: PostprocessResult, record: Any) -> dict[str, object]:
    return {
        "format": "httk-workflow-postprocess",
        "format_version": 1,
        "workspace_id": result.workspace_id,
        "job_id": result.job_id,
        "job_key": record.job_key,
        "script": result.script,
        "returncode": result.returncode,
        "output_dir": str(result.output_dir),
    }


def _error_mapping(record: Any, script: str, error: str) -> dict[str, object]:
    return {
        "format": "httk-workflow-postprocess",
        "format_version": 1,
        "workspace_id": record.workspace_id,
        "job_id": record.job_id,
        "job_key": record.job_key,
        "script": script,
        "error": error,
    }


def handle_postprocess(arguments: argparse.Namespace, context: Any) -> int:
    """Run one curated script for each selected collected job."""

    workspace = Workspace(_local_root(arguments, context, action="postprocess"), mutable=False)
    resolved = None
    if arguments.workflow_dir is not None:
        resolved = load_workflow_package(arguments.workflow_dir, register=False)
    failed = False
    for record in job_records(
        workspace,
        states=arguments.state or DEFAULT_COLLECT_STATES,
        placement=arguments.placement,
    ):
        try:
            selected: Any = resolved
            if selected is None:
                workflow = record.job.get("workflow")
                selected = registered_workflow(workflow) if isinstance(workflow, str) else None
            if selected is None:
                raise ValueError(f"workflow {record.job.get('workflow')!r} is not registered")
            result = run_postprocess_script(selected, arguments.script, record, timeout=arguments.timeout)
        except (OSError, ValueError, RuntimeError) as exc:
            failed = True
            error = _error_mapping(record, arguments.script, str(exc))
            if arguments.json:
                print(json.dumps(error, sort_keys=True, separators=(",", ":")))
            else:
                print(f"{record.job_key}\t{arguments.script}\tERROR\t{exc}")
            continue
        failed |= result.returncode != 0
        if arguments.json:
            print(json.dumps(_result_mapping(result, record), sort_keys=True, separators=(",", ":")))
        else:
            print(f"{record.job_key}\t{result.script}\t{result.returncode}\t{result.output_dir}")
    return 1 if failed else 0


def build_postprocess_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = _leaf(
        subparsers,
        "postprocess",
        summary="run a curated workflow postprocess script",
        description="Run a manifest-declared postprocess script against collected jobs",
        handler=handle_postprocess,
    )
    add_workspace_argument(parser, help_text="postprocess jobs from")
    parser.add_argument("--script", required=True, metavar="NAME", help="declared postprocess script name")
    parser.add_argument("--workflow-dir", metavar="PKG", help="use this workflow package for every job")
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help=f"job state to postprocess (repeatable, default: {', '.join(DEFAULT_COLLECT_STATES)})",
    )
    parser.add_argument("--placement", metavar="PREFIX", help="postprocess only jobs at or below this placement")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_POSTPROCESS_TIMEOUT,
        metavar="SECONDS",
        help=f"script timeout (default: {DEFAULT_POSTPROCESS_TIMEOUT:g})",
    )
    parser.add_argument("--json", action="store_true", help="emit one JSON object per job")


__all__ = ["build_postprocess_parser", "handle_postprocess"]
