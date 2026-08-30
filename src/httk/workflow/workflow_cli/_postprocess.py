"""The postprocess command."""

from __future__ import annotations

import argparse
import json
from typing import Any

from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, job_records
from ..packages import load_workflow_package
from ..postprocessing import (
    DEFAULT_POSTPROCESS_TIMEOUT,
    PostprocessResult,
    postprocess_root,
    run_postprocess_script,
)
from ..scaffold import registered_workflow
from ..workspace import Workspace
from ._common import _leaf, _local_root

#: How much of a failing script's stderr to keep in a report, from its tail.
_STDERR_TAIL_LIMIT = 2000


def _stderr_tail(stderr: str) -> str:
    """Return the trailing portion of *stderr*, bounded to :data:`_STDERR_TAIL_LIMIT`."""

    stripped = stderr.strip()
    if len(stripped) <= _STDERR_TAIL_LIMIT:
        return stripped
    return "…" + stripped[-_STDERR_TAIL_LIMIT:]


def _stderr_last_line(stderr: str) -> str:
    """Return the last non-blank line of *stderr*, or the empty string."""

    lines = [line for line in stderr.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _result_mapping(result: PostprocessResult, record: Any) -> dict[str, object]:
    mapping: dict[str, object] = {
        "format": "httk-workflow-postprocess",
        "format_version": 2,
        "workspace_id": result.workspace_id,
        "job_id": result.job_id,
        "job_key": record.job_key,
        "script": result.script,
        "returncode": result.returncode,
        "output_dir": str(result.output_dir),
    }
    if result.returncode != 0 and result.stderr.strip():
        mapping["stderr"] = _stderr_tail(result.stderr)
    return mapping


def _error_mapping(record: Any, script: str, error: str) -> dict[str, object]:
    return {
        "format": "httk-workflow-postprocess",
        "format_version": 2,
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
    output_root = postprocess_root(workspace, arguments.output_dir)
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
            result = run_postprocess_script(
                selected, arguments.script, record, output_root=output_root, timeout=arguments.timeout
            )
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
            line = f"{record.job_key}\t{result.script}\t{result.returncode}\t{result.output_dir}"
            if result.returncode != 0:
                tail = _stderr_last_line(result.stderr)
                if tail:
                    line = f"{line}\t{tail}"
            print(line)
    return 1 if failed else 0


def build_postprocess_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = _leaf(
        subparsers,
        "postprocess",
        summary="run a curated workflow postprocess script",
        description="Run a manifest-declared postprocess script against collected jobs",
        handler=handle_postprocess,
    )
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace to postprocess (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
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
        "--output-dir",
        metavar="DIR",
        help="output root for this run (default: the postprocess.directory setting, or <workspace>/postprocess); "
        "a relative path resolves against the workspace root",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_POSTPROCESS_TIMEOUT,
        metavar="SECONDS",
        help=f"script timeout (default: {DEFAULT_POSTPROCESS_TIMEOUT:g})",
    )
    parser.add_argument("--json", action="store_true", help="emit one JSON object per job")


__all__ = ["build_postprocess_parser", "handle_postprocess"]
