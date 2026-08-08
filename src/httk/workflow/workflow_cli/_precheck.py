"""The read-only workflow precheck command."""

import argparse
import json
from collections.abc import Mapping

from httk.core.cli import CLIContext

from ..precheck import (
    DEFAULT_PRECHECK_STATES,
    ENVIRONMENT_VARIABLE_CAVEAT,
    has_environment_problem,
    has_runner_problem,
    precheck_jobs,
)
from ..workspace import Workspace
from ._common import _leaf, _local_root, add_workspace_argument


def _summary(findings: list[dict[str, object]]) -> dict[str, int]:
    """Count the readiness problems in findings."""

    def runner_status(item: Mapping[str, object]) -> str | None:
        runner = item.get("runner")
        return runner.get("status") if isinstance(runner, Mapping) else None

    return {
        "checked": len(findings),
        "unresolved": sum(has_environment_problem(item) for item in findings),
        "runner_problems": sum(has_runner_problem(item) for item in findings),
        "runner_indeterminate": sum(runner_status(item) == "indeterminate" for item in findings),
    }


def handle_precheck(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Report pending-job environment and runner readiness without mutation."""

    workspace = Workspace(_local_root(arguments, context, action="run a precheck"), mutable=False)
    findings = list(
        precheck_jobs(
            workspace,
            placement=arguments.placement,
            runner_search_paths=arguments.runner_search_path,
        )
    )
    summary = _summary(findings)
    if arguments.json:
        print(
            json.dumps(
                {
                    "format": "httk-workflow-precheck",
                    "format_version": 1,
                    "environment_variable_caveat": ENVIRONMENT_VARIABLE_CAVEAT,
                    "states": list(DEFAULT_PRECHECK_STATES),
                    "jobs": findings,
                    "summary": summary,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"environment caveat: {ENVIRONMENT_VARIABLE_CAVEAT}")
        for finding in findings:
            environment = finding["environment"]
            assert isinstance(environment, list)
            statuses = (
                ", ".join(
                    f"{item['name']}={item['status']}({item.get('source') or 'none'})"
                    for item in environment
                    if isinstance(item, Mapping)
                )
                or "none"
            )
            problems: list[str] = []
            for item in environment:
                if isinstance(item, Mapping) and item.get("status") == "unresolved":
                    problems.append(f"environment {item['name']} unresolved")
            environment_problems = finding.get("environment_problems", [])
            if isinstance(environment_problems, list):
                for problem in environment_problems:
                    problems.append(str(problem))
            runner = finding["runner"]
            if isinstance(runner, Mapping) and runner.get("status") == "problem":
                problems.append(f"runner: {runner.get('problem')}")
            elif isinstance(runner, Mapping) and runner.get("status") == "indeterminate":
                problems.append(f"runner indeterminate: {runner.get('problem')}")
            suffix = f"; problems: {'; '.join(problems)}" if problems else "; ok"
            print(f"{finding['job_key']}\t{finding['workflow'] or '-'}\tenv: {statuses}{suffix}")
        print(
            f"checked {summary['checked']}, unresolved {summary['unresolved']}, "
            f"runner problems {summary['runner_problems']}"
        )
    return 1 if summary["unresolved"] or summary["runner_problems"] else 0


def build_precheck_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``precheck`` read-only command."""

    parser = _leaf(
        subparsers,
        "precheck",
        summary="check pending jobs before they start",
        description="Report declared-environment resolution and runner-reference readiness without changing the workspace",
        handler=handle_precheck,
    )
    add_workspace_argument(parser, help_text="the workspace to precheck")
    parser.add_argument("--placement", metavar="PLACEMENT", help="check only jobs at or below this placement")
    parser.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="root for plain installed runner references (repeatable)",
    )
    parser.add_argument("--json", action="store_true", help="print the complete report as JSON")
