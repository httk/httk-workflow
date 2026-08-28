"""The read-only workflow precheck command."""

import argparse
import json
from collections.abc import Mapping

from httk.core.cli import CLIContext

from ..precheck import (
    DEFAULT_PRECHECK_STATES,
    ENVIRONMENT_VARIABLE_CAVEAT,
    has_claim_problem,
    has_environment_problem,
    has_input_problem,
    has_language_problem,
    has_runner_problem,
    has_step_problem,
    manager_availability_notice,
    precheck_jobs,
)
from ..workspace import Workspace
from ._common import _leaf, _local_root


def _summary(findings: list[dict[str, object]]) -> dict[str, int]:
    """Count the readiness problems in findings."""

    def status_of(item: Mapping[str, object], member: str) -> str | None:
        value = item.get(member)
        return value.get("status") if isinstance(value, Mapping) else None

    return {
        "checked": len(findings),
        "unresolved": sum(has_environment_problem(item) for item in findings),
        "runner_problems": sum(has_runner_problem(item) for item in findings),
        "runner_indeterminate": sum(status_of(item, "runner") == "indeterminate" for item in findings),
        "claim_problems": sum(has_claim_problem(item) for item in findings),
        "language_problems": sum(has_language_problem(item) for item in findings),
        "language_indeterminate": sum(status_of(item, "language") == "indeterminate" for item in findings),
        "input_problems": sum(has_input_problem(item) for item in findings),
        "step_problems": sum(has_step_problem(item) for item in findings),
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
    notice = manager_availability_notice(workspace)
    if arguments.json:
        print(
            json.dumps(
                {
                    "format": "httk-workflow-precheck",
                    "format_version": 2,
                    "environment_variable_caveat": ENVIRONMENT_VARIABLE_CAVEAT,
                    "states": list(DEFAULT_PRECHECK_STATES),
                    "jobs": findings,
                    "summary": summary,
                    "manager_notice": notice,
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(f"environment caveat: {ENVIRONMENT_VARIABLE_CAVEAT}")
        if notice is not None:
            print(f"notice: {notice}")
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
            claim = finding.get("claim")
            if isinstance(claim, Mapping) and claim.get("status") == "problem":
                problems.append(f"claim: {claim.get('problem')}")
            language = finding.get("language")
            if isinstance(language, Mapping) and language.get("status") == "problem":
                problems.append(f"language: {language.get('problem')}")
            elif isinstance(language, Mapping) and language.get("status") == "indeterminate":
                problems.append(f"language indeterminate: {language.get('problem')}")
            inputs = finding.get("inputs")
            if isinstance(inputs, list):
                problems.extend(f"input: {problem}" for problem in inputs)
            step_problem = finding.get("step")
            if isinstance(step_problem, str) and step_problem:
                problems.append(f"step: {step_problem}")
            suffix = f"; problems: {'; '.join(problems)}" if problems else "; ok"
            print(f"{finding['job_key']}\t{finding['workflow'] or '-'}\tenv: {statuses}{suffix}")
        print(
            f"checked {summary['checked']}, unresolved {summary['unresolved']}, "
            f"runner problems {summary['runner_problems']}, unclaimable {summary['claim_problems']}, "
            f"language problems {summary['language_problems']}, input problems {summary['input_problems']}, "
            f"step problems {summary['step_problems']}"
        )
    return (
        1
        if summary["unresolved"]
        or summary["runner_problems"]
        or summary["claim_problems"]
        or summary["language_problems"]
        or summary["input_problems"]
        or summary["step_problems"]
        else 0
    )


def build_precheck_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Declare the ``precheck`` read-only command."""

    parser = _leaf(
        subparsers,
        "precheck",
        summary="check pending jobs before they start",
        description="Report declared-environment resolution and runner-reference readiness without changing the workspace",
        handler=handle_precheck,
    )
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace to precheck (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
    parser.add_argument("--placement", metavar="PLACEMENT", help="check only jobs at or below this placement")
    parser.add_argument(
        "--runner-search-path",
        action="append",
        default=[],
        metavar="DIRECTORY",
        help="root for plain installed runner references (repeatable)",
    )
    parser.add_argument("--json", action="store_true", help="print the complete report as JSON")
