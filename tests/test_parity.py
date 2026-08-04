"""One campaign, two languages: the Bash and Python SDKs publish the same bytes.

The same defect campaign is authored twice — once as a Python ``Runner`` file,
once as a Bash ``httk_workflow_runner`` script — published as a shared workspace
runner, and run through the real manager. Everything the two runs leave behind is
then compared: the published outcomes, the staged data transactions, the
synthesized child jobs, the final states, the job state, and the files in every
workdir and data directory. Only the identifiers and paths that must differ
between two independent workspaces are normalized away.
"""

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from conftest import TestProfile as _TestProfile
from httk.workflow import TaskManager, Workspace
from httk.workflow.protocol import JobSpec, prepare_job_payload

_PYTHON_RUNNER = '''#!/usr/bin/env python3
"""Defect campaign: characterize, relax every site, aggregate, triage."""

from httk.workflow import ChildSpec, Runner

run = Runner("tests.parity")


@run.step
def characterize(a):
    sites = a.input("sites")
    failing = str(a.input("failing", "")).split(",")
    a.state["sites"] = sites
    for site in range(sites):
        a.spawn(
            ChildSpec(
                step="relax",
                inputs={"site": site, "diverge": str(site) in failing},
                data_mode="transactional",
                maximum_attempts_per_activation=1,
            ),
            label="site-%d" % site,
            placement="project/children",
        )
    a.gather("aggregate", when="all_terminal", on_impossible="triage")


@run.step
def relax(a):
    site = a.input("site")
    (a.workdir / "site.txt").write_text("%s\\n" % site, encoding="utf-8")
    if a.input("diverge"):
        a.fail("relax.diverged", "site %s did not relax" % site)
    else:
        a.put(a.workdir / "site.txt", "results/site.txt")
        a.succeed()


@run.step
def aggregate(a):
    rows = [
        "%s\\t%s\\n" % (child.label, (child.workdir / "site.txt").read_text(encoding="utf-8").strip())
        for child in a.children.succeeded
    ]
    (a.workdir / "report.tsv").write_text("".join(rows), encoding="utf-8")
    failed = ",".join(child.label for child in a.children.failed)
    if failed:
        a.advance("triage", state={"failed": failed})
    else:
        a.succeed()


@run.step
def triage(a):
    failed = str(a.state.get("failed", ""))
    (a.workdir / "triage.txt").write_text("%s\\n" % failed, encoding="utf-8")
    a.fail("defects.child_failed", "failed: %s" % failed)


if __name__ == "__main__":
    raise SystemExit(run.main())
'''

_BASH_RUNNER = """#!/usr/bin/env bash
# Defect campaign: characterize, relax every site, aggregate, triage.
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner tests.parity characterize relax aggregate triage

step_characterize() {
    local sites failing site diverge
    sites=$(httk_workflow_input sites)
    failing=$(httk_workflow_input failing '')
    httk_workflow_state_set sites "$sites"
    site=0
    while [ "$site" -lt "$sites" ]; do
        diverge=false
        case ",$failing," in
            *",$site,"*) diverge=true ;;
        esac
        httk_workflow_spawn "site-$site" \\
            --step relax \\
            --input site="$site" \\
            --input diverge="$diverge" \\
            --data-mode transactional \\
            --max-attempts-per-activation 1 \\
            --placement project/children >/dev/null
        site=$((site + 1))
    done
    httk_workflow_gather aggregate --when all_terminal --on-impossible triage
}

step_relax() {
    local site
    site=$(httk_workflow_input site)
    printf '%s\\n' "$site" >site.txt
    if [ "$(httk_workflow_input diverge)" = true ]; then
        httk_workflow_fail relax.diverged "site $site did not relax"
    else
        httk_workflow_put site.txt results/site.txt >/dev/null
        httk_workflow_succeed
    fi
}

step_aggregate() {
    local label state key workdir data failed=
    : >report.tsv
    while IFS=$'\\t' read -r label state key workdir data; do
        printf '%s\\t%s\\n' "$label" "$(cat "$workdir/site.txt")" >>report.tsv
    done < <(httk_workflow_children --succeeded)
    while IFS=$'\\t' read -r label state key workdir data; do
        if [ -n "$failed" ]; then
            failed="$failed,$label"
        else
            failed=$label
        fi
    done < <(httk_workflow_children --failed)
    if [ -n "$failed" ]; then
        httk_workflow_advance triage --state failed="$failed"
    else
        httk_workflow_succeed
    fi
}

step_triage() {
    local failed
    failed=$(httk_workflow_state_get failed || true)
    printf '%s\\n' "$failed" >triage.txt
    httk_workflow_fail defects.child_failed "failed: $failed"
}

httk_workflow_main
"""

_CAMPAIGN_INPUTS: dict[str, object] = {"sites": 3, "failing": "1"}
_CAMPAIGN_STEPS = ["aggregate", "characterize", "relax", "triage"]


def _tag(job_key: str) -> str:
    """Return the readable half of one job key, which is its tag."""

    return job_key.split("--")[0]


def _campaign(root: Path, source: str, name: str, *, sites: int) -> Workspace:
    """Run the whole campaign of one runner file in its own workspace."""

    root.mkdir(parents=True)
    runner = root / name
    runner.write_text(source, encoding="utf-8")
    runner.chmod(0o755)
    workspace = Workspace.initialize(root / "workspace")
    reference = workspace.publish_runner(runner, name=f"parity/{name}")
    payload = root / "parent"
    prepare_job_payload(
        payload,
        JobSpec(
            name="Defect campaign",
            workflow="tests.parity",
            runner_path=str(reference["path"]),
            runner_source="workspace",
            runner_sha256=str(reference["sha256"]),
            tag="campaign",
            initial_step="characterize",
            maximum_attempts_per_activation=1,
            inputs={**_CAMPAIGN_INPUTS, "sites": sites},
        ),
    )
    workspace.submit(payload, "project/campaign")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=180.0)
    return workspace


def _normalized_outcome(body: Mapping[str, Any]) -> dict[str, Any]:
    """Drop the identifiers two independent runs cannot share."""

    result = {key: value for key, value in body.items() if key not in {"job_id", "activation_id", "attempt_id"}}
    join = result.get("join")
    if isinstance(join, Mapping):
        children = join.get("children")
        replaced = dict(join)
        if isinstance(children, list):
            replaced["children"] = [
                {"tag": _tag(str(child.get("job_key"))), "placement_hint": child.get("placement_hint")}
                for child in children
                if isinstance(child, Mapping)
            ]
        result["join"] = replaced
    return result


def _outcomes(workspace: Workspace) -> dict[str, list[dict[str, Any]]]:
    """Every published outcome of every job, in publication order per job."""

    collected: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for path in workspace.root.glob("**/.httk-attempt.*/outcome.ready/outcome.json"):
        body = json.loads(path.read_text(encoding="utf-8"))
        collected.setdefault(_tag(path.parents[2].name), []).append(
            (path.stat().st_mtime_ns, _normalized_outcome(body))
        )
    return {tag: [body for _, body in sorted(items)] for tag, items in collected.items()}


def _transactions(workspace: Workspace) -> dict[str, list[Any]]:
    """The operations of every published data transaction, keyed by job tag."""

    collected: dict[str, list[Any]] = {}
    for path in workspace.root.glob("**/.httk-attempt.*/outcome.ready/transaction/manifest.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        collected[_tag(path.parents[3].name)] = list(manifest["operations"])
    return collected


def _jobs(workspace: Workspace) -> dict[str, dict[str, Any]]:
    """Every job definition, minus the members that identify one workspace."""

    result: dict[str, dict[str, Any]] = {}
    for marker in workspace.scan_markers():
        raw = dict(workspace.load_job(marker).raw)
        for key in ("id", "parent", "runner"):
            raw.pop(key, None)
        result[_tag(marker.job_key)] = raw
    return result


def _states(workspace: Workspace) -> dict[str, dict[str, Any]]:
    """The terminal state frame of every job, keyed by job tag."""

    result: dict[str, dict[str, Any]] = {}
    for marker in workspace.scan_markers():
        state = workspace.read_state(marker)
        result[_tag(marker.job_key)] = {
            "kind": marker.kind,
            "reason": state.get("reason"),
            "failure": state.get("failure"),
            "runner_steps": state.get("runner_steps"),
            "data_generation": state.get("data_generation"),
        }
    return result


def _artifacts(workspace: Workspace) -> dict[str, str]:
    """The workdir files, data files, and job state of every job."""

    result: dict[str, str] = {}
    for marker in workspace.scan_markers():
        payload = workspace.payload_path(marker.placement, marker.job_key)
        tag = _tag(marker.job_key)
        for directory in ("run", "data", ".httk-job"):
            base = payload / directory
            for path in sorted(base.rglob("*")) if base.is_dir() else ():
                if path.is_file() and ".httk-runner" not in path.parts:
                    result[f"{tag}/{path.relative_to(payload).as_posix()}"] = path.read_text(encoding="utf-8")
    return result


def test_a_bash_campaign_and_a_python_campaign_publish_the_same_artifacts(
    tmp_path: Path, test_profile: _TestProfile
) -> None:
    sites = test_profile.scale(normal=2, extended=3)
    python = _campaign(tmp_path / "python", _PYTHON_RUNNER, "run.py", sites=sites)
    shell = _campaign(tmp_path / "bash", _BASH_RUNNER, "run.sh", sites=sites)

    # The campaign really ran: normal mode retains a success and a declared
    # failure, while extended mode also covers the third child.
    states = _states(python)
    assert sorted(states) == ["campaign", *(f"site-{site}" for site in range(sites))]
    assert states["campaign"]["kind"] == "failed"
    assert states["campaign"]["failure"] == {
        "code": "defects.child_failed",
        "message": "failed: site-1",
    }
    assert states["campaign"]["runner_steps"] == _CAMPAIGN_STEPS
    assert states["site-1"]["failure"]["code"] == "relax.diverged"
    assert [body["action"] for body in _outcomes(python)["campaign"]] == ["wait", "advance", "fail"]
    assert _artifacts(python)["campaign/run/report.tsv"] == "".join(
        f"site-{site}\t{site}\n" for site in range(sites) if site != 1
    )
    assert _artifacts(python)["site-0/data/results/site.txt"] == "0\n"
    assert [item["id"] for item in _transactions(python)["site-0"]] == ["op-0001"]

    # And the Bash runner published exactly the same thing.
    assert _states(shell) == states
    assert _jobs(shell) == _jobs(python)
    assert _outcomes(shell) == _outcomes(python)
    assert _transactions(shell) == _transactions(python)
    assert _artifacts(shell) == _artifacts(python)


def test_both_runners_describe_themselves_with_the_same_bytes(tmp_path: Path) -> None:
    root = tmp_path / "describe"
    root.mkdir()
    python_runner = root / "run.py"
    python_runner.write_text(_PYTHON_RUNNER, encoding="utf-8")
    bash_runner = root / "run.sh"
    bash_runner.write_text(_BASH_RUNNER, encoding="utf-8")
    environment = os.environ.copy()
    environment["HTTK_WORKFLOW_DESCRIBE"] = "1"
    environment["HTTK_WORKFLOW_BASH_API"] = str(
        Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"
    )
    # Describing a runner is a pure read of the program itself: neither language
    # needs an attempt context, a workspace, or even the manager's interpreter.
    for name in ("HTTK_WORKFLOW_CONTEXT", "HTTK_WORKFLOW_CONTROL_DIR", "HTTK_WORKFLOW_PYTHON"):
        environment.pop(name, None)
    described = [
        subprocess.run(command, cwd=root, env=environment, text=True, capture_output=True, check=False)
        for command in ([sys.executable, str(python_runner)], ["bash", str(bash_runner)])
    ]
    for completed in described:
        assert completed.returncode == 0, completed.stderr
    assert described[0].stdout == described[1].stdout
    assert json.loads(described[0].stdout) == {
        "format": "httk-workflow-runner-description",
        "format_version": 1,
        "workflow": "tests.parity",
        "steps": _CAMPAIGN_STEPS,
    }
    assert sorted(item.name for item in root.iterdir()) == ["run.py", "run.sh"]
