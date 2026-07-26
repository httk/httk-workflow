"""The Bash authoring SDK: dispatch, exit codes, and the persisted draft.

The parity of the Bash and Python SDKs is asserted end to end in
:mod:`tests.test_parity`. What is tested here is what only the Bash half can get
wrong: describing itself without an interpreter, dispatching into ``step_``
functions, reporting an aborted handler, keeping one outcome draft alive across
many short-lived bridge processes, and separating an absent answer from a refused
call in its exit status.
"""

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from httk.workflow import (
    JobSpec,
    ReplayableWorkdirBatch,
    TaskManager,
    WorkflowWorkspace,
    prepare_job_payload,
)

_SHELL = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "shell" / "httk-workflow.sh"


@dataclass(frozen=True)
class _Fixture:
    """One fabricated attempt a Bash runner can be dispatched into."""

    root: Path
    payload: Path
    control: Path
    workdir: Path
    environment: dict[str, str]

    def run(self, source: str, *arguments: str, name: str = "runner.sh") -> "subprocess.CompletedProcess[str]":
        runner = self.root / name
        runner.write_text(source, encoding="utf-8")
        return subprocess.run(
            ["bash", str(runner), *arguments],
            cwd=self.workdir,
            env=self.environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def outcome(self) -> dict[str, Any]:
        return json.loads((self.control / "outcome.ready" / "outcome.json").read_text(encoding="utf-8"))

    def drafts(self) -> list[Path]:
        return sorted(self.control.glob("outcome.tmp.*"))

    def breadcrumb(self) -> dict[str, Any]:
        return json.loads((self.control / "error.json").read_text(encoding="utf-8"))


def _child(label: str, kind: str, *, failure: dict[str, object] | None = None) -> dict[str, object]:
    """One join observation exactly as the manager writes it into the context."""

    job_key = f"{label}--{uuid.uuid4()}"
    return {
        "label": label,
        "job_id": str(uuid.uuid4()),
        "job_key": job_key,
        "kind": kind,
        "failure": failure,
        "placement": "project/children",
        "payload_path": f"project/children/{job_key}",
        "workdir_path": f"project/children/{job_key}/run",
        "data_generation": None,
    }


def _fixture(
    tmp_path: Path,
    *,
    step: str,
    inputs: dict[str, object] | None = None,
    children: list[dict[str, object]] | None = None,
    data_generation: int | None = None,
    name: str = "attempt",
) -> _Fixture:
    """Fabricate one attempt of one job, without a manager."""

    root = tmp_path / name
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    (files / "runner").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    prepare_job_payload(
        payload,
        JobSpec(
            name="Fabricated",
            workflow="tests.bash",
            runner_path="files/runner",
            initial_step=step,
            data_mode="none" if data_generation is None else "transactional",
            inputs=inputs or {},
        ),
    )
    control = payload / f".httk-attempt.{uuid.uuid4()}"
    control.mkdir()
    workdir = payload / "run"
    workdir.mkdir()
    (control / "context.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-attempt-context",
                "format_version": 1,
                "workspace_id": str(uuid.uuid4()),
                "job_id": str(uuid.uuid4()),
                "job_key": f"fabricated--{uuid.uuid4()}",
                "placement": "project/fabricated",
                "step": step,
                "activation_id": str(uuid.uuid4()),
                "attempt_id": str(uuid.uuid4()),
                "data_generation": data_generation,
                "children": children or [],
            }
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        {
            "HTTK_WORKFLOW_CONTEXT": str(control / "context.json"),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(payload),
            "HTTK_WORKFLOW_WORKDIR": str(workdir),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
            "HTTK_WORKFLOW_STEP": step,
            "HTTK_WORKFLOW_PYTHON": sys.executable,
            "HTTK_WORKFLOW_BASH_API": str(_SHELL),
        }
    )
    if data_generation is not None:
        environment["HTTK_WORKFLOW_DATA_DIR"] = str(payload / "data")
    for name_to_drop in ("HTTK_WORKFLOW_DESCRIBE", "HTTK_WORKFLOW_RUNNER_WORKFLOW", "HTTK_WORKFLOW_RUNNER_STEPS"):
        environment.pop(name_to_drop, None)
    return _Fixture(root, payload, control, workdir, environment)


def _runner(*steps: str, body: str = "", workflow: str = "tests.bash", main: str = "httk_workflow_main") -> str:
    """Assemble one Bash runner around the given step implementations."""

    return f"""#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner {workflow} {" ".join(steps)}
{body}
{main}
"""


def test_the_library_is_safe_with_set_u_and_reports_its_version() -> None:
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail; source "$1"; test "$HTTK_WORKFLOW_BASH_API_VERSION" = 2',
            "bash",
            str(_SHELL),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_describe_mode_prints_the_step_set_before_any_step_runs(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="collect")
    fixture.environment["HTTK_WORKFLOW_DESCRIBE"] = "1"
    source = _runner(
        "collect",
        "prepare",
        body='step_collect() { touch ran.txt; }\nstep_prepare() { touch ran.txt; }',
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == {
        "format": "httk-workflow-runner-description",
        "format_version": 1,
        "workflow": "tests.bash",
        "steps": ["collect", "prepare"],
    }
    # Describing a runner runs no step and publishes nothing, so the attempt is
    # untouched even though its context was right there in the environment.
    assert not (fixture.workdir / "ran.txt").exists()
    assert not (fixture.control / "outcome.ready").exists()


def test_an_unknown_step_names_the_registered_steps(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="realx")
    source = _runner("relax", "collect", body="step_relax() { httk_workflow_succeed; }\nstep_collect() { :; }")

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    outcome = fixture.outcome()
    assert outcome["action"] == "fail"
    assert outcome["failure"] == {
        "code": "unknown_step",
        "message": "step 'realx' is not implemented by the tests.bash runner; registered steps: collect, relax",
    }
    assert outcome["runner_steps"] == ["collect", "relax"]


def test_a_step_that_publishes_nothing_is_reported_as_no_outcome(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="silent")
    completed = fixture.run(_runner("silent", body="step_silent() { httk_workflow_log info nothing; }"))

    assert completed.returncode == 0, completed.stderr
    assert fixture.outcome()["failure"] == {
        "code": "no_outcome",
        "message": "step 'silent' finished without publishing an outcome",
    }


def test_registration_must_match_the_step_functions(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="relax")
    missing = fixture.run(_runner("relax", "collect", body="step_relax() { httk_workflow_succeed; }"))
    assert missing.returncode == 2
    assert "declares step collect but defines no step_collect function" in missing.stderr

    undeclared = fixture.run(
        _runner("relax", body="step_relax() { httk_workflow_succeed; }\nstep_collect() { :; }"),
        name="undeclared.sh",
    )
    assert undeclared.returncode == 2
    assert "defines step_collect but does not declare step collect" in undeclared.stderr

    duplicate = fixture.run(_runner("relax", "relax", body="step_relax() { :; }"), name="duplicate.sh")
    assert duplicate.returncode == 2
    assert "step relax is already registered on the tests.bash runner" in duplicate.stderr
    assert not (fixture.control / "outcome.ready").exists()


def test_an_aborted_handler_leaves_a_breadcrumb_and_no_draft(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="explode", data_generation=0)
    (fixture.workdir / "energy.json").write_text("{}\n", encoding="utf-8")
    source = _runner(
        "explode",
        body="""step_explode() {
    httk_workflow_put energy.json results/energy.json >/dev/null
    grep missing-marker energy.json
    touch kept-going.txt
    httk_workflow_succeed
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 1
    # `set -e` really aborts the handler where it failed: the step never reached
    # the publication it was about to perform.
    assert not (fixture.workdir / "kept-going.txt").exists()
    assert not (fixture.control / "outcome.ready").exists()
    # The draft the handler had already staged is discarded, so no half-outcome
    # survives the abort.
    assert fixture.drafts() == []
    breadcrumb = fixture.breadcrumb()
    assert breadcrumb["format"] == "httk-workflow-runner-error"
    assert breadcrumb["step"] == "explode" and breadcrumb["exception"] == "ShellError"
    assert breadcrumb["message"] == "step_explode exited with status 1"
    assert "grep missing-marker energy.json" in breadcrumb["traceback"]


def test_a_second_terminal_call_is_refused_and_keeps_the_first(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="double")
    source = _runner(
        "double",
        body="step_double() {\n    httk_workflow_succeed\n    httk_workflow_advance double\n}",
    )

    completed = fixture.run(source)
    assert completed.returncode == 2
    assert "already published its succeed outcome" in completed.stderr
    assert fixture.outcome()["action"] == "succeed"
    assert fixture.drafts() == []
    assert fixture.breadcrumb()["step"] == "double"


def test_exit_codes_separate_an_absent_answer_from_a_refused_call(tmp_path: Path) -> None:
    fixture = _fixture(
        tmp_path,
        step="only",
        inputs={"encut": 520},
        children=[_child("site-0", "succeeded")],
    )
    source = _runner(
        "only",
        body="""record() {
    local name=$1
    shift
    local code=0
    "$@" >/dev/null 2>&1 || code=$?
    printf '%s=%s\\n' "$name" "$code"
}
step_only() { :; }
httk_workflow_state_set answer 42
record present_state httk_workflow_state_get answer
record absent_state httk_workflow_state_get missing
record present_input httk_workflow_input encut
record absent_input httk_workflow_input nothing
record defaulted_input httk_workflow_input nothing fallback
record present_child httk_workflow_child site-0 state
record absent_child_field httk_workflow_child site-0 failure_code
record absent_child httk_workflow_child site-9 state
record refused_field httk_workflow_child site-0 nonsense
record refused_assignment httk_workflow_state_merge nonsense""",
        main="",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    assert dict(line.split("=") for line in completed.stdout.splitlines()) == {
        "present_state": "0",
        "absent_state": "1",
        "present_input": "0",
        "absent_input": "1",
        "defaulted_input": "0",
        "present_child": "0",
        "absent_child_field": "1",
        "absent_child": "1",
        "refused_field": "2",
        "refused_assignment": "2",
    }

    # A refused read says why on stderr; an absent one is silent when there is
    # nothing to say beyond its status.
    diagnosed = fixture.run(
        _runner("only", body="step_only() { :; }\nhttk_workflow_input nothing", main=""),
        name="diagnosed.sh",
    )
    assert diagnosed.returncode == 1
    assert "job input 'nothing' is not defined; defined inputs: encut" in diagnosed.stderr


def test_a_corrupt_attempt_context_is_refused_with_two(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="only")
    Path(fixture.environment["HTTK_WORKFLOW_CONTEXT"]).write_text("{}\n", encoding="utf-8")
    completed = fixture.run(_runner("only", body="step_only() { httk_workflow_succeed; }"))
    assert completed.returncode == 2
    assert "httk-workflow-attempt-context" in completed.stderr

    del fixture.environment["HTTK_WORKFLOW_WORKDIR"]
    missing = fixture.run(_runner("only", body="step_only() { :; }", main=""), name="missing.sh")
    assert missing.returncode == 0


def test_spawn_reads_json_input_values_from_files(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="branch")
    (fixture.workdir / "defect-0.json").write_text(
        json.dumps({"species": ["Si", "O"], "site": [0.5, 0.5, 0.0]}), encoding="utf-8"
    )
    source = _runner(
        "branch",
        "relax",
        body=f"""step_branch() {{
    httk_workflow_spawn site-0 \\
        --step relax \\
        --input structure=@defect-0.json \\
        --input supercell=2 \\
        --input label=alpha \\
        --runner ws:parity/run.sh@{"a" * 64} \\
        --tag defect \\
        --priority 700
    httk_workflow_gather relax --when any_succeeded
}}
step_relax() {{ httk_workflow_succeed; }}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    ready = fixture.control / "outcome.ready"
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [entry["label"] for entry in spawn["children"]] == ["site-0"]
    job_key = spawn["children"][0]["job_key"]
    # The spawn printed the child's job key, which is how a Bash step names the
    # payload it just registered.
    assert completed.stdout.split("\n")[0] == job_key
    child = json.loads((ready / "children" / "jobs" / job_key / "job.json").read_text(encoding="utf-8"))
    assert child["inputs"] == {
        "structure": {"species": ["Si", "O"], "site": [0.5, 0.5, 0.0]},
        "supercell": 2,
        "label": "alpha",
    }
    assert child["runner"] == {
        "backend": "path",
        "source": "workspace",
        "path": "parity/run.sh",
        "arguments": [],
        "sha256": "a" * 64,
    }
    assert child["tag"] == "defect" and child["priority"] == 700
    assert fixture.outcome()["join"]["condition"] == "any_succeeded"


def test_a_step_prepares_a_payload_and_spawns_the_directory(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="branch")
    (fixture.workdir / "spec.json").write_text(
        json.dumps(
            {
                "name": "Prepared child",
                "workflow": "tests.bash",
                "runner_backend": "path",
                "runner_source": "workspace",
                "runner_path": "parity/run.sh",
                "runner_sha256": "b" * 64,
                "initial_step": "branch",
                "tag": "prepared",
                "inputs": {"encut": 520},
            }
        ),
        encoding="utf-8",
    )
    source = _runner(
        "branch",
        body="""step_branch() {
    mkdir -p child
    httk_workflow_job_prepare child spec.json
    httk_workflow_spawn prepared --payload child >/dev/null
    httk_workflow_gather branch
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    prepared = json.loads(completed.stdout)
    assert prepared["runner_source"] == "workspace"
    assert prepared["runner_sha256"] == "b" * 64
    assert prepared["runner_backend"] == "path"
    assert prepared["inputs"] == {"encut": 520}
    ready = fixture.control / "outcome.ready"
    spawn = json.loads((ready / "children" / "spawn.json").read_text(encoding="utf-8"))
    assert [entry["label"] for entry in spawn["children"]] == ["prepared"]
    child = json.loads((ready / "children" / "jobs" / prepared["job_key"] / "job.json").read_text(encoding="utf-8"))
    assert child["id"] == prepared["id"] and child["inputs"] == {"encut": 520}


def test_a_step_name_that_is_not_registered_is_refused_at_the_call(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="relax")
    completed = fixture.run(
        _runner(
            "relax",
            "collect",
            body="step_relax() { httk_workflow_advance colect; }\nstep_collect() { :; }",
        )
    )
    assert completed.returncode == 2
    assert "registered steps: collect, relax" in completed.stderr
    assert not (fixture.control / "outcome.ready").exists()

    # Every call that names a step is checked, not only advance.
    refusals = fixture.run(
        _runner(
            "relax",
            "collect",
            body="""record() {
    local name=$1
    shift
    local code=0
    "$@" >/dev/null 2>>refused.txt || code=$?
    printf '%s=%s\\n' "$name" "$code"
}
step_relax() { :; }
step_collect() { :; }
record advance httk_workflow_advance colect
record gather httk_workflow_gather colect2
record on_impossible httk_workflow_gather collect --on-impossible colect3
record child_step httk_workflow_spawn one --step relx
record payload_runner httk_workflow_spawn two --step relax""",
            main="",
        ),
        name="refusals.sh",
    )
    assert completed.returncode == 2
    assert dict(line.split("=") for line in refusals.stdout.splitlines()) == {
        "advance": "2",
        "gather": "2",
        "on_impossible": "2",
        "child_step": "2",
        "payload_runner": "2",
    }
    refused = (fixture.workdir / "refused.txt").read_text(encoding="utf-8")
    for name in ("advance target 'colect'", "gather target 'colect2'", "spawned child step 'relx'"):
        assert name in refused
    # A synthesized child cannot inherit a runner that lives inside the payload,
    # and the refusal says exactly what to do instead.
    assert "publish it with WorkflowWorkspace.publish_runner" in refused
    assert not (fixture.control / "outcome.ready").exists()


def test_main_replays_a_workdir_batch_an_earlier_attempt_sealed(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="resume")
    source = fixture.root / "new.txt"
    source.write_text("new\n", encoding="utf-8")
    batch = ReplayableWorkdirBatch.create(fixture.workdir)
    batch.transaction.put_file("value", source, "results/value.txt")
    batch.seal()

    completed = fixture.run(
        _runner("resume", body="step_resume() {\n    test -f results/value.txt\n    httk_workflow_succeed\n}")
    )
    # Dispatch completes what an interrupted attempt sealed but never applied, so
    # the handler always starts from a workdir whose sealed changes are complete.
    assert completed.returncode == 0, completed.stderr
    assert (fixture.workdir / "results" / "value.txt").read_text(encoding="utf-8") == "new\n"
    assert fixture.outcome()["action"] == "succeed"


def test_children_are_reported_as_one_tab_separated_row_each(tmp_path: Path) -> None:
    children = [
        _child("site-0", "succeeded"),
        _child("site-1", "failed", failure={"code": "relax.diverged", "message": "site 1 did not relax"}),
        _child("site-2", "succeeded"),
    ]
    fixture = _fixture(tmp_path, step="aggregate", children=children)
    source = _runner(
        "aggregate",
        body="""step_aggregate() {
    printf 'all\\n'
    httk_workflow_children
    printf 'succeeded\\n'
    httk_workflow_children --succeeded
    printf 'failed\\n'
    httk_workflow_children --failed
    printf 'fields\\n'
    httk_workflow_child site-1 failure_code
    httk_workflow_child site-1 failure_message
    httk_workflow_child site-1 job_key
    httk_workflow_child site-0 payload
    httk_workflow_succeed
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    lines = completed.stdout.splitlines()
    assert lines[0] == "all" and lines[4] == "succeeded" and lines[7] == "failed"
    workspace = Path(fixture.environment["HTTK_WORKFLOW_WORKSPACE_DIR"])
    expected = [
        "\t".join(
            (
                str(child["label"]),
                str(child["kind"]),
                str(child["job_key"]),
                str(workspace / str(child["workdir_path"])),
                "",
            )
        )
        for child in children
    ]
    assert lines[1:4] == expected
    assert lines[5:7] == [expected[0], expected[2]]
    assert lines[8] == expected[1]
    assert lines[9:] == [
        "fields",
        "relax.diverged",
        "site 1 did not relax",
        str(children[1]["job_key"]),
        str(workspace / str(children[0]["payload_path"])),
    ]


def test_data_operation_identifiers_continue_across_bridge_processes(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="collect", data_generation=0)
    (fixture.workdir / "energy.json").write_text("{}\n", encoding="utf-8")
    (fixture.workdir / "bundle").mkdir()
    (fixture.workdir / "bundle" / "log.txt").write_text("done\n", encoding="utf-8")
    source = _runner(
        "collect",
        body="""step_collect() {
    httk_workflow_put energy.json results/energy.json
    httk_workflow_put bundle results/bundle
    httk_workflow_remove scratch --missing-ok
    httk_workflow_succeed
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    # Each call was its own interpreter, and the draft on disk was the counter.
    assert completed.stdout.splitlines() == ["op-0001", "op-0002", "op-0003"]
    manifest = json.loads(
        (fixture.control / "outcome.ready" / "transaction" / "manifest.json").read_text(encoding="utf-8")
    )
    assert [item["id"] for item in manifest["operations"]] == ["op-0001", "op-0002", "op-0003"]
    assert [item["op"] for item in manifest["operations"]] == ["put-file", "put-tree", "remove"]
    assert manifest["expected_data_generation"] == 0
    assert fixture.outcome()["expected_data_generation"] == 0


def test_data_operations_refuse_a_job_without_transactional_data(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="collect")
    completed = fixture.run(_runner("collect", body="step_collect() { httk_workflow_remove results; }"))
    assert completed.returncode == 2
    assert "data.mode none" in completed.stderr
    assert fixture.drafts() == []


def test_the_batch_subcommand_runs_many_commands_in_one_interpreter(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="collect", data_generation=0)
    (fixture.workdir / "energy.json").write_text("{}\n", encoding="utf-8")
    source = _runner(
        "collect",
        body="""step_collect() {
    httk_workflow_batch <<'EOF'
# one interpreter start for the whole group
state-set converged true
state-set energy -12.5
put energy.json results/energy.json
remove scratch --missing-ok
EOF
    httk_workflow_state_get energy
    httk_workflow_succeed
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == ["op-0001", "op-0002", "-12.5"]
    state = json.loads((fixture.payload / ".httk-job" / "state.json").read_text(encoding="utf-8"))
    assert state == {"converged": True, "energy": -12.5}
    manifest = json.loads(
        (fixture.control / "outcome.ready" / "transaction" / "manifest.json").read_text(encoding="utf-8")
    )
    assert [item["id"] for item in manifest["operations"]] == ["op-0001", "op-0002"]


def test_a_failing_batch_line_stops_the_batch_and_names_it(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path, step="collect")
    source = _runner(
        "collect",
        body="""step_collect() {
    local code=0
    httk_workflow_batch <<'EOF' || code=$?
state-set alpha 1
state-get missing
state-set beta 2
EOF
    printf 'code=%s\\n' "$code"
    httk_workflow_succeed
}""",
    )

    completed = fixture.run(source)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "code=1"
    assert "batch line 2 failed: state-get missing" in completed.stderr
    state = json.loads((fixture.payload / ".httk-job" / "state.json").read_text(encoding="utf-8"))
    assert state == {"alpha": 1}


_MANAGED_RUNNER = """#!/usr/bin/env bash
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_runner tests.bash.managed start collect

step_start() {
    printf '%s\\n' 'literal; $(touch unsafe)' >result.txt
    httk_workflow_put result.txt results/result.txt >/dev/null
    httk_workflow_state_set answer 42
    httk_workflow_advance collect --state stage=committed --priority 700
}

step_collect() {
    test "$(cat "$HTTK_WORKFLOW_DATA_DIR/results/result.txt")" = 'literal; $(touch unsafe)'
    test "$(httk_workflow_state_get answer)" = 42
    test "$(httk_workflow_state_get stage)" = committed
    test "$(httk_workflow_context step)" = collect
    httk_workflow_runlog_note 'the data of the previous step is committed'
    httk_workflow_succeed
}

httk_workflow_main
"""


def test_a_payload_bash_runner_advances_commits_data_and_succeeds(tmp_path: Path) -> None:
    workspace = WorkflowWorkspace.initialize(tmp_path / "workspace", extensions=["transactional-data-v1"])
    payload = tmp_path / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_MANAGED_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Managed bash runner",
            workflow="tests.bash.managed",
            runner_path="files/runner",
            tag="bash",
            initial_step="start",
            data_mode="transactional",
        ),
    )
    workspace.submit(payload, "bash/jobs")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    marker = workspace.find_marker_by_id(job.id)
    assert marker is not None and marker.kind == "succeeded"
    root = workspace.payload_path(marker.placement, marker.job_key)
    # The value passed through a Bash quoting hazard and a real transaction
    # without ever being evaluated by a shell.
    assert (root / "data" / "results" / "result.txt").read_text(encoding="utf-8") == "literal; $(touch unsafe)\n"
    assert not (root / "run" / "unsafe").exists()
    assert json.loads((root / ".httk-job" / "state.json").read_text(encoding="utf-8")) == {
        "answer": 42,
        "stage": "committed",
    }
    state = workspace.read_state(marker)
    assert state["runner_steps"] == ["collect", "start"]
    assert marker.priority == 700
    runlog = (root / "run" / ".httk-runner" / "runlog.jsonl").read_text(encoding="utf-8")
    assert "the data of the previous step is committed" in runlog
