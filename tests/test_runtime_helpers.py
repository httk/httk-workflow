import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from httk.workflow import Attempt
from httk.workflow.compat.v1 import bundled_v1_root
from httk.workflow.runtime import run_command


def _context(path: Path) -> dict[str, object]:
    return {
        "format": "httk-workflow-attempt-context",
        "durable": False,
        "deadline": None,
        "settings": {},
        "format_version": 2,
        "workspace_id": "workspace",
        "job_id": "job",
        "job_key": "job-key",
        "placement": "jobs",
        "payload": str(path.parent.parent / "job"),
        "step": "relax",
        "activation_id": "activation",
        "attempt_id": "attempt",
        "is_restart": True,
        "is_unclean_restart": False,
        "data_generation": None,
    }


def test_an_attempt_reads_its_context_and_publishes_one_outcome(tmp_path: Path) -> None:
    control = tmp_path / "control"
    control.mkdir()
    context = _context(control)
    environment = {
        "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
        "HTTK_WORKFLOW_CONTROL_DIR": str(control),
        "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
        "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
        "HTTK_WORKFLOW_WORKSPACE_DIR": str(tmp_path / "workspace"),
    }
    attempt = Attempt.initialize(environment)
    assert attempt.context.step == "relax"
    assert attempt.step == "relax"
    assert attempt.context.is_restart
    assert attempt.children.all == ()
    ready = attempt.advance("collect", priority=700)
    outcome = json.loads((ready / "outcome.json").read_text(encoding="utf-8"))
    assert outcome["action"] == "advance"
    assert outcome["next_step"] == "collect"
    assert outcome["priority"] == 700
    assert not list(control.glob("outcome.tmp.*"))
    assert attempt.published
    with pytest.raises(RuntimeError, match="already published"):
        attempt.succeed()


@pytest.mark.timing
def test_run_command_uses_argv_and_times_out_process_group(tmp_path: Path) -> None:
    sentinel = tmp_path / "not-created"
    literal = f"value;touch {sentinel}"
    result = run_command([sys.executable, "-c", "import sys; print(sys.argv[1])", literal])
    assert result.returncode == 0
    assert result.stdout.decode().strip() == literal
    assert not sentinel.exists()
    started = time.monotonic()
    timed = run_command([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.05, termination_grace=0.1)
    assert timed.timed_out
    assert time.monotonic() - started < 2


def test_exact_v1_shell_names_redirect_and_keep_functions(tmp_path: Path) -> None:
    root = bundled_v1_root()
    (tmp_path / "INCAR").write_text("ENCUT=520\n", encoding="utf-8")
    completed = subprocess.run(
        [
            "bash",
            "-c",
            (
                '. "$1/Execution/tasks/ht_tasks_api.sh"; '
                '. "$1/Execution/tasks/vasp/vasptools.sh"; '
                'printf "%s\\n" "$(HT_FCALC "1.5+2.5")" "$(VASP_GET_TAG ENCUT)"'
            ),
            "bash",
            str(root),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines() == ["4", "520"]
    assert "compat/ht_tasks_api_v1.sh" in (root / "Execution/tasks/ht_tasks_api.sh").read_text(encoding="utf-8")
    notice = (root / "NOTICE").read_text(encoding="utf-8")
    assert "Henrik Levämäki" in notice
    assert "Copyright (C) 2022 Henrik Levämäki" in notice
