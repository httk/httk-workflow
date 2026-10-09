"""Behaviour of the pure attempt-context and runner-environment builders."""

import json
from pathlib import Path
from typing import Any

import pytest

from httk.workflow._attempt_env import attempt_context, runner_environment
from httk.workflow.errors import FormatError


def _context(**overrides: Any) -> dict[str, object]:
    arguments: dict[str, Any] = {
        "workspace_id": "ws",
        "job_id": "job",
        "job_key": "project/job",
        "placement": "project",
        "payload": "/w/jobs/project/job",
        "step": "start",
        "activation_id": "act",
        "activation_ordinal": 1,
        "attempt_id": "att",
        "attempt_ordinal": 1,
        "total_attempts": 1,
        "is_unclean_restart": False,
        "attempt_reason": None,
        "previous_attempt_id": None,
        "activation_reason": None,
        "workdir_mode": "isolated",
        "workdir_reused": False,
        "unsafe_persistent_takeover": False,
        "data_generation": None,
        "durable": True,
        "settings": {"code.command": "run"},
        "resources": {"cores": 1},
        "deadline": None,
        "binding": None,
        "join": None,
        "children": [],
        **overrides,
    }
    return attempt_context(**arguments)


def _environment(context: dict[str, object], **overrides: Any) -> dict[str, str]:
    arguments: dict[str, Any] = {
        "base": {"PATH": "/usr/bin", "HTTK_WORKFLOW_RUNNER_ROOT": "stale", "HTTK_WORKFLOW_NODELIST": "stale"},
        "context": context,
        "control": Path("/w/control"),
        "workspace_root": Path("/w"),
        "payload": Path("/w/jobs/project/job"),
        "workdir": Path("/w/jobs/project/job/work"),
        "durable": True,
        "deadline": None,
        "binding_environment": {},
        "code_variables": {"HTTK_WORKFLOW_VASP_BASH_API": "/vasp.sh"},
        "data_dir": None,
        "declared_environment": {},
        "settings": context["settings"],
        **overrides,
    }
    return runner_environment(**arguments)


def test_minimal_context() -> None:
    context = _context()
    assert context["format"] == "httk-workflow-attempt-context"
    assert context["format_version"] == 2
    assert context["is_restart"] is False
    assert context["attempt_reason"] == "claim"
    assert context["deadline"] is None
    assert "binding" not in context
    assert _context(attempt_ordinal=2, attempt_reason="retry")["is_restart"] is True


def test_binding_and_deadline_only_when_given() -> None:
    binding = {"nodes": [{"host": "n1"}], "nodefile": "/w/control/nodefile"}
    context = _context(deadline=1234, binding=binding)
    assert context["binding"] == binding
    environment = _environment(context, deadline=1234, binding_environment={"HTTK_WORKFLOW_NODELIST": "n1"})
    assert environment["HTTK_WORKFLOW_DEADLINE"] == "1234"
    assert environment["HTTK_WORKFLOW_NODELIST"] == "n1"
    plain = _environment(_context())
    assert "HTTK_WORKFLOW_DEADLINE" not in plain
    assert "HTTK_WORKFLOW_NODELIST" not in plain


def test_environment_variables() -> None:
    context = _context()
    environment = _environment(context)
    assert json.loads(environment["HTTK_WORKFLOW_CONTEXT"]) == context
    assert environment["HTTK_WORKFLOW_CONTROL_DIR"] == "/w/control"
    assert environment["HTTK_WORKFLOW_WORKSPACE_DIR"] == "/w"
    assert environment["HTTK_WORKFLOW_JOB_DIR"] == "/w/jobs/project/job"
    assert environment["HTTK_WORKFLOW_WORKDIR"] == "/w/jobs/project/job/work"
    assert environment["HTTK_WORKFLOW_IS_RESTART"] == "0"
    assert environment["HTTK_WORKFLOW_UNCLEAN_RESTART"] == "0"
    assert environment["HTTK_WORKFLOW_DURABLE"] == "1"
    assert environment["HTTK_WORKFLOW_ATTEMPT_REASON"] == "claim"
    assert environment["HTTK_WORKFLOW_STEP"] == "start"
    assert environment["HTTK_WORKFLOW_VASP_BASH_API"] == "/vasp.sh"
    assert Path(environment["HTTK_WORKFLOW_BASH_API"]).is_file()
    assert environment["PATH"].endswith("/usr/bin")
    assert environment["HTTK_CODE_COMMAND"] == "run"
    # Runner-store variables are left to the caller; the data directory only for transactional jobs.
    assert "HTTK_WORKFLOW_RUNNER_ROOT" not in environment
    assert "HTTK_WORKFLOW_DATA_DIR" not in environment
    assert _environment(context, data_dir=Path("/w/data"))["HTTK_WORKFLOW_DATA_DIR"] == "/w/data"


def test_settings_export_skips_declared_and_reserved() -> None:
    settings = {"code.command": "run", "workflow.secret": "x", "flag": True}
    environment = _environment(
        _context(settings=settings),
        settings=settings,
        declared_environment={"command": {"setting": "code.command"}},
    )
    assert "HTTK_CODE_COMMAND" not in environment
    assert "HTTK_WORKFLOW_SECRET" not in environment
    assert "HTTK_FLAG" not in environment


def test_oversized_context_is_refused() -> None:
    with pytest.raises(FormatError, match="100000-byte"):
        _environment(_context(settings={"big": "x" * 100_000}))
