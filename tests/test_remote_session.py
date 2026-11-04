"""The hidden far-side ``workflow session`` command: pinned ``httk`` vectors over newline-delimited JSON."""

import io
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace
from httk.workflow.adapters import SESSION_FORMAT, SESSION_FORMAT_VERSION
from httk.workflow.workflow_cli import build_parser, workflow_command
from httk.workflow.workflow_cli._session import serve
from test_job_paging import _job as _marker


def _session(*lines: object) -> list[dict[str, Any]]:
    """Serve *lines* (strings verbatim, anything else JSON-encoded) and return the decoded output lines."""

    requests = io.StringIO("".join((line if isinstance(line, str) else json.dumps(line)) + "\n" for line in lines))
    responses = io.StringIO()
    assert serve(requests, responses) == 0
    return [json.loads(line) for line in responses.getvalue().splitlines()]


def test_session_hello_then_job_list_and_show_in_one_session(tmp_path: Path) -> None:
    """The hello line comes first; sequential requests on a real workspace answer in order."""

    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    job_id = _marker(workspace, "ready", "jobs")
    register_ws(CLIContext("httk", tmp_path), root, "station")

    hello, listed, shown = _session(
        {"id": 1, "argv": ["httk", "job", "list", "--json", "--counts", "--limit", "5", "--workspace", "station"]},
        "",
        {"id": "two", "argv": ["httk", "job", "show", "--json", job_id, "--no-children", "--workspace", "station"]},
    )

    assert hello == {"format": SESSION_FORMAT, "format_version": SESSION_FORMAT_VERSION}
    assert listed["id"] == 1 and listed["returncode"] == 0, listed
    document = json.loads(listed["stdout"])
    assert "jobs" in document and "counts" in document
    assert shown["id"] == "two" and shown["returncode"] == 0, shown
    assert job_id in shown["stdout"]


def test_session_refuses_unpinned_and_malformed_requests_and_continues() -> None:
    """Refusals and malformed lines answer ``returncode`` 2 without ending the session."""

    status = ["httk", "workspace", "status", "--help"]
    responses = _session(
        {"id": 1, "argv": ["httk", "job", "delete", "x"]},
        {"id": 2, "argv": ["rm", "-rf", "/"]},
        "not json",
        "[1, 2]",
        {"id": 3},
        {"id": 4, "argv": ["httk", 5]},
        {"id": 5, "argv": status, "cwd": 7},
        {"id": 6, "argv": status},
    )[1:]

    assert [response["id"] for response in responses] == [1, 2, None, None, 3, 4, 5, 6]
    for response in responses[:-1]:
        assert response["returncode"] == 2 and response["stdout"] == "" and response["stderr"].strip(), response
    assert responses[-1]["returncode"] == 0 and "status" in responses[-1]["stdout"]


def test_session_reports_handler_errors_and_continues(tmp_path: Path) -> None:
    """A handler failure or an argparse exit is one nonzero response, not the end of the session."""

    root = tmp_path / "workspace"
    Workspace.initialize(root)
    register_ws(CLIContext("httk", tmp_path), root, "station")

    missing, unknown, listed = _session(
        {"id": 1, "argv": ["httk", "job", "show", "--json", "nonexistent", "--workspace", "station"]},
        {"id": 2, "argv": ["httk", "job", "list", "--no-such-flag"]},
        {"id": 3, "argv": ["httk", "job", "list", "--json", "--workspace", "station"]},
    )[1:]

    assert missing["returncode"] != 0 and missing["stderr"].strip(), missing
    assert unknown["returncode"] != 0 and "--no-such-flag" in unknown["stderr"], unknown
    assert listed["returncode"] == 0, listed


def test_session_leaf_is_hidden_but_resolves(tmp_path: Path, capsys: Any) -> None:
    """``workflow --help`` does not advertise the leaf, while the parser still binds it."""

    context = CLIContext("httk", tmp_path)
    assert workflow_command(["--help"], context) == 0
    assert "session" not in capsys.readouterr().out
    arguments = build_parser("httk", context, include_workspace_job=False, only="session").parse_args(["session"])
    assert arguments.handler.__name__ == "handle_session"


def test_isolate_streams_keeps_the_protocol_pipe_private() -> None:
    """After isolation only the returned stream reaches fd 1; prints and raw fd 1 writes go to stderr."""

    script = textwrap.dedent(
        """
        import os
        from httk.workflow.workflow_cli._session import _isolate_streams

        requests, responses = _isolate_streams()
        print("stray")
        os.write(1, b"raw")
        responses.write("ok\\n")
        responses.flush()
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script], input="", capture_output=True, text=True, check=True, timeout=60
    )
    assert completed.stdout == "ok\n"
    assert "stray" in completed.stderr and "raw" in completed.stderr
