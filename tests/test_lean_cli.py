"""The remote job-transfer verbs start lean: ``httk`` builds and imports only the command group they name.

Every probe runs ``httk.core.cli.main`` in a fresh interpreter and asserts that numpy, the storage stack and the
unrelated command groups stayed unimported, since a remote start pays for every module it touches.
"""

import ast
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httk.core.cli
import pytest

import httk.workflow
from httk.workflow import git_workflows, workflow_cli
from test_deliver import hold_jobs, outgoing_dir, stream
from test_git_workflows import _git
from test_job_transfer import jobs, registered

FORBIDDEN = (
    "importlib.metadata",
    "numpy",
    "httk.core.datastream",
    "httk.core.vectors",
    "httk.core.exactmath",
    "httk.core.data_records",
    "httk.workflow.storing",
    "httk.workflow.collecting",
    "httk.workflow.campaigns",
    "httk.workflow.git_workflows",
    "httk.workflow._pip_repositories",
    "httk.workflow.monitor",
    "httk.workflow.workflow_cli._collect",
    "httk.workflow.workflow_cli._build",
    "httk.workflow.workflow_cli._campaign",
    "httk.workflow.workflow_cli._monitor",
)

# Runs each argv through ``main`` (``--help`` exits), then reports the exit codes, the captured standard output
# and which forbidden modules got imported.
_PROBE = """
import contextlib, io, json, sys
from httk.core.cli import main
argvs, forbidden = json.loads(sys.argv[1])
codes, outputs = [], []
for argv in argvs:
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        try:
            codes.append(main(argv))
        except SystemExit as exc:
            codes.append(exc.code)
    outputs.append(out.getvalue())
print(json.dumps({"codes": codes, "outputs": outputs, "loaded": [m for m in forbidden if m in sys.modules]}))
"""

# The sources of this checkout and of httk-core, ahead of whatever the interpreter would find.
_PYTHONPATH = os.pathsep.join(
    str(Path(module.__file__ or "").resolve().parents[2]) for module in (httk.workflow, httk.core.cli)
)


def probe(*argvs: Sequence[str], stdin: bytes = b"") -> dict[str, Any]:
    """Run *argvs* through ``httk.core.cli.main`` in one fresh interpreter, with this test's configuration."""

    existing = os.environ.get("PYTHONPATH")
    environment = {**os.environ, "PYTHONPATH": f"{_PYTHONPATH}{os.pathsep}{existing}" if existing else _PYTHONPATH}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, json.dumps([[list(argv) for argv in argvs], FORBIDDEN])],
        input=stdin,
        capture_output=True,
        check=True,
        env=environment,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize(
    "argvs",
    [
        pytest.param(
            [
                ["job", "adopt", "--json", "--stdin-tar", "--workspace", "WS", "--help"],
                ["job", "transfer", "--hold", "--json", "--help"],
                ["job", "transfer", "--release", "T", "WS", "--help"],
            ],
            id="job",
        ),
        pytest.param([["transfer", "status", "--json", "WS", "--help"]], id="transfer"),
        pytest.param([["workspace", "status", "--json", "WS", "--help"]], id="workspace"),
        pytest.param(
            [
                ["workflow", "install", "--json", "--stdin-tar", "--workspace", "WS", "--help"],
                ["workflow", "install", "--json", "--workspace", "WS", "--also-build", "ID", "--help"],
            ],
            id="workflow",
        ),
    ],
)
def test_help_of_a_transfer_verb_stays_lean(argvs: list[list[str]]) -> None:
    result = probe(*argvs)
    assert result["codes"] == [0] * len(argvs)
    assert all(output.startswith("usage: httk ") for output in result["outputs"])
    assert result["loaded"] == []


def test_adopting_a_stream_stays_lean(tmp_path: Path) -> None:
    # A fetched git workflow in another format on this machine: listing it in ``job new --workflow`` help would
    # parse it, importing every compat language and with them collecting.
    root = tmp_path / "repo"
    root.mkdir()
    (root / "httk_workflow.toml").write_text(
        '[workflow]\nname = "tests.lean.v1"\n[workflow.runner]\nformat = "httk-v1"\n', encoding="utf-8"
    )
    _git(root, "init", "-q", "-b", "main")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "one")
    git_workflows.fetch_workflow(f"git+file://{root}")
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(near, 1)
    ((waiting, _, _),) = jobs(near, 1)
    data = stream(outgoing_dir(near), [holds[0].transfer_id])
    # The far side adopts the stream, then the near side releases that hold and holds the next job.
    result = probe(
        ["job", "adopt", "--json", "--no-durable", "--stdin-tar", "--workspace", far_name],
        ["job", "transfer", "--no-durable", "--release", holds[0].transfer_id, "--keep"]
        + ["--destination-id", far.workspace_id, near_name],
        ["job", "transfer", "--no-durable", "--hold", "--json", "--destination-id", far.workspace_id]
        + ["--job", str(waiting["id"]), "--", near_name, "cluster:far"],
        stdin=data,
    )
    assert result["codes"] == [0, 0, 0]
    assert [entry["status"] for entry in json.loads(result["outputs"][0])["bundles"]] == ["adopted"]
    assert [hold["destination_workspace_id"] for hold in json.loads(result["outputs"][2])["holds"]] == [
        far.workspace_id
    ]
    assert result["loaded"] == []


def test_inspecting_and_building_in_a_workspace_stays_lean(tmp_path: Path) -> None:
    _, name = registered(tmp_path, "far")
    result = probe(
        ["workspace", "status", "--json", name],
        ["transfer", "status", "--json", name],
        ["workflow", "install", "--json", "--workspace", name, "--also-build", "local:absent"],
    )
    assert result["codes"] == [0, 0, 1]  # nothing to build: the build is reported failed
    assert json.loads(result["outputs"][2])["built"][0]["status"] == "failed"
    assert result["loaded"] == []


def test_stub_mirrors_the_lazy_exports() -> None:
    # The stub is the static view (type checkers, API reference); the runtime map must say the same.
    stub = ast.parse(Path(workflow_cli.__file__).with_suffix(".pyi").read_text(encoding="utf-8"))
    imported = {
        alias.name: "." * node.level + (node.module or "")
        for node in stub.body
        if isinstance(node, ast.ImportFrom) and node.level
        for alias in node.names
    }
    assert imported == {**vars(workflow_cli)["_LAZY_EXPORTS"], "remote_workspace_output": "._common"}
    defined = {node.name for node in stub.body if isinstance(node, ast.FunctionDef)}
    assert sorted({*imported, *defined}) == sorted(workflow_cli.__all__)
    for name in workflow_cli.__all__:
        assert getattr(workflow_cli, name) is getattr(__import__(workflow_cli.__name__, fromlist=[name]), name)
