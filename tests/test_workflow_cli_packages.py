"""CLI coverage for directory workflow packages."""

import json
from pathlib import Path

from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, job_records, scaffold
from httk.workflow._util import tree_digest
from httk.workflow.packages import load_workflow_package
from httk.workflow.scaffold import new_job
from httk.workflow.workflow_cli import command
from test_workflow_packages import _MANIFEST, _package

_POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""

_SUCCESS_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.package"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def _cli_package(root: Path) -> Path:
    manifest = _MANIFEST.replace("tests.package", "tests.cli.package").replace("test-package", "cli-package")
    manifest += '\n[workflow.postprocess.report]\nfile = "scripts/report.sh"\ndescription = "write a report"\n'
    package = _package(root, manifest)
    scripts = package / "scripts"
    scripts.mkdir()
    (scripts / "report.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    return package.resolve()


def _context(tmp_path: Path) -> CLIContext:
    return CLIContext("httk", tmp_path)


def _workspace(tmp_path: Path, context: CLIContext) -> str:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    return register_ws(context, root, "cli")


def test_job_new_accepts_workflow_dir_and_batches_parameter_sources(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structures = tmp_path / "structures"
    structures.mkdir()
    for name in ("one.vasp", "two.vasp"):
        (structures / name).write_text(_POSCAR, encoding="utf-8")

    assert (
        command(
            [
                "job",
                "new",
                workspace,
                "--workflow-dir",
                str(package),
                "--input-from",
                "structure",
                str(structures),
                "--parameter",
                "kpoint_density=30.0",
                "--placement",
                "project/screening",
                "--json",
            ],
            context,
        )
        == 0
    )
    reports = json.loads(capsys.readouterr().out)
    assert len(reports) == 2
    for report in reports:
        assert report["workflow"] == "tests.cli.package"
        assert report["runner"]["source"] == "workspace"
        assert report["runner"]["sha256"] == tree_digest(package)
        assert report["placement"] == "project/screening"
        job = json.loads((Path(report["payload_path"]) / "job.json").read_text(encoding="utf-8"))
        assert job["parameters"] == {"kpoint_density": 30.0}


def test_job_new_accepts_a_package_path_and_rejects_workflow_selection_errors(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")

    assert (
        command(
            [
                "job",
                "new",
                workspace,
                "--workflow",
                str(package),
                "--input",
                f"structure={structure}",
            ],
            context,
        )
        == 0
    )
    assert command(["job", "new", workspace, "--workflow", str(package), "--workflow-dir", str(package)], context) == 2
    assert "not allowed with argument" in capsys.readouterr().err
    assert command(["job", "new", workspace], context) == 2
    assert "one of the arguments --workflow --workflow-dir is required" in capsys.readouterr().err

    empty = tmp_path / "not-a-package"
    empty.mkdir()
    assert command(["job", "new", workspace, "--workflow-dir", str(empty)], context) == 2
    assert "containing httk_workflow.toml" in capsys.readouterr().err


def test_workflow_describe_is_read_only_and_resolves_id_alias_and_directory(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    package = _cli_package(tmp_path / "package")
    before = tmp_path / "describe-workspace"
    assert command(["describe", str(package)], context) == 0
    text = capsys.readouterr().out
    assert "workflow: tests.cli.package" in text
    assert "source: directory" in text
    assert "* start" in text
    assert "inputs:" in text and "initial_structure" in text
    assert "parameters:" in text and "label" in text
    assert "outputs:" in text and "relaxed_structure" in text
    assert "postprocess scripts:" in text and "report: scripts/report.sh — write a report" in text
    assert "generated from manifest" in text
    assert "instantiate hook: yes (instantiate.py)" in text
    assert "collect hook: yes (collect.py), kind=python" in text
    assert not before.exists()

    assert command(["describe", str(package), "--json"], context) == 0
    directory = json.loads(capsys.readouterr().out)
    assert directory["hooks"] == {
        "instantiate": {"present": True, "file": "instantiate.py", "kind": "python", "packaged": False},
        "collect": {"present": True, "file": "collect.py", "kind": "python", "packaged": False},
    }
    assert directory["postprocess"] == {"report": {"file": "scripts/report.sh", "description": "write a report"}}

    provider = load_workflow_package(package)
    try:
        assert command(["describe", "tests.cli.package", "--json"], context) == 0
        by_id = json.loads(capsys.readouterr().out)
        assert by_id["format"] == "httk-workflow-workflow-description"
        assert by_id["format_version"] == 1
        assert by_id["source"]["kind"] == "registered-directory"
        assert by_id["workflow"] == provider.workflow_id

        assert command(["describe", "cli-package", "--json"], context) == 0
        by_alias = json.loads(capsys.readouterr().out)
        assert by_alias["workflow"] == "tests.cli.package"
    finally:
        scaffold._WORKFLOW_PROVIDERS.pop(provider.workflow_id, None)


def test_workflow_describe_reports_packaged_and_missing_hooks_honestly(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    assert command(["describe", "vasp-relax", "--json"], context) == 0
    packaged = json.loads(capsys.readouterr().out)
    assert packaged["hooks"]["collect"] == {"present": True, "file": None, "kind": None, "packaged": True}
    assert packaged["hooks"]["instantiate"] == {"present": False, "file": None, "kind": None, "packaged": True}
    assert packaged["inputs"]["structure"]["role"] == "initial_structure"
    assert packaged["inputs"]["structure"]["entry_type"] == "structures"
    assert packaged["postprocess"] == {
        "relaxation-report": {
            "file": "scripts/relaxation_report",
            "description": "write a relaxation summary (text + JSON) into the job's postprocess directory",
        },
        "relaxation-plot": {
            "file": "scripts/relaxation_plot",
            "description": "plot ionic-step energies into the job's postprocess directory",
        },
    }
    assert command(["describe", "vasp-relax"], context) == 0
    described = capsys.readouterr().out
    assert "relaxation-report: scripts/relaxation_report" in described
    assert "relaxation-plot: scripts/relaxation_plot" in described

    no_hooks = _package(
        tmp_path / "no-hooks",
        _MANIFEST.replace('[workflow.instantiate]\nfile = "instantiate.py"\n\n', "").replace(
            '[workflow.collect]\nfile = "collect.py"\n\n', ""
        ),
    )
    assert command(["describe", str(no_hooks), "--json"], context) == 0
    directory = json.loads(capsys.readouterr().out)
    assert directory["hooks"] == {
        "instantiate": {"present": False, "file": None, "kind": None, "packaged": False},
        "collect": {"present": False, "file": None, "kind": None, "packaged": False},
    }
    assert directory["postprocess"] == {}
    assert command(["describe", str(no_hooks)], context) == 0
    assert "postprocess scripts:\n  -" in capsys.readouterr().out


def test_workflow_describe_renders_environment(tmp_path: Path, capsys) -> None:
    package = _package(
        tmp_path / "describe-environment",
        _MANIFEST
        + '\n[workflow.environment.command]\ntype = "string"\nsetting = "tool.command"\ndefault = "echo"\ndescription = "The command."\n',
    )
    context = _context(tmp_path)
    assert load_workflow_package(package, register=False).environment
    assert command(["describe", str(package)], context) == 0
    output = capsys.readouterr().out
    assert "environment:" in output
    assert "command: type=string, setting=tool.command, default=echo, description=The command." in output


def test_directory_package_runs_and_job_records_retain_the_tree_pin(tmp_path: Path) -> None:
    package = _cli_package(tmp_path / "package")
    (package / "run").write_text(_SUCCESS_RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    structure = tmp_path / "POSCAR"
    structure.write_text(_POSCAR, encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"structure": structure})

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120.0)

    records = list(job_records(workspace))
    assert len(records) == 1
    record = records[0]
    assert record.job_id == job.job_id
    assert record.job["workflow"] == "tests.cli.package"
    assert record.job["runner"] == {
        "executor": "path",
        "source": "workspace",
        "path": job.runner["path"],
        "sha256": tree_digest(package),
        "arguments": [],
    }
