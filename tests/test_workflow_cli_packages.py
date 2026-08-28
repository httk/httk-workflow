"""CLI coverage for directory workflow packages."""

import json
from pathlib import Path

from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, job_records, scaffold
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

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.package"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""


def test_workflow_describe_reports_manifest_step_drift(tmp_path: Path, capsys) -> None:
    src = str(Path(__file__).parents[1] / "src")
    manifest = _MANIFEST.replace("tests.package", "tests.drift").replace(
        'steps = ["start"]', 'steps = ["start", "gone"]'
    )
    package = _package(tmp_path / "drift", manifest)
    # A native runner entry that actually implements only 'start' — so its
    # --describe disagrees with the manifest's declared ["start", "gone"].
    (package / "run").write_text(
        "#!/usr/bin/env python3\n"
        f"import sys\nsys.path.insert(0, {src!r})\n"
        "from httk.workflow import Runner\n"
        "run = Runner('tests.drift')\n"
        "@run.step\n"
        "def start(a):\n    a.succeed()\n"
        "raise SystemExit(run.main())\n",
        encoding="utf-8",
    )
    (package / "run").chmod(0o755)
    context = _context(tmp_path)
    assert command(["describe", str(package)], context) == 0
    out = capsys.readouterr().out
    assert "WARNING: step drift" in out and "gone" in out

    assert command(["describe", "--json", str(package)], context) == 0
    described = json.loads(capsys.readouterr().out)[0]
    assert "gone" in described["manifest_step_drift"]


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
                "--workspace",
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
        # The declared 'label' default is applied for the name nobody supplied,
        # and the undeclared 'kpoint_density' is kept (it only warns on stderr).
        assert job["parameters"] == {"kpoint_density": 30.0, "label": "test"}


def test_job_new_batch_reconciles_structure_names_and_reports_skips_and_count(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structures = tmp_path / "structures"
    structures.mkdir()
    # POSCAR.Si2O registers no reader of its own but is a structure name, so the
    # scanner keeps it and reads it as POSCAR — matching the SDK's structure_files.
    (structures / "POSCAR.Si2O").write_text(_POSCAR, encoding="utf-8")
    (structures / "mp-1.vasp").write_text(_POSCAR, encoding="utf-8")
    for index in range(7):
        (structures / f"notes-{index}.txt").write_text("skip me", encoding="utf-8")

    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace,
                "--workflow-dir",
                str(package),
                "--input-from",
                "structure",
                str(structures),
            ],
            context,
        )
        == 0
    )
    captured = capsys.readouterr()
    # Two jobs were submitted, one per structure the reconciled scanner kept.
    assert len(captured.out.splitlines()) == 2
    # One skip note names the unreadable files, truncated with a (+K more) tail.
    assert "skipped 7 of 9 files in" in captured.err
    assert "notes-0.txt" in captured.err and "(+2 more)" in captured.err
    # And one submission count closes the batch.
    assert "submitted 2 jobs" in captured.err


def test_job_new_batch_tag_prefixes_each_derived_tag(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structures = tmp_path / "structures"
    structures.mkdir()
    (structures / "POSCAR.Si2O").write_text(_POSCAR, encoding="utf-8")
    (structures / "mp-1.vasp").write_text(_POSCAR, encoding="utf-8")

    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace,
                "--workflow-dir",
                str(package),
                "--tag",
                "run7",
                "--input-from",
                "structure",
                str(structures),
            ],
            context,
        )
        == 0
    )
    keys = [line.split("\t")[0] for line in capsys.readouterr().out.splitlines()]
    # --tag prefixes each derived structure tag rather than replacing it.
    assert keys and all("run7-" in key for key in keys)


def test_job_new_batch_tag_prefix_stays_within_the_tag_syntax(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structures = tmp_path / "structures"
    structures.mkdir()
    # A near-cap derived tag and a prefix ending in '-' would otherwise compose an
    # over-long tag or a forbidden '--'; re-sanitizing lands both jobs instead of
    # dying partway with an untaught FormatError.
    (structures / (("x" * 48) + ".vasp")).write_text(_POSCAR, encoding="utf-8")
    (structures / "mp-1.vasp").write_text(_POSCAR, encoding="utf-8")

    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace,
                "--workflow-dir",
                str(package),
                "--tag",
                "run-",
                "--input-from",
                "structure",
                str(structures),
            ],
            context,
        )
        == 0
    )
    keys = [line.split("\t")[0] for line in capsys.readouterr().out.splitlines()]
    assert len(keys) == 2
    for key in keys:
        tag = key.rsplit("--", 1)[0]
        assert "--" not in tag and 0 < len(tag) <= 48


def test_job_new_batch_reports_partial_progress_before_failing(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    workspace = _workspace(tmp_path, context)
    package = _cli_package(tmp_path / "package")
    structures = tmp_path / "structures"
    structures.mkdir()
    (structures / "one.vasp").write_text(_POSCAR, encoding="utf-8")
    (structures / "two.vasp").write_text(_POSCAR, encoding="utf-8")

    # A shared, undeclared input makes every item fail as the batch streams.
    assert (
        command(
            [
                "job",
                "new",
                "--workspace",
                workspace,
                "--workflow-dir",
                str(package),
                "--input",
                "bad=1",
                "--input-from",
                "structure",
                str(structures),
            ],
            context,
        )
        == 2
    )
    assert "of 2 jobs before failing" in capsys.readouterr().err


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
                "--workspace",
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
    assert (
        command(
            ["job", "new", "--workspace", workspace, "--workflow", str(package), "--workflow-dir", str(package)],
            context,
        )
        == 2
    )
    assert "not allowed with argument" in capsys.readouterr().err
    assert command(["job", "new", "--workspace", workspace], context) == 2
    assert "one of the arguments --workflow --workflow-dir is required" in capsys.readouterr().err

    empty = tmp_path / "not-a-package"
    empty.mkdir()
    assert command(["job", "new", "--workspace", workspace, "--workflow-dir", str(empty)], context) == 2
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

    assert command(["describe", "--json", str(package)], context) == 0
    directory = json.loads(capsys.readouterr().out)[0]
    assert directory["build"] == {"present": False}
    assert directory["hooks"] == {
        "instantiate": {"present": True, "file": "instantiate.py", "kind": "python", "packaged": False},
        "collect": {"present": True, "file": "collect.py", "kind": "python", "packaged": False},
    }
    assert directory["postprocess"] == {"report": {"file": "scripts/report.sh", "description": "write a report"}}

    provider = load_workflow_package(package)
    try:
        assert command(["describe", "--json", "tests.cli.package"], context) == 0
        by_id = json.loads(capsys.readouterr().out)[0]
        assert by_id["format"] == "httk-workflow-workflow-description"
        assert by_id["format_version"] == 2
        assert by_id["source"]["kind"] == "registered-directory"
        assert by_id["workflow"] == provider.workflow_id

        assert command(["describe", "--json", "cli-package"], context) == 0
        by_alias = json.loads(capsys.readouterr().out)[0]
        assert by_alias["workflow"] == "tests.cli.package"
    finally:
        scaffold._WORKFLOW_PROVIDERS.pop(provider.workflow_id, None)


def test_workflow_describe_reports_build_registration(tmp_path: Path, capsys) -> None:
    package = _package(
        tmp_path / "build",
        _MANIFEST
        + '\n[workflow.build]\ncommand = "python build.py"\nplatform = "linux x86_64"\nartifacts = ["build"]\n',
    )
    context = _context(tmp_path)
    assert command(["describe", "--json", str(package)], context) == 0
    described = json.loads(capsys.readouterr().out)[0]
    assert described["build"] == {
        "present": True,
        "command": "python build.py",
        "platform": "linux x86_64",
        "artifacts": ["build"],
    }
    assert command(["describe", str(package)], context) == 0
    assert "build: yes" in capsys.readouterr().out


def test_workflow_describe_reports_packaged_and_missing_hooks_honestly(tmp_path: Path, capsys) -> None:
    context = _context(tmp_path)
    assert command(["describe", "--json", "vasp-relax"], context) == 0
    packaged = json.loads(capsys.readouterr().out)[0]
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
    assert command(["describe", "--json", str(no_hooks)], context) == 0
    directory = json.loads(capsys.readouterr().out)[0]
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


def _compiled_cli_package(root: Path, *, build_command: str = "./build.sh") -> Path:
    package = _package(
        root,
        _MANIFEST + f'\n[workflow.build]\ncommand = "{build_command}"\nartifacts = ["out"]\n',
    )
    (package / "build.sh").write_text(
        "#!/bin/sh\nmkdir -p out\nprintf compiler-output\nprintf artifact > out/result\n", encoding="utf-8"
    )
    (package / "build.sh").chmod(0o755)
    return package


def test_workflow_build_registers_and_lists_a_package(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build")
    package = _compiled_cli_package(tmp_path / "package")

    assert command(["build", "--workspace", name, str(package)], context) == 0
    assert "platform probe" in capsys.readouterr().out
    assert command(["build", "--workspace", name, "--list"], context) == 0
    assert "any" in capsys.readouterr().out


def test_workflow_build_uses_syntactic_path_detection_and_pointer_listings(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-targets")
    package = _compiled_cli_package(tmp_path / "bare")
    workspace.publish_runner(package, name="bare")

    # The existing local directory named ``bare`` must not shadow the store selector.
    assert command(["build", "--workspace", name, "bare"], context) == 0
    capsys.readouterr()
    assert command(["build", "--workspace", name, "./bare"], context) == 0
    capsys.readouterr()
    current = next(workspace.runner_builds.rglob("current.json"))
    generation = json.loads(current.read_text(encoding="utf-8"))["generation"]
    (current.parent / generation / "artifacts" / "build.json").write_text("{}", encoding="utf-8")
    assert command(["build", "--workspace", name, "--list", "--json"], context) == 0
    rows = json.loads(capsys.readouterr().out)
    stores = {row["store"] for row in rows}
    assert "bare" in stores and len(stores) == 2


def test_workflow_build_store_option_handles_nested_selectors(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-nested")
    package = _compiled_cli_package(tmp_path / "nested")
    workspace.publish_runner(package, name="group/nested")

    assert command(["build", "--workspace", name, "--store", "group/nested"], context) == 0
    capsys.readouterr()
    assert command(["build", "--workspace", name, "group/nested"], context) == 1
    assert "does not exist" in capsys.readouterr().err


def test_workflow_build_refuses_a_buildless_package_and_reports_failures(tmp_path: Path, capsys) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-errors")
    buildless = _package(tmp_path / "buildless")
    assert command(["build", "--workspace", name, str(buildless)], context) == 1
    assert "[workflow.build]" in capsys.readouterr().err

    failed = _compiled_cli_package(tmp_path / "failed", build_command="./missing.sh")
    assert command(["build", "--workspace", name, str(failed)], context) == 1
    assert "missing.sh" in capsys.readouterr().err


def test_workflow_build_json_is_one_report(tmp_path: Path, capfd) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "build-json")
    package = _compiled_cli_package(tmp_path / "package-json")
    assert command(["build", "--workspace", name, "--json", str(package)], context) == 0
    captured = capfd.readouterr()
    report = json.loads(captured.out)[0]
    assert report["platform_tag"] == "any"
    assert "compiler-output" in captured.err
