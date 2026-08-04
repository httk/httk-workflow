"""The examples and the quickstart, executed exactly as they are documented.

The quickstart of ``docs/quickstart.md`` is not paraphrased here: its commands are
read out of the document itself and run verbatim, with ``httk`` and
``httk-taskmanager`` shims on ``PATH`` that are the installed console scripts in
every way that matters — the page uses the canonical ``httk workflow`` spelling,
and the alias shim is there so that a page falling back to it would still run.
What the assertions then read is the workspace those commands produced — a
succeeded job whose published data holds a real OUTCAR and CONTCAR — so the page
cannot drift from what works.
"""

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from conftest import TestProfile as _TestProfile
from httk.workflow import TaskManager, Workspace
from httk.workflow.scaffold import new_job

_ROOT = Path(__file__).parents[1]
_EXAMPLES = _ROOT / "examples"
_QUICKSTART = _ROOT / "docs" / "quickstart.md"
# The commands of the quickstart page are the ones before the section that
# explains them; everything after it is annotated with its own output.
_EXPLANATION = "## What each command did"
_CONSOLE_FENCE = "```console"


def _documented_commands(document: Path) -> list[str]:
    """Return the console lines of *document* up to the explanation section."""

    lines: list[str] = []
    inside = False
    for line in document.read_text(encoding="utf-8").splitlines():
        if line.startswith(_EXPLANATION):
            break
        if line.startswith("```"):
            inside = line.startswith(_CONSOLE_FENCE)
            continue
        if inside:
            # A prompt marks a command; anything else inside these blocks is the
            # body of a here-document and belongs to the command before it.
            lines.append(line.removeprefix("$ "))
    return lines


def _environment(bin_directory: Path) -> dict[str, str]:
    """Return the environment an example runs in: this checkout, on PATH."""

    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join([str(_ROOT / "src"), environment.get("PYTHONPATH", "")]).rstrip(
        os.pathsep
    )
    environment["PATH"] = os.pathsep.join([str(bin_directory), str(_EXAMPLES), environment.get("PATH", "")])
    # A test must never depend on a VASP the machine happens to have, nor on the
    # operator's configuration; every example names the mock one itself.
    for name in ("HTTK_VASP_COMMAND", "HTTK_MOCK_VASP_FAIL_ONCE"):
        environment.pop(name, None)
    return environment


@pytest.fixture()
def console_scripts(tmp_path: Path) -> Path:
    """Install ``httk`` and ``httk-taskmanager`` shims and return their directory.

    The published console scripts of the installed distributions do exactly this:
    run one module's ``main``. Shimming them keeps the documented command names
    intact in a checkout that was never installed.
    """

    directory = tmp_path / "bin"
    directory.mkdir()
    for name, module in (("httk", "httk.core.cli"), ("httk-taskmanager", "httk.workflow.cli")):
        script = directory / name
        script.write_text(f'#!/bin/sh\nexec "{sys.executable}" -m {module} "$@"\n', encoding="utf-8")
        script.chmod(0o755)
    return directory


@pytest.fixture()
def work(tmp_path: Path) -> Iterator[Path]:
    """An empty working directory with the examples beside it, as documented."""

    directory = tmp_path / "work"
    directory.mkdir()
    shutil.copytree(_EXAMPLES, directory / "examples")
    yield directory


def _run(command: list[str], *, cwd: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )
    assert completed.returncode == 0, f"{command} failed:\n{completed.stdout}\n{completed.stderr}"
    return completed


def _finished(workspace_root: Path) -> tuple[str, Path]:
    """Return the terminal state and payload of the one job of a workspace."""

    workspace = Workspace(workspace_root, mutable=False)
    markers = list(workspace.scan_markers())
    assert len(markers) == 1, f"expected exactly one job, found {[marker.job_key for marker in markers]}"
    return markers[0].kind, workspace.payload_path(markers[0].placement, markers[0].job_key)


def test_the_documented_quickstart_commands_produce_a_finished_relaxation(
    work: Path,
    console_scripts: Path,
) -> None:
    commands = [line.replace(" --remote local", "") for line in _documented_commands(_QUICKSTART)]

    # The page really is five commands, and they are the ones a newcomer types.
    assert sum(1 for line in commands if line.startswith(("httk", "export"))) == 5
    assert ("httk workflow workspace init quickstart-workspace --path quickstart-workspace") in commands
    assert any(line.startswith("httk workflow job new quickstart-workspace --template vasp-relax") for line in commands)

    completed = _run(
        ["bash", "-e", "-c", "\n".join(commands)],
        cwd=work,
        environment=_environment(console_scripts),
    )

    kind, payload = _finished(work / "quickstart-workspace")
    assert kind == "succeeded"
    # The published result is a real VASP result, produced by the documented run.
    published = payload / "data" / "vasp"
    assert (published / "OUTCAR").read_text(encoding="utf-8").startswith(" fake vasp")
    assert (published / "CONTCAR").read_text(encoding="utf-8").splitlines()[0] == "relaxed by the mock vasp"
    assert (published / "INCAR").is_file() and (published / "KPOINTS").is_file()
    assert (payload / "files" / "POSCAR").read_text(encoding="utf-8").splitlines()[0] == "silicon"

    # And the last documented command printed exactly one harvest record of it.
    records = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
    assert len(records) == 1
    assert records[0]["state"] == "succeeded"
    assert records[0]["job"]["workflow"] == "httk.vasp.relax"
    assert records[0]["job_key"].startswith("silicon--")


def test_the_quickstart_script_runs_the_same_path(work: Path, tmp_path: Path) -> None:
    # No console scripts on PATH at all: the script's documented module fallback is
    # what runs, which is the form a checkout without an install uses.
    empty = tmp_path / "no-scripts"
    empty.mkdir()
    completed = _run(
        ["bash", str(work / "examples" / "quickstart.sh")],
        cwd=work,
        environment=_environment(empty),
    )

    kind, payload = _finished(work / "quickstart-workspace")
    assert kind == "succeeded"
    assert (payload / "data" / "vasp" / "OUTCAR").is_file()
    assert "succeeded" in completed.stdout


def test_the_python_api_tour_runs(work: Path, tmp_path: Path) -> None:
    empty = tmp_path / "no-scripts"
    empty.mkdir()
    completed = _run(
        [sys.executable, str(work / "examples" / "example.py")],
        cwd=work,
        environment=_environment(empty),
    )

    kind, payload = _finished(work / "example-workflow-workspace")
    assert kind == "succeeded"
    assert (payload / "data" / "vasp" / "CONTCAR").is_file()
    lines = completed.stdout.splitlines()
    assert lines[0].startswith("workspace ")
    assert any(line.startswith("succeeded silicon--") for line in lines)
    assert "  published vasp/OUTCAR" in lines


@pytest.mark.parametrize("runner", ["defect_campaign.py", "defect_campaign.sh"])
def test_the_campaign_examples_run_in_either_language(runner: str, tmp_path: Path, test_profile: _TestProfile) -> None:
    sites = test_profile.scale(normal=2, extended=3)
    workspace = Workspace.initialize(tmp_path / runner)
    parent = new_job(
        workspace,
        _EXAMPLES / runner,
        step="characterize",
        inputs={"sites": sites, "diverging": "1"},
        tag="campaign",
    )
    assert parent.workflow == "examples.defects"

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=300.0)

    # The documented campaign happened: representative normal runs retain one
    # success and one failure, while extended also covers the third child.
    states = {marker.job_key.split("--")[0]: marker.kind for marker in workspace.scan_markers()}
    assert states == {
        "campaign": "failed",
        **{f"site-{site}": "failed" if site == 1 else "succeeded" for site in range(sites)},
    }
    campaign = workspace.payload_path(parent.placement, parent.job_key)
    assert (campaign / "run" / "report.tsv").read_text(encoding="utf-8") == "".join(
        f"site-{site}\t{site}\n" for site in range(sites) if site != 1
    )
    assert (campaign / "run" / "triage.txt").read_text(encoding="utf-8") == "site-1\n"
    marker = workspace.find_marker_by_id(parent.job_id)
    assert marker is not None
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "defects.child_failed" and failure["message"] == "failed: site-1"
