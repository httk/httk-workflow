"""Real ``ssh`` adapter behaviour and launcher-profile forwarding.

No test here needs a network or a batch system: the stand-in cluster of
``conftest`` provides an ``ssh`` that runs the remote command locally and an
``sbatch`` that spools the script it was handed, so genuine ``tar``
transfers, genuine quoting and genuine exit statuses are exercised.
"""

import io
import json
import os
import shlex
import shutil
import stat
import sys
import tarfile
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from conftest import FAKE_HOST, Remote, fake_remote, register_ws
from httk.workflow import Workspace, _kernel, adapter_runtime, launchers
from httk.workflow.adapters import RemoteTarget, add_remote, probe_remote_workspace, run_adapter
from httk.workflow.launchers import add_launcher
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _manager, command

_FAKE_PYTHON = '''#!{python}
"""Stand-in for python3 that records ``-m pip`` instead of running it."""

import json
import os
import subprocess
import sys

argv = sys.argv[1:]
if argv[:2] == ["-m", "pip"]:
    with open(os.environ["HTTK_FAKE_PIP_LOG"], "a", encoding="utf-8") as stream:
        stream.write(json.dumps(argv) + "\\n")
    sys.exit(int(os.environ.get("HTTK_FAKE_PIP_STATUS", "1")))
sys.exit(subprocess.run([{python!r}, *argv]).returncode)
'''

_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def _tree(root: Path) -> Path:
    (root / "files").mkdir(parents=True)
    (root / "files" / "runner").write_text(_RUNNER, encoding="utf-8")
    (root / "files" / "runner").chmod(0o750)
    (root / "files" / "data with spaces.txt").write_text("payload\n", encoding="utf-8")
    (root / "link").symlink_to("files/runner")
    return root


def test_ssh_push_and_pull_round_trip_a_real_tree(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="push-pull")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    destination = remote.root / "runs" / "incoming"

    pushed = run_adapter(
        bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)}
    )

    assert pushed["path"] == str(destination)
    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert stat.S_IMODE((destination / "files" / "runner").stat().st_mode) == 0o750
    assert (destination / "link").is_symlink()
    assert os.readlink(destination / "link") == "files/runner"

    back = tmp_path / "back"
    pulled = run_adapter(bundle, "pull", {"remote_settings": {}, "source": str(destination), "destination": str(back)})

    assert pulled["path"] == str(back)
    assert (back / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert stat.S_IMODE((back / "files" / "runner").stat().st_mode) == 0o750
    assert os.readlink(back / "link") == "files/runner"
    # Repeating the push is the resume path the detached transfer relies on.
    assert run_adapter(bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)})


def test_ssh_push_transfers_only_the_requested_files(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="batched")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    destination = remote.root / "runs" / "batched"

    run_adapter(
        bundle,
        "push",
        {
            "remote_settings": {},
            "source": str(source),
            "destination": str(destination),
            "files": ["files/data with spaces.txt"],
        },
    )

    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert not (destination / "files" / "runner").exists()


def test_ssh_push_refuses_a_path_that_escapes_the_transfer(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="escape")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    with pytest.raises(RuntimeError, match="not workspace relative"):
        run_adapter(
            bundle,
            "push",
            {
                "remote_settings": {},
                "source": str(source),
                "destination": str(remote.root / "runs" / "escape"),
                "files": ["../secret"],
            },
        )


def _push(bundle: Path, source: Path, destination: Path, **request: object) -> dict[str, object]:
    return run_adapter(
        bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination), **request}
    )


def _pull(bundle: Path, source: Path, destination: Path, **request: object) -> dict[str, object]:
    return run_adapter(
        bundle, "pull", {"remote_settings": {}, "source": str(source), "destination": str(destination), **request}
    )


def test_ssh_push_and_pull_each_use_one_connection(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="one-connection")
    bundle = fake_remote(project)
    source = tmp_path / "source"
    for index in range(5):
        (source / f"job{index}" / "data").mkdir(parents=True)
        (source / f"job{index}" / "data" / "file.txt").write_text(f"job {index}\n", encoding="utf-8")
    (source / "skipped").write_text("not listed\n", encoding="utf-8")
    destination = remote.root / "runs" / "batch"
    files = [f"job{index}" for index in range(5)]

    _push(bundle, source, destination, files=files)
    assert len(remote.commands()) == 1
    # Each listed directory travels recursively; nothing unlisted does.
    assert (destination / "job4" / "data" / "file.txt").read_text(encoding="utf-8") == "job 4\n"
    assert not (destination / "skipped").exists()

    back = tmp_path / "back"
    _pull(bundle, destination, back, files=files)
    assert len(remote.commands()) == 2
    assert sorted(item.name for item in back.iterdir()) == files
    assert (back / "job0" / "data" / "file.txt").read_text(encoding="utf-8") == "job 0\n"

    _push(bundle, source, remote.root / "runs" / "whole")
    _pull(bundle, remote.root / "runs" / "whole", tmp_path / "whole")
    assert len(remote.commands()) == 4
    assert (tmp_path / "whole" / "skipped").read_text(encoding="utf-8") == "not listed\n"
    assert not any("rsync" in item for item in remote.commands())


def test_ssh_push_and_pull_a_single_file(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="single-file")
    bundle = fake_remote(project)
    source = tmp_path / "local name.txt"
    source.write_text("one file\n", encoding="utf-8")
    destination = remote.root / "files" / "remote name.txt"

    assert _push(bundle, source, destination)["path"] == str(destination)
    assert destination.read_text(encoding="utf-8") == "one file\n"
    assert [item.name for item in destination.parent.iterdir()] == ["remote name.txt"]

    back = tmp_path / "back" / "pulled.txt"
    assert _pull(bundle, destination, back, directory=False)["path"] == str(back)
    assert back.read_text(encoding="utf-8") == "one file\n"
    assert [item.name for item in back.parent.iterdir()] == ["pulled.txt"]
    assert len(remote.commands()) == 2


def test_ssh_push_creates_a_missing_destination_directory(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mkdir")
    bundle = fake_remote(project)
    destination = remote.root / "deep" / "nested" / "target"

    _push(bundle, _tree(tmp_path / "source"), destination)

    assert (destination / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert len(remote.commands()) == 1


def test_ssh_push_needs_no_rsync(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="no-rsync")
    bundle = fake_remote(project)
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "python3").write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} "$@"\n', encoding="utf-8")
    (tools / "python3").chmod(0o755)
    for name in ("tar", "mkdir"):
        found = shutil.which(name)
        assert found is not None
        (tools / name).symlink_to(found)
    path = f"{remote.binaries}{os.pathsep}{tools}"
    assert shutil.which("rsync", path=path) is None
    monkeypatch.setenv("PATH", path)
    destination = remote.root / "runs" / "no-rsync"

    _push(bundle, _tree(tmp_path / "source"), destination)

    assert (destination / "files" / "runner").read_text(encoding="utf-8") == _RUNNER


def test_ssh_transfer_reports_a_refused_connection(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="refused")
    bundle = fake_remote(project)
    source = _tree(tmp_path / "source")
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")

    with pytest.raises(RuntimeError, match=r"ssh tar push failed \(255\).*Connection refused"):
        _push(bundle, source, remote.root / "runs" / "refused")
    with pytest.raises(RuntimeError, match=r"ssh tar pull failed \(255\).*Connection refused"):
        _pull(bundle, remote.root / "runs" / "refused", tmp_path / "back")
    assert not (remote.root / "runs" / "refused").exists()


def test_ssh_pull_reports_a_truncated_stream(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="truncated")
    bundle = fake_remote(project)
    big = tmp_path / "big"
    big.write_bytes(b"x" * 5000)
    # A remote ``tar`` that exits 0 halfway through a member: only the stream itself shows the damage.
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w", format=tarfile.USTAR_FORMAT) as writer:
        writer.add(big, arcname="big")
    half = tmp_path / "half.tar"
    half.write_bytes(archive.getvalue()[:2048])
    remote.install("tar", f"#!/bin/sh\ncat {shlex.quote(str(half))}\n")

    with pytest.raises(RuntimeError, match="unusable tar stream"):
        _pull(bundle, remote.root, tmp_path / "back")


def test_ssh_transfer_shares_the_interactive_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(adapter_runtime, "_terminal_present", lambda: True)
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
    argv = adapter_runtime._ssh_tar_argv({"host": FAKE_HOST, "interactive_auth": "yes"}, "true")
    assert any(item.startswith("ControlPath=") for item in argv)


def _hostile(members: list[tuple[tarfile.TarInfo, bytes]]) -> io.BytesIO:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as writer:
        for info, data in members:
            info.size = len(data)
            writer.addfile(info, io.BytesIO(data) if info.isreg() else None)
    stream.seek(0)
    return stream


def test_ssh_pull_refuses_a_member_that_escapes(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="hostile")
    bundle = fake_remote(project)
    hostile = tmp_path / "hostile.tar"
    hostile.write_bytes(_hostile([(tarfile.TarInfo("../escape"), b"x")]).getvalue())
    remote.install("tar", f"#!/bin/sh\ncat {shlex.quote(str(hostile))}\n")

    with pytest.raises(RuntimeError, match="outside the destination"):
        _pull(bundle, remote.root, tmp_path / "back")
    assert not (tmp_path / "escape").exists()


def _typed(name: str, kind: bytes, linkname: str = "") -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.type, info.linkname = kind, linkname
    return info


def test_local_extraction_refuses_a_hard_link_out_of_the_destination(tmp_path: Path) -> None:
    victim = tmp_path / "home" / "victim.txt"
    victim.parent.mkdir()
    victim.write_text("original\n", encoding="utf-8")
    destination = tmp_path / "landing"
    destination.mkdir()
    # The link would make x share the victim's inode, and the regular x would then overwrite it.
    stream = _hostile([(_typed("x", tarfile.LNKTYPE, "../home/victim.txt"), b""), (tarfile.TarInfo("x"), b"pwned\n")])

    with pytest.raises(tarfile.LinkOutsideDestinationError):
        adapter_runtime._extract(stream, destination)
    assert victim.read_text(encoding="utf-8") == "original\n"
    assert victim.stat().st_nlink == 1


@pytest.mark.parametrize("kind", [tarfile.FIFOTYPE, tarfile.CHRTYPE, tarfile.BLKTYPE])
def test_local_extraction_refuses_special_files(tmp_path: Path, kind: bytes) -> None:
    with pytest.raises(tarfile.SpecialFileError):
        adapter_runtime._extract(_hostile([(_typed("bundle.json", kind), b"")]), tmp_path)
    assert not (tmp_path / "bundle.json").exists()


def test_local_extraction_keeps_inner_hard_links_and_absolute_symlinks(tmp_path: Path) -> None:
    members = [
        (tarfile.TarInfo("data"), b"payload\n"),
        (_typed("alias", tarfile.LNKTYPE, "data"), b""),
        (_typed("potential", tarfile.SYMTYPE, "/opt/pseudopotentials/Si"), b""),
    ]

    adapter_runtime._extract(_hostile(members), tmp_path)

    assert (tmp_path / "alias").stat().st_ino == (tmp_path / "data").stat().st_ino
    assert os.readlink(tmp_path / "potential") == "/opt/pseudopotentials/Si"


@pytest.mark.parametrize("kind", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_single_file_pull_accepts_only_a_regular_file(tmp_path: Path, kind: bytes) -> None:
    sibling = tmp_path / "sibling.txt"
    sibling.write_text("original\n", encoding="utf-8")
    # Renamed onto the destination, the link would alias the sibling and the regular member would overwrite it.
    stream = _hostile([(_typed("remote.txt", kind, "sibling.txt"), b""), (tarfile.TarInfo("remote.txt"), b"pwned\n")])

    with pytest.raises(tarfile.FilterError, match="single-file pull"):
        adapter_runtime._extract(stream, tmp_path, rename=("remote.txt", "pulled.txt"))
    assert sibling.read_text(encoding="utf-8") == "original\n"
    assert not (tmp_path / "pulled.txt").exists()


def test_local_extraction_keeps_hostile_members_inside_the_destination(tmp_path: Path) -> None:
    destination = tmp_path / "destination"
    destination.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(tarfile.OutsideDestinationError):
        adapter_runtime._extract(_hostile([(tarfile.TarInfo("../escape"), b"x")]), destination)
    assert not (tmp_path / "escape").exists()

    # A symlink to an absolute target is kept, but nothing is extracted through it.
    link = tarfile.TarInfo("link")
    link.type, link.linkname = tarfile.SYMTYPE, str(outside)
    with pytest.raises(tarfile.OutsideDestinationError):
        adapter_runtime._extract(_hostile([(link, b""), (tarfile.TarInfo("link/evil"), b"x")]), destination)
    assert os.readlink(destination / "link") == str(outside)
    assert not (outside / "evil").exists()

    # tarfile strips a leading "/", so an absolute name lands inside the destination, never at its own path.
    absolute = outside / "absolute"
    adapter_runtime._extract(_hostile([(tarfile.TarInfo(str(absolute)), b"x")]), destination)
    assert not absolute.exists()
    assert (destination / str(absolute).lstrip("/")).read_bytes() == b"x"

    setuid = tarfile.TarInfo("setuid")
    setuid.mode = 0o4755
    adapter_runtime._extract(_hostile([(setuid, b"x")]), destination)
    assert stat.S_IMODE((destination / "setuid").stat().st_mode) == 0o755


def test_remote_invoke_keeps_quoting_hostile_arguments_intact(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="quoting")
    bundle = fake_remote(project)
    hostile = ["a b", "$HOME", "`id`", "'single'", '"double"', "semi;colon", "star*", "new\nline", "back\\slash"]
    sentinel = remote.root / "must-not-exist"

    invoked = run_adapter(
        bundle,
        "invoke",
        {
            "remote_settings": {},
            "argv": [
                "python3",
                "-c",
                "import json, sys; print(json.dumps(sys.argv[1:]))",
                *hostile,
                f"value; touch {sentinel}",
            ],
        },
    )

    assert invoked["returncode"] == 0
    assert json.loads(str(invoked["stdout"])) == [*hostile, f"value; touch {sentinel}"]
    assert not sentinel.exists()


def test_remote_invoke_honours_the_requested_directory(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cwd")
    bundle = fake_remote(project)
    directory = remote.root / "a directory"
    directory.mkdir()

    invoked = run_adapter(
        bundle,
        "invoke",
        {"remote_settings": {}, "argv": ["pwd"], "cwd": str(directory)},
    )

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == str(directory)


def test_remote_prelude_runs_before_the_command(tmp_path: Path, remote: Remote) -> None:
    # The prelude sets an environment variable the command then reports, proving
    # it ran in the same remote shell before the command (the mechanism a site
    # uses for module loads and a venv activation).
    project = tmp_path / "project"
    initialize_project(project, name="prelude")
    bundle = fake_remote(project, prelude="export HTTK_PRELUDE_MARKER=ran")

    invoked = run_adapter(
        bundle,
        "invoke",
        {"argv": ["python3", "-c", "import os; print(os.environ.get('HTTK_PRELUDE_MARKER', 'absent'))"]},
    )

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == "ran"


def test_remote_prelude_failure_aborts_before_the_command(tmp_path: Path, remote: Remote) -> None:
    # A failing prelude line must stop the command from running in a
    # half-configured shell (``set -e``), rather than running it regardless.
    project = tmp_path / "project"
    initialize_project(project, name="prelude-fail")
    bundle = fake_remote(project, prelude="false")
    sentinel = remote.root / "prelude-should-not-reach-this"

    invoked = run_adapter(
        bundle,
        "invoke",
        {"argv": ["touch", str(sentinel)]},
    )

    assert invoked["returncode"] != 0
    assert not sentinel.exists()


def test_remote_invoke_reports_a_failing_remote_command(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="failing")
    bundle = fake_remote(project)

    invoked = run_adapter(bundle, "invoke", {"remote_settings": {}, "argv": ["false"]})

    assert invoked["returncode"] == 1


def test_remote_status_returns_the_remote_workspace_json(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="status")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    bundle = fake_remote(project, workspace=str(workspace.root))

    result = run_adapter(
        bundle,
        "status",
        {
            "remote_settings": {},
            "argv": ["httk", "workspace", "status", "--by-path", "--json", str(workspace.root)],
        },
    )

    assert result["returncode"] == 0
    reported = json.loads(str(result["stdout"]))
    assert reported[0]["format"] == "httk-workflow-status"
    assert reported[0]["workspace_id"] == workspace.workspace_id


def test_workspace_launcher_profile_dispatches_run_to_slurm(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="workspace-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    for key, value in {
        "manager.launch": "cluster",
        "manager.count": "2",
        "manager.workers": "4",
        "slurm.account": "p2026-1",
        "slurm.partition": "main",
        "slurm.time_limit": "04:00:00",
        "slurm.nodes": "2",
        "slurm.cpus_per_task": "16",
    }.items():
        workspace.set_setting(key, value)
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert command(["run", "--workspace", "station"], context) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["job_ids"] == ["4201", "4202"]
    scripts = sorted(path.name for path in workspace.root.glob("logs/launchers/*.sbatch"))
    assert scripts == sorted(f"{launch_id}.sbatch" for launch_id in result["launch_ids"])
    script = next(workspace.root.glob("logs/launchers/*.sbatch")).read_text(encoding="utf-8")
    for directive in (
        "--account=p2026-1",
        "--partition=main",
        "--time=04:00:00",
        "--nodes=2",
        "--cpus-per-task=16",
    ):
        assert f"#SBATCH {directive}" in script
    assert f"#SBATCH --chdir={workspace.root}" in script
    assert (
        f"exec {sys.executable} -m httk.core.cli workflow manager run --by-path --workspace {workspace.root} --workers 4"
        in script
    )


def test_invocation_launcher_overrides_workspace_launcher(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="invocation-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cpu", template="slurm", project=project, settings={"slurm.partition": "batch"})
    add_launcher("gpu", template="slurm", project=project, settings={"slurm.partition": "gpu"})
    workspace.set_setting("manager.launch", "cpu")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert command(["run", "--workspace", "station", "--launcher", "gpu"], context) == 0
    capsys.readouterr()
    script = next(workspace.root.glob("logs/launchers/*.sbatch")).read_text(encoding="utf-8")
    assert "#SBATCH --partition=gpu" in script
    assert "--launcher" not in script


def test_process_launcher_option_uses_exact_local_child_and_detached_argv(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="process-invocation-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    workspace.set_setting("manager.launch", "cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    expected = [
        sys.executable,
        "-m",
        "httk.core.cli",
        "workflow",
        "manager",
        "run",
        "--by-path",
        "--workspace",
        str(workspace.root.resolve()),
    ]
    children: list[list[str]] = []

    class Child:
        def poll(self):
            return None

        def wait(self):
            return 0

        def terminate(self):
            return None

    def fake_popen(argv, **_kwargs):
        children.append(list(argv))
        return Child()

    monkeypatch.setattr(_manager.subprocess, "Popen", fake_popen)
    assert command(["run", "--workspace", "station", "--launcher", "process", "--count", "2"], context) == 0
    # Split foreground children only count; none probes an allocation to bind.
    assert children == [[*expected, "--allocation", "none"]] * 2
    assert all(argv.count("--launcher") == 0 for argv in children)
    capsys.readouterr()

    detached: list[dict[str, object]] = []

    def fake_launch_processes(**kwargs):
        detached.append(kwargs)
        return {"ok": True, "kind": "process", "count": 1, "pids": [7]}

    monkeypatch.setattr(_manager, "launch_processes", fake_launch_processes)
    assert command(["run", "--workspace", "station", "--launcher", "process", "--detach"], context) == 0
    assert detached[0]["argv"] == expected
    detached_argv = detached[0]["argv"]
    assert isinstance(detached_argv, list)
    assert detached_argv.count("--launcher") == 0
    capsys.readouterr()


def test_unresolved_workspace_launcher_names_workspace_and_setting(tmp_path: Path, remote: Remote, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown-workspace-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("manager.launch", "missing-cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")

    assert command(["run", "--workspace", "station"], context) == 1
    captured = capsys.readouterr()
    assert str(workspace.root) in captured.err
    assert "manager.launch='missing-cluster'" in captured.err
    assert "Traceback" not in captured.err


def test_inline_overrides_workspace_launcher(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="inline-launcher")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    workspace.set_setting("manager.launch", "cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    assert command(["run", "--workspace", "station", "--inline", "--detach"], context) == 2
    assert "not allowed with argument" in capsys.readouterr().err
    assert command(["run", "--workspace", "station", "--inline", "--count", "2"], context) == 2
    assert "--inline can only be combined" in capsys.readouterr().err
    workspace.set_setting("manager.count", "4")
    called: list[Path] = []

    def fake_in_process(_arguments, root, _context, _settings):
        called.append(root)
        return 0

    monkeypatch.setattr(_manager, "_run_in_process_manager", fake_in_process)

    assert command(["run", "--workspace", "station", "--inline"], context) == 0
    assert called == [workspace.root]
    assert not list(remote.spool.glob("*.json"))


def test_missing_sbatch_refusal_is_returned_by_workspace_launcher(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="missing-sbatch")
    workspace = Workspace.initialize(tmp_path / "workspace")
    add_launcher("cluster", template="slurm", project=project)
    workspace.set_setting("manager.launch", "cluster")
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "station")
    no_sbatch_path = tmp_path / "no-sbatch-bin"
    no_sbatch_path.mkdir()
    (no_sbatch_path / "python3").symlink_to(sys.executable)
    monkeypatch.setenv("PATH", str(no_sbatch_path))
    monkeypatch.setattr(launchers.shutil, "which", lambda name: "/fake/sbatch" if name == "sbatch" else None)

    assert command(["run", "--workspace", "station"], context) == 1
    captured = capsys.readouterr()
    assert "manager.launch='cluster'" in captured.err
    assert "sbatch" in captured.err


def test_remote_manager_from_the_command_line_invokes_once(
    tmp_path: Path,
    remote: Remote,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="cli-manager")
    workspace = Workspace.initialize(remote.root / "runs" / "workspace")
    fake_remote(project, workspace=str(workspace.root))
    workspace.set_setting("manager.workers", "4")
    context = CLIContext("httk", project)
    # `manager run` on a remote-bound workspace submits through the remote's
    # The remote command is invoked directly on the owning machine.
    register_ws(context, workspace.root, "station", remote="cluster")

    assert command(["manager", "run", "--workspace", "cluster:station", "--count", "2"], context) == 0
    capsys.readouterr()
    manager_commands = [item for item in remote.commands() if "workflow manager run" in item]
    assert len(manager_commands) == 1
    assert "httk workflow manager run --workspace station --detach --count 2" in manager_commands[0]


def test_remote_manager_forwards_execution_options(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-manager-options")
    workspace = Workspace.initialize(remote.root / "runs" / "options")
    fake_remote(project, workspace=str(workspace.root))
    context = CLIContext("httk", project)
    register_ws(context, workspace.root, "options", remote="cluster")

    assert (
        command(
            [
                "manager",
                "run",
                "--workspace",
                "cluster:options",
                "--pool",
                "gpu",
                "--pool",
                "cpu",
                "--capability",
                "cuda",
                "--placement-prefix",
                "volume/a",
                "--heartbeat-interval",
                "3",
                "--poll-interval",
                "4",
                "--idle-timeout",
                "9",
                "--workers",
                "2",
                "--no-durable",
                "--log-level",
                "debug",
                "--json-logs",
                "--launcher",
                "process",
            ],
            context,
        )
        == 0
    )
    capsys.readouterr()
    command_line = remote.commands()[-1]
    assert shlex.split(command_line) == [
        "httk",
        "workflow",
        "manager",
        "run",
        "--workspace",
        "options",
        "--detach",
        "--pool",
        "gpu",
        "--pool",
        "cpu",
        "--capability",
        "cuda",
        "--placement-prefix",
        "volume/a",
        "--heartbeat-interval",
        "3.0",
        "--poll-interval",
        "4.0",
        "--workers",
        "2",
        "--idle-timeout",
        "9.0",
        "--log-level",
        "debug",
        "--json-logs",
        "--no-durable",
        "--launcher",
        "process",
    ]
    assert command_line.count("--launcher") == 1
    for option in (
        "--pool gpu",
        "--pool cpu",
        "--capability cuda",
        "--placement-prefix volume/a",
        "--heartbeat-interval 3.0",
        "--poll-interval 4.0",
        "--idle-timeout 9.0",
        "--workers 2",
        "--no-durable",
        "--log-level debug",
        "--json-logs",
        "--launcher process",
    ):
        assert option in command_line


def test_local_transfers_stay_in_this_filesystem(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-copy")
    bundle = fake_remote(project, template="local", name="batch")
    source = _tree(tmp_path / "source")

    pushed = run_adapter(
        bundle,
        "push",
        {"remote_settings": {}, "source": str(source), "destination": str(tmp_path / "copy")},
    )

    assert (tmp_path / "copy" / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert pushed["path"] == str(tmp_path / "copy")
    assert not remote.log.exists()


def test_check_reports_a_missing_remote_httk_with_the_remedy(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-missing")
    bundle = fake_remote(project, httk_command=str(tmp_path / "nowhere" / "httk"))

    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"remote_settings": {}})


def test_check_finds_the_remote_httk_without_creating_a_workspace(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-found")
    workspace = remote.root / "runs" / "fresh"
    bundle = fake_remote(project, workspace=str(workspace))

    result = run_adapter(bundle, "install", {"remote_settings": {}})

    assert result["installed"] is True
    assert result["httk_command"] == ["httk"]
    assert str(result["httk_version"]).strip()
    assert "workspace_created" not in result
    assert not workspace.exists()


def test_check_refuses_an_unreachable_host(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="check-unreachable")
    bundle = fake_remote(project)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")

    with pytest.raises(RuntimeError, match=f"cannot reach someone@{FAKE_HOST}"):
        run_adapter(bundle, "install", {"remote_settings": {}})


def test_check_never_installs_software_on_the_target(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retired bootstrap=pip setting is inert: no pip attempt, ever."""

    project = tmp_path / "project"
    initialize_project(project, name="bootstrap")
    remote.install("python3", _FAKE_PYTHON)
    log = tmp_path / "pip.log"
    monkeypatch.setenv("HTTK_FAKE_PIP_LOG", str(log))
    absent = str(tmp_path / "nowhere" / "httk")

    bundle = fake_remote(project, name="opted-in", httk_command=absent, bootstrap="pip")
    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"remote_settings": {}})
    assert not log.exists()


def test_configure_verifies_connectivity_for_ssh_remotes(
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="configure")
    bundle = fake_remote(project)

    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})["connectivity"] == "ok"

    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    with pytest.raises(RuntimeError, match="cannot reach"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})
    skipped = run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {"check_connectivity": "no"}})
    assert skipped["connectivity"] == "skipped"


def test_configure_checks_the_host_the_command_line_is_about_to_store(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="pending")
    bundle = add_remote("cluster", template="ssh", project=project)

    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})["connectivity"] == "skipped"
    pending = {"host": FAKE_HOST, "username": "someone"}
    assert run_adapter(bundle, "configure", {"remote_settings": {}, "settings": pending})["connectivity"] == "ok"


def test_an_unrecognized_adapter_kind_still_refuses(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unknown")
    bundle = fake_remote(project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["kind"] = "torque"
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")

    with pytest.raises(RuntimeError, match="'torque' is not implemented"):
        run_adapter(bundle, "status", {"remote_settings": {}, "argv": ["true"]})


def test_a_job_reaches_a_remote_workspace_and_runs_there(tmp_path: Path, remote: Remote) -> None:
    source_root = tmp_path / "project"
    initialize_project(source_root, name="end-to-end")
    home = v3.workspace(source_root / "workspace")
    destination = v3.workspace(remote.root / "runs" / "workspace")
    fake_remote(source_root, workspace=str(destination.root))
    workflow = v3.install(home, tmp_path / "package-home")
    v3.install(destination, tmp_path / "package-remote")
    job_id = v3.submit(home, workflow, {"start": "succeed"}).job_id
    context = CLIContext("httk", source_root)
    register_ws(context, home.root, "home")
    register_ws(context, destination.root, "station", remote="cluster")

    assert command(["job", "transfer", "--job", job_id, "home", "cluster:station"], context) == 0

    assert _kernel.locate(home, job_id, placement_hint=None, exhaustive=True) is None
    assert v3.find(destination, job_id).state == "ready"
    v3.run(destination)
    assert v3.find(destination, job_id).state == "succeeded"
    # Every step of the flow really crossed the stand-in transport.
    commands = remote.commands()
    assert any("workspace status --json station" in item for item in commands)
    assert any("job adopt --json --by-path --workspace " in item for item in commands)
    assert any(item.startswith("mkdir -p ") and " && tar -xpf - -C " in item for item in commands)


def test_probe_remote_workspace_refuses_an_older_remote_with_the_upgrade(tmp_path: Path) -> None:
    target = RemoteTarget("far", tmp_path, False)

    def _old_adapter(bundle, verb, payload, *, timeout):
        document = {"format": "httk-workflow-status", "format_version": 2, "core_profile": "core-v2"}
        return {"returncode": 0, "stdout": json.dumps([{**document, "workspace_id": str(uuid.uuid4()), "root": "/ws"}])}

    with pytest.raises(ValueError, match="Upgrade httk-workflow on the remote"):
        probe_remote_workspace(target, "ws", timeout=None, adapter=_old_adapter)


def test_a_repeated_local_copy_of_one_bundle_is_a_no_op(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    (bundle / "jobs").mkdir(parents=True)
    (bundle / "bundle.json").write_text('{"transfer_id": "a"}')
    adapter_runtime._copy(bundle, tmp_path / "copy")
    adapter_runtime._copy(bundle, tmp_path / "copy")  # the same bundle.json: already copied
    (bundle / "bundle.json").write_text('{"transfer_id": "b"}')
    with pytest.raises(FileExistsError):
        adapter_runtime._copy(bundle, tmp_path / "copy")
