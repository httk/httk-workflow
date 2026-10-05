"""Check the fixed manager launch used after payload containment."""

import os
from pathlib import Path
from typing import cast

import pytest

from httk.workflow import _daemon_payload
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow.workspace import Workspace


class _ExecBoundary(Exception):
    pass


def _start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capacity: list[str]) -> tuple[Workspace, list[object]]:
    workspace = Workspace.initialize(tmp_path / "site/workspace")
    workspace.set_setting("environment.prelude", "export TEST_PRELUDE=changed-after-approval")
    runtime = tmp_path / "runtime"
    policy = Policy(
        workspace=tmp_path / "site/workspace",
        workspace_id=workspace.workspace_id,
        enrollment_id="e" * 32,
        exchange=tmp_path / "site/exchange",
        state=tmp_path / "state",
        snapshots=tmp_path / "snapshots",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=runtime / "sbatch",
        squeue=runtime / "squeue",
        scancel=runtime / "scancel",
        cluster="cluster",
        readonly_paths=(runtime,),
        profiles=(
            Profile(
                "cpu",
                4,
                2048,
                10,
                workers=3,
                prelude="export TEST_PRELUDE=inside",
                manager_command="/opt/approved httk",
            ),
        ),
    )
    observed: list[object] = []

    def execute(executable: str, argv: list[str], environment: dict[str, str]) -> None:
        observed.extend([executable, argv, environment])
        raise _ExecBoundary

    monkeypatch.setattr(_daemon_payload, "load_policy", lambda _path: policy)
    monkeypatch.setattr(_daemon_payload, "Workspace", lambda _path: workspace)
    monkeypatch.setattr(_daemon_payload.os, "dup2", lambda *_args: None)
    monkeypatch.setattr(_daemon_payload.os, "execve", execute)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(_ExecBoundary):
        _daemon_payload.main(["--profile", "cpu", "--handle", "a" * 32, *capacity])
    return workspace, observed


def test_prelude_runs_in_payload_shell_before_fixed_manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, observed = _start(tmp_path, monkeypatch, ["--procs", "4", "--mem-mb", "2048"])
    executable, argv, _environment = observed
    assert executable == "/bin/bash"
    assert isinstance(argv, list)
    assert argv[:4] == ["/bin/bash", "--noprofile", "--norc", "-c"]
    script = argv[4]
    # The launcher's prelude first, then the workspace's live one, both after set -e.
    assert script.startswith("set -e\n")
    assert (
        script.index("export TEST_PRELUDE=inside")
        < script.index("export TEST_PRELUDE=changed-after-approval")
        < script.index("\nexec ")
    )
    assert "exec '/opt/approved httk' workflow manager run " in script
    assert (
        "--count 1 --workers 3 --worker-resource procs 4 --worker-resource mem 2048 --exchange --allocation none --idle"
        in script
    )
    assert (workspace.control / ("daemon-manager-" + "a" * 32 + ".log")).is_file()
    assert os.getcwd() == str(workspace.root)


def test_unreported_memory_is_not_offered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _workspace, observed = _start(tmp_path, monkeypatch, ["--procs", "6"])
    script = cast(list[str], observed[1])[4]
    assert "--worker-resource procs 6 --exchange --allocation none --idle" in script
    assert "--worker-resource mem" not in script


@pytest.mark.parametrize("procs", ["0", "06", "4x", str(2**63)])
def test_malformed_capacity_is_refused(procs: str) -> None:
    assert _daemon_payload.main(["--profile", "cpu", "--handle", "a" * 32, "--procs", procs]) == 2
