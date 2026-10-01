"""Check the fixed manager launch used after payload containment."""

import os
from pathlib import Path

import pytest

from httk.workflow import _daemon_payload
from httk.workflow._daemon_policy import Policy, Profile
from httk.workflow.workspace import Workspace


class _ExecBoundary(Exception):
    pass


def test_prelude_runs_in_payload_shell_before_fixed_manager(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    workspace.set_setting("environment.prelude", "export TEST_PRELUDE=inside")
    runtime = tmp_path / "runtime"
    policy = Policy(
        workspace=tmp_path / "workspace",
        workspace_id=workspace.workspace_id,
        enrollment_id="e" * 32,
        requests=tmp_path / "requests",
        responses=tmp_path / "responses",
        state=tmp_path / "state",
        bwrap=runtime / "bwrap",
        python=runtime / "python",
        sbatch=runtime / "sbatch",
        squeue=runtime / "squeue",
        scancel=runtime / "scancel",
        cluster="cluster",
        readonly_paths=(runtime,),
        broker_paths=(),
        profiles=(Profile("cpu", 4, 2048, 10),),
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
        _daemon_payload.main(["--profile", "cpu", "--handle", "a" * 32])
    executable, argv, _environment = observed
    assert executable == "/bin/bash"
    assert isinstance(argv, list)
    assert argv[:4] == ["/bin/bash", "--noprofile", "--norc", "-c"]
    script = argv[4]
    assert script.index("export TEST_PRELUDE=inside") < script.index("\nexec ")
    assert " -I -m httk.core.cli workflow manager run " in script
    assert "--count 1 --workers 1 --worker-resource procs 4 --worker-resource mem 2048 --idle" in script
    assert (workspace.control / ("daemon-manager-" + "a" * 32 + ".log")).is_file()
    assert os.getcwd() == str(workspace.root)
