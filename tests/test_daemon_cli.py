"""Prove the daemon entry bypasses mutable workflow discovery."""

import json
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import _daemon_cli, workflow_cli


def test_daemon_dispatch_does_not_build_ordinary_parser(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("ordinary parser must not run")

    observed: list[object] = []

    def daemon(arguments: object, *, program: str) -> int:
        observed.extend([arguments, program])
        return 0

    monkeypatch.setattr(workflow_cli, "command", forbidden)
    monkeypatch.setattr(_daemon_cli, "command", daemon)
    context = CLIContext(program="httk", cwd=tmp_path)
    assert workflow_cli.workspace_command(["daemon", "/data", "--policy", "/policy", "--once"], context) == 0
    assert observed == [["/data", "--policy", "/policy", "--once"], "httk workspace daemon"]


@pytest.mark.parametrize(
    "arguments",
    [
        ["relative", "--policy", "/policy"],
        ["/data", "--policy", "relative"],
        ["/data/../other", "--policy", "/policy"],
        ["/data", "--policy", "/policy", "--check", "--once"],
        ["/data", "--policy", "/policy", "--command", "rm"],
    ],
)
def test_daemon_refuses_ambiguous_paths_and_execution_options(arguments: list[str]) -> None:
    assert _daemon_cli.command(arguments, program="httk workspace daemon") == 2


def test_isolated_handoff_cleans_environment_cwd_and_inherited_descriptors(tmp_path: Path) -> None:
    installed = tmp_path / "installed"
    installed.mkdir()
    untrusted = tmp_path / "untrusted"
    untrusted.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    trap = tmp_path / "trap"
    (untrusted / "sitecustomize.py").write_text(f"open({str(trap)!r},'w').write('executed')\n")
    (installed / "_daemon_bootstrap.py").write_text(
        "import json,os,sys\n"
        "targets=[]\n"
        "for fd in os.listdir('/proc/self/fd'):\n"
        " try: targets.append(os.readlink('/proc/self/fd/'+fd))\n"
        " except FileNotFoundError: pass\n"
        "print(json.dumps({'cwd':os.getcwd(),'env':dict(os.environ),'isolated':sys.flags.isolated,"
        "'no_site':sys.flags.no_site,'targets':targets,'httk_loaded':'httk.workflow' in sys.modules,'argv':sys.argv}))\n"
    )
    script = f"""
import os
from httk.workflow import _daemon_cli
_daemon_cli.__file__ = {str(installed / '_daemon_cli.py')!r}
fd = os.open({str(outside)!r}, os.O_RDONLY)
os.set_inheritable(fd, True)
os.environ.update(PYTHONPATH={str(untrusted)!r}, BASH_ENV='untrusted', SSH_AUTH_SOCK='untrusted')
os.chdir({str(untrusted)!r})
raise SystemExit(_daemon_cli.command(['/data','--policy','/policy','--once'], program='httk workspace daemon'))
"""
    result = subprocess.run(
        [sys.executable, "-c", script], cwd="/", capture_output=True, text=True, check=True, timeout=20
    )
    observed = json.loads(result.stdout)
    assert observed["cwd"] == "/"
    assert observed["isolated"] == 1 and observed["no_site"] == 1
    assert not observed["httk_loaded"]
    assert str(outside) not in observed["targets"]
    assert not trap.exists()
    assert "PYTHONPATH" not in observed["env"] and "BASH_ENV" not in observed["env"]
    assert "SSH_AUTH_SOCK" not in observed["env"]
    assert observed["argv"][1:] == ["--mode", "broker", "--workspace", "/data", "--policy", "/policy", "--once"]
