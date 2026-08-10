"""The stand-in cluster the remote-adapter tests drive for real.

No test that uses this needs a network or a batch system. A stand-in ``ssh`` on
``PATH`` runs the command it is given through a local shell rooted in a per-test
directory, exactly as a remote sshd would, so genuine ``rsync`` transfers,
genuine quoting and genuine exit statuses are exercised. A stand-in ``sbatch``
spools the script it was handed and prints a Slurm-shaped receipt.
"""

import json
import logging
import os
import shutil
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

# The suite deliberately launches many short-lived Python runners.  Under xdist
# each runner imports NumPy through httk-core; keeping each BLAS runtime to one
# thread prevents 28 workers from multiplying that into hundreds of threads.
# Set this before importing any httk module, and child runners inherit it.
for _thread_limit in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ[_thread_limit] = "1"

from httk.workflow.adapters import (
    add_remote,
)
from httk.workflow.registry import (
    register_workspace,
)


@pytest.fixture(autouse=True)
def _isolated_httk_config(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    """Give every test its own httk config home, so the global workspace registry
    (``$XDG_CONFIG_HOME/httk/workspaces.json``) never leaks between tests or into
    the developer's real configuration. A test that wants a different config home
    still overrides it: this only runs first."""

    monkeypatch.setenv("HTTK_CONFIG_HOME", str(tmp_path_factory.mktemp("httk-config")))
    monkeypatch.setenv("HTTK_DATA_HOME", str(tmp_path_factory.mktemp("httk-store")))


@pytest.fixture(autouse=True)
def _isolated_workflow_logging() -> Iterator[None]:
    """Restore the ``httk.workflow`` logger after every test.

    CLI entry points deliberately set ``propagate = False`` and install handlers
    on ``httk.workflow`` for their own process; inside one pytest worker that
    state would leak into later tests and starve ``caplog``, which captures on
    the root logger."""

    logger = logging.getLogger("httk.workflow")
    propagate = logger.propagate
    handlers = list(logger.handlers)
    level = logger.level
    yield
    logger.propagate = propagate
    logger.handlers[:] = handlers
    logger.setLevel(level)


def register_ws(
    context: object,
    path: object,
    name: str = "ws",
    *,
    remote: str = "local",
    scope: str = "global",
) -> str:
    """Register *path* under *name* and return the name.

    ``remote`` and ``scope`` remain accepted because many tests share this
    fixture; v2 has one local per-user registry, so both values are ignored.
    """

    register_workspace(name, str(path))
    return name


#: The host every fake ``ssh-slurm`` remote is pointed at. The stand-in ``ssh``
#: ignores it beyond logging it, but the adapters must still carry it around.
FAKE_HOST = "fake.example.test"

FAKE_SSH = '''#!{python}
"""Stand-in for ssh that runs the remote command locally."""

import json
import os
import pathlib
import subprocess
import sys

VALUE_OPTIONS = {{"-p", "-o", "-i", "-l", "-F", "-c", "-m", "-b", "-D", "-E", "-e", "-I", "-J", "-S", "-W", "-w"}}


def main(argv):
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--":
            index += 1
            break
        if item in VALUE_OPTIONS:
            index += 2
            continue
        if item.startswith("-"):
            index += 1
            continue
        break
    if index >= len(argv):
        print("fake ssh: no destination", file=sys.stderr)
        return 255
    destination = argv[index]
    command = " ".join(argv[index + 1 :])
    log = os.environ.get("HTTK_FAKE_SSH_LOG")
    if log:
        with open(log, "a", encoding="utf-8") as stream:
            stream.write(json.dumps({{"destination": destination, "command": command}}) + "\\n")
    if os.environ.get("HTTK_FAKE_SSH_REFUSE"):
        print(f"fake ssh: connect to host {{destination}} port 22: Connection refused", file=sys.stderr)
        return 255
    if not command:
        print("fake ssh: interactive sessions are not supported", file=sys.stderr)
        return 255
    # A talkative login shell on the far side: the banner lands on the standard
    # output of this connection, ahead of whatever the remote command prints.
    banner = os.environ.get("HTTK_FAKE_SSH_BANNER")
    when = os.environ.get("HTTK_FAKE_SSH_BANNER_WHEN", "")
    if banner and when in command:
        print(banner, flush=True)
    root = pathlib.Path(os.environ["HTTK_FAKE_SSH_ROOT"])
    root.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    # The fake transport loops back into this process, so the far side shares
    # the machine-owned workspace registry with the client.
    environment["HTTK_CONFIG_HOME"] = os.environ["HTTK_CONFIG_HOME"]
    environment["HTTK_DATA_HOME"] = os.environ["HTTK_DATA_HOME"]
    # sshd hands the joined command words to a login shell; do exactly that.
    return subprocess.run(["/bin/sh", "-c", command], cwd=root, env=environment).returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''

FAKE_SBATCH = '''#!{python}
"""Stand-in for sbatch that spools the submitted script."""

import json
import os
import pathlib
import shutil
import sys


def main(argv):
    spool = pathlib.Path(os.environ["HTTK_FAKE_SBATCH_SPOOL"])
    spool.mkdir(parents=True, exist_ok=True)
    script = argv[-1]
    job = 4200 + len(list(spool.glob("*.json"))) + 1
    shutil.copyfile(script, spool / f"{{job}}.sbatch")
    (spool / f"{{job}}.json").write_text(
        json.dumps({{"argv": argv, "cwd": os.getcwd(), "script": script}}),
        encoding="utf-8",
    )
    print(f"Submitted batch job {{job}}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''


@dataclass(frozen=True)
class Remote:
    """The stand-in cluster: its filesystem root, its PATH and its spools."""

    root: Path
    binaries: Path
    spool: Path
    log: Path

    def install(self, name: str, body: str) -> Path:
        """Put one more stand-in executable on the PATH of this fake cluster."""

        path = self.binaries / name
        path.write_text(body.format(python=sys.executable), encoding="utf-8")
        path.chmod(0o755)
        return path

    def commands(self) -> list[str]:
        """Every remote command string the stand-in ``ssh`` was asked to run."""

        if not self.log.exists():
            return []
        return [str(json.loads(line)["command"]) for line in self.log.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def remote(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Remote:
    if shutil.which("rsync") is None:  # pragma: no cover - depends on the host
        pytest.skip("rsync is unavailable, so no honest transfer can be exercised")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    root = tmp_path / "remote"
    root.mkdir()
    cluster = Remote(root=root, binaries=binaries, spool=tmp_path / "spool", log=tmp_path / "ssh.log")
    cluster.install("ssh", FAKE_SSH)
    cluster.install("sbatch", FAKE_SBATCH)
    (binaries / "httk").write_text(f'#!/bin/sh\nexec {sys.executable} -m httk.core.cli "$@"\n', encoding="utf-8")
    (binaries / "httk").chmod(0o755)
    source_root = Path(__file__).resolve().parents[1] / "src"
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv("PYTHONPATH", f"{source_root}{os.pathsep}{existing}" if existing else str(source_root))
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("HTTK_FAKE_SSH_ROOT", str(root))
    monkeypatch.setenv("HTTK_FAKE_SSH_LOG", str(cluster.log))
    monkeypatch.setenv("HTTK_FAKE_SBATCH_SPOOL", str(cluster.spool))
    return cluster


def fake_remote(
    project: Path,
    *,
    template: str = "ssh-slurm",
    name: str = "cluster",
    **settings: object,
) -> Path:
    """Add one remote to *project* and point it at the stand-in."""

    bundle = add_remote(name, template=template, project=project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    remote_settings = dict(metadata["settings"])
    if template == "ssh-slurm":
        remote_settings.update({"host": FAKE_HOST, "username": "someone"})
    remote_settings.update(settings)
    metadata["settings"] = remote_settings
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")
    return bundle


@dataclass(frozen=True)
class TestProfile:
    """Select normal or full-depth values without duplicating a test body."""

    name: str

    @property
    def extended(self) -> bool:
        return self.name == "extended"

    def scale[T](self, *, normal: T, extended: T) -> T:
        """Return the profile-appropriate member of a test's same-property pair."""

        return extended if self.extended else normal


@pytest.fixture(scope="session", autouse=True)
def test_profile() -> TestProfile:
    """The test-depth knob shared by every profiled test.

    Normal is deliberately the default for direct pytest invocations.  Extended
    runs set ``HTTK_TEST_PROFILE=extended`` as well as selecting ``extended``
    marker cases.
    """

    name = os.environ.get("HTTK_TEST_PROFILE", "normal")
    if name not in {"normal", "extended"}:
        raise pytest.UsageError("HTTK_TEST_PROFILE must be 'normal' or 'extended'")
    return TestProfile(name)
