"""Real ``mount`` adapter behaviour: a mounted filesystem plus an executor.

No test here needs a network. The far side is stood in for by two spellings of
one tree -- ``remote_root`` is what the remote machine sees, ``mount_root`` is a
symlink to it, exactly like an sshfs or NFS mount -- and by a stand-in executor
script that runs the single command line it is handed through ``sh -c`` locally,
appending it to a log. Genuine copies, genuine quoting, genuine prelude ordering
and genuine exit statuses are therefore exercised through the mount.
"""

import json
import os
import stat
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote, install_httk_toolchain, register_ws
from httk.workflow import Workspace
from httk.workflow.adapters import add_remote, run_adapter
from httk.workflow.manager import TaskManager
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command

# A stand-in executor: it appends the single command line it is handed to a log
# (so quoting and prelude are inspectable), then runs it through a shell, so the
# exit status, cwd and prelude ordering are genuinely exercised.
FAKE_EXEC = '''#!{python}
"""Stand-in executor that runs the appended command line through a shell."""

import json
import os
import subprocess
import sys

command = sys.argv[-1]
log = os.environ.get("HTTK_FAKE_EXEC_LOG")
if log:
    with open(log, "a", encoding="utf-8") as stream:
        stream.write(json.dumps({{"command": command}}) + "\\n")
if os.environ.get("HTTK_FAKE_EXEC_REFUSE"):
    print("fake exec: cannot reach the remote", file=sys.stderr)
    sys.exit(255)
sys.exit(subprocess.run(["/bin/sh", "-c", command]).returncode)
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


@dataclass(frozen=True)
class Mount:
    """The stand-in mount: the two spellings of one tree and the executor log."""

    remote_root: Path
    mount_root: Path
    exec_log: Path

    def commands(self) -> list[str]:
        """Every command line the stand-in executor was asked to run."""

        if not self.exec_log.exists():
            return []
        return [str(json.loads(line)["command"]) for line in self.exec_log.read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def mount(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Mount:
    """A mounted tree and an executor on PATH, both pointing at one filesystem.

    The mount kind needs neither ssh nor rsync, so this does not go through the
    ``remote`` fixture (and its rsync gate): it installs only the httk toolchain
    and the stand-in executor.
    """

    binaries = install_httk_toolchain(tmp_path, monkeypatch)
    remote_root = tmp_path / "cluster" / "proj"
    remote_root.mkdir(parents=True)
    mount_root = tmp_path / "mnt"
    mount_root.symlink_to(remote_root)
    executor = binaries / "hpcexec"
    executor.write_text(FAKE_EXEC.format(python=sys.executable), encoding="utf-8")
    executor.chmod(0o755)
    exec_log = tmp_path / "exec.log"
    monkeypatch.setenv("HTTK_FAKE_EXEC_LOG", str(exec_log))
    return Mount(remote_root=remote_root, mount_root=mount_root, exec_log=exec_log)


def _mount_remote(project: Path, mount: Mount, name: str = "cluster", **overrides: object) -> Path:
    """Add a ``mount`` remote pointed at the stand-in mount and executor."""

    settings: dict[str, object] = {
        "mount_root": str(mount.mount_root),
        "remote_root": str(mount.remote_root),
        "exec_command": "hpcexec",
    }
    settings.update(overrides)
    return fake_remote(project, template="mount", name=name, **settings)


def _tree(root: Path) -> Path:
    (root / "files").mkdir(parents=True)
    (root / "files" / "runner").write_text(_RUNNER, encoding="utf-8")
    (root / "files" / "runner").chmod(0o750)
    (root / "files" / "data with spaces.txt").write_text("payload\n", encoding="utf-8")
    (root / "link").symlink_to("files/runner")
    return root


def _payload(root: Path) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / "payload"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text(_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 2,
                "id": job_id,
                "tag": "test",
                "name": "test",
                "workflow": "tests",
                "runner": {"path": "files/runner", "arguments": []},
                "workdir": {"mode": "persistent", "path": "run"},
                "data": {"mode": "none"},
                "initial_step": "start",
                "priority": 500,
                "claim": {"pool": "default", "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


# 1. remote add / configure ----------------------------------------------------


def test_remote_add_mount_produces_a_valid_bundle(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="add-mount")

    bundle = add_remote("cluster", template="mount", project=project)

    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    assert metadata["kind"] == "mount"
    assert (bundle / "adapter").exists() and os.access(bundle / "adapter", os.X_OK)


def test_configure_refuses_relative_or_missing_roots(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-roots")
    bundle = add_remote("cluster", template="mount", project=project)

    with pytest.raises(RuntimeError, match="remote_root=PATH"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {"mount_root": str(mount.mount_root)}})
    with pytest.raises(RuntimeError, match="must be an absolute path"):
        run_adapter(
            bundle,
            "configure",
            {"remote_settings": {}, "settings": {"mount_root": "relative/mnt", "remote_root": str(mount.remote_root)}},
        )


def test_configure_refuses_an_empty_or_unknown_executor(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-exec")
    bundle = add_remote("cluster", template="mount", project=project)
    roots = {"mount_root": str(mount.mount_root), "remote_root": str(mount.remote_root)}

    with pytest.raises(RuntimeError, match="exec_command"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {**roots, "exec_command": ""}})
    with pytest.raises(RuntimeError, match="neither on PATH nor an existing file"):
        run_adapter(
            bundle,
            "configure",
            {"remote_settings": {}, "settings": {**roots, "exec_command": "definitely-not-a-real-program"}},
        )


def test_configure_checks_the_mount_and_the_executor(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-configure")
    bundle = _mount_remote(project, mount)

    result = run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})
    assert result["configured"] is True
    assert result["connectivity"] == "ok"
    assert result["mount"] == "ok"


def test_configure_refuses_a_missing_mount_unless_disabled(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-missing")
    absent = mount.mount_root.parent / "not-mounted-yet"
    bundle = _mount_remote(project, mount, mount_root=str(absent))

    with pytest.raises(RuntimeError, match="set check_mount=no"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})
    skipped = run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {"check_mount": "no"}})
    assert skipped["mount"] == "skipped"
    assert skipped["connectivity"] == "ok"


def test_configure_refuses_an_unreachable_executor_unless_disabled(
    tmp_path: Path, mount: Mount, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-unreachable")
    bundle = _mount_remote(project, mount)
    monkeypatch.setenv("HTTK_FAKE_EXEC_REFUSE", "1")

    with pytest.raises(RuntimeError, match="cannot reach the mount remote through exec_command"):
        run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {}})
    skipped = run_adapter(bundle, "configure", {"remote_settings": {}, "settings": {"check_connectivity": "no"}})
    assert skipped["connectivity"] == "skipped"


# 2-4. transfers through the mount --------------------------------------------


def test_push_and_pull_round_trip_a_real_tree_through_the_mount(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-round-trip")
    bundle = _mount_remote(project, mount)
    source = _tree(tmp_path / "source")
    destination = mount.remote_root / "runs" / "incoming"

    pushed = run_adapter(
        bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)}
    )

    # The result spells the destination in remote terms, and the bytes arrived at
    # that remote path (through the mount, which is the same tree).
    assert pushed["path"] == str(destination)
    assert (destination / "files" / "data with spaces.txt").read_text(encoding="utf-8") == "payload\n"
    assert stat.S_IMODE((destination / "files" / "runner").stat().st_mode) == 0o750
    assert os.readlink(destination / "link") == "files/runner"

    back = tmp_path / "back"
    pulled = run_adapter(bundle, "pull", {"remote_settings": {}, "source": str(destination), "destination": str(back)})

    assert pulled["path"] == str(back)
    assert (back / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert stat.S_IMODE((back / "files" / "runner").stat().st_mode) == 0o750
    assert os.readlink(back / "link") == "files/runner"
    # Nothing crossed the executor: transfers are bytes only.
    assert mount.commands() == []


def test_push_transfers_only_the_requested_files(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-batched")
    bundle = _mount_remote(project, mount)
    source = _tree(tmp_path / "source")
    destination = mount.remote_root / "runs" / "batched"

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


def test_push_refuses_a_destination_outside_the_mounted_tree(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-escape-push")
    bundle = _mount_remote(project, mount)
    source = _tree(tmp_path / "source")
    outside = tmp_path / "outside" / "runs"

    with pytest.raises(RuntimeError, match="outside the mounted tree"):
        run_adapter(bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(outside)})
    assert not outside.exists()


def test_pull_refuses_a_source_outside_the_mounted_tree(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-escape-pull")
    bundle = _mount_remote(project, mount)
    outside = tmp_path / "outside" / "secret"
    back = tmp_path / "back"

    with pytest.raises(RuntimeError, match="outside the mounted tree"):
        run_adapter(bundle, "pull", {"remote_settings": {}, "source": str(outside), "destination": str(back)})
    assert not back.exists()


def test_push_refuses_an_unmounted_mount_root(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-unmounted")
    absent = tmp_path / "not-mounted-yet"
    bundle = _mount_remote(project, mount, mount_root=str(absent))
    source = _tree(tmp_path / "source")
    destination = absent / "runs" / "x"

    with pytest.raises(RuntimeError, match="is the filesystem mounted"):
        run_adapter(bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)})
    assert not absent.exists()


def test_push_refuses_a_symlink_that_leaves_the_mount(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-symlink-escape")
    bundle = _mount_remote(project, mount)
    escape_target = tmp_path / "outside-mount"
    escape_target.mkdir()
    # An absolute symlink inside the mounted tree that points out of it: a lexical
    # map alone would let a push write to escape_target.
    (mount.remote_root / "leak").symlink_to(escape_target)
    source = _tree(tmp_path / "source")
    destination = mount.remote_root / "leak" / "x"

    with pytest.raises(RuntimeError, match="leaves the mount through a symlink"):
        run_adapter(bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)})
    assert not (escape_target / "x").exists()


def test_transfers_map_remote_paths_onto_a_real_mount_dir(tmp_path: Path, mount: Mount) -> None:
    # A real mount directory and a remote_root that does not exist locally: if the
    # mapping returned the remote path unchanged, the copy would create remote_root
    # instead of landing under the mount, which this asserts it never does.
    project = tmp_path / "project"
    initialize_project(project, name="mount-mapping")
    real_mount = tmp_path / "real-mnt"
    real_mount.mkdir()
    remote_root = tmp_path / "remote-view"  # deliberately never created
    bundle = _mount_remote(project, mount, mount_root=str(real_mount), remote_root=str(remote_root))
    source = _tree(tmp_path / "source")
    destination = remote_root / "runs" / "x"

    pushed = run_adapter(
        bundle, "push", {"remote_settings": {}, "source": str(source), "destination": str(destination)}
    )

    assert pushed["path"] == str(destination)
    assert (real_mount / "runs" / "x" / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert not remote_root.exists()

    back = tmp_path / "back"
    pulled = run_adapter(bundle, "pull", {"remote_settings": {}, "source": str(destination), "destination": str(back)})

    assert pulled["path"] == str(back)
    assert (back / "files" / "runner").read_text(encoding="utf-8") == _RUNNER
    assert not remote_root.exists()


# 5. invoke -------------------------------------------------------------------


def test_invoke_keeps_quoting_hostile_arguments_intact(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-quoting")
    bundle = _mount_remote(project, mount)
    hostile = ["a b", "$HOME", "`id`", "'single'", '"double"', "semi;colon", "star*", "new\nline", "back\\slash"]
    sentinel = mount.remote_root / "must-not-exist"

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


def test_invoke_honours_the_requested_directory(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-cwd")
    bundle = _mount_remote(project, mount)
    directory = mount.remote_root / "a directory"
    directory.mkdir()

    invoked = run_adapter(bundle, "invoke", {"remote_settings": {}, "argv": ["pwd"], "cwd": str(directory)})

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == str(directory)


def test_invoke_runs_the_prelude_before_the_command(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-prelude")
    bundle = _mount_remote(project, mount, prelude="export HTTK_PRELUDE_MARKER=ran")

    invoked = run_adapter(
        bundle,
        "invoke",
        {"argv": ["python3", "-c", "import os; print(os.environ.get('HTTK_PRELUDE_MARKER', 'absent'))"]},
    )

    assert invoked["returncode"] == 0
    assert str(invoked["stdout"]).strip() == "ran"


def test_invoke_prelude_failure_aborts_before_the_command(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-prelude-fail")
    bundle = _mount_remote(project, mount, prelude="false")
    sentinel = mount.remote_root / "prelude-should-not-reach-this"

    invoked = run_adapter(bundle, "invoke", {"argv": ["touch", str(sentinel)]})

    assert invoked["returncode"] != 0
    assert not sentinel.exists()


# 6. status -------------------------------------------------------------------


def test_status_returns_the_remote_workspace_json(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-status")
    workspace = Workspace.initialize(mount.remote_root / "runs" / "workspace")
    bundle = _mount_remote(project, mount)

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


# 7. check --------------------------------------------------------------------


def test_check_finds_the_remote_httk_through_the_executor(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-check-found")
    bundle = _mount_remote(project, mount)

    result = run_adapter(bundle, "install", {"remote_settings": {}})

    assert result["installed"] is True
    assert result["httk_command"] == ["httk"]
    assert str(result["httk_version"]).strip()
    assert "workspace_created" not in result


def test_check_refuses_when_the_executor_cannot_reach(
    tmp_path: Path, mount: Mount, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-check-unreachable")
    bundle = _mount_remote(project, mount)
    monkeypatch.setenv("HTTK_FAKE_EXEC_REFUSE", "1")

    with pytest.raises(RuntimeError, match="cannot reach the mount remote through exec_command"):
        run_adapter(bundle, "install", {"remote_settings": {}})


def test_check_reports_a_missing_remote_httk_with_the_remedy(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-check-missing")
    bundle = _mount_remote(project, mount, httk_command=str(tmp_path / "nowhere" / "httk"))

    with pytest.raises(RuntimeError, match="pipx install httk-workflow"):
        run_adapter(bundle, "install", {"remote_settings": {}})


# 8. end to end ---------------------------------------------------------------


def test_a_job_reaches_a_mount_workspace_and_runs_there(tmp_path: Path, mount: Mount) -> None:
    source_root = tmp_path / "project"
    initialize_project(source_root, name="mount-end-to-end")
    Workspace.initialize(source_root)
    destination = Workspace.initialize(mount.remote_root / "runs" / "workspace")
    _mount_remote(source_root, mount)
    payload, job_id = _payload(tmp_path / "incoming")
    Workspace(source_root).submit(payload, "jobs")
    context = CLIContext("httk", source_root)
    register_ws(context, source_root, "home")
    register_ws(context, destination.root, "station", remote="cluster")

    assert command(["transfer", "--job", job_id, "home", "cluster:station"], context) == 0

    assert Workspace(source_root).find_marker_by_id(job_id) is None
    marker = destination.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "submitted"
    with TaskManager(destination, heartbeat_interval=0.01) as manager:
        manager.run_until_idle()
    finished = destination.find_marker_by_id(job_id)
    assert finished is not None and finished.kind == "succeeded"
    # The workspace status and the remote receive really crossed the executor,
    # while the bundle bytes moved through the mount rather than the executor.
    commands = mount.commands()
    assert any("workspace status --json station" in item for item in commands)
    assert any("transfer receive --workspace station --bundle" in item for item in commands)
    assert not any(item.startswith("rsync ") or " rsync " in item for item in commands)


# 9. refusal still holds with three kinds -------------------------------------


def test_an_unrecognized_adapter_kind_still_refuses(tmp_path: Path, mount: Mount) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="mount-unknown")
    bundle = _mount_remote(project, mount)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["kind"] = "torque"
    (bundle / "remote.json").write_text(json.dumps(metadata, sort_keys=True), encoding="utf-8")

    with pytest.raises(RuntimeError, match="local, ssh, mount"):
        run_adapter(bundle, "status", {"remote_settings": {}, "argv": ["true"]})
