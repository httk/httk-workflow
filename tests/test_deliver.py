"""The far side of a push in one remote start: the ``deliver`` adapter operation and ``job adopt --stdin-tar``.

``deliver`` streams a batch as a tar on the stdin of one ``httk`` command over one connection; ``job adopt
--stdin-tar`` extracts such a stream into a fresh landing with the pull's filters and adopts every bundle in it,
reporting this workspace's id and the state of the workflows the sender asks about.
"""

import io
import json
import os
import sys
import tarfile
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from conftest import Remote, fake_remote
from httk.workflow import Workspace, _moving, _store
from httk.workflow.adapters import run_adapter
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command
from test_job_transfer import jobs, registered
from test_mount_adapter import Mount, _mount_remote, mount  # noqa: F401
from test_moving import assert_in, claim_root, find
from test_transfer_workflows import GIT_ID, workflows
from v3_helpers import cli_owner

LOCATOR = "cluster:far"


def hold_jobs(
    source: Workspace, count: int, *, destination_id: str | None = None
) -> tuple[list[_moving.Hold], list[tuple[dict[str, Any], str, int]]]:
    """Hold *count* single jobs of *source* for :data:`LOCATOR`, recording *destination_id*."""

    plan = jobs(source, count)
    with cli_owner(source) as owner:
        holds = [
            _moving.hold(
                source,
                owner,
                claim_root(source, owner, mapping["id"]),
                tree=True,
                destination_locator=LOCATOR,
                destination_workspace_id=destination_id,
            )
            for mapping, _, _ in plan
        ]
    return holds, plan


def outgoing_dir(workspace: Workspace) -> Path:
    return workspace.control / "transfers" / "outgoing"


def incoming(workspace: Workspace) -> list[str]:
    directory = workspace.control / "transfers" / "incoming"
    return sorted(os.listdir(directory)) if directory.exists() else []


def deliver(bundle: Path, source: Workspace, holds: Sequence[_moving.Hold], *argv: str) -> dict[str, Any]:
    return run_adapter(
        bundle,
        "deliver",
        {
            "source": str(outgoing_dir(source)),
            "files": [hold.transfer_id for hold in holds],
            "argv": ["httk", "job", "adopt", "--json", "--stdin-tar", *argv],
        },
    )


def stream(base: Path, names: Sequence[str], extra: Sequence[tarfile.TarInfo] = ()) -> bytes:
    """A tar of *names* below *base*, then the hand-made *extra* members."""

    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name in names:
            archive.add(base / name, arcname=name)
        for member in extra:
            archive.addfile(member, io.BytesIO(b"x" * member.size) if member.isreg() else None)
    return data.getvalue()


def adopt_stream(monkeypatch: pytest.MonkeyPatch, cwd: Path, data: bytes, *argv: str) -> int:
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    return command(["job", "adopt", "--json", "--no-durable", "--stdin-tar", *argv], CLIContext("httk", cwd))


# -- deliver --------------------------------------------------------------------------------------------------------


def test_ssh_deliver_adopts_a_batch_over_one_connection(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="deliver-ssh")
    bundle = fake_remote(project)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    holds, plan = hold_jobs(near, 2)
    before = len(remote.commands())
    result = deliver(bundle, near, holds, "--workspace", far_name, "--built-for", GIT_ID)
    assert set(result) >= {"returncode", "stdout", "stderr"} and result["returncode"] == 0, result
    (connection,) = remote.commands()[before:]
    assert connection.startswith("httk job adopt --json --stdin-tar ")  # no remote tar
    document = json.loads(result["stdout"])
    assert [entry["status"] for entry in document["bundles"]] == ["adopted", "adopted"]
    assert sorted(entry["bundle"] for entry in document["bundles"]) == sorted(hold.transfer_id for hold in holds)
    assert document["workspace_id"] == far.workspace_id
    assert document["workflows"] == [{"id": GIT_ID, "status": "missing", "tree_sha256": None}]
    assert_in(far, plan)
    assert incoming(far) == []
    # Delivery is not release: the holds stay until the client releases them.
    assert sorted(hold.transfer_id for hold in _moving.held(near)) == sorted(hold.transfer_id for hold in holds)


def test_a_prelude_that_prints_before_the_stream_is_read_cannot_stall_a_delivery(
    tmp_path: Path, remote: Remote
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="deliver-chatty")
    # 300 kB on stdout before the command starts reading, against a stream of more than 300 kB.
    bundle = fake_remote(project, cli_prelude="head -c 300000 /dev/zero | tr '\\0' x; echo")
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    holds, plan = hold_jobs(near, 1)
    (job_dir,) = [path.parent for path in Path(holds[0].path).rglob("input.txt")]
    (job_dir / "payload.bin").write_bytes(os.urandom(300_000))
    result = run_adapter(
        bundle,
        "deliver",
        {
            "source": str(outgoing_dir(near)),
            "files": [holds[0].transfer_id],
            "argv": ["httk", "job", "adopt", "--json", "--stdin-tar", "--workspace", far_name],
        },
        timeout=30,
    )
    banner, _, answer = result["stdout"].partition("\n")
    assert banner == "x" * 300_000 and result["returncode"] == 0
    assert [entry["status"] for entry in json.loads(answer)["bundles"]] == ["adopted"]
    assert_in(far, plan)


@pytest.mark.parametrize("kind", ["local", "mount"])
def test_local_and_mount_kinds_deliver_the_same_way(
    tmp_path: Path,
    kind: str,
    mount: Mount,  # noqa: F811
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name=f"deliver-{kind}")
    if kind == "local":
        bundle = fake_remote(project, template="local")
        root = tmp_path
    else:
        bundle = _mount_remote(project, mount)
        root = mount.remote_root
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(root, "far")
    holds, plan = hold_jobs(near, 2)
    result = deliver(bundle, near, holds, "--workspace", far_name)
    assert result["returncode"] == 0, result
    document = json.loads(result["stdout"])
    assert [entry["status"] for entry in document["bundles"]] == ["adopted", "adopted"]
    assert "workflows" not in document
    assert_in(far, plan)
    assert incoming(far) == []
    if kind == "mount":
        (line,) = mount.commands()
        assert "job adopt --json --stdin-tar" in line


def test_a_failing_far_side_command_returns_its_status_and_stderr(tmp_path: Path, remote: Remote) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="deliver-fails")
    bundle = fake_remote(project)
    near, _ = registered(tmp_path, "near")
    holds, _ = hold_jobs(near, 1)
    result = deliver(bundle, near, holds, "--workspace", "no-such-workspace")
    assert result["ok"] is True and result["returncode"] == 2
    assert "no-such-workspace" in result["stderr"]
    assert json.loads(result["stdout"])["refused"] and json.loads(result["stdout"])["workspace_id"] is None
    assert [hold.transfer_id for hold in _moving.held(near)] == [holds[0].transfer_id]


def test_a_refused_connection_is_reported_with_the_login_hint(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="deliver-refused")
    bundle = fake_remote(project)
    near, _ = registered(tmp_path, "near")
    holds, _ = hold_jobs(near, 1)
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    result = deliver(bundle, near, holds, "--workspace", "far")
    assert result["returncode"] == 255 and "Connection refused" in result["stderr"]
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "someone@fake.example.test: Permission denied (publickey).")
    result = deliver(bundle, near, holds, "--workspace", "far")
    assert result["returncode"] == 255 and "interactive_auth=yes" in result["stderr"]


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"argv": ["sh", "-c", "cat"]}, "httk command only"),
        ({"files": []}, "nonempty files batch"),
        ({"files": ["../elsewhere"]}, "not workspace relative"),
        ({"files": ["/etc"]}, "not workspace relative"),
        ({"source": ""}, "nonempty source"),
    ],
)
def test_a_malformed_deliver_request_is_refused(tmp_path: Path, change: dict[str, object], message: str) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="deliver-malformed")
    bundle = fake_remote(project, template="local")
    (tmp_path / "batch" / "item").mkdir(parents=True)
    request = {"source": str(tmp_path / "batch"), "files": ["item"], "argv": ["httk", "--version"], **change}
    with pytest.raises(RuntimeError, match=message):
        run_adapter(bundle, "deliver", request)


# -- job adopt --stdin-tar ------------------------------------------------------------------------------------------


@pytest.mark.parametrize("hostile", ["dotdot", "hardlink", "absolute-hardlink", "top-level-file", "top-level-link"])
def test_a_hostile_member_refuses_the_whole_stream(
    tmp_path: Path, hostile: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    (tmp_path / "outside").write_text("secret", encoding="utf-8")
    holds, plan = hold_jobs(near, 1)
    member = tarfile.TarInfo(holds[0].transfer_id + "/stray")
    if hostile == "dotdot":
        member = tarfile.TarInfo("../escaped")
        member.size = 1
    elif hostile in ("hardlink", "absolute-hardlink"):
        member.type = tarfile.LNKTYPE
        member.linkname = "../../../../../outside" if hostile == "hardlink" else str(tmp_path / "outside")
    elif hostile == "top-level-file":
        member = tarfile.TarInfo("loose")
        member.size = 1
    else:
        member = tarfile.TarInfo("alias")
        member.type, member.linkname = tarfile.SYMTYPE, str(outgoing_dir(near) / holds[0].transfer_id)
    data = stream(outgoing_dir(near), [holds[0].transfer_id], [member])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert document["bundles"] == [] and document["workspace_id"] == far.workspace_id
    assert document["refused"].startswith("refused the stream, nothing of it was used")
    assert find(far, plan[0][0]["id"]) is None and incoming(far) == []
    assert not (far.control / "transfers" / "escaped").exists()
    assert (tmp_path / "outside").read_text(encoding="utf-8") == "secret"


def test_an_absolute_symlink_in_a_job_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    (hold,) = holds
    (job_dir,) = [path.parent for path in Path(hold.path).rglob("input.txt")]
    (job_dir / "elsewhere").symlink_to("/etc/hostname")
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 0
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "adopted"
    adopted = find(far, plan[0][0]["id"])
    assert adopted is not None and os.readlink(adopted.path / "elsewhere") == "/etc/hostname"


def test_a_bundle_bound_for_another_workspace_is_refused_and_the_rest_adopted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    other = str(uuid.uuid4())
    foreign, foreign_plan = hold_jobs(near, 1, destination_id=other)
    ours, our_plan = hold_jobs(near, 1, destination_id=far.workspace_id)
    unbound, unbound_plan = hold_jobs(near, 1)
    names = [hold.transfer_id for hold in (*foreign, *ours, *unbound)]
    assert adopt_stream(monkeypatch, tmp_path, stream(outgoing_dir(near), names), "--workspace", far_name) == 1
    captured = capsys.readouterr()
    statuses = {entry["bundle"]: entry for entry in json.loads(captured.out)["bundles"]}
    assert statuses[foreign[0].transfer_id]["status"] == "refused"
    assert other in statuses[foreign[0].transfer_id]["message"] and far.workspace_id in captured.err
    assert statuses[ours[0].transfer_id]["status"] == statuses[unbound[0].transfer_id]["status"] == "adopted"
    assert find(far, foreign_plan[0][0]["id"]) is None
    assert_in(far, [*our_plan, *unbound_plan])
    assert incoming(far) == []  # the refused copy was discarded with the landing


def test_built_for_reports_present_unbuilt_and_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    caller = workflows(far, tmp_path, built=False)
    holds, _ = hold_jobs(near, 1)
    asked = ["--built-for", "local:caller", "--built-for", GIT_ID, "--built-for", "local:absent"]
    data = stream(outgoing_dir(near), [holds[0].transfer_id])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name, *asked) == 0
    document = json.loads(capsys.readouterr().out)
    callee = _store.lookup(far, GIT_ID)
    assert callee is not None
    assert document["workflows"] == [
        {"id": "local:caller", "status": "present", "tree_sha256": caller.record["tree_sha256"]},
        {"id": GIT_ID, "status": "unbuilt", "tree_sha256": callee.record["tree_sha256"]},
        {"id": "local:absent", "status": "missing", "tree_sha256": None},
    ]


def test_a_token_reuses_its_landing_and_an_empty_stream_adopts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    (hold,) = holds
    token = "abcdefghijklmnop"
    # A first delivery broke off after the manifest: its partial bundle waits in the token's landing.
    partial = far.control / "transfers" / "incoming" / token / hold.transfer_id
    partial.mkdir(parents=True)
    (partial / "bundle.json").write_bytes((Path(hold.path) / "bundle.json").read_bytes())
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name, "--token", token) == 0
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "adopted" and entry["bundle"] == hold.transfer_id
    assert_in(far, plan)
    assert incoming(far) == []
    # A retry after the adoption finds the jobs there already.
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name, "--token", token) == 0
    assert json.loads(capsys.readouterr().out)["bundles"][0]["status"] == "already_adopted"
    assert incoming(far) == []
    assert adopt_stream(monkeypatch, tmp_path, stream(tmp_path, []), "--workspace", far_name) == 0
    assert json.loads(capsys.readouterr().out) == {"bundles": [], "workspace_id": far.workspace_id}
    assert incoming(far) == []


@pytest.mark.parametrize(
    "argv",
    [
        ["--token", "../escape"],
        ["--token", "short"],
        ["some-bundle"],
    ],
)
def test_bad_stdin_tar_invocations_are_refused(
    tmp_path: Path, argv: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    far, far_name = registered(tmp_path, "far")
    assert adopt_stream(monkeypatch, tmp_path, stream(tmp_path, []), "--workspace", far_name, *argv) == 2
    # Refused before anything was adopted: the one document says so.
    document = json.loads(capsys.readouterr().out)
    assert document["bundles"] == [] and document["workspace_id"] in (far.workspace_id, None) and document["refused"]
    assert incoming(far) == []
    context = CLIContext("httk", tmp_path)
    assert command(["job", "adopt", "--workspace", far_name], context) == 2
    assert command(["job", "adopt", "--workspace", far_name, "--token", "abcdefghijklmnop", "x"], context) == 2
    assert "--stdin-tar" in capsys.readouterr().err


def test_an_unknown_workspace_is_refused_in_the_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [holds[0].transfer_id])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", "no-such-workspace") == 2
    document = json.loads(capsys.readouterr().out)
    assert document["bundles"] == [] and document["workspace_id"] is None
    assert "no-such-workspace" in document["refused"]
    # Without --json the error is the usual one, and no document.
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    context = CLIContext("httk", tmp_path)
    assert command(["job", "adopt", "--stdin-tar", "--workspace", "no-such-workspace"], context) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and "no-such-workspace" in captured.err


def test_a_ready_job_adopted_before_its_workflow_waits_and_runs_once_installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    package = v3.install(near, tmp_path / "package")
    ref = v3.submit(near, package, {"start": "succeed"}, tag="early", placement="proj/early")
    assert ref.state == "ready"
    with cli_owner(near) as owner:
        hold = _moving.hold(near, owner, claim_root(near, owner, ref.job_id), tree=True, destination_locator=LOCATOR)
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name, "--built-for", package.id) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["workflows"] == [{"id": package.id, "status": "missing", "tree_sha256": None}]
    assert document["bundles"][0]["missing_workflows"] == [package.id]
    # A manager pass skips the job (it is not failed) while its workflow is not installed.
    v3.run(far)
    waiting = find(far, ref.job_id)
    assert waiting is not None and waiting.state == "ready"
    with cli_owner(far) as owner:
        _store.install(far, owner, package.package)
    v3.run(far)
    done = find(far, ref.job_id)
    assert done is not None and done.state == "succeeded"
