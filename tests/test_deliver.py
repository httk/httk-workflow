"""The far side of a push: the transfer session's receiver (its ``stage`` and ``adopt_staged`` ops).

A pushed tar stream is extracted into a fresh landing with the pull's filters, committed, and every bundle in it
adopted, reporting this workspace's id; a stream refused whole adopts nothing. The helpers here are shared by the
transfer tests.
"""

import argparse
import io
import json
import os
import shutil
import tarfile
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from httk.workflow import Workspace, _moving, _store
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._common import _modifiable
from httk.workflow.workflow_cli._job import adopt_stage, stage_stream
from test_job_transfer import jobs, registered
from test_moving import assert_in, claim_root, find
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


def stream(base: Path, names: Sequence[str], extra: Sequence[tarfile.TarInfo] = ()) -> bytes:
    """A tar of *names* below *base*, then the hand-made *extra* members."""

    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name in names:
            archive.add(base / name, arcname=name)
        for member in extra:
            archive.addfile(member, io.BytesIO(b"x" * member.size) if member.isreg() else None)
    return data.getvalue()


def adopt_stream(cwd: Path, data: bytes, workspace: str, *, fsync: bool = False, durable: bool = False) -> int:
    """Receive *data* as the far side of a push does, in this process: the transfer session's ``stage``
    (:func:`~httk.workflow.workflow_cli._job.stage_stream`), then its ``adopt_staged``
    (:func:`~httk.workflow.workflow_cli._job.adopt_stage`), into the registered *workspace* (``--no-durable``
    unless *durable*). The document either answers is printed as JSON on standard output; the result is its exit
    status, 2 for a stream refused whole.

    :param fsync: A ``stage`` with ``sync: fsync``.
    """

    arguments = argparse.Namespace(workspace=workspace, by_path=False, no_durable=not durable)
    context = CLIContext("httk", cwd)
    document, stage = stage_stream(
        lambda: _modifiable(arguments, context, action="adopt jobs into it"), io.BytesIO(data), force_fsync=fsync
    )
    if stage is None:
        print(json.dumps(document))
        return 2
    adopted, returncode, _unclosed = adopt_stage(stage)
    print(json.dumps(adopted))
    return returncode


# -- the receiver ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("hostile", ["dotdot", "hardlink", "absolute-hardlink", "top-level-file", "top-level-link"])
def test_a_hostile_member_refuses_the_whole_stream(
    tmp_path: Path, hostile: str, capsys: pytest.CaptureFixture[str]
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
    assert adopt_stream(tmp_path, data, far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert document["bundles"] == [] and document["workspace_id"] == far.workspace_id
    assert document["refused"].startswith("refused the stream, nothing of it was used")
    assert find(far, plan[0][0]["id"]) is None and incoming(far) == []
    assert not (far.control / "transfers" / "escaped").exists()
    assert (tmp_path / "outside").read_text(encoding="utf-8") == "secret"


def test_an_absolute_symlink_in_a_job_is_kept(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    (hold,) = holds
    (job_dir,) = [path.parent for path in Path(hold.path).rglob("input.txt")]
    (job_dir / "elsewhere").symlink_to("/etc/hostname")
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(tmp_path, data, far_name) == 0
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "adopted"
    adopted = find(far, plan[0][0]["id"])
    assert adopted is not None and os.readlink(adopted.path / "elsewhere") == "/etc/hostname"


def test_a_bundle_bound_for_another_workspace_is_refused_and_the_rest_adopted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    other = str(uuid.uuid4())
    foreign, foreign_plan = hold_jobs(near, 1, destination_id=other)
    ours, our_plan = hold_jobs(near, 1, destination_id=far.workspace_id)
    unbound, unbound_plan = hold_jobs(near, 1)
    names = [hold.transfer_id for hold in (*foreign, *ours, *unbound)]
    assert adopt_stream(tmp_path, stream(outgoing_dir(near), names), far_name) == 1
    captured = capsys.readouterr()
    statuses = {entry["bundle"]: entry for entry in json.loads(captured.out)["bundles"]}
    assert statuses[foreign[0].transfer_id]["status"] == "refused"
    assert other in statuses[foreign[0].transfer_id]["message"] and far.workspace_id in captured.err
    assert statuses[ours[0].transfer_id]["status"] == statuses[unbound[0].transfer_id]["status"] == "adopted"
    assert find(far, foreign_plan[0][0]["id"]) is None
    assert_in(far, [*our_plan, *unbound_plan])
    assert incoming(far) == []  # the refused copy was discarded with the landing


def test_a_redelivery_is_already_adopted_and_an_empty_stream_adopts_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    (hold,) = holds
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(tmp_path, data, far_name) == 0
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "adopted" and entry["bundle"] == hold.transfer_id
    assert_in(far, plan)
    # A retry after the adoption (its answer lost) finds the jobs there already.
    assert adopt_stream(tmp_path, data, far_name) == 0
    assert json.loads(capsys.readouterr().out)["bundles"][0]["status"] == "already_adopted"
    assert adopt_stream(tmp_path, stream(tmp_path, []), far_name) == 0
    assert json.loads(capsys.readouterr().out) == {"bundles": [], "workspace_id": far.workspace_id}
    assert incoming(far) == []


def test_job_adopt_takes_bundles_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # The command-line receiver of the removed adapter path (``--stdin-tar``, ``--token``, ``--sync``,
    # ``--built-for``) is gone: only the transfer session receives a stream.
    _far, far_name = registered(tmp_path, "far")
    context = CLIContext("httk", tmp_path)
    assert command(["job", "adopt", "--workspace", far_name], context) == 2
    for removed in (["--stdin-tar"], ["--token", "abcdefghijklmnop"], ["--sync", "fsync"], ["--built-for", "x"]):
        capsys.readouterr()
        assert command(["job", "adopt", "--workspace", far_name, *removed, "some-bundle"], context) == 2
        assert "unrecognized arguments" in capsys.readouterr().err


def test_an_unknown_workspace_is_refused_in_the_document(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    near, _ = registered(tmp_path, "near")
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [holds[0].transfer_id])
    assert adopt_stream(tmp_path, data, "no-such-workspace") == 2
    document = json.loads(capsys.readouterr().out)
    assert document["bundles"] == [] and document["workspace_id"] is None
    assert "no-such-workspace" in document["refused"]


def test_a_ready_job_adopted_before_its_workflow_waits_and_runs_once_installed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    package = v3.install(near, tmp_path / "package")
    ref = v3.submit(near, package, {"start": "succeed"}, tag="early", placement="proj/early")
    assert ref.state == "ready"
    with cli_owner(near) as owner:
        hold = _moving.hold(near, owner, claim_root(near, owner, ref.job_id), tree=True, destination_locator=LOCATOR)
    data = stream(outgoing_dir(near), [hold.transfer_id])
    assert adopt_stream(tmp_path, data, far_name) == 0
    document = json.loads(capsys.readouterr().out)
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


def test_a_hold_copied_by_hand_adopts_as_a_bundle_and_its_release_completes_the_move(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # The manual remedy for a hold too large for one answer of a transfer session (plan WORKFLOW_TRANSFER_SESSION
    # §7.12, S1): copy transfers/outgoing/<id>/ by hand, `job adopt BUNDLE` it at the destination, then release it.
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1, destination_id=far.workspace_id)
    (hold,) = holds
    copy = tmp_path / "carried" / hold.transfer_id
    shutil.copytree(hold.path, copy, symlinks=True)
    context = CLIContext("httk", tmp_path)
    assert command(["job", "adopt", "--json", "--workspace", far_name, str(copy)], context) == 0
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "adopted"
    assert_in(far, plan)
    assert command(["job", "move", "--release", hold.transfer_id, near_name], context) == 0
    assert f"released {hold.transfer_id}" in capsys.readouterr().out
    assert _moving.held(near) == [] and all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_workspace_status_names_an_unreadable_hold_and_still_succeeds(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, name = registered(tmp_path, "a")
    good, damaged = hold_jobs(source, 2)[0]
    Path(damaged.path, "bundle.json").write_text("{not json", encoding="utf-8")
    context = CLIContext("httk", tmp_path)

    assert command(["workspace", "status", name], context) == 0
    out = capsys.readouterr().out
    assert f"held transfers: 1 (1 unreadable: {damaged.transfer_id})\n" in out and f"{good.transfer_id}\t" in out
    assert command(["workspace", "status", "--json", name], context) == 0
    (document,) = json.loads(capsys.readouterr().out)
    assert [hold["transfer_id"] for hold in document["transfers"]] == [good.transfer_id]
    assert document["transfers_unreadable"] == [damaged.transfer_id] and document["format_version"] == 3
