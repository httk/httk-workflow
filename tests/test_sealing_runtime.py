"""Runtime enforcement of seals: refused mutations, auto-sealing, and transfers.

These tests exercise the seal library wired into the write funnels, the
manager's auto-seal of succeeded jobs, garbage collection's protection of
sealed jobs, and the seal that travels with a detached transfer. The seal
library itself is covered by ``test_seals``; here every seal is produced or
enforced through the ordinary runtime paths an operator drives.
"""

import hashlib
from pathlib import Path

import pytest
from httk.core.project.sealing import seal_project

from conftest import configure_identity
from httk.workflow import Workspace, _kernel
from httk.workflow._state import Release, StateDoc
from httk.workflow.errors import SealedError
from httk.workflow.fsck import check_workspace
from httk.workflow.projects import initialize_project
from httk.workflow.removal import remove_jobs
from httk.workflow.seals import (
    is_job_sealed,
    is_workspace_sealed,
    job_seal_path,
    read_seal,
    require_cli_modifiable,
    seal_job,
    seal_workspace,
    verify_job_seal,
    verify_tree,
    verify_workspace_seal,
)
from v3_helpers import cli_owner, find, install, run, state_of, submit, workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


def _ran(tmp_path: Path, ws: Workspace) -> _kernel.JobRef:
    """Run one succeeding job through a real manager and return it."""

    ref = submit(ws, install(ws, tmp_path / "package"), {"start": "succeed"}, members={"files/runner": "x\n"})
    run(ws)
    done = find(ws, ref.job_id)
    assert done.state == "succeeded"
    return done


def _at(ws: Workspace, state: str, placement: str) -> _kernel.JobRef:
    """A job moved to *state* by a CLI owner, without a seal."""

    ref = submit(ws, _WORKFLOW, {"start": "succeed"}, placement=placement, members={"files/runner": "x\n"})
    with cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, ref)
        assert owned is not None
        return owned.release(StateDoc.empty(owned.job_id).next_activation("start", "initial"), Release(state, 500))


def _sealed(ws: Workspace, placement: str = "jobs/0") -> _kernel.JobRef:
    ref = _at(ws, "succeeded", placement)
    assert seal_job(ws, ref) is None
    return find(ws, ref.job_id)


# -- the seal step of a succeeded commit --------------------------------------


def test_manager_seals_a_succeeded_job(tmp_path: Path) -> None:
    configure_identity()
    ws = workspace(tmp_path / "workspace")
    done = _ran(tmp_path, ws)
    assert is_job_sealed(done.path) and verify_job_seal(done.path).valid
    doc = state_of(done)
    assert doc.seal is not None and doc.seal["signed"] is True
    assert doc.seal["sha256"] == hashlib.sha256(job_seal_path(done.path).read_bytes()).hexdigest()


def test_manager_does_not_seal_when_disabled_and_job_seal_repairs_it(tmp_path: Path) -> None:
    configure_identity()
    ws = workspace(tmp_path / "workspace")
    ws.set_setting("seal.succeeded", "false")
    done = _ran(tmp_path, ws)
    assert not is_job_sealed(done.path) and state_of(done).seal == {"disabled": True}
    assert seal_job(ws, done) is None
    sealed = find(ws, done.job_id)
    assert is_job_sealed(sealed.path) and verify_job_seal(sealed.path).valid


def test_without_a_signing_key_the_seal_is_unsigned(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    # No project and a project-only key ref: no signing key resolves, so the seal is unsigned.
    ws.set_setting("seal.keys", "project")
    done = _ran(tmp_path, ws)
    assert is_job_sealed(done.path) and state_of(done).seal == {
        "sha256": hashlib.sha256(job_seal_path(done.path).read_bytes()).hexdigest(),
        "signed": False,
    }
    assert list(read_seal(job_seal_path(done.path)).signatures) == []


# -- a succeeded job is protected until it is released -----------------------


def test_a_sealed_job_is_not_deleted_and_its_siblings_are(tmp_path: Path) -> None:
    configure_identity()
    ws = workspace(tmp_path / "workspace")
    sealed = _sealed(ws)
    failed = _at(ws, "failed", "jobs/1")
    report = remove_jobs(ws, [sealed, failed])
    assert [(outcome.job_key, outcome.removed) for outcome in report.outcomes] == [
        (sealed.job_key, False),
        (failed.job_key, True),
    ]
    assert "job unseal" in (report.outcomes[0].reason or "")
    assert find(ws, sealed.job_id).state == "succeeded"


# -- a workspace seal is a snapshot that blocks modifying CLI commands --------


def test_a_sealed_workspace_blocks_the_cli_and_stays_readable(tmp_path: Path) -> None:
    configure_identity()
    ws = workspace(tmp_path / "workspace")
    _sealed(ws)
    seal_workspace(ws)
    assert is_workspace_sealed(ws)
    with pytest.raises(SealedError, match="workspace is sealed"):
        require_cli_modifiable(ws)
    # Attach, gc and fsck keep working on a sealed workspace.
    Workspace(ws.root)
    ws.collect_garbage()
    assert check_workspace(ws).ok
    assert verify_workspace_seal(ws).valid


def test_sealed_project_refuses_new_workspace(tmp_path: Path) -> None:
    configure_identity()
    project = tmp_path / "project"
    initialize_project(project, name="sealed")
    ws = workspace(project / "work")
    _sealed(ws)
    seal_workspace(ws)
    seal_project(project)
    with pytest.raises(SealedError, match="sealed"):
        require_cli_modifiable(ws)
    with pytest.raises(SealedError, match="sealed"):
        Workspace.initialize(project / "work2")


def test_verify_tree_on_an_unsealed_subject_reports_not_sealed(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "workspace")
    submit(ws, _WORKFLOW, {"start": "succeed"})
    # No seal exists; verification reports it rather than leaking a file error.
    report = verify_tree(ws.root)
    assert not report.ok
    assert [(entry["level"], entry["reason"]) for entry in report.entries] == [("workspace", "not sealed")]
    assert verify_workspace_seal(ws).reason == "not sealed"


# -- project seals and postprocess output ------------------------------------


def _seal_root_project(tmp_path: Path, *, setting: str | None = None) -> tuple[Path, Workspace, _kernel.JobRef]:
    """Build a project with one sealed job, a sealed workspace and a sealed project."""

    configure_identity()
    project = tmp_path / "project"
    initialize_project(project, name="pp")
    ws = workspace(project / "workspace")
    if setting is not None:
        # The setting must be stored before sealing.
        ws.set_setting("postprocess.directory", setting)
    ref = _sealed(ws)
    seal_workspace(ws)
    seal_project(project)
    return project, ws, ref


def test_project_seal_excludes_the_default_postprocess_tree(tmp_path: Path) -> None:
    project, ws, ref = _seal_root_project(tmp_path)
    # Postprocess output lands under <root>/postprocess after sealing; it must not
    # count as a loose project file, or the project seal would break.
    out = ws.root / "postprocess" / "jobs" / ref.job_key / "plot"
    out.mkdir(parents=True)
    (out / "chart.svg").write_text("<svg/>\n", encoding="utf-8")
    assert verify_tree(project).ok


def test_project_seal_excludes_a_configured_postprocess_dir(tmp_path: Path) -> None:
    project, _ws, ref = _seal_root_project(tmp_path, setting=str(tmp_path / "project" / "reports"))
    out = project / "reports" / "jobs" / ref.job_key / "plot"
    out.mkdir(parents=True)
    (out / "chart.svg").write_text("<svg/>\n", encoding="utf-8")
    assert verify_tree(project).ok


# -- a seal travels with a transfer ------------------------------------------
# Detached transfers return on the kernel in phase D (eject + copy + adopt), and with them the tests that a job
# seal survives a transfer, is refused when changed in transit, and that a sealed workspace refuses an eject.
