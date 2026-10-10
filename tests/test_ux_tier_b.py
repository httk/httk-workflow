"""Operator-visible job CLI guarantees: unserved requests are named, bad steps refused, payloads submitted.

Every test pins one thing an operator sees: a request no live manager can
apply says so, an override onto a step the runner never recorded is refused
before anything is posted, a prepared payload is submitted (copied or moved)
on the filesystem kernel, and ``job log`` shows the owner's run log together
with the runner's own annotations.
"""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

import v3_helpers as v3
from conftest import configure_identity, register_ws
from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow.workflow_cli import command


@pytest.fixture()
def context(tmp_path: Path) -> CLIContext:
    configure_identity()
    return CLIContext("httk", tmp_path)


def _setup(tmp_path: Path, context: CLIContext, name: str) -> tuple[Workspace, _store.Installed, str]:
    workspace = v3.workspace(tmp_path / "ws")
    installed = v3.install(workspace, tmp_path / "package")
    return workspace, installed, register_ws(context, workspace.root, name)


def _request(name: str, job_id: str, action: str, *options: str) -> list[str]:
    return ["job", "request", action, "--workspace", name, job_id, "--reason", "x", *options]


def test_run_leaf_capability_claims_a_gated_job(tmp_path: Path, context: CLIContext, capsys) -> None:
    workspace, installed, name = _setup(tmp_path, context, "gated-ws")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, capabilities=("docker",))

    assert command(["run", "--workspace", name, "--capability", "docker", "--idle-timeout", "20"], context) == 0
    assert v3.find(workspace, ref.job_id).state == "succeeded"


def test_job_request_warns_when_no_live_manager_serves_the_pool(tmp_path: Path, context: CLIContext, capsys) -> None:
    workspace, installed, name = _setup(tmp_path, context, "orphan-ws")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, pool="vasp")

    assert command(_request(name, ref.job_id, "cancel"), context) == 0
    assert "no live manager currently serves claim pool 'vasp'" in capsys.readouterr().err
    with TaskManager(workspace, heartbeat_interval=0.01, pools=["vasp"]):
        assert command(_request(name, ref.job_id, "cancel"), context) == 0
        assert "no live manager currently serves" not in capsys.readouterr().err


def _failed_with_steps(workspace: Workspace, installed: _store.Installed) -> _kernel.JobRef:
    """A failed job whose runner recorded the steps ``start`` and ``other``."""

    ref = v3.submit(workspace, installed, {"start": "fail"}, parameters={"runner_steps": ["start", "other"]})
    v3.run(workspace)
    failed = v3.find(workspace, ref.job_id)
    assert failed.state == "failed" and v3.state_of(failed).runner_steps == ("start", "other")
    return failed


def test_override_step_is_refused_client_side_against_recorded_runner_steps(
    tmp_path: Path, context: CLIContext, capsys
) -> None:
    workspace, installed, name = _setup(tmp_path, context, "steps-ws")
    failed = _failed_with_steps(workspace, installed)
    capsys.readouterr()

    assert command(_request(name, failed.job_id, "override_step", "--step", "bogus"), context) == 2
    assert "does not implement the step 'bogus'" in capsys.readouterr().err
    assert not list(workspace.control.glob("requests/*.json"))

    # A runner may have been reinstalled with a recovery step: --force downgrades the refusal.
    assert command(_request(name, failed.job_id, "override_step", "--step", "recover", "--force"), context) == 0
    error = capsys.readouterr().err
    assert "--force was given" in error and "the runner will refuse it at the next attempt" in error


def test_override_step_is_allowed_with_a_note_before_runner_steps_are_recorded(
    tmp_path: Path, context: CLIContext, capsys
) -> None:
    workspace, installed, name = _setup(tmp_path, context, "unstarted-ws")
    ref = v3.submit(workspace, installed, {"start": "succeed"})

    assert command(_request(name, ref.job_id, "override_step", "--step", "whatever"), context) == 0
    assert "could not be pre-validated" in capsys.readouterr().err


def _payload(tmp_path: Path, installed: _store.Installed, tag: str) -> Path:
    payload = tmp_path / "prepared" / tag
    payload.mkdir(parents=True)
    (payload / "job.json").write_text(
        json.dumps(v3.job_mapping(installed, {"start": "succeed"}, tag=tag, placement="prepared")), encoding="utf-8"
    )
    (payload / "input.txt").write_text(tag, encoding="utf-8")
    return payload


def test_job_submit_copies_or_moves_a_prepared_payload_and_it_runs(tmp_path: Path, context: CLIContext, capsys) -> None:
    workspace, installed, name = _setup(tmp_path, context, "submit-ws")
    copied, moved = _payload(tmp_path, installed, "copied"), _payload(tmp_path, installed, "moved")

    assert command(["job", "submit", "--workspace", name, "--json", str(copied)], context) == 0
    assert command(["job", "submit", "--workspace", name, "--move", "--json", str(moved)], context) == 0
    capsys.readouterr()
    assert copied.is_dir() and not moved.exists()
    ready = {ref.job_key.split("--")[0]: ref for ref in _kernel.list_jobs(workspace, "ready")}
    assert set(ready) == {"copied", "moved"}
    assert (ready["moved"].path / "input.txt").read_text(encoding="utf-8") == "moved"
    assert not [owner for owner in _kernel.list_owners(workspace) if owner.record is not None]
    v3.run(workspace)
    assert {ref.job_key.split("--")[0] for ref in _kernel.list_jobs(workspace, "succeeded")} == {"copied", "moved"}


def test_job_submit_refuses_owner_files_and_keeps_a_moved_payload_on_failure(
    tmp_path: Path, context: CLIContext, capsys
) -> None:
    workspace, installed, name = _setup(tmp_path, context, "submit-refusals")
    forged = _payload(tmp_path, installed, "forged")
    (forged / "state.json").write_text("{}", encoding="utf-8")
    assert command(["job", "submit", "--workspace", name, str(forged)], context) == 1
    assert "may not carry state.json" in capsys.readouterr().err

    # A placement directory that is a symlink makes the submit fail after the move: the payload comes back.
    moved = _payload(tmp_path, installed, "moved")
    (workspace.jobs / "ready").mkdir(parents=True, exist_ok=True)
    (workspace.jobs / "ready" / "prepared").symlink_to(tmp_path)
    assert command(["job", "submit", "--workspace", name, "--move", str(moved)], context) == 1
    assert (moved / "job.json").is_file()
    assert not list(_kernel.list_jobs(workspace, "ready"))


def test_job_log_shows_owner_events_and_runner_annotations(tmp_path: Path, context: CLIContext, capsys) -> None:
    workspace, installed, name = _setup(tmp_path, context, "log-ws")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, parameters={"headline": {"start": "half way"}})
    v3.run(workspace)
    capsys.readouterr()

    assert command(["job", "log", "--workspace", name, ref.job_id], context) == 0
    text = capsys.readouterr().out
    assert "claimed" in text and "released" in text and "runner:headline  half way" in text
    assert command(["job", "log", "--workspace", name, "--json", "--limit", "1", ref.job_id], context) == 0
    (document,) = json.loads(capsys.readouterr().out)
    assert document["format_version"] == 3 and document["job_id"] == ref.job_id
    assert len(document["events"]) == 1 and document["annotations"][0]["message"] == "half way"


def test_job_show_list_and_why_read_the_kernel_state(tmp_path: Path, context: CLIContext, capsys) -> None:
    workspace, installed, name = _setup(tmp_path, context, "show-ws")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, tag="shown", priority=700)

    assert command(["job", "show", "--workspace", name, "shown"], context) == 0
    text = capsys.readouterr().out
    assert f"job {ref.job_key} (ready)" in text and "700" in text
    assert command(["job", "list", "--workspace", name, "--json", "--counts", "--kind", "ready"], context) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["counts"] == {"ready": 1} and document["jobs"][0]["priority"] == 700
    assert document["jobs"][0]["token"] == ref.token
    assert command(["job", "why", "--workspace", name, "--json", ref.job_id], context) == 0
    assert json.loads(capsys.readouterr().out)[0]["state"] == "ready"


def test_job_eject_and_adopt_report_bad_arguments(tmp_path: Path, context: CLIContext, capsys) -> None:
    _workspace, _installed, name = _setup(tmp_path, context, "moving-ws")
    # An unknown job is a usage error; a directory that is no bundle is refused.
    assert command(["job", "eject", "--workspace", name, "job", str(tmp_path)], context) == 2
    assert "job" in capsys.readouterr().err
    (tmp_path / "not-a-bundle").mkdir()
    assert command(["job", "adopt", "--workspace", name, str(tmp_path / "not-a-bundle")], context) == 1
    assert "bundle refused" in capsys.readouterr().err
