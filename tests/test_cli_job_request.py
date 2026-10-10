"""CLI behavior of operator requests (v3 request files), the remote signing protocol, and pause waiting."""

import argparse
import json
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import (
    ensure_identity_key,
    identity_config_path,
    identity_key_paths,
    identity_public_key,
    sign_document,
    write_identity_config,
)

import v3_helpers as v3
from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


@pytest.fixture(autouse=True)
def _configured_default_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)
    write_identity_config(
        {
            "identities": {"tester": {"name": "Test User", "email": "tester@example.test"}},
            "default_identity": "tester",
        }
    )
    ensure_identity_key("tester")


def _new_workspace(tmp_path: Path) -> tuple[Workspace, str]:
    root = tmp_path / "workspace"
    name = f"cli-{uuid.uuid4().hex}"
    create_workspace(name, root)
    return Workspace(root), name


def _job(tmp_path: Path, workspace: Workspace, tag: str, placement: str | None = None) -> _kernel.JobRef:
    """Submit one job of the scriptable runner (installed on first use) that simply succeeds."""

    installed = _store.lookup(workspace, "demo") or v3.install(workspace, tmp_path / "package")
    return v3.submit(workspace, installed, {"start": "succeed"}, tag=tag, placement=placement or f"project/{tag}")


def _requests(workspace: Workspace) -> list[dict[str, object]]:
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(workspace.control.glob("requests/*.json"))]


def _request_args(workspace_name: str, *job_ids: str, action: str = "pause") -> list[str]:
    return ["job", "request", action, "--workspace", workspace_name, "--reason", "test request", *job_ids]


def test_repeatable_job_ids_post_one_request_and_path_each(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_ids = [_job(tmp_path, workspace, tag).job_id for tag in ("first", "second")]

    assert command(_request_args(workspace_name, *job_ids), _context(tmp_path)) == 0
    output = capsys.readouterr().out.splitlines()
    assert len(output) == 2 and all(Path(line).is_file() for line in output)
    requests = _requests(workspace)
    assert {request["job_id"] for request in requests} == set(job_ids)
    assert {request["format_version"] for request in requests} == {3}
    assert all("expected_generation" not in request and "job_key" not in request for request in requests)


def test_job_request_accepts_tag_prefix_and_directory_selectors(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    tagged = _job(tmp_path, workspace, "tagged", "jobs/first")
    nested = _job(tmp_path, workspace, "nested", "jobs/nested/second")

    assert command(_request_args(workspace_name, "tagged"), _context(tmp_path)) == 0
    assert [request["job_id"] for request in _requests(workspace)] == [tagged.job_id]
    for path in workspace.control.glob("requests/*.json"):
        path.unlink()
    assert command(_request_args(workspace_name, str(workspace.jobs), action="cancel"), _context(tmp_path)) == 0
    capsys.readouterr()
    assert {request["job_id"] for request in _requests(workspace)} == {tagged.job_id, nested.job_id}


def test_job_request_uses_default_workspace_with_one_job_id(tmp_path: Path, capsys) -> None:
    workspace = Workspace.default()
    job_id = _job(tmp_path, workspace, "default-workspace").job_id

    assert command(["job", "request", "pause", "--reason", "default workspace", job_id], _context(tmp_path)) == 0
    capsys.readouterr()
    assert [request["job_id"] for request in _requests(workspace)] == [job_id]


@pytest.mark.parametrize(
    ("action", "options", "message"),
    [
        ("cancel", ["--step", "start"], "--step is required by, and only valid with, the override_step action"),
        ("set_priority", [], "--priority is required by"),
        ("eject", [], "--destination is required by"),
        ("pause", ["--force"], "--force applies only to"),
    ],
)
def test_job_request_options_belong_to_their_actions(
    tmp_path: Path, capsys, action: str, options: list[str], message: str
) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "options").job_id

    assert command([*_request_args(workspace_name, job_id, action=action), *options], _context(tmp_path)) == 2
    assert message in capsys.readouterr().err
    assert not _requests(workspace)


def test_job_request_carries_the_action_options(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "options").job_id

    for action, options in (("set_priority", ["--priority", "700"]), ("eject", ["--destination", "exchange/outbox"])):
        assert command([*_request_args(workspace_name, job_id, action=action), *options], _context(tmp_path)) == 0
    capsys.readouterr()
    by_action = {request["action"]: request for request in _requests(workspace)}
    assert by_action["set_priority"]["priority"] == 700
    assert by_action["eject"]["destination"] == "exchange/outbox"


def test_the_removed_confirm_launches_ended_verb_is_refused(tmp_path: Path, capsys) -> None:
    # Operator attestation of a dead owner replaces it (workspace attest-dead OWNER).
    assert command(["job", "confirm-launches-ended", "x"], _context(tmp_path)) == 2
    assert "invalid choice: 'confirm-launches-ended'" in capsys.readouterr().err


def test_protocol_request_envelopes_and_publish_requests_are_verbatim(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "protocol")
    argv = ["job", "request-envelopes", "pause", "--workspace", workspace_name]
    argv += ["--operator=Test User <tester@example.test>", "--reason=protocol", "--json", ref.job_id]
    assert command(argv, _context(tmp_path)) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["format"] == "httk-workflow-request-envelopes" and document["format_version"] == 2
    assert document["job_keys"] == [ref.job_key]
    (envelope,) = document["envelopes"]
    assert envelope["format_version"] == 3 and envelope["placement"] == "project/protocol"
    signed = sign_document(envelope, seed_path=identity_key_paths("tester")[0])
    publish = ["job", "publish-requests", "--workspace", workspace_name, "--document", json.dumps(signed)]
    assert command(publish, _context(tmp_path)) == 0
    capsys.readouterr()
    assert _requests(workspace) == [signed]


def _document(job_id: str, placement: str = "project/x") -> dict[str, object]:
    return {
        "format": "httk-workflow-request",
        "format_version": 3,
        "request_id": str(uuid.uuid4()),
        "job_id": job_id,
        "placement": placement,
        "action": "pause",
        "operator": "Test User <tester@example.test>",
        "reason": "protocol",
        "created_at": "2026-01-01T00:00:00Z",
    }


def test_publish_requests_locates_all_jobs_before_posting_and_checks_signatures(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    first = _job(tmp_path, workspace, "first-protocol")
    publish = ["job", "publish-requests", "--workspace", workspace_name]
    documents = [_document(first.job_id, "project/first-protocol"), _document(str(uuid.uuid4()))]
    argv = [*publish, *(part for document in documents for part in ("--document", json.dumps(document)))]
    assert command(argv, _context(tmp_path)) == 2
    assert "job does not exist" in capsys.readouterr().err
    assert not _requests(workspace)

    forged = {**sign_document(documents[0], seed_path=identity_key_paths("tester")[0]), "reason": "changed"}
    assert command([*publish, "--document", json.dumps(forged)], _context(tmp_path)) == 2
    assert "signature" in capsys.readouterr().err
    assert not _requests(workspace)


def test_remote_envelope_correspondence_keeps_tag_prefixes_and_rejects_impersonation(tmp_path: Path) -> None:
    job_id = str(uuid.uuid4())
    key = f"correspondence--{job_id}"
    envelope = {**_document(job_id), "reason": "correspondence"}
    arguments = argparse.Namespace(
        job_id=["correspondence"],
        action="pause",
        reason="correspondence",
        priority=None,
        step=None,
        force=False,
        destination=None,
    )
    operator = "Test User <tester@example.test>"

    def document(envelope: dict[str, object], key: str) -> dict[str, object]:
        return {
            "format": "httk-workflow-request-envelopes",
            "format_version": 2,
            "envelopes": [envelope],
            "job_keys": [key],
        }

    assert job_cli._validate_remote_envelopes(document(envelope, key), arguments, operator) == [envelope]

    other_id = str(uuid.uuid4())
    arguments.job_id = [job_id]
    with pytest.raises(ValueError, match="UUID selector"):
        job_cli._validate_remote_envelopes(
            document({**envelope, "job_id": other_id}, f"{job_id}--{other_id}"), arguments, operator
        )
    with pytest.raises(ValueError, match="job key disagrees"):
        job_cli._validate_remote_envelopes(document(envelope, f"correspondence--{other_id}"), arguments, operator)
    with pytest.raises(ValueError, match="'action' disagrees"):
        job_cli._validate_remote_envelopes(document({**envelope, "action": "cancel"}, key), arguments, operator)


def test_default_operator_identity_is_recorded_and_signs_request(tmp_path: Path) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "default").job_id

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 0
    (request,) = _requests(workspace)
    assert request["operator"] == "Test User <tester@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths("tester")[0])
    assert "signature" in request


def test_configured_identity_without_key_fails_loudly(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "missing-key").job_id
    key_path = identity_key_paths("tester")[0]
    key_path.unlink()

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 2
    error = capsys.readouterr().err
    assert f"identity 'tester' has no key file at {key_path}" in error
    assert "identity remove tester" in error and "identity add tester" in error
    assert not _requests(workspace)


def test_named_operator_identity_selects_its_key(tmp_path: Path) -> None:
    write_identity_config(
        {
            "identities": {
                "tester": {"name": "Test User", "email": "tester@example.test"},
                "bot": {"name": "Build Bot", "email": "bot@example.test"},
            },
            "default_identity": "tester",
        }
    )
    ensure_identity_key("bot")
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "named").job_id

    assert command(_request_args(workspace_name, job_id) + ["--operator", "bot"], _context(tmp_path)) == 0
    (request,) = _requests(workspace)
    assert request["operator"] == "Build Bot <bot@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths("bot")[0])


def test_unknown_operator_identity_publishes_nothing(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "unknown").job_id

    assert command(_request_args(workspace_name, job_id) + ["--operator", "missing"], _context(tmp_path)) == 2
    assert "configured identities: tester" in capsys.readouterr().err
    assert not _requests(workspace)


def test_request_without_any_identity_is_refused(tmp_path: Path, capsys) -> None:
    identity_config_path().unlink()
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "no-identity").job_id

    assert command(_request_args(workspace_name, job_id), _context(tmp_path)) == 2
    error = capsys.readouterr().err
    assert "no operator identity is configured" in error and "run `httk init`" in error
    assert not _requests(workspace)


def test_literal_operator_label_is_passed_through(tmp_path: Path) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "literal").job_id

    argv = _request_args(workspace_name, job_id) + ["--operator", "Ext Person <ext@x>"]
    assert command(argv, _context(tmp_path)) == 0
    assert _requests(workspace)[0]["operator"] == "Ext Person <ext@x>"


def test_literal_operator_on_empty_machine_publishes_unsigned_request(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "empty-config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "empty-data"))
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "literal-empty").job_id

    assert command(_request_args(workspace_name, job_id) + ["--operator", "Ext <e@x>"], _context(tmp_path)) == 0
    (request,) = _requests(workspace)
    assert request["operator"] == "Ext <e@x>"
    assert "operator_key" not in request and "signature" not in request


def test_wait_returns_zero_when_manager_pauses_jobs(tmp_path: Path, capsys, monkeypatch) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_ids = [_job(tmp_path, workspace, tag).job_id for tag in ("first", "second")]

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        monkeypatch.setattr(job_cli.time, "sleep", lambda _: manager.tick())
        assert command(_request_args(workspace_name, *job_ids) + ["--wait"], _context(tmp_path)) == 0

    output = capsys.readouterr().out
    assert output.count(": paused") == 2


def test_wait_reports_terminal_state_and_exits_one(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "finished").job_id
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60)
        assert v3.find(workspace, job_id).state == "succeeded"
        assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 1

    assert "succeeded (pause superseded)" in capsys.readouterr().out


def test_wait_without_live_manager_fails_after_publishing(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "unserved").job_id

    assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 1
    captured = capsys.readouterr()
    assert "no live manager currently serves claim pool 'default'" in captured.err
    assert "waiting is pointless until a manager starts" in captured.err
    assert len(_requests(workspace)) == 1


def test_wait_timeout_names_pending_job_and_keeps_request(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "pending").job_id

    with TaskManager(workspace, heartbeat_interval=0.01):
        argv = _request_args(workspace_name, job_id) + ["--wait", "--timeout", "0.01"]
        assert command(argv, _context(tmp_path)) == 1

    captured = capsys.readouterr()
    assert "timeout" in captured.out and job_id in captured.out
    assert _requests(workspace)


def test_wait_is_rejected_for_non_pause_action(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "cancel").job_id

    assert command(_request_args(workspace_name, job_id, action="cancel") + ["--wait"], _context(tmp_path)) == 2
    assert "--wait is only valid with the pause action" in capsys.readouterr().err
