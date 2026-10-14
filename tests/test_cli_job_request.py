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
from httk.workflow._job import JobDefinition
from httk.workflow._state import StateDoc, encode_state, read_state_unowned
from httk.workflow.registry import create_workspace
from httk.workflow.removal import serve_requests
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


def _applied(workspace: Workspace, job_id: str) -> list[dict[str, object]]:
    ref = _kernel.locate(workspace, job_id, placement_hint=None, exhaustive=True)
    assert ref is not None
    doc, _damage = read_state_unowned(ref.path / "state.json")
    assert doc is not None
    return [dict(entry) for entry in doc.history_tail if entry.get("event") in {"request_applied", "request_dropped"}]


def _request_args(workspace_name: str, *job_ids: str, action: str = "pause") -> list[str]:
    return ["job", "request", action, "--workspace", workspace_name, "--reason", "test request", *job_ids]


def test_repeatable_job_ids_post_one_request_and_path_each(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_ids = [_job(tmp_path, workspace, tag).job_id for tag in ("first", "second")]

    assert command(_request_args(workspace_name, *job_ids), _context(tmp_path)) == 0
    output = capsys.readouterr().out.splitlines()
    assert len(output) == 2 and all("\tapplied\t" in line for line in output)
    assert {line.split("\t", 1)[0] for line in output} == set(job_ids)
    assert not _requests(workspace)


def test_job_request_accepts_tag_prefix_and_directory_selectors(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    tagged = _job(tmp_path, workspace, "tagged", "jobs/first")
    nested = _job(tmp_path, workspace, "nested", "jobs/nested/second")

    assert command(_request_args(workspace_name, "tagged"), _context(tmp_path)) == 0
    assert v3.find(workspace, tagged.job_id).state == "paused"
    assert command(_request_args(workspace_name, str(workspace.jobs), action="cancel"), _context(tmp_path)) == 0
    capsys.readouterr()
    assert v3.find(workspace, tagged.job_id).state == "cancelled"
    assert v3.find(workspace, nested.job_id).state == "cancelled"


def test_job_request_uses_default_workspace_with_one_job_id(tmp_path: Path, capsys) -> None:
    workspace = Workspace.default()
    job_id = _job(tmp_path, workspace, "default-workspace").job_id

    assert command(["job", "request", "pause", "--reason", "default workspace", job_id], _context(tmp_path)) == 0
    capsys.readouterr()
    assert v3.find(workspace, job_id).state == "paused"


def test_request_id_is_rejected_for_local_jobs(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "request-id-local").job_id

    status = command(
        [*_request_args(workspace_name, job_id), "--request-id", "a" * 32],
        _context(tmp_path),
    )

    assert status == 2
    assert "--request-id is supported only for one mount-daemon job request" in capsys.readouterr().err
    assert not _requests(workspace)


@pytest.mark.parametrize(
    ("action", "options", "message"),
    [
        ("cancel", ["--step", "start"], "--step is required by, and only valid with, the override_step action"),
        ("set_priority", [], "--priority is required by"),
        (
            "set_budget",
            [],
            "one of --max-attempts-per-activation, --max-total-attempts, --max-activations is required by",
        ),
        ("cancel", ["--max-activations", "2"], "only valid with, the set_budget action"),
        ("set_budget", ["--max-activations", "0"], "an integer of at least 1 or 'unlimited'"),
        ("eject", [], "--destination is required by"),
        ("pause", ["--force"], "--force applies only to"),
        ("pause", ["--tree"], "--tree applies only to the continue action"),
        ("restart", ["--tree"], "--tree applies only to the continue action"),
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

    assert (
        command(
            [*_request_args(workspace_name, job_id, action="set_priority"), "--priority", "700"], _context(tmp_path)
        )
        == 0
    )
    assert (
        command(
            [*_request_args(workspace_name, job_id, action="eject"), "--destination", "relative/destination"],
            _context(tmp_path),
        )
        == 1
    )
    capsys.readouterr()
    assert v3.find(workspace, job_id).priority == 700
    assert not _requests(workspace)


def test_set_budget_records_the_given_limits_and_unlimited_as_null(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "budget").job_id
    argv = [*_request_args(workspace_name, job_id, action="set_budget"), "--max-attempts-per-activation", "unlimited"]
    assert command([*argv, "--max-activations", "4"], _context(tmp_path)) == 0
    capsys.readouterr()
    doc = v3.state_of(v3.find(workspace, job_id))
    assert doc.budget_override == {"maximum_attempts_per_activation": None, "maximum_activations": 4}
    assert _applied(workspace, job_id)[-1]["action"] == "set_budget"


def test_request_envelopes_carry_the_budget_and_the_far_side_cannot_change_it(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "budget-protocol")
    argv = ["job", "request-envelopes", "set_budget", "--workspace", workspace_name, "--max-total-attempts=7"]
    argv += ["--operator=Test User <tester@example.test>", "--reason=protocol", "--json", ref.job_id]
    assert command(argv, _context(tmp_path)) == 0
    document = json.loads(capsys.readouterr().out)
    (envelope,) = document["envelopes"]
    assert envelope["budget"] == {"maximum_total_attempts": 7}
    arguments = argparse.Namespace(
        job_id=[ref.job_id],
        action="set_budget",
        reason="protocol",
        priority=None,
        step=None,
        force=False,
        destination=None,
        maximum_total_attempts=7,
    )
    operator = "Test User <tester@example.test>"
    assert job_cli._validate_remote_envelopes(document, arguments, operator) == [envelope]
    changed = {**document, "envelopes": [{**envelope, "budget": {"maximum_total_attempts": None}}]}
    with pytest.raises(ValueError, match="'budget' disagrees"):
        job_cli._validate_remote_envelopes(changed, arguments, operator)


def test_remote_request_passes_the_budget_options_to_the_far_side(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def fake_adapter(_bundle: object, _verb: str, payload: dict[str, list[str]], **_options: object) -> dict:
        calls.append(payload["argv"])
        return {"returncode": 3, "stderr": "stop"}

    monkeypatch.setattr(job_cli, "resolve_remote", lambda *_args, **_kwargs: argparse.Namespace(bundle=tmp_path))
    monkeypatch.setattr(job_cli, "run_adapter", fake_adapter)
    namespace = argparse.Namespace(
        action="set_budget",
        reason="r",
        job_id=["abc"],
        priority=None,
        step=None,
        destination=None,
        force=False,
        adapter_timeout=None,
        maximum_attempts_per_activation=None,
        maximum_activations=5,
    )
    binding = job_cli.WorkspaceBinding("host:ws", "host", None)
    identity = job_cli._resolve_request_identity(None)
    assert job_cli.request_remote_job_result(binding, _context(tmp_path), namespace, identity) == (3, "", "stop")
    (argv,) = calls
    assert "--max-attempts-per-activation=unlimited" in argv and "--max-activations=5" in argv
    assert not any(part.startswith("--max-total-attempts") for part in argv)


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
    applied = _applied(workspace, ref.job_id)
    assert len(applied) == 1 and applied[0]["request_id"] == signed["request_id"]
    assert applied[0]["operator_key"] == signed["operator_key"]
    assert not _requests(workspace)


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


def test_local_batch_validates_every_signature_before_posting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    refs = [_job(tmp_path, workspace, tag) for tag in ("batch-one", "batch-two")]
    original = job_cli.sign_document
    calls = 0

    def tamper_second(document: dict[str, object], *, seed_path: Path) -> dict[str, object]:
        nonlocal calls
        calls += 1
        signed = original(document, seed_path=seed_path)
        if calls == 2:
            signed["reason"] = "tampered"
        return signed

    monkeypatch.setattr(job_cli, "sign_document", tamper_second)
    assert command(_request_args(workspace_name, *(ref.job_id for ref in refs)), _context(tmp_path)) == 2
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
    (request,) = _applied(workspace, job_id)
    assert request["operator"] == "Test User <tester@example.test>"
    assert request["operator_key"] == identity_public_key(identity_key_paths("tester")[0])


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
    (request,) = _applied(workspace, job_id)
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
    assert _applied(workspace, job_id)[0]["operator"] == "Ext Person <ext@x>"


def test_literal_operator_on_empty_machine_publishes_unsigned_request(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "empty-config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "empty-data"))
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "literal-empty").job_id

    assert command(_request_args(workspace_name, job_id) + ["--operator", "Ext <e@x>"], _context(tmp_path)) == 0
    assert _applied(workspace, job_id)[0]["operator"] == "Ext <e@x>"
    assert "operator_key" not in _applied(workspace, job_id)[0]


def test_wait_returns_zero_when_manager_pauses_jobs(tmp_path: Path, capsys, monkeypatch) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_ids = [_job(tmp_path, workspace, tag).job_id for tag in ("first", "second")]

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        monkeypatch.setattr(job_cli.time, "sleep", lambda _: manager.tick())
        assert command(_request_args(workspace_name, *job_ids) + ["--wait"], _context(tmp_path)) == 0

    output = capsys.readouterr().out
    assert output.count("state=paused") == 2


def test_wait_reports_terminal_state_and_exits_one(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "finished").job_id
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60)
        assert v3.find(workspace, job_id).state == "succeeded"
        assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 1

    assert "refused\tthe job is already succeeded" in capsys.readouterr().out


def test_wait_does_not_accept_pause_superseded_after_immediate_apply(tmp_path: Path, capsys) -> None:
    workspace, _workspace_name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "superseded-pause")
    pause = job_cli.publish_job_requests(workspace, [ref], action="pause", reason="pause first")
    assert serve_requests(workspace, pause)[0].status == "applied"

    paused_ref = _kernel.locate(workspace, ref.job_id, placement_hint=None, exhaustive=True)
    assert paused_ref is not None and paused_ref.state == "paused"
    cancel = job_cli.publish_job_requests(workspace, [paused_ref], action="cancel", reason="cancel afterward")
    assert serve_requests(workspace, cancel)[0].status == "applied"

    status = job_cli._complete_job_requests(workspace, pause, wait=True, timeout=0)

    assert status == 1
    assert "pause superseded" in capsys.readouterr().out


def test_wait_without_live_manager_fails_after_publishing(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "unserved").job_id

    assert command(_request_args(workspace_name, job_id) + ["--wait"], _context(tmp_path)) == 0
    captured = capsys.readouterr()
    assert not captured.err
    assert "\tapplied\t" in captured.out and "state=paused" in captured.out
    assert not _requests(workspace)


def test_wait_timeout_names_pending_job_and_keeps_request(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    ref = _job(tmp_path, workspace, "pending")

    with _kernel.register_owner(workspace, kind="manager", label="held", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        argv = _request_args(workspace_name, ref.job_id) + ["--wait", "--timeout", "0.01"]
        assert command(argv, _context(tmp_path)) == 1
        owned.give_back()

    captured = capsys.readouterr()
    assert "timeout" in captured.out and ref.job_id in captured.out
    assert _requests(workspace)


def test_wait_is_rejected_for_non_pause_action(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    job_id = _job(tmp_path, workspace, "cancel").job_id

    assert command(_request_args(workspace_name, job_id, action="cancel") + ["--wait"], _context(tmp_path)) == 2
    assert "--wait is only valid with the pause action" in capsys.readouterr().err


def _mapping(tmp_path: Path, workspace: Workspace, tag: str) -> dict[str, object]:
    installed = _store.lookup(workspace, "demo") or v3.install(workspace, tmp_path / "package")
    return v3.job_mapping(installed, {"start": "succeed"}, tag=tag, placement=f"project/{tag}")


def _link(mapping: dict[str, object]) -> tuple[str, str, str]:
    job = JobDefinition.from_mapping(mapping)
    return job.id, job.job_key, str(job.placement)


def _child_of(child: dict[str, object], parent: dict[str, object]) -> None:
    job_id, key, placement = _link(parent)
    child["parent"] = {
        "workspace_id": "2d7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e03",
        "job_id": job_id,
        "job_key": key,
        "placement": placement,
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def _failed(
    workspace: Workspace,
    mapping: dict[str, object],
    waits_on: tuple[tuple[str, str, str], ...] = (),
    *,
    state: str = "failed",
    started: bool = False,
) -> _kernel.JobRef:
    """Place a failed job; with *waits_on* (``(job_id, job_key, placement)`` per child) it failed at its wait.

    *started* records a started attempt, so a revival needs a second one (beyond the default budget of one).
    """

    job = JobDefinition.from_mapping(mapping)
    doc = StateDoc.empty(job.id)
    if started:
        doc = doc.next_activation("start", "initial").next_attempt("launch", unclean=False)
        assert doc.attempt is not None
        doc = doc.with_phase("launching", str(doc.attempt["id"])).with_phase("idle", None)
    doc = doc.with_failure({"code": "dependency_failure", "message": "a child failed"})
    if waits_on:
        children = [
            {"label": None, "job_id": job_id, "job_key": key, "placement_hint": placement}
            for job_id, key, placement in waits_on
        ]
        doc = doc.updated(join={"condition": "all_succeeded", "count": 0, "next_step": "gather", "children": children})
    with v3.cli_owner(workspace) as owner:
        staging = owner.scratch("submit") / "job"
        staging.mkdir()
        (staging / "job.json").write_bytes(job.encode())
        (staging / "state.json").write_bytes(encode_state(doc))
        return _kernel.submit(workspace, owner, staging, state=state)


def test_continue_tree_resumes_failed_descendants_deepest_first(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    tree = [_mapping(tmp_path, workspace, tag) for tag in ("grandchild", "child", "parent")]
    _child_of(tree[0], tree[1])
    _child_of(tree[1], tree[2])
    grandchild = _failed(workspace, tree[0])
    child = _failed(workspace, tree[1], (_link(tree[0]),))
    parent = _failed(workspace, tree[2], (_link(tree[1]),))

    assert (
        command([*_request_args(workspace_name, parent.job_id, action="continue"), "--tree"], _context(tmp_path)) == 0
    )
    lines = capsys.readouterr().out.splitlines()
    assert [line.split("\t", 1)[0] for line in lines] == [grandchild.job_id, child.job_id, parent.job_id]
    assert all("\tapplied\t" in line for line in lines)
    assert [v3.find(workspace, ref.job_id).state for ref in (grandchild, child, parent)] == [
        "ready",
        "waiting",
        "waiting",
    ]
    assert not _requests(workspace)


def test_continue_notes_the_children_that_would_fail_a_reopened_wait(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    child_mapping, parent_mapping = _mapping(tmp_path, workspace, "child"), _mapping(tmp_path, workspace, "parent")
    _child_of(child_mapping, parent_mapping)
    child = _failed(workspace, child_mapping)
    ghost = str(uuid.uuid4())
    parent = _failed(workspace, parent_mapping, (_link(child_mapping), (ghost, f"ghost--{ghost}", "project/x")))

    assert command(_request_args(workspace_name, parent.job_id, action="continue"), _context(tmp_path)) == 0
    err = capsys.readouterr().err
    assert f"note: {child.job_key} is still failed; the reopened wait of {parent.job_key} fails again" in err
    assert "continue --tree also resumes failed descendants" in err
    assert f"note: ghost--{ghost} was not found at its placement" in err
    assert v3.find(workspace, parent.job_id).state == "waiting"
    assert v3.find(workspace, child.job_id).state == "failed"


def test_restart_is_an_action_and_takes_force(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    child = _mapping(tmp_path, workspace, "child")
    parent = _failed(workspace, _mapping(tmp_path, workspace, "parent"), (_link(child),))

    assert (
        command([*_request_args(workspace_name, parent.job_id, action="restart"), "--force"], _context(tmp_path)) == 0
    )
    assert "\tapplied\t" in capsys.readouterr().out
    restarted = v3.find(workspace, parent.job_id)
    assert restarted.state == "ready" and v3.state_of(restarted).join is None


def test_job_request_help_lists_restart_and_tree_only_on_request(tmp_path: Path, capsys) -> None:
    assert command(["job", "request", "--help"], _context(tmp_path)) == 0
    out = capsys.readouterr().out
    assert "continue, restart, override_step" in out and "--tree" in out
    assert command(["job", "request-envelopes", "--help"], _context(tmp_path)) == 0
    assert "--tree" not in capsys.readouterr().out


def test_continue_tree_stops_at_a_descendant_that_does_not_resume(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    tree = [_mapping(tmp_path, workspace, tag) for tag in ("leaf", "child", "parent")]
    _child_of(tree[0], tree[1])
    _child_of(tree[1], tree[2])
    # The leaf's budget allows one attempt, which it used: its revival lands failed again.
    leaf = _failed(workspace, tree[0], started=True)
    child = _failed(workspace, tree[1], (_link(tree[0]),))
    parent = _failed(workspace, tree[2], (_link(tree[1]),))

    assert (
        command([*_request_args(workspace_name, parent.job_id, action="continue"), "--tree"], _context(tmp_path)) == 1
    )
    captured = capsys.readouterr()
    (line,) = captured.out.splitlines()
    assert line.startswith(f"{leaf.job_id}\t") and line.endswith("state=failed")
    for ref in (child, parent):
        assert (
            f"not posted: {ref.job_key} (an earlier request in the tree was applied but left the job failed; "
            "resolve it and rerun continue --tree)"
        ) in captured.err
    assert [v3.find(workspace, ref.job_id).state for ref in (leaf, child, parent)] == ["failed"] * 3
    assert not _requests(workspace)


def test_continue_tree_notes_the_children_it_does_not_resume(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    tree = [_mapping(tmp_path, workspace, tag) for tag in ("failed-child", "cancelled-child", "parent")]
    _child_of(tree[0], tree[2])
    _child_of(tree[1], tree[2])
    failed = _failed(workspace, tree[0])
    cancelled = _failed(workspace, tree[1], state="cancelled")
    ghost = str(uuid.uuid4())
    waits_on = (_link(tree[0]), _link(tree[1]), (ghost, f"ghost--{ghost}", "project/x"))
    parent = _failed(workspace, tree[2], waits_on)

    assert (
        command([*_request_args(workspace_name, parent.job_id, action="continue"), "--tree"], _context(tmp_path)) == 0
    )
    err = capsys.readouterr().err
    assert f"note: {cancelled.job_key} is still cancelled; the reopened wait of {parent.job_key} fails again" in err
    assert f"note: ghost--{ghost} was not found at its placement" in err
    assert failed.job_key not in err
    assert v3.find(workspace, failed.job_id).state == "ready"
    assert v3.find(workspace, parent.job_id).state == "waiting"


def test_remote_envelopes_note_a_failed_child_without_suggesting_tree(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    child_mapping, parent_mapping = _mapping(tmp_path, workspace, "child"), _mapping(tmp_path, workspace, "parent")
    _child_of(child_mapping, parent_mapping)
    child = _failed(workspace, child_mapping)
    parent = _failed(workspace, parent_mapping, (_link(child_mapping),))
    argv = ["job", "request-envelopes", "continue", "--workspace", workspace_name]
    argv += ["--operator=Test User <tester@example.test>", "--reason=protocol", "--json", parent.job_id]
    assert command(argv, _context(tmp_path)) == 0
    err = capsys.readouterr().err
    assert f"note: {child.job_key} is still failed; the reopened wait of {parent.job_key} fails again" in err
    assert "--tree" not in err


def test_continue_notes_a_held_child_and_tree_does_not_expand_it(tmp_path: Path, capsys) -> None:
    workspace, workspace_name = _new_workspace(tmp_path)
    child_mapping, parent_mapping = _mapping(tmp_path, workspace, "child"), _mapping(tmp_path, workspace, "parent")
    _child_of(child_mapping, parent_mapping)
    child = _failed(workspace, child_mapping)
    parent = _failed(workspace, parent_mapping, (_link(child_mapping),))

    with _kernel.register_owner(workspace, kind="manager", label="held", allocation=None, advertised={}) as owner:
        owned = _kernel.claim(workspace, owner, child)
        assert owned is not None
        argv = [*_request_args(workspace_name, parent.job_id, action="continue"), "--tree"]
        assert command(argv, _context(tmp_path)) == 0
        owned.give_back()

    captured = capsys.readouterr()
    assert (
        f"note: {child.job_key} is held by an owner right now; the reopened wait of {parent.job_key} fails again if "
        "it is released still failed or cancelled"
    ) in captured.err
    assert "was not found" not in captured.err
    # Only the parent was posted: the held child is not expanded.
    (line,) = captured.out.splitlines()
    assert line.startswith(f"{parent.job_id}\t")
    assert v3.find(workspace, parent.job_id).state == "waiting"
    assert v3.find(workspace, child.job_id).state == "failed"
