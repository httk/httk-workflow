"""Authenticated daemon job execution, arbitration and recovery tests."""

import base64
import dataclasses
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from httk.core.identity import identity_public_key

import v3_helpers as v3
from httk.workflow import _death, _kernel, _moving, _requests, _store, removal, seals
from httk.workflow._bundles import rekeyed_id
from httk.workflow._daemon_auth import sign_request, verify_request
from httk.workflow._daemon_jobs import JobExecutor, _Target
from httk.workflow._daemon_keys import initialize_response_seed, response_seed_path
from httk.workflow._daemon_mailbox import MailboxDirectory
from httk.workflow._daemon_policy import ApprovedLauncher, Policy
from httk.workflow._daemon_protocol import Request, decode_response, encode_request, encode_response
from httk.workflow._daemon_service import Broker
from httk.workflow._daemon_slurm import SlurmGateway
from httk.workflow._daemon_state import Ledger
from httk.workflow._job import JobDefinition
from httk.workflow._state import Release, StateDoc
from test_bundles import manifest_of, write_bundle

ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"
CONFIGURATION_DIGEST = "d" * 64
_DEFAULT_OWNER_KEY = object()


def _seed(tmp_path: Path) -> Path:
    path = tmp_path / "operator.seed"
    path.write_bytes(base64.b64encode(b"q" * 32) + b"\n")
    path.chmod(0o600)
    return path


def _request(
    tmp_path: Path,
    workspace_id: str,
    selector: str,
    *,
    number: int = 1,
    action: str = "pause",
    digest: str = CONFIGURATION_DIGEST,
    signer_seed: Path | None = None,
    **options: object,
) -> Request:
    unsigned = Request(
        f"{number:032x}",
        workspace_id,
        "job_request",
        enrollment_id=ENROLLMENT_ID,
        configuration_digest=digest,
        selector=selector,
        action=action,
        reason="authenticated operator request",
        **options,  # type: ignore[arg-type]
    )
    return sign_request(unsigned, seed_path=signer_seed or _seed(tmp_path), now=int(time.time()), lifetime=300)


def _setup(tmp_path: Path, *, adopted: bool = True):
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "package")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, tag="target")
    if adopted:
        ref = _mark_exchange(workspace, ref, str(uuid4()))
    state = tmp_path / "broker"
    state.mkdir()
    with Ledger(state, workspace.workspace_id, ENROLLMENT_ID, initialize=True):
        pass
    return workspace, ref, state


def _mark_exchange(
    workspace,
    ref,
    exchange_name: str,
    nonce: str = "nonce-a",
    owner_key: str | None | object = _DEFAULT_OWNER_KEY,
):
    if owner_key is _DEFAULT_OWNER_KEY:
        owner_key = identity_public_key(_seed(workspace.root.parent))
    with _kernel.register_owner(workspace, kind="cli", label="mark exchange", allocation=None, advertised={}) as owner:
        assert (
            _kernel.claim_exchange_name(
                workspace,
                owner,
                exchange_name,
                ref.job_id,
                ref.placement,
                nonce=nonce,
                owner_key=owner_key,  # type: ignore[arg-type]
            )
            is _kernel.ExchangeClaim.WON
        )
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        owned.write_state(StateDoc.empty(ref.job_id).updated(origin="exchange", exchange_name=exchange_name))
        return owned.give_back()


def _rekey_exchange(workspace, ref, exchange_name: str, before: str, after: str) -> None:
    with _kernel.register_owner(workspace, kind="cli", label="rekey exchange", allocation=None, advertised={}) as owner:
        _kernel.drop_exchange_index(owner, exchange_name, ref.job_id, before)
        assert (
            _kernel.claim_exchange_name(workspace, owner, exchange_name, ref.job_id, ref.placement, nonce=after)
            is _kernel.ExchangeClaim.WON
        )


def _release_in_state(workspace, ref, state: str, *, runner_steps: tuple[str, ...] | None = None):
    with _kernel.register_owner(
        workspace, kind="cli", label=f"prepare {state}", allocation=None, advertised={}
    ) as owner:
        owned = _kernel.claim(workspace, owner, ref)
        assert owned is not None
        doc = owned.read_state() or StateDoc.empty(ref.job_id)
        if doc.activation is None:
            doc = doc.next_activation("start", "initial")
        if runner_steps is not None:
            doc = doc.updated(runner_steps=runner_steps)
        return owned.release(doc, Release(state, owned.from_priority))


def test_authenticated_request_freezes_exact_internal_document_and_applies(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    request = _request(tmp_path, workspace.workspace_id, ref.job_id[:12])
    assert request.operator_key == identity_public_key(_seed(tmp_path))
    entry = ledger.admit(request)
    with _kernel.register_owner(workspace, kind="daemon", label="test daemon", allocation=None, advertised={}) as owner:
        executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
        response = executor.execute(entry)
        replay = executor.execute(entry)
    try:
        assert response.outcome == replay.outcome == "applied"
        assert response.job_id == ref.job_id
        assert response.internal_request_id == "00000000-0000-0000-0000-000000000001"
        assert response.current_state == "paused"
        assert ledger.execution_result(entry) is not None
        target = ledger.read_target(entry)
        assert target is not None
        frozen = _Target.decode(target)
        assert frozen.document["operator"] == f"daemon:{frozen.exchange_name}:{frozen.adoption_nonce}"
        assert request.signature is not None and request.signature.encode() not in target
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "paused"

        continued = _requests.post(
            workspace,
            action="continue",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="test",
            reason="advance after the recorded daemon result",
        )
        with _kernel.register_owner(
            workspace, kind="cli", label="continue", allocation=None, advertised={}
        ) as continuation_owner:
            (continued_outcome,) = removal.serve_requests(workspace, [(current, continued)], owner=continuation_owner)
        assert continued_outcome.status == "applied"
        status = Request(
            "00000000000000000000000000000002",
            workspace.workspace_id,
            "job_request_status",
            enrollment_id=ENROLLMENT_ID,
            command_id=request.request_id,
        )
        status = sign_request(status, seed_path=_seed(tmp_path), now=int(time.time()), lifetime=300)
        with _kernel.register_owner(
            workspace, kind="daemon", label="status", allocation=None, advertised={}
        ) as status_owner:
            executor = JobExecutor(workspace, ledger, status_owner, configuration_digest=CONFIGURATION_DIGEST)
            wrong_key_path = tmp_path / "other.seed"
            wrong_key_path.write_bytes(base64.b64encode(b"x" * 32) + b"\n")
            wrong_key_path.chmod(0o600)
            wrong_status = sign_request(status, seed_path=wrong_key_path, now=int(time.time()), lifetime=300)
            hidden = executor.status(wrong_status)
            fresh = executor.status(status)
        assert (hidden.outcome, hidden.reason, hidden.detail) == (
            "refused",
            "unknown_command",
            "the original job command is unknown",
        )
        assert fresh.outcome == "applied" and fresh.current_state == "ready"
    finally:
        ledger.close()


def test_local_job_is_refused_not_owner_without_freezing_or_posting(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path, adopted=False)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    state_path = ref.path / "state.json"
    assert not state_path.exists()
    with _kernel.register_owner(
        workspace, kind="daemon", label="local target", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    assert not state_path.exists()
    assert ledger.read_target(entry) is None and ledger.execution_decision(entry) is None
    ledger.close()


def test_other_key_cannot_control_keyed_exchange_root(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    other_seed = tmp_path / "other.seed"
    other_seed.write_bytes(base64.b64encode(b"x" * 32) + b"\n")
    other_seed.chmod(0o600)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id, signer_seed=other_seed))
    owner_key = identity_public_key(_seed(tmp_path))
    other_key = identity_public_key(other_seed)
    assert owner_key is not None and other_key is not None
    verify_request(entry.request, (owner_key, other_key))
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None
    state_before = (current.path / "state.json").read_bytes()
    with _kernel.register_owner(workspace, kind="daemon", label="foreign key", allocation=None, advertised={}) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and (current.path / "state.json").read_bytes() == state_before
    assert ledger.read_target(entry) is None and ledger.execution_decision(entry) is None
    ledger.close()


def test_keyless_exchange_index_cannot_authorize_control(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path, adopted=False)
    exchange_name = str(uuid4())
    ref = _mark_exchange(workspace, ref, exchange_name, owner_key=None)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    state_before = (ref.path / "state.json").read_bytes()
    with _kernel.register_owner(
        workspace, kind="daemon", label="keyless index", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    assert (ref.path / "state.json").read_bytes() == state_before
    assert ledger.read_target(entry) is None and ledger.execution_decision(entry) is None
    ledger.close()


def test_exchange_index_without_nonce_cannot_authorize_control(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path, adopted=False)
    exchange_name = str(uuid4())
    ref = _mark_exchange(workspace, ref, exchange_name)
    index_path = workspace.control / "exchange-jobs" / exchange_name / "index.json"
    index = json.loads(index_path.read_bytes())
    index.pop("adoption_nonce")
    index_path.write_bytes(json.dumps(index, sort_keys=True, separators=(",", ":")).encode("ascii"))
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    state_before = (ref.path / "state.json").read_bytes()
    with _kernel.register_owner(
        workspace, kind="daemon", label="nonce-free index", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    assert (ref.path / "state.json").read_bytes() == state_before
    assert ledger.read_target(entry) is None and ledger.execution_decision(entry) is None
    ledger.close()


def test_frozen_target_is_refused_when_the_root_has_no_recorded_owner(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path, adopted=False)
    exchange_name = str(uuid4())
    ref = _mark_exchange(workspace, ref, exchange_name)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    state_before = (ref.path / "state.json").read_bytes()
    with _kernel.register_owner(
        workspace, kind="daemon", label="freeze target", allocation=None, advertised={}
    ) as owner:
        executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
        executor._target(entry)
        index_path = workspace.control / "exchange-jobs" / exchange_name / "index.json"
        index = json.loads(index_path.read_bytes())
        index.pop("owner_key")
        index_path.write_bytes(json.dumps(index, sort_keys=True, separators=(",", ":")).encode("ascii"))
        response = executor.execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    assert (ref.path / "state.json").read_bytes() == state_before
    assert ledger.execution_decision(entry) is None
    ledger.close()


def test_owner_change_after_decision_is_refused_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, ref, state = _setup(tmp_path, adopted=False)
    exchange_name = str(uuid4())
    ref = _mark_exchange(workspace, ref, exchange_name)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    state_before = (ref.path / "state.json").read_bytes()
    real_decide = ledger.decide_execution

    def decide_then_drop_owner(*args, **kwargs):
        result = real_decide(*args, **kwargs)
        index_path = workspace.control / "exchange-jobs" / exchange_name / "index.json"
        index = json.loads(index_path.read_bytes())
        index.pop("owner_key")
        index_path.write_bytes(json.dumps(index, sort_keys=True, separators=(",", ":")).encode("ascii"))
        return result

    monkeypatch.setattr(ledger, "decide_execution", decide_then_drop_owner)
    with _kernel.register_owner(
        workspace, kind="daemon", label="owner change", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert not list((workspace.control / "requests").glob("*.json"))
    assert (ref.path / "state.json").read_bytes() == state_before
    assert ledger.execution_result(entry) is not None
    ledger.close()


def test_legacy_plain_daemon_target_without_adoption_fails_decode(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    with _kernel.register_owner(
        workspace, kind="daemon", label="legacy target", allocation=None, advertised={}
    ) as owner:
        target = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)._target(entry)
    legacy = json.loads(target.encode())
    legacy.pop("exchange_name")
    legacy.pop("adoption_nonce")
    legacy["request"]["operator"] = "daemon"
    legacy["digest"] = hashlib.sha256(
        json.dumps(legacy["request"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()
    encoded = json.dumps(legacy, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    with pytest.raises(ValueError, match="fields are invalid"):
        _Target.decode(encoded)
    ledger.close()


def test_inbox_bundle_owner_key_controls_only_its_adopted_root(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "package")
    signer_seed = _seed(tmp_path)
    owner_key = identity_public_key(signer_seed)
    assert owner_key is not None
    mapping = v3.job_mapping(installed, {"start": "succeed"}, tag="bundle-root", placement="0")
    manifest = dataclasses.replace(manifest_of([mapping]), owner_key=owner_key)
    bundle = workspace.root / "exchange" / "inbox" / "bundle"
    write_bundle(bundle, [mapping], manifest)
    refused_to = workspace.root / "exchange" / "refused"
    refused_to.mkdir()
    with _kernel.register_owner(workspace, kind="cli", label="adopt inbox", allocation=None, advertised={}) as adopter:
        adopted = _moving.adopt(workspace, adopter, bundle, untrusted=True, refused_to=refused_to)
    assert adopted is not None and len(adopted.published) == 1
    ref = adopted.published[0]
    state = tmp_path / "broker"
    state.mkdir()
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID, initialize=True)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id, signer_seed=signer_seed))
    with _kernel.register_owner(
        workspace, kind="daemon", label="bundle owner", allocation=None, advertised={}
    ) as owner:
        applied = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert (applied.outcome, applied.reason) == ("applied", "request_applied")

    other_seed = tmp_path / "other.seed"
    other_seed.write_bytes(base64.b64encode(b"x" * 32) + b"\n")
    other_seed.chmod(0o600)
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None
    state_before = (current.path / "state.json").read_bytes()
    other_entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id, number=2, signer_seed=other_seed))
    other_key = identity_public_key(other_seed)
    assert other_key is not None
    verify_request(other_entry.request, (owner_key, other_key))
    with _kernel.register_owner(
        workspace, kind="daemon", label="other bundle key", allocation=None, advertised={}
    ) as owner:
        refused = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(other_entry)
    assert (refused.outcome, refused.reason) == ("refused", "not_owner")
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and (current.path / "state.json").read_bytes() == state_before
    assert not list((workspace.control / "requests").glob("*.json"))
    ledger.close()


def test_live_owner_queues_then_fresh_status_observes_later_application(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    request = _request(tmp_path, workspace.workspace_id, ref.job_id)
    entry = ledger.admit(request)
    with _kernel.register_owner(workspace, kind="manager", label="holder", allocation=None, advertised={}) as holder:
        held = _kernel.claim(workspace, holder, ref)
        assert held is not None
        with _kernel.register_owner(
            workspace, kind="daemon", label="test daemon", allocation=None, advertised={}
        ) as owner:
            executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
            queued = executor.execute(entry)
            assert queued.outcome == "queued" and ledger.execution_result(entry) is None
        released = held.give_back()

    request_path = workspace.control / "requests" / f"{ref.job_id}.00000000-0000-0000-0000-000000000001.json"
    with _kernel.register_owner(workspace, kind="cli", label="apply pending", allocation=None, advertised={}) as owner:
        (outcome,) = removal.serve_requests(workspace, [(released, request_path)], owner=owner)
        assert outcome.status == "applied"

    status = Request(
        "00000000000000000000000000000002",
        workspace.workspace_id,
        "job_request_status",
        enrollment_id=ENROLLMENT_ID,
        command_id=request.request_id,
    )
    status = sign_request(status, seed_path=_seed(tmp_path), now=int(time.time()), lifetime=300)
    with _kernel.register_owner(workspace, kind="daemon", label="test daemon", allocation=None, advertised={}) as owner:
        observed = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).status(status)
    ledger.close()
    assert observed.outcome == "applied" and observed.current_state == "paused"


def test_two_instances_publish_and_apply_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, ref, state = _setup(tmp_path)
    first = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    second = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    request = _request(tmp_path, workspace.workspace_id, ref.job_id)
    entry = first.admit(request)
    first_owner = _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={})
    second_owner = _kernel.register_owner(workspace, kind="daemon", label="second", allocation=None, advertised={})
    executors = (
        JobExecutor(workspace, first, first_owner, configuration_digest=CONFIGURATION_DIGEST),
        JobExecutor(workspace, second, second_owner, configuration_digest=CONFIGURATION_DIGEST),
    )
    publications: list[str] = []
    real_post = _kernel.post_request

    def record_post(target_workspace, document):
        publications.append(str(document["request_id"]))
        return real_post(target_workspace, document)

    monkeypatch.setattr(_kernel, "post_request", record_post)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(lambda executor: executor.execute(entry), executors))
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "paused"
        doc = v3.state_of(current)
        events = [event for event in doc.history_tail if event.get("request_id") == responses[0].internal_request_id]
        assert len([event for event in events if event.get("event") == "request_applied"]) == 1
        assert sum(response.outcome == "applied" for response in responses) >= 1
        assert publications == ["00000000-0000-0000-0000-000000000001"]
    finally:
        first_owner.close()
        second_owner.close()
        first.close()
        second.close()


def test_publication_failure_after_decision_is_never_reposted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    request = _request(tmp_path, workspace.workspace_id, ref.job_id)
    entry = ledger.admit(request)
    calls: list[dict[str, object]] = []

    def fail_post(_workspace, document):
        calls.append(dict(document))
        raise OSError("injected publication failure")

    monkeypatch.setattr(_kernel, "post_request", fail_post)
    with _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={}) as owner:
        first = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert first.outcome == "uncertain" and len(calls) == 1

    with _kernel.register_owner(workspace, kind="daemon", label="restart", allocation=None, advertised={}) as owner:
        second = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    ledger.close()
    assert second.outcome == "uncertain" and len(calls) == 1


def test_declared_dead_daemon_stops_before_target_or_execution_decision(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    owner = _kernel.register_owner(workspace, kind="daemon", label="declared dead", allocation=None, advertised={})
    _kernel.attest_dead(
        workspace,
        owner.owner_id,
        by="operator",
        evidence=[],
        operator="test",
        reason="forced fail-stop test",
    )

    with pytest.raises(_kernel.OwnerDeclaredDead, match="tombstone"):
        JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert ledger.read_target(entry) is None
    assert ledger.execution_decision(entry) is None
    assert not list((workspace.control / "requests").glob("*.json"))
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"
    ledger.close()


@pytest.mark.parametrize("boundary", ["decision", "publication"])
def test_daemon_declared_dead_at_execution_boundary_never_reposts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    owner = _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={})
    publications: list[Path] = []

    def declare_dead() -> None:
        _kernel.attest_dead(
            workspace,
            owner.owner_id,
            by="operator",
            evidence=[],
            operator="test",
            reason=f"declared dead after {boundary}",
        )

    if boundary == "decision":
        real_decide = ledger.decide_execution

        def decide_then_die(*args, **kwargs):
            result = real_decide(*args, **kwargs)
            declare_dead()
            return result

        monkeypatch.setattr(ledger, "decide_execution", decide_then_die)
    else:
        real_post = _kernel.post_request

        def post_then_die(target_workspace, document):
            path = real_post(target_workspace, document)
            publications.append(path)
            declare_dead()
            return path

        monkeypatch.setattr(_kernel, "post_request", post_then_die)

    with pytest.raises(_kernel.OwnerDeclaredDead, match="tombstone"):
        JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert ledger.execution_decision(entry) is not None
    assert len(publications) == (boundary == "publication")
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"

    restarted_owner = _kernel.register_owner(workspace, kind="daemon", label="restart", allocation=None, advertised={})
    restarted = JobExecutor(workspace, ledger, restarted_owner, configuration_digest=CONFIGURATION_DIGEST).execute(
        entry
    )
    # The declared-dead decider's exact pending request is served once by its successor, never reposted.
    assert restarted.outcome == ("applied" if boundary == "publication" else "uncertain")
    assert len(publications) == (boundary == "publication")
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == ("paused" if boundary == "publication" else "ready")
    restarted_owner.close()
    ledger.close()


@pytest.mark.parametrize("checkpoint", ["target", "decision"])
def test_failure_before_execution_decision_allows_safe_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint: str
) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    owner = _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={})
    executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
    method = "store_target" if checkpoint == "target" else "decide_execution"
    original = getattr(ledger, method)

    def fail(*_args, **_kwargs):
        raise OSError(f"injected {checkpoint} failure")

    monkeypatch.setattr(ledger, method, fail)
    with pytest.raises(OSError, match=checkpoint):
        executor.execute(entry)
    monkeypatch.setattr(ledger, method, original)
    owner.close()

    with _kernel.register_owner(
        workspace, kind="daemon", label="restart", allocation=None, advertised={}
    ) as restarted_owner:
        restarted = JobExecutor(workspace, ledger, restarted_owner, configuration_digest=CONFIGURATION_DIGEST)
        response = restarted.execute(entry)
    ledger.close()
    assert response.outcome == "applied"
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused"


@pytest.mark.parametrize("checkpoint", ["effect", "result"])
def test_failure_after_decision_recovers_without_reposting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint: str
) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    publications: list[dict[str, object]] = []
    real_post = _kernel.post_request

    def record_post(target_workspace, document):
        publications.append(dict(document))
        return real_post(target_workspace, document)

    monkeypatch.setattr(_kernel, "post_request", record_post)
    real_serve = removal.serve_requests
    real_result = ledger.store_execution_result
    if checkpoint == "effect":

        def fail_effect(*_args, **_kwargs):
            raise OSError("injected effect failure")

        monkeypatch.setattr(removal, "serve_requests", fail_effect)
    else:

        def fail_result(*_args, **_kwargs):
            raise OSError("injected result failure")

        monkeypatch.setattr(ledger, "store_execution_result", fail_result)

    owner = _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={})
    executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
    if checkpoint == "effect":
        first = executor.execute(entry)
        assert first.outcome == "queued"
        monkeypatch.setattr(removal, "serve_requests", real_serve)
    else:
        with pytest.raises(OSError, match="result"):
            executor.execute(entry)
        monkeypatch.setattr(ledger, "store_execution_result", real_result)
    owner.close()

    with _kernel.register_owner(
        workspace, kind="daemon", label="restart", allocation=None, advertised={}
    ) as restarted_owner:
        restarted = JobExecutor(workspace, ledger, restarted_owner, configuration_digest=CONFIGURATION_DIGEST)
        response = restarted.execute(entry)
    ledger.close()
    # A crash between publication and serving: the successor serves the exact pending request without reposting.
    assert len(publications) == 1
    assert response.outcome == "applied"
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "paused"


def test_pending_request_of_a_live_other_decider_is_left_to_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    real_serve = removal.serve_requests
    served: list[str] = []

    def fail_effect(*_args, **_kwargs):
        raise OSError("injected effect failure")

    monkeypatch.setattr(removal, "serve_requests", fail_effect)
    decider = _kernel.register_owner(workspace, kind="daemon", label="live decider", allocation=None, advertised={})
    assert JobExecutor(workspace, ledger, decider, configuration_digest=CONFIGURATION_DIGEST).execute(
        entry
    ).outcome == ("queued")

    def record_serve(*args, **kwargs):
        served.append("serve")
        return real_serve(*args, **kwargs)

    monkeypatch.setattr(removal, "serve_requests", record_serve)
    with _kernel.register_owner(workspace, kind="daemon", label="other", allocation=None, advertised={}) as other:
        response = JobExecutor(workspace, ledger, other, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
        assert response.outcome == "queued" and served == []
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "ready"

        # The decider itself may serve its own pending request again; that applies it exactly once.
        response = JobExecutor(workspace, ledger, decider, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
        assert response.outcome == "applied" and served == ["serve"]
        replay = JobExecutor(workspace, ledger, other, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
        assert replay.outcome == "applied" and served == ["serve"]
    decider.close()
    ledger.close()


def test_dead_daemon_owner_on_this_host_is_recovered_by_the_bootstrap_entry_point(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "package")
    jobs = [v3.submit(workspace, installed, {"start": "succeed"}, tag=name) for name in ("dead", "alive", "elsewhere")]
    here = _death.process_identity()
    finished = subprocess.Popen(["true"])
    finished.wait()
    scopes = {
        "dead": dataclasses.replace(here, pid=finished.pid),
        "alive": _death.process_identity(os.getppid()),
        "elsewhere": dataclasses.replace(here, hostname="another-host", pid=finished.pid),
    }
    owners = {}
    for (name, scope), job in zip(scopes.items(), jobs, strict=True):
        owners[name] = _kernel.register_owner(
            workspace, kind="daemon", label=name, allocation=None, advertised={}, execution_scope=scope
        )
        assert _kernel.claim(workspace, owners[name], job) is not None

    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "httk.workflow._daemon_jobs",
            "--workspace",
            str(workspace.root),
            "--workspace-id",
            workspace.workspace_id,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"daemon_owner_recovered owner={owners['dead'].owner_id} returned=1 quarantined=0" in result.stdout
    located = [_kernel.locate(workspace, job.job_id, placement_hint=job.placement, exhaustive=True) for job in jobs]
    assert [(None if item is None else (item.state, item.owner_id)) for item in located] == [
        ("ready", None),
        ("owned", owners["alive"].owner_id),
        ("owned", owners["elsewhere"].owner_id),
    ]
    assert {item.owner_id for item in _kernel.list_owners(workspace) if item.record is not None} == {
        owners["alive"].owner_id,
        owners["elsewhere"].owner_id,
    }

    mismatch = subprocess.run(
        [sys.executable, "-I", "-m", "httk.workflow._daemon_jobs", "--workspace", str(workspace.root)]
        + ["--workspace-id", str(uuid4())],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert mismatch.returncode == 1 and "daemon_owner_recovery_failed" in mismatch.stdout


def test_frozen_target_rechecks_active_configuration_before_first_decision(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    with _kernel.register_owner(workspace, kind="daemon", label="resolver", allocation=None, advertised={}) as owner:
        executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
        executor._target(entry)
        response = JobExecutor(workspace, ledger, owner, configuration_digest="e" * 64).execute(entry)
    decision = ledger.execution_decision(entry)
    ledger.close()
    assert (response.outcome, response.reason) == ("refused", "stale_configuration")
    assert decision is None
    assert not list((workspace.control / "requests").glob("*.json"))


def test_exchange_adoption_nonce_is_frozen_but_exchange_uuid_is_not_a_selector(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    exchange_name = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    ref = _mark_exchange(workspace, ref, exchange_name)

    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    internal = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    exchange_selector = ledger.admit(_request(tmp_path, workspace.workspace_id, exchange_name, number=2))
    with _kernel.register_owner(workspace, kind="daemon", label="resolver", allocation=None, advertised={}) as owner:
        executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
        target = executor._target(internal)
        unknown = executor.execute(exchange_selector)
        _kernel.drop_exchange_index(owner, exchange_name, ref.job_id, "nonce-a")
        assert ref.placement is not None
        assert (
            _kernel.claim_exchange_name(workspace, owner, exchange_name, ref.job_id, ref.placement, nonce="nonce-b")
            is _kernel.ExchangeClaim.WON
        )
        changed = executor.execute(internal)
    ledger.close()
    assert (target.exchange_name, target.adoption_nonce) == (exchange_name, "nonce-a")
    assert (unknown.outcome, unknown.reason, unknown.job_id) == ("refused", "unknown_job", None)
    assert (changed.outcome, changed.reason) == ("refused", "adoption_changed")
    assert not list((workspace.control / "requests").glob("*.json"))


def test_queued_exchange_request_is_refused_if_adoption_changes_before_manager_boundary(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    exchange_name = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    ref = _mark_exchange(workspace, ref, exchange_name)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))

    with _kernel.register_owner(workspace, kind="manager", label="holder", allocation=None, advertised={}) as holder:
        held = _kernel.claim(workspace, holder, ref)
        assert held is not None
        with _kernel.register_owner(
            workspace, kind="daemon", label="publisher", allocation=None, advertised={}
        ) as daemon:
            queued = JobExecutor(workspace, ledger, daemon, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
        assert queued.outcome == "queued"
        ref = held.give_back()

    _rekey_exchange(workspace, ref, exchange_name, "nonce-a", "nonce-b")
    request_path = workspace.control / "requests" / (f"{ref.job_id}.00000000-0000-0000-0000-000000000001.json")
    with _kernel.register_owner(
        workspace, kind="manager", label="later manager", allocation=None, advertised={}
    ) as manager:
        (outcome,) = removal.serve_requests(workspace, [(ref, request_path)], owner=manager)
    ledger.close()

    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"
    assert outcome.status == "refused"
    assert "adoption changed" in outcome.explanation


def test_exchange_rekey_after_publication_is_refused_before_claim_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace, ref, state = _setup(tmp_path)
    exchange_name = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    ref = _mark_exchange(workspace, ref, exchange_name)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id))
    real_serve = removal.serve_requests

    def rekey_then_serve(target_workspace, published, *, owner=None):
        _rekey_exchange(target_workspace, ref, exchange_name, "nonce-a", "nonce-b")
        return real_serve(target_workspace, published, owner=owner)

    monkeypatch.setattr(removal, "serve_requests", rekey_then_serve)
    with _kernel.register_owner(workspace, kind="daemon", label="publisher", allocation=None, advertised={}) as daemon:
        response = JobExecutor(workspace, ledger, daemon, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    ledger.close()

    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"
    assert (response.outcome, response.reason) == ("refused", "request_refused")
    assert response.detail is not None and "adoption changed" in response.detail


@pytest.mark.parametrize(
    ("action", "from_state", "options"),
    [
        ("cancel", "ready", {}),
        ("pause", "paused", {}),
        ("continue", "paused", {"force": True}),
        ("override_step", "paused", {"step": "start", "force": True}),
        ("set_priority", "ready", {"priority": 7}),
        ("set_budget", "failed", {"budget": {"maximum_activations": 3}}),
        ("detach", "ready", {}),
        ("eject", "ready", {"destination": "/tmp/unused", "tree": True}),
        ("delete", "paused", {}),
        ("seal", "succeeded", {}),
        ("unseal", "succeeded", {}),
    ],
)
def test_daemon_adoption_refusal_is_checked_for_every_internal_action(
    tmp_path: Path, action: str, from_state: str, options: dict[str, object]
) -> None:
    workspace, ref, _state = _setup(tmp_path)
    job = JobDefinition.from_path(ref.path / "job.json")
    if action == "detach":
        job = dataclasses.replace(job, parent={"job_id": ref.job_id})
    path = _requests.post(
        workspace,
        action=action,
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="daemon:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa:nonce-a",
        reason="must stay with the authorized adoption",
        **options,  # type: ignore[arg-type]
    )
    request = _requests.parse(path)
    checks: list[str] = []

    def refuse(candidate: _requests.Request) -> str:
        checks.append(candidate.action)
        return "the exchange adoption changed"

    _doc, effect = _requests.apply(
        job,
        StateDoc.empty(ref.job_id),
        from_state,
        job.priority,
        request,
        refusal=refuse,
    )
    assert checks == [action]
    assert isinstance(effect, _requests.Drop)
    assert effect.reason == "the exchange adoption changed"


@pytest.mark.parametrize("keep_budget", [True, False])
def test_daemon_revival_forwards_keep_budget_into_the_internal_request(tmp_path: Path, keep_budget: bool) -> None:
    workspace, ref, state = _setup(tmp_path)
    ref = _release_in_state(workspace, ref, "paused")
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(
        _request(tmp_path, workspace.workspace_id, ref.job_id, action="restart", keep_budget=keep_budget)
    )
    with _kernel.register_owner(workspace, kind="daemon", label="restart", allocation=None, advertised={}) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    target = ledger.read_target(entry)
    ledger.close()
    assert (response.outcome, response.current_state) == ("applied", "ready")
    assert target is not None
    assert _Target.decode(target).document.get("keep_budget") is (True if keep_budget else None)


def test_malformed_daemon_adoption_attribution_is_refused_by_owner(tmp_path: Path) -> None:
    workspace, ref, _state = _setup(tmp_path)
    path = _requests.post(
        workspace,
        action="pause",
        job_id=ref.job_id,
        placement=ref.placement or "",
        operator="daemon:malformed",
        reason="must not escape the adoption guard",
    )
    with _kernel.register_owner(
        workspace, kind="manager", label="apply malformed", allocation=None, advertised={}
    ) as manager:
        (outcome,) = removal.serve_requests(workspace, [(ref, path)], owner=manager)
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"
    assert outcome.status == "refused"
    assert "malformed exchange adoption attribution" in outcome.explanation


@pytest.mark.parametrize(
    ("action", "from_state", "options", "expected_state"),
    [
        ("cancel", "ready", {}, "cancelled"),
        ("pause", "ready", {}, "paused"),
        ("continue", "paused", {}, "ready"),
        ("override_step", "paused", {"step": "finish"}, "ready"),
        ("set_priority", "ready", {"priority": 7}, "ready"),
        ("set_budget", "ready", {"budget": {"maximum_total_attempts": None}}, "ready"),
        ("detach", "ready", {}, "ready"),
        ("eject", "ready", {"destination": "eject-target"}, None),
        ("delete", "paused", {}, None),
        ("seal", "succeeded", {}, "succeeded"),
        ("unseal", "succeeded", {}, "succeeded"),
    ],
)
def test_authenticated_executor_applies_every_internal_action_on_real_jobs(
    tmp_path: Path,
    action: str,
    from_state: str,
    options: dict[str, object],
    expected_state: str | None,
) -> None:
    workspace, ref, state = _setup(tmp_path)
    if action == "detach":
        installed = _store.lookup(workspace, "demo")
        assert installed is not None
        mapping = v3.job_mapping(installed, {"start": "succeed"}, tag="child")
        mapping["parent"] = {
            "workspace_id": workspace.workspace_id,
            "job_id": ref.job_id,
            "job_key": ref.job_key,
            "placement": str(ref.placement),
            "activation_id": str(uuid4()),
            "spawn_id": str(uuid4()),
        }
        ref = v3.submit_mapping(workspace, mapping)
    runner_steps = ("start", "finish") if action == "override_step" else None
    if from_state != "ready" or runner_steps is not None:
        ref = _release_in_state(workspace, ref, from_state, runner_steps=runner_steps)
    if action in {"seal", "unseal"}:
        workspace.set_setting("seal.keys", str(_seed(tmp_path)))
    if action == "unseal":
        seal_request = _requests.post(
            workspace,
            action="seal",
            job_id=ref.job_id,
            placement=ref.placement or "",
            operator="test",
            reason="prepare an existing seal",
        )
        with _kernel.register_owner(
            workspace, kind="cli", label="prepare seal", allocation=None, advertised={}
        ) as seal_owner:
            (sealed,) = removal.serve_requests(workspace, [(ref, seal_request)], owner=seal_owner)
        assert sealed.status == "applied"
        ref = v3.find(workspace, ref.job_id)
        assert seals.is_job_sealed(ref.path)

    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(
        _request(tmp_path, workspace.workspace_id, ref.job_id, action=action, **options)  # type: ignore[arg-type]
    )
    with _kernel.register_owner(
        workspace, kind="daemon", label=f"apply {action}", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    ledger.close()

    if action == "detach":
        assert (response.outcome, response.reason) == ("refused", "not_owner")
        assert not list((workspace.control / "requests").glob("*.json"))
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "ready"
        return

    assert (response.outcome, response.reason) == ("applied", "request_applied")
    encoded = encode_response(response)
    assert str(workspace.root).encode() not in encoded
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    if expected_state is None:
        assert current is None
    else:
        assert current is not None and current.state == expected_state
    if action == "set_priority":
        assert current is not None and current.priority == 7
    elif action == "set_budget":
        assert current is not None and v3.state_of(current).budget_override == {"maximum_total_attempts": None}
    elif action == "detach":
        assert current is not None and v3.state_of(current).detached is not None
    elif action == "override_step":
        assert current is not None
        doc = v3.state_of(current)
        assert doc.activation is not None and doc.activation["step"] == "finish"
    elif action == "eject":
        assert (workspace.root / "exchange" / "outbox" / "eject-target" / ref.job_key).is_dir()
        assert response.detail == "job ejected to outbox/eject-target"
    elif action == "seal":
        assert current is not None and seals.is_job_sealed(current.path)
    elif action == "unseal":
        assert current is not None and not seals.is_job_sealed(current.path)
        doc = v3.state_of(current)
        assert doc.seal is not None and doc.seal["released"] is True


@pytest.mark.parametrize("adopted_here", [True, False])
def test_an_eject_without_destination_lands_in_the_outbox_root(tmp_path: Path, adopted_here: bool) -> None:
    workspace, _ref, state = _setup(tmp_path, adopted=False)
    installed = _store.lookup(workspace, "demo")
    assert installed is not None
    client_id = str(uuid4())
    mapping = v3.job_mapping(installed, {"start": "succeed"}, tag="client")
    if adopted_here:
        # The id this workspace's adoption of the client's job would give it: an exchange root of this workspace.
        mapping["id"] = rekeyed_id(workspace.workspace_id, client_id)
    ref = _mark_exchange(workspace, v3.submit_mapping(workspace, mapping), client_id)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id, action="eject"))
    with _kernel.register_owner(workspace, kind="daemon", label="eject", allocation=None, advertised={}) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    ledger.close()
    assert (response.outcome, response.detail) == ("applied", "job ejected to outbox/")
    name = f"client--{client_id}" if adopted_here else ref.job_key
    assert sorted(os.listdir(workspace.root / "exchange" / "outbox")) == [name]


@pytest.mark.parametrize("guard", ["sealed_workspace", "unknown_runner_step"])
def test_authenticated_executor_rechecks_mutable_workspace_guards(tmp_path: Path, guard: str) -> None:
    workspace, ref, state = _setup(tmp_path)
    action = "pause"
    options: dict[str, object] = {}
    if guard == "sealed_workspace":
        seals.workspace_seal_path(workspace).write_text("{}", encoding="utf-8")
    else:
        ref = _release_in_state(workspace, ref, "paused", runner_steps=("start", "finish"))
        action = "override_step"
        options = {"step": "gather"}
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(
        _request(tmp_path, workspace.workspace_id, ref.job_id, action=action, **options)  # type: ignore[arg-type]
    )
    with _kernel.register_owner(
        workspace, kind="daemon", label=f"guard {guard}", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    assert response.outcome == "refused"
    assert ledger.execution_decision(entry) is None
    assert not list((workspace.control / "requests").glob("*.json"))
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == ("ready" if guard == "sealed_workspace" else "paused")
    ledger.close()


def test_authenticated_continue_on_child_is_refused_as_not_owner(tmp_path: Path) -> None:
    workspace, _ref, state = _setup(tmp_path)
    installed = _store.lookup(workspace, "demo")
    assert installed is not None
    parent = v3.submit(
        workspace,
        installed,
        {"start": "spawn", "rescue": "succeed"},
        parameters={
            "spawn": {
                "children": [
                    {"label": "a", "script": {"start": "fail:first"}},
                    {"label": "b", "script": {"start": "fail:second"}},
                ],
                "on_impossible": "rescue",
            }
        },
        retry_policy={"maximum_attempts_per_activation": 3},
    )
    v3.run(workspace)
    assert v3.find(workspace, parent.job_id).state == "succeeded"
    children = [
        ref
        for ref, _placement in seals.workspace_jobs(workspace)
        if ref.job_id not in {parent.job_id, _ref.job_id} and ref.state == "failed"
    ]
    assert len(children) == 2
    child = children[0]
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(_request(tmp_path, workspace.workspace_id, child.job_id, action="continue"))
    with _kernel.register_owner(workspace, kind="daemon", label="join guard", allocation=None, advertised={}) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    current = v3.find(workspace, child.job_id)
    assert current.state == "failed"
    assert (response.outcome, response.reason) == ("refused", "not_owner")
    assert ledger.execution_decision(entry) is None
    assert not list((workspace.control / "requests").glob("*.json"))
    ledger.close()


@pytest.mark.parametrize("action", ["seal", "eject"])
def test_authenticated_destructive_effect_failure_is_definitively_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    workspace, ref, state = _setup(tmp_path)
    options: dict[str, object] = {}
    if action == "seal":
        ref = _release_in_state(workspace, ref, "succeeded")
        monkeypatch.setattr(
            seals,
            "seal_payload",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("injected invalid payload")),
        )
    else:
        destination = workspace.root / "exchange" / "outbox" / "occupied"
        (destination / ref.job_key).mkdir(parents=True)
        (destination / ref.job_key / "existing").write_text("occupied", encoding="utf-8")
        options["destination"] = "occupied"
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    entry = ledger.admit(
        _request(tmp_path, workspace.workspace_id, ref.job_id, action=action, **options)  # type: ignore[arg-type]
    )
    with _kernel.register_owner(
        workspace, kind="daemon", label=f"fail {action}", allocation=None, advertised={}
    ) as owner:
        response = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST).execute(entry)
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == ("succeeded" if action == "seal" else "ready")
    assert response.outcome == "refused"
    assert str(workspace.root).encode() not in encode_response(response)
    if action == "eject":
        assert response.detail is not None and "outbox/occupied" in response.detail
    assert ledger.execution_result(entry) is not None
    ledger.close()


@pytest.mark.parametrize("action", ["delete", "eject"])
@pytest.mark.parametrize("checkpoint", ["publication", "effect", "result"])
def test_irreversible_action_fault_never_reposts_and_missing_target_is_not_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str, checkpoint: str
) -> None:
    workspace, ref, state = _setup(tmp_path)
    if action == "delete":
        ref = _release_in_state(workspace, ref, "paused")
        options: dict[str, object] = {}
    else:
        options = {"destination": "fault-target"}
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    request = _request(
        tmp_path,
        workspace.workspace_id,
        ref.job_id,
        action=action,
        **options,  # type: ignore[arg-type]
    )
    entry = ledger.admit(request)
    publications: list[dict[str, object]] = []
    real_post = _kernel.post_request

    def publish(target_workspace, document):
        publications.append(dict(document))
        if checkpoint == "publication":
            raise OSError("injected publication fault")
        return real_post(target_workspace, document)

    monkeypatch.setattr(_kernel, "post_request", publish)
    if checkpoint == "effect":
        monkeypatch.setattr(
            removal,
            "serve_requests",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected effect fault")),
        )
    elif checkpoint == "result":
        monkeypatch.setattr(
            ledger,
            "store_execution_result",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("injected result fault")),
        )

    owner = _kernel.register_owner(workspace, kind="daemon", label="first", allocation=None, advertised={})
    executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
    if checkpoint == "result":
        with pytest.raises(OSError, match="result"):
            executor.execute(entry)
    else:
        first = executor.execute(entry)
        assert first.outcome == ("uncertain" if checkpoint == "publication" else "queued")
    owner.close()

    with _kernel.register_owner(
        workspace, kind="daemon", label="restart", allocation=None, advertised={}
    ) as restarted_owner:
        restarted = JobExecutor(workspace, ledger, restarted_owner, configuration_digest=CONFIGURATION_DIGEST).execute(
            entry
        )
        status = sign_request(
            Request(
                "00000000000000000000000000000002",
                workspace.workspace_id,
                "job_request_status",
                enrollment_id=ENROLLMENT_ID,
                command_id=request.request_id,
            ),
            seed_path=_seed(tmp_path),
            now=int(time.time()),
            lifetime=300,
        )
        observed = JobExecutor(workspace, ledger, restarted_owner, configuration_digest=CONFIGURATION_DIGEST).status(
            status
        )
    assert len(publications) == 1
    expected = {"publication": "uncertain", "effect": "queued", "result": "uncertain"}[checkpoint]
    assert restarted.outcome == observed.outcome == expected
    for response in (restarted, observed):
        encoded = encode_response(response)
        assert str(workspace.root).encode() not in encoded
        if action == "eject":
            assert response.detail is not None and "outbox/fault-target" in response.detail
    if checkpoint == "result":
        assert _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True) is None
    ledger.close()


def test_stale_configuration_and_unknown_prefix_have_no_effect(tmp_path: Path) -> None:
    workspace, ref, state = _setup(tmp_path)
    ledger = Ledger(state, workspace.workspace_id, ENROLLMENT_ID)
    stale = ledger.admit(_request(tmp_path, workspace.workspace_id, ref.job_id, digest="e" * 64))
    missing = ledger.admit(_request(tmp_path, workspace.workspace_id, "ffffffff", number=2))
    with _kernel.register_owner(workspace, kind="daemon", label="test daemon", allocation=None, advertised={}) as owner:
        executor = JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST)
        stale_response = executor.execute(stale)
        missing_response = executor.execute(missing)
    ledger.close()
    assert (stale_response.outcome, stale_response.reason) == ("refused", "stale_configuration")
    assert stale_response.job_id is None
    assert (missing_response.outcome, missing_response.reason) == ("refused", "unknown_job")
    assert missing_response.job_id is None
    current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
    assert current is not None and current.state == "ready"


class _UnusedGateway(SlurmGateway):
    """Scheduler surface that fails if a job command reaches it."""

    def check(self) -> None:
        pass

    def submit(
        self, launcher, handle, *, operator_key, time_limit=None
    ):  # pragma: no cover - a job command never submits
        raise AssertionError((launcher, handle))

    def status(self, job_id, cluster, handle):  # pragma: no cover
        raise AssertionError((job_id, cluster, handle))

    def cancel(self, job_id, cluster, handle):  # pragma: no cover
        raise AssertionError((job_id, cluster, handle))


def test_broker_authenticates_job_command_and_status_bypasses_full_quota(tmp_path: Path) -> None:
    workspace, ref, _state = _setup(tmp_path)
    seed = _seed(tmp_path)
    operator_key = identity_public_key(seed)
    assert operator_key is not None
    daemon = workspace.control / "daemon"
    broker_state = daemon / "broker"
    requests_path = workspace.root / "exchange" / "requests"
    responses_path = workspace.root / "exchange" / "responses"
    for path in (daemon, broker_state, requests_path, responses_path):
        path.mkdir(parents=True, exist_ok=True)
    initialize_response_seed(daemon)
    policy = Policy(
        workspace=workspace.root,
        workspace_id=workspace.workspace_id,
        enrollment_id=ENROLLMENT_ID,
        bwrap=tmp_path / "runtime/bwrap",
        python=tmp_path / "runtime/python",
        sbatch=tmp_path / "runtime/sbatch",
        squeue=tmp_path / "runtime/squeue",
        scancel=tmp_path / "runtime/scancel",
        cluster="test-cluster",
        launchers=(ApprovedLauncher("cpu", (("manager.confine", "bwrap"),), "a" * 64),),
        authorized_keys=(operator_key,),
        max_records=2,
        max_submissions=1,
    )
    ledger = Ledger(
        broker_state, workspace.workspace_id, ENROLLMENT_ID, initialize=True, max_records=2, max_submissions=1
    )
    requests = MailboxDirectory(requests_path)
    responses = MailboxDirectory(responses_path)
    owner = _kernel.register_owner(workspace, kind="daemon", label="broker", allocation=None, advertised={})
    broker = Broker(
        policy,
        _UnusedGateway(policy),
        ledger,
        requests,
        responses,
        response_seed=response_seed_path(daemon),
        job_executor=JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST),
    )
    try:
        unauthorized_seed = tmp_path / "unauthorized.seed"
        unauthorized_seed.write_bytes(base64.b64encode(b"x" * 32) + b"\n")
        unauthorized_seed.chmod(0o600)
        unauthorized = sign_request(
            Request(
                "00000000000000000000000000000070",
                workspace.workspace_id,
                "job_request",
                enrollment_id=ENROLLMENT_ID,
                configuration_digest=CONFIGURATION_DIGEST,
                selector=ref.job_id,
                action="pause",
                reason="must not apply",
            ),
            seed_path=unauthorized_seed,
            now=int(time.time()),
            lifetime=300,
        )
        broker.requests.replace(f"{unauthorized.request_id}.json", encode_request(unauthorized))
        broker.process_once(threading.Event())
        assert ledger.lookup_request(unauthorized.request_id) is None
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "ready"

        for number, wrong_workspace, enrollment, reason in (
            (66, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ENROLLMENT_ID, "wrong_workspace"),
            (67, workspace.workspace_id, "f" * 32, "wrong_enrollment"),
        ):
            wrong_identity = sign_request(
                Request(
                    f"{number:032x}",
                    wrong_workspace,
                    "job_request",
                    enrollment_id=enrollment,
                    configuration_digest=CONFIGURATION_DIGEST,
                    selector=ref.job_id,
                    action="pause",
                    reason="wrong daemon identity",
                ),
                seed_path=seed,
                now=int(time.time()),
                lifetime=300,
            )
            broker.requests.replace(f"{wrong_identity.request_id}.json", encode_request(wrong_identity))
            broker.process_once(threading.Event())
            refused = decode_response(broker.responses.read(f"{wrong_identity.request_id}.json"))
            assert (refused.outcome, refused.reason) == ("refused", reason)
            assert ledger.lookup_request(wrong_identity.request_id) is None

        broker.requests.replace("00000000000000000000000000000068.json", b"{}")
        broker.process_once(threading.Event())
        assert broker.requests.lstat("00000000000000000000000000000068.json") is None
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "ready"

        expired = sign_request(
            Request(
                "00000000000000000000000000000069",
                workspace.workspace_id,
                "job_request",
                enrollment_id=ENROLLMENT_ID,
                configuration_digest=CONFIGURATION_DIGEST,
                selector=ref.job_id,
                action="pause",
                reason="expired request",
            ),
            seed_path=seed,
            now=int(time.time()) - 10_000,
            lifetime=300,
        )
        broker.requests.replace(f"{expired.request_id}.json", encode_request(expired))
        broker.process_once(threading.Event())
        expired_response = decode_response(broker.responses.read(f"{expired.request_id}.json"))
        assert (expired_response.outcome, expired_response.reason) == ("refused", "request_expired")
        current = _kernel.locate(workspace, ref.job_id, placement_hint=ref.placement, exhaustive=True)
        assert current is not None and current.state == "ready"

        command = _request(tmp_path, workspace.workspace_id, ref.job_id, number=71)
        broker.requests.replace(f"{command.request_id}.json", encode_request(command))
        broker.process_once(threading.Event())
        command_response = decode_response(broker.responses.read(f"{command.request_id}.json"))
        assert command_response.outcome == "applied"
        assert ledger.verify() == 2

        status = sign_request(
            Request(
                "00000000000000000000000000000072",
                workspace.workspace_id,
                "job_request_status",
                enrollment_id=ENROLLMENT_ID,
                command_id=command.request_id,
            ),
            seed_path=seed,
            now=int(time.time()),
            lifetime=300,
        )
        broker.requests.replace(f"{status.request_id}.json", encode_request(status))
        broker.process_once(threading.Event())
        status_response = decode_response(broker.responses.read(f"{status.request_id}.json"))
        assert status_response.outcome == "applied" and status_response.command_id == command.request_id
        assert ledger.lookup_request(status.request_id) is None and ledger.verify() == 2

        _kernel.attest_dead(
            workspace,
            owner.owner_id,
            by="operator",
            evidence=[],
            operator="test",
            reason="broker poll fail-stop test",
        )
        with pytest.raises(_kernel.OwnerDeclaredDead, match="tombstone"):
            broker.process_once(threading.Event())
    finally:
        broker.close()
        owner.close()
        ledger.close()


def test_job_result_detail_sanitizes_control_characters() -> None:
    from httk.workflow._daemon_jobs import _ascii_detail
    from httk.workflow._daemon_protocol import Response

    detail = _ascii_detail("payload\x01with\x00controls\n" + "x" * 2000)
    response = Response(
        "a" * 32,
        "12345678-1234-1234-1234-123456789abc",
        ENROLLMENT_ID,
        "b" * 64,
        "refused",
        reason="request_refused",
        detail=detail,
        command_id="a" * 32,
    )
    assert response.detail is not None and len(response.detail) <= 1000
    assert all(" " <= character <= "~" for character in response.detail)


@pytest.mark.parametrize("read", ["read_target", "execution_result"])
def test_a_command_retired_during_its_status_observation_is_unknown_and_the_daemon_keeps_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, read: str
) -> None:
    workspace, ref, _state = _setup(tmp_path)
    seed = _seed(tmp_path)
    operator_key = identity_public_key(seed)
    assert operator_key is not None
    daemon = workspace.control / "daemon"
    broker_state = daemon / "broker"
    requests_path = workspace.root / "exchange" / "requests"
    responses_path = workspace.root / "exchange" / "responses"
    for path in (daemon, broker_state, requests_path, responses_path):
        path.mkdir(parents=True, exist_ok=True)
    initialize_response_seed(daemon)
    policy = Policy(
        workspace=workspace.root,
        workspace_id=workspace.workspace_id,
        enrollment_id=ENROLLMENT_ID,
        bwrap=tmp_path / "runtime/bwrap",
        python=tmp_path / "runtime/python",
        sbatch=tmp_path / "runtime/sbatch",
        squeue=tmp_path / "runtime/squeue",
        scancel=tmp_path / "runtime/scancel",
        cluster="test-cluster",
        launchers=(ApprovedLauncher("cpu", (("manager.confine", "bwrap"),), "a" * 64),),
        authorized_keys=(operator_key,),
    )
    ledger = Ledger(broker_state, workspace.workspace_id, ENROLLMENT_ID, initialize=True)
    other = Ledger(broker_state, workspace.workspace_id, ENROLLMENT_ID)
    requests = MailboxDirectory(requests_path)
    responses = MailboxDirectory(responses_path)
    owner = _kernel.register_owner(workspace, kind="daemon", label="broker", allocation=None, advertised={})
    broker = Broker(
        policy,
        _UnusedGateway(policy),
        ledger,
        requests,
        responses,
        response_seed=response_seed_path(daemon),
        job_executor=JobExecutor(workspace, ledger, owner, configuration_digest=CONFIGURATION_DIGEST),
    )
    try:
        command = _request(tmp_path, workspace.workspace_id, ref.job_id, number=71)
        broker.requests.replace(f"{command.request_id}.json", encode_request(command))
        broker.process_once(threading.Event())
        assert decode_response(broker.responses.read(f"{command.request_id}.json")).outcome == "applied"
        original = getattr(ledger, read)

        def retired_meanwhile(entry: object) -> bytes | None:
            # Another instance retires the command between the status lookup and this read.
            assert other.retire((), cutoff_ns=1 << 62, max_age=1 << 40) == [command.request_id]
            return original(entry)

        monkeypatch.setattr(ledger, read, retired_meanwhile)
        status = sign_request(
            Request(
                "00000000000000000000000000000072",
                workspace.workspace_id,
                "job_request_status",
                enrollment_id=ENROLLMENT_ID,
                command_id=command.request_id,
            ),
            seed_path=seed,
            now=int(time.time()),
            lifetime=300,
        )
        broker.requests.replace(f"{status.request_id}.json", encode_request(status))
        assert broker.process_once(threading.Event()) == 1
        answer = decode_response(broker.responses.read(f"{status.request_id}.json"))
        assert (answer.outcome, answer.reason) == ("refused", "unknown_command")
        assert ledger.verify() == 0
    finally:
        broker.close()
        owner.close()
        ledger.close()
        other.close()
