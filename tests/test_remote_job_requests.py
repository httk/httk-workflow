"""``job request`` against a workspace on a remote, end to end over the ``local`` adapter.

Ported from the remote-operator-request half of the pre-redesign ``test_fetch.py`` (its offer/fetch/retire half
was the legacy transfer protocol and is gone; ``test_job_transfer.py`` covers moving jobs).
"""

import json
from dataclasses import dataclass
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import (
    ensure_identity_key,
    identity_config_path,
    identity_key_paths,
    identity_public_key,
    write_identity_config,
)

from conftest import configure_identity, fake_remote
from httk.workflow import Workspace, _kernel
from httk.workflow._state import read_state_unowned
from httk.workflow.projects import initialize_project
from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import command
from test_moving import WORKFLOW, place
from v3_helpers import job_mapping


@dataclass(frozen=True)
class Pair:
    """A project reaching the workspace ``cluster:station`` that holds a ready and a succeeded job."""

    remote: Workspace
    context: CLIContext
    pending: str
    succeeded: str


@pytest.fixture
def pair(tmp_path: Path) -> Pair:
    configure_identity()
    project = tmp_path / "project"
    initialize_project(project, name="requests")
    fake_remote(project, template="local")
    create_workspace("station", tmp_path / "station")
    remote = Workspace(tmp_path / "station", durable=False)
    pending = place(remote, job_mapping(WORKFLOW, {"start": "succeed"}, tag="succeeding"), "ready").job_id
    succeeded = place(remote, job_mapping(WORKFLOW, {"start": "succeed"}, tag="done"), "succeeded").job_id
    return Pair(remote, CLIContext("httk", project), pending, succeeded)


def requests(pair: Pair) -> list[dict[str, object]]:
    return [json.loads(path.read_bytes()) for path in sorted((pair.remote.control / "requests").glob("*.json"))]


def applied(pair: Pair, job_id: str) -> list[dict[str, object]]:
    ref = _kernel.locate(pair.remote, job_id, placement_hint=None, exhaustive=True)
    assert ref is not None
    doc, _damage = read_state_unowned(ref.path / "state.json")
    assert doc is not None
    return [dict(entry) for entry in doc.history_tail if entry.get("event") in {"request_applied", "request_dropped"}]


def request(pair: Pair, *arguments: str) -> int:
    return command(["job", "request", "pause", "--workspace", "cluster:station", *arguments], pair.context)


def test_job_request_forwards_to_remote_workspace(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, pair.pending, "--reason", "r", "--adapter-timeout", "60") == 0
    output = capsys.readouterr().out
    assert "\tapplied\t" in output
    (document,) = applied(pair, pair.pending)
    assert document["action"] == "pause"
    assert document["operator"] == "Local User <local@example.test>" and document["reason"] == "r"
    assert document["operator_key"] == identity_public_key(identity_key_paths("local")[0])


def test_job_request_remote_uses_selected_identity(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    write_identity_config(
        {
            "identities": {
                "local": {"name": "Local User", "email": "local@example.test"},
                "bot": {"name": "Build Bot", "email": "bot@example.test"},
            },
            "default_identity": "local",
        }
    )
    ensure_identity_key("bot")
    assert request(pair, pair.pending, "--operator", "bot", "--reason", "r") == 0
    capsys.readouterr()
    (document,) = applied(pair, pair.pending)
    assert document["operator"] == "Build Bot <bot@example.test>"
    assert document["operator_key"] == identity_public_key(identity_key_paths("bot")[0])


def test_job_request_remote_accepts_tag_prefix_selector(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, f"succeeding--{pair.pending[:8]}", "--reason", "tag selector") == 0
    output = capsys.readouterr().out
    assert f"{pair.pending}\t" in output and "\tapplied\t" in output


def test_job_request_remote_literal_on_unconfigured_machine_is_unsigned(
    pair: Pair, capsys: pytest.CaptureFixture[str]
) -> None:
    identity_config_path().unlink()
    assert request(pair, pair.pending, "--operator", "External <external@example.test>", "--reason", "r") == 0
    capsys.readouterr()
    (document,) = applied(pair, pair.pending)
    assert "operator_key" not in document and "signature" not in document


def test_job_request_remote_tamper_is_rejected_unpublished(
    pair: Pair, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    original = job_cli.sign_document

    def tamper(document: dict[str, object], *, seed_path: Path) -> dict[str, object]:
        signed = original(document, seed_path=seed_path)
        signed["reason"] = "tampered"
        return signed

    monkeypatch.setattr(job_cli, "sign_document", tamper)
    assert request(pair, pair.pending, "--reason", "r") == 2
    assert "the signature does not verify" in capsys.readouterr().err
    assert requests(pair) == []


def test_job_request_forwards_leading_dash_reason(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, pair.pending, "--operator=local", "--reason=-maintenance") == 0
    capsys.readouterr()
    (document,) = applied(pair, pair.pending)
    assert document["reason"] == "-maintenance"


def test_job_request_forwards_multiple_ids(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, pair.pending, pair.succeeded, "--reason", "r") == 1
    output = capsys.readouterr().out
    assert f"{pair.pending}\t" in output and "\tapplied\t" in output
    assert f"{pair.succeeded}\t" in output and "\trefused\t" in output
    assert applied(pair, pair.pending)[0]["operator_key"] == identity_public_key(identity_key_paths("local")[0])
    assert requests(pair) == []


def test_job_request_remote_wait_relays_far_side_result(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, pair.pending, "--reason", "r", "--wait", "--timeout", "0") == 0
    captured = capsys.readouterr()
    assert "\tapplied\t" in captured.out and "state=paused" in captured.out
    assert not captured.err


def test_job_request_relays_remote_failure(pair: Pair, capsys: pytest.CaptureFixture[str]) -> None:
    assert request(pair, "does-not-exist", "--reason", "r") == 2
    error = capsys.readouterr().err
    assert "no job in" in error and "does-not-exist" in error
    assert "remote request envelope build failed (exit 2)" in error
