"""Transfer tests split out of ``test_trust_and_hygiene.py``: they drive the removed transfer stack."""

import base64
import json
import uuid
from pathlib import Path

import pytest
from httk.core.identity import (
    identity_public_key,
    verify_document,
)

from conftest import configure_identity
from httk.workflow import Workspace
from httk.workflow.errors import FormatError


def _isolate(tmp_path: Path, monkeypatch) -> None:
    """Keep every test out of the invoking user's real configuration."""

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)


def _payload(root: Path, *, pool: str = "default") -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / f"payload-{job_id[:8]}"
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
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
                "claim": {"pool": pool, "required_capabilities": []},
                "retry_policy": {"retry_on": []},
                "resources": {},
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


def test_transfer_acknowledgement_is_signed_and_a_forged_one_is_refused(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    payload, job_id = _payload(tmp_path)
    source.submit(payload, "jobs")
    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)

    acknowledgement = destination.import_bundle(bundle)
    assert acknowledgement["operator_key"] == identity_public_key()
    assert verify_document(acknowledgement).valid

    forged = {**acknowledgement, "signature": base64.b64encode(b"\0" * 64).decode("ascii")}
    with pytest.raises(FormatError, match="signature is invalid"):
        source.acknowledge_transfer(forged)

    assert bundle.is_dir()  # A rejected acknowledgement cannot reclaim the source.
    retired = source.acknowledge_transfer(acknowledgement)
    assert retired.is_dir()  # Kept for retention.trash_days.
