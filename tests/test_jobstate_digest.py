"""Runner-private payload entries are invisible to payload digests."""

import json
from pathlib import Path

from httk.workflow import (
    JobState,
    TaskManager,
    Workspace,
)
from httk.workflow.protocol import JobSpec, prepare_job_payload
from httk.workflow.transfers import _payload_digest

_SRC = str(Path(__file__).parents[1] / "src")

_STATEFUL_RUNNER = f"""#!/usr/bin/env python3
import sys

sys.path.insert(0, {_SRC!r})

from httk.workflow import Runner

run = Runner("tests.stateful")


@run.step
def only(a):
    a.state["energy"] = -12.5
    a.state.merge({{"converged": True}})
    (a.workdir / "energy.txt").write_text(str(a.state["energy"]), encoding="utf-8")
    a.succeed()


raise SystemExit(run.main())
"""


def _submit(workspace: Workspace, root: Path, placement: str = "project/stateful") -> str:
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(_STATEFUL_RUNNER, encoding="utf-8")
    runner.chmod(0o755)
    job = prepare_job_payload(
        payload,
        JobSpec(
            name="Stateful job",
            workflow="tests.stateful",
            runner_path="files/runner",
            tag="stateful",
            initial_step="only",
            maximum_attempts_per_activation=1,
        ),
    )
    workspace.submit(payload, placement)
    return job.id


def test_job_state_and_attempt_control_do_not_change_the_payload_digest(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    job_id = _submit(workspace, tmp_path / "source")
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    payload = workspace.payload_path(marker.placement, marker.job_key)
    digest = workspace.payload_digest(marker)

    state = JobState(payload)
    state["energy"] = -12.5
    state.merge({"converged": True, "history": [1, 2, 3]})
    assert (payload / ".httk-job" / "state.json").is_file()
    (payload / ".httk-attempt.0f0f0f0f").mkdir()
    (payload / ".httk-attempt.0f0f0f0f" / "stdout.log").write_text("noise\n", encoding="utf-8")
    # Neither the state a runner keeps nor the control directory of an attempt is
    # part of the immutable job, so neither can move the payload digest.
    assert workspace.payload_digest(marker) == digest

    (payload / "files" / "extra.txt").write_text("real payload change\n", encoding="utf-8")
    assert workspace.payload_digest(marker) != digest


def test_a_job_that_wrote_state_still_transfers_and_verifies(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source-workspace")
    destination = Workspace.initialize(tmp_path / "destination-workspace")
    job_id = _submit(source, tmp_path / "source")
    with TaskManager(source, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    marker = source.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"
    payload = source.payload_path(marker.placement, marker.job_key)
    assert json.loads((payload / ".httk-job" / "state.json").read_text(encoding="utf-8")) == {
        "energy": -12.5,
        "converged": True,
    }
    # The job ran, so its payload now holds both an attempt control directory and
    # job state; the bundle digest covers neither, and the import verifies it.
    assert sorted(payload.glob(".httk-attempt.*"))
    digest = source.payload_digest(marker)

    bundle = source.detach(job_id, destination_workspace_id=destination.workspace_id)
    manifest = json.loads((bundle / ".httk-transfer" / "manifest.json").read_text(encoding="utf-8"))
    acknowledgement = destination.import_bundle(bundle)
    assert acknowledgement["job_id"] == job_id

    imported = destination.find_marker_by_id(job_id)
    assert imported is not None and imported.kind == "succeeded"
    imported_payload = destination.payload_path(imported.placement, imported.job_key)
    assert json.loads((imported_payload / ".httk-job" / "state.json").read_text(encoding="utf-8")) == {
        "energy": -12.5,
        "converged": True,
    }
    assert destination.payload_digest(imported) == digest
    # The bundle digest is stable across the transfer even though the payload
    # carries runner-private entries that the digest deliberately ignores.
    assert _payload_digest(imported_payload) == manifest["payload_sha256"]
