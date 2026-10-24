"""The collection gate of unlocked jobs (debug mode): degraded unless forced, then collected from the runner copy."""

import json
from pathlib import Path
from unittest.mock import ANY

import pytest
from httk.core import DataRecord
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel, _store, collect, removal
from httk.workflow.collecting import _UNLOCKED_REASON
from httk.workflow.postprocessing import collect_postprocess
from httk.workflow.workflow_cli import command
from test_collect_fallback import _finished
from v3_helpers import cli_owner, find, only, state_of


def _unlocked(tmp_path: Path, *, copy: bool = True) -> tuple[Workspace, _kernel.JobRef]:
    """A workspace whose one succeeded job was unsealed and unlocked (with its runner copy unless not *copy*)."""

    workspace, _installed = _finished(tmp_path)
    ref = only(workspace, "succeeded")
    with cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "unseal", "debug") is None
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "unlock", "debug", copy=copy) is None
    return workspace, find(workspace, ref.job_id)


def test_an_unlocked_job_is_degraded_unless_forced(tmp_path: Path) -> None:
    workspace, ref = _unlocked(tmp_path)
    (gated,) = collect(workspace, allow_job_collector=True)
    assert gated.missing_collector == _UNLOCKED_REASON and gated.unfulfilled == ("relaxed_structure",)
    assert gated.record.job["unlocked"] == {"at": ANY, "operator": "cli", "reason": "debug"}
    # The forced job collector is the copy's, unverified: an edit there is what runs.
    hook = ref.path / _store.RUNNER_COPY / "collect.py"
    hook.write_text(hook.read_text(encoding="utf-8").replace('"total", 1.5', '"total", 2.5'), encoding="utf-8")
    (forced,) = collect(workspace, allow_job_collector=True, force=True)
    assert forced.missing_collector is None and forced.outputs["relaxed_structure"] == DataRecord.from_value(
        "https://example.test/energy", "total", 2.5
    )


def test_a_forced_job_without_a_copy_collects_with_the_reinstalled_store_tree(tmp_path: Path) -> None:
    workspace, ref = _unlocked(tmp_path, copy=False)
    assert not (ref.path / _store.RUNNER_COPY).exists()
    hook = tmp_path / "package" / "collect.py"
    hook.write_text(hook.read_text(encoding="utf-8").replace('"total", 1.5', '"total", 2.5'), encoding="utf-8")
    with cli_owner(workspace) as owner:
        reinstalled = _store.install(workspace, owner, tmp_path / "package")
    # The job's pin names the old tree: a locked job would degrade ("reinstalled since the job ran").
    pin = state_of(find(workspace, ref.job_id)).workflow_pin
    assert pin is not None and pin["tree_sha256"] != reinstalled.record["tree_sha256"]
    (forced,) = collect(workspace, allow_job_collector=True, force=True)
    assert forced.missing_collector is None
    assert forced.outputs["relaxed_structure"] == DataRecord.from_value("https://example.test/energy", "total", 2.5)


def test_collect_into_stores_an_unlocked_job_only_with_force(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("httk.store")
    pytest.importorskip("httk.atomistic")
    workspace, _ref = _unlocked(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "unlocked")
    into = ["--allow-job-collector", "--into", str(tmp_path / "results.sqlite"), "--id-base", "httk.collect"]

    assert command(["collect", "--workspace", name, *into], context) == 1
    report, summary = (json.loads(line) for line in capsys.readouterr().out.splitlines())
    assert report["missing_collector"] == _UNLOCKED_REASON and report["skipped"] == "degraded"
    assert report["stored"] is None and report["unlocked"] is True and summary["degraded"] == 1

    assert command(["collect", "--workspace", name, "--force", *into], context) == 0
    report, summary = (json.loads(line) for line in capsys.readouterr().out.splitlines())
    assert report["unlocked"] is True and report["stored"]["run"] and summary["collected"] == 1

    assert command(["collect", "--workspace", name, "--raw"], context) == 0
    raw = json.loads(capsys.readouterr().out.splitlines()[0])
    assert raw["job"]["unlocked"]["reason"] == "debug"


def test_postprocess_collection_of_an_unlocked_job_is_an_error_unless_forced(tmp_path: Path) -> None:
    workspace, _ref = _unlocked(tmp_path)
    (gated,) = collect_postprocess(workspace, "analyse")
    assert gated.error == _UNLOCKED_REASON
    (forced,) = collect_postprocess(workspace, "analyse", force=True)
    assert forced.error is None and forced.skipped is not None
