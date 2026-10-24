"""The collection gate of unlocked jobs (debug mode): degraded unless forced, then collected from the runner copy."""

import json
import re
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
from v3_helpers import cli_owner, find, only, run, state_of


def _edit_total(hook: Path, total: str) -> None:
    hook.write_text(
        re.sub(r'"total", [0-9.]+', f'"total", {total}', hook.read_text(encoding="utf-8")), encoding="utf-8"
    )


def _unlocked(tmp_path: Path, *, copy: bool = True, total: str | None = None) -> tuple[Workspace, _kernel.JobRef]:
    """A workspace whose one succeeded job was unsealed and unlocked.

    With *copy*, its runner copy's hook gets the energy *total* (when given) and the job runs again from it.
    """

    workspace, _installed = _finished(tmp_path)
    ref = only(workspace, "succeeded")
    with cli_owner(workspace) as owner:
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "unseal", "debug") is None
        assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "unlock", "debug", copy=copy) is None
        if copy:
            if total is not None:
                _edit_total(find(workspace, ref.job_id).path / _store.RUNNER_COPY / "collect.py", total)
            assert removal.request_now(workspace, owner, find(workspace, ref.job_id), "restart", "debug") is None
    if copy:
        run(workspace)
        assert (state_of(find(workspace, ref.job_id)).workflow_pin or {}).get("source") == "copy"
    return workspace, find(workspace, ref.job_id)


def test_an_unlocked_job_is_degraded_unless_forced(tmp_path: Path) -> None:
    # The copy's hook was edited before the job ran again from it: the forced job collector is that hook.
    workspace, _ref = _unlocked(tmp_path, total="2.5")
    (gated,) = collect(workspace, allow_job_collector=True)
    assert gated.missing_collector == _UNLOCKED_REASON and gated.unfulfilled == ("relaxed_structure",)
    assert gated.record.job["unlocked"] == {"at": ANY, "operator": "cli", "reason": "debug"}
    (forced,) = collect(workspace, allow_job_collector=True, force=True)
    assert forced.missing_collector is None and forced.outputs["relaxed_structure"] == DataRecord.from_value(
        "https://example.test/energy", "total", 2.5
    )


def test_a_copy_edited_after_the_run_is_never_collected_from(tmp_path: Path) -> None:
    workspace, ref = _unlocked(tmp_path)
    pin = state_of(ref).workflow_pin
    assert pin is not None
    # What the job (or anyone) changes in its copy after its last launch is not run outside its sandbox.
    _edit_total(ref.path / _store.RUNNER_COPY / "collect.py", "3.5")
    actual = _store.source_digest(ref.path / _store.RUNNER_COPY, declared_by=tmp_path / "package")
    (forced,) = collect(workspace, allow_job_collector=True, force=True)
    assert forced.missing_collector == (
        f"runner copy modified since the job ran: digest {actual} does not match pinned {pin['tree_sha256']}"
    )
    assert forced.unfulfilled == ("relaxed_structure",)


def test_a_copy_the_last_attempt_did_not_run_from_is_never_collected_from(tmp_path: Path) -> None:
    workspace, ref = _unlocked(tmp_path, copy=False)
    # A no-copy unlock with a copy placed later, before the job runs again.
    _store._copy_package(tmp_path / "package", ref.path / _store.RUNNER_COPY, None)
    (forced,) = collect(workspace, allow_job_collector=True, force=True)
    assert (
        forced.missing_collector
        == "the job's last attempt did not run from its runner copy; restart it before collecting"
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
