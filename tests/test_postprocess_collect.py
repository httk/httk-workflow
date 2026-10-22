"""Reading declared postprocess outputs back through their receipts."""

import json
from dataclasses import replace
from pathlib import Path, PurePosixPath

import pytest
from httk.core.digests import sha256_file

from httk.workflow import CollectedPostprocess, collect_postprocess
from httk.workflow import postprocessing as postprocessing_module
from httk.workflow.postprocessing import run_postprocess_script
from test_postprocessing import _finished

_WRITING_SCRIPT = """#!/bin/sh
set -e
mkdir -p figures/sub ../elsewhere
echo summary > summary.json
echo one > figures/one.png
echo two > figures/sub/two.png
echo hidden > .hidden.png
echo dot > figures/.dot.png
echo outside > ../elsewhere/three.png
ln -sf ../summary.json figures/link.png
ln -sfn sub figures/alias
ln -sfn ../../elsewhere figures/linked
"""


def _setup(tmp_path: Path, outputs: tuple[str, ...] | None):
    """Return a finished job, its package, and a provider whose ``report`` declares *outputs*."""

    package, provider, workspace, job, record = _finished(tmp_path)
    script = package / "scripts" / "report.sh"
    script.write_text(_WRITING_SCRIPT, encoding="utf-8")
    script.chmod(0o755)
    declared = replace(
        provider, postprocess_scripts={"report": {**provider.postprocess_scripts["report"], "outputs": outputs}}
    )
    return package, declared, workspace, job, record


def _only(workspace, script: str = "report") -> CollectedPostprocess:
    (item,) = list(collect_postprocess(workspace, script))
    return item


def test_collects_declared_files_with_workspace_urls(tmp_path: Path) -> None:
    outputs = ("summary.json", "**/*.png", "figures/*/*.png", "*")
    _package, provider, workspace, job, record = _setup(tmp_path, outputs)
    result = run_postprocess_script(provider, "report", record)
    assert result.returncode == 0

    item = _only(workspace)

    assert item.error is None and item.skipped is None
    assert item.record.job_id == job.job_id and item.script == "report"
    assert item.workflow_id == provider.workflow_id
    assert item.started_at is not None and item.finished_at is not None and item.started_at.tzinfo is not None
    # Symlinked files and directories, dot-prefixed paths and the receipt are never collected.
    assert [path for path, _ in item.files] == ["figures/one.png", "figures/sub/two.png", "summary.json"]
    for relative, file in item.files:
        path = result.output_dir / relative
        expected = PurePosixPath(
            "data", workspace.root.name, "postprocess", *record.placement.parts, record.job_key, "report", relative
        )
        assert file.url == expected.as_posix()
        assert file.name == path.name
        assert file.size == path.stat().st_size
        assert file.sha256 == sha256_file(path)


def test_skip_reasons(tmp_path: Path) -> None:
    package, provider, workspace, _job, record = _setup(tmp_path, ("*.nothing",))
    assert _only(workspace).skipped == "not postprocessed with report"

    run_postprocess_script(provider, "report", record)
    assert _only(workspace).skipped == "no output files"

    undeclared = replace(
        provider, postprocess_scripts={"report": {**provider.postprocess_scripts["report"], "outputs": None}}
    )
    run_postprocess_script(undeclared, "report", record)
    assert _only(workspace).skipped == "report declares no outputs"

    script = package / "scripts" / "report.sh"
    script.write_text("#!/bin/sh\nexit 3\n", encoding="utf-8")
    run_postprocess_script(provider, "report", record)
    item = _only(workspace)
    assert item.skipped == "postprocess report failed (exit 3)" and item.error is None and item.files == ()


def test_missing_literal_output_is_an_error(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _setup(tmp_path, ("summary.json", "missing.txt"))
    run_postprocess_script(provider, "report", record)
    item = _only(workspace)
    assert item.error is not None and "missing.txt" in item.error and item.files == ()


@pytest.mark.parametrize("literal", ["figures/link.png", "figures/alias/two.png"])
def test_literal_reached_through_a_symlink_is_an_error(tmp_path: Path, literal: str) -> None:
    _package, provider, workspace, _job, record = _setup(tmp_path, (literal,))
    run_postprocess_script(provider, "report", record)
    item = _only(workspace)
    assert item.error is not None and literal in item.error


def test_invalid_receipt_is_an_error(tmp_path: Path) -> None:
    _package, provider, workspace, _job, record = _setup(tmp_path, ("summary.json",))
    receipt = run_postprocess_script(provider, "report", record).receipt
    document = json.loads(receipt.read_text(encoding="utf-8"))
    for broken in (
        "not json",
        json.dumps({**document, "format_version": 2}),
        json.dumps({**document, "outputs": ["../x"]}),
        json.dumps({**document, "outputs": []}),
    ):
        receipt.write_text(broken, encoding="utf-8")
        item = _only(workspace)
        assert item.error is not None and item.skipped is None


def test_receipt_changed_while_hashing_is_an_error(tmp_path: Path, monkeypatch) -> None:
    _package, provider, workspace, _job, record = _setup(tmp_path, ("summary.json",))
    receipt = run_postprocess_script(provider, "report", record).receipt
    original = postprocessing_module.sha256_file

    def rewriting(path):
        receipt.write_text(receipt.read_text(encoding="utf-8").replace('"returncode":0', '"returncode": 0'))
        return original(path)

    monkeypatch.setattr(postprocessing_module, "sha256_file", rewriting)
    item = _only(workspace)
    assert item.error is not None and item.error.startswith("postprocess output changed during collection")
    assert item.files == ()


def test_refuses_a_postprocess_root_outside_the_workspace_and_bad_script_names(tmp_path: Path) -> None:
    _package, _provider, workspace, _job, _record = _setup(tmp_path, ("summary.json",))
    with pytest.raises(ValueError, match="single path component"):
        collect_postprocess(workspace, "a/b")
    workspace.set_setting("postprocess.directory", str(tmp_path / "analysis"))
    with pytest.raises(ValueError, match="postprocess.directory"):
        collect_postprocess(workspace, "report")
