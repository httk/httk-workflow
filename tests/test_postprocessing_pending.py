"""The postprocess CLI job selectors (legacy until the CLI port, C5b; split out of ``test_postprocessing.py``)."""

import json
from pathlib import Path

from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow.workflow_cli import command
from test_postprocessing import _finished


def test_postprocess_cli_targets_a_single_job_by_id(tmp_path: Path, capsys) -> None:
    package, _provider, workspace, _job, record = _finished(tmp_path)
    context = CLIContext("httk", tmp_path)
    workspace_name = register_ws(context, workspace.root, "postprocess-one")
    assert (
        command(
            [
                "postprocess",
                "--workspace",
                workspace_name,
                "--script",
                "report",
                "--workflow-dir",
                str(package),
                "--json",
                record.job_id,
            ],
            context,
        )
        == 0
    )
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert [line["job_id"] for line in lines] == [record.job_id]
    assert (Path(lines[0]["output_dir"]) / "report.json").is_file()
