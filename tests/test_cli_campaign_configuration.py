"""Campaign edits validate the whole candidate and preserve workspace contents."""

import json
from pathlib import Path

from httk.core.cli import CLIContext

from httk.workflow.campaigns import read_campaign
from httk.workflow.projects import initialize_project, read_project, write_project_section
from httk.workflow.workflow_cli import command


def test_campaign_configuration_and_removal(tmp_path: Path, capsys) -> None:
    initialize_project(tmp_path, name="campaign")
    write_project_section(tmp_path, "unrelated", {"keep": True})
    context = CLIContext("httk", tmp_path)
    assert command(["campaign", "init", "--partition", "a=alpha"], context) == 0
    assert command(["campaign", "set", "partitions.b=beta", "assignment=explicit"], context) == 0
    assert read_campaign(tmp_path).partitions == {"a": "alpha", "b": "beta"}
    assert read_campaign(tmp_path).assignment == "explicit"
    assert command(["campaign", "unset", "partitions.a", "assignment"], context) == 0
    assert read_campaign(tmp_path).partitions == {"b": "beta"}
    assert read_campaign(tmp_path).assignment == "hash"
    capsys.readouterr()
    assert command(["campaign", "show", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"partitions": {"b": "beta"}, "assignment": "hash"}
    retained = tmp_path / "workspace" / "job.txt"
    retained.parent.mkdir()
    retained.write_text("job payload")
    assert command(["campaign", "remove", "--yes"], context) == 0
    assert read_campaign(tmp_path).partitions == {}
    assert read_project(tmp_path)["unrelated"] == {"keep": True}
    assert retained.read_text() == "job payload"


def test_campaign_invalid_and_sealed_edits_preserve_saved_configuration(tmp_path: Path) -> None:
    initialize_project(tmp_path, name="campaign")
    context = CLIContext("httk", tmp_path)
    assert command(["campaign", "init", "--partition", "a=alpha"], context) == 0
    saved = (tmp_path / "httk_project" / "project.json").read_bytes()
    for empty in (["campaign", "set"], ["campaign", "unset"], ["campaign", "configure", "--set", "assignment=hash"]):
        assert command(empty, context) == 2
    assert (tmp_path / "httk_project" / "project.json").read_bytes() == saved
    for changes in (
        ["set", "partitions.b=beta", "assignment=invalid"],
        ["unset", "partitions.missing"],
        ["unset", "partitions.a=alpha"],
        ["set", "partitions.b="],
        ["set", "unknown=value"],
        ["set", "partitions.b"],
    ):
        assert command(["campaign", *changes], context) == 2
        assert (tmp_path / "httk_project" / "project.json").read_bytes() == saved
    (tmp_path / "httk_project" / "seal.json").write_text("{}")
    assert command(["campaign", "set", "partitions.b=beta"], context) == 2
    assert command(["campaign", "remove", "--yes"], context) == 2
    assert (tmp_path / "httk_project" / "project.json").read_bytes() == saved
