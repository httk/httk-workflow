"""User configuration has ordered, atomic key and list edits."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow.configuration import (
    config_path,
    configure_config,
    machine_names,
    read_config,
    set_config_key,
    write_config,
)
from httk.workflow.workflow_cli import command


def test_user_configuration_round_trip(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    assert (
        command(
            [
                "config",
                "configure",
                "--set",
                "machine_names=one,two",
                "--add",
                "machine_names=two,three",
                "--remove",
                "machine_names=one",
            ],
            context,
        )
        == 0
    )
    assert machine_names() == {"two", "three"}
    capsys.readouterr()
    assert command(["config", "show", "machine_names", "--json"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"machine_names": "two,three"}
    assert command(["config", "configure", "--remove", "machine_names=two,three"], context) == 0
    assert "machine_names" not in read_config()
    assert command(["config", "configure", "--add", "machine_names=last", "--unset", "machine_names"], context) == 0
    assert machine_names() == frozenset()


def test_invalid_user_configuration_is_not_partially_saved(tmp_path: Path) -> None:
    context = CLIContext("httk", tmp_path)
    assert command(["config", "set", "machine_names", "original"], context) == 0
    saved = config_path().read_bytes()
    for invalid in (
        ["--set", "unknown=value"],
        ["--set", "format=value"],
        ["--set", "machine_names=a,,b"],
        ["--remove", "machine_names=missing"],
        ["--unset", "machine_names=value"],
    ):
        assert command(["config", "configure", "--add", "machine_names=added", *invalid], context) == 2
        assert config_path().read_bytes() == saved


def test_config_api_rejects_invalid_keys_and_stored_lists() -> None:
    with pytest.raises(ValueError, match="unknown configuration key"):
        set_config_key("machine_names=x", "value")
    assert not config_path().exists()
    for malformed in ("one,,two", "", " "):
        write_config({"machine_names": malformed})
        saved = config_path().read_bytes()
        with pytest.raises(ValueError, match="empty name"):
            configure_config((("add", "machine_names=other"),))
        assert config_path().read_bytes() == saved


def test_empty_configure_is_refused_without_writing(tmp_path: Path) -> None:
    assert command(["config", "configure"], CLIContext("httk", tmp_path)) == 2
    assert not config_path().exists()
